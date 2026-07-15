// Package unfurl materializes link previews (P-15): a named event-log
// consumer watches message.created, extracts the first external links from
// the message AST, fetches page metadata through the SSRF-guarded egress
// client (the ONLY path these attacker-chosen URLs may ride), and caches the
// result globally by URL hash. Previews add no visibility surface of their
// own — they attach to messages and travel under the message read ACL.
package unfurl

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"mime"
	"net/url"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/abhinavjha0239/weft/internal/auth"
	"github.com/abhinavjha0239/weft/internal/db"
	"github.com/abhinavjha0239/weft/internal/domain/perms"
	"github.com/abhinavjha0239/weft/internal/enum"
	"github.com/abhinavjha0239/weft/internal/eventlog"
	"github.com/abhinavjha0239/weft/internal/platform/apperr"
	"github.com/abhinavjha0239/weft/internal/platform/egress"
)

const (
	statusOK         int16 = 1
	statusFailed     int16 = 2
	statusDisallowed int16 = 3

	// Failed fetches retry after an hour; ok content and guard verdicts
	// (which don't flap) live a day.
	ttlOK     = 24 * time.Hour
	ttlFailed = time.Hour

	maxLinksPerMessage = 2
	maxBodyBytes       = 1 << 20 // parse at most 1 MiB of HTML

	maxTitleRunes = 200
	maxDescRunes  = 500
	maxSiteRunes  = 100

	// settingsKey is the org.settings toggle unfurl OWNS (the per-subsystem
	// settings-ownership pattern): ABSENT = enabled (Zulip/Slack default-on).
	settingsKey = "link_previews_enabled"
)

type Service struct {
	pool   *pgxpool.Pool
	client *egress.Client
	// perms gates the admin toggle endpoints (SetPerms; nil is fine for
	// tests that never hit them — the consumer path needs no perms).
	perms *perms.Service
	// baseHost is this deployment's own host (from WEFT_BASE_URL): links to
	// ourselves are never unfurled (nothing to learn, and file links are the
	// attachment lane's business).
	baseHost string
}

func New(pool *pgxpool.Pool, client *egress.Client) *Service {
	return &Service{pool: pool, client: client}
}

// SetPerms wires the permission service for the admin toggle.
func (s *Service) SetPerms(p *perms.Service) { s.perms = p }

// SetBaseHost wires this deployment's own host for the self-link skip.
func (s *Service) SetBaseHost(host string) { s.baseHost = host }

// Enabled reads the org toggle; an absent key means enabled.
func (s *Service) Enabled(ctx context.Context, orgID int64) (bool, error) {
	var enabled bool
	if err := s.pool.QueryRow(ctx, `
		SELECT COALESCE((settings->>$2)::boolean, true) FROM org WHERE id = $1`,
		orgID, settingsKey).Scan(&enabled); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return false, nil
		}
		return false, apperr.Internal("read link-preview toggle", err)
	}
	return enabled, nil
}

// LinkPreviewSetting is the admin read model.
type LinkPreviewSetting struct {
	Enabled bool `json:"enabled"`
}

// LinkPreviewsSetting returns the org toggle. manage_org-gated (the
// storage-quota admin precedent).
func (s *Service) LinkPreviewsSetting(ctx context.Context, actor auth.Identity) (LinkPreviewSetting, error) {
	if s.perms == nil {
		return LinkPreviewSetting{}, apperr.Internal("link previews", errors.New("perms not wired"))
	}
	if err := db.WithTx(ctx, s.pool, func(tx pgx.Tx) error {
		return s.perms.Require(ctx, tx, actor, perms.VerbManageOrg, perms.OrgScope(actor.OrgID))
	}); err != nil {
		return LinkPreviewSetting{}, err
	}
	enabled, err := s.Enabled(ctx, actor.OrgID)
	if err != nil {
		return LinkPreviewSetting{}, err
	}
	return LinkPreviewSetting{Enabled: enabled}, nil
}

// SetLinkPreviews flips the org toggle, event-logged (the quota precedent).
func (s *Service) SetLinkPreviews(ctx context.Context, actor auth.Identity, enabled bool) error {
	if s.perms == nil {
		return apperr.Internal("link previews", errors.New("perms not wired"))
	}
	return db.WithTx(ctx, s.pool, func(tx pgx.Tx) error {
		if err := s.perms.Require(ctx, tx, actor, perms.VerbManageOrg, perms.OrgScope(actor.OrgID)); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `
			UPDATE org SET settings = jsonb_set(settings, ARRAY[$2::text], to_jsonb($3::boolean))
			WHERE id = $1`,
			actor.OrgID, settingsKey, enabled); err != nil {
			return apperr.Internal("set link-preview toggle", err)
		}
		if _, err := eventlog.Append(ctx, tx, eventlog.Event{
			OrgID: actor.OrgID, ActorKind: enum.ActorHuman, ActorID: &actor.UserID,
			EntityType: enum.EntityOrg, EntityID: actor.OrgID, Verb: "org.link_previews_changed",
			Payload: eventlog.MustPayload(map[string]any{"enabled": enabled}),
		}); err != nil {
			return apperr.Internal("append event", err)
		}
		return nil
	})
}

// externalLinks filters a message's link hrefs down to what the unfurler
// fetches: http(s), a real host, not this deployment itself, deduped in
// document order, capped.
func (s *Service) externalLinks(hrefs []string) []string {
	seen := map[string]bool{}
	var out []string
	for _, raw := range hrefs {
		u, err := url.Parse(strings.TrimSpace(raw))
		if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
			continue
		}
		if s.baseHost != "" && strings.EqualFold(u.Host, s.baseHost) {
			continue
		}
		norm := u.String()
		if seen[norm] {
			continue
		}
		seen[norm] = true
		out = append(out, norm)
		if len(out) == maxLinksPerMessage {
			break
		}
	}
	return out
}

// previewFor returns the cache row id for a URL, fetching on miss. ok
// reports whether the row is renderable (status 1) — failed/disallowed rows
// exist to suppress refetching, never to associate.
func (s *Service) previewFor(ctx context.Context, rawURL string) (id int64, ok bool, err error) {
	sum := sha256.Sum256([]byte(rawURL))
	hash := hex.EncodeToString(sum[:])
	var status int16
	err = s.pool.QueryRow(ctx, `
		SELECT id, status FROM link_preview
		WHERE url_hash = $1 AND expires_at > now()`, hash).Scan(&id, &status)
	if err == nil {
		return id, status == statusOK, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return 0, false, apperr.Internal("preview cache read", err)
	}

	meta, status := s.fetch(ctx, rawURL)
	ttl := ttlOK
	if status == statusFailed {
		ttl = ttlFailed
	}
	if err := s.pool.QueryRow(ctx, `
		INSERT INTO link_preview (url_hash, url, title, description, image_url, site_name, status, expires_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, now() + $8)
		ON CONFLICT (url_hash) DO UPDATE
		  SET title = EXCLUDED.title, description = EXCLUDED.description,
		      image_url = EXCLUDED.image_url, site_name = EXCLUDED.site_name,
		      status = EXCLUDED.status, fetched_at = now(), expires_at = EXCLUDED.expires_at
		RETURNING id`,
		hash, rawURL, meta.title, meta.description, meta.image, meta.site,
		status, ttl).Scan(&id); err != nil {
		return 0, false, apperr.Internal("preview cache write", err)
	}
	return id, status == statusOK, nil
}

// fetch pulls one page through the egress guard and extracts its metadata.
// It never returns an error: every failure mode becomes a cached status —
// guard rejections as disallowed (they don't flap), everything else as
// failed (retried after the short TTL).
func (s *Service) fetch(ctx context.Context, rawURL string) (pageMeta, int16) {
	resp, err := s.client.Get(ctx, rawURL)
	if err != nil {
		if errors.Is(err, egress.ErrDisallowed) {
			return pageMeta{}, statusDisallowed
		}
		return pageMeta{}, statusFailed
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return pageMeta{}, statusFailed
	}
	mediaType, _, _ := mime.ParseMediaType(resp.Header.Get("Content-Type"))
	if mediaType != "text/html" && mediaType != "application/xhtml+xml" {
		return pageMeta{}, statusFailed
	}
	meta := parseMeta(io.LimitReader(resp.Body, maxBodyBytes))
	meta.title = clean(meta.title, maxTitleRunes)
	meta.description = clean(meta.description, maxDescRunes)
	meta.site = clean(meta.site, maxSiteRunes)
	meta.image = strings.TrimSpace(meta.image)
	if u, err := url.Parse(meta.image); err != nil || (u.Scheme != "http" && u.Scheme != "https") {
		// Only absolute http(s) image URLs are kept; anything else (relative
		// paths, data:, javascript:) is dropped — the string is served to
		// clients verbatim.
		meta.image = ""
	}
	if meta.title == "" && meta.description == "" && meta.image == "" {
		return pageMeta{}, statusFailed
	}
	return meta, statusOK
}
