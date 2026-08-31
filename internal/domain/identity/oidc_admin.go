package identity

import (
	"context"
	"errors"
	"net/url"
	"regexp"
	"strings"
	"time"

	"github.com/coreos/go-oidc/v3/oidc"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/abhinavjha0239/weft/internal/auth"
	"github.com/abhinavjha0239/weft/internal/db"
	"github.com/abhinavjha0239/weft/internal/domain/perms"
	"github.com/abhinavjha0239/weft/internal/enum"
	"github.com/abhinavjha0239/weft/internal/eventlog"
	"github.com/abhinavjha0239/weft/internal/platform/apperr"
	"github.com/abhinavjha0239/weft/internal/platform/egress"
)

// Auth-provider CRUD (P-30) — the manage_auth_providers admin surface
// (P-47: its own verb because it controls who may log in at all). client_secret is
// WRITE-ONLY: it is accepted on create/rotate and never read back (List/GET
// return only has_secret, the invite-token show-once spirit), and it never
// appears in an event payload or a log line. Providers are created DISABLED;
// enabling runs a live discovery probe so a typo'd issuer is rejected at
// config time (422) rather than silently stranding every future login.

// providerNameRe is the per-org url-safe slug (the invite/channel slug family).
var providerNameRe = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{0,31}$`)

// discoveryProbeTimeout bounds the enable-time discovery fetch.
const discoveryProbeTimeout = 15 * time.Second

// AuthProvider is the admin-facing view: never the secret, only has_secret.
type AuthProvider struct {
	ID        int64     `json:"id"`
	Name      string    `json:"name"`
	Issuer    string    `json:"issuer"`
	ClientID  string    `json:"client_id"`
	HasSecret bool      `json:"has_secret"`
	Enabled   bool      `json:"enabled"`
	CreatedAt time.Time `json:"created_at"`
}

type CreateProviderParams struct {
	Name         string
	Issuer       string
	ClientID     string
	ClientSecret string
}

// UpdateProviderParams carries only the fields a PATCH touches; a nil pointer
// leaves the stored value alone. Enabled=true triggers the discovery probe.
type UpdateProviderParams struct {
	Issuer       *string
	ClientID     *string
	ClientSecret *string
	Enabled      *bool
}

// validateIssuer enforces the write-time issuer rules: a parseable https URL
// (OIDC issuers are always https) that also passes the egress static shape
// check (no userinfo, standard ports). The runtime SSRF address-class checks
// still run at every dial; this is the early operator feedback.
func validateIssuer(issuer string) error {
	u, err := url.Parse(issuer)
	if err != nil || u.Scheme != "https" {
		return apperr.Invalid("issuer must be an https URL")
	}
	if err := egress.VetURLShape(issuer); err != nil {
		return apperr.Invalid("issuer URL not allowed: " + err.Error())
	}
	return nil
}

// probeDiscovery fetches issuer/.well-known/openid-configuration through the
// egress client. Success is the precondition for enabling a provider.
func (s *Service) probeDiscovery(ctx context.Context, issuer string) error {
	if s.oidcEgress == nil {
		return errors.New("oidc egress client not configured")
	}
	pctx, cancel := context.WithTimeout(
		oidc.ClientContext(ctx, s.oidcEgress.HTTPClient()), discoveryProbeTimeout)
	defer cancel()
	_, err := oidc.NewProvider(pctx, issuer)
	return err
}

// CreateAuthProvider registers a DISABLED provider (manage_auth_providers). The name is a
// per-org slug, the issuer must be https + shape-valid, and a client_secret is
// required (confidential client; public/PKCE-only clients are a recorded gap).
func (s *Service) CreateAuthProvider(ctx context.Context, actor auth.Identity, p CreateProviderParams) (AuthProvider, error) {
	p.Name = strings.TrimSpace(p.Name)
	if !providerNameRe.MatchString(p.Name) {
		return AuthProvider{}, apperr.Invalid("name must match ^[a-z0-9][a-z0-9-]{0,31}$")
	}
	p.Issuer = strings.TrimSpace(p.Issuer)
	if err := validateIssuer(p.Issuer); err != nil {
		return AuthProvider{}, err
	}
	p.ClientID = strings.TrimSpace(p.ClientID)
	if p.ClientID == "" {
		return AuthProvider{}, apperr.Invalid("client_id is required")
	}
	if p.ClientSecret == "" {
		return AuthProvider{}, apperr.Invalid("client_secret is required")
	}
	out := AuthProvider{Name: p.Name, Issuer: p.Issuer, ClientID: p.ClientID, HasSecret: true}
	err := db.WithTx(ctx, s.pool, func(tx pgx.Tx) error {
		if err := s.perms.Require(ctx, tx, actor, perms.VerbManageAuthProviders,
			perms.OrgScope(actor.OrgID)); err != nil {
			return err
		}
		err := tx.QueryRow(ctx, `
			INSERT INTO auth_provider (org_id, name, issuer, client_id, client_secret)
			VALUES ($1, $2, $3, $4, $5) RETURNING id, created_at`,
			actor.OrgID, p.Name, p.Issuer, p.ClientID, p.ClientSecret).Scan(&out.ID, &out.CreatedAt)
		if err != nil {
			var pgErr *pgconn.PgError
			if errors.As(err, &pgErr) && pgErr.Code == "23505" {
				return apperr.Conflict("a provider with that name already exists")
			}
			return apperr.Internal("create auth provider", err)
		}
		_, err = eventlog.Append(ctx, tx, eventlog.Event{
			OrgID: actor.OrgID, ActorKind: enum.ActorHuman, ActorID: &actor.UserID,
			EntityType: enum.EntityOrg, EntityID: actor.OrgID, Verb: "auth_provider.created",
			Payload: eventlog.MustPayload(map[string]any{
				"provider_id": out.ID, "name": p.Name}),
		})
		return err
	})
	if err != nil {
		return AuthProvider{}, err
	}
	return out, nil
}

// ListAuthProviders returns the org's providers (manage_auth_providers) —
// never a secret,
// only has_secret.
func (s *Service) ListAuthProviders(ctx context.Context, actor auth.Identity) ([]AuthProvider, error) {
	out := []AuthProvider{}
	err := db.WithTx(ctx, s.pool, func(tx pgx.Tx) error {
		if err := s.perms.Require(ctx, tx, actor, perms.VerbManageAuthProviders,
			perms.OrgScope(actor.OrgID)); err != nil {
			return err
		}
		rows, err := tx.Query(ctx, `
			SELECT id, name, issuer, client_id, client_secret <> '', enabled, created_at
			FROM auth_provider WHERE org_id = $1 ORDER BY id`, actor.OrgID)
		if err != nil {
			return apperr.Internal("list auth providers", err)
		}
		defer rows.Close()
		for rows.Next() {
			var ap AuthProvider
			if err := rows.Scan(&ap.ID, &ap.Name, &ap.Issuer, &ap.ClientID,
				&ap.HasSecret, &ap.Enabled, &ap.CreatedAt); err != nil {
				return apperr.Internal("scan auth provider", err)
			}
			out = append(out, ap)
		}
		return rows.Err()
	})
	return out, err
}

// UpdateAuthProvider mutates a provider (manage_auth_providers): rotate the secret, change
// issuer/client_id, and enable/disable. Enabling — or changing the issuer of an
// enabled provider — runs the discovery probe and 422s on failure. The probe
// dials inside the tx; enabling a provider is a rare admin action, not a hot
// path, so briefly holding the connection is acceptable and keeps perms→probe→
// persist atomic.
func (s *Service) UpdateAuthProvider(ctx context.Context, actor auth.Identity, providerID int64, p UpdateProviderParams) (AuthProvider, error) {
	if p.Issuer != nil {
		*p.Issuer = strings.TrimSpace(*p.Issuer)
		if err := validateIssuer(*p.Issuer); err != nil {
			return AuthProvider{}, err
		}
	}
	if p.ClientID != nil {
		*p.ClientID = strings.TrimSpace(*p.ClientID)
		if *p.ClientID == "" {
			return AuthProvider{}, apperr.Invalid("client_id cannot be empty")
		}
	}
	if p.ClientSecret != nil && *p.ClientSecret == "" {
		return AuthProvider{}, apperr.Invalid("client_secret cannot be empty")
	}
	var out AuthProvider
	err := db.WithTx(ctx, s.pool, func(tx pgx.Tx) error {
		if err := s.perms.Require(ctx, tx, actor, perms.VerbManageAuthProviders,
			perms.OrgScope(actor.OrgID)); err != nil {
			return err
		}
		var cur AuthProvider
		var curSecret string
		err := tx.QueryRow(ctx, `
			SELECT id, name, issuer, client_id, client_secret, enabled, created_at
			FROM auth_provider WHERE id = $1 AND org_id = $2 FOR UPDATE`,
			providerID, actor.OrgID).Scan(&cur.ID, &cur.Name, &cur.Issuer,
			&cur.ClientID, &curSecret, &cur.Enabled, &cur.CreatedAt)
		if errors.Is(err, pgx.ErrNoRows) {
			return apperr.NotFound("provider not found")
		}
		if err != nil {
			return apperr.Internal("load auth provider", err)
		}
		newIssuer, newClientID, newSecret, newEnabled := cur.Issuer, cur.ClientID, curSecret, cur.Enabled
		if p.Issuer != nil {
			newIssuer = *p.Issuer
		}
		if p.ClientID != nil {
			newClientID = *p.ClientID
		}
		if p.ClientSecret != nil {
			newSecret = *p.ClientSecret
		}
		if p.Enabled != nil {
			newEnabled = *p.Enabled
		}
		enablingNow := p.Enabled != nil && *p.Enabled && !cur.Enabled
		issuerChangedWhileOn := newEnabled && p.Issuer != nil && newIssuer != cur.Issuer
		if enablingNow || issuerChangedWhileOn {
			if err := s.probeDiscovery(ctx, newIssuer); err != nil {
				return apperr.Unprocessable("provider discovery failed: " + err.Error())
			}
		}
		// Org-pinned even though the FOR UPDATE load above already proved
		// ownership: no auth_provider write is safe to read (or copy) bare.
		if _, err := tx.Exec(ctx, `
			UPDATE auth_provider
			SET issuer = $2, client_id = $3, client_secret = $4, enabled = $5
			WHERE id = $1 AND org_id = $6`,
			providerID, newIssuer, newClientID, newSecret, newEnabled, actor.OrgID); err != nil {
			return apperr.Internal("update auth provider", err)
		}
		_, err = eventlog.Append(ctx, tx, eventlog.Event{
			OrgID: actor.OrgID, ActorKind: enum.ActorHuman, ActorID: &actor.UserID,
			EntityType: enum.EntityOrg, EntityID: actor.OrgID, Verb: "auth_provider.updated",
			Payload: eventlog.MustPayload(map[string]any{
				"provider_id": providerID, "enabled": newEnabled}),
		})
		if err != nil {
			return err
		}
		out = AuthProvider{ID: providerID, Name: cur.Name, Issuer: newIssuer,
			ClientID: newClientID, HasSecret: newSecret != "", Enabled: newEnabled,
			CreatedAt: cur.CreatedAt}
		return nil
	})
	if err != nil {
		return AuthProvider{}, err
	}
	return out, nil
}

// DeleteAuthProvider removes a provider (manage_auth_providers). Its throwaway in-flight
// oidc_flow rows are cleared first; a provider that still has durable
// external_identity links refuses (Conflict) rather than orphan those logins.
func (s *Service) DeleteAuthProvider(ctx context.Context, actor auth.Identity, providerID int64) error {
	return db.WithTx(ctx, s.pool, func(tx pgx.Tx) error {
		if err := s.perms.Require(ctx, tx, actor, perms.VerbManageAuthProviders,
			perms.OrgScope(actor.OrgID)); err != nil {
			return err
		}
		var orgID int64
		if err := tx.QueryRow(ctx,
			`SELECT org_id FROM auth_provider WHERE id = $1 AND org_id = $2 FOR UPDATE`,
			providerID, actor.OrgID).Scan(&orgID); errors.Is(err, pgx.ErrNoRows) {
			return apperr.NotFound("provider not found")
		} else if err != nil {
			return apperr.Internal("load auth provider", err)
		}
		if _, err := tx.Exec(ctx,
			`DELETE FROM oidc_flow WHERE provider_id = $1`, providerID); err != nil {
			return apperr.Internal("clear oidc flows", err)
		}
		if _, err := tx.Exec(ctx,
			`DELETE FROM auth_provider WHERE id = $1 AND org_id = $2`,
			providerID, actor.OrgID); err != nil {
			var pgErr *pgconn.PgError
			if errors.As(err, &pgErr) && pgErr.Code == "23503" { // foreign_key_violation
				return apperr.Conflict("provider still has linked identities")
			}
			return apperr.Internal("delete auth provider", err)
		}
		_, err := eventlog.Append(ctx, tx, eventlog.Event{
			OrgID: actor.OrgID, ActorKind: enum.ActorHuman, ActorID: &actor.UserID,
			EntityType: enum.EntityOrg, EntityID: actor.OrgID, Verb: "auth_provider.deleted",
			Payload: eventlog.MustPayload(map[string]any{"provider_id": providerID}),
		})
		return err
	})
}
