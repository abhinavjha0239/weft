// Package files wakes the ADR-012 subsystem: upload once, reference N
// times, and access is the UNION of referencing entities — a file is
// visible to whoever can see at least one thing it's attached to (plus its
// uploader, always). Bytes live behind the blob seam; this package never
// knows which backend holds them.
package files

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/abhinavjha0239/weft/internal/auth"
	"github.com/abhinavjha0239/weft/internal/db"
	"github.com/abhinavjha0239/weft/internal/domain/perms"
	"github.com/abhinavjha0239/weft/internal/enum"
	"github.com/abhinavjha0239/weft/internal/eventlog"
	"github.com/abhinavjha0239/weft/internal/platform/apperr"
	"github.com/abhinavjha0239/weft/internal/platform/blob"
)

// MaxUploadBytes is the v1 cap (an org-level setting later).
const MaxUploadBytes = 25 << 20 // 25 MiB

type Service struct {
	pool  *pgxpool.Pool
	store blob.Store
	// signingSecret keys the HMAC for signed download links (P-07); empty
	// until wired from config, in which case link minting refuses.
	signingSecret string
	// scanner is the optional upload malware-scan seam (P-19); nil means no
	// scanning, and uploads STAY scan_status 0 (pending) — we never fake
	// "clean" without a scanner (the honest-rungs rule).
	scanner Scanner
	// perms gates the org storage-quota admin endpoints (P-19); wired via
	// SetPerms. Nil is fine for tests that never hit those endpoints —
	// enforcement in Upload/StoreDocument needs no perms.
	perms *perms.Service
	// log records best-effort work that must never fail an upload — today,
	// P-18 thumbnail generation. Wired via SetLogger; nil falls back to
	// slog.Default() (the logger() accessor).
	log *slog.Logger
	// thumbSem bounds concurrent renders and thumbOutcomes memoizes per-key
	// render/describe results — the P-18 DoS hardening (thumbnail.go).
	thumbSem      chan struct{}
	thumbMu       sync.Mutex
	thumbOutcomes map[string]thumbOutcome
}

func New(pool *pgxpool.Pool, store blob.Store) *Service {
	return &Service{
		pool:          pool,
		store:         store,
		thumbSem:      make(chan struct{}, renderConcurrency),
		thumbOutcomes: make(map[string]thumbOutcome),
	}
}

type File struct {
	ID   int64  `json:"file_id"`
	Name string `json:"name"`
	Mime string `json:"mime"`
	Size int64  `json:"size_bytes"`
	SHA  string `json:"sha256"`
	URL  string `json:"url"`
}

// Upload streams the body to a spool (hashing as it goes), then stores the
// blob content-addressed and records the row. Same content in the same org
// reuses the existing blob key — Put is an idempotent no-op.
func (s *Service) Upload(ctx context.Context, actor auth.Identity, name, mime string, body io.Reader) (File, error) {
	name = sanitizeName(name)
	if name == "" {
		return File{}, apperr.Invalid("filename required")
	}
	if mime == "" {
		mime = "application/octet-stream"
	}

	// Spool to disk while hashing: the content hash IS the storage key, so
	// the bytes cannot go to the store before they are fully read.
	spool, err := os.CreateTemp("", "weft-upload-*")
	if err != nil {
		return File{}, apperr.Internal("spool", err)
	}
	defer func() { spool.Close(); os.Remove(spool.Name()) }()
	h := sha256.New()
	size, err := io.Copy(io.MultiWriter(spool, h), io.LimitReader(body, MaxUploadBytes+1))
	if err != nil {
		return File{}, apperr.Internal("read upload", err)
	}
	if size == 0 {
		return File{}, apperr.Invalid("empty file")
	}
	if size > MaxUploadBytes {
		return File{}, apperr.Invalid(fmt.Sprintf("file too large (max %d MiB)", MaxUploadBytes>>20))
	}
	// Quota (P-19): reject before scanning or storing so an over-quota upload
	// leaves no blob and no row.
	if err := s.checkQuota(ctx, actor.OrgID, size); err != nil {
		return File{}, err
	}
	sum := h.Sum(nil)
	shaHex := hex.EncodeToString(sum)
	key := StorageKey(actor.OrgID, shaHex)

	// Malware scan (P-19) reads the fully-spooled bytes BEFORE the blob is
	// stored. A scanner error fails CLOSED (no row, no blob) so an outage never
	// admits unscanned bytes; Clean records status 1; Quarantined still stores
	// the bytes and row (status 2 — evidence for compliance/holds) but the
	// upload is rejected 422 below and no reference can ever form. With no
	// scanner wired the status stays 0 (pending) — never a fake clean.
	scanStatus := int16(0)
	quarantined := false
	if s.scanner != nil {
		if _, err := spool.Seek(0, io.SeekStart); err != nil {
			return File{}, apperr.Internal("rewind spool", err)
		}
		verdict, err := s.scanner.Scan(ctx, name, mime, spool)
		if err != nil {
			return File{}, apperr.Internal("scan upload", err)
		}
		switch verdict {
		case Clean:
			scanStatus = 1
		case Quarantined:
			scanStatus = 2
			quarantined = true
		default:
			return File{}, apperr.Internal("scan upload",
				fmt.Errorf("scanner returned invalid verdict %d", verdict))
		}
	}

	if _, err := spool.Seek(0, io.SeekStart); err != nil {
		return File{}, apperr.Internal("rewind spool", err)
	}
	if err := s.store.Put(ctx, key, spool); err != nil {
		return File{}, apperr.Internal("store blob", err)
	}

	var out File
	err = db.WithTx(ctx, s.pool, func(tx pgx.Tx) error {
		if err := tx.QueryRow(ctx, `
			INSERT INTO file (org_id, kind, name, mime, size_bytes, sha256,
				storage_key, uploader_id, scan_status)
			VALUES ($1, 1, $2, $3, $4, $5, $6, $7, $8) RETURNING id`,
			actor.OrgID, name, mime, size, sum, key, actor.UserID, scanStatus).Scan(&out.ID); err != nil {
			return apperr.Internal("record file", err)
		}
		if _, err := eventlog.Append(ctx, tx, eventlog.Event{
			OrgID: actor.OrgID, ActorKind: enum.ActorHuman, ActorID: &actor.UserID,
			EntityType: enum.EntityFile, EntityID: out.ID, Verb: "file.uploaded",
			Payload: eventlog.MustPayload(map[string]any{
				"file_id": out.ID, "name": name, "size_bytes": size}),
		}); err != nil {
			return apperr.Internal("append event", err)
		}
		return nil
	})
	if err != nil {
		return File{}, err
	}
	if quarantined {
		return File{}, apperr.Unprocessable("file rejected by malware scan")
	}
	// Best-effort thumbnail (P-18): render a derived JPEG for allowlisted
	// images, AFTER the scan verdict and the blob Put, so a clean image is
	// browsable inline. Generation NEVER gates the upload — a non-image is
	// silently ignored; a corrupt image or an over-cap decompression bomb is
	// logged and skipped. A GET can still backfill it lazily later.
	if _, seekErr := spool.Seek(0, io.SeekStart); seekErr == nil {
		if _, _, terr := s.renderThumbFrom(ctx, key, func() (io.ReadCloser, error) {
			return io.NopCloser(spool), nil
		}); terr != nil && !errors.Is(terr, errNotRenderable) {
			s.logger().Warn("thumbnail generation failed", "file_id", out.ID, "err", terr)
		}
	}
	out.Name, out.Mime, out.Size, out.SHA = name, mime, size, shaHex
	out.URL = fmt.Sprintf("/api/v1/files/%d", out.ID)
	return out, nil
}

// StoreDocumentStream records a system-produced artifact (compliance exports,
// export bundles) as a first-class file, streaming from r: content-addressed
// like any upload (spool + hash so the content hash is the storage key), but
// WITHOUT the 25 MiB HTTP cap — a system artifact's size is bounded by the
// lane that produces it (the export worker), not the request path. Uploader is
// the requesting officer, so the result is downloadable via the normal union
// ACL (uploader-always).
func (s *Service) StoreDocumentStream(ctx context.Context, actor auth.Identity, name, mime string, r io.Reader) (File, error) {
	name = sanitizeName(name)
	if name == "" {
		return File{}, apperr.Invalid("document name required")
	}
	if mime == "" {
		mime = "application/octet-stream"
	}
	spool, err := os.CreateTemp("", "weft-doc-*")
	if err != nil {
		return File{}, apperr.Internal("spool", err)
	}
	defer func() { spool.Close(); os.Remove(spool.Name()) }()
	h := sha256.New()
	size, err := io.Copy(io.MultiWriter(spool, h), r)
	if err != nil {
		return File{}, apperr.Internal("read document", err)
	}
	if size == 0 {
		return File{}, apperr.Invalid("empty document")
	}
	// Compliance-export artifacts are an org's own storage usage, so they are
	// quota-enforced too (P-19) — checked here, once the size is known, so the
	// []byte wrapper and the streaming path share one gate.
	if err := s.checkQuota(ctx, actor.OrgID, size); err != nil {
		return File{}, err
	}
	sum := h.Sum(nil)
	shaHex := hex.EncodeToString(sum)
	key := StorageKey(actor.OrgID, shaHex)
	if _, err := spool.Seek(0, io.SeekStart); err != nil {
		return File{}, apperr.Internal("rewind spool", err)
	}
	if err := s.store.Put(ctx, key, spool); err != nil {
		return File{}, apperr.Internal("store blob", err)
	}
	var out File
	err = db.WithTx(ctx, s.pool, func(tx pgx.Tx) error {
		if err := tx.QueryRow(ctx, `
			INSERT INTO file (org_id, kind, name, mime, size_bytes, sha256,
				storage_key, uploader_id)
			VALUES ($1, 1, $2, $3, $4, $5, $6, $7) RETURNING id`,
			actor.OrgID, name, mime, size, sum, key, actor.UserID).Scan(&out.ID); err != nil {
			return apperr.Internal("record file", err)
		}
		if _, err := eventlog.Append(ctx, tx, eventlog.Event{
			OrgID: actor.OrgID, ActorKind: enum.ActorHuman, ActorID: &actor.UserID,
			EntityType: enum.EntityFile, EntityID: out.ID, Verb: "file.uploaded",
			Payload: eventlog.MustPayload(map[string]any{
				"file_id": out.ID, "name": name, "size_bytes": size}),
		}); err != nil {
			return apperr.Internal("append event", err)
		}
		return nil
	})
	if err != nil {
		return File{}, err
	}
	out.Name, out.Mime, out.Size, out.SHA = name, mime, size, shaHex
	out.URL = fmt.Sprintf("/api/v1/files/%d", out.ID)
	return out, nil
}

// StoreDocument is the bytes convenience over StoreDocumentStream; its result
// is byte-identical to the pre-stream implementation (same content-addressed
// key, row, and event).
func (s *Service) StoreDocument(ctx context.Context, actor auth.Identity, name, mime string, data []byte) (File, error) {
	if sanitizeName(name) == "" || len(data) == 0 {
		return File{}, apperr.Invalid("document name and content required")
	}
	return s.StoreDocumentStream(ctx, actor, name, mime, bytes.NewReader(data))
}

// PinForExport records export-job references on files, freezing their
// bytes against GC (the janitor treats non-message references as live):
// an export's evidence outlives the retention of what it captured. Only
// same-org files pin; unknown ids are skipped.
func (s *Service) PinForExport(ctx context.Context, tx pgx.Tx, orgID, exportJobID int64, fileIDs []int64) error {
	for _, fid := range fileIDs {
		if _, err := tx.Exec(ctx, `
			INSERT INTO file_reference (file_id, entity_type, entity_id)
			SELECT f.id, $2, $3 FROM file f
			WHERE f.id = $1 AND f.org_id = $4 AND f.deleted_at IS NULL
			ON CONFLICT DO NOTHING`,
			fid, int16(enum.EntityExportJob), exportJobID, orgID); err != nil {
			return apperr.Internal("pin export reference", err)
		}
	}
	return nil
}

type Meta struct {
	Name string
	Mime string
	Size int64
}

// authorizeDownload runs the F-12 union ACL and returns the file's meta and
// its storage key WITHOUT opening the blob. It is shared by OpenDownload
// (which then opens the blob) and the signed-link minter (P-07), which must
// authorize a download but must NOT open the bytes just to hand out a URL.
// The uploader always reads their own file; anyone else needs at least one
// referencing message they can see — the same three-way container ACL as
// message fetch. Unauthorized and nonexistent are indistinguishable (no
// file-id oracle).
func (s *Service) authorizeDownload(ctx context.Context, actor auth.Identity, fileID int64) (Meta, string, error) {
	var m Meta
	var key string
	var uploader *int64
	err := s.pool.QueryRow(ctx, `
		SELECT name, mime, size_bytes, storage_key, uploader_id
		FROM file
		WHERE id = $1 AND org_id = $2 AND kind = 1 AND deleted_at IS NULL
		  AND scan_status <> 2`,
		fileID, actor.OrgID).Scan(&m.Name, &m.Mime, &m.Size, &key, &uploader)
	if err != nil {
		return Meta{}, "", apperr.NotFound("file not found")
	}
	allowed := uploader != nil && *uploader == actor.UserID
	if !allowed {
		if err := s.pool.QueryRow(ctx, `
			SELECT EXISTS (
			  SELECT 1
			  FROM file_reference fr
			  JOIN message m ON fr.entity_type = $3 AND m.id = fr.entity_id
			  WHERE fr.file_id = $1
			    AND m.org_id = $4 AND m.deleted_at IS NULL
			    AND ((m.channel_id IS NOT NULL AND EXISTS (
			           SELECT 1 FROM channel_member cm
			           WHERE cm.channel_id = m.channel_id AND cm.user_id = $2
			             AND cm.unsubscribed_at IS NULL))
			      OR (m.dm_space_id IS NOT NULL AND EXISTS (
			           SELECT 1 FROM dm_participant dp
			           WHERE dp.dm_space_id = m.dm_space_id AND dp.user_id = $2))
			      OR (m.channel_id IS NULL AND m.dm_space_id IS NULL)))`,
			fileID, actor.UserID, int16(enum.EntityMessage), actor.OrgID).Scan(&allowed); err != nil {
			return Meta{}, "", apperr.Internal("reference check", err)
		}
	}
	if !allowed {
		return Meta{}, "", apperr.NotFound("file not found")
	}
	return m, key, nil
}

// OpenDownload authorizes the viewer (the F-12 union rule) and opens the
// blob for streaming.
func (s *Service) OpenDownload(ctx context.Context, actor auth.Identity, fileID int64) (Meta, io.ReadCloser, error) {
	m, key, err := s.authorizeDownload(ctx, actor, fileID)
	if err != nil {
		return Meta{}, nil, err
	}
	rc, err := s.store.Open(ctx, key)
	if err != nil {
		return Meta{}, nil, apperr.Internal("open blob", err)
	}
	return m, rc, nil
}

// OpenForBundle opens a file's bytes for a compliance export bundle (P-32). It
// is NOT the F-12 union ACL — the export is already compliance_officer-
// authorized and the file set is the job's own EntityExportJob pins — but it
// stays org-scoped. It deliberately does NOT filter deleted_at: a pinned file
// whose source messages have since died still bundles as evidence (the pin
// keeps its blob alive against GC). Any error — a missing row or an unreadable
// blob — is the caller's signal to record the id under manifest `missing`,
// never to fail the whole bundle.
func (s *Service) OpenForBundle(ctx context.Context, orgID, fileID int64) (io.ReadCloser, error) {
	var key string
	if err := s.pool.QueryRow(ctx, `
		SELECT storage_key FROM file
		WHERE id = $1 AND org_id = $2 AND kind = 1`,
		fileID, orgID).Scan(&key); err != nil {
		return nil, apperr.NotFound("file not found")
	}
	rc, err := s.store.Open(ctx, key)
	if err != nil {
		return nil, apperr.Internal("open bundle blob", err)
	}
	return rc, nil
}

// AttachMessageReferences records file_reference rows for the given file
// ids on a message — called in the message-write transaction.
func (s *Service) AttachMessageReferences(ctx context.Context, tx pgx.Tx, actor auth.Identity, messageID int64, fileIDs []int64) (int, error) {
	return s.AttachEntityReferences(ctx, tx, actor, enum.EntityMessage, messageID, fileIDs)
}

// AttachEntityReferences records file_reference rows for any entity (live
// messages, scheduled messages awaiting delivery). Only files from the
// SAME ORG attach, and only ones the AUTHOR may read — their own uploads
// or files already visible via a live MESSAGE reference (the inner check
// stays entity_type=1: message references define visibility; other kinds
// only pin). Anything else is silently skipped, never an error (a bad
// link must not block a send).
func (s *Service) AttachEntityReferences(ctx context.Context, tx pgx.Tx, actor auth.Identity, entity enum.EntityType, entityID int64, fileIDs []int64) (int, error) {
	attached := 0
	for _, fid := range fileIDs {
		ct, err := tx.Exec(ctx, `
			INSERT INTO file_reference (file_id, entity_type, entity_id, created_by)
			SELECT f.id, $2, $3, $4
			FROM file f
			WHERE f.id = $1 AND f.org_id = $5 AND f.deleted_at IS NULL
			  AND f.scan_status <> 2
			  AND (f.uploader_id = $4 OR EXISTS (
			      SELECT 1 FROM file_reference fr2
			      JOIN message m2 ON fr2.entity_type = 1 AND m2.id = fr2.entity_id
			      WHERE fr2.file_id = f.id AND m2.deleted_at IS NULL
			        AND ((m2.channel_id IS NOT NULL AND EXISTS (
			               SELECT 1 FROM channel_member cm
			               WHERE cm.channel_id = m2.channel_id AND cm.user_id = $4
			                 AND cm.unsubscribed_at IS NULL))
			          OR (m2.dm_space_id IS NOT NULL AND EXISTS (
			               SELECT 1 FROM dm_participant dp
			               WHERE dp.dm_space_id = m2.dm_space_id AND dp.user_id = $4))
			          OR (m2.channel_id IS NULL AND m2.dm_space_id IS NULL))))
			ON CONFLICT DO NOTHING`,
			fid, int16(entity), entityID, actor.UserID, actor.OrgID)
		if err != nil {
			return attached, apperr.Internal("attach reference", err)
		}
		if ct.RowsAffected() > 0 {
			attached++
		}
	}
	return attached, nil
}

// ReleaseEntityReferences drops one entity's file references — a scheduled
// message delivered or cancelled releases its pins (the LIVE message
// re-claims its files itself).
func (s *Service) ReleaseEntityReferences(ctx context.Context, tx pgx.Tx, entity enum.EntityType, entityID int64) error {
	if _, err := tx.Exec(ctx, `
		DELETE FROM file_reference WHERE entity_type = $1 AND entity_id = $2`,
		int16(entity), entityID); err != nil {
		return apperr.Internal("release references", err)
	}
	return nil
}

// StorageKey is THE content-addressed blob key derivation: org-scoped with
// directory fan-out. Every writer (uploads, the importer's backfill lane)
// must derive keys here so dedup and idempotent puts hold across producers.
func StorageKey(orgID int64, shaHex string) string {
	return fmt.Sprintf("%d/%s/%s/%s", orgID, shaHex[:2], shaHex[2:4], shaHex)
}

// sanitizeName keeps a plain base name: no paths, no control characters,
// bounded length (the download header re-escapes it too).
func sanitizeName(name string) string {
	name = filepath.Base(strings.TrimSpace(name))
	if name == "." || name == "/" || name == ".." {
		return ""
	}
	name = strings.Map(func(r rune) rune {
		if r < 0x20 || r == 0x7f {
			return -1
		}
		return r
	}, name)
	if len(name) > 255 {
		name = name[:255]
	}
	return name
}
