// Package files wakes the ADR-012 subsystem: upload once, reference N
// times, and access is the UNION of referencing entities — a file is
// visible to whoever can see at least one thing it's attached to (plus its
// uploader, always). Bytes live behind the blob seam; this package never
// knows which backend holds them.
package files

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/abhinavjha0239/weft/internal/auth"
	"github.com/abhinavjha0239/weft/internal/db"
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
}

func New(pool *pgxpool.Pool, store blob.Store) *Service {
	return &Service{pool: pool, store: store}
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
	sum := h.Sum(nil)
	shaHex := hex.EncodeToString(sum)
	key := fmt.Sprintf("%d/%s/%s/%s", actor.OrgID, shaHex[:2], shaHex[2:4], shaHex)

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

type Meta struct {
	Name string
	Mime string
	Size int64
}

// OpenDownload enforces the F-12 union rule per (viewer, reference) at
// query time: the uploader always reads their own file; anyone else needs
// at least one referencing message they can see — the same three-way
// container ACL as message fetch.
func (s *Service) OpenDownload(ctx context.Context, actor auth.Identity, fileID int64) (Meta, io.ReadCloser, error) {
	var m Meta
	var key string
	var uploader *int64
	err := s.pool.QueryRow(ctx, `
		SELECT name, mime, size_bytes, storage_key, uploader_id
		FROM file
		WHERE id = $1 AND org_id = $2 AND kind = 1 AND deleted_at IS NULL`,
		fileID, actor.OrgID).Scan(&m.Name, &m.Mime, &m.Size, &key, &uploader)
	if err != nil {
		return Meta{}, nil, apperr.NotFound("file not found")
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
			return Meta{}, nil, apperr.Internal("reference check", err)
		}
	}
	if !allowed {
		// Indistinguishable from nonexistent: no file-id oracle.
		return Meta{}, nil, apperr.NotFound("file not found")
	}
	rc, err := s.store.Open(ctx, key)
	if err != nil {
		return Meta{}, nil, apperr.Internal("open blob", err)
	}
	return m, rc, nil
}

// AttachMessageReferences records file_reference rows for the given file
// ids on a message — called in the message-write transaction. Only files
// from the SAME ORG attach, and only ones the author may read (their own
// uploads or files already visible to them); anything else is silently
// skipped, never an error (a bad link must not block a send).
func (s *Service) AttachMessageReferences(ctx context.Context, tx pgx.Tx, actor auth.Identity, messageID int64, fileIDs []int64) (int, error) {
	attached := 0
	for _, fid := range fileIDs {
		ct, err := tx.Exec(ctx, `
			INSERT INTO file_reference (file_id, entity_type, entity_id, created_by)
			SELECT f.id, $2, $3, $4
			FROM file f
			WHERE f.id = $1 AND f.org_id = $5 AND f.deleted_at IS NULL
			  AND (f.uploader_id = $4 OR EXISTS (
			      SELECT 1 FROM file_reference fr2
			      JOIN message m2 ON fr2.entity_type = $2 AND m2.id = fr2.entity_id
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
			fid, int16(enum.EntityMessage), messageID, actor.UserID, actor.OrgID)
		if err != nil {
			return attached, apperr.Internal("attach reference", err)
		}
		if ct.RowsAffected() > 0 {
			attached++
		}
	}
	return attached, nil
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
