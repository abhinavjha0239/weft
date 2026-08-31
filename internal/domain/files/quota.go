package files

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"

	"github.com/abhinavjha0239/weft/internal/auth"
	"github.com/abhinavjha0239/weft/internal/db"
	"github.com/abhinavjha0239/weft/internal/domain/perms"
	"github.com/abhinavjha0239/weft/internal/enum"
	"github.com/abhinavjha0239/weft/internal/eventlog"
	"github.com/abhinavjha0239/weft/internal/platform/apperr"
)

// Org storage quota (P-19). The cap is a namespaced key in org.settings; files
// OWNS this key — it reads it for enforcement and writes it via jsonb_set,
// touching no other key — while the general org row stays identity's (a
// documented per-subsystem-settings ownership, LLD). 0 or absent = unlimited.
// Usage is ROW-accounting: SUM(size_bytes) over the org's LIVE files, so a
// dedup-shared blob still counts each referencing row (blob-level accounting
// would undercharge an org that deletes its first copy) and a GC purge frees
// the bytes as deleted_at drops the row from the sum. A per-org counter table
// is the recorded mitigation if the sum goes hot.

const storageQuotaKey = "storage_quota_bytes"

// StorageQuotaInfo is the admin read model: the cap and the current usage.
type StorageQuotaInfo struct {
	MaxBytes  int64 `json:"max_bytes"`
	UsedBytes int64 `json:"used_bytes"`
}

// SetPerms wires the permission service for the quota admin endpoints (the
// SetSigningSecret composition pattern). Enforcement in Upload/StoreDocument
// needs no perms, so tests that never hit the admin endpoints may leave it nil.
func (s *Service) SetPerms(p *perms.Service) { s.perms = p }

// storageUsage reads (used, quota) for an org in one round trip: the live-file
// byte sum (rides file_org_live_idx) and the configured cap (0 = unlimited).
func (s *Service) storageUsage(ctx context.Context, orgID int64) (used, quota int64, err error) {
	if err := s.pool.QueryRow(ctx, `
		SELECT COALESCE((SELECT SUM(size_bytes) FROM file
		                 WHERE org_id = $1 AND deleted_at IS NULL), 0),
		       COALESCE((SELECT (settings->>$2)::bigint FROM org WHERE id = $1), 0)`,
		orgID, storageQuotaKey).Scan(&used, &quota); err != nil {
		return 0, 0, apperr.Internal("storage usage", err)
	}
	return used, quota, nil
}

// checkQuota rejects an incoming write that would push the org past its cap. A
// 0/absent cap is unlimited (no check). The boundary is inclusive:
// used+incoming == cap passes, +1 fails (413).
func (s *Service) checkQuota(ctx context.Context, orgID, incoming int64) error {
	used, quota, err := s.storageUsage(ctx, orgID)
	if err != nil {
		return err
	}
	if quota > 0 && used+incoming > quota {
		return apperr.TooLarge("storage quota exceeded")
	}
	return nil
}

// StorageQuota returns the org's cap and current usage.
// manage_storage_quota-gated (the F-9 admin surface); the gate runs in a tx, the usage read follows.
func (s *Service) StorageQuota(ctx context.Context, actor auth.Identity) (StorageQuotaInfo, error) {
	if s.perms == nil {
		return StorageQuotaInfo{}, apperr.Internal("storage quota", errors.New("perms not wired"))
	}
	if err := db.WithTx(ctx, s.pool, func(tx pgx.Tx) error {
		return s.perms.Require(ctx, tx, actor, perms.VerbManageStorageQuota, perms.OrgScope(actor.OrgID))
	}); err != nil {
		return StorageQuotaInfo{}, err
	}
	used, quota, err := s.storageUsage(ctx, actor.OrgID)
	if err != nil {
		return StorageQuotaInfo{}, err
	}
	return StorageQuotaInfo{MaxBytes: quota, UsedBytes: used}, nil
}

// SetStorageQuota sets the org's storage cap in bytes (0 = unlimited).
// manage_storage_quota-gated in the same tx as the write; jsonb_set touches only the
// quota key, and the change is event-logged (org.quota_changed — the admin.go
// precedent). Setting a cap below current usage is allowed: it blocks NEW
// uploads without touching stored bytes.
func (s *Service) SetStorageQuota(ctx context.Context, actor auth.Identity, maxBytes int64) error {
	if s.perms == nil {
		return apperr.Internal("storage quota", errors.New("perms not wired"))
	}
	if maxBytes < 0 {
		return apperr.Invalid("max_bytes must be >= 0")
	}
	return db.WithTx(ctx, s.pool, func(tx pgx.Tx) error {
		if err := s.perms.Require(ctx, tx, actor, perms.VerbManageStorageQuota, perms.OrgScope(actor.OrgID)); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `
			UPDATE org SET settings = jsonb_set(settings, ARRAY[$2::text], to_jsonb($3::bigint))
			WHERE id = $1`,
			actor.OrgID, storageQuotaKey, maxBytes); err != nil {
			return apperr.Internal("set quota", err)
		}
		if _, err := eventlog.Append(ctx, tx, eventlog.Event{
			OrgID: actor.OrgID, ActorKind: enum.ActorHuman, ActorID: &actor.UserID,
			EntityType: enum.EntityOrg, EntityID: actor.OrgID, Verb: "org.quota_changed",
			Payload: eventlog.MustPayload(map[string]any{"max_bytes": maxBytes}),
		}); err != nil {
			return apperr.Internal("append event", err)
		}
		return nil
	})
}
