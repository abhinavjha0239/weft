package compliance

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/abhinavjha0239/weft/internal/auth"
	"github.com/abhinavjha0239/weft/internal/db"
	"github.com/abhinavjha0239/weft/internal/domain/files"
	"github.com/abhinavjha0239/weft/internal/domain/perms"
	"github.com/abhinavjha0239/weft/internal/enum"
	"github.com/abhinavjha0239/weft/internal/eventlog"
	"github.com/abhinavjha0239/weft/internal/platform/apperr"
)

// The AD-5 export lane: self-serve scoped compliance exports (Zulip's is
// CLI-only), INCLUDING private channels, DMs, edit history, and deleted-
// message tombstones — the event-log design retains every version, so the
// export can carry what Zulip's cannot. What retention already purged is
// gone from exports too, by design: scrubbed revisions surface as null.

// ExportStore is the files-domain surface the worker needs; *files.Service
// implements it.
type ExportStore interface {
	StoreDocument(ctx context.Context, actor auth.Identity, name, mime string, data []byte) (files.File, error)
	PinForExport(ctx context.Context, tx pgx.Tx, orgID, exportJobID int64, fileIDs []int64) error
}

// SetFiles injects the store (wired in weftd; tests wire their own).
func (s *Service) SetFiles(f ExportStore) { s.files = f }

// ExportScope selects what to export: dimensions OR together (a message
// matches if ANY dimension claims it); the date window ANDs on top. An
// empty scope is the full-org export (Zulip parity), still gated+audited.
type ExportScope struct {
	UserIDs    []int64    `json:"user_ids,omitempty"`    // authored-by (custodian)
	ChannelIDs []int64    `json:"channel_ids,omitempty"` // messages in channels
	SpaceIDs   []int64    `json:"space_ids,omitempty"`   // work-item discussions
	From       *time.Time `json:"from,omitempty"`
	To         *time.Time `json:"to,omitempty"`
}

// Export statuses (export_job.status, 0006 schema).
const (
	exportPending int16 = 1
	exportRunning int16 = 2
	exportDone    int16 = 3
	exportFailed  int16 = 4
)

type ExportJob struct {
	ID           int64           `json:"export_id"`
	RequestedBy  int64           `json:"requested_by"`
	Scope        json.RawMessage `json:"scope"`
	Status       int16           `json:"status"`
	ResultFileID *int64          `json:"result_file_id,omitempty"`
	CreatedAt    time.Time       `json:"created_at"`
	FinishedAt   *time.Time      `json:"finished_at,omitempty"`
}

const maxScopeIDs = 100

// RequestExport queues a job (compliance_officer gated) and nudges the
// worker. Scope ids must belong to the org — a foreign id is a 404, so a
// typo can't silently produce an empty export.
func (s *Service) RequestExport(ctx context.Context, actor auth.Identity, scope ExportScope) (ExportJob, error) {
	if len(scope.UserIDs) > maxScopeIDs || len(scope.ChannelIDs) > maxScopeIDs || len(scope.SpaceIDs) > maxScopeIDs {
		return ExportJob{}, apperr.Invalid(fmt.Sprintf("at most %d ids per dimension", maxScopeIDs))
	}
	if scope.From != nil && scope.To != nil && scope.To.Before(*scope.From) {
		return ExportJob{}, apperr.Invalid("to must not precede from")
	}
	var job ExportJob
	err := db.WithTx(ctx, s.pool, func(tx pgx.Tx) error {
		if err := s.perms.Require(ctx, tx, actor, perms.VerbComplianceOfficer,
			perms.OrgScope(actor.OrgID)); err != nil {
			return err
		}
		for _, check := range []struct {
			ids   []int64
			table string
		}{
			{scope.UserIDs, "user_account"},
			{scope.ChannelIDs, "channel"},
			{scope.SpaceIDs, "space"},
		} {
			if len(check.ids) == 0 {
				continue
			}
			var n int
			if err := tx.QueryRow(ctx,
				`SELECT count(*) FROM `+check.table+` WHERE id = ANY($1) AND org_id = $2`,
				check.ids, actor.OrgID).Scan(&n); err != nil {
				return apperr.Internal("scope check", err)
			}
			if n != len(check.ids) {
				return apperr.NotFound("scope id not found: " + check.table)
			}
		}
		raw, err := json.Marshal(scope)
		if err != nil {
			return apperr.Internal("marshal scope", err)
		}
		if err := tx.QueryRow(ctx, `
			INSERT INTO export_job (org_id, requested_by, scope)
			VALUES ($1, $2, $3) RETURNING id, created_at`,
			actor.OrgID, actor.UserID, raw).Scan(&job.ID, &job.CreatedAt); err != nil {
			return apperr.Internal("create export job", err)
		}
		job.RequestedBy, job.Scope, job.Status = actor.UserID, raw, exportPending
		_, err = eventlog.Append(ctx, tx, eventlog.Event{
			OrgID: actor.OrgID, ActorKind: enum.ActorHuman, ActorID: &actor.UserID,
			EntityType: enum.EntityExportJob, EntityID: job.ID, Verb: "export.requested",
			Payload: eventlog.MustPayload(map[string]any{"export_id": job.ID}),
		})
		return err
	})
	if err != nil {
		return ExportJob{}, err
	}
	s.nudgeExports()
	return job, nil
}

// ListExports returns the org's jobs, newest first (gated).
func (s *Service) ListExports(ctx context.Context, actor auth.Identity) ([]ExportJob, error) {
	out := []ExportJob{}
	err := db.WithTx(ctx, s.pool, func(tx pgx.Tx) error {
		if err := s.perms.Require(ctx, tx, actor, perms.VerbComplianceOfficer,
			perms.OrgScope(actor.OrgID)); err != nil {
			return err
		}
		rows, err := tx.Query(ctx, `
			SELECT id, requested_by, scope, status, result_file_id, created_at, finished_at
			FROM export_job WHERE org_id = $1 ORDER BY id DESC LIMIT 100`, actor.OrgID)
		if err != nil {
			return apperr.Internal("list exports", err)
		}
		defer rows.Close()
		for rows.Next() {
			var j ExportJob
			if err := rows.Scan(&j.ID, &j.RequestedBy, &j.Scope, &j.Status,
				&j.ResultFileID, &j.CreatedAt, &j.FinishedAt); err != nil {
				return apperr.Internal("scan export", err)
			}
			out = append(out, j)
		}
		return rows.Err()
	})
	return out, err
}

// nudgeExports wakes the in-process worker without waiting for its tick.
func (s *Service) nudgeExports() {
	select {
	case s.exportWake <- struct{}{}:
	default:
	}
}

// RunExportLoop drains pending jobs until ctx ends: nudged by requests in
// this process, ticking for other nodes' requests and crash recovery.
func (s *Service) RunExportLoop(ctx context.Context) {
	t := time.NewTicker(5 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-s.exportWake:
		case <-t.C:
		}
		if _, err := s.RunPendingExports(ctx); err != nil && ctx.Err() == nil {
			s.log().Warn("export: run failed", "err", err)
		}
	}
}

// RunPendingExports claims and executes queued jobs one at a time until
// none remain; returns how many it completed. Claims use SKIP LOCKED so
// concurrent workers never double-run a job; a job stuck RUNNING for over
// 15 minutes (a crashed worker) is reclaimed — the run is idempotent
// enough that a duplicate result simply replaces the pointer, and
// content addressing dedups the bytes.
func (s *Service) RunPendingExports(ctx context.Context) (int, error) {
	done := 0
	for {
		var jobID, orgID, requestedBy int64
		var rawScope []byte
		err := s.pool.QueryRow(ctx, `
			UPDATE export_job SET status = $1
			WHERE id = (
			  SELECT id FROM export_job
			  WHERE status = $2
			     OR (status = $1 AND finished_at IS NULL AND created_at < now() - interval '15 minutes')
			  ORDER BY id LIMIT 1
			  FOR UPDATE SKIP LOCKED)
			RETURNING id, org_id, requested_by, scope`,
			exportRunning, exportPending).Scan(&jobID, &orgID, &requestedBy, &rawScope)
		if err == pgx.ErrNoRows {
			return done, nil
		}
		if err != nil {
			return done, fmt.Errorf("export: claim: %w", err)
		}
		if err := s.executeExport(ctx, jobID, orgID, requestedBy, rawScope); err != nil {
			s.failExport(ctx, jobID, orgID, err)
			continue
		}
		done++
	}
}

// exportRowCap bounds one export's message count; a capped export says so
// in its manifest rather than silently truncating.
const exportRowCap = 50000

type exportRevision struct {
	RevisionNo int16     `json:"revision_no"`
	Kind       int16     `json:"kind"`
	EditedBy   int64     `json:"edited_by"`
	EditedAt   time.Time `json:"edited_at"`
	PrevSource *string   `json:"prev_source"` // null = purged by retention
}

type exportAttachment struct {
	FileID int64  `json:"file_id"`
	Name   string `json:"name"`
	SHA256 string `json:"sha256"`
	Size   int64  `json:"size_bytes"`
}

type exportMessage struct {
	MessageID   int64              `json:"message_id"`
	ThreadID    int64              `json:"thread_id"`
	ChannelID   *int64             `json:"channel_id,omitempty"`
	DMSpaceID   *int64             `json:"dm_space_id,omitempty"`
	SpaceID     *int64             `json:"space_id,omitempty"`
	AuthorID    int64              `json:"author_id"`
	Source      string             `json:"source"` // "" once deleted (tombstone)
	CreatedAt   time.Time          `json:"created_at"`
	EditedAt    *time.Time         `json:"edited_at,omitempty"`
	DeletedAt   *time.Time         `json:"deleted_at,omitempty"`
	Revisions   []exportRevision   `json:"revisions,omitempty"`
	Attachments []exportAttachment `json:"attachments,omitempty"`
}

type exportUser struct {
	ID       int64  `json:"id"`
	FullName string `json:"full_name"`
	Email    string `json:"email,omitempty"`
	Kind     int16  `json:"kind"`
}

type exportDocument struct {
	ExportID    int64           `json:"export_id"`
	GeneratedAt time.Time       `json:"generated_at"`
	Scope       json.RawMessage `json:"scope"`
	Truncated   bool            `json:"truncated,omitempty"`
	Messages    []exportMessage `json:"messages"`
	Users       []exportUser    `json:"users"`
}

func (s *Service) executeExport(ctx context.Context, jobID, orgID, requestedBy int64, rawScope []byte) error {
	if s.files == nil {
		return fmt.Errorf("export: no document store wired")
	}
	var scope ExportScope
	if err := json.Unmarshal(rawScope, &scope); err != nil {
		return fmt.Errorf("export: bad scope: %w", err)
	}
	doc := exportDocument{ExportID: jobID, GeneratedAt: time.Now().UTC(), Scope: rawScope,
		Messages: []exportMessage{}, Users: []exportUser{}}

	// One pass over messages: deleted rows INCLUDED (tombstones), the
	// dimension filter ORs, the window ANDs. NULL array params disable
	// their dimension.
	rows, err := s.pool.Query(ctx, `
		SELECT m.id, m.thread_id, m.channel_id, m.dm_space_id, t.space_id,
		       m.author_id, m.source, m.created_at, m.edited_at, m.deleted_at
		FROM message m JOIN thread t ON t.id = m.thread_id
		WHERE m.org_id = $1
		  AND (($2::bigint[] IS NULL AND $3::bigint[] IS NULL AND $4::bigint[] IS NULL)
		    OR m.author_id = ANY($2)
		    OR m.channel_id = ANY($3)
		    OR t.space_id = ANY($4))
		  AND ($5::timestamptz IS NULL OR m.created_at >= $5)
		  AND ($6::timestamptz IS NULL OR m.created_at <= $6)
		ORDER BY m.id
		LIMIT $7`,
		orgID, nilIfEmpty(scope.UserIDs), nilIfEmpty(scope.ChannelIDs),
		nilIfEmpty(scope.SpaceIDs), scope.From, scope.To, exportRowCap+1)
	if err != nil {
		return fmt.Errorf("export: messages: %w", err)
	}
	byID := map[int64]*exportMessage{}
	userSet := map[int64]bool{requestedBy: true}
	for rows.Next() {
		var m exportMessage
		if err := rows.Scan(&m.MessageID, &m.ThreadID, &m.ChannelID, &m.DMSpaceID,
			&m.SpaceID, &m.AuthorID, &m.Source, &m.CreatedAt, &m.EditedAt, &m.DeletedAt); err != nil {
			rows.Close()
			return fmt.Errorf("export: scan message: %w", err)
		}
		doc.Messages = append(doc.Messages, m)
		userSet[m.AuthorID] = true
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return fmt.Errorf("export: messages: %w", err)
	}
	if len(doc.Messages) > exportRowCap {
		doc.Messages = doc.Messages[:exportRowCap]
		doc.Truncated = true
	}
	ids := make([]int64, len(doc.Messages))
	for i := range doc.Messages {
		ids[i] = doc.Messages[i].MessageID
		byID[doc.Messages[i].MessageID] = &doc.Messages[i]
	}

	// Edit history and deletion capture; a revision retention already
	// scrubbed carries prev_source = null — absent from evidence by design.
	if len(ids) > 0 {
		rrows, err := s.pool.Query(ctx, `
			SELECT message_id, revision_no, kind, edited_by, edited_at, prev_source
			FROM message_revision WHERE message_id = ANY($1)
			ORDER BY message_id, revision_no`, ids)
		if err != nil {
			return fmt.Errorf("export: revisions: %w", err)
		}
		for rrows.Next() {
			var msgID int64
			var rev exportRevision
			if err := rrows.Scan(&msgID, &rev.RevisionNo, &rev.Kind,
				&rev.EditedBy, &rev.EditedAt, &rev.PrevSource); err != nil {
				rrows.Close()
				return fmt.Errorf("export: scan revision: %w", err)
			}
			if m := byID[msgID]; m != nil {
				m.Revisions = append(m.Revisions, rev)
				userSet[rev.EditedBy] = true
			}
		}
		rrows.Close()
		if err := rrows.Err(); err != nil {
			return fmt.Errorf("export: revisions: %w", err)
		}

		arows, err := s.pool.Query(ctx, `
			SELECT fr.entity_id, f.id, f.name, encode(f.sha256, 'hex'), f.size_bytes
			FROM file_reference fr JOIN file f ON f.id = fr.file_id
			WHERE fr.entity_type = 1 AND fr.entity_id = ANY($1) AND f.org_id = $2
			ORDER BY f.id`, ids, orgID)
		if err != nil {
			return fmt.Errorf("export: attachments: %w", err)
		}
		for arows.Next() {
			var msgID int64
			var a exportAttachment
			if err := arows.Scan(&msgID, &a.FileID, &a.Name, &a.SHA256, &a.Size); err != nil {
				arows.Close()
				return fmt.Errorf("export: scan attachment: %w", err)
			}
			if m := byID[msgID]; m != nil {
				m.Attachments = append(m.Attachments, a)
			}
		}
		arows.Close()
		if err := arows.Err(); err != nil {
			return fmt.Errorf("export: attachments: %w", err)
		}
	}

	// Directory of every principal the evidence mentions.
	userIDs := make([]int64, 0, len(userSet))
	for id := range userSet {
		userIDs = append(userIDs, id)
	}
	urows, err := s.pool.Query(ctx, `
		SELECT id, full_name, COALESCE(email, ''), kind
		FROM user_account WHERE org_id = $1 AND id = ANY($2) ORDER BY id`,
		orgID, userIDs)
	if err != nil {
		return fmt.Errorf("export: users: %w", err)
	}
	for urows.Next() {
		var u exportUser
		if err := urows.Scan(&u.ID, &u.FullName, &u.Email, &u.Kind); err != nil {
			urows.Close()
			return fmt.Errorf("export: scan user: %w", err)
		}
		doc.Users = append(doc.Users, u)
	}
	urows.Close()
	if err := urows.Err(); err != nil {
		return fmt.Errorf("export: users: %w", err)
	}

	data, err := json.MarshalIndent(doc, "", " ")
	if err != nil {
		return fmt.Errorf("export: marshal: %w", err)
	}
	result, err := s.files.StoreDocument(ctx,
		auth.Identity{UserID: requestedBy, OrgID: orgID},
		fmt.Sprintf("weft-export-%d.json", jobID), "application/json", data)
	if err != nil {
		return fmt.Errorf("export: store: %w", err)
	}

	// Completion: result pointer, attachment pins (evidence outlives
	// retention), and the audit event — one transaction.
	fileIDs := []int64{}
	for i := range doc.Messages {
		for _, a := range doc.Messages[i].Attachments {
			fileIDs = append(fileIDs, a.FileID)
		}
	}
	return db.WithTx(ctx, s.pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `
			UPDATE export_job SET status = $2, result_file_id = $3, finished_at = now()
			WHERE id = $1`, jobID, exportDone, result.ID); err != nil {
			return fmt.Errorf("export: finish: %w", err)
		}
		if err := s.files.PinForExport(ctx, tx, orgID, jobID, fileIDs); err != nil {
			return err
		}
		_, err := eventlog.Append(ctx, tx, eventlog.Event{
			OrgID: orgID, ActorKind: enum.ActorSystem,
			EntityType: enum.EntityExportJob, EntityID: jobID, Verb: "export.completed",
			Payload: eventlog.MustPayload(map[string]any{
				"export_id": jobID, "result_file_id": result.ID,
				"messages": len(doc.Messages), "pinned_files": len(fileIDs),
				"truncated": doc.Truncated}),
		})
		return err
	})
}

// failExport records the failure (status + audit event with the error).
func (s *Service) failExport(ctx context.Context, jobID, orgID int64, cause error) {
	err := db.WithTx(ctx, s.pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `
			UPDATE export_job SET status = $2, finished_at = now() WHERE id = $1`,
			jobID, exportFailed); err != nil {
			return err
		}
		_, err := eventlog.Append(ctx, tx, eventlog.Event{
			OrgID: orgID, ActorKind: enum.ActorSystem,
			EntityType: enum.EntityExportJob, EntityID: jobID, Verb: "export.failed",
			Payload: eventlog.MustPayload(map[string]any{
				"export_id": jobID, "error": cause.Error()}),
		})
		return err
	})
	if err != nil {
		s.log().Error("export: fail-mark failed", "job", jobID, "err", err)
	}
}

// nilIfEmpty maps an empty id list to NULL so its SQL dimension disables.
func nilIfEmpty(ids []int64) any {
	if len(ids) == 0 {
		return nil
	}
	return ids
}
