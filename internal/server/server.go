// Package server is the HTTP API. Writes are REST (ADR-002 P3): every domain
// write and its event-log append share ONE transaction — the outbox rule.
package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"html"
	"net/http"
	"strconv"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/abhinavjha0239/weft/internal/auth"
	"github.com/abhinavjha0239/weft/internal/enum"
	"github.com/abhinavjha0239/weft/internal/eventlog"
	"github.com/abhinavjha0239/weft/internal/gateway"
)

type Server struct {
	pool *pgxpool.Pool
	hub  *gateway.Hub
}

func New(pool *pgxpool.Pool, hub *gateway.Hub) *Server {
	return &Server{pool: pool, hub: hub}
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/v1/orgs/bootstrap", s.handleBootstrap)
	mux.HandleFunc("POST /api/v1/auth/login", s.handleLogin)
	mux.HandleFunc("POST /api/v1/channels/{id}/messages", s.withAuth(s.handleSendMessage))
	mux.HandleFunc("GET /api/v1/messages/{id}", s.withAuth(s.handleGetMessage))
	mux.HandleFunc("GET /api/v1/gateway", s.handleGateway)
	return mux
}

func (s *Server) withAuth(next func(http.ResponseWriter, *http.Request, auth.Identity)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id, err := auth.FromToken(r.Context(), s.pool, auth.BearerToken(r))
		if err != nil {
			httpError(w, http.StatusUnauthorized, "invalid or missing token")
			return
		}
		next(w, r, id)
	}
}

func (s *Server) handleGateway(w http.ResponseWriter, r *http.Request) {
	id, err := auth.FromToken(r.Context(), s.pool, auth.BearerToken(r))
	if err != nil {
		httpError(w, http.StatusUnauthorized, "invalid or missing token")
		return
	}
	s.hub.Serve(w, r, id)
}

type bootstrapRequest struct {
	OrgName  string `json:"org_name"`
	OrgSlug  string `json:"org_slug"`
	Email    string `json:"email"`
	Password string `json:"password"`
	FullName string `json:"full_name"`
}

// handleBootstrap creates org + workspace + owner + #general (with its
// channel-root thread, F-15) in one transaction and returns a session.
func (s *Server) handleBootstrap(w http.ResponseWriter, r *http.Request) {
	var req bootstrapRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil ||
		req.OrgSlug == "" || req.Email == "" || len(req.Password) < 8 {
		httpError(w, http.StatusBadRequest, "org_slug, email, password (min 8) required")
		return
	}
	ctx := r.Context()
	pwHash, err := auth.HashPassword(req.Password)
	if err != nil {
		httpError(w, http.StatusInternalServerError, "hash failure")
		return
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		httpError(w, http.StatusInternalServerError, "begin failed")
		return
	}
	defer tx.Rollback(ctx)

	var orgID, wsID, userID, channelID, rootThreadID int64
	if err := tx.QueryRow(ctx,
		`INSERT INTO org (name, slug) VALUES ($1, $2) RETURNING id`,
		orDefault(req.OrgName, req.OrgSlug), req.OrgSlug).Scan(&orgID); err != nil {
		httpError(w, http.StatusConflict, "org slug unavailable")
		return
	}
	must := func(err error) bool {
		if err != nil {
			httpError(w, http.StatusInternalServerError, "bootstrap failed")
			return false
		}
		return true
	}
	if !must(tx.QueryRow(ctx,
		`INSERT INTO workspace (org_id, name, slug) VALUES ($1, 'General', 'general') RETURNING id`,
		orgID).Scan(&wsID)) {
		return
	}
	if !must(tx.QueryRow(ctx, `
		INSERT INTO user_account (org_id, kind, email, full_name, role)
		VALUES ($1, $2, $3, $4, 10) RETURNING id`,
		orgID, enum.UserHuman, req.Email, orDefault(req.FullName, req.Email)).Scan(&userID)) {
		return
	}
	if _, err := tx.Exec(ctx,
		`INSERT INTO user_credential (user_id, password_hash) VALUES ($1, $2)`,
		userID, pwHash); !must(err) {
		return
	}
	if _, err := tx.Exec(ctx,
		`INSERT INTO membership (user_id, workspace_id, role) VALUES ($1, $2, 10)`,
		userID, wsID); !must(err) {
		return
	}
	if !must(tx.QueryRow(ctx, `
		INSERT INTO channel (org_id, workspace_id, name, creator_id)
		VALUES ($1, $2, 'general', $3) RETURNING id`,
		orgID, wsID, userID).Scan(&channelID)) {
		return
	}
	if !must(tx.QueryRow(ctx,
		`INSERT INTO thread (org_id, channel_id, kind) VALUES ($1, $2, 2) RETURNING id`,
		orgID, channelID).Scan(&rootThreadID)) {
		return
	}
	if _, err := tx.Exec(ctx,
		`UPDATE channel SET root_thread_id = $1 WHERE id = $2`,
		rootThreadID, channelID); !must(err) {
		return
	}
	if _, err := tx.Exec(ctx,
		`INSERT INTO channel_member (channel_id, user_id) VALUES ($1, $2)`,
		channelID, userID); !must(err) {
		return
	}
	if _, err := eventlog.Append(ctx, tx, eventlog.Event{
		OrgID: orgID, WorkspaceID: &wsID, ActorKind: enum.ActorHuman, ActorID: &userID,
		EntityType: enum.EntityChannel, EntityID: channelID, Verb: "channel.created",
		Payload: mustJSON(map[string]any{"channel_id": channelID, "name": "general"}),
	}); !must(err) {
		return
	}
	token, err := auth.CreateSession(ctx, tx, userID)
	if !must(err) {
		return
	}
	if !must(tx.Commit(ctx)) {
		return
	}
	writeJSON(w, http.StatusCreated, map[string]any{
		"org_id": orgID, "workspace_id": wsID, "user_id": userID,
		"channel_id": channelID, "token": token,
	})
}

func (s *Server) handleLogin(w http.ResponseWriter, r *http.Request) {
	var req struct {
		OrgSlug  string `json:"org_slug"`
		Email    string `json:"email"`
		Password string `json:"password"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		httpError(w, http.StatusBadRequest, "bad request")
		return
	}
	token, err := auth.Login(r.Context(), s.pool, req.OrgSlug, req.Email, req.Password)
	if err != nil {
		httpError(w, http.StatusUnauthorized, "invalid credentials")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"token": token})
}

type sendMessageRequest struct {
	ThreadID int64  `json:"thread_id,omitempty"` // omit = channel root
	Content  string `json:"content"`
}

func (s *Server) handleSendMessage(w http.ResponseWriter, r *http.Request, id auth.Identity) {
	channelID, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil {
		httpError(w, http.StatusBadRequest, "bad channel id")
		return
	}
	var req sendMessageRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Content == "" {
		httpError(w, http.StatusBadRequest, "content required")
		return
	}
	ctx := r.Context()

	msgID, err := s.sendMessage(ctx, id, channelID, req)
	switch {
	case errors.Is(err, errForbidden):
		httpError(w, http.StatusForbidden, "not a channel member")
	case err != nil:
		httpError(w, http.StatusInternalServerError, "send failed")
	default:
		writeJSON(w, http.StatusCreated, map[string]any{"message_id": msgID})
	}
}

var errForbidden = errors.New("forbidden")

// sendMessage is the first real domain write: permission check, message
// insert, thread bump (never on channel roots, F-15), and the event append —
// all one transaction (ADR-003 E1: no dual write).
func (s *Server) sendMessage(ctx context.Context, id auth.Identity, channelID int64, req sendMessageRequest) (int64, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback(ctx)

	// M0 permission check: channel membership. The full (verb,scope)→group
	// resolver (ADR-006) replaces this line, same transaction shape.
	var member bool
	if err := tx.QueryRow(ctx, `
		SELECT EXISTS (SELECT 1 FROM channel_member
		 WHERE channel_id = $1 AND user_id = $2 AND unsubscribed_at IS NULL)`,
		channelID, id.UserID).Scan(&member); err != nil {
		return 0, err
	}
	if !member {
		return 0, errForbidden
	}

	threadID := req.ThreadID
	var threadKind int16
	if threadID == 0 {
		if err := tx.QueryRow(ctx,
			`SELECT root_thread_id, 2 FROM channel WHERE id = $1 AND org_id = $2`,
			channelID, id.OrgID).Scan(&threadID, &threadKind); err != nil {
			return 0, err
		}
	} else {
		if err := tx.QueryRow(ctx,
			`SELECT kind FROM thread WHERE id = $1 AND channel_id = $2 AND org_id = $3`,
			threadID, channelID, id.OrgID).Scan(&threadKind); err != nil {
			return 0, err
		}
	}

	// Placeholder renderer until the Portable AST engine (ADR-007) lands:
	// escaped source in one paragraph node.
	rendered := "<p>" + html.EscapeString(req.Content) + "</p>"
	ast := mustJSON(map[string]any{"doc": []any{map[string]any{
		"type": "paragraph", "text": req.Content}}})

	var msgID int64
	if err := tx.QueryRow(ctx, `
		INSERT INTO message (org_id, thread_id, channel_id, author_id,
			source, ast, rendered, render_version)
		VALUES ($1,$2,$3,$4,$5,$6,$7,1) RETURNING id`,
		id.OrgID, threadID, channelID, id.UserID,
		req.Content, ast, rendered).Scan(&msgID); err != nil {
		return 0, err
	}
	// F-15: channel-root threads carry no denormalized counters.
	if threadKind == 1 {
		if _, err := tx.Exec(ctx, `
			UPDATE thread SET last_activity_at = now(),
			       message_count = message_count + 1 WHERE id = $1`,
			threadID); err != nil {
			return 0, err
		}
	}
	// F-4 payload indirection: ids only, never the content.
	if _, err := eventlog.Append(ctx, tx, eventlog.Event{
		OrgID: id.OrgID, ActorKind: enum.ActorHuman, ActorID: &id.UserID,
		EntityType: enum.EntityMessage, EntityID: msgID, Verb: "message.created",
		Payload: mustJSON(map[string]any{
			"message_id": msgID, "channel_id": channelID, "thread_id": threadID}),
	}); err != nil {
		return 0, err
	}
	return msgID, tx.Commit(ctx)
}

func (s *Server) handleGetMessage(w http.ResponseWriter, r *http.Request, id auth.Identity) {
	msgID, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil {
		httpError(w, http.StatusBadRequest, "bad message id")
		return
	}
	var out struct {
		ID        int64  `json:"id"`
		ChannelID int64  `json:"channel_id"`
		ThreadID  int64  `json:"thread_id"`
		AuthorID  int64  `json:"author_id"`
		Source    string `json:"source"`
		Rendered  string `json:"rendered"`
	}
	// Access = membership of the governing channel (M0 slice of the ACL model).
	err = s.pool.QueryRow(r.Context(), `
		SELECT m.id, m.channel_id, m.thread_id, m.author_id, m.source, m.rendered
		FROM message m
		JOIN channel_member cm ON cm.channel_id = m.channel_id
		 AND cm.user_id = $2 AND cm.unsubscribed_at IS NULL
		WHERE m.id = $1 AND m.org_id = $3 AND m.deleted_at IS NULL`,
		msgID, id.UserID, id.OrgID).Scan(&out.ID, &out.ChannelID, &out.ThreadID,
		&out.AuthorID, &out.Source, &out.Rendered)
	if errors.Is(err, pgx.ErrNoRows) {
		httpError(w, http.StatusNotFound, "not found")
		return
	}
	if err != nil {
		httpError(w, http.StatusInternalServerError, "lookup failed")
		return
	}
	writeJSON(w, http.StatusOK, out)
}

func orDefault(v, d string) string {
	if v == "" {
		return d
	}
	return v
}

func mustJSON(v any) json.RawMessage {
	b, err := json.Marshal(v)
	if err != nil {
		panic(fmt.Sprintf("marshal: %v", err))
	}
	return b
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func httpError(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, map[string]string{"error": msg})
}
