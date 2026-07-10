// Package rest is the HTTP transport (ARCHITECTURE.md §1): thin handlers
// that decode, authenticate, call domain services, and map taxonomy errors.
// No SQL, no business rules.
package rest

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/abhinavjha0239/weft/internal/auth"
	"github.com/abhinavjha0239/weft/internal/domain/dm"
	"github.com/abhinavjha0239/weft/internal/domain/identity"
	"github.com/abhinavjha0239/weft/internal/domain/messaging"
	"github.com/abhinavjha0239/weft/internal/domain/search"
	"github.com/abhinavjha0239/weft/internal/domain/worktrack"
	"github.com/abhinavjha0239/weft/internal/gateway"
	"github.com/abhinavjha0239/weft/internal/platform/apperr"
	"github.com/abhinavjha0239/weft/internal/platform/ratelimit"
)

type Deps struct {
	Pool      *pgxpool.Pool
	Hub       *gateway.Hub
	Log       *slog.Logger
	Identity  *identity.Service
	Messaging *messaging.Service
	Worktrack *worktrack.Service
	DM        *dm.Service
}

type api struct {
	Deps
	search *search.Service
	// authLimit: pre-auth endpoints, per IP (brute-force protection).
	// apiLimit: authenticated endpoints, per user.
	authLimit *ratelimit.Limiter
	apiLimit  *ratelimit.Limiter
}

// Handler builds the routed, middleware-wrapped HTTP handler. Limiter
// janitors run until ctx ends.
// markReadAdapter bridges gateway.MarkReader to the messaging service
// (gateway.Actor ↔ auth.Identity).
type markReadAdapter struct{ svc *messaging.Service }

func (m markReadAdapter) MarkRead(ctx context.Context, actor gateway.Actor, threadID, upTo int64) (int64, error) {
	return m.svc.MarkRead(ctx, auth.Identity{UserID: actor.UserID, OrgID: actor.OrgID}, threadID, upTo)
}

func Handler(ctx context.Context, d Deps) http.Handler {
	if d.Hub != nil && d.Messaging != nil {
		d.Hub.SetMarkReader(markReadAdapter{svc: d.Messaging})
	}
	a := &api{
		Deps:      d,
		search:    search.New(d.Pool),
		authLimit: ratelimit.New(0.5, 10), // ~30/min burst 10 per IP
		apiLimit:  ratelimit.New(50, 100), // 50 rps burst 100 per user
	}
	go a.authLimit.Janitor(ctx, time.Minute)
	go a.apiLimit.Janitor(ctx, time.Minute)

	mux := http.NewServeMux()
	preAuth := withIPLimit(a.authLimit)
	mux.Handle("POST /api/v1/orgs/bootstrap", preAuth(http.HandlerFunc(a.handleBootstrap)))
	mux.Handle("POST /api/v1/auth/login", preAuth(http.HandlerFunc(a.handleLogin)))
	mux.HandleFunc("POST /api/v1/channels", a.withAuth(a.handleCreateChannel))
	mux.HandleFunc("GET /api/v1/channels", a.withAuth(a.handleListChannels))
	mux.HandleFunc("POST /api/v1/channels/{id}/join", a.withAuth(a.handleJoinChannel))
	mux.HandleFunc("POST /api/v1/channels/{id}/leave", a.withAuth(a.handleLeaveChannel))
	mux.HandleFunc("POST /api/v1/channels/{id}/messages", a.withAuth(a.handleSendMessage))
	mux.HandleFunc("GET /api/v1/messages/{id}", a.withAuth(a.handleGetMessage))
	mux.HandleFunc("POST /api/v1/channels/{id}/threads", a.withAuth(a.handleCreateThread))
	mux.HandleFunc("GET /api/v1/channels/{id}/threads", a.withAuth(a.handleListThreads))
	mux.HandleFunc("PATCH /api/v1/threads/{id}", a.withAuth(a.handleUpdateThread))
	mux.HandleFunc("GET /api/v1/threads/{id}/messages", a.withAuth(a.handleListThreadMessages))
	mux.HandleFunc("POST /api/v1/threads/{id}/read", a.withAuth(a.handleMarkRead))
	mux.HandleFunc("GET /api/v1/unreads", a.withAuth(a.handleUnreads))
	mux.HandleFunc("POST /api/v1/dms", a.withAuth(a.handleOpenDM))
	mux.HandleFunc("GET /api/v1/dms", a.withAuth(a.handleListDMs))
	mux.HandleFunc("GET /api/v1/search", a.withAuth(a.handleSearch))
	mux.HandleFunc("GET /api/v1/me", a.withAuth(a.handleMe))
	mux.HandleFunc("GET /api/v1/users", a.withAuth(a.handleListUsers))
	mux.HandleFunc("GET /api/v1/presence", a.withAuth(a.handlePresence))
	mux.HandleFunc("POST /api/v1/spaces", a.withAuth(a.handleCreateSpace))
	mux.HandleFunc("GET /api/v1/spaces", a.withAuth(a.handleListSpaces))
	mux.HandleFunc("POST /api/v1/spaces/{id}/items", a.withAuth(a.handleCreateItem))
	mux.HandleFunc("GET /api/v1/spaces/{id}/items", a.withAuth(a.handleListItems))
	mux.HandleFunc("GET /api/v1/spaces/{id}/statuses", a.withAuth(a.handleSpaceStatuses))
	mux.HandleFunc("PATCH /api/v1/items/{id}", a.withAuth(a.handleUpdateItem))
	mux.HandleFunc("POST /api/v1/threads/{id}/promote", a.withAuth(a.handlePromoteThread))
	mux.HandleFunc("POST /api/v1/threads/{id}/messages", a.withAuth(a.handleSendToThread))
	mux.HandleFunc("GET /api/v1/gateway", a.handleGateway)

	return chain(mux, withRequestID, withRecover(d.Log), withLog(d.Log))
}

// withAuth authenticates and applies the per-user API limit.
func (a *api) withAuth(next func(http.ResponseWriter, *http.Request, auth.Identity)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id, err := auth.FromToken(r.Context(), a.Pool, auth.BearerToken(r))
		if err != nil {
			writeError(w, http.StatusUnauthorized, "invalid or missing token")
			return
		}
		if !a.apiLimit.Allow("u:" + itoa(id.UserID)) {
			writeError(w, http.StatusTooManyRequests, "rate limit exceeded")
			return
		}
		next(w, r, id)
	}
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, map[string]string{"error": msg})
}

// writeDomainError is THE single taxonomy→HTTP mapping (ARCHITECTURE.md §3).
func writeDomainError(w http.ResponseWriter, log *slog.Logger, r *http.Request, err error) {
	kind := apperr.KindOf(err)
	if kind == apperr.KindInternal {
		log.Error("internal", "req", RequestID(r.Context()),
			"path", r.URL.Path, "err", err)
	}
	writeError(w, apperr.HTTPStatus(kind), apperr.ClientMessage(err))
}
