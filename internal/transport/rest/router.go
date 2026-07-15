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
	"github.com/abhinavjha0239/weft/internal/domain/automation"
	"github.com/abhinavjha0239/weft/internal/domain/compliance"
	"github.com/abhinavjha0239/weft/internal/domain/dm"
	"github.com/abhinavjha0239/weft/internal/domain/files"
	"github.com/abhinavjha0239/weft/internal/domain/identity"
	"github.com/abhinavjha0239/weft/internal/domain/messaging"
	"github.com/abhinavjha0239/weft/internal/domain/notification"
	"github.com/abhinavjha0239/weft/internal/domain/search"
	"github.com/abhinavjha0239/weft/internal/domain/unfurl"
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
	// Notifications is optional in tests that never hit the endpoints.
	Notifications *notification.Service
	Files         *files.Service
	// Compliance is optional in tests that never hit the admin endpoints.
	Compliance *compliance.Service
	// Automations is optional in tests that never hit the endpoints.
	Automations *automation.Service
	// Unfurl is optional in tests that never hit the admin toggle.
	Unfurl *unfurl.Service
}

type api struct {
	Deps
	search *search.Service
	// authLimit: pre-auth endpoints, per IP (brute-force protection).
	// apiLimit: authenticated endpoints, per user.
	// publicLimit: the anonymous web-public read allowlist, per IP (P-16) —
	// its own bucket so page views never share a knob with login
	// brute-force protection.
	authLimit   *ratelimit.Limiter
	apiLimit    *ratelimit.Limiter
	publicLimit *ratelimit.Limiter
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
		Deps:        d,
		search:      search.New(d.Pool),
		authLimit:   ratelimit.New(0.5, 10), // ~30/min burst 10 per IP
		apiLimit:    ratelimit.New(50, 100), // 50 rps burst 100 per user
		publicLimit: ratelimit.New(5, 20),   // 5 rps burst 20 per IP
	}
	go a.authLimit.Janitor(ctx, time.Minute)
	go a.apiLimit.Janitor(ctx, time.Minute)
	go a.publicLimit.Janitor(ctx, time.Minute)

	mux := http.NewServeMux()
	preAuth := withIPLimit(a.authLimit)
	// P-16: the anonymous web-public read allowlist. NO identity is resolved
	// and no token is read; the Public* domain methods pin visibility=3 in
	// SQL. Add routes here only with that projection discipline.
	public := withIPLimit(a.publicLimit)
	mux.Handle("GET /api/v1/public/channels/{id}", public(http.HandlerFunc(a.handlePublicChannel)))
	mux.Handle("GET /api/v1/public/channels/{id}/threads", public(http.HandlerFunc(a.handlePublicChannelThreads)))
	mux.Handle("GET /api/v1/public/threads/{id}/messages", public(http.HandlerFunc(a.handlePublicThreadMessages)))
	mux.Handle("POST /api/v1/orgs/bootstrap", preAuth(http.HandlerFunc(a.handleBootstrap)))
	mux.Handle("POST /api/v1/auth/login", preAuth(http.HandlerFunc(a.handleLogin)))
	mux.Handle("POST /api/v1/invites/accept", preAuth(http.HandlerFunc(a.handleAcceptInvite)))
	// P-35 password reset: pre-auth (IP-limited like login); the emailed token
	// is the capability, so no Authorization is read.
	mux.Handle("POST /api/v1/password-reset/request", preAuth(http.HandlerFunc(a.handlePasswordResetRequest)))
	mux.Handle("POST /api/v1/password-reset/confirm", preAuth(http.HandlerFunc(a.handlePasswordResetConfirm)))
	mux.HandleFunc("POST /api/v1/invites", a.withAuth(a.handleCreateInvite))
	mux.HandleFunc("GET /api/v1/invites", a.withAuth(a.handleListInvites))
	mux.HandleFunc("DELETE /api/v1/invites/{id}", a.withAuth(a.handleRevokeInvite))
	mux.HandleFunc("POST /api/v1/channels", a.withAuth(a.handleCreateChannel))
	mux.HandleFunc("GET /api/v1/channels", a.withAuth(a.handleListChannels))
	mux.HandleFunc("PATCH /api/v1/channels/{id}", a.withAuth(a.handleUpdateChannel))
	mux.HandleFunc("POST /api/v1/channels/{id}/join", a.withAuth(a.handleJoinChannel))
	mux.HandleFunc("POST /api/v1/channels/{id}/leave", a.withAuth(a.handleLeaveChannel))
	// P-09 channel folders + default channels (manage_org, workspace-implicit).
	mux.HandleFunc("POST /api/v1/channel-folders", a.withAuth(a.handleCreateFolder))
	mux.HandleFunc("GET /api/v1/channel-folders", a.withAuth(a.handleListFolders))
	mux.HandleFunc("PATCH /api/v1/channel-folders/{id}", a.withAuth(a.handleUpdateFolder))
	mux.HandleFunc("DELETE /api/v1/channel-folders/{id}", a.withAuth(a.handleDeleteFolder))
	mux.HandleFunc("PUT /api/v1/default-channels", a.withAuth(a.handleSetDefaultChannels))
	mux.HandleFunc("GET /api/v1/default-channels", a.withAuth(a.handleListDefaultChannels))
	mux.HandleFunc("POST /api/v1/channels/{id}/messages", a.withAuth(a.handleSendMessage))
	mux.HandleFunc("GET /api/v1/messages/{id}", a.withAuth(a.handleGetMessage))
	mux.HandleFunc("PATCH /api/v1/messages/{id}", a.withAuth(a.handleEditMessage))
	mux.HandleFunc("DELETE /api/v1/messages/{id}", a.withAuth(a.handleDeleteMessage))
	mux.HandleFunc("POST /api/v1/messages/{id}/move", a.withAuth(a.handleMoveMessage))
	mux.HandleFunc("POST /api/v1/messages/{id}/forward", a.withAuth(a.handleForwardMessage))
	mux.HandleFunc("POST /api/v1/scheduled-messages", a.withAuth(a.handleScheduleMessage))
	mux.HandleFunc("GET /api/v1/scheduled-messages", a.withAuth(a.handleListScheduled))
	mux.HandleFunc("PATCH /api/v1/scheduled-messages/{id}", a.withAuth(a.handleUpdateScheduled))
	mux.HandleFunc("DELETE /api/v1/scheduled-messages/{id}", a.withAuth(a.handleCancelScheduled))
	mux.HandleFunc("POST /api/v1/drafts", a.withAuth(a.handleCreateDraft))
	mux.HandleFunc("GET /api/v1/drafts", a.withAuth(a.handleListDrafts))
	mux.HandleFunc("PATCH /api/v1/drafts/{id}", a.withAuth(a.handleUpdateDraft))
	mux.HandleFunc("DELETE /api/v1/drafts/{id}", a.withAuth(a.handleDeleteDraft))
	mux.HandleFunc("PUT /api/v1/messages/{id}/reactions/{emoji}", a.withAuth(a.handleAddReaction))
	mux.HandleFunc("DELETE /api/v1/messages/{id}/reactions/{emoji}", a.withAuth(a.handleRemoveReaction))
	mux.HandleFunc("PUT /api/v1/messages/{id}/pin", a.withAuth(a.handlePinMessage))
	mux.HandleFunc("DELETE /api/v1/messages/{id}/pin", a.withAuth(a.handleUnpinMessage))
	mux.HandleFunc("GET /api/v1/channels/{id}/pins", a.withAuth(a.handleListChannelPins))
	mux.HandleFunc("PUT /api/v1/messages/{id}/save", a.withAuth(a.handleSaveMessage))
	mux.HandleFunc("DELETE /api/v1/messages/{id}/save", a.withAuth(a.handleUnsaveMessage))
	mux.HandleFunc("GET /api/v1/saved", a.withAuth(a.handleListSaved))
	mux.HandleFunc("POST /api/v1/channels/{id}/threads", a.withAuth(a.handleCreateThread))
	mux.HandleFunc("GET /api/v1/channels/{id}/threads", a.withAuth(a.handleListThreads))
	mux.HandleFunc("PATCH /api/v1/threads/{id}", a.withAuth(a.handleUpdateThread))
	mux.HandleFunc("GET /api/v1/threads/{id}/messages", a.withAuth(a.handleListThreadMessages))
	mux.HandleFunc("POST /api/v1/threads/{id}/read", a.withAuth(a.handleMarkRead))
	mux.HandleFunc("PUT /api/v1/threads/{id}/subscription", a.withAuth(a.handleThreadSubscription))
	mux.HandleFunc("PUT /api/v1/channels/{id}/notification", a.withAuth(a.handleChannelNotification))
	mux.HandleFunc("PUT /api/v1/channels/{id}/sidebar", a.withAuth(a.handleSetSidebarPrefs))
	mux.HandleFunc("GET /api/v1/unreads", a.withAuth(a.handleUnreads))
	mux.HandleFunc("POST /api/v1/dms", a.withAuth(a.handleOpenDM))
	mux.HandleFunc("GET /api/v1/dms", a.withAuth(a.handleListDMs))
	mux.HandleFunc("DELETE /api/v1/dms/{id}/participants/me", a.withAuth(a.handleLeaveDM))
	mux.HandleFunc("GET /api/v1/search", a.withAuth(a.handleSearch))
	mux.HandleFunc("GET /api/v1/me", a.withAuth(a.handleMe))
	// P-29 profile edit, password change, session management.
	mux.HandleFunc("PATCH /api/v1/me", a.withAuth(a.handlePatchMe))
	mux.HandleFunc("POST /api/v1/me/password", a.withAuth(a.handleChangePassword))
	mux.HandleFunc("GET /api/v1/me/sessions", a.withAuth(a.handleListSessions))
	mux.HandleFunc("DELETE /api/v1/me/sessions/{id}", a.withAuth(a.handleRevokeSession))
	mux.HandleFunc("DELETE /api/v1/me/sessions", a.withAuth(a.handleRevokeOtherSessions))
	mux.HandleFunc("GET /api/v1/users", a.withAuth(a.handleListUsers))
	mux.HandleFunc("GET /api/v1/presence", a.withAuth(a.handlePresence))
	mux.HandleFunc("PUT /api/v1/status", a.withAuth(a.handleSetStatus))
	mux.HandleFunc("DELETE /api/v1/status", a.withAuth(a.handleClearStatus))
	mux.HandleFunc("POST /api/v1/files", a.withAuth(a.handleUploadFile))
	// Download is NOT withAuth-wrapped: it self-authenticates the bearer path
	// and leaves the signed-link path (?sig=) unauthenticated (P-07).
	mux.HandleFunc("GET /api/v1/files/{id}", a.handleDownloadFile)
	mux.HandleFunc("POST /api/v1/files/{id}/link", a.withAuth(a.handleSignLink))
	mux.HandleFunc("PUT /api/v1/me/avatar", a.withAuth(a.handleSetAvatar))
	mux.HandleFunc("DELETE /api/v1/me/avatar", a.withAuth(a.handleClearAvatar))
	mux.HandleFunc("GET /api/v1/users/{id}/avatar", a.withAuth(a.handleGetAvatar))
	mux.HandleFunc("POST /api/v1/emoji", a.withAuth(a.handleCreateEmoji))
	mux.HandleFunc("GET /api/v1/emoji", a.withAuth(a.handleListEmoji))
	mux.HandleFunc("DELETE /api/v1/emoji/{name}", a.withAuth(a.handleDeleteEmoji))
	// Unsubscribe is NOT withAuth-wrapped: the signed link self-authenticates
	// (the file-download precedent), reading no Authorization header (P-20).
	mux.HandleFunc("GET /api/v1/unsubscribe", a.handleUnsubscribeGet)
	mux.HandleFunc("POST /api/v1/unsubscribe", a.handleUnsubscribePost)
	mux.HandleFunc("GET /api/v1/notifications", a.withAuth(a.handleListNotifications))
	mux.HandleFunc("POST /api/v1/notifications/seen", a.withAuth(a.handleMarkNotificationsSeen))
	mux.HandleFunc("GET /api/v1/notification-prefs", a.withAuth(a.handleListNotificationPrefs))
	mux.HandleFunc("PUT /api/v1/notification-prefs", a.withAuth(a.handleSetNotificationPref))
	mux.HandleFunc("GET /api/v1/alert-words", a.withAuth(a.handleListAlertWords))
	mux.HandleFunc("PUT /api/v1/alert-words", a.withAuth(a.handleSetAlertWords))
	mux.HandleFunc("GET /api/v1/dnd", a.withAuth(a.handleGetDND))
	mux.HandleFunc("PUT /api/v1/dnd", a.withAuth(a.handleSetDND))
	mux.HandleFunc("GET /api/v1/vips", a.withAuth(a.handleListVIPs))
	mux.HandleFunc("PUT /api/v1/vips", a.withAuth(a.handleSetVIPs))
	mux.HandleFunc("POST /api/v1/dms/{id}/breakthrough", a.withAuth(a.handleDMBreakthrough))
	mux.HandleFunc("POST /api/v1/spaces", a.withAuth(a.handleCreateSpace))
	mux.HandleFunc("GET /api/v1/spaces", a.withAuth(a.handleListSpaces))
	mux.HandleFunc("POST /api/v1/spaces/{id}/items", a.withAuth(a.handleCreateItem))
	mux.HandleFunc("GET /api/v1/spaces/{id}/items", a.withAuth(a.handleListItems))
	mux.HandleFunc("GET /api/v1/spaces/{id}/statuses", a.withAuth(a.handleSpaceStatuses))
	mux.HandleFunc("PATCH /api/v1/items/{id}", a.withAuth(a.handleUpdateItem))
	mux.HandleFunc("POST /api/v1/threads/{id}/promote", a.withAuth(a.handlePromoteThread))
	mux.HandleFunc("POST /api/v1/threads/{id}/messages", a.withAuth(a.handleSendToThread))
	mux.HandleFunc("PUT /api/v1/admin/verbs", a.withAuth(a.handleAssignVerb))
	mux.HandleFunc("PUT /api/v1/admin/retention-policies", a.withAuth(a.handleSetRetentionPolicy))
	mux.HandleFunc("GET /api/v1/admin/retention-policies", a.withAuth(a.handleListRetentionPolicies))
	mux.HandleFunc("POST /api/v1/admin/legal-holds", a.withAuth(a.handleCreateLegalHold))
	mux.HandleFunc("GET /api/v1/admin/legal-holds", a.withAuth(a.handleListLegalHolds))
	mux.HandleFunc("POST /api/v1/admin/legal-holds/{id}/release", a.withAuth(a.handleReleaseLegalHold))
	mux.HandleFunc("POST /api/v1/admin/exports", a.withAuth(a.handleRequestExport))
	mux.HandleFunc("GET /api/v1/admin/exports", a.withAuth(a.handleListExports))
	// P-31: compliance_officer-gated read over the raw event log.
	mux.HandleFunc("GET /api/v1/audit/events", a.withAuth(a.handleAuditEvents))
	// P-19: manage_org-gated org storage quota (get usage + set cap).
	mux.HandleFunc("GET /api/v1/admin/storage-quota", a.withAuth(a.handleGetStorageQuota))
	mux.HandleFunc("PUT /api/v1/admin/storage-quota", a.withAuth(a.handleSetStorageQuota))
	// P-15: org link-preview toggle (manage_org in the domain).
	mux.HandleFunc("GET /api/v1/admin/link-previews", a.withAuth(a.handleGetLinkPreviews))
	mux.HandleFunc("PUT /api/v1/admin/link-previews", a.withAuth(a.handleSetLinkPreviews))
	mux.HandleFunc("POST /api/v1/automations", a.withAuth(a.handleCreateAutomation))
	mux.HandleFunc("GET /api/v1/automations", a.withAuth(a.handleListAutomations))
	mux.HandleFunc("PATCH /api/v1/automations/{id}", a.withAuth(a.handleUpdateAutomation))
	mux.HandleFunc("DELETE /api/v1/automations/{id}", a.withAuth(a.handleDeleteAutomation))
	mux.HandleFunc("POST /api/v1/automations/{id}/consent", a.withAuth(a.handleConsentAutomation))
	mux.HandleFunc("GET /api/v1/automations/{id}/runs", a.withAuth(a.handleListAutomationRuns))
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
