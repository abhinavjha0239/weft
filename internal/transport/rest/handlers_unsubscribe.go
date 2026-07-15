package rest

import (
	"html/template"
	"net/http"
	"strconv"

	"github.com/abhinavjha0239/weft/internal/brand"
)

// One-click unsubscribe (P-20). These routes are registered OUTSIDE withAuth
// (the handleDownloadFile self-auth precedent): they read NO Authorization
// header — the signed link IS the capability. Both 404 when the server has no
// signing secret configured, and 401 on a bad/forged signature (constant-time
// compare in the domain). The GET is safe to prefetch (mail clients do);
// only POST mutates.

// unsubPage is the confirmation page. It is built entirely from server-side
// values (brand.Name is a constant; Action is a server-recomputed path), so
// no request-controlled string reaches the HTML — html/template escaping is
// belt-and-suspenders against a future refactor.
var unsubPage = template.Must(template.New("unsub").Parse(`<!doctype html>
<html>
<head><meta charset="utf-8"><title>Unsubscribe — {{.Brand}}</title></head>
<body>
<p>Turn off all {{.Brand}} email notifications for this account?</p>
<form method="post" action="{{.Action}}">
<button type="submit">Unsubscribe</button>
</form>
</body>
</html>
`))

// verifyUnsubReq validates a one-click request and writes the failure response
// itself, returning ok=false when it did. Unset secret → 404 (nothing to
// verify against); non-positive ids or a bad signature → 401.
func (a *api) verifyUnsubReq(w http.ResponseWriter, r *http.Request) (orgID, userID int64, ok bool) {
	if a.Notifications == nil || !a.Notifications.UnsubscribeConfigured() {
		writeError(w, http.StatusNotFound, "not found")
		return 0, 0, false
	}
	orgID, _ = strconv.ParseInt(r.URL.Query().Get("o"), 10, 64)
	userID, _ = strconv.ParseInt(r.URL.Query().Get("u"), 10, 64)
	if orgID <= 0 || userID <= 0 || !a.Notifications.VerifyUnsub(orgID, userID, r.URL.Query().Get("sig")) {
		writeError(w, http.StatusUnauthorized, "invalid or expired unsubscribe link")
		return 0, 0, false
	}
	return orgID, userID, true
}

// handleUnsubscribeGet renders the confirmation page. It MUST NOT change state
// — mail clients prefetch GET links.
func (a *api) handleUnsubscribeGet(w http.ResponseWriter, r *http.Request) {
	orgID, userID, ok := a.verifyUnsubReq(w, r)
	if !ok {
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	_ = unsubPage.Execute(w, struct {
		Brand  string
		Action string
	}{Brand: brand.Name, Action: a.Notifications.UnsubscribeFormAction(orgID, userID)})
}

// handleUnsubscribePost performs the flip: it turns off every email medium for
// the user. The body is ignored (RFC 8058 sends "List-Unsubscribe=One-Click").
func (a *api) handleUnsubscribePost(w http.ResponseWriter, r *http.Request) {
	orgID, userID, ok := a.verifyUnsubReq(w, r)
	if !ok {
		return
	}
	if err := a.Notifications.Unsubscribe(r.Context(), orgID, userID); err != nil {
		writeDomainError(w, a.Log, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]bool{"unsubscribed": true})
}
