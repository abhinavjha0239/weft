package rest

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"strconv"

	"github.com/abhinavjha0239/weft/internal/auth"
)

// avatarMaxBytes caps an avatar upload (P-06); tiny by design, no resizing v1.
const avatarMaxBytes = 2 << 20 // 2 MiB

// handleSetAvatar: PUT /me/avatar — one multipart "file", image-only by magic
// bytes and ≤2 MiB, stored through the normal content-addressed upload path,
// then pointed at by the caller's account.
func (a *api) handleSetAvatar(w http.ResponseWriter, r *http.Request, id auth.Identity) {
	data, mime, err := readImageUpload(w, r, avatarMaxBytes)
	if err != nil {
		writeDomainError(w, a.Log, r, err)
		return
	}
	f, err := a.Files.Upload(r.Context(), id, "avatar", mime, bytes.NewReader(data))
	if err != nil {
		writeDomainError(w, a.Log, r, err)
		return
	}
	if err := a.Identity.SetAvatar(r.Context(), id, f.ID); err != nil {
		writeDomainError(w, a.Log, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]int64{"avatar_file_id": f.ID})
}

// handleClearAvatar: DELETE /me/avatar — drops the pointer (the file row
// survives, becoming GC-eligible once nothing references it).
func (a *api) handleClearAvatar(w http.ResponseWriter, r *http.Request, id auth.Identity) {
	if err := a.Identity.ClearAvatar(r.Context(), id); err != nil {
		writeDomainError(w, a.Log, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
}

// handleGetAvatar: GET /users/{id}/avatar — streams a user's avatar. The
// inline disposition is SAFE here (unlike generic downloads): the bytes were
// magic-validated as an image at upload, so there is no active-content vector.
// nosniff keeps a mistaken type from being reinterpreted; the cache is private
// and short so a changed avatar propagates within the hour.
func (a *api) handleGetAvatar(w http.ResponseWriter, r *http.Request, id auth.Identity) {
	userID, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil {
		writeError(w, http.StatusBadRequest, "bad user id")
		return
	}
	meta, rc, err := a.Files.OpenAvatar(r.Context(), id, userID)
	if err != nil {
		writeDomainError(w, a.Log, r, err)
		return
	}
	defer rc.Close()
	w.Header().Set("Content-Type", meta.Mime)
	w.Header().Set("Content-Length", fmt.Sprint(meta.Size))
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.Header().Set("Cache-Control", "private, max-age=3600")
	w.Header().Set("Content-Disposition", "inline")
	_, _ = io.Copy(w, rc)
}
