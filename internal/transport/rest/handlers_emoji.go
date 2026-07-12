package rest

import (
	"bytes"
	"net/http"

	"github.com/abhinavjha0239/weft/internal/auth"
	"github.com/abhinavjha0239/weft/internal/domain/messaging"
)

// emojiMaxBytes caps a custom-emoji image (P-06); tiny by design.
const emojiMaxBytes = 256 << 10 // 256 KiB

// handleCreateEmoji: POST /emoji?name=<name> + multipart "file" — manage_org
// gated. The name is validated up front so a bad one never spools an upload;
// the image is magic-byte + size gated, stored, then registered.
func (a *api) handleCreateEmoji(w http.ResponseWriter, r *http.Request, id auth.Identity) {
	name := r.URL.Query().Get("name")
	if !messaging.ValidEmojiName(name) {
		writeError(w, http.StatusBadRequest, "emoji name must be 2-32 chars of a-z, 0-9, or _")
		return
	}
	data, mime, err := readImageUpload(w, r, emojiMaxBytes)
	if err != nil {
		writeDomainError(w, a.Log, r, err)
		return
	}
	f, err := a.Files.Upload(r.Context(), id, "emoji-"+name, mime, bytes.NewReader(data))
	if err != nil {
		writeDomainError(w, a.Log, r, err)
		return
	}
	e, err := a.Messaging.CreateEmoji(r.Context(), id, name, f.ID)
	if err != nil {
		writeDomainError(w, a.Log, r, err)
		return
	}
	writeJSON(w, http.StatusCreated, e)
}

// handleListEmoji: GET /emoji — the org's live custom emoji.
func (a *api) handleListEmoji(w http.ResponseWriter, r *http.Request, id auth.Identity) {
	list, err := a.Messaging.ListEmoji(r.Context(), id)
	if err != nil {
		writeDomainError(w, a.Log, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"emoji": list})
}

// handleDeleteEmoji: DELETE /emoji/{name} — soft delete (manage_org).
func (a *api) handleDeleteEmoji(w http.ResponseWriter, r *http.Request, id auth.Identity) {
	if err := a.Messaging.DeleteEmoji(r.Context(), id, r.PathValue("name")); err != nil {
		writeDomainError(w, a.Log, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
}
