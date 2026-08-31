package rest

import (
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"strconv"

	"github.com/abhinavjha0239/weft/internal/auth"
	"github.com/abhinavjha0239/weft/internal/domain/files"
)

// handleUploadFile accepts one multipart part named "file".
func (a *api) handleUploadFile(w http.ResponseWriter, r *http.Request, id auth.Identity) {
	r.Body = http.MaxBytesReader(w, r.Body, files.MaxUploadBytes+1<<20)
	mr, err := r.MultipartReader()
	if err != nil {
		writeError(w, http.StatusBadRequest, "multipart body required")
		return
	}
	for {
		part, err := mr.NextPart()
		if err == io.EOF {
			break
		}
		if err != nil {
			writeError(w, http.StatusBadRequest, "malformed multipart body")
			return
		}
		if part.FormName() != "file" {
			continue
		}
		f, err := a.Files.Upload(r.Context(), id, part.FileName(),
			part.Header.Get("Content-Type"), part)
		if err != nil {
			writeDomainError(w, a.Log, r, err)
			return
		}
		writeJSON(w, http.StatusCreated, f)
		return
	}
	writeError(w, http.StatusBadRequest, `multipart part "file" required`)
}

// handleDownloadFile streams a file by EITHER a bearer token (the F-12 union
// ACL) OR a valid signed link (?sig=&exp=&org=) — the latter is what lets an
// <img src> load without an Authorization header (P-07). This route is NOT
// wrapped in withAuth: the handler authenticates the bearer path itself and
// leaves the signed path unauthenticated (the signature IS the capability).
// Always attachment + nosniff — serving arbitrary uploads inline is a
// stored-XSS vector (avatars use a separate magic-validated inline path).
func (a *api) handleDownloadFile(w http.ResponseWriter, r *http.Request) {
	fileID, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil {
		writeError(w, http.StatusBadRequest, "bad file id")
		return
	}
	if sig := r.URL.Query().Get("sig"); sig != "" {
		orgID, _ := strconv.ParseInt(r.URL.Query().Get("org"), 10, 64)
		meta, rc, err := a.Files.OpenSigned(r.Context(), fileID, orgID, sig, r.URL.Query().Get("exp"))
		if err != nil {
			writeDomainError(w, a.Log, r, err)
			return
		}
		defer rc.Close()
		a.streamFile(w, meta, rc)
		return
	}
	// Bearer path (the pre-signed-link behavior): authenticate, apply the
	// per-user limit, and enforce the union ACL.
	id, err := auth.FromToken(r.Context(), a.Pool, auth.BearerToken(r))
	if err != nil {
		writeError(w, http.StatusUnauthorized, "invalid or missing token")
		return
	}
	if !a.apiLimit.Allow("u:" + itoa(id.UserID)) {
		writeError(w, http.StatusTooManyRequests, "rate limit exceeded")
		return
	}
	meta, rc, err := a.Files.OpenDownload(r.Context(), id, fileID)
	if err != nil {
		writeDomainError(w, a.Log, r, err)
		return
	}
	defer rc.Close()
	a.streamFile(w, meta, rc)
}

// handleThumbnail: GET /api/v1/files/{id}/thumbnail — serves a derived JPEG
// rendition of an image file, INLINE. Authorization is EXACTLY the download
// ACL (OpenThumbnail → authorizeDownload): a file you cannot download has no
// thumbnail, so denied / absent / quarantined / non-image collapse to one
// oracle-free 404. Inline is safe ONLY because WE encoded these bytes as JPEG
// (the avatar precedent, handlers_avatar.go) — originals keep attachment
// disposition (handlers_files.go's stored-XSS stance is untouched). The
// original + thumbnail dimensions ride as headers (client layout hints; no
// schema change).
func (a *api) handleThumbnail(w http.ResponseWriter, r *http.Request, id auth.Identity) {
	fileID, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil {
		writeError(w, http.StatusBadRequest, "bad file id")
		return
	}
	meta, rc, err := a.Files.OpenThumbnail(r.Context(), id, fileID)
	if err != nil {
		writeDomainError(w, a.Log, r, err)
		return
	}
	defer rc.Close()
	h := w.Header()
	h.Set("Content-Type", "image/jpeg")
	h.Set("X-Content-Type-Options", "nosniff")
	h.Set("Cache-Control", "private, max-age=3600")
	// The response varies by bearer token (the ACL). "private" already bars
	// shared caches; Vary makes even a misconfigured one key per credential.
	h.Set("Vary", "Authorization")
	h.Set("Content-Disposition", "inline")
	h.Set("X-Image-Width", strconv.Itoa(meta.SrcW))
	h.Set("X-Image-Height", strconv.Itoa(meta.SrcH))
	h.Set("X-Thumbnail-Width", strconv.Itoa(meta.W))
	h.Set("X-Thumbnail-Height", strconv.Itoa(meta.H))
	_, _ = io.Copy(w, rc)
}

// handleSignLink mints a signed capability URL for a file (POST
// /files/{id}/link), running the SAME download ACL as a fetch. A server with
// no signing secret configured is a clear 500 (operator misconfiguration, not
// a sensitive detail).
func (a *api) handleSignLink(w http.ResponseWriter, r *http.Request, id auth.Identity) {
	fileID, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil {
		writeError(w, http.StatusBadRequest, "bad file id")
		return
	}
	res, err := a.Files.SignedLink(r.Context(), id, fileID)
	if errors.Is(err, files.ErrNoSigningSecret) {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if err != nil {
		writeDomainError(w, a.Log, r, err)
		return
	}
	writeJSON(w, http.StatusOK, res)
}

// handleGetStorageQuota: GET /admin/storage-quota → {max_bytes, used_bytes}
// (manage_storage_quota-gated in the domain). max_bytes 0 = unlimited.
func (a *api) handleGetStorageQuota(w http.ResponseWriter, r *http.Request, id auth.Identity) {
	info, err := a.Files.StorageQuota(r.Context(), id)
	if err != nil {
		writeDomainError(w, a.Log, r, err)
		return
	}
	writeJSON(w, http.StatusOK, info)
}

// handleSetStorageQuota: PUT /admin/storage-quota {max_bytes}
// (manage_storage_quota).
func (a *api) handleSetStorageQuota(w http.ResponseWriter, r *http.Request, id auth.Identity) {
	type req struct {
		MaxBytes int64 `json:"max_bytes"`
	}
	in, ok := decode[req](w, r)
	if !ok {
		return
	}
	if err := a.Files.SetStorageQuota(r.Context(), id, in.MaxBytes); err != nil {
		writeDomainError(w, a.Log, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]int64{"max_bytes": in.MaxBytes})
}

// streamFile writes the download response shared by both access paths.
func (a *api) streamFile(w http.ResponseWriter, meta files.Meta, rc io.ReadCloser) {
	w.Header().Set("Content-Type", meta.Mime)
	w.Header().Set("Content-Length", fmt.Sprint(meta.Size))
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.Header().Set("Content-Disposition",
		mime.FormatMediaType("attachment", map[string]string{"filename": meta.Name}))
	_, _ = io.Copy(w, rc)
}
