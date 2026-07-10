package rest

import (
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

// handleDownloadFile streams the blob. Always an attachment with nosniff:
// serving user uploads inline is a stored-XSS vector (HTML/SVG); inline
// rendering arrives with an allowlist later.
func (a *api) handleDownloadFile(w http.ResponseWriter, r *http.Request, id auth.Identity) {
	fileID, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil {
		writeError(w, http.StatusBadRequest, "bad file id")
		return
	}
	meta, rc, err := a.Files.OpenDownload(r.Context(), id, fileID)
	if err != nil {
		writeDomainError(w, a.Log, r, err)
		return
	}
	defer rc.Close()
	w.Header().Set("Content-Type", meta.Mime)
	w.Header().Set("Content-Length", fmt.Sprint(meta.Size))
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.Header().Set("Content-Disposition",
		mime.FormatMediaType("attachment", map[string]string{"filename": meta.Name}))
	_, _ = io.Copy(w, rc)
}
