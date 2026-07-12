package rest

import (
	"fmt"
	"io"
	"net/http"

	"github.com/abhinavjha0239/weft/internal/platform/apperr"
)

// Media upload validation shared by avatars and custom emoji (P-06). Both are
// tiny, image-only, and must be validated BEFORE the content-addressed store
// ever sees the bytes — so the part is read fully into memory (bounded), and
// the image type is decided by MAGIC BYTES via http.DetectContentType, never
// the client-declared mime. SVG is deliberately NOT allowed: it is an
// active-content XSS vector, so it is rejected like any non-image.
var imageMagicMimes = map[string]bool{
	"image/png":  true,
	"image/jpeg": true,
	"image/webp": true,
	"image/gif":  true,
}

// readImageUpload reads the "file" multipart part into memory (bounded by
// maxBytes+1 so an oversize part is caught, not truncated silently) and
// validates it is a real image. It returns the bytes and the sniffed mime, or
// a taxonomy Invalid (→ 400) for a missing/malformed/oversize part or
// non-image content. The whole request body is capped as a backstop against
// abusive extra parts.
func readImageUpload(w http.ResponseWriter, r *http.Request, maxBytes int64) (data []byte, mime string, err error) {
	r.Body = http.MaxBytesReader(w, r.Body, maxBytes+1<<20)
	mr, err := r.MultipartReader()
	if err != nil {
		return nil, "", apperr.Invalid("multipart body required")
	}
	for {
		part, err := mr.NextPart()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, "", apperr.Invalid("malformed multipart body")
		}
		if part.FormName() != "file" {
			continue
		}
		data, err = io.ReadAll(io.LimitReader(part, maxBytes+1))
		if err != nil {
			return nil, "", apperr.Invalid("could not read upload")
		}
		if len(data) == 0 {
			return nil, "", apperr.Invalid("empty file")
		}
		if int64(len(data)) > maxBytes {
			return nil, "", apperr.Invalid(fmt.Sprintf("file too large (max %d KiB)", maxBytes>>10))
		}
		mime = http.DetectContentType(data)
		if !imageMagicMimes[mime] {
			return nil, "", apperr.Invalid("file must be a PNG, JPEG, WebP, or GIF image")
		}
		return data, mime, nil
	}
	return nil, "", apperr.Invalid(`multipart part "file" required`)
}
