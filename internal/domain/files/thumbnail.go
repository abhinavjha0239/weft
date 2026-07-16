package files

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"

	"github.com/abhinavjha0239/weft/internal/auth"
	"github.com/abhinavjha0239/weft/internal/platform/apperr"
	"github.com/abhinavjha0239/weft/internal/platform/imaging"
)

// Image thumbnails (P-18). A thumbnail is a DERIVED blob, never a File row: it
// lives at a deterministic key under the ORIGINAL's content-addressed
// StorageKey, so org dedup rides free (two files sharing a sha share one
// thumb) and blob GC of the original reclaims the thumb too (the janitor
// mirrors store.Delete on ThumbKey, gated by the SAME twin rule). Only bytes
// WE encoded (JPEG) ever serve inline; originals keep attachment disposition
// (the stored-XSS stance in handlers_files.go is unchanged).

// ThumbMaxDim bounds the thumbnail's longer side (never upscaled). It is
// coupled to the "w480" segment of ThumbKey — change both together.
const ThumbMaxDim = 480

// thumbnailableMimes is the inline-rendering allowlist: the SAME magic-byte
// image set avatars and custom emoji validate (handlers_media.go —
// png/jpeg/webp/gif), with SVG deliberately excluded as an active-content XSS
// vector. The type is decided by http.DetectContentType over the STORED bytes,
// never a client-declared mime.
var thumbnailableMimes = map[string]bool{
	"image/png":  true,
	"image/jpeg": true,
	"image/webp": true,
	"image/gif":  true,
}

// errNotRenderable marks an original that is not an allowlisted raster image.
// It is a control signal, never surfaced to a client: the caller skips
// generation (upload) or returns an oracle-free 404 (serve).
var errNotRenderable = errors.New("files: not a thumbnailable image")

// ThumbKey derives the thumbnail blob key from an original's storage key. It
// is exported so the compliance janitor reclaims the derived blob at purge
// without duplicating the derivation (its GC lane cites this function).
//
// The suffix is ".thumb/w480.jpg", NOT the "/thumb/w480.jpg" the spec sketches:
// on the fs blob store the original's StorageKey IS a leaf file, so a child key
// would try to mkdir over it ("not a directory"). ".thumb/" makes the rendition
// directory a SIBLING of the original leaf instead — same content-addressed
// namespace (dedup + the GC twin rule still ride free), same "w480.jpg"
// rendition name, and it can never collide with an original key (those end in a
// bare 64-hex sha). Coupled to ThumbMaxDim — change "w480" and the const
// together.
func ThumbKey(originalKey string) string {
	return originalKey + ".thumb/w480.jpg"
}

// SetLogger overrides the default logger (weftd wires its own); mirrors the
// compliance service's optional-logger pattern.
func (s *Service) SetLogger(l *slog.Logger) { s.log = l }

func (s *Service) logger() *slog.Logger {
	if s.log != nil {
		return s.log
	}
	return slog.Default()
}

// renderThumbFrom sniffs src (an original's bytes) and, if it is an allowlisted
// raster image within the imaging caps, renders a JPEG thumbnail, stores it at
// ThumbKey, and returns the encoded bytes + dimensions. A non-image returns
// errNotRenderable; a corrupt or over-cap image returns imaging's error.
// Neither stores anything — the caller decides skip (upload) vs 404 (serve).
// The header is sniffed FIRST (a bounded peek) so a large NON-image original
// (e.g. an export bundle stored via StoreDocument) is rejected without reading
// it whole; the render read is then bounded by the upload cap.
func (s *Service) renderThumbFrom(ctx context.Context, originalKey string, src io.Reader) ([]byte, imaging.Meta, error) {
	br := bufio.NewReader(src)
	head, _ := br.Peek(512)
	if !thumbnailableMimes[http.DetectContentType(head)] {
		return nil, imaging.Meta{}, errNotRenderable
	}
	thumb, meta, err := imaging.Thumbnail(io.LimitReader(br, MaxUploadBytes), ThumbMaxDim)
	if err != nil {
		return nil, imaging.Meta{}, err
	}
	if err := s.store.Put(ctx, ThumbKey(originalKey), bytes.NewReader(thumb)); err != nil {
		return nil, imaging.Meta{}, apperr.Internal("store thumbnail", err)
	}
	return thumb, meta, nil
}

// describeOriginal reads only the original's header (bounded) to recover the
// dimensions a Thumbnail call would have produced — used on a cache HIT to
// fill the response's dimension headers without re-decoding or re-encoding.
func (s *Service) describeOriginal(ctx context.Context, originalKey string) (imaging.Meta, error) {
	orig, err := s.store.Open(ctx, originalKey)
	if err != nil {
		return imaging.Meta{}, err
	}
	defer orig.Close()
	br := bufio.NewReader(orig)
	head, _ := br.Peek(512)
	if !thumbnailableMimes[http.DetectContentType(head)] {
		return imaging.Meta{}, errNotRenderable
	}
	return imaging.Describe(br, ThumbMaxDim)
}

// OpenThumbnail authorizes the viewer with EXACTLY authorizeDownload — a file
// you cannot download has no thumbnail, so denied, absent, quarantined, and
// non-image all collapse to one oracle-free 404 — then opens the derived
// thumb. On a miss it lazily backfills once (pre-P-18 uploads and StoreDocument
// artifacts generate on first GET) if the original is an allowlisted image
// within caps, else 404. Returns imaging.Meta for the response headers.
func (s *Service) OpenThumbnail(ctx context.Context, actor auth.Identity, fileID int64) (imaging.Meta, io.ReadCloser, error) {
	_, key, err := s.authorizeDownload(ctx, actor, fileID)
	if err != nil {
		return imaging.Meta{}, nil, err
	}
	// Fast path: the thumbnail already exists (generated at upload or a prior
	// GET). Describe the original header for the response dimensions.
	if rc, err := s.store.Open(ctx, ThumbKey(key)); err == nil {
		meta, derr := s.describeOriginal(ctx, key)
		if derr != nil {
			rc.Close()
			return imaging.Meta{}, nil, apperr.NotFound("thumbnail not found")
		}
		return meta, rc, nil
	}
	// Miss → lazy generation from the original bytes.
	orig, err := s.store.Open(ctx, key)
	if err != nil {
		return imaging.Meta{}, nil, apperr.NotFound("thumbnail not found")
	}
	defer orig.Close()
	thumb, meta, err := s.renderThumbFrom(ctx, key, orig)
	if err != nil {
		// Non-image, corrupt, or over the bomb cap: no thumbnail exists and
		// none can be made. Oracle-free 404 — never a 500, never inline.
		return imaging.Meta{}, nil, apperr.NotFound("thumbnail not found")
	}
	return meta, io.NopCloser(bytes.NewReader(thumb)), nil
}
