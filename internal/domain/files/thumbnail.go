package files

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"io"
	"io/fs"
	"log/slog"
	"net/http"
	"time"

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

// renderConcurrency bounds simultaneous decodes process-wide. A render of an
// at-cap (40 MP) source holds ~160 MB of pixels plus the CatmullRom kernel's
// CPU, so unbounded concurrent first-views (or a deliberate herd) multiply
// that without limit; two at a time keeps the worst case bounded while a
// queued render is a sub-second wait, not an error.
const renderConcurrency = 2

// Thumb-cache tuning: outcomes are keyed by the content-addressed storage
// key, so a POSITIVE entry (the rendered dimensions) can never go stale and
// lives until evicted; a NEGATIVE entry (not an image / corrupt / over-cap /
// infra failure) retries after an hour — the unfurl failed-fetch precedent —
// so a transient blob hiccup self-heals while a poisoned file stays cheap.
// The cache is per-process and bounded: a restart costs one re-describe or
// re-render per key, and at capacity an arbitrary entry is evicted.
const (
	thumbCacheMax    = 4096
	thumbNegativeTTL = time.Hour
)

// thumbOutcome is one remembered render/describe result.
type thumbOutcome struct {
	meta       imaging.Meta
	renderable bool
	until      time.Time // zero for positive entries (content-addressed, never stale)
}

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

// thumbOutcomeGet returns the remembered outcome for a storage key, expiring
// stale negative entries on read.
func (s *Service) thumbOutcomeGet(key string) (thumbOutcome, bool) {
	s.thumbMu.Lock()
	defer s.thumbMu.Unlock()
	e, ok := s.thumbOutcomes[key]
	if !ok {
		return thumbOutcome{}, false
	}
	if !e.until.IsZero() && time.Now().After(e.until) {
		delete(s.thumbOutcomes, key)
		return thumbOutcome{}, false
	}
	return e, true
}

// thumbOutcomeSet remembers a render/describe outcome, evicting an arbitrary
// entry at capacity (the map is a bounded memo, not an LRU — eviction order
// does not matter for correctness, only the bound does).
func (s *Service) thumbOutcomeSet(key string, e thumbOutcome) {
	s.thumbMu.Lock()
	defer s.thumbMu.Unlock()
	if len(s.thumbOutcomes) >= thumbCacheMax {
		for k := range s.thumbOutcomes {
			delete(s.thumbOutcomes, k)
			break
		}
	}
	s.thumbOutcomes[key] = e
}

func (s *Service) thumbRemember(key string, meta imaging.Meta) {
	s.thumbOutcomeSet(key, thumbOutcome{meta: meta, renderable: true})
}

func (s *Service) thumbRememberFailure(key string) {
	s.thumbOutcomeSet(key, thumbOutcome{until: time.Now().Add(thumbNegativeTTL)})
}

// openOriginal opens an original's bytes, logging non-absent failures: a
// missing blob is normal (purged, never written) and stays a silent 404, but
// an infrastructure error is an OUTAGE the operator must hear about even
// though the response keeps the same oracle-free 404 mask.
func (s *Service) openOriginal(ctx context.Context, key string) (io.ReadCloser, error) {
	rc, err := s.store.Open(ctx, key)
	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		s.logger().Warn("thumbnail: original blob open failed", "key", key, "err", err)
	}
	return rc, err
}

// renderThumbFrom opens the original via open, sniffs it, and — if it is an
// allowlisted raster image within the imaging caps — renders a JPEG
// thumbnail, stores it at ThumbKey, and returns the encoded bytes +
// dimensions. A non-image returns errNotRenderable; a corrupt or over-cap
// image returns imaging's error. The header is sniffed FIRST (a bounded peek)
// so a large NON-image original (e.g. an export bundle stored via
// StoreDocument) is rejected without reading it whole; the render read is
// then bounded by the upload cap.
//
// The render semaphore is acquired BEFORE open, so at most renderConcurrency
// renders hold decode buffers (or open blob handles) at once — the DoS
// hardening's memory bound. Every outcome is remembered in the per-key memo:
// success caches the dimensions, any failure negative-caches for an hour, so
// a corrupt-but-under-caps image costs ONE full decode attempt per TTL
// instead of one per GET.
func (s *Service) renderThumbFrom(ctx context.Context, originalKey string, open func() (io.ReadCloser, error)) ([]byte, imaging.Meta, error) {
	select {
	case s.thumbSem <- struct{}{}:
		defer func() { <-s.thumbSem }()
	case <-ctx.Done():
		return nil, imaging.Meta{}, ctx.Err()
	}
	src, err := open()
	if err != nil {
		return nil, imaging.Meta{}, err
	}
	defer src.Close()
	br := bufio.NewReader(src)
	head, _ := br.Peek(512)
	if !thumbnailableMimes[http.DetectContentType(head)] {
		s.thumbRememberFailure(originalKey)
		return nil, imaging.Meta{}, errNotRenderable
	}
	thumb, meta, err := imaging.Thumbnail(io.LimitReader(br, MaxUploadBytes), ThumbMaxDim)
	if err != nil {
		s.thumbRememberFailure(originalKey)
		return nil, imaging.Meta{}, err
	}
	if err := s.store.Put(ctx, ThumbKey(originalKey), bytes.NewReader(thumb)); err != nil {
		s.thumbRememberFailure(originalKey)
		s.logger().Warn("thumbnail: store failed", "key", originalKey, "err", err)
		return nil, imaging.Meta{}, apperr.Internal("store thumbnail", err)
	}
	s.thumbRemember(originalKey, meta)
	return thumb, meta, nil
}

// describeOriginal reads only the original's header (bounded) to recover the
// dimensions a Thumbnail call would have produced — the warm path's fallback
// when the process-local memo has no entry (a restart, an eviction).
func (s *Service) describeOriginal(ctx context.Context, originalKey string) (imaging.Meta, error) {
	orig, err := s.openOriginal(ctx, originalKey)
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
//
// The per-key memo runs AFTER the ACL, so it changes cost, never visibility:
// a remembered failure is an immediate 404 with zero blob IO (the DoS
// hardening — repeat probes of a poisoned file cost a map read), and a
// remembered success serves a warm hit with ONE blob open (the thumb) instead
// of re-describing the original every time.
func (s *Service) OpenThumbnail(ctx context.Context, actor auth.Identity, fileID int64) (imaging.Meta, io.ReadCloser, error) {
	_, key, err := s.authorizeDownload(ctx, actor, fileID)
	if err != nil {
		return imaging.Meta{}, nil, err
	}
	if e, ok := s.thumbOutcomeGet(key); ok && !e.renderable {
		return imaging.Meta{}, nil, apperr.NotFound("thumbnail not found")
	}
	// Fast path: the thumbnail already exists (generated at upload or a prior
	// GET). Dimensions come from the memo, or one bounded header read.
	if rc, err := s.store.Open(ctx, ThumbKey(key)); err == nil {
		if e, ok := s.thumbOutcomeGet(key); ok && e.renderable {
			return e.meta, rc, nil
		}
		meta, derr := s.describeOriginal(ctx, key)
		if derr != nil {
			rc.Close()
			return imaging.Meta{}, nil, apperr.NotFound("thumbnail not found")
		}
		s.thumbRemember(key, meta)
		return meta, rc, nil
	}
	// Miss → lazy generation from the original bytes.
	thumb, meta, err := s.renderThumbFrom(ctx, key, func() (io.ReadCloser, error) {
		return s.openOriginal(ctx, key)
	})
	if err != nil {
		// Non-image, corrupt, or over the bomb cap: no thumbnail exists and
		// none can be made. Oracle-free 404 — never a 500, never inline.
		return imaging.Meta{}, nil, apperr.NotFound("thumbnail not found")
	}
	return meta, io.NopCloser(bytes.NewReader(thumb)), nil
}
