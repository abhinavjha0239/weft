// Package imaging renders small JPEG thumbnails from user-uploaded raster
// images, behind a pure-Go seam (no cgo, no libvips — the no-dep bias; the
// seam keeps a native encoder swappable later). It decodes PNG/JPEG/GIF via
// the standard library and WebP via golang.org/x/image (decode-only), scales
// with a high-quality resampling kernel, and always re-encodes as JPEG over a
// white background — one universal, small, alpha-free rendition.
//
// THE SECURITY PIN lives here: a decompression bomb (a tiny file whose header
// declares gigapixel dimensions) is refused at image.DecodeConfig — a bounded
// header read — BEFORE any full decode allocates width*height*4 bytes.
package imaging

import (
	"bytes"
	"errors"
	"fmt"
	"image"
	"image/color"
	"image/jpeg"
	"io"
	"math"

	// Register decoders for the allowlisted formats. WebP is decode-only from
	// x/image; the standard library covers PNG/JPEG/GIF (image/jpeg is imported
	// non-blank below for Encode, which also registers its decoder). An
	// animated GIF decodes to its first frame — a recorded gap.
	_ "image/gif"
	_ "image/png"

	"golang.org/x/image/draw"
	_ "golang.org/x/image/webp"
)

const (
	// maxPixels and maxDimension are THE decompression-bomb caps (see
	// withinCaps): a header declaring more than 40 MP, or either side beyond
	// 12000 px, is refused. A 50000×50000 PNG is a few KB on disk but ~10 GB
	// once decoded.
	maxPixels    = 40_000_000
	maxDimension = 12_000

	// jpegQuality trades a little fidelity for small files; invisible at
	// thumbnail sizes.
	jpegQuality = 80
)

// ErrTooLarge marks a source whose DECLARED dimensions exceed the
// decompression-bomb caps — refused from the header, before any full decode.
var ErrTooLarge = errors.New("imaging: source dimensions exceed decode caps")

// Meta describes a rendered thumbnail: the source's pixel dimensions and the
// thumbnail's. Callers surface these as response headers (layout hints); they
// are never persisted (no schema change).
type Meta struct {
	SrcW, SrcH int
	W, H       int
}

// Thumbnail decodes an image from r, scales it to fit within maxDim on its
// longer side (never upscaling), composites any transparency over white, and
// returns a JPEG (quality 80) plus its dimensions. Formats: PNG, JPEG, GIF
// (first frame) and WebP. Callers pass a bounded reader — the on-disk size is
// capped upstream (the upload limit) — so buffering the source for the
// header-then-decode two-pass is safe.
func Thumbnail(r io.Reader, maxDim int) ([]byte, Meta, error) {
	data, err := io.ReadAll(r)
	if err != nil {
		return nil, Meta{}, err
	}
	// THE SECURITY PIN (P-18): read only the header first (image.DecodeConfig
	// is a bounded read) and refuse a decompression bomb BEFORE image.Decode
	// allocates a full pixel buffer. RED/GREEN: delete the withinCaps guard in
	// plan() and the crafted over-cap image in TestImageThumbnails decodes and
	// serves a thumbnail, flipping the `bomb → 404` assertion red.
	cfg, _, err := image.DecodeConfig(bytes.NewReader(data))
	if err != nil {
		return nil, Meta{}, err
	}
	meta, err := plan(cfg.Width, cfg.Height, maxDim)
	if err != nil {
		return nil, Meta{}, err
	}
	src, _, err := image.Decode(bytes.NewReader(data))
	if err != nil {
		return nil, Meta{}, err
	}
	dst := image.NewRGBA(image.Rect(0, 0, meta.W, meta.H))
	// JPEG has no alpha: composite over an opaque white background so a
	// transparent PNG/WebP does not encode its transparent areas as black.
	// Fill white (Src), then scale the source over it (Over).
	draw.Draw(dst, dst.Bounds(), image.NewUniform(color.White), image.Point{}, draw.Src)
	draw.CatmullRom.Scale(dst, dst.Bounds(), src, src.Bounds(), draw.Over, nil)
	var buf bytes.Buffer
	if err := jpeg.Encode(&buf, dst, &jpeg.Options{Quality: jpegQuality}); err != nil {
		return nil, Meta{}, err
	}
	return buf.Bytes(), meta, nil
}

// Describe reads ONLY the image header (image.DecodeConfig) and returns the
// Meta a Thumbnail call would produce — without decoding pixels or
// re-encoding. Cache-hit serves use it to fill the response's dimension
// headers cheaply, while still applying THE decompression-bomb cap.
func Describe(r io.Reader, maxDim int) (Meta, error) {
	cfg, _, err := image.DecodeConfig(r)
	if err != nil {
		return Meta{}, err
	}
	return plan(cfg.Width, cfg.Height, maxDim)
}

// plan validates the source dimensions against the bomb caps and computes the
// thumbnail dimensions.
func plan(srcW, srcH, maxDim int) (Meta, error) {
	if srcW <= 0 || srcH <= 0 {
		return Meta{}, fmt.Errorf("imaging: invalid dimensions %dx%d", srcW, srcH)
	}
	if !withinCaps(srcW, srcH) {
		return Meta{}, ErrTooLarge
	}
	w, h := fit(srcW, srcH, maxDim)
	return Meta{SrcW: srcW, SrcH: srcH, W: w, H: h}, nil
}

// withinCaps is THE decompression-bomb guard (P-18): reject a header whose
// pixel count exceeds 40 MP or whose either side exceeds 12000 px. Kept as one
// named predicate so the RED/GREEN pin is a single, obvious deletion target.
func withinCaps(w, h int) bool {
	return w <= maxDimension && h <= maxDimension && int64(w)*int64(h) <= maxPixels
}

// fit scales (srcW, srcH) to fit within maxDim on the longer side, preserving
// aspect ratio and NEVER upscaling.
func fit(srcW, srcH, maxDim int) (int, int) {
	if srcW <= maxDim && srcH <= maxDim {
		return srcW, srcH
	}
	if srcW >= srcH {
		h := int(math.Round(float64(srcH) * float64(maxDim) / float64(srcW)))
		return maxDim, max(h, 1)
	}
	w := int(math.Round(float64(srcW) * float64(maxDim) / float64(srcH)))
	return max(w, 1), maxDim
}
