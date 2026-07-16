package imaging_test

import (
	"bytes"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"image"
	"image/color"
	"image/gif"
	"image/jpeg"
	"image/png"
	"testing"

	"github.com/abhinavjha0239/weft/internal/platform/imaging"
)

// transparentWebP600x400 is a 600×400 fully-transparent WebP (alpha 0),
// pre-encoded with cwebp: the standard library has NO WebP encoder, so this
// one decode fixture cannot be produced in-test the way the PNG/JPEG/GIF
// fixtures below are (which use stdlib encoders). Decoding it exercises the
// x/image WebP path; compositing its transparency over white must yield a
// white JPEG, never black — asserted in TestThumbnailCompositesAlphaOverWhite.
const transparentWebP600x400 = "UklGRi4AAABXRUJQVlA4TCEAAAAvV8JjEAdQiirUIwICkqT/+wMj+p/xn//85z//+c//awIA"

// TestThumbnail covers the imaging unit table: each allowlisted format decodes
// and scales, the JPEG output dimensions are exact, at-cap images are left as
// is, and smaller-than-max images are re-encoded WITHOUT upscaling.
func TestThumbnail(t *testing.T) {
	cases := []struct {
		name               string
		data               []byte
		wantSrcW, wantSrcH int
		wantW, wantH       int
	}{
		{"png-downscale-landscape", encodePNG(t, 960, 600), 960, 600, 480, 300},
		{"jpeg-downscale-portrait", encodeJPEG(t, 600, 960), 600, 960, 300, 480},
		{"gif-downscale-firstframe", encodeGIF(t, 600, 240), 600, 240, 480, 192},
		{"webp-downscale-alpha", decodeWebP(t), 600, 400, 480, 320},
		{"png-nonsquare-rounds", encodePNG(t, 1000, 333), 1000, 333, 480, 160},
		{"png-at-cap-unchanged", encodePNG(t, 480, 480), 480, 480, 480, 480},
		{"png-small-no-upscale", encodePNG(t, 40, 24), 40, 24, 40, 24},
		{"jpeg-tiny-no-upscale", encodeJPEG(t, 3, 7), 3, 7, 3, 7},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			out, meta, err := imaging.Thumbnail(bytes.NewReader(c.data), 480)
			if err != nil {
				t.Fatalf("Thumbnail: %v", err)
			}
			want := imaging.Meta{SrcW: c.wantSrcW, SrcH: c.wantSrcH, W: c.wantW, H: c.wantH}
			if meta != want {
				t.Fatalf("meta = %+v, want %+v", meta, want)
			}
			// The bytes must be a JPEG of EXACTLY the planned dimensions.
			cfg, format, err := image.DecodeConfig(bytes.NewReader(out))
			if err != nil {
				t.Fatalf("decode output: %v", err)
			}
			if format != "jpeg" {
				t.Fatalf("output format = %q, want jpeg", format)
			}
			if cfg.Width != c.wantW || cfg.Height != c.wantH {
				t.Fatalf("output dims = %dx%d, want %dx%d", cfg.Width, cfg.Height, c.wantW, c.wantH)
			}
		})
	}
}

// TestThumbnailRefusesDecompressionBomb is THE security pin: a header
// declaring dimensions past the caps is refused (ErrTooLarge) from the header
// alone, before any full decode. Both cap clauses are exercised — the
// pixel-count cap and the per-dimension cap — plus the classic gigapixel
// square. The inputs are PNG HEADERS ONLY (no pixel data): image.DecodeConfig
// reads just the IHDR, so we never attempt to allocate the bomb's buffer.
func TestThumbnailRefusesDecompressionBomb(t *testing.T) {
	bombs := []struct {
		name string
		w, h uint32
	}{
		{"gigapixel-square", 50000, 50000}, // trips both caps
		{"pixel-count-cap", 7000, 6000},    // 42 MP > 40 MP, both sides < 12000
		{"per-dimension-cap", 13000, 10},   // 13000 > 12000, only 130k px
	}
	for _, b := range bombs {
		t.Run(b.name, func(t *testing.T) {
			_, _, err := imaging.Thumbnail(bytes.NewReader(craftPNGHeader(b.w, b.h)), 480)
			if !errors.Is(err, imaging.ErrTooLarge) {
				t.Fatalf("bomb %dx%d: err = %v, want ErrTooLarge", b.w, b.h, err)
			}
		})
	}
}

// TestThumbnailCompositesAlphaOverWhite proves the white-background composite:
// a fully-transparent WebP must render to (near-)white, not black. JPEG has no
// alpha, so a missing composite step would encode transparent pixels as black.
func TestThumbnailCompositesAlphaOverWhite(t *testing.T) {
	out, _, err := imaging.Thumbnail(bytes.NewReader(decodeWebP(t)), 480)
	if err != nil {
		t.Fatalf("Thumbnail: %v", err)
	}
	img, err := jpeg.Decode(bytes.NewReader(out))
	if err != nil {
		t.Fatalf("decode output: %v", err)
	}
	b := img.Bounds()
	r, g, bl, _ := img.At(b.Dx()/2, b.Dy()/2).RGBA() // 16-bit channels
	if r>>8 < 240 || g>>8 < 240 || bl>>8 < 240 {
		t.Fatalf("center pixel = (%d,%d,%d)/65535, want near-white (transparency over white)", r, g, bl)
	}
}

// TestThumbnailRejectsNonImage: junk bytes are a decode error, not a panic and
// not a zero-dimension "success".
func TestThumbnailRejectsNonImage(t *testing.T) {
	if _, _, err := imaging.Thumbnail(bytes.NewReader([]byte("this is not an image")), 480); err == nil {
		t.Fatal("expected an error decoding non-image bytes")
	}
}

func encodePNG(t *testing.T, w, h int) []byte {
	t.Helper()
	var buf bytes.Buffer
	if err := png.Encode(&buf, gradient(w, h)); err != nil {
		t.Fatalf("encode png: %v", err)
	}
	return buf.Bytes()
}

func encodeJPEG(t *testing.T, w, h int) []byte {
	t.Helper()
	var buf bytes.Buffer
	if err := jpeg.Encode(&buf, gradient(w, h), &jpeg.Options{Quality: 90}); err != nil {
		t.Fatalf("encode jpeg: %v", err)
	}
	return buf.Bytes()
}

func encodeGIF(t *testing.T, w, h int) []byte {
	t.Helper()
	var buf bytes.Buffer
	if err := gif.Encode(&buf, gradient(w, h), nil); err != nil {
		t.Fatalf("encode gif: %v", err)
	}
	return buf.Bytes()
}

func decodeWebP(t *testing.T) []byte {
	t.Helper()
	data, err := base64.StdEncoding.DecodeString(transparentWebP600x400)
	if err != nil {
		t.Fatalf("decode webp fixture: %v", err)
	}
	return data
}

// gradient builds an opaque RGBA test image whose pixels vary in both axes, so
// scaling actually has content to resample.
func gradient(w, h int) *image.RGBA {
	img := image.NewRGBA(image.Rect(0, 0, w, h))
	for y := 0; y < h; y++ {
		for x := 0; x < w; x++ {
			img.SetRGBA(x, y, color.RGBA{
				R: uint8((x * 255) / max(w-1, 1)),
				G: uint8((y * 255) / max(h-1, 1)),
				B: 128,
				A: 255,
			})
		}
	}
	return img
}

// craftPNGHeader builds a PNG signature + IHDR chunk (valid CRC) declaring the
// given dimensions, with NO IDAT: image.DecodeConfig reads the IHDR and
// returns, which is all THE bomb cap needs.
func craftPNGHeader(w, h uint32) []byte {
	var buf bytes.Buffer
	buf.Write([]byte{0x89, 'P', 'N', 'G', 0x0d, 0x0a, 0x1a, 0x0a})
	ihdr := make([]byte, 13)
	binary.BigEndian.PutUint32(ihdr[0:4], w)
	binary.BigEndian.PutUint32(ihdr[4:8], h)
	ihdr[8] = 8 // bit depth
	ihdr[9] = 2 // color type 2 (truecolor, non-paletted): DecodeConfig returns after IHDR
	writePNGChunk(&buf, "IHDR", ihdr)
	return buf.Bytes()
}

func writePNGChunk(buf *bytes.Buffer, typ string, data []byte) {
	var length [4]byte
	binary.BigEndian.PutUint32(length[:], uint32(len(data)))
	buf.Write(length[:])
	c := crc32.NewIEEE()
	_, _ = c.Write([]byte(typ))
	_, _ = c.Write(data)
	buf.WriteString(typ)
	buf.Write(data)
	var crcb [4]byte
	binary.BigEndian.PutUint32(crcb[:], c.Sum32())
	buf.Write(crcb[:])
}
