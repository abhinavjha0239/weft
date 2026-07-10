package webui

import (
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/abhinavjha0239/weft/internal/brand"
)

func TestHandlerServesBrandedClient(t *testing.T) {
	h, err := Handler()
	if err != nil {
		t.Fatalf("handler: %v", err)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest("GET", "/", nil))
	if rec.Code != 200 {
		t.Fatalf("status %d", rec.Code)
	}
	body := rec.Body.String()
	if !strings.Contains(body, "<title>"+brand.Name+"</title>") {
		t.Fatal("brand name not injected into title")
	}
	if !strings.Contains(body, brand.Tagline) {
		t.Fatal("tagline not injected")
	}
	if cc := rec.Header().Get("Cache-Control"); cc != "no-store" {
		t.Fatalf("cache-control = %q", cc)
	}
	// Non-root paths under the UI handler are 404 (API lives under /api/).
	rec2 := httptest.NewRecorder()
	h.ServeHTTP(rec2, httptest.NewRequest("GET", "/nope", nil))
	if rec2.Code != 404 {
		t.Fatalf("non-root status %d", rec2.Code)
	}
}
