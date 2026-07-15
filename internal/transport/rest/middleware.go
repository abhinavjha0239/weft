package rest

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/abhinavjha0239/weft/internal/platform/ratelimit"
)

type ctxKey int

const requestIDKey ctxKey = 1

// RequestID returns the id attached by the middleware chain.
func RequestID(ctx context.Context) string {
	id, _ := ctx.Value(requestIDKey).(string)
	return id
}

// chain applies middleware outermost-first.
func chain(h http.Handler, mws ...func(http.Handler) http.Handler) http.Handler {
	for i := len(mws) - 1; i >= 0; i-- {
		h = mws[i](h)
	}
	return h
}

func withRequestID(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b := make([]byte, 8)
		_, _ = rand.Read(b)
		id := hex.EncodeToString(b)
		w.Header().Set("X-Request-Id", id)
		next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), requestIDKey, id)))
	})
}

func withRecover(log *slog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			defer func() {
				if rec := recover(); rec != nil {
					log.Error("panic", "req", RequestID(r.Context()),
						"path", r.URL.Path, "panic", rec)
					writeError(w, http.StatusInternalServerError, "internal error")
				}
			}()
			next.ServeHTTP(w, r)
		})
	}
}

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (s *statusRecorder) WriteHeader(code int) {
	s.status = code
	s.ResponseWriter.WriteHeader(code)
}

// Unwrap lets http.ResponseController reach the underlying writer, which is
// what keeps WebSocket upgrades working through this middleware.
func (s *statusRecorder) Unwrap() http.ResponseWriter { return s.ResponseWriter }

func withLog(log *slog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()
			rec := &statusRecorder{ResponseWriter: w, status: 200}
			next.ServeHTTP(rec, r)
			log.Info("http",
				"req", RequestID(r.Context()), "method", r.Method,
				"path", r.URL.Path, "status", rec.status,
				"ms", time.Since(start).Milliseconds())
		})
	}
}

func clientIP(r *http.Request) string {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}

// requestUserAgent returns the client's User-Agent as session metadata:
// flattened to a single line, forced to valid UTF-8 (the column is TEXT and
// the header is client-controlled bytes), and capped at 256 bytes without
// splitting a rune at the cut. Display only — never trusted for anything.
func requestUserAgent(r *http.Request) string {
	ua := strings.ReplaceAll(r.UserAgent(), "\r", " ")
	ua = strings.ReplaceAll(ua, "\n", " ")
	ua = strings.ToValidUTF8(ua, "")
	if len(ua) > 256 {
		ua = ua[:256]
		for len(ua) > 0 && !utf8.ValidString(ua) {
			ua = ua[:len(ua)-1]
		}
	}
	return ua
}

// withIPLimit protects a handler with a per-IP bucket (pre-auth surfaces).
func withIPLimit(l *ratelimit.Limiter) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if !l.Allow(clientIP(r)) {
				writeError(w, http.StatusTooManyRequests, "rate limit exceeded")
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}
