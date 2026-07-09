// Package apperr is the error taxonomy (ARCHITECTURE.md §3): domain code
// returns these; exactly one place in transport maps them to HTTP. SQL and
// driver errors never cross a service boundary unwrapped.
package apperr

import (
	"errors"
	"fmt"
	"net/http"
)

type Kind int

const (
	KindInternal Kind = iota
	KindNotFound
	KindForbidden
	KindUnauthorized
	KindInvalid
	KindConflict
	KindRateLimited
)

type Error struct {
	Kind Kind
	Msg  string // safe for clients
	err  error  // internal cause, logged never sent
}

func (e *Error) Error() string {
	if e.err != nil {
		return fmt.Sprintf("%s: %v", e.Msg, e.err)
	}
	return e.Msg
}

func (e *Error) Unwrap() error { return e.err }

func NotFound(msg string) error     { return &Error{Kind: KindNotFound, Msg: msg} }
func Forbidden(msg string) error    { return &Error{Kind: KindForbidden, Msg: msg} }
func Unauthorized(msg string) error { return &Error{Kind: KindUnauthorized, Msg: msg} }
func Invalid(msg string) error      { return &Error{Kind: KindInvalid, Msg: msg} }
func Conflict(msg string) error     { return &Error{Kind: KindConflict, Msg: msg} }
func RateLimited(msg string) error  { return &Error{Kind: KindRateLimited, Msg: msg} }
func Internal(op string, err error) error {
	return &Error{Kind: KindInternal, Msg: op, err: err}
}

// KindOf extracts the taxonomy kind; unknown errors are internal.
func KindOf(err error) Kind {
	var e *Error
	if errors.As(err, &e) {
		return e.Kind
	}
	return KindInternal
}

// ClientMessage is what the API may reveal. Internal causes are hidden.
func ClientMessage(err error) string {
	var e *Error
	if errors.As(err, &e) && e.Kind != KindInternal {
		return e.Msg
	}
	return "internal error"
}

func HTTPStatus(k Kind) int {
	switch k {
	case KindNotFound:
		return http.StatusNotFound
	case KindForbidden:
		return http.StatusForbidden
	case KindUnauthorized:
		return http.StatusUnauthorized
	case KindInvalid:
		return http.StatusBadRequest
	case KindConflict:
		return http.StatusConflict
	case KindRateLimited:
		return http.StatusTooManyRequests
	default:
		return http.StatusInternalServerError
	}
}
