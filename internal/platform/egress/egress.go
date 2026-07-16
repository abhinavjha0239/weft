// Package egress is the SSRF-guarded outbound HTTP seam: the ONLY way core
// code fetches an attacker-influenced URL (link previews today, outbound
// webhook steps later). The guard's spine is resolve-once-dial-pinned: the
// dialer resolves the host itself, vets EVERY resolved address, and dials
// only a vetted literal IP — a DNS answer that changes between check and
// connect (the rebinding TOCTOU) changes nothing, because the checked
// address IS the dialed address. Every redirect hop re-enters this dialer
// and re-runs the URL-level checks, so a public page cannot bounce the
// fetcher into an internal network.
package egress

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"time"
)

// ErrDisallowed marks every guard rejection (scheme, port, userinfo, or a
// destination address class). Callers distinguish "the guard said no" from
// ordinary network failure with errors.Is.
var ErrDisallowed = errors.New("egress: destination not allowed")

type Options struct {
	// LookupIP overrides address resolution (tests inject fakes; nil uses
	// net.DefaultResolver). Literal-IP hosts skip it but are vetted the same.
	LookupIP func(ctx context.Context, host string) ([]netip.Addr, error)
	// AllowLoopbackForTests permits loopback destinations AND non-standard
	// ports so tests can reach httptest listeners (which bind random high
	// ports on 127.0.0.1). Production wiring never sets it — grep for this
	// name to audit.
	AllowLoopbackForTests bool
	// UserAgent identifies the fetcher on every request.
	UserAgent string
}

type Client struct {
	http *http.Client
	opts Options
}

// New builds the guarded client. Timeouts are fixed by design: 5s dial, 10s
// total; the caller bounds BODY size (the guard bounds destinations and
// time, not bytes).
func New(opts Options) *Client {
	c := &Client{opts: opts}
	c.http = &http.Client{
		Timeout: 10 * time.Second,
		Transport: &http.Transport{
			// Env proxies (HTTP_PROXY et al.) would carry the request to a
			// proxy of the ENVIRONMENT's choosing before our dialer ran,
			// bypassing the IP pinning entirely — so no proxy, ever.
			Proxy:                  nil,
			DialContext:            c.dialContext,
			TLSHandshakeTimeout:    5 * time.Second,
			ResponseHeaderTimeout:  5 * time.Second,
			MaxIdleConns:           4,
			IdleConnTimeout:        30 * time.Second,
			MaxResponseHeaderBytes: 64 << 10,
		},
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			// A POST delivery NEVER follows a redirect: re-POSTing the body to
			// the 30x target is a cross-host header/credential-leak shape. Hand
			// the 3xx back to the caller (recorded as a non-2xx failure) rather
			// than chasing it. Get (unfurl) keeps its redirect budget untouched.
			if len(via) > 0 && via[0].Method == http.MethodPost {
				return http.ErrUseLastResponse
			}
			if len(via) >= 5 {
				return fmt.Errorf("%w: too many redirects", ErrDisallowed)
			}
			// The hop's DIAL is re-vetted by dialContext regardless; this
			// re-runs the URL-level checks (scheme/port/userinfo).
			return c.opts.vetURL(req.URL)
		},
	}
	return c
}

// Get fetches rawURL through the guard with the configured User-Agent. It
// never sends credentials (no cookie jar, no Authorization). The caller owns
// resp.Body and MUST cap reads with an io.LimitReader.
func (c *Client) Get(ctx context.Context, rawURL string) (*http.Response, error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrDisallowed, err)
	}
	if err := c.opts.vetURL(u); err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", c.opts.UserAgent)
	return c.http.Do(req)
}

// Post sends body to rawURL through the same guard as Get: the URL shape is
// vetted before any network work, the pinned dialer vets every resolved
// address, and no credentials of OUR own are attached (no cookie jar, no
// Authorization we add — the caller's validated headers may include one, which
// is the outbound-webhook auth use-case). Unlike Get it NEVER follows a
// redirect (New's CheckRedirect hands a 30x straight back as a non-2xx
// response). Content-Type is fixed application/json; caller headers are set
// first so our User-Agent and Content-Type always win. The caller owns
// resp.Body and MUST cap reads with an io.LimitReader.
func (c *Client) Post(ctx context.Context, rawURL string, headers map[string]string, body []byte) (*http.Response, error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrDisallowed, err)
	}
	if err := c.opts.vetURL(u); err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u.String(), bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	req.Header.Set("User-Agent", c.opts.UserAgent)
	req.Header.Set("Content-Type", "application/json")
	return c.http.Do(req)
}

// VetURLShape runs only the static, network-free URL checks (scheme http(s),
// no userinfo, standard ports) that vetURL applies with production options —
// for validating a configured destination at definition time, so an operator
// gets early feedback on an obviously-bad shape. The address-class checks and
// the pinned dial still run at send; this is not a substitute for them.
func VetURLShape(rawURL string) error {
	u, err := url.Parse(rawURL)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrDisallowed, err)
	}
	return Options{}.vetURL(u)
}

// vetURL rejects URL shapes before any network work: only plain http(s), no
// userinfo (http://user:pass@host smuggling), and only the standard ports —
// an unfurler has no business talking to :6379 or :5432.
func (o Options) vetURL(u *url.URL) error {
	switch u.Scheme {
	case "http", "https":
	default:
		return fmt.Errorf("%w: scheme %q", ErrDisallowed, u.Scheme)
	}
	if u.User != nil {
		return fmt.Errorf("%w: userinfo in URL", ErrDisallowed)
	}
	if u.Hostname() == "" {
		return fmt.Errorf("%w: empty host", ErrDisallowed)
	}
	switch u.Port() {
	case "", "80", "443":
	default:
		if !o.AllowLoopbackForTests {
			return fmt.Errorf("%w: port %q", ErrDisallowed, u.Port())
		}
	}
	return nil
}

// vetAddr classifies one resolved destination. Everything that is not a
// plain public unicast address is rejected: loopback, RFC1918 private and
// RFC4193 unique-local (both via IsPrivate), link-local v4 (169.254/16 —
// the cloud metadata range) and v6 (fe80::/10), unspecified, multicast in
// all scopes, and the v4 broadcast address. IPv4-mapped v6 forms are
// unmapped FIRST so ::ffff:127.0.0.1 classifies as the loopback it is.
func (o Options) vetAddr(a netip.Addr) error {
	a = a.Unmap()
	switch {
	case !a.IsValid(), a.IsUnspecified():
	case a.IsLoopback():
		if o.AllowLoopbackForTests {
			return nil
		}
	case a.IsPrivate():
	case a.IsLinkLocalUnicast(), a.IsLinkLocalMulticast(),
		a.IsInterfaceLocalMulticast(), a.IsMulticast():
	case a.Is4() && a == netip.AddrFrom4([4]byte{255, 255, 255, 255}):
	default:
		return nil
	}
	return fmt.Errorf("%w: address %s", ErrDisallowed, a)
}

// dialContext is the pinned dialer: resolve, vet every answer, dial the
// first vetted literal. ALL resolved addresses must pass — a mixed answer
// (one public record, one internal) is an attack shape, not a CDN, because
// net.Dialer would happily fall back to the internal one on "failure".
func (c *Client) dialContext(ctx context.Context, network, addr string) (net.Conn, error) {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrDisallowed, err)
	}
	if port != "80" && port != "443" && !c.opts.AllowLoopbackForTests {
		return nil, fmt.Errorf("%w: port %q", ErrDisallowed, port)
	}
	var addrs []netip.Addr
	if ip, perr := netip.ParseAddr(host); perr == nil {
		addrs = []netip.Addr{ip}
	} else {
		lookup := c.opts.LookupIP
		if lookup == nil {
			lookup = func(ctx context.Context, host string) ([]netip.Addr, error) {
				return net.DefaultResolver.LookupNetIP(ctx, "ip", host)
			}
		}
		if addrs, err = lookup(ctx, host); err != nil {
			return nil, err
		}
	}
	if len(addrs) == 0 {
		return nil, fmt.Errorf("%w: no addresses for %q", ErrDisallowed, host)
	}
	for _, a := range addrs {
		if err := c.opts.vetAddr(a); err != nil {
			return nil, err
		}
	}
	// TLS ServerName and the Host header come from the request URL, not
	// this literal, so certificate validation still checks the real name.
	d := net.Dialer{Timeout: 5 * time.Second}
	return d.DialContext(ctx, network, net.JoinHostPort(addrs[0].Unmap().String(), port))
}
