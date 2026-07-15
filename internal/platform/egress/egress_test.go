package egress

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"net/url"
	"strings"
	"testing"
)

// fakeLookup maps hostnames to fixed answers — the SSRF matrix never touches
// real DNS or the network (vetting happens before any dial).
func fakeLookup(m map[string][]string) func(context.Context, string) ([]netip.Addr, error) {
	return func(_ context.Context, host string) ([]netip.Addr, error) {
		ips, ok := m[host]
		if !ok {
			return nil, fmt.Errorf("no such host %q", host)
		}
		out := make([]netip.Addr, len(ips))
		for i, s := range ips {
			out[i] = netip.MustParseAddr(s)
		}
		return out, nil
	}
}

// TestVetAddr is the load-bearing SSRF matrix: every internal address class
// must be rejected, including the cloud-metadata range and IPv4-mapped v6
// disguises. Neuter the classification in vetAddr and this suite goes red.
func TestVetAddr(t *testing.T) {
	rejected := []string{
		"127.0.0.1", "127.8.8.8", // loopback
		"10.0.0.1", "172.16.0.1", "172.31.255.255", "192.168.1.1", // RFC1918
		"169.254.169.254", "169.254.0.1", // v4 link-local (cloud metadata)
		"::1",                     // v6 loopback
		"fe80::1",                 // v6 link-local
		"fc00::1", "fd12:3456::1", // unique-local
		"0.0.0.0", "::", // unspecified
		"255.255.255.255",      // broadcast
		"224.0.0.1", "ff02::1", // multicast
		"::ffff:127.0.0.1",       // v4-mapped loopback
		"::ffff:10.0.0.1",        // v4-mapped private
		"::ffff:169.254.169.254", // v4-mapped metadata
	}
	for _, s := range rejected {
		if err := (Options{}).vetAddr(netip.MustParseAddr(s)); !errors.Is(err, ErrDisallowed) {
			t.Errorf("vetAddr(%s) = %v, want ErrDisallowed", s, err)
		}
	}
	allowed := []string{"93.184.216.34", "8.8.8.8", "2606:4700::1111", "1.1.1.1"}
	for _, s := range allowed {
		if err := (Options{}).vetAddr(netip.MustParseAddr(s)); err != nil {
			t.Errorf("vetAddr(%s) = %v, want nil (public unicast)", s, err)
		}
	}
	// The test escape hatch flips ONLY loopback, nothing else.
	relaxed := Options{AllowLoopbackForTests: true}
	if err := relaxed.vetAddr(netip.MustParseAddr("127.0.0.1")); err != nil {
		t.Errorf("test option should allow loopback, got %v", err)
	}
	if err := relaxed.vetAddr(netip.MustParseAddr("169.254.169.254")); !errors.Is(err, ErrDisallowed) {
		t.Errorf("test option must NOT allow link-local, got %v", err)
	}
	if err := relaxed.vetAddr(netip.MustParseAddr("10.0.0.1")); !errors.Is(err, ErrDisallowed) {
		t.Errorf("test option must NOT allow private, got %v", err)
	}
}

func TestVetURL(t *testing.T) {
	bad := []string{
		"ftp://example.com/",           // scheme
		"file:///etc/passwd",           // scheme
		"gopher://example.com/",        // scheme
		"http://user:pass@example.com", // userinfo
		"http://example.com:8080/",     // port
		"https://example.com:6379/",    // port
		"http:///path",                 // empty host
	}
	for _, s := range bad {
		u, err := url.Parse(s)
		if err != nil {
			t.Fatalf("parse %q: %v", s, err)
		}
		if err := (Options{}).vetURL(u); !errors.Is(err, ErrDisallowed) {
			t.Errorf("vetURL(%s) = %v, want ErrDisallowed", s, err)
		}
	}
	good := []string{"http://example.com/", "https://example.com/a?b=c", "https://example.com:443/", "http://example.com:80/"}
	for _, s := range good {
		u, _ := url.Parse(s)
		if err := (Options{}).vetURL(u); err != nil {
			t.Errorf("vetURL(%s) = %v, want nil", s, err)
		}
	}
}

// TestGetBlocksPrivateResolution: a hostname resolving to an internal
// address is rejected BEFORE any dial (no listener exists to prove it).
func TestGetBlocksPrivateResolution(t *testing.T) {
	c := New(Options{
		UserAgent: "test",
		LookupIP: fakeLookup(map[string][]string{
			"internal.test": {"10.0.0.7"},
			"metadata.test": {"169.254.169.254"},
			"loop.test":     {"127.0.0.1"},
		}),
	})
	for _, u := range []string{"http://internal.test/", "http://metadata.test/latest/meta-data/", "http://loop.test/"} {
		if _, err := c.Get(context.Background(), u); !errors.Is(err, ErrDisallowed) {
			t.Errorf("Get(%s) = %v, want ErrDisallowed", u, err)
		}
	}
}

// TestGetMixedRecordsRejected: one public record does not launder an answer
// that also carries an internal one — ALL must pass, so the reject happens
// pre-dial (the error is the guard's, not a connection failure).
func TestGetMixedRecordsRejected(t *testing.T) {
	c := New(Options{
		UserAgent: "test",
		LookupIP: fakeLookup(map[string][]string{
			"mixed.test": {"93.184.216.34", "10.0.0.7"},
		}),
	})
	if _, err := c.Get(context.Background(), "http://mixed.test/"); !errors.Is(err, ErrDisallowed) {
		t.Fatalf("Get(mixed) = %v, want ErrDisallowed", err)
	}
}

// TestLiteralIPHostVetted: a literal-IP URL takes the same classification.
func TestLiteralIPHostVetted(t *testing.T) {
	c := New(Options{UserAgent: "test"})
	for _, u := range []string{"http://169.254.169.254/", "http://127.0.0.1/", "http://[::1]/", "http://10.0.0.1/"} {
		if _, err := c.Get(context.Background(), u); !errors.Is(err, ErrDisallowed) {
			t.Errorf("Get(%s) = %v, want ErrDisallowed", u, err)
		}
	}
}

// TestGetReachesAllowedTarget: with the test escape hatch the client reaches
// an httptest listener, sends the configured User-Agent, and ignores env
// proxies (a poisoned HTTP_PROXY would otherwise capture every fetch).
func TestGetReachesAllowedTarget(t *testing.T) {
	var gotUA string
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotUA = r.UserAgent()
		fmt.Fprint(w, "hello")
	}))
	defer ts.Close()
	t.Setenv("HTTP_PROXY", "http://127.0.0.1:1")
	t.Setenv("http_proxy", "http://127.0.0.1:1")

	c := New(Options{UserAgent: "weftbot-test/1.0", AllowLoopbackForTests: true})
	resp, err := c.Get(context.Background(), ts.URL)
	if err != nil {
		t.Fatalf("Get(httptest) = %v (env proxy must be ignored)", err)
	}
	resp.Body.Close()
	if resp.StatusCode != 200 || gotUA != "weftbot-test/1.0" {
		t.Fatalf("status %d, UA %q", resp.StatusCode, gotUA)
	}
}

// TestRedirectToPrivateRejected: a public-looking page 302ing into an
// internal host is stopped at the hop — the redirect re-enters the pinned
// dialer. Neuter the per-hop vetting and this goes red.
func TestRedirectToPrivateRejected(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "http://internal.test/", http.StatusFound)
	}))
	defer ts.Close()
	c := New(Options{
		UserAgent:             "test",
		AllowLoopbackForTests: true,
		LookupIP: fakeLookup(map[string][]string{
			"internal.test": {"10.0.0.7"},
		}),
	})
	if _, err := c.Get(context.Background(), ts.URL); !errors.Is(err, ErrDisallowed) {
		t.Fatalf("redirect-to-private = %v, want ErrDisallowed", err)
	}
}

// TestRedirectLoopCapped: more than 5 hops is refused.
func TestRedirectLoopCapped(t *testing.T) {
	var ts *httptest.Server
	ts = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, ts.URL+"/again", http.StatusFound)
	}))
	defer ts.Close()
	c := New(Options{UserAgent: "test", AllowLoopbackForTests: true})
	_, err := c.Get(context.Background(), ts.URL)
	if err == nil || !strings.Contains(err.Error(), "too many redirects") {
		t.Fatalf("redirect loop = %v, want too-many-redirects", err)
	}
}
