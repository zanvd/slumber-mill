package webhook

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"net/url"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"testing/synctest"
	"time"

	"github.com/zanvd/slumber-mill/src/egress"
)

const (
	testEventID = "0190a2b4-7c3e-7a52-9d41-5b6f8e0c1a23"
	testHost    = "hooks.slumber-mill.test"
	testPath    = "/hooks/slumber?tenant=demo"
)

var testSignature = "t=" + strconv.Itoa(unixSigned) + ",v1=" + knownAnswerDigest

func parseAddrs(ss ...string) []netip.Addr {
	out := make([]netip.Addr, len(ss))
	for i, s := range ss {
		out[i] = netip.MustParseAddr(s)
	}
	return out
}

type request struct {
	method        string
	requestURI    string
	header        http.Header
	contentLength int64
	body          []byte
}

// receiver is a loopback server that records the connections and requests that reach it.
type receiver struct {
	srv   *httptest.Server
	conns atomic.Int64

	mu       sync.Mutex
	requests []request
}

// newReceiver listens on 127.0.0.1 for "tcp4" and on [::1] for "tcp6",
// skipping the test when the container has no IPv6 loopback.
func newReceiver(t *testing.T, network string, respond http.HandlerFunc) *receiver {
	return startReceiver(t, network, respond, (*httptest.Server).Start)
}

func startReceiver(t *testing.T, network string, respond http.HandlerFunc, start func(*httptest.Server)) *receiver {
	t.Helper()
	address := "127.0.0.1:0"
	if network == "tcp6" {
		address = "[::1]:0"
	}
	l, err := net.Listen(network, address)
	if err != nil {
		if network == "tcp6" {
			t.Skipf("no IPv6 loopback: %v", err)
		}
		t.Fatal(err)
	}
	r := &receiver{}
	r.srv = httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		body, err := io.ReadAll(req.Body)
		if err != nil {
			t.Errorf("receiver: error reading the body: %v", err)
		}
		r.mu.Lock()
		r.requests = append(r.requests, request{req.Method, req.RequestURI, req.Header.Clone(), req.ContentLength, body})
		r.mu.Unlock()
		respond(w, req)
	}))
	_ = r.srv.Listener.Close()
	r.srv.Listener = l
	r.srv.Config.ConnState = func(_ net.Conn, s http.ConnState) {
		if s == http.StateNew {
			r.conns.Add(1)
		}
	}
	r.srv.Config.ErrorLog = log.New(io.Discard, "", 0)
	start(r.srv)
	t.Cleanup(r.srv.Close)
	return r
}

// urlFor is the receiver's URL with host in place of its address.
func (r *receiver) urlFor(host string) string {
	u, _ := url.Parse(r.srv.URL)
	return u.Scheme + "://" + net.JoinHostPort(host, u.Port()) + testPath
}

// stop closes the server, which waits until every connection it accepted has been counted,
// and returns what reached it.
func (r *receiver) stop() (conns int64, requests []request) {
	r.srv.Close()
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.conns.Load(), r.requests
}

func respondStatus(code int) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(code) }
}

// stall holds a request until the client goes away, or for a minute.
// The server watches for the client going away only once the request's body has been read.
func stall(_ http.ResponseWriter, r *http.Request) {
	_, _ = io.Copy(io.Discard, r.Body)
	select {
	case <-r.Context().Done():
	case <-time.After(time.Minute):
	}
}

func checkDelivered(t *testing.T, got request) {
	t.Helper()
	if got.method != http.MethodPost {
		t.Errorf("method = %s, want POST", got.method)
	}
	if got.requestURI != testPath {
		t.Errorf("request URI = %q, want %q", got.requestURI, testPath)
	}
	for name, want := range map[string]string{
		"Content-Type":  "application/json",
		"User-Agent":    "slumber-mill-webhooks/1",
		EventIDHeader:   testEventID,
		SignatureHeader: testSignature,
	} {
		if v := got.header.Values(name); len(v) != 1 || v[0] != want {
			t.Errorf("header %s = %q, want [%q]", name, v, want)
		}
	}
	if got.contentLength != int64(len(testBody)) {
		t.Errorf("Content-Length = %d, want %d", got.contentLength, len(testBody))
	}
	if !bytes.Equal(got.body, testBody) {
		t.Errorf("body = %q, want %q", got.body, testBody)
	}
	if err := Verify(testSecret, got.header.Get(SignatureHeader), got.body, signedAt, testTolerance); err != nil {
		t.Errorf("Verify() on what arrived = %v, want nil", err)
	}
}

// checkErr checks that err is want and no egress sentinel but want; with want nil, any other error will do.
// An egress sentinel must arrive through the http.Client's *url.Error and the dialer's *net.OpError,
// and no error may carry the signature or the body.
func checkErr(t *testing.T, err, want error) {
	t.Helper()
	if err == nil {
		t.Fatalf("Post() error = nil, want %v", want)
	}
	if want != nil && !errors.Is(err, want) {
		t.Errorf("Post() error = %v, want %v", err, want)
	}
	for _, sentinel := range []error{egress.ErrBlockedAddress, egress.ErrUnresolvable} {
		if sentinel != want {
			if errors.Is(err, sentinel) {
				t.Errorf("Post() error = %v, which is %v", err, sentinel)
			}
			continue
		}
		urlErr, ok := errors.AsType[*url.Error](err)
		if !ok {
			t.Fatalf("Post() error = %v, want a *url.Error", err)
		}
		opErr, ok := errors.AsType[*net.OpError](urlErr.Err)
		if !ok || !errors.Is(opErr.Err, want) {
			t.Errorf("url.Error.Err = %#v, want a *net.OpError wrapping %v", urlErr.Err, want)
		}
	}
	if msg := err.Error(); strings.Contains(msg, knownAnswerDigest) || strings.Contains(msg, string(testBody)) {
		t.Errorf("error %q carries the signature or the body", msg)
	}
}

const (
	rcodeSuccess       = 0
	rcodeServerFailure = 2
	rcodeNameError     = 3
)

// fakeDNS stands in for the DNS server behind a resolver, so lookups need no network.
// Every query gets rcode, with those of addrs that match the query's type.
type fakeDNS struct {
	rcode   byte
	addrs   []netip.Addr
	dialErr error // returned in place of a connection

	mu      sync.Mutex
	servers []string // what the resolver dialled
}

// use makes c's dialer resolve through d, after /etc/hosts.
func (d *fakeDNS) use(c *Client) {
	c.dialer.Resolver = &net.Resolver{PreferGo: true, Dial: d.dial}
}

func (d *fakeDNS) dial(_ context.Context, _, address string) (net.Conn, error) {
	d.mu.Lock()
	d.servers = append(d.servers, address)
	d.mu.Unlock()
	if d.dialErr != nil {
		return nil, d.dialErr
	}
	client, server := net.Pipe()
	go d.serve(server)
	return client, nil
}

// serve speaks DNS over a stream, where each message follows its length in two bytes.
func (d *fakeDNS) serve(conn net.Conn) {
	defer conn.Close()
	for {
		var n [2]byte
		if _, err := io.ReadFull(conn, n[:]); err != nil {
			return
		}
		query := make([]byte, binary.BigEndian.Uint16(n[:]))
		if _, err := io.ReadFull(conn, query); err != nil {
			return
		}
		resp, ok := d.answer(query)
		if !ok {
			return
		}
		if _, err := conn.Write(append(binary.BigEndian.AppendUint16(nil, uint16(len(resp))), resp...)); err != nil {
			return
		}
	}
}

func (d *fakeDNS) answer(query []byte) ([]byte, bool) {
	// A 12-byte header, then one question: the name's labels up to a zero byte, its type and its class.
	end := 12
	for end < len(query) && query[end] != 0 {
		end += 1 + int(query[end])
	}
	end += 5
	if end > len(query) {
		return nil, false
	}
	qtype := binary.BigEndian.Uint16(query[end-4:])
	var rdatas [][]byte
	for _, a := range d.addrs {
		if d.rcode == rcodeSuccess && (qtype == 1 && a.Is4() || qtype == 28 && a.Is6()) {
			rdatas = append(rdatas, a.AsSlice())
		}
	}
	resp := append([]byte(nil), query[:2]...)                       // ID
	resp = append(resp, 0x80|query[2]&0x01, 0x80|d.rcode)           // QR and RD; RA and RCODE
	resp = binary.BigEndian.AppendUint16(resp, 1)                   // QDCOUNT
	resp = binary.BigEndian.AppendUint16(resp, uint16(len(rdatas))) // ANCOUNT
	resp = append(resp, 0, 0, 0, 0)                                 // NSCOUNT, ARCOUNT
	resp = append(resp, query[12:end]...)
	for _, rdata := range rdatas {
		resp = append(resp, 0xc0, 12) // the question's name
		resp = binary.BigEndian.AppendUint16(resp, qtype)
		resp = binary.BigEndian.AppendUint16(resp, 1)  // IN
		resp = binary.BigEndian.AppendUint32(resp, 60) // TTL
		resp = binary.BigEndian.AppendUint16(resp, uint16(len(rdata)))
		resp = append(resp, rdata...)
	}
	return resp, true
}

func TestNewClientConfiguration(t *testing.T) {
	// The dial timeout acts inside net.Dialer on a real socket. A synctest bubble's clock doesn't advance
	// while a goroutine waits on one, and a connect that hangs needs an unroutable address, so the external
	// network; the value is checked here instead. The other timeouts also run on a fake clock in TestPostTimeouts.
	for _, allowPrivate := range []bool{false, true} {
		t.Run("allowPrivate="+strconv.FormatBool(allowPrivate), func(t *testing.T) {
			c := NewClient(allowPrivate)
			if c.dialer.Timeout != 5*time.Second {
				t.Errorf("dial timeout = %v, want 5s", c.dialer.Timeout)
			}
			if c.dialer.Control == nil || c.dialer.ControlContext != nil {
				t.Error("dialer doesn't check addresses with Control")
			}
			if c.dialer.Resolver != nil {
				t.Error("dialer has its own resolver, want the default")
			}
			tr := c.transport
			if tr.Proxy != nil {
				t.Error("transport has a Proxy func, want nil")
			}
			if tr.DialContext == nil || tr.Dial != nil || tr.DialTLSContext != nil || tr.DialTLS != nil {
				t.Error("transport doesn't dial every connection, TLS included, through DialContext alone")
			}
			if tr.TLSHandshakeTimeout != 5*time.Second {
				t.Errorf("TLS handshake timeout = %v, want 5s", tr.TLSHandshakeTimeout)
			}
			if tr.ResponseHeaderTimeout != 10*time.Second {
				t.Errorf("response header timeout = %v, want 10s", tr.ResponseHeaderTimeout)
			}
			if tr.IdleConnTimeout != 90*time.Second {
				t.Errorf("idle connection timeout = %v, want 90s", tr.IdleConnTimeout)
			}
			if c.http.Transport != tr {
				t.Error("http.Client doesn't use the checked transport")
			}
			if c.http.Timeout != 15*time.Second {
				t.Errorf("request timeout = %v, want 15s", c.http.Timeout)
			}
			if c.http.CheckRedirect == nil || c.http.CheckRedirect(nil, nil) != http.ErrUseLastResponse {
				t.Error("CheckRedirect doesn't return http.ErrUseLastResponse")
			}
		})
	}
}

func TestControl(t *testing.T) {
	tests := []struct {
		address     string
		public      bool
		unparseable bool
	}{
		{"8.8.8.8:443", true, false},
		{"[2001:4860:4860::8888]:443", true, false},
		{"[::ffff:8.8.8.8]:443", true, false},
		{"10.0.0.1:443", false, false},
		{"127.0.0.1:80", false, false},
		{"169.254.169.254:80", false, false},
		{"100.64.0.1:443", false, false},
		{"[::1]:443", false, false},
		{"[::ffff:10.0.0.1]:443", false, false},
		{"[64:ff9b::a00:1]:443", false, false},
		{"[fd00::1]:443", false, false},
		{"[fe80::1%eth0]:443", false, false},
		{"hooks.example.com:443", false, true},
		{"8.8.8.8", false, true},
	}
	for _, allowPrivate := range []bool{false, true} {
		c := NewClient(allowPrivate)
		for _, tc := range tests {
			t.Run("allowPrivate="+strconv.FormatBool(allowPrivate)+"/"+tc.address, func(t *testing.T) {
				var want error
				if tc.unparseable || !tc.public && !allowPrivate {
					want = egress.ErrBlockedAddress
				}
				if err := c.control("tcp", tc.address, nil); !errors.Is(err, want) {
					t.Errorf("control(%q) = %v, want %v", tc.address, err, want)
				}
			})
		}
	}
}

func TestPostAddressCheck(t *testing.T) {
	tests := []struct {
		name         string
		network      string // where the receiver listens
		host         string // the URL's host
		dns          []netip.Addr
		allowPrivate bool
		want         error // nil: the request arrives
	}{
		{"IPv4 loopback", "tcp4", "127.0.0.1", nil, false, egress.ErrBlockedAddress},
		{"IPv6 loopback", "tcp6", "::1", nil, false, egress.ErrBlockedAddress},
		{"IPv4-mapped loopback", "tcp4", "::ffff:127.0.0.1", nil, false, egress.ErrBlockedAddress},
		{"name resolving to loopback", "tcp4", testHost, parseAddrs("127.0.0.1"), false, egress.ErrBlockedAddress},
		{"name resolving to IPv4 and IPv6 loopback", "tcp4", testHost, parseAddrs("127.0.0.1", "::1"), false,
			egress.ErrBlockedAddress},
		{"IPv4 loopback, private allowed", "tcp4", "127.0.0.1", nil, true, nil},
		{"IPv6 loopback, private allowed", "tcp6", "::1", nil, true, nil},
		{"IPv4-mapped loopback, private allowed", "tcp4", "::ffff:127.0.0.1", nil, true, nil},
		{"name resolving to loopback, private allowed", "tcp4", testHost, parseAddrs("127.0.0.1"), true, nil},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rcv := newReceiver(t, tc.network, respondStatus(http.StatusAccepted))
			c := NewClient(tc.allowPrivate)
			if tc.dns != nil {
				(&fakeDNS{addrs: tc.dns}).use(c)
			}
			status, err := c.Post(t.Context(), rcv.urlFor(tc.host), testEventID, testSignature, testBody)
			conns, requests := rcv.stop()

			if tc.want != nil {
				checkErr(t, err, tc.want)
				if status != 0 {
					t.Errorf("Post() status = %d, want 0", status)
				}
				if conns != 0 || len(requests) != 0 {
					t.Errorf("receiver got %d connections and %d requests, want none", conns, len(requests))
				}
				return
			}
			if err != nil || status != http.StatusAccepted {
				t.Fatalf("Post() = %d, %v, want %d, nil", status, err, http.StatusAccepted)
			}
			if len(requests) != 1 {
				t.Fatalf("receiver got %d requests, want 1", len(requests))
			}
			checkDelivered(t, requests[0])
		})
	}
}

func TestPostReachesOnlyAllowedAddressesOfAName(t *testing.T) {
	// 127.0.0.1 plays a public address, since a test can't reach a real one.
	rcv := newReceiver(t, "tcp4", respondStatus(http.StatusAccepted))
	c := NewClient(false)
	(&fakeDNS{addrs: parseAddrs("127.0.0.2", "127.0.0.1")}).use(c)
	var (
		mu    sync.Mutex
		asked []netip.Addr
	)
	public := netip.MustParseAddr("127.0.0.1")
	c.allowed = func(a netip.Addr) bool {
		mu.Lock()
		defer mu.Unlock()
		asked = append(asked, a)
		return a == public
	}

	status, err := c.Post(t.Context(), rcv.urlFor(testHost), testEventID, testSignature, testBody)
	if err != nil || status != http.StatusAccepted {
		t.Fatalf("Post() = %d, %v, want %d, nil", status, err, http.StatusAccepted)
	}
	if _, requests := rcv.stop(); len(requests) != 1 {
		t.Errorf("receiver got %d requests, want 1", len(requests))
	}
	mu.Lock()
	defer mu.Unlock()
	if want := parseAddrs("127.0.0.2", "127.0.0.1"); !slices.Equal(asked, want) {
		t.Errorf("addresses checked = %v, want %v", asked, want)
	}
}

func TestPostUnresolvable(t *testing.T) {
	timedOut := &net.OpError{Op: "dial", Net: "udp", Err: os.ErrDeadlineExceeded}
	refused := &net.OpError{Op: "dial", Net: "udp", Err: syscall.ECONNREFUSED}
	tests := []struct {
		name         string
		dns          *fakeDNS
		allowPrivate bool
		wantReason   string
	}{
		{"no such name", &fakeDNS{rcode: rcodeNameError}, false, ": not found"},
		{"no such name, private allowed", &fakeDNS{rcode: rcodeNameError}, true, ": not found"},
		{"no addresses", &fakeDNS{rcode: rcodeSuccess}, false, ": not found"},
		{"server failure", &fakeDNS{rcode: rcodeServerFailure, addrs: parseAddrs("127.0.0.1")}, false, ""},
		{"server unreachable", &fakeDNS{dialErr: refused}, false, ""},
		{"lookup times out", &fakeDNS{dialErr: timedOut}, false, ": timed out"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := NewClient(tc.allowPrivate)
			tc.dns.use(c)
			status, err := c.Post(t.Context(), "http://"+testHost+":8080"+testPath, testEventID, testSignature, testBody)
			checkErr(t, err, egress.ErrUnresolvable)
			if status != 0 {
				t.Errorf("Post() status = %d, want 0", status)
			}
			msg := err.Error()
			if want := "dial tcp: lookup " + testHost + ": " + egress.ErrUnresolvable.Error() + tc.wantReason; !strings.HasSuffix(msg, want) {
				t.Errorf("Post() error = %q, want it to end %q", msg, want)
			}
			if _, ok := errors.AsType[*net.DNSError](err); ok {
				t.Errorf("Post() error = %v exposes the resolver's error", err)
			}
			tc.dns.mu.Lock()
			defer tc.dns.mu.Unlock()
			if len(tc.dns.servers) == 0 {
				t.Fatal("the resolver never dialled the fake DNS server")
			}
			for _, server := range tc.dns.servers {
				if host, _, _ := net.SplitHostPort(server); strings.Contains(msg, host) {
					t.Errorf("Post() error = %q names the DNS server %s", msg, host)
				}
			}
		})
	}
}

func TestPostReturnsEveryStatus(t *testing.T) {
	codes := []int{
		http.StatusOK, http.StatusAccepted, http.StatusNoContent,
		http.StatusMovedPermanently, http.StatusFound, http.StatusSeeOther,
		http.StatusTemporaryRedirect, http.StatusPermanentRedirect,
		http.StatusBadRequest, http.StatusNotFound, http.StatusGone, http.StatusTooManyRequests,
		http.StatusInternalServerError, http.StatusServiceUnavailable,
	}
	for _, code := range codes {
		t.Run(strconv.Itoa(code)+" "+http.StatusText(code), func(t *testing.T) {
			target := newReceiver(t, "tcp4", respondStatus(http.StatusOK))
			rcv := newReceiver(t, "tcp4", func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Location", target.urlFor("127.0.0.1"))
				w.WriteHeader(code)
			})
			status, err := NewClient(true).Post(t.Context(), rcv.urlFor("127.0.0.1"), testEventID, testSignature, testBody)
			if err != nil || status != code {
				t.Fatalf("Post() = %d, %v, want %d, nil", status, err, code)
			}
			if _, requests := rcv.stop(); len(requests) != 1 {
				t.Errorf("receiver got %d requests, want 1", len(requests))
			}
			if conns, requests := target.stop(); conns != 0 || len(requests) != 0 {
				t.Errorf("redirect target got %d connections and %d requests, want none", conns, len(requests))
			}
		})
	}
}

type countingConn struct {
	net.Conn
	read *atomic.Int64
}

func (c countingConn) Read(p []byte) (int, error) {
	n, err := c.Conn.Read(p)
	c.read.Add(int64(n))
	return n, err
}

func TestPostDrain(t *testing.T) {
	// Beyond what the drain asks for, the transport reads the headers into a 4 KiB buffer,
	// which can hold the start of the body.
	const slack = 8 << 10
	// Once a body is closed short of its end, Go's transport reads on by itself, after Close returns,
	// to reuse the connection: up to 256 KiB for at most 50 ms, unless the Content-Length says more remains.
	const transportDrain = 256 << 10
	tests := []struct {
		name      string
		size      int   // -1: chunked and endless
		maxRead   int64 // over both posts
		wantConns int64
	}{
		{"short body is drained and the connection reused", 1 << 10, 2 * (1<<10 + slack), 1},
		{"long body", 64 << 20, 2 * (drainLimit + slack), 2},
		{"endless body", -1, 2 * (drainLimit + transportDrain + slack), 2},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rcv := newReceiver(t, "tcp4", func(w http.ResponseWriter, _ *http.Request) {
				if tc.size >= 0 {
					w.Header().Set("Content-Length", strconv.Itoa(tc.size))
				}
				chunk := make([]byte, 32<<10)
				for written := 0; tc.size < 0 || written < tc.size; {
					n := len(chunk)
					if tc.size >= 0 {
						n = min(n, tc.size-written)
					}
					if _, err := w.Write(chunk[:n]); err != nil {
						return
					}
					written += n
				}
			})
			c := NewClient(true)
			var read atomic.Int64
			dial := c.transport.DialContext
			c.transport.DialContext = func(ctx context.Context, network, address string) (net.Conn, error) {
				conn, err := dial(ctx, network, address)
				if err != nil {
					return nil, err
				}
				return countingConn{conn, &read}, nil
			}

			for range 2 {
				status, err := c.Post(t.Context(), rcv.urlFor("127.0.0.1"), testEventID, testSignature, testBody)
				if err != nil || status != http.StatusOK {
					t.Fatalf("Post() = %d, %v, want %d, nil", status, err, http.StatusOK)
				}
			}
			// The receiver's handler returns once its writes fail, after the client has closed every connection
			// it didn't keep, so no reading is left to count.
			conns, _ := rcv.stop()
			if conns != tc.wantConns {
				t.Errorf("receiver got %d connections, want %d", conns, tc.wantConns)
			}
			if n := read.Load(); n > tc.maxRead {
				t.Errorf("client read %d bytes from its connections, want at most %d", n, tc.maxRead)
			}
		})
	}
}

func TestPostIgnoresProxyEnvironment(t *testing.T) {
	proxy := newReceiver(t, "tcp4", respondStatus(http.StatusOK))
	proxyURL := "http://" + proxy.srv.Listener.Addr().String()
	for _, name := range []string{"HTTP_PROXY", "HTTPS_PROXY", "http_proxy", "https_proxy"} {
		t.Setenv(name, proxyURL)
	}
	for _, name := range []string{"NO_PROXY", "no_proxy"} {
		t.Setenv(name, "")
	}
	rcv := newReceiver(t, "tcp4", respondStatus(http.StatusAccepted))
	c := NewClient(true)
	// net/http reads the proxy variables once per process, so a Proxy func set by mistake might not see these;
	// the nil Proxy is the guarantee, and the request shows the direct path working beside them.
	if c.transport.Proxy != nil {
		t.Error("transport has a Proxy func, want nil")
	}
	// A name, since the environment's proxy settings never apply to a loopback address.
	(&fakeDNS{addrs: parseAddrs("127.0.0.1")}).use(c)

	status, err := c.Post(t.Context(), rcv.urlFor(testHost), testEventID, testSignature, testBody)
	if err != nil || status != http.StatusAccepted {
		t.Fatalf("Post() = %d, %v, want %d, nil", status, err, http.StatusAccepted)
	}
	if _, requests := rcv.stop(); len(requests) != 1 {
		t.Errorf("receiver got %d requests, want 1", len(requests))
	}
	if conns, requests := proxy.stop(); conns != 0 || len(requests) != 0 {
		t.Errorf("proxy got %d connections and %d requests, want none", conns, len(requests))
	}
}

func TestPostOtherFailures(t *testing.T) {
	refusedURL := func(t *testing.T) string {
		l, err := net.Listen("tcp4", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		_ = l.Close()
		return "http://" + l.Addr().String() + testPath
	}
	tests := []struct {
		name    string
		url     func(t *testing.T) string
		wantMsg string
	}{
		{"connection refused", refusedURL, "connection refused"},
		{"untrusted certificate", func(t *testing.T) string {
			rcv := startReceiver(t, "tcp4", respondStatus(http.StatusOK), (*httptest.Server).StartTLS)
			t.Cleanup(func() {
				if conns, requests := rcv.stop(); conns != 1 || len(requests) != 0 {
					t.Errorf("receiver got %d connections and %d requests, want 1 and none", conns, len(requests))
				}
			})
			return rcv.urlFor("127.0.0.1")
		}, "certificate"},
		{"malformed URL", func(*testing.T) string { return "http://[::1" }, "missing ']'"},
		{"unsupported scheme", func(*testing.T) string { return "ftp://127.0.0.1" + testPath }, "unsupported protocol scheme"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			status, err := NewClient(true).Post(t.Context(), tc.url(t), testEventID, testSignature, testBody)
			checkErr(t, err, nil)
			if status != 0 {
				t.Errorf("Post() status = %d, want 0", status)
			}
			if !strings.Contains(err.Error(), tc.wantMsg) {
				t.Errorf("Post() error = %q, want it to mention %q", err, tc.wantMsg)
			}
		})
	}
}

// inMemory sends every connection c makes to srv over httptest's in-memory network,
// so a synctest bubble's clock drives every timeout.
func inMemory(t *testing.T, c *Client, srv *httptest.Server) {
	c.transport.DialContext = srv.Client().Transport.(*http.Transport).DialContext
	t.Cleanup(c.transport.CloseIdleConnections)
}

// swallow sends every connection c makes to a peer that reads everything and never answers.
func swallow(t *testing.T, c *Client) {
	c.transport.DialContext = func(context.Context, string, string) (net.Conn, error) {
		client, server := net.Pipe()
		go func() {
			_, _ = io.Copy(io.Discard, server)
			_ = server.Close()
		}()
		return client, nil
	}
	t.Cleanup(c.transport.CloseIdleConnections)
}

func TestPostTimeouts(t *testing.T) {
	headersAfter := func(d time.Duration) http.HandlerFunc {
		return func(w http.ResponseWriter, _ *http.Request) {
			time.Sleep(d)
			w.WriteHeader(http.StatusAccepted)
		}
	}
	stallingBody := func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(make([]byte, 1<<10))
		_ = http.NewResponseController(w).Flush()
		stall(w, r)
	}
	tests := []struct {
		name        string
		scheme      string
		respond     http.HandlerFunc // nil: a peer that never answers
		wantElapsed time.Duration
		wantStatus  int
		wantMsg     string // empty: no error
	}{
		{"TLS handshake", "https", nil, 5 * time.Second, 0, "TLS handshake timeout"},
		{"response headers", "http", stall, 10 * time.Second, 0, "timeout awaiting response headers"},
		{"response headers in time", "http", headersAfter(9 * time.Second), 9 * time.Second, http.StatusAccepted, ""},
		{"body outlasting the request", "http", stallingBody, 15 * time.Second, http.StatusOK, ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				c := NewClient(false)
				if tc.respond == nil {
					swallow(t, c)
				} else {
					inMemory(t, c, httptest.NewTestServer(t, tc.respond))
				}
				start := time.Now()
				status, err := c.Post(t.Context(), tc.scheme+"://"+testHost+testPath, testEventID, testSignature, testBody)
				if elapsed := time.Since(start); elapsed != tc.wantElapsed {
					t.Errorf("Post() returned after %v, want %v", elapsed, tc.wantElapsed)
				}
				if status != tc.wantStatus {
					t.Errorf("Post() status = %d, want %d", status, tc.wantStatus)
				}
				if tc.wantMsg == "" {
					if err != nil {
						t.Errorf("Post() error = %v, want nil", err)
					}
					return
				}
				checkErr(t, err, nil)
				if urlErr, ok := errors.AsType[*url.Error](err); !ok || !urlErr.Timeout() {
					t.Errorf("Post() error = %v, want a timeout", err)
				}
				if !strings.Contains(err.Error(), tc.wantMsg) {
					t.Errorf("Post() error = %q, want it to mention %q", err, tc.wantMsg)
				}
			})
		})
	}
}

func TestPostCancellation(t *testing.T) {
	type ctxFunc func(context.Context) (context.Context, context.CancelFunc)
	cancelled := func(ctx context.Context) (context.Context, context.CancelFunc) {
		ctx, cancel := context.WithCancel(ctx)
		cancel()
		return ctx, cancel
	}
	cancelledAfter := func(d time.Duration) ctxFunc {
		return func(ctx context.Context) (context.Context, context.CancelFunc) {
			ctx, cancel := context.WithCancel(ctx)
			time.AfterFunc(d, cancel)
			return ctx, cancel
		}
	}
	timeout := func(d time.Duration) ctxFunc {
		return func(ctx context.Context) (context.Context, context.CancelFunc) {
			return context.WithTimeout(ctx, d)
		}
	}
	tests := []struct {
		name        string
		ctx         ctxFunc
		wantElapsed time.Duration
		wantArrived bool
		want        error
	}{
		{"cancelled before the request", cancelled, 0, false, context.Canceled},
		{"cancelled awaiting the response", cancelledAfter(3 * time.Second), 3 * time.Second, true, context.Canceled},
		{"deadline awaiting the response", timeout(3 * time.Second), 3 * time.Second, true, context.DeadlineExceeded},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				var conns, arrived, aborted atomic.Int64
				srv := httptest.NewTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					arrived.Add(1)
					stall(w, r)
					if r.Context().Err() != nil {
						aborted.Add(1)
					}
				}))
				srv.Config.ConnState = func(_ net.Conn, s http.ConnState) {
					if s == http.StateNew {
						conns.Add(1)
					}
				}
				c := NewClient(false)
				inMemory(t, c, srv)
				ctx, cancel := tc.ctx(t.Context())
				defer cancel()

				start := time.Now()
				status, err := c.Post(ctx, "http://"+testHost+testPath, testEventID, testSignature, testBody)
				if elapsed := time.Since(start); elapsed != tc.wantElapsed {
					t.Errorf("Post() returned after %v, want %v", elapsed, tc.wantElapsed)
				}
				checkErr(t, err, tc.want)
				if status != 0 {
					t.Errorf("Post() status = %d, want 0", status)
				}
				synctest.Wait()
				if !tc.wantArrived {
					if conns.Load() != 0 || arrived.Load() != 0 {
						t.Errorf("server got %d connections and %d requests, want none", conns.Load(), arrived.Load())
					}
					return
				}
				if arrived.Load() != 1 || aborted.Load() != 1 {
					t.Errorf("server got %d requests, %d of them aborted, want 1 aborted", arrived.Load(), aborted.Load())
				}
			})
		})
	}
}
