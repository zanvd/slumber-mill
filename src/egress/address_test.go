package egress

import (
	"context"
	"errors"
	"net"
	"net/netip"
	"strings"
	"testing"
	"testing/synctest"
	"time"
)

var (
	_ resolver = (*net.Resolver)(nil)
	_ interface {
		CheckHost(ctx context.Context, host string) error
	} = (*HostChecker)(nil)
)

func addrs(ss ...string) []netip.Addr {
	out := make([]netip.Addr, len(ss))
	for i, s := range ss {
		out[i] = netip.MustParseAddr(s)
	}
	return out
}

func TestIsPublic(t *testing.T) {
	tests := []struct {
		name    string
		blocked []netip.Addr
		public  []netip.Addr
	}{
		{"zero value", []netip.Addr{{}}, nil},
		{"0.0.0.0/8 this network",
			addrs("0.0.0.0", "0.0.0.1", "0.255.255.255", "::ffff:0.0.0.0", "::ffff:0.0.0.1", "::ffff:0.255.255.255"),
			addrs("1.0.0.0")},
		{"10.0.0.0/8 private",
			addrs("10.0.0.0", "10.0.0.1", "10.255.255.255", "::ffff:10.0.0.0", "::ffff:10.0.0.1", "::ffff:10.255.255.255"),
			addrs("9.255.255.255", "11.0.0.0")},
		{"100.64.0.0/10 shared address space",
			addrs("100.64.0.0", "100.127.255.255", "::ffff:100.64.0.0", "::ffff:100.127.255.255"),
			addrs("100.63.255.255", "100.128.0.0")},
		{"127.0.0.0/8 loopback",
			addrs("127.0.0.0", "127.0.0.1", "127.255.255.255", "::ffff:127.0.0.1", "::ffff:127.255.255.255"),
			addrs("126.255.255.255", "128.0.0.0")},
		{"169.254.0.0/16 link-local",
			addrs("169.254.0.0", "169.254.169.254", "169.254.255.255", "::ffff:169.254.0.0", "::ffff:169.254.169.254"),
			addrs("169.253.255.255", "169.255.0.0")},
		{"172.16.0.0/12 private",
			addrs("172.16.0.0", "172.31.255.255", "::ffff:172.16.0.0", "::ffff:172.31.255.255"),
			addrs("172.15.255.255", "172.32.0.0")},
		{"192.0.0.0/24 IETF protocol assignments",
			addrs("192.0.0.0", "192.0.0.255", "::ffff:192.0.0.0", "::ffff:192.0.0.255"),
			addrs("191.255.255.255", "192.0.1.0")},
		{"192.88.99.0/24 6to4 relay anycast",
			addrs("192.88.99.0", "192.88.99.1", "192.88.99.255", "::ffff:192.88.99.0", "::ffff:192.88.99.1",
				"::ffff:192.88.99.255"),
			addrs("192.88.98.255", "192.88.100.0", "::ffff:192.88.98.255", "::ffff:192.88.100.0")},
		{"192.168.0.0/16 private",
			addrs("192.168.0.0", "192.168.255.255", "::ffff:192.168.0.0", "::ffff:192.168.255.255"),
			addrs("192.167.255.255", "192.169.0.0")},
		{"198.18.0.0/15 benchmarking",
			addrs("198.18.0.0", "198.19.255.255", "::ffff:198.18.0.0", "::ffff:198.19.255.255"),
			addrs("198.17.255.255", "198.20.0.0")},
		{"224.0.0.0/4 multicast",
			addrs("224.0.0.0", "224.0.0.1", "239.255.255.255", "::ffff:224.0.0.0", "::ffff:239.255.255.255"),
			addrs("223.255.255.255")},
		{"240.0.0.0/4 reserved",
			addrs("240.0.0.0", "255.255.255.254", "::ffff:240.0.0.0", "::ffff:255.255.255.254"),
			nil},
		{"255.255.255.255 broadcast",
			addrs("255.255.255.255", "::ffff:255.255.255.255"),
			nil},
		{"IPv6 unspecified", addrs("::"), nil},
		{"IPv6 loopback", addrs("::1"), nil},
		{"fc00::/7 unique local",
			addrs("fc00::", "fd00::1", "fdff:ffff:ffff:ffff:ffff:ffff:ffff:ffff"),
			addrs("fbff:ffff:ffff:ffff:ffff:ffff:ffff:ffff", "fe00::")},
		{"fe80::/10 link-local",
			addrs("fe80::", "fe80::1", "febf:ffff:ffff:ffff:ffff:ffff:ffff:ffff"),
			addrs("fe7f:ffff:ffff:ffff:ffff:ffff:ffff:ffff")},
		// Both neighbours are blocked too: the top of fe80::/10 and ff00::.
		{"fec0::/10 site-local",
			addrs("fec0::", "fec0::1", "feff:ffff:ffff:ffff:ffff:ffff:ffff:ffff"),
			nil},
		{"ff00::/8 multicast",
			addrs("ff00::", "ff02::1", "ff05::1:3", "ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff"),
			nil},
		{"64:ff9b::/96 NAT64",
			addrs("64:ff9b::", "64:ff9b::a00:1", "64:ff9b::7f00:1", "64:ff9b::a9fe:a9fe", "64:ff9b::808:808",
				"64:ff9b::ffff:ffff"),
			addrs("64:ff9a:ffff:ffff:ffff:ffff:ffff:ffff", "64:ff9b::1:0:0")},
		// Under a /48 prefix the IPv4 address is split around the u-octet: 10.0.0.1 is 64:ff9b:1:a00:0:100::.
		{"64:ff9b:1::/48 local-use NAT64",
			addrs("64:ff9b:1::", "64:ff9b:1:a00:0:100::", "64:ff9b:1:a9fe:a9:fe00::", "64:ff9b:1:808:8:800::",
				"64:ff9b:1:ffff:ffff:ffff:ffff:ffff"),
			addrs("64:ff9b:0:ffff:ffff:ffff:ffff:ffff", "64:ff9b:2::")},
		{"2001::/23 IETF protocol assignments",
			addrs("2001::", "2001::1", "2001:2::1", "2001:10::1", "2001:20::1",
				"2001:1ff:ffff:ffff:ffff:ffff:ffff:ffff"),
			addrs("2000:ffff:ffff:ffff:ffff:ffff:ffff:ffff", "2001:200::")},
		{"2002::/16 6to4",
			addrs("2002::", "2002:a00:1::1", "2002:7f00:1::1", "2002:808:808::1",
				"2002:ffff:ffff:ffff:ffff:ffff:ffff:ffff"),
			addrs("2001:ffff:ffff:ffff:ffff:ffff:ffff:ffff", "2003::")},
		{"5f00::/16 SRv6",
			addrs("5f00::", "5f00::1", "5f00:ffff:ffff:ffff:ffff:ffff:ffff:ffff"),
			addrs("5eff:ffff:ffff:ffff:ffff:ffff:ffff:ffff", "5f01::")},
		{"zoned",
			addrs("fe80::1%eth0", "fe80::1%1", "2001:4860:4860::8888%eth0", "::ffff:8.8.8.8%eth0"),
			nil},
		{"public IPv4", nil, addrs("1.1.1.1", "8.8.8.8", "9.9.9.9")},
		{"public IPv6", nil, addrs("2001:4860:4860::8888", "2606:4700:4700::1111")},
		{"public IPv4-mapped", nil, addrs("::ffff:1.1.1.1", "::ffff:8.8.8.8")},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			for _, addr := range tc.blocked {
				if IsPublic(addr) {
					t.Errorf("IsPublic(%v) = true, want false", addr)
				}
			}
			for _, addr := range tc.public {
				if !IsPublic(addr) {
					t.Errorf("IsPublic(%v) = false, want true", addr)
				}
			}
		})
	}
}

type fakeResolver struct {
	addrs []netip.Addr
	err   error
	// wait makes the lookup last until its context is done.
	wait bool

	calls   int
	network string
	host    string
}

func (r *fakeResolver) LookupNetIP(ctx context.Context, network, host string) ([]netip.Addr, error) {
	r.calls++
	r.network, r.host = network, host
	if r.wait {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	return r.addrs, r.err
}

// checkErr fails unless err is nil when want is, or else matches want and none of the other outcomes.
func checkErr(t *testing.T, err, want error) {
	t.Helper()
	if err == nil || want == nil {
		if err != want {
			t.Fatalf("CheckHost() error = %v, want %v", err, want)
		}
		return
	}
	for _, target := range []error{ErrBlockedAddress, ErrUnresolvable, context.Canceled, context.DeadlineExceeded} {
		if got := errors.Is(err, target); got != (target == want) {
			t.Errorf("errors.Is(%v, %v) = %v, want %v", err, target, got, !got)
		}
	}
}

func TestCheckHost(t *testing.T) {
	dnsErr := &net.DNSError{Err: "no such host", Name: "missing.example", Server: "10.53.0.1:53", IsNotFound: true}
	tests := []struct {
		name      string
		host      string
		addrs     []netip.Addr
		err       error
		want      error
		wantAddr  string // the blocked address the error names
		wantCalls int
	}{
		{"public IPv4 literal", "8.8.8.8", nil, nil, nil, "", 0},
		{"public IPv6 literal", "2001:4860:4860::8888", nil, nil, nil, "", 0},
		{"public IPv4-mapped literal", "::ffff:8.8.8.8", nil, nil, nil, "", 0},
		{"private IPv4 literal", "10.0.0.1", nil, nil, ErrBlockedAddress, "10.0.0.1", 0},
		{"metadata literal", "169.254.169.254", nil, nil, ErrBlockedAddress, "169.254.169.254", 0},
		{"IPv6 loopback literal", "::1", nil, nil, ErrBlockedAddress, "::1", 0},
		{"IPv4-mapped loopback literal", "::ffff:127.0.0.1", nil, nil, ErrBlockedAddress, "::ffff:127.0.0.1", 0},
		{"NAT64 literal", "64:ff9b::a00:1", nil, nil, ErrBlockedAddress, "64:ff9b::a00:1", 0},
		{"zoned literal", "fe80::1%eth0", nil, nil, ErrBlockedAddress, "fe80::1%eth0", 0},
		{"public name", "hooks.example.com", addrs("8.8.8.8"), nil, nil, "", 1},
		{"name with public IPv4 and IPv6", "hooks.example.com", addrs("8.8.8.8", "2001:4860:4860::8888"), nil, nil, "", 1},
		{"name resolving to private", "internal.example", addrs("10.0.0.5"), nil, ErrBlockedAddress, "10.0.0.5", 1},
		{"public then private", "rebind.example", addrs("8.8.8.8", "10.0.0.5"), nil, ErrBlockedAddress, "10.0.0.5", 1},
		{"private then public", "rebind.example", addrs("192.168.1.1", "8.8.8.8"), nil, ErrBlockedAddress, "192.168.1.1", 1},
		{"IPv4-mapped private", "mapped.example", addrs("8.8.8.8", "::ffff:172.16.0.1"), nil, ErrBlockedAddress, "::ffff:172.16.0.1", 1},
		{"NAT64 of a public address", "nat64.example", addrs("64:ff9b::808:808"), nil, ErrBlockedAddress, "64:ff9b::808:808", 1},
		{"zoned public result", "zoned.example", addrs("8.8.8.8", "2001:4860:4860::8888%eth0"), nil, ErrBlockedAddress, "2001:4860:4860::8888%eth0", 1},
		{"localhost", "localhost", addrs("::1", "127.0.0.1"), nil, ErrBlockedAddress, "::1", 1},
		{"empty result", "empty.example", nil, nil, ErrUnresolvable, "", 1},
		{"resolver error", "missing.example", nil, dnsErr, ErrUnresolvable, "", 1},
		{"resolver error with addresses", "missing.example", addrs("8.8.8.8"), dnsErr, ErrUnresolvable, "", 1},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			r := &fakeResolver{addrs: tc.addrs, err: tc.err}
			err := NewHostChecker(false, r).CheckHost(t.Context(), tc.host)
			checkErr(t, err, tc.want)

			if r.calls != tc.wantCalls {
				t.Errorf("resolver called %d times, want %d", r.calls, tc.wantCalls)
			}
			if r.calls > 0 && (r.network != "ip" || r.host != tc.host) {
				t.Errorf("LookupNetIP(%q, %q), want (%q, %q)", r.network, r.host, "ip", tc.host)
			}
			if err == nil {
				return
			}
			msg := err.Error()
			if !strings.Contains(msg, tc.host) {
				t.Errorf("error %q doesn't name the host %q", msg, tc.host)
			}
			if tc.wantAddr != "" && !strings.Contains(msg, ErrBlockedAddress.Error()+" "+tc.wantAddr) {
				t.Errorf("error %q doesn't name the blocked address %s", msg, tc.wantAddr)
			}
			if tc.err != nil && (errors.Is(err, tc.err) || strings.Contains(msg, dnsErr.Server)) {
				t.Errorf("error %q exposes the resolver's error", msg)
			}
		})
	}
}

func TestCheckHostAllowPrivate(t *testing.T) {
	hosts := []string{"10.0.0.1", "::1", "::ffff:127.0.0.1", "fe80::1%eth0", "8.8.8.8", "localhost", "internal.example", ""}
	r := &fakeResolver{addrs: addrs("10.0.0.5"), err: errors.New("lookup failed")}
	c := NewHostChecker(true, r)
	for _, host := range hosts {
		t.Run(host, func(t *testing.T) {
			if err := c.CheckHost(t.Context(), host); err != nil {
				t.Errorf("CheckHost(%q) error = %v, want nil", host, err)
			}
		})
	}
	if r.calls != 0 {
		t.Errorf("resolver called %d times, want 0", r.calls)
	}
}

func TestCheckHostContext(t *testing.T) {
	type parentFunc func(context.Context) (context.Context, context.CancelFunc)
	cancelled := func(ctx context.Context) (context.Context, context.CancelFunc) {
		ctx, cancel := context.WithCancel(ctx)
		cancel()
		return ctx, cancel
	}
	cancelledAfter := func(d time.Duration) parentFunc {
		return func(ctx context.Context) (context.Context, context.CancelFunc) {
			ctx, cancel := context.WithCancel(ctx)
			time.AfterFunc(d, cancel)
			return ctx, cancel
		}
	}
	timeout := func(d time.Duration) parentFunc {
		return func(ctx context.Context) (context.Context, context.CancelFunc) {
			return context.WithTimeout(ctx, d)
		}
	}
	const timedOut = "timed out after 5s"
	tests := []struct {
		name        string
		parent      parentFunc
		wait        bool
		wantElapsed time.Duration
		want        error
		wantMsg     string
	}{
		{"lookup hangs", context.WithCancel, true, resolveTimeout, ErrUnresolvable, timedOut},
		{"lookup outlasts a later caller deadline", timeout(time.Minute), true, resolveTimeout, ErrUnresolvable, timedOut},
		{"caller deadline is sooner", timeout(2 * time.Second), true, 2 * time.Second, context.DeadlineExceeded, ""},
		{"caller cancels during the lookup", cancelledAfter(time.Second), true, time.Second, context.Canceled, ""},
		{"caller cancelled before the lookup", cancelled, true, 0, context.Canceled, ""},
		{"caller cancelled, resolver ignores it", cancelled, false, 0, context.Canceled, ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := tc.parent(t.Context())
				defer cancel()
				r := &fakeResolver{addrs: addrs("8.8.8.8"), wait: tc.wait}
				start := time.Now()
				err := NewHostChecker(false, r).CheckHost(ctx, "slow.example")
				if elapsed := time.Since(start); elapsed != tc.wantElapsed {
					t.Errorf("CheckHost() returned after %v, want %v", elapsed, tc.wantElapsed)
				}
				checkErr(t, err, tc.want)
				if !strings.Contains(err.Error(), tc.wantMsg) {
					t.Errorf("error %q doesn't say %q", err, tc.wantMsg)
				}
			})
		})
	}
}
