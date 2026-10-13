package webhook

import (
	"context"
	"errors"
	"fmt"
	"net/netip"
	"time"
)

const resolveTimeout = 5 * time.Second

var (
	ErrBlockedAddress = errors.New("blocked address")
	errUnresolvable   = errors.New("unresolvable host")
)

// The blocked ranges netip's predicates don't cover. Read-only.
var blockedPrefixes = [...]netip.Prefix{
	netip.MustParsePrefix("0.0.0.0/8"),
	netip.MustParsePrefix("100.64.0.0/10"),
	netip.MustParsePrefix("192.0.0.0/24"),
	netip.MustParsePrefix("198.18.0.0/15"),
	netip.MustParsePrefix("240.0.0.0/4"),
	netip.MustParsePrefix("64:ff9b::/96"),
}

// IsPublic reports whether a webhook may be delivered to addr.
// An IPv4-mapped address is judged by the IPv4 address it carries, and an address with a zone is never public.
func IsPublic(addr netip.Addr) bool {
	if addr.Zone() != "" {
		return false
	}
	addr = addr.Unmap()
	if !addr.IsGlobalUnicast() || addr.IsPrivate() || addr.IsLoopback() || addr.IsLinkLocalUnicast() ||
		addr.IsMulticast() || addr.IsUnspecified() {
		return false
	}
	for _, p := range blockedPrefixes {
		if p.Contains(addr) {
			return false
		}
	}
	return true
}

type resolver interface {
	LookupNetIP(ctx context.Context, network, host string) ([]netip.Addr, error)
}

type HostChecker struct {
	allowPrivate bool
	resolver     resolver
}

// NewHostChecker resolves names with r, such as net.DefaultResolver.
// With allowPrivate, every host passes.
func NewHostChecker(allowPrivate bool, r resolver) *HostChecker {
	return &HostChecker{allowPrivate: allowPrivate, resolver: r}
}

// CheckHost takes a URL's Hostname. An IP address is checked as it is, and a name must resolve
// to at least one address, all of them public. A non-public address is an ErrBlockedAddress.
func (c *HostChecker) CheckHost(ctx context.Context, host string) error {
	if c.allowPrivate {
		return nil
	}
	if addr, err := netip.ParseAddr(host); err == nil {
		return checkAddr(host, addr)
	}

	lookupCtx, cancel := context.WithTimeout(ctx, resolveTimeout)
	defer cancel()
	// The resolver's error isn't wrapped, since its text can name the DNS server.
	addrs, err := c.resolver.LookupNetIP(lookupCtx, "ip", host)
	switch {
	case ctx.Err() != nil:
		return fmt.Errorf("error checking webhook host %q: %w", host, ctx.Err())
	case err != nil && lookupCtx.Err() != nil:
		return fmt.Errorf("error checking webhook host %q: %w: timed out after %s", host, errUnresolvable, resolveTimeout)
	case err != nil:
		return fmt.Errorf("error checking webhook host %q: %w", host, errUnresolvable)
	case len(addrs) == 0:
		return fmt.Errorf("error checking webhook host %q: %w: no addresses", host, errUnresolvable)
	}
	for _, addr := range addrs {
		if err := checkAddr(host, addr); err != nil {
			return err
		}
	}
	return nil
}

func checkAddr(host string, addr netip.Addr) error {
	if !IsPublic(addr) {
		return fmt.Errorf("error checking webhook host %q: %w %s", host, ErrBlockedAddress, addr)
	}
	return nil
}
