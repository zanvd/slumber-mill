package webhook

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/netip"
	"syscall"
	"time"

	"github.com/zanvd/slumber-mill/src/egress"
)

const EventIDHeader = "SlumberMill-Event-Id"

const (
	userAgent             = "slumber-mill-webhooks/1"
	dialTimeout           = 5 * time.Second
	tlsHandshakeTimeout   = 5 * time.Second
	responseHeaderTimeout = 10 * time.Second
	requestTimeout        = 15 * time.Second
	idleConnTimeout       = 90 * time.Second
	drainLimit            = 64 << 10
)

// Client posts signed events to tenant URLs. It checks every address as it is dialled,
// after resolution, so a name can't be rebound to a private address once a check has passed.
type Client struct {
	allowed   func(netip.Addr) bool
	dialer    *net.Dialer
	transport *http.Transport
	http      *http.Client
}

// NewClient returns a client that dials only addresses egress.IsPublic accepts, or any address with allowPrivate.
func NewClient(allowPrivate bool) *Client {
	c := &Client{allowed: egress.IsPublic}
	if allowPrivate {
		c.allowed = func(netip.Addr) bool { return true }
	}
	c.dialer = &net.Dialer{Timeout: dialTimeout, Control: c.control}
	c.transport = &http.Transport{
		Proxy:                 nil, // an environment proxy would dial in place of the checked dialer
		DialContext:           c.dial,
		TLSHandshakeTimeout:   tlsHandshakeTimeout,
		ResponseHeaderTimeout: responseHeaderTimeout,
		IdleConnTimeout:       idleConnTimeout,
	}
	c.http = &http.Client{
		Transport: c.transport,
		Timeout:   requestTimeout,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
	return c
}

// Post sends body to url and returns the status of whatever response arrives, a redirect included.
// A dial the address check refuses wraps egress.ErrBlockedAddress, and a failed lookup egress.ErrUnresolvable.
func (c *Client) Post(ctx context.Context, url, eventID, signature string, body []byte) (int, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return 0, fmt.Errorf("error building webhook request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", userAgent)
	req.Header.Set(EventIDHeader, eventID)
	req.Header.Set(SignatureHeader, signature)

	resp, err := c.http.Do(req)
	if err != nil {
		return 0, fmt.Errorf("error posting webhook: %w", err)
	}
	// Draining a short body keeps the connection alive; a longer one isn't worth reading.
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, drainLimit))
	_ = resp.Body.Close()
	return resp.StatusCode, nil
}

func (c *Client) control(_, address string, _ syscall.RawConn) error {
	ap, err := netip.ParseAddrPort(address)
	if err != nil || !c.allowed(ap.Addr()) {
		return egress.ErrBlockedAddress
	}
	return nil
}

func (c *Client) dial(ctx context.Context, network, address string) (net.Conn, error) {
	conn, err := c.dialer.DialContext(ctx, network, address)
	if err == nil {
		return conn, nil
	}
	dnsErr, ok := errors.AsType[*net.DNSError](err)
	if !ok || ctx.Err() != nil {
		return nil, err
	}
	// The resolver's error isn't wrapped, since its text can name the DNS server.
	host, _, _ := net.SplitHostPort(address)
	reason := ""
	switch {
	case dnsErr.IsTimeout:
		reason = ": timed out"
	case dnsErr.IsNotFound:
		reason = ": not found"
	}
	err = fmt.Errorf("lookup %s: %w%s", host, egress.ErrUnresolvable, reason)
	return nil, &net.OpError{Op: "dial", Net: network, Err: err}
}
