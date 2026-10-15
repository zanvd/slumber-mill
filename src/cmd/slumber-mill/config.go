package main

import (
	"errors"
	"fmt"
	"net/netip"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/zanvd/slumber-mill/src/httpsrv"
	"github.com/zanvd/slumber-mill/src/secret"
)

const (
	envDSN                 = "SLUMBER_MILL_DSN"
	envDataDir             = "SLUMBER_MILL_DATA_DIR"
	envHTTPAddr            = "SLUMBER_MILL_HTTP_ADDR"
	envMaxUploadBytes      = "SLUMBER_MILL_MAX_UPLOAD_BYTES"
	envMaxUploadsPerTenant = "SLUMBER_MILL_MAX_UPLOADS_PER_TENANT"
	envEncodeConcurrency   = "SLUMBER_MILL_ENCODE_CONCURRENCY"
	envWebhookConcurrency  = "SLUMBER_MILL_WEBHOOK_CONCURRENCY"
	envEncodeLease         = "SLUMBER_MILL_ENCODE_LEASE"
	envShutdownGrace       = "SLUMBER_MILL_SHUTDOWN_GRACE"
	envDrainDelay          = "SLUMBER_MILL_DRAIN_DELAY"
	envTrustedProxies      = "SLUMBER_MILL_TRUSTED_PROXIES"
	envWebhookAllowPrivate = "SLUMBER_MILL_WEBHOOK_ALLOW_PRIVATE"
	envSecretKeys          = "SLUMBER_MILL_SECRET_KEYS"

	defaultDSN                = "slumber_mill:slumber_mill@tcp(mysql:3306)/slumber_mill"
	defaultDataDir            = "/data"
	defaultHTTPAddr           = ":8080"
	defaultWebhookConcurrency = 8
	defaultEncodeLease        = 2 * time.Minute
	defaultShutdownGrace      = 30 * time.Second
	defaultDrainDelay         = 10 * time.Second

	// minEncodeLease keeps the startup sweep, whose age is the encode lease, well clear of staged uploads:
	// nothing refreshes their temp files, and an upload may go 30 s without a write.
	minEncodeLease = time.Minute
)

type migrateConfig struct {
	DSN string
}

type apiConfig struct {
	DSN                 string
	HTTPAddr            string
	DataDir             string
	MaxUploadBytes      int64
	MaxUploadsPerTenant int
	// TrustedProxies is nil when no proxy is trusted.
	TrustedProxies []string
	// EncodeLease is the encode pool's lease, which the startup temp sweep's age follows.
	EncodeLease   time.Duration
	DrainDelay    time.Duration
	ShutdownGrace time.Duration
}

type encodeConfig struct {
	DSN           string
	DataDir       string
	Concurrency   int
	Lease         time.Duration
	ShutdownGrace time.Duration
}

type webhookConfig struct {
	DSN         string
	DataDir     string
	Concurrency int
	// EncodeLease is the encode pool's lease, which the startup temp sweep's age follows.
	EncodeLease         time.Duration
	ShutdownGrace       time.Duration
	WebhookAllowPrivate bool
	SecretKeys          secret.Value
}

type tenantCreateConfig struct {
	DSN                 string
	SecretKeys          secret.Value
	WebhookAllowPrivate bool
}

func loadMigrateConfig(getenv func(string) string) (migrateConfig, error) {
	p := envParser{getenv: getenv}
	cfg := migrateConfig{DSN: p.string(envDSN, defaultDSN)}
	return cfg, p.err()
}

func loadAPIConfig(getenv func(string) string) (apiConfig, error) {
	p := envParser{getenv: getenv}
	cfg := apiConfig{
		DSN:                 p.string(envDSN, defaultDSN),
		HTTPAddr:            p.string(envHTTPAddr, defaultHTTPAddr),
		DataDir:             p.string(envDataDir, defaultDataDir),
		MaxUploadBytes:      p.positiveInt64(envMaxUploadBytes, httpsrv.DefaultMaxUploadBytes),
		MaxUploadsPerTenant: p.positiveInt(envMaxUploadsPerTenant, httpsrv.DefaultMaxUploadsPerTenant),
		TrustedProxies:      p.addresses(envTrustedProxies),
		EncodeLease:         p.durationAtLeast(envEncodeLease, defaultEncodeLease, minEncodeLease),
		DrainDelay:          p.duration(envDrainDelay, defaultDrainDelay),
		ShutdownGrace:       p.positiveDuration(envShutdownGrace, defaultShutdownGrace),
	}
	return cfg, p.err()
}

func loadEncodeConfig(getenv func(string) string) (encodeConfig, error) {
	p := envParser{getenv: getenv}
	cfg := encodeConfig{
		DSN:           p.string(envDSN, defaultDSN),
		DataDir:       p.string(envDataDir, defaultDataDir),
		Concurrency:   p.positiveInt(envEncodeConcurrency, runtime.GOMAXPROCS(0)),
		Lease:         p.durationAtLeast(envEncodeLease, defaultEncodeLease, minEncodeLease),
		ShutdownGrace: p.positiveDuration(envShutdownGrace, defaultShutdownGrace),
	}
	return cfg, p.err()
}

func loadWebhookConfig(getenv func(string) string) (webhookConfig, error) {
	p := envParser{getenv: getenv}
	cfg := webhookConfig{
		DSN:                 p.string(envDSN, defaultDSN),
		DataDir:             p.string(envDataDir, defaultDataDir),
		Concurrency:         p.positiveInt(envWebhookConcurrency, defaultWebhookConcurrency),
		EncodeLease:         p.durationAtLeast(envEncodeLease, defaultEncodeLease, minEncodeLease),
		ShutdownGrace:       p.positiveDuration(envShutdownGrace, defaultShutdownGrace),
		WebhookAllowPrivate: p.bool(envWebhookAllowPrivate, false),
		SecretKeys:          p.secret(envSecretKeys),
	}
	return cfg, p.err()
}

func loadTenantCreateConfig(getenv func(string) string) (tenantCreateConfig, error) {
	p := envParser{getenv: getenv}
	cfg := tenantCreateConfig{
		DSN:                 p.string(envDSN, defaultDSN),
		SecretKeys:          p.secret(envSecretKeys),
		WebhookAllowPrivate: p.bool(envWebhookAllowPrivate, false),
	}
	return cfg, p.err()
}

// envParser reads environment variables, treating an empty one as unset, and collects every invalid one.
type envParser struct {
	getenv func(string) string
	errs   []error
}

func (p *envParser) err() error {
	return errors.Join(p.errs...)
}

func (p *envParser) lookup(name string) string {
	return strings.TrimSpace(p.getenv(name))
}

func (p *envParser) invalid(name, want, got string) {
	p.errs = append(p.errs, fmt.Errorf("%s must be %s, got %q", name, want, got))
}

func (p *envParser) string(name, def string) string {
	if v := p.lookup(name); v != "" {
		return v
	}
	return def
}

// secret's errors never quote the value.
func (p *envParser) secret(name string) secret.Value {
	v := p.lookup(name)
	if v == "" {
		p.errs = append(p.errs, fmt.Errorf("%s is required", name))
		return secret.Value{}
	}
	return secret.NewValue([]byte(v))
}

func (p *envParser) positiveInt64(name string, def int64) int64 {
	v := p.lookup(name)
	if v == "" {
		return def
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil || n <= 0 {
		p.invalid(name, "a positive integer", v)
		return def
	}
	return n
}

func (p *envParser) positiveInt(name string, def int) int {
	v := p.lookup(name)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil || n <= 0 {
		p.invalid(name, "a positive integer", v)
		return def
	}
	return n
}

func (p *envParser) duration(name string, def time.Duration) time.Duration {
	return p.durationFrom(name, def, 0, "a duration of zero or more, such as 10s")
}

func (p *envParser) positiveDuration(name string, def time.Duration) time.Duration {
	return p.durationFrom(name, def, time.Nanosecond, "a positive duration, such as 30s")
}

func (p *envParser) durationAtLeast(name string, def, minimum time.Duration) time.Duration {
	return p.durationFrom(name, def, minimum, "a duration of at least "+minimum.String()+", such as "+def.String())
}

func (p *envParser) durationFrom(name string, def, minimum time.Duration, want string) time.Duration {
	v := p.lookup(name)
	if v == "" {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil || d < minimum {
		p.invalid(name, want, v)
		return def
	}
	return d
}

func (p *envParser) bool(name string, def bool) bool {
	v := p.lookup(name)
	if v == "" {
		return def
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		p.invalid(name, "true or false", v)
		return def
	}
	return b
}

// addresses reads a comma-separated list of IP addresses and CIDR prefixes.
func (p *envParser) addresses(name string) []string {
	v := p.lookup(name)
	if v == "" {
		return nil
	}
	var list []string
	for item := range strings.SplitSeq(v, ",") {
		item = strings.TrimSpace(item)
		if !validAddress(item) {
			p.invalid(name, "a comma-separated list of IP addresses and CIDR prefixes", item)
			continue
		}
		list = append(list, item)
	}
	return list
}

func validAddress(s string) bool {
	if strings.Contains(s, "/") {
		_, err := netip.ParsePrefix(s)
		return err == nil
	}
	addr, err := netip.ParseAddr(s)
	return err == nil && addr.Zone() == ""
}
