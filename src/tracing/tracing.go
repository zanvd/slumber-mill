// Package tracing exports the spans of traces that a Checkly check started, over OTLP/HTTP.
package tracing

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"unicode"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"

	"github.com/zanvd/slumber-mill/src/secret"
)

const (
	EnvEndpoint    = "OTEL_EXPORTER_OTLP_ENDPOINT"
	EnvHeaders     = "OTEL_EXPORTER_OTLP_HEADERS"
	EnvProtocol    = "OTEL_EXPORTER_OTLP_PROTOCOL"
	EnvServiceName = "OTEL_SERVICE_NAME"

	protocolHTTPProtobuf = "http/protobuf"
	tracesPath           = "v1/traces"
	defaultServiceName   = "slumber-mill"

	// checklyKey and checklyValue are the tracestate member a Checkly check sends with its traceparent.
	checklyKey   = "checkly"
	checklyValue = "true"
)

// Config is the exporter's configuration. An empty Endpoint turns tracing off.
type Config struct {
	// Endpoint is the OTLP/HTTP base URL; spans go to its /v1/traces.
	Endpoint string
	// Headers go with every export; their values are secrets, such as an API key.
	Headers     map[string]secret.Value
	ServiceName string
}

// unsupportedEnv lists the variables otlptracehttp v1.47.0 reads by itself, in internal/otlpconfig/envconfig.go
// and internal/x, other than the ones LoadConfig supports. Check it again whenever the OpenTelemetry version changes.
func unsupportedEnv() []string {
	return []string{
		"OTEL_EXPORTER_OTLP_TRACES_ENDPOINT",
		"OTEL_EXPORTER_OTLP_TRACES_HEADERS",
		"OTEL_EXPORTER_OTLP_TRACES_PROTOCOL",
		"OTEL_EXPORTER_OTLP_TIMEOUT",
		"OTEL_EXPORTER_OTLP_TRACES_TIMEOUT",
		"OTEL_EXPORTER_OTLP_COMPRESSION",
		"OTEL_EXPORTER_OTLP_TRACES_COMPRESSION",
		"OTEL_EXPORTER_OTLP_CERTIFICATE",
		"OTEL_EXPORTER_OTLP_TRACES_CERTIFICATE",
		"OTEL_EXPORTER_OTLP_CLIENT_CERTIFICATE",
		"OTEL_EXPORTER_OTLP_CLIENT_KEY",
		"OTEL_EXPORTER_OTLP_TRACES_CLIENT_CERTIFICATE",
		"OTEL_EXPORTER_OTLP_TRACES_CLIENT_KEY",
		"OTEL_EXPORTER_OTLP_INSECURE",
		"OTEL_EXPORTER_OTLP_TRACES_INSECURE",
		"OTEL_GO_X_OBSERVABILITY",
	}
}

// LoadConfig reads the standard OpenTelemetry variables. Only the http/protobuf protocol is supported.
// Without an endpoint the other supported variables are ignored, so tracing is off.
// Any other variable the exporter would read is refused, with tracing on or off: the exporter logs the values it
// can't parse as plain text, and with tracing off a setting such as a per-signal endpoint would silently do nothing.
// Errors never quote a header's value or a refused variable's.
func LoadConfig(getenv func(string) string) (Config, error) {
	var errs []error
	for _, name := range unsupportedEnv() {
		if strings.TrimSpace(getenv(name)) != "" {
			errs = append(errs, fmt.Errorf("%s isn't supported, so it must be unset", name))
		}
	}
	endpoint := strings.TrimSpace(getenv(EnvEndpoint))
	if endpoint == "" {
		return Config{}, errors.Join(errs...)
	}
	if err := checkEndpoint(endpoint); err != nil {
		// Not quoted, since a malformed value may hold credentials.
		errs = append(errs, fmt.Errorf("%s must be an http or https URL without credentials, a query or a fragment",
			EnvEndpoint))
	}
	if p := strings.TrimSpace(getenv(EnvProtocol)); p != "" && p != protocolHTTPProtobuf {
		errs = append(errs, fmt.Errorf("%s must be %s, got %q", EnvProtocol, protocolHTTPProtobuf, p))
	}
	headers, err := parseHeaders(getenv(EnvHeaders))
	if err != nil {
		errs = append(errs, fmt.Errorf("%s: %w", EnvHeaders, err))
	}
	name := strings.TrimSpace(getenv(EnvServiceName))
	if name == "" {
		name = defaultServiceName
	}
	return Config{Endpoint: endpoint, Headers: headers, ServiceName: name}, errors.Join(errs...)
}

// checkEndpoint refuses at least what the exporter's own parse of the variable would log: a value url.Parse rejects.
func checkEndpoint(endpoint string) error {
	u, err := url.Parse(endpoint)
	if err != nil {
		return errors.New("invalid endpoint")
	}
	if (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" || u.User != nil ||
		u.RawQuery != "" || u.Fragment != "" {
		return errors.New("invalid endpoint")
	}
	return nil
}

// parseHeaders reads comma-separated key=value pairs whose values may be percent-encoded.
//
// otlptracehttp.New parses the same variable again with its own parser (internal/envconfig.stringToHeader),
// which writes whatever it rejects to stderr as plain text, a header's value included. The two must agree:
// this one rejects everything that one would, namely an empty member, a member without '=',
// a name that isn't an HTTP token and a value that isn't validly percent-encoded.
func parseHeaders(s string) (map[string]secret.Value, error) {
	headers := map[string]secret.Value{}
	// Like the exporter, which doesn't parse a blank variable.
	if strings.TrimSpace(s) == "" {
		return headers, nil
	}
	for i, member := range strings.Split(s, ",") {
		key, value, ok := strings.Cut(member, "=")
		key = strings.TrimSpace(key)
		if !ok || !validHeaderName(key) {
			return nil, fmt.Errorf("pair %d must be a header name, '=' and a value", i+1)
		}
		decoded, err := url.PathUnescape(value)
		if err != nil {
			return nil, fmt.Errorf("the value of %s isn't validly percent-encoded", key)
		}
		decoded = strings.TrimSpace(decoded)
		if decoded == "" {
			return nil, fmt.Errorf("the value of %s is empty", key)
		}
		headers[key] = secret.NewValue([]byte(decoded))
	}
	return headers, nil
}

// validHeaderName reports whether s is an HTTP token, as the exporter's parser requires.
func validHeaderName(s string) bool {
	if s == "" {
		return false
	}
	for _, c := range s {
		if c > unicode.MaxASCII || !(unicode.IsLetter(c) || unicode.IsDigit(c) || strings.ContainsRune("!#$%&'*+-.^_`|~", c)) {
			return false
		}
	}
	return true
}

// Provider is a TracerProvider that Shutdown flushes.
type Provider struct {
	trace.TracerProvider
	sdk *sdktrace.TracerProvider
}

// Setup builds the provider for cfg. Without an endpoint it is a no-op that exports and dials nothing.
func Setup(ctx context.Context, cfg Config) (*Provider, error) {
	if cfg.Endpoint == "" {
		return &Provider{TracerProvider: noop.NewTracerProvider()}, nil
	}
	if err := checkEndpoint(cfg.Endpoint); err != nil {
		return nil, fmt.Errorf("error setting up tracing: %w", err)
	}
	target, err := url.JoinPath(cfg.Endpoint, tracesPath)
	if err != nil {
		return nil, fmt.Errorf("error setting up tracing: %w", err)
	}
	headers := make(map[string]string, len(cfg.Headers))
	for k, v := range cfg.Headers {
		headers[k] = string(v.Bytes())
	}
	exporter, err := otlptracehttp.New(ctx,
		otlptracehttp.WithEndpointURL(target),
		otlptracehttp.WithHeaders(headers),
	)
	if err != nil {
		return nil, fmt.Errorf("error creating the trace exporter: %w", err)
	}
	name := cfg.ServiceName
	if name == "" {
		name = defaultServiceName
	}
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithSampler(Sampler()),
		sdktrace.WithResource(resource.NewSchemaless(attribute.String("service.name", name))),
	)
	return &Provider{TracerProvider: tp, sdk: tp}, nil
}

// Enabled reports whether the provider exports spans.
func (p *Provider) Enabled() bool {
	return p.sdk != nil
}

// Shutdown exports the spans still queued and stops the exporter, giving up when ctx is done.
func (p *Provider) Shutdown(ctx context.Context) error {
	if p.sdk == nil {
		return nil
	}
	return p.sdk.Shutdown(ctx)
}

// Sampler never samples a root span, so a trace starts only from a caller's traceparent.
// A remote parent is followed only when it is sampled and its tracestate says Checkly started the trace;
// a local parent is followed as it is.
func Sampler() sdktrace.Sampler {
	return sdktrace.ParentBased(sdktrace.NeverSample(),
		sdktrace.WithRemoteParentSampled(checklySampler{}),
		sdktrace.WithRemoteParentNotSampled(sdktrace.NeverSample()),
	)
}

type checklySampler struct{}

func (checklySampler) ShouldSample(p sdktrace.SamplingParameters) sdktrace.SamplingResult {
	ts := trace.SpanContextFromContext(p.ParentContext).TraceState()
	decision := sdktrace.Drop
	if ts.Get(checklyKey) == checklyValue {
		decision = sdktrace.RecordAndSample
	}
	return sdktrace.SamplingResult{Decision: decision, Tracestate: ts}
}

func (checklySampler) Description() string {
	return "ChecklyStarted"
}
