package main

import (
	"reflect"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/zanvd/slumber-mill/src/secret"
	"github.com/zanvd/slumber-mill/src/tracing"
)

func envFrom(m map[string]string) func(string) string {
	return func(name string) string { return m[name] }
}

func TestLoadAPIConfig(t *testing.T) {
	defaults := apiConfig{
		DSN:                 "slumber_mill:slumber_mill@tcp(mysql:3306)/slumber_mill",
		HTTPAddr:            ":8080",
		DataDir:             "/data",
		MaxUploadBytes:      2147483648,
		MaxUploadsPerTenant: 4,
		EncodeLease:         2 * time.Minute,
		DrainDelay:          10 * time.Second,
		ShutdownGrace:       30 * time.Second,
	}
	tests := []struct {
		name    string
		env     map[string]string
		want    apiConfig
		wantErr []string
	}{
		{name: "defaults", env: nil, want: defaults},
		{
			name: "empty and blank values are unset",
			env: map[string]string{
				envDSN: "", envHTTPAddr: "  ", envMaxUploadBytes: "", envTrustedProxies: " ", envDrainDelay: "",
			},
			want: defaults,
		},
		{
			name: "every variable set",
			env: map[string]string{
				envDSN:                 "u:p@tcp(db:3306)/app",
				envHTTPAddr:            "127.0.0.1:9000",
				envDataDir:             "/srv/data",
				envMaxUploadBytes:      "1048576",
				envMaxUploadsPerTenant: "2",
				envTrustedProxies:      "10.0.0.0/8, 192.168.1.1,fd00::/8 ,2001:db8::1",
				envEncodeLease:         "5m",
				envDrainDelay:          "0s",
				envShutdownGrace:       "1m30s",
			},
			want: apiConfig{
				DSN:                 "u:p@tcp(db:3306)/app",
				HTTPAddr:            "127.0.0.1:9000",
				DataDir:             "/srv/data",
				MaxUploadBytes:      1 << 20,
				MaxUploadsPerTenant: 2,
				TrustedProxies:      []string{"10.0.0.0/8", "192.168.1.1", "fd00::/8", "2001:db8::1"},
				EncodeLease:         5 * time.Minute,
				DrainDelay:          0,
				ShutdownGrace:       90 * time.Second,
			},
		},
		{
			name: "tracing on",
			env: map[string]string{
				tracing.EnvEndpoint: "https://otel.eu-west-1.checklyhq.com", tracing.EnvServiceName: "slumber-mill-api",
			},
			want: func() apiConfig {
				c := defaults
				c.Tracing = tracing.Config{
					Endpoint: "https://otel.eu-west-1.checklyhq.com", Headers: map[string]secret.Value{},
					ServiceName: "slumber-mill-api",
				}
				return c
			}(),
		},
		{
			name: "tracing off without an endpoint",
			env:  map[string]string{tracing.EnvHeaders: "authorization=", tracing.EnvServiceName: "slumber-mill-api"},
			want: defaults,
		},
		{
			name:    "tracing with another protocol",
			env:     map[string]string{tracing.EnvEndpoint: "https://otel.test", tracing.EnvProtocol: "grpc"},
			wantErr: []string{`OTEL_EXPORTER_OTLP_PROTOCOL must be http/protobuf, got "grpc"`},
		},
		{
			name:    "encode lease below the minimum",
			env:     map[string]string{envEncodeLease: "59s"},
			wantErr: []string{`SLUMBER_MILL_ENCODE_LEASE must be a duration of at least 1m0s, such as 2m0s, got "59s"`},
		},
		{
			name: "encode lease at the minimum",
			env:  map[string]string{envEncodeLease: "1m"},
			want: func() apiConfig {
				c := defaults
				c.EncodeLease = time.Minute
				return c
			}(),
		},
		{
			name:    "upload limit not a number",
			env:     map[string]string{envMaxUploadBytes: "2GiB"},
			wantErr: []string{`SLUMBER_MILL_MAX_UPLOAD_BYTES must be a positive integer, got "2GiB"`},
		},
		{
			name:    "upload limit zero",
			env:     map[string]string{envMaxUploadBytes: "0"},
			wantErr: []string{`SLUMBER_MILL_MAX_UPLOAD_BYTES must be a positive integer, got "0"`},
		},
		{
			name:    "upload limit overflows",
			env:     map[string]string{envMaxUploadBytes: "9223372036854775808"},
			wantErr: []string{"SLUMBER_MILL_MAX_UPLOAD_BYTES must be a positive integer"},
		},
		{
			name:    "uploads per tenant negative",
			env:     map[string]string{envMaxUploadsPerTenant: "-1"},
			wantErr: []string{`SLUMBER_MILL_MAX_UPLOADS_PER_TENANT must be a positive integer, got "-1"`},
		},
		{
			name:    "drain delay without a unit",
			env:     map[string]string{envDrainDelay: "10"},
			wantErr: []string{`SLUMBER_MILL_DRAIN_DELAY must be a duration of zero or more, such as 10s, got "10"`},
		},
		{
			name:    "drain delay negative",
			env:     map[string]string{envDrainDelay: "-1s"},
			wantErr: []string{`SLUMBER_MILL_DRAIN_DELAY must be a duration of zero or more, such as 10s, got "-1s"`},
		},
		{
			name:    "shutdown grace zero",
			env:     map[string]string{envShutdownGrace: "0s"},
			wantErr: []string{`SLUMBER_MILL_SHUTDOWN_GRACE must be a positive duration, such as 30s, got "0s"`},
		},
		{
			name:    "trusted proxy is a name",
			env:     map[string]string{envTrustedProxies: "10.0.0.1,lb.internal"},
			wantErr: []string{`SLUMBER_MILL_TRUSTED_PROXIES must be a comma-separated list of IP addresses and CIDR prefixes, got "lb.internal"`},
		},
		{
			name:    "trusted proxy with a bad prefix length",
			env:     map[string]string{envTrustedProxies: "10.0.0.0/33"},
			wantErr: []string{`got "10.0.0.0/33"`},
		},
		{
			name:    "trusted proxy with a zone",
			env:     map[string]string{envTrustedProxies: "fe80::1%eth0"},
			wantErr: []string{`got "fe80::1%eth0"`},
		},
		{
			name:    "empty trusted proxy entry",
			env:     map[string]string{envTrustedProxies: "10.0.0.1,,10.0.0.2"},
			wantErr: []string{`SLUMBER_MILL_TRUSTED_PROXIES must be a comma-separated list of IP addresses and CIDR prefixes, got ""`},
		},
		{
			name: "every invalid variable is reported",
			env: map[string]string{
				envMaxUploadBytes: "x", envMaxUploadsPerTenant: "y", envDrainDelay: "z", envShutdownGrace: "w",
			},
			wantErr: []string{
				envMaxUploadBytes, envMaxUploadsPerTenant, envDrainDelay, envShutdownGrace,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := loadAPIConfig(envFrom(tt.env))
			if tt.wantErr != nil {
				if err == nil {
					t.Fatalf("loadAPIConfig() error = nil, want one containing %q", tt.wantErr)
				}
				for _, want := range tt.wantErr {
					if !strings.Contains(err.Error(), want) {
						t.Errorf("loadAPIConfig() error = %q, want it to contain %q", err, want)
					}
				}
				return
			}
			if err != nil {
				t.Fatalf("loadAPIConfig() error = %v", err)
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("loadAPIConfig() = %+v, want %+v", got, tt.want)
			}
		})
	}
}

func TestLoadEncodeConfig(t *testing.T) {
	defaults := encodeConfig{
		DSN:           "slumber_mill:slumber_mill@tcp(mysql:3306)/slumber_mill",
		DataDir:       "/data",
		Concurrency:   runtime.GOMAXPROCS(0),
		Lease:         2 * time.Minute,
		ShutdownGrace: 30 * time.Second,
	}
	tests := []struct {
		name    string
		env     map[string]string
		want    encodeConfig
		wantErr []string
	}{
		{name: "defaults", want: defaults},
		{
			name: "blank values are unset",
			env:  map[string]string{envEncodeConcurrency: " ", envEncodeLease: "", envShutdownGrace: "  "},
			want: defaults,
		},
		{
			name: "every variable set",
			env: map[string]string{
				envDSN:               "u:p@tcp(db:3306)/app",
				envDataDir:           "/srv/data",
				envEncodeConcurrency: "3",
				envEncodeLease:       "10m",
				envShutdownGrace:     "45s",
				// Read by the other commands only.
				envWebhookConcurrency: "x",
				envSecretKeys:         "x",
				envDrainDelay:         "x",
			},
			want: encodeConfig{
				DSN:           "u:p@tcp(db:3306)/app",
				DataDir:       "/srv/data",
				Concurrency:   3,
				Lease:         10 * time.Minute,
				ShutdownGrace: 45 * time.Second,
			},
		},
		{
			name:    "concurrency zero",
			env:     map[string]string{envEncodeConcurrency: "0"},
			wantErr: []string{`SLUMBER_MILL_ENCODE_CONCURRENCY must be a positive integer, got "0"`},
		},
		{
			name:    "lease without a unit",
			env:     map[string]string{envEncodeLease: "120"},
			wantErr: []string{`SLUMBER_MILL_ENCODE_LEASE must be a duration of at least 1m0s, such as 2m0s, got "120"`},
		},
		{
			name:    "lease negative",
			env:     map[string]string{envEncodeLease: "-2m"},
			wantErr: []string{`SLUMBER_MILL_ENCODE_LEASE must be a duration of at least 1m0s, such as 2m0s, got "-2m"`},
		},
		{
			name:    "lease below the minimum",
			env:     map[string]string{envEncodeLease: "59s"},
			wantErr: []string{`SLUMBER_MILL_ENCODE_LEASE must be a duration of at least 1m0s, such as 2m0s, got "59s"`},
		},
		{
			name: "lease at the minimum",
			env:  map[string]string{envEncodeLease: "1m"},
			want: func() encodeConfig {
				c := defaults
				c.Lease = time.Minute
				return c
			}(),
		},
		{
			name:    "every invalid variable is reported",
			env:     map[string]string{envEncodeConcurrency: "all", envEncodeLease: "long", envShutdownGrace: "0"},
			wantErr: []string{envEncodeConcurrency, envEncodeLease, envShutdownGrace},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := loadEncodeConfig(envFrom(tt.env))
			if tt.wantErr != nil {
				if err == nil {
					t.Fatalf("loadEncodeConfig() error = nil, want one containing %q", tt.wantErr)
				}
				for _, want := range tt.wantErr {
					if !strings.Contains(err.Error(), want) {
						t.Errorf("loadEncodeConfig() error = %q, want it to contain %q", err, want)
					}
				}
				return
			}
			if err != nil {
				t.Fatalf("loadEncodeConfig() error = %v", err)
			}
			if got != tt.want {
				t.Errorf("loadEncodeConfig() = %+v, want %+v", got, tt.want)
			}
		})
	}
}

func TestLoadWebhookConfig(t *testing.T) {
	// Parsed later, by secret.NewBox.
	const keys = "1:not-parsed-here"
	type plain struct {
		DSN, DataDir        string
		Concurrency         int
		EncodeLease         time.Duration
		ShutdownGrace       time.Duration
		WebhookAllowPrivate bool
	}
	defaults := plain{
		DSN:           "slumber_mill:slumber_mill@tcp(mysql:3306)/slumber_mill",
		DataDir:       "/data",
		Concurrency:   8,
		EncodeLease:   2 * time.Minute,
		ShutdownGrace: 30 * time.Second,
	}
	tests := []struct {
		name    string
		env     map[string]string
		want    plain
		wantErr []string
	}{
		{name: "defaults", env: map[string]string{envSecretKeys: keys}, want: defaults},
		{
			name: "every variable set",
			env: map[string]string{
				envSecretKeys:          keys,
				envDSN:                 "u:p@tcp(db:3306)/app",
				envDataDir:             "/srv/data",
				envWebhookConcurrency:  "32",
				envEncodeLease:         "90s",
				envShutdownGrace:       "1m",
				envWebhookAllowPrivate: "true",
				// Read by the other commands only.
				envEncodeConcurrency: "x",
				envDrainDelay:        "x",
			},
			want: plain{
				DSN:                 "u:p@tcp(db:3306)/app",
				DataDir:             "/srv/data",
				Concurrency:         32,
				EncodeLease:         90 * time.Second,
				ShutdownGrace:       time.Minute,
				WebhookAllowPrivate: true,
			},
		},
		{
			name:    "key ring missing",
			env:     map[string]string{},
			wantErr: []string{"SLUMBER_MILL_SECRET_KEYS is required"},
		},
		{
			name:    "concurrency negative",
			env:     map[string]string{envSecretKeys: keys, envWebhookConcurrency: "-8"},
			wantErr: []string{`SLUMBER_MILL_WEBHOOK_CONCURRENCY must be a positive integer, got "-8"`},
		},
		{
			name:    "encode lease below the minimum",
			env:     map[string]string{envSecretKeys: keys, envEncodeLease: "59s"},
			wantErr: []string{`SLUMBER_MILL_ENCODE_LEASE must be a duration of at least 1m0s, such as 2m0s, got "59s"`},
		},
		{
			name: "encode lease at the minimum",
			env:  map[string]string{envSecretKeys: keys, envEncodeLease: "1m"},
			want: func() plain {
				c := defaults
				c.EncodeLease = time.Minute
				return c
			}(),
		},
		{
			name: "every invalid variable is reported",
			env: map[string]string{
				envWebhookConcurrency: "many", envEncodeLease: "x", envShutdownGrace: "y", envWebhookAllowPrivate: "z",
			},
			wantErr: []string{
				envWebhookConcurrency, envEncodeLease, envShutdownGrace, envWebhookAllowPrivate, envSecretKeys,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := loadWebhookConfig(envFrom(tt.env))
			if tt.wantErr != nil {
				if err == nil {
					t.Fatalf("loadWebhookConfig() error = nil, want one containing %q", tt.wantErr)
				}
				for _, want := range tt.wantErr {
					if !strings.Contains(err.Error(), want) {
						t.Errorf("loadWebhookConfig() error = %q, want it to contain %q", err, want)
					}
				}
				return
			}
			if err != nil {
				t.Fatalf("loadWebhookConfig() error = %v", err)
			}
			gotPlain := plain{
				DSN: got.DSN, DataDir: got.DataDir, Concurrency: got.Concurrency, EncodeLease: got.EncodeLease,
				ShutdownGrace: got.ShutdownGrace, WebhookAllowPrivate: got.WebhookAllowPrivate,
			}
			if gotPlain != tt.want {
				t.Errorf("loadWebhookConfig() = %+v, want %+v", gotPlain, tt.want)
			}
			if string(got.SecretKeys.Bytes()) != keys {
				t.Error("loadWebhookConfig().SecretKeys doesn't hold the key ring")
			}
		})
	}
}

func TestLoadMigrateConfig(t *testing.T) {
	tests := []struct {
		name string
		env  map[string]string
		want string
	}{
		{name: "default", want: "slumber_mill:slumber_mill@tcp(mysql:3306)/slumber_mill"},
		{name: "set", env: map[string]string{envDSN: "u:p@tcp(db:3306)/app"}, want: "u:p@tcp(db:3306)/app"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := loadMigrateConfig(envFrom(tt.env))
			if err != nil {
				t.Fatalf("loadMigrateConfig() error = %v", err)
			}
			if got.DSN != tt.want {
				t.Errorf("loadMigrateConfig().DSN = %q, want %q", got.DSN, tt.want)
			}
		})
	}
}

func TestLoadTenantCreateConfig(t *testing.T) {
	// Parsed later, by secret.NewBox.
	const keys = "1:not-parsed-here"
	tests := []struct {
		name             string
		env              map[string]string
		wantDSN          string
		wantAllowPrivate bool
		wantErr          string
	}{
		{
			name:    "defaults",
			env:     map[string]string{envSecretKeys: keys},
			wantDSN: "slumber_mill:slumber_mill@tcp(mysql:3306)/slumber_mill",
		},
		{
			name:             "every variable set",
			env:              map[string]string{envSecretKeys: keys, envDSN: "u:p@tcp(db)/app", envWebhookAllowPrivate: "true"},
			wantDSN:          "u:p@tcp(db)/app",
			wantAllowPrivate: true,
		},
		{
			name:    "key ring missing",
			env:     map[string]string{},
			wantErr: "SLUMBER_MILL_SECRET_KEYS is required",
		},
		{
			name:    "allow private not a boolean",
			env:     map[string]string{envSecretKeys: keys, envWebhookAllowPrivate: "yes"},
			wantErr: `SLUMBER_MILL_WEBHOOK_ALLOW_PRIVATE must be true or false, got "yes"`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := loadTenantCreateConfig(envFrom(tt.env))
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("loadTenantCreateConfig() error = %v, want one containing %q", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("loadTenantCreateConfig() error = %v", err)
			}
			if got.DSN != tt.wantDSN || got.WebhookAllowPrivate != tt.wantAllowPrivate {
				t.Errorf("loadTenantCreateConfig() = {DSN: %q, WebhookAllowPrivate: %t}, want {%q, %t}",
					got.DSN, got.WebhookAllowPrivate, tt.wantDSN, tt.wantAllowPrivate)
			}
			if string(got.SecretKeys.Bytes()) != keys {
				t.Error("loadTenantCreateConfig().SecretKeys doesn't hold the key ring")
			}
		})
	}
}
