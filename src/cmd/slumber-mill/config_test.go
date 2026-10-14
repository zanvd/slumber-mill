package main

import (
	"reflect"
	"strings"
	"testing"
	"time"
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
				DrainDelay:          0,
				ShutdownGrace:       90 * time.Second,
			},
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
