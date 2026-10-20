//go:build integration

package webhook

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strconv"
	"sync"
	"testing"
	"time"
	"uuid"

	"github.com/zanvd/slumber-mill/src/apiv1"
	"github.com/zanvd/slumber-mill/src/asset"
	"github.com/zanvd/slumber-mill/src/db"
	"github.com/zanvd/slumber-mill/src/egress"
	"github.com/zanvd/slumber-mill/src/logging"
	"github.com/zanvd/slumber-mill/src/secret"
	"github.com/zanvd/slumber-mill/src/storage"
	"github.com/zanvd/slumber-mill/src/tenant"
	"github.com/zanvd/slumber-mill/src/testutil"
	"github.com/zanvd/slumber-mill/src/worker"
)

// testKeyRing seals webhook secrets with a test-only key of fixed bytes.
var testKeyRing = "1:" + base64.StdEncoding.EncodeToString(make([]byte, 32))

// relayStack is a relay on the real asset and tenant services, over a connection pool of its own.
type relayStack struct {
	*stack
	tenants *tenant.Service
	relay   *Relay
}

func newRelayStack(t *testing.T, dsn string) *relayStack {
	t.Helper()
	s := newStack(t, dsn)
	box, err := secret.NewBox(testKeyRing)
	if err != nil {
		t.Fatalf("NewBox() error = %v", err)
	}
	tenants := tenant.NewService(tenant.NewRepository(s.gdb), box, egress.NewHostChecker(true, net.DefaultResolver))
	assets := asset.NewService(asset.NewRepository(s.gdb), storage.NewLocal(t.TempDir()))
	relay, err := NewRelay(s.repo, assets, tenants)
	if err != nil {
		t.Fatalf("NewRelay() error = %v", err)
	}
	t.Cleanup(func() {
		if err := relay.Close(context.Background()); err != nil {
			t.Errorf("Close() error = %v", err)
		}
	})
	return &relayStack{stack: s, tenants: tenants, relay: relay}
}

func (s *relayStack) createTenant(t *testing.T, slug, webhookURL string) (tenant.Tenant, tenant.Credentials) {
	t.Helper()
	tn, creds, err := s.tenants.Create(t.Context(), tenant.CreateParams{Slug: slug, WebhookURL: webhookURL, TargetLUFS: -16})
	if err != nil {
		t.Fatalf("Create(%q) error = %v", slug, err)
	}
	return tn, creds
}

// addEvent records a failed asset of the tenant and its asset.failed event, as the encode pool would,
// and returns the event's and the asset's IDs.
func (s *relayStack) addEvent(t *testing.T, tenantID uint64, title string) (eventID, assetID uuid.UUID) {
	t.Helper()
	eventID, assetID = uuid.NewV7(), uuid.NewV7()
	sum := sha256.Sum256([]byte(title))
	s.exec(t, "INSERT INTO assets (public_id, tenant_id, title, status, attempts, source_sha256, source_bytes, "+
		"source_codec, target_lufs, error) VALUES (?, ?, ?, 'failed', 1, ?, 5, 'mp3', -16, 'not audio')",
		db.ID(assetID), tenantID, title, sum[:])
	s.exec(t, "INSERT INTO asset_events (event_id, tenant_id, asset_id, type) "+
		"SELECT ?, tenant_id, id, 'asset.failed' FROM assets WHERE public_id = ?", db.ID(eventID), db.ID(assetID))
	return eventID, assetID
}

func (s *relayStack) once(t *testing.T, wantHeld bool, wantRelayed int) {
	t.Helper()
	held, relayed, err := s.relay.Once(logging.CtxWithLogger(t.Context(), testutil.Logger(t)))
	if err != nil || held != wantHeld || relayed != wantRelayed {
		t.Errorf("Once() = %t, %d relayed, %v; want %t, %d", held, relayed, err, wantHeld, wantRelayed)
	}
}

// unrelayed returns how many events are waiting for the relay.
func (s *relayStack) unrelayed(t *testing.T) int {
	t.Helper()
	var n int
	if err := s.pool.QueryRowContext(t.Context(),
		"SELECT COUNT(*) FROM asset_events WHERE relayed_at IS NULL").Scan(&n); err != nil {
		t.Fatalf("error counting unrelayed events: %v", err)
	}
	return n
}

// TestRelayingTwiceYieldsOneDelivery relays events, then relays them again as after a crash between queueing
// the deliveries and marking the events relayed: the event keeps its one delivery, built from the asset
// as it was first relayed, and the event of a tenant without a webhook still has none.
func TestRelayingTwiceYieldsOneDelivery(t *testing.T) {
	s := newRelayStack(t, testutil.NewDatabase(t))
	hooked, _ := s.createTenant(t, "hooked", "https://hooks.example.com/slumber")
	plain, _ := s.createTenant(t, "plain", "")
	eventID, assetID := s.addEvent(t, hooked.ID, "Rain")
	s.addEvent(t, plain.ID, "Thunder")

	s.once(t, true, 2)

	ds := s.deliveries(t)
	if len(ds) != 1 || ds[0].EventID != db.ID(eventID) || ds[0].TenantID != hooked.ID || ds[0].State != "queued" ||
		ds[0].Attempts != 0 {
		t.Fatalf("deliveries = %+v, want one queued for event %s of tenant %d", ds, eventID, hooked.ID)
	}
	var body apiv1.Event
	if err := json.Unmarshal(ds[0].Body, &body); err != nil {
		t.Fatalf("error decoding the body %s: %v", ds[0].Body, err)
	}
	if body.ID != eventID.String() || body.Type != "asset.failed" || body.CreatedAt.IsZero() ||
		body.Data.ID != assetID.String() || body.Data.Title != "Rain" || body.Data.Status != "failed" ||
		body.Data.Error == nil || *body.Data.Error != "not audio" {
		t.Errorf("body = %s, want the asset.failed envelope of asset %s", ds[0].Body, assetID)
	}
	if n := s.unrelayed(t); n != 0 {
		t.Errorf("%d events unrelayed, want 0", n)
	}
	before := s.rows(t, "SELECT * FROM webhook_deliveries")

	s.exec(t, "UPDATE asset_events SET relayed_at = NULL")
	s.exec(t, "UPDATE assets SET title = 'Changed'")
	s.once(t, true, 2)

	if after := s.rows(t, "SELECT * FROM webhook_deliveries"); !reflect.DeepEqual(after, before) {
		t.Errorf("deliveries changed from %v\nto %v", before, after)
	}
	if n := s.unrelayed(t); n != 0 {
		t.Errorf("%d events unrelayed, want 0", n)
	}
}

// TestRelayCarriesTraceContext relays an event whose asset's upload Checkly traced and one whose upload it didn't:
// each delivery stores its event's trace context, or NULL, its claim reads it back, and neither body holds it.
func TestRelayCarriesTraceContext(t *testing.T) {
	s := newRelayStack(t, testutil.NewDatabase(t))
	hooked, _ := s.createTenant(t, "hooked", "https://hooks.example.com/slumber")
	other, _ := s.createTenant(t, "other", "")
	traced, _ := s.addEvent(t, hooked.ID, "Rain")
	untraced, _ := s.addEvent(t, hooked.ID, "Thunder")
	s.exec(t, "UPDATE asset_events SET trace_context = ? WHERE event_id = ?", checklyTraceContext, db.ID(traced))

	s.once(t, true, 2)

	for _, want := range []struct {
		eventID uuid.UUID
		stored  string
	}{{traced, checklyTraceContext}, {untraced, ""}} {
		var got sql.NullString
		var body []byte
		if err := s.pool.QueryRowContext(t.Context(),
			"SELECT trace_context, body FROM webhook_deliveries WHERE event_id = ?", db.ID(want.eventID)).
			Scan(&got, &body); err != nil {
			t.Fatalf("error reading the delivery of event %s: %v", want.eventID, err)
		}
		if got.String != want.stored || got.Valid != (want.stored != "") {
			t.Errorf("delivery of event %s stores %+v, want %q, or NULL for empty", want.eventID, got, want.stored)
		}
		if bytes.Contains(body, []byte("checkly")) || bytes.Contains(body, []byte("4bf92f3577b34da6")) {
			t.Errorf("delivery body %s holds the trace context", body)
		}

		l, ok, err := s.repo.claim(t.Context(), "webhook-1", time.Minute)
		if err != nil || !ok {
			t.Fatalf("claim() = %+v, %t, %v; want a lease", l, ok, err)
		}
		if stored, err := s.repo.leasedTraceContext(t.Context(), l); err != nil || stored != want.stored {
			t.Errorf("leasedTraceContext() = %q, %v; want %q", stored, err, want.stored)
		}
		wrongTenant := lease{ID: l.ID, TenantID: other.ID, Attempt: l.Attempt}
		if _, err := s.repo.leasedTraceContext(t.Context(), wrongTenant); !errors.Is(err, errNotFound) {
			t.Errorf("leasedTraceContext() under another tenant: error = %v, want %v", err, errNotFound)
		}
	}
}

// received is a webhook as the test receiver got it, with what checking its signature found.
type received struct {
	header    http.Header
	body      []byte
	verifyErr error
}

// TestDeliveryThroughThePool relays an event and drives its delivery through a webhook pool to a receiver that
// answers with one status and verifies every signature as a tenant would. Each delivery is posted once:
// a 2xx delivers it, a 410 fails it for good, and a 500 requeues it with a backoff, so it isn't due yet.
func TestDeliveryThroughThePool(t *testing.T) {
	tests := []struct {
		status        int
		wantState     string
		wantLastError string
		wantRetry     bool
	}{
		{http.StatusOK, "delivered", "", false},
		{http.StatusGone, "failed", "webhook endpoint answered 410", false},
		{http.StatusInternalServerError, "queued", "webhook endpoint answered 500", true},
	}
	for _, tc := range tests {
		t.Run(strconv.Itoa(tc.status), func(t *testing.T) {
			s := newRelayStack(t, testutil.NewDatabase(t))
			var mu sync.Mutex
			var got []received
			var key []byte
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				body, err := io.ReadAll(r.Body)
				if err != nil {
					t.Errorf("error reading the webhook: %v", err)
				}
				mu.Lock()
				defer mu.Unlock()
				got = append(got, received{
					header:    r.Header.Clone(),
					body:      body,
					verifyErr: Verify(key, r.Header.Get(SignatureHeader), body, time.Now(), 5*time.Minute),
				})
				w.WriteHeader(tc.status)
			}))
			defer srv.Close()
			hooked, creds := s.createTenant(t, "hooked", srv.URL+"/hooks?tenant=hooked")
			mu.Lock()
			key = creds.WebhookSecret.Bytes()
			mu.Unlock()
			eventID, assetID := s.addEvent(t, hooked.ID, "Rain")
			s.once(t, true, 1)

			d, err := NewDeliverer(s.repo, s.tenants, NewClient(true), DelivererConfig{WorkerID: "webhook-1", Lease: time.Minute})
			if err != nil {
				t.Fatalf("NewDeliverer() error = %v", err)
			}
			pool, err := worker.NewPool(d, worker.Config{
				WorkerID: "webhook-1", Pool: "webhook", Concurrency: 2, Lease: time.Minute, PollInterval: time.Second,
				Grace: time.Second, Log: testutil.Logger(t),
			})
			if err != nil {
				t.Fatalf("NewPool() error = %v", err)
			}
			for range 2 {
				if err := pool.RunUntilIdle(t.Context()); err != nil {
					t.Fatalf("RunUntilIdle() error = %v", err)
				}
			}

			mu.Lock()
			defer mu.Unlock()
			if len(got) != 1 {
				t.Fatalf("the receiver got %d webhooks, want 1", len(got))
			}
			h := got[0]
			if h.verifyErr != nil {
				t.Errorf("the signature doesn't verify: %v", h.verifyErr)
			}
			if h.header.Get(EventIDHeader) != eventID.String() || h.header.Get("Content-Type") != "application/json" ||
				h.header.Get("User-Agent") != "slumber-mill-webhooks/1" {
				t.Errorf("headers = %v, want event %s as JSON from slumber-mill-webhooks/1", h.header, eventID)
			}
			ds := s.deliveries(t)
			if len(ds) != 1 || !bytes.Equal(h.body, ds[0].Body) {
				t.Fatalf("the receiver got %s, want the stored body of %+v", h.body, ds)
			}
			var body apiv1.Event
			if err := json.Unmarshal(h.body, &body); err != nil || body.ID != eventID.String() ||
				body.Data.ID != assetID.String() {
				t.Errorf("body = %s (%v), want event %s of asset %s", h.body, err, eventID, assetID)
			}

			var st struct {
				state               string
				attempts            int
				lockedBy, lastError sql.NullString
				leaseExpiresAt      sql.NullTime
				dueInMicroseconds   int64
			}
			err = s.pool.QueryRowContext(t.Context(), "SELECT state, attempts, locked_by, lease_expires_at, last_error, "+
				"TIMESTAMPDIFF(MICROSECOND, NOW(6), run_after) FROM webhook_deliveries").
				Scan(&st.state, &st.attempts, &st.lockedBy, &st.leaseExpiresAt, &st.lastError, &st.dueInMicroseconds)
			if err != nil {
				t.Fatalf("error reading the delivery: %v", err)
			}
			if st.state != tc.wantState || st.attempts != 1 || st.lockedBy.Valid || st.leaseExpiresAt.Valid ||
				st.lastError.String != tc.wantLastError || st.lastError.Valid != (tc.wantLastError != "") {
				t.Errorf("delivery = %+v, want %s after 1 attempt, unleased, with last error %q",
					st, tc.wantState, tc.wantLastError)
			}
			due := time.Duration(st.dueInMicroseconds) * time.Microsecond
			if tc.wantRetry && (due <= 15*time.Second-slack || due > 30*time.Second) {
				t.Errorf("retry due in %v, want 15s to 30s", due)
			}
		})
	}
}

// slack is how long a test may take between a write and reading back a time it set from NOW(6).
const slack = 5 * time.Second

// TestRelayLockFailover runs two relays on pools of their own: only one relays at a time,
// and the other takes over once the holder releases the lock or its session ends.
func TestRelayLockFailover(t *testing.T) {
	dsn := testutil.NewDatabase(t)
	a, b := newRelayStack(t, dsn), newRelayStack(t, dsn)
	hooked, _ := a.createTenant(t, "hooked", "https://hooks.example.com/slumber")

	a.addEvent(t, hooked.ID, "Rain 1")
	a.once(t, true, 1)
	b.addEvent(t, hooked.ID, "Rain 2")
	b.once(t, false, 0)
	if n := b.unrelayed(t); n != 1 {
		t.Errorf("%d events unrelayed while b waits for the lock, want 1", n)
	}

	if err := a.relay.Close(t.Context()); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	b.once(t, true, 1)
	a.once(t, false, 0)

	b.killHolder(t)
	a.addEvent(t, hooked.ID, "Rain 3")
	b.once(t, false, 0)
	if n := a.unrelayed(t); n != 1 {
		t.Errorf("%d events unrelayed after b lost the lock, want 1", n)
	}
	a.waitUntilFree(t)
	a.once(t, true, 1)
	b.once(t, false, 0)

	if ds := a.deliveries(t); len(ds) != 3 {
		t.Errorf("%d deliveries, want 3", len(ds))
	}
}

// TestRelaysOnDifferentDatabasesDontContend runs a relay on each of two databases of the same server: both relay.
func TestRelaysOnDifferentDatabasesDontContend(t *testing.T) {
	a, b := newRelayStack(t, testutil.NewDatabase(t)), newRelayStack(t, testutil.NewDatabase(t))
	for _, s := range []*relayStack{a, b} {
		hooked, _ := s.createTenant(t, "hooked", "https://hooks.example.com/slumber")
		s.addEvent(t, hooked.ID, "Rain")
		s.once(t, true, 1)
		if ds := s.deliveries(t); len(ds) != 1 {
			t.Errorf("%d deliveries, want 1", len(ds))
		}
	}
}
