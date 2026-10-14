//go:build integration

package webhook

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"net"
	"reflect"
	"testing"
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
