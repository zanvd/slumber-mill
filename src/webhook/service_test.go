package webhook

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"testing"
	"testing/synctest"
	"time"
	"uuid"

	"github.com/zanvd/slumber-mill/src/apiv1"
	"github.com/zanvd/slumber-mill/src/asset"
	"github.com/zanvd/slumber-mill/src/logging"
	"github.com/zanvd/slumber-mill/src/secret"
	"github.com/zanvd/slumber-mill/src/tenant"
)

// hookTenant has a webhook, plainTenant has none, and goneTenant doesn't exist.
const (
	hookTenant  = 7
	plainTenant = 8
	goneTenant  = 9

	hookURL = "https://hooks.example.com/slumber?tenant=7"
)

var (
	eventTime  = time.Date(2026, 3, 1, 12, 5, 30, 250000000, time.UTC)
	hookSecret = []byte("whsec_unit-test-secret-not-a-real-key")
)

func testEvent(tenantID uint64, n int) asset.Event {
	return asset.Event{
		ID:        uuid.NewV7(),
		TenantID:  tenantID,
		Type:      asset.EventReady,
		CreatedAt: eventTime,
		Asset: asset.Asset{
			ID:          uuid.NewV7(),
			TenantID:    tenantID,
			Title:       fmt.Sprintf("Rain %d", n),
			Status:      asset.StatusReady,
			SourceBytes: 1000 + uint64(n),
			TargetLUFS:  -16,
			CreatedAt:   eventTime.Add(-time.Minute),
			UpdatedAt:   eventTime,
		},
	}
}

func testEvents(tenantID uint64, n int) []asset.Event {
	events := make([]asset.Event, n)
	for i := range events {
		events[i] = testEvent(tenantID, i)
	}
	return events
}

func eventIDs(events ...asset.Event) []uuid.UUID {
	ids := make([]uuid.UUID, len(events))
	for i, e := range events {
		ids[i] = e.ID
	}
	return ids
}

// logBuffer collects JSON logs, so tests can read them back.
type logBuffer struct {
	bytes.Buffer
}

func (b *logBuffer) ctx(ctx context.Context) context.Context {
	return logging.CtxWithLogger(ctx, slog.New(slog.NewJSONHandler(b, nil)))
}

// messages returns the level and message of each entry, as "LEVEL message".
func (b *logBuffer) messages(t *testing.T) []string {
	t.Helper()
	var out []string
	sc := bufio.NewScanner(bytes.NewReader(b.Bytes()))
	for sc.Scan() {
		var entry struct{ Level, Msg string }
		if err := json.Unmarshal(sc.Bytes(), &entry); err != nil {
			t.Fatalf("error decoding log entry %s: %v", sc.Text(), err)
		}
		out = append(out, entry.Level+" "+entry.Msg)
	}
	return out
}

type relayFixture struct {
	relay   *Relay
	repo    *fakeRelayRepo
	events  *fakeEvents
	tenants *fakeTenants
	logs    *logBuffer
}

func newRelayFixture(t *testing.T, events ...asset.Event) *relayFixture {
	t.Helper()
	start := time.Now()
	f := &relayFixture{
		repo:   &fakeRelayRepo{start: start},
		events: &fakeEvents{start: start, events: events},
		tenants: &fakeTenants{
			targets: map[uint64]tenant.Webhook{hookTenant: {URL: hookURL, Secret: secret.NewValue(hookSecret)}},
			errs:    map[uint64]error{goneTenant: tenant.ErrNotFound},
		},
		logs: &logBuffer{},
	}
	r, err := NewRelay(f.repo, f.events, f.tenants)
	if err != nil {
		t.Fatalf("NewRelay() error = %v", err)
	}
	f.relay = r
	return f
}

// once runs Relay.Once and checks that it reports holding the lock as wantHeld.
func (f *relayFixture) once(t *testing.T, wantHeld bool) (int, error) {
	t.Helper()
	held, relayed, err := f.relay.Once(f.logs.ctx(t.Context()))
	if held != wantHeld {
		t.Errorf("Once() held = %t, want %t", held, wantHeld)
	}
	return relayed, err
}

// checkDeliveries checks that every delivery inserted carries its event's envelope as the relay found the event.
func (f *relayFixture) checkDeliveries(t *testing.T) {
	t.Helper()
	byID := make(map[uuid.UUID]asset.Event)
	for _, e := range f.events.events {
		byID[e.ID] = e
	}
	for _, ds := range f.repo.inserts {
		for _, d := range ds {
			e := byID[d.EventID]
			want, err := json.Marshal(apiv1.NewEvent(e))
			if err != nil {
				t.Fatal(err)
			}
			if d.TenantID != e.TenantID || !bytes.Equal(d.Body, want) {
				t.Errorf("delivery %s = tenant %d, body %s\nwant tenant %d, body %s",
					d.EventID, d.TenantID, d.Body, e.TenantID, want)
			}
		}
	}
}

func TestNewRelay(t *testing.T) {
	repo, events, tenants := &fakeRelayRepo{}, &fakeEvents{}, &fakeTenants{}
	tests := []struct {
		name    string
		repo    relayRepo
		events  eventSource
		tenants webhookTargets
	}{
		{"no repository", nil, events, tenants},
		{"no event source", repo, nil, tenants},
		{"no tenant service", repo, events, nil},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if r, err := NewRelay(tc.repo, tc.events, tc.tenants); err == nil {
				t.Errorf("NewRelay() = %+v, want an error", r)
			}
		})
	}
}

func TestRelayOnce(t *testing.T) {
	hook1, plain, hook2, gone := testEvent(hookTenant, 1), testEvent(plainTenant, 2), testEvent(hookTenant, 3),
		testEvent(goneTenant, 4)
	all := []asset.Event{hook1, plain, hook2, gone}
	everyTenant := []uint64{hookTenant, plainTenant, goneTenant}
	tests := []struct {
		name         string
		setup        func(f *relayFixture)
		wantRelayed  int
		wantErr      error
		wantInserted []uuid.UUID
		wantMarked   []uuid.UUID
		wantLookups  []uint64
		wantLogs     []string
	}{
		{
			name:         "only tenants with a webhook get deliveries",
			wantRelayed:  4,
			wantInserted: eventIDs(hook1, hook2),
			wantMarked:   eventIDs(all...),
			wantLookups:  everyTenant,
			wantLogs:     []string{"INFO took the relay lock", "WARN relaying events of an unknown tenant without delivering them"},
		},
		{
			name:         "a lookup failing for another reason queues the delivery",
			setup:        func(f *relayFixture) { f.tenants.errs[plainTenant] = errBoom },
			wantRelayed:  4,
			wantInserted: eventIDs(hook1, plain, hook2),
			wantMarked:   eventIDs(all...),
			wantLookups:  everyTenant,
			wantLogs: []string{
				"INFO took the relay lock",
				"WARN error looking up a tenant's webhook, queueing its deliveries anyway",
				"WARN relaying events of an unknown tenant without delivering them",
			},
		},
		{
			name: "no key ring",
			setup: func(f *relayFixture) {
				f.tenants.errs[plainTenant] = fmt.Errorf("error reading webhook target: %w", tenant.ErrNoKeyRing)
			},
			wantErr:     tenant.ErrNoKeyRing,
			wantLookups: []uint64{hookTenant, plainTenant},
			wantLogs:    []string{"INFO took the relay lock"},
		},
		{
			name:     "reading events fails",
			setup:    func(f *relayFixture) { f.events.readErr = errBoom },
			wantErr:  errBoom,
			wantLogs: []string{"INFO took the relay lock"},
		},
		{
			name:        "inserting fails",
			setup:       func(f *relayFixture) { f.repo.insertErr = errBoom },
			wantErr:     errBoom,
			wantLookups: everyTenant,
		},
		{
			name:         "marking fails",
			setup:        func(f *relayFixture) { f.events.markErr = errBoom },
			wantErr:      errBoom,
			wantInserted: eventIDs(hook1, hook2),
			wantLookups:  everyTenant,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			f := newRelayFixture(t, all...)
			if tc.setup != nil {
				tc.setup(f)
			}

			relayed, err := f.once(t, true)

			if relayed != tc.wantRelayed || !errors.Is(err, tc.wantErr) || (err == nil) != (tc.wantErr == nil) {
				t.Errorf("Once() = %d relayed, %v; want %d, %v", relayed, err, tc.wantRelayed, tc.wantErr)
			}
			if got := f.repo.inserted(); !slices.Equal(got, tc.wantInserted) {
				t.Errorf("deliveries for %v, want %v", got, tc.wantInserted)
			}
			if got := f.events.relayedIDs(); !slices.Equal(got, tc.wantMarked) {
				t.Errorf("relayed %v, want %v", got, tc.wantMarked)
			}
			if !slices.Equal(f.tenants.lookups, tc.wantLookups) {
				t.Errorf("looked up tenants %v, want %v", f.tenants.lookups, tc.wantLookups)
			}
			if tc.wantLogs != nil {
				if got := f.logs.messages(t); !slices.Equal(got, tc.wantLogs) {
					t.Errorf("logs = %q, want %q", got, tc.wantLogs)
				}
			}
			f.checkDeliveries(t)
		})
	}
}

// TestRelayDrainsUntilShort reads full batches back to back, checking the lock before each, and stops at the first
// short one, an empty one included.
func TestRelayDrainsUntilShort(t *testing.T) {
	for _, n := range []int{0, 1, 99, 100, 101, 200, 250} {
		t.Run(fmt.Sprintf("%d events", n), func(t *testing.T) {
			events := testEvents(hookTenant, n)
			f := newRelayFixture(t, events...)

			relayed, err := f.once(t, true)

			if err != nil || relayed != n {
				t.Errorf("Once() = %d relayed, %v; want %d", relayed, err, n)
			}
			var wantSizes, wantLimits []int
			for rest := n; ; rest -= asset.MaxEventBatch {
				wantSizes = append(wantSizes, min(rest, asset.MaxEventBatch))
				wantLimits = append(wantLimits, asset.MaxEventBatch)
				if rest < asset.MaxEventBatch {
					break
				}
			}
			if !slices.Equal(f.events.sizes, wantSizes) || !slices.Equal(f.events.limits, wantLimits) {
				t.Errorf("read batches of %v with limits %v, want %v with %v",
					f.events.sizes, f.events.limits, wantSizes, wantLimits)
			}
			if checks := len(f.repo.locks[0].checks); checks != len(wantSizes) {
				t.Errorf("checked the lock %d times, want once per batch, %d", checks, len(wantSizes))
			}
			if got, want := f.repo.inserted(), eventIDs(events...); !slices.Equal(got, want) {
				t.Errorf("deliveries for %d events, want %d", len(got), len(want))
			}
			if got := f.events.relayedIDs(); len(got) != n {
				t.Errorf("relayed %d events, want %d", len(got), n)
			}
			f.checkDeliveries(t)
		})
	}
}

func TestRelayWithoutTheLock(t *testing.T) {
	f := newRelayFixture(t, testEvents(hookTenant, 3)...)
	f.repo.busy = func(time.Duration) bool { return true }

	for range 2 {
		if relayed, err := f.once(t, false); relayed != 0 || err != nil {
			t.Errorf("Once() = %d relayed, %v; want 0, nil", relayed, err)
		}
	}

	if len(f.repo.lockCalls) != 2 {
		t.Errorf("tried the lock %d times, want 2", len(f.repo.lockCalls))
	}
	if len(f.events.reads) > 0 || len(f.repo.inserts) > 0 || len(f.tenants.lookups) > 0 {
		t.Errorf("relayed without the lock: %d reads, %d inserts, %d lookups",
			len(f.events.reads), len(f.repo.inserts), len(f.tenants.lookups))
	}
}

func TestRelayLockError(t *testing.T) {
	f := newRelayFixture(t, testEvents(hookTenant, 3)...)
	f.repo.lockErr = errBoom

	if relayed, err := f.once(t, false); relayed != 0 || !errors.Is(err, errBoom) {
		t.Errorf("Once() = %d relayed, %v; want 0, %v", relayed, err, errBoom)
	}
	if len(f.events.reads) > 0 {
		t.Errorf("read events %d times without the lock", len(f.events.reads))
	}
}

// TestRelayLosesTheLock loses the lock between two calls: the relay stops relaying, doesn't release the lost lock,
// and relays again once it takes the lock anew.
func TestRelayLosesTheLock(t *testing.T) {
	first, second := testEvent(hookTenant, 1), testEvent(hookTenant, 2)
	f := newRelayFixture(t, first)
	if relayed, err := f.once(t, true); relayed != 1 || err != nil {
		t.Fatalf("Once() = %d relayed, %v; want 1", relayed, err)
	}
	f.events.events = append(f.events.events, second)
	f.repo.locks[0].lost = true

	if relayed, err := f.once(t, false); relayed != 0 || err != nil {
		t.Errorf("Once() after losing the lock = %d relayed, %v; want 0, nil", relayed, err)
	}
	if len(f.events.reads) != 1 || len(f.repo.lockCalls) != 1 {
		t.Errorf("after losing the lock: %d reads and %d tries of the lock, want 1 and 1",
			len(f.events.reads), len(f.repo.lockCalls))
	}
	if relayed, err := f.once(t, true); relayed != 1 || err != nil {
		t.Errorf("Once() with the lock taken again = %d relayed, %v; want 1", relayed, err)
	}

	if len(f.repo.locks) != 2 || f.repo.locks[0].releases != 0 {
		t.Errorf("handed out %d locks and released the lost one %d times, want 2 and 0",
			len(f.repo.locks), f.repo.locks[0].releases)
	}
	if got, want := f.repo.inserted(), eventIDs(first, second); !slices.Equal(got, want) {
		t.Errorf("deliveries for %v, want %v", got, want)
	}
	want := []string{"INFO took the relay lock", "WARN lost the relay lock", "INFO took the relay lock"}
	if got := f.logs.messages(t); !slices.Equal(got, want) {
		t.Errorf("logs = %q, want %q", got, want)
	}
}

func TestRelayClose(t *testing.T) {
	f := newRelayFixture(t)
	ctx := f.logs.ctx(t.Context())

	if err := f.relay.Close(ctx); err != nil {
		t.Errorf("Close() without the lock: error = %v", err)
	}
	f.once(t, true)
	if err := f.relay.Close(ctx); err != nil {
		t.Errorf("Close() error = %v", err)
	}
	if err := f.relay.Close(ctx); err != nil {
		t.Errorf("second Close() error = %v", err)
	}
	if f.repo.locks[0].releases != 1 {
		t.Errorf("released the lock %d times, want 1", f.repo.locks[0].releases)
	}

	f.once(t, true)
	f.repo.locks[1].releaseErr = errBoom
	if err := f.relay.Close(ctx); !errors.Is(err, errBoom) {
		t.Errorf("Close() error = %v, want %v", err, errBoom)
	}
	f.once(t, true)
	if len(f.repo.locks) != 3 {
		t.Errorf("handed out %d locks, want a new one after the failed release", len(f.repo.locks))
	}
	want := []string{
		"INFO took the relay lock", "INFO released the relay lock",
		"INFO took the relay lock", "INFO took the relay lock",
	}
	if got := f.logs.messages(t); !slices.Equal(got, want) {
		t.Errorf("logs = %q, want %q", got, want)
	}
}

// TestRelayRun follows Run on a fake clock: it tries the lock once a second while another relay holds it,
// drains back to back once it has the lock, waits a second after each drain, stops relaying when the lock is lost
// until it takes it again, and releases the lock on a live context when it stops.
func TestRelayRun(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const ms = time.Millisecond
		events := testEvents(hookTenant, 232)
		visible := make([]time.Duration, len(events))
		for i := range 230 {
			visible[i] = 3500 * ms
		}
		visible[230], visible[231] = 5200*ms, 8700*ms
		f := newRelayFixture(t, events...)
		f.events.visibleAfter = visible
		f.repo.busy = func(d time.Duration) bool { return d < 2500*ms || 6500*ms <= d && d < 8500*ms }
		f.repo.lostAfter = []time.Duration{6500 * ms}
		ctx, cancel := context.WithCancel(f.logs.ctx(t.Context()))
		done := make(chan struct{})

		go func() {
			defer close(done)
			f.relay.Run(ctx)
		}()
		time.Sleep(9500 * ms)
		cancel()
		<-done

		if stopped := f.repo.elapsed(); stopped != 9500*ms {
			t.Errorf("Run returned %v after the start, want 9.5s", stopped)
		}
		s := func(seconds ...int) []time.Duration {
			out := make([]time.Duration, len(seconds))
			for i, n := range seconds {
				out[i] = time.Duration(n) * time.Second
			}
			return out
		}
		if want := s(0, 1, 2, 3, 8, 9); !slices.Equal(f.repo.lockCalls, want) {
			t.Errorf("tried the lock at %v, want %v", f.repo.lockCalls, want)
		}
		if want := s(3, 4, 4, 4, 5, 6, 9); !slices.Equal(f.events.reads, want) {
			t.Errorf("read events at %v, want %v", f.events.reads, want)
		}
		if want := []int{0, 100, 100, 30, 0, 1, 1}; !slices.Equal(f.events.sizes, want) {
			t.Errorf("read batches of %v, want %v", f.events.sizes, want)
		}
		if len(f.repo.locks) != 2 {
			t.Fatalf("handed out %d locks, want 2", len(f.repo.locks))
		}
		lost, last := f.repo.locks[0], f.repo.locks[1]
		if want := s(3, 4, 4, 4, 5, 6, 7); !slices.Equal(lost.checks, want) {
			t.Errorf("checked the first lock at %v, want %v", lost.checks, want)
		}
		if want := s(9); !slices.Equal(last.checks, want) {
			t.Errorf("checked the second lock at %v, want %v", last.checks, want)
		}
		if lost.releases != 0 || last.releases != 1 {
			t.Errorf("released the locks %d and %d times, want 0 and 1", lost.releases, last.releases)
		}
		if last.releases == 1 {
			wantDeadline := f.repo.start.Add(9500*ms + releaseTimeout)
			if last.releaseCtxErrs[0] != nil || !last.releaseDeadlines[0].Equal(wantDeadline) {
				t.Errorf("released on a context with error %v and deadline %v, want a live one with deadline %v",
					last.releaseCtxErrs[0], last.releaseDeadlines[0], wantDeadline)
			}
		}
		if got := f.repo.inserted(); !slices.Equal(got, eventIDs(events...)) {
			t.Errorf("deliveries for %d events, want all %d", len(got), len(events))
		}
		want := []string{
			"INFO took the relay lock", "WARN lost the relay lock", "INFO took the relay lock",
			"INFO released the relay lock",
		}
		if got := f.logs.messages(t); !slices.Equal(got, want) {
			t.Errorf("logs = %q, want %q", got, want)
		}
	})
}

// TestRelayRunLogsFailures keeps going after a failed drain, and logs every failure but the one shutdown causes.
func TestRelayRunLogsFailures(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := newRelayFixture(t, testEvents(hookTenant, 1)...)
		f.events.readErr = errBoom
		ctx, cancel := context.WithCancel(f.logs.ctx(t.Context()))
		done := make(chan struct{})

		go func() {
			defer close(done)
			f.relay.Run(ctx)
		}()
		time.Sleep(2500 * time.Millisecond)
		cancel()
		<-done

		want := []string{
			"INFO took the relay lock", "ERROR relay failed", "ERROR relay failed", "ERROR relay failed",
			"INFO released the relay lock",
		}
		if got := f.logs.messages(t); !slices.Equal(got, want) {
			t.Errorf("logs = %q, want %q", got, want)
		}
	})
}
