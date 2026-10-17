package webhook

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"testing"
	"testing/synctest"
	"time"
	"uuid"

	"github.com/zanvd/slumber-mill/src/apiv1"
	"github.com/zanvd/slumber-mill/src/asset"
	"github.com/zanvd/slumber-mill/src/egress"
	"github.com/zanvd/slumber-mill/src/logging"
	"github.com/zanvd/slumber-mill/src/secret"
	"github.com/zanvd/slumber-mill/src/tenant"
	"github.com/zanvd/slumber-mill/src/worker"
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
	tenants *fakeWebhookTenants
	logs    *logBuffer
}

func newRelayFixture(t *testing.T, events ...asset.Event) *relayFixture {
	t.Helper()
	start := time.Now()
	f := &relayFixture{
		repo:    &fakeRelayRepo{start: start},
		events:  &fakeEvents{start: start, events: events},
		tenants: &fakeWebhookTenants{has: map[uint64]bool{hookTenant: true}},
		logs:    &logBuffer{},
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
	repo, events, tenants := &fakeRelayRepo{}, &fakeEvents{}, &fakeWebhookTenants{}
	tests := []struct {
		name    string
		repo    relayRepo
		events  eventSource
		tenants webhookTenants
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
	everyTenant := [][]uint64{{hookTenant, plainTenant, goneTenant}}
	tests := []struct {
		name         string
		setup        func(f *relayFixture)
		wantRelayed  int
		wantErr      error
		wantInserted []uuid.UUID
		wantMarked   []uuid.UUID
		wantLookups  [][]uint64
	}{
		{
			name:         "only tenants with a webhook get deliveries",
			wantRelayed:  4,
			wantInserted: eventIDs(hook1, hook2),
			wantMarked:   eventIDs(all...),
			wantLookups:  everyTenant,
		},
		{
			name:        "events of tenants without a webhook are only marked relayed",
			setup:       func(f *relayFixture) { delete(f.tenants.has, hookTenant) },
			wantRelayed: 4,
			wantMarked:  eventIDs(all...),
			wantLookups: everyTenant,
		},
		{
			name:         "every tenant has a webhook",
			setup:        func(f *relayFixture) { f.tenants.has[plainTenant], f.tenants.has[goneTenant] = true, true },
			wantRelayed:  4,
			wantInserted: eventIDs(all...),
			wantMarked:   eventIDs(all...),
			wantLookups:  everyTenant,
		},
		{
			name:        "a failed lookup aborts the batch",
			setup:       func(f *relayFixture) { f.tenants.err = errBoom },
			wantErr:     errBoom,
			wantLookups: everyTenant,
		},
		{
			name:    "reading events fails",
			setup:   func(f *relayFixture) { f.events.readErr = errBoom },
			wantErr: errBoom,
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
			if !slices.EqualFunc(f.tenants.lookups, tc.wantLookups, slices.Equal) {
				t.Errorf("looked up tenants %v, want %v", f.tenants.lookups, tc.wantLookups)
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

// TestRelayLooksTenantsUpOncePerBatch gives each batch of a drain its own lookup, of the batch's distinct tenants only.
func TestRelayLooksTenantsUpOncePerBatch(t *testing.T) {
	var events []asset.Event
	for i := range 2 * asset.MaxEventBatch {
		events = append(events, testEvent([]uint64{hookTenant, plainTenant, goneTenant}[i%3], i))
	}
	events = append(events, testEvents(plainTenant, 5)...)
	f := newRelayFixture(t, events...)

	relayed, err := f.once(t, true)

	if err != nil || relayed != len(events) {
		t.Errorf("Once() = %d relayed, %v; want %d", relayed, err, len(events))
	}
	want := [][]uint64{{hookTenant, plainTenant, goneTenant}, {plainTenant, goneTenant, hookTenant}, {plainTenant}}
	if !slices.EqualFunc(f.tenants.lookups, want, slices.Equal) {
		t.Errorf("looked up tenants %v, want %v", f.tenants.lookups, want)
	}
	for _, d := range f.repo.inserted() {
		if i := slices.IndexFunc(events, func(e asset.Event) bool { return e.ID == d }); events[i].TenantID != hookTenant {
			t.Errorf("delivery for an event of tenant %d, which has no webhook", events[i].TenantID)
		}
	}
	f.checkDeliveries(t)
}

// TestRelayLookupFailureInLaterBatch fails the lookup of the second batch: the first stays relayed,
// and the second is neither delivered nor marked.
func TestRelayLookupFailureInLaterBatch(t *testing.T) {
	events := testEvents(hookTenant, asset.MaxEventBatch+10)
	f := newRelayFixture(t, events...)
	f.tenants.errFrom = 2

	relayed, err := f.once(t, true)

	if relayed != asset.MaxEventBatch || !errors.Is(err, errBoom) {
		t.Errorf("Once() = %d relayed, %v; want %d, %v", relayed, err, asset.MaxEventBatch, errBoom)
	}
	if got, want := f.events.relayedIDs(), eventIDs(events[:asset.MaxEventBatch]...); !slices.Equal(got, want) {
		t.Errorf("relayed %d events, want the first batch of %d", len(got), len(want))
	}
	if got, want := f.repo.inserted(), eventIDs(events[:asset.MaxEventBatch]...); !slices.Equal(got, want) {
		t.Errorf("deliveries for %d events, want the first batch of %d", len(got), len(want))
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

func TestRelayWhileHeld(t *testing.T) {
	const ms = time.Millisecond
	tests := []struct {
		name       string
		busy       func(elapsed time.Duration) bool
		lostAfter  []time.Duration
		stopAt     time.Duration
		wantStarts []time.Duration
		wantEnds   []time.Duration
	}{
		{
			name:       "holding the lock throughout",
			stopAt:     3500 * ms,
			wantStarts: []time.Duration{0},
			wantEnds:   []time.Duration{3500 * ms},
		},
		{
			name:   "another relay holds the lock throughout",
			busy:   func(time.Duration) bool { return true },
			stopAt: 5 * time.Second,
		},
		{
			name:       "the lock is lost and taken again",
			busy:       func(d time.Duration) bool { return 5*time.Second <= d && d < 20*time.Second },
			lostAfter:  []time.Duration{5 * time.Second},
			stopAt:     30500 * ms,
			wantStarts: []time.Duration{0, 20 * time.Second},
			wantEnds:   []time.Duration{5 * time.Second, 30500 * ms},
		},
		{
			name:       "taking the lock late",
			busy:       func(d time.Duration) bool { return d < 2500*ms },
			stopAt:     4 * time.Second,
			wantStarts: []time.Duration{3 * time.Second},
			wantEnds:   []time.Duration{4 * time.Second},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				f := newRelayFixture(t)
				f.repo.busy, f.repo.lostAfter = tc.busy, tc.lostAfter
				var starts, ends []time.Duration
				unreleasedAtEnd := true
				f.relay.WhileHeld(func(ctx context.Context) {
					starts = append(starts, f.repo.elapsed())
					<-ctx.Done()
					ends = append(ends, f.repo.elapsed())
					unreleasedAtEnd = unreleasedAtEnd && f.repo.locks[len(f.repo.locks)-1].releases == 0
				})
				ctx, cancel := context.WithCancel(f.logs.ctx(t.Context()))
				done := make(chan struct{})

				go func() {
					defer close(done)
					f.relay.Run(ctx)
				}()
				time.Sleep(tc.stopAt)
				cancel()
				<-done

				if !slices.Equal(starts, tc.wantStarts) || !slices.Equal(ends, tc.wantEnds) {
					t.Errorf("ran from %v to %v, want from %v to %v", starts, ends, tc.wantStarts, tc.wantEnds)
				}
				if !unreleasedAtEnd {
					t.Error("the relay lock was released before the function returned")
				}
			})
		})
	}
}

// claimed is the lease the pool hands the deliverer in these tests.
var claimed = worker.Lease{ID: 11, TenantID: hookTenant, Attempt: 2}

type deliveryFixture struct {
	d       *Deliverer
	repo    *fakeDeliveryRepo
	tenants *fakeTenants
	client  *fakePoster
}

func newDeliveryFixture(t *testing.T) *deliveryFixture {
	t.Helper()
	f := &deliveryFixture{
		repo: &fakeDeliveryRepo{
			errs: map[string]error{},
			pending: delivery{
				EventID:  uuid.NewV7(),
				TenantID: hookTenant,
				Body:     []byte(`{"id":"event","type":"asset.ready","data":{"title":"Rain"}}`),
			},
		},
		tenants: &fakeTenants{
			targets: map[uint64]tenant.Webhook{hookTenant: {URL: hookURL, Secret: secret.NewValue(hookSecret)}},
			errs:    map[uint64]error{},
		},
		client: &fakePoster{status: http.StatusOK},
	}
	d, err := NewDeliverer(f.repo, f.tenants, f.client, DelivererConfig{WorkerID: "webhook-1", Lease: time.Minute})
	if err != nil {
		t.Fatalf("NewDeliverer() error = %v", err)
	}
	f.d = d
	return f
}

func TestNewDeliverer(t *testing.T) {
	repo, tenants, client := &fakeDeliveryRepo{}, &fakeTenants{}, &fakePoster{}
	valid := DelivererConfig{WorkerID: "webhook-1", Lease: time.Minute}
	tests := []struct {
		name    string
		repo    deliveryRepo
		tenants webhookTargets
		client  poster
		cfg     DelivererConfig
	}{
		{"no repository", nil, tenants, client, valid},
		{"no tenant service", repo, nil, client, valid},
		{"no client", repo, tenants, nil, valid},
		{"no worker ID", repo, tenants, client, DelivererConfig{Lease: time.Minute}},
		{"no lease", repo, tenants, client, DelivererConfig{WorkerID: "webhook-1"}},
		{"negative lease", repo, tenants, client, DelivererConfig{WorkerID: "webhook-1", Lease: -time.Second}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if d, err := NewDeliverer(tc.repo, tc.tenants, tc.client, tc.cfg); err == nil {
				t.Errorf("NewDeliverer() = %+v, want an error", d)
			}
		})
	}
}

func TestDelivererClaim(t *testing.T) {
	due := lease(claimed)
	dbErr := fmt.Errorf("error claiming delivery: %w: %v", errDatabase, errBoom)
	tests := []struct {
		name    string
		due     *lease
		err     error
		want    worker.Lease
		wantOK  bool
		wantErr error
	}{
		{"due", &due, nil, claimed, true, nil},
		{"nothing due", nil, nil, worker.Lease{}, false, nil},
		{"database error", &due, dbErr, worker.Lease{}, false, errDatabase},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			f := newDeliveryFixture(t)
			f.repo.due = tc.due
			f.repo.errs["claim"] = tc.err

			got, ok, err := f.d.Claim(t.Context())

			if got != tc.want || ok != tc.wantOK || !errors.Is(err, tc.wantErr) {
				t.Errorf("Claim() = %+v, %t, %v; want %+v, %t, %v", got, ok, err, tc.want, tc.wantOK, tc.wantErr)
			}
			if !slices.Equal(f.repo.workerIDs, []string{"webhook-1"}) ||
				!slices.Equal(f.repo.durations, []time.Duration{time.Minute}) {
				t.Errorf("claim() got worker IDs %v and leases %v, want webhook-1 for 1m",
					f.repo.workerIDs, f.repo.durations)
			}
		})
	}
}

func TestDelivererExtendAndRelease(t *testing.T) {
	f := newDeliveryFixture(t)

	if err := f.d.Extend(t.Context(), claimed); err != nil {
		t.Errorf("Extend() error = %v", err)
	}
	if err := f.d.Release(t.Context(), claimed); err != nil {
		t.Errorf("Release() error = %v", err)
	}

	if want := []lease{lease(claimed), lease(claimed)}; !slices.Equal(f.repo.leases, want) {
		t.Errorf("leases = %+v, want %+v", f.repo.leases, want)
	}
	if want := []time.Duration{time.Minute}; !slices.Equal(f.repo.durations, want) {
		t.Errorf("extend() got %v, want %v", f.repo.durations, want)
	}
}

// TestDelivererProcess covers every outcome: which ones complete the delivery, which are permanent,
// and which never reach the tenant's endpoint.
func TestDelivererProcess(t *testing.T) {
	dial := func(err error) error {
		return fmt.Errorf("error posting webhook: %w",
			&url.Error{Op: "Post", URL: hookURL, Err: &net.OpError{Op: "dial", Net: "tcp", Err: err}})
	}
	type want struct {
		delivered, permanent, posted, completed bool
		// is must match the error, and msg must be its text, when either is set.
		is  error
		msg string
	}
	type testCase struct {
		name  string
		setup func(f *deliveryFixture)
		want  want
	}
	status := func(code int) func(f *deliveryFixture) {
		return func(f *deliveryFixture) { f.client.status = code }
	}
	postErr := func(err error) func(f *deliveryFixture) {
		return func(f *deliveryFixture) { f.client.err = err }
	}
	var tests []testCase
	for _, code := range []int{200, 201, 202, 204, 299} {
		tests = append(tests, testCase{fmt.Sprintf("%d delivers", code), status(code),
			want{delivered: true, posted: true, completed: true}})
	}
	for _, code := range []int{100, 300, 301, 302, 303, 304, 307, 308, 400, 401, 403, 404, 408, 409, 429, 500, 502, 503, 504} {
		tests = append(tests, testCase{fmt.Sprintf("%d retries", code), status(code),
			want{posted: true, msg: fmt.Sprintf("webhook endpoint answered %d", code)}})
	}
	tests = append(tests, []testCase{
		{"410 is permanent", status(http.StatusGone),
			want{permanent: true, posted: true, msg: "webhook endpoint answered 410"}},
		{"a blocked address is permanent", postErr(dial(egress.ErrBlockedAddress)),
			want{permanent: true, posted: true, is: egress.ErrBlockedAddress}},
		{"an unresolvable host retries", postErr(dial(fmt.Errorf("lookup hooks.example.com: %w", egress.ErrUnresolvable))),
			want{posted: true, is: egress.ErrUnresolvable}},
		{"a timeout retries",
			postErr(fmt.Errorf("error posting webhook: %w", &url.Error{Op: "Post", URL: hookURL, Err: context.DeadlineExceeded})),
			want{posted: true, is: context.DeadlineExceeded}},
		{"another post error retries", postErr(fmt.Errorf("error posting webhook: %w", errBoom)),
			want{posted: true, is: errBoom}},
		{"a tenant that removed its webhook is permanent", func(f *deliveryFixture) { delete(f.tenants.targets, hookTenant) },
			want{permanent: true, is: tenant.ErrNoWebhook}},
		{"a tenant that is gone is permanent", func(f *deliveryFixture) {
			f.tenants.errs[hookTenant] = fmt.Errorf("error reading webhook target: %w", tenant.ErrNotFound)
		}, want{permanent: true, is: tenant.ErrNotFound}},
		{"an unreadable target retries", func(f *deliveryFixture) {
			f.tenants.errs[hookTenant] = fmt.Errorf("error opening webhook secret of tenant 7: %w", errBoom)
		}, want{is: errBoom}},
		{"an empty secret is permanent", func(f *deliveryFixture) { f.tenants.targets[hookTenant] = tenant.Webhook{URL: hookURL} },
			want{permanent: true, is: ErrEmptySecret}},
		{"an unreadable delivery retries", func(f *deliveryFixture) { f.repo.errs["leasedDelivery"] = errBoom },
			want{is: errBoom}},
		{"a failed completion is returned", func(f *deliveryFixture) {
			f.repo.errs["complete"] = fmt.Errorf("error completing delivery: %w: %v", errDatabase, errBoom)
		}, want{posted: true, completed: true, is: errDatabase}},
	}...)

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			f := newDeliveryFixture(t)
			tc.setup(f)

			err := f.d.Process(t.Context(), claimed)

			w := tc.want
			if (err == nil) != w.delivered || worker.IsPermanent(err) != w.permanent || errors.Is(err, worker.ErrLeaseLost) {
				t.Errorf("Process() error = %v; want delivered %t, permanent %t", err, w.delivered, w.permanent)
			}
			if w.is != nil && !errors.Is(err, w.is) {
				t.Errorf("Process() error = %v, want it to match %v", err, w.is)
			}
			if w.msg != "" && (err == nil || err.Error() != w.msg) {
				t.Errorf("Process() error = %v, want %q", err, w.msg)
			}
			if (f.repo.completed == 1) != w.completed || f.repo.completed > 1 {
				t.Errorf("completed %d times, want completed: %t", f.repo.completed, w.completed)
			}
			for i, l := range f.repo.leases {
				if l != lease(claimed) {
					t.Errorf("repository call %d got lease %+v, want %+v", i, l, claimed)
				}
			}
			if !w.posted {
				if len(f.client.posts) > 0 {
					t.Errorf("posted %d times, want no post", len(f.client.posts))
				}
				return
			}
			if len(f.client.posts) != 1 {
				t.Fatalf("posted %d times, want once", len(f.client.posts))
			}
			checkPost(t, f.client.posts[0], f.repo.pending)
		})
	}
}

// checkPost checks that the delivery went to the tenant's URL under its event ID, with its body exactly as stored,
// signed with the tenant's secret.
func checkPost(t *testing.T, p post, d delivery) {
	t.Helper()
	if p.url != hookURL || p.eventID != d.EventID.String() || !bytes.Equal(p.body, d.Body) {
		t.Errorf("posted %s to %s as event %s, want %s to %s as event %s", p.body, p.url, p.eventID,
			d.Body, hookURL, d.EventID)
	}
	if err := Verify(hookSecret, p.signature, d.Body, p.at, time.Second); err != nil {
		t.Errorf("signature %q doesn't verify at the time of posting: %v", p.signature, err)
	}
}

// TestDelivererContexts cancels the work's context as the endpoint answers, as a lost heartbeat or the end of
// the shutdown grace would: the completion still runs, on its own 15 s deadline and with the work's logger.
// The signature carries the time of sending.
func TestDelivererContexts(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := newDeliveryFixture(t)
		log := slog.New(slog.DiscardHandler)
		work, cancel := context.WithCancel(logging.CtxWithLogger(t.Context(), log))
		defer cancel()
		f.client.respond = func(context.Context) (int, error) {
			time.Sleep(time.Minute)
			cancel()
			return http.StatusNoContent, nil
		}
		start := time.Now()

		if err := f.d.Process(work, claimed); err != nil {
			t.Fatalf("Process() error = %v", err)
		}

		if f.repo.completed != 1 || f.repo.completeErr != nil {
			t.Fatalf("completed %d times on a context with error %v, want once on a live one",
				f.repo.completed, f.repo.completeErr)
		}
		if want := start.Add(time.Minute); !f.repo.completeAt.Equal(want) {
			t.Errorf("completed at %v, want %v", f.repo.completeAt, want)
		}
		if want := f.repo.completeAt.Add(15 * time.Second); !f.repo.completeDeadline.Equal(want) {
			t.Errorf("completion deadline = %v, want %v", f.repo.completeDeadline, want)
		}
		if f.repo.completeLog != log {
			t.Error("the completion's context lost the work's logger")
		}
		if want := fmt.Sprintf("t=%d,", start.Unix()); !strings.HasPrefix(f.client.posts[0].signature, want) {
			t.Errorf("signature %q, want one made at %v", f.client.posts[0].signature, start)
		}
	})
}

func TestDelivererFail(t *testing.T) {
	wrapped := fmt.Errorf("error posting webhook: %w", worker.Permanent(egress.ErrBlockedAddress))
	tests := []struct {
		name  string
		cause error
		want  failure
	}{
		{"permanent", worker.Permanent(errBoom), failure{Permanent: true, Detail: "boom"}},
		{"wrapped permanent", wrapped, failure{Permanent: true, Detail: wrapped.Error()}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			f := newDeliveryFixture(t)

			if err := f.d.Fail(t.Context(), claimed, tc.cause); err != nil {
				t.Fatalf("Fail() error = %v", err)
			}

			if want := []failure{tc.want}; !slices.Equal(f.repo.failures, want) {
				t.Errorf("failures = %+v, want %+v", f.repo.failures, want)
			}
			if want := []lease{lease(claimed)}; !slices.Equal(f.repo.leases, want) {
				t.Errorf("leases = %+v, want %+v", f.repo.leases, want)
			}
		})
	}
}

// TestDelivererFailRetries fails retryable errors many times on each attempt: every delay falls in [d/2, d],
// where d doubles from 30 s up to 6 h.
func TestDelivererFailRetries(t *testing.T) {
	cause := errors.New("webhook endpoint answered 503")
	tests := []struct {
		attempt int
		d       time.Duration
	}{
		{1, 30 * time.Second},
		{2, time.Minute},
		{3, 2 * time.Minute},
		{5, 8 * time.Minute},
		{9, 128 * time.Minute},
		{10, 256 * time.Minute},
		{11, 6 * time.Hour},
		{maxAttempts, 6 * time.Hour},
	}
	for _, tc := range tests {
		t.Run(fmt.Sprintf("attempt %d", tc.attempt), func(t *testing.T) {
			f := newDeliveryFixture(t)
			l := worker.Lease{ID: 11, TenantID: hookTenant, Attempt: tc.attempt}

			for range 100 {
				if err := f.d.Fail(t.Context(), l, cause); err != nil {
					t.Fatalf("Fail() error = %v", err)
				}
			}

			for _, got := range f.repo.failures {
				if got.Permanent || got.Detail != cause.Error() || got.Delay < tc.d/2 || got.Delay > tc.d {
					t.Fatalf("failure = %+v, want a retry after %v to %v with detail %q",
						got, tc.d/2, tc.d, cause.Error())
				}
			}
		})
	}
}

func TestDelivererLostLease(t *testing.T) {
	ops := []struct {
		method string
		run    func(ctx context.Context, d *Deliverer) error
	}{
		{"extend", func(ctx context.Context, d *Deliverer) error { return d.Extend(ctx, claimed) }},
		{"complete", func(ctx context.Context, d *Deliverer) error { return d.Process(ctx, claimed) }},
		{"fail", func(ctx context.Context, d *Deliverer) error { return d.Fail(ctx, claimed, errBoom) }},
		{"release", func(ctx context.Context, d *Deliverer) error { return d.Release(ctx, claimed) }},
	}
	errs := []struct {
		name     string
		err      error
		wantLost bool
	}{
		{"lease lost", fmt.Errorf("error writing delivery: %w", errLeaseLost), true},
		{"database error", fmt.Errorf("error writing delivery: %w: %v", errDatabase, errBoom), false},
	}
	for _, op := range ops {
		for _, e := range errs {
			t.Run(op.method+"/"+e.name, func(t *testing.T) {
				f := newDeliveryFixture(t)
				f.repo.errs[op.method] = e.err

				err := op.run(t.Context(), f.d)

				if err == nil || errors.Is(err, worker.ErrLeaseLost) != e.wantLost {
					t.Errorf("error = %v; matches worker.ErrLeaseLost: %t, want %t",
						err, errors.Is(err, worker.ErrLeaseLost), e.wantLost)
				}
				if err != nil && err.Error() != e.err.Error() {
					t.Errorf("error = %q, want the repository's %q", err, e.err)
				}
			})
		}
	}
}

func TestDelivererReap(t *testing.T) {
	tests := []struct {
		name             string
		requeued, failed int
		err              error
		wantLogs         []string
	}{
		{"nothing expired", 0, 0, nil, nil},
		{"requeued", 2, 0, nil, []string{"WARN reaped deliveries whose lease expired"}},
		{"failed", 0, 1, nil, []string{"WARN reaped deliveries whose lease expired"}},
		{"database error", 0, 0, fmt.Errorf("error reaping deliveries: %w: %v", errDatabase, errBoom), nil},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			f := newDeliveryFixture(t)
			f.repo.requeued, f.repo.failed, f.repo.errs["reap"] = tc.requeued, tc.failed, tc.err
			var logs logBuffer

			if err := f.d.Reap(logs.ctx(t.Context())); !errors.Is(err, tc.err) {
				t.Errorf("Reap() error = %v, want %v", err, tc.err)
			}

			if got := logs.messages(t); !slices.Equal(got, tc.wantLogs) {
				t.Errorf("logs = %q, want %q", got, tc.wantLogs)
			}
		})
	}
}

const retention = 30 * 24 * time.Hour

// retentionLog is a retention summary as logged.
type retentionLog struct {
	Level, Msg, Age string
	Deleted         int
	Batches         int
}

func (b *logBuffer) retentionLogs(t *testing.T) []retentionLog {
	t.Helper()
	var out []retentionLog
	sc := bufio.NewScanner(bytes.NewReader(b.Bytes()))
	for sc.Scan() {
		var entry retentionLog
		if err := json.Unmarshal(sc.Bytes(), &entry); err != nil {
			t.Fatalf("error decoding log entry %s: %v", sc.Text(), err)
		}
		out = append(out, entry)
	}
	return out
}

func wantRetentionLogs(deleted, batches int) []retentionLog {
	if deleted == 0 {
		return nil
	}
	return []retentionLog{{Level: "INFO", Msg: "deleted delivered webhook deliveries past retention",
		Age: "720h0m0s", Deleted: deleted, Batches: batches}}
}

// TestDeleteDelivered deletes in batches until one comes back short, each batch starting where the last stopped.
func TestDeleteDelivered(t *testing.T) {
	tests := []struct {
		name        string
		expired     int
		batch       int
		wantBatches int
	}{
		{"nothing expired", 0, 3, 1},
		{"less than a batch", 2, 3, 1},
		{"exactly a batch", 3, 3, 2},
		{"several batches", 7, 3, 3},
		{"several full batches", 9, 3, 4},
		{"batches of one", 4, 1, 5},
		{"the default batch", 1001, defaultRetentionBatch, 3},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			f := newDeliveryFixture(t)
			if f.d.retentionBatch != defaultRetentionBatch {
				t.Fatalf("retention batch = %d, want %d", f.d.retentionBatch, defaultRetentionBatch)
			}
			f.d.retentionBatch = tc.batch
			f.repo.expired = tc.expired
			var logs logBuffer

			deleted, err := f.d.DeleteDelivered(logs.ctx(t.Context()), retention)

			if err != nil || deleted != tc.expired {
				t.Errorf("DeleteDelivered() = %d, %v; want %d", deleted, err, tc.expired)
			}
			if len(f.repo.purges) != tc.wantBatches {
				t.Fatalf("ran %d batches, want %d", len(f.repo.purges), tc.wantBatches)
			}
			var after deliveryCursor
			for i, p := range f.repo.purges {
				if want := (purgeArgs{retention, after, tc.batch}); p != want {
					t.Errorf("batch %d got %+v, want %+v", i+1, p, want)
				}
				after = deliveryCursor{RunAfter: eventTime, ID: uint64(min((i+1)*tc.batch, tc.expired))}
			}
			if got, want := logs.retentionLogs(t), wantRetentionLogs(tc.expired, tc.wantBatches); !slices.Equal(got, want) {
				t.Errorf("logs = %+v, want %+v", got, want)
			}
		})
	}
}

// TestDeleteDeliveredStops stops the loop on a done context, a failed batch or an age that isn't positive,
// and reports and logs what the batches before deleted.
func TestDeleteDeliveredStops(t *testing.T) {
	// stopAt runs stop after batch n.
	stopAt := func(n int, stop func(f *deliveryFixture, cancel context.CancelFunc)) func(*deliveryFixture, context.CancelFunc, int) {
		return func(f *deliveryFixture, cancel context.CancelFunc, batch int) {
			if batch == n {
				stop(f, cancel)
			}
		}
	}
	cancelNow := func(_ *deliveryFixture, cancel context.CancelFunc) { cancel() }
	failNext := func(f *deliveryFixture, _ context.CancelFunc) { f.repo.errs["deleteDelivered"] = errBoom }
	tests := []struct {
		name string
		age  time.Duration
		// stop runs after each batch, numbered from 1, and with 0 before the first.
		stop        func(f *deliveryFixture, cancel context.CancelFunc, batch int)
		wantDeleted int
		wantBatches int
		wantErr     error
	}{
		{name: "cancelled before the first batch", age: retention, stop: stopAt(0, cancelNow), wantErr: context.Canceled},
		{
			name: "cancelled after the second batch", age: retention, stop: stopAt(2, cancelNow),
			wantDeleted: 6, wantBatches: 2, wantErr: context.Canceled,
		},
		{name: "the first batch fails", age: retention, stop: stopAt(0, failNext), wantBatches: 1, wantErr: errBoom},
		{
			name: "the third batch fails", age: retention, stop: stopAt(2, failNext),
			wantDeleted: 6, wantBatches: 3, wantErr: errBoom,
		},
		{name: "zero age", age: 0},
		{name: "negative age", age: -time.Second},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			f := newDeliveryFixture(t)
			f.d.retentionBatch = 3
			f.repo.expired = 10
			var logs logBuffer
			ctx, cancel := context.WithCancel(logs.ctx(t.Context()))
			defer cancel()
			if tc.stop != nil {
				tc.stop(f, cancel, 0)
				f.repo.afterPurge = func(batch int) { tc.stop(f, cancel, batch) }
			}

			deleted, err := f.d.DeleteDelivered(ctx, tc.age)

			if deleted != tc.wantDeleted || err == nil || tc.wantErr != nil && !errors.Is(err, tc.wantErr) {
				t.Errorf("DeleteDelivered() = %d, %v; want %d and an error matching %v",
					deleted, err, tc.wantDeleted, tc.wantErr)
			}
			if err != nil && !strings.HasPrefix(err.Error(), "error deleting delivered deliveries: ") {
				t.Errorf("error = %q, want it prefixed", err)
			}
			if len(f.repo.purges) != tc.wantBatches {
				t.Errorf("ran %d batches, want %d", len(f.repo.purges), tc.wantBatches)
			}
			if got, want := logs.retentionLogs(t), wantRetentionLogs(tc.wantDeleted, tc.wantBatches); !slices.Equal(got, want) {
				t.Errorf("logs = %+v, want %+v", got, want)
			}
		})
	}
}

// TestDeliveryKeepsTheSecretOut drives deliveries through a worker pool, and checks that neither the tenant's secret
// nor the signature made with it shows up in the logs or in what is stored as last_error.
func TestDeliveryKeepsTheSecretOut(t *testing.T) {
	tests := []struct {
		name  string
		setup func(f *deliveryFixture)
	}{
		{"delivered", func(f *deliveryFixture) {}},
		{"500", func(f *deliveryFixture) { f.client.status = http.StatusInternalServerError }},
		{"410", func(f *deliveryFixture) { f.client.status = http.StatusGone }},
		{"post fails", func(f *deliveryFixture) {
			f.client.err = fmt.Errorf("error posting webhook: %w", &url.Error{Op: "Post", URL: hookURL, Err: errBoom})
		}},
		{"blocked address", func(f *deliveryFixture) {
			f.client.err = fmt.Errorf("error posting webhook: %w", egress.ErrBlockedAddress)
		}},
		{"completion fails", func(f *deliveryFixture) {
			f.repo.errs["complete"] = fmt.Errorf("error completing delivery: %w: %v", errDatabase, errBoom)
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			f := newDeliveryFixture(t)
			tc.setup(f)
			f.repo.due = &lease{ID: 11, TenantID: hookTenant, Attempt: 1}
			var logs bytes.Buffer
			pool, err := worker.NewPool(f.d, worker.Config{
				WorkerID: "webhook-1", Pool: "webhook", Concurrency: 1, Lease: time.Minute, PollInterval: time.Second,
				Log: slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug})),
			})
			if err != nil {
				t.Fatalf("NewPool() error = %v", err)
			}

			if err := pool.RunUntilIdle(t.Context()); err != nil {
				t.Fatalf("RunUntilIdle() error = %v", err)
			}

			if len(f.client.posts) != 1 || !strings.Contains(logs.String(), "work finished") {
				t.Fatalf("posted %d times with logs %s, want one post and its outcome logged", len(f.client.posts), logs.String())
			}
			signature := f.client.posts[0].signature
			_, digest, _ := strings.Cut(signature, "v1=")
			texts := []string{logs.String()}
			for _, fl := range f.repo.failures {
				texts = append(texts, fl.Detail)
			}
			for _, text := range texts {
				for _, leak := range []string{string(hookSecret), signature, digest} {
					if strings.Contains(text, leak) {
						t.Errorf("%q leaks %q", text, leak)
					}
				}
			}
		})
	}
}
