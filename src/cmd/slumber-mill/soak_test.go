//go:build soak

// The full-stack soak drives the Compose stack that scripts/soak.sh starts: the API and both pools, each pool with
// two replicas, the real ffmpeg, and workers killed and restarted by the script while it runs. As several tenants
// it uploads through the API for a while, receives their webhooks, and once everything has finished checks the
// invariants over the database, the data directory, the API and what its receiver saw.
// It runs only inside that stack, as the soak service; see scripts/soak.sh.
package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"maps"
	"math"
	"math/rand/v2"
	"mime/multipart"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
	"uuid"

	"github.com/zanvd/slumber-mill/src/apiv1"
	"github.com/zanvd/slumber-mill/src/audio"
	"github.com/zanvd/slumber-mill/src/db"
	"github.com/zanvd/slumber-mill/src/testutil"
	"github.com/zanvd/slumber-mill/src/webhook"
)

const (
	soakDurationEnv     = "SLUMBER_MILL_SOAK_DURATION"
	soakDrainTimeoutEnv = "SLUMBER_MILL_SOAK_DRAIN_TIMEOUT"
	soakAPIEnv          = "SLUMBER_MILL_SOAK_API"
	soakListenEnv       = "SLUMBER_MILL_SOAK_LISTEN"
	soakOutEnv          = "SLUMBER_MILL_SOAK_OUT"

	soakDefaultDuration     = 10 * time.Minute
	soakDefaultDrainTimeout = 15 * time.Minute

	soakUploadersPerTenant = 2
	// soakMaxBacklog is how many unfinished assets the uploaders allow before they wait, so the encode pool always
	// has work but the backlog left when uploads stop drains in a few minutes.
	soakMaxBacklog = 12
	// soakRetryFor bounds how long one upload is retried through connection errors and 429s, as across a restart.
	soakRetryFor     = 3 * time.Minute
	soakStartTimeout = 5 * time.Minute

	soakMaxAssetAttempts    = 3
	soakMaxDeliveryAttempts = 12
	soakCeilingDBTP         = -1.5
	soakToleranceLU         = 1.0
	soakSignatureTolerance  = 5 * time.Minute
	// soakFailedMessage and soakReapMessage are what an asset whose leases expired on every attempt is left with.
	soakFailedMessage = "processing failed after repeated attempts"
	soakReapMessage   = "lease expired"
	// soakHang outlasts the webhook client's overall timeout of 15 s.
	soakHang = 20 * time.Second
	// Every soakDownloadEvery-th ready asset is also downloaded through the API, whole and by range.
	soakDownloadEvery = 10
)

// The invariants checked at the end. Problems found earlier, while uploading or receiving, are kept under the
// invariant they break and reported with the rest.
const (
	invAccepted   = "the API answered every upload 202, or 422 to a reused key, once retries were done"
	invIdempotent = "replays and concurrent retries of a key got one asset, and a reused key was refused"
	invUploaded   = "the assets are exactly the soak's uploads, one per idempotency key"
	invFinished   = "every asset is finished"
	invOneEvent   = "every finished asset has exactly one event, matching its state"
	invRenditions = "only ready assets have renditions, two each"
	invDelivery   = "every event was relayed and has exactly one delivery, none for a tenant without a webhook"
	invDelivered  = "every delivery has its event and is finished"
	invLeased     = "nothing is left leased"
	invAttempts   = "every asset and delivery used 1 to its maximum attempts"
	invAudio      = "every audio upload is ready with its measurements, or failed after its lease expired every time"
	invRejected   = "every rejected upload failed on its first attempt with its tenant-safe message"
	invInSpec     = "every rendition is in spec, and its file has the recorded size and hash"
	invDownload   = "the API serves renditions whole and by range"
	invSigned     = "every webhook the receiver got was signed and well-formed, for its own tenant"
	invReceived   = "every delivered event reached the receiver and was answered 2xx, and only 410s failed"
	invUnknown    = "the receiver got no event without a delivery"
	invPayload    = "every webhook's asset is the API's final asset, and its type matches the asset's status"
)

var soakInvariants = []string{
	invAccepted, invIdempotent, invUploaded, invFinished, invOneEvent, invRenditions, invDelivery, invDelivered,
	invLeased, invAttempts, invAudio, invRejected, invInSpec, invDownload, invSigned, invReceived, invUnknown,
	invPayload,
}

type soakConfig struct {
	duration, drainTimeout time.Duration
	api, listen, out       string
	dsn, dataDir           string
}

func loadSoakConfig(t *testing.T) soakConfig {
	t.Helper()
	cfg := soakConfig{
		duration:     soakEnvDuration(t, soakDurationEnv, soakDefaultDuration),
		drainTimeout: soakEnvDuration(t, soakDrainTimeoutEnv, soakDefaultDrainTimeout),
		api:          strings.TrimSuffix(os.Getenv(soakAPIEnv), "/"),
		listen:       os.Getenv(soakListenEnv),
		out:          os.Getenv(soakOutEnv),
		dsn:          os.Getenv(envDSN),
		dataDir:      os.Getenv(envDataDir),
	}
	for name, v := range map[string]string{
		soakAPIEnv: cfg.api, soakListenEnv: cfg.listen, soakOutEnv: cfg.out, envDSN: cfg.dsn, envDataDir: cfg.dataDir,
	} {
		if v == "" {
			t.Fatalf("%s is not set; the soak runs as the soak service of scripts/soak.sh", name)
		}
	}
	return cfg
}

func soakEnvDuration(t *testing.T, name string, def time.Duration) time.Duration {
	t.Helper()
	s := os.Getenv(name)
	if s == "" {
		return def
	}
	d, err := time.ParseDuration(s)
	if err != nil || d <= 0 {
		t.Fatalf("%s = %q, want a positive duration such as 10m", name, s)
	}
	return d
}

// soakFixture is a file the uploaders send. An audio fixture becomes ready with renditions, the aac_128k one with
// channels channels; any other is rejected with the tenant-safe message rejected.
type soakFixture struct {
	name     string
	weight   int
	data     []byte
	channels uint8
	rejected string
}

func soakFixtures(t *testing.T) []*soakFixture {
	t.Helper()
	read := func(path string) []byte {
		b, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		return b
	}
	return []*soakFixture{
		{name: "dual-mono pink 10s", weight: 14, channels: 2, data: read(testutil.DualMonoPink(t))},
		{name: "uncorrelated pink 10s", weight: 14, channels: 2, data: read(testutil.UncorrelatedPink(t))},
		{name: "uncorrelated white 10s", weight: 12, channels: 2, data: read(testutil.UncorrelatedWhite(t))},
		{name: "mono pink 10s", weight: 12, channels: 1, data: read(testutil.MonoPink(t))},
		{name: "mono pink 5s", weight: 12, channels: 1, data: read(testutil.PinkClip(t, 5*time.Second))},
		{name: "mono pink 20s", weight: 10, channels: 1, data: read(testutil.PinkClip(t, 20*time.Second))},
		{name: "5.1 pink 10s", weight: 8, channels: 2, data: read(testutil.Surround(t))},
		{name: "thunder 60s", weight: 3, channels: 2, data: read(testutil.Thunder(t))},
		{name: "text named .wav", weight: 6, rejected: audio.MessageNotAudio, data: read(testutil.NotAudio(t))},
		{name: "silence 10s", weight: 5, rejected: audio.MessageSilent, data: read(testutil.Silence(t))},
		{name: "mono pink 1s", weight: 4, rejected: audio.MessageTooShort, data: read(testutil.PinkClip(t, time.Second))},
	}
}

// soakKind is how an upload's idempotency key is used.
type soakKind string

const (
	kindSingle     soakKind = "single"
	kindReplay     soakKind = "replayed"
	kindConcurrent soakKind = "concurrent"
	kindReused     soakKind = "reused key"
)

func pickKind(rng *rand.Rand) soakKind {
	switch n := rng.IntN(100); {
	case n < 80:
		return kindSingle
	case n < 90:
		return kindReplay
	case n < 95:
		return kindConcurrent
	default:
		return kindReused
	}
}

type soakTenant struct {
	slug   string
	apiKey string
	// secret is nil for a tenant without a webhook.
	secret []byte
}

// soakUpload is a logical upload and the asset it got.
type soakUpload struct {
	tenant  *soakTenant
	key     string
	title   string
	fixture *soakFixture
	kind    soakKind
}

type soakResponse struct {
	status   int
	replayed bool
	body     []byte
}

type soakDriver struct {
	cfg      soakConfig
	client   *http.Client
	pool     *sql.DB
	fixtures []*soakFixture
	tenants  []*soakTenant
	receiver *soakReceiver

	mu       sync.Mutex
	uploads  map[string]*soakUpload // by asset ID
	logical  int
	kinds    map[string]int
	statuses map[string]int
	samples  map[string]string // an example error for each kind of retried failure
	problems map[string][]string
	result   *soakResult
}

func (d *soakDriver) problem(inv, format string, args ...any) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.problems[inv] = append(d.problems[inv], fmt.Sprintf(format, args...))
}

func TestFullStackSoak(t *testing.T) {
	cfg := loadSoakConfig(t)
	started := time.Now()
	d := &soakDriver{
		cfg:      cfg,
		client:   &http.Client{Timeout: 2 * time.Minute},
		fixtures: soakFixtures(t),
		receiver: newSoakReceiver(),
		uploads:  map[string]*soakUpload{},
		kinds:    map[string]int{},
		statuses: map[string]int{},
		samples:  map[string]string{},
		problems: map[string][]string{},
	}

	ln, err := net.Listen("tcp", cfg.listen)
	if err != nil {
		t.Fatalf("error listening on %s: %v", cfg.listen, err)
	}
	srv := &http.Server{Handler: d.receiver, ReadHeaderTimeout: 10 * time.Second}
	go srv.Serve(ln)
	t.Cleanup(func() { srv.Close() })
	t.Logf("soak: receiver listening on %s; waiting for the tenants in %s", ln.Addr(), d.tenantsDir())

	d.tenants = d.waitTenants(t)
	d.receiver.setTenants(d.tenants)
	gdb, err := db.Open(cfg.dsn, testutil.Logger(t))
	if err != nil {
		t.Fatalf("db.Open() error = %v", err)
	}
	d.pool, err = gdb.DB()
	if err != nil {
		t.Fatalf("error getting the database pool: %v", err)
	}
	t.Cleanup(func() { d.pool.Close() })
	d.waitReady(t)

	t.Logf("soak: uploading for %v as %d tenants", cfg.duration, len(d.tenants))
	uploadsStarted := time.Now()
	d.uploadFor(t, cfg.duration)
	t.Logf("soak: uploads stopped after %v; waiting up to %v for everything to finish",
		time.Since(uploadsStarted).Round(time.Second), cfg.drainTimeout)
	drainStarted := time.Now()
	d.drain(t)
	drained := time.Since(drainStarted).Round(time.Second)
	t.Logf("soak: everything finished %v after uploads stopped; checking", drained)

	d.check(t)
	d.report(t, time.Since(started).Round(time.Second), drained)
}

func (d *soakDriver) tenantsDir() string { return filepath.Join(d.cfg.out, "tenants") }

// waitTenants waits for soak.sh to create the tenants and reads their credentials: one file per tenant, named after
// its slug with tenant create's output in it, and a file named ready once they are all there.
func (d *soakDriver) waitTenants(t *testing.T) []*soakTenant {
	t.Helper()
	dir := d.tenantsDir()
	soakWaitUntil(t, soakStartTimeout, "the tenants", func() bool {
		_, err := os.Stat(filepath.Join(dir, "ready"))
		return err == nil
	})
	paths, err := filepath.Glob(filepath.Join(dir, "*.creds"))
	if err != nil || len(paths) == 0 {
		t.Fatalf("no tenant credentials in %s: %v", dir, err)
	}
	var tenants []*soakTenant
	for _, path := range paths {
		b, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		tn := &soakTenant{slug: strings.TrimSuffix(filepath.Base(path), ".creds")}
		for line := range strings.Lines(string(b)) {
			line = strings.TrimSpace(line)
			if v, ok := strings.CutPrefix(line, "api_key: "); ok {
				tn.apiKey = v
			}
			if v, ok := strings.CutPrefix(line, "webhook_secret: "); ok {
				tn.secret = []byte(v)
			}
		}
		if !strings.HasPrefix(tn.apiKey, "sm_") {
			t.Fatalf("%s has no API key", path)
		}
		tenants = append(tenants, tn)
	}
	return tenants
}

func (d *soakDriver) waitReady(t *testing.T) {
	t.Helper()
	soakWaitUntil(t, soakStartTimeout, "the API to be ready", func() bool {
		req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, d.cfg.api+"/readyz", nil)
		if err != nil {
			return false
		}
		resp, err := d.client.Do(req)
		if err != nil {
			return false
		}
		resp.Body.Close()
		return resp.StatusCode == http.StatusOK
	})
}

func soakWaitUntil(t *testing.T, timeout time.Duration, what string, ok func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !ok() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out after %v waiting for %s", timeout, what)
		}
		soakPause(t.Context(), time.Second)
	}
}

// uploadFor runs the uploaders for d. An upload that has started when d ends is finished.
func (d *soakDriver) uploadFor(t *testing.T, duration time.Duration) {
	ctx, cancel := context.WithTimeout(t.Context(), duration)
	defer cancel()
	progress := time.NewTicker(time.Minute)
	defer progress.Stop()
	var wg sync.WaitGroup
	for i, tn := range d.tenants {
		for j := range soakUploadersPerTenant {
			seed := uint64(i*soakUploadersPerTenant + j + 1)
			wg.Go(func() { d.uploader(ctx, tn, rand.New(rand.NewPCG(seed, seed))) })
		}
	}
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	for {
		select {
		case <-done:
			return
		case <-progress.C:
			d.mu.Lock()
			logical, assets := d.logical, len(d.uploads)
			d.mu.Unlock()
			t.Logf("soak: %d uploads, %d assets; unfinished: %s", logical, assets, d.unfinishedText(t.Context()))
		}
	}
}

func (d *soakDriver) uploader(ctx context.Context, tn *soakTenant, rng *rand.Rand) {
	for d.waitBacklog(ctx) {
		f := d.pick(rng)
		u := &soakUpload{tenant: tn, key: uuid.NewV7().String(), fixture: f, kind: pickKind(rng)}
		u.title = "Soak " + f.name + " " + u.key[len(u.key)-12:]
		d.run(context.WithoutCancel(ctx), u, rng)
	}
}

// waitBacklog waits until fewer than soakMaxBacklog assets are unfinished, and reports false once ctx is done.
func (d *soakDriver) waitBacklog(ctx context.Context) bool {
	for ctx.Err() == nil {
		var n int
		err := d.pool.QueryRowContext(ctx, "SELECT COUNT(*) FROM assets WHERE status IN ('queued', 'processing')").
			Scan(&n)
		if err == nil && n < soakMaxBacklog {
			return true
		}
		soakPause(ctx, time.Second)
	}
	return false
}

func (d *soakDriver) pick(rng *rand.Rand) *soakFixture {
	total := 0
	for _, f := range d.fixtures {
		total += f.weight
	}
	n := rng.IntN(total)
	for _, f := range d.fixtures {
		if n < f.weight {
			return f
		}
		n -= f.weight
	}
	return d.fixtures[len(d.fixtures)-1]
}

// run uploads u as its kind says and records the asset it got.
func (d *soakDriver) run(ctx context.Context, u *soakUpload, rng *rand.Rand) {
	d.mu.Lock()
	d.logical++
	d.kinds[string(u.kind)]++
	d.mu.Unlock()

	var first soakResponse
	var err error
	if u.kind == kindConcurrent {
		var other soakResponse
		var otherErr error
		var wg sync.WaitGroup
		wg.Go(func() { other, otherErr = d.send(ctx, u.tenant, u.key, u.title, u.fixture) })
		first, err = d.send(ctx, u.tenant, u.key, u.title, u.fixture)
		wg.Wait()
		if err == nil && otherErr == nil {
			a, b := d.accepted(u, first), d.accepted(u, other)
			if a != "" && b != "" && a != b {
				d.problem(invIdempotent, "concurrent uploads with key %s got assets %s and %s", u.key, a, b)
			}
			if a != "" && b != "" && first.replayed == other.replayed {
				d.problem(invIdempotent, "concurrent uploads with key %s both answered Idempotent-Replayed %t",
					u.key, first.replayed)
			}
		}
		err = errors.Join(err, otherErr)
	} else {
		first, err = d.send(ctx, u.tenant, u.key, u.title, u.fixture)
	}
	if err != nil {
		d.problem(invAccepted, "upload %s with key %s: %v", u.title, u.key, err)
		return
	}
	id := d.accepted(u, first)
	if id == "" {
		return
	}

	switch u.kind {
	case kindReplay:
		again, err := d.send(ctx, u.tenant, u.key, u.title, u.fixture)
		if err != nil {
			d.problem(invAccepted, "replay of key %s: %v", u.key, err)
			return
		}
		if got := d.accepted(u, again); got != "" && (got != id || !again.replayed) {
			d.problem(invIdempotent, "replay of key %s got asset %s with Idempotent-Replayed %t, want %s replayed",
				u.key, got, again.replayed, id)
		}
	case kindReused:
		other := u.fixture
		for other == u.fixture {
			other = d.pick(rng)
		}
		resp, err := d.send(ctx, u.tenant, u.key, u.title+" again", other)
		if err != nil {
			d.problem(invAccepted, "reuse of key %s: %v", u.key, err)
			return
		}
		var e apiv1.ErrorResponse
		if resp.status != http.StatusUnprocessableEntity || json.Unmarshal(resp.body, &e) != nil ||
			e.Error.Code != "idempotency_key_reused" {
			d.problem(invIdempotent, "reuse of key %s for another upload = %d %s, want 422 idempotency_key_reused",
				u.key, resp.status, resp.body)
		}
	}
}

// accepted checks that resp accepted u, records the asset it got, and returns its ID, or "" if it wasn't accepted.
func (d *soakDriver) accepted(u *soakUpload, resp soakResponse) string {
	if resp.status != http.StatusAccepted {
		d.problem(invAccepted, "upload %s with key %s = %d %s, want 202", u.title, u.key, resp.status, resp.body)
		return ""
	}
	var a apiv1.Asset
	if err := json.Unmarshal(resp.body, &a); err != nil || a.ID == "" {
		d.problem(invAccepted, "upload %s with key %s: no asset in %s", u.title, u.key, resp.body)
		return ""
	}
	if a.IdempotencyKey == nil || *a.IdempotencyKey != u.key || a.Title != u.title {
		d.problem(invIdempotent, "upload with key %s and title %q got asset %s with key %v and title %q",
			u.key, u.title, a.ID, a.IdempotencyKey, a.Title)
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if prev, ok := d.uploads[a.ID]; ok && prev != u {
		d.problems[invIdempotent] = append(d.problems[invIdempotent],
			fmt.Sprintf("keys %s and %s both got asset %s", prev.key, u.key, a.ID))
	}
	d.uploads[a.ID] = u
	return a.ID
}

// send posts an upload, retrying with the same key through connection errors and 429s for up to soakRetryFor,
// and returns the first other answer.
func (d *soakDriver) send(ctx context.Context, tn *soakTenant, key, title string, f *soakFixture) (soakResponse, error) {
	deadline := time.Now().Add(soakRetryFor)
	for {
		resp, err := d.post(ctx, tn, key, title, f)
		var kind string
		switch {
		case err != nil:
			kind = "connection error"
		case resp.status == http.StatusTooManyRequests:
			kind = "429"
		default:
			d.count(strconv.Itoa(resp.status), "")
			return resp, nil
		}
		sample := fmt.Sprint(err)
		if err == nil {
			sample = string(resp.body)
		}
		d.count(kind, sample)
		if time.Now().After(deadline) {
			return resp, fmt.Errorf("still failing after %v, last with %s: %s", soakRetryFor, kind, sample)
		}
		soakPause(ctx, 500*time.Millisecond+rand.N(time.Second))
	}
}

func (d *soakDriver) count(status, sample string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.statuses[status]++
	if sample != "" && d.samples[status] == "" {
		d.samples[status] = sample
	}
}

func (d *soakDriver) post(ctx context.Context, tn *soakTenant, key, title string, f *soakFixture) (soakResponse, error) {
	var body bytes.Buffer
	mw := multipart.NewWriter(&body)
	if err := mw.WriteField("title", title); err != nil {
		return soakResponse{}, err
	}
	part, err := mw.CreateFormFile("file", strings.ReplaceAll(f.name, " ", "-")+".wav")
	if err != nil {
		return soakResponse{}, err
	}
	if _, err := part.Write(f.data); err != nil {
		return soakResponse{}, err
	}
	if err := mw.Close(); err != nil {
		return soakResponse{}, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, d.cfg.api+"/v1/assets", &body)
	if err != nil {
		return soakResponse{}, err
	}
	req.Header.Set("Content-Type", mw.FormDataContentType())
	req.Header.Set("Authorization", "Bearer "+tn.apiKey)
	req.Header.Set("Idempotency-Key", key)
	resp, err := d.client.Do(req)
	if err != nil {
		return soakResponse{}, err
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return soakResponse{}, err
	}
	return soakResponse{status: resp.StatusCode, replayed: resp.Header.Get("Idempotent-Replayed") == "true", body: b}, nil
}

func (d *soakDriver) get(ctx context.Context, tn *soakTenant, path string, header http.Header) (int, http.Header,
	[]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, d.cfg.api+path, nil)
	if err != nil {
		return 0, nil, nil, err
	}
	maps.Copy(req.Header, header)
	req.Header.Set("Authorization", "Bearer "+tn.apiKey)
	resp, err := d.client.Do(req)
	if err != nil {
		return 0, nil, nil, err
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	return resp.StatusCode, resp.Header, b, err
}

const soakUnfinishedQuery = "SELECT " +
	"(SELECT COUNT(*) FROM assets WHERE status NOT IN ('ready', 'failed')), " +
	"(SELECT COUNT(*) FROM asset_events WHERE relayed_at IS NULL), " +
	"(SELECT COUNT(*) FROM webhook_deliveries WHERE state NOT IN ('delivered', 'failed'))"

func (d *soakDriver) unfinished(ctx context.Context) (assets, events, deliveries int, err error) {
	err = d.pool.QueryRowContext(ctx, soakUnfinishedQuery).Scan(&assets, &events, &deliveries)
	return assets, events, deliveries, err
}

func (d *soakDriver) unfinishedText(ctx context.Context) string {
	a, e, dl, err := d.unfinished(ctx)
	if err != nil {
		return err.Error()
	}
	return fmt.Sprintf("%d assets, %d unrelayed events, %d deliveries", a, e, dl)
}

// drain waits until every asset and delivery has finished and every event has been relayed.
func (d *soakDriver) drain(t *testing.T) {
	t.Helper()
	deadline := time.Now().Add(d.cfg.drainTimeout)
	lastLog := time.Now()
	for {
		a, e, dl, err := d.unfinished(t.Context())
		switch {
		case err != nil:
			t.Logf("soak: error counting unfinished work: %v", err)
		case a+e+dl == 0:
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("still unfinished after %v: %s", d.cfg.drainTimeout, d.unfinishedText(t.Context()))
		}
		if time.Since(lastLog) >= 30*time.Second {
			lastLog = time.Now()
			t.Logf("soak: unfinished: %s", d.unfinishedText(t.Context()))
		}
		soakPause(t.Context(), 2*time.Second)
	}
}

// soakAnswer is how the receiver answers an event's webhooks, chosen by a hash of the event ID so that every
// attempt at an event is answered consistently.
type soakAnswer string

const (
	answerOK          soakAnswer = "2xx"
	answerErrorOnce   soakAnswer = "500 once"
	answerUnavailable soakAnswer = "503 twice"
	answerRedirect    soakAnswer = "302 once"
	answerSlow        soakAnswer = "2xx after 2-4s"
	answerHang        soakAnswer = "past the client's timeout once"
	answerGone        soakAnswer = "410"
)

func soakBucket(eventID string) int {
	h := fnv.New32a()
	h.Write([]byte(eventID))
	return int(h.Sum32() % 100)
}

func soakAnswerFor(eventID string) soakAnswer {
	switch b := soakBucket(eventID); {
	case b < 55:
		return answerOK
	case b < 67:
		return answerErrorOnce
	case b < 75:
		return answerUnavailable
	case b < 80:
		return answerRedirect
	case b < 90:
		return answerSlow
	case b < 96:
		return answerHang
	default:
		return answerGone
	}
}

// respond returns the status for an event's attempt-th webhook and how long to wait before answering.
func soakRespond(eventID string, attempt int) (int, time.Duration) {
	b := soakBucket(eventID)
	ok := []int{http.StatusOK, http.StatusAccepted, http.StatusNoContent}[b%3]
	switch soakAnswerFor(eventID) {
	case answerErrorOnce:
		if attempt == 1 {
			return http.StatusInternalServerError, 0
		}
	case answerUnavailable:
		if attempt <= 2 {
			return http.StatusServiceUnavailable, 0
		}
	case answerRedirect:
		if attempt == 1 {
			return http.StatusFound, 0
		}
	case answerSlow:
		return ok, 2*time.Second + time.Duration(b%3)*time.Second
	case answerHang:
		if attempt == 1 {
			return ok, soakHang
		}
	case answerGone:
		return http.StatusGone, 0
	}
	return ok, 0
}

// soakHit is one webhook request as the receiver got and answered it.
type soakHit struct {
	slug   string
	signed bool
	event  apiv1.Event
	status int
	// aborted means the client went away before the receiver answered.
	aborted bool
}

type soakReceiver struct {
	mu       sync.Mutex
	tenants  map[string]*soakTenant
	arrivals map[string]int // by event ID
	hits     map[string][]soakHit
	total    int
	problems []string
}

func newSoakReceiver() *soakReceiver {
	return &soakReceiver{tenants: map[string]*soakTenant{}, arrivals: map[string]int{}, hits: map[string][]soakHit{}}
}

func (r *soakReceiver) setTenants(tenants []*soakTenant) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, tn := range tenants {
		r.tenants[tn.slug] = tn
	}
}

func (r *soakReceiver) problem(format string, args ...any) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.problems = append(r.problems, fmt.Sprintf(format, args...))
}

func (r *soakReceiver) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	slug, _ := strings.CutPrefix(req.URL.Path, "/hooks/")
	body, err := io.ReadAll(req.Body)
	if err != nil {
		r.problem("error reading a webhook for %s: %v", slug, err)
		return
	}
	r.mu.Lock()
	tn := r.tenants[slug]
	r.mu.Unlock()
	if req.Method != http.MethodPost || tn == nil || tn.secret == nil {
		r.problem("%s %s from %s isn't a webhook for a tenant with one: %s", req.Method, req.URL.Path,
			req.UserAgent(), body)
		w.WriteHeader(http.StatusNotFound)
		return
	}

	hit := soakHit{slug: slug}
	if err := webhook.Verify(tn.secret, req.Header.Get(webhook.SignatureHeader), body, time.Now(),
		soakSignatureTolerance); err != nil {
		r.problem("webhook for %s doesn't verify: %v", slug, err)
	} else {
		hit.signed = true
	}
	if err := json.Unmarshal(body, &hit.event); err != nil || hit.event.ID == "" {
		r.problem("webhook for %s isn't an event: %s", slug, body)
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	if got := req.Header.Get(webhook.EventIDHeader); got != hit.event.ID {
		r.problem("webhook %s for %s has %s %q", hit.event.ID, slug, webhook.EventIDHeader, got)
	}
	if got := req.Header.Get("Content-Type"); got != "application/json" {
		r.problem("webhook %s for %s has Content-Type %q", hit.event.ID, slug, got)
	}
	if got := req.UserAgent(); got != "slumber-mill-webhooks/1" {
		r.problem("webhook %s for %s has User-Agent %q", hit.event.ID, slug, got)
	}

	r.mu.Lock()
	r.arrivals[hit.event.ID]++
	attempt := r.arrivals[hit.event.ID]
	r.mu.Unlock()
	status, wait := soakRespond(hit.event.ID, attempt)
	if wait > 0 {
		timer := time.NewTimer(wait)
		select {
		case <-req.Context().Done():
			hit.aborted = true
		case <-timer.C:
		}
		timer.Stop()
	}
	if !hit.aborted {
		hit.status = status
	}
	r.mu.Lock()
	r.hits[hit.event.ID] = append(r.hits[hit.event.ID], hit)
	r.total++
	r.mu.Unlock()
	if hit.aborted {
		return
	}
	if status == http.StatusFound {
		w.Header().Set("Location", "/elsewhere")
	}
	w.WriteHeader(status)
}

func (r *soakReceiver) snapshot() (map[string][]soakHit, int, []string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	hits := make(map[string][]soakHit, len(r.hits))
	for id, hs := range r.hits {
		hits[id] = slices.Clone(hs)
	}
	return hits, r.total, slices.Clone(r.problems)
}

// soakAsset is an asset row with what the checks need.
type soakAsset struct {
	id, slug, key, status, errorMsg, lastError string
	tenantID                                   uint64
	attempts                                   int
	measured                                   bool
	target                                     float64
	keyNull                                    bool
}

type soakDelivery struct {
	eventID, slug, state string
	attempts             int
}

type soakEvent struct {
	eventID, typ, assetID string
}

// soakResult is what the checks found, for the report.
type soakResult struct {
	assets      map[string]*soakAsset
	deliveries  map[string]*soakDelivery
	events      map[string]*soakEvent
	hits        map[string][]soakHit
	hitTotal    int
	exhausted   int
	downloads   int
	problems    map[string][]string
	invariantOK map[string]bool
}

func (d *soakDriver) check(t *testing.T) {
	t.Helper()
	ctx := t.Context()
	res := &soakResult{problems: map[string][]string{}}
	d.mu.Lock()
	for inv, ps := range d.problems {
		res.problems[inv] = slices.Clone(ps)
	}
	d.mu.Unlock()
	add := func(inv, format string, args ...any) {
		res.problems[inv] = append(res.problems[inv], fmt.Sprintf(format, args...))
	}

	sqlInvariants := []struct{ inv, query string }{
		{invFinished, "SELECT BIN_TO_UUID(public_id) AS asset, status FROM assets WHERE status NOT IN ('ready', 'failed')"},
		{invOneEvent, "SELECT BIN_TO_UUID(a.public_id) AS asset, a.status, " +
			"GROUP_CONCAT(e.type ORDER BY e.id) AS events " +
			"FROM assets a LEFT JOIN asset_events e ON e.asset_id = a.id GROUP BY a.id " +
			"HAVING COUNT(e.id) != 1 OR COALESCE(SUM(e.type = CONCAT('asset.', a.status)), 0) != 1"},
		{invRenditions, "SELECT BIN_TO_UUID(a.public_id) AS asset, a.status, COUNT(r.id) AS renditions, " +
			"GROUP_CONCAT(r.profile ORDER BY r.profile) AS profiles " +
			"FROM assets a LEFT JOIN renditions r ON r.asset_id = a.id GROUP BY a.id " +
			"HAVING COUNT(r.id) != IF(a.status = 'ready', 2, 0) " +
			"OR (COUNT(r.id) = 2 AND COUNT(DISTINCT r.profile) != 2)"},
		{invDelivery, "SELECT BIN_TO_UUID(MAX(e.event_id)) AS event, MAX(e.relayed_at) AS relayed_at, " +
			"MAX(t.webhook_url IS NOT NULL) AS hooked, COUNT(d.id) AS deliveries " +
			"FROM asset_events e JOIN tenants t ON t.id = e.tenant_id " +
			"LEFT JOIN webhook_deliveries d ON d.event_id = e.event_id GROUP BY e.id " +
			"HAVING MAX(e.relayed_at) IS NULL OR COUNT(d.id) != MAX(t.webhook_url IS NOT NULL)"},
		{invDelivered, "SELECT BIN_TO_UUID(d.event_id) AS event, d.state, e.id AS event_row " +
			"FROM webhook_deliveries d LEFT JOIN asset_events e ON e.event_id = d.event_id " +
			"WHERE e.id IS NULL OR e.tenant_id != d.tenant_id OR d.state NOT IN ('delivered', 'failed')"},
		{invLeased, "SELECT 'assets' AS tbl, BIN_TO_UUID(public_id) AS id, status AS state, attempts, locked_by, " +
			"lease_expires_at FROM assets " +
			"WHERE status = 'processing' OR locked_by IS NOT NULL OR lease_expires_at IS NOT NULL " +
			"UNION ALL SELECT 'webhook_deliveries', BIN_TO_UUID(event_id), state, attempts, locked_by, " +
			"lease_expires_at FROM webhook_deliveries " +
			"WHERE state = 'sending' OR locked_by IS NOT NULL OR lease_expires_at IS NOT NULL"},
		{invAttempts, fmt.Sprintf("SELECT 'assets' AS tbl, BIN_TO_UUID(public_id) AS id, attempts FROM assets "+
			"WHERE attempts NOT BETWEEN 1 AND %d "+
			"UNION ALL SELECT 'webhook_deliveries', BIN_TO_UUID(event_id), attempts FROM webhook_deliveries "+
			"WHERE attempts NOT BETWEEN 1 AND %d", soakMaxAssetAttempts, soakMaxDeliveryAttempts)},
	}
	for _, q := range sqlInvariants {
		for _, row := range soakRows(t, d.pool, q.query) {
			add(q.inv, "%s", row)
		}
	}

	res.assets = d.loadAssets(t)
	d.checkAssets(res, add)
	d.checkRenditions(t, res, add)
	d.checkDownloads(t, res, add)

	res.deliveries, res.events = d.loadDeliveries(t)
	var receiverProblems []string
	res.hits, res.hitTotal, receiverProblems = d.receiver.snapshot()
	for _, p := range receiverProblems {
		add(invSigned, "%s", p)
	}
	d.checkReceived(res, add)
	d.checkPayloads(ctx, res, add)

	res.invariantOK = map[string]bool{}
	for _, inv := range soakInvariants {
		ps := res.problems[inv]
		res.invariantOK[inv] = len(ps) == 0
		if len(ps) > 0 {
			t.Errorf("FAIL %s: %d problems, such as %s", inv, len(ps), strings.Join(ps[:min(len(ps), 10)], "; "))
		}
	}
	d.result = res
}

func (d *soakDriver) loadAssets(t *testing.T) map[string]*soakAsset {
	t.Helper()
	rows, err := d.pool.QueryContext(t.Context(), "SELECT BIN_TO_UUID(a.public_id), a.tenant_id, t.slug, "+
		"a.idempotency_key IS NULL, COALESCE(a.idempotency_key, ''), a.status, a.attempts, COALESCE(a.error, ''), "+
		"COALESCE(a.last_error, ''), a.input_lufs IS NOT NULL AND a.input_true_peak_dbtp IS NOT NULL "+
		"AND a.duration_ms IS NOT NULL, a.target_lufs "+
		"FROM assets a JOIN tenants t ON t.id = a.tenant_id")
	if err != nil {
		t.Fatalf("error reading the assets: %v", err)
	}
	defer rows.Close()
	assets := map[string]*soakAsset{}
	for rows.Next() {
		a := &soakAsset{}
		if err := rows.Scan(&a.id, &a.tenantID, &a.slug, &a.keyNull, &a.key, &a.status, &a.attempts, &a.errorMsg,
			&a.lastError, &a.measured, &a.target); err != nil {
			t.Fatalf("error reading the assets: %v", err)
		}
		assets[a.id] = a
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("error reading the assets: %v", err)
	}
	return assets
}

func (d *soakDriver) checkAssets(res *soakResult, add func(inv, format string, args ...any)) {
	d.mu.Lock()
	uploads := maps.Clone(d.uploads)
	d.mu.Unlock()
	for id, u := range uploads {
		if res.assets[id] == nil {
			add(invUploaded, "asset %s, uploaded with key %s, isn't in the database", id, u.key)
		}
	}
	for id, a := range res.assets {
		u := uploads[id]
		if u == nil {
			add(invUploaded, "asset %s of %s with key %q wasn't uploaded by the soak", id, a.slug, a.key)
			continue
		}
		if a.slug != u.tenant.slug || a.keyNull || a.key != u.key {
			add(invUploaded, "asset %s is %s's with key %q, want %s's with key %s", id, a.slug, a.key,
				u.tenant.slug, u.key)
		}
		if a.status != "ready" && a.status != "failed" {
			continue
		}
		if u.fixture.rejected != "" {
			if a.status != "failed" || a.errorMsg != u.fixture.rejected || a.attempts != 1 {
				add(invRejected, "asset %s (%s) is %s on attempt %d with error %q, want failed on attempt 1 with %q",
					id, u.fixture.name, a.status, a.attempts, a.errorMsg, u.fixture.rejected)
			}
			continue
		}
		switch {
		case a.status == "ready" && a.measured && a.errorMsg == "":
		case a.status == "failed" && a.errorMsg == soakFailedMessage && a.attempts == soakMaxAssetAttempts &&
			a.lastError == soakReapMessage:
			res.exhausted++
		default:
			add(invAudio, "asset %s (%s) is %s on attempt %d with error %q (internal: %q) and measurements %t",
				id, u.fixture.name, a.status, a.attempts, a.errorMsg, a.lastError, a.measured)
		}
	}
}

func (d *soakDriver) checkRenditions(t *testing.T, res *soakResult, add func(inv, format string, args ...any)) {
	t.Helper()
	rows, err := d.pool.QueryContext(t.Context(), "SELECT BIN_TO_UUID(a.public_id), r.profile, r.codec, "+
		"r.bitrate_kbps, r.channels, r.bytes, HEX(r.sha256), r.lufs, r.true_peak_dbtp "+
		"FROM renditions r JOIN assets a ON a.id = r.asset_id")
	if err != nil {
		t.Fatalf("error reading the renditions: %v", err)
	}
	defer rows.Close()
	d.mu.Lock()
	uploads := maps.Clone(d.uploads)
	d.mu.Unlock()
	for rows.Next() {
		var id, profile, codec, sum string
		var kbps, channels int
		var size int64
		var lufs, peak float64
		if err := rows.Scan(&id, &profile, &codec, &kbps, &channels, &size, &sum, &lufs, &peak); err != nil {
			t.Fatalf("error reading the renditions: %v", err)
		}
		a, u := res.assets[id], uploads[id]
		if a == nil || u == nil {
			continue
		}
		want := map[string]struct{ channels, kbps int }{
			"aac_128k": {int(u.fixture.channels), 128},
			"aac_64k":  {1, 64},
		}[profile]
		if codec != "aac" || channels != want.channels || kbps != want.kbps {
			add(invInSpec, "asset %s (%s) %s is %s, %d channels, %d kbps; want aac, %d channels, %d kbps",
				id, u.fixture.name, profile, codec, channels, kbps, want.channels, want.kbps)
		}
		if math.Abs(lufs-a.target) > soakToleranceLU || peak > soakCeilingDBTP {
			add(invInSpec, "asset %s (%s) %s measures %.2f LUFS and %.2f dBTP, want %.1f ± %.0f LUFS and at most %.1f dBTP",
				id, u.fixture.name, profile, lufs, peak, a.target, soakToleranceLU, soakCeilingDBTP)
		}
		path := filepath.Join(d.cfg.dataDir, "renditions", strconv.FormatUint(a.tenantID, 10), id, profile+".m4a")
		b, err := os.ReadFile(path)
		if err != nil {
			add(invInSpec, "asset %s %s: %v", id, profile, err)
			continue
		}
		got := sha256.Sum256(b)
		if int64(len(b)) != size || !strings.EqualFold(hex.EncodeToString(got[:]), sum) {
			add(invInSpec, "asset %s %s file has %d bytes hashing to %x, want the recorded %d bytes hashing to %s",
				id, profile, len(b), got, size, sum)
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("error reading the renditions: %v", err)
	}
}

// checkDownloads downloads every soakDownloadEvery-th ready asset's renditions through the API, whole and by range.
func (d *soakDriver) checkDownloads(t *testing.T, res *soakResult, add func(inv, format string, args ...any)) {
	t.Helper()
	d.mu.Lock()
	uploads := maps.Clone(d.uploads)
	d.mu.Unlock()
	ids := slices.Sorted(maps.Keys(res.assets))
	n := 0
	for _, id := range ids {
		a, u := res.assets[id], uploads[id]
		if a.status != "ready" || u == nil {
			continue
		}
		n++
		if n%soakDownloadEvery != 0 {
			continue
		}
		status, _, body, err := d.get(t.Context(), u.tenant, "/v1/assets/"+id, nil)
		var got apiv1.Asset
		if err != nil || status != http.StatusOK || json.Unmarshal(body, &got) != nil {
			add(invDownload, "GET asset %s = %d %v %s", id, status, err, body)
			continue
		}
		for _, r := range got.Renditions {
			res.downloads++
			path := "/v1/assets/" + id + "/renditions/" + r.Profile
			status, _, full, err := d.get(t.Context(), u.tenant, path, nil)
			sum := sha256.Sum256(full)
			if err != nil || status != http.StatusOK || uint64(len(full)) != r.Bytes || hex.EncodeToString(sum[:]) != r.SHA256 {
				add(invDownload, "GET %s = %d %v with %d bytes, want 200 with the recorded %d bytes", path, status, err,
					len(full), r.Bytes)
				continue
			}
			first, last := len(full)/3, len(full)/3+4095
			status, header, part, err := d.get(t.Context(), u.tenant, path,
				http.Header{"Range": {fmt.Sprintf("bytes=%d-%d", first, last)}})
			if err != nil || status != http.StatusPartialContent || !bytes.Equal(part, full[first:last+1]) ||
				header.Get("Content-Range") != fmt.Sprintf("bytes %d-%d/%d", first, last, len(full)) {
				add(invDownload, "ranged GET %s = %d %v with %d bytes and Content-Range %q", path, status, err, len(part),
					header.Get("Content-Range"))
			}
		}
	}
}

func (d *soakDriver) loadDeliveries(t *testing.T) (map[string]*soakDelivery, map[string]*soakEvent) {
	t.Helper()
	deliveries := map[string]*soakDelivery{}
	rows, err := d.pool.QueryContext(t.Context(), "SELECT BIN_TO_UUID(d.event_id), t.slug, d.state, d.attempts "+
		"FROM webhook_deliveries d JOIN tenants t ON t.id = d.tenant_id")
	if err != nil {
		t.Fatalf("error reading the deliveries: %v", err)
	}
	for rows.Next() {
		dl := &soakDelivery{}
		if err := rows.Scan(&dl.eventID, &dl.slug, &dl.state, &dl.attempts); err != nil {
			t.Fatalf("error reading the deliveries: %v", err)
		}
		deliveries[dl.eventID] = dl
	}
	if err := errors.Join(rows.Err(), rows.Close()); err != nil {
		t.Fatalf("error reading the deliveries: %v", err)
	}

	events := map[string]*soakEvent{}
	rows, err = d.pool.QueryContext(t.Context(), "SELECT BIN_TO_UUID(e.event_id), e.type, BIN_TO_UUID(a.public_id) "+
		"FROM asset_events e JOIN assets a ON a.id = e.asset_id")
	if err != nil {
		t.Fatalf("error reading the events: %v", err)
	}
	for rows.Next() {
		e := &soakEvent{}
		if err := rows.Scan(&e.eventID, &e.typ, &e.assetID); err != nil {
			t.Fatalf("error reading the events: %v", err)
		}
		events[e.eventID] = e
	}
	if err := errors.Join(rows.Err(), rows.Close()); err != nil {
		t.Fatalf("error reading the events: %v", err)
	}
	return deliveries, events
}

// checkReceived compares the deliveries with what the receiver saw and how it answered.
func (d *soakDriver) checkReceived(res *soakResult, add func(inv, format string, args ...any)) {
	for id, dl := range res.deliveries {
		hits := res.hits[id]
		answered2xx, answered410 := false, 0
		for _, h := range hits {
			if h.signed && h.status >= 200 && h.status <= 299 {
				answered2xx = true
			}
			if h.status == http.StatusGone {
				answered410++
			}
		}
		switch want := soakAnswerFor(id); {
		case want == answerGone:
			if dl.state != "failed" || dl.attempts != 1 || answered410 == 0 {
				add(invReceived, "delivery %s answered 410 is %s after %d attempts and %d webhooks, "+
					"want failed after 1", id, dl.state, dl.attempts, len(hits))
			}
		case dl.state != "delivered" || !answered2xx:
			add(invReceived, "delivery %s (%s) is %s after %d attempts, and the receiver got %d webhooks for it, "+
				"answered 2xx %t; want delivered and answered 2xx", id, want, dl.state, dl.attempts, len(hits),
				answered2xx)
		}
	}
	for id, hits := range res.hits {
		dl := res.deliveries[id]
		if dl == nil {
			add(invUnknown, "the receiver got %d webhooks for event %s, which has no delivery", len(hits), id)
			continue
		}
		for _, h := range hits {
			if h.slug != dl.slug {
				add(invSigned, "event %s of %s reached %s's endpoint", id, dl.slug, h.slug)
			}
		}
	}
}

// checkPayloads compares each delivered webhook's asset with the API's asset, which is final by now.
func (d *soakDriver) checkPayloads(ctx context.Context, res *soakResult, add func(inv, format string, args ...any)) {
	d.mu.Lock()
	uploads := maps.Clone(d.uploads)
	d.mu.Unlock()
	for _, id := range slices.Sorted(maps.Keys(res.hits)) {
		var ev *apiv1.Event
		for _, h := range res.hits[id] {
			if h.signed && h.status >= 200 && h.status <= 299 {
				ev = &h.event
				break
			}
		}
		if ev == nil {
			continue
		}
		e, u := res.events[id], uploads[ev.Data.ID]
		if e == nil || u == nil {
			add(invPayload, "webhook %s is for asset %s, which isn't one of the soak's uploads with that event",
				id, ev.Data.ID)
			continue
		}
		if ev.Type != e.typ || ev.Data.ID != e.assetID {
			add(invPayload, "webhook %s is %s for asset %s, want %s for %s", id, ev.Type, ev.Data.ID, e.typ, e.assetID)
		}
		status, _, body, err := d.get(ctx, u.tenant, "/v1/assets/"+ev.Data.ID, nil)
		var got apiv1.Asset
		if err != nil || status != http.StatusOK || json.Unmarshal(body, &got) != nil {
			add(invPayload, "GET asset %s = %d %v %s", ev.Data.ID, status, err, body)
			continue
		}
		if ev.Type != "asset."+got.Status {
			add(invPayload, "webhook %s is %s, but asset %s is %s", id, ev.Type, got.ID, got.Status)
		}
		if !reflect.DeepEqual(ev.Data, got) {
			add(invPayload, "webhook %s's asset differs from the API's: %+v, API %+v", id, ev.Data, got)
		}
	}
}

// report logs the soak's counts and the invariants' results, and writes them to summary.txt in the output directory.
func (d *soakDriver) report(t *testing.T, took, drained time.Duration) {
	t.Helper()
	res := d.result
	d.mu.Lock()
	uploads := maps.Clone(d.uploads)
	logical, kinds, statuses, samples := d.logical, maps.Clone(d.kinds), maps.Clone(d.statuses), maps.Clone(d.samples)
	d.mu.Unlock()
	var lines []string
	line := func(format string, args ...any) { lines = append(lines, fmt.Sprintf(format, args...)) }
	hooked := 0
	for _, tn := range d.tenants {
		if tn.secret != nil {
			hooked++
		}
	}

	line("soak of %v uploading, then %v to finish everything; %v in all", d.cfg.duration, drained, took)
	line("tenants: %d, %d of them with a webhook", len(d.tenants), hooked)
	line("uploads: %d (%s); requests answered: %s", logical, soakCounts(kinds), soakCounts(statuses))
	for status, sample := range samples {
		line("  e.g. %s: %s", status, sample)
	}

	type tally struct{ uploads, ready, failed int }
	byFixture := map[string]*tally{}
	byStatus, byError, byAttempts := map[string]int{}, map[string]int{}, map[string]int{}
	for id, a := range res.assets {
		byStatus[a.status]++
		byAttempts[strconv.Itoa(a.attempts)]++
		if a.errorMsg != "" {
			byError[a.errorMsg]++
		}
		u := uploads[id]
		if u == nil {
			continue
		}
		if byFixture[u.fixture.name] == nil {
			byFixture[u.fixture.name] = &tally{}
		}
		f := byFixture[u.fixture.name]
		f.uploads++
		switch a.status {
		case "ready":
			f.ready++
		case "failed":
			f.failed++
		}
	}
	line("assets: %d (%s); failed with: %s; by attempts: %s; failed after every lease expired: %d",
		len(res.assets), soakCounts(byStatus), soakCounts(byError), soakCounts(byAttempts), res.exhausted)
	for _, f := range d.fixtures {
		if c := byFixture[f.name]; c != nil {
			line("  %-22s %4d assets, %4d ready, %4d failed", f.name, c.uploads, c.ready, c.failed)
		}
	}

	byType := map[string]int{}
	for _, e := range res.events {
		byType[e.typ]++
	}
	byState, deliveryAttempts := map[string]int{}, map[string]int{}
	retried := 0
	for _, dl := range res.deliveries {
		byState[dl.state]++
		deliveryAttempts[strconv.Itoa(dl.attempts)]++
		if dl.attempts > 1 {
			retried++
		}
	}
	line("events: %d (%s); deliveries: %d (%s), %d retried; by attempts: %s", len(res.events), soakCounts(byType),
		len(res.deliveries), soakCounts(byState), retried, soakCounts(deliveryAttempts))

	answers, byAnswer := map[string]int{}, map[string]int{}
	duplicates := 0
	for id, hits := range res.hits {
		byAnswer[string(soakAnswerFor(id))]++
		ok := 0
		for _, h := range hits {
			switch {
			case h.aborted:
				answers["aborted by the client"]++
			default:
				answers[strconv.Itoa(h.status)]++
			}
			if h.signed && h.status >= 200 && h.status <= 299 {
				ok++
			}
		}
		if ok > 1 {
			duplicates++
		}
	}
	line("receiver: %d webhooks for %d events; answered: %s", res.hitTotal, len(res.hits), soakCounts(answers))
	line("  events by planned answer: %s", soakCounts(byAnswer))
	line("  events answered 2xx more than once (at-least-once duplicates): %d", duplicates)
	line("downloads through the API: %d renditions, whole and by range", res.downloads)
	line("invariants:")
	for _, inv := range soakInvariants {
		if res.invariantOK[inv] {
			line("  PASS  %s", inv)
		} else {
			line("  FAIL  %s (%d problems)", inv, len(res.problems[inv]))
		}
	}

	for _, l := range lines {
		t.Logf("soak: %s", l)
	}
	summary := filepath.Join(d.cfg.out, "summary.txt")
	if err := os.WriteFile(summary, []byte(strings.Join(lines, "\n")+"\n"), 0o644); err != nil {
		t.Errorf("error writing %s: %v", summary, err)
	}
}

// soakCounts formats counts by key, largest first.
func soakCounts(m map[string]int) string {
	keys := slices.SortedFunc(maps.Keys(m), func(a, b string) int {
		if m[a] != m[b] {
			return m[b] - m[a]
		}
		return strings.Compare(a, b)
	})
	if len(keys) == 0 {
		return "none"
	}
	parts := make([]string, len(keys))
	for i, k := range keys {
		parts[i] = fmt.Sprintf("%s %d", k, m[k])
	}
	return strings.Join(parts, ", ")
}

// soakRows runs query and returns its rows, formatted.
func soakRows(t *testing.T, pool *sql.DB, query string) []string {
	t.Helper()
	rows, err := pool.QueryContext(t.Context(), query)
	if err != nil {
		t.Fatalf("error running %q: %v", query, err)
	}
	defer rows.Close()
	cols, err := rows.Columns()
	if err != nil {
		t.Fatalf("error reading the columns of %q: %v", query, err)
	}
	var found []string
	for rows.Next() {
		vals := make([]sql.NullString, len(cols))
		ptrs := make([]any, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			t.Fatalf("error scanning %q: %v", query, err)
		}
		fields := make([]string, len(cols))
		for i, col := range cols {
			fields[i] = col + "=NULL"
			if vals[i].Valid {
				fields[i] = col + "=" + vals[i].String
			}
		}
		found = append(found, "{"+strings.Join(fields, " ")+"}")
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("error reading %q: %v", query, err)
	}
	return found
}

func soakPause(ctx context.Context, d time.Duration) {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
	case <-timer.C:
	}
}
