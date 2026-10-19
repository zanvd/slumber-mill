package main

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"go.opentelemetry.io/otel"

	"github.com/zanvd/slumber-mill/src/asset"
	"github.com/zanvd/slumber-mill/src/audio"
	"github.com/zanvd/slumber-mill/src/db"
	"github.com/zanvd/slumber-mill/src/egress"
	"github.com/zanvd/slumber-mill/src/httpsrv"
	"github.com/zanvd/slumber-mill/src/logging"
	"github.com/zanvd/slumber-mill/src/secret"
	"github.com/zanvd/slumber-mill/src/storage"
	"github.com/zanvd/slumber-mill/src/tenant"
	"github.com/zanvd/slumber-mill/src/tracing"
	"github.com/zanvd/slumber-mill/src/webhook"
	"github.com/zanvd/slumber-mill/src/worker"
)

const (
	exitOK    = 0
	exitError = 1
	exitUsage = 2

	readHeaderTimeout = 10 * time.Second
	idleTimeout       = 2 * time.Minute
	// traceFlushTimeout bounds the export of the last spans. With the default drain delay and grace
	// it keeps the API's longest stop at 43 s, inside Compose's 45 s stop_grace_period.
	traceFlushTimeout = 3 * time.Second

	defaultTargetLUFS = -20

	// pollInterval is how long an idle slot waits, give or take half, before it claims again.
	pollInterval = time.Second
	// webhookLease is how long a claim or a heartbeat holds a delivery: well past the client's 15 s overall timeout,
	// so a delivery rarely needs a heartbeat.
	webhookLease = time.Minute
	retention    = 30 * 24 * time.Hour
	// retentionInterval is how long the worker holding the relay lock waits after a retention run before the next.
	retentionInterval = time.Hour
)

const usage = `usage:
  slumber-mill migrate                          apply database migrations
  slumber-mill api                              serve the HTTP API
  slumber-mill worker -pool encode|webhook      run a worker pool
  slumber-mill tenant create -slug S [-webhook-url U] [-target-lufs F]
                                                create a tenant (-target-lufs defaults to -20) and print
                                                its API key and webhook secret on stdout
`

type usageError struct {
	msg string
}

func (e usageError) Error() string {
	return e.msg
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	// After the first signal, a second one ends the process at once.
	context.AfterFunc(ctx, stop)
	code := run(ctx, os.Args[1:], os.Getenv, os.Stdout, os.Stderr)
	stop()
	os.Exit(code)
}

// run executes the command in args and returns the process's exit code.
// Stdout carries nothing but tenant create's secrets; logs go to stderr as JSON.
func run(ctx context.Context, args []string, getenv func(string) string, stdout, stderr io.Writer) int {
	log := slog.New(slog.NewJSONHandler(stderr, nil))
	ctx = logging.CtxWithLogger(ctx, log)

	err := dispatch(ctx, log, args, getenv, stdout)
	if err == nil {
		return exitOK
	}
	if errors.Is(err, flag.ErrHelp) {
		fmt.Fprint(stderr, usage)
		return exitOK
	}
	if ue, ok := errors.AsType[usageError](err); ok {
		fmt.Fprintf(stderr, "slumber-mill: %s\n%s", ue.msg, usage)
		return exitUsage
	}
	log.ErrorContext(ctx, "command failed", "error", err)
	return exitError
}

func dispatch(ctx context.Context, log *slog.Logger, args []string, getenv func(string) string, stdout io.Writer) error {
	if len(args) == 0 {
		return usageError{"no command given"}
	}
	cmd, rest := args[0], args[1:]
	switch cmd {
	case "migrate":
		if err := parseFlags(flag.NewFlagSet(cmd, flag.ContinueOnError), rest); err != nil {
			return err
		}
		return runMigrate(ctx, log, getenv)
	case "api":
		if err := parseFlags(flag.NewFlagSet(cmd, flag.ContinueOnError), rest); err != nil {
			return err
		}
		return runAPI(ctx, log, getenv)
	case "worker":
		return runWorker(ctx, log, rest, getenv)
	case "tenant":
		if len(rest) == 0 || rest[0] != "create" {
			return usageError{"tenant: the only subcommand is create"}
		}
		return runTenantCreate(ctx, log, rest[1:], getenv, stdout)
	case "help", "-h", "-help", "--help":
		return flag.ErrHelp
	default:
		return usageError{fmt.Sprintf("unknown command %q", cmd)}
	}
}

// parseFlags parses args into fs and refuses positional arguments. Errors are left for run to print.
func parseFlags(fs *flag.FlagSet, args []string) error {
	fs.SetOutput(io.Discard)
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return err
		}
		return usageError{fs.Name() + ": " + err.Error()}
	}
	if fs.NArg() > 0 {
		return usageError{fmt.Sprintf("%s: unexpected argument %q", fs.Name(), fs.Arg(0))}
	}
	return nil
}

func runMigrate(ctx context.Context, log *slog.Logger, getenv func(string) string) error {
	cfg, err := loadMigrateConfig(getenv)
	if err != nil {
		return err
	}
	applied, err := db.Migrate(ctx, cfg.DSN, log)
	if err != nil {
		return err
	}
	if len(applied) == 0 {
		log.InfoContext(ctx, "database is up to date")
		return nil
	}
	log.InfoContext(ctx, "migrations applied", "migrations", applied)
	return nil
}

func runAPI(ctx context.Context, log *slog.Logger, getenv func(string) string) error {
	httpsrv.ReleaseMode()
	cfg, err := loadAPIConfig(getenv)
	if err != nil {
		return err
	}
	files := storage.NewLocal(cfg.DataDir)
	sweepTemp(ctx, log, files, cfg.EncodeLease)

	tp, err := setupTracing(ctx, log, cfg.Tracing)
	if err != nil {
		return err
	}
	// Deferred first, so it runs last: once the HTTP server has stopped and every span has ended.
	defer flushTraces(ctx, log, tp)

	gdb, err := db.Open(cfg.DSN, log)
	if err != nil {
		return err
	}
	pool, err := gdb.DB()
	if err != nil {
		return fmt.Errorf("error getting the database pool: %w", err)
	}
	defer pool.Close()

	tenants := tenant.NewService(tenant.NewRepository(gdb), nil, nil)
	assets := asset.NewService(asset.NewRepository(gdb), files)
	handler, err := httpsrv.NewServer(log, tenants, assets, httpsrv.Config{
		TrustedProxies:      cfg.TrustedProxies,
		Readiness:           pool,
		MaxUploadBytes:      cfg.MaxUploadBytes,
		MaxUploadsPerTenant: cfg.MaxUploadsPerTenant,
		Tracer:              tp,
	})
	if err != nil {
		return err
	}
	srv := &http.Server{
		Handler:           handler.Handler(),
		ReadHeaderTimeout: readHeaderTimeout,
		IdleTimeout:       idleTimeout,
		ErrorLog:          slog.NewLogLogger(log.Handler(), slog.LevelWarn),
	}

	var lc net.ListenConfig
	ln, err := lc.Listen(ctx, "tcp", cfg.HTTPAddr)
	// A signal before the server serves is a stop, even when it failed the listen: there is nothing to drain.
	if ctx.Err() != nil {
		if ln != nil {
			ln.Close()
		}
		log.InfoContext(ctx, "stopped before serving HTTP")
		return nil
	}
	if err != nil {
		return fmt.Errorf("error listening on %s: %w", cfg.HTTPAddr, err)
	}
	log.InfoContext(ctx, "serving HTTP", "addr", ln.Addr().String())
	return serve(ctx, log, srv, ln, handler, cfg.DrainDelay, cfg.ShutdownGrace)
}

// setupTracing builds the tracer provider. When it exports, the SDK's errors, such as a failed export,
// go to log as warnings rather than as plain text through the log package.
func setupTracing(ctx context.Context, log *slog.Logger, cfg tracing.Config) (*tracing.Provider, error) {
	tp, err := tracing.Setup(ctx, cfg)
	if err != nil {
		return nil, err
	}
	if !tp.Enabled() {
		return tp, nil
	}
	otel.SetErrorHandler(otel.ErrorHandlerFunc(func(err error) {
		log.Warn("tracing error", "error", err)
	}))
	log.InfoContext(ctx, "exporting traces started by Checkly", "endpoint", cfg.Endpoint, "service", cfg.ServiceName)
	return tp, nil
}

// flushTraces exports the spans still queued, within traceFlushTimeout, after the HTTP server has stopped.
func flushTraces(ctx context.Context, log *slog.Logger, tp *tracing.Provider) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), traceFlushTimeout)
	defer cancel()
	if err := tp.Shutdown(ctx); err != nil {
		log.WarnContext(ctx, "error flushing traces", "error", err)
	}
}

func runWorker(ctx context.Context, log *slog.Logger, args []string, getenv func(string) string) error {
	fs := flag.NewFlagSet("worker", flag.ContinueOnError)
	pool := fs.String("pool", "", "")
	if err := parseFlags(fs, args); err != nil {
		return err
	}
	switch *pool {
	case "encode":
		return runEncodePool(ctx, log, getenv)
	case "webhook":
		return runWebhookPool(ctx, log, getenv)
	case "":
		return usageError{"worker: -pool is required"}
	default:
		return usageError{fmt.Sprintf("worker: unknown pool %q, want encode or webhook", *pool)}
	}
}

func runEncodePool(ctx context.Context, log *slog.Logger, getenv func(string) string) error {
	cfg, err := loadEncodeConfig(getenv)
	if err != nil {
		return err
	}
	id := workerID()
	poolLog := log.With("worker", id, "pool", "encode")
	ff := audio.New()
	files := storage.NewLocal(cfg.DataDir)
	if err := startEncode(ctx, poolLog, ff, files, cfg.Lease); err != nil {
		if ctx.Err() != nil {
			return nil
		}
		return err
	}

	gdb, err := db.Open(cfg.DSN, log)
	if err != nil {
		return err
	}
	pool, err := gdb.DB()
	if err != nil {
		return fmt.Errorf("error getting the database pool: %w", err)
	}
	defer pool.Close()

	processor, err := asset.NewProcessor(asset.NewRepository(gdb), files, ff,
		asset.ProcessorConfig{WorkerID: id, Lease: cfg.Lease})
	if err != nil {
		return fmt.Errorf("error creating the processor: %w", err)
	}
	encode, err := worker.NewPool(processor, worker.Config{
		WorkerID:     id,
		Pool:         "encode",
		Concurrency:  cfg.Concurrency,
		Lease:        cfg.Lease,
		PollInterval: pollInterval,
		Grace:        cfg.ShutdownGrace,
		Log:          log,
	})
	if err != nil {
		return fmt.Errorf("error creating the encode pool: %w", err)
	}

	poolLog.InfoContext(ctx, "worker pool started", "concurrency", cfg.Concurrency, "lease", cfg.Lease.String(),
		"grace", cfg.ShutdownGrace.String())
	encode.Run(ctx)
	poolLog.InfoContext(ctx, "worker pool stopped")
	return nil
}

func runWebhookPool(ctx context.Context, log *slog.Logger, getenv func(string) string) error {
	cfg, err := loadWebhookConfig(getenv)
	if err != nil {
		return err
	}
	box, err := secret.NewBox(string(cfg.SecretKeys.Bytes()))
	if err != nil {
		return fmt.Errorf("error loading %s: %w", envSecretKeys, err)
	}
	id := workerID()
	poolLog := log.With("worker", id, "pool", "webhook")
	files := storage.NewLocal(cfg.DataDir)
	sweepTemp(ctx, poolLog, files, cfg.EncodeLease)

	gdb, err := db.Open(cfg.DSN, log)
	if err != nil {
		return err
	}
	pool, err := gdb.DB()
	if err != nil {
		return fmt.Errorf("error getting the database pool: %w", err)
	}
	defer pool.Close()

	tenants := tenant.NewService(tenant.NewRepository(gdb), box, nil)
	assets := asset.NewService(asset.NewRepository(gdb), files)
	repo := webhook.NewRepository(gdb)
	relay, err := webhook.NewRelay(repo, assets, tenants)
	if err != nil {
		return fmt.Errorf("error creating the relay: %w", err)
	}
	client := webhook.NewClient(cfg.WebhookAllowPrivate)
	deliverer, err := webhook.NewDeliverer(repo, tenants, client,
		webhook.DelivererConfig{WorkerID: id, Lease: webhookLease})
	if err != nil {
		return fmt.Errorf("error creating the deliverer: %w", err)
	}
	deliveries, err := worker.NewPool(deliverer, worker.Config{
		WorkerID:     id,
		Pool:         "webhook",
		Concurrency:  cfg.Concurrency,
		Lease:        webhookLease,
		PollInterval: pollInterval,
		Grace:        cfg.ShutdownGrace,
		Log:          log,
	})
	if err != nil {
		return fmt.Errorf("error creating the webhook pool: %w", err)
	}

	poolLog.InfoContext(ctx, "worker pool started", "concurrency", cfg.Concurrency, "lease", webhookLease.String(),
		"grace", cfg.ShutdownGrace.String())
	relay.WhileHeld(func(ctx context.Context) {
		retain(ctx, retention, retentionInterval, assets.DeleteRelayedEvents, deliverer.DeleteDelivered)
	})
	runWebhook(logging.CtxWithLogger(ctx, poolLog), relay, deliveries, client)
	poolLog.InfoContext(ctx, "worker pool stopped")
	return nil
}

func runTenantCreate(ctx context.Context, log *slog.Logger, args []string, getenv func(string) string,
	stdout io.Writer) error {
	fs := flag.NewFlagSet("tenant create", flag.ContinueOnError)
	slug := fs.String("slug", "", "")
	webhookURL := fs.String("webhook-url", "", "")
	targetLUFS := fs.Float64("target-lufs", defaultTargetLUFS, "")
	if err := parseFlags(fs, args); err != nil {
		return err
	}
	if *slug == "" {
		return usageError{"tenant create: -slug is required"}
	}
	cfg, err := loadTenantCreateConfig(getenv)
	if err != nil {
		return err
	}
	box, err := secret.NewBox(string(cfg.SecretKeys.Bytes()))
	if err != nil {
		return fmt.Errorf("error loading %s: %w", envSecretKeys, err)
	}

	gdb, err := db.Open(cfg.DSN, log)
	if err != nil {
		return err
	}
	pool, err := gdb.DB()
	if err != nil {
		return fmt.Errorf("error getting the database pool: %w", err)
	}
	defer pool.Close()

	tenants := tenant.NewService(tenant.NewRepository(gdb), box,
		egress.NewHostChecker(cfg.WebhookAllowPrivate, net.DefaultResolver))
	_, creds, err := tenants.Create(ctx, tenant.CreateParams{
		Slug:       *slug,
		WebhookURL: *webhookURL,
		TargetLUFS: *targetLUFS,
	})
	if err != nil {
		return fmt.Errorf("error creating tenant %q: %w", *slug, err)
	}
	if err := printCredentials(stdout, creds); err != nil {
		return fmt.Errorf("tenant %q was created, but its credentials couldn't be printed: %w", *slug, err)
	}
	return nil
}

func printCredentials(w io.Writer, creds tenant.Credentials) error {
	var b bytes.Buffer
	fmt.Fprintf(&b, "api_key: %s\n", creds.APIKey.Bytes())
	if s := creds.WebhookSecret.Bytes(); s != nil {
		fmt.Fprintf(&b, "webhook_secret: %s\n", s)
	}
	_, err := w.Write(b.Bytes())
	return err
}
