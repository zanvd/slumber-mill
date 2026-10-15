package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/zanvd/slumber-mill/src/logging"
)

type versioner interface {
	Version(ctx context.Context) (ffmpeg, ffprobe string, err error)
}

type tempSweeper interface {
	SweepTemp(olderThan time.Duration) (int, error)
}

type runner interface {
	Run(ctx context.Context)
}

type idleCloser interface {
	CloseIdleConnections()
}

// workerID names this process in locked_by: its host name, which Compose sets to the container's ID, and its PID.
func workerID() string {
	host, err := os.Hostname()
	if err != nil || host == "" {
		host = "unknown-host"
	}
	return host + ":" + strconv.Itoa(os.Getpid())
}

func startEncode(ctx context.Context, log *slog.Logger, ff versioner, files tempSweeper, sweepAge time.Duration) error {
	ffmpeg, ffprobe, err := ff.Version(ctx)
	if err != nil {
		return fmt.Errorf("the encode pool needs ffmpeg and ffprobe: %w", err)
	}
	log.InfoContext(ctx, "found ffmpeg", "ffmpeg", ffmpeg, "ffprobe", ffprobe)
	sweepTemp(ctx, log, files, sweepAge)
	return nil
}

// sweepTemp removes temp files older than age, the encode lease. The encode pool refreshes the temp files it holds
// every lease/3, so only those of a process that died get that old. A failure is only logged.
func sweepTemp(ctx context.Context, log *slog.Logger, files tempSweeper, age time.Duration) {
	removed, err := files.SweepTemp(age)
	if err != nil {
		log.WarnContext(ctx, "error sweeping temp files", "older_than", age.String(), "removed", removed, "error", err)
		return
	}
	log.InfoContext(ctx, "swept temp files", "older_than", age.String(), "removed", removed)
}

// runWebhook runs the relay, the delivery pool and retention until ctx is done and all of them have stopped.
// The relay gives up its lock as it stops, and the pool releases deliveries still in flight once the grace runs out.
// The client's idle connections are closed after the pool has stopped, when nothing can reuse them.
func runWebhook(ctx context.Context, relay, deliveries runner, client idleCloser, deleteOld func(context.Context)) {
	var wg sync.WaitGroup
	wg.Go(func() { relay.Run(ctx) })
	wg.Go(func() {
		deliveries.Run(ctx)
		client.CloseIdleConnections()
	})
	wg.Go(func() { deleteOld(ctx) })
	wg.Wait()
}

type retentionDelete func(ctx context.Context, age time.Duration) (int, error)

// retain runs each delete with age at once, and again interval after each run ends, until ctx is done.
// The deletes log what they delete. An error is logged unless ctx ending caused it.
func retain(ctx context.Context, age, interval time.Duration, deletes ...retentionDelete) {
	log := logging.LoggerFromContext(ctx)
	for {
		for _, del := range deletes {
			if ctx.Err() != nil {
				return
			}
			if _, err := del(ctx, age); err != nil && ctx.Err() == nil {
				log.ErrorContext(ctx, "retention failed", "error", err)
			}
		}
		wait := time.NewTimer(interval)
		select {
		case <-ctx.Done():
			wait.Stop()
			return
		case <-wait.C:
		}
	}
}
