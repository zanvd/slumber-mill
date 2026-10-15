package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"sync"
	"time"
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

// startEncode checks that ffmpeg and ffprobe run, and only then sweeps temp files.
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

// runWebhook runs the relay and the delivery pool until ctx is done and both have stopped. The relay gives up
// its lock as it stops, and the pool releases deliveries still in flight once the grace runs out.
// The client's idle connections are closed after the pool has stopped, when nothing can reuse them.
func runWebhook(ctx context.Context, relay, deliveries runner, client idleCloser) {
	var wg sync.WaitGroup
	wg.Go(func() { relay.Run(ctx) })
	wg.Go(func() {
		deliveries.Run(ctx)
		client.CloseIdleConnections()
	})
	wg.Wait()
}
