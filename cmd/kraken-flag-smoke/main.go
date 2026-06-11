// Command kraken-flag-smoke is a local one-shot CLI that polls
// gs://${KRAKEN_FLAG_BUCKET}/control/kraken_enabled.json on the same cadence
// the production poller will (HOL-48). Logs the current flag value at boot
// and on every generation-change transition. Operator-run with ADC.
//
// Usage:
//
//	export KRAKEN_FLAG_BUCKET=<project>-kalshi-archive
//	go run ./cmd/kraken-flag-smoke
//
// Ctrl-C to stop. Not deployed; HOL-47 smoke harness only.
package main

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"cloud.google.com/go/storage"

	"github.com/paulschick/kalshiflow-pipeline/internal/featureflag"
)

const (
	envBucket = "KRAKEN_FLAG_BUCKET"
	objectKey = "control/kraken_enabled.json"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(logger)

	bucket := os.Getenv(envBucket)
	if bucket == "" {
		slog.Error(envBucket + " not set; expected e.g. <project>-kalshi-archive")
		os.Exit(2)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	client, err := storage.NewClient(ctx)
	if err != nil {
		slog.Error("storage client", "err", err)
		os.Exit(1)
	}
	defer func() { _ = client.Close() }()

	flag := featureflag.NewGCSKrakenFlag(client, bucket, objectKey)
	if err := flag.LoadOnce(ctx); err != nil && !errors.Is(err, featureflag.ErrAbsent) {
		slog.Warn("kraken_flag boot load failed; proceeding with default-on",
			"err", err, "enabled", flag.Enabled())
	}
	slog.Info("kraken_flag boot",
		"bucket", bucket, "object", objectKey, "enabled", flag.Enabled())

	flag.Start(ctx) // blocks until SIGINT/SIGTERM
	slog.Info("kraken_flag smoke exiting", "enabled", flag.Enabled())
}
