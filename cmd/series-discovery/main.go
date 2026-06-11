// Command series-discovery runs a single Kalshi /series + per-market /markets
// catalog refresh and MERGEs hash-diffed rows into kalshi_raw.series_catalog and
// kalshi_raw.market_catalog. Triggered by Cloud Scheduler every 6h (see
// infra/series-discovery.tf). Single-shot; exits non-zero on the first error so
// Cloud Scheduler retries per its retry_config.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/storage"

	"github.com/paulschick/kalshiflow-pipeline/internal/kalshi"
	"github.com/paulschick/kalshiflow-pipeline/internal/metrics"
	"github.com/paulschick/kalshiflow-pipeline/internal/secrets"
	"github.com/paulschick/kalshiflow-pipeline/internal/seriescat"
)

const (
	envProjectID        = "EXPORT_PROJECT_ID"
	envDataset          = "EXPORT_TARGET_DATASET"
	envSeriesCatalogTbl = "EXPORT_SERIES_CATALOG_TABLE"
	envMarketCatalogTbl = "EXPORT_MARKET_CATALOG_TABLE"
	envSecretName       = "KALSHI_SECRET_NAME"
	envRESTURL          = "KALSHI_REST_URL"
	envSeriesList       = "KALSHI_SERIES"
	envControlBucket    = "KALSHI_CONTROL_BUCKET"
	envControlObject    = "KALSHI_CONTROL_OBJECT"
	envViewSubscribed   = "EXPORT_VIEW_SERIES_SUBSCRIBED"

	envMetricsDisabled = "DISCOVERY_METRICS_DISABLED" // "1" → noop metrics
)

// errEmptyDesiredSet is returned by run() to signal exit-code 2.
// main() matches it via errors.Is.
var errEmptyDesiredSet = errors.New("series-discovery: empty desired set")

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(logger)
	if err := run(); err != nil {
		slog.Error("series-discovery failed", "err", err)
		if errors.Is(err, errEmptyDesiredSet) {
			os.Exit(2)
		}
		os.Exit(1)
	}
}

func run() error {
	projectID := mustEnv(envProjectID)
	dataset := mustEnv(envDataset)
	seriesTable := mustEnv(envSeriesCatalogTbl)
	marketTable := mustEnv(envMarketCatalogTbl)
	secretName := mustEnv(envSecretName)
	restURL := os.Getenv(envRESTURL)
	if restURL == "" {
		restURL = "https://api.elections.kalshi.com"
	}
	subscribedTickers := splitSeriesEnv(os.Getenv(envSeriesList))
	controlBucket := mustEnv(envControlBucket)
	controlObject := mustEnv(envControlObject)
	viewSubscribed := mustEnv(envViewSubscribed)

	mctx, mcancel := context.WithCancel(context.Background())
	defer mcancel()
	m, err := metrics.New(mctx, metrics.Options{
		ProjectID: projectID,
		Service:   "series-discovery",
		Disabled:  os.Getenv(envMetricsDisabled) == "1",
	})
	if err != nil {
		return fmt.Errorf("metrics init: %w", err)
	}
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = m.Shutdown(shutdownCtx)
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	loader, closeLoader, err := secrets.NewLoader(ctx)
	if err != nil {
		return fmt.Errorf("secrets loader: %w", err)
	}
	defer func() { _ = closeLoader() }()

	creds, err := loader.LoadKalshi(ctx, secretName)
	if err != nil {
		return fmt.Errorf("load kalshi creds: %w", err)
	}
	priv, err := kalshi.ParsePrivateKeyPEM([]byte(creds.PrivateKeyPEM))
	if err != nil {
		return fmt.Errorf("parse kalshi private key: %w", err)
	}
	signer := &kalshi.Signer{KeyID: creds.APIKeyID, PrivateKey: priv}
	restClient := kalshi.NewClient(kalshi.Config{
		BaseURL: restURL,
		Signer:  signer,
		Now:     time.Now,
	})

	bq, err := bigquery.NewClient(ctx, projectID)
	if err != nil {
		return fmt.Errorf("bq client: %w", err)
	}
	defer func() { _ = bq.Close() }()

	gcsClient, err := storage.NewClient(ctx)
	if err != nil {
		return fmt.Errorf("gcs client: %w", err)
	}
	defer func() { _ = gcsClient.Close() }()

	seriesFetcher := seriescat.NewKalshiSeriesFetcher(restClient)
	seriesWriter := seriescat.NewBQCatalogWriter(bq, projectID, dataset, seriesTable)
	marketsFetcher := seriescat.NewKalshiMarketsFetcher(restClient)
	marketWriter := seriescat.NewBQMarketCatalogWriter(bq, projectID, dataset, marketTable)

	captureTS := time.Now().UTC()
	slog.Info("starting series-discovery run",
		"capture_ts", captureTS.Format(time.RFC3339),
		"subscribed_count", len(subscribedTickers))
	if len(subscribedTickers) == 0 {
		slog.Warn("KALSHI_SERIES empty; markets pass will be skipped")
	}
	if err := seriescat.RunOnce(
		ctx,
		seriesFetcher, seriesWriter,
		marketsFetcher, marketWriter,
		subscribedTickers,
		m, captureTS,
	); err != nil {
		return fmt.Errorf("run once: %w", err)
	}

	vr := &seriescat.BQViewReader{Client: bq}
	gw := &seriescat.GCSStorageWriter{Client: gcsClient}
	n, gen, err := seriescat.WriteDesiredSet(ctx, vr, gw, seriescat.DesiredSetConfig{
		ProjectID: projectID,
		DatasetID: dataset,
		ViewName:  viewSubscribed,
		Bucket:    controlBucket,
		ObjectKey: controlObject,
	})
	switch {
	case errors.Is(err, seriescat.ErrEmptyDesiredSet):
		slog.Error("empty desired set; refusing to write series_desired.json", "exit_code", 2)
		return errEmptyDesiredSet
	case err != nil:
		return fmt.Errorf("write series_desired.json: %w", err)
	}
	slog.Info("series_desired.json written", "tickers", n, "generation", gen)

	slog.Info("series-discovery run complete")
	return nil
}

// splitSeriesEnv parses a comma-separated KALSHI_SERIES env value into a
// trimmed, deduped, order-preserving list of tickers. Empty input → empty
// slice. Same trim+drop-empty shape as cmd/ws-worker/main.go::splitCSV, with
// an additional dedupe pass (defensive against an accidentally double-listed
// ticker in tfvars). Intentionally inlined pending HOL-23 slice 3, which
// obsoletes the env-driven source.
func splitSeriesEnv(raw string) []string {
	if raw == "" {
		return nil
	}
	var out []string
	seen := map[string]struct{}{}
	for _, p := range strings.Split(raw, ",") {
		t := strings.TrimSpace(p)
		if t == "" {
			continue
		}
		if _, dup := seen[t]; dup {
			continue
		}
		seen[t] = struct{}{}
		out = append(out, t)
	}
	return out
}

func mustEnv(name string) string {
	v := os.Getenv(name)
	if v == "" {
		panic(fmt.Sprintf("env %s required but not set", name))
	}
	return v
}
