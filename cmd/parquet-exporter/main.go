// Command parquet-exporter runs a daily BigQuery → GCS Parquet export covering
// the previous day's partition for every stream listed in streamConfigs():
// Kalshi snapshot_1s/trade/lifecycle/settlement.
// Triggered by Cloud Scheduler at 01:00 UTC (see infra/cloud-scheduler.tf).
// Streams export sequentially in the order returned by streamConfigs(); the
// job exits non-zero on the first stream-level failure and Cloud Scheduler
// retries the whole job per its retry_config (BQ Extract is idempotent —
// re-runs overwrite GCS objects).
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"time"

	"cloud.google.com/go/bigquery"
	"google.golang.org/api/iterator"

	"github.com/paulschick/kalshiflow-pipeline/internal/metrics"
)

const (
	envProjectID     = "EXPORT_PROJECT_ID"
	envKalshiDataset = "EXPORT_KALSHI_DATASET"
	envBucket        = "EXPORT_GCS_BUCKET"
	envDate          = "EXPORT_DATE" // optional backfill override (YYYY-MM-DD)

	envMetricsDisabled = "EXPORT_METRICS_DISABLED" // "1" → noop metrics (used in tests)
)

// streamConfig binds one stream (per venue) to its source BQ dataset+table and
// the GCS prefix under the archive bucket. The list is stable; new streams are
// added by extending streamConfigs() and updating infra/locals.tf in the same
// change. Venue is a discriminator carried only into metric labels and slog
// fields — the binary never branches on it.
type streamConfig struct {
	Venue   string // "kalshi"
	Dataset string // BQ dataset for this stream (e.g. "kalshi_raw")
	Stream  string // matches local.streams keys in infra/locals.tf
	Table   string // BQ table_id under Dataset
	Prefix  string // GCS path prefix under EXPORT_GCS_BUCKET
}

// streamConfigs returns the ordered stream list for one nightly run.
func streamConfigs(kalshiDataset string) []streamConfig {
	return []streamConfig{
		{Venue: "kalshi", Dataset: kalshiDataset, Stream: "snapshot_1s", Table: "orderbook_snapshots_1s", Prefix: "snapshots_1s"},
		{Venue: "kalshi", Dataset: kalshiDataset, Stream: "trade", Table: "trade_events", Prefix: "trade"},
		{Venue: "kalshi", Dataset: kalshiDataset, Stream: "lifecycle", Table: "lifecycle_events", Prefix: "lifecycle"},
		{Venue: "kalshi", Dataset: kalshiDataset, Stream: "settlement", Table: "settlement_events", Prefix: "settlement"},
	}
}

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(logger)
	if err := run(); err != nil {
		slog.Error("parquet-exporter failed", "err", err)
		os.Exit(1)
	}
}

// runStream submits a BQ Extract for one stream's partition and emits per-series
// row-count metrics on success. Returns the first error encountered. An empty
// partition is treated as success (zero output files; row-count map is empty).
func runStream(
	ctx context.Context,
	mctx context.Context,
	client *bigquery.Client,
	m *metrics.Metrics,
	projectID, bucket string,
	cfg streamConfig,
	date time.Time,
) error {
	src := partitionDecorator(projectID, cfg.Dataset, cfg.Table, date)
	dest := destinationURI(bucket, cfg.Prefix, date)
	slog.Info("starting export",
		"venue", cfg.Venue, "stream", cfg.Stream,
		"src", src, "dest", dest, "date", date.Format("2006-01-02"))

	extractor := client.DatasetInProject(projectID, cfg.Dataset).Table(cfg.Table + "$" + dateSuffix(date)).ExtractorTo(&bigquery.GCSReference{
		URIs:              []string{dest},
		DestinationFormat: bigquery.Parquet,
		Compression:       bigquery.Snappy,
	})
	job, err := extractor.Run(ctx)
	if err != nil {
		return fmt.Errorf("%s/%s extract submit: %w", cfg.Venue, cfg.Stream, err)
	}
	slog.Info("export job submitted", "venue", cfg.Venue, "stream", cfg.Stream, "job_id", job.ID())

	status, err := job.Wait(ctx)
	if err != nil {
		return fmt.Errorf("%s/%s extract wait: %w", cfg.Venue, cfg.Stream, err)
	}
	if err := status.Err(); err != nil {
		return fmt.Errorf("%s/%s extract job error: %w", cfg.Venue, cfg.Stream, err)
	}

	rowsBySeries, err := countRowsExportedBySeries(ctx, client, cfg.Dataset, cfg.Table, date)
	if err != nil {
		slog.Warn("row-count by series failed; metric will be missing for this stream",
			"venue", cfg.Venue, "stream", cfg.Stream, "err", err)
	} else {
		for series, n := range rowsBySeries {
			m.ParquetRowsExported.Add(mctx, n, "venue", cfg.Venue, "stream", cfg.Stream, "series", series)
		}
		if len(rowsBySeries) == 0 {
			slog.Info("export complete with empty partition (no rows, no metric points emitted)",
				"venue", cfg.Venue, "stream", cfg.Stream, "date", date.Format("2006-01-02"))
		}
	}

	slog.Info("export complete",
		"venue", cfg.Venue,
		"stream", cfg.Stream,
		"job_id", job.ID(),
		"src", src,
		"dest", dest,
		"duration_s", status.Statistics.EndTime.Sub(status.Statistics.StartTime).Seconds(),
	)
	return nil
}

func run() error {
	projectID := mustEnv(envProjectID)
	kalshiDataset := mustEnv(envKalshiDataset)
	bucket := mustEnv(envBucket)

	startedAt := time.Now()
	outcome := "error" // overwritten to "ok" on success path

	mctx, mcancel := context.WithCancel(context.Background())
	defer mcancel()
	m, err := metrics.New(mctx, metrics.Options{
		ProjectID: projectID,
		Service:   "parquet-exporter",
		Disabled:  os.Getenv(envMetricsDisabled) == "1",
	})
	if err != nil {
		return fmt.Errorf("metrics init: %w", err)
	}
	defer func() {
		m.ParquetRunDuration.Record(mctx, time.Since(startedAt).Seconds(), "outcome", outcome)
		m.ParquetRunOutcome.Add(mctx, 1, "outcome", outcome)
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = m.Shutdown(shutdownCtx)
	}()

	date := computeExportDate(time.Now, os.Getenv(envDate))

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()

	client, err := bigquery.NewClient(ctx, projectID)
	if err != nil {
		return fmt.Errorf("bq client: %w", err)
	}
	defer func() { _ = client.Close() }()

	for _, cfg := range streamConfigs(kalshiDataset) {
		if err := runStream(ctx, mctx, client, m, projectID, bucket, cfg, date); err != nil {
			return err
		}
	}
	outcome = "ok"
	return nil
}

// countRowsExportedBySeries returns the row count for the just-exported partition
// grouped by series_id. Used to label parquet_rows_exported_total. Returns an
// empty map (not an error) when the partition is empty.
func countRowsExportedBySeries(ctx context.Context, client *bigquery.Client, dataset, table string, d time.Time) (map[string]int64, error) {
	day := d.UTC().Format("2006-01-02")
	q := client.Query(fmt.Sprintf(
		"SELECT series_id, COUNT(*) AS n FROM `%s.%s` "+
			"WHERE event_ts >= TIMESTAMP('%s') "+
			"AND event_ts < TIMESTAMP_ADD(TIMESTAMP('%s'), INTERVAL 1 DAY) "+
			"GROUP BY series_id",
		dataset, table, day, day,
	))
	it, err := q.Read(ctx)
	if err != nil {
		return nil, fmt.Errorf("rows-by-series query: %w", err)
	}
	out := map[string]int64{}
	for {
		var row struct {
			SeriesID string `bigquery:"series_id"`
			N        int64  `bigquery:"n"`
		}
		if err := it.Next(&row); err != nil {
			if errors.Is(err, iterator.Done) {
				break
			}
			return nil, fmt.Errorf("rows-by-series iterate: %w", err)
		}
		out[row.SeriesID] = row.N
	}
	return out, nil
}

// computeExportDate returns the UTC midnight date to export. If override is
// non-empty it must be a YYYY-MM-DD string; panics on parse failure (the job
// is invoked via terraform-set env vars and Cloud Scheduler — a malformed
// override is a configuration bug, not a runtime condition).
func computeExportDate(now func() time.Time, override string) time.Time {
	if override != "" {
		d, err := time.Parse("2006-01-02", override)
		if err != nil {
			panic(fmt.Sprintf("EXPORT_DATE = %q is not YYYY-MM-DD: %v", override, err))
		}
		return d.UTC()
	}
	return now().UTC().AddDate(0, 0, -1).Truncate(24 * time.Hour)
}

// partitionDecorator returns the BQ partition reference for the given date.
// Format: "<project>:<dataset>.<table>$YYYYMMDD".
func partitionDecorator(project, dataset, table string, d time.Time) string {
	return fmt.Sprintf("%s:%s.%s$%s", project, dataset, table, dateSuffix(d))
}

// destinationURI returns the Hive-partitioned GCS URI for the given date.
func destinationURI(bucket, prefix string, d time.Time) string {
	return fmt.Sprintf("gs://%s/%s/year=%04d/month=%02d/day=%02d/*.parquet",
		bucket, prefix, d.Year(), int(d.Month()), d.Day())
}

func dateSuffix(d time.Time) string {
	return fmt.Sprintf("%04d%02d%02d", d.Year(), int(d.Month()), d.Day())
}

func mustEnv(name string) string {
	v := os.Getenv(name)
	if v == "" {
		panic(fmt.Sprintf("env %s required but not set", name))
	}
	return v
}
