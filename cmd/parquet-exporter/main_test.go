// cmd/parquet-exporter/main_test.go
package main

import (
	"testing"
	"time"
)

func TestExportDate_DefaultsToYesterdayUTC(t *testing.T) {
	t.Parallel()
	fixedNow := func() time.Time { return time.Date(2026, 5, 4, 1, 30, 0, 0, time.UTC) }
	got := computeExportDate(fixedNow, "")
	want := time.Date(2026, 5, 3, 0, 0, 0, 0, time.UTC)
	if !got.Equal(want) {
		t.Errorf("computeExportDate default = %v; want %v", got, want)
	}
}

func TestExportDate_HonorsEnvOverride(t *testing.T) {
	t.Parallel()
	got := computeExportDate(time.Now, "2026-04-15")
	want := time.Date(2026, 4, 15, 0, 0, 0, 0, time.UTC)
	if !got.Equal(want) {
		t.Errorf("computeExportDate override = %v; want %v", got, want)
	}
}

func TestExportDate_RejectsBadOverride(t *testing.T) {
	t.Parallel()
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("expected panic on malformed EXPORT_DATE")
		}
	}()
	_ = computeExportDate(time.Now, "not-a-date")
}

func TestPartitionDecorator(t *testing.T) {
	t.Parallel()
	d := time.Date(2026, 5, 3, 0, 0, 0, 0, time.UTC)
	got := partitionDecorator("your-gcp-project", "kalshi_raw", "orderbook_snapshots_1s", d)
	want := "your-gcp-project:kalshi_raw.orderbook_snapshots_1s$20260503"
	if got != want {
		t.Errorf("partitionDecorator = %q; want %q", got, want)
	}
}

func TestDestinationURI(t *testing.T) {
	t.Parallel()
	d := time.Date(2026, 5, 3, 0, 0, 0, 0, time.UTC)
	got := destinationURI("<your-archive-bucket>", "snapshots_1s", d)
	want := "gs://<your-archive-bucket>/snapshots_1s/year=2026/month=05/day=03/*.parquet"
	if got != want {
		t.Errorf("destinationURI = %q; want %q", got, want)
	}
}

func TestRun_PanicsWhenProjectIDEmpty(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic from mustEnv with EXPORT_PROJECT_ID unset")
		}
	}()
	t.Setenv(envProjectID, "")
	_ = run()
}

func TestRun_PanicsWhenKalshiDatasetEmpty(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic from mustEnv with EXPORT_KALSHI_DATASET unset")
		}
	}()
	t.Setenv(envProjectID, "your-gcp-project")
	t.Setenv(envKalshiDataset, "")
	_ = run()
}

func TestRun_PanicsWhenBucketEmpty(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic from mustEnv with EXPORT_GCS_BUCKET unset")
		}
	}()
	t.Setenv(envProjectID, "your-gcp-project")
	t.Setenv(envKalshiDataset, "kalshi_raw")
	t.Setenv(envBucket, "")
	_ = run()
}

func TestStreamConfigs_DestinationURIPerStream(t *testing.T) {
	t.Parallel()
	d := time.Date(2026, 5, 5, 0, 0, 0, 0, time.UTC)
	want := map[string]string{
		"kalshi/snapshot_1s": "gs://<your-archive-bucket>/snapshots_1s/year=2026/month=05/day=05/*.parquet",
		"kalshi/trade":       "gs://<your-archive-bucket>/trade/year=2026/month=05/day=05/*.parquet",
		"kalshi/lifecycle":   "gs://<your-archive-bucket>/lifecycle/year=2026/month=05/day=05/*.parquet",
		"kalshi/settlement":  "gs://<your-archive-bucket>/settlement/year=2026/month=05/day=05/*.parquet",
	}
	for _, cfg := range streamConfigs("kalshi_raw") {
		key := cfg.Venue + "/" + cfg.Stream
		got := destinationURI("<your-archive-bucket>", cfg.Prefix, d)
		if got != want[key] {
			t.Errorf("destinationURI for %q = %q; want %q", key, got, want[key])
		}
	}
}

func TestStreamConfigs_CoversAllFourStreams(t *testing.T) {
	t.Parallel()
	got := streamConfigs("kalshi_raw")
	want := []streamConfig{
		{Venue: "kalshi", Dataset: "kalshi_raw", Stream: "snapshot_1s", Table: "orderbook_snapshots_1s", Prefix: "snapshots_1s"},
		{Venue: "kalshi", Dataset: "kalshi_raw", Stream: "trade", Table: "trade_events", Prefix: "trade"},
		{Venue: "kalshi", Dataset: "kalshi_raw", Stream: "lifecycle", Table: "lifecycle_events", Prefix: "lifecycle"},
		{Venue: "kalshi", Dataset: "kalshi_raw", Stream: "settlement", Table: "settlement_events", Prefix: "settlement"},
	}
	if len(got) != len(want) {
		t.Fatalf("streamConfigs() len = %d; want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("streamConfigs()[%d] = %+v; want %+v", i, got[i], want[i])
		}
	}
}
