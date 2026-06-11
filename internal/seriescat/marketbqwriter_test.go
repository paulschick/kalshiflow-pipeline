//go:build integration

package seriescat

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"cloud.google.com/go/bigquery"
)

// TestBQMarketCatalogWriter_Merge_Integration requires:
//
//	BQ_TEST_PROJECT  — project owning the sandbox dataset
//	BQ_TEST_DATASET  — dataset that does NOT contain a real market_catalog
//
// Creates a temp table per run (named market_catalog_test_<unix-nanos>), MERGEs a
// canned 3-row batch, asserts NumDMLAffectedRows=3, MERGEs the same batch again
// (asserts 0 — idempotent), MERGEs again with a content-hash flip on row 0 (asserts 1).
func TestBQMarketCatalogWriter_Merge_Integration(t *testing.T) {
	project := os.Getenv("BQ_TEST_PROJECT")
	dataset := os.Getenv("BQ_TEST_DATASET")
	if project == "" || dataset == "" {
		t.Skip("BQ_TEST_PROJECT / BQ_TEST_DATASET not set")
	}

	ctx := context.Background()
	client, err := bigquery.NewClient(ctx, project)
	if err != nil {
		t.Fatalf("bq client: %v", err)
	}
	defer func() { _ = client.Close() }()

	tableName := fmt.Sprintf("market_catalog_test_%d", time.Now().UnixNano())
	t.Cleanup(func() { _ = client.Dataset(dataset).Table(tableName).Delete(ctx) })
	if err := createMarketCatalogTable(ctx, client, dataset, tableName); err != nil {
		t.Fatalf("create temp table: %v", err)
	}

	w := NewBQMarketCatalogWriter(client, project, dataset, tableName)
	now := time.Now().UTC().Truncate(time.Second)
	rows := []MarketCatalogRow{
		mkMarketRow(now, "KXBTCD-A", "KXBTCD", "open", "h-a"),
		mkMarketRow(now, "KXBTCD-B", "KXBTCD", "settled", "h-b"),
		mkMarketRow(now, "KXBTCD-C", "KXBTCD", "expired", "h-c"),
	}

	first, err := w.Merge(ctx, rows)
	if err != nil {
		t.Fatalf("merge 1: %v", err)
	}
	if first != 3 {
		t.Fatalf("merge 1 inserted %d, want 3", first)
	}

	second, err := w.Merge(ctx, rows)
	if err != nil {
		t.Fatalf("merge 2: %v", err)
	}
	if second != 0 {
		t.Fatalf("merge 2 inserted %d (idempotency broken), want 0", second)
	}

	rows[0].ContentHash = "h-a-changed"
	third, err := w.Merge(ctx, rows)
	if err != nil {
		t.Fatalf("merge 3: %v", err)
	}
	if third != 1 {
		t.Fatalf("merge 3 inserted %d, want 1", third)
	}
}

func mkMarketRow(ts time.Time, ticker, series, status, hash string) MarketCatalogRow {
	return MarketCatalogRow{
		CaptureTS:       ts,
		MarketTicker:    ticker,
		SeriesTicker:    series,
		Title:           "title-" + ticker,
		Status:          status,
		OpenTime:        bigquery.NullTimestamp{Timestamp: ts, Valid: true},
		CloseTime:       bigquery.NullTimestamp{Timestamp: ts.Add(23 * time.Hour), Valid: true},
		ExpirationTime:  bigquery.NullTimestamp{Timestamp: ts.Add(24 * time.Hour), Valid: true},
		SettlementValue: bigquery.NullString{StringVal: "", Valid: false},
		ContentHash:     hash,
	}
}

func createMarketCatalogTable(ctx context.Context, c *bigquery.Client, dataset, table string) error {
	schema := bigquery.Schema{
		{Name: "capture_ts", Type: bigquery.TimestampFieldType, Required: true},
		{Name: "market_ticker", Type: bigquery.StringFieldType, Required: true},
		{Name: "series_ticker", Type: bigquery.StringFieldType, Required: true},
		{Name: "title", Type: bigquery.StringFieldType},
		{Name: "status", Type: bigquery.StringFieldType},
		{Name: "open_time", Type: bigquery.TimestampFieldType},
		{Name: "close_time", Type: bigquery.TimestampFieldType},
		{Name: "expiration_time", Type: bigquery.TimestampFieldType},
		{Name: "settlement_value", Type: bigquery.StringFieldType},
		{Name: "content_hash", Type: bigquery.StringFieldType, Required: true},
	}
	return c.Dataset(dataset).Table(table).Create(ctx, &bigquery.TableMetadata{
		Schema: schema,
		TimePartitioning: &bigquery.TimePartitioning{
			Type:  bigquery.DayPartitioningType,
			Field: "capture_ts",
		},
		Clustering: &bigquery.Clustering{Fields: []string{"series_ticker", "market_ticker"}},
	})
}
