//go:build integration

package seriescat

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"

	"cloud.google.com/go/bigquery"
)

// TestBQCatalogWriter_Merge_Integration requires:
//
//	BQ_TEST_PROJECT  — project owning the sandbox dataset
//	BQ_TEST_DATASET  — dataset that does NOT contain a real series_catalog
//
// Creates a temp table per run (named series_catalog_test_<unix-nanos>), MERGEs a
// canned 3-row batch, asserts NumDMLAffectedRows=3, MERGEs the same batch again,
// asserts NumDMLAffectedRows=0 (idempotency), MERGEs the batch with a content-hash
// flip on row 0, asserts NumDMLAffectedRows=1.
func TestBQCatalogWriter_Merge_Integration(t *testing.T) {
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

	tableName := fmt.Sprintf("series_catalog_test_%d", time.Now().UnixNano())
	t.Cleanup(func() {
		_ = client.Dataset(dataset).Table(tableName).Delete(ctx)
	})
	if err := createCatalogTable(ctx, client, dataset, tableName); err != nil {
		t.Fatalf("create temp table: %v", err)
	}

	w := NewBQCatalogWriter(client, project, dataset, tableName)
	now := time.Now().UTC().Truncate(time.Second)
	rows := []CatalogRow{
		mkRow(now, "KXA", "title-a", "h-a"),
		mkRow(now, "KXB", "title-b", "h-b"),
		mkRow(now, "KXC", "title-c", "h-c"),
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

func mkRow(ts time.Time, ticker, title, hash string) CatalogRow {
	body, _ := json.Marshal(map[string]string{"ticker": ticker, "title": title})
	return CatalogRow{
		CaptureTS:   ts,
		Ticker:      ticker,
		Title:       title,
		Category:    "Crypto",
		Frequency:   "DAILY",
		Tags:        []string{"x"},
		ContractURL: "https://example.com",
		RawJSONStr:  string(body),
		ContentHash: hash,
		Source:      "series-discovery",
	}
}

func createCatalogTable(ctx context.Context, c *bigquery.Client, dataset, table string) error {
	schema := bigquery.Schema{
		{Name: "capture_ts", Type: bigquery.TimestampFieldType, Required: true},
		{Name: "ticker", Type: bigquery.StringFieldType, Required: true},
		{Name: "title", Type: bigquery.StringFieldType},
		{Name: "category", Type: bigquery.StringFieldType},
		{Name: "frequency", Type: bigquery.StringFieldType},
		{Name: "tags", Type: bigquery.StringFieldType, Repeated: true},
		{Name: "contract_url", Type: bigquery.StringFieldType},
		{Name: "raw_json", Type: bigquery.JSONFieldType},
		{Name: "content_hash", Type: bigquery.StringFieldType, Required: true},
		{Name: "source", Type: bigquery.StringFieldType, Required: true},
	}
	return c.Dataset(dataset).Table(table).Create(ctx, &bigquery.TableMetadata{
		Schema: schema,
		TimePartitioning: &bigquery.TimePartitioning{
			Type:  bigquery.DayPartitioningType,
			Field: "capture_ts",
		},
		Clustering: &bigquery.Clustering{Fields: []string{"ticker"}},
	})
}
