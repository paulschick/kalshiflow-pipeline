package seriescat

import (
	"context"
	"fmt"

	"cloud.google.com/go/bigquery"
)

// MarketCatalogWriter abstracts the BQ side of the markets pass so RunOnce
// can be tested without hitting BigQuery. The concrete implementation is
// BQMarketCatalogWriter.
type MarketCatalogWriter interface {
	Merge(ctx context.Context, rows []MarketCatalogRow) (insertedRows int64, err error)
}

// BQMarketCatalogWriter implements MarketCatalogWriter via parameterized
// BQ MERGE keyed on (market_ticker, content_hash). Two racing runs
// observing the same payload collapse to one row.
type BQMarketCatalogWriter struct {
	client  *bigquery.Client
	project string
	dataset string
	table   string
}

// NewBQMarketCatalogWriter constructs a writer.
func NewBQMarketCatalogWriter(c *bigquery.Client, project, dataset, table string) *BQMarketCatalogWriter {
	return &BQMarketCatalogWriter{client: c, project: project, dataset: dataset, table: table}
}

// Merge MERGEs rows into the market_catalog table in batches of ≤mergeBatchSize.
// Returns the cumulative NumDMLAffectedRows across batches.
func (w *BQMarketCatalogWriter) Merge(ctx context.Context, rows []MarketCatalogRow) (int64, error) {
	var total int64
	for i := 0; i < len(rows); i += mergeBatchSize {
		end := i + mergeBatchSize
		if end > len(rows) {
			end = len(rows)
		}
		n, err := w.mergeBatch(ctx, rows[i:end])
		if err != nil {
			return total, fmt.Errorf("merge market batch [%d:%d]: %w", i, end, err)
		}
		total += n
	}
	return total, nil
}

func (w *BQMarketCatalogWriter) mergeBatch(ctx context.Context, batch []MarketCatalogRow) (int64, error) {
	sql := fmt.Sprintf(`
MERGE `+"`%s.%s.%s`"+` T
USING (SELECT * FROM UNNEST(@rows)) S
ON T.market_ticker = S.market_ticker AND T.content_hash = S.content_hash
WHEN NOT MATCHED THEN
  INSERT (capture_ts, market_ticker, series_ticker, title, status,
          open_time, close_time, expiration_time, settlement_value,
          content_hash)
  VALUES (S.capture_ts, S.market_ticker, S.series_ticker, S.title, S.status,
          S.open_time, S.close_time, S.expiration_time, S.settlement_value,
          S.content_hash)
`, w.project, w.dataset, w.table)

	q := w.client.Query(sql)
	q.Parameters = []bigquery.QueryParameter{{Name: "rows", Value: batch}}

	job, err := q.Run(ctx)
	if err != nil {
		return 0, fmt.Errorf("submit: %w", err)
	}
	status, err := job.Wait(ctx)
	if err != nil {
		return 0, fmt.Errorf("wait: %w", err)
	}
	if err := status.Err(); err != nil {
		return 0, fmt.Errorf("job: %w", err)
	}
	if stats, ok := status.Statistics.Details.(*bigquery.QueryStatistics); ok {
		return stats.NumDMLAffectedRows, nil
	}
	return 0, nil
}
