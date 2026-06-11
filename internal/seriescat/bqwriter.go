package seriescat

import (
	"context"
	"fmt"

	"cloud.google.com/go/bigquery"
)

// mergeBatchSize keeps each MERGE statement well under BQ's 10 MB query payload
// limit. ~10k catalog rows at ~1KB raw_json each = ~10MB; chunking at 5000
// halves it with comfortable headroom.
const mergeBatchSize = 5000

// BQCatalogWriter implements CatalogWriter via parameterized BQ MERGE.
type BQCatalogWriter struct {
	client  *bigquery.Client
	project string
	dataset string
	table   string
}

// NewBQCatalogWriter constructs a writer.
func NewBQCatalogWriter(c *bigquery.Client, project, dataset, table string) *BQCatalogWriter {
	return &BQCatalogWriter{client: c, project: project, dataset: dataset, table: table}
}

// Merge MERGEs rows into the catalog table in batches of ≤mergeBatchSize. Returns
// the cumulative NumDMLAffectedRows across batches.
func (w *BQCatalogWriter) Merge(ctx context.Context, rows []CatalogRow) (int64, error) {
	var total int64
	for i := 0; i < len(rows); i += mergeBatchSize {
		end := i + mergeBatchSize
		if end > len(rows) {
			end = len(rows)
		}
		n, err := w.mergeBatch(ctx, rows[i:end])
		if err != nil {
			return total, fmt.Errorf("merge batch [%d:%d]: %w", i, end, err)
		}
		total += n
	}
	return total, nil
}

func (w *BQCatalogWriter) mergeBatch(ctx context.Context, batch []CatalogRow) (int64, error) {
	sql := fmt.Sprintf(`
MERGE `+"`%s.%s.%s`"+` T
USING (SELECT * FROM UNNEST(@rows)) S
ON T.ticker = S.ticker AND T.content_hash = S.content_hash
WHEN NOT MATCHED THEN
  INSERT (capture_ts, ticker, title, category, frequency, tags, contract_url, raw_json, content_hash, source)
  VALUES (S.capture_ts, S.ticker, S.title, S.category, S.frequency, S.tags, S.contract_url, PARSE_JSON(S.raw_json_str), S.content_hash, S.source)
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
