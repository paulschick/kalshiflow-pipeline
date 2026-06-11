// Package seriescat fetches the Kalshi series catalog and merges hash-diffed rows
// into BigQuery. Single-shot per Cloud Run Job invocation.
package seriescat

import (
	"context"
	"encoding/json"
	"time"

	"cloud.google.com/go/bigquery"
)

// Series is one row of the Kalshi /trade-api/v2/series response, with the original
// JSON bytes preserved alongside the projected fields. RawJSON feeds the catalog
// table's `raw_json` column verbatim so future extractor columns can be added
// without re-running discovery.
type Series struct {
	Ticker      string
	Title       string
	Category    string
	Frequency   string
	Tags        []string
	ContractURL string
	RawJSON     json.RawMessage
}

// CatalogRow is the BQ row layout for kalshi_raw.series_catalog. RawJSONStr holds
// the JSON bytes as a string; the MERGE statement parses it via PARSE_JSON. This
// indirection is required because BQ array parameters cannot carry the JSON type
// directly.
type CatalogRow struct {
	CaptureTS   time.Time `bigquery:"capture_ts"`
	Ticker      string    `bigquery:"ticker"`
	Title       string    `bigquery:"title"`
	Category    string    `bigquery:"category"`
	Frequency   string    `bigquery:"frequency"`
	Tags        []string  `bigquery:"tags"`
	ContractURL string    `bigquery:"contract_url"`
	RawJSONStr  string    `bigquery:"raw_json_str"`
	ContentHash string    `bigquery:"content_hash"`
	Source      string    `bigquery:"source"`
}

// SeriesFetcher abstracts the Kalshi REST side so RunOnce can be tested with a
// stub. The concrete implementation is KalshiSeriesFetcher.
type SeriesFetcher interface {
	FetchAll(ctx context.Context) ([]Series, error)
}

// CatalogWriter abstracts the BQ side so RunOnce can be tested without hitting
// BigQuery. The concrete implementation is BQCatalogWriter.
type CatalogWriter interface {
	Merge(ctx context.Context, rows []CatalogRow) (insertedRows int64, err error)
}

// MarketCatalogRow is the BQ row layout for kalshi_raw.market_catalog. Nullable
// columns use the bigquery.Null* wrappers so the parameterized MERGE sees SQL
// NULL rather than zero values. SettlementValue is the fixed-point dollar string
// emitted by Kalshi (e.g. "0.5600") wrapped in NullString — kept verbatim because
// the OpenAPI definition is text, not numeric.
type MarketCatalogRow struct {
	CaptureTS       time.Time              `bigquery:"capture_ts"`
	MarketTicker    string                 `bigquery:"market_ticker"`
	SeriesTicker    string                 `bigquery:"series_ticker"`
	Title           string                 `bigquery:"title"`
	Status          string                 `bigquery:"status"`
	OpenTime        bigquery.NullTimestamp `bigquery:"open_time"`
	CloseTime       bigquery.NullTimestamp `bigquery:"close_time"`
	ExpirationTime  bigquery.NullTimestamp `bigquery:"expiration_time"`
	SettlementValue bigquery.NullString    `bigquery:"settlement_value"`
	ContentHash     string                 `bigquery:"content_hash"`
}
