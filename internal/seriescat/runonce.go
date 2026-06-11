package seriescat

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"cloud.google.com/go/bigquery"

	"github.com/paulschick/kalshiflow-pipeline/internal/metrics"
)

// marketsLookback bounds the per-series markets fetch to recently-active markets.
// 48h gives an 8x cushion over the 6h scheduler cadence so settled markets remain
// visible across at least one re-run before Kalshi removes them, while keeping
// the per-series response far under the OOM threshold that "all historical
// markets" was hitting.
const marketsLookback = 48 * time.Hour

// RunOnce executes one full discovery pass: series fetch+MERGE, then per-ticker
// markets fetch+MERGE for every ticker in subscribedTickers. captureTS is the
// timestamp written to every row in this batch — pinned once by the caller so all
// rows share a partition. Fail-fast: any per-ticker error in the markets pass
// returns immediately; downstream tickers are not attempted.
func RunOnce(
	ctx context.Context,
	seriesFetcher SeriesFetcher,
	catalogWriter CatalogWriter,
	marketsFetcher MarketsFetcher,
	marketWriter MarketCatalogWriter,
	subscribedTickers []string,
	m *metrics.Metrics,
	captureTS time.Time,
) (retErr error) {
	started := time.Now()
	outcome := "error"
	defer func() {
		m.SeriesDiscoveryRunDuration.Record(ctx, time.Since(started).Seconds(), "outcome", outcome)
		m.SeriesDiscoveryRunOutcome.Add(ctx, 1, "outcome", outcome)
	}()

	if err := runSeriesPass(ctx, seriesFetcher, catalogWriter, m, captureTS); err != nil {
		return err
	}

	if err := runMarketsPass(ctx, marketsFetcher, marketWriter, subscribedTickers, m, captureTS); err != nil {
		return err
	}

	outcome = "ok"
	return nil
}

func runSeriesPass(
	ctx context.Context,
	fetcher SeriesFetcher,
	writer CatalogWriter,
	m *metrics.Metrics,
	captureTS time.Time,
) error {
	series, err := fetcher.FetchAll(ctx)
	if err != nil {
		return fmt.Errorf("seriescat: fetch series: %w", err)
	}

	rows := make([]CatalogRow, 0, len(series))
	for _, s := range series {
		h, err := CanonicalHash(s.RawJSON)
		if err != nil {
			return fmt.Errorf("seriescat: hash series %q: %w", s.Ticker, err)
		}
		rows = append(rows, CatalogRow{
			CaptureTS:   captureTS,
			Ticker:      s.Ticker,
			Title:       s.Title,
			Category:    s.Category,
			Frequency:   s.Frequency,
			Tags:        s.Tags,
			ContractURL: s.ContractURL,
			RawJSONStr:  string(s.RawJSON),
			ContentHash: h,
			Source:      "series-discovery",
		})
	}

	inserted, err := writer.Merge(ctx, rows)
	if err != nil {
		return fmt.Errorf("seriescat: merge series: %w", err)
	}
	m.SeriesDiscoveryRowsInserted.Add(ctx, inserted, "kind", "catalog")
	return nil
}

func runMarketsPass(
	ctx context.Context,
	fetcher MarketsFetcher,
	writer MarketCatalogWriter,
	tickers []string,
	m *metrics.Metrics,
	captureTS time.Time,
) error {
	minCloseTS := captureTS.Add(-marketsLookback)
	for _, ticker := range tickers {
		markets, err := fetcher.FetchAllForSeries(ctx, ticker, minCloseTS)
		if err != nil {
			return fmt.Errorf("seriescat: fetch markets %q: %w", ticker, err)
		}

		rows := make([]MarketCatalogRow, 0, len(markets))
		for _, mk := range markets {
			proj, err := json.Marshal(struct {
				Ticker          string  `json:"ticker"`
				Title           string  `json:"title"`
				Status          string  `json:"status"`
				OpenTime        string  `json:"open_time,omitempty"`
				CloseTime       string  `json:"close_time,omitempty"`
				ExpirationTime  string  `json:"expiration_time,omitempty"`
				SettlementValue *string `json:"settlement_value,omitempty"`
			}{
				Ticker:          mk.Ticker,
				Title:           mk.Title,
				Status:          mk.Status,
				OpenTime:        formatRFC3339Ptr(mk.OpenTime),
				CloseTime:       formatRFC3339Ptr(mk.CloseTime),
				ExpirationTime:  formatRFC3339Ptr(mk.ExpirationTime),
				SettlementValue: mk.SettlementValue,
			})
			if err != nil {
				return fmt.Errorf("seriescat: marshal market projection %q: %w", mk.Ticker, err)
			}
			h, err := CanonicalHash(proj)
			if err != nil {
				return fmt.Errorf("seriescat: hash market %q: %w", mk.Ticker, err)
			}
			rows = append(rows, MarketCatalogRow{
				CaptureTS:       captureTS,
				MarketTicker:    mk.Ticker,
				SeriesTicker:    mk.SeriesTicker,
				Title:           mk.Title,
				Status:          mk.Status,
				OpenTime:        nullTS(mk.OpenTime),
				CloseTime:       nullTS(mk.CloseTime),
				ExpirationTime:  nullTS(mk.ExpirationTime),
				SettlementValue: nullStr(mk.SettlementValue),
				ContentHash:     h,
			})
		}

		inserted, err := writer.Merge(ctx, rows)
		if err != nil {
			return fmt.Errorf("seriescat: merge markets %q: %w", ticker, err)
		}
		m.SeriesDiscoveryRowsInserted.Add(ctx, inserted, "kind", "market")
	}
	return nil
}

func nullTS(t *time.Time) bigquery.NullTimestamp {
	if t == nil {
		return bigquery.NullTimestamp{}
	}
	return bigquery.NullTimestamp{Timestamp: *t, Valid: true}
}

func nullStr(v *string) bigquery.NullString {
	if v == nil {
		return bigquery.NullString{}
	}
	return bigquery.NullString{StringVal: *v, Valid: true}
}

func formatRFC3339Ptr(t *time.Time) string {
	if t == nil {
		return ""
	}
	return t.UTC().Format(time.RFC3339)
}
