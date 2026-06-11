package seriescat

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/paulschick/kalshiflow-pipeline/internal/kalshi"
)

// Market is one row of the Kalshi /trade-api/v2/markets response, projected to
// the typed fields the catalog needs.
//
// SeriesTicker is NOT decoded from the JSON payload — Kalshi's Market response
// schema does not include `series_ticker` (it lives on Event/Series). The
// fetcher stamps it from the per-call argument.
//
// SettlementValue is the fixed-point dollar string Kalshi emits (e.g.
// "0.5600"); per OpenAPI it's `settlement_value_dollars`, but older payloads
// may emit the unprefixed `settlement_value` name. The fetcher tries both.
type Market struct {
	Ticker          string
	SeriesTicker    string
	Title           string
	Status          string
	OpenTime        *time.Time
	CloseTime       *time.Time
	ExpirationTime  *time.Time
	SettlementValue *string
}

// MarketsFetcher abstracts the Kalshi REST side so RunOnce can be tested
// with a stub. The concrete implementation is KalshiMarketsFetcher.
//
// minCloseTS scopes the fetch to markets whose close_time is at or after that
// instant; pass the zero value to disable the filter.
type MarketsFetcher interface {
	FetchAllForSeries(ctx context.Context, seriesTicker string, minCloseTS time.Time) ([]Market, error)
}

// KalshiMarketsFetcher implements MarketsFetcher over a *kalshi.Client.
// Walks the /markets cursor; omits the `status` query param so settled,
// expired, and deactivated markets are captured alongside open ones.
type KalshiMarketsFetcher struct {
	client *kalshi.Client
}

// NewKalshiMarketsFetcher constructs a fetcher.
func NewKalshiMarketsFetcher(c *kalshi.Client) *KalshiMarketsFetcher {
	return &KalshiMarketsFetcher{client: c}
}

type marketsPage struct {
	Markets []json.RawMessage `json:"markets"`
	Cursor  string            `json:"cursor"`
}

// marketProj is the JSON projection. `series_ticker` is intentionally omitted
// — it is NOT a Market response field (lives on Event/Series). Both
// `settlement_value_dollars` (current OpenAPI name) and `settlement_value`
// (legacy) are read; whichever is non-nil wins.
type marketProj struct {
	Ticker                 string  `json:"ticker"`
	Title                  string  `json:"title"`
	Status                 string  `json:"status"`
	OpenTime               string  `json:"open_time"`
	CloseTime              string  `json:"close_time"`
	ExpirationTime         string  `json:"expiration_time"`
	SettlementValueDollars *string `json:"settlement_value_dollars,omitempty"`
	SettlementValue        *string `json:"settlement_value,omitempty"`
}

// FetchAllForSeries walks the cursor and returns every Market row for the
// given series_ticker, including non-open statuses. Each Market's SeriesTicker
// is stamped from the seriesTicker argument (the response payload does not
// carry it).
//
// minCloseTS is forwarded to the underlying client as Kalshi's `min_close_ts`
// query param; pass the zero value to disable the filter.
func (f *KalshiMarketsFetcher) FetchAllForSeries(ctx context.Context, seriesTicker string, minCloseTS time.Time) ([]Market, error) {
	var out []Market
	cursor := ""
	for {
		body, err := f.client.GetMarketsPage(ctx, seriesTicker, cursor, minCloseTS)
		if err != nil {
			return nil, fmt.Errorf("seriescat: GetMarketsPage %s: %w", seriesTicker, err)
		}
		var page marketsPage
		if err := json.Unmarshal(body, &page); err != nil {
			return nil, fmt.Errorf("seriescat: decode markets page for %s: %w", seriesTicker, err)
		}
		for _, raw := range page.Markets {
			var p marketProj
			if err := json.Unmarshal(raw, &p); err != nil {
				return nil, fmt.Errorf("seriescat: decode market row: %w", err)
			}
			settlement := p.SettlementValueDollars
			if settlement == nil {
				settlement = p.SettlementValue
			}
			out = append(out, Market{
				Ticker:          p.Ticker,
				SeriesTicker:    seriesTicker, // stamped from arg; not in response
				Title:           p.Title,
				Status:          p.Status,
				OpenTime:        parseRFC3339(p.OpenTime),
				CloseTime:       parseRFC3339(p.CloseTime),
				ExpirationTime:  parseRFC3339(p.ExpirationTime),
				SettlementValue: settlement,
			})
		}
		if page.Cursor == "" {
			return out, nil
		}
		cursor = page.Cursor
	}
}

// parseRFC3339 returns a pointer to the parsed time or nil for empty / unparseable
// input. The catalog table tolerates NULL for these columns; nil-on-bad-input is
// equivalent to "Kalshi didn't include it."
func parseRFC3339(s string) *time.Time {
	if s == "" {
		return nil
	}
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		return nil
	}
	return &t
}
