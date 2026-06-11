package seriescat

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/paulschick/kalshiflow-pipeline/internal/kalshi"
)

// KalshiSeriesFetcher implements SeriesFetcher over a *kalshi.Client. Cursor-loop
// is defensive: /series returns all rows in one GET today (verified prod
// 2026-05-10), but the loop tolerates pagination if Kalshi flips it on later.
type KalshiSeriesFetcher struct {
	client *kalshi.Client
}

// NewKalshiSeriesFetcher constructs a fetcher.
func NewKalshiSeriesFetcher(c *kalshi.Client) *KalshiSeriesFetcher {
	return &KalshiSeriesFetcher{client: c}
}

type seriesPage struct {
	Series []json.RawMessage `json:"series"`
	Cursor string            `json:"cursor"`
}

type seriesProj struct {
	Ticker      string   `json:"ticker"`
	Title       string   `json:"title"`
	Category    string   `json:"category"`
	Frequency   string   `json:"frequency"`
	Tags        []string `json:"tags"`
	ContractURL string   `json:"contract_url"`
}

// FetchAll walks the cursor and returns every Series row.
func (f *KalshiSeriesFetcher) FetchAll(ctx context.Context) ([]Series, error) {
	var out []Series
	cursor := ""
	for {
		body, err := f.client.GetSeries(ctx, cursor)
		if err != nil {
			return nil, fmt.Errorf("seriescat: GetSeries: %w", err)
		}
		var page seriesPage
		if err := json.Unmarshal(body, &page); err != nil {
			return nil, fmt.Errorf("seriescat: decode page: %w", err)
		}
		for _, raw := range page.Series {
			var p seriesProj
			if err := json.Unmarshal(raw, &p); err != nil {
				return nil, fmt.Errorf("seriescat: decode row: %w", err)
			}
			out = append(out, Series{
				Ticker:      p.Ticker,
				Title:       p.Title,
				Category:    p.Category,
				Frequency:   p.Frequency,
				Tags:        p.Tags,
				ContractURL: p.ContractURL,
				RawJSON:     append(json.RawMessage(nil), raw...),
			})
		}
		if page.Cursor == "" {
			return out, nil
		}
		cursor = page.Cursor
	}
}
