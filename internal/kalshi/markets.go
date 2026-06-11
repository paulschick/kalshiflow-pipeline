package kalshi

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"time"
)

type market struct {
	Ticker string `json:"ticker"`
}

type marketsPage struct {
	Markets []market `json:"markets"`
	Cursor  string   `json:"cursor"`
}

// GetOpenMarkets returns market_ticker values for every market in the given series whose
// Kalshi-side status equals openStatus. The openStatus value is probe-derived (see
// scripts/probe-kalshi-status.py) and configurable via KALSHI_OPEN_STATUS at the cmd
// layer; the 2026-05-03 probe verdict is "open".
//
// Iterates the cursor until empty or maxPages reached. maxPages = 0 means unbounded.
// hitCap is true iff the loop returned because pages reached maxPages with a non-empty
// cursor still pending — surfaces "series has more markets than expected" at the caller
// so it can fire discovery_pagination_capped_total.
func (c *Client) GetOpenMarkets(ctx context.Context, seriesTicker, openStatus string, maxPages int) (tickers []string, hitCap bool, err error) {
	q := url.Values{}
	q.Set("series_ticker", seriesTicker)
	q.Set("status", openStatus)
	q.Set("limit", "1000")

	cursor := ""
	out := []string{}
	pages := 0
	for {
		if cursor != "" {
			q.Set("cursor", cursor)
		}
		path := "/trade-api/v2/markets?" + q.Encode()
		resp, err := c.do(ctx, http.MethodGet, path, nil)
		if err != nil {
			return nil, false, fmt.Errorf("GetOpenMarkets %s: %w", seriesTicker, err)
		}
		body, err := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if err != nil {
			return nil, false, fmt.Errorf("GetOpenMarkets read: %w", err)
		}
		if resp.StatusCode >= 400 {
			return nil, false, fmt.Errorf("GetOpenMarkets %s: status %d, body %s", seriesTicker, resp.StatusCode, string(body))
		}

		var page marketsPage
		if err := json.Unmarshal(body, &page); err != nil {
			return nil, false, fmt.Errorf("GetOpenMarkets decode: %w", err)
		}
		for _, m := range page.Markets {
			if m.Ticker != "" {
				out = append(out, m.Ticker)
			}
		}
		pages++
		if page.Cursor == "" {
			return out, false, nil
		}
		if maxPages > 0 && pages >= maxPages {
			return out, true, nil
		}
		cursor = page.Cursor
	}
}

// GetMarketsPage fetches one page of /trade-api/v2/markets for the given series_ticker.
// Returns the raw response body so callers can preserve per-row JSON for archival columns.
//
// Unlike GetOpenMarkets, this helper omits the `status` query param — settled / expired /
// deactivated markets are returned. Cursor handling is the caller's responsibility; pass
// the empty string for the first page, then the value of the response's `cursor` field
// until it comes back empty.
//
// When minCloseTS is non-zero, the request adds Kalshi's `min_close_ts=<unix-seconds>`
// query param so the server only returns markets whose close_time is at or after that
// instant. Pass the zero value to skip the filter and request everything.
func (c *Client) GetMarketsPage(ctx context.Context, seriesTicker, cursor string, minCloseTS time.Time) ([]byte, error) {
	q := url.Values{}
	q.Set("series_ticker", seriesTicker)
	q.Set("limit", "1000")
	if !minCloseTS.IsZero() {
		q.Set("min_close_ts", strconv.FormatInt(minCloseTS.Unix(), 10))
	}
	if cursor != "" {
		q.Set("cursor", cursor)
	}
	path := "/trade-api/v2/markets?" + q.Encode()
	resp, err := c.do(ctx, http.MethodGet, path, nil)
	if err != nil {
		return nil, fmt.Errorf("GetMarketsPage %s: %w", seriesTicker, err)
	}
	body, err := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if err != nil {
		return nil, fmt.Errorf("GetMarketsPage read: %w", err)
	}
	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("GetMarketsPage %s: status %d, body %s", seriesTicker, resp.StatusCode, string(body))
	}
	return body, nil
}
