package kalshi

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
)

// GetSeries fetches a single page of /trade-api/v2/series. cursor may be empty for
// the first page. Returns the raw response body so callers can preserve per-row JSON
// for archival columns. /series historically returns all rows in one GET (verified
// prod 2026-05-10); the cursor parameter is honored defensively in case Kalshi flips
// pagination on later.
func (c *Client) GetSeries(ctx context.Context, cursor string) ([]byte, error) {
	q := url.Values{}
	if cursor != "" {
		q.Set("cursor", cursor)
	}
	path := "/trade-api/v2/series"
	if encoded := q.Encode(); encoded != "" {
		path += "?" + encoded
	}
	resp, err := c.do(ctx, http.MethodGet, path, nil)
	if err != nil {
		return nil, fmt.Errorf("GetSeries: %w", err)
	}
	body, err := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if err != nil {
		return nil, fmt.Errorf("GetSeries read: %w", err)
	}
	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("GetSeries: status %d, body %s", resp.StatusCode, string(body))
	}
	return body, nil
}
