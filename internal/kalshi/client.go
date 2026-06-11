package kalshi

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math"
	"math/rand"
	"net/http"
	"strconv"
	"time"

	"golang.org/x/time/rate"
)

// Config configures the REST client.
type Config struct {
	BaseURL     string
	Signer      *Signer
	HTTPClient  *http.Client
	Now         func() time.Time
	MaxRetries  int
	BaseBackoff time.Duration

	// Defaults: 10 RPS / 20 burst — 50% of Kalshi Basic tier read budget.
	// 0 = use default. Configurable at cmd layer via KALSHI_REST_RPS / KALSHI_REST_BURST.
	RPS   float64
	Burst int
}

// Client is the Kalshi REST client. Auto-injects auth via Signer; client-side token bucket
// throttles bursts as defense-in-depth across initial-pop + future audits.
type Client struct {
	cfg Config
	lim *rate.Limiter
}

// NewClient constructs a Client, applying defaults for unset fields.
func NewClient(cfg Config) *Client {
	if cfg.HTTPClient == nil {
		cfg.HTTPClient = &http.Client{Timeout: 30 * time.Second}
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	if cfg.MaxRetries == 0 {
		cfg.MaxRetries = 5
	}
	if cfg.BaseBackoff == 0 {
		cfg.BaseBackoff = 250 * time.Millisecond
	}
	rps := cfg.RPS
	if rps == 0 {
		rps = 10
	}
	burst := cfg.Burst
	if burst == 0 {
		burst = 20
	}
	return &Client{cfg: cfg, lim: rate.NewLimiter(rate.Limit(rps), burst)}
}

// do issues an authenticated request with retries on 429 / 503.
// Blocks on the client-side token bucket before each attempt.
func (c *Client) do(ctx context.Context, method, path string, body []byte) (*http.Response, error) {
	var attempt int
	for {
		if err := c.lim.Wait(ctx); err != nil {
			return nil, fmt.Errorf("kalshi: %s %s: rate limiter wait: %w", method, path, err)
		}
		req, err := c.buildRequest(ctx, method, path, body)
		if err != nil {
			return nil, err
		}

		resp, err := c.cfg.HTTPClient.Do(req)
		if err != nil {
			if attempt >= c.cfg.MaxRetries {
				return nil, fmt.Errorf("kalshi: %s %s: %w", method, path, err)
			}
			c.sleepBackoff(ctx, attempt, 0)
			attempt++
			continue
		}

		switch resp.StatusCode {
		case http.StatusTooManyRequests, http.StatusServiceUnavailable:
			retryAfter := parseRetryAfter(resp.Header.Get("Retry-After"))
			_ = resp.Body.Close()
			if attempt >= c.cfg.MaxRetries {
				return nil, fmt.Errorf("kalshi: %s %s: max retries exceeded (status %d)", method, path, resp.StatusCode)
			}
			c.sleepBackoff(ctx, attempt, retryAfter)
			attempt++
			continue
		}
		return resp, nil
	}
}

func (c *Client) buildRequest(ctx context.Context, method, path string, body []byte) (*http.Request, error) {
	url := c.cfg.BaseURL + path
	var rdr io.Reader
	if body != nil {
		rdr = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, method, url, rdr)
	if err != nil {
		return nil, err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	ts := strconv.FormatInt(c.cfg.Now().UnixMilli(), 10)
	signPath := stripQuery(path)
	headers, err := c.cfg.Signer.Headers(ts, method, signPath)
	if err != nil {
		return nil, err
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	return req, nil
}

func (c *Client) sleepBackoff(ctx context.Context, attempt int, hint time.Duration) {
	d := hint
	if d <= 0 {
		base := time.Duration(math.Pow(2, float64(attempt))) * c.cfg.BaseBackoff
		if base > 30*time.Second {
			base = 30 * time.Second
		}
		d = base + time.Duration(rand.Int63n(int64(c.cfg.BaseBackoff)))
	}
	select {
	case <-ctx.Done():
	case <-time.After(d):
	}
}

func parseRetryAfter(h string) time.Duration {
	if h == "" {
		return 0
	}
	if secs, err := strconv.Atoi(h); err == nil {
		return time.Duration(secs) * time.Second
	}
	if t, err := http.ParseTime(h); err == nil {
		return time.Until(t)
	}
	return 0
}
