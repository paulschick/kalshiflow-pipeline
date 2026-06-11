// Package rest is a thin Kraken REST client. Provides AssetPairs CSV pre-flight;
// counter cost 1 per call regardless of pair count.
package rest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"time"
)

// ErrUnknownPair is returned when the AssetPairs body carries a non-empty
// error array. Kraken returns the entire call as failed on any unknown pair;
// the result block is OMITTED (not null).
var ErrUnknownPair = errors.New("kraken/rest: unknown asset pair in CSV request")

// UnknownPairError is the concrete type returned by AssetPairs when Kraken
// rejects the CSV. It satisfies errors.Is(err, ErrUnknownPair) via its Is
// method and additionally exposes the verbatim Kraken `error` array contents
// for the bisect-and-log path in AssetPairsWithFallback + subscription_log.
type UnknownPairError struct {
	KrakenMessage string // verbatim Kraken `error` element(s), joined by "; "
}

func (e *UnknownPairError) Error() string {
	return ErrUnknownPair.Error() + ": " + e.KrakenMessage
}

// Is makes errors.Is(err, ErrUnknownPair) true for *UnknownPairError.
func (e *UnknownPairError) Is(target error) bool { return target == ErrUnknownPair }

// Rejection identifies one pair the CSV preflight rejected, with the verbatim
// Kraken error string. Used to populate the reason column of
// kraken_raw.subscription_log.
type Rejection struct {
	Pair       string
	ErrMessage string
}

// Client is a thin AssetPairs caller. One method by design.
type Client struct {
	httpClient *http.Client
	baseURL    string
}

// New constructs a client. baseURL defaults to https://api.kraken.com if empty.
func New(httpClient *http.Client, baseURL string) *Client {
	if httpClient == nil {
		httpClient = &http.Client{Timeout: 10 * time.Second}
	}
	if baseURL == "" {
		baseURL = "https://api.kraken.com"
	}
	return &Client{httpClient: httpClient, baseURL: strings.TrimRight(baseURL, "/")}
}

// AssetPairs issues one CSV call covering every pair in the input. Counter
// cost is 1 regardless of pair count. Returns a map keyed by INPUT slash form.
func (c *Client) AssetPairs(ctx context.Context, pairs []string) (map[string]PairInfo, error) {
	if len(pairs) == 0 {
		return nil, errors.New("kraken/rest: pairs must be non-empty")
	}
	sorted := append([]string{}, pairs...)
	sort.Strings(sorted)
	qp := url.Values{}
	qp.Set("pair", strings.Join(sorted, ","))
	endpoint := c.baseURL + "/0/public/AssetPairs?" + qp.Encode()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, fmt.Errorf("kraken/rest: build request: %w", err)
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("kraken/rest: do: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("kraken/rest: read body: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("kraken/rest: status %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	var env struct {
		Error  []string                   `json:"error"`
		Result map[string]json.RawMessage `json:"result"`
	}
	dec := json.NewDecoder(strings.NewReader(string(body)))
	dec.UseNumber()
	if err := dec.Decode(&env); err != nil {
		return nil, fmt.Errorf("kraken/rest: decode envelope: %w", err)
	}
	if len(env.Error) > 0 {
		return nil, &UnknownPairError{KrakenMessage: strings.Join(env.Error, "; ")}
	}
	out := make(map[string]PairInfo, len(env.Result))
	for key, raw := range env.Result {
		var info PairInfo
		d := json.NewDecoder(strings.NewReader(string(raw)))
		d.UseNumber()
		if err := d.Decode(&info); err != nil {
			return nil, fmt.Errorf("kraken/rest: decode %q: %w", key, err)
		}
		info.Pair = key
		out[key] = info
	}
	return out, nil
}

// AssetPairsWithFallback issues one CSV call; on UnknownPairError (Kraken
// rejects the whole CSV on any one bad pair), bisects the input until each
// rejected pair is isolated, then issues per-pair calls for the rest.
//
// Counter cost: 1 on the happy path; bounded by O(log N) + (N-k) for k
// rejected pairs on the worst path. At 5-20 pairs and rare operator typos,
// well within the Starter-tier 15-counter cap.
//
// Returns:
//   - results: map keyed by INPUT slash form (only successful pairs).
//   - rejections: per-pair, verbatim Kraken error string suitable for
//     kraken_raw.subscription_log.
//   - err: non-nil only on network/protocol error, not on pair rejections.
func (c *Client) AssetPairsWithFallback(ctx context.Context, pairs []string) (map[string]PairInfo, []Rejection, error) {
	if len(pairs) == 0 {
		return nil, nil, errors.New("kraken/rest: pairs must be non-empty")
	}
	// Dedup + stable order for determinism in tests and logs.
	dedup := make([]string, 0, len(pairs))
	seen := make(map[string]struct{}, len(pairs))
	for _, p := range pairs {
		if _, ok := seen[p]; ok {
			continue
		}
		seen[p] = struct{}{}
		dedup = append(dedup, p)
	}
	sort.Strings(dedup)
	results := map[string]PairInfo{}
	rejections := []Rejection{}
	if err := c.bisect(ctx, dedup, results, &rejections); err != nil {
		return nil, nil, err
	}
	sort.Slice(rejections, func(i, j int) bool { return rejections[i].Pair < rejections[j].Pair })
	return results, rejections, nil
}

// bisect recursively isolates the rejected pair(s) when AssetPairs returns
// UnknownPairError for a multi-pair CSV. Non-UnknownPair errors propagate.
func (c *Client) bisect(ctx context.Context, pairs []string, results map[string]PairInfo, rejections *[]Rejection) error {
	got, err := c.AssetPairs(ctx, pairs)
	if err == nil {
		for k, v := range got {
			results[k] = v
		}
		return nil
	}
	var upe *UnknownPairError
	if !errors.As(err, &upe) {
		return err
	}
	if len(pairs) == 1 {
		*rejections = append(*rejections, Rejection{Pair: pairs[0], ErrMessage: upe.KrakenMessage})
		return nil
	}
	mid := len(pairs) / 2
	if err := c.bisect(ctx, pairs[:mid], results, rejections); err != nil {
		return err
	}
	return c.bisect(ctx, pairs[mid:], results, rejections)
}
