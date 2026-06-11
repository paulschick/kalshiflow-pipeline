package seriescat

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/paulschick/kalshiflow-pipeline/internal/metrics"
)

type fakeFetcher struct {
	out []Series
	err error
}

func (f *fakeFetcher) FetchAll(_ context.Context) ([]Series, error) { return f.out, f.err }

type fakeWriter struct {
	got      []CatalogRow
	inserted int64
	err      error
}

func (w *fakeWriter) Merge(_ context.Context, rows []CatalogRow) (int64, error) {
	w.got = append([]CatalogRow(nil), rows...)
	return w.inserted, w.err
}

type fakeMarketsFetcher struct {
	byTicker map[string][]Market
	errFor   map[string]error
	calls    []string
}

func (f *fakeMarketsFetcher) FetchAllForSeries(_ context.Context, t string, _ time.Time) ([]Market, error) {
	f.calls = append(f.calls, t)
	if err, ok := f.errFor[t]; ok {
		return nil, err
	}
	return f.byTicker[t], nil
}

type fakeMarketWriter struct {
	byCallRows [][]MarketCatalogRow
	inserted   int64
	err        error
}

func (w *fakeMarketWriter) Merge(_ context.Context, rows []MarketCatalogRow) (int64, error) {
	w.byCallRows = append(w.byCallRows, append([]MarketCatalogRow(nil), rows...))
	return w.inserted, w.err
}

func mkSeries(ticker, title string) Series {
	body, _ := json.Marshal(map[string]string{"ticker": ticker, "title": title})
	return Series{Ticker: ticker, Title: title, RawJSON: body}
}

func mkMarket(ticker, series, status string) Market {
	return Market{Ticker: ticker, SeriesTicker: series, Status: status}
}

func TestRunOnce_HappyPath_SeriesOnly(t *testing.T) {
	ctx := context.Background()
	m, err := metrics.New(ctx, metrics.Options{Disabled: true})
	if err != nil {
		t.Fatalf("metrics: %v", err)
	}
	f := &fakeFetcher{out: []Series{mkSeries("KXA", "A"), mkSeries("KXB", "B")}}
	w := &fakeWriter{inserted: 2}
	mf := &fakeMarketsFetcher{}
	mw := &fakeMarketWriter{}

	captureTS := time.Date(2026, 5, 10, 12, 0, 0, 0, time.UTC)
	if err := RunOnce(ctx, f, w, mf, mw, nil, m, captureTS); err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if len(w.got) != 2 {
		t.Fatalf("series writer got %d rows, want 2", len(w.got))
	}
	if len(mf.calls) != 0 {
		t.Fatalf("markets fetcher called with empty subscribed set: %v", mf.calls)
	}
	if len(mw.byCallRows) != 0 {
		t.Fatalf("market writer called with empty subscribed set: %d times", len(mw.byCallRows))
	}
}

func TestRunOnce_HappyPath_WithMarkets(t *testing.T) {
	ctx := context.Background()
	m, _ := metrics.New(ctx, metrics.Options{Disabled: true})
	f := &fakeFetcher{out: []Series{mkSeries("KXA", "A")}}
	w := &fakeWriter{inserted: 1}
	mf := &fakeMarketsFetcher{
		byTicker: map[string][]Market{
			"KXBTCD": {mkMarket("KXBTCD-A", "KXBTCD", "open"), mkMarket("KXBTCD-B", "KXBTCD", "settled")},
			"KXETHD": {mkMarket("KXETHD-A", "KXETHD", "open")},
		},
	}
	mw := &fakeMarketWriter{inserted: 99}

	captureTS := time.Date(2026, 5, 10, 12, 0, 0, 0, time.UTC)
	if err := RunOnce(ctx, f, w, mf, mw, []string{"KXBTCD", "KXETHD"}, m, captureTS); err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if len(mf.calls) != 2 {
		t.Fatalf("markets fetcher calls: %v (want 2)", mf.calls)
	}
	if len(mw.byCallRows) != 2 {
		t.Fatalf("market writer call count: %d (want 2)", len(mw.byCallRows))
	}
	if len(mw.byCallRows[0]) != 2 {
		t.Fatalf("first KXBTCD call row count: %d (want 2)", len(mw.byCallRows[0]))
	}
	for _, batch := range mw.byCallRows {
		for _, r := range batch {
			if !r.CaptureTS.Equal(captureTS) {
				t.Errorf("capture_ts=%v, want %v", r.CaptureTS, captureTS)
			}
			if r.ContentHash == "" {
				t.Errorf("empty content_hash on %s", r.MarketTicker)
			}
		}
	}
}

func TestRunOnce_MarketsFetchError_FailsFast(t *testing.T) {
	ctx := context.Background()
	m, _ := metrics.New(ctx, metrics.Options{Disabled: true})
	f := &fakeFetcher{out: []Series{mkSeries("KXA", "A")}}
	w := &fakeWriter{inserted: 1}
	mf := &fakeMarketsFetcher{
		byTicker: map[string][]Market{"KXBTCD": {mkMarket("KXBTCD-A", "KXBTCD", "open")}},
		errFor:   map[string]error{"KXETHD": errors.New("kalshi down")},
	}
	mw := &fakeMarketWriter{inserted: 1}

	err := RunOnce(ctx, f, w, mf, mw, []string{"KXBTCD", "KXETHD", "KXSOLD"}, m, time.Now())
	if err == nil {
		t.Fatal("expected error from KXETHD failure")
	}
	if !contains(mf.calls, "KXBTCD") || !contains(mf.calls, "KXETHD") {
		t.Fatalf("calls should include KXBTCD then KXETHD: %v", mf.calls)
	}
	if contains(mf.calls, "KXSOLD") {
		t.Fatalf("KXSOLD should NOT be called after KXETHD error (fail-fast): %v", mf.calls)
	}
}

func TestRunOnce_MarketsMergeError_FailsFast(t *testing.T) {
	ctx := context.Background()
	m, _ := metrics.New(ctx, metrics.Options{Disabled: true})
	f := &fakeFetcher{out: []Series{mkSeries("KXA", "A")}}
	w := &fakeWriter{inserted: 1}
	mf := &fakeMarketsFetcher{byTicker: map[string][]Market{"KXBTCD": {mkMarket("KXBTCD-A", "KXBTCD", "open")}}}
	mw := &fakeMarketWriter{err: errors.New("bq down")}

	if err := RunOnce(ctx, f, w, mf, mw, []string{"KXBTCD", "KXETHD"}, m, time.Now()); err == nil {
		t.Fatal("expected error from BQ merge failure")
	}
}

func TestRunOnce_SeriesPassFailureSkipsMarkets(t *testing.T) {
	ctx := context.Background()
	m, _ := metrics.New(ctx, metrics.Options{Disabled: true})
	f := &fakeFetcher{err: errors.New("series fetch down")}
	w := &fakeWriter{}
	mf := &fakeMarketsFetcher{}
	mw := &fakeMarketWriter{}

	if err := RunOnce(ctx, f, w, mf, mw, []string{"KXBTCD"}, m, time.Now()); err == nil {
		t.Fatal("expected error from series fetch failure")
	}
	if len(mf.calls) != 0 {
		t.Fatalf("markets fetcher should not be called when series pass fails: %v", mf.calls)
	}
}

func TestRunOnce_HashFailureSurfaces(t *testing.T) {
	ctx := context.Background()
	m, _ := metrics.New(ctx, metrics.Options{Disabled: true})
	bad := Series{Ticker: "KXBAD", RawJSON: json.RawMessage(`{not json`)}
	f := &fakeFetcher{out: []Series{bad}}
	w := &fakeWriter{}
	mf := &fakeMarketsFetcher{}
	mw := &fakeMarketWriter{}
	if err := RunOnce(ctx, f, w, mf, mw, nil, m, time.Now()); err == nil {
		t.Fatal("expected error from CanonicalHash on bad JSON")
	}
}

func contains(xs []string, want string) bool {
	for _, x := range xs {
		if x == want {
			return true
		}
	}
	return false
}
