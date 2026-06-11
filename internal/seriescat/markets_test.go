package seriescat

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/paulschick/kalshiflow-pipeline/internal/kalshi"
)

func TestKalshiMarketsFetcher_FetchAllForSeries(t *testing.T) {
	t.Run("decodes mixed-status rows", func(t *testing.T) {
		// Row 1 uses the legacy `settlement_value` key; row 2 uses the new
		// `settlement_value_dollars` key. Fetcher should accept either.
		body := `{"markets":[
			{"ticker":"KXBTCD-25MAR05-T100","title":"BTC > 100","status":"open","open_time":"2026-03-05T00:00:00Z","close_time":"2026-03-05T23:00:00Z","expiration_time":"2026-03-06T00:00:00Z"},
			{"ticker":"KXBTCD-25MAR04-T100","title":"BTC > 100","status":"settled","open_time":"2026-03-04T00:00:00Z","close_time":"2026-03-04T23:00:00Z","expiration_time":"2026-03-05T00:00:00Z","settlement_value_dollars":"0.5000"}
		],"cursor":""}`
		srv := newMarketsServer(t, []string{body})
		defer srv.Close()

		c := newMarketsTestClient(t, srv.URL)
		f := NewKalshiMarketsFetcher(c)
		got, err := f.FetchAllForSeries(context.Background(), "KXBTCD", time.Time{})
		if err != nil {
			t.Fatalf("FetchAllForSeries: %v", err)
		}
		if len(got) != 2 {
			t.Fatalf("want 2 rows, got %d", len(got))
		}
		if got[0].Ticker != "KXBTCD-25MAR05-T100" || got[0].Status != "open" {
			t.Fatalf("row 0: %+v", got[0])
		}
		// SeriesTicker must come from the argument, not the JSON (Markets
		// don't carry series_ticker in their response payload).
		if got[0].SeriesTicker != "KXBTCD" || got[1].SeriesTicker != "KXBTCD" {
			t.Fatalf("series_ticker not stamped from arg: row0=%q row1=%q", got[0].SeriesTicker, got[1].SeriesTicker)
		}
		if got[1].Status != "settled" {
			t.Fatalf("row 1 status: %q", got[1].Status)
		}
		if got[1].SettlementValue == nil || *got[1].SettlementValue != "0.5000" {
			t.Fatalf("row 1 settlement_value: %v", got[1].SettlementValue)
		}
		if got[0].SettlementValue != nil {
			t.Fatalf("row 0 settlement_value should be nil, got %v", *got[0].SettlementValue)
		}
	})

	t.Run("legacy settlement_value key still decodes", func(t *testing.T) {
		body := `{"markets":[{"ticker":"M0","status":"settled","settlement_value":"0.2500"}],"cursor":""}`
		srv := newMarketsServer(t, []string{body})
		defer srv.Close()
		c := newMarketsTestClient(t, srv.URL)
		got, err := NewKalshiMarketsFetcher(c).FetchAllForSeries(context.Background(), "KXBTCD", time.Time{})
		if err != nil {
			t.Fatalf("FetchAllForSeries: %v", err)
		}
		if len(got) != 1 || got[0].SettlementValue == nil || *got[0].SettlementValue != "0.2500" {
			t.Fatalf("legacy key not decoded: %+v", got)
		}
	})

	t.Run("cursor loop walks pages", func(t *testing.T) {
		page1 := `{"markets":[{"ticker":"M1","status":"open"}],"cursor":"c1"}`
		page2 := `{"markets":[{"ticker":"M2","status":"settled"}],"cursor":"c2"}`
		page3 := `{"markets":[{"ticker":"M3","status":"closed"}],"cursor":""}`
		srv := newMarketsServer(t, []string{page1, page2, page3})
		defer srv.Close()

		c := newMarketsTestClient(t, srv.URL)
		got, err := NewKalshiMarketsFetcher(c).FetchAllForSeries(context.Background(), "KXBTCD", time.Time{})
		if err != nil {
			t.Fatalf("FetchAllForSeries: %v", err)
		}
		if len(got) != 3 {
			t.Fatalf("want 3 rows after cursor loop, got %d", len(got))
		}
	})

	t.Run("empty markets array returns empty slice without error", func(t *testing.T) {
		srv := newMarketsServer(t, []string{`{"markets":[],"cursor":""}`})
		defer srv.Close()
		c := newMarketsTestClient(t, srv.URL)
		got, err := NewKalshiMarketsFetcher(c).FetchAllForSeries(context.Background(), "KXBOGUS", time.Time{})
		if err != nil {
			t.Fatalf("FetchAllForSeries: %v", err)
		}
		if len(got) != 0 {
			t.Fatalf("want 0 rows, got %d", len(got))
		}
	})

	t.Run("forwards min_close_ts to underlying client", func(t *testing.T) {
		minTS := time.Date(2026, 5, 9, 0, 0, 0, 0, time.UTC)
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Query().Get("min_close_ts") == "" {
				t.Error("expected min_close_ts query param, got none")
			}
			_, _ = w.Write([]byte(`{"markets":[],"cursor":""}`))
		}))
		defer srv.Close()
		c := newMarketsTestClient(t, srv.URL)
		if _, err := NewKalshiMarketsFetcher(c).FetchAllForSeries(context.Background(), "KXBTCD", minTS); err != nil {
			t.Fatalf("FetchAllForSeries: %v", err)
		}
	})
}

func newMarketsServer(t *testing.T, bodies []string) *httptest.Server {
	t.Helper()
	var calls int
	wantCursors := []string{""}
	for _, b := range bodies {
		var page struct {
			Cursor string `json:"cursor"`
		}
		_ = json.Unmarshal([]byte(b), &page)
		wantCursors = append(wantCursors, page.Cursor)
	}
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/trade-api/v2/markets" {
			t.Errorf("path: %s", r.URL.Path)
		}
		if r.URL.Query().Has("status") {
			t.Errorf("status filter must NOT be set on /markets calls in markets-fetcher; got %q", r.URL.Query().Get("status"))
		}
		if calls >= len(bodies) {
			http.Error(w, "extra call", http.StatusInternalServerError)
			return
		}
		got := r.URL.Query().Get("cursor")
		if got != wantCursors[calls] {
			t.Errorf("call %d cursor: got %q, want %q", calls, got, wantCursors[calls])
		}
		_, _ = fmt.Fprint(w, bodies[calls])
		calls++
	}))
}

func newMarketsTestClient(t *testing.T, baseURL string) *kalshi.Client {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("rsa: %v", err)
	}
	return kalshi.NewClient(kalshi.Config{
		BaseURL:    baseURL,
		Signer:     &kalshi.Signer{KeyID: "kid", PrivateKey: key},
		Now:        func() time.Time { return time.Unix(0, 0) },
		MaxRetries: 1,
	})
}
