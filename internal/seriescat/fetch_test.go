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

func TestKalshiSeriesFetcher_FetchAll(t *testing.T) {
	t.Run("single page returns all rows with raw bytes", func(t *testing.T) {
		body := `{"series":[
			{"ticker":"KXBTC","title":"Bitcoin","category":"Crypto","frequency":"DAILY","tags":["a"],"contract_url":"https://example.com/x"},
			{"ticker":"KXETH","title":"Eth","category":"Crypto","frequency":"DAILY","tags":["b"],"contract_url":"https://example.com/y"}
		]}`
		srv := newSeriesServer(t, []string{body})
		defer srv.Close()

		c := newTestClient(t, srv.URL)
		f := NewKalshiSeriesFetcher(c)
		got, err := f.FetchAll(context.Background())
		if err != nil {
			t.Fatalf("FetchAll: %v", err)
		}
		if len(got) != 2 {
			t.Fatalf("want 2 rows, got %d", len(got))
		}
		if got[0].Ticker != "KXBTC" || got[1].Ticker != "KXETH" {
			t.Fatalf("tickers: %s %s", got[0].Ticker, got[1].Ticker)
		}
		// raw bytes preserve the source object, not the entire response
		var probe map[string]any
		if err := json.Unmarshal(got[0].RawJSON, &probe); err != nil {
			t.Fatalf("raw[0] not valid JSON: %v", err)
		}
		if probe["ticker"] != "KXBTC" {
			t.Fatalf("raw[0] decoded ticker: %v", probe["ticker"])
		}
	})

	t.Run("cursor loop", func(t *testing.T) {
		page1 := `{"series":[{"ticker":"K1"}],"cursor":"c1"}`
		page2 := `{"series":[{"ticker":"K2"}],"cursor":"c2"}`
		page3 := `{"series":[{"ticker":"K3"}]}`
		srv := newSeriesServer(t, []string{page1, page2, page3})
		defer srv.Close()

		c := newTestClient(t, srv.URL)
		got, err := NewKalshiSeriesFetcher(c).FetchAll(context.Background())
		if err != nil {
			t.Fatalf("FetchAll: %v", err)
		}
		if len(got) != 3 {
			t.Fatalf("want 3 rows after cursor loop, got %d", len(got))
		}
	})

	t.Run("empty page short-circuits", func(t *testing.T) {
		srv := newSeriesServer(t, []string{`{"series":[]}`})
		defer srv.Close()

		c := newTestClient(t, srv.URL)
		got, err := NewKalshiSeriesFetcher(c).FetchAll(context.Background())
		if err != nil {
			t.Fatalf("FetchAll: %v", err)
		}
		if len(got) != 0 {
			t.Fatalf("want 0 rows, got %d", len(got))
		}
	})
}

// newSeriesServer returns an httptest server whose nth request returns bodies[n].
// Asserts the path is /trade-api/v2/series and that cursor= matches the previous
// page's cursor (empty on first call).
func newSeriesServer(t *testing.T, bodies []string) *httptest.Server {
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
		if r.URL.Path != "/trade-api/v2/series" {
			t.Errorf("path: %s", r.URL.Path)
		}
		if calls >= len(bodies) {
			t.Errorf("too many calls (%d > %d)", calls+1, len(bodies))
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

func newTestClient(t *testing.T, baseURL string) *kalshi.Client {
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
