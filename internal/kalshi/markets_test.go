package kalshi

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sort"
	"strconv"
	"testing"
	"time"
)

func TestGetOpenMarkets_PaginatesUntilCursorEmpty(t *testing.T) {
	priv, _ := rsa.GenerateKey(rand.Reader, 2048)
	page := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		page++
		switch page {
		case 1:
			if r.URL.Query().Get("cursor") != "" {
				t.Errorf("page 1 cursor = %q; want empty", r.URL.Query().Get("cursor"))
			}
			if r.URL.Query().Get("series_ticker") != "KXBTCD" {
				t.Errorf("series_ticker = %q", r.URL.Query().Get("series_ticker"))
			}
			if r.URL.Query().Get("status") != "open" {
				t.Errorf("status = %q", r.URL.Query().Get("status"))
			}
			_, _ = w.Write([]byte(`{"markets":[{"ticker":"KXBTCD-A"},{"ticker":"KXBTCD-B"}],"cursor":"next"}`))
		case 2:
			if r.URL.Query().Get("cursor") != "next" {
				t.Errorf("page 2 cursor = %q; want next", r.URL.Query().Get("cursor"))
			}
			_, _ = w.Write([]byte(`{"markets":[{"ticker":"KXBTCD-C"}],"cursor":""}`))
		default:
			t.Fatalf("too many pages: %d", page)
		}
	}))
	defer srv.Close()

	c := NewClient(Config{BaseURL: srv.URL, Signer: &Signer{KeyID: "k", PrivateKey: priv}, Now: func() time.Time { return time.Unix(0, 0) }})
	got, hitCap, err := c.GetOpenMarkets(context.Background(), "KXBTCD", "open", 5)
	if err != nil {
		t.Fatalf("GetOpenMarkets: %v", err)
	}
	if hitCap {
		t.Errorf("hitCap = true; want false (cursor went empty before cap)")
	}
	sort.Strings(got)
	if !reflect.DeepEqual(got, []string{"KXBTCD-A", "KXBTCD-B", "KXBTCD-C"}) {
		t.Errorf("tickers = %v", got)
	}
	if page != 2 {
		t.Errorf("expected 2 pages, got %d", page)
	}
}

func TestGetOpenMarkets_RespectsMaxPagesAndReportsHitCap(t *testing.T) {
	priv, _ := rsa.GenerateKey(rand.Reader, 2048)
	page := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		page++
		_, _ = w.Write([]byte(`{"markets":[{"ticker":"X"}],"cursor":"more"}`))
	}))
	defer srv.Close()

	c := NewClient(Config{BaseURL: srv.URL, Signer: &Signer{KeyID: "k", PrivateKey: priv}, Now: func() time.Time { return time.Unix(0, 0) }})
	tickers, hitCap, err := c.GetOpenMarkets(context.Background(), "KXBTCD", "open", 3)
	if err != nil {
		t.Fatalf("GetOpenMarkets: %v", err)
	}
	if !hitCap {
		t.Errorf("hitCap = false; want true (server kept returning non-empty cursor at cap)")
	}
	if len(tickers) != 3 {
		t.Errorf("tickers count = %d; want 3 (one per page)", len(tickers))
	}
	if page != 3 {
		t.Errorf("expected exactly 3 pages, got %d", page)
	}
}

func TestGetOpenMarkets_PassesOpenStatusValue(t *testing.T) {
	priv, _ := rsa.GenerateKey(rand.Reader, 2048)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.URL.Query().Get("status"); got != "tradable" {
			t.Errorf("status = %q; want tradable (probe-derived value passed through)", got)
		}
		_, _ = w.Write([]byte(`{"markets":[],"cursor":""}`))
	}))
	defer srv.Close()

	c := NewClient(Config{BaseURL: srv.URL, Signer: &Signer{KeyID: "k", PrivateKey: priv}, Now: func() time.Time { return time.Unix(0, 0) }})
	if _, _, err := c.GetOpenMarkets(context.Background(), "KXBTCD", "tradable", 5); err != nil {
		t.Fatalf("GetOpenMarkets: %v", err)
	}
}

func TestGetMarketsPage_OmitsStatusFilter(t *testing.T) {
	priv, _ := rsa.GenerateKey(rand.Reader, 2048)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/trade-api/v2/markets" {
			t.Errorf("path = %q", r.URL.Path)
		}
		if got := r.URL.Query().Get("series_ticker"); got != "KXBTCD" {
			t.Errorf("series_ticker = %q", got)
		}
		if r.URL.Query().Has("status") {
			t.Errorf("status param present: %q (should be absent so settled/expired markets return)", r.URL.Query().Get("status"))
		}
		if got := r.URL.Query().Get("limit"); got != "1000" {
			t.Errorf("limit = %q; want 1000", got)
		}
		if r.URL.Query().Has("min_close_ts") {
			t.Errorf("min_close_ts unexpectedly set when zero time passed: %q", r.URL.Query().Get("min_close_ts"))
		}
		_, _ = w.Write([]byte(`{"markets":[{"ticker":"KXBTCD-A","status":"open"},{"ticker":"KXBTCD-B","status":"settled"}],"cursor":""}`))
	}))
	defer srv.Close()

	c := NewClient(Config{BaseURL: srv.URL, Signer: &Signer{KeyID: "k", PrivateKey: priv}, Now: func() time.Time { return time.Unix(0, 0) }})
	body, err := c.GetMarketsPage(context.Background(), "KXBTCD", "", time.Time{})
	if err != nil {
		t.Fatalf("GetMarketsPage: %v", err)
	}
	if !bytes.Contains(body, []byte(`"settled"`)) {
		t.Errorf("expected raw body to include settled market; got %s", body)
	}
}

func TestGetMarketsPage_PassesCursor(t *testing.T) {
	priv, _ := rsa.GenerateKey(rand.Reader, 2048)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.URL.Query().Get("cursor"); got != "abc" {
			t.Errorf("cursor = %q; want abc", got)
		}
		_, _ = w.Write([]byte(`{"markets":[],"cursor":""}`))
	}))
	defer srv.Close()

	c := NewClient(Config{BaseURL: srv.URL, Signer: &Signer{KeyID: "k", PrivateKey: priv}, Now: func() time.Time { return time.Unix(0, 0) }})
	if _, err := c.GetMarketsPage(context.Background(), "KXBTCD", "abc", time.Time{}); err != nil {
		t.Fatalf("GetMarketsPage: %v", err)
	}
}

func TestGetMarketsPage_ErrorOnNon2xx(t *testing.T) {
	priv, _ := rsa.GenerateKey(rand.Reader, 2048)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "boom", http.StatusInternalServerError)
	}))
	defer srv.Close()

	c := NewClient(Config{BaseURL: srv.URL, Signer: &Signer{KeyID: "k", PrivateKey: priv}, Now: func() time.Time { return time.Unix(0, 0) }, MaxRetries: 1})
	if _, err := c.GetMarketsPage(context.Background(), "KXBTCD", "", time.Time{}); err == nil {
		t.Fatal("expected error on 500 status")
	}
}

func TestGetMarketsPage_PassesMinCloseTS(t *testing.T) {
	priv, _ := rsa.GenerateKey(rand.Reader, 2048)
	minTS := time.Date(2026, 5, 9, 0, 0, 0, 0, time.UTC)
	wantUnix := strconv.FormatInt(minTS.Unix(), 10)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.URL.Query().Get("min_close_ts"); got != wantUnix {
			t.Errorf("min_close_ts = %q; want %q", got, wantUnix)
		}
		_, _ = w.Write([]byte(`{"markets":[],"cursor":""}`))
	}))
	defer srv.Close()

	c := NewClient(Config{BaseURL: srv.URL, Signer: &Signer{KeyID: "k", PrivateKey: priv}, Now: func() time.Time { return time.Unix(0, 0) }})
	if _, err := c.GetMarketsPage(context.Background(), "KXBTCD", "", minTS); err != nil {
		t.Fatalf("GetMarketsPage: %v", err)
	}
}
