package rest

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestAssetPairs_HappyPath_SinglePair(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got, want := r.URL.Query().Get("pair"), "BTC/USD"; got != want {
			t.Errorf("pair query = %q; want %q", got, want)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"error":[],"result":{"BTC/USD":{"altname":"XBTUSD","wsname":"XBT/USD","pair_decimals":1,"lot_decimals":8,"tick_size":"0.1","status":"online"}}}`))
	}))
	defer srv.Close()
	c := New(srv.Client(), srv.URL)
	got, err := c.AssetPairs(context.Background(), []string{"BTC/USD"})
	if err != nil {
		t.Fatalf("AssetPairs: %v", err)
	}
	info, ok := got["BTC/USD"]
	if !ok {
		t.Fatal("missing BTC/USD")
	}
	if info.PairDecimals != 1 || info.LotDecimals != 8 || info.Wsname != "XBT/USD" || info.Status != "online" {
		t.Errorf("BTC/USD info = %+v", info)
	}
}

func TestAssetPairs_HappyPath_CSV_FivePairs_CounterCost1(t *testing.T) {
	calls := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		pair := r.URL.Query().Get("pair")
		want := "BTC/USD,DOGE/USD,ETH/USD,SOL/USD,XRP/USD"
		if pair != want {
			t.Errorf("pair query = %q; want %q (sorted CSV)", pair, want)
		}
		w.Header().Set("Content-Type", "application/json")
		body := `{"error":[],"result":{`
		for i, p := range []string{"BTC/USD", "DOGE/USD", "ETH/USD", "SOL/USD", "XRP/USD"} {
			if i > 0 {
				body += ","
			}
			body += `"` + p + `":{"altname":"X","wsname":"X","pair_decimals":2,"lot_decimals":8,"tick_size":"0.01","status":"online"}`
		}
		body += `}}`
		_, _ = w.Write([]byte(body))
	}))
	defer srv.Close()
	c := New(srv.Client(), srv.URL)
	got, err := c.AssetPairs(context.Background(), []string{"BTC/USD", "DOGE/USD", "ETH/USD", "SOL/USD", "XRP/USD"})
	if err != nil {
		t.Fatalf("AssetPairs: %v", err)
	}
	if len(got) != 5 {
		t.Errorf("got %d entries; want 5", len(got))
	}
	if calls != 1 {
		t.Errorf("HTTP calls = %d; want 1 (counter cost 1)", calls)
	}
}

func TestAssetPairs_UnknownPair_FullError_NoResult(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"error":["EQuery:Unknown asset pair"]}`))
	}))
	defer srv.Close()
	c := New(srv.Client(), srv.URL)
	_, err := c.AssetPairs(context.Background(), []string{"DOESNOTEXIST"})
	if !errors.Is(err, ErrUnknownPair) {
		t.Fatalf("want ErrUnknownPair; got %v", err)
	}
	if !strings.Contains(err.Error(), "EQuery:Unknown asset pair") {
		t.Errorf("error should embed verbatim body string; got %v", err)
	}
}

func TestAssetPairs_MixedValidInvalid_AllOrNothing(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"error":["EQuery:Unknown asset pair"]}`))
	}))
	defer srv.Close()
	c := New(srv.Client(), srv.URL)
	_, err := c.AssetPairs(context.Background(), []string{"BTC/USD", "FOOBAR"})
	if !errors.Is(err, ErrUnknownPair) {
		t.Fatalf("want ErrUnknownPair; got %v", err)
	}
}

func TestAssetPairs_Non200_WrappedErr(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "boom", http.StatusInternalServerError)
	}))
	defer srv.Close()
	c := New(srv.Client(), srv.URL)
	_, err := c.AssetPairs(context.Background(), []string{"BTC/USD"})
	if err == nil {
		t.Fatal("want error for 500 response")
	}
	if !strings.Contains(err.Error(), "500") {
		t.Errorf("error should mention status code; got %v", err)
	}
}

// assetPairsHandler builds an httptest handler whose response is determined
// by the input CSV. badPairs lists pairs that, if any appear in the CSV,
// cause Kraken to reject the whole request. goodPairs is the universe of
// known pairs (any other pair in the CSV is treated as bad).
func assetPairsHandler(t *testing.T, knownPairs map[string]struct{}, callCount *int) http.Handler {
	t.Helper()
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if callCount != nil {
			*callCount++
		}
		raw := r.URL.Query().Get("pair")
		input := strings.Split(raw, ",")
		bad := false
		for _, p := range input {
			if _, ok := knownPairs[p]; !ok {
				bad = true
				break
			}
		}
		w.Header().Set("Content-Type", "application/json")
		if bad {
			_, _ = w.Write([]byte(`{"error":["EQuery:Unknown asset pair"]}`))
			return
		}
		body := `{"error":[],"result":{`
		for i, p := range input {
			if i > 0 {
				body += ","
			}
			body += `"` + p + `":{"altname":"X","wsname":"X","pair_decimals":2,"lot_decimals":8,"tick_size":"0.01","status":"online"}`
		}
		body += `}}`
		_, _ = w.Write([]byte(body))
	})
}

func TestAssetPairsWithFallback_HappyPath(t *testing.T) {
	calls := 0
	known := map[string]struct{}{"BTC/USD": {}, "ETH/USD": {}, "SOL/USD": {}}
	srv := httptest.NewServer(assetPairsHandler(t, known, &calls))
	defer srv.Close()
	c := New(srv.Client(), srv.URL)
	results, rej, err := c.AssetPairsWithFallback(context.Background(), []string{"BTC/USD", "ETH/USD", "SOL/USD"})
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if len(results) != 3 {
		t.Errorf("results len = %d; want 3", len(results))
	}
	if len(rej) != 0 {
		t.Errorf("rejections = %v; want none", rej)
	}
	if calls != 1 {
		t.Errorf("HTTP calls = %d; want 1 on happy path", calls)
	}
}

func TestAssetPairsWithFallback_SinglePair_Bad(t *testing.T) {
	known := map[string]struct{}{"BTC/USD": {}}
	srv := httptest.NewServer(assetPairsHandler(t, known, nil))
	defer srv.Close()
	c := New(srv.Client(), srv.URL)
	results, rej, err := c.AssetPairsWithFallback(context.Background(), []string{"FOOBAR"})
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if len(results) != 0 {
		t.Errorf("results = %v; want empty", results)
	}
	if len(rej) != 1 || rej[0].Pair != "FOOBAR" {
		t.Errorf("rejections = %v; want [{FOOBAR ...}]", rej)
	}
	if rej[0].ErrMessage != "EQuery:Unknown asset pair" {
		t.Errorf("ErrMessage = %q; want verbatim Kraken string", rej[0].ErrMessage)
	}
}

func TestAssetPairsWithFallback_MixedBisect(t *testing.T) {
	known := map[string]struct{}{"BTC/USD": {}, "ETH/USD": {}, "SOL/USD": {}, "XRP/USD": {}}
	srv := httptest.NewServer(assetPairsHandler(t, known, nil))
	defer srv.Close()
	c := New(srv.Client(), srv.URL)
	// FOOBAR + WIDGET are bad; BTC/USD, ETH/USD, SOL/USD, XRP/USD are good.
	got, rej, err := c.AssetPairsWithFallback(context.Background(), []string{
		"BTC/USD", "ETH/USD", "FOOBAR", "SOL/USD", "WIDGET", "XRP/USD",
	})
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if len(got) != 4 {
		t.Errorf("results len = %d; want 4; got %v", len(got), got)
	}
	for _, p := range []string{"BTC/USD", "ETH/USD", "SOL/USD", "XRP/USD"} {
		if _, ok := got[p]; !ok {
			t.Errorf("missing %s in results", p)
		}
	}
	if len(rej) != 2 {
		t.Fatalf("rejections len = %d; want 2; got %v", len(rej), rej)
	}
	rejPairs := []string{rej[0].Pair, rej[1].Pair}
	if rejPairs[0] != "FOOBAR" || rejPairs[1] != "WIDGET" {
		t.Errorf("rejections (sorted) = %v; want [FOOBAR WIDGET]", rejPairs)
	}
	for _, r := range rej {
		if r.ErrMessage != "EQuery:Unknown asset pair" {
			t.Errorf("rejection %s: ErrMessage = %q; want verbatim", r.Pair, r.ErrMessage)
		}
	}
}

func TestAssetPairsWithFallback_NetworkError_PropagatesVerbatim(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "boom", http.StatusServiceUnavailable)
	}))
	defer srv.Close()
	c := New(srv.Client(), srv.URL)
	_, _, err := c.AssetPairsWithFallback(context.Background(), []string{"BTC/USD", "ETH/USD"})
	if err == nil {
		t.Fatal("want network error")
	}
	if !strings.Contains(err.Error(), "503") {
		t.Errorf("want wrapped 503; got %v", err)
	}
}

func TestAssetPairsWithFallback_EmptyInput(t *testing.T) {
	c := New(nil, "")
	_, _, err := c.AssetPairsWithFallback(context.Background(), nil)
	if err == nil {
		t.Fatal("want error on empty input")
	}
}

func TestAssetPairsWithFallback_DedupInput(t *testing.T) {
	calls := 0
	known := map[string]struct{}{"BTC/USD": {}}
	srv := httptest.NewServer(assetPairsHandler(t, known, &calls))
	defer srv.Close()
	c := New(srv.Client(), srv.URL)
	got, rej, err := c.AssetPairsWithFallback(context.Background(), []string{"BTC/USD", "BTC/USD", "BTC/USD"})
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if len(got) != 1 || len(rej) != 0 {
		t.Errorf("got=%v rej=%v; want 1 result no rejections", got, rej)
	}
	if calls != 1 {
		t.Errorf("HTTP calls = %d; want 1 (dedup avoids the multi-call path)", calls)
	}
}

func TestUnknownPairError_IsErrUnknownPair(t *testing.T) {
	var upe *UnknownPairError
	err := &UnknownPairError{KrakenMessage: "EQuery:Unknown asset pair"}
	if !errors.Is(err, ErrUnknownPair) {
		t.Error("errors.Is(*UnknownPairError, ErrUnknownPair) must be true")
	}
	if !errors.As(err, &upe) {
		t.Error("errors.As(*UnknownPairError) must populate")
	}
	if upe == nil || upe.KrakenMessage != "EQuery:Unknown asset pair" {
		t.Errorf("captured = %+v", upe)
	}
}

func TestAssetPairs_TickSizeAsJSONNumber_NotFloat64(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"error":[],"result":{"DOGE/USD":{"altname":"XDGUSD","wsname":"XDG/USD","pair_decimals":7,"lot_decimals":8,"tick_size":"0.0000001","status":"online"}}}`))
	}))
	defer srv.Close()
	c := New(srv.Client(), srv.URL)
	got, err := c.AssetPairs(context.Background(), []string{"DOGE/USD"})
	if err != nil {
		t.Fatalf("AssetPairs: %v", err)
	}
	info := got["DOGE/USD"]
	if got, want := info.TickSize, json.Number("0.0000001"); got != want {
		t.Errorf("TickSize = %q; want %q", got, want)
	}
	if info.PairDecimals != 7 {
		t.Errorf("DOGE PairDecimals = %d; want 7", info.PairDecimals)
	}
}
