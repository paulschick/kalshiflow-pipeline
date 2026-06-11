package symbol

import (
	"reflect"
	"sort"
	"testing"
)

func TestAssetToKrakenWSPair_HardcodedFiveModernForms(t *testing.T) {
	want := map[string]string{
		"BTC":  "BTC/USD",
		"ETH":  "ETH/USD",
		"SOL":  "SOL/USD",
		"XRP":  "XRP/USD",
		"DOGE": "DOGE/USD",
	}
	if !reflect.DeepEqual(AssetToKrakenWSPair, want) {
		t.Fatalf("AssetToKrakenWSPair = %#v; want %#v", AssetToKrakenWSPair, want)
	}
}

func TestKrakenPairToAsset_IsExactInverse(t *testing.T) {
	if len(KrakenPairToAsset) != len(AssetToKrakenWSPair) {
		t.Fatalf("inverse size mismatch: %d vs %d", len(KrakenPairToAsset), len(AssetToKrakenWSPair))
	}
	for asset, pair := range AssetToKrakenWSPair {
		if KrakenPairToAsset[pair] != asset {
			t.Errorf("KrakenPairToAsset[%q] = %q; want %q", pair, KrakenPairToAsset[pair], asset)
		}
	}
}

func TestHardcodedPairs_DeterministicAndComplete(t *testing.T) {
	got := HardcodedPairs()
	want := []string{"BTC/USD", "DOGE/USD", "ETH/USD", "SOL/USD", "XRP/USD"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("HardcodedPairs() = %v; want %v (must be sorted for determinism)", got, want)
	}
	got[0] = "MUTATED"
	if HardcodedPairs()[0] == "MUTATED" {
		t.Fatal("HardcodedPairs() must return a fresh slice each call")
	}
	sort.Strings(got)
}

func TestNoLegacyForms(t *testing.T) {
	for _, p := range HardcodedPairs() {
		if p == "XBT/USD" || p == "XDG/USD" {
			t.Errorf("legacy form %q must never appear in the hardcoded set", p)
		}
	}
}

func TestExtractAssets(t *testing.T) {
	cases := []struct {
		name   string
		ticker string
		want   []string
	}{
		{name: "KXBTCD canonical above-below", ticker: "KXBTCD", want: []string{"BTC"}},
		{name: "KXBTC range form", ticker: "KXBTC", want: []string{"BTC"}},
		{name: "KXBTC15M fifteen-minute", ticker: "KXBTC15M", want: []string{"BTC"}},
		{name: "KXBTCMAXY annual max", ticker: "KXBTCMAXY", want: []string{"BTC"}},
		{name: "KXDOGE range form", ticker: "KXDOGE", want: []string{"DOGE"}},
		{name: "KXDOGED canonical above-below", ticker: "KXDOGED", want: []string{"DOGE"}},
		{name: "KXETHMINY annual min", ticker: "KXETHMINY", want: []string{"ETH"}},
		{name: "KXETHD canonical", ticker: "KXETHD", want: []string{"ETH"}},
		{name: "KXSOLD canonical", ticker: "KXSOLD", want: []string{"SOL"}},
		{name: "KXXRPD canonical", ticker: "KXXRPD", want: []string{"XRP"}},
		{name: "empty", ticker: "", want: nil},
		{name: "unrelated KX series", ticker: "KXFOO", want: nil},
		{name: "no KX prefix", ticker: "BTC", want: nil},
		{name: "prefix-of-prefix KXBT", ticker: "KXBT", want: nil},
		{name: "non-Kalshi prefix", ticker: "BTCDOGE", want: nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := ExtractAssets(tc.ticker)
			if !reflect.DeepEqual(got, tc.want) {
				t.Errorf("ExtractAssets(%q) = %v; want %v", tc.ticker, got, tc.want)
			}
		})
	}
}

func TestExtractAssets_DeterministicLexicographicOrder(t *testing.T) {
	// If a hypothetical future ticker contributed multiple assets, the
	// output must be in lexicographic order. None of BTC/DOGE/ETH/SOL/XRP
	// is a prefix of another at this asset set, so we can't construct a
	// real multi-asset ticker, but we can lock the contract on the loop
	// iteration order.
	got := supportedAssets
	want := []string{"BTC", "DOGE", "ETH", "SOL", "XRP"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("supportedAssets = %v; want %v (lexicographic)", got, want)
	}
}
