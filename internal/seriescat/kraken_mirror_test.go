package seriescat

import (
	"reflect"
	"testing"
)

func TestPairsFromTickers(t *testing.T) {
	cases := []struct {
		name    string
		tickers []string
		want    []string
	}{
		{
			name:    "current subscribed set 2026-05-15 round-trips to all 5 pairs minus BTC/USD if KXBTC absent",
			tickers: []string{"KXBTCD", "KXDOGE", "KXDOGED", "KXETHD", "KXSOLD", "KXXRPD"},
			want:    []string{"BTC/USD", "DOGE/USD", "ETH/USD", "SOL/USD", "XRP/USD"},
		},
		{
			name:    "single BTC variant",
			tickers: []string{"KXBTCD"},
			want:    []string{"BTC/USD"},
		},
		{
			name:    "non-canonical BTC shapes all map to BTC/USD",
			tickers: []string{"KXBTC15M", "KXBTCMAXY", "KXBTC"},
			want:    []string{"BTC/USD"},
		},
		{
			name:    "DOGE both shapes",
			tickers: []string{"KXDOGE", "KXDOGED"},
			want:    []string{"DOGE/USD"},
		},
		{
			name:    "unrelated tickers contribute nothing",
			tickers: []string{"KXFOO", "BTC", "KXBT"},
			want:    []string{},
		},
		{
			name:    "empty",
			tickers: nil,
			want:    []string{},
		},
		{
			name:    "duplicates in input dedup",
			tickers: []string{"KXBTCD", "KXBTCD", "KXBTC"},
			want:    []string{"BTC/USD"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := PairsFromTickers(tc.tickers)
			if !reflect.DeepEqual(got, tc.want) {
				t.Errorf("PairsFromTickers(%v) = %v; want %v", tc.tickers, got, tc.want)
			}
		})
	}
}

func TestUnionPairs(t *testing.T) {
	cases := []struct {
		name          string
		catalog, view []string
		want          []string
	}{
		{name: "both empty", catalog: nil, view: nil, want: []string{}},
		{name: "only catalog", catalog: []string{"BTC/USD", "ETH/USD"}, view: nil, want: []string{"BTC/USD", "ETH/USD"}},
		{name: "only view", catalog: nil, view: []string{"LTC/USD"}, want: []string{"LTC/USD"}},
		{name: "disjoint",
			catalog: []string{"BTC/USD", "ETH/USD"},
			view:    []string{"LTC/USD"},
			want:    []string{"BTC/USD", "ETH/USD", "LTC/USD"},
		},
		{name: "overlap deduplicates",
			catalog: []string{"BTC/USD", "ETH/USD"},
			view:    []string{"ETH/USD", "LTC/USD"},
			want:    []string{"BTC/USD", "ETH/USD", "LTC/USD"},
		},
		{name: "intra-input duplicates",
			catalog: []string{"BTC/USD", "BTC/USD"},
			view:    []string{"ETH/USD", "ETH/USD"},
			want:    []string{"BTC/USD", "ETH/USD"},
		},
		{name: "unsorted input → sorted output",
			catalog: []string{"XRP/USD", "BTC/USD"},
			view:    []string{"SOL/USD", "DOGE/USD"},
			want:    []string{"BTC/USD", "DOGE/USD", "SOL/USD", "XRP/USD"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := UnionPairs(tc.catalog, tc.view)
			if !reflect.DeepEqual(got, tc.want) {
				t.Errorf("UnionPairs(%v, %v) = %v; want %v", tc.catalog, tc.view, got, tc.want)
			}
		})
	}
}
