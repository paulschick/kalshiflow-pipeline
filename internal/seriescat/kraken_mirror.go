package seriescat

import (
	"sort"

	"github.com/paulschick/kalshiflow-pipeline/internal/kraken/symbol"
)

// PairsFromTickers maps a Kalshi-subscribed ticker set to the corresponding
// Kraken WS pair set. Each ticker contributes zero or more assets via
// symbol.ExtractAssets; each asset maps to its Kraken pair via
// symbol.AssetToKrakenWSPair. Output is sorted, deduplicated.
//
// Returns an empty (non-nil) slice when no tickers contribute.
func PairsFromTickers(tickers []string) []string {
	seen := map[string]struct{}{}
	for _, t := range tickers {
		for _, asset := range symbol.ExtractAssets(t) {
			if pair, ok := symbol.AssetToKrakenWSPair[asset]; ok {
				seen[pair] = struct{}{}
			}
		}
	}
	out := make([]string, 0, len(seen))
	for p := range seen {
		out = append(out, p)
	}
	sort.Strings(out)
	return out
}

// UnionPairs returns the deduplicated, lexicographically-sorted union of two
// pair sets. Both inputs may be nil or empty; both-empty yields an empty
// (non-nil) slice.
func UnionPairs(catalogPairs, viewPairs []string) []string {
	seen := make(map[string]struct{}, len(catalogPairs)+len(viewPairs))
	for _, p := range catalogPairs {
		seen[p] = struct{}{}
	}
	for _, p := range viewPairs {
		seen[p] = struct{}{}
	}
	out := make([]string, 0, len(seen))
	for p := range seen {
		out = append(out, p)
	}
	sort.Strings(out)
	return out
}
