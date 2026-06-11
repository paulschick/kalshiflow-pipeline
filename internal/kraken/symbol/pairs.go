// Package symbol carries the hardcoded asset ↔ Kraken WS pair mapping.
// HARDCODED by design — do NOT derive from REST AssetPairs.wsname.
// AssetPairs.wsname returns the legacy form (XBT/USD, XDG/USD) for BTC and DOGE;
// WS v2 rejects those with `Currency pair not supported XBT/USD`.
package symbol

import "strings"

// AssetToKrakenWSPair maps asset family → Kraken WS v2 modern slash form.
var AssetToKrakenWSPair = map[string]string{
	"BTC":  "BTC/USD",
	"ETH":  "ETH/USD",
	"SOL":  "SOL/USD",
	"XRP":  "XRP/USD",
	"DOGE": "DOGE/USD",
}

// KrakenPairToAsset is the 5-entry inverse.
var KrakenPairToAsset = map[string]string{
	"BTC/USD":  "BTC",
	"ETH/USD":  "ETH",
	"SOL/USD":  "SOL",
	"XRP/USD":  "XRP",
	"DOGE/USD": "DOGE",
}

// HardcodedPairs returns the deterministic-ordered slice of WS pairs for HOL-48.
// Sorted lexicographically so logs and metrics labels are stable across runs.
// Returns a fresh slice each call so callers may sort or mutate.
func HardcodedPairs() []string {
	return []string{"BTC/USD", "DOGE/USD", "ETH/USD", "SOL/USD", "XRP/USD"}
}

// supportedAssets is the deterministic asset-code set Kraken collection
// supports. Lexicographic order. Keep in sync with AssetToKrakenWSPair.
var supportedAssets = []string{"BTC", "DOGE", "ETH", "SOL", "XRP"}

// ExtractAssets returns asset codes whose KX<asset> prefix appears in ticker,
// restricted to Kraken-supported assets. Output is in deterministic
// lexicographic order. A single ticker can contribute multiple assets when
// prefixes overlap (none of BTC/DOGE/ETH/SOL/XRP is a prefix of another
// today, so collisions are impossible — but the contract is preserved for
// future asset additions).
//
// Examples:
//
//	KXBTCD       → [BTC]
//	KXBTC        → [BTC]
//	KXBTC15M     → [BTC]
//	KXBTCMAXY    → [BTC]
//	KXDOGE       → [DOGE]
//	KXDOGED      → [DOGE]
//	KXETHMINY    → [ETH]
//	""           → nil
//	KXFOO        → nil
//	BTC          → nil  (missing KX prefix)
func ExtractAssets(ticker string) []string {
	var out []string
	for _, asset := range supportedAssets {
		if strings.HasPrefix(ticker, "KX"+asset) {
			out = append(out, asset)
		}
	}
	return out
}
