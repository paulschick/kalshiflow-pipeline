// Package orderbook provides Kalshi orderbook parsing and state management.
package orderbook

import (
	"fmt"
	"math"
	"strconv"
	"strings"
)

// ParsePriceUnits parses a Kalshi price_dollars string ("0.7700") into an int64
// in 1/10000ths of a dollar. Strict format: leading "0." or "1." then exactly
// four decimal digits. Negative values are rejected.
//
// String parsing (not float64) is deliberate: at 1/10000ths granularity the
// boundary prices ("0.0100", "0.9900") survive a float round-trip in practice,
// but precision drift across thousands of quotes makes equality comparison on
// the resulting int64 untrustworthy. String split has no rounding error.
func ParsePriceUnits(s string) (int64, error) {
	if s == "" {
		return 0, fmt.Errorf("empty price")
	}
	if s[0] == '-' {
		return 0, fmt.Errorf("negative price: %q", s)
	}
	dot := strings.IndexByte(s, '.')
	if dot < 0 {
		return 0, fmt.Errorf("price missing '.': %q", s)
	}
	whole, frac := s[:dot], s[dot+1:]
	if len(frac) != 4 {
		return 0, fmt.Errorf("price needs 4 decimals: %q", s)
	}
	w, err := strconv.ParseInt(whole, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("price whole part: %w", err)
	}
	f, err := strconv.ParseInt(frac, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("price frac part: %w", err)
	}
	return w*10000 + f, nil
}

// ParseDeltaFP parses a Kalshi delta_fp string ("-350.00") into a signed int64
// share-count delta. Fractional inputs round to the nearest integer.
func ParseDeltaFP(s string) (int64, error) {
	if s == "" {
		return 0, fmt.Errorf("empty delta_fp")
	}
	f, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return 0, fmt.Errorf("delta_fp parse: %w", err)
	}
	return int64(math.Round(f)), nil
}
