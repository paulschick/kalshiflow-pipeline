// Package orderbook provides the Kraken-side Bookkeeper, scaled fixed-point
// arithmetic, and the CRC32 checksum validator. The bookkeeper reuses the
// venue-agnostic internal/orderbook.{Book,Registry,SnapshotPayload} primitives
// and adds Kraken-specific absolute-set update semantics and bid/ask side
// labels.
package orderbook

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

// ErrPrecisionOverflow is returned when a wire literal has more fractional
// digits than the venue claims via pair_decimals / lot_decimals. Indicates a
// stale scale map (or a Kraken-side schema change) — fail-fast.
var ErrPrecisionOverflow = errors.New("kraken/orderbook: wire literal exceeds declared precision")

// ScalePrice converts a wire-literal price to int64 scaled by 10^decimals.
// Operates on the byte string, never via float64.
//
//	ScalePrice("81011.5", 1)    == 810115
//	ScalePrice("0.1149362", 7)  == 1149362
//	ScalePrice("0.0000001", 7)  == 1
func ScalePrice(n json.Number, decimals int) (int64, error) {
	return scaleFixedPoint(string(n), decimals)
}

// ScaleSize converts a wire-literal qty to int64 scaled by 10^decimals.
// Same semantics as ScalePrice; broken out for label clarity at call sites.
func ScaleSize(n json.Number, decimals int) (int64, error) {
	return scaleFixedPoint(string(n), decimals)
}

func scaleFixedPoint(s string, decimals int) (int64, error) {
	if s == "" {
		return 0, fmt.Errorf("scale: empty wire literal")
	}
	if strings.HasPrefix(s, "-") {
		return 0, fmt.Errorf("scale: negative literal %q", s)
	}
	s = strings.TrimPrefix(s, "+")
	intPart, fracPart, hasFrac := strings.Cut(s, ".")
	if intPart == "" {
		intPart = "0"
	}
	for _, r := range intPart {
		if r < '0' || r > '9' {
			return 0, fmt.Errorf("scale: non-numeric integer part in %q", s)
		}
	}
	if hasFrac {
		for _, r := range fracPart {
			if r < '0' || r > '9' {
				return 0, fmt.Errorf("scale: non-numeric fractional part in %q", s)
			}
		}
		if len(fracPart) > decimals {
			tail := fracPart[decimals:]
			for _, r := range tail {
				if r != '0' {
					return 0, fmt.Errorf("%w: %q has %d frac digits; decimals=%d",
						ErrPrecisionOverflow, s, len(fracPart), decimals)
				}
			}
			fracPart = fracPart[:decimals]
		}
	}
	for len(fracPart) < decimals {
		fracPart += "0"
	}
	var acc int64
	for _, r := range intPart {
		acc = acc*10 + int64(r-'0')
	}
	for _, r := range fracPart {
		acc = acc*10 + int64(r-'0')
	}
	return acc, nil
}
