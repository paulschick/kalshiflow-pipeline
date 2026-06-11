package orderbook

import (
	"hash/crc32"
	"strings"
)

// VerifyChecksum re-derives the CRC32 of the top-10 (asks asc, bids desc) and
// compares to the wire-supplied checksum. Returns (matched, computed).
//
// Algorithm (per spec §9 hypothesis, captured-frame unit test is the binding
// contract):
//
//	For each of top-10 asks (price asc) then top-10 bids (price desc):
//	  append priceStr followed by qtyStr, where each is the wire-literal
//	  string with the decimal point removed and leading zeros stripped.
//
// The byte sequence is fed to crc32.IEEE.
//
// If this hypothesis is wrong the unit test against testdata/btcusd-snapshot.json
// will fail — that test is the contract; this function is the implementation.
func VerifyChecksum(asks, bids []WireLevel, expected uint32) (bool, uint32) {
	var sb strings.Builder
	appendLevels(&sb, asks, 10)
	appendLevels(&sb, bids, 10)
	got := crc32.ChecksumIEEE([]byte(sb.String()))
	return got == expected, got
}

// WireLevelsToChecksumBytes returns the exact byte sequence VerifyChecksum
// feeds to crc32.IEEE for the given (asks, bids) pair. Used by the worker's
// mismatch-debug log path so the wire-bytes-Kraken-disagreed-with can be
// diffed against f.Raw post-merge.
func WireLevelsToChecksumBytes(asks, bids []WireLevel) []byte {
	var sb strings.Builder
	appendLevels(&sb, asks, 10)
	appendLevels(&sb, bids, 10)
	return []byte(sb.String())
}

func appendLevels(sb *strings.Builder, levels []WireLevel, n int) {
	if len(levels) > n {
		levels = levels[:n]
	}
	for _, lv := range levels {
		sb.WriteString(normalizeChecksumLiteral(string(lv.Price)))
		sb.WriteString(normalizeChecksumLiteral(string(lv.Qty)))
	}
}

// normalizeChecksumLiteral strips the decimal point and any leading zeros.
// Trailing zeros are preserved (they carry information up to lot_decimals).
func normalizeChecksumLiteral(s string) string {
	if i := strings.IndexByte(s, '.'); i >= 0 {
		s = s[:i] + s[i+1:]
	}
	j := 0
	for j < len(s)-1 && s[j] == '0' {
		j++
	}
	return s[j:]
}
