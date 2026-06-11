package orderbook

import (
	"encoding/json"
	"errors"
	"testing"
)

func TestSide_String(t *testing.T) {
	if SideBid.String() != "bid" {
		t.Errorf("SideBid.String() = %q", SideBid.String())
	}
	if SideAsk.String() != "ask" {
		t.Errorf("SideAsk.String() = %q", SideAsk.String())
	}
}

func TestScalePrice_FromWireLiteral(t *testing.T) {
	cases := []struct {
		name     string
		wire     string
		decimals int
		want     int64
		wantErr  bool
	}{
		{"BTC plain", "81011.5", 1, 810115, false},
		{"DOGE 7-decimal", "0.1149362", 7, 1149362, false},
		{"ETH 2-decimal", "2284.15", 2, 228415, false},
		{"integer wire", "12345", 2, 1234500, false},
		{"trailing zeros", "0.10000000", 7, 1000000, false},
		{"leading zeros", "0.0000001", 7, 1, false},
		{"too many fractional digits", "0.12345678", 7, 0, true},
		{"negative price", "-1.0", 1, 0, true},
		{"non-numeric", "abc", 1, 0, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ScalePrice(json.Number(tc.wire), tc.decimals)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("ScalePrice(%q,%d) want err; got %d", tc.wire, tc.decimals, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("ScalePrice(%q,%d) err = %v", tc.wire, tc.decimals, err)
			}
			if got != tc.want {
				t.Errorf("ScalePrice(%q,%d) = %d; want %d", tc.wire, tc.decimals, got, tc.want)
			}
		})
	}
}

func TestScaleSize_FromWireLiteral_LotDecimals8(t *testing.T) {
	cases := []struct {
		wire string
		want int64
	}{
		{"0.00123440", 123440},
		{"1603.12500000", 160312500000},
		{"1.00000000", 100000000},
		{"0", 0},
	}
	for _, tc := range cases {
		got, err := ScaleSize(json.Number(tc.wire), 8)
		if err != nil {
			t.Fatalf("ScaleSize(%q) err = %v", tc.wire, err)
		}
		if got != tc.want {
			t.Errorf("ScaleSize(%q) = %d; want %d", tc.wire, got, tc.want)
		}
	}
}

func TestScaleSize_RejectsExcessPrecision(t *testing.T) {
	_, err := ScaleSize(json.Number("0.000000001"), 8)
	if err == nil || !errors.Is(err, ErrPrecisionOverflow) {
		t.Fatalf("want ErrPrecisionOverflow; got %v", err)
	}
}
