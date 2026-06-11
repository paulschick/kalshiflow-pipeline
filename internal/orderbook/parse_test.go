// internal/orderbook/parse_test.go
package orderbook

import "testing"

func TestParsePriceUnits(t *testing.T) {
	t.Parallel()
	cases := []struct {
		in      string
		want    int64
		wantErr bool
	}{
		{"0.0100", 100, false},
		{"0.7700", 7700, false},
		{"0.9900", 9900, false},
		{"0.5000", 5000, false},
		{"0.1234", 1234, false},
		{"1.0000", 10000, false},
		{"", 0, true},
		{"abc", 0, true},
		{"0.77", 0, true},    // wrong precision (4 decimals required)
		{"-0.5000", 0, true}, // negative not valid
	}
	for _, tc := range cases {
		t.Run(tc.in, func(t *testing.T) {
			got, err := ParsePriceUnits(tc.in)
			if (err != nil) != tc.wantErr {
				t.Fatalf("err = %v; wantErr %v", err, tc.wantErr)
			}
			if got != tc.want {
				t.Errorf("got %d; want %d", got, tc.want)
			}
		})
	}
}

func TestParseDeltaFP(t *testing.T) {
	t.Parallel()
	cases := []struct {
		in      string
		want    int64
		wantErr bool
	}{
		{"0", 0, false},
		{"0.00", 0, false},
		{"350.00", 350, false},
		{"-350.00", -350, false},
		{"1000.00", 1000, false},
		{"-1.49", -1, false},
		{"-1.51", -2, false},
		{"", 0, true},
		{"abc", 0, true},
	}
	for _, tc := range cases {
		t.Run(tc.in, func(t *testing.T) {
			got, err := ParseDeltaFP(tc.in)
			if (err != nil) != tc.wantErr {
				t.Fatalf("err = %v; wantErr %v", err, tc.wantErr)
			}
			if got != tc.want {
				t.Errorf("got %d; want %d", got, tc.want)
			}
		})
	}
}
