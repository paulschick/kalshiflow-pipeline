package main

import (
	"reflect"
	"testing"
	"time"
)

func TestParseConfig_RequiresProjectID(t *testing.T) {
	t.Setenv("KRAKEN_PROJECT_ID", "")
	t.Setenv("KRAKEN_FLAG_BUCKET", "anything")
	if _, err := parseConfig(); err == nil {
		t.Fatal("want error for missing KRAKEN_PROJECT_ID")
	}
}

func TestParseConfig_RequiresFlagBucket(t *testing.T) {
	t.Setenv("KRAKEN_PROJECT_ID", "p")
	t.Setenv("KRAKEN_FLAG_BUCKET", "")
	if _, err := parseConfig(); err == nil {
		t.Fatal("want error for missing KRAKEN_FLAG_BUCKET")
	}
}

func TestParseConfig_Defaults(t *testing.T) {
	t.Setenv("KRAKEN_PROJECT_ID", "p")
	t.Setenv("KRAKEN_FLAG_BUCKET", "b")
	c, err := parseConfig()
	if err != nil {
		t.Fatal(err)
	}
	if c.Region != "us-east4" {
		t.Errorf("Region = %q; want us-east4", c.Region)
	}
	if c.ControlObject != "control/kraken_enabled.json" {
		t.Errorf("ControlObject = %q; want control/kraken_enabled.json", c.ControlObject)
	}
	if c.WSURL != "wss://ws.kraken.com/v2" {
		t.Errorf("WSURL = %q; want wss://ws.kraken.com/v2", c.WSURL)
	}
	if c.RESTURL != "https://api.kraken.com" {
		t.Errorf("RESTURL = %q; want https://api.kraken.com", c.RESTURL)
	}
	if c.SilentStall != 60*time.Second {
		t.Errorf("SilentStall = %v; want 60s default", c.SilentStall)
	}
}

// HOL-49 — kraken_pairs.json control surface.

func TestParseConfig_PairsObjectDefault(t *testing.T) {
	t.Setenv("KRAKEN_PROJECT_ID", "p")
	t.Setenv("KRAKEN_FLAG_BUCKET", "b")
	t.Setenv("KRAKEN_PAIRS_OBJECT", "")
	c, err := parseConfig()
	if err != nil {
		t.Fatal(err)
	}
	if c.PairsObject != "control/kraken_pairs.json" {
		t.Errorf("PairsObject default = %q; want control/kraken_pairs.json", c.PairsObject)
	}
}

func TestParseConfig_PairsBucketInheritsControlBucket(t *testing.T) {
	t.Setenv("KRAKEN_PROJECT_ID", "p")
	t.Setenv("KRAKEN_FLAG_BUCKET", "ctrl-bucket")
	t.Setenv("KRAKEN_PAIRS_BUCKET", "")
	c, err := parseConfig()
	if err != nil {
		t.Fatal(err)
	}
	if c.PairsBucket != "ctrl-bucket" {
		t.Errorf("PairsBucket = %q; want it to inherit KRAKEN_FLAG_BUCKET", c.PairsBucket)
	}
}

func TestParseConfig_PairsBucketOverride(t *testing.T) {
	t.Setenv("KRAKEN_PROJECT_ID", "p")
	t.Setenv("KRAKEN_FLAG_BUCKET", "ctrl-bucket")
	t.Setenv("KRAKEN_PAIRS_BUCKET", "pairs-bucket")
	c, err := parseConfig()
	if err != nil {
		t.Fatal(err)
	}
	if c.PairsBucket != "pairs-bucket" {
		t.Errorf("PairsBucket = %q; want pairs-bucket override", c.PairsBucket)
	}
}

func TestParseConfig_PairsFallbackCSV(t *testing.T) {
	cases := []struct {
		name string
		csv  string
		want []string
	}{
		{"unset", "", nil},
		{"single", "BTC/USD", []string{"BTC/USD"}},
		{"trim + drop empty", " BTC/USD , ETH/USD ,, SOL/USD ", []string{"BTC/USD", "ETH/USD", "SOL/USD"}},
		{"only whitespace", " , , ", nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("KRAKEN_PROJECT_ID", "p")
			t.Setenv("KRAKEN_FLAG_BUCKET", "b")
			t.Setenv("KRAKEN_PAIRS", tc.csv)
			c, err := parseConfig()
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(c.PairsFallback, tc.want) {
				t.Errorf("PairsFallback = %v; want %v", c.PairsFallback, tc.want)
			}
		})
	}
}

// TestSubscriptionLogRow_BQTagShape locks the BQ tag → column mapping for
// kraken_raw.subscription_log. A typo at field rename would silently fail
// streaming inserts (per-row error, no client-side type check) — this is
// the only place a unit test catches it before prod.
func TestSubscriptionLogRow_BQTagShape(t *testing.T) {
	t.Parallel()
	want := map[string]string{
		"Ts":     "ts",
		"Pair":   "pair",
		"Action": "action",
		"Reason": "reason",
		"Actor":  "actor",
	}
	rt := reflect.TypeOf(subscriptionLogRow{})
	if rt.NumField() != len(want) {
		t.Fatalf("subscriptionLogRow has %d fields; want %d", rt.NumField(), len(want))
	}
	for i := 0; i < rt.NumField(); i++ {
		f := rt.Field(i)
		got := f.Tag.Get("bigquery")
		expect, ok := want[f.Name]
		if !ok {
			t.Errorf("unexpected field %s", f.Name)
			continue
		}
		if got != expect {
			t.Errorf("field %s bigquery tag = %q; want %q", f.Name, got, expect)
		}
	}
}

func TestParseConfig_SilentStallEnvOverride(t *testing.T) {
	t.Setenv("KRAKEN_PROJECT_ID", "p")
	t.Setenv("KRAKEN_FLAG_BUCKET", "b")

	t.Run("override 30s", func(t *testing.T) {
		t.Setenv("KRAKEN_WS_SILENT_STALL_THRESHOLD", "30s")
		c, err := parseConfig()
		if err != nil {
			t.Fatalf("parseConfig: %v", err)
		}
		if c.SilentStall != 30*time.Second {
			t.Errorf("SilentStall = %v; want 30s", c.SilentStall)
		}
	})

	t.Run("invalid duration errors", func(t *testing.T) {
		t.Setenv("KRAKEN_WS_SILENT_STALL_THRESHOLD", "not-a-duration")
		if _, err := parseConfig(); err == nil {
			t.Fatal("want error for invalid KRAKEN_WS_SILENT_STALL_THRESHOLD")
		}
	})
}
