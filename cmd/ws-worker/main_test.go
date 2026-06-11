package main

import (
	"testing"
	"time"
)

func TestParseConfig_RequiresProject(t *testing.T) {
	t.Setenv("KALSHI_PROJECT_ID", "")
	if _, err := parseConfig(); err == nil {
		t.Fatalf("expected error for missing project id")
	}
}

func TestParseConfig_SplitsKalshiSeriesCSV(t *testing.T) {
	t.Setenv("KALSHI_PROJECT_ID", "p")
	t.Setenv("KALSHI_SERIES", "KXBTCD, KXETHD ,KXSOLD")
	cfg, err := parseConfig()
	if err != nil {
		t.Fatalf("parseConfig: %v", err)
	}
	want := []string{"KXBTCD", "KXETHD", "KXSOLD"}
	if len(cfg.Series) != len(want) {
		t.Fatalf("Series len = %d; want %d (%v)", len(cfg.Series), len(want), cfg.Series)
	}
	for i, w := range want {
		if cfg.Series[i] != w {
			t.Errorf("Series[%d] = %q; want %q", i, cfg.Series[i], w)
		}
	}
}

func TestParseConfig_DefaultSeriesIsKXBTCD(t *testing.T) {
	t.Setenv("KALSHI_PROJECT_ID", "p")
	t.Setenv("KALSHI_SERIES", "")
	cfg, err := parseConfig()
	if err != nil {
		t.Fatalf("parseConfig: %v", err)
	}
	if len(cfg.Series) != 1 || cfg.Series[0] != "KXBTCD" {
		t.Errorf("default Series = %v; want [KXBTCD]", cfg.Series)
	}
}

func TestParseConfig_OpenStatusDefaultsToOpen(t *testing.T) {
	t.Setenv("KALSHI_PROJECT_ID", "p")
	t.Setenv("KALSHI_SERIES", "KXBTCD")
	t.Setenv("KALSHI_OPEN_STATUS", "")
	cfg, err := parseConfig()
	if err != nil {
		t.Fatalf("parseConfig: %v", err)
	}
	if cfg.OpenStatus != "open" {
		t.Errorf("OpenStatus = %q; want open (probe verdict)", cfg.OpenStatus)
	}
}

func TestParseConfig_OpenStatusOverride(t *testing.T) {
	t.Setenv("KALSHI_PROJECT_ID", "p")
	t.Setenv("KALSHI_SERIES", "KXBTCD")
	t.Setenv("KALSHI_OPEN_STATUS", "tradable")
	cfg, err := parseConfig()
	if err != nil {
		t.Fatalf("parseConfig: %v", err)
	}
	if cfg.OpenStatus != "tradable" {
		t.Errorf("OpenStatus = %q; want tradable", cfg.OpenStatus)
	}
}

func TestParseConfig_SweepDefaults(t *testing.T) {
	t.Setenv("KALSHI_PROJECT_ID", "p")
	t.Setenv("KALSHI_SERIES", "KXBTCD")
	// no KALSHI_SWEEP_INTERVAL / KALSHI_SWEEP_MISS_THRESHOLD set

	c, err := parseConfig()
	if err != nil {
		t.Fatalf("parseConfig: %v", err)
	}
	if c.SweepInterval != 3*time.Minute {
		t.Errorf("SweepInterval = %v, want 3m", c.SweepInterval)
	}
	if c.SweepMissThreshold != 2 {
		t.Errorf("SweepMissThreshold = %d, want 2", c.SweepMissThreshold)
	}
}

func TestParseConfig_SweepOverrides(t *testing.T) {
	t.Setenv("KALSHI_PROJECT_ID", "p")
	t.Setenv("KALSHI_SERIES", "KXBTCD")
	t.Setenv("KALSHI_SWEEP_INTERVAL", "90s")
	t.Setenv("KALSHI_SWEEP_MISS_THRESHOLD", "5")

	c, err := parseConfig()
	if err != nil {
		t.Fatalf("parseConfig: %v", err)
	}
	if c.SweepInterval != 90*time.Second {
		t.Errorf("SweepInterval = %v, want 90s", c.SweepInterval)
	}
	if c.SweepMissThreshold != 5 {
		t.Errorf("SweepMissThreshold = %d, want 5", c.SweepMissThreshold)
	}
}

func TestParseConfig_RESTRateLimitDefaults(t *testing.T) {
	t.Setenv("KALSHI_PROJECT_ID", "p")
	t.Setenv("KALSHI_SERIES", "KXBTCD")
	t.Setenv("KALSHI_REST_RPS", "")
	t.Setenv("KALSHI_REST_BURST", "")
	cfg, err := parseConfig()
	if err != nil {
		t.Fatalf("parseConfig: %v", err)
	}
	if cfg.RESTRPS != 10 {
		t.Errorf("RESTRPS = %v; want 10", cfg.RESTRPS)
	}
	if cfg.RESTBurst != 20 {
		t.Errorf("RESTBurst = %v; want 20", cfg.RESTBurst)
	}
}
