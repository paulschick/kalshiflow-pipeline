package main

import (
	"strings"
	"testing"
)

func TestMustEnvPanics(t *testing.T) {
	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("expected panic on missing env")
		}
		s, ok := r.(string)
		if !ok || !strings.Contains(s, "FAKE_REQUIRED") {
			t.Fatalf("panic message %v should contain env name", r)
		}
	}()
	mustEnv("FAKE_REQUIRED")
}

func TestMustEnvReturnsValue(t *testing.T) {
	t.Setenv("FAKE_PRESENT", "value-x")
	if got := mustEnv("FAKE_PRESENT"); got != "value-x" {
		t.Fatalf("mustEnv = %q, want value-x", got)
	}
}

func TestSplitSeriesEnv(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want []string
	}{
		{"empty", "", nil},
		{"single", "KXBTCD", []string{"KXBTCD"}},
		{"two with spaces", "KXBTCD, KXETHD", []string{"KXBTCD", "KXETHD"}},
		{"trim + dedupe + drop empties", "KXBTCD,, KXBTCD ,KXETHD", []string{"KXBTCD", "KXETHD"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := splitSeriesEnv(tc.in)
			if len(got) != len(tc.want) {
				t.Fatalf("got %v, want %v", got, tc.want)
			}
			for i := range got {
				if got[i] != tc.want[i] {
					t.Errorf("idx %d: got %q want %q", i, got[i], tc.want[i])
				}
			}
		})
	}
}
