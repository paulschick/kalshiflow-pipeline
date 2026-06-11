package worker

import (
	"reflect"
	"sort"
	"testing"
)

func TestRoster_AddPending_RecordSidLater(t *testing.T) {
	r := NewRoster()
	r.AddPending(123, []string{"X", "Y"}, []string{"orderbook_delta", "trade"})
	if got := r.Tickers(); !reflect.DeepEqual(sortedStrings(got), []string{"X", "Y"}) {
		t.Errorf("tickers = %v", got)
	}
	r.RecordSid(123, 4242)
	if r.Sid("X") != 4242 || r.Sid("Y") != 4242 {
		t.Errorf("sid not recorded")
	}
}

func TestRoster_RemoveTicker(t *testing.T) {
	r := NewRoster()
	r.AddPending(1, []string{"X"}, []string{"trade"})
	r.RecordSid(1, 100)
	if _, ok := r.Remove("X"); !ok {
		t.Error("Remove returned ok=false for present ticker")
	}
	if r.Has("X") {
		t.Error("Has returned true after Remove")
	}
}

func TestRoster_HasIsIdempotent(t *testing.T) {
	r := NewRoster()
	r.AddPending(1, []string{"X"}, []string{"trade"})
	if !r.Has("X") {
		t.Error("Has returned false after AddPending")
	}
	if r.Has("Y") {
		t.Error("Has returned true for absent ticker")
	}
}

func sortedStrings(in []string) []string {
	out := append([]string(nil), in...)
	sort.Strings(out)
	return out
}

func TestRoster_SweepDiff_AddsNewTickers(t *testing.T) {
	r := NewRoster()
	r.AddPending(1, []string{"A", "B"}, []string{"orderbook_delta"})
	r.RecordSid(1, 100)

	toAdd, toTrim := r.SweepDiff([]string{"A", "B", "C", "D"}, 2)

	if got, want := toAdd, []string{"C", "D"}; !equalSorted(got, want) {
		t.Errorf("toAdd = %v, want %v", got, want)
	}
	if len(toTrim) != 0 {
		t.Errorf("toTrim = %v, want empty", toTrim)
	}
}

func TestRoster_SweepDiff_TrimAfterThreshold(t *testing.T) {
	r := NewRoster()
	r.AddPending(1, []string{"A", "B"}, []string{"orderbook_delta"})
	r.RecordSid(1, 100)

	// Tick 1: B disappears. Below threshold, no trim.
	toAdd, toTrim := r.SweepDiff([]string{"A"}, 2)
	if len(toAdd) != 0 || len(toTrim) != 0 {
		t.Fatalf("tick 1: toAdd=%v toTrim=%v", toAdd, toTrim)
	}

	// Tick 2: B still missing. missCount reaches 2 → trimmed.
	toAdd, toTrim = r.SweepDiff([]string{"A"}, 2)
	if len(toAdd) != 0 {
		t.Errorf("tick 2 toAdd = %v, want empty", toAdd)
	}
	if len(toTrim) != 1 || toTrim[0].Ticker != "B" || toTrim[0].Sid != 100 {
		t.Errorf("tick 2 toTrim = %+v, want [{Ticker:B Sid:100}]", toTrim)
	}
}

func TestRoster_SweepDiff_ResetMissOnReappear(t *testing.T) {
	r := NewRoster()
	r.AddPending(1, []string{"A"}, []string{"orderbook_delta"})
	r.RecordSid(1, 100)

	// Tick 1: A missing. missCount = 1.
	_, toTrim := r.SweepDiff([]string{}, 2)
	if len(toTrim) != 0 {
		t.Fatalf("tick 1 toTrim = %v, want empty", toTrim)
	}

	// Tick 2: A reappears. missCount resets to 0.
	_, toTrim = r.SweepDiff([]string{"A"}, 2)
	if len(toTrim) != 0 {
		t.Fatalf("tick 2 toTrim = %v, want empty", toTrim)
	}

	// Tick 3: A missing again. Counter starts at 0, so still below threshold.
	_, toTrim = r.SweepDiff([]string{}, 2)
	if len(toTrim) != 0 {
		t.Fatalf("tick 3 toTrim = %v, want empty (counter reset)", toTrim)
	}
}

func TestRoster_SweepDiff_PendingTickersStillCount(t *testing.T) {
	// A ticker added by AddPending but not yet acked (Sid=0) is still subject to
	// sweep diff. SweepDiff returns it in toTrim with Sid=0; caller knows to skip
	// the WS.Unsubscribe (no sid available).
	r := NewRoster()
	r.AddPending(1, []string{"A"}, []string{"orderbook_delta"})

	_, _ = r.SweepDiff([]string{}, 2)       // tick 1
	_, toTrim := r.SweepDiff([]string{}, 2) // tick 2

	if len(toTrim) != 1 || toTrim[0].Ticker != "A" || toTrim[0].Sid != 0 {
		t.Errorf("toTrim = %+v, want [{Ticker:A Sid:0}]", toTrim)
	}
}

// equalSorted is a tiny helper — equal as multisets after sort.
func equalSorted(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	sa := append([]string(nil), a...)
	sb := append([]string(nil), b...)
	sort.Strings(sa)
	sort.Strings(sb)
	for i := range sa {
		if sa[i] != sb[i] {
			return false
		}
	}
	return true
}
