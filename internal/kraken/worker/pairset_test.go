package worker

import (
	"reflect"
	"sync"
	"testing"
)

func TestPairSet_Store_DedupSort(t *testing.T) {
	s := newPairSet([]string{"ETH/USD", "BTC/USD", "ETH/USD"})
	got := s.Load()
	want := []string{"BTC/USD", "ETH/USD"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %v; want %v", got, want)
	}
}

func TestPairSet_Add_Idempotent(t *testing.T) {
	s := newPairSet([]string{"BTC/USD"})
	if added := s.Add("BTC/USD"); added {
		t.Error("Add of existing pair must return false")
	}
	if added := s.Add("LTC/USD"); !added {
		t.Error("Add of new pair must return true")
	}
	got := s.Load()
	want := []string{"BTC/USD", "LTC/USD"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %v; want %v", got, want)
	}
}

func TestPairSet_Remove_Idempotent(t *testing.T) {
	s := newPairSet([]string{"BTC/USD", "LTC/USD"})
	if removed := s.Remove("LTC/USD"); !removed {
		t.Error("Remove of present pair must return true")
	}
	if removed := s.Remove("LTC/USD"); removed {
		t.Error("Remove of absent pair must return false")
	}
	got := s.Load()
	want := []string{"BTC/USD"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %v; want %v", got, want)
	}
}

func TestPairSet_Diff(t *testing.T) {
	s := newPairSet([]string{"BTC/USD", "ETH/USD", "SOL/USD"})
	add, rm := s.Diff([]string{"ETH/USD", "SOL/USD", "XRP/USD", "DOGE/USD"})
	if !reflect.DeepEqual(add, []string{"DOGE/USD", "XRP/USD"}) {
		t.Errorf("toAdd = %v; want [DOGE/USD XRP/USD]", add)
	}
	if !reflect.DeepEqual(rm, []string{"BTC/USD"}) {
		t.Errorf("toRemove = %v; want [BTC/USD]", rm)
	}
}

func TestPairSet_Diff_NoChange(t *testing.T) {
	s := newPairSet([]string{"BTC/USD", "ETH/USD"})
	add, rm := s.Diff([]string{"ETH/USD", "BTC/USD"}) // intentionally unsorted
	if len(add) != 0 || len(rm) != 0 {
		t.Errorf("expected no diff; got add=%v rm=%v", add, rm)
	}
}

func TestPairSet_Load_DefensiveCopy(t *testing.T) {
	s := newPairSet([]string{"BTC/USD"})
	got := s.Load()
	got[0] = "MUTATED"
	if got2 := s.Load(); got2[0] == "MUTATED" {
		t.Error("Load() must return a defensive copy")
	}
}

func TestPairSet_RaceAddRemoveLoad(_ *testing.T) {
	s := newPairSet([]string{"BTC/USD"})
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(3)
		go func() { defer wg.Done(); s.Add("ETH/USD") }()
		go func() { defer wg.Done(); s.Remove("ETH/USD") }()
		go func() { defer wg.Done(); _ = s.Load() }()
	}
	wg.Wait()
	// No panic / -race clean == pass. The final set is non-deterministic.
}
