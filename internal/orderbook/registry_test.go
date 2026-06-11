// internal/orderbook/registry_test.go
package orderbook

import (
	"sort"
	"testing"
)

func TestRegistry_GetOrCreate(t *testing.T) {
	t.Parallel()
	r := NewRegistry()
	a := r.GetOrCreate("KXBTCD-X")
	b := r.GetOrCreate("KXBTCD-X")
	if a != b {
		t.Errorf("GetOrCreate returned different *Book for same ticker")
	}
	c := r.GetOrCreate("KXBTCD-Y")
	if a == c {
		t.Errorf("GetOrCreate returned same *Book for different tickers")
	}
}

func TestRegistry_ResetMarket(t *testing.T) {
	t.Parallel()
	r := NewRegistry()
	book := r.GetOrCreate("KXBTCD-X")
	book.Apply(SideYes, 5000, 100)
	r.ResetMarket("KXBTCD-X")
	fresh := r.GetOrCreate("KXBTCD-X")
	if got := fresh.SizeAt(SideYes, 5000); got != 0 {
		t.Errorf("size after ResetMarket = %d; want 0", got)
	}
}

func TestRegistry_Reset(t *testing.T) {
	t.Parallel()
	r := NewRegistry()
	r.GetOrCreate("A").Apply(SideYes, 1000, 1)
	r.GetOrCreate("B").Apply(SideNo, 2000, 2)
	r.Reset()
	if r.GetOrCreate("A").SizeAt(SideYes, 1000) != 0 {
		t.Errorf("A non-zero after Reset")
	}
	if r.GetOrCreate("B").SizeAt(SideNo, 2000) != 0 {
		t.Errorf("B non-zero after Reset")
	}
}

func TestRegistry_Markets(t *testing.T) {
	t.Parallel()
	r := NewRegistry()
	r.GetOrCreate("A")
	r.GetOrCreate("B")
	r.GetOrCreate("C")
	got := r.Markets()
	sort.Strings(got)
	want := []string{"A", "B", "C"}
	if len(got) != len(want) {
		t.Fatalf("Markets count = %d; want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("Markets[%d] = %q; want %q", i, got[i], want[i])
		}
	}
}

func TestRegistry_DeleteMarketRemovesEntry(t *testing.T) {
	t.Parallel()
	r := NewRegistry()
	r.GetOrCreate("A").Apply(SideYes, 1000, 1)
	r.GetOrCreate("B").Apply(SideNo, 2000, 2)
	if got := r.Size(); got != 2 {
		t.Fatalf("Size before delete = %d; want 2", got)
	}

	r.DeleteMarket("A")

	if got := r.Size(); got != 1 {
		t.Errorf("Size after delete = %d; want 1", got)
	}
	got := r.Markets()
	if len(got) != 1 || got[0] != "B" {
		t.Errorf("Markets after delete = %v; want [B]", got)
	}
}
