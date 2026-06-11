// internal/orderbook/book_test.go
package orderbook

import (
	"reflect"
	"testing"
)

func TestBook_AddLevel(t *testing.T) {
	t.Parallel()
	b := NewBook()
	prev, cur := b.Apply(SideYes, 7700, 100)
	if prev != 0 || cur != 100 {
		t.Errorf("Apply: prev=%d cur=%d; want 0,100", prev, cur)
	}
	if got := b.SizeAt(SideYes, 7700); got != 100 {
		t.Errorf("SizeAt = %d; want 100", got)
	}
}

func TestBook_ModifyAndRemoveLevel(t *testing.T) {
	t.Parallel()
	b := NewBook()
	b.Apply(SideYes, 7700, 500)
	prev, cur := b.Apply(SideYes, 7700, -200)
	if prev != 500 || cur != 300 {
		t.Errorf("modify: prev=%d cur=%d; want 500,300", prev, cur)
	}
	prev, cur = b.Apply(SideYes, 7700, -300)
	if prev != 300 || cur != 0 {
		t.Errorf("remove: prev=%d cur=%d; want 300,0", prev, cur)
	}
	if got := b.SizeAt(SideYes, 7700); got != 0 {
		t.Errorf("SizeAt after remove = %d; want 0", got)
	}
}

func TestBook_NegativeSizeClamps(t *testing.T) {
	t.Parallel()
	b := NewBook()
	b.Apply(SideYes, 7700, 100)
	prev, cur := b.Apply(SideYes, 7700, -500)
	if prev != 100 || cur != 0 {
		t.Errorf("neg-clamp: prev=%d cur=%d; want 100,0", prev, cur)
	}
}

func TestBook_TopN(t *testing.T) {
	t.Parallel()
	b := NewBook()
	b.Apply(SideYes, 1000, 10)
	b.Apply(SideYes, 4000, 10)
	b.Apply(SideYes, 2000, 10)
	b.Apply(SideYes, 3000, 10)
	if got, want := b.TopN(SideYes, 2), []int64{4000, 3000}; !reflect.DeepEqual(got, want) {
		t.Errorf("TopN = %v; want %v", got, want)
	}
	b.Apply(SideYes, 4000, -10)
	if got, want := b.TopN(SideYes, 2), []int64{3000, 2000}; !reflect.DeepEqual(got, want) {
		t.Errorf("TopN after remove = %v; want %v", got, want)
	}
}

func TestBook_TopNFewerThanN(t *testing.T) {
	t.Parallel()
	b := NewBook()
	b.Apply(SideYes, 5000, 10)
	if got, want := b.TopN(SideYes, 3), []int64{5000}; !reflect.DeepEqual(got, want) {
		t.Errorf("TopN = %v; want %v", got, want)
	}
	if got := b.TopN(SideYes, 0); len(got) != 0 {
		t.Errorf("TopN(0) = %v; want empty", got)
	}
}

func TestBook_SidesAreIndependent(t *testing.T) {
	t.Parallel()
	b := NewBook()
	b.Apply(SideYes, 7700, 100)
	b.Apply(SideNo, 7700, 200)
	if got := b.SizeAt(SideYes, 7700); got != 100 {
		t.Errorf("yes 7700 = %d; want 100", got)
	}
	if got := b.SizeAt(SideNo, 7700); got != 200 {
		t.Errorf("no 7700 = %d; want 200", got)
	}
}
