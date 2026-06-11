// Package orderbook provides per-market two-sided order book state for Kalshi binary markets.
package orderbook

import "sort"

// Side identifies the YES or NO side of a Kalshi binary-market book.
type Side int

const (
	// SideYes is the YES side of the binary market book.
	SideYes Side = iota
	// SideNo is the NO side of the binary market book.
	SideNo
)

// String returns the wire-format side name ("yes" or "no").
func (s Side) String() string {
	if s == SideYes {
		return "yes"
	}
	return "no"
}

// Book is one market's two-sided level table. Sizes are int64 share counts;
// a level with size 0 is absent from the map.
type Book struct {
	yes map[int64]int64
	no  map[int64]int64
}

// NewBook returns an empty Book.
func NewBook() *Book {
	return &Book{yes: map[int64]int64{}, no: map[int64]int64{}}
}

func (b *Book) sideMap(s Side) map[int64]int64 {
	if s == SideYes {
		return b.yes
	}
	return b.no
}

// Apply adds sizeDelta to the level at priceUnits on the given side. Returns
// (prevSize, newSize). If newSize would go negative it is clamped to 0; the
// level is removed from the map. A delta of 0 is a no-op (returns the
// current size for both prev and new).
func (b *Book) Apply(side Side, priceUnits int64, sizeDelta int64) (prev, cur int64) {
	m := b.sideMap(side)
	prev = m[priceUnits]
	cur = prev + sizeDelta
	if cur <= 0 {
		cur = 0
		delete(m, priceUnits)
		return prev, cur
	}
	m[priceUnits] = cur
	return prev, cur
}

// SizeAt returns the current size at (side, priceUnits), 0 if absent.
func (b *Book) SizeAt(side Side, priceUnits int64) int64 {
	return b.sideMap(side)[priceUnits]
}

// TopN returns the top-n highest priceUnits on the given side, sorted
// descending. Levels with size 0 are excluded. If the book has fewer than n
// levels, the slice is shorter; if n <= 0, returns nil.
func (b *Book) TopN(side Side, n int) []int64 {
	if n <= 0 {
		return nil
	}
	m := b.sideMap(side)
	prices := make([]int64, 0, len(m))
	for p := range m {
		prices = append(prices, p)
	}
	sort.Slice(prices, func(i, j int) bool { return prices[i] > prices[j] })
	if len(prices) > n {
		prices = prices[:n]
	}
	return prices
}
