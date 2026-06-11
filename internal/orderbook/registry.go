// Package orderbook provides per-market order-book state and a Registry for
// multi-market tracking.
package orderbook

// Registry maps market_ticker → *Book. Single-goroutine access is assumed;
// the bookkeeper owns mutation.
type Registry struct {
	books map[string]*Book
}

// NewRegistry returns an empty Registry.
func NewRegistry() *Registry {
	return &Registry{books: map[string]*Book{}}
}

// GetOrCreate returns the Book for ticker, creating a fresh one if absent.
func (r *Registry) GetOrCreate(ticker string) *Book {
	b, ok := r.books[ticker]
	if !ok {
		b = NewBook()
		r.books[ticker] = b
	}
	return b
}

// ResetMarket replaces the Book for ticker with a fresh empty one. Used on
// orderbook_snapshot frames (the snapshot signals "your book for this market
// is now empty; rebuild from forthcoming deltas").
func (r *Registry) ResetMarket(ticker string) {
	r.books[ticker] = NewBook()
}

// DeleteMarket removes the entry for ticker from the registry. Used by the
// bookkeeper for eviction (post-RetainOnly, sweep-trim) where the market is
// gone, not just empty. ResetMarket keeps the entry around for the
// orderbook_snapshot anchor case where subsequent deltas must land on the
// same Book pointer; DeleteMarket releases it.
func (r *Registry) DeleteMarket(ticker string) {
	delete(r.books, ticker)
}

// Reset replaces the entire registry with a fresh empty map. Used on
// reconnect.
func (r *Registry) Reset() {
	r.books = map[string]*Book{}
}

// Markets returns the current ticker set in undefined order. Used by the
// bookkeeper's heartbeat tick to walk every (market, side).
func (r *Registry) Markets() []string {
	out := make([]string, 0, len(r.books))
	for t := range r.books {
		out = append(out, t)
	}
	return out
}

// Size returns the number of markets currently tracked.
func (r *Registry) Size() int {
	return len(r.books)
}
