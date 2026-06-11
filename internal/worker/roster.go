package worker

import "sync"

// Roster is the in-memory subscription set: market_ticker → entry.
//
// Two-state lifecycle:
//  1. AddPending(reqID, tickers, channels) — sid is 0; subscribe sent, awaiting `ok` ack.
//  2. RecordSid(reqID, sid)               — ack arrived; sid stamped on every ticker that
//     shared the reqID. Subsequent unsubscribes use sid.
type Roster struct {
	mu      sync.Mutex
	entries map[string]subscription
	pending map[int64][]string
}

type subscription struct {
	sid       int64
	channels  []string
	missCount int
}

// NewRoster constructs an empty Roster.
func NewRoster() *Roster {
	return &Roster{
		entries: map[string]subscription{},
		pending: map[int64][]string{},
	}
}

// AddPending records a subscribe whose `ok` ack hasn't yet arrived.
func (r *Roster) AddPending(reqID int64, tickers, channels []string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.pending[reqID] = tickers
	for _, t := range tickers {
		r.entries[t] = subscription{sid: 0, channels: channels}
	}
}

// RecordSid attaches the server-assigned sid to every ticker whose subscribe shared this
// reqID. Returns count of tickers updated.
func (r *Roster) RecordSid(reqID, sid int64) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	tickers, ok := r.pending[reqID]
	if !ok {
		return 0
	}
	delete(r.pending, reqID)
	for _, t := range tickers {
		entry := r.entries[t]
		entry.sid = sid
		r.entries[t] = entry
	}
	return len(tickers)
}

// Remove deletes a ticker entry, returning the prior server-assigned sid for caller-side
// Unsubscribe (or 0 if the ack hadn't yet arrived). ok=false means the ticker wasn't
// rostered.
func (r *Roster) Remove(ticker string) (sid int64, ok bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	entry, ok := r.entries[ticker]
	if !ok {
		return 0, false
	}
	delete(r.entries, ticker)
	return entry.sid, true
}

// Has reports whether a ticker is currently rostered (pending or acked).
func (r *Roster) Has(ticker string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	_, ok := r.entries[ticker]
	return ok
}

// Sid returns the server-assigned sid for a ticker, or 0 if not yet acked / not rostered.
func (r *Roster) Sid(ticker string) int64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.entries[ticker].sid
}

// Tickers returns a snapshot of currently-rostered tickers.
func (r *Roster) Tickers() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]string, 0, len(r.entries))
	for t := range r.entries {
		out = append(out, t)
	}
	return out
}

// Size returns the count of rostered tickers.
func (r *Roster) Size() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.entries)
}

// TrimEntry is a single sweep-trim action: which ticker to remove from the roster and
// the sid (if any) the caller can pass to WS.Unsubscribe. Sid = 0 means the entry never
// got an ack and the caller should only do a local Remove.
type TrimEntry struct {
	Ticker string
	Sid    int64
}

// SweepDiff reconciles the roster against the latest open-markets sweep response.
//
//   - Tickers in `seen` but not in the roster are returned in `toAdd`.
//   - Tickers in the roster present in `seen` have their missCount reset to 0.
//   - Tickers in the roster absent from `seen` get missCount += 1; once missCount >=
//     threshold, the entry is returned in `toTrim`. The caller is responsible for
//     calling Remove(ticker) AFTER sending the WS unsubscribe — SweepDiff does not
//     mutate roster membership for trim entries (only the missCount).
//
// Thread-safe: holds the roster lock for the duration of the diff.
func (r *Roster) SweepDiff(seen []string, threshold int) (toAdd []string, toTrim []TrimEntry) {
	r.mu.Lock()
	defer r.mu.Unlock()

	seenSet := make(map[string]struct{}, len(seen))
	for _, t := range seen {
		seenSet[t] = struct{}{}
	}

	for ticker, entry := range r.entries {
		if _, ok := seenSet[ticker]; ok {
			entry.missCount = 0
			r.entries[ticker] = entry
			continue
		}
		entry.missCount++
		r.entries[ticker] = entry
		if entry.missCount >= threshold {
			toTrim = append(toTrim, TrimEntry{Ticker: ticker, Sid: entry.sid})
		}
	}

	for _, t := range seen {
		if _, ok := r.entries[t]; !ok {
			toAdd = append(toAdd, t)
		}
	}
	return toAdd, toTrim
}
