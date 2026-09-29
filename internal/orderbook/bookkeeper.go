package orderbook

import (
	"context"
	"fmt"
	"time"
)

// SnapshotLevel is one (price, size) entry from an orderbook_snapshot
// payload. Sizes are integer share counts as they arrive on the Kalshi
// wire; the bookkeeper treats them as absolute (not delta) values.
type SnapshotLevel struct {
	PriceUnits int64
	Size       int64
}

// SnapshotMsg is the input shape for ApplySnapshot. The bookkeeper
// replaces the existing book for Ticker with a fresh one and writes
// every level into it. An empty SnapshotMsg (no Yes / No levels) is
// treated as "the book is empty" and wipes any pre-existing entries
// for Ticker — distinguish from "no snapshot frame at all" at the
// caller. Done is closed once handleSnapshot completes so the worker
// can serialize a subsequent delta against the snapshot's effect. The
// Kalshi wire ordering guarantees deltas arrive after their snapshot;
// this channel preserves that ordering through the bookkeeper's
// select loop.
type SnapshotMsg struct {
	Ticker string
	Yes    []SnapshotLevel
	No     []SnapshotLevel
	Done   chan struct{}
}

// DeltaMsg is the input shape for Apply: the channel-borne unit of work the
// bookkeeper consumes.
type DeltaMsg struct {
	Ticker     string
	Side       Side
	PriceUnits int64
	SizeDelta  int64
}

// PublishFn is the bookkeeper's publish callback. ticker + side ride alongside
// the payload because the proto inner drops market_ticker and ts (they ride on
// the outer envelope). The cmd-side closure uses ticker for series resolution
// + contract IDs; side is passed for symmetry / future labels.
type PublishFn func(ticker string, side Side, p SnapshotPayload) error

// BookkeeperConfig configures the bookkeeper.
type BookkeeperConfig struct {
	Publish PublishFn
	// ChangeTickInterval is the cadence at which dirty markets emit. 0 → 1s.
	ChangeTickInterval time.Duration
	// HeartbeatInterval is the cadence at which every tracked (market, side)
	// emits regardless of dirty. 0 → 300s.
	HeartbeatInterval time.Duration
	// DeltaBufferSize is the deltas channel capacity. 0 → 4096.
	DeltaBufferSize int
	// ResetBufferSize is the snapshots / deleteMarkets channel capacity.
	// 0 → 256. Must remain ≥ peak per-session snapshot fan-out (one
	// snapshot per (re)subscribed market) so ApplySnapshot's blocking
	// send does not back-pressure the WS read loop on resubscribe of a
	// large series set. 10–20 series prod target ≈ ≤ 480 open markets;
	// raise this cap if that lands.
	ResetBufferSize int
	// Now returns the current time. nil → time.Now (UTC inside the bookkeeper).
	Now func() time.Time
	// OnRegistrySize, if non-nil, is called from the Run goroutine after every
	// change to the registry size. Use to update a metrics gauge without
	// external polling (which would race on the registry map).
	OnRegistrySize func(int)
}

const (
	defaultChangeTick  = 1 * time.Second
	defaultHeartbeat   = 300 * time.Second
	defaultDeltaBuffer = 4096
	defaultResetBuffer = 256
)

// Bookkeeper owns the registry and dirty set. Mutation happens only inside Run.
// External callers push work via Apply / ApplySnapshot / DeleteMarket / Reset /
// RetainOnly, which forward to internal channels.
type Bookkeeper struct {
	cfg BookkeeperConfig

	deltas        chan DeltaMsg
	snapshots     chan SnapshotMsg
	deleteMarkets chan string
	resetAll      chan struct{}
	retainOnly    chan []string

	// Owned by the Run goroutine; do not touch from outside.
	registry *Registry
	dirty    map[string]map[Side]struct{}
}

// NewBookkeeper constructs a bookkeeper with defaults filled in. Run must be
// called for the bookkeeper to start emitting.
func NewBookkeeper(cfg BookkeeperConfig) *Bookkeeper {
	if cfg.ChangeTickInterval <= 0 {
		cfg.ChangeTickInterval = defaultChangeTick
	}
	if cfg.HeartbeatInterval <= 0 {
		cfg.HeartbeatInterval = defaultHeartbeat
	}
	if cfg.DeltaBufferSize <= 0 {
		cfg.DeltaBufferSize = defaultDeltaBuffer
	}
	if cfg.ResetBufferSize <= 0 {
		cfg.ResetBufferSize = defaultResetBuffer
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	return &Bookkeeper{
		cfg:           cfg,
		deltas:        make(chan DeltaMsg, cfg.DeltaBufferSize),
		snapshots:     make(chan SnapshotMsg, cfg.ResetBufferSize),
		deleteMarkets: make(chan string, cfg.ResetBufferSize),
		resetAll:      make(chan struct{}, 1),
		retainOnly:    make(chan []string, 1),
		registry:      NewRegistry(),
		dirty:         map[string]map[Side]struct{}{},
	}
}

// Apply enqueues a delta for the bookkeeper to apply on its goroutine. Blocks
// if the deltas channel is full (backpressure on the worker's read loop).
func (b *Bookkeeper) Apply(d DeltaMsg) {
	b.deltas <- d
}

// ApplySnapshot enqueues a snapshot for the bookkeeper to apply on its
// goroutine. Blocks if the snapshots channel is full. The bookkeeper
// closes msg.Done after handleSnapshot has updated the registry.
func (b *Bookkeeper) ApplySnapshot(msg SnapshotMsg) {
	b.snapshots <- msg
}

// DeleteMarket enqueues a per-market eviction (driven by post-outage
// roster reconciliation or sweep-trim). Removes the registry entry
// entirely so heartbeat stops iterating it. The orderbook_snapshot
// anchor frame is handled by ApplySnapshot, which resets and
// repopulates the book in one atomic step on the Run goroutine.
// Blocks if the deleteMarkets channel is full.
func (b *Bookkeeper) DeleteMarket(ticker string) {
	b.deleteMarkets <- ticker
}

// Reset enqueues a full registry reset (driven by reconnect). Coalesces
// multiple pending resets into one.
func (b *Bookkeeper) Reset() {
	select {
	case b.resetAll <- struct{}{}:
	default:
	}
}

// RetainOnly evicts every market from the Registry that is not in `keep`.
// Used by the worker post-resubscribe to drop markets that disappeared
// during a Kalshi-side outage so heartbeat stops emitting stale top-2 for
// them. Channel-driven for the same reason as Reset / DeleteMarket — the
// Registry is owned by the Run goroutine.
//
// Coalescing semantics: if a previous RetainOnly hasn't been processed
// yet, the new call is dropped. The queued one is at least as fresh and
// the loss is bounded by one Run-loop iteration.
func (b *Bookkeeper) RetainOnly(keep []string) {
	cp := append([]string(nil), keep...)
	select {
	case b.retainOnly <- cp:
	default:
	}
}

// Run blocks until ctx is done. Owns the registry and dirty set.
func (b *Bookkeeper) Run(ctx context.Context) {
	changeTick := time.NewTicker(b.cfg.ChangeTickInterval)
	defer changeTick.Stop()
	heartbeatTick := time.NewTicker(b.cfg.HeartbeatInterval)
	defer heartbeatTick.Stop()

	for {
		// A pending reset takes priority. Callers queue Reset before the
		// session's snapshot anchors; select picks randomly among ready
		// cases, so without this check a late-scheduled Run could apply
		// the anchors first and then wipe them with the reset.
		select {
		case <-b.resetAll:
			b.handleReset()
			continue
		default:
		}
		select {
		case <-ctx.Done():
			return
		case d := <-b.deltas:
			b.handleDelta(d)
		case msg := <-b.snapshots:
			b.handleSnapshot(msg)
		case t := <-b.deleteMarkets:
			b.handleDeleteMarket(t)
		case <-b.resetAll:
			b.handleReset()
		case keep := <-b.retainOnly:
			keepSet := make(map[string]struct{}, len(keep))
			for _, t := range keep {
				keepSet[t] = struct{}{}
			}
			for _, t := range b.registry.Markets() {
				if _, ok := keepSet[t]; !ok {
					b.registry.DeleteMarket(t)
					delete(b.dirty, t)
				}
			}
			b.notifySize()
		case <-changeTick.C:
			b.flushDirty(EmitReasonChange)
		case <-heartbeatTick.C:
			b.flushHeartbeat()
		}
	}
}

func (b *Bookkeeper) handleReset() {
	b.registry.Reset()
	b.dirty = map[string]map[Side]struct{}{}
	b.notifySize()
}

func (b *Bookkeeper) notifySize() {
	if b.cfg.OnRegistrySize != nil {
		b.cfg.OnRegistrySize(b.registry.Size())
	}
}

func (b *Bookkeeper) handleDelta(d DeltaMsg) {
	sizeBefore := b.registry.Size()
	book := b.registry.GetOrCreate(d.Ticker)
	if b.registry.Size() != sizeBefore {
		b.notifySize() // new market entered registry
	}
	pre := book.TopN(d.Side, 2)
	book.Apply(d.Side, d.PriceUnits, d.SizeDelta)
	post := book.TopN(d.Side, 2)
	if !sameTop(pre, post) {
		b.markDirty(d.Ticker, d.Side)
	}
}

func (b *Bookkeeper) handleSnapshot(msg SnapshotMsg) {
	b.registry.ResetMarket(msg.Ticker)
	book := b.registry.GetOrCreate(msg.Ticker)
	for _, lv := range msg.Yes {
		if lv.Size > 0 {
			book.Apply(SideYes, lv.PriceUnits, lv.Size)
		}
	}
	for _, lv := range msg.No {
		if lv.Size > 0 {
			book.Apply(SideNo, lv.PriceUnits, lv.Size)
		}
	}
	delete(b.dirty, msg.Ticker)
	if len(msg.Yes) > 0 {
		b.markDirty(msg.Ticker, SideYes)
	}
	if len(msg.No) > 0 {
		b.markDirty(msg.Ticker, SideNo)
	}
	b.notifySize()
	if msg.Done != nil {
		close(msg.Done)
	}
}

func (b *Bookkeeper) handleDeleteMarket(ticker string) {
	b.registry.DeleteMarket(ticker)
	delete(b.dirty, ticker)
	b.notifySize()
}

func (b *Bookkeeper) markDirty(ticker string, side Side) {
	sides, ok := b.dirty[ticker]
	if !ok {
		sides = map[Side]struct{}{}
		b.dirty[ticker] = sides
	}
	sides[side] = struct{}{}
}

func (b *Bookkeeper) flushDirty(reason string) {
	if len(b.dirty) == 0 {
		return
	}
	for ticker, sides := range b.dirty {
		for side := range sides {
			book := b.registry.GetOrCreate(ticker)
			payload, ok := buildPayload(side, book, reason)
			if !ok {
				continue
			}
			// Best-effort per NFR-3: publish failures are counted via
			// PubsubPublishFailures in the cmd-layer callback; dirty is always
			// cleared so the heartbeat tick will re-emit correct state.
			_ = b.cfg.Publish(ticker, side, payload)
		}
	}
	b.dirty = map[string]map[Side]struct{}{}
}

func (b *Bookkeeper) flushHeartbeat() {
	for _, ticker := range b.registry.Markets() {
		book := b.registry.GetOrCreate(ticker)
		for _, side := range []Side{SideYes, SideNo} {
			payload, ok := buildPayload(side, book, EmitReasonHeartbeat)
			if !ok {
				continue
			}
			if err := b.cfg.Publish(ticker, side, payload); err != nil {
				_ = fmt.Errorf("bookkeeper heartbeat publish: %w", err)
			}
		}
	}
}

// buildPayload constructs a SnapshotPayload from the current top-2 of (book, side).
// Returns false if the side has no levels (nothing to publish). market_ticker and ts
// are dropped from the inner payload (carried on the outer envelope, not here).
func buildPayload(side Side, book *Book, reason string) (SnapshotPayload, bool) {
	top := book.TopN(side, 2)
	if len(top) == 0 {
		return SnapshotPayload{}, false
	}
	top1Price := top[0]
	top1Size := book.SizeAt(side, top[0])
	var top2Price, top2Size int64
	hasTop2 := len(top) >= 2
	if hasTop2 {
		top2Price = top[1]
		top2Size = book.SizeAt(side, top[1])
	}
	return NewSnapshotPayload(side.String(), top1Price, top1Size, top2Price, top2Size, hasTop2, reason), true
}

// sameTop returns true if two top-N price slices are element-wise equal.
func sameTop(a, b []int64) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
