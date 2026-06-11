package orderbook

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	kob "github.com/paulschick/kalshiflow-pipeline/internal/orderbook"
)

// KrakenBookDepth is the WS v2 book channel depth this worker subscribes at and the
// hard cap on per-side levels held in the local book. The Kraken book channel emits
// (adds, qty>0) and (deletes, qty=0) only; levels naturally popped off the bottom of
// the depth-N window get no explicit delete frame. The local book must trim to this
// depth after every apply path or the top-N view drifts from Kraken's and CRC32
// verification fails on every subsequent frame.
const KrakenBookDepth = 10

// PairScale carries pair_decimals / lot_decimals for one Kraken pair.
type PairScale struct {
	PairDecimals int
	LotDecimals  int
}

// WireLevel is one (price, qty) entry on the WS wire — strings preserved as
// json.Number for precision-safe scaling and CRC32 input.
type WireLevel struct {
	Price json.Number `json:"price"`
	Qty   json.Number `json:"qty"`
}

// ScaleLevelError wraps a per-level scale failure with the offending wire
// literal so the bookkeeper's scale-error log path can attribute the failure.
type ScaleLevelError struct {
	Level WireLevel
	Cause error
}

func (e *ScaleLevelError) Error() string {
	return fmt.Sprintf("scale level (price=%s qty=%s): %v", e.Level.Price, e.Level.Qty, e.Cause)
}

func (e *ScaleLevelError) Unwrap() error { return e.Cause }

// SnapshotMsg is the input to ApplySnapshot. Done is closed by the bookkeeper
// after the book is repopulated.
type SnapshotMsg struct {
	Symbol string
	Bids   []WireLevel
	Asks   []WireLevel
	Done   chan struct{}
}

// UpdateMsg is the input to ApplyUpdate. One wire frame may carry updates on
// both sides; both slices may also be empty (= "no change this side").
type UpdateMsg struct {
	Symbol string
	Bids   []WireLevel
	Asks   []WireLevel
	Done   chan struct{}
}

// Config configures the Kraken-side Bookkeeper.
type Config struct {
	Publish            func(ticker string, side Side, p kob.SnapshotPayload) error
	ChangeTickInterval time.Duration
	HeartbeatInterval  time.Duration
	// PairScale is captured at boot from REST AssetPairs. Required.
	PairScale map[string]PairScale
}

// Bookkeeper holds Kraken-side per-(symbol,side) books and emits SnapshotPayloads
// on change-tick + heartbeat cadence.
type Bookkeeper struct {
	cfg       Config
	snapshots chan SnapshotMsg
	updates   chan UpdateMsg

	mu       sync.Mutex
	registry *kob.Registry
	dirty    map[string]map[Side]bool
	topWire  map[string]map[Side][]WireLevel
	snapSeq  map[string]int64
}

// New constructs a Bookkeeper.
func New(cfg Config) *Bookkeeper {
	if cfg.ChangeTickInterval == 0 {
		cfg.ChangeTickInterval = 1 * time.Second
	}
	if cfg.HeartbeatInterval == 0 {
		cfg.HeartbeatInterval = 5 * time.Minute
	}
	return &Bookkeeper{
		cfg:       cfg,
		snapshots: make(chan SnapshotMsg, 128),
		updates:   make(chan UpdateMsg, 1024),
		registry:  kob.NewRegistry(),
		dirty:     make(map[string]map[Side]bool),
		topWire:   make(map[string]map[Side][]WireLevel),
		snapSeq:   make(map[string]int64),
	}
}

// ApplySnapshot enqueues a snapshot. Blocks if the channel is full.
func (b *Bookkeeper) ApplySnapshot(msg SnapshotMsg) { b.snapshots <- msg }

// ApplyUpdate enqueues an update. Blocks if the channel is full.
func (b *Bookkeeper) ApplyUpdate(msg UpdateMsg) { b.updates <- msg }

// SnapshotSeq returns the current snapshot anchor for a symbol (test-only).
func (b *Bookkeeper) SnapshotSeq(symbol string) int64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.snapSeq[symbol]
}

// SetPairScale replaces the pair scale map. Safe to call before Run starts
// consuming snapshots/updates; concurrent callers serialize via the mutex.
// Use this on the boot preflight path (all-or-nothing replace). For dynamic
// AddPair after boot, use UpsertPairScale to avoid dropping scales for
// already-active pairs.
func (b *Bookkeeper) SetPairScale(scale map[string]PairScale) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.cfg.PairScale = scale
}

// UpsertPairScale adds or replaces the scale entry for one pair without
// dropping other entries. Use on the dynamic AddPair path after the per-pair
// AssetPairs preflight returns. Safe to call concurrently with Run.
func (b *Bookkeeper) UpsertPairScale(pair string, scale PairScale) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.cfg.PairScale == nil {
		b.cfg.PairScale = map[string]PairScale{}
	}
	b.cfg.PairScale[pair] = scale
}

// DeleteSymbol removes per-(symbol,side) bookkeeper state for the pair: it
// drops the registry entry (stops heartbeat emit via flushAll), the
// PairScale entry (so a stray late-arriving update is dropped silently in
// handleUpdate), the topWire/dirty/snapSeq state. Idempotent. Safe to call
// concurrently with Run. Use on the dynamic RemovePair path after the WS
// unsubscribe ack returns.
func (b *Bookkeeper) DeleteSymbol(pair string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.registry.DeleteMarket(pair)
	delete(b.cfg.PairScale, pair)
	delete(b.topWire, pair)
	delete(b.dirty, pair)
	delete(b.snapSeq, pair)
}

// VerifyChecksumForSymbol re-derives the CRC32 from the post-apply top-10
// wire-byte view for (symbol). Returns (matched, computed). Returns
// (true, 0) if no view exists yet (initial state — checksum applies only
// after an apply path runs).
func (b *Bookkeeper) VerifyChecksumForSymbol(symbol string, expected uint32) (bool, uint32) {
	b.mu.Lock()
	defer b.mu.Unlock()
	sides, ok := b.topWire[symbol]
	if !ok {
		return true, 0
	}
	return VerifyChecksum(sides[SideAsk], sides[SideBid], expected)
}

// TopWireForSymbol returns a defensive copy of the current top-10 wire view
// for (symbol). Returns nil slices if the view does not exist yet. Used by
// the worker's mismatch-debug log path to capture the byte sequence the
// verifier hashed against the wire-supplied checksum.
func (b *Bookkeeper) TopWireForSymbol(symbol string) (asks, bids []WireLevel) {
	b.mu.Lock()
	defer b.mu.Unlock()
	sides, ok := b.topWire[symbol]
	if !ok {
		return nil, nil
	}
	asks = append([]WireLevel(nil), sides[SideAsk]...)
	bids = append([]WireLevel(nil), sides[SideBid]...)
	return asks, bids
}

// Level is the (price, size) pair used by PeekBook + emit-path arithmetic.
type Level struct {
	PriceUnits int64
	Size       int64
}

// PeekBook returns the top-10 of (symbol, side) as Level pairs. Best-first
// ordering per side: bids descending, asks ascending. Test-only.
func (b *Bookkeeper) PeekBook(symbol string, side Side) []Level {
	b.mu.Lock()
	defer b.mu.Unlock()
	book := b.registry.GetOrCreate(symbol)
	ks := toKalshiSide(side)
	prices := topNForSide(book, ks, side, 10)
	out := make([]Level, 0, len(prices))
	for _, p := range prices {
		out = append(out, Level{PriceUnits: p, Size: book.SizeAt(ks, p)})
	}
	return out
}

// Run drives the bookkeeper goroutine. Blocks until ctx cancellation.
func (b *Bookkeeper) Run(ctx context.Context) {
	changeT := time.NewTicker(b.cfg.ChangeTickInterval)
	defer changeT.Stop()
	hbT := time.NewTicker(b.cfg.HeartbeatInterval)
	defer hbT.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case msg := <-b.snapshots:
			b.handleSnapshot(msg)
		case msg := <-b.updates:
			b.handleUpdate(msg)
		case <-changeT.C:
			b.flushDirty(kob.EmitReasonChange)
		case <-hbT.C:
			b.flushAll(kob.EmitReasonHeartbeat)
		}
	}
}

func (b *Bookkeeper) handleSnapshot(msg SnapshotMsg) {
	b.mu.Lock()
	b.registry.ResetMarket(msg.Symbol)
	b.snapSeq[msg.Symbol]++
	scale, ok := b.cfg.PairScale[msg.Symbol]
	if !ok {
		b.mu.Unlock()
		if msg.Done != nil {
			close(msg.Done)
		}
		return
	}
	book := b.registry.GetOrCreate(msg.Symbol)
	if err := applyLevels(book, toKalshiSide(SideBid), msg.Bids, scale); err == nil && len(msg.Bids) > 0 {
		b.markDirtyLocked(msg.Symbol, SideBid)
	}
	if err := applyLevels(book, toKalshiSide(SideAsk), msg.Asks, scale); err == nil && len(msg.Asks) > 0 {
		b.markDirtyLocked(msg.Symbol, SideAsk)
	}
	b.trimToDepthLocked(book, SideBid, KrakenBookDepth)
	b.trimToDepthLocked(book, SideAsk, KrakenBookDepth)
	b.setTopWireLocked(msg.Symbol, SideBid, msg.Bids)
	b.setTopWireLocked(msg.Symbol, SideAsk, msg.Asks)
	b.mu.Unlock()
	if msg.Done != nil {
		close(msg.Done)
	}
}

func (b *Bookkeeper) handleUpdate(msg UpdateMsg) {
	b.mu.Lock()
	scale, ok := b.cfg.PairScale[msg.Symbol]
	if !ok {
		b.mu.Unlock()
		if msg.Done != nil {
			close(msg.Done)
		}
		return
	}
	book := b.registry.GetOrCreate(msg.Symbol)
	if err := applyAbsoluteLevels(book, toKalshiSide(SideBid), msg.Bids, scale); err != nil {
		logScaleErr(msg.Symbol, SideBid, err)
	} else if len(msg.Bids) > 0 {
		b.markDirtyLocked(msg.Symbol, SideBid)
	}
	if err := applyAbsoluteLevels(book, toKalshiSide(SideAsk), msg.Asks, scale); err != nil {
		logScaleErr(msg.Symbol, SideAsk, err)
	} else if len(msg.Asks) > 0 {
		b.markDirtyLocked(msg.Symbol, SideAsk)
	}
	if len(msg.Bids) > 0 || len(msg.Asks) > 0 {
		b.trimToDepthLocked(book, SideBid, KrakenBookDepth)
		b.trimToDepthLocked(book, SideAsk, KrakenBookDepth)
		b.refreshTopWireLocked(msg.Symbol, book, scale)
	}
	b.mu.Unlock()
	if msg.Done != nil {
		close(msg.Done)
	}
}

func logScaleErr(symbol string, side Side, err error) {
	attrs := []any{
		"symbol", symbol,
		"side", side.String(),
		"err", err.Error(),
	}
	var sle *ScaleLevelError
	if errors.As(err, &sle) {
		attrs = append(attrs, "wire_price", sle.Level.Price.String(), "wire_qty", sle.Level.Qty.String())
	}
	slog.Warn("kraken_handleupdate_scale_err", attrs...)
}

func (b *Bookkeeper) markDirtyLocked(symbol string, side Side) {
	if b.dirty[symbol] == nil {
		b.dirty[symbol] = map[Side]bool{}
	}
	b.dirty[symbol][side] = true
}

func (b *Bookkeeper) flushDirty(reason string) {
	b.mu.Lock()
	type emit struct {
		sym  string
		side Side
	}
	var pending []emit
	for sym, sides := range b.dirty {
		for s := range sides {
			pending = append(pending, emit{sym, s})
		}
	}
	b.dirty = map[string]map[Side]bool{}
	b.mu.Unlock()
	for _, e := range pending {
		b.emit(e.sym, e.side, reason)
	}
}

func (b *Bookkeeper) flushAll(reason string) {
	b.mu.Lock()
	syms := b.registry.Markets()
	b.mu.Unlock()
	for _, sym := range syms {
		b.emit(sym, SideBid, reason)
		b.emit(sym, SideAsk, reason)
	}
}

func (b *Bookkeeper) emit(symbol string, side Side, reason string) {
	b.mu.Lock()
	book := b.registry.GetOrCreate(symbol)
	ks := toKalshiSide(side)
	prices := topNForSide(book, ks, side, 2)
	var top1P, top1S, top2P, top2S int64
	hasTop2 := false
	if len(prices) >= 1 {
		top1P = prices[0]
		top1S = book.SizeAt(ks, prices[0])
	}
	if len(prices) >= 2 {
		hasTop2 = true
		top2P = prices[1]
		top2S = book.SizeAt(ks, prices[1])
	}
	b.mu.Unlock()
	if len(prices) == 0 {
		return
	}
	payload := kob.NewSnapshotPayload(side.String(), top1P, top1S, top2P, top2S, hasTop2, reason)
	if err := b.cfg.Publish(symbol, side, payload); err != nil {
		slog.Warn("kraken_emit_publish_err", "symbol", symbol, "side", side.String(), "err", err)
	}
}

// topNForSide returns the top-n prices for (book, side) in the Kraken-correct
// order: bids descending (best=highest first), asks ascending (best=lowest
// first). Book.TopN sorts descending unconditionally; for the ask we pull all
// prices and reverse + truncate.
func topNForSide(book *kob.Book, ks kob.Side, side Side, n int) []int64 {
	if side == SideBid {
		return book.TopN(ks, n)
	}
	all := book.TopN(ks, 1<<30)
	for i, j := 0, len(all)-1; i < j; i, j = i+1, j-1 {
		all[i], all[j] = all[j], all[i]
	}
	if len(all) > n {
		all = all[:n]
	}
	return all
}

func applyLevels(book *kob.Book, side kob.Side, levels []WireLevel, scale PairScale) error {
	for _, lv := range levels {
		p, err := ScalePrice(lv.Price, scale.PairDecimals)
		if err != nil {
			return &ScaleLevelError{Level: lv, Cause: err}
		}
		s, err := ScaleSize(lv.Qty, scale.LotDecimals)
		if err != nil {
			return &ScaleLevelError{Level: lv, Cause: err}
		}
		if s > 0 {
			book.Apply(side, p, s)
		}
	}
	return nil
}

func applyAbsoluteLevels(book *kob.Book, side kob.Side, levels []WireLevel, scale PairScale) error {
	for _, lv := range levels {
		p, err := ScalePrice(lv.Price, scale.PairDecimals)
		if err != nil {
			return &ScaleLevelError{Level: lv, Cause: err}
		}
		s, err := ScaleSize(lv.Qty, scale.LotDecimals)
		if err != nil {
			return &ScaleLevelError{Level: lv, Cause: err}
		}
		cur := book.SizeAt(side, p)
		delta := s - cur
		if delta != 0 {
			book.Apply(side, p, delta)
		}
	}
	return nil
}

func (b *Bookkeeper) setTopWireLocked(symbol string, side Side, levels []WireLevel) {
	if b.topWire[symbol] == nil {
		b.topWire[symbol] = map[Side][]WireLevel{}
	}
	capped := levels
	if len(capped) > 10 {
		capped = capped[:10]
	}
	cp := make([]WireLevel, len(capped))
	copy(cp, capped)
	b.topWire[symbol][side] = cp
}

func (b *Bookkeeper) refreshTopWireLocked(symbol string, book *kob.Book, scale PairScale) {
	for _, side := range []Side{SideBid, SideAsk} {
		ks := toKalshiSide(side)
		prices := topNForSide(book, ks, side, 10)
		out := make([]WireLevel, 0, len(prices))
		for _, p := range prices {
			out = append(out, WireLevel{
				Price: int64ToFixedPoint(p, scale.PairDecimals),
				Qty:   int64ToFixedPoint(book.SizeAt(ks, p), scale.LotDecimals),
			})
		}
		if b.topWire[symbol] == nil {
			b.topWire[symbol] = map[Side][]WireLevel{}
		}
		b.topWire[symbol][side] = out
	}
}

// trimToDepthLocked drops every level except the best-n by Kraken-side price priority
// (best=highest for bids, best=lowest for asks) from the per-symbol kob.Book. Called
// under b.mu after each successful apply path. End-of-frame is equivalent to per-level
// trimming for Kraken's multi-level frames (every level in a frame writes the absolute
// qty for its price; final state depends only on the set of (price, qty) pairs) and
// cheaper at O(book_size) once per frame versus N times.
func (b *Bookkeeper) trimToDepthLocked(book *kob.Book, side Side, n int) {
	ks := toKalshiSide(side)
	keep := topNForSide(book, ks, side, n)
	keepSet := make(map[int64]struct{}, len(keep))
	for _, p := range keep {
		keepSet[p] = struct{}{}
	}
	for _, p := range book.TopN(ks, 1<<30) {
		if _, ok := keepSet[p]; ok {
			continue
		}
		if cur := book.SizeAt(ks, p); cur != 0 {
			book.Apply(ks, p, -cur)
		}
	}
}

func int64ToFixedPoint(units int64, decimals int) json.Number {
	s := fmt.Sprintf("%d", units)
	if decimals == 0 {
		return json.Number(s)
	}
	for len(s) <= decimals {
		s = "0" + s
	}
	return json.Number(s[:len(s)-decimals] + "." + s[len(s)-decimals:])
}

func toKalshiSide(s Side) kob.Side {
	switch s {
	case SideBid:
		return kob.SideYes
	case SideAsk:
		return kob.SideNo
	}
	return kob.Side(0)
}
