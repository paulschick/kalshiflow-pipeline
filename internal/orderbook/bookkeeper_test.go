// internal/orderbook/bookkeeper_test.go
package orderbook

import (
	"context"
	"sync"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/paulschick/kalshiflow-pipeline/internal/orderbook/orderbookpb"
)

type capturedEmit struct {
	ticker  string
	side    Side
	payload SnapshotPayload
}

type stubPublisher struct {
	mu       sync.Mutex
	captured []capturedEmit
}

func (s *stubPublisher) publish(ticker string, side Side, p SnapshotPayload) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.captured = append(s.captured, capturedEmit{ticker: ticker, side: side, payload: p})
	return nil
}

func (s *stubPublisher) snapshot() []capturedEmit {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]capturedEmit, len(s.captured))
	copy(out, s.captured)
	return out
}

// decode unmarshals the emit's payload bytes back to the proto message for assertions.
func decode(t *testing.T, c capturedEmit) *orderbookpb.SnapshotPayload {
	t.Helper()
	body, err := c.payload.Marshal()
	if err != nil {
		t.Fatalf("payload.Marshal: %v", err)
	}
	var pb orderbookpb.SnapshotPayload
	if err := proto.Unmarshal(body, &pb); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	return &pb
}

func waitFor(t *testing.T, d time.Duration, msg string, pred func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if pred() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("waitFor timed out: %s", msg)
}

func TestBookkeeper_EmitsOnTopChange(t *testing.T) {
	t.Parallel()
	pub := &stubPublisher{}
	now := time.Now
	b := NewBookkeeper(BookkeeperConfig{
		Publish:            pub.publish,
		ChangeTickInterval: 50 * time.Millisecond,
		HeartbeatInterval:  10 * time.Second, // no heartbeat in this test
		Now:                now,
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go b.Run(ctx)

	// First delta on empty book → top-1 created → dirty
	b.Apply(DeltaMsg{Ticker: "KXBTCD-A", Side: SideYes, PriceUnits: 7700, SizeDelta: 100})

	waitFor(t, time.Second, "first emit lands", func() bool {
		return len(pub.snapshot()) >= 1
	})
	got := pub.snapshot()[0]
	if got.ticker != "KXBTCD-A" || got.side != SideYes {
		t.Errorf("captured (ticker, side) = (%q, %v)", got.ticker, got.side)
	}
	pb := decode(t, got)
	if pb.EmitReason != EmitReasonChange {
		t.Errorf("emit_reason = %q; want change", pb.EmitReason)
	}
	if pb.Top1PriceUnits != 7700 || pb.Top1Size != 100 {
		t.Errorf("top1 = (%d, %d); want (7700, 100)", pb.Top1PriceUnits, pb.Top1Size)
	}
	if pb.Top2PriceUnits != nil {
		t.Errorf("top2_price_units = %v; want nil", pb.Top2PriceUnits)
	}
}

func TestBookkeeper_DropsDeltaThatDoesNotMoveTopN(t *testing.T) {
	t.Parallel()
	pub := &stubPublisher{}
	b := NewBookkeeper(BookkeeperConfig{
		Publish:            pub.publish,
		ChangeTickInterval: 50 * time.Millisecond,
		HeartbeatInterval:  10 * time.Second,
		Now:                time.Now,
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go b.Run(ctx)

	b.Apply(DeltaMsg{Ticker: "KXBTCD-A", Side: SideYes, PriceUnits: 7700, SizeDelta: 100})
	b.Apply(DeltaMsg{Ticker: "KXBTCD-A", Side: SideYes, PriceUnits: 7600, SizeDelta: 100})
	// Per spec FR-2 (per-tick aggregation): both deltas to the same (ticker, side)
	// within one tick window collapse to one emit. Wait for >= 1, then let any
	// pending tick settle so pre is stable before the negative assertion.
	waitFor(t, time.Second, "first emit", func() bool {
		return len(pub.snapshot()) >= 1
	})
	time.Sleep(100 * time.Millisecond)
	pre := len(pub.snapshot())

	// Deeper level — top-2 unchanged (still 7700, 7600) → no emit on next tick
	b.Apply(DeltaMsg{Ticker: "KXBTCD-A", Side: SideYes, PriceUnits: 7500, SizeDelta: 100})
	time.Sleep(150 * time.Millisecond) // let 2-3 change ticks pass
	if got := len(pub.snapshot()); got != pre {
		t.Errorf("deep delta unexpectedly emitted: count went %d -> %d", pre, got)
	}
}

func TestBookkeeper_HeartbeatEmitsAllTrackedMarkets(t *testing.T) {
	t.Parallel()
	pub := &stubPublisher{}
	b := NewBookkeeper(BookkeeperConfig{
		Publish:            pub.publish,
		ChangeTickInterval: 1 * time.Second, // no change ticks during the test window
		HeartbeatInterval:  100 * time.Millisecond,
		Now:                time.Now,
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go b.Run(ctx)

	// Seed two markets with one level each (these fire change emits via the 1s tick;
	// we don't care about those — just ensure registry has the markets).
	b.Apply(DeltaMsg{Ticker: "A", Side: SideYes, PriceUnits: 5000, SizeDelta: 100})
	b.Apply(DeltaMsg{Ticker: "B", Side: SideNo, PriceUnits: 6000, SizeDelta: 200})

	// Wait for at least one heartbeat round.
	waitFor(t, time.Second, "heartbeat round", func() bool {
		for _, c := range pub.snapshot() {
			if c.payload.EmitReason() == EmitReasonHeartbeat {
				return true
			}
		}
		return false
	})
	var sawA, sawB bool
	for _, c := range pub.snapshot() {
		if c.payload.EmitReason() != EmitReasonHeartbeat {
			continue
		}
		if c.ticker == "A" && c.side == SideYes {
			sawA = true
		}
		if c.ticker == "B" && c.side == SideNo {
			sawB = true
		}
	}
	if !sawA || !sawB {
		t.Errorf("heartbeat coverage: sawA=%v sawB=%v", sawA, sawB)
	}
}

func TestBookkeeper_ResetClearsAll(t *testing.T) {
	t.Parallel()
	pub := &stubPublisher{}
	b := NewBookkeeper(BookkeeperConfig{
		Publish:            pub.publish,
		ChangeTickInterval: 50 * time.Millisecond,
		HeartbeatInterval:  10 * time.Second,
		Now:                time.Now,
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go b.Run(ctx)

	b.Apply(DeltaMsg{Ticker: "A", Side: SideYes, PriceUnits: 5000, SizeDelta: 100})
	b.Apply(DeltaMsg{Ticker: "B", Side: SideNo, PriceUnits: 6000, SizeDelta: 200})
	waitFor(t, time.Second, "two emits", func() bool { return len(pub.snapshot()) >= 2 })

	b.Reset()

	// After full reset, registry is empty. New deltas create fresh top-1 books.
	pre := len(pub.snapshot())
	b.Apply(DeltaMsg{Ticker: "A", Side: SideYes, PriceUnits: 5000, SizeDelta: 99})
	waitFor(t, time.Second, "post-reset emit", func() bool { return len(pub.snapshot()) > pre })
	last := decode(t, pub.snapshot()[len(pub.snapshot())-1])
	if last.Top1Size != 99 {
		t.Errorf("post-reset size = %d; want 99 (fresh book)", last.Top1Size)
	}
}

func TestBookkeeper_RetainOnlyEvictsMissingMarkets(t *testing.T) {
	t.Parallel()
	pub := &stubPublisher{}
	b := NewBookkeeper(BookkeeperConfig{
		Publish:            pub.publish,
		ChangeTickInterval: 50 * time.Millisecond,
		HeartbeatInterval:  10 * time.Second,
		Now:                time.Now,
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go b.Run(ctx)

	b.Apply(DeltaMsg{Ticker: "A", Side: SideYes, PriceUnits: 5000, SizeDelta: 100})
	b.Apply(DeltaMsg{Ticker: "B", Side: SideNo, PriceUnits: 6000, SizeDelta: 200})
	b.Apply(DeltaMsg{Ticker: "C", Side: SideYes, PriceUnits: 7000, SizeDelta: 300})
	waitFor(t, time.Second, "three emits", func() bool { return len(pub.snapshot()) >= 3 })

	// Keep only B; A and C must get ResetMarket'd.
	b.RetainOnly([]string{"B"})

	// Re-apply on A at the same price expecting fresh top-1 (book was cleared).
	pre := len(pub.snapshot())
	b.Apply(DeltaMsg{Ticker: "A", Side: SideYes, PriceUnits: 5000, SizeDelta: 99})
	waitFor(t, time.Second, "post-retain emit on A", func() bool { return len(pub.snapshot()) > pre })
	last := decode(t, pub.snapshot()[len(pub.snapshot())-1])
	if last.Top1Size != 99 {
		t.Errorf("post-retain top1 size for A = %d; want 99 (fresh book after RetainOnly evicted A)", last.Top1Size)
	}
}

func TestBookkeeper_RetainOnlyKeepsListedMarkets(t *testing.T) {
	t.Parallel()
	pub := &stubPublisher{}
	b := NewBookkeeper(BookkeeperConfig{
		Publish:            pub.publish,
		ChangeTickInterval: 50 * time.Millisecond,
		HeartbeatInterval:  10 * time.Second,
		Now:                time.Now,
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go b.Run(ctx)

	b.Apply(DeltaMsg{Ticker: "A", Side: SideYes, PriceUnits: 5000, SizeDelta: 100})
	waitFor(t, time.Second, "first emit", func() bool { return len(pub.snapshot()) >= 1 })

	// RetainOnly with A in keep set must NOT clear A's book.
	b.RetainOnly([]string{"A"})

	pre := len(pub.snapshot())
	b.Apply(DeltaMsg{Ticker: "A", Side: SideYes, PriceUnits: 5100, SizeDelta: 50})
	waitFor(t, time.Second, "post-retain emit", func() bool { return len(pub.snapshot()) > pre })
	last := decode(t, pub.snapshot()[len(pub.snapshot())-1])
	// Book preserved → 5100 is now top, 5000 still present at depth-2.
	if last.Top1PriceUnits != 5100 {
		t.Errorf("post-retain top1 price for A = %d; want 5100 (book preserved when A in keep set)", last.Top1PriceUnits)
	}
}

func TestBookkeeper_HeartbeatFiresOver300s(t *testing.T) {
	t.Parallel()
	// The load-bearing property: with Bookkeeper.Run alive past the
	// heartbeat interval, heartbeat fires for every tracked (market, side)
	// pair. Heartbeat interval is shrunk to 100ms for fast test runtime;
	// the same timer behavior is what makes the production 300s heartbeat
	// fire when Run outlives a sub-300s reconnect storm.
	pub := &stubPublisher{}
	b := NewBookkeeper(BookkeeperConfig{
		Publish:            pub.publish,
		ChangeTickInterval: 1 * time.Second,
		HeartbeatInterval:  100 * time.Millisecond,
		Now:                time.Now,
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go b.Run(ctx)

	b.Apply(DeltaMsg{Ticker: "A", Side: SideYes, PriceUnits: 5000, SizeDelta: 100})
	b.Apply(DeltaMsg{Ticker: "B", Side: SideNo, PriceUnits: 6000, SizeDelta: 200})

	waitFor(t, time.Second, "heartbeat fired for both", func() bool {
		var sawA, sawB bool
		for _, c := range pub.snapshot() {
			if c.payload.EmitReason() != EmitReasonHeartbeat {
				continue
			}
			if c.ticker == "A" && c.side == SideYes {
				sawA = true
			}
			if c.ticker == "B" && c.side == SideNo {
				sawB = true
			}
		}
		return sawA && sawB
	})
}

func TestBookkeeper_DeleteMarketRemovesFromRegistry(t *testing.T) {
	t.Parallel()
	pub := &stubPublisher{}

	var sizeMu sync.Mutex
	var lastSize int
	b := NewBookkeeper(BookkeeperConfig{
		Publish:            pub.publish,
		ChangeTickInterval: 50 * time.Millisecond,
		HeartbeatInterval:  10 * time.Second,
		Now:                time.Now,
		OnRegistrySize: func(n int) {
			sizeMu.Lock()
			lastSize = n
			sizeMu.Unlock()
		},
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go b.Run(ctx)

	// Seed two markets.
	b.Apply(DeltaMsg{Ticker: "A", Side: SideYes, PriceUnits: 5000, SizeDelta: 100})
	b.Apply(DeltaMsg{Ticker: "B", Side: SideNo, PriceUnits: 6000, SizeDelta: 200})
	waitFor(t, time.Second, "registry size hits 2", func() bool {
		sizeMu.Lock()
		defer sizeMu.Unlock()
		return lastSize == 2
	})

	// Evict A.
	b.DeleteMarket("A")

	waitFor(t, time.Second, "registry size drops to 1", func() bool {
		sizeMu.Lock()
		defer sizeMu.Unlock()
		return lastSize == 1
	})
}

// TestBookkeeper_ApplySnapshotPopulatesBookAndMarksDirty asserts that an
// orderbook_snapshot is decoded INTO the book (HOL-52 Bug B fix). Pre-fix
// behavior wiped the book via ResetMarket and discarded the snapshot's
// levels. Post-fix the snapshot levels populate the book and the next
// change-tick emits the populated top-2.
func TestBookkeeper_ApplySnapshotPopulatesBookAndMarksDirty(t *testing.T) {
	t.Parallel()
	pub := &stubPublisher{}
	b := NewBookkeeper(BookkeeperConfig{
		Publish:            pub.publish,
		ChangeTickInterval: 25 * time.Millisecond,
		HeartbeatInterval:  10 * time.Second,
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go b.Run(ctx)

	done := make(chan struct{})
	b.ApplySnapshot(SnapshotMsg{
		Ticker: "KXBTCD-A",
		Yes: []SnapshotLevel{
			{PriceUnits: 7700, Size: 100},
			{PriceUnits: 7600, Size: 80},
		},
		No:   []SnapshotLevel{{PriceUnits: 2300, Size: 50}},
		Done: done,
	})
	<-done

	waitFor(t, time.Second, "snapshot populates both sides", func() bool {
		emits := pub.snapshot()
		var yes, no bool
		for _, e := range emits {
			if e.ticker != "KXBTCD-A" {
				continue
			}
			if e.side == SideYes {
				yes = true
			}
			if e.side == SideNo {
				no = true
			}
		}
		return yes && no
	})

	emits := pub.snapshot()
	for _, e := range emits {
		if e.ticker != "KXBTCD-A" || e.side != SideYes {
			continue
		}
		pb := decode(t, e)
		if pb.Top1PriceUnits != 7700 || pb.Top1Size != 100 {
			t.Errorf("yes top1 = (%d, %d); want (7700, 100)", pb.Top1PriceUnits, pb.Top1Size)
		}
		if pb.Top2PriceUnits == nil || *pb.Top2PriceUnits != 7600 || pb.Top2Size == nil || *pb.Top2Size != 80 {
			t.Errorf("yes top2 = (%v, %v); want (7600, 80)", pb.Top2PriceUnits, pb.Top2Size)
		}
		break
	}

	// Re-apply an empty snapshot — the book becomes empty and the next
	// heartbeat tick should NOT emit for that ticker (buildPayload ok=false).
	done2 := make(chan struct{})
	b.ApplySnapshot(SnapshotMsg{Ticker: "KXBTCD-A", Done: done2})
	<-done2

	preCount := len(pub.snapshot())
	// One change-tick window is enough — no dirty marker → no emit.
	time.Sleep(75 * time.Millisecond)
	post := pub.snapshot()
	for _, e := range post[preCount:] {
		if e.ticker == "KXBTCD-A" {
			t.Errorf("empty snapshot produced emit %+v; want no emit (empty book)", e)
		}
	}
}

func TestBookkeeper_RetainOnlyDeletesMissingMarkets(t *testing.T) {
	t.Parallel()
	pub := &stubPublisher{}

	var sizeMu sync.Mutex
	var lastSize int
	b := NewBookkeeper(BookkeeperConfig{
		Publish:            pub.publish,
		ChangeTickInterval: 50 * time.Millisecond,
		HeartbeatInterval:  10 * time.Second,
		Now:                time.Now,
		OnRegistrySize: func(n int) {
			sizeMu.Lock()
			lastSize = n
			sizeMu.Unlock()
		},
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go b.Run(ctx)

	b.Apply(DeltaMsg{Ticker: "A", Side: SideYes, PriceUnits: 5000, SizeDelta: 100})
	b.Apply(DeltaMsg{Ticker: "B", Side: SideNo, PriceUnits: 6000, SizeDelta: 200})
	b.Apply(DeltaMsg{Ticker: "C", Side: SideYes, PriceUnits: 7000, SizeDelta: 300})
	waitFor(t, time.Second, "registry size hits 3", func() bool {
		sizeMu.Lock()
		defer sizeMu.Unlock()
		return lastSize == 3
	})

	// Keep only B; A and C must be DELETED, not just zeroed.
	b.RetainOnly([]string{"B"})

	waitFor(t, time.Second, "registry size drops to 1", func() bool {
		sizeMu.Lock()
		defer sizeMu.Unlock()
		return lastSize == 1
	})
}
