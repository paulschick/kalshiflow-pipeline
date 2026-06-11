package orderbook

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	kob "github.com/paulschick/kalshiflow-pipeline/internal/orderbook"
)

type emitRecord struct {
	Ticker string
	Side   Side
	Reason string
}

type stubPub struct {
	mu   sync.Mutex
	emit []emitRecord
}

func (s *stubPub) publish(t *testing.T) func(string, Side, kob.SnapshotPayload) error {
	t.Helper()
	return func(ticker string, side Side, p kob.SnapshotPayload) error {
		if _, err := p.Marshal(); err != nil {
			return err
		}
		s.mu.Lock()
		s.emit = append(s.emit, emitRecord{Ticker: ticker, Side: side, Reason: p.EmitReason()})
		s.mu.Unlock()
		return nil
	}
}

func (s *stubPub) snapshot() []emitRecord {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]emitRecord, len(s.emit))
	copy(out, s.emit)
	return out
}

func TestBookkeeper_ApplySnapshot_PopulatesBookAndEmitsOnChangeTick(t *testing.T) {
	pub := &stubPub{}
	bk := New(Config{
		Publish:            pub.publish(t),
		ChangeTickInterval: 25 * time.Millisecond,
		HeartbeatInterval:  10 * time.Second,
		PairScale:          map[string]PairScale{"BTC/USD": {PairDecimals: 1, LotDecimals: 8}},
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go bk.Run(ctx)

	done := make(chan struct{})
	bk.ApplySnapshot(SnapshotMsg{
		Symbol: "BTC/USD",
		Bids:   []WireLevel{{Price: json.Number("81011.5"), Qty: json.Number("0.00123440")}, {Price: json.Number("81010.0"), Qty: json.Number("0.5")}},
		Asks:   []WireLevel{{Price: json.Number("81020.0"), Qty: json.Number("1.0")}},
		Done:   done,
	})
	<-done

	waitFor(t, time.Second, "both sides emit on first change-tick", func() bool {
		emits := pub.snapshot()
		var bid, ask bool
		for _, e := range emits {
			if e.Ticker == "BTC/USD" && e.Side == SideBid {
				bid = true
			}
			if e.Ticker == "BTC/USD" && e.Side == SideAsk {
				ask = true
			}
		}
		return bid && ask
	})
}

func TestBookkeeper_ApplySnapshot_EmptyBookSidesProduceNoEmit(t *testing.T) {
	pub := &stubPub{}
	bk := New(Config{
		Publish:            pub.publish(t),
		ChangeTickInterval: 20 * time.Millisecond,
		HeartbeatInterval:  100 * time.Millisecond,
		PairScale:          map[string]PairScale{"BTC/USD": {PairDecimals: 1, LotDecimals: 8}},
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go bk.Run(ctx)

	done := make(chan struct{})
	bk.ApplySnapshot(SnapshotMsg{Symbol: "BTC/USD", Done: done})
	<-done
	time.Sleep(200 * time.Millisecond)
	for _, e := range pub.snapshot() {
		if e.Ticker == "BTC/USD" {
			t.Errorf("unexpected emit for empty book: %+v", e)
		}
	}
}

func TestBookkeeper_ApplyUpdate_AbsoluteSemantics(t *testing.T) {
	pub := &stubPub{}
	bk := New(Config{
		Publish:            pub.publish(t),
		ChangeTickInterval: 24 * time.Hour,
		HeartbeatInterval:  24 * time.Hour,
		PairScale:          map[string]PairScale{"BTC/USD": {PairDecimals: 1, LotDecimals: 8}},
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go bk.Run(ctx)

	d1 := make(chan struct{})
	bk.ApplySnapshot(SnapshotMsg{
		Symbol: "BTC/USD",
		Bids: []WireLevel{
			{Price: json.Number("100.0"), Qty: json.Number("5.00000000")},
			{Price: json.Number("99.0"), Qty: json.Number("3.00000000")},
		},
		Done: d1,
	})
	<-d1

	d2 := make(chan struct{})
	bk.ApplyUpdate(UpdateMsg{
		Symbol: "BTC/USD",
		Bids: []WireLevel{
			{Price: json.Number("100.0"), Qty: json.Number("8.00000000")},
			{Price: json.Number("99.0"), Qty: json.Number("0")},
		},
		Done: d2,
	})
	<-d2

	levels := bk.PeekBook("BTC/USD", SideBid)
	if len(levels) != 1 {
		t.Fatalf("after update, len(top-N) = %d; want 1 (99.0 deleted)", len(levels))
	}
	if levels[0].PriceUnits != 1000 || levels[0].Size != 800000000 {
		t.Errorf("top1 = %+v; want price=1000 size=800000000", levels[0])
	}
}

func TestBookkeeper_HandleUpdate_ScaleErr_LogsAndDoesNotMarkDirty(t *testing.T) {
	// Capture WARN logs via a custom default slog handler.
	var buf bytes.Buffer
	h := slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn})
	prev := slog.Default()
	slog.SetDefault(slog.New(h))
	t.Cleanup(func() { slog.SetDefault(prev) })

	pub := &stubPub{}
	bk := New(Config{
		Publish:            pub.publish(t),
		ChangeTickInterval: 24 * time.Hour, // suppress change-tick emit
		HeartbeatInterval:  24 * time.Hour,
		PairScale:          map[string]PairScale{"BTC/USD": {PairDecimals: 1, LotDecimals: 8}},
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go bk.Run(ctx)

	// Seed: clean snapshot so the book exists.
	d1 := make(chan struct{})
	bk.ApplySnapshot(SnapshotMsg{
		Symbol: "BTC/USD",
		Bids:   []WireLevel{{Price: json.Number("100.0"), Qty: json.Number("1.00000000")}},
		Asks:   []WireLevel{{Price: json.Number("200.0"), Qty: json.Number("1.00000000")}},
		Done:   d1,
	})
	<-d1

	// Trigger scale-err on the bid side: price "100.12" w/ PairDecimals=1
	// returns ErrPrecisionOverflow. Ask side is well-formed and parses.
	d2 := make(chan struct{})
	bk.ApplyUpdate(UpdateMsg{
		Symbol: "BTC/USD",
		Bids:   []WireLevel{{Price: json.Number("100.12"), Qty: json.Number("2.00000000")}},
		Asks:   []WireLevel{{Price: json.Number("201.0"), Qty: json.Number("3.00000000")}},
		Done:   d2,
	})
	<-d2

	logs := buf.String()
	if !strings.Contains(logs, "kraken_handleupdate_scale_err") {
		t.Fatalf("expected kraken_handleupdate_scale_err WARN; got logs=%q", logs)
	}
	if !strings.Contains(logs, `"symbol":"BTC/USD"`) {
		t.Fatalf("WARN missing symbol field: %q", logs)
	}
	if !strings.Contains(logs, `"side":"bid"`) {
		t.Fatalf("WARN missing side=bid field: %q", logs)
	}
	if !strings.Contains(logs, `"wire_price":"100.12"`) {
		t.Fatalf("WARN missing wire_price=100.12: %q", logs)
	}

	// Bid stays at the seeded 1.0 qty (scale-err prevented the update). Ask
	// has been updated to the new 201.0 / 3.0 level.
	bidLevels := bk.PeekBook("BTC/USD", SideBid)
	if len(bidLevels) != 1 || bidLevels[0].PriceUnits != 1000 || bidLevels[0].Size != 100000000 {
		t.Errorf("bid side mutated despite scale-err; got %+v", bidLevels)
	}
}

func TestBookkeeper_UpsertPairScale_AdditiveDoesNotDropExisting(t *testing.T) {
	pub := &stubPub{}
	bk := New(Config{
		Publish:            pub.publish(t),
		ChangeTickInterval: 24 * time.Hour,
		HeartbeatInterval:  24 * time.Hour,
		PairScale:          map[string]PairScale{"BTC/USD": {PairDecimals: 1, LotDecimals: 8}},
	})

	bk.UpsertPairScale("LTC/USD", PairScale{PairDecimals: 2, LotDecimals: 8})

	bk.mu.Lock()
	btc, ok1 := bk.cfg.PairScale["BTC/USD"]
	ltc, ok2 := bk.cfg.PairScale["LTC/USD"]
	bk.mu.Unlock()
	if !ok1 || btc.PairDecimals != 1 {
		t.Errorf("UpsertPairScale dropped existing BTC/USD entry; got ok=%v scale=%+v", ok1, btc)
	}
	if !ok2 || ltc.PairDecimals != 2 || ltc.LotDecimals != 8 {
		t.Errorf("UpsertPairScale failed to add LTC/USD; got ok=%v scale=%+v", ok2, ltc)
	}
}

func TestBookkeeper_UpsertPairScale_ReplacesExisting(t *testing.T) {
	bk := New(Config{
		Publish:            func(string, Side, kob.SnapshotPayload) error { return nil },
		ChangeTickInterval: 24 * time.Hour,
		HeartbeatInterval:  24 * time.Hour,
		PairScale:          map[string]PairScale{"BTC/USD": {PairDecimals: 1, LotDecimals: 8}},
	})

	bk.UpsertPairScale("BTC/USD", PairScale{PairDecimals: 2, LotDecimals: 7})

	bk.mu.Lock()
	got := bk.cfg.PairScale["BTC/USD"]
	bk.mu.Unlock()
	if got.PairDecimals != 2 || got.LotDecimals != 7 {
		t.Errorf("UpsertPairScale did not replace existing entry; got %+v", got)
	}
}

func TestBookkeeper_UpsertPairScale_OnNilMap(t *testing.T) {
	bk := New(Config{
		Publish:            func(string, Side, kob.SnapshotPayload) error { return nil },
		ChangeTickInterval: 24 * time.Hour,
		HeartbeatInterval:  24 * time.Hour,
		// PairScale: nil — boot path may legitimately start with nil before
		// the preflight populates it.
	})

	bk.UpsertPairScale("BTC/USD", PairScale{PairDecimals: 1, LotDecimals: 8})

	bk.mu.Lock()
	got, ok := bk.cfg.PairScale["BTC/USD"]
	bk.mu.Unlock()
	if !ok || got.PairDecimals != 1 {
		t.Errorf("UpsertPairScale on nil map failed; got ok=%v scale=%+v", ok, got)
	}
}

func TestBookkeeper_SetPairScale_StillReplacesWholesale(t *testing.T) {
	bk := New(Config{
		Publish:            func(string, Side, kob.SnapshotPayload) error { return nil },
		ChangeTickInterval: 24 * time.Hour,
		HeartbeatInterval:  24 * time.Hour,
		PairScale:          map[string]PairScale{"BTC/USD": {PairDecimals: 1, LotDecimals: 8}},
	})

	bk.SetPairScale(map[string]PairScale{"ETH/USD": {PairDecimals: 2, LotDecimals: 8}})

	bk.mu.Lock()
	_, hasBTC := bk.cfg.PairScale["BTC/USD"]
	_, hasETH := bk.cfg.PairScale["ETH/USD"]
	bk.mu.Unlock()
	if hasBTC {
		t.Error("SetPairScale boot path must replace wholesale; BTC/USD should be gone")
	}
	if !hasETH {
		t.Error("SetPairScale boot path failed to install ETH/USD")
	}
}

func TestBookkeeper_DeleteSymbol_RemovesAllState(t *testing.T) {
	pub := &stubPub{}
	bk := New(Config{
		Publish:            pub.publish(t),
		ChangeTickInterval: 24 * time.Hour, // suppress change-tick emit
		HeartbeatInterval:  24 * time.Hour, // suppress heartbeat
		PairScale: map[string]PairScale{
			"BTC/USD": {PairDecimals: 1, LotDecimals: 8},
			"LTC/USD": {PairDecimals: 2, LotDecimals: 8},
		},
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go bk.Run(ctx)

	// Seed both books.
	for _, sym := range []string{"BTC/USD", "LTC/USD"} {
		d := make(chan struct{})
		bk.ApplySnapshot(SnapshotMsg{
			Symbol: sym,
			Bids:   []WireLevel{{Price: json.Number("100.0"), Qty: json.Number("1.00000000")}},
			Asks:   []WireLevel{{Price: json.Number("200.0"), Qty: json.Number("1.00000000")}},
			Done:   d,
		})
		<-d
	}

	bk.DeleteSymbol("LTC/USD")

	bk.mu.Lock()
	_, hasScale := bk.cfg.PairScale["LTC/USD"]
	_, hasWire := bk.topWire["LTC/USD"]
	_, hasDirty := bk.dirty["LTC/USD"]
	_, hasSeq := bk.snapSeq["LTC/USD"]
	syms := bk.registry.Markets()
	bk.mu.Unlock()
	if hasScale {
		t.Error("DeleteSymbol left PairScale entry")
	}
	if hasWire {
		t.Error("DeleteSymbol left topWire entry")
	}
	if hasDirty {
		t.Error("DeleteSymbol left dirty entry")
	}
	if hasSeq {
		t.Error("DeleteSymbol left snapSeq entry")
	}
	for _, s := range syms {
		if s == "LTC/USD" {
			t.Error("DeleteSymbol left registry entry")
		}
	}

	// Heartbeat-via-flushAll should now emit only for BTC/USD if invoked.
	bk.flushAll(kob.EmitReasonHeartbeat)
	for _, r := range pub.snapshot() {
		if r.Ticker == "LTC/USD" {
			t.Errorf("LTC/USD still emits after DeleteSymbol: %+v", r)
		}
	}
}

func TestBookkeeper_DeleteSymbol_Idempotent(_ *testing.T) {
	bk := New(Config{
		Publish:            func(string, Side, kob.SnapshotPayload) error { return nil },
		ChangeTickInterval: 24 * time.Hour,
		HeartbeatInterval:  24 * time.Hour,
		PairScale:          map[string]PairScale{"BTC/USD": {PairDecimals: 1, LotDecimals: 8}},
	})
	bk.DeleteSymbol("UNKNOWN/USD") // never seeded
	bk.DeleteSymbol("UNKNOWN/USD") // again
	// No panic == pass.
}

func waitFor(t *testing.T, timeout time.Duration, msg string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("waitFor timeout: %s", msg)
}

// --- HOL-54 live-replay regression -----------------------------------------

type replayFrame struct {
	Type      string      `json:"type"`
	Bids      []WireLevel `json:"bids"`
	Asks      []WireLevel `json:"asks"`
	Checksum  uint32      `json:"checksum,omitempty"`
	Timestamp string      `json:"timestamp,omitempty"`
}

type replayFixture struct {
	Pair         string        `json:"pair"`
	PairDecimals int           `json:"pair_decimals"`
	LotDecimals  int           `json:"lot_decimals"`
	Frames       []replayFrame `json:"frames"`
}

func loadKrakenReplayFixture(t *testing.T, path string) replayFixture {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read fixture %s: %v", path, err)
	}
	var fx replayFixture
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber() // preserve wire-literal price/qty digits
	if err := dec.Decode(&fx); err != nil {
		t.Fatalf("decode fixture %s: %v", path, err)
	}
	if fx.Pair == "" || len(fx.Frames) == 0 {
		t.Fatalf("fixture %s missing pair or frames", path)
	}
	return fx
}

// TestBookkeeper_LiveReplay_XRPUSD_AllChecksumsMatch is the binding contract for the
// bookkeeper state machine: every update frame from a real XRP/USD `book depth=10`
// capture must verify-checksum cleanly after apply. Pre-HOL-54 fix this fails on the
// first net-positive update frame (typically frame #2 or #3) because the unbounded
// local book retains levels Kraken has silently popped from its own depth-10 view.
func TestBookkeeper_LiveReplay_XRPUSD_AllChecksumsMatch(t *testing.T) {
	fx := loadKrakenReplayFixture(t, "testdata/xrp-replay-2026-05-16.json")

	noopPub := func(string, Side, kob.SnapshotPayload) error { return nil }
	bk := New(Config{
		Publish:            noopPub,
		ChangeTickInterval: 24 * time.Hour, // suppress emit loop
		HeartbeatInterval:  24 * time.Hour,
		PairScale: map[string]PairScale{
			fx.Pair: {PairDecimals: fx.PairDecimals, LotDecimals: fx.LotDecimals},
		},
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go bk.Run(ctx)

	for i, fr := range fx.Frames {
		done := make(chan struct{})
		switch fr.Type {
		case "snapshot":
			bk.ApplySnapshot(SnapshotMsg{
				Symbol: fx.Pair, Bids: fr.Bids, Asks: fr.Asks, Done: done,
			})
		case "update":
			bk.ApplyUpdate(UpdateMsg{
				Symbol: fx.Pair, Bids: fr.Bids, Asks: fr.Asks, Done: done,
			})
		default:
			t.Fatalf("frame %d: unexpected type %q", i, fr.Type)
		}
		<-done
		if fr.Type != "update" {
			continue
		}
		matched, computed := bk.VerifyChecksumForSymbol(fx.Pair, fr.Checksum)
		if !matched {
			t.Fatalf("frame %d (ts=%s): CRC mismatch expected=%08x computed=%08x",
				i, fr.Timestamp, fr.Checksum, computed)
		}
	}
}
