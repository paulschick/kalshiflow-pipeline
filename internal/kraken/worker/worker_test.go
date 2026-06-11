package worker

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"

	"github.com/paulschick/kalshiflow-pipeline/internal/featureflag"
	krob "github.com/paulschick/kalshiflow-pipeline/internal/kraken/orderbook"
	"github.com/paulschick/kalshiflow-pipeline/internal/kraken/rest"
	"github.com/paulschick/kalshiflow-pipeline/internal/kraken/wsv2"
	"github.com/paulschick/kalshiflow-pipeline/internal/metrics"
	kob "github.com/paulschick/kalshiflow-pipeline/internal/orderbook"
)

type fakeConn struct {
	mu             sync.Mutex
	subscribes     [][]string
	unsubscribes   [][]string
	reads          chan wsv2.Frame
	closed         atomic.Bool
	unsubscribeErr error
	subscribeErr   error
}

func newFakeConn() *fakeConn { return &fakeConn{reads: make(chan wsv2.Frame, 64)} }

func (f *fakeConn) Subscribe(_ context.Context, channel string, syms []string, _ wsv2.SubscribeOpts) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.subscribes = append(f.subscribes, append([]string{channel}, syms...))
	return f.subscribeErr
}

func (f *fakeConn) Unsubscribe(_ context.Context, channel string, syms []string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.unsubscribes = append(f.unsubscribes, append([]string{channel}, syms...))
	return f.unsubscribeErr
}

func (f *fakeConn) setUnsubscribeErr(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.unsubscribeErr = err
}

func (f *fakeConn) Read(ctx context.Context) (wsv2.Frame, error) {
	select {
	case fr := <-f.reads:
		return fr, nil
	case <-ctx.Done():
		return wsv2.Frame{}, ctx.Err()
	}
}

func (f *fakeConn) Close(websocket.StatusCode, string) error {
	f.closed.Store(true)
	return nil
}

func (f *fakeConn) snapshotSubscribes() [][]string {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([][]string, len(f.subscribes))
	copy(out, f.subscribes)
	return out
}

func (f *fakeConn) snapshotUnsubscribes() [][]string {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([][]string, len(f.unsubscribes))
	copy(out, f.unsubscribes)
	return out
}

func newPreflightServer(t *testing.T) string {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		body := `{"error":[],"result":{"BTC/USD":{"altname":"X","wsname":"X","pair_decimals":1,"lot_decimals":8,"tick_size":"0.1","status":"online"}}}`
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(srv.Close)
	return srv.URL
}

func newStubMetrics(t *testing.T) Metrics {
	t.Helper()
	m, err := metrics.New(context.Background(), metrics.Options{Service: "kraken-test", Disabled: true})
	if err != nil {
		t.Fatalf("metrics.New: %v", err)
	}
	t.Cleanup(func() { _ = m.Shutdown(context.Background()) })
	return Metrics{
		WSMessagesReceived:    m.KrakenWSMessagesReceived,
		WSReconnects:          m.KrakenWSReconnects,
		SilentStallReconnects: m.KrakenWSSilentStallReconnects,
		SubscribeRejected:     m.KrakenWSSubscribeRejected,
		ChecksumMismatches:    m.KrakenWSChecksumMismatches,
		PerSymbolResubs:       m.KrakenWSPerSymbolResubs,
		ResubDropped:          m.KrakenWSResubDropped,
		FlagEnabled:           m.KrakenFlagEnabled,
	}
}

func newStubBookkeeper(t *testing.T) *krob.Bookkeeper {
	t.Helper()
	return krob.New(krob.Config{
		Publish:            func(string, krob.Side, kob.SnapshotPayload) error { return nil },
		ChangeTickInterval: 24 * time.Hour,
		HeartbeatInterval:  24 * time.Hour,
		PairScale:          map[string]krob.PairScale{"BTC/USD": {PairDecimals: 1, LotDecimals: 8}},
	})
}

func waitFor(t *testing.T, timeout time.Duration, msg string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("waitFor: %s", msg)
}

func TestWorker_AllowResub_DenyAfterFirst(t *testing.T) {
	w := New(Config{})
	if !w.allowResub("BTC/USD") {
		t.Fatal("first call for BTC/USD must allow")
	}
	if w.allowResub("BTC/USD") {
		t.Fatal("immediate second call for BTC/USD must deny (1/60s budget)")
	}
	// Different symbol still allowed — each symbol gets its own limiter.
	if !w.allowResub("ETH/USD") {
		t.Fatal("first call for ETH/USD must allow (per-symbol bucket)")
	}
	if w.allowResub("BTC/USD") {
		t.Fatal("BTC/USD still rate-limited")
	}
}

func TestWorker_ResubLoop_RateLimitedDrops(t *testing.T) {
	fc := newFakeConn()
	w := New(Config{Metrics: newStubMetrics(t)})

	// Pre-consume BTC/USD's 1/60s token so the next request will be denied.
	if !w.allowResub("BTC/USD") {
		t.Fatal("setup: first allowResub should succeed")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ch := make(chan string, 4)
	done := make(chan struct{})
	go func() { defer close(done); w.resubLoop(ctx, fc, ch) }()

	ch <- "BTC/USD" // limiter denies → no Unsubscribe
	ch <- "ETH/USD" // first ETH token allowed → one Unsubscribe + Subscribe

	waitFor(t, time.Second, "ETH/USD resub observed", func() bool {
		for _, u := range fc.snapshotUnsubscribes() {
			if len(u) == 2 && u[0] == "book" && u[1] == "ETH/USD" {
				return true
			}
		}
		return false
	})

	// Confirm BTC was NOT resubbed.
	for _, u := range fc.snapshotUnsubscribes() {
		if len(u) == 2 && u[1] == "BTC/USD" {
			t.Fatalf("BTC/USD was resubbed despite rate-limit; unsubscribes=%v", fc.snapshotUnsubscribes())
		}
	}

	cancel()
	<-done
}

func TestWorker_ResubLoop_ContinuesOnWriteError(t *testing.T) {
	fc := newFakeConn()
	fc.setUnsubscribeErr(fmt.Errorf("boom"))
	w := New(Config{Metrics: newStubMetrics(t)})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ch := make(chan string, 4)
	done := make(chan struct{})
	go func() { defer close(done); w.resubLoop(ctx, fc, ch) }()

	ch <- "BTC/USD" // Unsubscribe errors → loop logs WARN, continues
	// Clear the error and send another symbol; loop must still be draining.
	fc.setUnsubscribeErr(nil)
	ch <- "ETH/USD"

	waitFor(t, time.Second, "second unsubscribe observed after first errored", func() bool {
		count := 0
		for _, u := range fc.snapshotUnsubscribes() {
			if len(u) == 2 && u[0] == "book" {
				count++
			}
		}
		return count >= 2
	})

	cancel()
	<-done
}

func TestWorker_ResubLoop_ExitsOnChannelClose(t *testing.T) {
	fc := newFakeConn()
	w := New(Config{Metrics: newStubMetrics(t)})

	ch := make(chan string, 4)
	done := make(chan struct{})
	go func() { defer close(done); w.resubLoop(context.Background(), fc, ch) }()

	close(ch)
	select {
	case <-done:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("resubLoop did not return on channel close")
	}
}

func TestWorker_ResubLoop_ExitsOnCtxCancel(t *testing.T) {
	fc := newFakeConn()
	w := New(Config{Metrics: newStubMetrics(t)})

	ctx, cancel := context.WithCancel(context.Background())
	ch := make(chan string, 4)
	done := make(chan struct{})
	go func() { defer close(done); w.resubLoop(ctx, fc, ch) }()

	cancel()
	select {
	case <-done:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("resubLoop did not return on ctx cancel")
	}
}

func TestWorker_Dispatch_MismatchSendsToResubChan(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	bk := newStubBookkeeper(t)
	go bk.Run(ctx)

	w := New(Config{Metrics: newStubMetrics(t), Bookkeeper: bk})

	// Seed the bookkeeper so topWire[BTC/USD] is non-empty — otherwise
	// VerifyChecksumForSymbol returns (true, 0) on an empty book.
	snapDone := make(chan struct{})
	bk.ApplySnapshot(krob.SnapshotMsg{
		Symbol: "BTC/USD",
		Bids:   []krob.WireLevel{{Price: "100.0", Qty: "1.00000000"}},
		Asks:   []krob.WireLevel{{Price: "101.0", Qty: "1.00000000"}},
		Done:   snapDone,
	})
	<-snapDone

	resubReq := make(chan string, 4)
	f := wsv2.Frame{Kind: wsv2.FrameBookUpdate, Book: &wsv2.BookFrame{
		Symbol:   "BTC/USD",
		Bids:     []wsv2.BookLevel{{Price: "100.0", Qty: "2.00000000"}},
		Checksum: 0xDEADBEEF,
	}}
	if err := w.dispatch(ctx, f, resubReq); err != nil {
		t.Fatalf("dispatch returned err: %v", err)
	}
	select {
	case sym := <-resubReq:
		if sym != "BTC/USD" {
			t.Fatalf("got sym=%q want BTC/USD", sym)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("resubReq did not receive symbol")
	}
}

func TestWorker_Dispatch_MismatchDropsOnFullChan(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	bk := newStubBookkeeper(t)
	go bk.Run(ctx)

	w := New(Config{Metrics: newStubMetrics(t), Bookkeeper: bk})

	snapDone := make(chan struct{})
	bk.ApplySnapshot(krob.SnapshotMsg{
		Symbol: "BTC/USD",
		Bids:   []krob.WireLevel{{Price: "100.0", Qty: "1.00000000"}},
		Asks:   []krob.WireLevel{{Price: "101.0", Qty: "1.00000000"}},
		Done:   snapDone,
	})
	<-snapDone

	resubReq := make(chan string, 1)
	resubReq <- "filler" // pre-fill to capacity

	f := wsv2.Frame{Kind: wsv2.FrameBookUpdate, Book: &wsv2.BookFrame{
		Symbol:   "BTC/USD",
		Bids:     []wsv2.BookLevel{{Price: "100.0", Qty: "2.00000000"}},
		Checksum: 0xDEADBEEF,
	}}
	// MUST NOT block. If it blocks, the test deadline catches the bug.
	doneDispatch := make(chan error, 1)
	go func() { doneDispatch <- w.dispatch(ctx, f, resubReq) }()
	select {
	case err := <-doneDispatch:
		if err != nil {
			t.Fatalf("dispatch returned err: %v", err)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("dispatch blocked when resubReq was full")
	}
}

func TestWorker_SilentStall_DefaultIs60s(t *testing.T) {
	w := New(Config{})
	if w.cfg.SilentStall != 60*time.Second {
		t.Fatalf("SilentStall default = %v; want 60s", w.cfg.SilentStall)
	}
}

func TestWorker_KillSwitchTransitions_OffOnObservedByOpen(t *testing.T) {
	flag := featureflag.NewTestFlag(true)
	openCount := atomic.Int64{}

	open := func(context.Context, string) (wsv2.Conn, error) {
		openCount.Add(1)
		fc := newFakeConn()
		// drip a heartbeat so the readloop has at least one frame in flight
		fc.reads <- wsv2.Frame{Kind: wsv2.FrameHeartbeat}
		return fc, nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	bk := newStubBookkeeper(t)
	go bk.Run(ctx)

	w := New(Config{
		Flag:        flag,
		REST:        rest.New(nil, newPreflightServer(t)),
		Open:        open,
		WSURL:       "wss://test",
		Bookkeeper:  bk,
		Pairs:       []string{"BTC/USD"},
		SilentStall: 24 * time.Hour,
		StallTick:   1 * time.Hour,
		FlagPoll:    20 * time.Millisecond,
		Backoff:     BackoffPolicy{Initial: 5 * time.Millisecond, Max: 50 * time.Millisecond, JitterFrac: 0.1},
		Metrics:     newStubMetrics(t),
	})
	runDone := make(chan error, 1)
	go func() { runDone <- w.Run(ctx) }()

	waitFor(t, 2*time.Second, "first connect", func() bool { return openCount.Load() >= 1 })

	flag.SetEnabled(false)
	// Wait for the inner ctx to cancel; the outer loop reaches waitForEnable
	// and parks. No new Opens should fire while disabled.
	time.Sleep(150 * time.Millisecond)
	before := openCount.Load()

	flag.SetEnabled(true)
	waitFor(t, 3*time.Second, "second connect after re-enable", func() bool {
		return openCount.Load() > before
	})

	cancel()
	<-runDone
}

func TestWorker_ChecksumMismatch_TriggersPerSymbolResub(t *testing.T) {
	fc := newFakeConn()
	flag := featureflag.NewTestFlag(true)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	bk := newStubBookkeeper(t)
	go bk.Run(ctx)

	w := New(Config{
		Flag:        flag,
		REST:        rest.New(nil, newPreflightServer(t)),
		Open:        func(context.Context, string) (wsv2.Conn, error) { return fc, nil },
		WSURL:       "wss://test",
		Bookkeeper:  bk,
		Pairs:       []string{"BTC/USD"},
		SilentStall: 24 * time.Hour,
		StallTick:   1 * time.Hour,
		FlagPoll:    20 * time.Millisecond,
		Metrics:     newStubMetrics(t),
	})
	runDone := make(chan error, 1)
	go func() { runDone <- w.Run(ctx) }()

	// Push a snapshot, then an update with a deliberately-bad checksum.
	fc.reads <- wsv2.Frame{Kind: wsv2.FrameBookSnapshot, Book: &wsv2.BookFrame{
		Symbol: "BTC/USD",
		Bids:   []wsv2.BookLevel{{Price: "100.0", Qty: "5.00000000"}},
		Asks:   []wsv2.BookLevel{{Price: "101.0", Qty: "5.00000000"}},
	}}
	fc.reads <- wsv2.Frame{Kind: wsv2.FrameBookUpdate, Book: &wsv2.BookFrame{
		Symbol:   "BTC/USD",
		Bids:     []wsv2.BookLevel{{Price: "100.0", Qty: "6.00000000"}},
		Checksum: 0xDEADBEEF,
	}}

	waitFor(t, 2*time.Second, "Unsubscribe(book, BTC/USD) observed", func() bool {
		for _, u := range fc.snapshotUnsubscribes() {
			if len(u) == 2 && u[0] == "book" && u[1] == "BTC/USD" {
				return true
			}
		}
		return false
	})
	waitFor(t, 2*time.Second, "Subscribe(book, BTC/USD) resub observed", func() bool {
		count := 0
		for _, s := range fc.snapshotSubscribes() {
			if len(s) == 2 && s[0] == "book" && s[1] == "BTC/USD" {
				count++
			}
		}
		return count >= 1
	})

	cancel()
	<-runDone
}

func TestWorker_SilentStallWatchdog_ForceClosesAfterThreshold(t *testing.T) {
	fc := newFakeConn()
	fc.reads <- wsv2.Frame{Kind: wsv2.FrameHeartbeat}

	base := time.Date(2026, 5, 14, 12, 0, 0, 0, time.UTC)
	var nowAtom atomic.Int64
	nowAtom.Store(base.UnixNano())
	now := func() time.Time { return time.Unix(0, nowAtom.Load()) }

	flag := featureflag.NewTestFlag(true)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	bk := newStubBookkeeper(t)
	go bk.Run(ctx)

	w := New(Config{
		Flag:        flag,
		REST:        rest.New(nil, newPreflightServer(t)),
		Open:        func(context.Context, string) (wsv2.Conn, error) { return fc, nil },
		WSURL:       "wss://test",
		Bookkeeper:  bk,
		Pairs:       []string{"BTC/USD"},
		SilentStall: 10 * time.Second,
		StallTick:   20 * time.Millisecond,
		FlagPoll:    20 * time.Millisecond,
		Metrics:     newStubMetrics(t),
		Now:         now,
	})

	runDone := make(chan error, 1)
	go func() { runDone <- w.Run(ctx) }()

	// Give preflight + subscribe time to run before advancing the clock.
	time.Sleep(80 * time.Millisecond)
	nowAtom.Store(base.Add(11 * time.Second).UnixNano())

	waitFor(t, 2*time.Second, "watchdog force-closes the connection", func() bool {
		return fc.closed.Load()
	})

	cancel()
	<-runDone
}
