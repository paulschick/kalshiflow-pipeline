package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/paulschick/kalshiflow-pipeline/internal/kalshi"
	"github.com/paulschick/kalshiflow-pipeline/internal/orderbook"
	"github.com/paulschick/kalshiflow-pipeline/internal/orderbook/orderbookpb"
	"github.com/paulschick/kalshiflow-pipeline/internal/testutil"
)

type fakeWS struct {
	// framesMu guards the frames field itself (not the chan contents). Needed
	// because the reconnect test reassigns frames between sessions while the
	// read loop may concurrently load it. Sends/receives still race-safely on
	// the channel value once snapshotted.
	framesMu   sync.Mutex
	frames     chan kalshi.Frame
	openCalls  atomic.Int64
	closeCalls atomic.Int64
	subCalls   atomic.Int64
	unsubCalls atomic.Int64
	pingCalls  atomic.Int64
	pingErr    atomic.Pointer[error]
	openErr    error
}

func (f *fakeWS) Open(_ context.Context) error                     { f.openCalls.Add(1); return f.openErr }
func (f *fakeWS) Close() error                                     { f.closeCalls.Add(1); return nil }
func (f *fakeWS) Subscribe(_ context.Context, _, _ []string) error { f.subCalls.Add(1); return nil }
func (f *fakeWS) Unsubscribe(_ context.Context, _ []int64) error   { f.unsubCalls.Add(1); return nil }
func (f *fakeWS) LastSubID() int64                                 { return f.subCalls.Load() }
func (f *fakeWS) Ping(_ context.Context) error {
	f.pingCalls.Add(1)
	if errPtr := f.pingErr.Load(); errPtr != nil {
		return *errPtr
	}
	return nil
}
func (f *fakeWS) ReadMessage(ctx context.Context) (kalshi.Frame, error) {
	f.framesMu.Lock()
	ch := f.frames
	f.framesMu.Unlock()
	select {
	case <-ctx.Done():
		return kalshi.Frame{}, ctx.Err()
	case fr, ok := <-ch:
		if !ok {
			return kalshi.Frame{}, errors.New("ws closed")
		}
		return fr, nil
	}
}

type fakeREST struct {
	called atomic.Int64
	mu     sync.Mutex
	byName map[string][]string // series_ticker → tickers
	hitCap map[string]bool     // series_ticker → hitCap return
}

func (f *fakeREST) GetOpenMarkets(_ context.Context, seriesTicker, _ string, _ int) ([]string, bool, error) {
	f.called.Add(1)
	f.mu.Lock()
	defer f.mu.Unlock()
	tickers := append([]string(nil), f.byName[seriesTicker]...)
	return tickers, f.hitCap[seriesTicker], nil
}

type capturedPublish struct {
	stream string
	body   []byte
}

type stubFanout struct {
	mu       sync.Mutex
	captured []capturedPublish
}

func (s *stubFanout) PublishStream(_ context.Context, stream string, body []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.captured = append(s.captured, capturedPublish{stream: stream, body: append([]byte(nil), body...)})
	return nil
}

func (s *stubFanout) snapshot() []capturedPublish {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]capturedPublish, len(s.captured))
	copy(out, s.captured)
	return out
}

// newREST is a small constructor for the most common single-series fakeREST shape.
func newREST(seriesTicker string, tickers ...string) *fakeREST {
	return &fakeREST{
		byName: map[string][]string{seriesTicker: tickers},
		hitCap: map[string]bool{},
	}
}

func newTestBookkeeper(t *testing.T) *orderbook.Bookkeeper {
	t.Helper()
	return orderbook.NewBookkeeper(orderbook.BookkeeperConfig{
		Publish:            func(string, orderbook.Side, orderbook.SnapshotPayload) error { return nil },
		ChangeTickInterval: 50 * time.Millisecond,
		HeartbeatInterval:  10 * time.Second,
	})
}

func TestWorker_DropsAcksAndUnknownTypes(t *testing.T) {
	t.Parallel()
	ws := &fakeWS{frames: make(chan kalshi.Frame, 4)}
	pub := &stubFanout{}
	rest := newREST("KXBTCD", "X")

	w := New(Deps{WS: ws, Pub: pub, REST: rest, Series: []string{"KXBTCD"}, Bookkeeper: newTestBookkeeper(t)})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() { _ = w.Run(ctx) }()
	testutil.WaitFor(t, time.Second, "Open + 2x Subscribe", func() bool {
		return ws.openCalls.Load() > 0 && ws.subCalls.Load() >= 2
	})

	ws.frames <- kalshi.Frame{Type: "ok", SID: 1, Seq: 1, RawPayload: json.RawMessage(`{}`)}
	ws.frames <- kalshi.Frame{Type: "error", SID: 1, RawPayload: json.RawMessage(`{"code":1}`)}
	ws.frames <- kalshi.Frame{Type: "ticker", SID: 2, RawPayload: json.RawMessage(`{}`)}

	// NEGATIVE assertion: bounded sleep is the only way. 100ms is comfortably longer than
	// the worker's per-frame work; if a publish were going to happen, it would have by now.
	time.Sleep(100 * time.Millisecond)
	if got := pub.snapshot(); len(got) != 0 {
		t.Errorf("expected zero publishes for ack/error/ticker frames; got %d", len(got))
	}
}

func TestClassifyStream_AllPlanOneStreams(t *testing.T) {
	cases := []struct {
		frameType string
		eventType string
		want      string
	}{
		{"orderbook_delta", "", ""},
		{"orderbook_snapshot", "", ""},
		{"trade", "", "trade"},
		{"market_lifecycle_v2", "created", "lifecycle"},
		{"market_lifecycle_v2", "activated", "lifecycle"},
		{"market_lifecycle_v2", "deactivated", "lifecycle"},
		{"market_lifecycle_v2", "close_date_updated", "lifecycle"},
		{"market_lifecycle_v2", "determined", "lifecycle"},
		{"market_lifecycle_v2", "settled", "settlement"},
		{"market_lifecycle_v2", "fractional_trading_updated", "lifecycle"},
		{"ok", "", ""},
		{"error", "", ""},
		{"ticker", "", ""},
		{"unknown", "", ""},
	}
	for _, tc := range cases {
		t.Run(tc.frameType+"/"+tc.eventType, func(t *testing.T) {
			if got := classifyStream(tc.frameType, tc.eventType); got != tc.want {
				t.Errorf("classifyStream(%q, %q) = %q; want %q", tc.frameType, tc.eventType, got, tc.want)
			}
		})
	}
}

func TestWorker_RoutesTradeAndLifecycleSeparately(t *testing.T) {
	t.Parallel()
	ws := &fakeWS{frames: make(chan kalshi.Frame, 4)}
	pub := &stubFanout{}
	rest := newREST("KXBTCD", "KXBTCD-26APR2917-T67000")

	w := New(Deps{WS: ws, Pub: pub, REST: rest, Series: []string{"KXBTCD"}, Bookkeeper: newTestBookkeeper(t)})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() { _ = w.Run(ctx) }()
	testutil.WaitFor(t, time.Second, "Open + 2x Subscribe (initial-pop + lifecycle)", func() bool {
		return ws.openCalls.Load() > 0 && ws.subCalls.Load() >= 2
	})

	ws.frames <- kalshi.Frame{
		Type: "trade", SID: 1, Seq: 1,
		RawPayload: json.RawMessage(`{"market_ticker":"KXBTCD-26APR2917-T67000","yes_price_dollars":"0.5","ts_ms":1714400000000}`),
	}
	ws.frames <- kalshi.Frame{
		Type: "market_lifecycle_v2", SID: 2,
		RawPayload: json.RawMessage(`{"event_type":"created","market_ticker":"KXBTCD-26APR2917-T67000","open_ts":1714000000}`),
	}
	ws.frames <- kalshi.Frame{
		Type: "market_lifecycle_v2", SID: 2,
		RawPayload: json.RawMessage(`{"event_type":"settled","market_ticker":"KXBTCD-26APR2917-T67000","settlement_value":"1.0000"}`),
	}

	testutil.WaitFor(t, time.Second, "all 3 publishes land", func() bool {
		return len(pub.snapshot()) >= 3
	})

	streams := map[string]int{}
	for _, c := range pub.snapshot() {
		streams[c.stream]++
	}
	if streams["trade"] != 1 || streams["lifecycle"] != 1 || streams["settlement"] != 1 {
		t.Errorf("stream counts = %v; want trade=1 lifecycle=1 settlement=1", streams)
	}
}

// syncBuf is a goroutine-safe wrapper around bytes.Buffer for capturing slog output
// written from one goroutine and read from another.
type syncBuf struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuf) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuf) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func TestWorker_LogsServerErrorFrames(t *testing.T) {
	t.Parallel()
	ws := &fakeWS{frames: make(chan kalshi.Frame, 1)}
	pub := &stubFanout{}
	rest := newREST("KXBTCD", "KXBTCD-26APR2917-T67000")

	logs := &syncBuf{}
	logger := slog.New(slog.NewJSONHandler(logs, &slog.HandlerOptions{Level: slog.LevelDebug}))

	w := New(Deps{
		WS:         ws,
		Pub:        pub,
		REST:       rest,
		Logger:     logger,
		Series:     []string{"KXBTCD"},
		Bookkeeper: newTestBookkeeper(t),
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() { _ = w.Run(ctx) }()
	testutil.WaitFor(t, time.Second, "Open + 2x Subscribe", func() bool {
		return ws.openCalls.Load() > 0 && ws.subCalls.Load() >= 2
	})

	ws.frames <- kalshi.Frame{
		Type:       "error",
		RawPayload: json.RawMessage(`{"code":11,"msg":"Invalid parameter"}`),
	}

	testutil.WaitFor(t, time.Second, "error frame logged at warn level", func() bool {
		s := logs.String()
		return strings.Contains(s, `"level":"WARN"`) &&
			strings.Contains(s, `ws server error frame`)
	})
	if got := pub.snapshot(); len(got) != 0 {
		t.Errorf("error frames must not publish; got %d", len(got))
	}
}

func TestWorker_DropsLifecycleForOtherTickers(t *testing.T) {
	t.Parallel()
	ws := &fakeWS{frames: make(chan kalshi.Frame, 2)}
	pub := &stubFanout{}
	rest := newREST("KXBTCD", "KXBTCD-26APR2917-T67000")

	w := New(Deps{WS: ws, Pub: pub, REST: rest, Series: []string{"KXBTCD"}, Bookkeeper: newTestBookkeeper(t)})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() { _ = w.Run(ctx) }()
	testutil.WaitFor(t, time.Second, "Open + 2x Subscribe", func() bool {
		return ws.openCalls.Load() > 0 && ws.subCalls.Load() >= 2
	})

	// Lifecycle for an unconfigured series — channel is exchange-wide so the worker
	// receives this and must drop it (series-prefix mismatch).
	ws.frames <- kalshi.Frame{
		Type: "market_lifecycle_v2", SID: 99,
		RawPayload: json.RawMessage(`{"event_type":"created","market_ticker":"KXETHD-some-other-ticker","open_ts":1714000000}`),
	}

	// NEGATIVE assertion: bounded sleep — see TestWorker_DropsAcksAndUnknownTypes.
	time.Sleep(100 * time.Millisecond)
	if got := pub.snapshot(); len(got) != 0 {
		t.Errorf("expected zero publishes for non-matching lifecycle; got %d (%v)", len(got), got)
	}
}

// --- Discovery tests (slice 2.3a) -------------------------------------------------------

func TestWorker_InitialPopulation_SubscribesAllOpenMarkets(t *testing.T) {
	t.Parallel()
	ws := &fakeWS{frames: make(chan kalshi.Frame, 4)}
	pub := &stubFanout{}
	rest := newREST("KXBTCD", "KXBTCD-A", "KXBTCD-B")

	w := New(Deps{WS: ws, Pub: pub, REST: rest, Series: []string{"KXBTCD"}, Bookkeeper: newTestBookkeeper(t)})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = w.Run(ctx) }()

	testutil.WaitFor(t, time.Second, "REST + 2 Subscribe calls (initial-pop + lifecycle)", func() bool {
		return rest.called.Load() >= 1 && ws.subCalls.Load() >= 2
	})
}

func TestWorker_LifecycleCreatedTriggersSubscribe(t *testing.T) {
	t.Parallel()
	ws := &fakeWS{frames: make(chan kalshi.Frame, 4)}
	pub := &stubFanout{}
	rest := newREST("KXBTCD") // empty initial roster

	w := New(Deps{WS: ws, Pub: pub, REST: rest, Series: []string{"KXBTCD"}, Bookkeeper: newTestBookkeeper(t)})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = w.Run(ctx) }()

	// Empty initial pop → only the lifecycle subscribe (1 call).
	testutil.WaitFor(t, time.Second, "lifecycle subscribed", func() bool { return ws.subCalls.Load() >= 1 })
	subBefore := ws.subCalls.Load()

	ws.frames <- kalshi.Frame{
		Type: "market_lifecycle_v2", SID: 9,
		RawPayload: json.RawMessage(`{"event_type":"created","market_ticker":"KXBTCD-NEW","open_ts":1714000000}`),
	}
	testutil.WaitFor(t, time.Second, "dynamic subscribe fired", func() bool {
		return ws.subCalls.Load() > subBefore
	})
}

func TestWorker_LifecycleSettledIssuesUnsubWhenLegacy(t *testing.T) {
	t.Parallel()
	ws := &fakeWS{frames: make(chan kalshi.Frame, 4)}
	pub := &stubFanout{}
	rest := newREST("KXBTCD", "KXBTCD-A")

	w := New(Deps{
		WS: ws, Pub: pub, REST: rest, Series: []string{"KXBTCD"},
		Bookkeeper:     newTestBookkeeper(t),
		LifecycleUnsub: true, // legacy parity — explicit unsub on terminal events
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = w.Run(ctx) }()

	testutil.WaitFor(t, time.Second, "initial-pop + lifecycle subscribed", func() bool {
		return ws.subCalls.Load() >= 2
	})

	// Stamp the sid by acking the initial-pop subscribe (req id = 1; lifecycle = 2).
	ws.frames <- kalshi.Frame{ID: 1, Type: "ok", SID: 333, RawPayload: json.RawMessage(`{}`)}
	// Settle KXBTCD-A → triggers Unsubscribe([333]) ONLY because LifecycleUnsub=true.
	ws.frames <- kalshi.Frame{
		Type: "market_lifecycle_v2", SID: 99,
		RawPayload: json.RawMessage(`{"event_type":"settled","market_ticker":"KXBTCD-A","settlement_value":"1.0"}`),
	}
	testutil.WaitFor(t, time.Second, "Unsubscribe fired", func() bool {
		return ws.unsubCalls.Load() > 0
	})
}

func TestWorker_LifecycleDeactivatedIssuesUnsubWhenLegacy(t *testing.T) {
	t.Parallel()
	ws := &fakeWS{frames: make(chan kalshi.Frame, 4)}
	pub := &stubFanout{}
	rest := newREST("KXBTCD", "KXBTCD-A")

	w := New(Deps{
		WS: ws, Pub: pub, REST: rest, Series: []string{"KXBTCD"},
		Bookkeeper:     newTestBookkeeper(t),
		LifecycleUnsub: true, // legacy parity
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = w.Run(ctx) }()
	testutil.WaitFor(t, time.Second, "Open + Subscribe + Subscribe lifecycle", func() bool {
		return ws.openCalls.Load() > 0 && ws.subCalls.Load() >= 2
	})

	// Ack the initial population subscribe so the roster has Sid > 0.
	ws.frames <- kalshi.Frame{Type: "ok", ID: 1, SID: 42}

	ws.frames <- kalshi.Frame{
		Type: "market_lifecycle_v2", SID: 99,
		RawPayload: json.RawMessage(`{"event_type":"deactivated","market_ticker":"KXBTCD-A"}`),
	}

	testutil.WaitFor(t, time.Second, "Unsubscribe call", func() bool {
		return ws.unsubCalls.Load() >= 1
	})
}

// TestWorker_LifecycleSettledDeletesFromBookkeeper asserts the new default
// terminal-lifecycle path: roster.Remove + Bookkeeper.DeleteMarket WITHOUT
// an explicit WS.Unsubscribe. Kalshi auto-retires sids server-side at
// terminal states; the explicit unsub triggered the 2026-05-08 12:01 UTC
// rollover-boundary code:7 flood. LifecycleUnsub is left at its zero value
// (false) — the cmd default.
func TestWorker_LifecycleSettledDeletesFromBookkeeper(t *testing.T) {
	t.Parallel()
	ws := &fakeWS{frames: make(chan kalshi.Frame, 4)}
	pub := &stubFanout{}
	rest := newREST("KXBTCD", "KXBTCD-A")
	bk := &stubBookkeeper{}

	w := New(Deps{
		WS: ws, Pub: pub, REST: rest, Series: []string{"KXBTCD"},
		Bookkeeper:         bk,
		PreserveBookkeeper: true, // delete only fires on the preserve path
		// LifecycleUnsub: false (zero value) → no explicit unsub
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = w.Run(ctx) }()

	testutil.WaitFor(t, time.Second, "initial-pop + lifecycle subscribed", func() bool {
		return ws.subCalls.Load() >= 2
	})

	// Stamp the sid by acking the initial-pop subscribe (req id = 1; lifecycle = 2).
	ws.frames <- kalshi.Frame{ID: 1, Type: "ok", SID: 333, RawPayload: json.RawMessage(`{}`)}
	// Settle KXBTCD-A → DeleteMarket fires; Unsubscribe must NOT fire.
	ws.frames <- kalshi.Frame{
		Type: "market_lifecycle_v2", SID: 99,
		RawPayload: json.RawMessage(`{"event_type":"settled","market_ticker":"KXBTCD-A","settlement_value":"1.0"}`),
	}

	testutil.WaitFor(t, time.Second, "Bookkeeper.DeleteMarket called for settled ticker", func() bool {
		for _, k := range bk.deleted() {
			if k == "KXBTCD-A" {
				return true
			}
		}
		return false
	})

	// NEGATIVE: bounded sleep — give the read loop time to process and the
	// unsub branch is right there in the lifecycle handler. If unsub were
	// going to fire, it would have by now.
	time.Sleep(100 * time.Millisecond)
	if got := ws.unsubCalls.Load(); got != 0 {
		t.Errorf("Unsubscribe fired with LifecycleUnsub=false; unsubCalls=%d (want 0)", got)
	}
}

func TestWorker_DropsLifecycleForUnconfiguredSeries(t *testing.T) {
	t.Parallel()
	ws := &fakeWS{frames: make(chan kalshi.Frame, 2)}
	pub := &stubFanout{}
	rest := newREST("KXBTCD") // empty roster

	w := New(Deps{WS: ws, Pub: pub, REST: rest, Series: []string{"KXBTCD"}, Bookkeeper: newTestBookkeeper(t)})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = w.Run(ctx) }()

	testutil.WaitFor(t, time.Second, "lifecycle subscribed", func() bool { return ws.subCalls.Load() >= 1 })
	subBefore := ws.subCalls.Load()

	ws.frames <- kalshi.Frame{
		Type: "market_lifecycle_v2", SID: 99,
		RawPayload: json.RawMessage(`{"event_type":"created","market_ticker":"KXETHD-OTHER","open_ts":0}`),
	}

	// NEGATIVE: bounded sleep proves no subscribe / no publish happened for the
	// foreign-series lifecycle event.
	time.Sleep(50 * time.Millisecond)

	if ws.subCalls.Load() != subBefore {
		t.Errorf("subscribe fired for unconfigured series KXETHD; subCalls = %d → %d",
			subBefore, ws.subCalls.Load())
	}
	if got := pub.snapshot(); len(got) != 0 {
		t.Errorf("envelope published for unconfigured series; got %d publishes", len(got))
	}
}

// --- Reconnect tests (slice 2.4) --------------------------------------------------------

func TestWorker_ReconnectsOnWSError(t *testing.T) {
	t.Parallel()
	ws := &fakeWS{frames: make(chan kalshi.Frame, 1)}
	pub := &stubFanout{}
	rest := newREST("KXBTCD", "KXBTCD-A")

	w := New(Deps{
		WS:          ws,
		REST:        rest,
		Pub:         pub,
		Series:      []string{"KXBTCD"},
		BaseBackoff: 1 * time.Millisecond,
		MaxBackoff:  5 * time.Millisecond,
		Bookkeeper:  newTestBookkeeper(t),
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() { _ = w.Run(ctx) }()
	testutil.WaitFor(t, time.Second, "first session up", func() bool {
		return ws.openCalls.Load() >= 1 && ws.subCalls.Load() >= 2
	})

	// Force the read loop to error: closing the frames channel makes ReadMessage return
	// "ws closed". Swap in a fresh channel for the next session under the framesMu lock.
	ws.framesMu.Lock()
	close(ws.frames)
	ws.frames = make(chan kalshi.Frame, 1)
	ws.framesMu.Unlock()

	testutil.WaitFor(t, time.Second, "second Open after reconnect", func() bool {
		return ws.openCalls.Load() >= 2
	})
}

func TestWorker_ReturnsErrReconnectExhaustedAfterCap(t *testing.T) {
	t.Parallel()
	ws := &fakeWS{frames: make(chan kalshi.Frame, 1), openErr: errors.New("permanent open failure")}
	pub := &stubFanout{}
	rest := newREST("KXBTCD")

	w := New(Deps{
		WS:          ws,
		REST:        rest,
		Pub:         pub,
		Series:      []string{"KXBTCD"},
		BaseBackoff: 1 * time.Microsecond,
		MaxBackoff:  10 * time.Microsecond,
		Bookkeeper:  newTestBookkeeper(t),
	})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	err := w.Run(ctx)
	if !errors.Is(err, ErrReconnectExhausted) {
		t.Errorf("Run err = %v; want ErrReconnectExhausted", err)
	}
	if got := ws.openCalls.Load(); got < int64(maxReconnectAttempts) {
		t.Errorf("Open calls = %d; want >= %d (attempt cap)", got, maxReconnectAttempts)
	}
}

// HOL-46 regression: when WS.Open fast-fails (e.g. Kalshi handshake 503), the
// per-session frame counter must not retain the prior session's count. If it
// does, shouldResetBackoff trips every iteration on the frame-threshold gate
// and pins attempt at 0, defeating the exponential ladder. This test wires
// the production gate path (BackoffDataThreshold=10) and pre-seeds a stale
// count to simulate state left by a successful prior session, then asserts
// that Run still exhausts attempts within budget.
func TestWorker_ReconnectExhausts_WithFrameThresholdGate_AfterPriorSession(t *testing.T) {
	t.Parallel()
	ws := &fakeWS{frames: make(chan kalshi.Frame, 1), openErr: errors.New("simulated Kalshi 503")}
	pub := &stubFanout{}
	rest := newREST("KXBTCD")

	w := New(Deps{
		WS:                   ws,
		REST:                 rest,
		Pub:                  pub,
		Series:               []string{"KXBTCD"},
		BaseBackoff:          1 * time.Microsecond,
		MaxBackoff:           10 * time.Microsecond,
		BackoffDataThreshold: 10,
		Bookkeeper:           newTestBookkeeper(t),
	})
	// State left by a prior successful session — exact condition observed
	// at 2026-05-13 11:09:21 UTC when the Kalshi gateway started returning
	// 503s on the WS handshake.
	w.tradingFramesThisSession.Store(1316788)
	w.tradingDeltaFramesThisSession.Store(1316788)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	err := w.Run(ctx)
	if !errors.Is(err, ErrReconnectExhausted) {
		t.Fatalf("Run err = %v; want ErrReconnectExhausted (backoff ladder stuck on stale frames)", err)
	}
	if got := ws.openCalls.Load(); got < int64(maxReconnectAttempts) {
		t.Fatalf("Open calls = %d; want >= %d (attempt cap)", got, maxReconnectAttempts)
	}
}

func TestWorker_RoutesOrderbookDeltaToBookkeeper(t *testing.T) {
	t.Parallel()

	var emits int64
	var emitsMu sync.Mutex
	bk := orderbook.NewBookkeeper(orderbook.BookkeeperConfig{
		Publish: func(string, orderbook.Side, orderbook.SnapshotPayload) error {
			emitsMu.Lock()
			defer emitsMu.Unlock()
			emits++
			return nil
		},
		ChangeTickInterval: 25 * time.Millisecond,
		HeartbeatInterval:  10 * time.Second,
	})

	ws := &fakeWS{frames: make(chan kalshi.Frame, 4)}
	pub := &stubFanout{}
	rest := newREST("KXBTCD", "KXBTCD-A")

	w := New(Deps{
		WS: ws, Pub: pub, REST: rest,
		Series:     []string{"KXBTCD"},
		Bookkeeper: bk,
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = w.Run(ctx) }()

	testutil.WaitFor(t, time.Second, "open + 2 subscribes", func() bool {
		return ws.openCalls.Load() > 0 && ws.subCalls.Load() >= 2
	})

	ws.frames <- kalshi.Frame{
		Type: kalshi.ChannelOrderbookDelta, SID: 1, Seq: 1,
		RawPayload: json.RawMessage(
			`{"market_ticker":"KXBTCD-A","side":"yes","price_dollars":"0.7700","delta_fp":"100.00","ts_ms":1714400000000}`),
	}

	testutil.WaitFor(t, time.Second, "first snapshot emit", func() bool {
		emitsMu.Lock()
		defer emitsMu.Unlock()
		return emits >= 1
	})

	for _, c := range pub.snapshot() {
		if c.stream == "orderbook" || c.stream == "snapshot" || c.stream == "snapshot_1s" {
			t.Errorf("unexpected raw publish on stream %q", c.stream)
		}
	}
}

// TestWorker_RoutesOrderbookSnapshotToBookkeeperApplySnapshot is the
// HOL-52 Bug B regression guard. Pre-fix, an orderbook_snapshot frame
// wiped the book via Bookkeeper.ResetMarket and the snapshot's level
// data was discarded — under a snapshot-only session this silenced the
// heartbeat. Post-fix, the snapshot's levels populate the book directly,
// and a subsequent delta lands on the populated state.
func TestWorker_RoutesOrderbookSnapshotToBookkeeperApplySnapshot(t *testing.T) {
	t.Parallel()

	type emit struct {
		side    orderbook.Side
		payload orderbook.SnapshotPayload
	}
	var emits []emit
	var emitsMu sync.Mutex
	bk := orderbook.NewBookkeeper(orderbook.BookkeeperConfig{
		Publish: func(ticker string, side orderbook.Side, p orderbook.SnapshotPayload) error {
			if ticker != "KXBTCD-A" {
				return nil
			}
			emitsMu.Lock()
			emits = append(emits, emit{side: side, payload: p})
			emitsMu.Unlock()
			return nil
		},
		ChangeTickInterval: 25 * time.Millisecond,
		HeartbeatInterval:  10 * time.Second,
	})

	ws := &fakeWS{frames: make(chan kalshi.Frame, 4)}
	pub := &stubFanout{}
	rest := newREST("KXBTCD", "KXBTCD-A")
	w := New(Deps{
		WS: ws, Pub: pub, REST: rest,
		Series:     []string{"KXBTCD"},
		Bookkeeper: bk,
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = w.Run(ctx) }()
	testutil.WaitFor(t, time.Second, "open + subscribes", func() bool {
		return ws.openCalls.Load() > 0 && ws.subCalls.Load() >= 2
	})

	// Snapshot with two YES levels and one NO level; bookkeeper must
	// populate the book directly from this payload.
	ws.frames <- kalshi.Frame{
		Type: "orderbook_snapshot", SID: 1, Seq: 1,
		RawPayload: json.RawMessage(`{"market_ticker":"KXBTCD-A","yes":[["0.7700",100],["0.7600",80]],"no":[["0.2300",50]]}`),
	}

	testutil.WaitFor(t, 2*time.Second, "snapshot emits yes top1=7700 size=100", func() bool {
		emitsMu.Lock()
		defer emitsMu.Unlock()
		for _, e := range emits {
			if e.side != orderbook.SideYes {
				continue
			}
			body, err := e.payload.Marshal()
			if err != nil {
				continue
			}
			var pb orderbookpb.SnapshotPayload
			if proto.Unmarshal(body, &pb) != nil {
				continue
			}
			if pb.Top1PriceUnits == 7700 && pb.Top1Size == 100 {
				return true
			}
		}
		return false
	})

	// Delta applied AFTER the snapshot should land on the populated book
	// (the worker's sync ack on ApplySnapshot guarantees order). The delta
	// adds a NEW level at 7800 that takes over top1 — confirms the delta
	// merged with the snapshot's existing levels rather than landing on an
	// empty book.
	ws.frames <- kalshi.Frame{
		Type: kalshi.ChannelOrderbookDelta, SID: 1, Seq: 2,
		RawPayload: json.RawMessage(`{"market_ticker":"KXBTCD-A","side":"yes","price_dollars":"0.7800","delta_fp":"50.00","ts_ms":1714400000000}`),
	}

	testutil.WaitFor(t, 2*time.Second, "post-delta emits top1=7800 size=50, top2=7700 size=100", func() bool {
		emitsMu.Lock()
		defer emitsMu.Unlock()
		for _, e := range emits {
			if e.side != orderbook.SideYes {
				continue
			}
			body, err := e.payload.Marshal()
			if err != nil {
				continue
			}
			var pb orderbookpb.SnapshotPayload
			if proto.Unmarshal(body, &pb) != nil {
				continue
			}
			if pb.Top1PriceUnits != 7800 || pb.Top1Size != 50 {
				continue
			}
			if pb.Top2PriceUnits == nil || *pb.Top2PriceUnits != 7700 {
				continue
			}
			if pb.Top2Size == nil || *pb.Top2Size != 100 {
				continue
			}
			return true
		}
		return false
	})

	for _, c := range pub.snapshot() {
		if c.stream == "snapshot" {
			t.Errorf("snapshot frame should not have published stream=snapshot")
		}
	}
}

// growingREST returns the union of all per-call ticker lists in order. Each Set call
// installs the next response. Used to test sweep adds.
type growingREST struct {
	mu    sync.Mutex
	calls atomic.Int64
	queue map[string][][]string // series → next responses
}

func newGrowingREST() *growingREST {
	return &growingREST{queue: map[string][][]string{}}
}

func (r *growingREST) Set(series string, tickers []string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.queue[series] = append(r.queue[series], tickers)
}

func (r *growingREST) GetOpenMarkets(_ context.Context, series, _ string, _ int) ([]string, bool, error) {
	r.calls.Add(1)
	r.mu.Lock()
	defer r.mu.Unlock()
	q := r.queue[series]
	if len(q) == 0 {
		return nil, false, errors.New("no response queued")
	}
	tickers := q[0]
	if len(q) > 1 {
		r.queue[series] = q[1:]
	}
	return append([]string(nil), tickers...), false, nil
}

func TestWorker_SweepDiscoversNewMarkets(t *testing.T) {
	t.Parallel()
	ws := &fakeWS{frames: make(chan kalshi.Frame, 16)}
	pub := &stubFanout{}
	rest := newGrowingREST()
	rest.Set("KXBTCD", []string{"KXBTCD-A"})             // initial populate
	rest.Set("KXBTCD", []string{"KXBTCD-A", "KXBTCD-B"}) // sweep tick 1 — B new

	w := New(Deps{
		WS: ws, Pub: pub, REST: rest,
		Series:     []string{"KXBTCD"},
		Bookkeeper: newTestBookkeeper(t),
	})
	w.sweepInterval = 50 * time.Millisecond
	w.sweepJitter = 0
	w.sweepMissThreshold = 2

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = w.Run(ctx) }()

	// initial population: 1 sub call for orderbook+trade, 1 for lifecycle
	testutil.WaitFor(t, time.Second, "initial subs", func() bool {
		return ws.subCalls.Load() >= 2
	})
	subBefore := ws.subCalls.Load()

	// after one sweep tick, expect another Subscribe for the new ticker B
	testutil.WaitFor(t, 2*time.Second, "sweep subscribe", func() bool {
		return ws.subCalls.Load() > subBefore
	})
}

func TestWorker_SweepTrimsAfterMissThreshold(t *testing.T) {
	t.Parallel()
	ws := &fakeWS{frames: make(chan kalshi.Frame, 16)}
	pub := &stubFanout{}
	rest := newGrowingREST()
	rest.Set("KXBTCD", []string{"KXBTCD-A", "KXBTCD-B"}) // populate
	rest.Set("KXBTCD", []string{"KXBTCD-A"})             // tick 1 — B missing
	rest.Set("KXBTCD", []string{"KXBTCD-A"})             // tick 2 — B missing again, trim

	w := New(Deps{
		WS: ws, Pub: pub, REST: rest,
		Series:     []string{"KXBTCD"},
		Bookkeeper: newTestBookkeeper(t),
	})
	w.sweepInterval = 30 * time.Millisecond
	w.sweepJitter = 0
	w.sweepMissThreshold = 2

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = w.Run(ctx) }()
	testutil.WaitFor(t, time.Second, "initial subs", func() bool {
		return ws.subCalls.Load() >= 2
	})

	// ack initial population so the roster has a sid for B
	ws.frames <- kalshi.Frame{Type: "ok", ID: 1, SID: 100}

	testutil.WaitFor(t, 2*time.Second, "trim unsubscribe after 2 misses", func() bool {
		return ws.unsubCalls.Load() >= 1
	})
}

func TestWorker_SweepResetsMissCounterOnReappear(t *testing.T) {
	t.Parallel()
	ws := &fakeWS{frames: make(chan kalshi.Frame, 16)}
	pub := &stubFanout{}
	rest := newGrowingREST()
	rest.Set("KXBTCD", []string{"KXBTCD-A", "KXBTCD-B"}) // populate
	rest.Set("KXBTCD", []string{"KXBTCD-A"})             // tick 1 — B missing
	rest.Set("KXBTCD", []string{"KXBTCD-A", "KXBTCD-B"}) // tick 2 — B back, counter reset
	rest.Set("KXBTCD", []string{"KXBTCD-A"})             // tick 3 — missing again, missCount=1
	rest.Set("KXBTCD", []string{"KXBTCD-A"})             // tick 4 — missing twice in a row → trim

	w := New(Deps{
		WS: ws, Pub: pub, REST: rest,
		Series:     []string{"KXBTCD"},
		Bookkeeper: newTestBookkeeper(t),
	})
	w.sweepInterval = 25 * time.Millisecond
	w.sweepJitter = 0
	w.sweepMissThreshold = 2

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = w.Run(ctx) }()
	testutil.WaitFor(t, time.Second, "initial subs", func() bool {
		return ws.subCalls.Load() >= 2
	})
	ws.frames <- kalshi.Frame{Type: "ok", ID: 1, SID: 100}

	// At least 4 sweep ticks + 1 trim. Allow 3s wall.
	testutil.WaitFor(t, 3*time.Second, "trim after counter reset + 2 misses", func() bool {
		return ws.unsubCalls.Load() >= 1 && rest.calls.Load() >= 5 // 1 populate + ≥4 sweeps
	})
}

type erroringREST struct {
	calls atomic.Int64
}

func (e *erroringREST) GetOpenMarkets(_ context.Context, _, _ string, _ int) ([]string, bool, error) {
	n := e.calls.Add(1)
	if n == 1 {
		// allow initial populate to succeed with a single ticker
		return []string{"KXBTCD-A"}, false, nil
	}
	return nil, false, errors.New("simulated REST 5xx")
}

func TestWorker_SweepSkipsTickOnRESTError(t *testing.T) {
	t.Parallel()
	ws := &fakeWS{frames: make(chan kalshi.Frame, 16)}
	pub := &stubFanout{}
	rest := &erroringREST{}

	w := New(Deps{
		WS: ws, Pub: pub, REST: rest,
		Series:     []string{"KXBTCD"},
		Bookkeeper: newTestBookkeeper(t),
	})
	w.sweepInterval = 25 * time.Millisecond
	w.sweepJitter = 0
	w.sweepMissThreshold = 2

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = w.Run(ctx) }()
	testutil.WaitFor(t, time.Second, "initial subs", func() bool {
		return ws.subCalls.Load() >= 2
	})
	ws.frames <- kalshi.Frame{Type: "ok", ID: 1, SID: 100}

	// Wait long enough for several sweep ticks. None should result in a trim because
	// every sweep errored — A must remain subscribed.
	time.Sleep(150 * time.Millisecond)

	if got := ws.unsubCalls.Load(); got != 0 {
		t.Errorf("unsubscribe called despite REST errors; unsubCalls=%d", got)
	}
}

func TestWorker_TradeAndLifecycleStillPublish(t *testing.T) {
	t.Parallel()
	bk := newTestBookkeeper(t)
	ws := &fakeWS{frames: make(chan kalshi.Frame, 4)}
	pub := &stubFanout{}
	rest := newREST("KXBTCD", "KXBTCD-A")
	w := New(Deps{
		WS: ws, Pub: pub, REST: rest,
		Series:     []string{"KXBTCD"},
		Bookkeeper: bk,
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = w.Run(ctx) }()
	testutil.WaitFor(t, time.Second, "open + 2 subscribes", func() bool {
		return ws.openCalls.Load() > 0 && ws.subCalls.Load() >= 2
	})

	ws.frames <- kalshi.Frame{
		Type: "trade", SID: 1, Seq: 1,
		RawPayload: json.RawMessage(`{"market_ticker":"KXBTCD-A","yes_price_dollars":"0.5","ts_ms":1714400000000}`),
	}
	testutil.WaitFor(t, time.Second, "trade publish", func() bool {
		for _, c := range pub.snapshot() {
			if c.stream == StreamTrade {
				return true
			}
		}
		return false
	})
}

// TestWatchdog_HappyPath_NoFire confirms the watchdog does NOT close the
// connection when Ping returns nil. Runs the loop directly (not via
// runOnce) for tighter isolation.
func TestWatchdog_HappyPath_NoFire(t *testing.T) {
	t.Parallel()
	ws := &fakeWS{frames: make(chan kalshi.Frame, 4)}
	// pingErr stays nil → Ping returns nil.

	w := New(Deps{
		WS:                   ws,
		Pub:                  &stubFanout{},
		REST:                 newREST("KXBTCD"),
		Series:               []string{"KXBTCD"},
		Bookkeeper:           newTestBookkeeper(t),
		WatchdogEnabled:      true,
		WatchdogPingInterval: 20 * time.Millisecond,
		WatchdogPongDeadline: 10 * time.Millisecond,
	})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		w.watchdogLoop(ctx)
		close(done)
	}()

	// Let several ticks elapse.
	time.Sleep(120 * time.Millisecond)
	cancel()
	<-done

	if got := ws.pingCalls.Load(); got < 3 {
		t.Errorf("pingCalls = %d; want >= 3", got)
	}
	if got := ws.closeCalls.Load(); got != 0 {
		t.Errorf("closeCalls = %d; want 0 (no fire on healthy ping)", got)
	}
}

// TestWatchdog_StallFires_ForcesClose confirms a Ping error triggers exactly
// one Close call and one goroutine return.
func TestWatchdog_StallFires_ForcesClose(t *testing.T) {
	t.Parallel()
	ws := &fakeWS{frames: make(chan kalshi.Frame, 4)}
	stallErr := context.DeadlineExceeded
	ws.pingErr.Store(&stallErr)

	w := New(Deps{
		WS:                   ws,
		Pub:                  &stubFanout{},
		REST:                 newREST("KXBTCD"),
		Series:               []string{"KXBTCD"},
		Bookkeeper:           newTestBookkeeper(t),
		Logger:               slog.New(slog.NewJSONHandler(&bytes.Buffer{}, nil)),
		WatchdogEnabled:      true,
		WatchdogPingInterval: 20 * time.Millisecond,
		WatchdogPongDeadline: 10 * time.Millisecond,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	done := make(chan struct{})
	go func() {
		w.watchdogLoop(ctx)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(300 * time.Millisecond):
		t.Fatal("watchdogLoop did not return within 300ms after stall")
	}

	if got := ws.closeCalls.Load(); got != 1 {
		t.Errorf("closeCalls = %d; want exactly 1", got)
	}
	if got := ws.pingCalls.Load(); got < 1 {
		t.Errorf("pingCalls = %d; want >= 1", got)
	}
}

// TestWatchdog_Disabled_NoGoroutine confirms WatchdogEnabled=false skips the
// spawn entirely — Ping is never called even after multiple intervals elapse.
func TestWatchdog_Disabled_NoGoroutine(t *testing.T) {
	t.Parallel()
	ws := &fakeWS{frames: make(chan kalshi.Frame, 4)}

	w := New(Deps{
		WS:                   ws,
		Pub:                  &stubFanout{},
		REST:                 newREST("KXBTCD"),
		Series:               []string{"KXBTCD"},
		Bookkeeper:           newTestBookkeeper(t),
		WatchdogEnabled:      false,
		WatchdogPingInterval: 20 * time.Millisecond,
		WatchdogPongDeadline: 10 * time.Millisecond,
	})

	if w.watchdogEnabled {
		t.Fatalf("watchdogEnabled = true; Deps.WatchdogEnabled was false")
	}

	time.Sleep(60 * time.Millisecond)
	if got := ws.pingCalls.Load(); got != 0 {
		t.Errorf("pingCalls = %d; want 0 when watchdog disabled", got)
	}
}

// TestDataFrameTimestamp_DispatcherUpdatesOnDataOnly drives a single frame
// through the read loop for each of: an "ok" ack, an "error" frame, an
// "orderbook_snapshot" frame, an "orderbook_delta" data frame, and a "trade"
// data frame. After each, asserts that w.lastTradingDataAt is advanced ONLY
// for trading-bucket frames.
//
// The orderbook_delta payload here is deliberately well-formed so that
// applyDelta succeeds — the new placement stamps BEFORE publishFrame, so
// even a malformed delta would stamp, but using a valid payload keeps the
// test free of incidental publish-error log noise.
func TestDataFrameTimestamp_DispatcherUpdatesOnDataOnly(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name      string
		frame     kalshi.Frame
		wantStamp bool
	}{
		{"ok_ack_no_stamp", kalshi.Frame{Type: "ok", ID: 1}, false},
		{"error_no_stamp", kalshi.Frame{Type: "error", ID: 1, RawPayload: json.RawMessage(`{"code":7,"msg":"x"}`)}, false},
		{"orderbook_snapshot_stamps", kalshi.Frame{Type: "orderbook_snapshot", SID: 1, RawPayload: json.RawMessage(`{"market_ticker":"KXBTCD-A"}`)}, true},
		{"orderbook_delta_stamps", kalshi.Frame{Type: "orderbook_delta", SID: 1, RawPayload: json.RawMessage(`{"market_ticker":"KXBTCD-A","side":"yes","price_dollars":"0.7700","delta_fp":"100.00","ts_ms":1714400000000}`)}, true},
		{"trade_stamps", kalshi.Frame{Type: "trade", SID: 1, RawPayload: json.RawMessage(`{"market_ticker":"KXBTCD-A","yes_price_dollars":"0.5","ts_ms":1714400000000}`)}, true},
		{"market_lifecycle_v2_no_stamp_on_trading", kalshi.Frame{Type: "market_lifecycle_v2", SID: 1, RawPayload: json.RawMessage(`{"market_ticker":"KXBTCD-A","status":"settled"}`)}, false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ws := &fakeWS{frames: make(chan kalshi.Frame, 2)}
			ws.frames <- tc.frame
			close(ws.frames) // EOF after one frame → runOnce returns "ws read" err

			w := New(Deps{
				WS:         ws,
				Pub:        &stubFanout{},
				REST:       newREST("KXBTCD"),
				Series:     []string{"KXBTCD"},
				Bookkeeper: newTestBookkeeper(t),
				Logger:     slog.New(slog.NewJSONHandler(&bytes.Buffer{}, nil)),
			})

			// Inject a monotonically advancing clock so we can distinguish the
			// seed call (first w.now() in runOnce) from a subsequent frame-loop
			// stamp. Each call advances by 1 ns.
			const epochBase = int64(1_000_000_000)
			var nowCalls atomic.Int64
			w.now = func() time.Time { return time.Unix(0, epochBase+nowCalls.Add(1)) }
			// seedAt will be epochBase+1 (first call). A data-frame stamp (second
			// call) yields epochBase+2, which is strictly > seedAt.
			const seedAt = epochBase + 1

			ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
			defer cancel()
			_ = w.runOnce(ctx) // expected to return "ws read" error after channel close

			// For control frames the frame loop is bypassed, so only seed ran.
			// For data frames the loop store ran too, advancing the clock once more.
			got := w.lastTradingDataAt.Load() > seedAt
			if got != tc.wantStamp {
				t.Errorf("lastTradingDataAt advanced past seed = %v; want %v (frame.Type=%s)", got, tc.wantStamp, tc.frame.Type)
			}
		})
	}
}

// TestWorker_TradingFramesThisSessionExcludesNonTrading drives a single
// frame through the read loop for each frame type and asserts
// tradingFramesThisSession only bumps for trading-bucket frames.
// market_lifecycle_v2 and control frames must NOT bump — that preserves
// the v0.12.0 backoff-escalation gate under the new bucketing.
func TestWorker_TradingFramesThisSessionExcludesNonTrading(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name     string
		frame    kalshi.Frame
		wantBump bool
	}{
		{"ok_ack_no_bump", kalshi.Frame{Type: "ok", ID: 1}, false},
		{"error_no_bump", kalshi.Frame{Type: "error", ID: 1, RawPayload: json.RawMessage(`{"code":7,"msg":"x"}`)}, false},
		{"orderbook_snapshot_bumps", kalshi.Frame{Type: "orderbook_snapshot", SID: 1, RawPayload: json.RawMessage(`{"market_ticker":"KXBTCD-A"}`)}, true},
		{"orderbook_delta_bumps", kalshi.Frame{Type: "orderbook_delta", SID: 1, RawPayload: json.RawMessage(`{"market_ticker":"KXBTCD-A","side":"yes","price_dollars":"0.7700","delta_fp":"100.00","ts_ms":1714400000000}`)}, true},
		{"trade_bumps", kalshi.Frame{Type: "trade", SID: 1, RawPayload: json.RawMessage(`{"market_ticker":"KXBTCD-A","yes_price_dollars":"0.5","ts_ms":1714400000000}`)}, true},
		{"market_lifecycle_v2_no_bump", kalshi.Frame{Type: "market_lifecycle_v2", SID: 1, RawPayload: json.RawMessage(`{"market_ticker":"KXBTCD-A"}`)}, false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ws := &fakeWS{frames: make(chan kalshi.Frame, 2)}
			ws.frames <- tc.frame
			close(ws.frames) // EOF after one frame → runOnce returns "ws read" err

			w := New(Deps{
				WS:         ws,
				Pub:        &stubFanout{},
				REST:       newREST("KXBTCD"),
				Series:     []string{"KXBTCD"},
				Bookkeeper: newTestBookkeeper(t),
				Logger:     slog.New(slog.NewJSONHandler(&bytes.Buffer{}, nil)),
			})

			ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
			defer cancel()
			_ = w.runOnce(ctx)

			got := w.tradingFramesThisSession.Load() > 0
			if got != tc.wantBump {
				t.Errorf("tradingFramesThisSession>0 = %v; want %v (frame.Type=%s)", got, tc.wantBump, tc.frame.Type)
			}
		})
	}
}

// TestWorker_BackoffNoResetOnZeroDataSession asserts the new frame-counted
// gate does NOT reset when the session received fewer than threshold
// non-control frames, regardless of session duration.
func TestWorker_BackoffNoResetOnZeroDataSession(t *testing.T) {
	t.Parallel()
	w := New(Deps{
		WS: &fakeWS{frames: make(chan kalshi.Frame, 1)}, REST: newREST("KXBTCD"),
		Pub: &stubFanout{}, Series: []string{"KXBTCD"}, Bookkeeper: newTestBookkeeper(t),
		BackoffDataThreshold: 10,
	})
	if w.shouldResetBackoff(2*time.Minute, 0) {
		t.Errorf("zero-data session reset; want NO reset")
	}
	if w.shouldResetBackoff(2*time.Minute, 5) {
		t.Errorf("frames=5 < threshold=10 reset; want NO reset")
	}
	if w.shouldResetBackoff(2*time.Minute, 9) {
		t.Errorf("frames=9 < threshold=10 reset; want NO reset")
	}
}

// TestWorker_BackoffResetOnDataBurst asserts reset fires when frames meet
// or exceed the threshold.
func TestWorker_BackoffResetOnDataBurst(t *testing.T) {
	t.Parallel()
	w := New(Deps{
		WS: &fakeWS{frames: make(chan kalshi.Frame, 1)}, REST: newREST("KXBTCD"),
		Pub: &stubFanout{}, Series: []string{"KXBTCD"}, Bookkeeper: newTestBookkeeper(t),
		BackoffDataThreshold: 10,
	})
	if !w.shouldResetBackoff(1*time.Second, 10) {
		t.Errorf("frames=10 == threshold=10 no reset; want reset")
	}
	if !w.shouldResetBackoff(1*time.Second, 100) {
		t.Errorf("frames=100 > threshold=10 no reset; want reset")
	}
}

// TestWorker_BackoffLegacyGateOnSentinelZero confirms BackoffDataThreshold=0
// restores the pre-Slice-2 reset condition: dur > stableThreshold (60s).
func TestWorker_BackoffLegacyGateOnSentinelZero(t *testing.T) {
	t.Parallel()
	w := New(Deps{
		WS: &fakeWS{frames: make(chan kalshi.Frame, 1)}, REST: newREST("KXBTCD"),
		Pub: &stubFanout{}, Series: []string{"KXBTCD"}, Bookkeeper: newTestBookkeeper(t),
		BackoffDataThreshold: 0, // sentinel → legacy fallback path
	})
	if !w.shouldResetBackoff(61*time.Second, 0) {
		t.Errorf("legacy: dur=61s > stableThreshold=60s no reset; want reset (frames irrelevant)")
	}
	if w.shouldResetBackoff(60*time.Second, 1000) {
		t.Errorf("legacy: dur=60s !> stableThreshold=60s reset; want NO reset (frames irrelevant)")
	}
}

// TestWorker_BackoffLadderCapsAtMaxBackoff confirms the sleep-delay function
// caps at MaxBackoff and traces the expected exp ladder up to attempt 10.
// MaxBackoff defaults to 600s in worker.New (this slice).
func TestWorker_BackoffLadderCapsAtMaxBackoff(t *testing.T) {
	t.Parallel()
	w := New(Deps{
		WS: &fakeWS{frames: make(chan kalshi.Frame, 1)}, REST: newREST("KXBTCD"),
		Pub: &stubFanout{}, Series: []string{"KXBTCD"}, Bookkeeper: newTestBookkeeper(t),
		BaseBackoff: 1 * time.Second,
		MaxBackoff:  600 * time.Second,
	})
	cases := []struct {
		attempt int
		want    time.Duration
	}{
		{0, 1 * time.Second},
		{1, 2 * time.Second},
		{2, 4 * time.Second},
		{3, 8 * time.Second},
		{4, 16 * time.Second},
		{5, 32 * time.Second},
		{6, 64 * time.Second},
		{7, 128 * time.Second},
		{8, 256 * time.Second},
		{9, 512 * time.Second},
		{10, 600 * time.Second},
		{15, 600 * time.Second},
	}
	for _, tc := range cases {
		t.Run(fmt.Sprintf("attempt=%d", tc.attempt), func(t *testing.T) {
			if got := w.backoffDelay(tc.attempt); got != tc.want {
				t.Errorf("backoffDelay(%d) = %v; want %v", tc.attempt, got, tc.want)
			}
		})
	}
}

// TestWorker_DefaultMaxBackoffIs600s pins the new default introduced by
// this slice. Prior default was 60s.
func TestWorker_DefaultMaxBackoffIs600s(t *testing.T) {
	t.Parallel()
	w := New(Deps{
		WS: &fakeWS{frames: make(chan kalshi.Frame, 1)}, REST: newREST("KXBTCD"),
		Pub: &stubFanout{}, Series: []string{"KXBTCD"}, Bookkeeper: newTestBookkeeper(t),
	})
	if got := w.maxBackoff; got != 600*time.Second {
		t.Errorf("default MaxBackoff = %v; want 600s", got)
	}
}

// TestDataStallLoop_DataTimeout_Fires asserts the loop force-closes when
// the inbound data-frame atomic is older than the deadline.
func TestDataStallLoop_DataTimeout_Fires(t *testing.T) {
	t.Parallel()

	ws := &fakeWS{frames: make(chan kalshi.Frame, 4)}

	w := New(Deps{
		WS:                    ws,
		Pub:                   &stubFanout{},
		REST:                  newREST("KXBTCD"),
		Series:                []string{"KXBTCD"},
		Bookkeeper:            newTestBookkeeper(t),
		Logger:                slog.New(slog.NewJSONHandler(&bytes.Buffer{}, nil)),
		DataStallEnabled:      true,
		DataFrameDeadline:     50 * time.Millisecond,
		DataStallTickInterval: 10 * time.Millisecond,
	})
	w.lastTradingDataAt.Store(time.Now().Add(-1 * time.Second).UnixNano())

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	done := make(chan struct{})
	go func() { w.dataStallLoop(ctx); close(done) }()

	select {
	case <-done:
	case <-time.After(300 * time.Millisecond):
		t.Fatal("dataStallLoop did not return within 300ms after stall")
	}

	if got := ws.closeCalls.Load(); got != 1 {
		t.Errorf("closeCalls = %d; want 1", got)
	}
}

// TestDataStallLoop_DataFresh_NoFire asserts the loop does NOT close when
// the data timestamp stays fresh under continuous refresh.
func TestDataStallLoop_DataFresh_NoFire(t *testing.T) {
	t.Parallel()

	ws := &fakeWS{frames: make(chan kalshi.Frame, 4)}
	w := New(Deps{
		WS:                    ws,
		Pub:                   &stubFanout{},
		REST:                  newREST("KXBTCD"),
		Series:                []string{"KXBTCD"},
		Bookkeeper:            newTestBookkeeper(t),
		Logger:                slog.New(slog.NewJSONHandler(&bytes.Buffer{}, nil)),
		DataStallEnabled:      true,
		DataFrameDeadline:     500 * time.Millisecond,
		DataStallTickInterval: 10 * time.Millisecond,
	})

	// Seed lastTradingDataAt to now so the first tick (10ms) does not fire before
	// the refresh goroutine has a chance to run.
	w.lastTradingDataAt.Store(time.Now().UnixNano())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	refreshDone := make(chan struct{})
	go func() {
		defer close(refreshDone)
		tick := time.NewTicker(20 * time.Millisecond)
		defer tick.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-tick.C:
				w.lastTradingDataAt.Store(time.Now().UnixNano())
			}
		}
	}()

	done := make(chan struct{})
	go func() { w.dataStallLoop(ctx); close(done) }()

	time.Sleep(200 * time.Millisecond)
	cancel()
	<-done
	<-refreshDone

	if got := ws.closeCalls.Load(); got != 0 {
		t.Errorf("closeCalls = %d; want 0 (no fire when data is fresh)", got)
	}
}

// TestDataStallLoop_CtxCancelMidStale asserts that if sessionCtx is canceled
// while the data timestamp is stale, the loop returns cleanly without
// force-closing or incrementing the counter.
func TestDataStallLoop_CtxCancelMidStale(t *testing.T) {
	t.Parallel()

	ws := &fakeWS{frames: make(chan kalshi.Frame, 4)}

	w := New(Deps{
		WS:                    ws,
		Pub:                   &stubFanout{},
		REST:                  newREST("KXBTCD"),
		Series:                []string{"KXBTCD"},
		Bookkeeper:            newTestBookkeeper(t),
		Logger:                slog.New(slog.NewJSONHandler(&bytes.Buffer{}, nil)),
		DataStallEnabled:      true,
		DataFrameDeadline:     50 * time.Millisecond,
		DataStallTickInterval: 200 * time.Millisecond, // first tick is 200ms out
	})
	w.lastTradingDataAt.Store(time.Now().Add(-1 * time.Second).UnixNano())

	ctx, cancel := context.WithCancel(context.Background())
	// Cancel BEFORE the first tick fires.
	cancel()

	done := make(chan struct{})
	go func() { w.dataStallLoop(ctx); close(done) }()

	select {
	case <-done:
	case <-time.After(300 * time.Millisecond):
		t.Fatal("dataStallLoop did not return after ctx cancel")
	}

	if got := ws.closeCalls.Load(); got != 0 {
		t.Errorf("closeCalls = %d; want 0 (clean shutdown, no false fire)", got)
	}
}

// recordingBookkeeper wraps a real Bookkeeper and records calls to
// RetainOnly / ApplySnapshot / DeleteMarket / Reset so tests can assert
// the worker invokes them at the right moments.
type recordingBookkeeper struct {
	*orderbook.Bookkeeper
	mu               sync.Mutex
	retainCalls      [][]string
	deleteMarketKeys []string
	resetCalls       atomic.Int64
}

func newRecordingBookkeeper(t *testing.T) *recordingBookkeeper {
	t.Helper()
	return &recordingBookkeeper{Bookkeeper: newTestBookkeeper(t)}
}

func (r *recordingBookkeeper) RetainOnly(keep []string) {
	r.mu.Lock()
	r.retainCalls = append(r.retainCalls, append([]string(nil), keep...))
	r.mu.Unlock()
	r.Bookkeeper.RetainOnly(keep)
}

func (r *recordingBookkeeper) ApplySnapshot(msg orderbook.SnapshotMsg) {
	r.Bookkeeper.ApplySnapshot(msg)
}

func (r *recordingBookkeeper) DeleteMarket(ticker string) {
	r.mu.Lock()
	r.deleteMarketKeys = append(r.deleteMarketKeys, ticker)
	r.mu.Unlock()
	r.Bookkeeper.DeleteMarket(ticker)
}

func (r *recordingBookkeeper) Reset() {
	r.resetCalls.Add(1)
	r.Bookkeeper.Reset()
}

func (r *recordingBookkeeper) snapshotRetain() [][]string {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([][]string, len(r.retainCalls))
	for i, c := range r.retainCalls {
		out[i] = append([]string(nil), c...)
	}
	return out
}

func (r *recordingBookkeeper) snapshotDeleteMarkets() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.deleteMarketKeys...)
}

func TestWorker_RetainOnlyCalledPostResubscribe(t *testing.T) {
	t.Parallel()
	ws := &fakeWS{frames: make(chan kalshi.Frame, 4)}
	close(ws.frames)

	rest := newREST("KXBTCD", "KXBTCD-T1", "KXBTCD-T2")
	bk := newRecordingBookkeeper(t)
	w := New(Deps{
		WS: ws, Pub: &stubFanout{}, REST: rest,
		Series:             []string{"KXBTCD"},
		Bookkeeper:         bk,
		Logger:             slog.New(slog.NewJSONHandler(&bytes.Buffer{}, nil)),
		PreserveBookkeeper: true,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	_ = w.runOnce(ctx)

	calls := bk.snapshotRetain()
	if len(calls) < 1 {
		t.Fatalf("RetainOnly was not called; want >= 1 invocation post-populate")
	}
	got := map[string]bool{}
	for _, k := range calls[0] {
		got[k] = true
	}
	if !got["KXBTCD-T1"] || !got["KXBTCD-T2"] {
		t.Errorf("RetainOnly keep set = %v; want both KXBTCD-T1 and KXBTCD-T2", calls[0])
	}
}

func TestWorker_PreserveBookkeeper_FalseStillCallsReset(t *testing.T) {
	t.Parallel()
	ws := &fakeWS{frames: make(chan kalshi.Frame, 4)}
	close(ws.frames)

	rest := newREST("KXBTCD", "KXBTCD-T1")
	bk := newRecordingBookkeeper(t)
	w := New(Deps{
		WS: ws, Pub: &stubFanout{}, REST: rest,
		Series:             []string{"KXBTCD"},
		Bookkeeper:         bk,
		Logger:             slog.New(slog.NewJSONHandler(&bytes.Buffer{}, nil)),
		PreserveBookkeeper: false,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	_ = w.runOnce(ctx)

	if got := bk.resetCalls.Load(); got < 1 {
		t.Errorf("Bookkeeper.Reset calls = %d; want >= 1 in legacy path", got)
	}
	if calls := bk.snapshotRetain(); len(calls) != 0 {
		t.Errorf("RetainOnly calls = %d; want 0 in legacy path", len(calls))
	}
}

func TestWorker_PreserveBookkeeper_TrueSkipsReset(t *testing.T) {
	t.Parallel()
	ws := &fakeWS{frames: make(chan kalshi.Frame, 4)}
	close(ws.frames)

	rest := newREST("KXBTCD", "KXBTCD-T1")
	bk := newRecordingBookkeeper(t)
	w := New(Deps{
		WS: ws, Pub: &stubFanout{}, REST: rest,
		Series:             []string{"KXBTCD"},
		Bookkeeper:         bk,
		Logger:             slog.New(slog.NewJSONHandler(&bytes.Buffer{}, nil)),
		PreserveBookkeeper: true,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	_ = w.runOnce(ctx)

	if got := bk.resetCalls.Load(); got != 0 {
		t.Errorf("Bookkeeper.Reset calls = %d; want 0 (preserve=true must skip Reset)", got)
	}
}

type stubBookkeeper struct {
	mu               sync.Mutex
	deletedMarkets   []string
	appliedSnapshots []string
}

func (s *stubBookkeeper) Run(ctx context.Context)    { <-ctx.Done() }
func (s *stubBookkeeper) Apply(_ orderbook.DeltaMsg) {}
func (s *stubBookkeeper) Reset()                     {}
func (s *stubBookkeeper) RetainOnly(_ []string)      {}
func (s *stubBookkeeper) ApplySnapshot(msg orderbook.SnapshotMsg) {
	s.mu.Lock()
	s.appliedSnapshots = append(s.appliedSnapshots, msg.Ticker)
	s.mu.Unlock()
	if msg.Done != nil {
		close(msg.Done)
	}
}
func (s *stubBookkeeper) DeleteMarket(ticker string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.deletedMarkets = append(s.deletedMarkets, ticker)
}
func (s *stubBookkeeper) deleted() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]string, len(s.deletedMarkets))
	copy(out, s.deletedMarkets)
	return out
}

func TestWorker_SweepTrimDeletesFromBookkeeper(t *testing.T) {
	t.Parallel()
	ws := &fakeWS{frames: make(chan kalshi.Frame, 4)}
	pub := &stubFanout{}
	rest := newREST("KXBTCD", "KXBTCD-A")
	bk := &stubBookkeeper{}

	w := New(Deps{
		WS:                 ws,
		REST:               rest,
		Pub:                pub,
		Series:             []string{"KXBTCD"},
		Bookkeeper:         bk,
		PreserveBookkeeper: true,
		SweepInterval:      20 * time.Millisecond,
		SweepJitter:        1 * time.Millisecond,
		SweepMissThreshold: 1, // trim on first miss for fast test
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() { _ = w.Run(ctx) }()

	// Wait until the initial population subscribes KXBTCD-A.
	testutil.WaitFor(t, time.Second, "initial sub", func() bool {
		return ws.subCalls.Load() >= 2
	})

	// Drop KXBTCD-A from REST so the next sweep tick trims it.
	rest.mu.Lock()
	rest.byName["KXBTCD"] = nil
	rest.mu.Unlock()

	testutil.WaitFor(t, 2*time.Second, "sweep trim deletes from bookkeeper", func() bool {
		for _, t := range bk.deleted() {
			if t == "KXBTCD-A" {
				return true
			}
		}
		return false
	})
}

func TestWorker_SweepTrimEvictsBookkeeperMarket(t *testing.T) {
	t.Parallel()
	ws := &fakeWS{frames: make(chan kalshi.Frame, 4)}

	// REST returns only KXBTCD-T1; pre-populate roster with a stale ticker
	// so SweepDiff trims it. The trim path must call Bookkeeper.DeleteMarket
	// for the stale ticker.
	rest := newREST("KXBTCD", "KXBTCD-T1")
	bk := newRecordingBookkeeper(t)
	w := New(Deps{
		WS: ws, Pub: &stubFanout{}, REST: rest,
		Series:             []string{"KXBTCD"},
		Bookkeeper:         bk,
		Logger:             slog.New(slog.NewJSONHandler(&bytes.Buffer{}, nil)),
		SweepInterval:      0, // disable the loop; drive sweepOnce directly
		SweepMissThreshold: 1, // trim immediately on first miss
		PreserveBookkeeper: true,
	})

	w.roster.AddPending(1, []string{"KXBTCD-STALE"}, []string{"orderbook_delta"})
	w.roster.RecordSid(1, 99)

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	w.sweepOnce(ctx)

	got := bk.snapshotDeleteMarkets()
	found := false
	for _, k := range got {
		if k == "KXBTCD-STALE" {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("Bookkeeper.DeleteMarket calls = %v; want to include KXBTCD-STALE after sweep trim", got)
	}
}

func TestLiveBucket_FrameTypeRouting(t *testing.T) {
	t.Parallel()
	cases := []struct {
		frameType string
		want      bucket
	}{
		{"orderbook_snapshot", bucketTrading},
		{"orderbook_delta", bucketTrading},
		{"trade", bucketTrading},
		{"market_lifecycle_v2", bucketLifecycle},
		{"unknown_type_settlement", bucketLifecycle},
		{"future_channel_x", bucketLifecycle},
		{"", bucketLifecycle},
	}
	for _, tc := range cases {
		t.Run(tc.frameType, func(t *testing.T) {
			if got := liveBucket(tc.frameType); got != tc.want {
				t.Errorf("liveBucket(%q) = %v; want %v", tc.frameType, got, tc.want)
			}
		})
	}
}

// TestDataStall_LifecycleOnly_Trips drives lifecycle frames through the
// read-loop while no trading frames arrive. The data-stall watchdog should
// fire on lastTradingDataAt staleness regardless of how recent
// lastLifecycleAt is. This is the 2026-05-08 silent-stall failure mode.
func TestDataStall_LifecycleOnly_Trips(t *testing.T) {
	t.Parallel()

	ws := &fakeWS{frames: make(chan kalshi.Frame, 4)}
	w := New(Deps{
		WS:                    ws,
		Pub:                   &stubFanout{},
		REST:                  newREST("KXBTCD"),
		Series:                []string{"KXBTCD"},
		Bookkeeper:            newTestBookkeeper(t),
		Logger:                slog.New(slog.NewJSONHandler(&bytes.Buffer{}, nil)),
		DataStallEnabled:      true,
		DataFrameDeadline:     50 * time.Millisecond,
		DataStallTickInterval: 10 * time.Millisecond,
	})
	w.lastTradingDataAt.Store(time.Now().Add(-1 * time.Second).UnixNano())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	refreshDone := make(chan struct{})
	go func() {
		defer close(refreshDone)
		tick := time.NewTicker(5 * time.Millisecond)
		defer tick.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-tick.C:
				w.lastLifecycleAt.Store(time.Now().UnixNano())
			}
		}
	}()

	done := make(chan struct{})
	go func() { w.dataStallLoop(ctx); close(done) }()

	select {
	case <-done:
	case <-time.After(300 * time.Millisecond):
		t.Fatal("dataStallLoop did not return within 300ms despite trading staleness")
	}

	cancel()
	<-refreshDone

	if got := ws.closeCalls.Load(); got != 1 {
		t.Errorf("closeCalls = %d; want 1 (watchdog must trip on trading staleness even with fresh lifecycle)", got)
	}
}

// TestDataStall_PanicRecovered asserts that a panic in dataStallLoop is
// recovered: the panic counter increments, WS.Close is called, and the
// loop returns cleanly. The panic is injected via the test-only
// panicNextTick hook so the prod tick path is unchanged.
func TestDataStall_PanicRecovered(t *testing.T) {
	t.Parallel()

	ws := &fakeWS{frames: make(chan kalshi.Frame, 4)}
	w := New(Deps{
		WS:                    ws,
		Pub:                   &stubFanout{},
		REST:                  newREST("KXBTCD"),
		Series:                []string{"KXBTCD"},
		Bookkeeper:            newTestBookkeeper(t),
		Logger:                slog.New(slog.NewJSONHandler(&bytes.Buffer{}, nil)),
		DataStallEnabled:      true,
		DataFrameDeadline:     1 * time.Second,
		DataStallTickInterval: 10 * time.Millisecond,
	})
	w.lastTradingDataAt.Store(time.Now().UnixNano())
	w.lastLifecycleAt.Store(time.Now().UnixNano())
	w.panicNextTick.Store(true)

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	done := make(chan struct{})
	go func() { w.dataStallLoop(ctx); close(done) }()

	select {
	case <-done:
	case <-time.After(300 * time.Millisecond):
		t.Fatal("dataStallLoop did not return within 300ms after panic")
	}

	if got := ws.closeCalls.Load(); got != 1 {
		t.Errorf("closeCalls = %d; want 1 (panic-recover must force-close WS)", got)
	}
}

// TestRunOnce_LogsStallConfig asserts runOnce emits an INFO log at
// session start with the data-stall watchdog config — proves the
// running revision's config from the worker's own log stream without
// needing the cmd/main.go boot log.
func TestRunOnce_LogsStallConfig(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo}))

	ws := &fakeWS{frames: make(chan kalshi.Frame, 1)}
	close(ws.frames)

	w := New(Deps{
		WS:                    ws,
		Pub:                   &stubFanout{},
		REST:                  newREST("KXBTCD"),
		Series:                []string{"KXBTCD"},
		Bookkeeper:            newTestBookkeeper(t),
		Logger:                logger,
		DataStallEnabled:      true,
		DataFrameDeadline:     90 * time.Second,
		DataStallTickInterval: 5 * time.Second,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	_ = w.runOnce(ctx)

	out := buf.String()
	if !strings.Contains(out, `"msg":"ws session config"`) {
		t.Errorf("missing session-start config log; got:\n%s", out)
	}
	if !strings.Contains(out, `"data_stall_enabled":true`) {
		t.Errorf("session-start log missing data_stall_enabled; got:\n%s", out)
	}
	if !strings.Contains(out, `"data_frame_deadline"`) {
		t.Errorf("session-start log missing data_frame_deadline; got:\n%s", out)
	}
	if !strings.Contains(out, `"data_stall_tick_interval"`) {
		t.Errorf("session-start log missing data_stall_tick_interval; got:\n%s", out)
	}
}

// TestReadLoop_UnknownFrameType_WarnsAndBucketsLifecycle asserts that an
// unknown inbound frame Type is bucketed as lifecycle (not trading) and
// triggers a single slog.Warn. Lock-step with the bucket helper: a
// future Kalshi channel addition lands in production logs at PR review
// time without silently keeping the watchdog blind.
func TestReadLoop_UnknownFrameType_WarnsAndBucketsLifecycle(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn}))

	ws := &fakeWS{frames: make(chan kalshi.Frame, 4)}
	ws.frames <- kalshi.Frame{Type: "future_channel_x", SID: 1, RawPayload: json.RawMessage(`{}`)}
	ws.frames <- kalshi.Frame{Type: "future_channel_x", SID: 2, RawPayload: json.RawMessage(`{}`)}
	close(ws.frames)

	w := New(Deps{
		WS:         ws,
		Pub:        &stubFanout{},
		REST:       newREST("KXBTCD"),
		Series:     []string{"KXBTCD"},
		Bookkeeper: newTestBookkeeper(t),
		Logger:     logger,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	_ = w.runOnce(ctx)

	out := buf.String()
	if !strings.Contains(out, `"frame_type":"future_channel_x"`) {
		t.Errorf("missing unknown-frame-type warn; got:\n%s", out)
	}
	if got := strings.Count(out, `"frame_type":"future_channel_x"`); got != 1 {
		t.Errorf("warn fired %d times; want 1 (sync.Once gate)", got)
	}

	if w.tradingFramesThisSession.Load() != 0 {
		t.Errorf("tradingFramesThisSession = %d; want 0 (unknown frames are lifecycle-bucketed)", w.tradingFramesThisSession.Load())
	}
}

// TestDataStall_MixedBurst_TradingBumpResetsDeadline drives a lifecycle
// burst, then a single orderbook_delta, then trading silence under the
// deadline. Asserts the watchdog does NOT trip during the lifecycle-only
// window if the trading frame has already arrived (deadline reset by
// the delta), and DOES trip after deadline elapses past the delta.
func TestDataStall_MixedBurst_TradingBumpResetsDeadline(t *testing.T) {
	t.Parallel()

	ws := &fakeWS{frames: make(chan kalshi.Frame, 4)}
	w := New(Deps{
		WS:                    ws,
		Pub:                   &stubFanout{},
		REST:                  newREST("KXBTCD"),
		Series:                []string{"KXBTCD"},
		Bookkeeper:            newTestBookkeeper(t),
		Logger:                slog.New(slog.NewJSONHandler(&bytes.Buffer{}, nil)),
		DataStallEnabled:      true,
		DataFrameDeadline:     100 * time.Millisecond,
		DataStallTickInterval: 10 * time.Millisecond,
	})
	now := time.Now()
	w.lastTradingDataAt.Store(now.UnixNano())
	w.lastLifecycleAt.Store(now.UnixNano())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tradingBumpAt := now.Add(50 * time.Millisecond)
	go func() {
		tick := time.NewTicker(5 * time.Millisecond)
		defer tick.Stop()
		bumped := false
		for {
			select {
			case <-ctx.Done():
				return
			case tt := <-tick.C:
				w.lastLifecycleAt.Store(tt.UnixNano())
				if !bumped && !tt.Before(tradingBumpAt) {
					w.lastTradingDataAt.Store(tt.UnixNano())
					bumped = true
				}
			}
		}
	}()

	done := make(chan struct{})
	go func() { w.dataStallLoop(ctx); close(done) }()

	select {
	case <-done:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("dataStallLoop did not trip after trading silence past deadline")
	}

	if got := ws.closeCalls.Load(); got != 1 {
		t.Errorf("closeCalls = %d; want 1 (trip after the post-delta silence window exceeds deadline)", got)
	}
}

// TestWorker_TradingDeltaFramesThisSessionExcludesSnapshot is the HOL-52
// Bug A regression guard. The data-stall watchdog counter
// (tradingFramesThisSession) still bumps on snapshot frames so the protocol
// layer is treated as alive across the ~95s post-subscribe window; the
// backoff-gate counter (tradingDeltaFramesThisSession) must NOT bump on
// snapshot frames, so a snapshot-only session correctly fails the gate
// and lets the exp ladder escalate.
func TestWorker_TradingDeltaFramesThisSessionExcludesSnapshot(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name     string
		frame    kalshi.Frame
		wantBump bool
	}{
		{"orderbook_snapshot_no_bump", kalshi.Frame{Type: "orderbook_snapshot", SID: 1, RawPayload: json.RawMessage(`{"market_ticker":"KXBTCD-A","yes":[],"no":[]}`)}, false},
		{"orderbook_delta_bumps", kalshi.Frame{Type: "orderbook_delta", SID: 1, RawPayload: json.RawMessage(`{"market_ticker":"KXBTCD-A","side":"yes","price_dollars":"0.7700","delta_fp":"100.00","ts_ms":1714400000000}`)}, true},
		{"trade_bumps", kalshi.Frame{Type: "trade", SID: 1, RawPayload: json.RawMessage(`{"market_ticker":"KXBTCD-A","yes_price_dollars":"0.5","ts_ms":1714400000000}`)}, true},
		{"market_lifecycle_v2_no_bump", kalshi.Frame{Type: "market_lifecycle_v2", SID: 1, RawPayload: json.RawMessage(`{"market_ticker":"KXBTCD-A"}`)}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ws := &fakeWS{frames: make(chan kalshi.Frame, 2)}
			ws.frames <- tc.frame
			close(ws.frames)

			w := New(Deps{
				WS:         ws,
				Pub:        &stubFanout{},
				REST:       newREST("KXBTCD"),
				Series:     []string{"KXBTCD"},
				Bookkeeper: newTestBookkeeper(t),
				Logger:     slog.New(slog.NewJSONHandler(&bytes.Buffer{}, nil)),
			})
			ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
			defer cancel()
			_ = w.runOnce(ctx)

			got := w.tradingDeltaFramesThisSession.Load() > 0
			if got != tc.wantBump {
				t.Errorf("tradingDeltaFramesThisSession>0 = %v; want %v (frame.Type=%s)", got, tc.wantBump, tc.frame.Type)
			}
		})
	}
}

// TestWorker_SnapshotOnlySession_BackoffGateAndHeartbeat is the HOL-52
// compound-bug regression. It drives N orderbook_snapshot frames and
// zero deltas through the worker (the exact 2026-05-14 wire shape) and
// asserts BOTH the backoff gate counter stays at zero (Bug A) AND the
// bookkeeper emits populated payloads (Bug B). A failing assertion on
// either is a regression of HOL-52.
func TestWorker_SnapshotOnlySession_BackoffGateAndHeartbeat(t *testing.T) {
	t.Parallel()

	type emit struct {
		ticker string
		side   orderbook.Side
		top1   int64
	}
	var emits []emit
	var emitsMu sync.Mutex
	bk := orderbook.NewBookkeeper(orderbook.BookkeeperConfig{
		Publish: func(ticker string, side orderbook.Side, p orderbook.SnapshotPayload) error {
			body, err := p.Marshal()
			if err != nil {
				return err
			}
			var pb orderbookpb.SnapshotPayload
			if proto.Unmarshal(body, &pb) != nil {
				return nil
			}
			emitsMu.Lock()
			emits = append(emits, emit{ticker: ticker, side: side, top1: pb.Top1PriceUnits})
			emitsMu.Unlock()
			return nil
		},
		ChangeTickInterval: 25 * time.Millisecond,
		HeartbeatInterval:  75 * time.Millisecond,
	})

	tickers := []string{"KXBTCD-A", "KXBTCD-B", "KXBTCD-C"}

	ws := &fakeWS{frames: make(chan kalshi.Frame, len(tickers)+1)}
	for i, tk := range tickers {
		ws.frames <- kalshi.Frame{
			Type: "orderbook_snapshot", SID: int64(i + 1), Seq: int64(i + 1),
			RawPayload: json.RawMessage(`{"market_ticker":"` + tk + `","yes":[["0.7700",100]],"no":[["0.2300",50]]}`),
		}
	}

	pub := &stubFanout{}
	rest := newREST("KXBTCD", tickers...)
	w := New(Deps{
		WS: ws, Pub: pub, REST: rest,
		Series:               []string{"KXBTCD"},
		Bookkeeper:           bk,
		BackoffDataThreshold: 10,
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = w.Run(ctx) }()
	testutil.WaitFor(t, time.Second, "open + subscribes", func() bool {
		return ws.openCalls.Load() > 0 && ws.subCalls.Load() >= 2
	})

	testutil.WaitFor(t, 2*time.Second, "all three tickers have a YES emit at top1=7700", func() bool {
		emitsMu.Lock()
		defer emitsMu.Unlock()
		seen := map[string]bool{}
		for _, e := range emits {
			if e.side == orderbook.SideYes && e.top1 == 7700 {
				seen[e.ticker] = true
			}
		}
		return seen["KXBTCD-A"] && seen["KXBTCD-B"] && seen["KXBTCD-C"]
	})

	if got := w.tradingDeltaFramesThisSession.Load(); got != 0 {
		t.Errorf("tradingDeltaFramesThisSession = %d after snapshot-only session; want 0 (Bug A regression)", got)
	}
	if w.shouldResetBackoff(95*time.Second, 0) {
		t.Errorf("shouldResetBackoff(95s, 0) returned true; want false at threshold=10 (Bug A regression)")
	}
	if got := w.tradingFramesThisSession.Load(); got < int64(len(tickers)) {
		t.Errorf("tradingFramesThisSession = %d; want >= %d (snapshots still stamp data-stall watchdog)", got, len(tickers))
	}
}
