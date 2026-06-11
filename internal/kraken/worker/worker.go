package worker

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coder/websocket"
	"golang.org/x/time/rate"

	"github.com/paulschick/kalshiflow-pipeline/internal/envelope"
	"github.com/paulschick/kalshiflow-pipeline/internal/featureflag"
	"github.com/paulschick/kalshiflow-pipeline/internal/kraken/control"
	krob "github.com/paulschick/kalshiflow-pipeline/internal/kraken/orderbook"
	"github.com/paulschick/kalshiflow-pipeline/internal/kraken/rest"
	"github.com/paulschick/kalshiflow-pipeline/internal/kraken/symbol"
	"github.com/paulschick/kalshiflow-pipeline/internal/kraken/wsv2"
)

// BackoffPolicy controls reconnect timing.
type BackoffPolicy struct {
	Initial    time.Duration // 1s default
	Max        time.Duration // 600s default
	JitterFrac float64       // 0.10 default = ±10%
}

// SubscriptionLogger writes per-pair rejection rows to
// kraken_raw.subscription_log when the worker-side dynamic preflight rejects
// a pair (e.g. operator typo in ops:kraken:add). The cmd-layer wraps a BQ
// Inserter; tests inject a stub. Nil disables the rejection-logging path.
type SubscriptionLogger interface {
	LogRejection(ctx context.Context, pair, reason, actor string) error
}

// Config wires the worker.
type Config struct {
	Flag         *featureflag.KrakenFlag
	REST         *rest.Client
	Open         func(ctx context.Context, url string) (wsv2.Conn, error)
	WSURL        string
	Bookkeeper   *krob.Bookkeeper
	Pairs        []string
	SilentStall  time.Duration
	StallTick    time.Duration // watchdog poll cadence; default 200ms
	FlagPoll     time.Duration // inner flag-watch cadence; default 1s
	Backoff      BackoffPolicy
	Metrics      Metrics
	PublishTrade func(ctx context.Context, body []byte) error
	Now          func() time.Time
	Logger       *slog.Logger

	// DesiredSet is the optional GCS-backed pair-set loader. When non-nil,
	// the worker spawns a reconciler goroutine on the root ctx that polls
	// DesiredSet on PollInterval ± PollJitter and calls AddPair / RemovePair
	// for the delta. Nil disables the reconciler (DR mode — pair set comes
	// only from cfg.Pairs / env at boot).
	DesiredSet   control.DesiredSetLoader
	PollInterval time.Duration // default 60s
	PollJitter   time.Duration // default 10s

	// SubscriptionLog is invoked when AddPair's per-pair preflight rejects
	// a pair (e.g. operator typo). Nil disables the BQ-log path; the
	// rejection is still logged via slog.
	SubscriptionLog SubscriptionLogger
}

// Worker is the long-lived supervisor.
type Worker struct {
	cfg Config
	log *slog.Logger

	mu        sync.Mutex
	lastFrame time.Time

	resubMu       sync.Mutex
	resubLimiters map[string]*rate.Limiter

	// pairs is the live, race-safe subscription universe. Snapshotted at
	// session start for the subscribe broadcast; live-mutated by AddPair /
	// RemovePair after the WS subscribe / unsubscribe completes.
	pairs *pairSet

	// wsMu serializes WS writes (Subscribe/Unsubscribe) against AddPair /
	// RemovePair on the active conn. coder/websocket.Conn.Write is
	// internally concurrent-safe, but keeping the higher-level surface
	// serialized makes the ack-correlation simpler and mirrors the Kalshi
	// internal/worker pattern (wsMu).
	wsMu        sync.Mutex
	currentConn atomic.Pointer[wsv2.Conn]

	// lastDesiredGen carries the last-observed GCS generation across
	// reconciler polls so the skip-gate is preserved on success.
	lastDesiredGen atomic.Int64
}

// New constructs a Worker.
func New(cfg Config) *Worker {
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	if cfg.SilentStall == 0 {
		cfg.SilentStall = 60 * time.Second
	}
	if cfg.StallTick == 0 {
		cfg.StallTick = 200 * time.Millisecond
	}
	if cfg.FlagPoll == 0 {
		cfg.FlagPoll = 1 * time.Second
	}
	if cfg.Backoff.Initial == 0 {
		cfg.Backoff.Initial = 1 * time.Second
	}
	if cfg.Backoff.Max == 0 {
		cfg.Backoff.Max = 600 * time.Second
	}
	if cfg.Backoff.JitterFrac == 0 {
		cfg.Backoff.JitterFrac = 0.10
	}
	if cfg.PollInterval == 0 {
		cfg.PollInterval = 60 * time.Second
		// Only inject the default jitter when the caller accepted the
		// default interval; explicit interval with zero jitter is a
		// legitimate request (tests and deterministic deploys want it).
		if cfg.PollJitter == 0 {
			cfg.PollJitter = 10 * time.Second
		}
	}
	return &Worker{
		cfg:           cfg,
		log:           cfg.Logger,
		resubLimiters: make(map[string]*rate.Limiter),
		pairs:         newPairSet(cfg.Pairs),
	}
}

// Pairs returns a snapshot of the current subscription universe. Race-safe.
func (w *Worker) Pairs() []string { return w.pairs.Load() }

// allowResub consults the per-symbol token bucket (1 token / 60s). Returns
// true if a resub is permitted for sym; false if rate-limited. Limiters are
// lazily created on first reference and persist across reconnects.
func (w *Worker) allowResub(sym string) bool {
	w.resubMu.Lock()
	lim, ok := w.resubLimiters[sym]
	if !ok {
		lim = rate.NewLimiter(rate.Every(60*time.Second), 1)
		w.resubLimiters[sym] = lim
	}
	w.resubMu.Unlock()
	return lim.Allow()
}

// Run blocks until ctx cancellation. Re-enters connect cycle on kill-switch
// false → true and on any inner Run() exit. Returns ctx.Err() on normal stop.
func (w *Worker) Run(ctx context.Context) error {
	// Spawn the desired-set reconciler on the root ctx so it survives WS
	// reconnects and only stops when Run itself stops.
	if w.cfg.DesiredSet != nil {
		go w.runDesiredSetReconciler(ctx)
	}
	attempt := 0
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if !w.cfg.Flag.Enabled() {
			w.log.Info("kraken_flag_disabled; awaiting enable")
			w.cfg.Metrics.FlagEnabled.Record(ctx, 0)
			if !w.waitForEnable(ctx) {
				return ctx.Err()
			}
		}
		w.cfg.Metrics.FlagEnabled.Record(ctx, 1)
		if err := w.preflight(ctx); err != nil {
			w.log.Error("kraken_preflight_failed", "err", err)
			return fmt.Errorf("worker: preflight: %w", err)
		}
		inner, cancelInner := context.WithCancel(ctx)
		watcherDone := w.watchFlag(inner, cancelInner)
		err := w.runOnce(inner)
		cancelInner()
		<-watcherDone
		if errors.Is(err, context.Canceled) {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			attempt = 0
			continue
		}
		w.cfg.Metrics.WSReconnects.Add(ctx, 1)
		d := w.backoff(attempt)
		attempt++
		w.log.Warn("ws_session_ended; reconnecting", "attempt", attempt, "sleep", d.String(), "err", err)
		select {
		case <-time.After(d):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// waitForEnable polls the flag once per FlagPoll until enabled or ctx canceled.
func (w *Worker) waitForEnable(ctx context.Context) bool {
	t := time.NewTicker(w.cfg.FlagPoll)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return false
		case <-t.C:
			if w.cfg.Flag.Enabled() {
				return true
			}
		}
	}
}

// watchFlag spawns a goroutine that cancels the inner ctx if the flag flips
// to disabled. Returns a channel that closes when the watcher exits.
func (w *Worker) watchFlag(ctx context.Context, cancel context.CancelFunc) <-chan struct{} {
	done := make(chan struct{})
	go func() {
		defer close(done)
		t := time.NewTicker(w.cfg.FlagPoll)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				if !w.cfg.Flag.Enabled() {
					w.log.Info("kraken_flag_disabled; canceling inner ctx")
					w.cfg.Metrics.FlagEnabled.Record(context.Background(), 0)
					cancel()
					return
				}
			}
		}
	}()
	return done
}

func (w *Worker) preflight(ctx context.Context) error {
	pairs := w.pairs.Load()
	if len(pairs) == 0 {
		// Empty pair set on boot is a config bug; the cmd layer should have
		// rejected this. Defensive guard.
		return errors.New("no pairs configured")
	}
	pairInfos, err := w.cfg.REST.AssetPairs(ctx, pairs)
	if err != nil {
		return fmt.Errorf("AssetPairs: %w", err)
	}
	scale := make(map[string]krob.PairScale, len(pairInfos))
	for _, p := range pairs {
		info, ok := pairInfos[p]
		if !ok {
			return fmt.Errorf("AssetPairs result missing key %s", p)
		}
		if info.Status != "online" {
			return fmt.Errorf("pair %s status=%q; want online", p, info.Status)
		}
		scale[p] = krob.PairScale{PairDecimals: info.PairDecimals, LotDecimals: info.LotDecimals}
	}
	w.cfg.Bookkeeper.SetPairScale(scale)
	return nil
}

// runOnce manages one WS session: connect, subscribe, readloop. Returns when
// the session ends (err) or the context is canceled (ctx.Err()).
func (w *Worker) runOnce(ctx context.Context) error {
	conn, err := w.cfg.Open(ctx, w.cfg.WSURL)
	if err != nil {
		return fmt.Errorf("open: %w", err)
	}
	w.currentConn.Store(&conn)
	defer func() {
		w.currentConn.Store(nil)
		_ = conn.Close(websocket.StatusNormalClosure, "")
	}()
	// Snapshot the pair set under wsMu so a concurrent AddPair / RemovePair
	// (from the reconciler) observes a consistent post-subscribe state.
	w.wsMu.Lock()
	pairs := w.pairs.Load()
	if err := conn.Subscribe(ctx, "book", pairs, wsv2.SubscribeOpts{Depth: krob.KrakenBookDepth, Snapshot: true}); err != nil {
		w.wsMu.Unlock()
		return fmt.Errorf("subscribe book: %w", err)
	}
	if err := conn.Subscribe(ctx, "trade", pairs, wsv2.SubscribeOpts{Snapshot: true}); err != nil {
		w.wsMu.Unlock()
		return fmt.Errorf("subscribe trade: %w", err)
	}
	w.wsMu.Unlock()
	w.bumpLastFrame()
	stallCtx, stallCancel := context.WithCancel(ctx)
	defer stallCancel()
	stallDone := make(chan struct{})
	go w.runStallWatchdog(stallCtx, conn, stallDone)

	resubReq := make(chan string, 32)
	resubDone := make(chan struct{})
	go func() { defer close(resubDone); w.resubLoop(stallCtx, conn, resubReq) }()

	// Defer order is load-bearing: close(resubReq) runs after readLoop returns
	// (so dispatch can no longer non-blocking-send onto it), then waits for
	// resubLoop to drain remaining buffered items under stallCtx, then waits
	// for the stall watchdog. stallCancel runs last (deferred above) and
	// unwedges any resubLoop write blocked on a closed conn.
	defer func() {
		close(resubReq)
		<-resubDone
		<-stallDone
	}()

	return w.readLoop(ctx, conn, resubReq)
}

func (w *Worker) readLoop(ctx context.Context, conn wsv2.Conn, resubReq chan<- string) error {
	for {
		f, err := conn.Read(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return fmt.Errorf("read: %w", err)
		}
		w.bumpLastFrame()
		if err := w.dispatch(ctx, f, resubReq); err != nil {
			return err
		}
	}
}

func (w *Worker) dispatch(ctx context.Context, f wsv2.Frame, resubReq chan<- string) error {
	switch f.Kind {
	case wsv2.FrameStatus:
		w.cfg.Metrics.WSMessagesReceived.Add(ctx, 1, "channel", "status")
		if f.Status != nil {
			w.log.Info("kraken_status",
				"system", f.Status.System,
				"api_version", f.Status.APIVersion,
				"version", f.Status.Version,
				"connection_id", f.Status.ConnectionID.String(),
			)
		}
	case wsv2.FrameHeartbeat:
		w.cfg.Metrics.WSMessagesReceived.Add(ctx, 1, "channel", "heartbeat")
	case wsv2.FrameSubscribeAck:
		ack := f.SubscribeAck
		if ack == nil {
			return nil
		}
		if !ack.Success {
			w.cfg.Metrics.SubscribeRejected.Add(ctx, 1, "pair", ack.Symbol)
			return fmt.Errorf("subscribe rejected: %s (symbol=%s req_id=%d)", ack.Error, ack.Symbol, ack.ReqID)
		}
		w.log.Info("kraken_subscribe_ack", "channel", ack.Channel, "symbol", ack.Symbol, "req_id", ack.ReqID)
	case wsv2.FrameBookSnapshot:
		w.cfg.Metrics.WSMessagesReceived.Add(ctx, 1, "channel", "book")
		if f.Book == nil {
			return nil
		}
		done := make(chan struct{})
		w.cfg.Bookkeeper.ApplySnapshot(krob.SnapshotMsg{
			Symbol: f.Book.Symbol,
			Bids:   cloneLevels(f.Book.Bids),
			Asks:   cloneLevels(f.Book.Asks),
			Done:   done,
		})
		select {
		case <-done:
		case <-ctx.Done():
			return ctx.Err()
		}
	case wsv2.FrameBookUpdate:
		w.cfg.Metrics.WSMessagesReceived.Add(ctx, 1, "channel", "book")
		if f.Book == nil {
			return nil
		}
		done := make(chan struct{})
		w.cfg.Bookkeeper.ApplyUpdate(krob.UpdateMsg{
			Symbol: f.Book.Symbol,
			Bids:   cloneLevels(f.Book.Bids),
			Asks:   cloneLevels(f.Book.Asks),
			Done:   done,
		})
		select {
		case <-done:
		case <-ctx.Done():
			return ctx.Err()
		}
		if matched, computed := w.cfg.Bookkeeper.VerifyChecksumForSymbol(f.Book.Symbol, f.Book.Checksum); !matched {
			w.cfg.Metrics.ChecksumMismatches.Add(ctx, 1, "pair", f.Book.Symbol)
			w.logMismatch(f.Book.Symbol, f.Book.Checksum, computed, f.Raw)
			select {
			case resubReq <- f.Book.Symbol:
			default:
				w.cfg.Metrics.ResubDropped.Add(ctx, 1, "pair", f.Book.Symbol, "reason", "chan_full")
				w.log.Info("kraken_ws_resub_dropped", "symbol", f.Book.Symbol, "reason", "chan_full")
			}
		}
	case wsv2.FrameTradeSnapshot, wsv2.FrameTradeUpdate:
		w.cfg.Metrics.WSMessagesReceived.Add(ctx, 1, "channel", "trade")
		for _, tr := range f.Trades {
			if err := w.publishTrade(ctx, tr); err != nil {
				return fmt.Errorf("publish trade %s: %w", tr.Symbol, err)
			}
		}
	}
	return nil
}

func (w *Worker) publishTrade(ctx context.Context, tr wsv2.TradeFrame) error {
	if w.cfg.PublishTrade == nil {
		return nil
	}
	asset := symbol.KrakenPairToAsset[tr.Symbol]
	env := envelope.New("trade", asset, w.cfg.Now(), tr.RawJSON).
		WithKalshiTS(tr.Timestamp).
		WithContract(tr.Symbol, tr.Symbol)
	body, err := env.Marshal()
	if err != nil {
		return err
	}
	return w.cfg.PublishTrade(ctx, body)
}

func (w *Worker) resubSymbol(ctx context.Context, conn wsv2.Conn, sym string) error {
	if err := conn.Unsubscribe(ctx, "book", []string{sym}); err != nil {
		return err
	}
	return conn.Subscribe(ctx, "book", []string{sym}, wsv2.SubscribeOpts{Depth: krob.KrakenBookDepth, Snapshot: true})
}

// logMismatch emits a WARN log carrying the exact byte sequence the verifier
// hashed against the wire-supplied checksum, plus a prefix of the raw frame
// bytes. Telemetry-only — feeds the §5.5 algorithmic-fix follow-up.
func (w *Worker) logMismatch(sym string, expected, computed uint32, frameRaw []byte) {
	asks, bids := w.cfg.Bookkeeper.TopWireForSymbol(sym)
	wireBytes := krob.WireLevelsToChecksumBytes(asks, bids)
	// 4 KiB prefix carries the whole frame for realistic top-10 update payloads,
	// so operators can diff f.Raw vs the post-apply top-10 byte view when
	// triaging the §5.5 algorithmic-fix follow-up.
	prefix := frameRaw
	if len(prefix) > 4096 {
		prefix = prefix[:4096]
	}
	w.log.Warn("kraken_ws_checksum_mismatch_debug",
		"symbol", sym,
		"expected_crc", fmt.Sprintf("%08x", expected),
		"computed_crc", fmt.Sprintf("%08x", computed),
		"wire_bytes_hex", hex.EncodeToString(wireBytes),
		"frame_raw_prefix", hex.EncodeToString(prefix),
	)
}

// resubLoop drains resubReq off the dispatch synchronous path. Each request
// is gated by the per-symbol rate limiter (1/60s/sym, persistent across
// reconnects). Limiter denials and write errors are logged + counted; the
// loop keeps draining so the readLoop is never blocked by a wedged WS write.
func (w *Worker) resubLoop(ctx context.Context, conn wsv2.Conn, resubReq <-chan string) {
	for {
		select {
		case <-ctx.Done():
			return
		case sym, ok := <-resubReq:
			if !ok {
				return
			}
			if !w.allowResub(sym) {
				w.cfg.Metrics.ResubDropped.Add(ctx, 1, "pair", sym, "reason", "rate_limited")
				w.log.Info("kraken_ws_resub_dropped", "symbol", sym, "reason", "rate_limited")
				continue
			}
			if err := w.resubSymbol(ctx, conn, sym); err != nil {
				w.log.Warn("kraken_ws_resub_write_err", "symbol", sym, "err", err)
				continue
			}
			w.cfg.Metrics.PerSymbolResubs.Add(ctx, 1, "pair", sym)
		}
	}
}

func (w *Worker) runStallWatchdog(ctx context.Context, conn wsv2.Conn, done chan<- struct{}) {
	defer close(done)
	t := time.NewTicker(w.cfg.StallTick)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			w.mu.Lock()
			last := w.lastFrame
			w.mu.Unlock()
			if w.cfg.Now().Sub(last) > w.cfg.SilentStall {
				w.log.Warn("kraken_silent_stall; force-closing", "last_frame_at", last)
				w.cfg.Metrics.SilentStallReconnects.Add(ctx, 1)
				_ = conn.Close(websocket.StatusGoingAway, "silent stall")
				return
			}
		}
	}
}

func (w *Worker) bumpLastFrame() {
	w.mu.Lock()
	w.lastFrame = w.cfg.Now()
	w.mu.Unlock()
}

func (w *Worker) backoff(attempt int) time.Duration {
	shift := attempt
	if shift > 12 {
		shift = 12
	}
	d := w.cfg.Backoff.Initial * (1 << shift)
	if d > w.cfg.Backoff.Max {
		d = w.cfg.Backoff.Max
	}
	jitter := time.Duration(rand.Float64() * float64(d) * w.cfg.Backoff.JitterFrac)
	if rand.IntN(2) == 0 {
		return d - jitter
	}
	return d + jitter
}

func cloneLevels(lv []wsv2.BookLevel) []krob.WireLevel {
	out := make([]krob.WireLevel, len(lv))
	for i, l := range lv {
		out[i] = krob.WireLevel{Price: l.Price, Qty: l.Qty}
	}
	return out
}

// AddPair brings one pair into the active subscription set. Idempotent —
// adding an already-subscribed pair is a no-op. The flow is:
//
//  1. Per-pair AssetPairs preflight (with bisect fallback so a typo is
//     isolated rather than poisoning a hypothetical batch). On Kraken-side
//     rejection, log via SubscriptionLog and return the wrapped error WITHOUT
//     adding pair to the in-process set.
//  2. Bookkeeper.UpsertPairScale (additive — does not drop existing entries).
//  3. If there is an active WS session, send book + trade subscribes for the
//     pair. WS write failure surfaces as an error and leaves the pair OUT of
//     the in-process set; on the next reconnect the reconciler's diff path
//     will re-attempt.
//  4. On all-success, atomically add pair to w.pairs.
func (w *Worker) AddPair(ctx context.Context, pair string) error {
	if w.pairs.Contains(pair) {
		return nil
	}
	results, rejections, err := w.cfg.REST.AssetPairsWithFallback(ctx, []string{pair})
	if err != nil {
		return fmt.Errorf("AddPair %s preflight: %w", pair, err)
	}
	if len(rejections) > 0 {
		rej := rejections[0]
		w.log.Warn("kraken_addpair_rejected", "pair", rej.Pair, "kraken_err", rej.ErrMessage)
		if w.cfg.SubscriptionLog != nil {
			if logErr := w.cfg.SubscriptionLog.LogRejection(ctx, rej.Pair, rej.ErrMessage, "kraken-ws-worker"); logErr != nil {
				w.log.Warn("kraken_subscription_log_write_failed", "pair", rej.Pair, "err", logErr)
			}
		}
		return fmt.Errorf("AddPair %s rejected: %s", rej.Pair, rej.ErrMessage)
	}
	info, ok := results[pair]
	if !ok {
		return fmt.Errorf("AddPair %s: AssetPairs missing key", pair)
	}
	if info.Status != "online" {
		return fmt.Errorf("AddPair %s status=%q; want online", pair, info.Status)
	}
	w.cfg.Bookkeeper.UpsertPairScale(pair, krob.PairScale{PairDecimals: info.PairDecimals, LotDecimals: info.LotDecimals})

	// Best-effort subscribe on the active session.
	if connPtr := w.currentConn.Load(); connPtr != nil {
		w.wsMu.Lock()
		conn := *connPtr
		errBook := conn.Subscribe(ctx, "book", []string{pair}, wsv2.SubscribeOpts{Depth: krob.KrakenBookDepth, Snapshot: true})
		errTrade := conn.Subscribe(ctx, "trade", []string{pair}, wsv2.SubscribeOpts{Snapshot: true})
		w.wsMu.Unlock()
		if errBook != nil || errTrade != nil {
			// Don't add to the set — next reconnect will pick it up via Pairs() if reconciler retains it.
			return fmt.Errorf("AddPair %s WS subscribe: book=%v trade=%v", pair, errBook, errTrade)
		}
	}

	w.pairs.Add(pair)
	w.log.Info("kraken_pair_added", "pair", pair)
	return nil
}

// RemovePair drops a pair from the active subscription set. Idempotent.
// Order:
//  1. Best-effort WS unsubscribe on the active session.
//  2. Bookkeeper.DeleteSymbol (frees per-symbol state + stops heartbeat).
//  3. Atomic remove from w.pairs.
//
// Unlike AddPair, RemovePair always proceeds with steps 2-3 even if step 1
// fails (we want the bookkeeper + atomic set to converge on the desired
// state; a stale WS subscription will be cleaned up on the next reconnect).
func (w *Worker) RemovePair(ctx context.Context, pair string) error {
	if !w.pairs.Contains(pair) {
		return nil
	}

	if connPtr := w.currentConn.Load(); connPtr != nil {
		w.wsMu.Lock()
		conn := *connPtr
		errBook := conn.Unsubscribe(ctx, "book", []string{pair})
		errTrade := conn.Unsubscribe(ctx, "trade", []string{pair})
		w.wsMu.Unlock()
		if errBook != nil || errTrade != nil {
			w.log.Warn("kraken_removepair_unsubscribe_failed",
				"pair", pair, "book_err", errBook, "trade_err", errTrade)
			// Fall through to local eviction.
		}
	}

	w.cfg.Bookkeeper.DeleteSymbol(pair)
	w.pairs.Remove(pair)
	w.log.Info("kraken_pair_removed", "pair", pair)
	return nil
}

// runDesiredSetReconciler polls the DesiredSetLoader on a jittered interval
// and applies AddPair / RemovePair for the delta between the manifest and the
// current in-process set. Bound to the root ctx so it survives reconnects.
func (w *Worker) runDesiredSetReconciler(ctx context.Context) {
	loader := w.cfg.DesiredSet
	if loader == nil {
		return
	}
	w.log.Info("kraken_desired_set_reconciler_started",
		"poll_interval", w.cfg.PollInterval.String(),
		"poll_jitter", w.cfg.PollJitter.String())
	defer w.log.Info("kraken_desired_set_reconciler_stopped")
	sleep := func() bool {
		d := w.nextDesiredSetDelay()
		t := time.NewTimer(d)
		defer t.Stop()
		select {
		case <-ctx.Done():
			return false
		case <-t.C:
			return true
		}
	}
	// Initial jittered sleep before first poll — cmd layer already did a
	// synchronous boot load; no reason to double-poll at t=0.
	if !sleep() {
		return
	}
	for {
		w.reconcileDesiredSetOnce(ctx, loader)
		if !sleep() {
			return
		}
	}
}

func (w *Worker) reconcileDesiredSetOnce(ctx context.Context, loader control.DesiredSetLoader) {
	pairs, gen, changed, err := loader.Load(ctx, w.lastDesiredGen.Load())
	if errors.Is(err, control.ErrAbsent) {
		w.log.Warn("kraken_desired_set_absent; reconciler waiting for first write")
		return
	}
	if err != nil {
		w.log.Warn("kraken_desired_set_load_failed", "err", err)
		return
	}
	if !changed {
		return
	}
	if len(pairs) == 0 {
		w.log.Warn("kraken_desired_set_empty; retaining prior in-memory set")
		return
	}
	toAdd, toRemove := w.pairs.Diff(pairs)
	for _, p := range toAdd {
		if err := w.AddPair(ctx, p); err != nil {
			w.log.Warn("kraken_reconciler_addpair_failed", "pair", p, "err", err)
		}
	}
	for _, p := range toRemove {
		if err := w.RemovePair(ctx, p); err != nil {
			w.log.Warn("kraken_reconciler_removepair_failed", "pair", p, "err", err)
		}
	}
	w.lastDesiredGen.Store(gen)
}

// nextDesiredSetDelay returns a jittered poll interval (PollInterval ± PollJitter).
func (w *Worker) nextDesiredSetDelay() time.Duration {
	if w.cfg.PollJitter <= 0 {
		return w.cfg.PollInterval
	}
	jitter := time.Duration(rand.Int64N(int64(2*w.cfg.PollJitter))) - w.cfg.PollJitter
	d := w.cfg.PollInterval + jitter
	if d <= 0 {
		d = w.cfg.PollInterval
	}
	return d
}
