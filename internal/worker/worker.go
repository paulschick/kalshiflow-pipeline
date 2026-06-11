// Package worker drives the WS subscription loop.
//
// Slice 2.3a: lifecycle-driven discovery. KALSHI_SERIES is comma-split into configured
// series prefixes; the worker enumerates open markets via REST at startup, subscribes
// orderbook_delta + trade for each, subscribes the exchange-wide market_lifecycle_v2
// channel, and reacts to created/settled/determined events to dynamically subscribe and
// unsubscribe.
//
// Slice 2.4 wraps Run in a bounded exp-backoff reconnect lifecycle. Snapshot
// anchors come from Kalshi-sent orderbook_snapshot frames on every (re)subscribe
// via Plan 2.2's classifier — no REST GetOrderbook path exists post-2026-04-30
// pivot.
package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"math/rand/v2"
	"runtime/debug"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/paulschick/kalshiflow-pipeline/internal/envelope"
	"github.com/paulschick/kalshiflow-pipeline/internal/kalshi"
	"github.com/paulschick/kalshiflow-pipeline/internal/metrics"
	"github.com/paulschick/kalshiflow-pipeline/internal/orderbook"
)

// Plan 2.5 stream keys. Pub/Sub topic name is "kalshi." + stream (built at the cmd layer).
const (
	StreamSnapshot1s = "snapshot_1s"
	StreamTrade      = "trade"
	StreamLifecycle  = "lifecycle"
	StreamSettlement = "settlement"
)

// channelMarketLifecycleV2 is the Kalshi WS channel name for market lifecycle events.
// The channel is exchange-wide and rejects a market_tickers filter on subscribe.
const channelMarketLifecycleV2 = "market_lifecycle_v2"

// channelTrade is the Kalshi WS trade-feed channel.
const channelTrade = "trade"

// bucket classifies an inbound non-control frame for liveness tracking.
// Trading frames (per-market subscriptions) feed the data-stall watchdog
// deadline; lifecycle frames (exchange-wide events) do not. Lifecycle bleed
// at rollover boundaries was the 2026-05-08 watchdog under-fire root cause.
type bucket int

const (
	bucketLifecycle bucket = iota
	bucketTrading
)

// liveBucket maps a Kalshi WS frame Type to its liveness bucket. Unknown
// types route to lifecycle (safe default — never starves the trading
// deadline) and the read-loop emits a once-per-process slog.Warn so a
// future Kalshi channel addition is visible at PR review time.
func liveBucket(frameType string) bucket {
	switch frameType {
	case "orderbook_snapshot", "orderbook_delta", "trade":
		return bucketTrading
	default:
		return bucketLifecycle
	}
}

// knownLifecycleType recognizes the market_lifecycle_v2 channel — the only
// non-trading frame Type Kalshi emits today. Settlement events arrive on the
// same channel and are distinguished by payload, not Type. Unknown types still
// route to lifecycle as a safe default but trigger a once-per-process
// slog.Warn at the read-loop call site so a future Kalshi channel addition
// is visible at PR review time.
func knownLifecycleType(frameType string) bool {
	return frameType == channelMarketLifecycleV2
}

// WSConn abstracts kalshi.WS for testability.
type WSConn interface {
	Open(ctx context.Context) error
	Close() error
	Subscribe(ctx context.Context, channels, marketTickers []string) error
	Unsubscribe(ctx context.Context, sids []int64) error
	ReadMessage(ctx context.Context) (kalshi.Frame, error)
	LastSubID() int64
	Ping(ctx context.Context) error
}

// BookkeeperIface abstracts the Bookkeeper surface the worker drives.
// Concrete implementation: *orderbook.Bookkeeper. The interface exists so
// tests can record method calls (RetainOnly, Reset, ApplySnapshot) and so
// the cmd layer's bookkeeper construction stays decoupled from worker
// internals.
type BookkeeperIface interface {
	Run(ctx context.Context)
	Apply(d orderbook.DeltaMsg)
	ApplySnapshot(msg orderbook.SnapshotMsg)
	Reset()
	DeleteMarket(ticker string)
	RetainOnly(keep []string)
}

// REST abstracts the Kalshi REST client for testability. Surface stays minimal
// (only GetOpenMarkets); the on-reconnect snapshot fan-out the original Plan 2.4
// proposed was eliminated by the 2026-04-30 pivot.
type REST interface {
	GetOpenMarkets(ctx context.Context, seriesTicker, openStatus string, maxPages int) (tickers []string, hitCap bool, err error)
}

// Fanout publishes envelope bytes to one of the Plan 1 stream topics.
type Fanout interface {
	PublishStream(ctx context.Context, stream string, body []byte) error
}

// Deps wires the worker's dependencies.
type Deps struct {
	WS      WSConn
	REST    REST
	Pub     Fanout
	Metrics *metrics.Metrics
	Logger  *slog.Logger

	// Series is the initial list of Kalshi series tickers to subscribe under (parsed from
	// comma-separated KALSHI_SERIES env var at cmd layer). The worker takes ownership of
	// this slice at construction time; the live set is read via Worker.Series() after New.
	// Slice 3 will reconcile it dynamically via a GCS-backed desired-set loader.
	Series []string

	// OpenStatus is the Kalshi-side status filter value for "currently-tradable" markets,
	// passed through to GetOpenMarkets. Probe-derived (see scripts/probe-kalshi-status.py);
	// 2026-05-03 verdict is "open". Configurable via KALSHI_OPEN_STATUS at cmd layer.
	OpenStatus string

	// MaxRESTPages caps initial-population pagination per series. 0 → default 5.
	MaxRESTPages int

	// BaseBackoff is the unit of exp-backoff between reconnect attempts. 0 → default 1s.
	BaseBackoff time.Duration

	// MaxBackoff caps per-attempt sleep. 0 → default 600s. Bumped from
	// 60s → 600s in the post-Slice-1 cleanup slice so the exp ladder can
	// escalate past the 90s data-stall watchdog deadline during sustained
	// Kalshi-side outages. Restore legacy behavior with KALSHI_BACKOFF_MAX=60s.
	MaxBackoff time.Duration

	// BackoffDataThreshold is the non-control frame count a session must
	// receive for the reconnect-attempt counter to reset. 0 → sentinel
	// for the legacy reset condition (dur > stableThreshold). Default 10
	// (set by cmd layer; if Deps doesn't override, worker.New leaves it
	// at the zero value, which IS the sentinel — cmd is responsible for
	// passing the runtime default).
	BackoffDataThreshold int64

	// Bookkeeper consumes orderbook_delta frames and emits derived 1s top-2
	// snapshots via the Pub callback. Must be non-nil; cmd layer constructs.
	Bookkeeper BookkeeperIface

	// SweepInterval is the base cadence for the periodic REST reconciliation sweep that
	// closes the discovery gap left by Kalshi's lifecycle channel (which does NOT emit
	// `activated` for hourly KXBTCD/KXETHD markets — verified by probe 2026-05-05).
	// 0 → default 3m. Per Decision 9 there is no kill-switch env var; if the sweep
	// itself needs disabling in prod, image rollback to the prior tag is the recovery
	// path (see Roll-back section).
	SweepInterval time.Duration

	// SweepJitter is the absolute jitter applied per sweep tick. 0 → default 30s.
	SweepJitter time.Duration

	// SweepMissThreshold is the consecutive-miss count required before a roster ticker
	// absent from sweep responses is trimmed. Lower = more aggressive cleanup, higher =
	// safer against REST hiccups. 0 → default 2.
	SweepMissThreshold int

	// WatchdogEnabled toggles the WS-stall watchdog goroutine. Default true.
	// Production kill-switch wired from KALSHI_WS_WATCHDOG_ENABLED at the
	// cmd layer; flip to false + restart the worker pool to disable without
	// rebuilding the image.
	WatchdogEnabled bool

	// WatchdogPingInterval is the cadence between WS control pings issued by
	// the watchdog (K). 0 → default 30s. Each tick fires one Ping; a
	// non-nil Ping return (the pong did not arrive within
	// WatchdogPongDeadline) declares the connection stalled and force-closes.
	WatchdogPingInterval time.Duration

	// WatchdogPongDeadline is the per-ping pong wait (D). 0 → default 15s.
	// Generous vs Kalshi normal pong RTT (~ms) to avoid false-fires on
	// transient network blips.
	WatchdogPongDeadline time.Duration

	// DataStallEnabled toggles the WS data-stall watchdog goroutine. Default
	// true. Production kill-switch wired from KALSHI_WS_DATA_STALL_ENABLED at
	// the cmd boundary.
	DataStallEnabled bool
	// DataFrameDeadline is the max age the most recent inbound data frame may
	// have before the data-stall watchdog force-closes the WS. 0 → 90s.
	// Catches the "Kalshi-side market-data freeze" failure mode where the WS
	// protocol layer remains healthy but no market-data frames arrive (the
	// existing pong-deadline watchdog would not see this).
	DataFrameDeadline time.Duration
	// DataStallTickInterval is the cadence at which the loop compares
	// lastTradingDataAt to DataFrameDeadline. 0 → 5s. Internal cadence;
	// not env-exposed.
	DataStallTickInterval time.Duration

	// PreserveBookkeeper controls whether Bookkeeper.Run runs at Worker
	// lifetime (true) or per-session (false). When true, the Registry
	// persists across reconnects — heartbeat trickle survives Kalshi-side
	// outages. When false, restores the prior behavior: Bookkeeper.Run
	// starts fresh per session and Bookkeeper.Reset() fires on every
	// reconnect. Production cmd layer wires `true` unless the env override
	// KALSHI_PRESERVE_BOOKKEEPER=false is set.
	PreserveBookkeeper bool

	// LifecycleUnsub controls whether handleLifecycle issues an explicit
	// WS.Unsubscribe on settled/determined/deactivated events. Default
	// false: roster.Remove + Bookkeeper.DeleteMarket is sufficient because
	// Kalshi auto-retires sids server-side at terminal states. Set true
	// (KALSHI_LIFECYCLE_UNSUB=true) to restore pre-cleanup behavior — the
	// 2026-05-08 12:01 outage is the canonical evidence that explicit
	// unsubs cause harm at rollover boundaries (Kalshi rejects the sid as
	// already-retired, ~40 simultaneous code:7 errors degraded the conn).
	LifecycleUnsub bool

	// DesiredSetLoader is the source-of-truth lookup for the worker's desired
	// series set. When non-nil, Worker.Run starts a reconciler goroutine that
	// polls this loader on DesiredSetPollInterval cadence and calls AddSeries /
	// RemoveSeries for the delta. When nil, the worker uses Deps.Series as a
	// static set (legacy behavior).
	DesiredSetLoader DesiredSetLoader

	// DesiredSetPollInterval is the base cadence between reconciler ticks. 0 → default 60s.
	DesiredSetPollInterval time.Duration

	// DesiredSetPollJitter is the absolute jitter applied per reconcile tick. 0 → default 5s.
	DesiredSetPollJitter time.Duration
}

// Worker is the slice-2.3a discovery-driven event loop.
type Worker struct {
	d           Deps
	roster      *Roster
	baseBackoff time.Duration
	maxBackoff  time.Duration

	// wsMu guards every WS.Subscribe / WS.Unsubscribe write call. Required because
	// the sweep loop (introduced 2026-05-05, fix-discovery-gap plan) becomes a
	// second writer alongside the read-loop's inline subscribe/unsubscribe calls;
	// coder/websocket is single-writer-safe so concurrent writes need explicit
	// serialization. Read path (WS.ReadMessage) is independent and lock-free.
	wsMu sync.Mutex

	// sweepInterval is the base cadence between REST reconciliation ticks. 0 disables
	// the sweep loop (used by tests; production wires a default of 3m via Deps).
	sweepInterval time.Duration

	// sweepJitter is the absolute uniform jitter applied per tick: each sleep is
	// sweepInterval + uniform(-sweepJitter, +sweepJitter). 0 disables jitter (tests).
	sweepJitter time.Duration

	// sweepMissThreshold is the consecutive-miss count after which a roster ticker
	// absent from the latest sweep response is trimmed. Default 2 from Deps.
	sweepMissThreshold int

	// now is overridable for tests; nil → time.Now. Used by sweepOnce for
	// per-tick latency, by runOnce for the data-stall watchdog seed, and
	// by the read-loop dispatcher for the data-frame liveness stamp — all
	// without coupling tests to the wall clock.
	now func() time.Time

	watchdogEnabled      bool
	watchdogPingInterval time.Duration
	watchdogPongDeadline time.Duration

	// lastTradingDataAt and lastLifecycleAt split inbound-frame liveness
	// per channel. The data-stall watchdog deadline keys on
	// lastTradingDataAt only; lifecycle bleed (chatty at rollover
	// boundaries) cannot mask per-market trading silence. lastLifecycleAt
	// is consulted by the per-tick debug breadcrumb and reserved for
	// future per-channel alerts; the watchdog ignores it.
	lastTradingDataAt     atomic.Int64
	lastLifecycleAt       atomic.Int64
	unknownFrameWarnOnce  sync.Once
	panicNextTick         atomic.Bool
	dataStallEnabled      bool
	dataFrameDeadline     time.Duration
	dataStallTickInterval time.Duration

	preserveBookkeeper bool

	// series is the live desired-series set. Seeded from Deps.Series at construction;
	// reads go through atomic.Pointer to allow a future reconciler goroutine (slice 4)
	// to swap the slice without a data race against the hot-path readers.
	series atomic.Pointer[[]string]

	// tradingFramesThisSession counts trading-bucket inbound frames
	// (orderbook_snapshot, orderbook_delta, trade) since the most recent
	// runOnce call. Reset at runOnce start; bumped in the read-loop
	// dispatcher only when liveBucket(fr.Type) == bucketTrading. Worker.Run
	// reads it after runOnce returns to gate the backoff-attempt reset on
	// data-burst vs. zero-data sessions. Lifecycle-bucket frames do NOT
	// count toward this gate — a session that received only lifecycle
	// frames during a Kalshi-side stall is treated as zero-data, so the
	// exp ladder escalates instead of resetting. See spec
	// internal design doc (not included in this public snapshot).
	tradingFramesThisSession atomic.Int64

	// tradingDeltaFramesThisSession counts only orderbook_delta and trade
	// frames since the most recent runOnce call. Excludes orderbook_snapshot
	// (HOL-52 Bug A — snapshot bursts on every reconnect would otherwise
	// satisfy the backoff-reset gate and pin the exp ladder at attempt=0).
	// Reset at runOnce start; bumped in the read-loop dispatcher only when
	// fr.Type is orderbook_delta or trade. Worker.Run reads it after runOnce
	// returns to gate the backoff-attempt reset. tradingFramesThisSession
	// still bumps on snapshot frames so the data-stall watchdog deadline
	// continues to treat snapshots as evidence the protocol layer is alive.
	tradingDeltaFramesThisSession atomic.Int64

	// backoffDataThreshold is the inclusive frame count required for a
	// session to reset the reconnect-attempt counter. 0 = sentinel for
	// the legacy gate (dur > stableThreshold).
	backoffDataThreshold int64

	// lifecycleUnsub controls whether handleLifecycle issues an explicit
	// WS.Unsubscribe on terminal lifecycle events. See Deps.LifecycleUnsub.
	lifecycleUnsub bool

	// lastDesiredGen is the GCS int64 generation last applied by the reconciler.
	lastDesiredGen atomic.Int64

	// desiredSetPollInterval and desiredSetPollJitter drive the reconciler cadence.
	desiredSetPollInterval time.Duration
	desiredSetPollJitter   time.Duration
}

// New constructs a Worker, applying defaults for unset fields.
func New(d Deps) *Worker {
	if d.Logger == nil {
		d.Logger = slog.Default()
	}
	if d.MaxRESTPages == 0 {
		d.MaxRESTPages = 5
	}
	if d.BaseBackoff <= 0 {
		d.BaseBackoff = 1 * time.Second
	}
	if d.MaxBackoff <= 0 {
		d.MaxBackoff = 600 * time.Second
	}
	if d.SweepInterval == 0 {
		d.SweepInterval = 3 * time.Minute
	}
	if d.SweepJitter == 0 {
		d.SweepJitter = 30 * time.Second
	}
	if d.SweepMissThreshold == 0 {
		d.SweepMissThreshold = 2
	}
	if d.WatchdogPingInterval <= 0 {
		d.WatchdogPingInterval = 30 * time.Second
	}
	if d.WatchdogPongDeadline <= 0 {
		d.WatchdogPongDeadline = 15 * time.Second
	}
	if d.DesiredSetPollInterval <= 0 {
		d.DesiredSetPollInterval = 60 * time.Second
	}
	if d.DesiredSetPollJitter <= 0 {
		d.DesiredSetPollJitter = 5 * time.Second
	}
	// WatchdogEnabled defaults to false (Go zero value). Production cmd
	// layer flips it on explicitly; tests opt-in case-by-case.
	w := &Worker{
		d:                      d,
		roster:                 NewRoster(),
		baseBackoff:            d.BaseBackoff,
		maxBackoff:             d.MaxBackoff,
		sweepInterval:          d.SweepInterval,
		sweepJitter:            d.SweepJitter,
		sweepMissThreshold:     d.SweepMissThreshold,
		now:                    time.Now,
		watchdogEnabled:        d.WatchdogEnabled,
		watchdogPingInterval:   d.WatchdogPingInterval,
		watchdogPongDeadline:   d.WatchdogPongDeadline,
		dataStallEnabled:       d.DataStallEnabled,
		dataFrameDeadline:      defaultDuration(d.DataFrameDeadline, 90*time.Second),
		dataStallTickInterval:  defaultDuration(d.DataStallTickInterval, 5*time.Second),
		preserveBookkeeper:     d.PreserveBookkeeper,
		backoffDataThreshold:   d.BackoffDataThreshold,
		lifecycleUnsub:         d.LifecycleUnsub,
		desiredSetPollInterval: d.DesiredSetPollInterval,
		desiredSetPollJitter:   d.DesiredSetPollJitter,
	}
	initial := append([]string(nil), d.Series...)
	w.series.Store(&initial)
	return w
}

// Series returns a fresh copy of the current desired series set.
// Callers must not retain the returned slice across reconciles —
// a concurrent AddSeries/RemoveSeries (slice 4) will store a different
// underlying slice; the next call to Series will return the updated set.
func (w *Worker) Series() []string {
	cur := w.series.Load()
	if cur == nil {
		return nil
	}
	return append([]string(nil), (*cur)...)
}

// AddSeries idempotently brings a new series into the in-process subscription set.
// REST enumeration happens before any roster or series-pointer mutation, so a REST
// error leaves state unchanged.
func (w *Worker) AddSeries(ctx context.Context, ticker string) error {
	cur := *w.series.Load()
	for _, s := range cur {
		if s == ticker {
			w.d.Logger.Debug("AddSeries: already subscribed, no-op", "ticker", ticker)
			return nil
		}
	}

	tickers, hitCap, err := w.d.REST.GetOpenMarkets(ctx, ticker, w.d.OpenStatus, w.d.MaxRESTPages)
	if err != nil {
		return fmt.Errorf("AddSeries REST %s: %w", ticker, err)
	}
	if hitCap {
		w.d.Logger.Warn("REST pagination cap hit; series likely has more open markets than expected",
			"series", ticker, "max_pages", w.d.MaxRESTPages, "tickers_seen", len(tickers))
		if w.d.Metrics != nil {
			w.d.Metrics.DiscoveryPaginationCapped.Add(ctx, 1, "series", ticker)
		}
	}

	if len(tickers) == 0 {
		w.d.Logger.Info("no open markets for added series", "ticker", ticker)
		// Record the series in the pointer even with no live markets.
		newSeries := append(append([]string(nil), cur...), ticker)
		w.series.Store(&newSeries)
		if w.d.Metrics != nil {
			w.d.Metrics.WorkerSubscribedSeriesCount.Record(ctx, int64(len(newSeries)))
		}
		return nil
	}

	channels := []string{kalshi.ChannelOrderbookDelta, channelTrade}
	w.wsMu.Lock()
	err = w.d.WS.Subscribe(ctx, channels, tickers)
	if err == nil {
		w.roster.AddPending(w.d.WS.LastSubID(), tickers, channels)
	}
	w.wsMu.Unlock()
	if err != nil {
		return fmt.Errorf("AddSeries Subscribe %s: %w", ticker, err)
	}

	newSeries := append(append([]string(nil), cur...), ticker)
	w.series.Store(&newSeries)

	if w.d.Metrics != nil {
		w.d.Metrics.WorkerSubscribedSeriesCount.Record(ctx, int64(len(newSeries)))
		w.d.Metrics.DiscoveryContractsAdded.Add(ctx, int64(len(tickers)), "trigger", "reconcile")
		w.d.Metrics.SubscriptionRosterSize.Record(ctx, int64(w.roster.Size()))
	}
	w.d.Logger.Info("series added", "ticker", ticker, "market_count", len(tickers))
	return nil
}

// RemoveSeries idempotently removes a series and evicts its rostered market tickers.
// If the WS unsubscribe fails the local roster and series-pointer mutation still proceed
// (Kalshi auto-retires sids server-side; re-issuing is non-harmful).
func (w *Worker) RemoveSeries(ctx context.Context, ticker string) error {
	cur := *w.series.Load()
	found := false
	for _, s := range cur {
		if s == ticker {
			found = true
			break
		}
	}
	if !found {
		w.d.Logger.Debug("RemoveSeries: not in series set, no-op", "ticker", ticker)
		return nil
	}

	prefix := ticker + "-"
	evict := []string{}
	for _, t := range w.roster.Tickers() {
		if strings.HasPrefix(t, prefix) {
			evict = append(evict, t)
		}
	}

	// Collect deduplicated non-zero sids to pass to Unsubscribe.
	sidSet := map[int64]struct{}{}
	for _, t := range evict {
		if sid := w.roster.Sid(t); sid != 0 {
			sidSet[sid] = struct{}{}
		}
	}
	sids := make([]int64, 0, len(sidSet))
	for sid := range sidSet {
		sids = append(sids, sid)
	}

	w.wsMu.Lock()
	if len(sids) > 0 {
		if err := w.d.WS.Unsubscribe(ctx, sids); err != nil {
			w.d.Logger.Warn("RemoveSeries Unsubscribe failed; continuing local eviction",
				"ticker", ticker, "sids", sids, "err", err)
		}
	}
	for _, t := range evict {
		w.roster.Remove(t)
		if w.preserveBookkeeper {
			w.d.Bookkeeper.DeleteMarket(t)
		}
	}
	w.wsMu.Unlock()

	newSeries := make([]string, 0, len(cur)-1)
	for _, s := range cur {
		if s != ticker {
			newSeries = append(newSeries, s)
		}
	}
	w.series.Store(&newSeries)

	if w.d.Metrics != nil {
		w.d.Metrics.WorkerSubscribedSeriesCount.Record(ctx, int64(len(newSeries)))
		w.d.Metrics.DiscoveryContractsRemoved.Add(ctx, int64(len(evict)), "trigger", "reconcile")
		w.d.Metrics.SubscriptionRosterSize.Record(ctx, int64(w.roster.Size()))
	}
	w.d.Logger.Info("series removed", "ticker", ticker, "evicted_count", len(evict))
	return nil
}

const (
	stableThreshold      = 60 * time.Second
	maxReconnectAttempts = 30
)

// ErrReconnectExhausted signals the reconnect loop hit its attempt cap. Returned
// from Run so main can log and exit non-zero; Cloud Run worker pool restarts the
// instance on process exit (manual_instance_count=1).
var ErrReconnectExhausted = errors.New("worker: reconnect attempts exhausted")

// Run blocks until ctx is done. On WS error it sleeps with bounded exp backoff
// (BaseBackoff doubling, capped at MaxBackoff) and reconnects. The attempt
// counter resets at the end of any session that received >=
// BackoffDataThreshold non-control frames (default 10); zero-data sessions
// allow the ladder to escalate so a sustained Kalshi-side outage doesn't
// produce a reconnect storm at the watchdog cadence. Sentinel
// BackoffDataThreshold=0 restores the legacy `dur > stableThreshold` gate.
// On hitting maxReconnectAttempts, returns ErrReconnectExhausted.
func (w *Worker) Run(ctx context.Context) error {
	// Bookkeeper outlives WS sessions when PreserveBookkeeper is true so
	// the heartbeat ticker (300s default) is uninterrupted across
	// reconnects and the BQ minute-granularity trickle survives a
	// Kalshi-side outage. The legacy path (PreserveBookkeeper=false) keeps
	// Bookkeeper.Run session-scoped — see runOnce.
	var bkDone sync.WaitGroup
	if w.preserveBookkeeper {
		bkDone.Add(1)
		go func() {
			defer bkDone.Done()
			w.d.Bookkeeper.Run(ctx)
		}()
	}
	defer bkDone.Wait()

	var dsrDone sync.WaitGroup
	if w.d.DesiredSetLoader != nil {
		dsrDone.Add(1)
		go func() {
			defer dsrDone.Done()
			w.runDesiredSetReconciler(ctx)
		}()
	}
	defer dsrDone.Wait()

	attempt := 0
	for {
		if err := ctx.Err(); err != nil {
			return nil
		}
		startedAt := time.Now()
		err := w.runOnce(ctx)
		if errors.Is(err, context.Canceled) || ctx.Err() != nil {
			return nil
		}
		dur := time.Since(startedAt)
		deltaFrames := w.tradingDeltaFramesThisSession.Load()
		totalTrading := w.tradingFramesThisSession.Load()

		w.d.Logger.Warn("ws session ended; reconnecting",
			"err", err,
			"attempt", attempt,
			"session_duration_s", dur.Seconds(),
			"delta_frames_this_session", deltaFrames,
			"trading_frames_this_session", totalTrading,
			"bookkeeper_preserved", w.preserveBookkeeper)

		if w.d.Metrics != nil {
			w.d.Metrics.WSReconnects.Add(ctx, 1)
			// Record the pre-reset attempt — the metric must reflect how
			// far the ladder climbed during the most recent failure
			// window, not the value after the gate fires.
			w.d.Metrics.WSBackoffAttemptCurrent.Record(ctx, int64(attempt))
		}
		if w.shouldResetBackoff(dur, deltaFrames) {
			attempt = 0
		}
		if attempt >= maxReconnectAttempts {
			w.d.Logger.Error("reconnect attempts exhausted; exiting",
				"max_attempts", maxReconnectAttempts)
			return ErrReconnectExhausted
		}

		w.sleepBackoff(ctx, attempt)
		attempt++
	}
}

func (w *Worker) sleepBackoff(ctx context.Context, attempt int) {
	base := w.backoffDelay(attempt)
	jitter := time.Duration(rand.Int64N(int64(w.baseBackoff)))
	select {
	case <-ctx.Done():
	case <-time.After(base + jitter):
	}
}

// backoffDelay returns the deterministic per-attempt sleep duration before
// jitter, capped at MaxBackoff. Pulled out of sleepBackoff so the cap-and-
// ladder behavior is unit-testable without driving the time.After path.
func (w *Worker) backoffDelay(attempt int) time.Duration {
	base := time.Duration(math.Pow(2, float64(attempt))) * w.baseBackoff
	if base > w.maxBackoff {
		base = w.maxBackoff
	}
	return base
}

// shouldResetBackoff is the Run-loop gate for resetting the reconnect-attempt
// counter. Two paths:
//   - backoffDataThreshold > 0 (default): a session resets if it received
//     >= backoffDataThreshold non-control frames. A sustained Kalshi-side
//     outage produces zero-data sessions and the attempt counter escalates
//     past stableThreshold up to MaxBackoff.
//   - backoffDataThreshold == 0 (sentinel): legacy gate — a session resets
//     if its duration exceeds stableThreshold (60s), regardless of frames.
//     Kill-switch path; restored via KALSHI_BACKOFF_DATA_THRESHOLD=0.
func (w *Worker) shouldResetBackoff(dur time.Duration, frames int64) bool {
	if w.backoffDataThreshold == 0 {
		return dur > stableThreshold
	}
	return frames >= w.backoffDataThreshold
}

func (w *Worker) runOnce(ctx context.Context) error {
	// Reset per-session frame count before WS.Open so a fast-fail Open
	// (Kalshi handshake 503, etc.) doesn't leave the prior session's count
	// visible to shouldResetBackoff, which would otherwise pin the
	// reconnect-attempt counter at 0 indefinitely. See HOL-46.
	w.tradingFramesThisSession.Store(0)
	w.tradingDeltaFramesThisSession.Store(0)
	if err := w.d.WS.Open(ctx); err != nil {
		return fmt.Errorf("ws open: %w", err)
	}

	// sessionCtx bounds the bookkeeper goroutine to this WS session. The
	// combined defer cancels the session context THEN waits for the goroutine
	// to fully exit before runOnce returns. This guarantees that on the next
	// reconnect attempt, a fresh goroutine is the sole writer of
	// registry/dirty — no overlap between consecutive sessions.
	sessionCtx, cancelSession := context.WithCancel(ctx)
	var bkDone sync.WaitGroup
	if !w.preserveBookkeeper {
		// Legacy path: Bookkeeper.Run is session-scoped and dies on
		// reconnect. Bookkeeper.Reset() at session start also fires per
		// session in this mode, matching the pre-slice behavior.
		bkDone.Add(1)
		go func() {
			defer bkDone.Done()
			w.d.Bookkeeper.Run(sessionCtx)
		}()
	}
	var swDone sync.WaitGroup
	swDone.Add(1)
	go func() {
		defer swDone.Done()
		w.sweepLoop(sessionCtx)
	}()
	now := w.now().UnixNano()
	w.lastTradingDataAt.Store(now)
	w.lastLifecycleAt.Store(now)

	w.d.Logger.Info("ws session config",
		"data_stall_enabled", w.dataStallEnabled,
		"data_frame_deadline", w.dataFrameDeadline,
		"data_stall_tick_interval", w.dataStallTickInterval,
		"watchdog_enabled", w.watchdogEnabled,
		"backoff_data_threshold", w.backoffDataThreshold,
		"max_backoff", w.maxBackoff)

	var wdDone sync.WaitGroup
	if w.watchdogEnabled {
		wdDone.Add(1)
		go func() {
			defer wdDone.Done()
			w.watchdogLoop(sessionCtx)
		}()
	}
	var dsDone sync.WaitGroup
	if w.dataStallEnabled {
		dsDone.Add(1)
		go func() {
			defer dsDone.Done()
			w.dataStallLoop(sessionCtx)
		}()
	}
	defer func() {
		if err := w.d.WS.Close(); err != nil {
			w.d.Logger.Warn("ws close error", "err", err)
		}
		if w.d.Metrics != nil {
			w.d.Metrics.SetConnectionStart(time.Time{}) // gauge → no-observation while disconnected
		}
		cancelSession()
		bkDone.Wait()
		swDone.Wait()
		wdDone.Wait()
		dsDone.Wait()
	}()

	w.roster = NewRoster() // discard stale state from prior session
	if !w.preserveBookkeeper {
		w.d.Bookkeeper.Reset()
	}
	if w.d.Metrics != nil {
		w.d.Metrics.SetConnectionStart(time.Now())
	}

	if err := w.populate(ctx); err != nil {
		return fmt.Errorf("initial population: %w", err)
	}

	// Evict markets the Bookkeeper Registry holds that are not in the new
	// roster. Markets that settled during a Kalshi-side outage are absent
	// from the post-outage REST `open` response; without this, heartbeat
	// would keep emitting stale top-2 for them indefinitely.
	if w.preserveBookkeeper {
		w.d.Bookkeeper.RetainOnly(w.roster.Tickers())
	}

	// Lifecycle subscribe is exchange-wide; channel rejects a market_tickers filter. No
	// roster entry — roster is keyed by market_ticker; lifecycle sub is process-lifetime.
	w.wsMu.Lock()
	err := w.d.WS.Subscribe(ctx, []string{channelMarketLifecycleV2}, nil)
	w.wsMu.Unlock()
	if err != nil {
		return fmt.Errorf("ws subscribe lifecycle: %w", err)
	}
	w.d.Logger.Info("ws subscribed lifecycle channel (exchange-wide)")

	for {
		fr, err := w.d.WS.ReadMessage(ctx)
		if err != nil {
			return fmt.Errorf("ws read: %w", err)
		}

		switch fr.Type {
		case "ok":
			w.handleAck(fr)
			continue
		case "error":
			w.d.Logger.Warn("ws server error frame", "req_id", fr.ID, "raw_payload", string(fr.RawPayload))
			continue
		}

		nowNanos := w.now().UnixNano()
		switch liveBucket(fr.Type) {
		case bucketTrading:
			w.lastTradingDataAt.Store(nowNanos)
			w.tradingFramesThisSession.Add(1)
			if fr.Type == kalshi.ChannelOrderbookDelta || fr.Type == channelTrade {
				w.tradingDeltaFramesThisSession.Add(1)
			}
		case bucketLifecycle:
			w.lastLifecycleAt.Store(nowNanos)
			if !knownLifecycleType(fr.Type) {
				w.warnUnknownTypeOnce(fr.Type)
			}
		}

		stream, seriesID, err := w.publishFrame(ctx, fr)
		if err != nil {
			w.d.Logger.Error("publish failed", "stream", stream, "err", err)
			continue
		}
		if stream != "" && w.d.Metrics != nil {
			w.d.Metrics.WSMessagesReceived.Add(ctx, 1, "stream", stream, "series", seriesID)
		}

		// Lifecycle frames double as discovery signals.
		if fr.Type == channelMarketLifecycleV2 {
			w.handleLifecycle(ctx, fr)
		}
	}
}

// populate enumerates open markets per configured series and issues one batched subscribe
// for orderbook_delta + trade across the union.
func (w *Worker) populate(ctx context.Context) error {
	series := *w.series.Load()
	all := []string{}
	for _, s := range series {
		tickers, hitCap, err := w.d.REST.GetOpenMarkets(ctx, s, w.d.OpenStatus, w.d.MaxRESTPages)
		if err != nil {
			return fmt.Errorf("REST GetOpenMarkets %s: %w", s, err)
		}
		w.d.Logger.Info("rest enumerated open markets", "series", s, "count", len(tickers), "hit_cap", hitCap)
		if hitCap {
			w.d.Logger.Warn("REST pagination cap hit; series likely has more open markets than expected",
				"series", s, "max_pages", w.d.MaxRESTPages, "tickers_seen", len(tickers))
			if w.d.Metrics != nil {
				w.d.Metrics.DiscoveryPaginationCapped.Add(ctx, 1, "series", s)
			}
		}
		all = append(all, tickers...)
	}
	if len(all) == 0 {
		w.d.Logger.Warn("no open markets in configured series at startup", "series", series)
		return nil
	}

	channels := []string{kalshi.ChannelOrderbookDelta, channelTrade}
	w.wsMu.Lock()
	err := w.d.WS.Subscribe(ctx, channels, all)
	if err == nil {
		w.roster.AddPending(w.d.WS.LastSubID(), all, channels)
	}
	w.wsMu.Unlock()
	if err != nil {
		return fmt.Errorf("ws subscribe initial: %w", err)
	}
	if w.d.Metrics != nil {
		w.d.Metrics.SubscriptionRosterSize.Record(ctx, int64(w.roster.Size()))
		w.d.Metrics.DiscoveryContractsAdded.Add(ctx, int64(len(all)), "trigger", "initial_population")
	}
	w.d.Logger.Info("ws subscribed initial population", "channels", channels, "ticker_count", len(all))
	return nil
}

// handleAck stamps the server-assigned sid onto every ticker that shared the ack's req id.
func (w *Worker) handleAck(fr kalshi.Frame) {
	if fr.ID == 0 {
		return // ack to a command we didn't track (e.g. lifecycle subscribe)
	}
	updated := w.roster.RecordSid(fr.ID, fr.SID)
	if updated > 0 {
		w.d.Logger.Debug("ws ack recorded", "req_id", fr.ID, "sid", fr.SID, "tickers", updated)
	}
}

// handleLifecycle reacts to market_lifecycle_v2 events. Subscribes to matching-series new
// markets; unsubscribes settled/determined markets.
//
// Subscribe/Unsubscribe called inline from the read loop. Single-goroutine = no roster
// race; coder/websocket Write is single-writer-safe. Trade-off: socket backpressure on
// Write blocks frame draining. Acceptable as the steady-state behavior.
func (w *Worker) handleLifecycle(ctx context.Context, fr kalshi.Frame) {
	type lifecycleMsg struct {
		EventType    string `json:"event_type"`
		MarketTicker string `json:"market_ticker"`
	}
	var m lifecycleMsg
	if err := json.Unmarshal(fr.RawPayload, &m); err != nil {
		return
	}
	if m.MarketTicker == "" {
		return
	}
	if !w.matchesAnySeries(m.MarketTicker) {
		return // unconfigured series — drop
	}

	switch m.EventType {
	case "created":
		if w.roster.Has(m.MarketTicker) {
			return // idempotent: REST initial-pop + lifecycle race
		}
		channels := []string{kalshi.ChannelOrderbookDelta, channelTrade}
		w.wsMu.Lock()
		err := w.d.WS.Subscribe(ctx, channels, []string{m.MarketTicker})
		if err == nil {
			w.roster.AddPending(w.d.WS.LastSubID(), []string{m.MarketTicker}, channels)
		}
		w.wsMu.Unlock()
		if err != nil {
			w.d.Logger.Warn("dynamic subscribe failed", "ticker", m.MarketTicker, "err", err)
			return
		}
		if w.d.Metrics != nil {
			w.d.Metrics.SubscriptionRosterSize.Record(ctx, int64(w.roster.Size()))
			w.d.Metrics.DiscoveryContractsAdded.Add(ctx, 1, "trigger", "created")
		}
		w.d.Logger.Info("dynamic subscribe", "ticker", m.MarketTicker, "trigger", "created")

	case "settled", "determined", "deactivated":
		sid, ok := w.roster.Remove(m.MarketTicker)
		if !ok {
			return
		}
		// Kalshi auto-retires sids when markets reach a terminal state.
		// Issuing an explicit Unsubscribe is redundant and triggered
		// `code:7 Unknown subscription ID` error frames at the
		// 2026-05-08 12:01 UTC rollover boundary (~40 simultaneous
		// errors preceded a 27-min partial stall). roster.Remove +
		// Bookkeeper eviction is sufficient. Legacy explicit-unsub
		// behavior is restorable via KALSHI_LIFECYCLE_UNSUB=true.
		if w.lifecycleUnsub && sid > 0 {
			w.wsMu.Lock()
			err := w.d.WS.Unsubscribe(ctx, []int64{sid})
			w.wsMu.Unlock()
			if err != nil {
				w.d.Logger.Warn("dynamic unsubscribe failed",
					"ticker", m.MarketTicker, "sid", sid, "err", err)
			}
		}
		if w.preserveBookkeeper {
			w.d.Bookkeeper.DeleteMarket(m.MarketTicker)
		}
		w.d.Logger.Info("lifecycle eviction",
			"ticker", m.MarketTicker,
			"trigger", m.EventType,
			"sid", sid,
			"lifecycle_unsub", w.lifecycleUnsub)
		if w.d.Metrics != nil {
			w.d.Metrics.SubscriptionRosterSize.Record(ctx, int64(w.roster.Size()))
			w.d.Metrics.DiscoveryContractsRemoved.Add(ctx, 1, "trigger", m.EventType)
		}
	}
}

// matchesAnySeries reports whether ticker has the form "<series>-..." for any configured
// series. Lifecycle frames lack event_ticker, so prefix is the only available signal.
func (w *Worker) matchesAnySeries(ticker string) bool {
	series := *w.series.Load()
	for _, s := range series {
		if strings.HasPrefix(ticker, s+"-") {
			return true
		}
	}
	return false
}

// publishFrame routes orderbook frames to the bookkeeper and publishes envelopes
// for trade/lifecycle/settlement frames.
func (w *Worker) publishFrame(ctx context.Context, fr kalshi.Frame) (string, string, error) {
	type minimal struct {
		MarketTicker string `json:"market_ticker"`
		TSMS         int64  `json:"ts_ms"`
		EventType    string `json:"event_type"`
	}
	var m minimal
	_ = json.Unmarshal(fr.RawPayload, &m)

	// Route orderbook_delta + orderbook_snapshot to the bookkeeper. They are
	// not published as raw streams in this slice — derived 1s snapshots emit
	// on the bookkeeper's own timers.
	switch fr.Type {
	case kalshi.ChannelOrderbookDelta:
		return w.applyDelta(ctx, fr.RawPayload, m.MarketTicker)
	case "orderbook_snapshot":
		return w.applySnapshot(ctx, fr.RawPayload, m.MarketTicker)
	}

	stream := classifyStream(fr.Type, m.EventType)
	if stream == "" {
		return "", "", nil
	}

	// Lifecycle filter: only emit envelopes for matching-series tickers (channel is
	// exchange-wide so foreign-series frames arrive and must be dropped).
	if fr.Type == channelMarketLifecycleV2 && !w.matchesAnySeries(m.MarketTicker) {
		return "", "", nil
	}

	seriesID := w.SeriesForTicker(m.MarketTicker)

	now := time.Now().UTC()
	env := envelope.New(fr.Type, seriesID, now, fr.RawPayload)
	if m.MarketTicker != "" {
		// In Kalshi's data model, a market_ticker IS the contract id (each market is one contract).
		env = env.WithContract(m.MarketTicker, m.MarketTicker)
	}
	if m.TSMS > 0 {
		env = env.WithKalshiTS(time.UnixMilli(m.TSMS).UTC())
	}
	if fr.Seq > 0 {
		env = env.WithSeq(fr.Seq)
	}

	body, err := env.Marshal()
	if err != nil {
		return stream, seriesID, fmt.Errorf("envelope marshal: %w", err)
	}
	return stream, seriesID, w.d.Pub.PublishStream(ctx, stream, body)
}

// SeriesForTicker returns the configured series prefix that matches the ticker, or "" if
// none. Lifecycle frames are pre-filtered by matchesAnySeries; orderbook/trade frames are
// server-filtered via the subscribe's market_tickers, so a non-match here is unusual.
// Exported because the cmd-layer bookkeeper Publish closure also needs a live
// (atomic-pointer-backed) series lookup — see HOL-43.
func (w *Worker) SeriesForTicker(ticker string) string {
	series := *w.series.Load()
	for _, s := range series {
		if strings.HasPrefix(ticker, s+"-") {
			return s
		}
	}
	if ticker != "" {
		w.d.Logger.Warn("ticker matched no configured series prefix", "ticker", ticker, "series", series)
	}
	return ""
}

// applyDelta parses a raw orderbook_delta payload and forwards it to the bookkeeper.
func (w *Worker) applyDelta(_ context.Context, raw json.RawMessage, ticker string) (string, string, error) {
	var d struct {
		Side         string `json:"side"`
		PriceDollars string `json:"price_dollars"`
		DeltaFP      string `json:"delta_fp"`
	}
	if err := json.Unmarshal(raw, &d); err != nil {
		return "orderbook_delta", "", fmt.Errorf("delta unmarshal: %w", err)
	}
	if ticker == "" {
		return "orderbook_delta", "", fmt.Errorf("delta has empty market_ticker")
	}
	var side orderbook.Side
	switch d.Side {
	case "yes":
		side = orderbook.SideYes
	case "no":
		side = orderbook.SideNo
	default:
		return "orderbook_delta", "", fmt.Errorf("delta side: %q", d.Side)
	}
	priceUnits, err := orderbook.ParsePriceUnits(d.PriceDollars)
	if err != nil {
		return "orderbook_delta", "", fmt.Errorf("delta price: %w", err)
	}
	sizeDelta, err := orderbook.ParseDeltaFP(d.DeltaFP)
	if err != nil {
		return "orderbook_delta", "", fmt.Errorf("delta size: %w", err)
	}
	w.d.Bookkeeper.Apply(orderbook.DeltaMsg{
		Ticker: ticker, Side: side, PriceUnits: priceUnits, SizeDelta: sizeDelta,
	})
	seriesID := w.SeriesForTicker(ticker)
	if w.d.Metrics != nil {
		w.d.Metrics.BookkeeperDeltasProcessed.Add(context.Background(), 1, "series", seriesID)
	}
	return "orderbook_delta", seriesID, nil
}

// applySnapshot parses a raw orderbook_snapshot payload and forwards it
// to the bookkeeper. Replaces the previous Bookkeeper.ResetMarket call
// so the book is populated from the snapshot's level data instead of
// being wiped to empty — HOL-52 Bug B fix.
//
// Synchronously waits on the bookkeeper's Done close before returning so
// any delta that arrives next on the wire is applied to a book that
// already reflects the snapshot. The wait also honors ctx cancellation
// (process shutdown / parent ctx cancel) to avoid blocking forever if
// the bookkeeper Run goroutine has exited.
//
// Coupling note: this call freezes the WS read loop until handleSnapshot
// completes. The bookkeeper's Publish callback (the cmd-layer Pub/Sub
// publisher) is invoked from the same goroutine that closes Done on
// change-tick / heartbeat boundaries — but NOT inline with the snapshot
// itself, so a slow Publish does not directly extend the read-loop stall
// here. Even so, any future regression that introduces a synchronous
// Publish path inside handleSnapshot would couple WS read liveness to
// downstream backpressure. Keep handleSnapshot non-blocking.
func (w *Worker) applySnapshot(ctx context.Context, raw json.RawMessage, ticker string) (string, string, error) {
	if ticker == "" {
		return "", "", fmt.Errorf("snapshot has empty market_ticker")
	}
	var p struct {
		Yes [][2]json.RawMessage `json:"yes"`
		No  [][2]json.RawMessage `json:"no"`
	}
	if err := json.Unmarshal(raw, &p); err != nil {
		return "", "", fmt.Errorf("snapshot unmarshal: %w", err)
	}
	yesLevels, err := decodeLevels(p.Yes)
	if err != nil {
		return "", "", fmt.Errorf("snapshot yes levels: %w", err)
	}
	noLevels, err := decodeLevels(p.No)
	if err != nil {
		return "", "", fmt.Errorf("snapshot no levels: %w", err)
	}
	done := make(chan struct{})
	w.d.Bookkeeper.ApplySnapshot(orderbook.SnapshotMsg{
		Ticker: ticker, Yes: yesLevels, No: noLevels, Done: done,
	})
	select {
	case <-done:
	case <-ctx.Done():
		return "", "", ctx.Err()
	}
	return "", "", nil
}

// decodeLevels parses a Kalshi orderbook_snapshot levels array. Each
// element is [price_dollars (string), size (number)]. Levels with size
// <= 0 are dropped (defensive — Kalshi should not send them, and a
// zero-size level is observationally absent).
func decodeLevels(raw [][2]json.RawMessage) ([]orderbook.SnapshotLevel, error) {
	out := make([]orderbook.SnapshotLevel, 0, len(raw))
	for i, pair := range raw {
		var priceStr string
		if err := json.Unmarshal(pair[0], &priceStr); err != nil {
			return nil, fmt.Errorf("level[%d] price: %w", i, err)
		}
		price, err := orderbook.ParsePriceUnits(priceStr)
		if err != nil {
			return nil, fmt.Errorf("level[%d] price parse: %w", i, err)
		}
		var size int64
		if err := json.Unmarshal(pair[1], &size); err != nil {
			return nil, fmt.Errorf("level[%d] size: %w", i, err)
		}
		if size <= 0 {
			continue
		}
		out = append(out, orderbook.SnapshotLevel{PriceUnits: price, Size: size})
	}
	return out, nil
}

// classifyStream maps a Kalshi WS (frame.type, msg.event_type) to a Plan 2.5 stream key.
// Returns "" for ack/error/orderbook/unknown frames. Orderbook frames are routed
// to the bookkeeper by publishFrame before classifyStream is called.
func classifyStream(frameType, eventType string) string {
	switch frameType {
	case channelTrade:
		return StreamTrade
	case channelMarketLifecycleV2:
		if eventType == "settled" {
			return StreamSettlement
		}
		return StreamLifecycle
	default:
		return ""
	}
}

// sweepLoop is the periodic-REST-reconciliation goroutine. Bound to sessionCtx; dies
// cleanly on WS reconnect (the next runOnce spawns a fresh one). Sleeps
// sweepInterval + uniform(-sweepJitter, +sweepJitter) between ticks; first tick fires
// AFTER the initial sleep (no immediate-on-launch sweep — that would duplicate the
// populate() call that just ran).
func (w *Worker) sweepLoop(ctx context.Context) {
	if w.sweepInterval <= 0 {
		return
	}
	timer := time.NewTimer(w.nextSweepDelay())
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			w.sweepOnce(ctx)
			timer.Reset(w.nextSweepDelay())
		}
	}
}

func (w *Worker) nextSweepDelay() time.Duration {
	if w.sweepJitter <= 0 {
		return w.sweepInterval
	}
	jitter := time.Duration(rand.Int64N(int64(2*w.sweepJitter))) - w.sweepJitter
	d := w.sweepInterval + jitter
	if d <= 0 {
		d = w.sweepInterval
	}
	return d
}

// sweepOnce is one REST-reconciliation tick. Per-series GetOpenMarkets, union into a
// single set, Roster.SweepDiff, batched Subscribe for adds, sequential Unsubscribe for
// trims. Per Decision 3: any per-series error aborts the entire tick (no miss-counter
// bumps for any rostered ticker), to avoid false trims on per-series infra flakes.
func (w *Worker) sweepOnce(ctx context.Context) {
	started := w.now()
	series := *w.series.Load()
	all := []string{}
	for _, s := range series {
		sStart := w.now()
		tickers, _, err := w.d.REST.GetOpenMarkets(ctx, s, w.d.OpenStatus, w.d.MaxRESTPages)
		dur := w.now().Sub(sStart)
		if w.d.Metrics != nil {
			w.d.Metrics.DiscoverySweepDuration.Record(ctx, float64(dur.Milliseconds()), "series", s)
		}
		if err != nil {
			w.d.Logger.Warn("sweep REST error; skipping tick", "series", s, "err", err)
			if w.d.Metrics != nil {
				w.d.Metrics.DiscoverySweepRuns.Add(ctx, 1, "series", s, "outcome", "rest_error")
			}
			return
		}
		if w.d.Metrics != nil {
			w.d.Metrics.DiscoverySweepRuns.Add(ctx, 1, "series", s, "outcome", "ok")
		}
		all = append(all, tickers...)
	}

	toAdd, toTrim := w.roster.SweepDiff(all, w.sweepMissThreshold)

	if len(toAdd) > 0 {
		channels := []string{kalshi.ChannelOrderbookDelta, channelTrade}
		w.wsMu.Lock()
		err := w.d.WS.Subscribe(ctx, channels, toAdd)
		if err == nil {
			w.roster.AddPending(w.d.WS.LastSubID(), toAdd, channels)
		}
		w.wsMu.Unlock()
		if err != nil {
			w.d.Logger.Warn("sweep subscribe failed", "count", len(toAdd), "err", err)
		} else {
			if w.d.Metrics != nil {
				w.d.Metrics.DiscoveryContractsAdded.Add(ctx, int64(len(toAdd)), "trigger", "sweep")
				w.d.Metrics.SubscriptionRosterSize.Record(ctx, int64(w.roster.Size()))
			}
			w.d.Logger.Info("sweep subscribe", "count", len(toAdd), "elapsed_ms", w.now().Sub(started).Milliseconds())
		}
	}

	if len(toTrim) > 0 {
		for _, e := range toTrim {
			w.wsMu.Lock()
			// Idempotency: lifecycle handler may have removed this ticker between
			// SweepDiff returning and the trim loop reaching it. Skip the duplicate
			// Unsubscribe (Kalshi would otherwise reject a retired sid). Matches the
			// `roster.Has` guard the `created` lifecycle arm uses.
			if !w.roster.Has(e.Ticker) {
				w.wsMu.Unlock()
				continue
			}
			if e.Sid > 0 {
				if err := w.d.WS.Unsubscribe(ctx, []int64{e.Sid}); err != nil {
					w.d.Logger.Warn("sweep unsubscribe failed", "ticker", e.Ticker, "sid", e.Sid, "err", err)
				}
			}
			w.roster.Remove(e.Ticker)
			// Sweep-trim removes the ticker from the WS subscription set.
			// Bookkeeper Registry must also evict it so heartbeat stops
			// emitting stale top-2 once the market is no longer tracked.
			// Legacy path (preserve=false) wipes Registry on next reconnect
			// via Reset, so the explicit DeleteMarket is unnecessary there.
			if w.preserveBookkeeper {
				w.d.Bookkeeper.DeleteMarket(e.Ticker)
			}
			w.wsMu.Unlock()
		}
		if w.d.Metrics != nil {
			w.d.Metrics.DiscoveryContractsRemoved.Add(ctx, int64(len(toTrim)), "trigger", "sweep_grace")
			w.d.Metrics.SubscriptionRosterSize.Record(ctx, int64(w.roster.Size()))
		}
		w.d.Logger.Info("sweep trim", "count", len(toTrim))
	}
}

// watchdogLoop is the WS-stall-detection goroutine. Bound to sessionCtx;
// dies cleanly on WS reconnect (the next runOnce spawns a fresh one).
//
// On each tick, sends a WS control ping with a bounded pong deadline. A
// non-nil Ping return is the stall signal: increment the watchdog counter,
// force-close the WS, and return. The blocked ReadMessage in runOnce
// then returns an error, runOnce returns, and the outer Worker.Run loop
// reconnects via its existing exp-backoff path. Watchdog-driven reconnects
// are NOT distinguished in the existing ws_reconnects_total counter — that
// metric stays as "all reconnects"; the new watchdog counter is the
// per-cause sub-rate.
//
// The ctx.Err() guard after Ping covers the shutdown race: if sessionCtx
// is canceled while a Ping is in flight, the Ping returns an error wrapping
// ctx.Err() — that's a clean shutdown, not a stall, so do NOT increment
// the counter and do NOT call Close.
func (w *Worker) watchdogLoop(ctx context.Context) {
	if w.watchdogPingInterval <= 0 {
		return
	}
	ticker := time.NewTicker(w.watchdogPingInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			pingCtx, cancel := context.WithTimeout(ctx, w.watchdogPongDeadline)
			err := w.d.WS.Ping(pingCtx)
			cancel()
			if err == nil {
				continue
			}
			if ctx.Err() != nil {
				return
			}
			w.d.Logger.Warn("ws watchdog: stall detected, force-closing conn",
				"ping_err", err,
				"ping_interval", w.watchdogPingInterval,
				"pong_deadline", w.watchdogPongDeadline)
			if w.d.Metrics != nil {
				w.d.Metrics.WSWatchdogForceReconnects.Add(ctx, 1)
			}
			_ = w.d.WS.Close()
			return
		}
	}
}

// warnUnknownTypeOnce logs a slog.Warn at most once per process for an
// unknown inbound frame Type. The frame still routes to the lifecycle
// bucket (safe default), so the watchdog continues to function on known
// trading channels; the warn surfaces the gap at PR review time the next
// time someone touches the bucket helper.
func (w *Worker) warnUnknownTypeOnce(frameType string) {
	w.unknownFrameWarnOnce.Do(func() {
		w.d.Logger.Warn("ws read: unknown frame Type — bucketed as lifecycle",
			"frame_type", frameType)
	})
}

// dataStallLoop watches inbound trading-frame freshness. Bound to
// sessionCtx; dies cleanly on WS reconnect (the next runOnce spawns a
// fresh one). Wrapped in a panic-recover so a crash in the loop body
// becomes ws_data_stall_panics_total + slog.Error + WS.Close rather than
// silent death — see the 2026-05-08 under-fire incident in
// internal design doc (not included in this public snapshot).
//
// On each tick, compares the current time to lastTradingDataAt — set by
// the read-loop dispatcher in runOnce on every trading-bucket inbound
// frame (orderbook_snapshot, orderbook_delta, trade). Lifecycle-bucket
// frames (lifecycle, settlement, unknown types) update lastLifecycleAt
// only and do NOT refresh the deadline. This split closes the
// 2026-05-08 under-fire bug where exchange-wide lifecycle chatter masked
// per-market trading silence at rollover boundaries.
//
// Each tick emits ws_data_stall_ticks_total{result=ok|tripped|skipped}
// for liveness alerting (rate==0 means the loop is dead) and a
// slog.Debug breadcrumb (trading_age, lifecycle_age, deadline) for
// forensics.
//
// On miss, increments ws_data_stall_force_reconnects_total with
// trigger="data_timeout", force-closes the WS via the existing
// idempotent Close path, and returns. The blocked ReadMessage in
// runOnce then returns an error, runOnce returns, and the outer
// Worker.Run loop reconnects via its existing exp-backoff path.
func (w *Worker) dataStallLoop(ctx context.Context) {
	if w.dataStallTickInterval <= 0 {
		return
	}
	defer func() {
		if r := recover(); r != nil {
			stack := debug.Stack()
			w.d.Logger.Error("ws data-stall loop panic; force-closing conn",
				"recover", fmt.Sprintf("%v", r),
				"stack", string(stack))
			if w.d.Metrics != nil {
				w.d.Metrics.WSDataStallPanics.Add(ctx, 1)
			}
			_ = w.d.WS.Close()
		}
	}()

	ticker := time.NewTicker(w.dataStallTickInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case t := <-ticker.C:
			if w.panicNextTick.Swap(false) {
				panic("dataStallLoop test-only injected panic")
			}
			if ctx.Err() != nil {
				if w.d.Metrics != nil {
					w.d.Metrics.WSDataStallTicks.Add(ctx, 1, "result", "skipped")
				}
				return
			}
			tradingAge := time.Duration(t.UnixNano() - w.lastTradingDataAt.Load())
			lifecycleAge := time.Duration(t.UnixNano() - w.lastLifecycleAt.Load())
			if tradingAge <= w.dataFrameDeadline {
				if w.d.Metrics != nil {
					w.d.Metrics.WSDataStallTicks.Add(ctx, 1, "result", "ok")
				}
				w.d.Logger.Debug("ws data-stall tick",
					"result", "ok",
					"trading_age", tradingAge,
					"lifecycle_age", lifecycleAge,
					"deadline", w.dataFrameDeadline)
				continue
			}
			if w.d.Metrics != nil {
				w.d.Metrics.WSDataStallTicks.Add(ctx, 1, "result", "tripped")
				w.d.Metrics.WSDataStallForceReconnects.Add(ctx, 1, "trigger", "data_timeout")
			}
			w.d.Logger.Warn("ws data-stall: detected, force-closing conn",
				"trigger", "data_timeout",
				"trading_age", tradingAge,
				"lifecycle_age", lifecycleAge,
				"data_frame_deadline", w.dataFrameDeadline)
			_ = w.d.WS.Close()
			return
		}
	}
}

func defaultDuration(v, def time.Duration) time.Duration {
	if v <= 0 {
		return def
	}
	return v
}
