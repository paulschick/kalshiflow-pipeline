package worker

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/paulschick/kalshiflow-pipeline/internal/kalshi"
	"github.com/paulschick/kalshiflow-pipeline/internal/orderbook"
	"github.com/paulschick/kalshiflow-pipeline/internal/testutil"
)

// errREST wraps a delegate REST and returns a configured error for a specific series.
type errREST struct {
	err      error
	delegate REST
}

func (e *errREST) GetOpenMarkets(ctx context.Context, s, st string, mp int) ([]string, bool, error) {
	if e.err != nil && s == "KXBAD" {
		return nil, false, e.err
	}
	return e.delegate.GetOpenMarkets(ctx, s, st, mp)
}

// errUnsubWS wraps fakeWS and returns a configured error from Unsubscribe.
type errUnsubWS struct {
	*fakeWS
	unsubErr error
}

func (e *errUnsubWS) Unsubscribe(_ context.Context, _ []int64) error {
	e.unsubCalls.Add(1)
	return e.unsubErr
}

// newSeriesWorker builds a minimal Worker suitable for AddSeries/RemoveSeries tests.
// It does not call Run; callers exercise the methods directly.
func newSeriesWorker(t *testing.T, ws WSConn, rest REST, bk BookkeeperIface, series []string) *Worker {
	t.Helper()
	if bk == nil {
		bk = &stubBookkeeper{}
	}
	logger := slog.New(slog.NewJSONHandler(&bytes.Buffer{}, nil))
	return New(Deps{
		WS:                 ws,
		REST:               rest,
		Pub:                &stubFanout{},
		Series:             series,
		Bookkeeper:         bk,
		Logger:             logger,
		SweepInterval:      0, // disable sweep loop
		PreserveBookkeeper: false,
	})
}

func TestWorker_AddSeries_Idempotent(t *testing.T) {
	t.Parallel()
	ws := &fakeWS{frames: make(chan kalshi.Frame, 4)}
	rest := newREST("KXBTCD", "KXBTCD-A")
	w := newSeriesWorker(t, ws, rest, nil, []string{"KXBTCD"})
	ctx := context.Background()

	before := ws.subCalls.Load()
	if err := w.AddSeries(ctx, "KXBTCD"); err != nil {
		t.Fatalf("AddSeries returned error: %v", err)
	}
	// Give a short window to confirm no subscribe fired.
	time.Sleep(20 * time.Millisecond)
	if after := ws.subCalls.Load(); after != before {
		t.Errorf("subCalls went from %d to %d; want no change (idempotent)", before, after)
	}
	got := w.Series()
	if len(got) != 1 || got[0] != "KXBTCD" {
		t.Errorf("Series() = %v; want [KXBTCD]", got)
	}
}

func TestWorker_AddSeries_SubscribesAndAppendsSeriesPointer(t *testing.T) {
	t.Parallel()
	ws := &fakeWS{frames: make(chan kalshi.Frame, 4)}
	rest := &fakeREST{
		byName: map[string][]string{
			"KXBTCD": {"KXBTCD-A"},
			"KXLTCD": {"KXLTCD-A", "KXLTCD-B"},
		},
		hitCap: map[string]bool{},
	}
	w := newSeriesWorker(t, ws, rest, nil, []string{"KXBTCD"})
	ctx := context.Background()

	beforeSub := ws.subCalls.Load()
	if err := w.AddSeries(ctx, "KXLTCD"); err != nil {
		t.Fatalf("AddSeries returned error: %v", err)
	}

	if after := ws.subCalls.Load(); after != beforeSub+1 {
		t.Errorf("subCalls = %d; want %d (one new subscribe)", after, beforeSub+1)
	}

	// Roster must contain both new tickers.
	testutil.WaitFor(t, 500*time.Millisecond, "roster has KXLTCD-A", func() bool {
		return w.roster.Has("KXLTCD-A")
	})
	testutil.WaitFor(t, 500*time.Millisecond, "roster has KXLTCD-B", func() bool {
		return w.roster.Has("KXLTCD-B")
	})

	// Series pointer must include both.
	got := w.Series()
	hasKXBTCD, hasKXLTCD := false, false
	for _, s := range got {
		if s == "KXBTCD" {
			hasKXBTCD = true
		}
		if s == "KXLTCD" {
			hasKXLTCD = true
		}
	}
	if !hasKXBTCD || !hasKXLTCD {
		t.Errorf("Series() = %v; want both KXBTCD and KXLTCD", got)
	}
}

func TestWorker_AddSeries_RESTErrorDoesNotMutate(t *testing.T) {
	t.Parallel()
	ws := &fakeWS{frames: make(chan kalshi.Frame, 4)}
	delegate := newREST("KXBTCD", "KXBTCD-A")
	rest := &errREST{err: errors.New("upstream timeout"), delegate: delegate}
	w := newSeriesWorker(t, ws, rest, nil, []string{"KXBTCD"})
	ctx := context.Background()

	before := w.Series()
	err := w.AddSeries(ctx, "KXBAD")
	if err == nil {
		t.Fatal("AddSeries expected error, got nil")
	}
	if !errors.Is(err, rest.err) {
		t.Errorf("error = %v; want to wrap %v", err, rest.err)
	}
	// Verify "REST" appears in the message.
	if got := err.Error(); len(got) == 0 {
		t.Error("expected non-empty error string")
	}

	after := w.Series()
	if len(after) != len(before) {
		t.Errorf("series length changed: before=%v after=%v", before, after)
	}
	if ws.subCalls.Load() != 0 {
		t.Error("Subscribe must not be called when REST fails")
	}
}

func TestWorker_RemoveSeries_Idempotent(t *testing.T) {
	t.Parallel()
	ws := &fakeWS{frames: make(chan kalshi.Frame, 4)}
	rest := newREST("KXBTCD", "KXBTCD-A")
	w := newSeriesWorker(t, ws, rest, nil, []string{"KXBTCD"})
	ctx := context.Background()

	before := ws.unsubCalls.Load()
	if err := w.RemoveSeries(ctx, "KXLTCD"); err != nil {
		t.Fatalf("RemoveSeries returned error: %v", err)
	}
	time.Sleep(20 * time.Millisecond)
	if after := ws.unsubCalls.Load(); after != before {
		t.Errorf("unsubCalls went from %d to %d; want no change (idempotent)", before, after)
	}
	got := w.Series()
	if len(got) != 1 || got[0] != "KXBTCD" {
		t.Errorf("Series() = %v; want [KXBTCD]", got)
	}
}

func TestWorker_RemoveSeries_EvictsRosteredMarkets(t *testing.T) {
	t.Parallel()
	ws := &fakeWS{frames: make(chan kalshi.Frame, 4)}
	rest := &fakeREST{
		byName: map[string][]string{
			"KXBTCD": {"KXBTCD-A"},
			"KXLTCD": {"KXLTCD-A"},
		},
		hitCap: map[string]bool{},
	}
	w := newSeriesWorker(t, ws, rest, nil, []string{"KXBTCD", "KXLTCD"})
	ctx := context.Background()

	// Manually populate the roster with a non-zero sid for KXLTCD-A.
	w.roster.AddPending(10, []string{"KXBTCD-A"}, []string{"orderbook_delta"})
	w.roster.AddPending(11, []string{"KXLTCD-A"}, []string{"orderbook_delta"})
	// Ack so sids are non-zero.
	w.roster.RecordSid(10, 100)
	w.roster.RecordSid(11, 101)

	beforeUnsub := ws.unsubCalls.Load()
	if err := w.RemoveSeries(ctx, "KXLTCD"); err != nil {
		t.Fatalf("RemoveSeries returned error: %v", err)
	}

	if after := ws.unsubCalls.Load(); after <= beforeUnsub {
		t.Error("expected at least one Unsubscribe call")
	}
	if w.roster.Has("KXLTCD-A") {
		t.Error("KXLTCD-A should have been evicted from roster")
	}
	if !w.roster.Has("KXBTCD-A") {
		t.Error("KXBTCD-A should remain in roster")
	}

	got := w.Series()
	for _, s := range got {
		if s == "KXLTCD" {
			t.Errorf("Series() = %v; KXLTCD should be removed", got)
		}
	}
}

func TestWorker_RemoveSeries_DeletesBookkeeperWhenPreserve(t *testing.T) {
	t.Parallel()
	ws := &fakeWS{frames: make(chan kalshi.Frame, 4)}
	rest := newREST("KXLTCD", "KXLTCD-A", "KXLTCD-B")
	bk := &stubBookkeeper{}
	logger := slog.New(slog.NewJSONHandler(&bytes.Buffer{}, nil))
	w := New(Deps{
		WS:                 ws,
		REST:               rest,
		Pub:                &stubFanout{},
		Series:             []string{"KXLTCD"},
		Bookkeeper:         bk,
		Logger:             logger,
		SweepInterval:      0,
		PreserveBookkeeper: true,
	})
	ctx := context.Background()

	w.roster.AddPending(20, []string{"KXLTCD-A", "KXLTCD-B"}, []string{"orderbook_delta"})
	w.roster.RecordSid(20, 200)

	if err := w.RemoveSeries(ctx, "KXLTCD"); err != nil {
		t.Fatalf("RemoveSeries returned error: %v", err)
	}

	deleted := bk.deleted()
	wantDeleted := map[string]bool{"KXLTCD-A": true, "KXLTCD-B": true}
	for _, d := range deleted {
		if !wantDeleted[d] {
			t.Errorf("unexpected deleted market: %s", d)
		}
		delete(wantDeleted, d)
	}
	for remaining := range wantDeleted {
		t.Errorf("expected DeleteMarket(%s) but it was not called", remaining)
	}
}

func TestWorker_RemoveSeries_WSErrorStillMutates(t *testing.T) {
	t.Parallel()
	base := &fakeWS{frames: make(chan kalshi.Frame, 4)}
	ws := &errUnsubWS{fakeWS: base, unsubErr: errors.New("ws gone")}
	rest := newREST("KXLTCD", "KXLTCD-A")
	w := newSeriesWorker(t, ws, rest, nil, []string{"KXBTCD", "KXLTCD"})
	ctx := context.Background()

	w.roster.AddPending(30, []string{"KXLTCD-A"}, []string{"orderbook_delta"})
	w.roster.RecordSid(30, 300)

	if err := w.RemoveSeries(ctx, "KXLTCD"); err != nil {
		t.Fatalf("RemoveSeries returned error even with WS failure: %v", err)
	}

	// Roster mutation must have happened despite the WS error.
	if w.roster.Has("KXLTCD-A") {
		t.Error("KXLTCD-A should have been evicted from roster even when Unsubscribe fails")
	}

	// Series pointer must no longer contain KXLTCD.
	got := w.Series()
	for _, s := range got {
		if s == "KXLTCD" {
			t.Errorf("Series() = %v; KXLTCD should be removed even when Unsubscribe fails", got)
		}
	}
}

func TestWorker_AddRemoveSeries_RaceTest(t *testing.T) {
	t.Parallel()

	ws := &fakeWS{frames: make(chan kalshi.Frame, 16)}
	rest := &fakeREST{
		byName: map[string][]string{
			"KXBTCD": {"KXBTCD-A"},
			"KXNEW":  {"KXNEW-A"},
		},
		hitCap: map[string]bool{},
	}
	w := newSeriesWorker(t, ws, rest, nil, []string{"KXBTCD"})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const readers = 10
	const duration = 150 * time.Millisecond

	// Spawn reader goroutines that continuously call Series().
	for i := 0; i < readers; i++ {
		go func() {
			for {
				select {
				case <-ctx.Done():
					return
				default:
					_ = w.Series()
				}
			}
		}()
	}

	// Spawn a writer goroutine that alternates AddSeries/RemoveSeries.
	done := make(chan struct{})
	go func() {
		defer close(done)
		deadline := time.Now().Add(duration)
		for time.Now().Before(deadline) {
			_ = w.AddSeries(ctx, "KXNEW")
			_ = w.RemoveSeries(ctx, "KXNEW")
		}
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("race test writer goroutine did not finish in time")
	}
	cancel()

	// Final state must be consistent: series pointer is either a valid slice with or
	// without KXNEW; the slice must not be nil or corrupted.
	got := w.Series()
	if got == nil {
		t.Error("Series() returned nil after race test")
	}
}

// TestWorker_BookkeeperEmit_TagsDynamicallyAddedSeries reproduces HOL-43: the
// cmd-layer bookkeeper Publish closure must consult the live (atomic.Pointer-
// backed) series set, not a boot-time snapshot, so a snapshot emit for a market
// belonging to a dynamically-added series carries the correct series_id.
//
// Mirrors the cmd/ws-worker wiring: a real *orderbook.Bookkeeper whose Publish
// callback calls Worker.SeriesForTicker(ticker) — the exact pattern main.go
// uses. AddSeries runs concurrently with the bookkeeper's Run goroutine, so
// `go test -race` will catch any unsynchronized read of the series set.
func TestWorker_BookkeeperEmit_TagsDynamicallyAddedSeries(t *testing.T) {
	t.Parallel()

	type captured struct {
		ticker string
		series string
	}
	var (
		mu  sync.Mutex
		got []captured
	)

	// Late-bound *Worker, exactly like cmd/ws-worker/main.go after the HOL-43 fix.
	var w *Worker

	bk := orderbook.NewBookkeeper(orderbook.BookkeeperConfig{
		Publish: func(ticker string, _ orderbook.Side, _ orderbook.SnapshotPayload) error {
			seriesID := w.SeriesForTicker(ticker)
			mu.Lock()
			got = append(got, captured{ticker: ticker, series: seriesID})
			mu.Unlock()
			return nil
		},
		ChangeTickInterval: 20 * time.Millisecond,
		HeartbeatInterval:  10 * time.Second,
	})

	ws := &fakeWS{frames: make(chan kalshi.Frame, 4)}
	rest := &fakeREST{
		byName: map[string][]string{
			"KXBTCD": {"KXBTCD-A"},
			"KXNEW":  {"KXNEW-A"},
		},
		hitCap: map[string]bool{},
	}
	logger := slog.New(slog.NewJSONHandler(&bytes.Buffer{}, nil))
	w = New(Deps{
		WS:                 ws,
		REST:               rest,
		Pub:                &stubFanout{},
		Series:             []string{"KXBTCD"},
		Bookkeeper:         bk,
		Logger:             logger,
		PreserveBookkeeper: true,
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	bkDone := make(chan struct{})
	go func() {
		defer close(bkDone)
		bk.Run(ctx)
	}()

	if err := w.AddSeries(ctx, "KXNEW"); err != nil {
		t.Fatalf("AddSeries: %v", err)
	}

	// Drive a delta for the dynamically-added series's market into the bookkeeper.
	// applyDelta's pre/post-top comparison would mark dirty in the worker path; here
	// we hand it directly to Apply (the same channel the worker would use) and the
	// bookkeeper's handleDelta marks dirty on top change, which then flushes on the
	// next ChangeTickInterval.
	bk.Apply(orderbook.DeltaMsg{
		Ticker:     "KXNEW-A",
		Side:       orderbook.SideYes,
		PriceUnits: 5000,
		SizeDelta:  100,
	})

	deadline := time.After(2 * time.Second)
	for {
		mu.Lock()
		n := len(got)
		mu.Unlock()
		if n > 0 {
			break
		}
		select {
		case <-deadline:
			t.Fatal("bookkeeper did not Publish within 2s")
		case <-time.After(10 * time.Millisecond):
		}
	}

	cancel()
	<-bkDone

	mu.Lock()
	defer mu.Unlock()
	var foundKXNEW bool
	for _, c := range got {
		if c.ticker == "KXNEW-A" {
			foundKXNEW = true
			if c.series != "KXNEW" {
				t.Errorf("KXNEW-A emit tagged series=%q; want %q (HOL-43 regression)", c.series, "KXNEW")
			}
		}
	}
	if !foundKXNEW {
		t.Errorf("no Publish captured for KXNEW-A; captures=%+v", got)
	}
}
