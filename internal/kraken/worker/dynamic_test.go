package worker

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/paulschick/kalshiflow-pipeline/internal/featureflag"
	"github.com/paulschick/kalshiflow-pipeline/internal/kraken/control"
	"github.com/paulschick/kalshiflow-pipeline/internal/kraken/rest"
)

// assetPairsHandler returns a httptest handler whose response depends on the
// CSV passed in `pair=...`: any pair not in `known` causes the whole CSV to
// fail with EQuery:Unknown asset pair (matching real Kraken behaviour).
func assetPairsHandler(t *testing.T, known map[string]struct{}) http.Handler {
	t.Helper()
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw := r.URL.Query().Get("pair")
		input := strings.Split(raw, ",")
		for _, p := range input {
			if _, ok := known[p]; !ok {
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write([]byte(`{"error":["EQuery:Unknown asset pair"]}`))
				return
			}
		}
		w.Header().Set("Content-Type", "application/json")
		body := `{"error":[],"result":{`
		for i, p := range input {
			if i > 0 {
				body += ","
			}
			body += fmt.Sprintf(`%q:{"altname":"X","wsname":"X","pair_decimals":2,"lot_decimals":8,"tick_size":"0.01","status":"online"}`, p)
		}
		body += `}}`
		_, _ = w.Write([]byte(body))
	})
}

type stubSubLogger struct {
	mu       sync.Mutex
	rejects  []rest.Rejection
	wantErr  error
	failCall int
}

func (s *stubSubLogger) LogRejection(_ context.Context, pair, reason, _ string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.rejects = append(s.rejects, rest.Rejection{Pair: pair, ErrMessage: reason})
	if s.wantErr != nil && len(s.rejects) >= s.failCall+1 {
		return s.wantErr
	}
	return nil
}

func (s *stubSubLogger) snapshot() []rest.Rejection {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]rest.Rejection, len(s.rejects))
	copy(out, s.rejects)
	return out
}

func newDynamicWorker(t *testing.T, knownPairs map[string]struct{}, initialPairs []string, sub *stubSubLogger) *Worker {
	t.Helper()
	srv := httptest.NewServer(assetPairsHandler(t, knownPairs))
	t.Cleanup(srv.Close)
	return New(Config{
		Flag:       featureflag.NewTestFlag(true),
		REST:       rest.New(srv.Client(), srv.URL),
		Open:       nil,
		WSURL:      "",
		Bookkeeper: newStubBookkeeper(t),
		Pairs:      initialPairs,
		Metrics:    newStubMetrics(t),
		Logger:     nil,
		Now:        time.Now,
		SubscriptionLog: func() SubscriptionLogger {
			if sub == nil {
				return nil
			}
			return sub
		}(),
	})
}

func TestAddPair_Idempotent(t *testing.T) {
	known := map[string]struct{}{"BTC/USD": {}}
	w := newDynamicWorker(t, known, []string{"BTC/USD"}, nil)
	if err := w.AddPair(context.Background(), "BTC/USD"); err != nil {
		t.Errorf("AddPair already-present should be no-op, got %v", err)
	}
	pairs := w.Pairs()
	if len(pairs) != 1 || pairs[0] != "BTC/USD" {
		t.Errorf("Pairs() = %v; want [BTC/USD]", pairs)
	}
}

func TestAddPair_SuccessAddsToSet_NoActiveConn(t *testing.T) {
	known := map[string]struct{}{"BTC/USD": {}, "LTC/USD": {}}
	w := newDynamicWorker(t, known, []string{"BTC/USD"}, nil)
	if err := w.AddPair(context.Background(), "LTC/USD"); err != nil {
		t.Fatalf("AddPair: %v", err)
	}
	pairs := w.Pairs()
	if len(pairs) != 2 || pairs[0] != "BTC/USD" || pairs[1] != "LTC/USD" {
		t.Errorf("Pairs() = %v; want [BTC/USD LTC/USD]", pairs)
	}
}

func TestAddPair_RejectedPreflight_LogsAndDoesNotAdd(t *testing.T) {
	known := map[string]struct{}{"BTC/USD": {}}
	sub := &stubSubLogger{}
	w := newDynamicWorker(t, known, []string{"BTC/USD"}, sub)
	err := w.AddPair(context.Background(), "FOOBAR")
	if err == nil {
		t.Fatal("AddPair on bad pair must return error")
	}
	if !strings.Contains(err.Error(), "EQuery:Unknown asset pair") {
		t.Errorf("err should embed verbatim Kraken message; got %v", err)
	}
	pairs := w.Pairs()
	if len(pairs) != 1 || pairs[0] != "BTC/USD" {
		t.Errorf("Pairs() should not contain rejected pair; got %v", pairs)
	}
	rej := sub.snapshot()
	if len(rej) != 1 || rej[0].Pair != "FOOBAR" {
		t.Errorf("subscription_log expected 1 rejection for FOOBAR; got %v", rej)
	}
	if rej[0].ErrMessage != "EQuery:Unknown asset pair" {
		t.Errorf("rejection reason = %q; want verbatim Kraken string", rej[0].ErrMessage)
	}
}

func TestAddPair_NilSubscriptionLog_StillSurfacesError(t *testing.T) {
	known := map[string]struct{}{"BTC/USD": {}}
	w := newDynamicWorker(t, known, []string{"BTC/USD"}, nil)
	err := w.AddPair(context.Background(), "FOOBAR")
	if err == nil {
		t.Fatal("AddPair must return error even when SubscriptionLog is nil")
	}
}

func TestRemovePair_NotPresent_NoOp(t *testing.T) {
	known := map[string]struct{}{"BTC/USD": {}}
	w := newDynamicWorker(t, known, []string{"BTC/USD"}, nil)
	if err := w.RemovePair(context.Background(), "LTC/USD"); err != nil {
		t.Errorf("RemovePair not-present should be no-op, got %v", err)
	}
}

func TestRemovePair_Success(t *testing.T) {
	known := map[string]struct{}{"BTC/USD": {}, "LTC/USD": {}}
	w := newDynamicWorker(t, known, []string{"BTC/USD", "LTC/USD"}, nil)
	if err := w.RemovePair(context.Background(), "LTC/USD"); err != nil {
		t.Fatalf("RemovePair: %v", err)
	}
	pairs := w.Pairs()
	if len(pairs) != 1 || pairs[0] != "BTC/USD" {
		t.Errorf("Pairs() = %v; want [BTC/USD]", pairs)
	}
}

// stubLoader implements control.DesiredSetLoader for reconciler tests.
type stubLoader struct {
	calls   atomic.Int32
	results chan loadResult
}

type loadResult struct {
	pairs   []string
	gen     int64
	changed bool
	err     error
}

func (s *stubLoader) Load(ctx context.Context, lastGen int64) ([]string, int64, bool, error) {
	s.calls.Add(1)
	select {
	case r := <-s.results:
		return r.pairs, r.gen, r.changed, r.err
	case <-ctx.Done():
		return nil, lastGen, false, ctx.Err()
	}
}

func TestReconciler_DiffDrivesAddRemove(t *testing.T) {
	known := map[string]struct{}{"BTC/USD": {}, "ETH/USD": {}, "SOL/USD": {}, "XRP/USD": {}, "DOGE/USD": {}}
	loader := &stubLoader{results: make(chan loadResult, 4)}

	srv := httptest.NewServer(assetPairsHandler(t, known))
	t.Cleanup(srv.Close)

	w := New(Config{
		Flag:         featureflag.NewTestFlag(true),
		REST:         rest.New(srv.Client(), srv.URL),
		Bookkeeper:   newStubBookkeeper(t),
		Pairs:        []string{"BTC/USD", "ETH/USD"},
		Metrics:      newStubMetrics(t),
		DesiredSet:   loader,
		PollInterval: 20 * time.Millisecond,
		PollJitter:   0,
		Now:          time.Now,
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go w.runDesiredSetReconciler(ctx)

	// First tick: target set is [BTC/USD, SOL/USD]. Expect SOL added, ETH removed.
	loader.results <- loadResult{pairs: []string{"BTC/USD", "SOL/USD"}, gen: 1, changed: true}
	waitFor(t, 3*time.Second, "reconciler add/remove first wave", func() bool {
		p := w.Pairs()
		return len(p) == 2 && p[0] == "BTC/USD" && p[1] == "SOL/USD"
	})

	// Second tick: target set is [XRP/USD, DOGE/USD]. Expect BTC + SOL removed, XRP + DOGE added.
	loader.results <- loadResult{pairs: []string{"DOGE/USD", "XRP/USD"}, gen: 2, changed: true}
	waitFor(t, 3*time.Second, "reconciler second wave", func() bool {
		p := w.Pairs()
		return len(p) == 2 && p[0] == "DOGE/USD" && p[1] == "XRP/USD"
	})

	// Third tick: changed=false → no diff applied.
	loader.results <- loadResult{pairs: nil, gen: 2, changed: false}
	time.Sleep(50 * time.Millisecond)
	p := w.Pairs()
	if len(p) != 2 || p[0] != "DOGE/USD" || p[1] != "XRP/USD" {
		t.Errorf("changed=false should leave set unchanged; got %v", p)
	}
}

func TestReconciler_ErrAbsentSurvives(t *testing.T) {
	known := map[string]struct{}{"BTC/USD": {}}
	loader := &stubLoader{results: make(chan loadResult, 4)}
	srv := httptest.NewServer(assetPairsHandler(t, known))
	t.Cleanup(srv.Close)
	w := New(Config{
		Flag:         featureflag.NewTestFlag(true),
		REST:         rest.New(srv.Client(), srv.URL),
		Bookkeeper:   newStubBookkeeper(t),
		Pairs:        []string{"BTC/USD"},
		Metrics:      newStubMetrics(t),
		DesiredSet:   loader,
		PollInterval: 20 * time.Millisecond,
		PollJitter:   0,
		Now:          time.Now,
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go w.runDesiredSetReconciler(ctx)

	loader.results <- loadResult{err: control.ErrAbsent}
	loader.results <- loadResult{pairs: []string{"BTC/USD"}, gen: 1, changed: true}
	waitFor(t, 3*time.Second, "reconciler eventually succeeds after ErrAbsent", func() bool {
		return loader.calls.Load() >= 2
	})
}

func TestReconciler_EmptySetRetainsPriorSet(t *testing.T) {
	known := map[string]struct{}{"BTC/USD": {}}
	loader := &stubLoader{results: make(chan loadResult, 4)}
	srv := httptest.NewServer(assetPairsHandler(t, known))
	t.Cleanup(srv.Close)
	w := New(Config{
		Flag:         featureflag.NewTestFlag(true),
		REST:         rest.New(srv.Client(), srv.URL),
		Bookkeeper:   newStubBookkeeper(t),
		Pairs:        []string{"BTC/USD"},
		Metrics:      newStubMetrics(t),
		DesiredSet:   loader,
		PollInterval: 20 * time.Millisecond,
		PollJitter:   0,
		Now:          time.Now,
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go w.runDesiredSetReconciler(ctx)

	loader.results <- loadResult{pairs: []string{}, gen: 1, changed: true}
	time.Sleep(80 * time.Millisecond)
	p := w.Pairs()
	if len(p) != 1 || p[0] != "BTC/USD" {
		t.Errorf("empty desired set should retain prior set; got %v", p)
	}
}
