package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"cloud.google.com/go/storage"

	"github.com/paulschick/kalshiflow-pipeline/internal/kalshi"
	"github.com/paulschick/kalshiflow-pipeline/internal/testutil"
)

// ---------------------------------------------------------------------------
// In-package objHandle shim so GCSDesiredSetLoader tests don't need a live
// HTTP server. The shim routes Load through a narrow interface; tests inject
// a fakeObjHandle.
// ---------------------------------------------------------------------------

type objHandle interface {
	Attrs(ctx context.Context) (*storage.ObjectAttrs, error)
	NewReader(ctx context.Context) (io.ReadCloser, error)
}

// gcsLoaderViaHandle is the testable core of GCSDesiredSetLoader.
type gcsLoaderViaHandle struct {
	obj objHandle
}

func (l *gcsLoaderViaHandle) Load(ctx context.Context, lastGen int64) ([]string, int64, bool, error) {
	attrs, err := l.obj.Attrs(ctx)
	switch {
	case errors.Is(err, storage.ErrObjectNotExist):
		return nil, 0, false, ErrAbsent
	case err != nil:
		return nil, lastGen, false, err
	}
	// We key poll-skip on the GCS int64 generation, not the ULID in the JSON body.
	if attrs.Generation == lastGen {
		return nil, lastGen, false, nil
	}
	r, err := l.obj.NewReader(ctx)
	if err != nil {
		return nil, lastGen, false, err
	}
	defer func() { _ = r.Close() }()
	var payload desiredSetFile
	if err := json.NewDecoder(r).Decode(&payload); err != nil {
		return nil, lastGen, false, wrapDecodeErr(err)
	}
	return payload.Tickers, attrs.Generation, true, nil
}

// wrapDecodeErr adds a "decode" prefix so tests can assert the error origin.
func wrapDecodeErr(err error) error {
	return &decodeErr{err}
}

type decodeErr struct{ cause error }

func (e *decodeErr) Error() string { return "decode: " + e.cause.Error() }
func (e *decodeErr) Unwrap() error { return e.cause }

// ---------------------------------------------------------------------------
// fakeObjHandle
// ---------------------------------------------------------------------------

type fakeObjHandle struct {
	mu       sync.Mutex
	gen      int64
	body     string
	attrsErr error
	readErr  error
}

func (f *fakeObjHandle) Attrs(_ context.Context) (*storage.ObjectAttrs, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.attrsErr != nil {
		return nil, f.attrsErr
	}
	return &storage.ObjectAttrs{Generation: f.gen}, nil
}

func (f *fakeObjHandle) NewReader(_ context.Context) (io.ReadCloser, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.readErr != nil {
		return nil, f.readErr
	}
	return io.NopCloser(bytes.NewBufferString(f.body)), nil
}

// readerCallTracker counts NewReader invocations.
type readerCallTracker struct {
	*fakeObjHandle
	mu    sync.Mutex
	count int
}

func (r *readerCallTracker) NewReader(ctx context.Context) (io.ReadCloser, error) {
	r.mu.Lock()
	r.count++
	r.mu.Unlock()
	return r.fakeObjHandle.NewReader(ctx)
}

func (r *readerCallTracker) readerCalls() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.count
}

// ---------------------------------------------------------------------------
// GCSDesiredSetLoader unit tests
// ---------------------------------------------------------------------------

func TestGCSDesiredSetLoader_Absent(t *testing.T) {
	t.Parallel()
	obj := &fakeObjHandle{attrsErr: storage.ErrObjectNotExist}
	l := &gcsLoaderViaHandle{obj: obj}
	_, gen, changed, err := l.Load(context.Background(), 0)
	if !errors.Is(err, ErrAbsent) {
		t.Fatalf("want ErrAbsent; got %v", err)
	}
	if changed {
		t.Error("changed must be false on ErrAbsent")
	}
	if gen != 0 {
		t.Errorf("gen = %d; want 0", gen)
	}
}

func TestGCSDesiredSetLoader_GenerationMatch(t *testing.T) {
	t.Parallel()
	tracker := &readerCallTracker{fakeObjHandle: &fakeObjHandle{gen: 7}}
	l := &gcsLoaderViaHandle{obj: tracker}
	_, gen, changed, err := l.Load(context.Background(), 7)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if changed {
		t.Error("changed must be false when gen matches lastGen")
	}
	if gen != 7 {
		t.Errorf("gen = %d; want 7", gen)
	}
	if n := tracker.readerCalls(); n != 0 {
		t.Errorf("NewReader called %d times; want 0 (no body read on gen match)", n)
	}
}

func TestGCSDesiredSetLoader_NewGeneration(t *testing.T) {
	t.Parallel()
	body := `{"generation":"01HX","generated_at":"2026-05-11T00:00:00Z","tickers":["KXBTCD","KXETHD"]}`
	obj := &fakeObjHandle{gen: 42, body: body}
	l := &gcsLoaderViaHandle{obj: obj}
	tickers, gen, changed, err := l.Load(context.Background(), 0)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if !changed {
		t.Error("changed must be true when gen differs")
	}
	if gen != 42 {
		t.Errorf("gen = %d; want 42", gen)
	}
	want := []string{"KXBTCD", "KXETHD"}
	if len(tickers) != len(want) {
		t.Fatalf("tickers = %v; want %v", tickers, want)
	}
	for i, w := range want {
		if tickers[i] != w {
			t.Errorf("tickers[%d] = %q; want %q", i, tickers[i], w)
		}
	}
}

func TestGCSDesiredSetLoader_DecodeError(t *testing.T) {
	t.Parallel()
	obj := &fakeObjHandle{gen: 5, body: `{NOT VALID JSON`}
	l := &gcsLoaderViaHandle{obj: obj}
	_, _, _, err := l.Load(context.Background(), 0)
	if err == nil {
		t.Fatal("expected error on bad JSON; got nil")
	}
	if got := err.Error(); len(got) < 7 || got[:7] != "decode:" {
		t.Errorf("error %q does not start with 'decode:'", got)
	}
}

// ---------------------------------------------------------------------------
// stubDesiredSetLoader for reconciler tests
// ---------------------------------------------------------------------------

type dslResponse struct {
	tickers []string
	gen     int64
	changed bool
	err     error
}

type stubDesiredSetLoader struct {
	mu        sync.Mutex
	responses []dslResponse
	idx       int
	calls     int
}

func (s *stubDesiredSetLoader) Load(_ context.Context, _ int64) ([]string, int64, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls++
	if s.idx >= len(s.responses) {
		last := s.responses[len(s.responses)-1]
		return last.tickers, last.gen, last.changed, last.err
	}
	r := s.responses[s.idx]
	s.idx++
	return r.tickers, r.gen, r.changed, r.err
}

func (s *stubDesiredSetLoader) callCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls
}

// ---------------------------------------------------------------------------
// Worker builder helpers for reconciler tests
// ---------------------------------------------------------------------------

func newReconcilerWorker(t *testing.T, loader DesiredSetLoader, series []string, interval, jitter time.Duration) *Worker {
	t.Helper()
	ws := &fakeWS{frames: make(chan kalshi.Frame, 4)}
	byName := make(map[string][]string, len(series))
	for _, s := range series {
		byName[s] = []string{}
	}
	rest := &fakeREST{byName: byName, hitCap: map[string]bool{}}
	logger := slog.New(slog.NewJSONHandler(&bytes.Buffer{}, nil))
	w := New(Deps{
		WS:                     ws,
		REST:                   rest,
		Pub:                    &stubFanout{},
		Series:                 series,
		Bookkeeper:             &stubBookkeeper{},
		Logger:                 logger,
		SweepInterval:          0,
		DesiredSetLoader:       loader,
		DesiredSetPollInterval: interval,
	})
	// Override jitter directly (same pattern as w.sweepJitter = 0 in sweep tests)
	// to prevent the 5s default from inflating test wall-clock time.
	w.desiredSetPollJitter = jitter
	return w
}

// seriesErrREST returns err for a specific series; delegates all others.
type seriesErrREST struct {
	failSeries string
	err        error
	delegate   REST
}

func (s *seriesErrREST) GetOpenMarkets(ctx context.Context, series, status string, maxPages int) ([]string, bool, error) {
	if series == s.failSeries {
		return nil, false, s.err
	}
	return s.delegate.GetOpenMarkets(ctx, series, status, maxPages)
}

// ---------------------------------------------------------------------------
// Reconciler tests
// ---------------------------------------------------------------------------

func TestWorker_Reconciler_UnchangedGenerationNoOp(t *testing.T) {
	t.Parallel()
	loader := &stubDesiredSetLoader{
		responses: []dslResponse{
			{gen: 7, changed: false},
		},
	}
	w := newReconcilerWorker(t, loader, []string{"KXBTCD"}, 20*time.Millisecond, 0)
	w.lastDesiredGen.Store(7)

	ws := w.d.WS.(*fakeWS)
	beforeSub := ws.subCalls.Load()
	beforeUnsub := ws.unsubCalls.Load()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go w.runDesiredSetReconciler(ctx)

	testutil.WaitFor(t, 500*time.Millisecond, "at least one load call", func() bool {
		return loader.callCount() >= 1
	})
	cancel()

	if ws.subCalls.Load() != beforeSub {
		t.Errorf("subCalls changed; want no subscribe on unchanged gen")
	}
	if ws.unsubCalls.Load() != beforeUnsub {
		t.Errorf("unsubCalls changed; want no unsubscribe on unchanged gen")
	}
}

func TestWorker_Reconciler_NewGenerationAppliesDelta(t *testing.T) {
	t.Parallel()
	loader := &stubDesiredSetLoader{
		responses: []dslResponse{
			{tickers: []string{"KXBTCD", "KXLTCD"}, gen: 11, changed: true},
		},
	}
	ws := &fakeWS{frames: make(chan kalshi.Frame, 4)}
	rest := &fakeREST{
		byName: map[string][]string{
			"KXBTCD": {},
			"KXETHD": {},
			"KXLTCD": {},
		},
		hitCap: map[string]bool{},
	}
	logger := slog.New(slog.NewJSONHandler(&bytes.Buffer{}, nil))
	w := New(Deps{
		WS:                     ws,
		REST:                   rest,
		Pub:                    &stubFanout{},
		Series:                 []string{"KXBTCD", "KXETHD"},
		Bookkeeper:             &stubBookkeeper{},
		Logger:                 logger,
		SweepInterval:          0,
		DesiredSetLoader:       loader,
		DesiredSetPollInterval: 20 * time.Millisecond,
	})
	w.desiredSetPollJitter = 0

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go w.runDesiredSetReconciler(ctx)

	testutil.WaitFor(t, 500*time.Millisecond, "gen advanced to 11", func() bool {
		return w.lastDesiredGen.Load() == 11
	})
	cancel()

	got := w.Series()
	hasKXBTCD, hasKXLTCD, hasKXETHD := false, false, false
	for _, s := range got {
		switch s {
		case "KXBTCD":
			hasKXBTCD = true
		case "KXLTCD":
			hasKXLTCD = true
		case "KXETHD":
			hasKXETHD = true
		}
	}
	if !hasKXBTCD {
		t.Error("KXBTCD should remain in series")
	}
	if !hasKXLTCD {
		t.Error("KXLTCD should have been added")
	}
	if hasKXETHD {
		t.Error("KXETHD should have been removed")
	}
	if len(got) != 2 {
		t.Errorf("Series() len = %d; want 2", len(got))
	}
}

func TestWorker_Reconciler_EmptyTickersRetainsPriorSet(t *testing.T) {
	t.Parallel()
	loader := &stubDesiredSetLoader{
		responses: []dslResponse{
			{tickers: []string{}, gen: 12, changed: true},
		},
	}
	w := newReconcilerWorker(t, loader, []string{"KXBTCD"}, 20*time.Millisecond, 0)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go w.runDesiredSetReconciler(ctx)

	testutil.WaitFor(t, 500*time.Millisecond, "at least one load call", func() bool {
		return loader.callCount() >= 1
	})
	cancel()

	got := w.Series()
	if len(got) != 1 || got[0] != "KXBTCD" {
		t.Errorf("Series() = %v; want [KXBTCD] (retained on empty desired set)", got)
	}
	if w.lastDesiredGen.Load() == 12 {
		t.Error("lastDesiredGen must not advance when tickers is empty")
	}
}

func TestWorker_Reconciler_TwoConsecutiveErrorsFlipStale(t *testing.T) {
	t.Parallel()
	sentinel := errors.New("upstream timeout")
	loader := &stubDesiredSetLoader{
		responses: []dslResponse{
			{err: sentinel},
			{err: sentinel},
			{err: sentinel},
		},
	}
	w := newReconcilerWorker(t, loader, []string{"KXBTCD"}, 20*time.Millisecond, 0)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go w.runDesiredSetReconciler(ctx)

	testutil.WaitFor(t, 500*time.Millisecond, "at least 2 error loads", func() bool {
		return loader.callCount() >= 2
	})
	cancel()

	// Series unchanged.
	got := w.Series()
	if len(got) != 1 || got[0] != "KXBTCD" {
		t.Errorf("Series() = %v; want [KXBTCD] unchanged after errors", got)
	}
	if w.lastDesiredGen.Load() != 0 {
		t.Errorf("lastDesiredGen = %d; want 0 on consecutive errors", w.lastDesiredGen.Load())
	}
}

func TestWorker_Reconciler_ErrAbsentDoesNotFlipStale(t *testing.T) {
	t.Parallel()
	loader := &stubDesiredSetLoader{
		responses: []dslResponse{
			{err: ErrAbsent},
			{err: ErrAbsent},
			{err: ErrAbsent},
		},
	}
	w := newReconcilerWorker(t, loader, []string{"KXBTCD"}, 20*time.Millisecond, 0)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go w.runDesiredSetReconciler(ctx)

	testutil.WaitFor(t, 500*time.Millisecond, "at least 2 ErrAbsent loads", func() bool {
		return loader.callCount() >= 2
	})
	cancel()

	got := w.Series()
	if len(got) != 1 || got[0] != "KXBTCD" {
		t.Errorf("Series() = %v; want [KXBTCD] unchanged on ErrAbsent", got)
	}
	if w.lastDesiredGen.Load() != 0 {
		t.Errorf("lastDesiredGen = %d; want 0 on ErrAbsent", w.lastDesiredGen.Load())
	}
}

func TestWorker_Reconciler_PerTickerErrorDoesNotBlockOthers(t *testing.T) {
	t.Parallel()
	loader := &stubDesiredSetLoader{
		responses: []dslResponse{
			// KXAAA sorts before KXBBB; reconciler processes sorted toAdd.
			{tickers: []string{"KXAAA", "KXBBB"}, gen: 20, changed: true},
		},
	}
	ws := &fakeWS{frames: make(chan kalshi.Frame, 4)}
	rest := &seriesErrREST{
		failSeries: "KXAAA",
		err:        errors.New("rest fail for KXAAA"),
		delegate: &fakeREST{
			byName: map[string][]string{"KXAAA": {}, "KXBBB": {}},
			hitCap: map[string]bool{},
		},
	}
	logger := slog.New(slog.NewJSONHandler(&bytes.Buffer{}, nil))
	w := New(Deps{
		WS:                     ws,
		REST:                   rest,
		Pub:                    &stubFanout{},
		Series:                 []string{},
		Bookkeeper:             &stubBookkeeper{},
		Logger:                 logger,
		SweepInterval:          0,
		DesiredSetLoader:       loader,
		DesiredSetPollInterval: 20 * time.Millisecond,
	})
	w.desiredSetPollJitter = 0

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go w.runDesiredSetReconciler(ctx)

	testutil.WaitFor(t, 500*time.Millisecond, "gen advanced to 20", func() bool {
		return w.lastDesiredGen.Load() == 20
	})
	cancel()

	got := w.Series()
	hasKXBBB := false
	for _, s := range got {
		if s == "KXBBB" {
			hasKXBBB = true
		}
	}
	if !hasKXBBB {
		t.Errorf("Series() = %v; want KXBBB added despite KXAAA error", got)
	}
	if w.lastDesiredGen.Load() != 20 {
		t.Errorf("lastDesiredGen = %d; want 20 even after per-ticker error", w.lastDesiredGen.Load())
	}
}
