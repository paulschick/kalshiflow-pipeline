package featureflag

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"

	"cloud.google.com/go/storage"
)

// captureSlog swaps slog.Default for a JSON handler writing to a buffer.
// Returns the buffer and a restore func; not parallel-safe (global default).
func captureSlog(t *testing.T) (*bytes.Buffer, func()) {
	t.Helper()
	var buf bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})))
	return &buf, func() { slog.SetDefault(prev) }
}

// countSlogEvents returns the number of JSON log lines in buf whose "msg"
// field equals msg.
func countSlogEvents(t *testing.T, buf *bytes.Buffer, msg string) int {
	t.Helper()
	n := 0
	for _, line := range strings.Split(strings.TrimSpace(buf.String()), "\n") {
		if line == "" {
			continue
		}
		var rec map[string]any
		if err := json.Unmarshal([]byte(line), &rec); err != nil {
			t.Fatalf("non-JSON slog line %q: %v", line, err)
		}
		if rec["msg"] == msg {
			n++
		}
	}
	return n
}

// fakeObjHandle is the test seam for KrakenFlag. Mirrors the
// internal/worker/desiredset_test.go pattern.
type fakeObjHandle struct {
	mu       sync.Mutex
	gen      int64
	body     string
	attrsErr error
	readErr  error
}

func (f *fakeObjHandle) set(gen int64, body string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.gen = gen
	f.body = body
	f.attrsErr = nil
	f.readErr = nil
}

func (f *fakeObjHandle) setAttrsErr(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.attrsErr = err
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

// readerCallTracker counts NewReader invocations to assert cheap-unchanged behaviour.
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

func (r *readerCallTracker) calls() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.count
}

func bodyJSON(enabled bool) string {
	if enabled {
		return `{"enabled":true,"generation":"01HX","generated_at":"2026-05-13T00:00:00.000Z","reason":"t","actor":"t"}`
	}
	return `{"enabled":false,"generation":"01HY","generated_at":"2026-05-13T00:01:00.000Z","reason":"t","actor":"t"}`
}

func TestKrakenFlag_AbsentNeverLoaded_DefaultsOn(t *testing.T) {
	t.Parallel()
	obj := &fakeObjHandle{attrsErr: storage.ErrObjectNotExist}
	f := newKrakenFlag(obj)
	err := f.LoadOnce(context.Background())
	if !errors.Is(err, ErrAbsent) {
		t.Fatalf("want ErrAbsent; got %v", err)
	}
	if !f.Enabled() {
		t.Errorf("Enabled() = false; want true (default-on iff never-loaded)")
	}
}

func TestKrakenFlag_FirstLoadEnabledTrue(t *testing.T) {
	t.Parallel()
	obj := &fakeObjHandle{gen: 7, body: bodyJSON(true)}
	f := newKrakenFlag(obj)
	if err := f.LoadOnce(context.Background()); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if !f.Enabled() {
		t.Errorf("Enabled() = false; want true")
	}
}

func TestKrakenFlag_FirstLoadEnabledFalse(t *testing.T) {
	t.Parallel()
	obj := &fakeObjHandle{gen: 7, body: bodyJSON(false)}
	f := newKrakenFlag(obj)
	if err := f.LoadOnce(context.Background()); err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if f.Enabled() {
		t.Errorf("Enabled() = true; want false")
	}
}

func TestKrakenFlag_GenerationUnchangedSkipsBody(t *testing.T) {
	t.Parallel()
	tracker := &readerCallTracker{fakeObjHandle: &fakeObjHandle{gen: 7, body: bodyJSON(true)}}
	f := newKrakenFlag(tracker)
	if err := f.LoadOnce(context.Background()); err != nil {
		t.Fatalf("first LoadOnce err: %v", err)
	}
	if got := tracker.calls(); got != 1 {
		t.Fatalf("first NewReader calls = %d; want 1", got)
	}
	if err := f.LoadOnce(context.Background()); err != nil {
		t.Fatalf("second LoadOnce err: %v", err)
	}
	if got := tracker.calls(); got != 1 {
		t.Errorf("second LoadOnce caused %d NewReader call(s); want still 1", got)
	}
}

func TestKrakenFlag_GenerationChangeFlips(t *testing.T) {
	t.Parallel()
	obj := &fakeObjHandle{gen: 7, body: bodyJSON(true)}
	f := newKrakenFlag(obj)
	if err := f.LoadOnce(context.Background()); err != nil {
		t.Fatalf("first LoadOnce: %v", err)
	}
	if !f.Enabled() {
		t.Fatal("want enabled after first load")
	}
	obj.set(8, bodyJSON(false))
	if err := f.LoadOnce(context.Background()); err != nil {
		t.Fatalf("second LoadOnce: %v", err)
	}
	if f.Enabled() {
		t.Error("want enabled=false after generation change to enabled:false body")
	}
}

func TestKrakenFlag_AbsentAfterPriorLoad_RetainsLastKnown(t *testing.T) {
	t.Parallel()
	obj := &fakeObjHandle{gen: 7, body: bodyJSON(false)}
	f := newKrakenFlag(obj)
	if err := f.LoadOnce(context.Background()); err != nil {
		t.Fatalf("first LoadOnce: %v", err)
	}
	if f.Enabled() {
		t.Fatal("want enabled=false after first load")
	}
	obj.setAttrsErr(storage.ErrObjectNotExist)
	err := f.LoadOnce(context.Background())
	if !errors.Is(err, ErrAbsent) {
		t.Fatalf("want ErrAbsent; got %v", err)
	}
	if f.Enabled() {
		t.Error("Enabled() flipped to true after absent-post-load; want retain false")
	}
}

func TestKrakenFlag_ParseFailNeverLoaded_RetainsDefaultOn(t *testing.T) {
	t.Parallel()
	obj := &fakeObjHandle{gen: 7, body: `{NOT VALID JSON`}
	f := newKrakenFlag(obj)
	err := f.LoadOnce(context.Background())
	if err == nil {
		t.Fatal("want decode error; got nil")
	}
	if !f.Enabled() {
		t.Errorf("Enabled() = false after parse fail at boot; want true (construction default unchanged)")
	}
}

func TestKrakenFlag_ParseFailAfterPriorLoad_RetainsLastKnown(t *testing.T) {
	t.Parallel()
	obj := &fakeObjHandle{gen: 7, body: bodyJSON(false)}
	f := newKrakenFlag(obj)
	if err := f.LoadOnce(context.Background()); err != nil {
		t.Fatalf("first LoadOnce: %v", err)
	}
	if f.Enabled() {
		t.Fatal("want enabled=false after first load")
	}
	obj.set(8, `{NOT VALID JSON`)
	err := f.LoadOnce(context.Background())
	if err == nil {
		t.Fatal("want decode error")
	}
	if f.Enabled() {
		t.Error("Enabled() flipped to true after parse fail; want retain false")
	}
}

func TestKrakenFlag_PollConsecutiveErrors(t *testing.T) {
	t.Parallel()
	obj := &fakeObjHandle{attrsErr: errors.New("boom")}
	f := newKrakenFlag(obj)
	counter := 0
	for i := 0; i < 3; i++ {
		f.pollOnce(context.Background(), &counter)
	}
	if counter != 3 {
		t.Errorf("consecutive_errs = %d after 3 transient errors; want 3", counter)
	}
	obj.setAttrsErr(storage.ErrObjectNotExist)
	f.pollOnce(context.Background(), &counter)
	if counter != 0 {
		t.Errorf("consecutive_errs = %d after ErrAbsent reset; want 0", counter)
	}
}

// TestKrakenFlag_PollParseFailDoesNotEscalate verifies that parse-fail does
// NOT count toward the consecutive-error WARN escalation in pollOnce (spec
// §4.1: parse-fail is ERROR, transient is WARN-after-≥2; they should not
// double-log).
func TestKrakenFlag_PollParseFailDoesNotEscalate(t *testing.T) {
	obj := &fakeObjHandle{gen: 7, body: `{NOT VALID JSON`}
	f := newKrakenFlag(obj)
	counter := 0
	for i := 0; i < 3; i++ {
		f.pollOnce(context.Background(), &counter)
	}
	if counter != 0 {
		t.Errorf("consecutive_errs = %d after 3 parse-fail polls; want 0 (parse-fail must not escalate)", counter)
	}
}

// TestKrakenFlag_ParseFailAdvancesLastGen verifies that a parse-fail object
// is read once, then skipped via the cheap-Generation check on subsequent
// polls. Otherwise a stuck-bad-body would re-download + re-ERROR every
// 60s until operator republish.
func TestKrakenFlag_ParseFailAdvancesLastGen(t *testing.T) {
	tracker := &readerCallTracker{fakeObjHandle: &fakeObjHandle{gen: 7, body: `{NOT VALID JSON`}}
	f := newKrakenFlag(tracker)
	if err := f.LoadOnce(context.Background()); err == nil {
		t.Fatal("want parse error; got nil")
	}
	if got := tracker.calls(); got != 1 {
		t.Fatalf("after first parse-fail LoadOnce: NewReader calls = %d; want 1", got)
	}
	// Second LoadOnce at same generation must skip body read.
	if err := f.LoadOnce(context.Background()); err != nil {
		t.Fatalf("second LoadOnce: %v (want nil — cheap-Generation skip)", err)
	}
	if got := tracker.calls(); got != 1 {
		t.Errorf("second LoadOnce caused %d NewReader call(s); want still 1 (parse-fail must advance lastGen)", got)
	}
}

// TestKrakenFlag_ChangeEventTransitionOnly verifies that kraken_flag_changed
// is emitted strictly on a value transition: first successful load matching
// the construction default (true) is silent; same-value re-loads are silent;
// only true→false / false→true flip emits.
func TestKrakenFlag_ChangeEventTransitionOnly(t *testing.T) {
	buf, restore := captureSlog(t)
	defer restore()

	obj := &fakeObjHandle{gen: 7, body: bodyJSON(true)}
	f := newKrakenFlag(obj)

	// 1. First load, body.enabled=true matches construction default → no emit.
	if err := f.LoadOnce(context.Background()); err != nil {
		t.Fatalf("first LoadOnce: %v", err)
	}
	if got := countSlogEvents(t, buf, "kraken_flag_changed"); got != 0 {
		t.Errorf("kraken_flag_changed emitted %d times after first load true; want 0", got)
	}

	// 2. Generation change, same value → no emit.
	obj.set(8, bodyJSON(true))
	if err := f.LoadOnce(context.Background()); err != nil {
		t.Fatalf("second LoadOnce: %v", err)
	}
	if got := countSlogEvents(t, buf, "kraken_flag_changed"); got != 0 {
		t.Errorf("kraken_flag_changed emitted %d times after same-value reload; want 0", got)
	}

	// 3. true → false flip → emit.
	obj.set(9, bodyJSON(false))
	if err := f.LoadOnce(context.Background()); err != nil {
		t.Fatalf("third LoadOnce: %v", err)
	}
	if got := countSlogEvents(t, buf, "kraken_flag_changed"); got != 1 {
		t.Errorf("kraken_flag_changed emitted %d times after true→false; want 1", got)
	}

	// 4. false → true flip → emit (total now 2).
	obj.set(10, bodyJSON(true))
	if err := f.LoadOnce(context.Background()); err != nil {
		t.Fatalf("fourth LoadOnce: %v", err)
	}
	if got := countSlogEvents(t, buf, "kraken_flag_changed"); got != 2 {
		t.Errorf("kraken_flag_changed total = %d after second flip; want 2", got)
	}
}

// TestKrakenFlag_ChangeEventEmitsOnBootFalse verifies that a first
// successful load with enabled=false emits the change event — because the
// construction default is true, false IS a transition (off-at-boot is an
// operator-visible signal).
func TestKrakenFlag_ChangeEventEmitsOnBootFalse(t *testing.T) {
	buf, restore := captureSlog(t)
	defer restore()

	obj := &fakeObjHandle{gen: 7, body: bodyJSON(false)}
	f := newKrakenFlag(obj)
	if err := f.LoadOnce(context.Background()); err != nil {
		t.Fatalf("LoadOnce: %v", err)
	}
	if got := countSlogEvents(t, buf, "kraken_flag_changed"); got != 1 {
		t.Errorf("kraken_flag_changed emitted %d times after first load false; want 1 (transition from default-on)", got)
	}
}

func TestKrakenFlag_NextDelayWithinBounds(t *testing.T) {
	t.Parallel()
	f := newKrakenFlag(&fakeObjHandle{})
	lo := f.pollInterval - f.pollJitter
	hi := f.pollInterval + f.pollJitter
	for i := 0; i < 100; i++ {
		d := f.nextDelay()
		if d < lo || d > hi {
			t.Fatalf("nextDelay = %v; want in [%v, %v]", d, lo, hi)
		}
	}
}
