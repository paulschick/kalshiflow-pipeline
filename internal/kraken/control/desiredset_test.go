package control

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"sync"
	"testing"

	"cloud.google.com/go/storage"
)

// ---------------------------------------------------------------------------
// fakeObjHandle — exercises the production loadFromHandle path directly. No
// clone of the loader. A test failure here means GCSDesiredSetLoader.Load is
// broken in production.
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
// Tests
// ---------------------------------------------------------------------------

const (
	testBucket = "<your-archive-bucket>"
	testObject = "control/kraken_pairs.json"
)

func TestLoadFromHandle_Absent(t *testing.T) {
	t.Parallel()
	obj := &fakeObjHandle{attrsErr: storage.ErrObjectNotExist}
	_, gen, changed, err := loadFromHandle(context.Background(), obj, testBucket, testObject, 0)
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

func TestLoadFromHandle_GenerationMatch_NoBodyRead(t *testing.T) {
	t.Parallel()
	tracker := &readerCallTracker{fakeObjHandle: &fakeObjHandle{gen: 7}}
	_, gen, changed, err := loadFromHandle(context.Background(), tracker, testBucket, testObject, 7)
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

func TestLoadFromHandle_NewGeneration(t *testing.T) {
	t.Parallel()
	body := `{"generation":"01HX","generated_at":"2026-05-15T00:00:00Z","pairs":["BTC/USD","ETH/USD","LTC/USD"]}`
	obj := &fakeObjHandle{gen: 42, body: body}
	pairs, gen, changed, err := loadFromHandle(context.Background(), obj, testBucket, testObject, 0)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if !changed {
		t.Error("changed must be true when gen differs")
	}
	if gen != 42 {
		t.Errorf("gen = %d; want 42", gen)
	}
	want := []string{"BTC/USD", "ETH/USD", "LTC/USD"}
	if len(pairs) != len(want) {
		t.Fatalf("pairs = %v; want %v", pairs, want)
	}
	for i, w := range want {
		if pairs[i] != w {
			t.Errorf("pairs[%d] = %q; want %q", i, pairs[i], w)
		}
	}
}

func TestLoadFromHandle_DecodeError(t *testing.T) {
	t.Parallel()
	obj := &fakeObjHandle{gen: 5, body: `{NOT VALID JSON`}
	_, _, _, err := loadFromHandle(context.Background(), obj, testBucket, testObject, 0)
	if err == nil {
		t.Fatal("expected error on bad JSON; got nil")
	}
	// Error must carry the bucket/object context for ops triage and wrap the
	// underlying json error so callers can errors.Is/As if needed.
	got := err.Error()
	if !strings.HasPrefix(got, "decode "+testBucket+"/"+testObject+":") {
		t.Errorf("error %q missing bucket/object context", got)
	}
}

func TestLoadFromHandle_EmptyPairsArraySurfacesAsChanged(t *testing.T) {
	t.Parallel()
	// Empty manifest arrives if discovery wrote an empty desired set (which
	// it shouldn't — WriteKrakenPairsDesired guards). The loader does not
	// itself enforce non-empty; the cmd layer treats empty as a separate
	// failure mode. Asserting "changed=true, pairs=[]" so loader behaviour
	// is deterministic.
	body := `{"generation":"01HX","generated_at":"2026-05-15T00:00:00Z","pairs":[]}`
	obj := &fakeObjHandle{gen: 100, body: body}
	pairs, gen, changed, err := loadFromHandle(context.Background(), obj, testBucket, testObject, 0)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if !changed {
		t.Error("changed must be true on first-load even with empty array")
	}
	if gen != 100 {
		t.Errorf("gen = %d; want 100", gen)
	}
	if len(pairs) != 0 {
		t.Errorf("pairs = %v; want empty", pairs)
	}
}

func TestLoadFromHandle_TransientAttrsError(t *testing.T) {
	t.Parallel()
	transient := errors.New("transient")
	obj := &fakeObjHandle{attrsErr: transient}
	_, gen, changed, err := loadFromHandle(context.Background(), obj, testBucket, testObject, 99)
	if err == nil {
		t.Fatal("expected error")
	}
	if errors.Is(err, ErrAbsent) {
		t.Error("transient error must NOT be reported as ErrAbsent")
	}
	if !errors.Is(err, transient) {
		t.Errorf("error chain must wrap original transient; got %v", err)
	}
	if changed {
		t.Error("changed must be false on error")
	}
	if gen != 99 {
		t.Errorf("gen on error should preserve lastGen, got %d want 99", gen)
	}
}

func TestLoadFromHandle_TransientReaderError(t *testing.T) {
	t.Parallel()
	transient := errors.New("reader-boom")
	obj := &fakeObjHandle{gen: 5, readErr: transient}
	_, gen, changed, err := loadFromHandle(context.Background(), obj, testBucket, testObject, 0)
	if err == nil {
		t.Fatal("expected error")
	}
	if !errors.Is(err, transient) {
		t.Errorf("error chain must wrap original transient; got %v", err)
	}
	if changed {
		t.Error("changed must be false on reader error")
	}
	if gen != 0 {
		t.Errorf("gen on reader error should preserve lastGen, got %d want 0", gen)
	}
}

func TestNewGCSDesiredSetLoader_Constructor(t *testing.T) {
	t.Parallel()
	l := NewGCSDesiredSetLoader(nil, "bucket", "control/kraken_pairs.json")
	if l == nil {
		t.Fatal("constructor returned nil")
	}
	if l.Bucket != "bucket" || l.Object != "control/kraken_pairs.json" {
		t.Errorf("constructor wired wrong fields: %+v", l)
	}
}
