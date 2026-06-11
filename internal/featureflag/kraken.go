// Package featureflag carries kalshiflow's concrete process-scoped feature flags.
// Today the only one is KrakenFlag (HOL-47), which gates the Kraken WS bootstrap
// implemented in HOL-48. The package is intentionally not a generic registry —
// it holds one concrete flag with a hand-built poller mirroring
// internal/worker/desiredset.go.
package featureflag

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"sync/atomic"
	"time"

	"cloud.google.com/go/storage"
)

// ErrAbsent is the sentinel for "GCS object does not yet exist". LoadOnce
// returns it on the absent path so the caller can distinguish "never written"
// from "transient error". The poller treats absent the same as transient
// (retain last-known); LoadOnce surfaces it so a deploy that raced a
// Terraform apply logs INFO instead of WARN.
var ErrAbsent = errors.New("featureflag: kraken_enabled.json absent")

// errParseFail tags decode failures so the poller can distinguish them from
// transient (5xx / network) errors. Parse-fail is already logged ERROR at the
// site of failure and lastGen is advanced past the broken object — so the
// poller resets its consecutive-error counter on parse-fail rather than
// emitting an additional WARN every tick for the same persistent condition.
var errParseFail = errors.New("featureflag: parse fail")

// objHandle is the narrow interface KrakenFlag uses on the GCS side.
// Production wires storage.ObjectHandle through gcsObjAdapter; tests inject
// a fake. Mirrors the internal/worker/desiredset_test.go pattern.
type objHandle interface {
	Attrs(ctx context.Context) (*storage.ObjectAttrs, error)
	NewReader(ctx context.Context) (io.ReadCloser, error)
}

// gcsObjAdapter adapts *storage.ObjectHandle to objHandle. Required because
// storage.ObjectHandle.NewReader returns *storage.Reader, not io.ReadCloser.
type gcsObjAdapter struct{ obj *storage.ObjectHandle }

func (a *gcsObjAdapter) Attrs(ctx context.Context) (*storage.ObjectAttrs, error) {
	return a.obj.Attrs(ctx)
}

func (a *gcsObjAdapter) NewReader(ctx context.Context) (io.ReadCloser, error) {
	return a.obj.NewReader(ctx)
}

// fileBody mirrors the JSON shape written by ops:kraken:on/off and the
// Terraform seed. Only the enabled field is operative; the rest are advisory
// and surface in slog lines on flips.
type fileBody struct {
	Enabled     bool   `json:"enabled"`
	Generation  string `json:"generation"`
	GeneratedAt string `json:"generated_at"`
	Reason      string `json:"reason"`
	Actor       string `json:"actor"`
}

// KrakenFlag is the live in-process projection of control/kraken_enabled.json.
// Concurrent-safe: Enabled() is backed by atomic.Bool; LoadOnce and Start are
// expected to be single-caller. Construct with NewGCSKrakenFlag.
type KrakenFlag struct {
	obj objHandle

	enabled atomic.Bool  // default-on iff never-loaded
	lastGen atomic.Int64 // GCS int64 generation of the last body successfully read
	loaded  atomic.Bool  // has any successful body-read completed?

	pollInterval time.Duration
	pollJitter   time.Duration
}

// NewGCSKrakenFlag binds a KrakenFlag to a GCS object. The flag is default-on
// iff LoadOnce has never returned successfully.
func NewGCSKrakenFlag(client *storage.Client, bucket, object string) *KrakenFlag {
	return newKrakenFlag(&gcsObjAdapter{obj: client.Bucket(bucket).Object(object)})
}

// newKrakenFlag is the testable constructor that takes any objHandle.
func newKrakenFlag(obj objHandle) *KrakenFlag {
	f := &KrakenFlag{
		obj:          obj,
		pollInterval: 60 * time.Second,
		pollJitter:   10 * time.Second,
	}
	f.enabled.Store(true) // default-on iff never-loaded
	return f
}

// Enabled returns the current in-memory snapshot. Returns true if the flag
// has never been loaded.
func (f *KrakenFlag) Enabled() bool { return f.enabled.Load() }

// LoadOnce performs a single synchronous read. Call at boot before Start.
// Returns ErrAbsent when the object does not exist — the caller treats this
// as default-on (Enabled() is already true on construction). Any other error
// is returned wrapped; the snapshot is unchanged.
func (f *KrakenFlag) LoadOnce(ctx context.Context) error {
	return f.loadOnce(ctx)
}

// Start runs the background poll loop with 60s ± 10s jitter. Blocks until
// ctx cancellation. Call at most once per *KrakenFlag.
func (f *KrakenFlag) Start(ctx context.Context) {
	consecutiveErrs := 0
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(f.nextDelay()):
		}
		f.pollOnce(ctx, &consecutiveErrs)
	}
}

// loadOnce is the shared core of LoadOnce + pollOnce. Returns ErrAbsent on
// the absent path; logs on parse fail / WARN-after-prior-load and retains
// last-known on any failure.
func (f *KrakenFlag) loadOnce(ctx context.Context) error {
	attrs, err := f.obj.Attrs(ctx)
	switch {
	case errors.Is(err, storage.ErrObjectNotExist):
		if f.loaded.Load() {
			slog.WarnContext(ctx, "kraken_flag object absent after prior load; retaining last-known",
				"enabled", f.enabled.Load())
		} else {
			slog.InfoContext(ctx, "kraken_flag object absent at boot; default-on",
				"enabled", true)
		}
		return ErrAbsent
	case err != nil:
		return fmt.Errorf("attrs: %w", err)
	}

	if attrs.Generation == f.lastGen.Load() {
		return nil // cheap-unchanged; no body read
	}

	r, err := f.obj.NewReader(ctx)
	if err != nil {
		return fmt.Errorf("reader: %w", err)
	}
	defer func() { _ = r.Close() }()

	var body fileBody
	if err := json.NewDecoder(r).Decode(&body); err != nil {
		// Advance lastGen past the broken object so we don't re-download and
		// re-parse the same persistent-bad body every poll tick. Operator
		// republish (new generation) re-triggers a body read.
		f.lastGen.Store(attrs.Generation)
		slog.ErrorContext(ctx, "kraken_flag parse fail; retaining last-known",
			"err", err, "enabled", f.enabled.Load())
		return fmt.Errorf("%w: %w", errParseFail, err)
	}

	prev := f.enabled.Load()
	f.enabled.Store(body.Enabled)
	f.lastGen.Store(attrs.Generation)
	f.loaded.Store(true)

	// Transition-only: emit `kraken_flag_changed` strictly when the boolean
	// flips. First successful load with enabled=true (the construction
	// default) is a no-op; cmd-layer boot logging is the operator-visible
	// "current value" signal at startup.
	if prev != body.Enabled {
		slog.InfoContext(ctx, "kraken_flag_changed",
			"enabled", body.Enabled,
			"generation", body.Generation,
			"reason", body.Reason,
			"actor", body.Actor,
			"generated_at", body.GeneratedAt)
	}
	return nil
}

// pollOnce wraps loadOnce with consecutive-error tracking for the poller.
// ErrAbsent does not count toward consecutive errors (it is normal pre-write
// state); errParseFail also does not — it is already logged ERROR at the site
// of failure and lastGen is advanced past the broken object, so escalating to
// a per-tick WARN here would just double-log a stuck operator-republish
// condition. Only true transient errors (5xx, network, IAM) drive the
// counter.
func (f *KrakenFlag) pollOnce(ctx context.Context, consecutiveErrs *int) {
	err := f.loadOnce(ctx)
	if err == nil || errors.Is(err, ErrAbsent) || errors.Is(err, errParseFail) {
		*consecutiveErrs = 0
		return
	}
	*consecutiveErrs++
	if *consecutiveErrs >= 2 {
		slog.WarnContext(ctx, "kraken_flag poll repeatedly failing; retaining last-known",
			"err", err, "consecutive_errs", *consecutiveErrs, "enabled", f.enabled.Load())
	}
}

// NewTestFlag returns a KrakenFlag with the given initial value. Test-only.
// The flag has no objHandle; LoadOnce / Start must not be called.
func NewTestFlag(initial bool) *KrakenFlag {
	f := &KrakenFlag{pollInterval: 60 * time.Second, pollJitter: 10 * time.Second}
	f.enabled.Store(initial)
	return f
}

// SetEnabled is test-only; in production the flag's value comes from GCS.
func (f *KrakenFlag) SetEnabled(v bool) { f.enabled.Store(v) }

// nextDelay returns the next jittered poll interval.
func (f *KrakenFlag) nextDelay() time.Duration {
	if f.pollJitter <= 0 {
		return f.pollInterval
	}
	jitter := time.Duration(rand.Int64N(int64(2*f.pollJitter))) - f.pollJitter
	d := f.pollInterval + jitter
	if d <= 0 {
		d = f.pollInterval
	}
	return d
}
