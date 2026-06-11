package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"sort"
	"time"

	"cloud.google.com/go/storage"
)

// DesiredSetLoader returns the current desired ticker set and a generation token
// the caller passes back next time. When the underlying generation has not
// changed since lastGen, changed=false and tickers may be nil (no body read).
//
// Returns ErrAbsent when the source object does not yet exist. The cmd layer
// uses this signal to fall back to KALSHI_SERIES env at boot time (DR escape
// hatch). The reconciler loop also tolerates it as "nothing yet; try again".
type DesiredSetLoader interface {
	Load(ctx context.Context, lastGen int64) (tickers []string, generation int64, changed bool, err error)
}

// ErrAbsent is the sentinel for "desired-set source not yet initialized".
var ErrAbsent = errors.New("worker: desired set source absent")

// GCSDesiredSetLoader reads series_desired.json from a GCS object.
type GCSDesiredSetLoader struct {
	Client *storage.Client
	Bucket string
	Object string
}

// NewGCSDesiredSetLoader constructs a loader. Provided so cmd layers don't need to know field names.
func NewGCSDesiredSetLoader(client *storage.Client, bucket, object string) *GCSDesiredSetLoader {
	return &GCSDesiredSetLoader{Client: client, Bucket: bucket, Object: object}
}

// Load implements DesiredSetLoader. See interface doc for semantics.
func (l *GCSDesiredSetLoader) Load(ctx context.Context, lastGen int64) ([]string, int64, bool, error) {
	obj := l.Client.Bucket(l.Bucket).Object(l.Object)
	attrs, err := obj.Attrs(ctx)
	switch {
	case errors.Is(err, storage.ErrObjectNotExist):
		return nil, 0, false, ErrAbsent
	case err != nil:
		return nil, lastGen, false, fmt.Errorf("attrs %s/%s: %w", l.Bucket, l.Object, err)
	}
	// We key poll-skip on the GCS int64 generation, not the ULID in the JSON body,
	// because the int64 is authoritative (set by GCS on write) and requires no body read.
	if attrs.Generation == lastGen {
		return nil, lastGen, false, nil
	}
	r, err := obj.NewReader(ctx)
	if err != nil {
		return nil, lastGen, false, fmt.Errorf("reader %s/%s: %w", l.Bucket, l.Object, err)
	}
	defer func() { _ = r.Close() }()
	var payload desiredSetFile
	if err := json.NewDecoder(r).Decode(&payload); err != nil {
		return nil, lastGen, false, fmt.Errorf("decode %s/%s: %w", l.Bucket, l.Object, err)
	}
	return payload.Tickers, attrs.Generation, true, nil
}

// desiredSetFile mirrors the on-disk JSON shape written by
// internal/seriescat::WriteDesiredSet. Kept unexported because the worker
// package is the only consumer; the writer side has its own type.
type desiredSetFile struct {
	Generation  string   `json:"generation"`
	GeneratedAt string   `json:"generated_at"`
	Tickers     []string `json:"tickers"`
}

// runDesiredSetReconciler polls the DesiredSetLoader on a jittered interval and
// applies AddSeries / RemoveSeries for the delta between the manifest and the
// current in-process set.
//
// Bound to the root ctx (not a per-session ctx) so it survives WS reconnects.
func (w *Worker) runDesiredSetReconciler(ctx context.Context) {
	if w.d.DesiredSetLoader == nil {
		return
	}
	loader := w.d.DesiredSetLoader

	sleep := func() bool {
		d := w.nextDesiredSetDelay()
		select {
		case <-ctx.Done():
			return false
		case <-time.After(d):
			return true
		}
	}

	// Initial jittered sleep before first poll — cmd layer already did a
	// synchronous boot load; no reason to double-poll at t=0.
	if !sleep() {
		return
	}

	consecutiveErrs := 0
	for {
		w.reconcileDesiredSetOnce(ctx, loader, &consecutiveErrs)

		if !sleep() {
			return
		}
	}
}

func (w *Worker) reconcileDesiredSetOnce(ctx context.Context, loader DesiredSetLoader, consecutiveErrs *int) {
	tickers, gen, changed, err := loader.Load(ctx, w.lastDesiredGen.Load())

	// ErrAbsent is pre-first-write normal state, not a failure — never count
	// toward the stale gauge.
	if errors.Is(err, ErrAbsent) {
		slog.WarnContext(ctx, "desired-set absent; reconciler waiting for first write")
		return
	}

	if err != nil {
		*consecutiveErrs++
		slog.WarnContext(ctx, "desired-set load failed", "err", err, "consecutive_errs", *consecutiveErrs)
		if *consecutiveErrs >= 2 && w.d.Metrics != nil {
			w.d.Metrics.WorkerDesiredSetStale.Record(ctx, 1)
		}
		return
	}

	// Successful load: reset error state.
	*consecutiveErrs = 0
	if w.d.Metrics != nil {
		w.d.Metrics.WorkerDesiredSetStale.Record(ctx, 0)
	}

	if !changed {
		return
	}

	if len(tickers) == 0 {
		if w.d.Metrics != nil {
			w.d.Metrics.WorkerDesiredSetEmpty.Record(ctx, 1)
		}
		slog.WarnContext(ctx, "empty desired set; retaining prior in-memory set")
		// Do NOT update lastDesiredGen — so the next generation change
		// will re-fire and the operator can fix by republishing.
		return
	}
	if w.d.Metrics != nil {
		w.d.Metrics.WorkerDesiredSetEmpty.Record(ctx, 0)
	}

	// Compute delta.
	wantSet := make(map[string]struct{}, len(tickers))
	for _, t := range tickers {
		wantSet[t] = struct{}{}
	}
	haveSet := make(map[string]struct{})
	for _, t := range w.Series() {
		haveSet[t] = struct{}{}
	}

	var toAdd, toRemove []string
	for t := range wantSet {
		if _, ok := haveSet[t]; !ok {
			toAdd = append(toAdd, t)
		}
	}
	for t := range haveSet {
		if _, ok := wantSet[t]; !ok {
			toRemove = append(toRemove, t)
		}
	}
	sort.Strings(toAdd)
	sort.Strings(toRemove)

	for _, t := range toAdd {
		if err := w.AddSeries(ctx, t); err != nil {
			slog.WarnContext(ctx, "desired-set reconciler: AddSeries failed", "ticker", t, "err", err)
		}
	}
	for _, t := range toRemove {
		if err := w.RemoveSeries(ctx, t); err != nil {
			slog.WarnContext(ctx, "desired-set reconciler: RemoveSeries failed", "ticker", t, "err", err)
		}
	}

	// Advance generation after attempted apply regardless of per-ticker errors.
	// A transient REST blip should not pin lastGen at the prior value forever;
	// the next Load with the SAME gen will be a no-op via changed=false, and
	// the operator can republish a new manifest to force a retry.
	w.lastDesiredGen.Store(gen)

	if w.d.Metrics != nil {
		w.d.Metrics.WorkerSubscribedSeriesCount.Record(ctx, int64(len(*w.series.Load())))
	}
}

// nextDesiredSetDelay returns a jittered poll interval for the desired-set reconciler.
func (w *Worker) nextDesiredSetDelay() time.Duration {
	if w.desiredSetPollJitter <= 0 {
		return w.desiredSetPollInterval
	}
	jitter := time.Duration(rand.Int64N(int64(2*w.desiredSetPollJitter))) - w.desiredSetPollJitter
	d := w.desiredSetPollInterval + jitter
	if d <= 0 {
		d = w.desiredSetPollInterval
	}
	return d
}
