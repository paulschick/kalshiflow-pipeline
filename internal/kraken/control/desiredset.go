// Package control reads the Kraken desired-pair set from GCS.
//
// The control file at gs://<bucket>/control/kraken_pairs.json carries the
// authoritative pair set the kraken-ws-worker should subscribe to. It is
// written by cmd/series-discovery (union of catalog mirror over
// kalshi_raw.v_series_subscribed + kraken_raw.v_kraken_pairs_subscribed) and
// polled by the worker on a 60s ± jitter cadence.
//
// This loader is a pair-typed clone of internal/worker.GCSDesiredSetLoader.
// Kept as a clone rather than refactored to a shared package so the live
// Kalshi prod read path is not perturbed mid-Kraken-rollout.
package control

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"cloud.google.com/go/storage"
)

// DesiredSetLoader returns the current desired Kraken WS pair set and a
// generation token the caller passes back on the next call. When the
// underlying GCS generation has not changed since lastGen, changed=false and
// pairs may be nil (no body read).
//
// Returns ErrAbsent when the source object does not yet exist. The cmd layer
// uses this signal to fall back to KRAKEN_PAIRS env at boot time (DR escape
// hatch). The reconciler loop also tolerates it as "nothing yet; try again".
type DesiredSetLoader interface {
	Load(ctx context.Context, lastGen int64) (pairs []string, generation int64, changed bool, err error)
}

// ErrAbsent is the sentinel for "desired-set source not yet initialized".
var ErrAbsent = errors.New("control: kraken_pairs.json absent")

// objHandle abstracts the GCS handle so loadFromHandle is testable without a
// live storage.Client. Production wraps *storage.ObjectHandle via
// realObjHandle; tests pass an in-memory fake.
type objHandle interface {
	Attrs(ctx context.Context) (*storage.ObjectAttrs, error)
	NewReader(ctx context.Context) (io.ReadCloser, error)
}

// realObjHandle adapts *storage.ObjectHandle to objHandle. Needed because the
// real NewReader returns *storage.Reader (a concrete type), but the test fake
// returns io.ReadCloser.
type realObjHandle struct{ h *storage.ObjectHandle }

func (r realObjHandle) Attrs(ctx context.Context) (*storage.ObjectAttrs, error) {
	return r.h.Attrs(ctx)
}

func (r realObjHandle) NewReader(ctx context.Context) (io.ReadCloser, error) {
	return r.h.NewReader(ctx)
}

// GCSDesiredSetLoader reads kraken_pairs.json from a GCS object.
type GCSDesiredSetLoader struct {
	Client *storage.Client
	Bucket string
	Object string
}

// NewGCSDesiredSetLoader constructs a loader.
func NewGCSDesiredSetLoader(client *storage.Client, bucket, object string) *GCSDesiredSetLoader {
	return &GCSDesiredSetLoader{Client: client, Bucket: bucket, Object: object}
}

// Load implements DesiredSetLoader.
//
// Skip-gate semantics: when the GCS object generation matches lastGen, the
// body is not fetched and changed=false. This makes the steady-state poll
// effectively one HEAD/Attrs request, well under the 50k/mo Class B free tier
// envelope at the 60s ± 10s cadence.
func (l *GCSDesiredSetLoader) Load(ctx context.Context, lastGen int64) ([]string, int64, bool, error) {
	obj := realObjHandle{h: l.Client.Bucket(l.Bucket).Object(l.Object)}
	return loadFromHandle(ctx, obj, l.Bucket, l.Object, lastGen)
}

// loadFromHandle is the testable core of GCSDesiredSetLoader.Load. The bucket
// and object args participate only in error wrapping context.
func loadFromHandle(ctx context.Context, obj objHandle, bucket, object string, lastGen int64) ([]string, int64, bool, error) {
	attrs, err := obj.Attrs(ctx)
	switch {
	case errors.Is(err, storage.ErrObjectNotExist):
		return nil, 0, false, ErrAbsent
	case err != nil:
		return nil, lastGen, false, fmt.Errorf("attrs %s/%s: %w", bucket, object, err)
	}
	if attrs.Generation == lastGen {
		return nil, lastGen, false, nil
	}
	r, err := obj.NewReader(ctx)
	if err != nil {
		return nil, lastGen, false, fmt.Errorf("reader %s/%s: %w", bucket, object, err)
	}
	defer func() { _ = r.Close() }()
	var payload krakenPairsFile
	if err := json.NewDecoder(r).Decode(&payload); err != nil {
		return nil, lastGen, false, fmt.Errorf("decode %s/%s: %w", bucket, object, err)
	}
	return payload.Pairs, attrs.Generation, true, nil
}

// krakenPairsFile mirrors the on-disk JSON shape written by
// internal/seriescat.WriteKrakenPairsDesired.
type krakenPairsFile struct {
	Generation  string   `json:"generation"`
	GeneratedAt string   `json:"generated_at"`
	Pairs       []string `json:"pairs"`
}
