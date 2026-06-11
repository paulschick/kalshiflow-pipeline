package seriescat

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"time"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/storage"
	"github.com/oklog/ulid/v2"
	"google.golang.org/api/iterator"
)

// DesiredSetConfig parameterizes WriteDesiredSet.
type DesiredSetConfig struct {
	ProjectID string // BQ project
	DatasetID string // BQ dataset (e.g. "kalshi_raw")
	ViewName  string // BQ view (e.g. "v_series_subscribed")
	Bucket    string // GCS bucket (e.g. "<project>-kalshi-archive")
	ObjectKey string // GCS object key (e.g. "control/series_desired.json")
}

// ErrEmptyDesiredSet is the sentinel for "the view returned zero tickers".
// The caller treats this as exit-code 2 (operator error / guarded state) and
// does NOT write GCS.
var ErrEmptyDesiredSet = errors.New("seriescat: desired set is empty; refusing to write")

// ViewReader abstracts the BQ side so tests don't need a real bigquery.Client.
// The concrete production wrapper is BQViewReader.
type ViewReader interface {
	ReadTickers(ctx context.Context, project, dataset, view string) ([]string, error)
}

// BQViewReader is the production ViewReader over *bigquery.Client.
type BQViewReader struct{ Client *bigquery.Client }

// ReadTickers queries the view and returns the ticker column values.
func (r *BQViewReader) ReadTickers(ctx context.Context, project, dataset, view string) ([]string, error) {
	q := r.Client.Query(fmt.Sprintf("SELECT ticker FROM `%s.%s.%s`", project, dataset, view))
	it, err := q.Read(ctx)
	if err != nil {
		return nil, fmt.Errorf("query view: %w", err)
	}
	var out []string
	for {
		var row struct {
			Ticker string `bigquery:"ticker"`
		}
		err := it.Next(&row)
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("iter view: %w", err)
		}
		out = append(out, row.Ticker)
	}
	return out, nil
}

// GCSWriter abstracts the GCS side. The production impl is GCSStorageWriter.
type GCSWriter interface {
	// ReadAttrsGeneration returns the current GCS object generation,
	// 0 + ErrObjectAbsent if the object does not exist, or an unwrapped error otherwise.
	ReadAttrsGeneration(ctx context.Context, bucket, object string) (int64, error)
	// Write the body under either DoesNotExist (when expectGeneration == 0) or
	// GenerationMatch == expectGeneration. A 412 PreconditionFailed surfaces as a wrapped error.
	Write(ctx context.Context, bucket, object string, body []byte, contentType string, expectGeneration int64) error
}

// ErrObjectAbsent is the GCSWriter-side sentinel for "object does not yet exist".
// (Distinct from worker.ErrAbsent, which is the loader-side sentinel.)
var ErrObjectAbsent = errors.New("seriescat: gcs object absent")

// GCSStorageWriter wraps *storage.Client.
type GCSStorageWriter struct{ Client *storage.Client }

// ReadAttrsGeneration returns the GCS object's current generation, or 0 + ErrObjectAbsent when absent.
func (w *GCSStorageWriter) ReadAttrsGeneration(ctx context.Context, bucket, object string) (int64, error) {
	obj := w.Client.Bucket(bucket).Object(object)
	attrs, err := obj.Attrs(ctx)
	switch {
	case errors.Is(err, storage.ErrObjectNotExist):
		return 0, ErrObjectAbsent
	case err != nil:
		return 0, fmt.Errorf("attrs: %w", err)
	}
	return attrs.Generation, nil
}

func (w *GCSStorageWriter) Write(ctx context.Context, bucket, object string, body []byte, contentType string, expectGeneration int64) error {
	obj := w.Client.Bucket(bucket).Object(object)
	var conds storage.Conditions
	if expectGeneration == 0 {
		conds = storage.Conditions{DoesNotExist: true}
	} else {
		conds = storage.Conditions{GenerationMatch: expectGeneration}
	}
	writer := obj.If(conds).NewWriter(ctx)
	writer.ContentType = contentType
	if _, err := writer.Write(body); err != nil {
		_ = writer.Close()
		return fmt.Errorf("write: %w", err)
	}
	return writer.Close() // 412 surfaces as *googleapi.Error{Code: 412}
}

// DesiredSetFile is the JSON shape written to GCS. Exported so that downstream
// consumers (smoke tests, ops tooling) can decode it without redefining.
type DesiredSetFile struct {
	Generation  string   `json:"generation"`
	GeneratedAt string   `json:"generated_at"`
	Tickers     []string `json:"tickers"`
}

// WriteDesiredSet reads tickers from v_series_subscribed, sorts them, and writes
// the manifest to gs://<Bucket>/<ObjectKey> under an If-Generation-Match
// precondition (first write uses DoesNotExist).
//
// Returns ErrEmptyDesiredSet when the view returned zero tickers (caller exits
// with code 2). Returns the wrapped underlying error otherwise. On success,
// returns (tickersWritten, ulidGeneration, nil).
//
// The body is byte-for-byte deterministic for the same input ticker set:
// tickers are sorted lex-asc and the encoder writes a canonical JSON layout.
func WriteDesiredSet(ctx context.Context, vr ViewReader, gw GCSWriter, cfg DesiredSetConfig) (count int, generationULID string, err error) {
	tickers, err := vr.ReadTickers(ctx, cfg.ProjectID, cfg.DatasetID, cfg.ViewName)
	if err != nil {
		return 0, "", fmt.Errorf("read view %s.%s.%s: %w", cfg.ProjectID, cfg.DatasetID, cfg.ViewName, err)
	}
	if len(tickers) == 0 {
		return 0, "", ErrEmptyDesiredSet
	}
	sort.Strings(tickers)

	expectGen, err := gw.ReadAttrsGeneration(ctx, cfg.Bucket, cfg.ObjectKey)
	switch {
	case errors.Is(err, ErrObjectAbsent):
		expectGen = 0
	case err != nil:
		return 0, "", fmt.Errorf("attrs %s/%s: %w", cfg.Bucket, cfg.ObjectKey, err)
	}

	// ULID provides human-traceable provenance per write, independent of the
	// GCS int64 generation (which resets if the object is deleted and recreated).
	u := ulid.Make().String()
	payload := DesiredSetFile{
		Generation:  u,
		GeneratedAt: time.Now().UTC().Format("2006-01-02T15:04:05.000Z"),
		Tickers:     tickers,
	}
	body, err := json.MarshalIndent(payload, "", "  ")
	if err != nil {
		return 0, "", fmt.Errorf("marshal: %w", err)
	}
	if err := gw.Write(ctx, cfg.Bucket, cfg.ObjectKey, body, "application/json", expectGen); err != nil {
		return 0, "", fmt.Errorf("write %s/%s: %w", cfg.Bucket, cfg.ObjectKey, err)
	}
	return len(tickers), u, nil
}

// KrakenPairsConfig parameterizes WriteKrakenPairsDesired.
type KrakenPairsConfig struct {
	Bucket    string // GCS bucket (e.g. "<project>-kalshi-archive")
	ObjectKey string // GCS object key (e.g. "control/kraken_pairs.json")
}

// ErrEmptyKrakenPairs is the sentinel for "the union pair set is empty".
// Caller treats this as exit-code 2 (same exit semantics as
// ErrEmptyDesiredSet) and does NOT write GCS.
var ErrEmptyKrakenPairs = errors.New("seriescat: kraken pair set is empty; refusing to write")

// KrakenPairsFile is the JSON shape written to GCS for kraken_pairs.json.
type KrakenPairsFile struct {
	Generation  string   `json:"generation"`
	GeneratedAt string   `json:"generated_at"`
	Pairs       []string `json:"pairs"`
}

// WriteKrakenPairsDesired writes the (pre-computed) Kraken WS pair set to
// gs://<Bucket>/<ObjectKey> under an If-Generation-Match precondition
// (first write uses DoesNotExist). The caller computes the union of the
// catalog-mirror set (PairsFromTickers over kalshi_raw.v_series_subscribed)
// and the operator-controlled set (kraken_raw.v_kraken_pairs_subscribed) via
// UnionPairs and passes the result here.
//
// Returns ErrEmptyKrakenPairs when pairs is empty (caller exits with code 2).
// Returns the wrapped underlying error otherwise. On success, returns
// (pairCount, ulidGeneration, nil).
//
// The body is byte-for-byte deterministic for the same input pair set:
// pairs are sorted lex-asc and the encoder writes canonical JSON.
func WriteKrakenPairsDesired(ctx context.Context, gw GCSWriter, cfg KrakenPairsConfig, pairs []string) (count int, generationULID string, err error) {
	if len(pairs) == 0 {
		return 0, "", ErrEmptyKrakenPairs
	}
	sorted := append([]string(nil), pairs...)
	sort.Strings(sorted)

	expectGen, err := gw.ReadAttrsGeneration(ctx, cfg.Bucket, cfg.ObjectKey)
	switch {
	case errors.Is(err, ErrObjectAbsent):
		expectGen = 0
	case err != nil:
		return 0, "", fmt.Errorf("attrs %s/%s: %w", cfg.Bucket, cfg.ObjectKey, err)
	}

	u := ulid.Make().String()
	payload := KrakenPairsFile{
		Generation:  u,
		GeneratedAt: time.Now().UTC().Format("2006-01-02T15:04:05.000Z"),
		Pairs:       sorted,
	}
	body, err := json.MarshalIndent(payload, "", "  ")
	if err != nil {
		return 0, "", fmt.Errorf("marshal: %w", err)
	}
	if err := gw.Write(ctx, cfg.Bucket, cfg.ObjectKey, body, "application/json", expectGen); err != nil {
		return 0, "", fmt.Errorf("write %s/%s: %w", cfg.Bucket, cfg.ObjectKey, err)
	}
	return len(sorted), u, nil
}
