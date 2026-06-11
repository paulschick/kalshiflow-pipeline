// Package envelope defines the canonical wire format the WS worker publishes to Pub/Sub.
// The wire format is protobuf (cut over from JSON 2026-05-04). Proto field numbers +
// names are pinned; the proto-vs-BQ-schema drift detector test in envelope_test.go
// fails CI if internal/envelope/schema.json drifts from the .proto.
//
// Timestamps are int64 microseconds since Unix epoch. PS schema registry rejects
// external imports, so google.protobuf.Timestamp is unusable. BQ Storage Write API
// converts proto int64 to TIMESTAMP columns as micros-since-epoch.
//
// All BQ columns are NULLABLE (Task 1 spike finding: PS topic schema treats every
// proto3 non-`optional` field as nullable; BQ-direct sub create rejects REQUIRED
// columns). Non-null is enforced at publish-time in this package's New() — it
// always populates envelope_id, event_ts, ingested_ts, type, series_id. Empty
// values only ship if a caller passes empty strings, which is a code bug.
//
// envelope_id is the dedup key. Generated as UUIDv7 in New(); stored on the proto
// Envelope. proto.MarshalOptions{Deterministic: true} keeps Marshal byte-stable so a
// redelivered Pub/Sub message carries the same envelope_id; downstream BQ dedup uses:
//
//	QUALIFY ROW_NUMBER() OVER (PARTITION BY envelope_id ORDER BY ingested_ts) = 1
package envelope

import (
	"encoding/json"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"

	"github.com/paulschick/kalshiflow-pipeline/internal/envelope/envelopepb"
)

// Envelope wraps a *envelopepb.Envelope. The wrapper exists so the public API is the
// same shape callers had pre-Plan-2.7 (constructors + accessors); the underlying
// proto type is private to this package and the BQ-schema drift test.
type Envelope struct {
	pb *envelopepb.Envelope
}

// New constructs an envelope. event_ts defaults to ingested_ts; call WithKalshiTS afterwards
// if Kalshi provided a wire timestamp.
//
// raw is the inner Kalshi message body. For snapshot_1s envelopes the caller passes
// protobuf-marshalled SnapshotPayload bytes. For other streams the caller passes UTF-8
// JSON bytes of the raw Kalshi WS frame body.
func New(typ, seriesID string, ingestedTS time.Time, raw json.RawMessage) Envelope {
	micros := ingestedTS.UnixMicro()
	return Envelope{
		pb: &envelopepb.Envelope{
			EnvelopeId: newUUIDv7(),
			EventTs:    micros,
			IngestedTs: micros,
			Type:       typ,
			SeriesId:   seriesID,
			RawPayload: []byte(raw),
		},
	}
}

// WithKalshiTS sets KalshiTS and promotes EventTS to that value.
func (e Envelope) WithKalshiTS(t time.Time) Envelope {
	micros := t.UnixMicro()
	e.pb.KalshiTs = proto.Int64(micros)
	e.pb.EventTs = micros
	return e
}

// WithContract attaches the contract identifiers.
func (e Envelope) WithContract(contractID, marketTicker string) Envelope {
	e.pb.ContractId = proto.String(contractID)
	e.pb.MarketTicker = proto.String(marketTicker)
	return e
}

// WithSeq attaches Kalshi's monotonic per-channel sequence number.
func (e Envelope) WithSeq(seq int64) Envelope {
	e.pb.Seq = proto.Int64(seq)
	return e
}

// WithSnapshotSeq is set only on orderbook_snapshot envelopes (carried over from
// Plan 2.4's REST/WS classifier story).
func (e Envelope) WithSnapshotSeq(seq int64) Envelope {
	e.pb.SnapshotSeq = proto.Int64(seq)
	return e
}

// Marshal returns deterministic protobuf-encoded bytes ready for Pub/Sub publish.
func (e Envelope) Marshal() ([]byte, error) {
	return proto.MarshalOptions{Deterministic: true}.Marshal(e.pb)
}

// EnvelopeID returns the dedup key.
func (e Envelope) EnvelopeID() string { return e.pb.EnvelopeId }

// EventTS returns the event timestamp (UTC).
func (e Envelope) EventTS() time.Time {
	return time.UnixMicro(e.pb.EventTs).UTC()
}

// IngestedTS returns the ingestion timestamp (UTC).
func (e Envelope) IngestedTS() time.Time {
	return time.UnixMicro(e.pb.IngestedTs).UTC()
}

func newUUIDv7() string {
	id, err := uuid.NewV7()
	if err != nil {
		slog.Warn("uuid.NewV7 failed; falling back to v4 (envelope_id loses time-ordering)", "err", err)
		return uuid.NewString()
	}
	return id.String()
}
