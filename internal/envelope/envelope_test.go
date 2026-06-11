package envelope_test

import (
	"encoding/json"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/paulschick/kalshiflow-pipeline/internal/envelope"
	"github.com/paulschick/kalshiflow-pipeline/internal/envelope/envelopepb"
	"github.com/paulschick/kalshiflow-pipeline/internal/kalshi"
)

// TestEnvelopeFieldsMatchSchema asserts every proto field name appears in the BQ
// schema (internal/envelope/schema.json) with a compatible type. This is the
// drift detector replacing the JSON-tag-shaped reflection-bind test.
//
// Timestamps are int64 micros on the wire (PS schema registry disallows WKT
// imports), but BQ columns are TIMESTAMP — Storage Write API does the int64-to-
// TIMESTAMP conversion. So the test treats `TIMESTAMP` BQ type as "must be
// proto int64".
func TestEnvelopeFieldsMatchSchema(t *testing.T) {
	t.Parallel()
	desc := (&envelopepb.Envelope{}).ProtoReflect().Descriptor()
	fields := desc.Fields()

	bqByName := map[string]envelope.Field{}
	for _, f := range envelope.Schema {
		bqByName[f.Name] = f
	}

	for i := 0; i < fields.Len(); i++ {
		f := fields.Get(i)
		name := string(f.Name())
		bqf, ok := bqByName[name]
		if !ok {
			t.Errorf("proto field %q has no matching BQ column in schema.json", name)
			continue
		}
		switch bqf.Type {
		case "STRING":
			if f.Kind().String() != "string" {
				t.Errorf("field %q proto kind %q does not match BQ STRING", name, f.Kind())
			}
		case "INT64", "TIMESTAMP":
			if f.Kind().String() != "int64" {
				t.Errorf("field %q proto kind %q does not match BQ %s (expected int64)", name, f.Kind(), bqf.Type)
			}
		case "BYTES":
			if f.Kind().String() != "bytes" {
				t.Errorf("field %q proto kind %q does not match BQ BYTES", name, f.Kind())
			}
		default:
			t.Errorf("BQ type %q for field %q has no proto-kind mapping rule in this test", bqf.Type, name)
		}
	}
}

func TestNew_GeneratesUUIDv7AndDefaultsTimes(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 4, 29, 12, 0, 0, 0, time.UTC)
	e := envelope.New(kalshi.ChannelOrderbookDelta, "KXBTCD", now, json.RawMessage(`{}`))

	if len(e.EnvelopeID()) != 36 {
		t.Errorf("envelope_id = %q; want 36-char UUID", e.EnvelopeID())
	}
	if e.EnvelopeID()[14] != '7' {
		t.Errorf("envelope_id version nibble = %c; want 7", e.EnvelopeID()[14])
	}
	if !e.EventTS().Equal(now) {
		t.Errorf("event_ts = %v; want fallback to ingested_ts %v", e.EventTS(), now)
	}
	if !e.IngestedTS().Equal(now) {
		t.Errorf("ingested_ts = %v", e.IngestedTS())
	}
}

// TestNew_TimestampsRoundTripMicros guards against precision loss on the
// time.Time → int64-micros → time.Time round trip. UnixMicro truncates to
// microseconds; nanoseconds finer than 1µs are lost (acceptable — the
// pre-Plan-2.7 RFC3339 representation also wasn't sub-µs).
func TestNew_TimestampsRoundTripMicros(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 4, 29, 12, 0, 0, 123_456_000, time.UTC) // µs precision
	e := envelope.New("trade", "K", now, json.RawMessage(`{}`))

	if !e.EventTS().Equal(now) {
		t.Errorf("event_ts round-trip = %v; want %v", e.EventTS(), now)
	}
	if !e.IngestedTS().Equal(now) {
		t.Errorf("ingested_ts round-trip = %v; want %v", e.IngestedTS(), now)
	}
}

func TestMarshal_DecodesAsProtobuf(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 4, 29, 12, 0, 0, 123_000_000, time.UTC)
	e := envelope.New(kalshi.ChannelOrderbookDelta, "KXBTCD", now, json.RawMessage(`{"yes":[]}`)).
		WithKalshiTS(now.Add(-time.Millisecond)).
		WithContract("KXBTCD-26APR2917-T67000", "KXBTCD-26APR2917-T67000").
		WithSeq(42)

	body, err := e.Marshal()
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}

	var pb envelopepb.Envelope
	if err := proto.Unmarshal(body, &pb); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}

	if pb.EnvelopeId == "" {
		t.Error("envelope_id missing")
	}
	if pb.Type != string(kalshi.ChannelOrderbookDelta) {
		t.Errorf("type = %q", pb.Type)
	}
	if pb.SeriesId != "KXBTCD" {
		t.Errorf("series_id = %q", pb.SeriesId)
	}
	if pb.Seq == nil || *pb.Seq != 42 {
		t.Errorf("seq = %v; want 42", pb.Seq)
	}
	if pb.KalshiTs == nil {
		t.Error("kalshi_ts missing")
	} else if *pb.KalshiTs != now.Add(-time.Millisecond).UnixMicro() {
		t.Errorf("kalshi_ts = %d; want %d", *pb.KalshiTs, now.Add(-time.Millisecond).UnixMicro())
	}
	if pb.EventTs != now.Add(-time.Millisecond).UnixMicro() {
		t.Errorf("event_ts = %d; want %d (kalshi_ts promoted)", pb.EventTs, now.Add(-time.Millisecond).UnixMicro())
	}
	if string(pb.RawPayload) != `{"yes":[]}` {
		t.Errorf("raw_payload bytes = %q", pb.RawPayload)
	}
}

func TestMarshal_RoundTripsRawPayload(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 4, 29, 12, 0, 0, 0, time.UTC)
	raw := json.RawMessage(`{"market_ticker":"X","seq":1,"ts_ms":1714400000000}`)
	e := envelope.New("trade", "KXBTCD", now, raw)

	body, err := e.Marshal()
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	var pb envelopepb.Envelope
	if err := proto.Unmarshal(body, &pb); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if string(pb.RawPayload) != string(raw) {
		t.Errorf("raw_payload round-trip differs:\n  got:  %s\n  want: %s", pb.RawPayload, raw)
	}
}

func TestMarshal_ByteStableAcrossCalls(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 4, 29, 12, 0, 0, 0, time.UTC)
	e := envelope.New(kalshi.ChannelOrderbookDelta, "KXBTCD", now, json.RawMessage(`{}`))

	b1, err := e.Marshal()
	if err != nil {
		t.Fatalf("Marshal #1: %v", err)
	}
	b2, err := e.Marshal()
	if err != nil {
		t.Fatalf("Marshal #2: %v", err)
	}
	if string(b1) != string(b2) {
		t.Errorf("repeated Marshal of same Envelope produced different bytes:\n  b1=%x\n  b2=%x", b1, b2)
	}
}
