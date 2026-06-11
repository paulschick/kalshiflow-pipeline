package orderbook

import (
	"google.golang.org/protobuf/proto"

	"github.com/paulschick/kalshiflow-pipeline/internal/orderbook/orderbookpb"
)

// EmitReason values for SnapshotPayload.
const (
	EmitReasonChange    = "change"
	EmitReasonHeartbeat = "heartbeat"
)

// SnapshotPayload is the inner payload of a kalshi.snapshot_1s envelope.
// Wraps *orderbookpb.SnapshotPayload. market_ticker and ts no longer live on the
// inner — they ride on the outer envelope. EmitReason is exposed via accessor
// because cmd-side metric labels read it.
type SnapshotPayload struct {
	pb *orderbookpb.SnapshotPayload
}

// NewSnapshotPayload constructs a SnapshotPayload. If hasTop2 is false, top2_price_units
// and top2_size are emitted as proto null (BQ NULL).
func NewSnapshotPayload(side string, top1Price, top1Size, top2Price, top2Size int64, hasTop2 bool, emitReason string) SnapshotPayload {
	p := &orderbookpb.SnapshotPayload{
		Side:           side,
		Top1PriceUnits: top1Price,
		Top1Size:       top1Size,
		EmitReason:     emitReason,
	}
	if hasTop2 {
		p.Top2PriceUnits = proto.Int64(top2Price)
		p.Top2Size = proto.Int64(top2Size)
	}
	return SnapshotPayload{pb: p}
}

// Marshal returns deterministic protobuf-encoded inner payload bytes.
func (p SnapshotPayload) Marshal() ([]byte, error) {
	return proto.MarshalOptions{Deterministic: true}.Marshal(p.pb)
}

// EmitReason returns the emit reason ("change" / "heartbeat") so cmd-side
// metric labels can read it without reaching into the proto.
func (p SnapshotPayload) EmitReason() string {
	if p.pb == nil {
		return ""
	}
	return p.pb.EmitReason
}
