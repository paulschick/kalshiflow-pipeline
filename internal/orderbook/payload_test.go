package orderbook_test

import (
	"testing"

	"google.golang.org/protobuf/proto"

	"github.com/paulschick/kalshiflow-pipeline/internal/orderbook"
	"github.com/paulschick/kalshiflow-pipeline/internal/orderbook/orderbookpb"
)

func TestSnapshotPayload_MarshalRoundTrip(t *testing.T) {
	t.Parallel()
	p := orderbook.NewSnapshotPayload(
		"yes",     // side
		7700,      // top1_price_units
		200,       // top1_size
		7600, 350, // top2_price_units, top2_size
		true,     // hasTop2
		"change", // emit_reason
	)

	body, err := p.Marshal()
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}

	var pb orderbookpb.SnapshotPayload
	if err := proto.Unmarshal(body, &pb); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}

	if pb.Side != "yes" {
		t.Errorf("side = %q", pb.Side)
	}
	if pb.Top1PriceUnits != 7700 || pb.Top1Size != 200 {
		t.Errorf("top1 = (%d, %d)", pb.Top1PriceUnits, pb.Top1Size)
	}
	if pb.Top2PriceUnits == nil || *pb.Top2PriceUnits != 7600 {
		t.Errorf("top2_price_units = %v", pb.Top2PriceUnits)
	}
	if pb.Top2Size == nil || *pb.Top2Size != 350 {
		t.Errorf("top2_size = %v", pb.Top2Size)
	}
	if pb.EmitReason != "change" {
		t.Errorf("emit_reason = %q", pb.EmitReason)
	}
}

func TestSnapshotPayload_NoTop2(t *testing.T) {
	t.Parallel()
	p := orderbook.NewSnapshotPayload("no", 6500, 100, 0, 0, false, "heartbeat")

	body, err := p.Marshal()
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	var pb orderbookpb.SnapshotPayload
	if err := proto.Unmarshal(body, &pb); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}

	if pb.Top2PriceUnits != nil {
		t.Errorf("top2_price_units = %v; want nil", pb.Top2PriceUnits)
	}
	if pb.Top2Size != nil {
		t.Errorf("top2_size = %v; want nil", pb.Top2Size)
	}
	if pb.EmitReason != "heartbeat" {
		t.Errorf("emit_reason = %q", pb.EmitReason)
	}
}
