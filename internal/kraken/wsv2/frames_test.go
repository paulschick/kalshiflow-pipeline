package wsv2

import (
	"encoding/json"
	"testing"
	"time"
)

func TestParse_Status(t *testing.T) {
	raw := []byte(`{"channel":"status","type":"update","data":[{"version":"2.0.10","system":"online","api_version":"v2","connection_id":16389994220698793191}]}`)
	f, err := Parse(raw)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if f.Kind != FrameStatus {
		t.Fatalf("Kind = %v; want FrameStatus", f.Kind)
	}
	if f.Status.System != "online" || f.Status.APIVersion != "v2" || f.Status.Version != "2.0.10" {
		t.Errorf("Status = %+v", f.Status)
	}
}

func TestParse_Heartbeat(t *testing.T) {
	raw := []byte(`{"channel":"heartbeat"}`)
	f, err := Parse(raw)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if f.Kind != FrameHeartbeat {
		t.Fatalf("Kind = %v", f.Kind)
	}
}

func TestParse_SubscribeAck_Success(t *testing.T) {
	raw := []byte(`{"method":"subscribe","req_id":1,"result":{"channel":"book","depth":10,"snapshot":true,"symbol":"BTC/USD"},"success":true,"time_in":"2026-05-14T15:06:07.846137Z","time_out":"2026-05-14T15:06:07.846205Z"}`)
	f, err := Parse(raw)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if f.Kind != FrameSubscribeAck {
		t.Fatalf("Kind = %v", f.Kind)
	}
	ack := f.SubscribeAck
	if !ack.Success || ack.ReqID != 1 || ack.Channel != "book" || ack.Symbol != "BTC/USD" {
		t.Errorf("ack = %+v", ack)
	}
}

func TestParse_SubscribeAck_Error_TopLevelSymbol(t *testing.T) {
	raw := []byte(`{"error":"Currency pair not supported XBT/USD","method":"subscribe","req_id":999,"success":false,"symbol":"XBT/USD","time_in":"...","time_out":"..."}`)
	f, err := Parse(raw)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	ack := f.SubscribeAck
	if ack.Success {
		t.Fatal("expected success=false")
	}
	if ack.Symbol != "XBT/USD" || ack.Error != "Currency pair not supported XBT/USD" {
		t.Errorf("ack = %+v", ack)
	}
}

func TestParse_BookSnapshot_PriceQtyAsJSONNumber(t *testing.T) {
	raw := []byte(`{"channel":"book","type":"snapshot","data":[{"symbol":"BTC/USD","bids":[{"price":81011.5,"qty":0.00123440}],"asks":[{"price":81015.4,"qty":0.5}],"checksum":2348297322,"timestamp":"2026-05-14T15:06:08.234374Z"}]}`)
	f, err := Parse(raw)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if f.Kind != FrameBookSnapshot {
		t.Fatalf("Kind = %v", f.Kind)
	}
	bf := f.Book
	if bf.Symbol != "BTC/USD" || bf.Checksum != 2348297322 {
		t.Errorf("book = %+v", bf)
	}
	if got, want := bf.Bids[0].Price, json.Number("81011.5"); got != want {
		t.Errorf("bid price = %q; want %q (wire-byte preservation)", got, want)
	}
	if got, want := bf.Bids[0].Qty, json.Number("0.00123440"); got != want {
		t.Errorf("bid qty = %q; want %q (trailing zeros preserved)", got, want)
	}
	if bf.Timestamp.IsZero() {
		t.Error("timestamp not parsed")
	}
}

func TestParse_BookUpdate_EmptyBidsMeansNoChangeNotWipe(t *testing.T) {
	raw := []byte(`{"channel":"book","type":"update","data":[{"symbol":"DOGE/USD","bids":[],"asks":[{"price":0.1149362,"qty":1603.12500000}],"checksum":1234,"timestamp":"2026-05-14T15:06:08.234374Z"}]}`)
	f, err := Parse(raw)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if f.Kind != FrameBookUpdate {
		t.Fatalf("Kind = %v", f.Kind)
	}
	if len(f.Book.Bids) != 0 {
		t.Errorf("bids should be empty slice (no changes this side), not nil; got %#v", f.Book.Bids)
	}
	if len(f.Book.Asks) != 1 {
		t.Fatalf("asks len = %d; want 1", len(f.Book.Asks))
	}
}

func TestParse_TradeSnapshot_OneTradePerRecord(t *testing.T) {
	raw := []byte(`{"channel":"trade","type":"snapshot","data":[{"symbol":"ETH/USD","side":"sell","price":2284.15,"qty":0.001,"ord_type":"market","trade_id":62886870,"timestamp":"2026-05-14T15:03:41.748964Z"},{"symbol":"BTC/USD","side":"buy","price":81011.5,"qty":0.00123440,"ord_type":"limit","trade_id":100320028,"timestamp":"2026-05-14T15:06:09.020392Z"}]}`)
	f, err := Parse(raw)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if f.Kind != FrameTradeSnapshot {
		t.Fatalf("Kind = %v", f.Kind)
	}
	if len(f.Trades) != 2 {
		t.Fatalf("trades = %d; want 2", len(f.Trades))
	}
	if got, want := f.Trades[1].Price, json.Number("81011.5"); got != want {
		t.Errorf("trade[1] price = %q; want %q", got, want)
	}
	if f.Trades[0].Timestamp.IsZero() {
		t.Error("trade timestamp not parsed")
	}
	if len(f.Trades[1].RawJSON) == 0 {
		t.Error("RawJSON must carry the exact wire bytes of the record")
	}
}

func TestParse_UnknownChannel_ReturnsErr(t *testing.T) {
	raw := []byte(`{"channel":"executions","type":"update","data":[]}`)
	_, err := Parse(raw)
	if err == nil {
		t.Fatal("expected error for unknown channel")
	}
}

func TestParse_TimestampPrecisionRoundsToMicro(t *testing.T) {
	raw := []byte(`{"channel":"book","type":"update","data":[{"symbol":"BTC/USD","bids":[],"asks":[],"checksum":0,"timestamp":"2026-05-14T15:06:08.234374Z"}]}`)
	f, err := Parse(raw)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	want := time.Date(2026, 5, 14, 15, 6, 8, 234374000, time.UTC)
	if !f.Book.Timestamp.Equal(want) {
		t.Errorf("timestamp = %v; want %v", f.Book.Timestamp, want)
	}
}
