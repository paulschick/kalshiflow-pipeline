// Package wsv2 parses Kraken WebSocket v2 frames into typed structs and
// provides a minimal Conn surface for connect / subscribe / read / close.
//
// All price and qty fields stay as json.Number — wire bytes are preserved so
// the bookkeeper's CRC32 validator can re-derive the checksum from the same
// byte sequence the Kraken server produced. float64 is never on the path.
package wsv2

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// FrameKind discriminates parsed Kraken WS v2 frames by structural shape.
type FrameKind int

// FrameKind enum.
const (
	FrameUnknown FrameKind = iota
	FrameStatus
	FrameHeartbeat
	FrameSubscribeAck
	FrameBookSnapshot
	FrameBookUpdate
	FrameTradeSnapshot
	FrameTradeUpdate
)

// Frame is the typed result of Parse. Exactly one of Status / SubscribeAck /
// Book / Trades is non-nil depending on Kind.
type Frame struct {
	Kind FrameKind
	Raw  []byte

	Status       *StatusFrame
	SubscribeAck *SubscribeAck
	Book         *BookFrame
	Trades       []TradeFrame
}

// StatusFrame mirrors the Kraken WS v2 `status` channel record.
type StatusFrame struct {
	System       string      `json:"system"`
	APIVersion   string      `json:"api_version"`
	Version      string      `json:"version"`
	ConnectionID json.Number `json:"connection_id"`
}

// SubscribeAck is the parsed ack body for a subscribe / unsubscribe request.
// On success the Channel + Symbol come from the nested `result` block; on
// error the Symbol + Error come from top-level fields.
type SubscribeAck struct {
	ReqID   uint64
	Success bool
	Channel string
	Symbol  string
	Error   string
}

// BookFrame is one element of `data[]` for a `book` channel snapshot or update.
type BookFrame struct {
	Symbol    string
	Bids      []BookLevel
	Asks      []BookLevel
	Checksum  uint32
	Timestamp time.Time
}

// BookLevel is one (price, qty) entry on the wire. Both values are preserved
// as json.Number so the byte sequence is available to the CRC32 validator.
type BookLevel struct {
	Price json.Number `json:"price"`
	Qty   json.Number `json:"qty"`
}

// TradeFrame is one element of `data[]` for a `trade` channel snapshot or update.
// RawJSON holds the exact wire bytes of this record for raw_payload passthrough.
type TradeFrame struct {
	Symbol    string
	Side      string
	Price     json.Number
	Qty       json.Number
	OrdType   string
	TradeID   uint64
	Timestamp time.Time
	RawJSON   []byte
}

// Parse discriminates by `channel` / `method` / `type` and returns a typed Frame.
func Parse(raw []byte) (Frame, error) {
	f := Frame{Raw: raw}
	var peek struct {
		Method  string          `json:"method"`
		Channel string          `json:"channel"`
		Type    string          `json:"type"`
		Data    json.RawMessage `json:"data"`
		ReqID   uint64          `json:"req_id"`
		Success *bool           `json:"success"`
		Error   string          `json:"error"`
		Symbol  string          `json:"symbol"`
		Result  json.RawMessage `json:"result"`
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	if err := dec.Decode(&peek); err != nil {
		return f, fmt.Errorf("wsv2: peek: %w; raw=%s", err, raw)
	}
	if peek.Method == "subscribe" || peek.Method == "unsubscribe" {
		ack := SubscribeAck{ReqID: peek.ReqID, Symbol: peek.Symbol, Error: peek.Error}
		if peek.Success != nil {
			ack.Success = *peek.Success
		}
		if ack.Success && len(peek.Result) > 0 {
			var r struct {
				Channel string `json:"channel"`
				Symbol  string `json:"symbol"`
			}
			if err := json.Unmarshal(peek.Result, &r); err != nil {
				return f, fmt.Errorf("wsv2: ack result: %w", err)
			}
			ack.Channel = r.Channel
			if ack.Symbol == "" {
				ack.Symbol = r.Symbol
			}
		}
		f.Kind = FrameSubscribeAck
		f.SubscribeAck = &ack
		return f, nil
	}
	switch peek.Channel {
	case "heartbeat":
		f.Kind = FrameHeartbeat
		return f, nil
	case "status":
		var data []StatusFrame
		if err := json.Unmarshal(peek.Data, &data); err != nil {
			return f, fmt.Errorf("wsv2: status data: %w", err)
		}
		if len(data) > 0 {
			f.Status = &data[0]
		}
		f.Kind = FrameStatus
		return f, nil
	case "book":
		var data []BookFrame
		d := json.NewDecoder(bytes.NewReader(peek.Data))
		d.UseNumber()
		if err := d.Decode(&data); err != nil {
			return f, fmt.Errorf("wsv2: book data: %w", err)
		}
		if len(data) > 0 {
			f.Book = &data[0]
			if f.Book.Bids == nil {
				f.Book.Bids = []BookLevel{}
			}
			if f.Book.Asks == nil {
				f.Book.Asks = []BookLevel{}
			}
		}
		if peek.Type == "snapshot" {
			f.Kind = FrameBookSnapshot
		} else {
			f.Kind = FrameBookUpdate
		}
		return f, nil
	case "trade":
		var data []TradeFrame
		d := json.NewDecoder(bytes.NewReader(peek.Data))
		d.UseNumber()
		if err := d.Decode(&data); err != nil {
			return f, fmt.Errorf("wsv2: trade data: %w", err)
		}
		var rawRecs []json.RawMessage
		if err := json.Unmarshal(peek.Data, &rawRecs); err != nil {
			return f, fmt.Errorf("wsv2: trade raw: %w", err)
		}
		for i := range data {
			if i < len(rawRecs) {
				data[i].RawJSON = append([]byte{}, rawRecs[i]...)
			}
		}
		f.Trades = data
		if peek.Type == "snapshot" {
			f.Kind = FrameTradeSnapshot
		} else {
			f.Kind = FrameTradeUpdate
		}
		return f, nil
	}
	return f, fmt.Errorf("wsv2: unknown channel %q", peek.Channel)
}

// UnmarshalJSON parses one wire trade record into a TradeFrame.
func (t *TradeFrame) UnmarshalJSON(data []byte) error {
	type alias struct {
		Symbol    string      `json:"symbol"`
		Side      string      `json:"side"`
		Price     json.Number `json:"price"`
		Qty       json.Number `json:"qty"`
		OrdType   string      `json:"ord_type"`
		TradeID   uint64      `json:"trade_id"`
		Timestamp string      `json:"timestamp"`
	}
	var a alias
	d := json.NewDecoder(bytes.NewReader(data))
	d.UseNumber()
	if err := d.Decode(&a); err != nil {
		return err
	}
	ts, err := time.Parse(time.RFC3339Nano, a.Timestamp)
	if err != nil {
		return fmt.Errorf("trade timestamp: %w", err)
	}
	*t = TradeFrame{
		Symbol: a.Symbol, Side: a.Side, Price: a.Price, Qty: a.Qty,
		OrdType: a.OrdType, TradeID: a.TradeID, Timestamp: ts,
	}
	return nil
}

// UnmarshalJSON parses one wire book-data record into a BookFrame.
func (b *BookFrame) UnmarshalJSON(data []byte) error {
	type alias struct {
		Symbol    string      `json:"symbol"`
		Bids      []BookLevel `json:"bids"`
		Asks      []BookLevel `json:"asks"`
		Checksum  uint32      `json:"checksum"`
		Timestamp string      `json:"timestamp"`
	}
	var a alias
	d := json.NewDecoder(bytes.NewReader(data))
	d.UseNumber()
	if err := d.Decode(&a); err != nil {
		return err
	}
	var ts time.Time
	if a.Timestamp != "" {
		var err error
		ts, err = time.Parse(time.RFC3339Nano, a.Timestamp)
		if err != nil {
			return fmt.Errorf("book timestamp: %w", err)
		}
	}
	*b = BookFrame{Symbol: a.Symbol, Bids: a.Bids, Asks: a.Asks, Checksum: a.Checksum, Timestamp: ts}
	return nil
}

// ErrBadFrame is sentinel for tests that need a typed compare.
var ErrBadFrame = errors.New("wsv2: bad frame")
