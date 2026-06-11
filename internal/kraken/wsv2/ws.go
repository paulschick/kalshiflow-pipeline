package wsv2

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/rand/v2"

	"github.com/coder/websocket"
)

// SubscribeOpts carries channel-specific subscribe parameters.
type SubscribeOpts struct {
	Depth    int  // book channel only; 10/25/100/500/1000; default 10
	Snapshot bool // include the initial snapshot; default true
}

// Conn is the minimal Kraken WS v2 surface kalshiflow needs.
type Conn interface {
	Subscribe(ctx context.Context, channel string, symbols []string, opts SubscribeOpts) error
	Unsubscribe(ctx context.Context, channel string, symbols []string) error
	Read(ctx context.Context) (Frame, error)
	Close(code websocket.StatusCode, reason string) error
}

type wsConn struct {
	c        *websocket.Conn
	reqIDGen func() uint64
}

// Open dials wss://ws.kraken.com/v2 (or the override URL for tests).
func Open(ctx context.Context, url string) (Conn, error) {
	c, _, err := websocket.Dial(ctx, url, nil) //nolint:bodyclose // coder/websocket sets resp.Body=nil on success
	if err != nil {
		return nil, fmt.Errorf("wsv2: dial %s: %w", url, err)
	}
	// Kraken book frames at 10 pairs can spike past 32 KiB; raise the limit.
	c.SetReadLimit(8 * 1024 * 1024)
	return &wsConn{c: c, reqIDGen: defaultReqIDGen}, nil
}

func defaultReqIDGen() uint64 { return rand.Uint64() }

type subscribeEnvelope struct {
	Method string          `json:"method"`
	Params subscribeParams `json:"params"`
	ReqID  uint64          `json:"req_id"`
}

type subscribeParams struct {
	Channel  string   `json:"channel"`
	Symbol   []string `json:"symbol"`
	Depth    int      `json:"depth,omitempty"`
	Snapshot bool     `json:"snapshot,omitempty"`
}

func (w *wsConn) Subscribe(ctx context.Context, channel string, symbols []string, opts SubscribeOpts) error {
	return w.writeMethod(ctx, "subscribe", channel, symbols, opts)
}

func (w *wsConn) Unsubscribe(ctx context.Context, channel string, symbols []string) error {
	return w.writeMethod(ctx, "unsubscribe", channel, symbols, SubscribeOpts{})
}

func (w *wsConn) writeMethod(ctx context.Context, method, channel string, symbols []string, opts SubscribeOpts) error {
	env := subscribeEnvelope{
		Method: method,
		Params: subscribeParams{
			Channel:  channel,
			Symbol:   symbols,
			Depth:    opts.Depth,
			Snapshot: opts.Snapshot,
		},
		ReqID: w.reqIDGen(),
	}
	body, err := json.Marshal(env)
	if err != nil {
		return fmt.Errorf("wsv2: marshal %s: %w", method, err)
	}
	if err := w.c.Write(ctx, websocket.MessageText, body); err != nil {
		return fmt.Errorf("wsv2: write %s: %w", method, err)
	}
	return nil
}

func (w *wsConn) Read(ctx context.Context) (Frame, error) {
	_, r, err := w.c.Reader(ctx)
	if err != nil {
		return Frame{}, fmt.Errorf("wsv2: read: %w", err)
	}
	b, err := io.ReadAll(r)
	if err != nil {
		return Frame{}, fmt.Errorf("wsv2: drain: %w", err)
	}
	return Parse(b)
}

func (w *wsConn) Close(code websocket.StatusCode, reason string) error {
	return w.c.Close(code, reason)
}
