package kalshi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/coder/websocket"
)

// ChannelOrderbookDelta is the Kalshi WS channel name and inbound frame type for orderbook
// delta updates. The same token serves as the Subscribe channel argument and as the inbound
// frame.Type discriminator, so it has one source of truth.
const ChannelOrderbookDelta = "orderbook_delta"

// ErrNotOpened is returned by ReadMessage / writeJSON when called before Open.
var ErrNotOpened = errors.New("kalshi: ws not opened")

// WSConfig configures the Kalshi WebSocket client.
type WSConfig struct {
	URL    string
	Signer *Signer
	Now    func() time.Time

	// SignPath is the path used for the WS handshake signature. Defaults to "/trade-api/ws/v2"
	// — set explicitly only if Kalshi changes the WS path.
	SignPath string
}

// WS is a minimal, single-connection Kalshi WebSocket client.
//
// Slice 2.1a uses Open / Subscribe / ReadMessage / Close. Slice 2.3 adds Unsubscribe (which
// takes server-assigned sids, not channels/tickers — Kalshi protocol requirement). Slice 2.4
// adds the surrounding reconnect lifecycle.
type WS struct {
	cfg    WSConfig
	conn   atomic.Pointer[websocket.Conn]
	cmdSeq atomic.Int64
}

// NewWS constructs a WS. It does not dial yet; call Open first.
func NewWS(cfg WSConfig) *WS {
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	if cfg.SignPath == "" {
		cfg.SignPath = "/trade-api/ws/v2"
	}
	return &WS{cfg: cfg}
}

// Frame is the parsed envelope of an inbound WS message. RawPayload is the `msg` object
// retained as raw bytes for the Pub/Sub envelope.
//
// ID is the client-side request id Kalshi echoes back on `ok`/`error` ack frames.
// Inbound channel frames have no `id` (defaults to 0); only ack frames carry it.
// The worker uses ID to correlate ack → roster entries (see internal/worker.Roster).
type Frame struct {
	ID         int64           `json:"id"`
	Type       string          `json:"type"`
	SID        int64           `json:"sid"`
	Seq        int64           `json:"seq"`
	RawPayload json.RawMessage `json:"msg"`
}

// Open dials the WS endpoint with auth headers in the handshake.
func (w *WS) Open(ctx context.Context) error {
	ts := strconv.FormatInt(w.cfg.Now().UnixMilli(), 10)
	headers, err := w.cfg.Signer.Headers(ts, http.MethodGet, w.cfg.SignPath)
	if err != nil {
		return fmt.Errorf("kalshi: ws auth headers: %w", err)
	}
	hdr := http.Header{}
	for k, v := range headers {
		hdr.Set(k, v)
	}
	c, _, err := websocket.Dial(ctx, w.cfg.URL, &websocket.DialOptions{HTTPHeader: hdr}) //nolint:bodyclose // coder/websocket sets resp.Body=nil on success
	if err != nil {
		return fmt.Errorf("kalshi: ws dial %s: %w", w.cfg.URL, err)
	}
	c.SetReadLimit(1 << 20) // 1 MiB — Kalshi snapshots can be large
	w.conn.Store(c)
	return nil
}

// Subscribe sends a Kalshi WS subscribe frame.
//
// Per Kalshi WS protocol, params.market_tickers is optional and only valid for
// channels that support per-ticker filtering. The market_lifecycle_v2 channel
// rejects market_tickers ("Invalid parameter", code 11) — pass a nil/empty
// slice to omit the key entirely.
func (w *WS) Subscribe(ctx context.Context, channels, marketTickers []string) error {
	params := map[string]any{
		"channels": channels,
	}
	if len(marketTickers) > 0 {
		params["market_tickers"] = marketTickers
	}
	cmd := map[string]any{
		"id":     w.cmdSeq.Add(1),
		"cmd":    "subscribe",
		"params": params,
	}
	return w.writeJSON(ctx, cmd)
}

// ReadMessage blocks until one frame arrives. Returns ctx.Err on cancel.
func (w *WS) ReadMessage(ctx context.Context) (Frame, error) {
	c := w.conn.Load()
	if c == nil {
		return Frame{}, ErrNotOpened
	}
	typ, body, err := c.Read(ctx)
	if err != nil {
		return Frame{}, fmt.Errorf("kalshi: ws read: %w", err)
	}
	if typ != websocket.MessageText {
		return Frame{}, fmt.Errorf("kalshi: ws unexpected message type %d", typ)
	}
	var f Frame
	if err := json.Unmarshal(body, &f); err != nil {
		return Frame{}, fmt.Errorf("kalshi: ws decode frame: %w", err)
	}
	return f, nil
}

// Unsubscribe cancels one or more subscriptions by their server-assigned sids. Per Kalshi
// WS protocol, unsubscribe is keyed by sid (not channel/ticker), so the worker must capture
// each subscribe's sid from its `ok` ack frame before it can unsubscribe.
func (w *WS) Unsubscribe(ctx context.Context, sids []int64) error {
	cmd := map[string]any{
		"id":  w.cmdSeq.Add(1),
		"cmd": "unsubscribe",
		"params": map[string]any{
			"sids": sids,
		},
	}
	return w.writeJSON(ctx, cmd)
}

// LastSubID returns the most recently issued client-side command id. Used by the worker to
// correlate the upcoming `ok` ack frame back to the just-sent subscribe. CLIENT-side
// request id, NOT the server-assigned sid.
func (w *WS) LastSubID() int64 {
	return w.cmdSeq.Load()
}

// Close closes the WS connection cleanly. Idempotent and safe to call
// concurrently from multiple goroutines (e.g. the worker's read-loop defer
// AND the watchdog goroutine's stall-detection path). Exactly one
// underlying coder/websocket Close fires per session; concurrent losers
// return nil without error.
func (w *WS) Close() error {
	c := w.conn.Load()
	if c == nil {
		return nil
	}
	if !w.conn.CompareAndSwap(c, nil) {
		return nil // someone else already swapped in nil — they own the close
	}
	if err := c.Close(websocket.StatusNormalClosure, ""); err != nil {
		return fmt.Errorf("kalshi: ws close: %w", err)
	}
	return nil
}

// Ping sends a WS control ping and blocks until the matching pong arrives
// or ctx deadlines / cancels. The watchdog passes a context with the
// configured pong deadline as its only timeout — a non-nil return is the
// stall signal.
func (w *WS) Ping(ctx context.Context) error {
	c := w.conn.Load()
	if c == nil {
		return ErrNotOpened
	}
	if err := c.Ping(ctx); err != nil {
		return fmt.Errorf("kalshi: ws ping: %w", err)
	}
	return nil
}

func (w *WS) writeJSON(ctx context.Context, v any) error {
	c := w.conn.Load()
	if c == nil {
		return ErrNotOpened
	}
	body, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("kalshi: marshal cmd: %w", err)
	}
	if err := c.Write(ctx, websocket.MessageText, body); err != nil {
		return fmt.Errorf("kalshi: ws write: %w", err)
	}
	return nil
}
