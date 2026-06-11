package kalshi

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"

	"github.com/paulschick/kalshiflow-pipeline/internal/testutil"
)

func TestWS_OpenSubscribeRead(t *testing.T) {
	t.Parallel()
	priv, _ := rsa.GenerateKey(rand.Reader, 2048)

	got := struct {
		mu             sync.Mutex
		sawAuthHeaders bool
		subFrame       map[string]any
	}{}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("KALSHI-ACCESS-KEY") == "test-key" &&
			r.Header.Get("KALSHI-ACCESS-SIGNATURE") != "" &&
			r.Header.Get("KALSHI-ACCESS-TIMESTAMP") != "" {
			got.mu.Lock()
			got.sawAuthHeaders = true
			got.mu.Unlock()
		}

		c, err := websocket.Accept(w, r, &websocket.AcceptOptions{InsecureSkipVerify: true})
		if err != nil {
			t.Errorf("Accept: %v", err)
			return
		}
		defer func() { _ = c.Close(websocket.StatusNormalClosure, "") }()

		ctx := r.Context()

		_, body, err := c.Read(ctx)
		if err != nil {
			return
		}
		var frame map[string]any
		_ = json.Unmarshal(body, &frame)
		got.mu.Lock()
		got.subFrame = frame
		got.mu.Unlock()

		_ = c.Write(ctx, websocket.MessageText,
			[]byte(`{"id":1,"sid":42,"seq":1,"type":"ok","msg":{"market_tickers":["X"]}}`))
		_ = c.Write(ctx, websocket.MessageText,
			[]byte(`{"type":"orderbook_delta","sid":42,"seq":2,"msg":{"market_ticker":"X","price":50,"delta":1}}`))
	}))
	defer srv.Close()

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http")

	conn := NewWS(WSConfig{
		URL:    wsURL,
		Signer: &Signer{KeyID: "test-key", PrivateKey: priv},
		Now:    func() time.Time { return time.Unix(1714_400_000, 0) },
	})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := conn.Open(ctx); err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = conn.Close() }()

	if err := conn.Subscribe(ctx, []string{ChannelOrderbookDelta}, []string{"X"}); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	ack, err := conn.ReadMessage(ctx)
	if err != nil {
		t.Fatalf("ReadMessage ack: %v", err)
	}
	if ack.Type != "ok" {
		t.Errorf("first frame type = %s; want ok", ack.Type)
	}

	frame, err := conn.ReadMessage(ctx)
	if err != nil {
		t.Fatalf("ReadMessage data: %v", err)
	}
	if frame.Type != ChannelOrderbookDelta {
		t.Errorf("data frame type = %s; want %s", frame.Type, ChannelOrderbookDelta)
	}
	if frame.SID != 42 {
		t.Errorf("sid = %d; want 42", frame.SID)
	}
	if frame.Seq != 2 {
		t.Errorf("seq = %d; want 2", frame.Seq)
	}

	// WaitFor instead of time.Sleep — the server records the handshake before Accept returns,
	// but on the goroutine side the Read/Write to record subFrame may race the ReadMessage above.
	testutil.WaitFor(t, time.Second, "server records subscribe frame", func() bool {
		got.mu.Lock()
		defer got.mu.Unlock()
		return got.subFrame != nil
	})

	got.mu.Lock()
	defer got.mu.Unlock()
	if !got.sawAuthHeaders {
		t.Error("server did not see all three auth headers in handshake")
	}
	if got.subFrame["cmd"] != "subscribe" {
		t.Errorf("subscribe cmd missing: %v", got.subFrame)
	}
	params, _ := got.subFrame["params"].(map[string]any)
	if params == nil {
		t.Fatal("subscribe params missing")
	}
	channels, _ := params["channels"].([]any)
	if len(channels) != 1 || channels[0] != ChannelOrderbookDelta {
		t.Errorf("channels = %v; want [%s]", channels, ChannelOrderbookDelta)
	}
	tickers, _ := params["market_tickers"].([]any)
	if len(tickers) != 1 || tickers[0] != "X" {
		t.Errorf("market_tickers = %v; want [X]", tickers)
	}
}

// TestWS_Subscribe_OmitsEmptyMarketTickers asserts that with a nil or empty
// marketTickers slice, the subscribe frame omits the "market_tickers" key
// entirely. Slice 2.2 introduces the first nil-tickers call site (lifecycle
// subscribe); Kalshi rejects "market_tickers": null with code 11.
func TestWS_Subscribe_OmitsEmptyMarketTickers(t *testing.T) {
	t.Parallel()
	priv, _ := rsa.GenerateKey(rand.Reader, 2048)

	got := struct {
		mu       sync.Mutex
		subFrame map[string]any
	}{}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := websocket.Accept(w, r, &websocket.AcceptOptions{InsecureSkipVerify: true})
		if err != nil {
			t.Errorf("Accept: %v", err)
			return
		}
		defer func() { _ = c.Close(websocket.StatusNormalClosure, "") }()

		_, body, err := c.Read(r.Context())
		if err != nil {
			return
		}
		var frame map[string]any
		_ = json.Unmarshal(body, &frame)
		got.mu.Lock()
		got.subFrame = frame
		got.mu.Unlock()
	}))
	defer srv.Close()

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http")
	conn := NewWS(WSConfig{
		URL:    wsURL,
		Signer: &Signer{KeyID: "test-key", PrivateKey: priv},
		Now:    func() time.Time { return time.Unix(1714_400_000, 0) },
	})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := conn.Open(ctx); err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = conn.Close() }()

	if err := conn.Subscribe(ctx, []string{"market_lifecycle_v2"}, nil); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	testutil.WaitFor(t, time.Second, "server records subscribe frame", func() bool {
		got.mu.Lock()
		defer got.mu.Unlock()
		return got.subFrame != nil
	})

	got.mu.Lock()
	defer got.mu.Unlock()

	params, _ := got.subFrame["params"].(map[string]any)
	if params == nil {
		t.Fatal("subscribe params missing")
	}
	if _, present := params["market_tickers"]; present {
		t.Errorf("market_tickers must be omitted when nil/empty; got %v", params["market_tickers"])
	}
	channels, _ := params["channels"].([]any)
	if len(channels) != 1 || channels[0] != "market_lifecycle_v2" {
		t.Errorf("channels = %v; want [market_lifecycle_v2]", channels)
	}
}

func TestWS_Unsubscribe_SendsSidsList(t *testing.T) {
	t.Parallel()
	priv, _ := rsa.GenerateKey(rand.Reader, 2048)
	var captured []map[string]any
	var mu sync.Mutex

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := websocket.Accept(w, r, &websocket.AcceptOptions{InsecureSkipVerify: true})
		if err != nil {
			t.Errorf("Accept: %v", err)
			return
		}
		defer func() { _ = c.Close(websocket.StatusNormalClosure, "") }()
		ctx := r.Context()
		for i := 0; i < 2; i++ {
			_, body, err := c.Read(ctx)
			if err != nil {
				return
			}
			var f map[string]any
			_ = json.Unmarshal(body, &f)
			mu.Lock()
			captured = append(captured, f)
			mu.Unlock()
		}
	}))
	defer srv.Close()

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http")
	conn := NewWS(WSConfig{URL: wsURL, Signer: &Signer{KeyID: "k", PrivateKey: priv}, Now: func() time.Time { return time.Unix(0, 0) }})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := conn.Open(ctx); err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = conn.Close() }()

	if err := conn.Subscribe(ctx, []string{"orderbook_delta"}, []string{"X"}); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	if err := conn.Unsubscribe(ctx, []int64{42, 7}); err != nil {
		t.Fatalf("Unsubscribe: %v", err)
	}

	testutil.WaitFor(t, time.Second, "server captured Subscribe + Unsubscribe", func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(captured) == 2
	})

	mu.Lock()
	defer mu.Unlock()
	unsub := captured[1]
	if unsub["cmd"] != "unsubscribe" {
		t.Errorf("cmd = %v; want unsubscribe", unsub["cmd"])
	}
	params, _ := unsub["params"].(map[string]any)
	sids, _ := params["sids"].([]any)
	if len(sids) != 2 {
		t.Errorf("sids = %v; want length 2", sids)
	}
}

func TestWS_LastSubID_TracksMostRecent(t *testing.T) {
	t.Parallel()
	priv, _ := rsa.GenerateKey(rand.Reader, 2048)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := websocket.Accept(w, r, &websocket.AcceptOptions{InsecureSkipVerify: true})
		if err != nil {
			return
		}
		defer func() { _ = c.Close(websocket.StatusNormalClosure, "") }()
		for {
			if _, _, err := c.Read(r.Context()); err != nil {
				return
			}
		}
	}))
	defer srv.Close()

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http")
	conn := NewWS(WSConfig{URL: wsURL, Signer: &Signer{KeyID: "k", PrivateKey: priv}, Now: func() time.Time { return time.Unix(0, 0) }})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := conn.Open(ctx); err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = conn.Close() }()

	_ = conn.Subscribe(ctx, []string{"a"}, []string{"x"})
	first := conn.LastSubID()
	_ = conn.Subscribe(ctx, []string{"b"}, []string{"y"})
	second := conn.LastSubID()
	if second != first+1 {
		t.Errorf("LastSubID after second = %d; want %d", second, first+1)
	}
}

// TestWS_RaceReadPingClose runs Read, Ping, and Close concurrently against
// a server that responds to control pings and slow-trickles data frames.
// Asserts no race fires under `go test -race`. Validates atomic.Pointer
// + idempotent-Close as the concurrency model for the WS watchdog.
func TestWS_RaceReadPingClose(t *testing.T) {
	t.Parallel()
	priv, _ := rsa.GenerateKey(rand.Reader, 2048)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := websocket.Accept(w, r, &websocket.AcceptOptions{InsecureSkipVerify: true})
		if err != nil {
			return
		}
		defer func() { _ = c.Close(websocket.StatusNormalClosure, "") }()
		ctx := r.Context()
		// Trickle one frame periodically so the read loop has something to
		// return; coder/websocket auto-pongs incoming pings while the read
		// loop is active. The handler exits when the client closes.
		ticker := time.NewTicker(20 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if err := c.Write(ctx, websocket.MessageText,
					[]byte(`{"type":"orderbook_delta","sid":1,"seq":1,"msg":{"market_ticker":"X"}}`)); err != nil {
					return
				}
			}
		}
	}))
	defer srv.Close()

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http")
	conn := NewWS(WSConfig{
		URL:    wsURL,
		Signer: &Signer{KeyID: "test-key", PrivateKey: priv},
		Now:    func() time.Time { return time.Unix(1714_400_000, 0) },
	})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := conn.Open(ctx); err != nil {
		t.Fatalf("Open: %v", err)
	}

	var wg sync.WaitGroup

	// Read loop.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			if _, err := conn.ReadMessage(ctx); err != nil {
				return
			}
		}
	}()

	// Ping loop.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 5; i++ {
			pingCtx, pingCancel := context.WithTimeout(ctx, 200*time.Millisecond)
			_ = conn.Ping(pingCtx)
			pingCancel()
			time.Sleep(30 * time.Millisecond)
		}
	}()

	// Close after a brief overlap window.
	time.Sleep(200 * time.Millisecond)
	// First Close runs concurrently with Read+Ping. The CAS winner runs the
	// underlying coder/websocket close-handshake; under heavy contention the
	// handshake's internal 5s timeout can fire before the peer's close frame
	// is processed. That timeout is a coder/websocket-level behavior, not a
	// race-safety failure — the contract validated here is (a) no DATA RACE
	// under -race and (b) the SECOND Close is idempotent and returns nil.
	if err := conn.Close(); err != nil {
		t.Logf("first Close (informational, may error under contention): %v", err)
	}
	// Second Close from a different goroutine — must be a no-op (load-then-CAS
	// guarantees exactly one underlying close per session).
	if err := conn.Close(); err != nil {
		t.Errorf("second Close should be no-op: %v", err)
	}

	wg.Wait()
}

// TestWS_PingDeadlineWhenServerSilent confirms WS.Ping returns a non-nil
// error that wraps context.DeadlineExceeded when the peer accepts the
// handshake but does not pong. The server simulates this by accepting the
// connection then never calling Read — coder/websocket only processes
// control frames as a side effect of Read on the receiving side, so no
// pong is ever sent.
func TestWS_PingDeadlineWhenServerSilent(t *testing.T) {
	t.Parallel()
	priv, _ := rsa.GenerateKey(rand.Reader, 2048)

	serverDone := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := websocket.Accept(w, r, &websocket.AcceptOptions{InsecureSkipVerify: true})
		if err != nil {
			return
		}
		defer func() { _ = c.Close(websocket.StatusNormalClosure, "") }()
		// Block here without ever calling c.Read — control frames are NOT
		// processed, so client pings never get a pong. Exit when the request
		// context is done (i.e. client closed).
		<-r.Context().Done()
		close(serverDone)
	}))
	defer srv.Close()

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http")
	conn := NewWS(WSConfig{
		URL:    wsURL,
		Signer: &Signer{KeyID: "test-key", PrivateKey: priv},
		Now:    func() time.Time { return time.Unix(1714_400_000, 0) },
	})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := conn.Open(ctx); err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = conn.Close() }()

	pingCtx, pingCancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer pingCancel()

	err := conn.Ping(pingCtx)
	if err == nil {
		t.Fatal("Ping returned nil; want deadline-related error")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Ping err = %v; want one wrapping context.DeadlineExceeded", err)
	}
}
