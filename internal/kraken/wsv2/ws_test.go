package wsv2

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
)

func wsTestServer(t *testing.T, handler func(*websocket.Conn)) (*httptest.Server, string) {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := websocket.Accept(w, r, nil)
		if err != nil {
			t.Errorf("accept: %v", err)
			return
		}
		defer func() { _ = c.Close(websocket.StatusNormalClosure, "") }()
		handler(c)
	}))
	url := "ws" + strings.TrimPrefix(srv.URL, "http")
	return srv, url
}

func TestConn_Subscribe_WritesExpectedEnvelope(t *testing.T) {
	var got string
	var mu sync.Mutex
	srv, url := wsTestServer(t, func(c *websocket.Conn) {
		_, r, err := c.Reader(context.Background())
		if err != nil {
			return
		}
		b, _ := io.ReadAll(r)
		mu.Lock()
		got = string(b)
		mu.Unlock()
	})
	defer srv.Close()
	conn, err := Open(context.Background(), url)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = conn.Close(websocket.StatusNormalClosure, "") }()
	conn.(*wsConn).reqIDGen = func() uint64 { return 42 }
	if err := conn.Subscribe(context.Background(), "book", []string{"BTC/USD", "ETH/USD"}, SubscribeOpts{Depth: 10, Snapshot: true}); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	time.Sleep(50 * time.Millisecond)
	mu.Lock()
	defer mu.Unlock()
	var parsed map[string]any
	if err := json.Unmarshal([]byte(got), &parsed); err != nil {
		t.Fatalf("parse sent envelope: %v; got=%s", err, got)
	}
	if parsed["method"] != "subscribe" {
		t.Errorf("method = %v; want subscribe", parsed["method"])
	}
	params := parsed["params"].(map[string]any)
	if params["channel"] != "book" {
		t.Errorf("channel = %v", params["channel"])
	}
	if parsed["req_id"].(float64) != 42 {
		t.Errorf("req_id = %v; want 42", parsed["req_id"])
	}
	syms := params["symbol"].([]any)
	if len(syms) != 2 || syms[0] != "BTC/USD" || syms[1] != "ETH/USD" {
		t.Errorf("symbols = %v", syms)
	}
	if params["depth"].(float64) != 10 {
		t.Errorf("depth = %v; want 10", params["depth"])
	}
	if params["snapshot"] != true {
		t.Errorf("snapshot = %v; want true", params["snapshot"])
	}
}

func TestConn_Read_ParsesStatusFrame(t *testing.T) {
	srv, url := wsTestServer(t, func(c *websocket.Conn) {
		_ = c.Write(context.Background(), websocket.MessageText, []byte(`{"channel":"status","type":"update","data":[{"version":"2.0.10","system":"online","api_version":"v2","connection_id":1}]}`))
		time.Sleep(50 * time.Millisecond)
	})
	defer srv.Close()
	conn, err := Open(context.Background(), url)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer func() { _ = conn.Close(websocket.StatusNormalClosure, "") }()
	f, err := conn.Read(context.Background())
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if f.Kind != FrameStatus || f.Status.System != "online" {
		t.Errorf("frame = %+v", f)
	}
}
