package kalshi

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestClient_AddsAuthHeaders(t *testing.T) {
	priv, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		key := r.Header.Get("KALSHI-ACCESS-KEY")
		ts := r.Header.Get("KALSHI-ACCESS-TIMESTAMP")
		sig := r.Header.Get("KALSHI-ACCESS-SIGNATURE")
		if key != "test-key" || ts == "" || sig == "" {
			t.Errorf("missing auth headers: key=%q ts=%q sig=%q", key, ts, sig)
		}
		raw, err := base64.StdEncoding.DecodeString(sig)
		if err != nil {
			t.Fatalf("sig decode: %v", err)
		}
		path := strings.SplitN(r.URL.RequestURI(), "?", 2)[0]
		msg := []byte(ts + r.Method + path)
		hash := sha256.Sum256(msg)
		if err := rsa.VerifyPSS(&priv.PublicKey, sha256Alg, hash[:], raw, &rsa.PSSOptions{SaltLength: rsa.PSSSaltLengthEqualsHash}); err != nil {
			t.Errorf("signature failed verify: %v", err)
		}
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer srv.Close()

	c := NewClient(Config{
		BaseURL: srv.URL,
		Signer:  &Signer{KeyID: "test-key", PrivateKey: priv},
		Now:     func() time.Time { return time.Unix(1714400000, 0) },
	})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	resp, err := c.do(ctx, http.MethodGet, "/trade-api/v2/markets?series_ticker=KXBTCD&status=open", nil)
	if err != nil {
		t.Fatalf("do: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != 200 {
		t.Errorf("status = %d", resp.StatusCode)
	}
}

func TestClient_RetriesOn429(t *testing.T) {
	priv, _ := rsa.GenerateKey(rand.Reader, 2048)
	calls := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls++
		if calls < 3 {
			w.Header().Set("Retry-After", "0")
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer srv.Close()

	c := NewClient(Config{
		BaseURL:     srv.URL,
		Signer:      &Signer{KeyID: "k", PrivateKey: priv},
		Now:         time.Now,
		MaxRetries:  5,
		BaseBackoff: 1 * time.Millisecond,
	})

	resp, err := c.do(context.Background(), http.MethodGet, "/x", nil)
	if err != nil {
		t.Fatalf("do: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if calls != 3 {
		t.Errorf("calls = %d; want 3", calls)
	}
}

func TestClient_RateLimiterThrottlesBursts(t *testing.T) {
	priv, _ := rsa.GenerateKey(rand.Reader, 2048)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer srv.Close()

	c := NewClient(Config{
		BaseURL: srv.URL,
		Signer:  &Signer{KeyID: "k", PrivateKey: priv},
		Now:     time.Now,
		RPS:     5,
		Burst:   5,
	})

	start := time.Now()
	for i := 0; i < 15; i++ {
		resp, err := c.do(context.Background(), http.MethodGet, "/x", nil)
		if err != nil {
			t.Fatalf("do[%d]: %v", i, err)
		}
		_ = resp.Body.Close()
	}
	elapsed := time.Since(start)
	if elapsed < 1500*time.Millisecond {
		t.Errorf("rate limiter didn't throttle: 15 calls in %v with RPS=5 burst=5 (expected >= 1.5s)", elapsed)
	}
}
