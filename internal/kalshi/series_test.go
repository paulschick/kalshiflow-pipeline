package kalshi

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestGetSeries(t *testing.T) {
	cases := []struct {
		name       string
		cursor     string
		status     int
		body       string
		wantBody   string
		wantErr    bool
		wantErrSub string
	}{
		{
			name:     "single page ok",
			cursor:   "",
			status:   200,
			body:     `{"series":[{"ticker":"KXBTC"}]}`,
			wantBody: `{"series":[{"ticker":"KXBTC"}]}`,
		},
		{
			name:     "cursor-paged ok",
			cursor:   "abc",
			status:   200,
			body:     `{"series":[{"ticker":"KXETH"}],"cursor":"def"}`,
			wantBody: `{"series":[{"ticker":"KXETH"}],"cursor":"def"}`,
		},
		{
			name:       "5xx surfaces error",
			cursor:     "",
			status:     500,
			body:       `{"error":"down"}`,
			wantErr:    true,
			wantErrSub: "status 500",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != "/trade-api/v2/series" {
					t.Errorf("path: got %s, want /trade-api/v2/series", r.URL.Path)
				}
				if got := r.URL.Query().Get("cursor"); got != tc.cursor {
					t.Errorf("cursor: got %q, want %q", got, tc.cursor)
				}
				w.WriteHeader(tc.status)
				_, _ = w.Write([]byte(tc.body))
			}))
			defer srv.Close()

			key, _ := rsa.GenerateKey(rand.Reader, 2048)
			signer := &Signer{KeyID: "kid", PrivateKey: key}
			c := NewClient(Config{
				BaseURL:    srv.URL,
				Signer:     signer,
				Now:        func() time.Time { return time.Unix(0, 0) },
				MaxRetries: 1,
			})

			body, err := c.GetSeries(context.Background(), tc.cursor)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected error, got body=%s", body)
				}
				if !strings.Contains(err.Error(), tc.wantErrSub) {
					t.Fatalf("err substring %q not in %v", tc.wantErrSub, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if string(body) != tc.wantBody {
				t.Fatalf("body: got %s, want %s", body, tc.wantBody)
			}
		})
	}
}
