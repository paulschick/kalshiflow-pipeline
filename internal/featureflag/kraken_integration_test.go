//go:build integration

package featureflag

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"cloud.google.com/go/storage"
)

// TestKrakenFlag_GCS_RoundTrip requires:
//
//	KRAKEN_FLAG_TEST_BUCKET — GCS bucket the test SA can write
//
// Writes a temp object per run; cleans up via t.Cleanup. Mirrors the
// internal/seriescat bqwriter integration tests in spirit.
func TestKrakenFlag_GCS_RoundTrip(t *testing.T) {
	bucket := os.Getenv("KRAKEN_FLAG_TEST_BUCKET")
	if bucket == "" {
		t.Skip("KRAKEN_FLAG_TEST_BUCKET not set")
	}

	ctx := context.Background()
	client, err := storage.NewClient(ctx)
	if err != nil {
		t.Fatalf("storage client: %v", err)
	}
	defer func() { _ = client.Close() }()

	objectName := fmt.Sprintf("control/kraken_enabled_test_%d.json", time.Now().UnixNano())
	obj := client.Bucket(bucket).Object(objectName)
	t.Cleanup(func() {
		_ = obj.Delete(context.Background())
	})

	write := func(enabled bool, gen string) {
		w := obj.NewWriter(ctx)
		w.ContentType = "application/json"
		body := fmt.Sprintf(
			`{"enabled":%v,"generation":%q,"generated_at":%q,"reason":"int-test","actor":"int-test"}`,
			enabled, gen, time.Now().UTC().Format("2006-01-02T15:04:05.000Z"),
		)
		if _, err := w.Write([]byte(body)); err != nil {
			t.Fatalf("write %s: %v", objectName, err)
		}
		if err := w.Close(); err != nil {
			t.Fatalf("close %s: %v", objectName, err)
		}
	}

	f := NewGCSKrakenFlag(client, bucket, objectName)

	// 1. Object absent before any write → LoadOnce returns ErrAbsent.
	if err := f.LoadOnce(ctx); !errors.Is(err, ErrAbsent) {
		t.Fatalf("LoadOnce on absent object: err=%v; want ErrAbsent", err)
	}
	if !f.Enabled() {
		t.Fatalf("Enabled() before any load = false; want true (default-on)")
	}

	// 2. Write enabled:true → LoadOnce returns nil; Enabled() == true.
	write(true, "01H_ON")
	if err := f.LoadOnce(ctx); err != nil {
		t.Fatalf("LoadOnce after write true: %v", err)
	}
	if !f.Enabled() {
		t.Fatal("Enabled() = false after enabled:true write")
	}

	// 3. Overwrite with enabled:false → next LoadOnce flips.
	write(false, "01H_OFF")
	if err := f.LoadOnce(ctx); err != nil {
		t.Fatalf("LoadOnce after write false: %v", err)
	}
	if f.Enabled() {
		t.Fatal("Enabled() = true after enabled:false write")
	}

	// 4. Delete object → retain last-known false.
	if err := obj.Delete(ctx); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if err := f.LoadOnce(ctx); !errors.Is(err, ErrAbsent) {
		t.Fatalf("LoadOnce after delete: err=%v; want ErrAbsent", err)
	}
	if f.Enabled() {
		t.Errorf("Enabled() = true after delete; want retain false")
	}
}
