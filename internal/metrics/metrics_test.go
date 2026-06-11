package metrics

import (
	"context"
	"strings"
	"testing"
	"time"

	"go.opentelemetry.io/otel/attribute"
)

func TestNew_DisabledMeter_AllInstrumentsCallable(t *testing.T) {
	t.Parallel()
	m, err := New(context.Background(), Options{Disabled: true, Service: "ws-worker"})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer func() {
		if err := m.Shutdown(context.Background()); err != nil {
			t.Errorf("Shutdown: %v", err)
		}
	}()

	ctx := context.Background()
	m.SetConnectionStart(time.Now())
	m.SetConnectionStart(time.Time{})
	m.WSMessagesReceived.Add(ctx, 1, "stream", "orderbook")
	m.WSReconnects.Add(ctx, 1)
	m.PubsubPublishFailures.Add(ctx, 1, "stream", "orderbook")
	m.PubsubPublishLatency.Record(ctx, 12.5, "stream", "orderbook")
	m.PubsubDrainUnflushed.Add(ctx, 1, "stream", "orderbook")
	m.SubscriptionRosterSize.Record(ctx, 0)
	m.DiscoveryContractsAdded.Add(ctx, 1, "series_id", "KXBTCD")
	m.DiscoveryContractsRemoved.Add(ctx, 1, "series_id", "KXBTCD")
	m.DiscoveryPaginationCapped.Add(ctx, 1, "series_id", "KXBTCD")
	m.DiscoverySweepRuns.Add(ctx, 1, "series", "KXBTCD", "outcome", "ok")
	m.DiscoverySweepDuration.Record(ctx, 220.0, "series", "KXBTCD")
	m.BookkeeperDeltasProcessed.Add(ctx, 1)
	m.BookkeeperSnapshotsEmitted.Add(ctx, 1, "reason", "change")
	m.BookkeeperRegistrySize.Record(ctx, 0)
	m.ParquetRowsExported.Add(ctx, 1)
	m.ParquetRunDuration.Record(ctx, 0.5, "outcome", "ok")
	m.ParquetRunOutcome.Add(ctx, 1, "outcome", "ok")
	m.SeriesDiscoveryRunOutcome.Add(ctx, 1, "outcome", "ok")
	m.SeriesDiscoveryRunDuration.Record(ctx, 0.5, "outcome", "ok")
	m.SeriesDiscoveryRowsInserted.Add(ctx, 5, "kind", "catalog")
}

func TestNew_DisabledFalse_RequiresProjectID(t *testing.T) {
	t.Parallel()
	if _, err := New(context.Background(), Options{Disabled: false}); err == nil {
		t.Errorf("expected error when Disabled=false and ProjectID is empty")
	}
}

// assertHex16 checks that s is exactly 16 lowercase hex characters.
func assertHex16(t *testing.T, label, s string) {
	t.Helper()
	if len(s) != 16 {
		t.Errorf("%s: got len %d (%q), want 16 hex chars", label, len(s), s)
		return
	}
	for _, c := range s {
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			t.Errorf("%s: non-hex char %q in %q", label, c, s)
			return
		}
	}
}

func TestBuildResource_HostIDFromKRevision(t *testing.T) {
	cases := []struct {
		name        string
		kRevision   string
		service     string
		wantPresent bool
	}{
		{
			name:        "revision set populates host.id with ws-worker suffix",
			kRevision:   "ws-worker-00099-zzz",
			service:     "ws-worker",
			wantPresent: true,
		},
		{
			name:        "revision set populates host.id with kraken-ws-worker suffix",
			kRevision:   "ws-worker-00099-zzz",
			service:     "kraken-ws-worker",
			wantPresent: true,
		},
		{
			name:        "revision unset omits host.id",
			kRevision:   "",
			service:     "ws-worker",
			wantPresent: false,
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("K_REVISION", tc.kRevision)
			res, err := buildResource(tc.service)
			if err != nil {
				t.Fatalf("buildResource: %v", err)
			}

			// service.instance.id must stay UNSET (HOL-111): setting it flips
			// the GCM exporter from generic_node (host.id->node_id) to
			// generic_task (service.instance.id->task_id), which silently
			// breaks every alert/dashboard keyed on generic_node.
			if _, instOK := res.Set().Value(attribute.Key("service.instance.id")); instOK {
				t.Fatalf("service.instance.id present; must be unset to keep generic_node mapping (attrs=%v)", res.Attributes())
			}

			hostIDVal, hostIDOK := res.Set().Value(attribute.Key("host.id"))
			if hostIDOK != tc.wantPresent {
				t.Fatalf("host.id present=%v, want %v (attrs=%v)", hostIDOK, tc.wantPresent, res.Attributes())
			}
			if tc.wantPresent {
				prefix := tc.kRevision + "/" + tc.service + "/"
				hostID := hostIDVal.AsString()
				if len(hostID) <= len(prefix) || hostID[:len(prefix)] != prefix {
					t.Errorf("host.id = %q, want prefix %q", hostID, prefix)
				}
				assertHex16(t, "host.id boot suffix", hostID[len(prefix):])
			}
		})
	}

	t.Run("sibling containers produce distinct host.id within same revision", func(t *testing.T) {
		t.Setenv("K_REVISION", "ws-worker-00100-aaa")
		ws, err := buildResource("ws-worker")
		if err != nil {
			t.Fatalf("buildResource(ws-worker): %v", err)
		}
		kraken, err := buildResource("kraken-ws-worker")
		if err != nil {
			t.Fatalf("buildResource(kraken-ws-worker): %v", err)
		}
		wsHostID, _ := ws.Set().Value(attribute.Key("host.id"))
		krakenHostID, _ := kraken.Set().Value(attribute.Key("host.id"))
		if wsHostID.AsString() == krakenHostID.AsString() {
			t.Fatalf("siblings share host.id=%q; want distinct values", wsHostID.AsString())
		}
		// Distinctness must come from the /<service> segment (HOL-70), not just
		// the random boot-id suffix — otherwise this guard passes even if the
		// service suffix regresses. Assert the service-specific prefix.
		if wantPrefix := "ws-worker-00100-aaa/ws-worker/"; !strings.HasPrefix(wsHostID.AsString(), wantPrefix) {
			t.Errorf("ws-worker host.id = %q, want prefix %q", wsHostID.AsString(), wantPrefix)
		}
		if wantPrefix := "ws-worker-00100-aaa/kraken-ws-worker/"; !strings.HasPrefix(krakenHostID.AsString(), wantPrefix) {
			t.Errorf("kraken-ws-worker host.id = %q, want prefix %q", krakenHostID.AsString(), wantPrefix)
		}
	})

	t.Run("two boots produce distinct host.id within same revision", func(t *testing.T) {
		t.Setenv("K_REVISION", "ws-worker-00101-bbb")
		res1, err := buildResource("ws-worker")
		if err != nil {
			t.Fatalf("buildResource first: %v", err)
		}
		res2, err := buildResource("ws-worker")
		if err != nil {
			t.Fatalf("buildResource second: %v", err)
		}
		h1, _ := res1.Set().Value(attribute.Key("host.id"))
		h2, _ := res2.Set().Value(attribute.Key("host.id"))
		if h1.AsString() == h2.AsString() {
			t.Fatalf("successive boots share host.id=%q; want distinct (HOL-111)", h1.AsString())
		}
	})
}

func TestKrakenInstruments_AllPresentAndCallable(t *testing.T) {
	t.Parallel()
	m, err := New(context.Background(), Options{Disabled: true, Service: "kraken-ws-worker"})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer func() {
		if err := m.Shutdown(context.Background()); err != nil {
			t.Errorf("Shutdown: %v", err)
		}
	}()

	ctx := context.Background()
	m.SetKrakenConnectionStart(time.Now())
	m.SetKrakenConnectionStart(time.Time{})
	m.KrakenWSMessagesReceived.Add(ctx, 1, "channel", "book")
	m.KrakenWSReconnects.Add(ctx, 1)
	m.KrakenWSSilentStallReconnects.Add(ctx, 1)
	m.KrakenWSSubscribeRejected.Add(ctx, 1, "pair", "BTC/USD")
	m.KrakenWSChecksumMismatches.Add(ctx, 1, "pair", "BTC/USD")
	m.KrakenWSPerSymbolResubs.Add(ctx, 1, "pair", "BTC/USD")
	m.KrakenBookkeeperSnapshotsEmitted.Add(ctx, 1, "pair", "BTC/USD", "emit_reason", "change")
	m.KrakenBookkeeperRegistrySize.Record(ctx, 5)
	m.KrakenPubsubPublishFailures.Add(ctx, 1, "stream", "kraken_snapshot_1s")
	m.KrakenPubsubPublishLatencyMs.Record(ctx, 12.5, "stream", "kraken_snapshot_1s")
	m.KrakenPubsubDrainUnflushed.Add(ctx, 1, "stream", "kraken_snapshot_1s")
	m.KrakenFlagEnabled.Record(ctx, 1)
}
