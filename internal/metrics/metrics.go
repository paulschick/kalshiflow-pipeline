// Package metrics wires OpenTelemetry meters for Plan 2 workloads. Slice 2.4 wires the
// real Cloud Monitoring exporter; Disabled=true keeps the noop path for unit tests.
package metrics

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"sync/atomic"
	"time"

	mexporter "github.com/GoogleCloudPlatform/opentelemetry-operations-go/exporter/metric"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	semconv "go.opentelemetry.io/otel/semconv/v1.40.0"
)

// Options configures the meter provider.
type Options struct {
	ProjectID string
	Service   string
	Disabled  bool
	Interval  time.Duration // 0 → 60s
}

// Metrics holds the Plan 2 instrument surface. Every field is a typed wrapper around the OTel
// instrument; in noop mode the underlying instruments are nil and the wrapper methods
// short-circuit, so callers never need to nil-check.
type Metrics struct {
	provider *sdkmetric.MeterProvider

	WSMessagesReceived         Counter
	WSReconnects               Counter
	WSWatchdogForceReconnects  Counter
	WSDataStallForceReconnects Counter
	WSDataStallTicks           Counter
	WSDataStallPanics          Counter
	WSBackoffAttemptCurrent    Gauge
	PubsubPublishFailures      Counter
	PubsubPublishLatency       Histogram
	PubsubDrainUnflushed       Counter
	SubscriptionRosterSize     Gauge
	DiscoveryContractsAdded    Counter
	DiscoveryContractsRemoved  Counter
	DiscoveryPaginationCapped  Counter
	DiscoverySweepRuns         Counter
	DiscoverySweepDuration     Histogram

	BookkeeperDeltasProcessed  Counter
	BookkeeperSnapshotsEmitted Counter
	BookkeeperRegistrySize     Gauge

	ParquetRowsExported Counter
	ParquetRunDuration  Histogram
	ParquetRunOutcome   Counter

	SeriesDiscoveryRunOutcome   Counter
	SeriesDiscoveryRunDuration  Histogram
	SeriesDiscoveryRowsInserted Counter

	WorkerSubscribedSeriesCount Gauge
	WorkerDesiredSetStale       Gauge
	WorkerDesiredSetEmpty       Gauge

	// Kraken (HOL-48) — parallel instrument set scoped to the kraken-ws-worker
	// binary. Names emit under workload.googleapis.com/<name>; HOL-51 wires
	// dashboard tiles + alerts against these.
	KrakenWSMessagesReceived         Counter
	KrakenWSReconnects               Counter
	KrakenWSSilentStallReconnects    Counter
	KrakenWSSubscribeRejected        Counter
	KrakenWSChecksumMismatches       Counter
	KrakenWSPerSymbolResubs          Counter
	KrakenWSResubDropped             Counter
	KrakenBookkeeperSnapshotsEmitted Counter
	KrakenBookkeeperRegistrySize     Gauge
	KrakenPubsubPublishFailures      Counter
	KrakenPubsubPublishLatencyMs     Histogram
	KrakenPubsubDrainUnflushed       Counter
	KrakenFlagEnabled                Gauge

	// krakenConnStart is the unix-nanos start of the current Kraken WS session.
	// Read by the kraken_ws_connection_age_seconds observable gauge.
	krakenConnStart atomic.Int64

	// connStart is the unix-nanos start of the current WS session. 0 = no
	// connection yet (initial state, or in the reconnect-backoff gap).
	connStart atomic.Int64
}

// SetKrakenConnectionStart records the wall-clock start of a fresh Kraken WS
// session. Pass time.Time{} to indicate "disconnected".
func (m *Metrics) SetKrakenConnectionStart(t time.Time) {
	if t.IsZero() {
		m.krakenConnStart.Store(0)
		return
	}
	m.krakenConnStart.Store(t.UnixNano())
}

// SetConnectionStart records the wall-clock start of a fresh WS session. The
// observable callback for ws_connection_age_seconds reads this on each export
// to compute current age. Pass time.Time{} (zero value) to indicate
// "disconnected" — the callback then skips observation.
func (m *Metrics) SetConnectionStart(t time.Time) {
	if t.IsZero() {
		m.connStart.Store(0)
		return
	}
	m.connStart.Store(t.UnixNano())
}

// Counter wraps an OTel Int64Counter to take alternating key/value label pairs.
type Counter struct{ inst metric.Int64Counter }

// Add increments the counter. labels is alternating key, value strings.
func (c Counter) Add(ctx context.Context, n int64, labels ...string) {
	if c.inst == nil {
		return
	}
	c.inst.Add(ctx, n, metric.WithAttributes(toAttrs(labels)...))
}

// Gauge wraps an OTel Int64Gauge.
type Gauge struct{ inst metric.Int64Gauge }

// Record records a gauge value.
func (g Gauge) Record(ctx context.Context, v int64, labels ...string) {
	if g.inst == nil {
		return
	}
	g.inst.Record(ctx, v, metric.WithAttributes(toAttrs(labels)...))
}

// Histogram wraps an OTel Float64Histogram.
type Histogram struct{ inst metric.Float64Histogram }

// Record records a histogram value (e.g. a latency in milliseconds).
func (h Histogram) Record(ctx context.Context, v float64, labels ...string) {
	if h.inst == nil {
		return
	}
	h.inst.Record(ctx, v, metric.WithAttributes(toAttrs(labels)...))
}

func toAttrs(kv []string) []attribute.KeyValue {
	out := make([]attribute.KeyValue, 0, len(kv)/2)
	for i := 0; i+1 < len(kv); i += 2 {
		out = append(out, attribute.String(kv[i], kv[i+1]))
	}
	return out
}

// bootID returns a process-unique token used to make the exported OTel
// host.id distinct per process start, so an in-place container restart never
// collides on the shared Cloud Monitoring generic_node series with the
// draining old process (HOL-111).
func bootID() (string, error) {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("boot id: %w", err)
	}
	return hex.EncodeToString(b[:]), nil
}

// buildResource composes the OTel resource the meter provider exports under.
// host.id is populated from K_REVISION (Cloud Run system-injected), suffixed
// with the service name and a per-boot token so each process start lands on a
// distinct generic_node.node_id in Cloud Monitoring. The /<service> suffix
// (HOL-70) separates sibling pool containers; the /<boot-id> suffix (HOL-111)
// separates successive process starts so an in-place restart cannot poison the
// draining old process's shared per-resource start_time scope and have its
// writes rejected (HOL-69 was the first occurrence of that fingerprint).
//
// Deliberately NOT setting service.instance.id: the GCM exporter maps a
// resource carrying service.name + service.instance.id to generic_task (with
// task_id), which takes priority over the host.id -> generic_node mapping.
// Setting it flips every ws-worker/kraken OTel series off generic_node and
// silently breaks the alerts and dashboard panels keyed on generic_node
// (kraken_ws_book_input_silence, kraken_bookkeeper_change_silence). host.id
// carries the boot token alone so the series stays generic_node.
func buildResource(service string) (*resource.Resource, error) {
	attrs := []attribute.KeyValue{
		semconv.ServiceName(service),
	}
	if rev := os.Getenv("K_REVISION"); rev != "" {
		boot, err := bootID()
		if err != nil {
			return nil, err
		}
		attrs = append(attrs, semconv.HostID(rev+"/"+service+"/"+boot))
	}
	return resource.Merge(resource.Default(), resource.NewWithAttributes(
		semconv.SchemaURL,
		attrs...,
	))
}

// New constructs a *Metrics. With Disabled=true, instruments are no-ops.
func New(_ context.Context, opts Options) (*Metrics, error) {
	var meter metric.Meter
	var provider *sdkmetric.MeterProvider

	if opts.Disabled {
		meter = noop.NewMeterProvider().Meter("noop")
	} else {
		if opts.ProjectID == "" {
			return nil, fmt.Errorf("metrics: ProjectID required when Disabled=false")
		}
		exp, err := mexporter.New(mexporter.WithProjectID(opts.ProjectID))
		if err != nil {
			return nil, fmt.Errorf("otel cloud-monitoring exporter: %w", err)
		}
		interval := opts.Interval
		if interval == 0 {
			interval = 60 * time.Second
		}
		res, err := buildResource(opts.Service)
		if err != nil {
			return nil, fmt.Errorf("otel resource: %w", err)
		}
		provider = sdkmetric.NewMeterProvider(
			sdkmetric.WithReader(sdkmetric.NewPeriodicReader(exp, sdkmetric.WithInterval(interval))),
			sdkmetric.WithResource(res),
		)
		meter = provider.Meter("kalshiflow/" + opts.Service)
	}

	m := &Metrics{provider: provider}
	var err error
	if m.WSMessagesReceived.inst, err = meter.Int64Counter("ws_messages_received_total"); err != nil {
		return nil, err
	}
	if m.WSReconnects.inst, err = meter.Int64Counter("ws_reconnects_total"); err != nil {
		return nil, err
	}
	if m.WSWatchdogForceReconnects.inst, err = meter.Int64Counter("ws_watchdog_force_reconnects_total"); err != nil {
		return nil, fmt.Errorf("ws_watchdog_force_reconnects_total: %w", err)
	}
	if m.WSDataStallForceReconnects.inst, err = meter.Int64Counter("ws_data_stall_force_reconnects_total"); err != nil {
		return nil, fmt.Errorf("ws_data_stall_force_reconnects_total: %w", err)
	}
	if m.WSDataStallTicks.inst, err = meter.Int64Counter("ws_data_stall_ticks_total"); err != nil {
		return nil, fmt.Errorf("ws_data_stall_ticks_total: %w", err)
	}
	if m.WSDataStallPanics.inst, err = meter.Int64Counter("ws_data_stall_panics_total"); err != nil {
		return nil, fmt.Errorf("ws_data_stall_panics_total: %w", err)
	}
	if m.WSBackoffAttemptCurrent.inst, err = meter.Int64Gauge("ws_backoff_attempt_current"); err != nil {
		return nil, fmt.Errorf("ws_backoff_attempt_current: %w", err)
	}
	if m.PubsubPublishFailures.inst, err = meter.Int64Counter("pubsub_publish_failures_total"); err != nil {
		return nil, err
	}
	if m.PubsubPublishLatency.inst, err = meter.Float64Histogram("pubsub_publish_latency_ms"); err != nil {
		return nil, err
	}
	if m.PubsubDrainUnflushed.inst, err = meter.Int64Counter("pubsub_drain_unflushed_total"); err != nil {
		return nil, err
	}
	if m.SubscriptionRosterSize.inst, err = meter.Int64Gauge("subscription_roster_size"); err != nil {
		return nil, err
	}
	if m.DiscoveryContractsAdded.inst, err = meter.Int64Counter("discovery_contracts_added_total"); err != nil {
		return nil, err
	}
	if m.DiscoveryContractsRemoved.inst, err = meter.Int64Counter("discovery_contracts_removed_total"); err != nil {
		return nil, err
	}
	if m.DiscoveryPaginationCapped.inst, err = meter.Int64Counter("discovery_pagination_capped_total"); err != nil {
		return nil, err
	}
	if m.DiscoverySweepRuns.inst, err = meter.Int64Counter("discovery_sweep_runs_total"); err != nil {
		return nil, fmt.Errorf("discovery_sweep_runs_total: %w", err)
	}
	if m.DiscoverySweepDuration.inst, err = meter.Float64Histogram("discovery_sweep_duration_ms"); err != nil {
		return nil, fmt.Errorf("discovery_sweep_duration_ms: %w", err)
	}
	if m.BookkeeperDeltasProcessed.inst, err = meter.Int64Counter("bookkeeper_deltas_processed_total"); err != nil {
		return nil, err
	}
	if m.BookkeeperSnapshotsEmitted.inst, err = meter.Int64Counter("bookkeeper_snapshots_emitted_total"); err != nil {
		return nil, err
	}
	if m.BookkeeperRegistrySize.inst, err = meter.Int64Gauge("bookkeeper_registry_size"); err != nil {
		return nil, err
	}
	if m.ParquetRowsExported.inst, err = meter.Int64Counter("parquet_rows_exported_total"); err != nil {
		return nil, fmt.Errorf("parquet_rows_exported_total: %w", err)
	}
	if m.ParquetRunDuration.inst, err = meter.Float64Histogram("parquet_run_duration_seconds"); err != nil {
		return nil, fmt.Errorf("parquet_run_duration_seconds: %w", err)
	}
	if m.ParquetRunOutcome.inst, err = meter.Int64Counter("parquet_run_outcome_total"); err != nil {
		return nil, fmt.Errorf("parquet_run_outcome_total: %w", err)
	}

	if m.SeriesDiscoveryRunOutcome.inst, err = meter.Int64Counter("series_discovery_run_outcome_total"); err != nil {
		return nil, fmt.Errorf("series_discovery_run_outcome_total: %w", err)
	}
	if m.SeriesDiscoveryRunDuration.inst, err = meter.Float64Histogram("series_discovery_run_duration_seconds"); err != nil {
		return nil, fmt.Errorf("series_discovery_run_duration_seconds: %w", err)
	}
	if m.SeriesDiscoveryRowsInserted.inst, err = meter.Int64Counter("series_discovery_rows_inserted_total"); err != nil {
		return nil, fmt.Errorf("series_discovery_rows_inserted_total: %w", err)
	}

	if m.WorkerSubscribedSeriesCount.inst, err = meter.Int64Gauge("worker_subscribed_series_count"); err != nil {
		return nil, fmt.Errorf("worker_subscribed_series_count: %w", err)
	}
	if m.WorkerDesiredSetStale.inst, err = meter.Int64Gauge("worker_desired_set_stale"); err != nil {
		return nil, fmt.Errorf("worker_desired_set_stale: %w", err)
	}
	if m.WorkerDesiredSetEmpty.inst, err = meter.Int64Gauge("worker_desired_set_empty"); err != nil {
		return nil, fmt.Errorf("worker_desired_set_empty: %w", err)
	}

	if _, err = meter.Int64ObservableGauge(
		"ws_connection_age_seconds",
		metric.WithInt64Callback(func(_ context.Context, o metric.Int64Observer) error {
			startNanos := m.connStart.Load()
			if startNanos == 0 {
				return nil
			}
			age := time.Since(time.Unix(0, startNanos)).Seconds()
			o.Observe(int64(age))
			return nil
		}),
	); err != nil {
		return nil, err
	}

	// Kraken (HOL-48) instrument constructors.
	if m.KrakenWSMessagesReceived.inst, err = meter.Int64Counter("kraken_ws_messages_received_total"); err != nil {
		return nil, fmt.Errorf("kraken_ws_messages_received_total: %w", err)
	}
	if m.KrakenWSReconnects.inst, err = meter.Int64Counter("kraken_ws_reconnects_total"); err != nil {
		return nil, fmt.Errorf("kraken_ws_reconnects_total: %w", err)
	}
	if m.KrakenWSSilentStallReconnects.inst, err = meter.Int64Counter("kraken_ws_silent_stall_force_reconnects_total"); err != nil {
		return nil, fmt.Errorf("kraken_ws_silent_stall_force_reconnects_total: %w", err)
	}
	if m.KrakenWSSubscribeRejected.inst, err = meter.Int64Counter("kraken_ws_subscribe_rejected_total"); err != nil {
		return nil, fmt.Errorf("kraken_ws_subscribe_rejected_total: %w", err)
	}
	if m.KrakenWSChecksumMismatches.inst, err = meter.Int64Counter("kraken_ws_checksum_mismatches_total"); err != nil {
		return nil, fmt.Errorf("kraken_ws_checksum_mismatches_total: %w", err)
	}
	if m.KrakenWSPerSymbolResubs.inst, err = meter.Int64Counter("kraken_ws_per_symbol_resubscribes_total"); err != nil {
		return nil, fmt.Errorf("kraken_ws_per_symbol_resubscribes_total: %w", err)
	}
	if m.KrakenWSResubDropped.inst, err = meter.Int64Counter("kraken_ws_resub_dropped_total"); err != nil {
		return nil, fmt.Errorf("kraken_ws_resub_dropped_total: %w", err)
	}
	if m.KrakenBookkeeperSnapshotsEmitted.inst, err = meter.Int64Counter("kraken_bookkeeper_snapshots_emitted_total"); err != nil {
		return nil, fmt.Errorf("kraken_bookkeeper_snapshots_emitted_total: %w", err)
	}
	if m.KrakenBookkeeperRegistrySize.inst, err = meter.Int64Gauge("kraken_bookkeeper_registry_size"); err != nil {
		return nil, fmt.Errorf("kraken_bookkeeper_registry_size: %w", err)
	}
	if m.KrakenPubsubPublishFailures.inst, err = meter.Int64Counter("kraken_pubsub_publish_failures_total"); err != nil {
		return nil, fmt.Errorf("kraken_pubsub_publish_failures_total: %w", err)
	}
	if m.KrakenPubsubPublishLatencyMs.inst, err = meter.Float64Histogram("kraken_pubsub_publish_latency_ms"); err != nil {
		return nil, fmt.Errorf("kraken_pubsub_publish_latency_ms: %w", err)
	}
	if m.KrakenPubsubDrainUnflushed.inst, err = meter.Int64Counter("kraken_pubsub_drain_unflushed_total"); err != nil {
		return nil, fmt.Errorf("kraken_pubsub_drain_unflushed_total: %w", err)
	}
	if m.KrakenFlagEnabled.inst, err = meter.Int64Gauge("kraken_flag_enabled"); err != nil {
		return nil, fmt.Errorf("kraken_flag_enabled: %w", err)
	}

	if _, err = meter.Int64ObservableGauge(
		"kraken_ws_connection_age_seconds",
		metric.WithInt64Callback(func(_ context.Context, o metric.Int64Observer) error {
			startNanos := m.krakenConnStart.Load()
			if startNanos == 0 {
				return nil
			}
			age := time.Since(time.Unix(0, startNanos)).Seconds()
			o.Observe(int64(age))
			return nil
		}),
	); err != nil {
		return nil, fmt.Errorf("kraken_ws_connection_age_seconds: %w", err)
	}

	return m, nil
}

// Shutdown flushes any pending metrics. Noop when Disabled=true (provider stays nil).
func (m *Metrics) Shutdown(ctx context.Context) error {
	if m.provider == nil {
		return nil
	}
	return m.provider.Shutdown(ctx)
}
