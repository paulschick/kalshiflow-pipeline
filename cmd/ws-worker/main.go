// Command ws-worker is the Kalshi WebSocket ingest binary. Slice 2.3a: lifecycle-driven
// discovery — KALSHI_SERIES is comma-split into series prefixes; the worker enumerates
// open markets via REST at startup and reacts to market_lifecycle_v2 events to
// dynamically subscribe and unsubscribe. Slice 2.4 adds reconnect + soak.
package main

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	gpubsub "cloud.google.com/go/pubsub/v2"
	vkit "cloud.google.com/go/pubsub/v2/apiv1"
	"cloud.google.com/go/storage"
	gax "github.com/googleapis/gax-go/v2"
	"google.golang.org/grpc/codes"

	"github.com/paulschick/kalshiflow-pipeline/internal/envelope"
	"github.com/paulschick/kalshiflow-pipeline/internal/kalshi"
	"github.com/paulschick/kalshiflow-pipeline/internal/metrics"
	"github.com/paulschick/kalshiflow-pipeline/internal/orderbook"
	psk "github.com/paulschick/kalshiflow-pipeline/internal/pubsub"
	"github.com/paulschick/kalshiflow-pipeline/internal/secrets"
	"github.com/paulschick/kalshiflow-pipeline/internal/worker"
)

type config struct {
	ProjectID            string
	SecretName           string
	Series               []string
	OpenStatus           string
	WSURL                string
	RESTURL              string
	RESTRPS              float64
	RESTBurst            int
	SweepInterval        time.Duration
	SweepMissThreshold   int
	WatchdogEnabled      bool
	WatchdogPingInterval time.Duration
	WatchdogPongDeadline time.Duration
	DataStallEnabled     bool
	DataFrameDeadline    time.Duration
	PreserveBookkeeper   bool
	BackoffDataThreshold int64
	BackoffMax           time.Duration
	LifecycleUnsub       bool
	ControlBucket        string
	ControlObject        string
}

func (c config) topicName(stream string) string { return "kalshi." + stream }

func parseConfig() (config, error) {
	rawSeries := cmp.Or(os.Getenv("KALSHI_SERIES"), "KXBTCD")
	c := config{
		ProjectID:  os.Getenv("KALSHI_PROJECT_ID"),
		SecretName: os.Getenv("KALSHI_SECRET_NAME"),
		Series:     splitCSV(rawSeries),
		// Probe-derived default ("open"), confirmed across 8 samples on 2026-05-03 via
		// scripts/probe-kalshi-status.py.
		OpenStatus:           cmp.Or(os.Getenv("KALSHI_OPEN_STATUS"), "open"),
		WSURL:                cmp.Or(os.Getenv("KALSHI_WS_URL_PROD"), "wss://api.elections.kalshi.com/trade-api/ws/v2"),
		RESTURL:              cmp.Or(os.Getenv("KALSHI_REST_BASE_PROD"), "https://api.elections.kalshi.com"),
		RESTRPS:              parseFloat("KALSHI_REST_RPS", 10),
		RESTBurst:            parseInt("KALSHI_REST_BURST", 20),
		SweepInterval:        parseDuration("KALSHI_SWEEP_INTERVAL", 3*time.Minute),
		SweepMissThreshold:   parseInt("KALSHI_SWEEP_MISS_THRESHOLD", 2),
		WatchdogEnabled:      parseBool("KALSHI_WS_WATCHDOG_ENABLED", true),
		WatchdogPingInterval: parseDuration("KALSHI_WS_WATCHDOG_PING_INTERVAL", 30*time.Second),
		WatchdogPongDeadline: parseDuration("KALSHI_WS_WATCHDOG_PONG_DEADLINE", 15*time.Second),
		DataStallEnabled:     parseBool("KALSHI_WS_DATA_STALL_ENABLED", true),
		DataFrameDeadline:    parseDuration("KALSHI_WS_DATA_FRAME_DEADLINE", 90*time.Second),
		PreserveBookkeeper:   parseBool("KALSHI_PRESERVE_BOOKKEEPER", true),
		BackoffDataThreshold: parseInt64("KALSHI_BACKOFF_DATA_THRESHOLD", 10),
		BackoffMax:           parseDuration("KALSHI_BACKOFF_MAX", 600*time.Second),
		LifecycleUnsub:       parseBool("KALSHI_LIFECYCLE_UNSUB", false),
		ControlBucket:        os.Getenv("KALSHI_CONTROL_BUCKET"),
		ControlObject:        cmp.Or(os.Getenv("KALSHI_CONTROL_OBJECT"), "control/series_desired.json"),
	}
	if c.ProjectID == "" {
		return c, errors.New("KALSHI_PROJECT_ID is required")
	}
	if c.SecretName == "" {
		c.SecretName = fmt.Sprintf("projects/%s/secrets/kalshi-creds", c.ProjectID)
	}
	if len(c.Series) == 0 {
		return c, errors.New("KALSHI_SERIES must contain at least one series ticker")
	}
	return c, nil
}

func parseFloat(env string, def float64) float64 {
	if v := os.Getenv(env); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			return f
		}
	}
	return def
}

func parseInt(env string, def int) int {
	if v := os.Getenv(env); v != "" {
		if i, err := strconv.Atoi(v); err == nil {
			return i
		}
	}
	return def
}

func parseInt64(env string, def int64) int64 {
	if v := os.Getenv(env); v != "" {
		if i, err := strconv.ParseInt(v, 10, 64); err == nil {
			return i
		}
	}
	return def
}

func parseDuration(env string, def time.Duration) time.Duration {
	if v := os.Getenv(env); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return def
}

func parseBool(env string, def bool) bool {
	if v := os.Getenv(env); v != "" {
		if b, err := strconv.ParseBool(v); err == nil {
			return b
		}
	}
	return def
}

func splitCSV(s string) []string {
	parts := strings.Split(s, ",")
	out := parts[:0]
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

// pubsubClientConfig builds the SDK retry configuration baked into every Publisher on the
// resulting client. Per OQ5 / OQ5a / OQ5b: time-bounded retry only (no MaxAttempts), SDK
// default broad retry-code set; envelope_id dedup in BQ handles redelivery cleanly.
func pubsubClientConfig() *gpubsub.ClientConfig {
	return &gpubsub.ClientConfig{
		TopicAdminCallOptions: &vkit.TopicAdminCallOptions{
			Publish: []gax.CallOption{
				gax.WithRetry(func() gax.Retryer {
					return gax.OnCodes(
						[]codes.Code{
							codes.Aborted,
							codes.Canceled,
							codes.Internal,
							codes.ResourceExhausted,
							codes.Unknown,
							codes.Unavailable,
							codes.DeadlineExceeded,
						},
						gax.Backoff{
							Initial:    100 * time.Millisecond,
							Max:        10 * time.Second,
							Multiplier: 2.0,
						},
					)
				}),
			},
		},
	}
}

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(logger)

	if err := run(logger); err != nil {
		slog.Error("ws-worker exited with error", "err", err)
		os.Exit(1)
	}
}

func run(logger *slog.Logger) error {
	cfg, err := parseConfig()
	if err != nil {
		return err
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	// Cloud Monitoring exporter.
	m, err := metrics.New(ctx, metrics.Options{
		ProjectID: cfg.ProjectID,
		Service:   "ws-worker",
		Disabled:  false,
		Interval:  60 * time.Second,
	})
	if err != nil {
		return fmt.Errorf("metrics: %w", err)
	}
	defer func() {
		if err := m.Shutdown(context.Background()); err != nil {
			logger.Warn("metrics shutdown", "err", err)
		}
	}()

	// Secrets
	loader, closeLoader, err := secrets.NewLoader(ctx)
	if err != nil {
		return fmt.Errorf("secrets loader: %w", err)
	}
	defer func() { _ = closeLoader() }()

	creds, err := loader.LoadKalshi(ctx, cfg.SecretName)
	if err != nil {
		return fmt.Errorf("load kalshi creds: %w", err)
	}
	priv, err := kalshi.ParsePrivateKeyPEM([]byte(creds.PrivateKeyPEM))
	if err != nil {
		return fmt.Errorf("parse kalshi private key: %w", err)
	}
	signer := &kalshi.Signer{KeyID: creds.APIKeyID, PrivateKey: priv}

	restClient := kalshi.NewClient(kalshi.Config{
		BaseURL: cfg.RESTURL,
		Signer:  signer,
		Now:     time.Now,
		RPS:     cfg.RESTRPS,
		Burst:   cfg.RESTBurst,
	})
	ws := kalshi.NewWS(kalshi.WSConfig{URL: cfg.WSURL, Signer: signer, Now: time.Now})

	psClient, err := gpubsub.NewClientWithConfig(ctx, cfg.ProjectID, pubsubClientConfig())
	if err != nil {
		return fmt.Errorf("pubsub client: %w", err)
	}
	defer func() { _ = psClient.Close() }()

	streams := []string{
		worker.StreamSnapshot1s,
		worker.StreamTrade,
		worker.StreamLifecycle,
		worker.StreamSettlement,
	}
	topics := make(map[string]psk.Topic, len(streams))
	for _, s := range streams {
		p := psClient.Publisher(cfg.topicName(s))
		p.PublishSettings.DelayThreshold = 50 * time.Millisecond
		p.PublishSettings.CountThreshold = 100
		topics[s] = psk.NewGCPTopic(p)
	}

	fanout := psk.NewFanout(topics, psk.FanoutConfig{
		OnSuccess: func(stream string, latency time.Duration) {
			m.PubsubPublishLatency.Record(context.Background(), float64(latency.Milliseconds()), "stream", stream)
		},
		OnFailure: func(stream string, _ error) {
			m.PubsubPublishFailures.Add(context.Background(), 1, "stream", stream)
		},
	})
	defer func() {
		if err := fanout.Stop(); err != nil {
			if errors.Is(err, psk.ErrDrainTimeout) {
				m.PubsubDrainUnflushed.Add(context.Background(), 1, "stream", "fanout")
			}
			logger.Warn("fanout stop", "err", err)
		}
	}()

	// w is late-bound: declared here so the bookkeeper's Publish closure can
	// route series-id lookups through Worker.SeriesForTicker, which reads the
	// live atomic.Pointer-backed series set (mutated by AddSeries/RemoveSeries).
	// Assigned by worker.New(...) below before w.Run starts the bookkeeper
	// goroutine; goroutine spawn establishes happens-before for the
	// Publish closure's read of w. Pre-fix the closure read the boot-time
	// cfg.Series snapshot and emitted empty series_id for dynamically-added
	// series (HOL-43).
	var w *worker.Worker

	bookkeeper := orderbook.NewBookkeeper(orderbook.BookkeeperConfig{
		Publish: func(ticker string, _ orderbook.Side, p orderbook.SnapshotPayload) error {
			inner, err := p.Marshal()
			if err != nil {
				return fmt.Errorf("snapshot payload marshal: %w", err)
			}
			seriesID := w.SeriesForTicker(ticker)
			now := time.Now().UTC()
			env := envelope.New(worker.StreamSnapshot1s, seriesID, now, inner).
				WithContract(ticker, ticker)
			body, err := env.Marshal()
			if err != nil {
				return fmt.Errorf("envelope marshal: %w", err)
			}
			if err := fanout.PublishStream(context.Background(), worker.StreamSnapshot1s, body); err != nil {
				m.PubsubPublishFailures.Add(context.Background(), 1, "stream", worker.StreamSnapshot1s)
				return err
			}
			m.BookkeeperSnapshotsEmitted.Add(context.Background(), 1, "reason", p.EmitReason(), "series", seriesID)
			return nil
		},
		OnRegistrySize: func(n int) {
			m.BookkeeperRegistrySize.Record(context.Background(), int64(n))
		},
	})

	var (
		desiredLoader  worker.DesiredSetLoader
		initialTickers = cfg.Series
	)
	if cfg.ControlBucket != "" {
		gcsClient, err := storage.NewClient(ctx)
		if err != nil {
			return fmt.Errorf("gcs client: %w", err)
		}
		// gcsClient must outlive worker.Run; the loader holds a reference.
		defer func() { _ = gcsClient.Close() }()

		loader := worker.NewGCSDesiredSetLoader(gcsClient, cfg.ControlBucket, cfg.ControlObject)
		tickers, gen, _, err := loader.Load(ctx, 0)
		switch {
		case errors.Is(err, worker.ErrAbsent):
			logger.Warn("series_desired.json absent; falling back to KALSHI_SERIES env",
				"bucket", cfg.ControlBucket, "object", cfg.ControlObject,
				"bootstrap_source", "env")
		case err != nil:
			return fmt.Errorf("initial desired-set load: %w", err)
		case len(tickers) == 0:
			logger.Warn("series_desired.json had empty tickers; falling back to KALSHI_SERIES env",
				"bootstrap_source", "env")
		default:
			initialTickers = tickers
			logger.Info("loaded initial desired set",
				"bootstrap_source", "gcs",
				"tickers", tickers,
				"generation", gen)
		}
		desiredLoader = loader
	} else {
		logger.Warn("KALSHI_CONTROL_BUCKET unset; running with static KALSHI_SERIES (DR mode)",
			"bootstrap_source", "env")
	}

	if len(initialTickers) == 0 {
		return errors.New("no series available from GCS or KALSHI_SERIES env; cannot start")
	}

	w = worker.New(worker.Deps{
		WS:                     ws,
		REST:                   restClient,
		Pub:                    fanout,
		Metrics:                m,
		Logger:                 logger,
		Series:                 initialTickers,
		OpenStatus:             cfg.OpenStatus,
		Bookkeeper:             bookkeeper,
		SweepInterval:          cfg.SweepInterval,
		SweepMissThreshold:     cfg.SweepMissThreshold,
		WatchdogEnabled:        cfg.WatchdogEnabled,
		WatchdogPingInterval:   cfg.WatchdogPingInterval,
		WatchdogPongDeadline:   cfg.WatchdogPongDeadline,
		DataStallEnabled:       cfg.DataStallEnabled,
		DataFrameDeadline:      cfg.DataFrameDeadline,
		PreserveBookkeeper:     cfg.PreserveBookkeeper,
		BackoffDataThreshold:   cfg.BackoffDataThreshold,
		MaxBackoff:             cfg.BackoffMax,
		LifecycleUnsub:         cfg.LifecycleUnsub,
		DesiredSetLoader:       desiredLoader, // nil iff cfg.ControlBucket == ""
		DesiredSetPollInterval: 60 * time.Second,
		DesiredSetPollJitter:   5 * time.Second,
	})

	logger.Info("ws-worker starting",
		"project", cfg.ProjectID,
		"series", initialTickers,
		"open_status", cfg.OpenStatus,
		"sweep_interval", cfg.SweepInterval,
		"sweep_miss_threshold", cfg.SweepMissThreshold,
		"watchdog_enabled", cfg.WatchdogEnabled,
		"watchdog_ping_interval", cfg.WatchdogPingInterval,
		"watchdog_pong_deadline", cfg.WatchdogPongDeadline,
		"data_stall_enabled", cfg.DataStallEnabled,
		"data_frame_deadline", cfg.DataFrameDeadline,
		"preserve_bookkeeper", cfg.PreserveBookkeeper,
		"backoff_data_threshold", cfg.BackoffDataThreshold,
		"backoff_max", cfg.BackoffMax,
		"lifecycle_unsub", cfg.LifecycleUnsub,
		"control_bucket", cfg.ControlBucket,
		"control_object", cfg.ControlObject,
		"desired_set_loader_enabled", desiredLoader != nil,
	)

	return w.Run(ctx)
}
