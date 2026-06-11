// Command kraken-ws-worker is the Kraken WebSocket ingest binary. HOL-48 slice:
// hardcoded 5-pair set, public WS only, gated behind the HOL-47 kill switch.
package main

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"cloud.google.com/go/bigquery"
	gpubsub "cloud.google.com/go/pubsub/v2"
	"cloud.google.com/go/storage"

	"github.com/paulschick/kalshiflow-pipeline/internal/envelope"
	"github.com/paulschick/kalshiflow-pipeline/internal/featureflag"
	"github.com/paulschick/kalshiflow-pipeline/internal/kraken/control"
	krob "github.com/paulschick/kalshiflow-pipeline/internal/kraken/orderbook"
	"github.com/paulschick/kalshiflow-pipeline/internal/kraken/rest"
	"github.com/paulschick/kalshiflow-pipeline/internal/kraken/symbol"
	"github.com/paulschick/kalshiflow-pipeline/internal/kraken/worker"
	"github.com/paulschick/kalshiflow-pipeline/internal/kraken/wsv2"
	"github.com/paulschick/kalshiflow-pipeline/internal/metrics"
	kob "github.com/paulschick/kalshiflow-pipeline/internal/orderbook"
	psk "github.com/paulschick/kalshiflow-pipeline/internal/pubsub"
)

const (
	streamSnapshot1s = "kraken_snapshot_1s"
	streamTrade      = "kraken_trade"
	topicSnapshot1s  = "kraken.snapshot_1s"
	topicTrade       = "kraken.trade"
)

type config struct {
	ProjectID     string
	Region        string
	ControlBucket string
	ControlObject string
	PairsBucket   string // GCS bucket for kraken_pairs.json; defaults to ControlBucket
	PairsObject   string // GCS object key for kraken_pairs.json
	PairsFallback []string
	WSURL         string
	RESTURL       string
	SilentStall   time.Duration
}

func parseConfig() (config, error) {
	c := config{
		ProjectID:     os.Getenv("KRAKEN_PROJECT_ID"),
		Region:        cmp.Or(os.Getenv("KRAKEN_REGION"), "us-east4"),
		ControlBucket: os.Getenv("KRAKEN_FLAG_BUCKET"),
		ControlObject: cmp.Or(os.Getenv("KRAKEN_FLAG_OBJECT"), "control/kraken_enabled.json"),
		PairsObject:   cmp.Or(os.Getenv("KRAKEN_PAIRS_OBJECT"), "control/kraken_pairs.json"),
		WSURL:         cmp.Or(os.Getenv("KRAKEN_WS_URL"), "wss://ws.kraken.com/v2"),
		RESTURL:       cmp.Or(os.Getenv("KRAKEN_REST_URL"), "https://api.kraken.com"),
		SilentStall:   60 * time.Second,
	}
	// PairsBucket defaults to ControlBucket so production env-var surface is
	// minimal (one bucket for all control objects).
	c.PairsBucket = cmp.Or(os.Getenv("KRAKEN_PAIRS_BUCKET"), c.ControlBucket)
	if csv := os.Getenv("KRAKEN_PAIRS"); csv != "" {
		for _, p := range strings.Split(csv, ",") {
			if p = strings.TrimSpace(p); p != "" {
				c.PairsFallback = append(c.PairsFallback, p)
			}
		}
	}
	if c.ProjectID == "" {
		return c, errors.New("KRAKEN_PROJECT_ID is required")
	}
	if c.ControlBucket == "" {
		return c, errors.New("KRAKEN_FLAG_BUCKET is required")
	}
	if v := os.Getenv("KRAKEN_WS_SILENT_STALL_THRESHOLD"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			return c, fmt.Errorf("KRAKEN_WS_SILENT_STALL_THRESHOLD: %w", err)
		}
		c.SilentStall = d
	}
	return c, nil
}

// bqSubscriptionLogger satisfies worker.SubscriptionLogger by inserting a row
// into kraken_raw.subscription_log on every per-pair AddPair rejection.
type bqSubscriptionLogger struct {
	inserter *bigquery.Inserter
	now      func() time.Time
}

type subscriptionLogRow struct {
	Ts     time.Time `bigquery:"ts"`
	Pair   string    `bigquery:"pair"`
	Action string    `bigquery:"action"`
	Reason string    `bigquery:"reason"`
	Actor  string    `bigquery:"actor"`
}

func (b *bqSubscriptionLogger) LogRejection(ctx context.Context, pair, reason, actor string) error {
	return b.inserter.Put(ctx, &subscriptionLogRow{
		Ts:     b.now().UTC(),
		Pair:   pair,
		Action: "reject",
		Reason: reason,
		Actor:  actor,
	})
}

func main() {
	if err := run(); err != nil {
		slog.Error("kraken_ws_worker_exit", "err", err)
		os.Exit(1)
	}
}

func run() error {
	handler := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo})
	slog.SetDefault(slog.New(handler))
	cfg, err := parseConfig()
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	gcs, err := storage.NewClient(ctx)
	if err != nil {
		return fmt.Errorf("storage.NewClient: %w", err)
	}
	defer func() { _ = gcs.Close() }()

	flag := featureflag.NewGCSKrakenFlag(gcs, cfg.ControlBucket, cfg.ControlObject)
	if err := flag.LoadOnce(ctx); err != nil && !errors.Is(err, featureflag.ErrAbsent) {
		slog.Warn("kraken_flag_load_once_err; using default-on", "err", err)
	}
	if !flag.Enabled() {
		slog.Info("kraken_flag_disabled_at_boot; exit 0")
		return nil
	}
	go flag.Start(ctx)

	m, err := metrics.New(ctx, metrics.Options{Service: "kraken-ws-worker", ProjectID: cfg.ProjectID})
	if err != nil {
		return fmt.Errorf("metrics.New: %w", err)
	}
	defer func() { _ = m.Shutdown(context.Background()) }()

	psClient, err := gpubsub.NewClient(ctx, cfg.ProjectID)
	if err != nil {
		return fmt.Errorf("pubsub.NewClient: %w", err)
	}
	defer func() { _ = psClient.Close() }()

	topics := map[string]psk.Topic{
		streamSnapshot1s: psk.NewGCPTopic(psClient.Publisher(topicSnapshot1s)),
		streamTrade:      psk.NewGCPTopic(psClient.Publisher(topicTrade)),
	}
	fanout := psk.NewFanout(topics, psk.FanoutConfig{
		OnSuccess: func(stream string, latency time.Duration) {
			m.KrakenPubsubPublishLatencyMs.Record(context.Background(), float64(latency.Milliseconds()), "stream", stream)
		},
		OnFailure: func(stream string, _ error) {
			m.KrakenPubsubPublishFailures.Add(context.Background(), 1, "stream", stream)
		},
	})
	defer func() {
		if err := fanout.Stop(); err != nil {
			if errors.Is(err, psk.ErrDrainTimeout) {
				m.KrakenPubsubDrainUnflushed.Add(context.Background(), 1, "stream", "fanout")
			}
			slog.Warn("kraken_fanout_stop", "err", err)
		}
	}()

	// BQ client for kraken_raw.subscription_log writes on AddPair rejections.
	// Constructed once; lifetime tied to the process.
	bq, err := bigquery.NewClient(ctx, cfg.ProjectID)
	if err != nil {
		return fmt.Errorf("bigquery.NewClient: %w", err)
	}
	defer func() { _ = bq.Close() }()
	subLogger := &bqSubscriptionLogger{
		inserter: bq.Dataset("kraken_raw").Table("subscription_log").Inserter(),
		now:      time.Now,
	}

	// Synchronous boot-load of the desired pair set from
	// gs://<PairsBucket>/<PairsObject>. ErrAbsent → DR fallback to the
	// KRAKEN_PAIRS env CSV; empty fallback → fatal (operator-actionable).
	pairsLoader := control.NewGCSDesiredSetLoader(gcs, cfg.PairsBucket, cfg.PairsObject)
	bootCtx, bootCancel := context.WithTimeout(ctx, 30*time.Second)
	loadedPairs, loadedGen, _, loadErr := pairsLoader.Load(bootCtx, 0)
	bootCancel()
	var pairs []string
	switch {
	case errors.Is(loadErr, control.ErrAbsent):
		pairs = cfg.PairsFallback
		slog.Warn("kraken_pairs_loaded",
			"bootstrap_source", "env",
			"count", len(pairs))
	case loadErr != nil:
		return fmt.Errorf("kraken_pairs_load: %w", loadErr)
	default:
		pairs = loadedPairs
		slog.Info("kraken_pairs_loaded",
			"bootstrap_source", "gcs",
			"count", len(pairs),
			"generation", loadedGen)
	}
	if len(pairs) == 0 {
		return errors.New("kraken_pairs_empty: no pairs in GCS manifest and KRAKEN_PAIRS fallback is empty")
	}

	// Late-bind bk so the Publish closure can read bk.SnapshotSeq(ticker).
	// bk is assigned before bk.Run starts, which establishes happens-before
	// for the closure's first call. Mirrors cmd/ws-worker/main.go.
	var bk *krob.Bookkeeper
	bk = krob.New(krob.Config{
		Publish: func(ticker string, _ krob.Side, p kob.SnapshotPayload) error {
			inner, mErr := p.Marshal()
			if mErr != nil {
				return fmt.Errorf("snapshot payload marshal: %w", mErr)
			}
			asset := symbol.KrakenPairToAsset[ticker]
			env := envelope.New(streamSnapshot1s, asset, time.Now().UTC(), inner).
				WithContract(ticker, ticker).
				WithSnapshotSeq(bk.SnapshotSeq(ticker))
			body, eErr := env.Marshal()
			if eErr != nil {
				return fmt.Errorf("envelope marshal: %w", eErr)
			}
			if pErr := fanout.PublishStream(context.Background(), streamSnapshot1s, body); pErr != nil {
				m.KrakenPubsubPublishFailures.Add(context.Background(), 1, "stream", streamSnapshot1s)
				return pErr
			}
			m.KrakenBookkeeperSnapshotsEmitted.Add(context.Background(), 1, "pair", ticker, "emit_reason", p.EmitReason())
			return nil
		},
		ChangeTickInterval: 1 * time.Second,
		HeartbeatInterval:  300 * time.Second,
		PairScale:          map[string]krob.PairScale{},
	})
	go bk.Run(ctx)

	w := worker.New(worker.Config{
		Flag:            flag,
		REST:            rest.New(nil, cfg.RESTURL),
		Open:            wsv2.Open,
		WSURL:           cfg.WSURL,
		Bookkeeper:      bk,
		Pairs:           pairs,
		SilentStall:     cfg.SilentStall,
		DesiredSet:      pairsLoader,
		SubscriptionLog: subLogger,
		Metrics: worker.Metrics{
			WSMessagesReceived:    m.KrakenWSMessagesReceived,
			WSReconnects:          m.KrakenWSReconnects,
			SilentStallReconnects: m.KrakenWSSilentStallReconnects,
			SubscribeRejected:     m.KrakenWSSubscribeRejected,
			ChecksumMismatches:    m.KrakenWSChecksumMismatches,
			PerSymbolResubs:       m.KrakenWSPerSymbolResubs,
			ResubDropped:          m.KrakenWSResubDropped,
			FlagEnabled:           m.KrakenFlagEnabled,
		},
		PublishTrade: func(ctx context.Context, body []byte) error {
			return fanout.PublishStream(ctx, streamTrade, body)
		},
	})

	if err := w.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		return err
	}
	return nil
}
