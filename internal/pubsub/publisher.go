// Package pubsub wraps Google Cloud Pub/Sub publishing with a strict-async dispatcher.
// SDK-level retry is configured at client construction time (cmd/ws-worker/main.go) via
// pubsub.NewClientWithConfig + vkit.TopicAdminCallOptions; the Publisher itself has
// no app-level retry loop.
package pubsub

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	gpubsub "cloud.google.com/go/pubsub/v2"
)

// ErrPublisherClosed is returned from Publish after Stop has run.
var ErrPublisherClosed = errors.New("pubsub: publisher closed")

// ErrDrainTimeout is returned from Stop when the dispatcher fails to drain within
// DrainTimeout. The cmd layer should treat this as a signal to increment
// pubsub_drain_unflushed_total{stream}
var ErrDrainTimeout = errors.New("pubsub: drain timeout exceeded")

// Result abstracts *pubsub.PublishResult for testability. *pubsub.PublishResult satisfies
// this interface naturally — its Get method has the same signature.
type Result interface {
	Get(ctx context.Context) (serverID string, err error)
}

// Topic is the minimum surface a Publisher needs.
type Topic interface {
	// Publish hands data to the SDK and returns a Result that the dispatcher resolves
	// asynchronously. Publish itself does NOT block on ack.
	Publish(ctx context.Context, data []byte, attrs map[string]string) Result
	// Stop flushes outstanding publishes and stops goroutines created by the SDK.
	Stop()
}

// PublisherConfig configures the dispatcher and observability hooks.
type PublisherConfig struct {
	Topic Topic

	// ResultBufferSize is the dispatcher's backing channel capacity. Defaults to 1024.
	// Matches the SDK's default FlowControlSettings.MaxOutstandingMessages = 1000 with a
	// tiny headroom margin. Slice 2.4 soak verifies; tighten/loosen via this knob, not in
	// the type signature.
	ResultBufferSize int

	// DrainTimeout bounds Stop's wait for the dispatcher to exit after topic.Stop returns.
	// Defaults to 8s. If exceeded, Stop cancels the drain ctx (unblocks any in-flight
	// Result.Get) and returns ErrDrainTimeout.
	DrainTimeout time.Duration

	// OnSuccess fires once per successfully-published message, with the wall-clock latency
	// from Publish-call to Result-resolved. May be nil.
	OnSuccess func(latency time.Duration)

	// OnFailure fires once per failed publish, with the SDK error. May be nil.
	OnFailure func(err error)
}

// Publisher is the strict-async dispatcher.
type Publisher struct {
	cfg PublisherConfig

	results chan publishInFlight

	// drainCtx ties Result.Get calls to the publisher's lifetime, NOT the per-publish ctx.
	// Cancelled in Stop after DrainTimeout to unblock any stuck Get.
	drainCtx    context.Context
	drainCancel context.CancelFunc

	// sendMu guards both the send into p.results (Publish) and the close of p.results
	// (Stop). Prevents the send-on-closed-channel data race that -race detects even when
	// a panic-recover is in place: the race detector instruments the channel operation
	// itself, not just the resulting panic.
	sendMu   sync.Mutex
	closed   atomic.Bool
	wg       sync.WaitGroup
	stopOnce sync.Once
	stopErr  error
}

type publishInFlight struct {
	res    Result
	sentAt time.Time
}

// NewPublisher applies sensible defaults and starts the dispatcher goroutine.
func NewPublisher(cfg PublisherConfig) *Publisher {
	if cfg.ResultBufferSize <= 0 {
		cfg.ResultBufferSize = 1024
	}
	if cfg.DrainTimeout <= 0 {
		cfg.DrainTimeout = 8 * time.Second
	}
	ctx, cancel := context.WithCancel(context.Background())
	p := &Publisher{
		cfg:         cfg,
		results:     make(chan publishInFlight, cfg.ResultBufferSize),
		drainCtx:    ctx,
		drainCancel: cancel,
	}
	p.wg.Add(1)
	go p.dispatch()
	return p
}

// Publish enqueues a fresh publish via the configured Topic. Strict-async:
// returns immediately after Topic.Publish returns its Result handle and the
// dispatcher has been notified.
//
// sendMu eliminates the TOCTOU race between the channel send here and the
// close(p.results) in Stop. The atomic closed.Load fast-path avoids lock
// contention on the hot path; sendMu is only acquired after we know we need
// to send.
func (p *Publisher) Publish(ctx context.Context, data []byte, attrs map[string]string) error {
	if p.closed.Load() {
		return ErrPublisherClosed
	}
	res := p.cfg.Topic.Publish(ctx, data, attrs)
	sent := time.Now()
	p.sendMu.Lock()
	defer p.sendMu.Unlock()
	if p.closed.Load() {
		return ErrPublisherClosed
	}
	select {
	case p.results <- publishInFlight{res: res, sentAt: sent}:
		return nil
	case <-ctx.Done():
		// SDK has already accepted the publish; we lose the result-tracking. The SDK will
		// still attempt delivery, but OnSuccess/OnFailure won't fire for this message. The
		// per-publish ctx going Done before we can enqueue is rare in practice (the result
		// channel buffers 1024 slots) and signals upstream cancellation, which the worker
		// loop handles separately.
		return ctx.Err()
	}
}

// Stop flushes outstanding publishes and waits for the dispatcher to exit, bounded by
// DrainTimeout.
func (p *Publisher) Stop() error {
	p.stopOnce.Do(func() {
		// Hold sendMu while marking closed and closing the channel so no Publish
		// goroutine can race a send against the close.
		p.sendMu.Lock()
		p.closed.Store(true)
		close(p.results)
		p.sendMu.Unlock()
		p.cfg.Topic.Stop()
		done := make(chan struct{})
		go func() {
			p.wg.Wait()
			close(done)
		}()
		timer := time.NewTimer(p.cfg.DrainTimeout)
		defer timer.Stop()
		select {
		case <-done:
			p.drainCancel()
			p.stopErr = nil
		case <-timer.C:
			p.drainCancel()
			<-done
			p.stopErr = ErrDrainTimeout
		}
	})
	return p.stopErr
}

func (p *Publisher) dispatch() {
	defer p.wg.Done()
	for inFlight := range p.results {
		_, err := inFlight.res.Get(p.drainCtx)
		if err != nil {
			if p.cfg.OnFailure != nil {
				p.cfg.OnFailure(err)
			}
			continue
		}
		if p.cfg.OnSuccess != nil {
			p.cfg.OnSuccess(time.Since(inFlight.sentAt))
		}
	}
}

// gcpTopic adapts *pubsub.Publisher to the Topic interface; *pubsub.PublishResult
// already satisfies Result.
type gcpTopic struct {
	t *gpubsub.Publisher
}

// NewGCPTopic wraps a real Pub/Sub Publisher.
func NewGCPTopic(t *gpubsub.Publisher) Topic { return &gcpTopic{t: t} }

func (g *gcpTopic) Publish(ctx context.Context, data []byte, attrs map[string]string) Result {
	return g.t.Publish(ctx, &gpubsub.Message{Data: data, Attributes: attrs})
}

func (g *gcpTopic) Stop() { g.t.Stop() }

// FanoutConfig configures a multi-stream publisher. Mirrors PublisherConfig but
// per-stream: callbacks receive the stream name so metrics can be labelled.
type FanoutConfig struct {
	// ResultBufferSize and DrainTimeout are forwarded to each underlying Publisher;
	// zero values fall back to PublisherConfig defaults (1024 / 8s).
	ResultBufferSize int
	DrainTimeout     time.Duration

	// OnSuccess fires once per acked publish, labelled with the stream key.
	OnSuccess func(stream string, latency time.Duration)
	// OnFailure fires once per failed publish (or drained-on-timeout), labelled with the stream key.
	OnFailure func(stream string, err error)
}

// Fanout holds a Publisher per stream key (matching Plan 1 stream names) and
// dispatches PublishStream calls to the right one.
type Fanout struct {
	pubs map[string]*Publisher
}

// NewFanout constructs a Fanout. Each entry in topics is wrapped in its own
// Publisher; OnSuccess/OnFailure receive the stream name so the caller can
// label per-stream metrics.
func NewFanout(topics map[string]Topic, cfg FanoutConfig) *Fanout {
	f := &Fanout{pubs: make(map[string]*Publisher, len(topics))}
	for stream, topic := range topics {
		stream := stream // capture for closure (defensive even on Go 1.22+)
		f.pubs[stream] = NewPublisher(PublisherConfig{
			Topic:            topic,
			ResultBufferSize: cfg.ResultBufferSize,
			DrainTimeout:     cfg.DrainTimeout,
			OnSuccess: func(latency time.Duration) {
				if cfg.OnSuccess != nil {
					cfg.OnSuccess(stream, latency)
				}
			},
			OnFailure: func(err error) {
				if cfg.OnFailure != nil {
					cfg.OnFailure(stream, err)
				}
			},
		})
	}
	return f
}

// PublishStream sends body to the publisher registered for stream.
func (f *Fanout) PublishStream(ctx context.Context, stream string, body []byte) error {
	p, ok := f.pubs[stream]
	if !ok {
		return fmt.Errorf("fanout: unknown stream %q", stream)
	}
	return p.Publish(ctx, body, nil)
}

// Stop drains every underlying Publisher in parallel. Sequential drain would
// add up to N × DrainTimeout (5 × 8s = 40s for slice 2.2's five streams) and
// risk blowing past Cloud Run's SIGTERM grace; parallel drain caps wall time
// at max(DrainTimeout). Returns the first error observed (typically
// ErrDrainTimeout) so the caller can increment a drain-unflushed metric.
func (f *Fanout) Stop() error {
	errs := make(chan error, len(f.pubs))
	var wg sync.WaitGroup
	for _, p := range f.pubs {
		wg.Add(1)
		go func(p *Publisher) {
			defer wg.Done()
			errs <- p.Stop()
		}(p)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			return err
		}
	}
	return nil
}
