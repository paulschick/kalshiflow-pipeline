package pubsub

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/paulschick/kalshiflow-pipeline/internal/testutil"
)

// fakeResult resolves Get(ctx) by returning the configured err after a small delay (to
// simulate the SDK's async behavior).
type fakeResult struct {
	err   error
	delay time.Duration
}

func (f *fakeResult) Get(ctx context.Context) (string, error) {
	if f.delay > 0 {
		select {
		case <-time.After(f.delay):
		case <-ctx.Done():
			return "", ctx.Err()
		}
	}
	return "fake-id", f.err
}

// fakeTopic records every Publish call and returns a configurable Result. publishErr (if
// non-nil) is returned from Result.Get; pre-Publish errors are not modelled here. Fields
// are atomic / immutable-after-construction so the fake is safe for the dispatcher
// goroutine to call concurrently without a mutex.
type fakeTopic struct {
	publishes   atomic.Int64
	stopped     atomic.Bool
	publishErr  error
	resultDelay time.Duration
}

func (t *fakeTopic) Publish(_ context.Context, _ []byte, _ map[string]string) Result {
	t.publishes.Add(1)
	return &fakeResult{err: t.publishErr, delay: t.resultDelay}
}
func (t *fakeTopic) Stop() { t.stopped.Store(true) }

func TestPublisher_FiresOnSuccess(t *testing.T) {
	t.Parallel()
	ft := &fakeTopic{}
	var ok atomic.Int64
	p := NewPublisher(PublisherConfig{
		Topic:     ft,
		OnSuccess: func(time.Duration) { ok.Add(1) },
	})
	defer p.Stop() //nolint:errcheck

	if err := p.Publish(context.Background(), []byte("x"), nil); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	testutil.WaitFor(t, time.Second, "OnSuccess fires", func() bool { return ok.Load() == 1 })
}

func TestPublisher_FiresOnFailure(t *testing.T) {
	t.Parallel()
	ft := &fakeTopic{publishErr: errors.New("transient")}
	var fails atomic.Int64
	p := NewPublisher(PublisherConfig{
		Topic:     ft,
		OnFailure: func(error) { fails.Add(1) },
	})
	defer p.Stop() //nolint:errcheck

	if err := p.Publish(context.Background(), []byte("x"), nil); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	testutil.WaitFor(t, time.Second, "OnFailure fires", func() bool { return fails.Load() == 1 })
}

func TestPublisher_Stop_DrainsInFlightResults(t *testing.T) {
	t.Parallel()
	ft := &fakeTopic{resultDelay: 20 * time.Millisecond}
	var ok atomic.Int64
	p := NewPublisher(PublisherConfig{
		Topic:     ft,
		OnSuccess: func(time.Duration) { ok.Add(1) },
	})

	for i := 0; i < 5; i++ {
		if err := p.Publish(context.Background(), []byte("x"), nil); err != nil {
			t.Fatalf("Publish: %v", err)
		}
	}
	if err := p.Stop(); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if ok.Load() != 5 {
		t.Errorf("OnSuccess fired %d times; want 5 (Stop must drain all enqueued results)", ok.Load())
	}
	if !ft.stopped.Load() {
		t.Errorf("topic.Stop not called from Publisher.Stop")
	}
}

func TestPublisher_Stop_IsIdempotent(t *testing.T) {
	t.Parallel()
	ft := &fakeTopic{}
	p := NewPublisher(PublisherConfig{Topic: ft})
	if err := p.Stop(); err != nil {
		t.Fatalf("first Stop: %v", err)
	}
	// Second Stop must not panic, must not block.
	if err := p.Stop(); err != nil {
		t.Errorf("second Stop: %v", err)
	}
}

func TestPublisher_Publish_AfterStop_ReturnsError(t *testing.T) {
	t.Parallel()
	ft := &fakeTopic{}
	p := NewPublisher(PublisherConfig{Topic: ft})
	_ = p.Stop()

	if err := p.Publish(context.Background(), []byte("x"), nil); err == nil {
		t.Errorf("Publish after Stop returned nil; want ErrPublisherClosed")
	} else if !errors.Is(err, ErrPublisherClosed) {
		t.Errorf("Publish after Stop returned %v; want ErrPublisherClosed", err)
	}
}

func TestPublisher_Stop_DrainTimeout_ReturnsError(t *testing.T) {
	t.Parallel()
	// The fake's per-result delay (200ms) is much longer than DrainTimeout (50ms). Stop
	// should give up after 50ms, cancel drainCtx, and return ErrDrainTimeout. The pending
	// Result.Get calls receive a cancelled drainCtx and return; the dispatcher exits.
	ft := &fakeTopic{resultDelay: 200 * time.Millisecond}
	p := NewPublisher(PublisherConfig{
		Topic:        ft,
		DrainTimeout: 50 * time.Millisecond,
	})
	_ = p.Publish(context.Background(), []byte("x"), nil)

	err := p.Stop()
	if !errors.Is(err, ErrDrainTimeout) {
		t.Errorf("Stop after exceeded drain budget returned %v; want ErrDrainTimeout", err)
	}
}

func TestPublisher_Publish_RaceWithStop(t *testing.T) {
	t.Parallel()
	ft := &fakeTopic{}
	p := NewPublisher(PublisherConfig{Topic: ft})

	const goroutines = 20
	const perGoroutine = 50
	var wg sync.WaitGroup
	var ready sync.WaitGroup
	start := make(chan struct{})

	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		ready.Add(1)
		go func() {
			defer wg.Done()
			ready.Done()
			<-start
			for j := 0; j < perGoroutine; j++ {
				err := p.Publish(context.Background(), []byte("x"), nil)
				if err != nil && !errors.Is(err, ErrPublisherClosed) && !errors.Is(err, context.Canceled) {
					t.Errorf("unexpected publish error: %v", err)
					return
				}
			}
		}()
	}

	ready.Wait()
	close(start)
	// Race Stop against the in-flight Publish loops.
	if err := p.Stop(); err != nil && !errors.Is(err, ErrDrainTimeout) {
		t.Errorf("Stop: unexpected error %v", err)
	}
	wg.Wait()
}

func TestFanout_PublishStream_DispatchesByKey(t *testing.T) {
	t.Parallel()
	ftOrderbook := &fakeTopic{}
	ftTrade := &fakeTopic{}

	f := NewFanout(map[string]Topic{
		"orderbook": ftOrderbook,
		"trade":     ftTrade,
	}, FanoutConfig{})
	defer f.Stop() //nolint:errcheck

	if err := f.PublishStream(context.Background(), "orderbook", []byte("ob")); err != nil {
		t.Fatalf("orderbook: %v", err)
	}
	if err := f.PublishStream(context.Background(), "trade", []byte("tr")); err != nil {
		t.Fatalf("trade: %v", err)
	}

	if ftOrderbook.publishes.Load() != 1 || ftTrade.publishes.Load() != 1 {
		t.Errorf("orderbook=%d trade=%d; both want 1",
			ftOrderbook.publishes.Load(), ftTrade.publishes.Load())
	}
}

func TestFanout_PublishStream_RejectsUnknownStream(t *testing.T) {
	t.Parallel()
	f := NewFanout(map[string]Topic{"orderbook": &fakeTopic{}}, FanoutConfig{})
	defer f.Stop() //nolint:errcheck

	err := f.PublishStream(context.Background(), "nope", []byte("x"))
	if err == nil {
		t.Fatalf("expected error for unknown stream")
	}
}

func TestFanout_OnFailureFiresPerStream(t *testing.T) {
	t.Parallel()
	ft := &fakeTopic{publishErr: errors.New("boom")}

	var failures atomic.Int64
	var sawStream atomic.Value // string
	sawStream.Store("")

	f := NewFanout(map[string]Topic{"trade": ft}, FanoutConfig{
		OnFailure: func(stream string, _ error) {
			failures.Add(1)
			sawStream.Store(stream)
		},
	})
	defer f.Stop() //nolint:errcheck

	if err := f.PublishStream(context.Background(), "trade", []byte("x")); err != nil {
		t.Fatalf("publish: %v", err)
	}
	testutil.WaitFor(t, time.Second, "OnFailure fires once with stream label", func() bool {
		return failures.Load() == 1 && sawStream.Load() == "trade"
	})
}

func TestFanout_OnSuccessFiresPerStream(t *testing.T) {
	t.Parallel()
	ft := &fakeTopic{}

	var hits atomic.Int64
	var sawStream atomic.Value // string
	sawStream.Store("")

	f := NewFanout(map[string]Topic{"orderbook": ft}, FanoutConfig{
		OnSuccess: func(stream string, _ time.Duration) {
			hits.Add(1)
			sawStream.Store(stream)
		},
	})
	defer f.Stop() //nolint:errcheck

	if err := f.PublishStream(context.Background(), "orderbook", []byte("x")); err != nil {
		t.Fatalf("publish: %v", err)
	}
	testutil.WaitFor(t, time.Second, "OnSuccess fires once with stream label", func() bool {
		return hits.Load() == 1 && sawStream.Load() == "orderbook"
	})
}
