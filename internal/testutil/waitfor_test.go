package testutil

import (
	"sync/atomic"
	"testing"
	"time"
)

func TestWaitFor_PassesWhenPredicateBecomesTrue(t *testing.T) {
	t.Parallel()
	var flipped atomic.Bool
	go func() {
		time.Sleep(20 * time.Millisecond)
		flipped.Store(true)
	}()
	WaitFor(t, 500*time.Millisecond, "flipped should become true", flipped.Load)
}

func TestWaitFor_FailsWhenPredicateStaysFalse(t *testing.T) {
	t.Parallel()
	mock := &mockT{}
	WaitFor(mock, 30*time.Millisecond, "never true", func() bool { return false })
	if !mock.failed.Load() {
		t.Fatal("expected WaitFor to call t.Fatalf when timeout elapsed")
	}
	if mock.lastMsg == "" {
		t.Fatal("expected WaitFor to surface a message on timeout")
	}
}

// mockT captures Fatalf calls so we can verify WaitFor's failure path without exiting the
// outer test. Implements the subset of testing.TB that WaitFor depends on (Helper, Fatalf).
type mockT struct {
	failed  atomic.Bool
	lastMsg string
}

func (m *mockT) Helper() {}
func (m *mockT) Fatalf(format string, _ ...interface{}) {
	m.failed.Store(true)
	m.lastMsg = format
}
