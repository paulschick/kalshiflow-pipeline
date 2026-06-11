// Package testutil contains shared helpers for hermetic tests inside this module. Helpers
// here are imported from *_test.go files in internal/{kalshi,pubsub,worker}; they live in a
// non-test package so the import works across package boundaries.
package testutil

import (
	"time"
)

// TB is the subset of testing.TB that WaitFor depends on. testing.T and testing.B both
// satisfy it; the local mockT in waitfor_test.go also satisfies it for test-the-tester
// coverage of the failure path.
type TB interface {
	Helper()
	Fatalf(format string, args ...interface{})
}

// WaitFor polls pred every 10ms until it returns true or the timeout elapses. On timeout,
// calls t.Fatalf with msg. Use this in *positive-assertion* tests where you are waiting for
// a known event to happen; for negative assertions ("expect zero events for X duration"),
// use a bounded time.Sleep instead — a poll cannot prove a negative.
func WaitFor(t TB, timeout time.Duration, msg string, pred func() bool) {
	t.Helper()
	const interval = 10 * time.Millisecond
	deadline := time.Now().Add(timeout)
	if pred() {
		return
	}
	tick := time.NewTicker(interval)
	defer tick.Stop()
	for time.Now().Before(deadline) {
		<-tick.C
		if pred() {
			return
		}
	}
	t.Fatalf("WaitFor timeout (%s): %s", timeout, msg)
}
