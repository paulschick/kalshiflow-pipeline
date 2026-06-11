// Package worker carries the Kraken WS supervisor. metrics.go isolates the
// dependency surface on internal/metrics so worker tests can wire just the
// instruments they need.
package worker

import "github.com/paulschick/kalshiflow-pipeline/internal/metrics"

// Metrics is the subset of *internal/metrics.Metrics the worker reads.
// Holds wrapper types (not raw OTel) so labels are alternating key/value pairs
// per the project convention.
type Metrics struct {
	WSMessagesReceived    metrics.Counter
	WSReconnects          metrics.Counter
	SilentStallReconnects metrics.Counter
	SubscribeRejected     metrics.Counter
	ChecksumMismatches    metrics.Counter
	PerSymbolResubs       metrics.Counter
	ResubDropped          metrics.Counter
	FlagEnabled           metrics.Gauge
}
