// Package secrets carries the Kraken-side secrets-loader interface. HOL-48
// uses only public WS; the stub satisfies the interface so a future on-prem
// adapter can be wired without refactoring callers.
package secrets

import (
	"context"
	"errors"
)

// Loader is the seam future private-channel auth will fill.
type Loader interface {
	KrakenAPIKey(ctx context.Context) (apiKey string, apiSecret []byte, err error)
}

// ErrNotConfigured is returned by the stub. Callers must not treat this as
// fatal in HOL-48 — public WS does not need credentials.
var ErrNotConfigured = errors.New("kraken/secrets: no private-channel credentials configured")

type stub struct{}

// NewStub returns a Loader that always reports ErrNotConfigured.
func NewStub() Loader { return stub{} }

func (stub) KrakenAPIKey(_ context.Context) (string, []byte, error) {
	return "", nil, ErrNotConfigured
}
