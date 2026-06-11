// Package secrets loads workload credentials from Google Secret Manager.
package secrets

import (
	"context"
	"encoding/json"
	"fmt"

	secretmanager "cloud.google.com/go/secretmanager/apiv1"
	"cloud.google.com/go/secretmanager/apiv1/secretmanagerpb"
)

// KalshiCreds is the JSON shape stored in the kalshi-creds secret. Two fields only — the
// legacy `environment` discriminator is gone (slice 2.1a targets Kalshi prod directly per
// OQ4; WS URL is set via env var at cmd layer).
type KalshiCreds struct {
	APIKeyID      string `json:"api_key_id"`
	PrivateKeyPEM string `json:"private_key_pem"`
}

// Accessor abstracts Secret Manager so tests can fake it.
type Accessor interface {
	Access(ctx context.Context, name string) ([]byte, error)
}

// Loader is the high-level wrapper.
type Loader struct {
	Accessor Accessor
}

// NewLoader wires up a real Secret Manager client. Caller closes via the returned func.
func NewLoader(ctx context.Context) (*Loader, func() error, error) {
	client, err := secretmanager.NewClient(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("secretmanager.NewClient: %w", err)
	}
	return &Loader{Accessor: &smAccessor{client: client}}, client.Close, nil
}

// LoadKalshi fetches the latest version of the kalshi-creds secret and parses it.
//
// secretName is the full resource name, e.g. projects/<project>/secrets/kalshi-creds.
func (l *Loader) LoadKalshi(ctx context.Context, secretName string) (KalshiCreds, error) {
	body, err := l.Accessor.Access(ctx, secretName)
	if err != nil {
		return KalshiCreds{}, fmt.Errorf("AccessSecret: %w", err)
	}
	return decodeKalshi(body)
}

func decodeKalshi(body []byte) (KalshiCreds, error) {
	var c KalshiCreds
	if err := json.Unmarshal(body, &c); err != nil {
		return KalshiCreds{}, fmt.Errorf("decode kalshi creds: %w", err)
	}
	if c.APIKeyID == "" {
		return KalshiCreds{}, fmt.Errorf("kalshi creds: empty api_key_id")
	}
	if c.PrivateKeyPEM == "" {
		return KalshiCreds{}, fmt.Errorf("kalshi creds: empty private_key_pem")
	}
	return c, nil
}

type smAccessor struct {
	client *secretmanager.Client
}

func (a *smAccessor) Access(ctx context.Context, name string) ([]byte, error) {
	resp, err := a.client.AccessSecretVersion(ctx, &secretmanagerpb.AccessSecretVersionRequest{
		Name: name + "/versions/latest",
	})
	if err != nil {
		return nil, err
	}
	return resp.Payload.Data, nil
}
