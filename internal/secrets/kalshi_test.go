package secrets

import (
	"context"
	"errors"
	"testing"
)

type fakeSM struct {
	body []byte
	err  error
}

func (f *fakeSM) Access(_ context.Context, _ string) ([]byte, error) {
	return f.body, f.err
}

func TestDecodeKalshi_Valid(t *testing.T) {
	t.Parallel()
	src := []byte(`{"api_key_id":"k","private_key_pem":"-----BEGIN PRIVATE KEY-----\nA\n-----END PRIVATE KEY-----\n"}`)
	creds, err := decodeKalshi(src)
	if err != nil {
		t.Fatalf("decodeKalshi: %v", err)
	}
	if creds.APIKeyID != "k" {
		t.Errorf("api_key_id = %q; want k", creds.APIKeyID)
	}
	if creds.PrivateKeyPEM == "" {
		t.Errorf("private_key_pem empty")
	}
}

func TestDecodeKalshi_RejectsEmptyKeyID(t *testing.T) {
	t.Parallel()
	src := []byte(`{"api_key_id":"","private_key_pem":"x"}`)
	if _, err := decodeKalshi(src); err == nil {
		t.Errorf("expected error for empty api_key_id")
	}
}

func TestDecodeKalshi_RejectsEmptyPEM(t *testing.T) {
	t.Parallel()
	src := []byte(`{"api_key_id":"k","private_key_pem":""}`)
	if _, err := decodeKalshi(src); err == nil {
		t.Errorf("expected error for empty private_key_pem")
	}
}

func TestDecodeKalshi_TolerantOfExtraFields(t *testing.T) {
	t.Parallel()
	// If a secret blob accidentally retains the legacy "environment" field (e.g., during a
	// migration), the loader should ignore it rather than fail. Forward-compat with future
	// fields too.
	src := []byte(`{"api_key_id":"k","private_key_pem":"x","environment":"legacy","note":"ignore"}`)
	creds, err := decodeKalshi(src)
	if err != nil {
		t.Fatalf("decodeKalshi rejected extra fields: %v", err)
	}
	if creds.APIKeyID != "k" {
		t.Errorf("api_key_id = %q", creds.APIKeyID)
	}
}

func TestLoader_LoadKalshi_Roundtrips(t *testing.T) {
	t.Parallel()
	body := []byte(`{"api_key_id":"k","private_key_pem":"x"}`)
	loader := &Loader{Accessor: &fakeSM{body: body}}

	got, err := loader.LoadKalshi(context.Background(), "projects/p/secrets/kalshi-creds")
	if err != nil {
		t.Fatalf("LoadKalshi: %v", err)
	}
	if got.APIKeyID != "k" {
		t.Errorf("api_key_id = %q", got.APIKeyID)
	}
}

func TestLoader_PropagatesAccessorError(t *testing.T) {
	t.Parallel()
	loader := &Loader{Accessor: &fakeSM{err: errors.New("denied")}}
	if _, err := loader.LoadKalshi(context.Background(), "x"); err == nil {
		t.Errorf("expected error")
	}
}
