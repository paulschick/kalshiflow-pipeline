// Package kalshi contains the Kalshi REST + WebSocket client. Slice 2.1a ships the auth
// signer + minimal WS client; slices 2.2 / 2.3 / 2.4 extend.
package kalshi

import (
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/pem"
	"errors"
	"fmt"
	"strings"
)

// sha256Alg is the hash identifier passed to crypto/rsa for PSS operations.
const sha256Alg = crypto.SHA256

// Signer signs Kalshi REST + WS handshake requests with RSA-PSS over
// (timestamp || method || path), per https://docs.kalshi.com/getting_started/api_keys.
//
// The path passed to Sign / Headers MUST exclude query parameters; Sign strips anything after
// the first '?' defensively.
type Signer struct {
	KeyID      string          // KALSHI-ACCESS-KEY value (api key id from dashboard)
	PrivateKey *rsa.PrivateKey // PKCS8-PEM-decoded RSA private key
}

// Sign returns the base64-encoded RSA-PSS signature.
func (s *Signer) Sign(timestamp, method, path string) (string, error) {
	if s == nil || s.PrivateKey == nil {
		return "", errors.New("kalshi: signer has no private key")
	}
	pathForSig := stripQuery(path)
	msg := []byte(timestamp + method + pathForSig)
	hash := sha256.Sum256(msg)
	sig, err := rsa.SignPSS(rand.Reader, s.PrivateKey, sha256Alg, hash[:], &rsa.PSSOptions{
		SaltLength: rsa.PSSSaltLengthEqualsHash, // matches Kalshi's PSS.DIGEST_LENGTH choice
	})
	if err != nil {
		return "", fmt.Errorf("kalshi: rsa-pss sign: %w", err)
	}
	return base64.StdEncoding.EncodeToString(sig), nil
}

// Headers returns the three Kalshi auth headers.
func (s *Signer) Headers(timestamp, method, path string) (map[string]string, error) {
	sig, err := s.Sign(timestamp, method, path)
	if err != nil {
		return nil, err
	}
	return map[string]string{
		"KALSHI-ACCESS-KEY":       s.KeyID,
		"KALSHI-ACCESS-SIGNATURE": sig,
		"KALSHI-ACCESS-TIMESTAMP": timestamp,
	}, nil
}

// ParsePrivateKeyPEM decodes a PKCS8-encoded RSA private key (the format Kalshi exports from
// the dashboard).
func ParsePrivateKeyPEM(in []byte) (*rsa.PrivateKey, error) {
	block, _ := pem.Decode(in)
	if block == nil {
		return nil, errors.New("kalshi: PEM decode failed")
	}
	key, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		return nil, fmt.Errorf("kalshi: parse PKCS8: %w", err)
	}
	priv, ok := key.(*rsa.PrivateKey)
	if !ok {
		return nil, fmt.Errorf("kalshi: expected *rsa.PrivateKey, got %T", key)
	}
	return priv, nil
}

func stripQuery(path string) string {
	p, _, _ := strings.Cut(path, "?")
	return p
}
