package kalshi

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/pem"
	"strings"
	"testing"
)

func TestSigner_VerifiesAgainstPublicKey(t *testing.T) {
	t.Parallel()
	priv, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	signer := &Signer{KeyID: "test-key", PrivateKey: priv}

	ts := "1714400000000"
	sig, err := signer.Sign(ts, "GET", "/trade-api/v2/markets")
	if err != nil {
		t.Fatalf("Sign: %v", err)
	}

	raw, err := base64.StdEncoding.DecodeString(sig)
	if err != nil {
		t.Fatalf("not base64: %v", err)
	}
	msg := []byte(ts + "GET" + "/trade-api/v2/markets")
	hash := sha256.Sum256(msg)
	if err := rsa.VerifyPSS(&priv.PublicKey, sha256Alg, hash[:], raw, &rsa.PSSOptions{SaltLength: rsa.PSSSaltLengthEqualsHash}); err != nil {
		t.Errorf("signature did not verify: %v", err)
	}
}

func TestSigner_StripsQueryFromPath(t *testing.T) {
	t.Parallel()
	priv, _ := rsa.GenerateKey(rand.Reader, 2048)
	signer := &Signer{KeyID: "k", PrivateKey: priv}

	// Both should sign the canonical message "0GET/trade-api/v2/markets". RSA-PSS is
	// randomized so we cannot byte-compare; instead verify both signatures validate
	// against the canonical (no-query) message.
	withQuery, err := signer.Sign("0", "GET", "/trade-api/v2/markets?series_ticker=KXBTCD&status=open")
	if err != nil {
		t.Fatalf("Sign with query: %v", err)
	}
	withoutQuery, err := signer.Sign("0", "GET", "/trade-api/v2/markets")
	if err != nil {
		t.Fatalf("Sign without query: %v", err)
	}

	canonical := []byte("0" + "GET" + "/trade-api/v2/markets")
	hash := sha256.Sum256(canonical)
	for _, sig := range []string{withQuery, withoutQuery} {
		raw, err := base64.StdEncoding.DecodeString(sig)
		if err != nil {
			t.Fatalf("not base64: %v", err)
		}
		if err := rsa.VerifyPSS(&priv.PublicKey, sha256Alg, hash[:], raw, &rsa.PSSOptions{SaltLength: rsa.PSSSaltLengthEqualsHash}); err != nil {
			t.Errorf("signature did not verify against canonical (no-query) message: %v", err)
		}
	}
}

func TestSigner_HeadersIncludeAllThreeRequiredFields(t *testing.T) {
	t.Parallel()
	priv, _ := rsa.GenerateKey(rand.Reader, 2048)
	signer := &Signer{KeyID: "kid-123", PrivateKey: priv}

	headers, err := signer.Headers("1714400000000", "GET", "/trade-api/v2/markets")
	if err != nil {
		t.Fatalf("Headers: %v", err)
	}
	for _, k := range []string{"KALSHI-ACCESS-KEY", "KALSHI-ACCESS-SIGNATURE", "KALSHI-ACCESS-TIMESTAMP"} {
		if headers[k] == "" {
			t.Errorf("missing header %q", k)
		}
	}
	if headers["KALSHI-ACCESS-KEY"] != "kid-123" {
		t.Errorf("key id = %q; want kid-123", headers["KALSHI-ACCESS-KEY"])
	}
}

func TestParsePrivateKeyPEM_AcceptsPKCS8(t *testing.T) {
	t.Parallel()
	priv, _ := rsa.GenerateKey(rand.Reader, 2048)
	der, _ := x509.MarshalPKCS8PrivateKey(priv)
	pemBytes := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der})

	got, err := ParsePrivateKeyPEM(pemBytes)
	if err != nil {
		t.Fatalf("ParsePrivateKeyPEM: %v", err)
	}
	if got.N.Cmp(priv.N) != 0 {
		t.Errorf("modulus mismatch after round-trip")
	}
}

func TestParsePrivateKeyPEM_RejectsNonRSA(t *testing.T) {
	t.Parallel()
	garbage := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: []byte("not-asn1")})
	if _, err := ParsePrivateKeyPEM(garbage); err == nil || !strings.Contains(err.Error(), "PKCS8") {
		t.Errorf("expected PKCS8 parse error; got %v", err)
	}
}
