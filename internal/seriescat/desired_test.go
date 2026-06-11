package seriescat

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"testing"
)

// fakeViewReader stubs ViewReader for unit tests.
type fakeViewReader struct {
	tickers []string
	err     error
	calls   int
}

func (f *fakeViewReader) ReadTickers(_ context.Context, _, _, _ string) ([]string, error) {
	f.calls++
	return f.tickers, f.err
}

// fakeGCSWriter stubs GCSWriter for unit tests.
type fakeGCSWriter struct {
	attrsGen      int64
	attrsErr      error
	writeErr      error
	lastExpectGen int64
	lastBody      []byte
	writeCalls    int
	attrsCalls    int
}

func (f *fakeGCSWriter) ReadAttrsGeneration(_ context.Context, _, _ string) (int64, error) {
	f.attrsCalls++
	return f.attrsGen, f.attrsErr
}

func (f *fakeGCSWriter) Write(_ context.Context, _, _ string, body []byte, _ string, expectGeneration int64) error {
	f.writeCalls++
	f.lastExpectGen = expectGeneration
	f.lastBody = body
	return f.writeErr
}

var ulidRe = regexp.MustCompile(`^[0-9A-HJKMNP-TV-Z]{26}$`)

func TestWriteDesiredSet_EmptyTickers(t *testing.T) {
	t.Helper()
	vr := &fakeViewReader{tickers: []string{}}
	gw := &fakeGCSWriter{}

	count, gen, err := WriteDesiredSet(context.Background(), vr, gw, DesiredSetConfig{})

	if !errors.Is(err, ErrEmptyDesiredSet) {
		t.Fatalf("want ErrEmptyDesiredSet, got %v", err)
	}
	if count != 0 {
		t.Errorf("want count=0, got %d", count)
	}
	if gen != "" {
		t.Errorf("want generationULID empty, got %q", gen)
	}
	if gw.attrsCalls != 0 || gw.writeCalls != 0 {
		t.Errorf("GCSWriter should not have been called (attrsCalls=%d writeCalls=%d)", gw.attrsCalls, gw.writeCalls)
	}
}

func TestWriteDesiredSet_ObjectAbsent_UsesDoesNotExist(t *testing.T) {
	t.Helper()
	vr := &fakeViewReader{tickers: []string{"KXLTCD", "KXBTCD"}} // intentionally unsorted
	gw := &fakeGCSWriter{attrsErr: ErrObjectAbsent}

	count, gen, err := WriteDesiredSet(context.Background(), vr, gw, DesiredSetConfig{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if gw.writeCalls != 1 {
		t.Fatalf("want 1 Write call, got %d", gw.writeCalls)
	}
	if gw.lastExpectGen != 0 {
		t.Errorf("want expectGeneration=0 (DoesNotExist), got %d", gw.lastExpectGen)
	}

	var got DesiredSetFile
	if err := json.Unmarshal(gw.lastBody, &got); err != nil {
		t.Fatalf("unmarshal body: %v", err)
	}
	if len(got.Tickers) != 2 || got.Tickers[0] != "KXBTCD" || got.Tickers[1] != "KXLTCD" {
		t.Errorf("want sorted tickers [KXBTCD KXLTCD], got %v", got.Tickers)
	}
	if count != 2 {
		t.Errorf("want count=2, got %d", count)
	}
	if !ulidRe.MatchString(gen) {
		t.Errorf("generationULID %q does not match ULID pattern", gen)
	}
}

func TestWriteDesiredSet_ObjectPresent_UsesGenerationMatch(t *testing.T) {
	t.Helper()
	vr := &fakeViewReader{tickers: []string{"KXBTCD"}}
	gw := &fakeGCSWriter{attrsGen: 42}

	_, _, err := WriteDesiredSet(context.Background(), vr, gw, DesiredSetConfig{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if gw.lastExpectGen != 42 {
		t.Errorf("want expectGeneration=42, got %d", gw.lastExpectGen)
	}
}

func TestWriteDesiredSet_AttrsTransientError(t *testing.T) {
	t.Helper()
	vr := &fakeViewReader{tickers: []string{"KXBTCD"}}
	gw := &fakeGCSWriter{attrsErr: errors.New("transient")} // NOT ErrObjectAbsent

	_, _, err := WriteDesiredSet(context.Background(), vr, gw, DesiredSetConfig{})
	if err == nil {
		t.Fatal("want error, got nil")
	}
	if !containsSubstr(err.Error(), "attrs") {
		t.Errorf("want error to contain %q, got %q", "attrs", err.Error())
	}
	if gw.writeCalls != 0 {
		t.Errorf("Write should not have been called, got %d calls", gw.writeCalls)
	}
}

func TestWriteDesiredSet_WriteError(t *testing.T) {
	t.Helper()
	vr := &fakeViewReader{tickers: []string{"KXBTCD"}}
	gw := &fakeGCSWriter{
		attrsErr: ErrObjectAbsent,
		writeErr: fmt.Errorf("precondition: %w", errors.New("412")),
	}

	_, _, err := WriteDesiredSet(context.Background(), vr, gw, DesiredSetConfig{})
	if err == nil {
		t.Fatal("want error, got nil")
	}
	if !containsSubstr(err.Error(), "write") {
		t.Errorf("want error to contain %q, got %q", "write", err.Error())
	}
	if !containsSubstr(err.Error(), "412") {
		t.Errorf("want underlying 412 error propagated, got %q", err.Error())
	}
}

func TestWriteDesiredSet_ViewReaderError(t *testing.T) {
	t.Helper()
	vr := &fakeViewReader{err: errors.New("auth")}
	gw := &fakeGCSWriter{}

	_, _, err := WriteDesiredSet(context.Background(), vr, gw, DesiredSetConfig{})
	if err == nil {
		t.Fatal("want error, got nil")
	}
	if !containsSubstr(err.Error(), "read view") {
		t.Errorf("want error to contain %q, got %q", "read view", err.Error())
	}
	if gw.attrsCalls != 0 {
		t.Errorf("ReadAttrsGeneration should not have been called, got %d calls", gw.attrsCalls)
	}
}

func TestWriteKrakenPairsDesired_EmptyPairs(t *testing.T) {
	gw := &fakeGCSWriter{}
	count, gen, err := WriteKrakenPairsDesired(context.Background(), gw, KrakenPairsConfig{Bucket: "b", ObjectKey: "o"}, nil)
	if !errors.Is(err, ErrEmptyKrakenPairs) {
		t.Fatalf("want ErrEmptyKrakenPairs, got %v", err)
	}
	if count != 0 || gen != "" {
		t.Errorf("want (0, \"\"), got (%d, %q)", count, gen)
	}
	if gw.attrsCalls != 0 || gw.writeCalls != 0 {
		t.Errorf("GCSWriter must not be called on empty input (attrs=%d write=%d)", gw.attrsCalls, gw.writeCalls)
	}
}

func TestWriteKrakenPairsDesired_ObjectAbsent_UsesDoesNotExist(t *testing.T) {
	gw := &fakeGCSWriter{attrsErr: ErrObjectAbsent}
	count, gen, err := WriteKrakenPairsDesired(
		context.Background(), gw,
		KrakenPairsConfig{Bucket: "b", ObjectKey: "control/kraken_pairs.json"},
		[]string{"LTC/USD", "BTC/USD"}, // intentionally unsorted
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if gw.writeCalls != 1 {
		t.Fatalf("want 1 Write call, got %d", gw.writeCalls)
	}
	if gw.lastExpectGen != 0 {
		t.Errorf("want expectGeneration=0 (DoesNotExist), got %d", gw.lastExpectGen)
	}

	var got KrakenPairsFile
	if err := json.Unmarshal(gw.lastBody, &got); err != nil {
		t.Fatalf("unmarshal body: %v", err)
	}
	if len(got.Pairs) != 2 || got.Pairs[0] != "BTC/USD" || got.Pairs[1] != "LTC/USD" {
		t.Errorf("want sorted pairs [BTC/USD LTC/USD], got %v", got.Pairs)
	}
	if count != 2 {
		t.Errorf("want count=2, got %d", count)
	}
	if !ulidRe.MatchString(gen) {
		t.Errorf("generationULID %q does not match ULID pattern", gen)
	}
	if got.GeneratedAt == "" {
		t.Errorf("GeneratedAt must be populated")
	}
}

func TestWriteKrakenPairsDesired_ObjectPresent_UsesGenerationMatch(t *testing.T) {
	gw := &fakeGCSWriter{attrsGen: 99}
	_, _, err := WriteKrakenPairsDesired(
		context.Background(), gw,
		KrakenPairsConfig{Bucket: "b", ObjectKey: "control/kraken_pairs.json"},
		[]string{"BTC/USD"},
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if gw.lastExpectGen != 99 {
		t.Errorf("want expectGeneration=99, got %d", gw.lastExpectGen)
	}
}

func TestWriteKrakenPairsDesired_AttrsTransientError(t *testing.T) {
	gw := &fakeGCSWriter{attrsErr: errors.New("transient")}
	_, _, err := WriteKrakenPairsDesired(
		context.Background(), gw,
		KrakenPairsConfig{Bucket: "b", ObjectKey: "control/kraken_pairs.json"},
		[]string{"BTC/USD"},
	)
	if err == nil || !containsSubstr(err.Error(), "attrs") {
		t.Errorf("want wrapped attrs error, got %v", err)
	}
	if gw.writeCalls != 0 {
		t.Errorf("Write must not be called on attrs error, got %d", gw.writeCalls)
	}
}

func TestWriteKrakenPairsDesired_WriteError(t *testing.T) {
	gw := &fakeGCSWriter{
		attrsErr: ErrObjectAbsent,
		writeErr: fmt.Errorf("precondition: %w", errors.New("412")),
	}
	_, _, err := WriteKrakenPairsDesired(
		context.Background(), gw,
		KrakenPairsConfig{Bucket: "b", ObjectKey: "control/kraken_pairs.json"},
		[]string{"BTC/USD"},
	)
	if err == nil {
		t.Fatal("want error, got nil")
	}
	if !containsSubstr(err.Error(), "write") || !containsSubstr(err.Error(), "412") {
		t.Errorf("want wrapped 412 write error, got %v", err)
	}
}

func TestWriteKrakenPairsDesired_DoesNotMutateInput(t *testing.T) {
	gw := &fakeGCSWriter{attrsErr: ErrObjectAbsent}
	input := []string{"LTC/USD", "BTC/USD"}
	_, _, err := WriteKrakenPairsDesired(
		context.Background(), gw,
		KrakenPairsConfig{Bucket: "b", ObjectKey: "o"},
		input,
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if input[0] != "LTC/USD" || input[1] != "BTC/USD" {
		t.Errorf("input mutated: %v", input)
	}
}

func TestWriteKrakenPairsDesired_DistinctULIDsBetweenWrites(t *testing.T) {
	gw := &fakeGCSWriter{attrsErr: ErrObjectAbsent}
	_, g1, _ := WriteKrakenPairsDesired(
		context.Background(), gw,
		KrakenPairsConfig{Bucket: "b", ObjectKey: "o"},
		[]string{"BTC/USD"},
	)
	_, g2, _ := WriteKrakenPairsDesired(
		context.Background(), gw,
		KrakenPairsConfig{Bucket: "b", ObjectKey: "o"},
		[]string{"BTC/USD"},
	)
	if g1 == "" || g2 == "" || g1 == g2 {
		t.Errorf("expect two distinct non-empty ULIDs, got g1=%q g2=%q", g1, g2)
	}
}

func containsSubstr(s, sub string) bool {
	return len(s) >= len(sub) && (s == sub || len(sub) == 0 || findSubstr(s, sub))
}

func findSubstr(s, sub string) bool {
	for i := 0; i <= len(s)-len(sub); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
