package orderbook

import (
	"bytes"
	"encoding/json"
	"hash/crc32"
	"os"
	"testing"
)

type btcSnapshotFixture struct {
	Symbol   string
	Bids     []WireLevel
	Asks     []WireLevel
	Checksum uint32
}

func loadBTCSnapshotFixture(t *testing.T) btcSnapshotFixture {
	t.Helper()
	raw, err := os.ReadFile("testdata/btcusd-snapshot.json")
	if err != nil {
		t.Fatalf("read fixture: %v", err)
	}
	var outer struct {
		Data []struct {
			Symbol   string      `json:"symbol"`
			Bids     []WireLevel `json:"bids"`
			Asks     []WireLevel `json:"asks"`
			Checksum uint32      `json:"checksum"`
		} `json:"data"`
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	d.UseNumber()
	if err := d.Decode(&outer); err != nil {
		t.Fatalf("decode fixture: %v", err)
	}
	if len(outer.Data) == 0 {
		t.Fatal("fixture has empty data array")
	}
	e := outer.Data[0]
	return btcSnapshotFixture{Symbol: e.Symbol, Bids: e.Bids, Asks: e.Asks, Checksum: e.Checksum}
}

func TestVerifyChecksum_GoldenVector_BTCUSD(t *testing.T) {
	fx := loadBTCSnapshotFixture(t)
	matched, computed := VerifyChecksum(fx.Asks, fx.Bids, fx.Checksum)
	if !matched {
		t.Fatalf("checksum mismatch: computed=%d expected=%d. The captured frame is the binding contract; if this fails, find the correct algorithm.", computed, fx.Checksum)
	}
}

func TestWireLevelsToChecksumBytes_GoldenVector(t *testing.T) {
	fx := loadBTCSnapshotFixture(t)
	got := WireLevelsToChecksumBytes(fx.Asks, fx.Bids)
	if len(got) == 0 {
		t.Fatal("expected non-empty checksum bytes")
	}
	if crc := crc32.ChecksumIEEE(got); crc != fx.Checksum {
		t.Fatalf("crc mismatch via helper: got=0x%08x want=0x%08x", crc, fx.Checksum)
	}
}
