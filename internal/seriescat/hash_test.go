package seriescat

import (
	"encoding/json"
	"testing"
)

func TestCanonicalHash(t *testing.T) {
	t.Run("stable under top-level reorder", func(t *testing.T) {
		a := json.RawMessage(`{"ticker":"KXBTC","title":"BTC"}`)
		b := json.RawMessage(`{"title":"BTC","ticker":"KXBTC"}`)
		ha, err := CanonicalHash(a)
		if err != nil {
			t.Fatalf("hash a: %v", err)
		}
		hb, err := CanonicalHash(b)
		if err != nil {
			t.Fatalf("hash b: %v", err)
		}
		if ha != hb {
			t.Fatalf("hashes differ for reordered keys: %s vs %s", ha, hb)
		}
	})

	t.Run("stable under nested object reorder", func(t *testing.T) {
		a := json.RawMessage(`{"meta":{"a":1,"b":2},"ticker":"K"}`)
		b := json.RawMessage(`{"ticker":"K","meta":{"b":2,"a":1}}`)
		ha, _ := CanonicalHash(a)
		hb, _ := CanonicalHash(b)
		if ha != hb {
			t.Fatalf("nested reorder differs: %s vs %s", ha, hb)
		}
	})

	t.Run("array order is significant", func(t *testing.T) {
		a := json.RawMessage(`{"tags":["x","y"]}`)
		b := json.RawMessage(`{"tags":["y","x"]}`)
		ha, _ := CanonicalHash(a)
		hb, _ := CanonicalHash(b)
		if ha == hb {
			t.Fatalf("array-order change MUST change hash; both = %s", ha)
		}
	})

	t.Run("distinct payloads → distinct hashes", func(t *testing.T) {
		a := json.RawMessage(`{"ticker":"KXBTC","title":"BTC"}`)
		b := json.RawMessage(`{"ticker":"KXBTC","title":"Bitcoin"}`)
		ha, _ := CanonicalHash(a)
		hb, _ := CanonicalHash(b)
		if ha == hb {
			t.Fatalf("title change did not change hash")
		}
	})

	t.Run("whitespace insensitive", func(t *testing.T) {
		a := json.RawMessage(`{"a":1,"b":2}`)
		b := json.RawMessage(`{ "a" : 1 , "b" : 2 }`)
		ha, _ := CanonicalHash(a)
		hb, _ := CanonicalHash(b)
		if ha != hb {
			t.Fatalf("whitespace changed hash: %s vs %s", ha, hb)
		}
	})

	t.Run("invalid json errors", func(t *testing.T) {
		_, err := CanonicalHash(json.RawMessage(`{not json`))
		if err == nil {
			t.Fatal("expected error on bad JSON")
		}
	})
}
