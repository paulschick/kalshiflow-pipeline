package seriescat

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
)

// CanonicalHash decodes the JSON value, re-emits it with sorted object keys
// (recursive), and returns the hex SHA-256 of the canonical bytes. Stable under
// object-field reorder; arrays remain order-significant. Whitespace insensitive
// because the decoder normalizes.
func CanonicalHash(raw json.RawMessage) (string, error) {
	var v any
	dec := json.NewDecoder(strings.NewReader(string(raw)))
	dec.UseNumber()
	if err := dec.Decode(&v); err != nil {
		return "", fmt.Errorf("seriescat: decode: %w", err)
	}
	var sb strings.Builder
	if err := writeCanonical(&sb, v); err != nil {
		return "", fmt.Errorf("seriescat: canonicalize: %w", err)
	}
	sum := sha256.Sum256([]byte(sb.String()))
	return hex.EncodeToString(sum[:]), nil
}

func writeCanonical(sb *strings.Builder, v any) error {
	switch x := v.(type) {
	case nil:
		sb.WriteString("null")
	case bool:
		if x {
			sb.WriteString("true")
		} else {
			sb.WriteString("false")
		}
	case json.Number:
		sb.WriteString(string(x))
	case string:
		b, err := json.Marshal(x)
		if err != nil {
			return err
		}
		sb.Write(b)
	case []any:
		sb.WriteByte('[')
		for i, item := range x {
			if i > 0 {
				sb.WriteByte(',')
			}
			if err := writeCanonical(sb, item); err != nil {
				return err
			}
		}
		sb.WriteByte(']')
	case map[string]any:
		keys := make([]string, 0, len(x))
		for k := range x {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		sb.WriteByte('{')
		for i, k := range keys {
			if i > 0 {
				sb.WriteByte(',')
			}
			b, err := json.Marshal(k)
			if err != nil {
				return err
			}
			sb.Write(b)
			sb.WriteByte(':')
			if err := writeCanonical(sb, x[k]); err != nil {
				return err
			}
		}
		sb.WriteByte('}')
	default:
		return fmt.Errorf("unsupported type %T", v)
	}
	return nil
}
