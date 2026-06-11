package envelope

import (
	"strings"
	"testing"
)

func TestExpectedFieldsHasNoDuplicates(t *testing.T) {
	t.Parallel()
	seen := make(map[string]struct{}, len(ExpectedFields))
	for _, f := range ExpectedFields {
		if _, dup := seen[f]; dup {
			t.Fatalf("duplicate envelope field: %q", f)
		}
		seen[f] = struct{}{}
	}
}

func TestExpectedFieldsAreSnakeCase(t *testing.T) {
	t.Parallel()
	for _, f := range ExpectedFields {
		if f == "" {
			t.Fatal("empty envelope field name")
		}
		for _, r := range f {
			switch {
			case r >= 'a' && r <= 'z':
			case r >= '0' && r <= '9':
			case r == '_':
			default:
				t.Fatalf("field %q contains non-snake_case character %q", f, r)
			}
		}
		if strings.HasPrefix(f, "_") || strings.HasSuffix(f, "_") {
			t.Fatalf("field %q has leading/trailing underscore", f)
		}
	}
}

func TestExpectedFieldsCount(t *testing.T) {
	t.Parallel()
	const want = 11
	if got := len(ExpectedFields); got != want {
		t.Fatalf("expected %d envelope fields, got %d (internal/envelope/schema.json is the source of truth)", want, got)
	}
}

func TestSchemaTypesAreValid(t *testing.T) {
	t.Parallel()
	valid := map[string]bool{
		"STRING": true, "TIMESTAMP": true, "INT64": true, "BYTES": true,
	}
	for _, f := range Schema {
		if !valid[f.Type] {
			t.Fatalf("field %q has unrecognized BigQuery type %q", f.Name, f.Type)
		}
	}
}

func TestSchemaModesAreValid(t *testing.T) {
	t.Parallel()
	valid := map[string]bool{"REQUIRED": true, "NULLABLE": true, "REPEATED": true}
	for _, f := range Schema {
		if !valid[f.Mode] {
			t.Fatalf("field %q has unrecognized BigQuery mode %q", f.Name, f.Mode)
		}
	}
}
