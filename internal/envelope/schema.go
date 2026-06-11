// Package envelope defines the canonical Kalshi event envelope schema. The
// embedded schema.json is the single source of truth for both the Go envelope
// contract and the BigQuery table schema (infra/bigquery.tf reads the same
// file via file()). Plan 2's WS worker binds its envelope struct to
// ExpectedFields; a reflection-based test in Plan 2 asserts the struct's JSON
// tags match this list exactly. To add or remove a field, edit schema.json
// only — Terraform and Go both pick up the change on the next build/apply.
//
// Repo-layout caveat: if you restructure the repo, update this //go:embed
// directive AND the path in infra/bigquery.tf
// (${path.module}/../internal/envelope/schema.json) in lockstep, or the BQ
// schema and Go envelope contract will silently drift.
package envelope

import (
	_ "embed"
	"encoding/json"
)

//go:embed schema.json
var schemaJSON []byte

// Field mirrors the BigQuery schema field shape. Type values are BQ scalar
// types (STRING, TIMESTAMP, INT64, JSON); Mode is REQUIRED, NULLABLE, or
// REPEATED. The Go envelope struct does not need to mirror Type/Mode at the
// type level — those are enforced at BQ ingest time — but the test set
// validates them so a typo in schema.json fails CI rather than production.
type Field struct {
	Name string `json:"name"`
	Type string `json:"type"`
	Mode string `json:"mode"`
}

// Schema is the parsed canonical schema in declaration order. Schema[i].Name
// equals ExpectedFields[i] for all i.
var Schema = mustParseSchema(schemaJSON)

// ExpectedFields is the ordered field-name list extracted from Schema. Plan 2
// binds its envelope struct's JSON tag set to this slice.
var ExpectedFields = fieldNames(Schema)

func mustParseSchema(b []byte) []Field {
	var s []Field
	if err := json.Unmarshal(b, &s); err != nil {
		panic("envelope: parse schema.json: " + err.Error())
	}
	return s
}

func fieldNames(fs []Field) []string {
	names := make([]string, len(fs))
	for i, f := range fs {
		names[i] = f.Name
	}
	return names
}
