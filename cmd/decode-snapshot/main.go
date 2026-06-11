// Command decode-snapshot reads a base64-encoded SnapshotPayload protobuf body
// (the BYTES `raw_payload` column from BigQuery `orderbook_snapshots_1s`) on
// stdin or as the first argument, decodes it, and prints the result as
// protojson on stdout. Used by `task ops:bq:row:decode-snapshot`.
package main

import (
	"encoding/base64"
	"fmt"
	"io"
	"os"
	"strings"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	orderbookpb "github.com/paulschick/kalshiflow-pipeline/internal/orderbook/orderbookpb"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "decode-snapshot:", err)
		os.Exit(1)
	}
}

func run() error {
	var b64 string
	if len(os.Args) > 1 {
		b64 = os.Args[1]
	} else {
		buf, err := io.ReadAll(os.Stdin)
		if err != nil {
			return fmt.Errorf("read stdin: %w", err)
		}
		b64 = string(buf)
	}
	b64 = strings.TrimSpace(b64)
	if b64 == "" {
		return fmt.Errorf("no input (pass base64 string on stdin or as arg)")
	}

	raw, err := base64.StdEncoding.DecodeString(b64)
	if err != nil {
		return fmt.Errorf("base64 decode: %w", err)
	}

	var payload orderbookpb.SnapshotPayload
	if err := proto.Unmarshal(raw, &payload); err != nil {
		return fmt.Errorf("proto unmarshal: %w", err)
	}

	out, err := protojson.MarshalOptions{Multiline: true, Indent: "  "}.Marshal(&payload)
	if err != nil {
		return fmt.Errorf("protojson marshal: %w", err)
	}
	fmt.Println(string(out))
	return nil
}
