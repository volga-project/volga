package controller

import (
	"encoding/json"
	"testing"
)

func TestParseInMemorySinkEmbeddedEmptyAddr(t *testing.T) {
	raw := json.RawMessage(`{
		"sink": {
			"InMemoryStorageGrpc": "{\n  \"server_addr\": \"\"\n}"
		}
	}`)
	sink, err := parseInMemorySink(raw)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if !sink.present || sink.addr != "" {
		t.Fatalf("got %+v, want present empty-addr InMemory", sink)
	}
	if !ownsInMemoryStore(sink, "http://sample-storage.default.svc.cluster.local:50071") {
		t.Fatal("empty addr should be operator-owned")
	}
}

func TestParseInMemorySinkObjectForm(t *testing.T) {
	raw := json.RawMessage(`{
		"sink": {
			"InMemoryStorageGrpc": {
				"server_addr": "",
				"upsert_key_columns": []
			}
		}
	}`)
	sink, err := parseInMemorySink(raw)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if !sink.present || sink.addr != "" {
		t.Fatalf("got %+v", sink)
	}
}

func TestParseInMemorySinkExternalAddrNotOwned(t *testing.T) {
	raw := json.RawMessage(`{
		"sink": {
			"InMemoryStorageGrpc": {
				"server_addr": "http://volga-test-storage.default.svc.cluster.local:50071"
			}
		}
	}`)
	sink, err := parseInMemorySink(raw)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	owned := ownedStorageHTTPAddr("default", "sample-pipeline")
	if ownsInMemoryStore(sink, owned) {
		t.Fatalf("external singleton addr should not be owned; owned=%s", owned)
	}
}

func TestOwnsInMemoryStoreMatchingDNS(t *testing.T) {
	owned := ownedStorageHTTPAddr("default", "kube-harness-abc")
	sink := inMemorySink{present: true, addr: owned}
	if !ownsInMemoryStore(sink, owned) {
		t.Fatalf("filled owned DNS %s should still be owned", owned)
	}
}

func TestParseCountSinkCreatesNothing(t *testing.T) {
	raw := json.RawMessage(`{"sink": "Count"}`)
	sink, err := parseInMemorySink(raw)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if sink.present {
		t.Fatalf("Count must not look like InMemory: %+v", sink)
	}
	if ownsInMemoryStore(sink, ownedStorageHTTPAddr("default", "p")) {
		t.Fatal("Count must not own a store")
	}
}

func TestParseParquetSinkCreatesNothing(t *testing.T) {
	raw := json.RawMessage(`{"sink": {"Parquet": {"path": "/tmp/out"}}}`)
	sink, err := parseInMemorySink(raw)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if sink.present || ownsInMemoryStore(sink, "http://x") {
		t.Fatalf("Parquet must not own a store: %+v", sink)
	}
}

func TestParseMissingSinkCreatesNothing(t *testing.T) {
	raw := json.RawMessage(`{"sql": "SELECT 1"}`)
	sink, err := parseInMemorySink(raw)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if sink.present {
		t.Fatalf("missing sink: %+v", sink)
	}
}

func TestWithInMemorySinkAddrFillsEmptyEmbedded(t *testing.T) {
	raw := json.RawMessage(`{
		"sql": "SELECT 1",
		"sink": {
			"InMemoryStorageGrpc": "{\n  \"server_addr\": \"\",\n  \"upsert_key_columns\": []\n}"
		}
	}`)
	addr := ownedStorageHTTPAddr("default", "sample-pipeline")
	updated, err := withInMemorySinkAddr(raw, addr)
	if err != nil {
		t.Fatalf("fill: %v", err)
	}
	sink, err := parseInMemorySink(updated)
	if err != nil {
		t.Fatalf("reparse: %v", err)
	}
	if !sink.present || sink.addr != addr {
		t.Fatalf("got %+v, want addr %s", sink, addr)
	}
	if !ownsInMemoryStore(sink, addr) {
		t.Fatal("filled DNS should remain owned")
	}
}
