package controller

import (
	"encoding/json"
	"testing"
)

func TestInMemorySinkCreateTable(t *testing.T) {
	cases := []struct {
		name       string
		raw        string
		wantCreate bool
		wantErr    bool
	}{
		{
			name:       "create true no addr embedded",
			raw:        `{"sink":{"InMemoryStorageGrpc":"{\n  \"create\": true\n}"}}`,
			wantCreate: true,
		},
		{
			name:    "create true plus addr",
			raw:     `{"sink":{"InMemoryStorageGrpc":{"create":true,"server_addr":"http://x:50071"}}}`,
			wantErr: true,
		},
		{
			name:    "create omitted no addr",
			raw:     `{"sink":{"InMemoryStorageGrpc":{}}}`,
			wantErr: true,
		},
		{
			name:    "create false no addr",
			raw:     `{"sink":{"InMemoryStorageGrpc":{"create":false}}}`,
			wantErr: true,
		},
		{
			name: "create false plus addr",
			raw:  `{"sink":{"InMemoryStorageGrpc":{"create":false,"server_addr":"http://x:50071"}}}`,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			sink, err := parseInMemorySink(json.RawMessage(tc.raw))
			if err != nil {
				t.Fatalf("parse: %v", err)
			}
			creates, err := sink.createsStore()
			if tc.wantErr {
				if err == nil {
					t.Fatalf("got create=%v, want error", creates)
				}
				return
			}
			if err != nil {
				t.Fatalf("createsStore: %v", err)
			}
			if creates != tc.wantCreate {
				t.Fatalf("create=%v, want %v", creates, tc.wantCreate)
			}
		})
	}
}

func TestParseInMemorySinkExternalAddrNotCreated(t *testing.T) {
	raw := json.RawMessage(`{
		"sink": {
			"InMemoryStorageGrpc": {
				"server_addr": "http://my-store:50071"
			}
		}
	}`)
	sink, err := parseInMemorySink(raw)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	creates, err := sink.createsStore()
	if err != nil {
		t.Fatalf("createsStore: %v", err)
	}
	if creates {
		t.Fatal("omitted create + server_addr must not create a store")
	}
}

func TestParseCountSinkCreatesNothing(t *testing.T) {
	raw := json.RawMessage(`{"sink": "Count"}`)
	sink, err := parseInMemorySink(raw)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	creates, err := sink.createsStore()
	if err != nil {
		t.Fatalf("createsStore: %v", err)
	}
	if sink.present || creates {
		t.Fatalf("Count must not create a store: %+v", sink)
	}
}
