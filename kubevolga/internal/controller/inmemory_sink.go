package controller

import (
	"encoding/json"
	"fmt"
	"strings"
)

const storagePort = int32(50071)

type inMemorySink struct {
	present bool
	create  bool
	addr    string
}

func storageResourceName(pipelineName string) string {
	return fmt.Sprintf("%s-storage", pipelineName)
}

func (sink inMemorySink) createsStore() (bool, error) {
	if !sink.present {
		return false, nil
	}
	hasAddr := sink.addr != ""
	switch {
	case sink.create && !hasAddr:
		return true, nil
	case !sink.create && hasAddr:
		return false, nil
	case sink.create && hasAddr:
		return false, fmt.Errorf("InMemoryStorageGrpc create and server_addr are mutually exclusive")
	default:
		return false, fmt.Errorf("InMemoryStorageGrpc requires create: true or server_addr")
	}
}

func parseInMemorySink(pipelineSpec json.RawMessage) (inMemorySink, error) {
	if len(pipelineSpec) == 0 || string(pipelineSpec) == "null" {
		return inMemorySink{}, nil
	}
	var root map[string]json.RawMessage
	if err := json.Unmarshal(pipelineSpec, &root); err != nil {
		return inMemorySink{}, fmt.Errorf("pipelineSpec: %w", err)
	}
	sinkRaw, ok := root["sink"]
	if !ok || len(sinkRaw) == 0 || string(sinkRaw) == "null" {
		return inMemorySink{}, nil
	}
	var asString string
	if err := json.Unmarshal(sinkRaw, &asString); err == nil {
		return inMemorySink{}, nil
	}
	var asObj map[string]json.RawMessage
	if err := json.Unmarshal(sinkRaw, &asObj); err != nil {
		return inMemorySink{}, fmt.Errorf("pipelineSpec.sink: %w", err)
	}
	im, ok := asObj["InMemoryStorageGrpc"]
	if !ok {
		return inMemorySink{}, nil
	}
	im = unwrapJSONValue(im)
	var inner struct {
		Create     bool    `json:"create"`
		ServerAddr *string `json:"server_addr"`
	}
	if err := json.Unmarshal(im, &inner); err != nil {
		return inMemorySink{}, fmt.Errorf("pipelineSpec.sink.InMemoryStorageGrpc: %w", err)
	}
	addr := ""
	if inner.ServerAddr != nil {
		addr = strings.TrimSpace(*inner.ServerAddr)
	}
	return inMemorySink{present: true, create: inner.Create, addr: addr}, nil
}

func unwrapJSONValue(raw json.RawMessage) json.RawMessage {
	var s string
	if err := json.Unmarshal(raw, &s); err != nil {
		return raw
	}
	trimmed := strings.TrimSpace(s)
	if json.Valid([]byte(trimmed)) {
		return json.RawMessage(trimmed)
	}
	return raw
}
