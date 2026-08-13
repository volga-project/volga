package controller

import (
	"encoding/json"
	"fmt"
	"strings"
)

const storagePort = int32(50071)

type inMemorySink struct {
	present bool
	addr    string
}

func storageResourceName(pipelineName string) string {
	return fmt.Sprintf("%s-storage", pipelineName)
}

func ownedStorageHTTPAddr(namespace, pipelineName string) string {
	return fmt.Sprintf(
		"http://%s.%s.svc.cluster.local:%d",
		storageResourceName(pipelineName),
		namespace,
		storagePort,
	)
}

func ownsInMemoryStore(sink inMemorySink, ownedAddr string) bool {
	if !sink.present {
		return false
	}
	return sink.addr == "" || sink.addr == ownedAddr
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
		ServerAddr string `json:"server_addr"`
	}
	if err := json.Unmarshal(im, &inner); err != nil {
		return inMemorySink{}, fmt.Errorf("pipelineSpec.sink.InMemoryStorageGrpc: %w", err)
	}
	return inMemorySink{present: true, addr: strings.TrimSpace(inner.ServerAddr)}, nil
}

func withInMemorySinkAddr(pipelineSpec json.RawMessage, addr string) (json.RawMessage, error) {
	var root map[string]json.RawMessage
	if err := json.Unmarshal(pipelineSpec, &root); err != nil {
		return nil, fmt.Errorf("pipelineSpec: %w", err)
	}
	sinkRaw, ok := root["sink"]
	if !ok {
		return nil, fmt.Errorf("pipelineSpec.sink is missing")
	}
	var asObj map[string]json.RawMessage
	if err := json.Unmarshal(sinkRaw, &asObj); err != nil {
		return nil, fmt.Errorf("pipelineSpec.sink: %w", err)
	}
	im, ok := asObj["InMemoryStorageGrpc"]
	if !ok {
		return nil, fmt.Errorf("pipelineSpec.sink is not InMemoryStorageGrpc")
	}
	im = unwrapJSONValue(im)
	var inner map[string]any
	if err := json.Unmarshal(im, &inner); err != nil {
		return nil, fmt.Errorf("pipelineSpec.sink.InMemoryStorageGrpc: %w", err)
	}
	inner["server_addr"] = addr
	updatedInner, err := json.Marshal(inner)
	if err != nil {
		return nil, err
	}
	asObj["InMemoryStorageGrpc"] = updatedInner
	updatedSink, err := json.Marshal(asObj)
	if err != nil {
		return nil, err
	}
	root["sink"] = updatedSink
	return json.Marshal(root)
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
