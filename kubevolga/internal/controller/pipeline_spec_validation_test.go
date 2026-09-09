package controller

import (
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"kubevolga/api/v1alpha1"

	"sigs.k8s.io/yaml"
)

func TestSamplePipelineSpecValidates(t *testing.T) {
	validateSampleManifest(t, "volga_v1alpha1_pipeline.yaml")
}

func TestScyllaSamplePipelineSpecValidates(t *testing.T) {
	validateSampleManifest(t, "volga_v1alpha1_pipeline_scylla.yaml")
}

func TestScyllaRequestStoreValidates(t *testing.T) {
	raw := loadSamplePipelineSpec(t, "volga_v1alpha1_pipeline_scylla.yaml")
	var spec map[string]any
	if err := json.Unmarshal(raw, &spec); err != nil {
		t.Fatalf("unmarshal pipelineSpec: %v", err)
	}
	state, ok := spec["state"].(map[string]any)
	if !ok {
		t.Fatal("pipelineSpec.state missing")
	}
	scylla, ok := state["operator_backend"].(map[string]any)["scylla"]
	if !ok {
		t.Fatal("operator_backend.scylla missing")
	}
	state["request_store"] = map[string]any{"scylla": scylla}
	patched, err := json.Marshal(spec)
	if err != nil {
		t.Fatalf("marshal patched spec: %v", err)
	}
	if err := validatePipelineSpec(patched); err != nil {
		t.Fatalf("scylla request_store should be valid, got error: %v", err)
	}
}

func TestOperatorBackendScyllaMissingKeyspaceRejected(t *testing.T) {
	raw := loadSamplePipelineSpec(t, "volga_v1alpha1_pipeline_scylla.yaml")
	var spec map[string]any
	if err := json.Unmarshal(raw, &spec); err != nil {
		t.Fatalf("unmarshal pipelineSpec: %v", err)
	}
	scylla := spec["state"].(map[string]any)["operator_backend"].(map[string]any)["scylla"].(map[string]any)
	delete(scylla, "keyspace")
	patched, err := json.Marshal(spec)
	if err != nil {
		t.Fatalf("marshal patched spec: %v", err)
	}
	if err := validatePipelineSpec(patched); err == nil {
		t.Fatal("expected invalid pipelineSpec when scylla.keyspace is missing")
	}
}

func validateSampleManifest(t *testing.T, name string) {
	t.Helper()
	if err := validatePipelineSpec(loadSamplePipelineSpec(t, name)); err != nil {
		t.Fatalf("%s pipelineSpec should be valid, got error: %v", name, err)
	}
}

func loadSamplePipelineSpec(t *testing.T, name string) json.RawMessage {
	t.Helper()
	content, err := os.ReadFile(sampleManifestPath(t, name))
	if err != nil {
		t.Fatalf("read sample manifest %s: %v", name, err)
	}
	var vp v1alpha1.VolgaPipeline
	if err := yaml.Unmarshal(content, &vp); err != nil {
		t.Fatalf("unmarshal sample manifest %s: %v", name, err)
	}
	return vp.Spec.PipelineSpec
}

func sampleManifestPath(t *testing.T, name string) string {
	t.Helper()
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("failed to resolve caller file")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(thisFile), "..", "..", "config", "samples", name))
}
