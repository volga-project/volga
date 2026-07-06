package controller

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"kubevolga/api/v1alpha1"

	"sigs.k8s.io/yaml"
)

func TestSamplePipelineSpecValidates(t *testing.T) {
	samplePath := sampleManifestPath(t)
	content, err := os.ReadFile(samplePath)
	if err != nil {
		t.Fatalf("read sample manifest: %v", err)
	}

	var vp v1alpha1.VolgaPipeline
	if err := yaml.Unmarshal(content, &vp); err != nil {
		t.Fatalf("unmarshal sample manifest: %v", err)
	}

	if err := validatePipelineSpec(vp.Spec.PipelineSpec); err != nil {
		t.Fatalf("sample pipelineSpec should be valid, got error: %v", err)
	}
}

func sampleManifestPath(t *testing.T) string {
	t.Helper()
	_, thisFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("failed to resolve caller file")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(thisFile), "..", "..", "config", "samples", "volga_v1alpha1_pipeline.yaml"))
}
