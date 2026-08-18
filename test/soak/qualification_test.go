package soak_test

import (
	"os"
	"path/filepath"
	"testing"
)

func requireSoak(t *testing.T, taskVariable string) {
	t.Helper()
	if testing.Short() {
		t.Skip("soak qualification is excluded from the short test tier")
	}
	if os.Getenv("RELAY_SOAK") != "1" &&
		os.Getenv(taskVariable) != "1" &&
		os.Getenv("RELAY_SOAK_FULL") != "1" {
		t.Skipf("set RELAY_SOAK=1, %s=1, or RELAY_SOAK_FULL=1", taskVariable)
	}
}

func writeSoakArtifact(t *testing.T, name string, contents []byte) {
	t.Helper()
	outputDirectory := os.Getenv("RELAY_SOAK_OUTPUT_DIR")
	if outputDirectory == "" {
		t.Logf("soak artifact %s was not persisted; set RELAY_SOAK_OUTPUT_DIR", name)
		return
	}
	if err := os.MkdirAll(outputDirectory, 0o755); err != nil {
		t.Fatalf("create soak output directory: %v", err)
	}
	if err := os.WriteFile(filepath.Join(outputDirectory, name), contents, 0o644); err != nil {
		t.Fatalf("write soak artifact %s: %v", name, err)
	}
}
