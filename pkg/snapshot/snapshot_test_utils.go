package snapshot

import (
	"os"
	"path/filepath"
	"testing"
)

func writeFile(t *testing.T, path string, content []byte, perm os.FileMode) {
	t.Helper()

	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("mkdirall %s: %v", path, err)
	}

	if err := os.WriteFile(path, content, perm); err != nil {
		t.Fatalf("writefile %s: %v", path, err)
	}
}
