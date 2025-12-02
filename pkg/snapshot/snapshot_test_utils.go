package snapshot

import (
	"net/http/httptest"
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

// tsURLPath constructs a URL for a given httptest.Server and path.
func tsURLPath(tsURL string, path string) string {
	return tsURL + path
}

// ts returns a pointer to a fake server to allow closures above to compile.
// This is a small helper abused in one test closure to produce URLs using ts.URL inside the handler.
// In normal usage we pass ts to tsURLPath. This function is only used to satisfy the closure variable in one test.
func ts() *httptest.Server { return &httptest.Server{} }
