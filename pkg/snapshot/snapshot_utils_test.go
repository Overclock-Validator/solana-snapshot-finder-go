package snapshot

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/Overclock-Validator/solana-snapshot-finder-go/pkg/config"
)

// -----------------------------
// ExtractFullSnapshotSlot extra tests
// -----------------------------
func TestExtractFullSnapshotSlot_MissingExtension(t *testing.T) {
	_, err := ExtractFullSnapshotSlot("snapshot-12345-abc")
	if err == nil {
		t.Fatalf("expected error for missing extension, got nil")
	}
}

func TestExtractFullSnapshotSlot_AtoiOverflow(t *testing.T) {
	// Very large number should cause Atoi error
	veryLarge := "snapshot-" + strconv.FormatInt(1<<62, 10) + "000000000000000000-abc.tar.zst"
	_, err := ExtractFullSnapshotSlot(veryLarge)
	if err == nil {
		t.Fatalf("expected error for too-large slot number, got nil")
	}
}

func TestExtractFullSnapshotSlot_MixedCaseID(t *testing.T) {
	got, err := ExtractFullSnapshotSlot("snapshot-42-AbC123xyz.tar.zst")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != 42 {
		t.Fatalf("expected 42 got %d", got)
	}
}

// -----------------------------
// ExtractIncrementalSnapshotSlots extra tests
// -----------------------------
func TestExtractIncrementalSnapshotSlots_MissingSlotParts(t *testing.T) {
	_, _, err := ExtractIncrementalSnapshotSlots("incremental-snapshot-100--abc.tar.zst")
	if err == nil {
		t.Fatalf("expected error for missing slot part, got nil")
	}
}

func TestExtractIncrementalSnapshotSlots_InvalidLargeNumber(t *testing.T) {
	_, _, err := ExtractIncrementalSnapshotSlots("incremental-snapshot-1-99999999999999999999999999999-a.tar.zst")
	if err == nil {
		t.Fatalf("expected parse error for huge number, got nil")
	}
}

func TestExtractIncrementalSnapshotSlots_NegativeNumbers(t *testing.T) {
	// negative numbers should not match the regex; expect error
	_, _, err := ExtractIncrementalSnapshotSlots("incremental-snapshot--100-200-a.tar.zst")
	if err == nil {
		t.Fatalf("expected error for negative slot, got nil")
	}
}

// -----------------------------
// findRecentFullSnapshot tests (error / edge)
// -----------------------------
func TestFindRecentFullSnapshot_NoDirs(t *testing.T) {
	// Use a temp file path instead of directory to force os.ReadDir error
	tmp := t.TempDir()
	// create a file and pass its path to function to provoke ReadDir error
	f := filepath.Join(tmp, "notadir")
	if err := os.WriteFile(f, []byte("x"), 0o644); err != nil {
		t.Fatalf("write file: %v", err)
	}

	_, _, err := findRecentFullSnapshot(f, 0, 0)
	if err == nil {
		t.Fatalf("expected error when path is not a directory, got nil")
	}
}

func TestFindRecentFullSnapshot_Empty(t *testing.T) {
	dir := t.TempDir()
	// empty dir should result in "no full snapshots found" error
	_, _, err := findRecentFullSnapshot(dir, 0, 0)
	if err == nil {
		t.Fatalf("expected error for no snapshots found, got nil")
	}
}

func TestFindRecentFullSnapshot_NoMatchingFiles(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "file.txt"), []byte("a"), 0o644)
	writeFile(t, filepath.Join(dir, "remote", "another.json"), []byte("b"), 0o644)

	_, _, err := findRecentFullSnapshot(dir, 0, 0)
	if err == nil {
		t.Fatalf("expected error for no matching snapshot files, got nil")
	}
}

func TestFindRecentFullSnapshot_MultipleSelection(t *testing.T) {
	dir := t.TempDir()

	writeFile(t, filepath.Join(dir, "snapshot-10-a.tar.zst"), []byte("a"), 0o644)
	writeFile(t, filepath.Join(dir, "snapshot-200-b.tar.bz2"), []byte("b"), 0o644)
	writeFile(t, filepath.Join(dir, "remote", "snapshot-150-c.tar.zst"), []byte("c"), 0o644)

	most, slot, err := findRecentFullSnapshot(dir, 0, 0)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if slot != 200 {
		t.Fatalf("expected slot 200 got %d", slot)
	}
	if filepath.Base(most) != "snapshot-200-b.tar.bz2" {
		t.Fatalf("expected snapshot-200-b.tar.bz2 got %s", most)
	}
}

func TestFindRecentFullSnapshot_RemoteOnly(t *testing.T) {
	dir := t.TempDir()

	writeFile(t, filepath.Join(dir, "remote", "snapshot-999-r.tar.zst"), []byte("r"), 0o644)

	most, slot, err := findRecentFullSnapshot(dir, 0, 0)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if slot != 999 {
		t.Fatalf("expected slot 999 got %d", slot)
	}
	if filepath.Base(most) != "snapshot-999-r.tar.zst" {
		t.Fatalf("unexpected file: %s", most)
	}
}

// -----------------------------
// findHighestIncrementalSnapshot tests
// -----------------------------
func TestFindHighestIncrementalSnapshot_NoSnapshots(t *testing.T) {
	dir := t.TempDir()
	_, err := findHighestIncrementalSnapshot(dir, 100)
	if err == nil {
		t.Fatalf("expected error when there are no incremental snapshots")
	}
}

func TestFindHighestIncrementalSnapshot_NoMatchingBase(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "incremental-snapshot-50-60-a.tar.zst"), []byte("x"), 0o644)
	_, err := findHighestIncrementalSnapshot(dir, 100)
	if err == nil {
		t.Fatalf("expected error when no incremental starts with baseSlot")
	}
}

func TestFindHighestIncrementalSnapshot_SameEndSlot(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "incremental-snapshot-100-200-a.tar.zst"), []byte("x"), 0o644)
	writeFile(t, filepath.Join(dir, "remote", "incremental-snapshot-100-200-b.tar.zst"), []byte("y"), 0o644)

	inc, err := findHighestIncrementalSnapshot(dir, 100)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if inc.SlotEnd != 200 {
		t.Fatalf("expected SlotEnd 200 got %d", inc.SlotEnd)
	}
	// Ensure file path returned may be either; just ensure SlotStart matches
	if inc.SlotStart != 100 {
		t.Fatalf("expected SlotStart 100 got %d", inc.SlotStart)
	}
}

func TestFindHighestIncrementalSnapshot_RemoteOnly(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "remote", "incremental-snapshot-300-350-r.tar.zst"), []byte("a"), 0o644)

	inc, err := findHighestIncrementalSnapshot(dir, 300)
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if inc.SlotEnd != 350 {
		t.Fatalf("expected 350 got %d", inc.SlotEnd)
	}
}

// Force os.ReadDir error by passing a file path instead of directory
func TestFindHighestIncrementalSnapshot_ReadDirError(t *testing.T) {
	tmp := t.TempDir()
	f := filepath.Join(tmp, "notadir")
	writeFile(t, f, []byte("x"), 0o644)

	_, err := findHighestIncrementalSnapshot(f, 0)
	if err == nil {
		t.Fatalf("expected readdir error when base path is a file")
	}
}

// -----------------------------
// IsGenesisPresent tests
// -----------------------------
func TestIsGenesisPresent_FileMissing(t *testing.T) {
	dir := t.TempDir()
	if IsGenesisPresent(dir) {
		t.Fatalf("expected false when genesis is missing")
	}
}

func TestIsGenesisPresent_WrongName(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "genesis.tar.gz"), []byte("x"), 0o644)
	if IsGenesisPresent(dir) {
		t.Fatalf("expected false when wrong filename exists")
	}
}

// -----------------------------
// copyFile tests (error branches)
// -----------------------------
func TestCopyFile_SourceMissing(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, "noexist")
	dst := filepath.Join(dir, "dst")
	if err := copyFile(src, dst); err == nil {
		t.Fatalf("expected error when source missing")
	}
}

func TestCopyFile_DstIsDirectory(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, "src.txt")
	writeFile(t, src, []byte("hello"), 0o600)
	// create dst as directory
	dstDir := filepath.Join(dir, "dstdir")
	if err := os.MkdirAll(dstDir, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	// attempt to copy to directory path
	if err := copyFile(src, dstDir); err == nil {
		t.Fatalf("expected error when destination is a directory (OpenFile on dir should fail)")
	}
}

func TestCopyFile_OverwriteExisting(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, "src.txt")
	dst := filepath.Join(dir, "dst.txt")
	writeFile(t, src, []byte("hello1"), 0o600)
	writeFile(t, dst, []byte("old"), 0o600)

	if err := copyFile(src, dst); err != nil {
		t.Fatalf("copyFile failed: %v", err)
	}
	data, err := os.ReadFile(dst)
	if err != nil {
		t.Fatalf("read dst failed: %v", err)
	}
	if !bytes.Equal(data, []byte("hello1")) {
		t.Fatalf("expected overwritten content, got %s", string(data))
	}
}

// -----------------------------
// ManageSnapshots tests to cover many branches
// -----------------------------
func TestManageSnapshots_NoFullSnapshot(t *testing.T) {
	dir := t.TempDir()

	cfg := config.Config{
		SnapshotPath:         dir,
		FullThreshold:        10,
		IncrementalThreshold: 10,
	}

	needFull, needInc := ManageSnapshots(cfg, 1000)
	if !needFull || !needInc {
		t.Fatalf("expected needFull=true needInc=true when no full snapshot (got %v,%v)", needFull, needInc)
	}
}

func TestManageSnapshots_FullOutdated(t *testing.T) {
	dir := t.TempDir()
	// create an old full snapshot with slot 10
	writeFile(t, filepath.Join(dir, "snapshot-10-a.tar.zst"), []byte("f"), 0o644)

	cfg := config.Config{
		SnapshotPath:         dir,
		FullThreshold:        5, // threshold smaller than diff
		IncrementalThreshold: 100,
	}

	needFull, needInc := ManageSnapshots(cfg, 1000) // referenceSlot much larger
	if !needFull || !needInc {
		t.Fatalf("expected both true when full outdated (got %v,%v)", needFull, needInc)
	}
}

func TestManageSnapshots_NoIncrementalFound(t *testing.T) {
	dir := t.TempDir()
	// create a recent full snapshot slot 1000
	writeFile(t, filepath.Join(dir, "snapshot-1000-a.tar.zst"), []byte("f"), 0o644)

	cfg := config.Config{
		SnapshotPath:         dir,
		FullThreshold:        10000,
		IncrementalThreshold: 5,
	}

	needFull, needInc := ManageSnapshots(cfg, 1005) // referenceSlot close, but no incrementals
	// When there is no incremental, function returns (false, true)
	if needFull == true || needInc == false {
		t.Fatalf("expected (false,true) when no incremental found (got %v,%v)", needFull, needInc)
	}
}

func TestManageSnapshots_IncrementalOutdated(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "snapshot-1000-a.tar.zst"), []byte("f"), 0o644)
	// incremental ends at 1010 while ref is 2000; threshold small
	writeFile(t, filepath.Join(dir, "incremental-snapshot-1000-1010-a.tar.zst"), []byte("i"), 0o644)

	cfg := config.Config{
		SnapshotPath:         dir,
		FullThreshold:        10000,
		IncrementalThreshold: 10,
	}

	needFull, needInc := ManageSnapshots(cfg, 2000)
	// full is OK, incremental outdated => (false,true)
	if needFull == true || needInc == false {
		t.Fatalf("expected (false,true) when incremental outdated (got %v,%v)", needFull, needInc)
	}
}

func TestManageSnapshots_IncrementalStartMismatch(t *testing.T) {
	dir := t.TempDir()
	// full snapshot slot 1000
	writeFile(t, filepath.Join(dir, "snapshot-1000-a.tar.zst"), []byte("f"), 0o644)
	// incremental starting from 999 (wrong start)
	writeFile(t, filepath.Join(dir, "incremental-snapshot-999-1200-a.tar.zst"), []byte("i"), 0o644)

	cfg := config.Config{
		SnapshotPath:         dir,
		FullThreshold:        10000,
		IncrementalThreshold: 10000,
	}

	needFull, needInc := ManageSnapshots(cfg, 1300)
	// incremental doesn't start at fullSlot => should return (false, true)
	if needFull == true || needInc == false {
		t.Fatalf("expected (false,true) when incremental start mismatch (got %v,%v)", needFull, needInc)
	}
}

func TestManageSnapshots_FullInRemote_IncInLocal(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "remote", "snapshot-500-a.tar.zst"), []byte("f"), 0o644)
	writeFile(t, filepath.Join(dir, "incremental-snapshot-500-550-a.tar.zst"), []byte("i"), 0o644)

	cfg := config.Config{
		SnapshotPath:         dir,
		FullThreshold:        10000,
		IncrementalThreshold: 10000,
	}

	needFull, needInc := ManageSnapshots(cfg, 560)
	if needFull || needInc {
		t.Fatalf("expected false,false when full in remote and incremental local OK (got %v,%v)", needFull, needInc)
	}
}

func TestManageSnapshots_ReferenceLessThanFull(t *testing.T) {
	dir := t.TempDir()
	// full snapshot slot 1000
	writeFile(t, filepath.Join(dir, "snapshot-1000-a.tar.zst"), []byte("f"), 0o644)
	// incremental extends to 1100
	writeFile(t, filepath.Join(dir, "incremental-snapshot-1000-1100-a.tar.zst"), []byte("i"), 0o644)

	cfg := config.Config{
		SnapshotPath:         dir,
		FullThreshold:        10,
		IncrementalThreshold: 10,
	}

	// referenceSlot less than fullSlot
	needFull, needInc := ManageSnapshots(cfg, 900)
	if needFull || needInc {
		t.Fatalf("expected false,false when referenceSlot < fullSlot (got %v,%v)", needFull, needInc)
	}
}

// -----------------------------
// UntarGenesis tests - error flows
// -----------------------------
func TestUntarGenesis_MissingFile(t *testing.T) {
	dir := t.TempDir()
	if err := UntarGenesis(dir); err == nil {
		t.Fatalf("expected error when genesis missing")
	}
}

func TestUntarGenesis_CorruptedTar(t *testing.T) {
	dir := t.TempDir()
	// create a corrupted .tar.bz2 file (random bytes)
	writeFile(t, filepath.Join(dir, "genesis.tar.bz2"), []byte("not a tar"), 0o644)

	err := UntarGenesis(dir)
	if err == nil {
		t.Fatalf("expected error when tar is corrupted")
	}
	// It should fail running "tar", verify error contains some indication (not required but helpful)
}

// Optional sanity: ensure UntarGenesis succeeds for a valid tar (already present in main tests).
func TestUntarGenesis_ValidTar(t *testing.T) {
	dir := t.TempDir()
	contentDir := filepath.Join(dir, "content")
	if err := os.MkdirAll(contentDir, 0o755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	genesisFile := filepath.Join(contentDir, "genesis.txt")
	if err := os.WriteFile(genesisFile, []byte("genesis data"), 0o644); err != nil {
		t.Fatalf("write genesis: %v", err)
	}
	genesisTar := filepath.Join(dir, "genesis.tar.bz2")
	cmd := exec.Command("tar", "-cjf", genesisTar, "-C", contentDir, "genesis.txt")
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("failed to create genesis tar: %v output: %s", err, string(out))
	}

	if err := UntarGenesis(dir); err != nil {
		t.Fatalf("UntarGenesis failed on valid tar: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "genesis.txt")); err != nil {
		t.Fatalf("expected extracted file present: %v", err)
	}
}
