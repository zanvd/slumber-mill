package storage

import (
	"errors"
	"io"
	"io/fs"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"testing/synctest"
	"time"
	"uuid"
)

const testAssetID = "01890a5d-ac96-774b-bcce-b302099a8057"

func TestPaths(t *testing.T) {
	root := t.TempDir()
	l := NewLocal(root)
	asset := uuid.MustParse(testAssetID)

	tests := []struct {
		name string
		path func() (string, error)
		want string
	}{
		{
			name: "source",
			path: func() (string, error) { return l.SourcePath(42, asset), nil },
			want: filepath.Join(root, "sources", "42", testAssetID),
		},
		{
			name: "source of the largest tenant ID",
			path: func() (string, error) { return l.SourcePath(math.MaxUint64, asset), nil },
			want: filepath.Join(root, "sources", "18446744073709551615", testAssetID),
		},
		{
			name: "source of an ID parsed from upper case",
			path: func() (string, error) { return l.SourcePath(42, uuid.MustParse(strings.ToUpper(testAssetID))), nil },
			want: filepath.Join(root, "sources", "42", testAssetID),
		},
		{
			name: "source under an unclean root",
			path: func() (string, error) { return NewLocal(root+"/./").SourcePath(42, asset), nil },
			want: filepath.Join(root, "sources", "42", testAssetID),
		},
		{
			name: "rendition aac_128k",
			path: func() (string, error) { return l.RenditionPath(42, asset, "aac_128k") },
			want: filepath.Join(root, "renditions", "42", testAssetID, "aac_128k.m4a"),
		},
		{
			name: "rendition aac_64k",
			path: func() (string, error) { return l.RenditionPath(42, asset, "aac_64k") },
			want: filepath.Join(root, "renditions", "42", testAssetID, "aac_64k.m4a"),
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := tc.path()
			if err != nil {
				t.Fatalf("error = %v", err)
			}
			if got != tc.want {
				t.Errorf("path = %q, want %q", got, tc.want)
			}
			if rel, err := filepath.Rel(root, got); err != nil || !filepath.IsLocal(rel) {
				t.Errorf("path %q escapes the root %q", got, root)
			}
		})
	}
}

func TestRenditionPathRefusesProfile(t *testing.T) {
	l := NewLocal(t.TempDir())
	asset := uuid.MustParse(testAssetID)

	tests := []struct {
		name    string
		profile string
	}{
		{"empty", ""},
		{"dot", "."},
		{"dot dot", ".."},
		{"slash", "aac/128k"},
		{"absolute", "/etc/passwd"},
		{"climbing out of the root", "../../../etc/passwd"},
		{"climbing one level", "../aac_128k"},
		{"ending in dot dot", "aac_128k/.."},
		{"backslash", `..\aac_128k`},
		{"NUL", "aac\x00128k"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := l.RenditionPath(42, asset, tc.profile)
			if !errors.Is(err, ErrInvalidProfile) {
				t.Errorf("RenditionPath(%q) error = %v, want ErrInvalidProfile", tc.profile, err)
			}
			if got != "" {
				t.Errorf("RenditionPath(%q) = %q, want an empty path", tc.profile, got)
			}
		})
	}
}

func TestTempFile(t *testing.T) {
	root := t.TempDir()
	l := NewLocal(root)

	seen := make(map[string]bool)
	for i := range 2 {
		f, err := l.TempFile()
		if err != nil {
			t.Fatalf("TempFile() #%d error = %v", i, err)
		}
		name := f.Name()
		if _, err := f.WriteString("data"); err != nil {
			t.Fatalf("writing %s: %v", name, err)
		}
		if err := f.Close(); err != nil {
			t.Fatalf("closing %s: %v", name, err)
		}

		if dir := filepath.Dir(name); dir != filepath.Join(root, "tmp") {
			t.Errorf("TempFile() #%d is in %q, want %q", i, dir, filepath.Join(root, "tmp"))
		}
		if base := filepath.Base(name); !strings.HasPrefix(base, "part-") || base == "part-" {
			t.Errorf("TempFile() #%d is named %q, want part-*", i, base)
		}
		if info, err := os.Lstat(name); err != nil || !info.Mode().IsRegular() {
			t.Errorf("TempFile() #%d: Lstat() = %v, %v; want a regular file", i, info, err)
		}
		if seen[name] {
			t.Errorf("TempFile() #%d reused %q", i, name)
		}
		seen[name] = true
	}
}

func TestPromote(t *testing.T) {
	tests := []struct {
		name       string
		existing   string
		promotions []string
	}{
		{name: "creates parent directories", promotions: []string{"rendition"}},
		{name: "replaces an existing file", existing: "old", promotions: []string{"new"}},
		{name: "same output twice", promotions: []string{"identical", "identical"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			l := NewLocal(t.TempDir())
			final, err := l.RenditionPath(7, uuid.MustParse(testAssetID), "aac_128k")
			if err != nil {
				t.Fatalf("RenditionPath() error = %v", err)
			}
			if tc.existing != "" {
				if err := os.MkdirAll(filepath.Dir(final), 0o750); err != nil {
					t.Fatal(err)
				}
				writeFile(t, final, tc.existing)
			}

			for _, content := range tc.promotions {
				tmp := writeTemp(t, l, content)
				if err := l.Promote(tmp, final); err != nil {
					t.Fatalf("Promote() error = %v", err)
				}
				if exists(t, tmp) {
					t.Errorf("%s still exists after Promote()", tmp)
				}
				if got := readFile(t, final); got != content {
					t.Errorf("final file holds %q, want %q", got, content)
				}
			}
		})
	}
}

func TestOpen(t *testing.T) {
	l := NewLocal(t.TempDir())
	path, err := l.RenditionPath(7, uuid.MustParse(testAssetID), "aac_64k")
	if err != nil {
		t.Fatalf("RenditionPath() error = %v", err)
	}
	if err := l.Promote(writeTemp(t, l, "0123456789"), path); err != nil {
		t.Fatalf("Promote() error = %v", err)
	}

	r, err := l.Open(path)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if size, err := r.Seek(0, io.SeekEnd); err != nil || size != 10 {
		t.Errorf("Seek(0, io.SeekEnd) = %d, %v; want 10, nil", size, err)
	}
	if _, err := r.Seek(4, io.SeekStart); err != nil {
		t.Fatalf("Seek(4, io.SeekStart) error = %v", err)
	}
	if got, err := io.ReadAll(r); err != nil || string(got) != "456789" {
		t.Errorf("reading from offset 4 = %q, %v; want %q, nil", got, err, "456789")
	}
	if err := r.Close(); err != nil {
		t.Errorf("Close() error = %v", err)
	}
}

func TestRemove(t *testing.T) {
	l := NewLocal(t.TempDir())
	path := l.SourcePath(7, uuid.MustParse(testAssetID))
	if err := l.Promote(writeTemp(t, l, "source"), path); err != nil {
		t.Fatalf("Promote() error = %v", err)
	}

	if err := l.Remove(path); err != nil {
		t.Fatalf("Remove() error = %v", err)
	}
	if exists(t, path) {
		t.Errorf("%s still exists after Remove()", path)
	}
}

func TestMissingFile(t *testing.T) {
	l := NewLocal(t.TempDir())
	asset := uuid.MustParse(testAssetID)
	source := l.SourcePath(7, asset)
	rendition, err := l.RenditionPath(7, asset, "aac_128k")
	if err != nil {
		t.Fatalf("RenditionPath() error = %v", err)
	}

	tests := []struct {
		name string
		call func() error
	}{
		{"open", func() error {
			r, err := l.Open(rendition)
			if r != nil {
				t.Errorf("Open() returned a reader for a missing file")
			}
			return err
		}},
		{"remove", func() error { return l.Remove(source) }},
		{"promote", func() error { return l.Promote(filepath.Join(filepath.Dir(source), "part-missing"), source) }},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.call(); !errors.Is(err, fs.ErrNotExist) {
				t.Errorf("error = %v, want fs.ErrNotExist", err)
			}
		})
	}
}

func TestSweepTemp(t *testing.T) {
	const olderThan = time.Hour

	tests := []struct {
		name     string
		age      time.Duration
		create   func(t *testing.T, path, outside string, mtime time.Time) (target string)
		wantKept bool
	}{
		{"part-old", 2 * time.Hour, regularFile, false},
		{"part-just-over", olderThan + time.Second, regularFile, false},
		{"part-at-the-boundary", olderThan, regularFile, true},
		{"part-just-under", olderThan - time.Second, regularFile, true},
		{"part-new", 0, regularFile, true},
		{"upload-old", 2 * time.Hour, regularFile, true},
		{"part-dir", 2 * time.Hour, directory, true},
		{"part-link-to-file", 2 * time.Hour, symlinkToFile, true},
		{"part-link-to-dir", 2 * time.Hour, symlinkToDir, true},
	}

	var (
		removed    int
		sweepErr   error
		entryKept  = make(map[string]bool)
		targetKept = make(map[string]bool)
	)
	synctest.Test(t, func(t *testing.T) {
		root, outside := t.TempDir(), t.TempDir()
		tmp := filepath.Join(root, "tmp")
		if err := os.Mkdir(tmp, 0o750); err != nil {
			t.Fatal(err)
		}
		// New entries get the real time, decades after the bubble's clock, and os.Chtimes can't backdate
		// a symlink itself. Moving the clock a day past the real time makes the links old too.
		info, err := os.Lstat(tmp)
		if err != nil {
			t.Fatal(err)
		}
		time.Sleep(time.Until(info.ModTime()) + 24*time.Hour)
		now := time.Now()
		targets := make(map[string]string)
		for _, tc := range tests {
			targets[tc.name] = tc.create(t, filepath.Join(tmp, tc.name), outside, now.Add(-tc.age))
		}

		removed, sweepErr = NewLocal(root).SweepTemp(olderThan)

		for _, tc := range tests {
			entryKept[tc.name] = exists(t, filepath.Join(tmp, tc.name))
			if target := targets[tc.name]; target != "" {
				targetKept[tc.name] = exists(t, target)
			}
		}
	})

	if sweepErr != nil {
		t.Fatalf("SweepTemp() error = %v", sweepErr)
	}
	wantRemoved := 0
	for _, tc := range tests {
		if !tc.wantKept {
			wantRemoved++
		}
	}
	if removed != wantRemoved {
		t.Errorf("SweepTemp() removed %d files, want %d", removed, wantRemoved)
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := entryKept[tc.name]; got != tc.wantKept {
				t.Errorf("kept = %t, want %t", got, tc.wantKept)
			}
			if kept, ok := targetKept[tc.name]; ok && !kept {
				t.Errorf("the sweep removed what %s points to", tc.name)
			}
		})
	}
}

func TestSweepTempRemovesOldTempFiles(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		l := NewLocal(t.TempDir())
		f, err := l.TempFile()
		if err != nil {
			t.Fatalf("TempFile() error = %v", err)
		}
		if err := f.Close(); err != nil {
			t.Fatal(err)
		}
		old := time.Now().Add(-2 * time.Hour)
		if err := os.Chtimes(f.Name(), old, old); err != nil {
			t.Fatal(err)
		}

		removed, err := l.SweepTemp(time.Hour)
		if err != nil || removed != 1 {
			t.Errorf("SweepTemp() = %d, %v; want 1, nil", removed, err)
		}
		if exists(t, f.Name()) {
			t.Errorf("%s still exists after SweepTemp()", f.Name())
		}
	})
}

func TestSweepTempWithoutTempDir(t *testing.T) {
	removed, err := NewLocal(t.TempDir()).SweepTemp(time.Hour)
	if err != nil || removed != 0 {
		t.Errorf("SweepTemp() = %d, %v; want 0, nil", removed, err)
	}
}

func regularFile(t *testing.T, path, _ string, mtime time.Time) string {
	writeFile(t, path, "x")
	chtimes(t, path, mtime)
	return ""
}

func directory(t *testing.T, path, _ string, mtime time.Time) string {
	if err := os.Mkdir(path, 0o750); err != nil {
		t.Fatal(err)
	}
	inner := filepath.Join(path, "part-inner")
	writeFile(t, inner, "x")
	chtimes(t, inner, mtime)
	chtimes(t, path, mtime)
	return inner
}

func symlinkToFile(t *testing.T, path, outside string, mtime time.Time) string {
	target := filepath.Join(outside, filepath.Base(path))
	writeFile(t, target, "x")
	chtimes(t, target, mtime)
	symlink(t, target, path, mtime)
	return target
}

func symlinkToDir(t *testing.T, path, outside string, mtime time.Time) string {
	dir := filepath.Join(outside, filepath.Base(path))
	inner := directory(t, dir, "", mtime)
	symlink(t, dir, path, mtime)
	return inner
}

func writeTemp(t *testing.T, l *Local, content string) string {
	t.Helper()
	f, err := l.TempFile()
	if err != nil {
		t.Fatalf("TempFile() error = %v", err)
	}
	if _, err := f.WriteString(content); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	return f.Name()
}

func writeFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
}

func readFile(t *testing.T, path string) string {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func chtimes(t *testing.T, path string, mtime time.Time) {
	t.Helper()
	if err := os.Chtimes(path, mtime, mtime); err != nil {
		t.Fatal(err)
	}
}

// symlink fails the test unless the link's own mtime is at or before maxMtime.
func symlink(t *testing.T, target, path string, maxMtime time.Time) {
	t.Helper()
	if err := os.Symlink(target, path); err != nil {
		t.Fatal(err)
	}
	info, err := os.Lstat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.ModTime().After(maxMtime) {
		t.Fatalf("symlink %s has mtime %v, after %v", path, info.ModTime(), maxMtime)
	}
}

func exists(t *testing.T, path string) bool {
	t.Helper()
	_, err := os.Lstat(path)
	if errors.Is(err, fs.ErrNotExist) {
		return false
	}
	if err != nil {
		t.Fatal(err)
	}
	return true
}
