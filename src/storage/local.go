package storage

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
	"uuid"
)

const (
	tmpDir        = "tmp"
	sourcesDir    = "sources"
	renditionsDir = "renditions"
	tempPrefix    = "part-"
	renditionExt  = ".m4a"
	dirPerm       = 0o750
)

var ErrInvalidProfile = errors.New("invalid rendition profile")

type Local struct {
	root string
}

func NewLocal(root string) *Local {
	return &Local{root: filepath.Clean(root)}
}

// TempFile creates root/tmp/part-*, on the same filesystem as the final paths so Promote is an atomic rename.
func (l *Local) TempFile() (*os.File, error) {
	dir := filepath.Join(l.root, tmpDir)
	if err := os.MkdirAll(dir, dirPerm); err != nil {
		return nil, fmt.Errorf("error creating temp directory: %w", err)
	}
	f, err := os.CreateTemp(dir, tempPrefix+"*")
	if err != nil {
		return nil, fmt.Errorf("error creating temp file: %w", err)
	}
	return f, nil
}

func (l *Local) SourcePath(tenantID uint64, assetID uuid.UUID) string {
	return filepath.Join(l.root, sourcesDir, strconv.FormatUint(tenantID, 10), assetID.String())
}

func (l *Local) RenditionPath(tenantID uint64, assetID uuid.UUID, profile string) (string, error) {
	if profile == "" || profile == "." || profile == ".." || strings.ContainsAny(profile, "/\\\x00") {
		return "", fmt.Errorf("%w: %q", ErrInvalidProfile, profile)
	}
	return filepath.Join(l.root, renditionsDir, strconv.FormatUint(tenantID, 10), assetID.String(),
		profile+renditionExt), nil
}

// Promote renames tmp to final, atomically replacing any file already there.
func (l *Local) Promote(tmp, final string) error {
	if err := os.MkdirAll(filepath.Dir(final), dirPerm); err != nil {
		return fmt.Errorf("error creating directory for promote: %w", err)
	}
	if err := os.Rename(tmp, final); err != nil {
		return fmt.Errorf("error promoting file: %w", err)
	}
	return nil
}

func (l *Local) Open(path string) (io.ReadSeekCloser, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("error opening file: %w", err)
	}
	return f, nil
}

func (l *Local) Remove(path string) error {
	if err := os.Remove(path); err != nil {
		return fmt.Errorf("error removing file: %w", err)
	}
	return nil
}

// SweepTemp removes the regular part-* files directly in root/tmp that were last modified more than olderThan ago,
// and returns how many it removed. It leaves symlinks and directories alone, and skips files that vanish meanwhile,
// since every process sharing the root sweeps at startup.
func (l *Local) SweepTemp(olderThan time.Duration) (int, error) {
	dir := filepath.Join(l.root, tmpDir)
	entries, err := os.ReadDir(dir)
	if errors.Is(err, fs.ErrNotExist) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("error reading temp directory: %w", err)
	}

	cutoff := time.Now().Add(-olderThan)
	removed := 0
	var errs []error
	for _, e := range entries {
		if !strings.HasPrefix(e.Name(), tempPrefix) || !e.Type().IsRegular() {
			continue
		}
		info, err := e.Info()
		if errors.Is(err, fs.ErrNotExist) {
			continue
		}
		if err != nil {
			errs = append(errs, fmt.Errorf("error reading temp file: %w", err))
			continue
		}
		if !info.Mode().IsRegular() || !info.ModTime().Before(cutoff) {
			continue
		}
		err = os.Remove(filepath.Join(dir, e.Name()))
		if errors.Is(err, fs.ErrNotExist) {
			continue
		}
		if err != nil {
			errs = append(errs, fmt.Errorf("error removing temp file: %w", err))
			continue
		}
		removed++
	}
	return removed, errors.Join(errs...)
}
