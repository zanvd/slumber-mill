package asset

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"os"
	"strings"
	"time"
	"unicode/utf8"
	"uuid"

	"github.com/zanvd/slumber-mill/src/logging"
	"github.com/zanvd/slumber-mill/src/validation"
)

const (
	// DefaultListLimit is the page size the API uses when a request doesn't give one.
	DefaultListLimit = 20
	MaxListLimit     = 100

	maxTitleChars          = 255
	maxIdempotencyKeyBytes = 255

	// defaultInsertTimeout bounds the upload's insert, which doesn't stop when the request's context does:
	// a client hanging up mid-insert mustn't make a committed row look failed and lose its source.
	defaultInsertTimeout = 15 * time.Second
)

type repo interface {
	insertAsset(ctx context.Context, a Asset) error
	assetByID(ctx context.Context, tenantID uint64, id uuid.UUID) (Asset, error)
	assetByIdempotencyKey(ctx context.Context, tenantID uint64, key string) (Asset, error)
	listAssets(ctx context.Context, tenantID, before uint64, limit int) ([]Asset, uint64, error)
	rendition(ctx context.Context, tenantID uint64, assetID uuid.UUID, profile Profile) (Rendition, error)
}

type fileStore interface {
	TempFile() (*os.File, error)
	SourcePath(tenantID uint64, assetID uuid.UUID) string
	RenditionPath(tenantID uint64, assetID uuid.UUID, profile string) (string, error)
	Promote(tmp, final string) error
	Open(path string) (io.ReadSeekCloser, error)
	Remove(path string) error
}

type Service struct {
	repo  repo
	files fileStore
	// insertTimeout is defaultInsertTimeout outside tests.
	insertTimeout time.Duration
}

func NewService(r repo, files fileStore) *Service {
	return &Service{repo: r, files: files, insertTimeout: defaultInsertTimeout}
}

// Upload is a file staged by Stage. Pass it to Create, which takes it over, or to Discard.
type Upload struct {
	path   string
	sha256 [sha256.Size]byte
	size   uint64
}

// Stage streams r to a temp file while hashing it. On error nothing is left behind.
func (s *Service) Stage(ctx context.Context, r io.Reader) (Upload, error) {
	f, err := s.files.TempFile()
	if err != nil {
		return Upload{}, fmt.Errorf("error staging upload: %w", err)
	}
	h := sha256.New()
	n, err := io.Copy(f, io.TeeReader(r, h))
	if closeErr := f.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		s.remove(ctx, f.Name())
		return Upload{}, fmt.Errorf("error staging upload: %w", err)
	}
	u := Upload{path: f.Name(), size: uint64(n)}
	h.Sum(u.sha256[:0])
	return u, nil
}

// Discard removes a staged upload that won't be passed to Create. Discarding twice is harmless.
func (s *Service) Discard(ctx context.Context, u Upload) {
	s.remove(ctx, u.path)
}

type CreateParams struct {
	TenantID uint64
	// TargetLUFS is the tenant's loudness target, which the asset keeps as it was at upload.
	TargetLUFS float64
	// Title is trimmed of surrounding whitespace before it is validated and stored.
	Title string
	// IdempotencyKey is empty when the request has none.
	IdempotencyKey string
	Upload         Upload
}

// Create registers a staged upload as a queued asset, or replays the asset an earlier upload
// with the same idempotency key created; replayed reports which.
// Create owns p.Upload: the file is removed unless it becomes the new asset's source.
func (s *Service) Create(ctx context.Context, p CreateParams) (a Asset, replayed bool, err error) {
	staged := p.Upload.path
	defer func() { s.remove(ctx, staged) }()

	p.Title = strings.TrimSpace(p.Title)
	if err := validateCreate(p); err != nil {
		return Asset{}, false, err
	}
	if p.IdempotencyKey != "" {
		existing, err := s.repo.assetByIdempotencyKey(ctx, p.TenantID, p.IdempotencyKey)
		if err == nil {
			return replay(existing, p)
		}
		if !errors.Is(err, ErrNotFound) {
			return Asset{}, false, fmt.Errorf("error looking up idempotency key: %w", err)
		}
	}

	id := uuid.NewV7()
	source := s.files.SourcePath(p.TenantID, id)
	if err := s.files.Promote(staged, source); err != nil {
		return Asset{}, false, fmt.Errorf("error promoting upload: %w", err)
	}
	staged = ""

	insertCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), s.insertTimeout)
	err = s.repo.insertAsset(insertCtx, Asset{
		ID:             id,
		TenantID:       p.TenantID,
		IdempotencyKey: p.IdempotencyKey,
		Title:          p.Title,
		Status:         StatusQueued,
		SourceSHA256:   p.Upload.sha256,
		SourceBytes:    p.Upload.size,
		TargetLUFS:     p.TargetLUFS,
	})
	cancel()
	if errors.Is(err, errOutcomeUnknown) {
		logging.LoggerFromContext(ctx).LogAttrs(ctx, slog.LevelWarn,
			"kept the source of an asset whose insert may have committed",
			slog.String("asset_id", id.String()), slog.String("error", err.Error()))
		return Asset{}, false, fmt.Errorf("error creating asset: %w", err)
	}
	if err != nil {
		s.remove(ctx, source)
		if !errors.Is(err, errDuplicate) || p.IdempotencyKey == "" {
			return Asset{}, false, fmt.Errorf("error creating asset: %w", err)
		}
		winner, err := s.repo.assetByIdempotencyKey(ctx, p.TenantID, p.IdempotencyKey)
		if err != nil {
			// %v: the asset must exist, so ErrNotFound here is an internal error, not a 404.
			return Asset{}, false, fmt.Errorf("error looking up idempotency key after a duplicate insert: %v", err)
		}
		return replay(winner, p)
	}

	created, err := s.repo.assetByID(ctx, p.TenantID, id)
	if err != nil {
		return Asset{}, false, fmt.Errorf("error reading created asset: %v", err)
	}
	return created, false, nil
}

func replay(existing Asset, p CreateParams) (Asset, bool, error) {
	if existing.Title != p.Title || existing.SourceSHA256 != p.Upload.sha256 {
		return Asset{}, false, ErrIdempotencyKeyReused
	}
	return existing, true, nil
}

func validateCreate(p CreateParams) error {
	var v validation.Collector
	switch {
	case p.Title == "":
		v.Add("title", "is required")
	case !utf8.ValidString(p.Title):
		v.Add("title", "must be valid UTF-8")
	case utf8.RuneCountInString(p.Title) > maxTitleChars:
		v.Add("title", "must be at most 255 characters")
	}
	if p.Upload.size == 0 {
		v.Add("file", "is empty")
	}
	key := p.IdempotencyKey
	switch {
	case len(key) > maxIdempotencyKeyBytes:
		v.Add("Idempotency-Key", "must be at most 255 bytes")
	case !isPrintableASCII(key):
		v.Add("Idempotency-Key", "must be printable ASCII")
	case strings.HasPrefix(key, " ") || strings.HasSuffix(key, " "):
		v.Add("Idempotency-Key", "must not start or end with a space")
	}
	return v.Err()
}

func isPrintableASCII(s string) bool {
	for i := range len(s) {
		if s[i] < ' ' || s[i] > '~' {
			return false
		}
	}
	return true
}

// Get returns the tenant's asset. A malformed ID, an unknown one and another tenant's asset are all ErrNotFound.
func (s *Service) Get(ctx context.Context, tenantID uint64, id string) (Asset, error) {
	assetID, err := uuid.Parse(id)
	if err != nil {
		return Asset{}, ErrNotFound
	}
	a, err := s.repo.assetByID(ctx, tenantID, assetID)
	if err != nil {
		return Asset{}, fmt.Errorf("error getting asset: %w", err)
	}
	return a, nil
}

type Page struct {
	Assets []Asset
	// NextCursor continues the listing, and is empty on the last page.
	NextCursor string
}

// List returns a page of the tenant's assets, newest first. limit must be 1 to MaxListLimit;
// the API applies DefaultListLimit when the request has none. cursor is empty for the first page,
// and otherwise a NextCursor from an earlier page.
func (s *Service) List(ctx context.Context, tenantID uint64, limit int, cursor string) (Page, error) {
	var v validation.Collector
	if limit < 1 || limit > MaxListLimit {
		v.Add("limit", "must be between 1 and 100")
	}
	before, ok := decodeCursor(cursor)
	if !ok {
		v.Add("cursor", "is invalid")
	}
	if err := v.Err(); err != nil {
		return Page{}, err
	}

	assets, next, err := s.repo.listAssets(ctx, tenantID, before, limit)
	if err != nil {
		return Page{}, fmt.Errorf("error listing assets: %w", err)
	}
	page := Page{Assets: assets}
	if next != 0 {
		page.NextCursor = encodeCursor(next)
	}
	return page, nil
}

// encodeCursor returns base64url of the big-endian internal ID.
func encodeCursor(id uint64) string {
	return base64.RawURLEncoding.EncodeToString(binary.BigEndian.AppendUint64(nil, id))
}

// decodeCursor returns 0 for the empty cursor, and false for anything encodeCursor didn't produce.
func decodeCursor(cursor string) (uint64, bool) {
	if cursor == "" {
		return 0, true
	}
	b, err := base64.RawURLEncoding.DecodeString(cursor)
	if err != nil || len(b) != 8 {
		return 0, false
	}
	id := binary.BigEndian.Uint64(b)
	return id, id != 0 && encodeCursor(id) == cursor
}

// OpenRendition returns a rendition of the tenant's asset and its file, which the caller closes.
// An unknown profile or asset, another tenant's asset and a missing rendition are all ErrNotFound.
func (s *Service) OpenRendition(ctx context.Context, tenantID uint64, id, profile string) (
	Rendition, io.ReadSeekCloser, error) {
	p := Profile(profile)
	if !p.known() {
		return Rendition{}, nil, ErrNotFound
	}
	assetID, err := uuid.Parse(id)
	if err != nil {
		return Rendition{}, nil, ErrNotFound
	}
	r, err := s.repo.rendition(ctx, tenantID, assetID, p)
	if err != nil {
		return Rendition{}, nil, fmt.Errorf("error getting rendition: %w", err)
	}
	path, err := s.files.RenditionPath(tenantID, assetID, string(p))
	if err != nil {
		return Rendition{}, nil, fmt.Errorf("error locating rendition: %w", err)
	}
	f, err := s.files.Open(path)
	if err != nil {
		return Rendition{}, nil, fmt.Errorf("error opening rendition: %w", err)
	}
	return r, f, nil
}

// remove deletes a file the service is done with. A failure is only logged: the temp sweep or
// an operator can clean up later, and the caller's outcome doesn't depend on it.
func (s *Service) remove(ctx context.Context, path string) {
	if path == "" {
		return
	}
	if err := s.files.Remove(path); err != nil && !errors.Is(err, fs.ErrNotExist) {
		logging.LoggerFromContext(ctx).LogAttrs(ctx, slog.LevelWarn, "error removing file",
			slog.String("path", path), slog.String("error", err.Error()))
	}
}
