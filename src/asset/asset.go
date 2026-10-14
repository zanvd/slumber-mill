package asset

import (
	"errors"
	"fmt"
	"time"
	"uuid"
)

var (
	ErrNotFound             = errors.New("asset not found")
	ErrIdempotencyKeyReused = errors.New("idempotency key already used for a different upload")

	errDuplicate = errors.New("duplicate key")
	errDatabase  = errors.New("database error")
	// errOutcomeUnknown marks a write that MySQL may have committed although the call failed.
	errOutcomeUnknown = fmt.Errorf("%w: outcome unknown", errDatabase)
	// errLeaseLost means a fenced write matched no row: the lease expired and was reaped,
	// or the asset has been claimed again or finished since.
	errLeaseLost = errors.New("lease lost")
)

type Status string

const (
	StatusQueued     Status = "queued"
	StatusProcessing Status = "processing"
	StatusReady      Status = "ready"
	StatusFailed     Status = "failed"
)

type Warning string

const (
	WarningSuddenLoudEvent      Warning = "sudden_loud_event"
	WarningDynamicNormalization Warning = "dynamic_normalization"
	WarningSourceMayClip        Warning = "source_may_clip"
)

type Profile string

const (
	ProfileAAC128k Profile = "aac_128k"
	ProfileAAC64k  Profile = "aac_64k"
)

func (p Profile) known() bool {
	return p == ProfileAAC128k || p == ProfileAAC64k
}

type Codec string

const CodecAAC Codec = "aac"

type Normalization string

const (
	NormalizationLinear  Normalization = "linear"
	NormalizationDynamic Normalization = "dynamic"
)

type EventType string

const (
	EventReady  EventType = "asset.ready"
	EventFailed EventType = "asset.failed"
)

// Asset is an uploaded source and what processing has learned about it.
// Nullable measurements are nil until the encode pool has taken them.
type Asset struct {
	ID       uuid.UUID
	TenantID uint64
	// IdempotencyKey is empty when the upload had none.
	IdempotencyKey string
	Title          string
	Status         Status

	SourceSHA256 [32]byte
	SourceBytes  uint64
	SourceCodec  string
	DurationMS   *uint64

	TargetLUFS        float64
	InputLUFS         *float64
	InputTruePeakDBTP *float64
	InputLRALU        *float64
	InputMaxMomentary *float64
	Warnings          []Warning

	// Error is the tenant-safe reason a failed asset failed.
	Error      string
	Renditions []Rendition

	CreatedAt time.Time
	UpdatedAt time.Time
}

type Rendition struct {
	Profile       Profile
	Codec         Codec
	BitrateKbps   uint32
	Channels      uint8
	Bytes         uint64
	SHA256        [32]byte
	LUFS          float64
	TruePeakDBTP  float64
	LRALU         float64
	Normalization Normalization
	CreatedAt     time.Time
}

// lease is a claimed asset. Its fields are those of worker.Lease and db.Lease, so it converts to and from either.
type lease struct {
	ID, TenantID uint64
	Attempt      int
}

// processed is what processing learned about an asset, stored when the asset finishes.
type processed struct {
	SourceCodec       string
	DurationMS        uint64
	InputLUFS         float64
	InputTruePeakDBTP float64
	InputLRALU        float64
	InputMaxMomentary float64
	Warnings          []Warning
	Renditions        []Rendition
}

// failure is why an attempt failed.
type failure struct {
	// Permanent fails the asset whatever its attempts, with Reason as its error. Otherwise the asset is retried
	// after Delay, or, on its last attempt, fails with the generic message.
	Permanent bool
	// Reason is a permanent failure's tenant-safe message.
	Reason string
	// Detail is the internal cause, kept in last_error and never shown to the tenant.
	Detail string
	Delay  time.Duration
}
