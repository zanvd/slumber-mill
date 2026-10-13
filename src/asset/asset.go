package asset

import (
	"errors"
	"time"
	"uuid"
)

var (
	ErrNotFound             = errors.New("asset not found")
	ErrIdempotencyKeyReused = errors.New("idempotency key already used for a different upload")

	errDuplicate = errors.New("duplicate key")
	errDatabase  = errors.New("database error")
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
