package apiv1

import (
	"encoding/hex"
	"time"

	"github.com/zanvd/slumber-mill/src/asset"
	"github.com/zanvd/slumber-mill/src/validation"
)

type Asset struct {
	ID                string      `json:"id"`
	Title             string      `json:"title"`
	Status            string      `json:"status"`
	IdempotencyKey    *string     `json:"idempotency_key"`
	SourceSHA256      string      `json:"source_sha256"`
	SourceBytes       uint64      `json:"source_bytes"`
	SourceCodec       *string     `json:"source_codec"`
	DurationMS        *uint64     `json:"duration_ms"`
	TargetLUFS        float64     `json:"target_lufs"`
	InputLUFS         *float64    `json:"input_lufs"`
	InputTruePeakDBTP *float64    `json:"input_true_peak_dbtp"`
	InputLRALU        *float64    `json:"input_lra_lu"`
	InputMaxMomentary *float64    `json:"input_max_momentary"`
	Warnings          []string    `json:"warnings"`
	Error             *string     `json:"error"`
	Renditions        []Rendition `json:"renditions"`
	CreatedAt         time.Time   `json:"created_at"`
	UpdatedAt         time.Time   `json:"updated_at"`
}

type Rendition struct {
	Profile       string    `json:"profile"`
	Codec         string    `json:"codec"`
	BitrateKbps   uint32    `json:"bitrate_kbps"`
	Channels      uint8     `json:"channels"`
	Bytes         uint64    `json:"bytes"`
	SHA256        string    `json:"sha256"`
	LUFS          float64   `json:"lufs"`
	TruePeakDBTP  float64   `json:"true_peak_dbtp"`
	LRALU         float64   `json:"lra_lu"`
	Normalization string    `json:"normalization"`
	CreatedAt     time.Time `json:"created_at"`
}

type AssetList struct {
	Data       []Asset `json:"data"`
	NextCursor *string `json:"next_cursor"`
}

type ErrorResponse struct {
	Error ErrorBody `json:"error"`
}

type ErrorBody struct {
	Code    string       `json:"code"`
	Message string       `json:"message"`
	Fields  []FieldError `json:"fields,omitempty"`
}

type FieldError struct {
	Name    string `json:"name"`
	Message string `json:"message"`
}

func NewAsset(a asset.Asset) Asset {
	warnings := make([]string, len(a.Warnings))
	for i, w := range a.Warnings {
		warnings[i] = string(w)
	}
	renditions := make([]Rendition, len(a.Renditions))
	for i, r := range a.Renditions {
		renditions[i] = newRendition(r)
	}
	return Asset{
		ID:                a.ID.String(),
		Title:             a.Title,
		Status:            string(a.Status),
		IdempotencyKey:    nonEmpty(a.IdempotencyKey),
		SourceSHA256:      hex.EncodeToString(a.SourceSHA256[:]),
		SourceBytes:       a.SourceBytes,
		SourceCodec:       nonEmpty(a.SourceCodec),
		DurationMS:        copyPtr(a.DurationMS),
		TargetLUFS:        a.TargetLUFS,
		InputLUFS:         copyPtr(a.InputLUFS),
		InputTruePeakDBTP: copyPtr(a.InputTruePeakDBTP),
		InputLRALU:        copyPtr(a.InputLRALU),
		InputMaxMomentary: copyPtr(a.InputMaxMomentary),
		Warnings:          warnings,
		Error:             nonEmpty(a.Error),
		Renditions:        renditions,
		CreatedAt:         a.CreatedAt.UTC(),
		UpdatedAt:         a.UpdatedAt.UTC(),
	}
}

func newRendition(r asset.Rendition) Rendition {
	return Rendition{
		Profile:       string(r.Profile),
		Codec:         string(r.Codec),
		BitrateKbps:   r.BitrateKbps,
		Channels:      r.Channels,
		Bytes:         r.Bytes,
		SHA256:        hex.EncodeToString(r.SHA256[:]),
		LUFS:          r.LUFS,
		TruePeakDBTP:  r.TruePeakDBTP,
		LRALU:         r.LRALU,
		Normalization: string(r.Normalization),
		CreatedAt:     r.CreatedAt.UTC(),
	}
}

func NewAssetList(p asset.Page) AssetList {
	data := make([]Asset, len(p.Assets))
	for i, a := range p.Assets {
		data[i] = NewAsset(a)
	}
	return AssetList{Data: data, NextCursor: nonEmpty(p.NextCursor)}
}

func NewError(code, message string) ErrorResponse {
	return ErrorResponse{Error: ErrorBody{Code: code, Message: message}}
}

// NewValidationError builds the envelope for err, with one entry in fields per invalid field.
func NewValidationError(code, message string, err *validation.Error) ErrorResponse {
	resp := NewError(code, message)
	if err == nil {
		return resp
	}
	for _, f := range err.Fields() {
		resp.Error.Fields = append(resp.Error.Fields, FieldError{Name: f.Name, Message: f.Message})
	}
	return resp
}

func nonEmpty(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

func copyPtr[T any](p *T) *T {
	if p == nil {
		return nil
	}
	v := *p
	return &v
}
