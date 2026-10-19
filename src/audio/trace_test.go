package audio

import (
	"context"
	"testing"

	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

type wantSpan struct {
	name string
	// attrs holds attributes the span must have, as Emit prints their values.
	attrs map[string]string
}

// encodeSpan starts the span an encode runs under, sampled or not, from a provider that records to the recorder.
func encodeSpan(t *testing.T, sampled bool) (context.Context, trace.Span, *tracetest.SpanRecorder) {
	t.Helper()
	rec := tracetest.NewSpanRecorder()
	sampler := sdktrace.NeverSample()
	if sampled {
		sampler = sdktrace.AlwaysSample()
	}
	tp := sdktrace.NewTracerProvider(sdktrace.WithSampler(sampler), sdktrace.WithSpanProcessor(rec))
	ctx, span := tp.Tracer("test").Start(t.Context(), "asset.encode")
	return ctx, span, rec
}

// TestProcessSpans runs Process under an encode's span: each step is a child span of it, in the order they ran,
// each verification carries its rendition's loudness, and a corrective re-encode is a render and a verification
// of its own.
func TestProcessSpans(t *testing.T) {
	verified := func(profile, lufs, truePeak, inSpec string) wantSpan {
		return wantSpan{"audio.verify", map[string]string{
			attrProfile: profile, attrLUFS: lufs, attrTruePeak: truePeak, attrInSpec: inSpec,
		}}
	}
	first := []wantSpan{
		{"audio.probe", nil},
		{"audio.measure", nil},
		{"audio.render", map[string]string{attrProfiles: `["aac_128k","aac_64k"]`}},
		verified("aac_128k", "-16.02", "-5.41", "true"),
	}
	tests := []struct {
		name   string
		script []fakeResponse
		want   []wantSpan
	}{
		{"in spec", stereoScript(), append(first[:4:4], verified("aac_64k", "-15.85", "-2.99", "true"))},
		{"corrective re-encode", correctiveScript(), append(first[:4:4],
			verified("aac_64k", "-17.3", "-6.79", "false"),
			wantSpan{"audio.render", map[string]string{attrProfiles: `["aac_64k"]`, attrGain: "1.3"}},
			verified("aac_64k", "-15.98", "-5.47", "true"),
		)},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx, encode, rec := encodeSpan(t, true)
			if _, err := (&FFmpeg{run: &fakeRunner{responses: tc.script}}).Process(ctx, "/src.wav", -16,
				outputs()); err != nil {
				t.Fatalf("Process() error = %v", err)
			}
			encode.End()
			checkSpans(t, rec.Ended(), encode, tc.want)
		})
	}
}

// TestProcessSpanErrors fails a step: its span is an error, with the message of a rejection
// and nothing for any other failure, and no later step has a span.
func TestProcessSpanErrors(t *testing.T) {
	failed := &exitError{name: "ffmpeg", code: 255, status: "exit status 255", stderr: "/data/tmp/part-1: error"}
	tests := []struct {
		name        string
		script      func([]fakeResponse) []fakeResponse
		want        []string
		wantMessage string
	}{
		{
			name: "rejected as silent",
			script: func(s []fakeResponse) []fakeResponse {
				s[1].stderr = frames(inputInstance, -120.7) + frames("ebur128@stereo", -120.7) +
					frames("ebur128@mono", -120.7) +
					reportWithOffset("loudnorm@stereo", Loudness{LUFS: -80, ThresholdLUFS: -90}, Dynamic, "inf") +
					reportWithOffset("loudnorm@mono", Loudness{LUFS: -80, ThresholdLUFS: -90}, Dynamic, "inf")
				return s
			},
			want:        []string{"audio.probe", "audio.measure"},
			wantMessage: MessageSilent,
		},
		{
			name: "ffmpeg failed",
			script: func(s []fakeResponse) []fakeResponse {
				s[2].err = failed
				return s
			},
			want: []string{"audio.probe", "audio.measure", "audio.render"},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx, encode, rec := encodeSpan(t, true)
			if _, err := (&FFmpeg{run: &fakeRunner{responses: tc.script(stereoScript())}}).Process(ctx, "/src.wav",
				-16, outputs()); err == nil {
				t.Fatal("Process() succeeded, want an error")
			}
			encode.End()
			want := make([]wantSpan, len(tc.want))
			for i, name := range tc.want {
				want[i] = wantSpan{name: name}
			}
			spans := checkSpans(t, rec.Ended(), encode, want)
			for i, s := range spans {
				wantStatus := codes.Unset
				if i == len(spans)-1 {
					wantStatus = codes.Error
				}
				if s.Status().Code != wantStatus {
					t.Errorf("%s status = %+v, want %v", s.Name(), s.Status(), wantStatus)
				}
			}
			if last := spans[len(spans)-1]; last.Status().Description != tc.wantMessage {
				t.Errorf("%s status message = %q, want %q", last.Name(), last.Status().Description, tc.wantMessage)
			}
		})
	}
}

func TestProcessUnsampled(t *testing.T) {
	ctx, encode, rec := encodeSpan(t, false)
	if _, err := (&FFmpeg{run: &fakeRunner{responses: stereoScript()}}).Process(ctx, "/src.wav", -16,
		outputs()); err != nil {
		t.Fatalf("Process() error = %v", err)
	}
	encode.End()
	if n := len(rec.Started()); n != 0 {
		t.Errorf("recorded %d spans, want none", n)
	}
}

// checkSpans checks that ended holds the spans in want, children of encode in that order, and then encode,
// and returns the children.
func checkSpans(t *testing.T, ended []sdktrace.ReadOnlySpan, encode trace.Span, want []wantSpan) []sdktrace.ReadOnlySpan {
	t.Helper()
	if len(ended) != len(want)+1 || ended[len(ended)-1].SpanContext().SpanID() != encode.SpanContext().SpanID() {
		names := make([]string, len(ended))
		for i, s := range ended {
			names[i] = s.Name()
		}
		t.Fatalf("ended spans %v, want %d steps and then the encode's", names, len(want))
	}
	children := ended[:len(want)]
	for i, s := range children {
		if s.Name() != want[i].name || s.Parent().SpanID() != encode.SpanContext().SpanID() {
			t.Errorf("span %d is %q under %s, want %q under the encode's %s", i, s.Name(), s.Parent().SpanID(),
				want[i].name, encode.SpanContext().SpanID())
		}
		got := map[string]string{}
		for _, kv := range s.Attributes() {
			got[string(kv.Key)] = kv.Value.Emit()
		}
		for k, v := range want[i].attrs {
			if got[k] != v {
				t.Errorf("span %d (%s) has %s = %q, want %q", i, s.Name(), k, got[k], v)
			}
		}
	}
	return children
}
