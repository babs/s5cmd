package storage

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
)

// fakeAPIErr lets us synthesize smithy.APIError values without depending on
// a real OperationError chain.
type fakeAPIErr struct {
	code, msg string
}

func (e *fakeAPIErr) Error() string                      { return e.code + ": " + e.msg }
func (e *fakeAPIErr) ErrorCode() string                  { return e.code }
func (e *fakeAPIErr) ErrorMessage() string               { return e.msg }
func (e *fakeAPIErr) ErrorFault() smithy.ErrorFault      { return smithy.FaultClient }

func TestCleanV2Err(t *testing.T) {
	apiErr := &fakeAPIErr{code: "BucketNotEmpty", msg: "UnknownError"}
	wrapped := fmt.Errorf("operation error S3: DeleteBucket, %w", apiErr)

	cases := []struct {
		name      string
		in        error
		wantNil   bool
		wantStr   string
		wantUnwrap bool
	}{
		{"nil passthrough", nil, true, "", false},
		{"plain error", errors.New("network: connection reset"), false, "network: connection reset", false},
		{"bare APIError", apiErr, false, "BucketNotEmpty: UnknownError", true},
		{"wrapped APIError", wrapped, false, "BucketNotEmpty: UnknownError", true},
		{"empty message", &fakeAPIErr{code: "NotFound"}, false, "NotFound", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			out := cleanV2Err(tc.in)
			if tc.wantNil {
				if out != nil {
					t.Fatalf("cleanV2Err(nil) = %v, want nil", out)
				}
				return
			}
			if out == nil {
				t.Fatalf("cleanV2Err(%v) returned nil", tc.in)
			}
			if got := out.Error(); got != tc.wantStr {
				t.Errorf("cleanV2Err(%v).Error() = %q, want %q", tc.in, got, tc.wantStr)
			}

			// Critical invariant: the smithy.APIError chain must remain
			// reachable via errors.As so command/sync can still classify
			// errors by code.
			var apiErr smithy.APIError
			gotAPIErr := errors.As(out, &apiErr)
			if gotAPIErr != tc.wantUnwrap {
				t.Errorf("errors.As(out, &apiErr) = %v, want %v (out=%v)", gotAPIErr, tc.wantUnwrap, out)
			}
		})
	}
}

func TestCleanV2Err_IdempotentClassification(t *testing.T) {
	// cleanV2Err wraps once; calling it again on a cleanedAPIErr should
	// still expose the underlying smithy.APIError to downstream classifiers.
	apiErr := &fakeAPIErr{code: "AccessDenied", msg: "denied"}
	once := cleanV2Err(apiErr)
	twice := cleanV2Err(once)

	var got smithy.APIError
	if !errors.As(twice, &got) {
		t.Fatal("after two cleanV2Err passes, smithy.APIError no longer reachable")
	}
	if got.ErrorCode() != "AccessDenied" {
		t.Errorf("ErrorCode = %q, want AccessDenied", got.ErrorCode())
	}
}

func TestErrIsNotFound(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"plain", errors.New("boom"), false},
		{"typed NotFound", &s3types.NotFound{}, true},
		{"wrapped NotFound", fmt.Errorf("wrap: %w", &s3types.NotFound{}), true},
		{"smithy APIError code NotFound", &fakeAPIErr{code: "NotFound"}, true},
		{"smithy APIError code NoSuchKey", &fakeAPIErr{code: "NoSuchKey"}, true},
		{"smithy APIError other code", &fakeAPIErr{code: "AccessDenied"}, false},
		{"cleanedAPIErr NotFound", cleanV2Err(&fakeAPIErr{code: "NotFound"}), true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := errIsNotFound(tc.err); got != tc.want {
				t.Errorf("errIsNotFound(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}

func TestErrIsNoSuchUpload(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"plain", errors.New("boom"), false},
		{"typed NoSuchUpload", &s3types.NoSuchUpload{}, true},
		{"wrapped NoSuchUpload", fmt.Errorf("wrap: %w", &s3types.NoSuchUpload{}), true},
		{"smithy APIError code", &fakeAPIErr{code: "NoSuchUpload"}, true},
		{"unrelated code", &fakeAPIErr{code: "InvalidUpload"}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := errIsNoSuchUpload(tc.err); got != tc.want {
				t.Errorf("errIsNoSuchUpload(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}

func TestRequestPayerEnum(t *testing.T) {
	cases := []struct {
		in   string
		want s3types.RequestPayer
	}{
		{"", s3types.RequestPayer("")},
		{"requester", s3types.RequestPayer("requester")},
	}
	for _, tc := range cases {
		t.Run(tc.in, func(t *testing.T) {
			s := &S3{requestPayer: tc.in}
			if got := s.requestPayerEnum(); got != tc.want {
				t.Errorf("requestPayerEnum() = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestParseInputSerialization(t *testing.T) {
	cases := []struct {
		name        string
		ev          eventType
		compression string
		delim       string
		header      string
		wantErr     bool
		check       func(t *testing.T, in *s3types.InputSerialization)
	}{
		{
			name: "json document type",
			ev:   jsonType, delim: "DOCUMENT",
			check: func(t *testing.T, in *s3types.InputSerialization) {
				if in.JSON == nil {
					t.Fatal("JSON nil")
				}
				if in.JSON.Type != "DOCUMENT" {
					t.Errorf("JSON.Type = %q", in.JSON.Type)
				}
				if in.CompressionType != "" {
					t.Errorf("CompressionType = %q, want empty", in.CompressionType)
				}
			},
		},
		{
			name: "json with gzip compression",
			ev:   jsonType, delim: "LINES", compression: "GZIP",
			check: func(t *testing.T, in *s3types.InputSerialization) {
				if in.CompressionType != "GZIP" {
					t.Errorf("CompressionType = %q, want GZIP", in.CompressionType)
				}
			},
		},
		{
			name: "csv with header",
			ev:   csvType, delim: ",", header: "USE",
			check: func(t *testing.T, in *s3types.InputSerialization) {
				if in.CSV == nil {
					t.Fatal("CSV nil")
				}
				if in.CSV.FileHeaderInfo != "USE" {
					t.Errorf("FileHeaderInfo = %q, want USE", in.CSV.FileHeaderInfo)
				}
				if got := in.CSV.FieldDelimiter; got == nil || *got != "," {
					t.Errorf("FieldDelimiter = %v, want ','", got)
				}
			},
		},
		{
			name: "parquet (no compression / delimiter applies)",
			ev:   parquetType,
			check: func(t *testing.T, in *s3types.InputSerialization) {
				if in.Parquet == nil {
					t.Fatal("Parquet nil")
				}
				if in.JSON != nil || in.CSV != nil {
					t.Error("Parquet input must not also set JSON/CSV")
				}
			},
		},
		{
			name: "unknown format -> error",
			ev:   "yaml", wantErr: true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			in, err := parseInputSerialization(tc.ev, tc.compression, tc.delim, tc.header)
			if (err != nil) != tc.wantErr {
				t.Fatalf("err = %v, wantErr = %v", err, tc.wantErr)
			}
			if !tc.wantErr {
				tc.check(t, in)
			}
		})
	}
}

func TestParseOutputSerialization(t *testing.T) {
	cases := []struct {
		name    string
		ev      eventType
		delim   string
		wantErr bool
		check   func(t *testing.T, out *s3types.OutputSerialization, dec EventStreamDecoder)
	}{
		{
			name: "json output",
			ev:   jsonType,
			check: func(t *testing.T, out *s3types.OutputSerialization, dec EventStreamDecoder) {
				if out.JSON == nil {
					t.Error("JSON output nil")
				}
				if dec == nil {
					t.Error("decoder nil")
				}
			},
		},
		{
			name: "csv output", ev: csvType, delim: ";",
			check: func(t *testing.T, out *s3types.OutputSerialization, dec EventStreamDecoder) {
				if out.CSV == nil {
					t.Fatal("CSV output nil")
				}
				if got := out.CSV.FieldDelimiter; got == nil || *got != ";" {
					t.Errorf("FieldDelimiter = %v, want ';'", got)
				}
				if dec == nil {
					t.Error("decoder nil")
				}
			},
		},
		{
			name: "unknown format -> error",
			ev:   "parquet", wantErr: true, // parquet output not supported by S3 Select
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			out, dec, err := parseOutputSerialization(tc.ev, tc.delim, strings.NewReader(""))
			if (err != nil) != tc.wantErr {
				t.Fatalf("err = %v, wantErr = %v", err, tc.wantErr)
			}
			if !tc.wantErr {
				tc.check(t, out, dec)
			}
		})
	}
}

// Note: full Select event-stream wire-level test (server returns properly
// framed binary event-stream payload with CRC32C checksums) is not provided
// here -- the framing is non-trivial to hand-craft. Select coverage at the
// dispatch level (tagged-union member type-switch on RecordsEvent) is left
// to e2e against gofakes3 / real S3. The serialization helpers above cover
// the request-side Select wiring at unit level.

func TestS3SatisfiesStorageInterface(t *testing.T) {
	// Compile-time check exists in s3v2_api.go via `var _ Storage = (*S3)(nil)`.
	// This runtime check is a belt-and-braces guard: if the interface gains
	// a method and someone forgets to port it, the var-assertion catches it
	// at compile time, but if a method's signature changes ambiguously, the
	// interface-conversion below makes the regression unmistakable in test
	// output.
	var s any = &S3{}
	if _, ok := s.(Storage); !ok {
		t.Fatal("*S3 does not implement Storage")
	}
}
