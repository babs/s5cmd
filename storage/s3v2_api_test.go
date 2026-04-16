package storage

import (
	"errors"
	"fmt"
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
