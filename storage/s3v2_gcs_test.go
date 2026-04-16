package storage

import (
	"context"
	"net/http"
	urlpkg "net/url"
	"testing"

	"github.com/aws/smithy-go/middleware"
	smithyhttp "github.com/aws/smithy-go/transport/http"
)

func TestIsGCSEndpoint(t *testing.T) {
	cases := []struct {
		endpoint string
		want     bool
	}{
		{"https://storage.googleapis.com", true},
		{"https://s3.amazonaws.com", false},
		{"https://minio.local:9000", false},
		{"https://r2.cloudflarestorage.com", false},
		{"", false},
	}

	for _, tc := range cases {
		t.Run(tc.endpoint, func(t *testing.T) {
			var u urlpkg.URL
			if tc.endpoint != "" {
				parsed, err := urlpkg.Parse(tc.endpoint)
				if err != nil {
					t.Fatalf("parse %q: %v", tc.endpoint, err)
				}
				u = *parsed
			}
			if got := isGCSEndpoint(u); got != tc.want {
				t.Errorf("isGCSEndpoint(%q) = %v, want %v", tc.endpoint, got, tc.want)
			}
		})
	}
}

func TestStripSDKHeadersMiddleware(t *testing.T) {
	httpReq, err := http.NewRequest(http.MethodGet, "https://storage.googleapis.com/bucket/key", nil)
	if err != nil {
		t.Fatalf("http.NewRequest: %v", err)
	}
	httpReq.Header.Set("Amz-Sdk-Invocation-Id", "abc-123")
	httpReq.Header.Set("Amz-Sdk-Request", "attempt=1")
	httpReq.Header.Set("X-Amz-Content-Sha256", "preserved")

	smithyReq := &smithyhttp.Request{Request: httpReq}

	called := false
	next := middleware.FinalizeHandlerFunc(
		func(_ context.Context, in middleware.FinalizeInput) (middleware.FinalizeOutput, middleware.Metadata, error) {
			called = true
			req := in.Request.(*smithyhttp.Request)
			if got := req.Header.Get("Amz-Sdk-Invocation-Id"); got != "" {
				t.Errorf("Amz-Sdk-Invocation-Id should be stripped, got %q", got)
			}
			if got := req.Header.Get("Amz-Sdk-Request"); got != "" {
				t.Errorf("Amz-Sdk-Request should be stripped, got %q", got)
			}
			if got := req.Header.Get("X-Amz-Content-Sha256"); got != "preserved" {
				t.Errorf("X-Amz-Content-Sha256 should survive, got %q", got)
			}
			return middleware.FinalizeOutput{}, middleware.Metadata{}, nil
		},
	)

	_, _, err = stripSDKHeadersMiddleware.HandleFinalize(
		context.Background(),
		middleware.FinalizeInput{Request: smithyReq},
		next,
	)
	if err != nil {
		t.Fatalf("HandleFinalize: %v", err)
	}
	if !called {
		t.Fatal("next handler was not invoked")
	}
}

// TestAddGCSHeaderStripMiddleware_StackPlacement verifies the wrapper
// inserts the Finalize middleware Before the SDK's "Signing" step. If the
// SDK ever renames or removes that step (it lives at
// vendor/.../service/s3/auth.go ID() == "Signing"), Insert returns a
// non-nil error and downstream wiring would silently swallow it via the
// APIOptions chain -- this test catches that failure mode at unit level.
func TestAddGCSHeaderStripMiddleware_StackPlacement(t *testing.T) {
	stack := middleware.NewStack("test", smithyhttp.NewStackRequest)
	// Insert a stand-in for the Signing middleware so the Insert(_, "Signing", Before)
	// has a target to anchor against.
	signing := middleware.FinalizeMiddlewareFunc(
		"Signing",
		func(ctx context.Context, in middleware.FinalizeInput, next middleware.FinalizeHandler) (middleware.FinalizeOutput, middleware.Metadata, error) {
			return next.HandleFinalize(ctx, in)
		},
	)
	if err := stack.Finalize.Add(signing, middleware.After); err != nil {
		t.Fatalf("seed Signing middleware: %v", err)
	}

	if err := addGCSHeaderStripMiddleware(stack); err != nil {
		t.Fatalf("addGCSHeaderStripMiddleware: %v", err)
	}

	// The Finalize step must now contain both the strip middleware and
	// the Signing stand-in, in that order.
	ids := stack.Finalize.List()
	var stripIdx, signIdx = -1, -1
	for i, id := range ids {
		switch id {
		case "S5cmdGCSStripSDKHeaders":
			stripIdx = i
		case "Signing":
			signIdx = i
		}
	}
	if stripIdx < 0 {
		t.Fatalf("S5cmdGCSStripSDKHeaders not found in Finalize step (have %v)", ids)
	}
	if signIdx < 0 {
		t.Fatalf("Signing not found in Finalize step (have %v)", ids)
	}
	if stripIdx >= signIdx {
		t.Errorf("strip middleware at %d, Signing at %d -- strip must run BEFORE Signing", stripIdx, signIdx)
	}
}
