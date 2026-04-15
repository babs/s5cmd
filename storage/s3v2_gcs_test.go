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
