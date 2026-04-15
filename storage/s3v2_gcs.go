package storage

import (
	"context"
	urlpkg "net/url"

	"github.com/aws/smithy-go/middleware"
	smithyhttp "github.com/aws/smithy-go/transport/http"
)

// GCS rejects SigV4 signatures that include the SDK-emitted retry tracing
// headers (Amz-Sdk-Invocation-Id, Amz-Sdk-Request) — they appear in
// SignedHeaders but GCS does not preserve/echo them, so the canonical
// request the server reconstructs differs from what the SDK signed.
//
// Upstream issue: https://github.com/aws/aws-sdk-go-v2/issues/1816
//
// Workaround: drop these headers from the request immediately before the
// Signing middleware runs, so they never enter the canonical request.
//
// Scope: only injected when the configured endpoint resolves to GCS, leaving
// AWS / MinIO / R2 / Wasabi / etc. paths untouched.
const gcsHostname = "storage.googleapis.com"

func isGCSEndpoint(endpoint urlpkg.URL) bool {
	return endpoint.Hostname() == gcsHostname
}

var stripSDKHeadersMiddleware = middleware.FinalizeMiddlewareFunc(
	"S5cmdGCSStripSDKHeaders",
	func(ctx context.Context, in middleware.FinalizeInput, next middleware.FinalizeHandler) (middleware.FinalizeOutput, middleware.Metadata, error) {
		if req, ok := in.Request.(*smithyhttp.Request); ok {
			req.Header.Del("Amz-Sdk-Invocation-Id")
			req.Header.Del("Amz-Sdk-Request")
		}
		return next.HandleFinalize(ctx, in)
	},
)

func addGCSHeaderStripMiddleware(stack *middleware.Stack) error {
	return stack.Finalize.Insert(stripSDKHeadersMiddleware, "Signing", middleware.Before)
}
