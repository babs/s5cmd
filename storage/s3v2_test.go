package storage

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	s3v2 "github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/peak/s5cmd/v2/log"
	"github.com/peak/s5cmd/v2/storage/url"
)

// newTestS3 builds an *S3 wired against the given httptest.Server. region is
// preset (skipping the bucket-region auto-detection probe) and credentials
// are anonymous so the test does not rely on the host's AWS config.
func newTestS3(t *testing.T, srv *httptest.Server) *S3 {
	t.Helper()
	resetClientCache(t)

	opts := Options{
		Endpoint:      srv.URL,
		NoSignRequest: true,
		region:        "us-east-1",
		// LogLevel zero-value is LevelTrace which routes SDK output
		// through the global log.Logger; tests don't init it. Pinning
		// to LevelInfo keeps the trace middleware out of the stack.
		LogLevel: log.LevelInfo,
	}
	s, err := newS3Storage(context.Background(), opts)
	if err != nil {
		t.Fatalf("newS3Storage: %v", err)
	}
	return s
}

// resetClientCache swaps in a fresh client cache for the duration of the
// test so concurrent tests don't share cached *s3.Client instances pinned
// to dead httptest servers.
func resetClientCache(t *testing.T) {
	t.Helper()
	prev := globalClientCache
	t.Cleanup(func() { globalClientCache = prev })
	globalClientCache = &clientCache{clients: map[Options]*s3v2.Client{}}
}

// TestNewS3Storage_AddressingStyle observes the HTTP request actually
// emitted by the constructed client. Custom (non-AWS) endpoints must produce
// path-style URLs (/bucket/key) -- if the constructor wired UsePathStyle to
// the wrong value, the bucket would land in the Host header instead.
//
// Virtual-host detection for AWS is harder to test in-process because the
// SDK would dial bucket.s3.amazonaws.com via real DNS; the path-style case
// against a custom endpoint is the strongest unit-level guarantee.
func TestNewS3Storage_AddressingStyle(t *testing.T) {
	var capturedPath, capturedHost string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedPath = r.URL.Path
		capturedHost = r.Host
		w.Header().Set("Content-Length", "0")
		w.Header().Set("ETag", `"x"`)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	s := newTestS3(t, srv) // custom endpoint -> must be path-style
	u, _ := url.New("s3://my-bucket/my-key")
	if _, err := s.Stat(context.Background(), u); err != nil {
		t.Fatalf("Stat: %v", err)
	}

	wantPath := "/my-bucket/my-key"
	if capturedPath != wantPath {
		t.Errorf("path-style request path = %q, want %q (host=%q)", capturedPath, wantPath, capturedHost)
	}
	// The bucket name MUST NOT appear as a subdomain prefix in the Host
	// header for path-style requests.
	if strings.HasPrefix(capturedHost, "my-bucket.") {
		t.Errorf("path-style request leaked bucket into Host header: %q", capturedHost)
	}
}

// TestNewS3Storage_AccelerateEndpointCleared verifies the constructor's
// special case: when the configured endpoint hostname matches the S3
// transfer-acceleration host, the endpoint should be cleared (so the SDK
// uses the accelerated routing instead of pinning a literal URL).
func TestNewS3Storage_AccelerateEndpointCleared(t *testing.T) {
	endpointURL, _ := parseEndpoint("https://s3-accelerate.amazonaws.com")
	if !supportsTransferAcceleration(endpointURL) {
		t.Fatalf("test premise broken: accel host not detected")
	}
	// The constructor logic in s3v2.go: useAccelerate true -> endpointURL
	// reassigned to sentinelURL. We can re-walk the same predicate here to
	// confirm the invariant the constructor relies on still holds.
	if supportsTransferAcceleration(sentinelURL) {
		t.Errorf("sentinelURL must NOT match transfer-acceleration; constructor's clear-on-accel logic depends on this")
	}
}

func TestStat_RoundTrip(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Minimal HeadObject response: 200 + headers.
		w.Header().Set("Content-Length", "12")
		w.Header().Set("ETag", `"d41d8cd98f00b204e9800998ecf8427e"`)
		w.Header().Set("Last-Modified", "Mon, 02 Jan 2006 15:04:05 GMT")
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	s := newTestS3(t, srv)
	u, err := url.New("s3://my-bucket/key")
	if err != nil {
		t.Fatalf("url.New: %v", err)
	}

	obj, err := s.Stat(context.Background(), u)
	if err != nil {
		t.Fatalf("Stat: %v", err)
	}
	if obj.Size != 12 {
		t.Errorf("Size = %d, want 12", obj.Size)
	}
	if obj.Etag != "d41d8cd98f00b204e9800998ecf8427e" {
		t.Errorf("Etag = %q, want stripped of quotes", obj.Etag)
	}
	if obj.ModTime == nil || obj.ModTime.IsZero() {
		t.Errorf("ModTime is zero, expected parsed value")
	}
}

func TestStat_NotFound(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer srv.Close()

	s := newTestS3(t, srv)
	u, _ := url.New("s3://my-bucket/missing")

	_, err := s.Stat(context.Background(), u)
	if err == nil {
		t.Fatal("Stat: want error for 404, got nil")
	}
	var notFound *ErrGivenObjectNotFound
	if !errors.As(err, &notFound) {
		t.Errorf("err = %v (%T), want *ErrGivenObjectNotFound", err, err)
	}
}

func TestHeadBucket_RoundTrip(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Walk the request path; HEAD /<bucket>/ for HeadBucket. Always 200.
		if r.Method != http.MethodHead {
			t.Errorf("unexpected method %q", r.Method)
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	s := newTestS3(t, srv)
	u, _ := url.New("s3://my-bucket")
	if err := s.HeadBucket(context.Background(), u); err != nil {
		t.Fatalf("HeadBucket: %v", err)
	}
}

func TestHeadObject_PreservesUserMetadata(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", "0")
		w.Header().Set("ETag", `"abc123"`)
		// User metadata arrives via x-amz-meta-* response headers.
		w.Header().Set("X-Amz-Meta-Owner", "alice")
		w.Header().Set("X-Amz-Meta-Project", "atlas")
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	s := newTestS3(t, srv)
	u, _ := url.New("s3://my-bucket/key")

	_, meta, err := s.HeadObject(context.Background(), u)
	if err != nil {
		t.Fatalf("HeadObject: %v", err)
	}
	if meta == nil {
		t.Fatal("metadata is nil")
	}
	// v2 SDK lowercases user-metadata keys during deserialization
	// (vendor/.../service/s3/deserializers.go ToLower). v1 SDK behaved
	// the same way, and the existing e2e suite (TestHeadObjectWithMetadata,
	// TestHeadObjectJSONMetadata) already expects lowercase output. The
	// storage layer must NOT re-case keys -- doing so was attempted in an
	// earlier migration commit and silently broke those e2e regexes.
	if got := meta.UserDefined["owner"]; got != "alice" {
		t.Errorf("UserDefined[owner] = %q, want alice (keys=%v)", got, meta.UserDefined)
	}
	if got := meta.UserDefined["project"]; got != "atlas" {
		t.Errorf("UserDefined[project] = %q, want atlas (keys=%v)", got, meta.UserDefined)
	}
}

func TestHeadObject_EmptyMetadataIsMap(t *testing.T) {
	// Regression: v2 returns nil for empty metadata; storage layer must
	// substitute an empty map so JSON marshalling produces "metadata":{}
	// instead of "metadata":null (matches v1 behavior for HeadObjectMessage).
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", "5")
		w.Header().Set("ETag", `"deadbeef"`)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	s := newTestS3(t, srv)
	u, _ := url.New("s3://my-bucket/key")

	_, meta, err := s.HeadObject(context.Background(), u)
	if err != nil {
		t.Fatalf("HeadObject: %v", err)
	}
	if meta.UserDefined == nil {
		t.Fatal("UserDefined is nil; want empty map for v1-compat JSON output")
	}
	if len(meta.UserDefined) != 0 {
		t.Errorf("UserDefined = %v, want empty map", meta.UserDefined)
	}
}

func TestHeadObject_StorageClassDefaultStandard(t *testing.T) {
	// S3 omits the StorageClass header for STANDARD-class objects; storage
	// layer must default to "STANDARD" rather than empty.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", "5")
		w.Header().Set("ETag", `"x"`)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	s := newTestS3(t, srv)
	u, _ := url.New("s3://my-bucket/key")

	obj, _, err := s.HeadObject(context.Background(), u)
	if err != nil {
		t.Fatalf("HeadObject: %v", err)
	}
	if string(obj.StorageClass) != "STANDARD" {
		t.Errorf("StorageClass = %q, want STANDARD", obj.StorageClass)
	}
}

func TestRead_StreamsBody(t *testing.T) {
	body := "the quick brown fox"
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", "19")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(body))
	}))
	defer srv.Close()

	s := newTestS3(t, srv)
	u, _ := url.New("s3://my-bucket/key")

	rc, err := s.Read(context.Background(), u)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	defer rc.Close()

	got, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if string(got) != body {
		t.Errorf("body = %q, want %q", string(got), body)
	}
}

func TestMakeBucket_DryRunSkipsRequest(t *testing.T) {
	hits := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits++
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	resetClientCache(t)
	opts := Options{Endpoint: srv.URL, NoSignRequest: true, region: "us-east-1", DryRun: true, LogLevel: log.LevelInfo}
	s, err := newS3Storage(context.Background(), opts)
	if err != nil {
		t.Fatalf("newS3Storage: %v", err)
	}
	if err := s.MakeBucket(context.Background(), "any"); err != nil {
		t.Fatalf("MakeBucket: %v", err)
	}
	if hits != 0 {
		t.Errorf("hits = %d, want 0 (write ops must respect dry-run)", hits)
	}
}

func TestRemoveBucket_DryRunSkipsRequest(t *testing.T) {
	hits := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits++
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	resetClientCache(t)
	opts := Options{Endpoint: srv.URL, NoSignRequest: true, region: "us-east-1", DryRun: true, LogLevel: log.LevelInfo}
	s, err := newS3Storage(context.Background(), opts)
	if err != nil {
		t.Fatalf("newS3Storage: %v", err)
	}
	if err := s.RemoveBucket(context.Background(), "any"); err != nil {
		t.Fatalf("RemoveBucket: %v", err)
	}
	if hits != 0 {
		t.Errorf("hits = %d, want 0 (write ops must respect dry-run)", hits)
	}
}

func TestListBuckets_FiltersByPrefix(t *testing.T) {
	const body = `<?xml version="1.0" encoding="UTF-8"?>
<ListAllMyBucketsResult>
  <Owner><ID>x</ID><DisplayName>x</DisplayName></Owner>
  <Buckets>
    <Bucket><Name>alpha-one</Name><CreationDate>2024-01-01T00:00:00Z</CreationDate></Bucket>
    <Bucket><Name>alpha-two</Name><CreationDate>2024-01-02T00:00:00Z</CreationDate></Bucket>
    <Bucket><Name>beta-one</Name><CreationDate>2024-01-03T00:00:00Z</CreationDate></Bucket>
  </Buckets>
</ListAllMyBucketsResult>`
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/xml")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(body))
	}))
	defer srv.Close()

	s := newTestS3(t, srv)

	all, err := s.ListBuckets(context.Background(), "")
	if err != nil {
		t.Fatalf("ListBuckets(empty prefix): %v", err)
	}
	if len(all) != 3 {
		t.Errorf("len(all) = %d, want 3", len(all))
	}

	alpha, err := s.ListBuckets(context.Background(), "alpha-")
	if err != nil {
		t.Fatalf("ListBuckets(alpha-): %v", err)
	}
	if len(alpha) != 2 {
		t.Errorf("len(alpha) = %d, want 2", len(alpha))
	}
	for _, b := range alpha {
		if !strings.HasPrefix(b.Name, "alpha-") {
			t.Errorf("bucket %q leaked through alpha- prefix filter", b.Name)
		}
	}
}

func TestPresign_GeneratesURL(t *testing.T) {
	// Presign requires a real signer; anonymous credentials short-circuit
	// to the smithy "noAuth" signer which the v2 SDK refuses to use for
	// presigning. Inject static credentials via env so LoadDefaultConfig
	// resolves a usable signer without touching the host's AWS config.
	t.Setenv("AWS_ACCESS_KEY_ID", "test-key")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "test-secret")
	t.Setenv("AWS_REGION", "us-east-1")

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Fatalf("Presign should not hit the network: got %s %s", r.Method, r.URL)
	}))
	defer srv.Close()

	resetClientCache(t)
	opts := Options{Endpoint: srv.URL, region: "us-east-1", LogLevel: log.LevelInfo}
	s, err := newS3Storage(context.Background(), opts)
	if err != nil {
		t.Fatalf("newS3Storage: %v", err)
	}
	u, _ := url.New("s3://my-bucket/key")

	signed, err := s.Presign(context.Background(), u, 60_000_000_000)
	if err != nil {
		t.Fatalf("Presign: %v", err)
	}
	if signed == "" {
		t.Fatal("Presign returned empty URL")
	}
	if !strings.Contains(signed, "my-bucket") || !strings.Contains(signed, "key") {
		t.Errorf("Presign URL %q missing bucket/key components", signed)
	}
	// The presign URL must include a SigV4 signature query parameter.
	if !strings.Contains(signed, "X-Amz-Signature=") {
		t.Errorf("Presign URL %q missing X-Amz-Signature query parameter", signed)
	}
}

// TestList_PaginatedV2 covers the ListObjectsV2 paginator: two pages worth
// of contents must both reach the channel. Catches a regression where the
// paginator loop exits prematurely or the second page is dropped.
func TestList_PaginatedV2(t *testing.T) {
	page := 0
	page1 := `<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult>
  <IsTruncated>true</IsTruncated>
  <NextContinuationToken>tok</NextContinuationToken>
  <Contents><Key>a/1</Key><LastModified>2024-01-01T00:00:00Z</LastModified><ETag>"e1"</ETag><Size>10</Size><StorageClass>STANDARD</StorageClass></Contents>
  <Contents><Key>a/2</Key><LastModified>2024-01-01T00:00:00Z</LastModified><ETag>"e2"</ETag><Size>20</Size><StorageClass>STANDARD</StorageClass></Contents>
</ListBucketResult>`
	page2 := `<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult>
  <IsTruncated>false</IsTruncated>
  <Contents><Key>a/3</Key><LastModified>2024-01-01T00:00:00Z</LastModified><ETag>"e3"</ETag><Size>30</Size><StorageClass>STANDARD</StorageClass></Contents>
</ListBucketResult>`
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/xml")
		w.WriteHeader(http.StatusOK)
		page++
		if page == 1 {
			_, _ = w.Write([]byte(page1))
		} else {
			_, _ = w.Write([]byte(page2))
		}
	}))
	defer srv.Close()

	s := newTestS3(t, srv)
	u, _ := url.New("s3://my-bucket/a/*")

	var got []string
	for obj := range s.List(context.Background(), u, false) {
		if obj.Err != nil {
			t.Fatalf("list error: %v", obj.Err)
		}
		got = append(got, obj.URL.Path)
	}
	if len(got) != 3 {
		t.Errorf("got %d objects, want 3 (across two pages): %v", len(got), got)
	}
	if page < 2 {
		t.Errorf("paginator made only %d HTTP requests, want 2", page)
	}
}

// TestDelete_SingleObject hits the DeleteObjects batch endpoint (the
// non-GCS path) with a single key. Validates URL/body wiring through the
// chunkV2 helper.
func TestDelete_SingleObject(t *testing.T) {
	gotMethod := ""
	gotPath := ""
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		gotPath = r.URL.Path
		w.Header().Set("Content-Type", "application/xml")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?><DeleteResult><Deleted><Key>k1</Key></Deleted></DeleteResult>`))
	}))
	defer srv.Close()

	s := newTestS3(t, srv)
	u, _ := url.New("s3://my-bucket/k1")
	if err := s.Delete(context.Background(), u); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if gotMethod != http.MethodPost {
		t.Errorf("method = %q, want POST (DeleteObjects batch)", gotMethod)
	}
	if !strings.Contains(gotPath, "my-bucket") {
		t.Errorf("path = %q, want to contain my-bucket", gotPath)
	}
}

// TestDelete_GCSEndpointPerObjectLoop verifies the GCS workaround: when
// the endpoint is GCS, Delete falls back to per-object DeleteObject (DELETE
// requests) rather than the batch DeleteObjects POST that GCS doesn't
// implement.
func TestDelete_GCSEndpointPerObjectLoop(t *testing.T) {
	methods := []string{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		methods = append(methods, r.Method)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()

	resetClientCache(t)
	// Emulate GCS endpoint: the storage layer keys per-object loop on
	// IsGoogleEndpoint(s.endpointURL), which checks Hostname() == gcsEndpoint.
	// We construct an *S3 by hand so the test doesn't have to spoof DNS.
	client, err := globalClientCache.newClient(context.Background(), Options{
		Endpoint: srv.URL, NoSignRequest: true, region: "us-east-1", LogLevel: log.LevelInfo,
	})
	if err != nil {
		t.Fatalf("newClient: %v", err)
	}
	gcsURL, _ := parseEndpoint("https://storage.googleapis.com")
	s := &S3{
		api:         client,
		endpointURL: gcsURL,
	}

	urls := make(chan *url.URL, 2)
	u1, _ := url.New("s3://my-bucket/k1")
	u2, _ := url.New("s3://my-bucket/k2")
	urls <- u1
	urls <- u2
	close(urls)

	for obj := range s.MultiDelete(context.Background(), urls) {
		if obj.Err != nil {
			t.Errorf("MultiDelete err: %v", obj.Err)
		}
	}
	for _, m := range methods {
		if m != http.MethodDelete {
			t.Errorf("got method %q, want DELETE for GCS per-object loop", m)
		}
	}
	if len(methods) != 2 {
		t.Errorf("got %d requests, want 2 (one per key, no batch)", len(methods))
	}
}

// TestCopy_Roundtrip verifies the CopyObject request reaches the server
// with the expected CopySource header and bucket/key path.
func TestCopy_Roundtrip(t *testing.T) {
	gotCopySource := ""
	gotPath := ""
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotCopySource = r.Header.Get("X-Amz-Copy-Source")
		gotPath = r.URL.Path
		w.Header().Set("Content-Type", "application/xml")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?><CopyObjectResult><ETag>"abc"</ETag></CopyObjectResult>`))
	}))
	defer srv.Close()

	s := newTestS3(t, srv)
	src, _ := url.New("s3://src-bucket/src-key")
	dst, _ := url.New("s3://dst-bucket/dst-key")
	if err := s.Copy(context.Background(), src, dst, Metadata{}); err != nil {
		t.Fatalf("Copy: %v", err)
	}
	if !strings.Contains(gotCopySource, "src-bucket") || !strings.Contains(gotCopySource, "src-key") {
		t.Errorf("X-Amz-Copy-Source = %q, want to reference src-bucket/src-key", gotCopySource)
	}
	if !strings.Contains(gotPath, "dst-bucket") || !strings.Contains(gotPath, "dst-key") {
		t.Errorf("dst path = %q, want dst-bucket/dst-key", gotPath)
	}
}

func TestCopy_VersionedSource(t *testing.T) {
	got := ""
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = r.Header.Get("X-Amz-Copy-Source")
		w.Header().Set("Content-Type", "application/xml")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?><CopyObjectResult/>`))
	}))
	defer srv.Close()

	s := newTestS3(t, srv)
	src, _ := url.New("s3://src/k")
	src.VersionID = "v123"
	dst, _ := url.New("s3://dst/k")
	if err := s.Copy(context.Background(), src, dst, Metadata{}); err != nil {
		t.Fatalf("Copy: %v", err)
	}
	if !strings.Contains(got, "versionId=v123") {
		t.Errorf("CopySource %q missing ?versionId=v123 suffix", got)
	}
}

func TestCopy_DryRunSkipsRequest(t *testing.T) {
	hits := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { hits++ }))
	defer srv.Close()

	resetClientCache(t)
	opts := Options{Endpoint: srv.URL, NoSignRequest: true, region: "us-east-1", DryRun: true, LogLevel: log.LevelInfo}
	s, err := newS3Storage(context.Background(), opts)
	if err != nil {
		t.Fatalf("newS3Storage: %v", err)
	}
	src, _ := url.New("s3://b/k")
	dst, _ := url.New("s3://b/k2")
	if err := s.Copy(context.Background(), src, dst, Metadata{}); err != nil {
		t.Fatalf("Copy dry-run: %v", err)
	}
	if hits != 0 {
		t.Errorf("hits = %d, want 0 (Copy must respect dry-run)", hits)
	}
}

// TestGet_SingleShot exercises the manager.Downloader single-shot path
// (concurrency=1 collapses to one GetObject).
func TestGet_SingleShot(t *testing.T) {
	body := "hello world"
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", fmt.Sprintf("%d", len(body)))
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(body))
	}))
	defer srv.Close()

	s := newTestS3(t, srv)
	u, _ := url.New("s3://b/k")
	w := newWriterAtBuf()
	n, err := s.Get(context.Background(), u, w, 1, 5*1024*1024)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if n != int64(len(body)) {
		t.Errorf("n = %d, want %d", n, len(body))
	}
	if w.String() != body {
		t.Errorf("body = %q, want %q", w.String(), body)
	}
}

// TestGet_DryRunSkipsRequest: writes are gated; Get is a download but s5cmd
// gates Get behind dry-run too (matches v1).
func TestGet_DryRunSkipsRequest(t *testing.T) {
	hits := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { hits++ }))
	defer srv.Close()
	resetClientCache(t)
	opts := Options{Endpoint: srv.URL, NoSignRequest: true, region: "us-east-1", DryRun: true, LogLevel: log.LevelInfo}
	s, err := newS3Storage(context.Background(), opts)
	if err != nil {
		t.Fatalf("newS3Storage: %v", err)
	}
	u, _ := url.New("s3://b/k")
	if _, err := s.Get(context.Background(), u, newWriterAtBuf(), 1, 5*1024*1024); err != nil {
		t.Fatalf("Get dry-run: %v", err)
	}
	if hits != 0 {
		t.Errorf("hits = %d, want 0 (Get must respect dry-run)", hits)
	}
}

// TestPut_SingleShot exercises the manager.Uploader single-shot path
// (small body fits in one PutObject below the part-size threshold).
func TestPut_SingleShot(t *testing.T) {
	gotBody := []byte{}
	gotMethod := ""
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		buf := make([]byte, 1024)
		n, _ := r.Body.Read(buf)
		gotBody = append(gotBody, buf[:n]...)
		w.Header().Set("ETag", `"x"`)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	s := newTestS3(t, srv)
	u, _ := url.New("s3://b/k")
	body := strings.NewReader("payload-12345")
	if err := s.Put(context.Background(), body, u, Metadata{}, 1, 5*1024*1024); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if gotMethod != http.MethodPut {
		t.Errorf("method = %q, want PUT", gotMethod)
	}
	if string(gotBody) != "payload-12345" {
		t.Errorf("body = %q, want payload-12345", string(gotBody))
	}
}

func TestSetBucketVersioning(t *testing.T) {
	gotMethod := ""
	gotQuery := ""
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		gotQuery = r.URL.RawQuery
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	s := newTestS3(t, srv)
	if err := s.SetBucketVersioning(context.Background(), "Enabled", "any"); err != nil {
		t.Fatalf("SetBucketVersioning: %v", err)
	}
	if gotMethod != http.MethodPut {
		t.Errorf("method = %q, want PUT", gotMethod)
	}
	if !strings.Contains(gotQuery, "versioning") {
		t.Errorf("query = %q, want to contain 'versioning'", gotQuery)
	}
}

func TestGetBucketVersioning(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/xml")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?><VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>`))
	}))
	defer srv.Close()

	s := newTestS3(t, srv)
	got, err := s.GetBucketVersioning(context.Background(), "any")
	if err != nil {
		t.Fatalf("GetBucketVersioning: %v", err)
	}
	if got != "Enabled" {
		t.Errorf("got %q, want Enabled", got)
	}
}

func TestGetBucketVersioning_Empty(t *testing.T) {
	// S3 returns no <Status> element when versioning was never enabled --
	// storage layer must surface this as empty string, not error.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/xml")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?><VersioningConfiguration/>`))
	}))
	defer srv.Close()

	s := newTestS3(t, srv)
	got, err := s.GetBucketVersioning(context.Background(), "any")
	if err != nil {
		t.Fatalf("GetBucketVersioning: %v", err)
	}
	if got != "" {
		t.Errorf("got %q, want empty (no Status element)", got)
	}
}

func TestPut_DryRunSkipsRequest(t *testing.T) {
	hits := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { hits++ }))
	defer srv.Close()
	resetClientCache(t)
	opts := Options{Endpoint: srv.URL, NoSignRequest: true, region: "us-east-1", DryRun: true, LogLevel: log.LevelInfo}
	s, err := newS3Storage(context.Background(), opts)
	if err != nil {
		t.Fatalf("newS3Storage: %v", err)
	}
	u, _ := url.New("s3://b/k")
	if err := s.Put(context.Background(), strings.NewReader("x"), u, Metadata{}, 1, 5*1024*1024); err != nil {
		t.Fatalf("Put dry-run: %v", err)
	}
	if hits != 0 {
		t.Errorf("hits = %d, want 0", hits)
	}
}

// writerAtBuf is a tiny io.WriterAt for in-memory captures used by TestGet_*.
// The manager.Downloader writes ranges out of order so a plain bytes.Buffer
// is insufficient.
type writerAtBuf struct {
	mu  sync.Mutex
	buf []byte
}

func newWriterAtBuf() *writerAtBuf { return &writerAtBuf{} }
func (w *writerAtBuf) WriteAt(p []byte, off int64) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	end := int(off) + len(p)
	if end > len(w.buf) {
		w.buf = append(w.buf, make([]byte, end-len(w.buf))...)
	}
	copy(w.buf[off:], p)
	return len(p), nil
}
func (w *writerAtBuf) String() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return string(w.buf)
}

// TestResolveBucketRegion_Success exercises the happy path: probe HeadBucket
// returns 200 with X-Amz-Bucket-Region; the resolved region must be
// surfaced and folded into opts before the real client is built.
func TestResolveBucketRegion_Success(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Amz-Bucket-Region", "eu-west-1")
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()
	resetClientCache(t)

	region, err := resolveBucketRegion(context.Background(), Options{
		Endpoint:      srv.URL,
		NoSignRequest: true,
		bucket:        "any",
		LogLevel:      log.LevelInfo,
	})
	if err != nil {
		t.Fatalf("resolveBucketRegion: %v", err)
	}
	if region != "eu-west-1" {
		t.Errorf("region = %q, want eu-west-1", region)
	}
}

// TestResolveBucketRegion_BucketNotFound: GetBucketRegion returns
// BucketNotFound for 404. resolveBucketRegion must swallow it (return nil
// error so the caller doesn't double-log) -- the actual operation surfaces
// the NotFound itself.
func TestResolveBucketRegion_BucketNotFound(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer srv.Close()
	resetClientCache(t)

	region, err := resolveBucketRegion(context.Background(), Options{
		Endpoint:      srv.URL,
		NoSignRequest: true,
		bucket:        "missing",
		LogLevel:      log.LevelInfo,
	})
	if err != nil {
		t.Errorf("BucketNotFound must be silently swallowed, got: %v", err)
	}
	if region != "" {
		t.Errorf("region = %q, want empty for BucketNotFound", region)
	}
}

// TestResolveBucketRegion_OtherError: 5xx (or any non-404) must propagate
// as a real error so newS3Storage logs it (without surfacing as a fatal).
func TestResolveBucketRegion_OtherError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()
	resetClientCache(t)

	_, err := resolveBucketRegion(context.Background(), Options{
		Endpoint:      srv.URL,
		NoSignRequest: true,
		bucket:        "any",
		MaxRetries:    1, // keep the test fast: don't retry the 500
		LogLevel:      log.LevelInfo,
	})
	if err == nil {
		t.Fatal("resolveBucketRegion: want non-nil error for 500")
	}
}

// TestNewS3Storage_RegionAutoDetect: end-to-end -- when opts.region is
// empty and opts.bucket is set, newS3Storage must invoke the probe and
// fold the discovered region into the cached real client.
func TestNewS3Storage_RegionAutoDetect(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Amz-Bucket-Region", "ap-southeast-2")
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()
	resetClientCache(t)

	// region empty, bucket set -> probe runs.
	s, err := newS3Storage(context.Background(), Options{
		Endpoint:      srv.URL,
		NoSignRequest: true,
		bucket:        "any",
		LogLevel:      log.LevelInfo,
	})
	if err != nil {
		t.Fatalf("newS3Storage: %v", err)
	}
	if got := s.api.Options().Region; got != "ap-southeast-2" {
		t.Errorf("client region = %q, want ap-southeast-2 (probe-resolved)", got)
	}
}

func TestStat_DryRunUnaffected(t *testing.T) {
	// Stat is a read; dry-run does NOT short-circuit it (only writes are
	// gated). Confirm that opts.DryRun=true still issues the HEAD request.
	hits := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits++
		w.Header().Set("Content-Length", "1")
		w.Header().Set("ETag", `"x"`)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	resetClientCache(t)

	opts := Options{Endpoint: srv.URL, NoSignRequest: true, region: "us-east-1", DryRun: true, LogLevel: log.LevelInfo}
	s, err := newS3Storage(context.Background(), opts)
	if err != nil {
		t.Fatalf("newS3Storage: %v", err)
	}

	u, _ := url.New("s3://my-bucket/key")
	if _, err := s.Stat(context.Background(), u); err != nil {
		t.Fatalf("Stat: %v", err)
	}
	if hits != 1 {
		t.Errorf("hits = %d, want 1 (Stat must hit the network even in dry-run)", hits)
	}
}
