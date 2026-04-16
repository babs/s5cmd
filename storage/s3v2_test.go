package storage

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	s3v2 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go/logging"

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

// TestMain sets up a single package-wide log.Logger. The log package uses
// a package-level outputCh channel that cannot be re-opened after Close,
// so per-test Init/Close pairs would panic on the second test ("send on
// closed channel"). Initialize once at "error" level (silent for normal
// tests) and never tear down -- the test binary process exits anyway.
//
// Note: log.Init starts a consumer goroutine on outputCh. We intentionally
// do not Close() -- process termination reaps it. If a future test needs
// to assert clean-shutdown semantics on the log package itself, that test
// will need a different init pattern (e.g. a fresh child process or a
// refactor of the log package to accept a per-instance channel).
func TestMain(m *testing.M) {
	log.Init("error", false)
	os.Exit(m.Run())
}

// resetClientCache swaps in a fresh client cache for the duration of the
// test so concurrent tests don't share cached *s3.Client instances pinned
// to dead httptest servers.
//
// Tests in this file MUST NOT call t.Parallel(): this helper mutates the
// package-level globalClientCache without synchronization, and Cleanup
// restores the previous value when the test ends. With t.Parallel() the
// swap and the restore would race other tests' swaps and the cache would
// be in an undefined state.
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

// TestHeadObject_VersionedPath covers the `if u.VersionID != ""` branch:
// the request must carry ?versionId=... as a query parameter so S3 serves
// the specific version rather than the current one.
func TestHeadObject_VersionedPath(t *testing.T) {
	var gotQuery atomic.Value
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotQuery.Store(r.URL.RawQuery)
		w.Header().Set("Content-Length", "0")
		w.Header().Set("ETag", `"v"`)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	s := newTestS3(t, srv)
	u, _ := url.New("s3://b/k", url.WithVersion("v-abc-123"))

	if _, _, err := s.HeadObject(context.Background(), u); err != nil {
		t.Fatalf("HeadObject: %v", err)
	}
	q, _ := gotQuery.Load().(string)
	if !strings.Contains(q, "versionId=v-abc-123") {
		t.Errorf("query = %q, want to contain versionId=v-abc-123", q)
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

func TestMakeBucket_Success(t *testing.T) {
	gotMethod := ""
	gotPath := ""
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		gotPath = r.URL.Path
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	s := newTestS3(t, srv)
	if err := s.MakeBucket(context.Background(), "new-bucket"); err != nil {
		t.Fatalf("MakeBucket: %v", err)
	}
	if gotMethod != http.MethodPut {
		t.Errorf("method = %q, want PUT", gotMethod)
	}
	if !strings.Contains(gotPath, "new-bucket") {
		t.Errorf("path = %q, want to contain new-bucket", gotPath)
	}
}

func TestRemoveBucket_Success(t *testing.T) {
	gotMethod := ""
	gotPath := ""
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		gotPath = r.URL.Path
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()

	s := newTestS3(t, srv)
	if err := s.RemoveBucket(context.Background(), "old-bucket"); err != nil {
		t.Fatalf("RemoveBucket: %v", err)
	}
	if gotMethod != http.MethodDelete {
		t.Errorf("method = %q, want DELETE", gotMethod)
	}
	if !strings.Contains(gotPath, "old-bucket") {
		t.Errorf("path = %q, want to contain old-bucket", gotPath)
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
	// atomic: the SDK serializes paginator requests today, but a future
	// SDK change firing concurrent page reads would race a plain int.
	var page atomic.Int32
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
		n := page.Add(1)
		if n == 1 {
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
	if got := page.Load(); got < 2 {
		t.Errorf("paginator made only %d HTTP requests, want 2", got)
	}
}

// TestList_PaginatedV1NextMarkerFallback covers the GCS-fallback path
// (--use-list-objects-v1). v2 has no NewListObjectsPaginator for the v1
// API, so the storage layer iterates manually with two NextMarker
// strategies: the response's <NextMarker> when delimiter is set, otherwise
// the last <Contents><Key>. Without delimiter, this test feeds two pages
// and asserts the second page is fetched -- a regression that drops the
// last-key fallback would make pagination silently truncate after page 1.
func TestList_PaginatedV1NextMarkerFallback(t *testing.T) {
	var page atomic.Int32
	page1 := `<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult>
  <IsTruncated>true</IsTruncated>
  <Contents><Key>a/1</Key><LastModified>2024-01-01T00:00:00Z</LastModified><ETag>"e1"</ETag><Size>10</Size><StorageClass>STANDARD</StorageClass></Contents>
  <Contents><Key>a/2</Key><LastModified>2024-01-01T00:00:00Z</LastModified><ETag>"e2"</ETag><Size>20</Size><StorageClass>STANDARD</StorageClass></Contents>
</ListBucketResult>`
	page2 := `<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult>
  <IsTruncated>false</IsTruncated>
  <Contents><Key>a/3</Key><LastModified>2024-01-01T00:00:00Z</LastModified><ETag>"e3"</ETag><Size>30</Size><StorageClass>STANDARD</StorageClass></Contents>
</ListBucketResult>`
	var lastQuery atomic.Value
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		lastQuery.Store(r.URL.RawQuery)
		w.Header().Set("Content-Type", "application/xml")
		w.WriteHeader(http.StatusOK)
		if page.Add(1) == 1 {
			_, _ = w.Write([]byte(page1))
		} else {
			_, _ = w.Write([]byte(page2))
		}
	}))
	defer srv.Close()

	resetClientCache(t)
	s, err := newS3Storage(context.Background(), Options{
		Endpoint:         srv.URL,
		NoSignRequest:    true,
		region:           "us-east-1",
		UseListObjectsV1: true,
		LogLevel:         log.LevelInfo,
	})
	if err != nil {
		t.Fatalf("newS3Storage: %v", err)
	}
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
	if got := page.Load(); got < 2 {
		t.Errorf("paginator made only %d HTTP requests, want 2 (NextMarker fallback failed)", got)
	}
	// The second request must carry a marker derived from the last key
	// of the first page (a/2), since no <NextMarker> was provided.
	if q, _ := lastQuery.Load().(string); !strings.Contains(q, "marker=a%2F2") && !strings.Contains(q, "marker=a/2") {
		t.Errorf("second request query %q missing expected marker=a/2", q)
	}
}

// TestList_PaginatedV1WithExplicitNextMarker covers the other branch:
// when S3 returns <NextMarker> (which it does only when delimiter is set),
// the storage layer uses it directly instead of the last-key fallback.
func TestList_PaginatedV1WithExplicitNextMarker(t *testing.T) {
	var page atomic.Int32
	page1 := `<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult>
  <IsTruncated>true</IsTruncated>
  <NextMarker>explicit-marker</NextMarker>
  <Contents><Key>a</Key><LastModified>2024-01-01T00:00:00Z</LastModified><ETag>"e"</ETag><Size>1</Size><StorageClass>STANDARD</StorageClass></Contents>
</ListBucketResult>`
	page2 := `<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult>
  <IsTruncated>false</IsTruncated>
  <Contents><Key>b</Key><LastModified>2024-01-01T00:00:00Z</LastModified><ETag>"e"</ETag><Size>1</Size><StorageClass>STANDARD</StorageClass></Contents>
</ListBucketResult>`
	var lastQuery atomic.Value
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		lastQuery.Store(r.URL.RawQuery)
		w.Header().Set("Content-Type", "application/xml")
		w.WriteHeader(http.StatusOK)
		if page.Add(1) == 1 {
			_, _ = w.Write([]byte(page1))
		} else {
			_, _ = w.Write([]byte(page2))
		}
	}))
	defer srv.Close()

	resetClientCache(t)
	s, err := newS3Storage(context.Background(), Options{
		Endpoint:         srv.URL,
		NoSignRequest:    true,
		region:           "us-east-1",
		UseListObjectsV1: true,
		LogLevel:         log.LevelInfo,
	})
	if err != nil {
		t.Fatalf("newS3Storage: %v", err)
	}
	u, _ := url.New("s3://my-bucket/")

	count := 0
	for obj := range s.List(context.Background(), u, false) {
		if obj.Err != nil {
			t.Fatalf("list error: %v", obj.Err)
		}
		count++
	}
	if count != 2 {
		t.Errorf("got %d objects, want 2", count)
	}
	if q, _ := lastQuery.Load().(string); !strings.Contains(q, "marker=explicit-marker") {
		t.Errorf("second request query %q missing explicit NextMarker", q)
	}
}

// TestListObjectVersions covers the bucket-version listing path. Single
// XML response with one Version, one DeleteMarker, and one CommonPrefix
// exercises the three emit branches plus the version-id filter and
// future-mod-time skip (latter implicitly: mod times are in the past).
func TestListObjectVersions(t *testing.T) {
	body := `<?xml version="1.0" encoding="UTF-8"?>
<ListVersionsResult>
  <IsTruncated>false</IsTruncated>
  <CommonPrefixes><Prefix>dir/</Prefix></CommonPrefixes>
  <Version>
    <Key>key1</Key>
    <VersionId>v1</VersionId>
    <LastModified>2024-01-01T00:00:00Z</LastModified>
    <ETag>"e1"</ETag>
    <Size>10</Size>
    <StorageClass>STANDARD</StorageClass>
  </Version>
  <DeleteMarker>
    <Key>key1</Key>
    <VersionId>vDel</VersionId>
    <LastModified>2024-01-02T00:00:00Z</LastModified>
  </DeleteMarker>
</ListVersionsResult>`
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/xml")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(body))
	}))
	defer srv.Close()

	s := newTestS3(t, srv)
	u, err := url.New("s3://my-bucket/", url.WithAllVersions(true))
	if err != nil {
		t.Fatalf("url.New: %v", err)
	}

	type seen struct {
		path, vid string
		isDir     bool
		size      int64
	}
	var got []seen
	for obj := range s.List(context.Background(), u, false) {
		if obj.Err != nil {
			t.Fatalf("list error: %v", obj.Err)
		}
		got = append(got, seen{
			path:  obj.URL.Path,
			vid:   obj.URL.VersionID,
			isDir: obj.Type.IsDir(),
			size:  obj.Size,
		})
	}

	// Expect: one CommonPrefix dir, one Version, one DeleteMarker.
	if len(got) != 3 {
		t.Fatalf("got %d objects, want 3 (1 dir + 1 version + 1 delete-marker): %+v", len(got), got)
	}

	var dirs, versions, markers int
	for _, s := range got {
		switch {
		case s.isDir:
			dirs++
		case s.vid == "v1" && s.size == 10:
			versions++
		case s.vid == "vDel" && s.size == 0:
			markers++
		}
	}
	if dirs != 1 || versions != 1 || markers != 1 {
		t.Errorf("dirs=%d versions=%d markers=%d, want 1/1/1 each (got=%+v)", dirs, versions, markers, got)
	}
}

// TestListObjectVersions_VersionIDFilter exercises the path where the URL
// pins a specific VersionID: only the matching Version should be emitted,
// non-matching versions and unrelated delete markers are dropped.
func TestListObjectVersions_VersionIDFilter(t *testing.T) {
	body := `<?xml version="1.0" encoding="UTF-8"?>
<ListVersionsResult>
  <IsTruncated>false</IsTruncated>
  <Version>
    <Key>key1</Key><VersionId>v1</VersionId>
    <LastModified>2024-01-01T00:00:00Z</LastModified><ETag>"a"</ETag><Size>1</Size><StorageClass>STANDARD</StorageClass>
  </Version>
  <Version>
    <Key>key1</Key><VersionId>v2</VersionId>
    <LastModified>2024-01-02T00:00:00Z</LastModified><ETag>"b"</ETag><Size>2</Size><StorageClass>STANDARD</StorageClass>
  </Version>
</ListVersionsResult>`
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/xml")
		_, _ = w.Write([]byte(body))
	}))
	defer srv.Close()

	s := newTestS3(t, srv)
	u, _ := url.New("s3://my-bucket/key1", url.WithVersion("v2"))

	var got []string
	for obj := range s.List(context.Background(), u, false) {
		if obj.Err != nil {
			t.Fatalf("list error: %v", obj.Err)
		}
		got = append(got, obj.URL.VersionID)
	}
	if len(got) != 1 {
		t.Fatalf("got %d objects, want 1 (only v2 should pass the version-id filter): %v", len(got), got)
	}
	if got[0] != "v2" {
		t.Errorf("got version %q, want v2", got[0])
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
	var methodsMu sync.Mutex
	var methods []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		methodsMu.Lock()
		methods = append(methods, r.Method)
		methodsMu.Unlock()
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
	methodsMu.Lock()
	defer methodsMu.Unlock()
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

// TestCopy_MetadataFields verifies every Metadata field wires to the
// correct outgoing header. One oversight in a typed-enum cast (e.g.
// ObjectCannedACL vs StorageClass) would silently swap or drop
// user-specified attributes; catching it at unit level is cheaper than
// finding out from a report about missing encryption on a production
// bucket.
func TestCopy_MetadataFields(t *testing.T) {
	var gotHeaders atomic.Value
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Snapshot headers as a map so the test-side asserts read
		// deterministically.
		snapshot := make(map[string]string, len(r.Header))
		for k, v := range r.Header {
			if len(v) > 0 {
				snapshot[k] = v[0]
			}
		}
		gotHeaders.Store(snapshot)
		w.Header().Set("Content-Type", "application/xml")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?><CopyObjectResult><ETag>"x"</ETag></CopyObjectResult>`))
	}))
	defer srv.Close()

	s := newTestS3(t, srv)
	src, _ := url.New("s3://src/k")
	dst, _ := url.New("s3://dst/k")
	meta := Metadata{
		ACL:                "private",
		CacheControl:       "max-age=60",
		Expires:            "2030-01-02T03:04:05Z",
		StorageClass:       "STANDARD_IA",
		ContentType:        "application/json",
		ContentEncoding:    "gzip",
		ContentDisposition: "attachment",
		EncryptionMethod:   "aws:kms",
		EncryptionKeyID:    "arn:aws:kms:us-east-1:111122223333:key/abcd",
		Directive:          "REPLACE",
		UserDefined:        map[string]string{"owner": "alice"},
	}
	if err := s.Copy(context.Background(), src, dst, meta); err != nil {
		t.Fatalf("Copy: %v", err)
	}

	h, _ := gotHeaders.Load().(map[string]string)
	checks := map[string]string{
		"X-Amz-Acl":                                   "private",
		"Cache-Control":                               "max-age=60",
		"X-Amz-Storage-Class":                         "STANDARD_IA",
		"Content-Type":                                "application/json",
		"Content-Encoding":                            "gzip",
		"Content-Disposition":                         "attachment",
		"X-Amz-Server-Side-Encryption":                "aws:kms",
		"X-Amz-Server-Side-Encryption-Aws-Kms-Key-Id": "arn:aws:kms:us-east-1:111122223333:key/abcd",
		"X-Amz-Metadata-Directive":                    "REPLACE",
		"X-Amz-Meta-Owner":                            "alice",
	}
	for k, want := range checks {
		if got := h[k]; got != want {
			t.Errorf("header %q = %q, want %q", k, got, want)
		}
	}
	// Expires goes into a parsed field (input.Expires *time.Time) and then
	// the SDK renders it as RFC1123 in the X-Amz-Expires or Expires header.
	// We don't assert the exact rendering (SDK quirk); asserting it is
	// non-empty catches the branch where time.Parse failed silently.
	if h["Expires"] == "" && h["X-Amz-Expires"] == "" {
		t.Errorf("Expires metadata field did not produce any Expires/X-Amz-Expires header (headers=%v)", h)
	}
}

// TestCopy_ExpiresParseError covers the time.Parse error branch: an
// unparseable Expires string must surface as an error before the request
// is issued.
func TestCopy_ExpiresParseError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Errorf("Copy must not dial when Expires fails to parse: got %s %s", r.Method, r.URL)
	}))
	defer srv.Close()

	s := newTestS3(t, srv)
	src, _ := url.New("s3://src/k")
	dst, _ := url.New("s3://dst/k")
	err := s.Copy(context.Background(), src, dst, Metadata{Expires: "not-a-time"})
	if err == nil {
		t.Fatal("Copy: want non-nil error for unparseable Expires, got nil")
	}
}

// TestPut_MetadataFields mirrors the Copy test for the upload path.
// MetadataDirective is Copy-only so it's not asserted here.
func TestPut_MetadataFields(t *testing.T) {
	var gotHeaders atomic.Value
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		snapshot := make(map[string]string, len(r.Header))
		for k, v := range r.Header {
			if len(v) > 0 {
				snapshot[k] = v[0]
			}
		}
		gotHeaders.Store(snapshot)
		w.Header().Set("ETag", `"x"`)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	s := newTestS3(t, srv)
	u, _ := url.New("s3://b/k")
	meta := Metadata{
		ACL:                "public-read",
		CacheControl:       "no-cache",
		Expires:            "2030-01-02T03:04:05Z",
		StorageClass:       "ONEZONE_IA",
		ContentType:        "text/plain",
		ContentEncoding:    "identity",
		ContentDisposition: "inline",
		EncryptionMethod:   "AES256",
		UserDefined:        map[string]string{"project": "atlas"},
	}
	if err := s.Put(context.Background(), strings.NewReader("hello"), u, meta, 1, 5*1024*1024); err != nil {
		t.Fatalf("Put: %v", err)
	}

	h, _ := gotHeaders.Load().(map[string]string)
	checks := map[string]string{
		"X-Amz-Acl":                    "public-read",
		"Cache-Control":                "no-cache",
		"X-Amz-Storage-Class":          "ONEZONE_IA",
		"Content-Type":                 "text/plain",
		"Content-Encoding":             "identity",
		"Content-Disposition":          "inline",
		"X-Amz-Server-Side-Encryption": "AES256",
		"X-Amz-Meta-Project":           "atlas",
	}
	for k, want := range checks {
		if got := h[k]; got != want {
			t.Errorf("header %q = %q, want %q", k, got, want)
		}
	}
}

func TestPut_ExpiresParseError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Errorf("Put must not dial when Expires fails to parse: got %s %s", r.Method, r.URL)
	}))
	defer srv.Close()

	s := newTestS3(t, srv)
	u, _ := url.New("s3://b/k")
	err := s.Put(context.Background(), strings.NewReader("hello"), u, Metadata{Expires: "bogus"}, 1, 5*1024*1024)
	if err == nil {
		t.Fatal("Put: want non-nil error for unparseable Expires, got nil")
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
	var gotBody []byte
	gotMethod := ""
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		// io.ReadAll, not a single Read into a fixed-size buffer: a single
		// Read may chunk and an oversized buffer silently truncates the
		// tail. Either pattern would falsely pass on a regression that
		// mangles bytes past the first chunk.
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("read body: %v", err)
		}
		gotBody = body
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

// TestRetryOnNoSuchUpload_SuccessViaStatMatch covers the race-recovery
// branch: the upload returned NoSuchUpload but the object actually landed
// with our generated retry-ID stamped into its user-metadata. retryOnNoSuchUpload
// must Stat, observe the matching retryID, and treat the upload as a
// success (returning nil instead of the NoSuchUpload error).
func TestRetryOnNoSuchUpload_SuccessViaStatMatch(t *testing.T) {
	const noSuchUploadXML = `<?xml version="1.0" encoding="UTF-8"?>
<Error><Code>NoSuchUpload</Code><Message>The specified upload does not exist.</Message></Error>`

	// We don't know the retry-ID up front (it's generated inside Put). The
	// handler captures it from the first PUT's user-metadata header and
	// echoes it back on the subsequent HEAD.
	var capturedRetryID atomic.Value
	var phase atomic.Int32 // 0=expect PUT, 1=expect HEAD

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch phase.Load() {
		case 0:
			if r.Method != http.MethodPut {
				t.Errorf("phase 0 method = %q, want PUT", r.Method)
			}
			capturedRetryID.Store(r.Header.Get("X-Amz-Meta-S5cmd-Upload-Retry-Id"))
			phase.Store(1)
			w.Header().Set("Content-Type", "application/xml")
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(noSuchUploadXML))
		case 1:
			if r.Method != http.MethodHead {
				t.Errorf("phase 1 method = %q, want HEAD (Stat in retry loop)", r.Method)
			}
			id, _ := capturedRetryID.Load().(string)
			w.Header().Set("X-Amz-Meta-S5cmd-Upload-Retry-Id", id)
			w.Header().Set("Content-Length", "5")
			w.Header().Set("ETag", `"abc"`)
			w.WriteHeader(http.StatusOK)
		default:
			t.Errorf("unexpected request after retry success: %s %s", r.Method, r.URL.Path)
			w.WriteHeader(http.StatusInternalServerError)
		}
	}))
	defer srv.Close()

	resetClientCache(t)
	s, err := newS3Storage(context.Background(), Options{
		Endpoint:               srv.URL,
		NoSignRequest:          true,
		region:                 "us-east-1",
		NoSuchUploadRetryCount: 3,
		LogLevel:               log.LevelInfo,
	})
	if err != nil {
		t.Fatalf("newS3Storage: %v", err)
	}

	u, _ := url.New("s3://b/k")
	if err := s.Put(context.Background(), strings.NewReader("hello"), u, Metadata{}, 1, 5*1024*1024); err != nil {
		t.Errorf("Put: want nil (Stat-match must rescue NoSuchUpload), got %v", err)
	}
	if got := capturedRetryID.Load(); got == nil || got == "" {
		t.Error("retry-ID was never set on the PUT request")
	}
}

// TestRetryOnNoSuchUpload_GivesUp covers the exhaust-retries branch: every
// PUT returns NoSuchUpload AND every Stat returns 404, so retryOnNoSuchUpload
// runs out of attempts and wraps the final error with "RetryOnNoSuchUpload".
func TestRetryOnNoSuchUpload_GivesUp(t *testing.T) {
	const noSuchUploadXML = `<?xml version="1.0" encoding="UTF-8"?>
<Error><Code>NoSuchUpload</Code><Message>The specified upload does not exist.</Message></Error>`

	var puts, heads atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPut:
			puts.Add(1)
			w.Header().Set("Content-Type", "application/xml")
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(noSuchUploadXML))
		case http.MethodHead:
			heads.Add(1)
			w.WriteHeader(http.StatusNotFound)
		default:
			w.WriteHeader(http.StatusInternalServerError)
		}
	}))
	defer srv.Close()

	resetClientCache(t)
	s, err := newS3Storage(context.Background(), Options{
		Endpoint:               srv.URL,
		NoSignRequest:          true,
		region:                 "us-east-1",
		NoSuchUploadRetryCount: 2,
		LogLevel:               log.LevelInfo,
	})
	if err != nil {
		t.Fatalf("newS3Storage: %v", err)
	}

	u, _ := url.New("s3://b/k")
	err = s.Put(context.Background(), strings.NewReader("hello"), u, Metadata{}, 1, 5*1024*1024)
	if err == nil {
		t.Fatal("Put: want non-nil error after exhausting retries")
	}
	if !strings.Contains(err.Error(), "RetryOnNoSuchUpload") {
		t.Errorf("err = %v, want to contain 'RetryOnNoSuchUpload'", err)
	}
	if puts.Load() < 2 {
		t.Errorf("puts = %d, want >= 2 (initial + retries)", puts.Load())
	}
	if heads.Load() < 1 {
		t.Errorf("heads = %d, want >= 1 (Stat in retry loop)", heads.Load())
	}
}

// TestRetryOnNoSuchUpload_RetryReturnsDifferentError covers the branch
// where a retry attempt produces a non-NoSuchUpload error (e.g. credentials
// rotated mid-flight so the second PUT comes back AccessDenied). The loop
// exits because errIsNoSuchUpload is now false on the new error; the
// give-up wrapper also doesn't fire. The returned error must be normalized
// to the v1-shape "Code: Message" string rather than the raw verbose
// smithy OperationError -- the rest of Put's contract is the clean form.
func TestRetryOnNoSuchUpload_RetryReturnsDifferentError(t *testing.T) {
	const noSuchUploadXML = `<?xml version="1.0" encoding="UTF-8"?>
<Error><Code>NoSuchUpload</Code><Message>The specified upload does not exist.</Message></Error>`
	const accessDeniedXML = `<?xml version="1.0" encoding="UTF-8"?>
<Error><Code>AccessDenied</Code><Message>Access Denied</Message></Error>`

	var puts atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPut:
			n := puts.Add(1)
			w.Header().Set("Content-Type", "application/xml")
			if n == 1 {
				w.WriteHeader(http.StatusNotFound)
				_, _ = w.Write([]byte(noSuchUploadXML))
			} else {
				w.WriteHeader(http.StatusForbidden)
				_, _ = w.Write([]byte(accessDeniedXML))
			}
		case http.MethodHead:
			// Stat in the retry loop: object not yet there, retry PUT.
			w.WriteHeader(http.StatusNotFound)
		default:
			w.WriteHeader(http.StatusInternalServerError)
		}
	}))
	defer srv.Close()

	resetClientCache(t)
	s, err := newS3Storage(context.Background(), Options{
		Endpoint:               srv.URL,
		NoSignRequest:          true,
		region:                 "us-east-1",
		NoSuchUploadRetryCount: 2,
		LogLevel:               log.LevelInfo,
	})
	if err != nil {
		t.Fatalf("newS3Storage: %v", err)
	}

	u, _ := url.New("s3://b/k")
	err = s.Put(context.Background(), strings.NewReader("hello"), u, Metadata{}, 1, 5*1024*1024)
	if err == nil {
		t.Fatal("Put: want non-nil error (AccessDenied from retry)")
	}
	// v1-shape: "Code: Message". Must NOT contain the verbose smithy
	// OperationError wrapping ("operation error S3: ...").
	if strings.Contains(err.Error(), "operation error S3") {
		t.Errorf("err = %v; want v1-shape 'Code: Message', got raw smithy OperationError", err)
	}
	if !strings.Contains(err.Error(), "AccessDenied") {
		t.Errorf("err = %v; want to contain 'AccessDenied'", err)
	}
	// Retry loop must NOT have wrapped with RetryOnNoSuchUpload since the
	// final error is not NoSuchUpload.
	if strings.Contains(err.Error(), "RetryOnNoSuchUpload") {
		t.Errorf("err = %v; must NOT be wrapped with RetryOnNoSuchUpload when the final error is different", err)
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

// TestSDKV2Logger_Logf is a smoke test guarding against the nil-deref on
// the package-global log.Logger that bit during test development. The
// global is initialized in TestMain, so this test only needs to verify the
// Logf adapter doesn't panic when called.
func TestSDKV2Logger_Logf(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("sdkV2Logger.Logf panicked: %v", r)
		}
	}()
	sdkV2Logger{}.Logf(logging.Debug, "smoke %s %d", "trace", 42)
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
