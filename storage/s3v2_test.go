package storage

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
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

func TestNewS3Storage_OptionsRouting(t *testing.T) {
	cases := []struct {
		name           string
		endpoint       string
		wantPathStyle  bool
		wantAccelerate bool
	}{
		{"empty -> aws virtual host", "", false, false},
		{"transfer accel", "https://s3-accelerate.amazonaws.com", false, true},
		{"gcs -> virtual host", "https://storage.googleapis.com", false, false},
		{"minio -> path style", "http://minio.local:9000", true, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// We can't easily inspect *s3.Options after construction; instead
			// re-derive the same booleans the constructor uses, ensuring our
			// supportsTransferAcceleration / isVirtualHostStyle wiring stays
			// in sync with the constructor.
			endpointURL, _ := parseEndpoint(tc.endpoint)
			gotAccel := supportsTransferAcceleration(endpointURL)
			gotPath := !isVirtualHostStyle(endpointURL)
			if gotAccel {
				// The constructor clears the endpoint when accel is on, so
				// path-style detection runs on the cleared URL.
				gotPath = !isVirtualHostStyle(sentinelURL)
			}
			if gotAccel != tc.wantAccelerate {
				t.Errorf("UseAccelerate = %v, want %v", gotAccel, tc.wantAccelerate)
			}
			if gotPath != tc.wantPathStyle {
				t.Errorf("UsePathStyle = %v, want %v", gotPath, tc.wantPathStyle)
			}
		})
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
	// v2 SDK lowercases user-metadata keys when extracting from the
	// X-Amz-Meta-* headers (the HTTP spec lets servers casefold headers
	// freely, so the SDK normalizes to lowercase for stable lookup).
	t.Logf("UserDefined keys = %v", meta.UserDefined)
	gotOwner := metaLookup(meta.UserDefined, "Owner")
	if gotOwner != "alice" {
		t.Errorf("UserDefined Owner = %q, want alice", gotOwner)
	}
	gotProject := metaLookup(meta.UserDefined, "Project")
	if gotProject != "atlas" {
		t.Errorf("UserDefined Project = %q, want atlas", gotProject)
	}
}

// metaLookup is case-insensitive because v2 SDK normalizes user-metadata
// header names (HTTP semantics).
func metaLookup(m map[string]string, key string) string {
	for k, v := range m {
		if strings.EqualFold(k, key) {
			return v
		}
	}
	return ""
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
