package storage

// Shared S3-storage helpers, constants, and SDK-agnostic types. Survives
// the v1->v2 migration unchanged: anything here is independent of which
// AWS SDK powers the wire calls.

import (
	"context"
	"crypto/rand"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"math/big"
	urlpkg "net/url"
)

const (
	// deleteObjectsMax is the max number of keys S3 accepts in a single
	// DeleteObjects request.
	deleteObjectsMax = 1000

	// transferAccelEndpoint is the canonical Amazon S3 Transfer Acceleration
	// hostname; presence in opts flips s3.Options.UseAccelerate on.
	transferAccelEndpoint = "s3-accelerate.amazonaws.com"

	// gcsEndpoint identifies Google Cloud Storage; used by the v2 GCS
	// header-strip middleware and by IsGoogleEndpoint.
	gcsEndpoint = "storage.googleapis.com"

	// metadataKeyRetryID names the user-metadata key carrying the
	// idempotent-retry token for the NoSuchUpload workaround.
	metadataKeyRetryID = "s5cmd-upload-retry-id"
)

// sentinelURL is the zero-value URL used to signal "no endpoint configured".
var sentinelURL = urlpkg.URL{}

// SelectQuery describes an S3 Select request. SDK-agnostic.
type SelectQuery struct {
	InputFormat           string
	InputContentStructure string
	FileHeaderInfo        string
	OutputFormat          string
	ExpressionType        string
	Expression            string
	CompressionType       string
}

type eventType string

const (
	jsonType    eventType = "json"
	csvType     eventType = "csv"
	parquetType eventType = "parquet"
)

func parseEndpoint(endpoint string) (urlpkg.URL, error) {
	if endpoint == "" {
		return sentinelURL, nil
	}

	u, err := urlpkg.Parse(endpoint)
	if err != nil {
		return sentinelURL, fmt.Errorf("parse endpoint %q: %v", endpoint, err)
	}
	return *u, nil
}

// supportsTransferAcceleration reports whether endpoint is the canonical
// S3 Transfer Acceleration hostname.
func supportsTransferAcceleration(endpoint urlpkg.URL) bool {
	return endpoint.Hostname() == transferAccelEndpoint
}

// IsGoogleEndpoint reports whether endpoint targets GCS's S3-compatible
// XML API. Used to enable special-cased behavior (per-object delete loop,
// header-strip middleware).
func IsGoogleEndpoint(endpoint urlpkg.URL) bool {
	return endpoint.Hostname() == gcsEndpoint
}

// isVirtualHostStyle returns true if the endpoint is known to support
// virtual-host-style addressing. Default to path-style for unknown
// endpoints (S3-compat backends often only support path-style).
func isVirtualHostStyle(endpoint urlpkg.URL) bool {
	return endpoint == sentinelURL || supportsTransferAcceleration(endpoint) || IsGoogleEndpoint(endpoint)
}

// IsCancelationError reports whether err signals a context cancellation
// (request was abandoned before completion). Used by error/error.go to
// distinguish user-driven aborts from real failures.
func IsCancelationError(err error) bool {
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}

// generateRetryID returns a fresh idempotent-upload token as *string. v1
// stored object metadata as map[string]*string so the pointer flavor was
// natural; v2 keeps the function for compatibility with helpers that still
// produce the pointer (generateRetryIDStr derives a string from this).
func generateRetryID() *string {
	num, _ := rand.Int(rand.Reader, big.NewInt(math.MaxInt64))
	s := num.String()
	return &s
}

// EventStreamDecoder decodes one record at a time from an S3 Select
// streaming response.
type EventStreamDecoder interface {
	Decode() ([]byte, error)
}

type JSONDecoder struct {
	decoder *json.Decoder
}

func NewJSONDecoder(reader io.Reader) EventStreamDecoder {
	return &JSONDecoder{decoder: json.NewDecoder(reader)}
}

func (jd *JSONDecoder) Decode() ([]byte, error) {
	var val json.RawMessage
	if err := jd.decoder.Decode(&val); err != nil {
		return nil, err
	}
	return val, nil
}

type CsvDecoder struct {
	decoder   *csv.Reader
	delimiter string
}

func NewCsvDecoder(reader io.Reader) EventStreamDecoder {
	d := &CsvDecoder{
		decoder:   csv.NewReader(reader),
		delimiter: ",",
	}
	// AWS S3 Select returns CSV values that may contain embedded quotes;
	// LazyQuotes lets the parser accept them without erroring.
	d.decoder.LazyQuotes = true
	return d
}

func (cd *CsvDecoder) Decode() ([]byte, error) {
	res, err := cd.decoder.Read()
	if err != nil {
		return nil, err
	}

	var out []byte
	for i, s := range res {
		if i != len(res)-1 {
			s = fmt.Sprintf("%s%s", s, cd.delimiter)
		}
		out = append(out, s...)
	}
	return out, nil
}

