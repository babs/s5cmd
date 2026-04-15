package storage

// S3v2 mirrors S3 but talks to S3 via aws-sdk-go-v2. Methods are ported from
// s3.go incrementally; once parity is reached, NewRemoteClient is switched
// to return *S3v2 and the v1 implementation is removed.

import (
	"context"
	"errors"
	urlpkg "net/url"
	"strings"

	awsv2 "github.com/aws/aws-sdk-go-v2/aws"
	s3v2 "github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"

	"github.com/peak/s5cmd/v2/storage/url"
)

type S3v2 struct {
	api                    *s3v2.Client
	endpointURL            urlpkg.URL
	dryRun                 bool
	useListObjectsV1       bool
	noSuchUploadRetryCount int
	requestPayer           string
}

// newS3StorageV2 builds a v2-backed S3 storage handle. Mirrors newS3Storage
// and is used during incremental migration; once all methods are ported and
// callers swap, this becomes the sole constructor.
func newS3StorageV2(ctx context.Context, opts Options) (*S3v2, error) {
	endpointURL, err := parseEndpoint(opts.Endpoint)
	if err != nil {
		return nil, err
	}

	client, err := globalClientCacheV2.newClient(ctx, opts)
	if err != nil {
		return nil, err
	}

	return &S3v2{
		api:                    client,
		endpointURL:            endpointURL,
		dryRun:                 opts.DryRun,
		useListObjectsV1:       opts.UseListObjectsV1,
		requestPayer:           opts.RequestPayer,
		noSuchUploadRetryCount: opts.NoSuchUploadRetryCount,
	}, nil
}

func (s *S3v2) requestPayerEnum() s3types.RequestPayer {
	if s.requestPayer == "" {
		return ""
	}
	return s3types.RequestPayer(s.requestPayer)
}

// errIsNotFound covers both the typed *types.NotFound (HEAD operations) and
// the generic smithy.APIError fallback for "NotFound" / "NoSuchKey".
func errIsNotFound(err error) bool {
	var notFound *s3types.NotFound
	if errors.As(err, &notFound) {
		return true
	}
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		switch apiErr.ErrorCode() {
		case "NotFound", "NoSuchKey":
			return true
		}
	}
	return false
}

// Stat retrieves metadata from S3 object without returning the object itself.
func (s *S3v2) Stat(ctx context.Context, u *url.URL) (*Object, error) {
	input := &s3v2.HeadObjectInput{
		Bucket:       awsv2.String(u.Bucket),
		Key:          awsv2.String(u.Path),
		RequestPayer: s.requestPayerEnum(),
	}
	if u.VersionID != "" {
		input.VersionId = awsv2.String(u.VersionID)
	}

	output, err := s.api.HeadObject(ctx, input)
	if err != nil {
		if errIsNotFound(err) {
			return nil, &ErrGivenObjectNotFound{ObjectAbsPath: u.Absolute()}
		}
		return nil, err
	}

	mod := awsv2.ToTime(output.LastModified)
	obj := &Object{
		URL:     u,
		Etag:    strings.Trim(awsv2.ToString(output.ETag), `"`),
		ModTime: &mod,
		Size:    awsv2.ToInt64(output.ContentLength),
	}

	if s.noSuchUploadRetryCount > 0 {
		if retryID, ok := output.Metadata[metadataKeyRetryID]; ok {
			obj.retryID = retryID
		}
	}
	return obj, nil
}

// HeadBucket performs a HEAD on the bucket; returns nil if reachable and
// authorized, otherwise the underlying error.
func (s *S3v2) HeadBucket(ctx context.Context, u *url.URL) error {
	_, err := s.api.HeadBucket(ctx, &s3v2.HeadBucketInput{
		Bucket: awsv2.String(u.Bucket),
	})
	return err
}

// HeadObject returns Object + Metadata for a single key; ErrGivenObjectNotFound
// when the key is absent.
func (s *S3v2) HeadObject(ctx context.Context, u *url.URL) (*Object, *Metadata, error) {
	input := &s3v2.HeadObjectInput{
		Bucket:       awsv2.String(u.Bucket),
		Key:          awsv2.String(u.Path),
		RequestPayer: s.requestPayerEnum(),
	}
	if u.VersionID != "" {
		input.VersionId = awsv2.String(u.VersionID)
	}

	output, err := s.api.HeadObject(ctx, input)
	if err != nil {
		if errIsNotFound(err) {
			return nil, nil, &ErrGivenObjectNotFound{ObjectAbsPath: u.Absolute()}
		}
		return nil, nil, err
	}

	// STANDARD storage class is omitted from the response per S3 API spec.
	storageClassStr := "STANDARD"
	if output.StorageClass != "" {
		storageClassStr = string(output.StorageClass)
	}

	obj := &Object{
		URL:          u,
		ModTime:      output.LastModified,
		Etag:         strings.Trim(awsv2.ToString(output.ETag), `"`),
		Size:         awsv2.ToInt64(output.ContentLength),
		StorageClass: StorageClass(storageClassStr),
	}

	metadata := &Metadata{
		ContentType:      awsv2.ToString(output.ContentType),
		EncryptionMethod: string(output.ServerSideEncryption),
		UserDefined:      output.Metadata,
	}
	return obj, metadata, nil
}
