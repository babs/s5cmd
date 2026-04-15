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

// ListBuckets returns the buckets visible to the configured credentials,
// optionally filtered by name prefix.
func (s *S3v2) ListBuckets(ctx context.Context, prefix string) ([]Bucket, error) {
	out, err := s.api.ListBuckets(ctx, &s3v2.ListBucketsInput{})
	if err != nil {
		return nil, err
	}

	var buckets []Bucket
	for _, b := range out.Buckets {
		name := awsv2.ToString(b.Name)
		if prefix == "" || strings.HasPrefix(name, prefix) {
			buckets = append(buckets, Bucket{
				CreationDate: awsv2.ToTime(b.CreationDate),
				Name:         name,
			})
		}
	}
	return buckets, nil
}

// MakeBucket creates an S3 bucket with the given name.
func (s *S3v2) MakeBucket(ctx context.Context, name string) error {
	if s.dryRun {
		return nil
	}

	_, err := s.api.CreateBucket(ctx, &s3v2.CreateBucketInput{
		Bucket: awsv2.String(name),
	})
	return err
}

// RemoveBucket removes an S3 bucket with the given name.
func (s *S3v2) RemoveBucket(ctx context.Context, name string) error {
	if s.dryRun {
		return nil
	}

	_, err := s.api.DeleteBucket(ctx, &s3v2.DeleteBucketInput{
		Bucket: awsv2.String(name),
	})
	return err
}

// SetBucketVersioning sets the versioning property of the bucket. The
// versioningStatus value is the typed BucketVersioningStatus enum
// ("Enabled" / "Suspended") in v2.
func (s *S3v2) SetBucketVersioning(ctx context.Context, versioningStatus, bucket string) error {
	if s.dryRun {
		return nil
	}

	_, err := s.api.PutBucketVersioning(ctx, &s3v2.PutBucketVersioningInput{
		Bucket: awsv2.String(bucket),
		VersioningConfiguration: &s3types.VersioningConfiguration{
			Status: s3types.BucketVersioningStatus(versioningStatus),
		},
	})
	return err
}

// GetBucketVersioning returns the versioning property of the bucket. Empty
// string indicates "never enabled" (S3 returns no Status field in that case).
func (s *S3v2) GetBucketVersioning(ctx context.Context, bucket string) (string, error) {
	out, err := s.api.GetBucketVersioning(ctx, &s3v2.GetBucketVersioningInput{
		Bucket: awsv2.String(bucket),
	})
	if err != nil {
		return "", err
	}
	return string(out.Status), nil
}
