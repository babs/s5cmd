package storage

// S3 mirrors S3 but talks to S3 via aws-sdk-go-v2. Methods are ported from
// s3.go incrementally; once parity is reached, NewRemoteClient is switched
// to return *S3 and the v1 implementation is removed.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	urlpkg "net/url"
	"os"
	"strings"
	"sync"
	"time"

	awsv2 "github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	s3v2 "github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"

	"github.com/peak/s5cmd/v2/log"
	"github.com/peak/s5cmd/v2/storage/url"
)

type S3 struct {
	api                    *s3v2.Client
	uploader               *manager.Uploader
	downloader             *manager.Downloader
	endpointURL            urlpkg.URL
	dryRun                 bool
	useListObjectsV1       bool
	noSuchUploadRetryCount int
	requestPayer           string
}

// Compile-time assertion that *S3 satisfies the Storage interface, so any
// missing port surfaces as a build error rather than a runtime nil-method.
var _ Storage = (*S3)(nil)


// newS3StorageV2 builds a v2-backed S3 storage handle. Mirrors newS3Storage
// and is used during incremental migration; once all methods are ported and
// callers swap, this becomes the sole constructor.
//
// Region resolution: when opts.region is empty and opts.bucket is set, the
// bucket's region is auto-discovered against a temporary us-east-1 client
// (matches v1 setSessionRegion behavior). The resolved region is then folded
// into opts before constructing the cached client, so cache lookups for the
// same bucket reuse the same regional client.
func newS3Storage(ctx context.Context, opts Options) (*S3, error) {
	endpointURL, err := parseEndpoint(opts.Endpoint)
	if err != nil {
		return nil, err
	}

	if opts.region == "" && opts.bucket != "" {
		region, rerr := resolveBucketRegion(ctx, opts)
		if rerr != nil {
			// Don't deny the request: surface to logs and let the actual
			// operation report the underlying error in context (matches v1).
			log.Error(log.ErrorMessage{
				Err: fmt.Sprintf("session: fetching region failed: %v", rerr),
			})
		} else if region != "" {
			opts.region = region
		}
	}

	client, err := globalClientCache.newClient(ctx, opts)
	if err != nil {
		return nil, err
	}

	return &S3{
		api:                    client,
		uploader:               manager.NewUploader(client),
		downloader:             manager.NewDownloader(client),
		endpointURL:            endpointURL,
		dryRun:                 opts.DryRun,
		useListObjectsV1:       opts.UseListObjectsV1,
		requestPayer:           opts.RequestPayer,
		noSuchUploadRetryCount: opts.NoSuchUploadRetryCount,
	}, nil
}

// resolveBucketRegion discovers the region of opts.bucket using a
// throwaway us-east-1 client that bypasses the per-Options cache (the cache
// would otherwise pin the client to the wrong region).
func resolveBucketRegion(ctx context.Context, opts Options) (string, error) {
	probe := opts
	probe.region = "us-east-1"
	probe.bucket = "" // skip the recursion guard

	client, err := globalClientCache.newClient(ctx, probe)
	if err != nil {
		return "", err
	}

	region, err := manager.GetBucketRegion(ctx, client, opts.bucket, func(o *s3v2.Options) {
		// HeadBucket via path-style is the safest fallback for S3-compat
		// backends; v1's setSessionRegion did the same.
		o.UsePathStyle = true
	})
	if err != nil {
		// BucketNotFound is the bucket genuinely not existing in any
		// reachable partition. Don't surface as a region-detection
		// failure: the actual operation (HeadBucket, etc.) will report
		// the NotFound itself, and double-logging confuses callers /
		// breaks e2e regex expectations.
		var notFound manager.BucketNotFound
		if errors.As(err, &notFound) {
			return "", nil
		}
		return "", err
	}
	return region, nil
}

// cleanedAPIErr wraps a smithy.APIError so callers see a v1-style
// "Code: Message" string while errors.As(err, &apiErr) still surfaces the
// underlying smithy.APIError for classification (NoSuchKey / NoSuchBucket /
// AccessDenied checks in command/sync).
type cleanedAPIErr struct {
	wrapped smithy.APIError
}

func (e *cleanedAPIErr) Error() string {
	msg := e.wrapped.ErrorMessage()
	if msg == "" {
		return e.wrapped.ErrorCode()
	}
	return e.wrapped.ErrorCode() + ": " + msg
}

// Unwrap returns the wrapped smithy.APIError unchanged so errors.As /
// errors.Is traversals reach the typed underlying error. smithy.APIError
// always satisfies the error interface (it embeds Error() string), so the
// type assertion in earlier code was dead -- removed.
func (e *cleanedAPIErr) Unwrap() error {
	return e.wrapped.(error)
}

// cleanV2Err unwraps verbose smithy OperationError chains down to the inner
// API error and returns a "Code: Message" formatted error. Preserves v1-era
// error string shape that command-level UX and e2e regexes expect.
// errors.As traversal still finds the underlying smithy.APIError so callers
// like command/sync.shouldStopSync continue to classify by code.
//
// Returns the original error unchanged when no smithy.APIError is present
// (e.g. local filesystem errors, context cancellation, network errors).
func cleanV2Err(err error) error {
	if err == nil {
		return nil
	}
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		return &cleanedAPIErr{wrapped: apiErr}
	}
	return err
}

func (s *S3) requestPayerEnum() s3types.RequestPayer {
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
func (s *S3) Stat(ctx context.Context, u *url.URL) (*Object, error) {
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
		return nil, cleanV2Err(err)
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
func (s *S3) HeadBucket(ctx context.Context, u *url.URL) error {
	_, err := s.api.HeadBucket(ctx, &s3v2.HeadBucketInput{
		Bucket: awsv2.String(u.Bucket),
	})
	return cleanV2Err(err)
}

// HeadObject returns Object + Metadata for a single key; ErrGivenObjectNotFound
// when the key is absent.
func (s *S3) HeadObject(ctx context.Context, u *url.URL) (*Object, *Metadata, error) {
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
		return nil, nil, cleanV2Err(err)
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

	// v2 returns nil when no user metadata is present; v1 returned an empty
	// map. Preserve the empty-map shape so JSON callers see "metadata":{}
	// instead of "metadata":null. Key case is left as-is: v2 SDK lowercases
	// keys during deserialization (deserializers.go ToLower), and v1 SDK
	// did the same -- e2e regexes already expect lowercase, so there's no
	// case translation to do.
	userMeta := output.Metadata
	if userMeta == nil {
		userMeta = map[string]string{}
	}

	metadata := &Metadata{
		ContentType:      awsv2.ToString(output.ContentType),
		EncryptionMethod: string(output.ServerSideEncryption),
		UserDefined:      userMeta,
	}
	return obj, metadata, nil
}

// ListBuckets returns the buckets visible to the configured credentials,
// optionally filtered by name prefix.
func (s *S3) ListBuckets(ctx context.Context, prefix string) ([]Bucket, error) {
	out, err := s.api.ListBuckets(ctx, &s3v2.ListBucketsInput{})
	if err != nil {
		return nil, cleanV2Err(err)
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
func (s *S3) MakeBucket(ctx context.Context, name string) error {
	if s.dryRun {
		return nil
	}

	_, err := s.api.CreateBucket(ctx, &s3v2.CreateBucketInput{
		Bucket: awsv2.String(name),
	})
	return cleanV2Err(err)
}

// RemoveBucket removes an S3 bucket with the given name.
func (s *S3) RemoveBucket(ctx context.Context, name string) error {
	if s.dryRun {
		return nil
	}

	_, err := s.api.DeleteBucket(ctx, &s3v2.DeleteBucketInput{
		Bucket: awsv2.String(name),
	})
	return cleanV2Err(err)
}

// SetBucketVersioning sets the versioning property of the bucket. The
// versioningStatus value is the typed BucketVersioningStatus enum
// ("Enabled" / "Suspended") in v2.
func (s *S3) SetBucketVersioning(ctx context.Context, versioningStatus, bucket string) error {
	if s.dryRun {
		return nil
	}

	_, err := s.api.PutBucketVersioning(ctx, &s3v2.PutBucketVersioningInput{
		Bucket: awsv2.String(bucket),
		VersioningConfiguration: &s3types.VersioningConfiguration{
			Status: s3types.BucketVersioningStatus(versioningStatus),
		},
	})
	return cleanV2Err(err)
}

// GetBucketVersioning returns the versioning property of the bucket. Empty
// string indicates "never enabled" (S3 returns no Status field in that case).
func (s *S3) GetBucketVersioning(ctx context.Context, bucket string) (string, error) {
	out, err := s.api.GetBucketVersioning(ctx, &s3v2.GetBucketVersioningInput{
		Bucket: awsv2.String(bucket),
	})
	if err != nil {
		return "", cleanV2Err(err)
	}
	return string(out.Status), nil
}

// chunk is the v2 equivalent of chunk{} -- holds a batch of object IDs to
// delete. ObjectIdentifier in v2 is a value type rather than a pointer.
type chunk struct {
	Bucket string
	Keys   []s3types.ObjectIdentifier
}

// calculateChunks pulls URLs off ch and emits delete batches sized by S3's
// 1000-key limit (1 for GCS, which does not support multi-delete).
func (s *S3) calculateChunks(ch <-chan *url.URL) <-chan chunk {
	chunkch := make(chan chunk)
	chunkSize := deleteObjectsMax
	if IsGoogleEndpoint(s.endpointURL) {
		chunkSize = 1
	}

	go func() {
		defer close(chunkch)

		var keys []s3types.ObjectIdentifier
		var bucket string
		for u := range ch {
			bucket = u.Bucket
			objid := s3types.ObjectIdentifier{Key: awsv2.String(u.Path)}
			if u.VersionID != "" {
				objid.VersionId = awsv2.String(u.VersionID)
			}
			keys = append(keys, objid)

			if len(keys) == chunkSize {
				chunkch <- chunk{Bucket: bucket, Keys: keys}
				keys = nil
			}
		}
		if len(keys) > 0 {
			chunkch <- chunk{Bucket: bucket, Keys: keys}
		}
	}()

	return chunkch
}

// Delete removes a single object.
func (s *S3) Delete(ctx context.Context, u *url.URL) error {
	c := chunk{
		Bucket: u.Bucket,
		Keys:   []s3types.ObjectIdentifier{{Key: awsv2.String(u.Path)}},
	}
	if u.VersionID != "" {
		c.Keys[0].VersionId = awsv2.String(u.VersionID)
	}

	resultch := make(chan *Object, 1)
	defer close(resultch)
	s.doDelete(ctx, c, resultch)
	obj := <-resultch
	return obj.Err
}

// doDelete executes a single chunk against DeleteObjects (or per-object
// DeleteObject for GCS, which does not implement the batch endpoint).
func (s *S3) doDelete(ctx context.Context, c chunk, resultch chan *Object) {
	if s.dryRun {
		for _, k := range c.Keys {
			key := fmt.Sprintf("s3://%v/%v", c.Bucket, awsv2.ToString(k.Key))
			u, _ := url.New(key)
			u.VersionID = awsv2.ToString(k.VersionId)
			resultch <- &Object{URL: u}
		}
		return
	}

	if IsGoogleEndpoint(s.endpointURL) {
		for _, k := range c.Keys {
			_, err := s.api.DeleteObject(ctx, &s3v2.DeleteObjectInput{
				Bucket:       awsv2.String(c.Bucket),
				Key:          k.Key,
				RequestPayer: s.requestPayerEnum(),
			})
			if err != nil {
				resultch <- &Object{Err: cleanV2Err(err)}
				return
			}
			key := fmt.Sprintf("s3://%v/%v", c.Bucket, awsv2.ToString(k.Key))
			u, _ := url.New(key)
			resultch <- &Object{URL: u}
		}
		return
	}

	out, err := s.api.DeleteObjects(ctx, &s3v2.DeleteObjectsInput{
		Bucket:       awsv2.String(c.Bucket),
		Delete:       &s3types.Delete{Objects: c.Keys},
		RequestPayer: s.requestPayerEnum(),
	})
	if err != nil {
		resultch <- &Object{Err: cleanV2Err(err)}
		return
	}

	for _, d := range out.Deleted {
		key := fmt.Sprintf("s3://%v/%v", c.Bucket, awsv2.ToString(d.Key))
		u, _ := url.New(key)
		u.VersionID = awsv2.ToString(d.VersionId)
		resultch <- &Object{URL: u}
	}
	for _, e := range out.Errors {
		key := fmt.Sprintf("s3://%v/%v", c.Bucket, awsv2.ToString(e.Key))
		u, _ := url.New(key)
		u.VersionID = awsv2.ToString(e.VersionId)
		resultch <- &Object{
			URL: u,
			Err: fmt.Errorf("%v", awsv2.ToString(e.Message)),
		}
	}
}

// List dispatches to a paginator variant based on URL flags.
//
// followSymlinks is unused for S3 (only meaningful on local FS), kept to
// satisfy the Storage interface.
func (s *S3) List(ctx context.Context, u *url.URL, _ bool) <-chan *Object {
	if u.VersionID != "" || u.AllVersions {
		return s.listObjectVersions(ctx, u)
	}
	if s.useListObjectsV1 {
		return s.listObjects(ctx, u)
	}
	return s.listObjectsV2(ctx, u)
}

// listObjectsV2 paginates ListObjectsV2; matches v1 listObjectsV2 semantics
// (skip future-dated objects, emit dirs for common prefixes, ErrNoObjectFound
// when the result set is empty for a non-bucket URL).
func (s *S3) listObjectsV2(ctx context.Context, u *url.URL) <-chan *Object {
	in := &s3v2.ListObjectsV2Input{
		Bucket:       awsv2.String(u.Bucket),
		Prefix:       awsv2.String(u.Prefix),
		RequestPayer: s.requestPayerEnum(),
	}
	if u.Delimiter != "" {
		in.Delimiter = awsv2.String(u.Delimiter)
	}

	objCh := make(chan *Object)
	go func() {
		defer close(objCh)
		objectFound := false
		var now time.Time

		paginator := s3v2.NewListObjectsV2Paginator(s.api, in)
		for paginator.HasMorePages() {
			page, err := paginator.NextPage(ctx)
			if err != nil {
				objCh <- &Object{Err: cleanV2Err(err)}
				return
			}

			for _, c := range page.CommonPrefixes {
				prefix := awsv2.ToString(c.Prefix)
				if !u.Match(prefix) {
					continue
				}
				newurl := u.Clone()
				newurl.Path = prefix
				objCh <- &Object{URL: newurl, Type: ObjectType{os.ModeDir}}
				objectFound = true
			}

			if now.IsZero() {
				now = time.Now().UTC()
			}

			for _, c := range page.Contents {
				key := awsv2.ToString(c.Key)
				if !u.Match(key) {
					continue
				}
				mod := awsv2.ToTime(c.LastModified).UTC()
				if mod.After(now) {
					objectFound = true
					continue
				}

				var objtype os.FileMode
				if strings.HasSuffix(key, "/") {
					objtype = os.ModeDir
				}

				newurl := u.Clone()
				newurl.Path = key
				objCh <- &Object{
					URL:          newurl,
					Etag:         strings.Trim(awsv2.ToString(c.ETag), `"`),
					ModTime:      &mod,
					Type:         ObjectType{objtype},
					Size:         awsv2.ToInt64(c.Size),
					StorageClass: StorageClass(c.StorageClass),
				}
				objectFound = true
			}
		}

		if !objectFound && !u.IsBucket() {
			objCh <- &Object{Err: ErrNoObjectFound}
		}
	}()
	return objCh
}

// listObjects is the v1 ListObjects fallback for backends that do not
// implement ListObjectsV2 (notably GCS).
func (s *S3) listObjects(ctx context.Context, u *url.URL) <-chan *Object {
	in := &s3v2.ListObjectsInput{
		Bucket:       awsv2.String(u.Bucket),
		Prefix:       awsv2.String(u.Prefix),
		RequestPayer: s.requestPayerEnum(),
	}
	if u.Delimiter != "" {
		in.Delimiter = awsv2.String(u.Delimiter)
	}

	objCh := make(chan *Object)
	go func() {
		defer close(objCh)
		objectFound := false
		var now time.Time

		// v2 has no paginator for ListObjects (v1 API); iterate via Marker.
		var marker *string
		for {
			in.Marker = marker
			page, err := s.api.ListObjects(ctx, in)
			if err != nil {
				objCh <- &Object{Err: cleanV2Err(err)}
				return
			}

			for _, c := range page.CommonPrefixes {
				prefix := awsv2.ToString(c.Prefix)
				if !u.Match(prefix) {
					continue
				}
				newurl := u.Clone()
				newurl.Path = prefix
				objCh <- &Object{URL: newurl, Type: ObjectType{os.ModeDir}}
				objectFound = true
			}

			if now.IsZero() {
				now = time.Now().UTC()
			}

			for _, c := range page.Contents {
				key := awsv2.ToString(c.Key)
				if !u.Match(key) {
					continue
				}
				mod := awsv2.ToTime(c.LastModified).UTC()
				if mod.After(now) {
					objectFound = true
					continue
				}

				var objtype os.FileMode
				if strings.HasSuffix(key, "/") {
					objtype = os.ModeDir
				}

				newurl := u.Clone()
				newurl.Path = key
				objCh <- &Object{
					URL:          newurl,
					Etag:         strings.Trim(awsv2.ToString(c.ETag), `"`),
					ModTime:      &mod,
					Type:         ObjectType{objtype},
					Size:         awsv2.ToInt64(c.Size),
					StorageClass: StorageClass(c.StorageClass),
				}
				objectFound = true
			}

			if !awsv2.ToBool(page.IsTruncated) {
				break
			}
			// S3 returns NextMarker only when delimiter is set; otherwise the
			// last key from Contents is the next marker.
			if page.NextMarker != nil {
				marker = page.NextMarker
			} else if len(page.Contents) > 0 {
				marker = page.Contents[len(page.Contents)-1].Key
			} else {
				break
			}
		}

		if !objectFound && !u.IsBucket() {
			objCh <- &Object{Err: ErrNoObjectFound}
		}
	}()
	return objCh
}

// listObjectVersions paginates ListObjectVersions, emitting objects, version
// markers, and delete markers. Matches v1 semantics including the version-id
// filter and the future-dated-object skip.
func (s *S3) listObjectVersions(ctx context.Context, u *url.URL) <-chan *Object {
	in := &s3v2.ListObjectVersionsInput{
		Bucket: awsv2.String(u.Bucket),
		Prefix: awsv2.String(u.Prefix),
	}
	if u.Delimiter != "" {
		in.Delimiter = awsv2.String(u.Delimiter)
	}

	objCh := make(chan *Object)
	go func() {
		defer close(objCh)
		objectFound := false
		var now time.Time

		paginator := s3v2.NewListObjectVersionsPaginator(s.api, in)
		for paginator.HasMorePages() {
			page, err := paginator.NextPage(ctx)
			if err != nil {
				objCh <- &Object{Err: cleanV2Err(err)}
				return
			}

			for _, c := range page.CommonPrefixes {
				prefix := awsv2.ToString(c.Prefix)
				if !u.Match(prefix) {
					continue
				}
				newurl := u.Clone()
				newurl.Path = prefix
				objCh <- &Object{URL: newurl, Type: ObjectType{os.ModeDir}}
				objectFound = true
			}

			if now.IsZero() {
				now = time.Now().UTC()
			}

			for _, v := range page.Versions {
				key := awsv2.ToString(v.Key)
				if !u.Match(key) {
					continue
				}
				if u.VersionID != "" && u.VersionID != awsv2.ToString(v.VersionId) {
					continue
				}

				mod := awsv2.ToTime(v.LastModified).UTC()
				if mod.After(now) {
					objectFound = true
					continue
				}

				var objtype os.FileMode
				if strings.HasSuffix(key, "/") {
					objtype = os.ModeDir
				}

				newurl := u.Clone()
				newurl.Path = key
				newurl.VersionID = awsv2.ToString(v.VersionId)
				objCh <- &Object{
					URL:          newurl,
					Etag:         strings.Trim(awsv2.ToString(v.ETag), `"`),
					ModTime:      &mod,
					Type:         ObjectType{objtype},
					Size:         awsv2.ToInt64(v.Size),
					StorageClass: StorageClass(v.StorageClass),
				}
				objectFound = true
			}

			for _, d := range page.DeleteMarkers {
				key := awsv2.ToString(d.Key)
				if !u.Match(key) {
					continue
				}
				if u.VersionID != "" && u.VersionID != awsv2.ToString(d.VersionId) {
					continue
				}

				mod := awsv2.ToTime(d.LastModified).UTC()
				if mod.After(now) {
					objectFound = true
					continue
				}

				var objtype os.FileMode
				if strings.HasSuffix(key, "/") {
					objtype = os.ModeDir
				}

				newurl := u.Clone()
				newurl.Path = key
				newurl.VersionID = awsv2.ToString(d.VersionId)
				objCh <- &Object{
					URL:     newurl,
					ModTime: &mod,
					Type:    ObjectType{objtype},
					Size:    0,
				}
				objectFound = true
			}
		}

		if !objectFound && !u.IsBucket() {
			objCh <- &Object{Err: ErrNoObjectFound}
		}
	}()
	return objCh
}

// errIsNoSuchUpload reports whether err is the NoSuchUpload condition that
// triggers s5cmd's idempotent multipart-upload retry workaround.
func errIsNoSuchUpload(err error) bool {
	var noSuchUpload *s3types.NoSuchUpload
	if errors.As(err, &noSuchUpload) {
		return true
	}
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) && apiErr.ErrorCode() == "NoSuchUpload" {
		return true
	}
	return false
}

// Copy is a server-side copy from one S3 object to another.
func (s *S3) Copy(ctx context.Context, from, to *url.URL, metadata Metadata) error {
	if s.dryRun {
		return nil
	}

	// CopySource format: "bucket/key" -- v2 still requires the version-id
	// query suffix appended to the source string rather than as a field.
	copySource := from.EscapedPath()
	if from.VersionID != "" {
		copySource = copySource + "?versionId=" + from.VersionID
	}

	input := &s3v2.CopyObjectInput{
		Bucket:       awsv2.String(to.Bucket),
		Key:          awsv2.String(to.Path),
		CopySource:   awsv2.String(copySource),
		RequestPayer: s.requestPayerEnum(),
	}

	if metadata.StorageClass != "" {
		input.StorageClass = s3types.StorageClass(metadata.StorageClass)
	}
	if metadata.ACL != "" {
		input.ACL = s3types.ObjectCannedACL(metadata.ACL)
	}
	if metadata.CacheControl != "" {
		input.CacheControl = awsv2.String(metadata.CacheControl)
	}
	if metadata.Expires != "" {
		t, err := time.Parse(time.RFC3339, metadata.Expires)
		if err != nil {
			return err
		}
		input.Expires = awsv2.Time(t)
	}
	if metadata.EncryptionMethod != "" {
		input.ServerSideEncryption = s3types.ServerSideEncryption(metadata.EncryptionMethod)
		if metadata.EncryptionKeyID != "" {
			input.SSEKMSKeyId = awsv2.String(metadata.EncryptionKeyID)
		}
	}
	if metadata.ContentEncoding != "" {
		input.ContentEncoding = awsv2.String(metadata.ContentEncoding)
	}
	if metadata.ContentDisposition != "" {
		input.ContentDisposition = awsv2.String(metadata.ContentDisposition)
	}
	if metadata.Directive != "" {
		input.MetadataDirective = s3types.MetadataDirective(metadata.Directive)
	}
	if metadata.ContentType != "" {
		input.ContentType = awsv2.String(metadata.ContentType)
	}
	if len(metadata.UserDefined) != 0 {
		input.Metadata = make(map[string]string, len(metadata.UserDefined))
		for k, v := range metadata.UserDefined {
			input.Metadata[k] = v
		}
	}
	if s.noSuchUploadRetryCount > 0 {
		if input.Metadata == nil {
			input.Metadata = make(map[string]string)
		}
		input.Metadata[metadataKeyRetryID] = generateRetryID()
	}

	_, err := s.api.CopyObject(ctx, input)
	return cleanV2Err(err)
}

// Read fetches the remote object and returns its body as an io.ReadCloser.
func (s *S3) Read(ctx context.Context, src *url.URL) (io.ReadCloser, error) {
	input := &s3v2.GetObjectInput{
		Bucket:       awsv2.String(src.Bucket),
		Key:          awsv2.String(src.Path),
		RequestPayer: s.requestPayerEnum(),
	}
	if src.VersionID != "" {
		input.VersionId = awsv2.String(src.VersionID)
	}

	resp, err := s.api.GetObject(ctx, input)
	if err != nil {
		return nil, cleanV2Err(err)
	}
	return resp.Body, nil
}

// Presign produces a time-limited URL for a GetObject request.
func (s *S3) Presign(ctx context.Context, from *url.URL, expire time.Duration) (string, error) {
	input := &s3v2.GetObjectInput{
		Bucket:       awsv2.String(from.Bucket),
		Key:          awsv2.String(from.Path),
		RequestPayer: s.requestPayerEnum(),
	}

	presigner := s3v2.NewPresignClient(s.api, func(o *s3v2.PresignOptions) {
		o.Expires = expire
	})
	req, err := presigner.PresignGetObject(ctx, input)
	if err != nil {
		return "", cleanV2Err(err)
	}
	return req.URL, nil
}

// Get is a parallel multipart download via the v2 transfer manager.
// Concurrency==1 collapses to a single GetObject (partSize ignored), matching
// v1 behavior.
func (s *S3) Get(
	ctx context.Context,
	from *url.URL,
	to io.WriterAt,
	concurrency int,
	partSize int64,
) (int64, error) {
	if s.dryRun {
		return 0, nil
	}

	input := &s3v2.GetObjectInput{
		Bucket:       awsv2.String(from.Bucket),
		Key:          awsv2.String(from.Path),
		RequestPayer: s.requestPayerEnum(),
	}
	if from.VersionID != "" {
		input.VersionId = awsv2.String(from.VersionID)
	}

	n, err := s.downloader.Download(ctx, to, input, func(d *manager.Downloader) {
		d.PartSize = partSize
		d.Concurrency = concurrency
	})
	return n, cleanV2Err(err)
}

// Put uploads the contents of reader to the destination via the v2 transfer
// manager. On NoSuchUpload (race with concurrent multipart abort) the
// configured number of idempotent retries is attempted, mirroring v1.
func (s *S3) Put(
	ctx context.Context,
	reader io.Reader,
	to *url.URL,
	metadata Metadata,
	concurrency int,
	partSize int64,
) error {
	if s.dryRun {
		return nil
	}

	contentType := metadata.ContentType
	if contentType == "" {
		contentType = "application/octet-stream"
	}

	input := &s3v2.PutObjectInput{
		Bucket:       awsv2.String(to.Bucket),
		Key:          awsv2.String(to.Path),
		Body:         reader,
		ContentType:  awsv2.String(contentType),
		Metadata:     make(map[string]string),
		RequestPayer: s.requestPayerEnum(),
	}

	if metadata.StorageClass != "" {
		input.StorageClass = s3types.StorageClass(metadata.StorageClass)
	}
	if metadata.ACL != "" {
		input.ACL = s3types.ObjectCannedACL(metadata.ACL)
	}
	if metadata.CacheControl != "" {
		input.CacheControl = awsv2.String(metadata.CacheControl)
	}
	if metadata.Expires != "" {
		t, err := time.Parse(time.RFC3339, metadata.Expires)
		if err != nil {
			return err
		}
		input.Expires = awsv2.Time(t)
	}
	if metadata.EncryptionMethod != "" {
		input.ServerSideEncryption = s3types.ServerSideEncryption(metadata.EncryptionMethod)
		if metadata.EncryptionKeyID != "" {
			input.SSEKMSKeyId = awsv2.String(metadata.EncryptionKeyID)
		}
	}
	if metadata.ContentEncoding != "" {
		input.ContentEncoding = awsv2.String(metadata.ContentEncoding)
	}
	if metadata.ContentDisposition != "" {
		input.ContentDisposition = awsv2.String(metadata.ContentDisposition)
	}
	if s.noSuchUploadRetryCount > 0 {
		input.Metadata[metadataKeyRetryID] = generateRetryID()
	}
	if len(metadata.UserDefined) != 0 {
		for k, v := range metadata.UserDefined {
			input.Metadata[k] = v
		}
	}

	uploaderOptsFn := func(u *manager.Uploader) {
		u.PartSize = partSize
		u.Concurrency = concurrency
	}

	_, err := s.uploader.Upload(ctx, input, uploaderOptsFn)
	if errIsNoSuchUpload(err) && s.noSuchUploadRetryCount > 0 {
		return cleanV2Err(s.retryOnNoSuchUpload(ctx, to, input, err, uploaderOptsFn))
	}
	return cleanV2Err(err)
}

// retryOnNoSuchUpload re-attempts the upload up to noSuchUploadRetryCount
// times, treating success as "either Upload returned nil OR a Stat shows the
// object already exists with the metadata-tagged retry ID we generated"
// (covers the race where a previous attempt did succeed despite the error).
func (s *S3) retryOnNoSuchUpload(
	ctx context.Context,
	to *url.URL,
	input *s3v2.PutObjectInput,
	err error,
	uploaderOpts ...func(*manager.Uploader),
) error {
	expectedRetryID := input.Metadata[metadataKeyRetryID]

	attempts := 0
	for ; errIsNoSuchUpload(err) && attempts < s.noSuchUploadRetryCount; attempts++ {
		obj, sErr := s.Stat(ctx, to)
		if sErr == nil && obj.retryID == expectedRetryID {
			err = nil
			break
		}

		log.Debug(log.DebugMessage{
			Err: fmt.Sprintf("Retrying to upload %v upon error: %q", to, err.Error()),
		})

		_, err = s.uploader.Upload(ctx, input, uploaderOpts...)
	}

	if errIsNoSuchUpload(err) && s.noSuchUploadRetryCount > 0 {
		err = fmt.Errorf("RetryOnNoSuchUpload: %d attempts to retry resulted in NoSuchUpload: %w",
			attempts, err)
	}
	return err
}

// parseInputSerialization builds the v2 InputSerialization for the given
// query shape. v2 enums replace the v1 string fields:
//
//	JSONInput.Type           -> types.JSONType
//	CSVInput.FileHeaderInfo  -> types.FileHeaderInfo
//	InputSerialization.CompressionType -> types.CompressionType
//
// CSVInput.FieldDelimiter remains *string (no enum).
func parseInputSerialization(e eventType, compression, delimiter, headerInfo string) (*s3types.InputSerialization, error) {
	switch e {
	case jsonType:
		in := &s3types.InputSerialization{
			JSON: &s3types.JSONInput{
				Type: s3types.JSONType(delimiter),
			},
		}
		if compression != "" {
			in.CompressionType = s3types.CompressionType(compression)
		}
		return in, nil
	case csvType:
		in := &s3types.InputSerialization{
			CSV: &s3types.CSVInput{
				FieldDelimiter: awsv2.String(delimiter),
				FileHeaderInfo: s3types.FileHeaderInfo(headerInfo),
			},
		}
		if compression != "" {
			in.CompressionType = s3types.CompressionType(compression)
		}
		return in, nil
	case parquetType:
		return &s3types.InputSerialization{Parquet: &s3types.ParquetInput{}}, nil
	default:
		return nil, fmt.Errorf("input format is not valid")
	}
}

func parseOutputSerialization(e eventType, delimiter string, reader io.Reader) (*s3types.OutputSerialization, EventStreamDecoder, error) {
	switch e {
	case jsonType:
		return &s3types.OutputSerialization{JSON: &s3types.JSONOutput{}}, NewJSONDecoder(reader), nil
	case csvType:
		return &s3types.OutputSerialization{
			CSV: &s3types.CSVOutput{FieldDelimiter: awsv2.String(delimiter)},
		}, NewCsvDecoder(reader), nil
	default:
		return nil, nil, fmt.Errorf("output serialization is not valid")
	}
}

// Select streams S3 Select results into resultCh. Tagged-union event handling
// in v2: types.SelectObjectContentEventStream is an interface; the records
// payload arrives as *types.SelectObjectContentEventStreamMemberRecords.
func (s *S3) Select(ctx context.Context, u *url.URL, query *SelectQuery, resultCh chan<- json.RawMessage) error {
	if s.dryRun {
		return nil
	}

	reader, writer := io.Pipe()

	inputFormat, err := parseInputSerialization(
		eventType(query.InputFormat),
		query.CompressionType,
		query.InputContentStructure,
		query.FileHeaderInfo,
	)
	if err != nil {
		return err
	}

	// JSON->CSV requires a comma delimiter on the input side; v1 patched the
	// query in-place, preserve that behavior.
	if query.InputFormat == string(jsonType) && query.OutputFormat == string(csvType) {
		query.InputContentStructure = ","
	}

	outputFormat, decoder, err := parseOutputSerialization(
		eventType(query.OutputFormat),
		query.InputContentStructure,
		reader,
	)
	if err != nil {
		return err
	}

	resp, err := s.api.SelectObjectContent(ctx, &s3v2.SelectObjectContentInput{
		Bucket:              awsv2.String(u.Bucket),
		Key:                 awsv2.String(u.Path),
		ExpressionType:      s3types.ExpressionType(query.ExpressionType),
		Expression:          awsv2.String(query.Expression),
		InputSerialization:  inputFormat,
		OutputSerialization: outputFormat,
	})
	if err != nil {
		return cleanV2Err(err)
	}

	stream := resp.GetStream()

	go func() {
		defer writer.Close()
		defer stream.Close()

		eventch := stream.Events()
		for {
			select {
			case <-ctx.Done():
				return
			case event, ok := <-eventch:
				if !ok {
					return
				}
				if rec, ok := event.(*s3types.SelectObjectContentEventStreamMemberRecords); ok {
					writer.Write(rec.Value.Payload)
				}
			}
		}
	}()

	for {
		val, err := decoder.Decode()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		resultCh <- val
	}

	return cleanV2Err(stream.Err())
}

// MultiDelete asynchronously removes objects from the input channel,
// fan-out at most 10 in-flight DeleteObjects requests.
func (s *S3) MultiDelete(ctx context.Context, urlch <-chan *url.URL) <-chan *Object {
	resultch := make(chan *Object)

	go func() {
		sem := make(chan struct{}, 10)
		defer close(sem)
		defer close(resultch)

		chunks := s.calculateChunks(urlch)
		var wg sync.WaitGroup
		for c := range chunks {
			c := c
			wg.Add(1)
			sem <- struct{}{}
			go func() {
				defer wg.Done()
				s.doDelete(ctx, c, resultch)
				<-sem
			}()
		}
		wg.Wait()
	}()

	return resultch
}
