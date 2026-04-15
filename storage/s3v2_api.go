package storage

// S3v2 mirrors S3 but talks to S3 via aws-sdk-go-v2. Methods are ported from
// s3.go incrementally; once parity is reached, NewRemoteClient is switched
// to return *S3v2 and the v1 implementation is removed.

import (
	"context"
	"errors"
	"fmt"
	urlpkg "net/url"
	"os"
	"strings"
	"sync"
	"time"

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

// chunkV2 is the v2 equivalent of chunk{} -- holds a batch of object IDs to
// delete. ObjectIdentifier in v2 is a value type rather than a pointer.
type chunkV2 struct {
	Bucket string
	Keys   []s3types.ObjectIdentifier
}

// calculateChunks pulls URLs off ch and emits delete batches sized by S3's
// 1000-key limit (1 for GCS, which does not support multi-delete).
func (s *S3v2) calculateChunks(ch <-chan *url.URL) <-chan chunkV2 {
	chunkch := make(chan chunkV2)
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
				chunkch <- chunkV2{Bucket: bucket, Keys: keys}
				keys = nil
			}
		}
		if len(keys) > 0 {
			chunkch <- chunkV2{Bucket: bucket, Keys: keys}
		}
	}()

	return chunkch
}

// Delete removes a single object.
func (s *S3v2) Delete(ctx context.Context, u *url.URL) error {
	c := chunkV2{
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
func (s *S3v2) doDelete(ctx context.Context, c chunkV2, resultch chan *Object) {
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
				resultch <- &Object{Err: err}
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
		resultch <- &Object{Err: err}
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
func (s *S3v2) List(ctx context.Context, u *url.URL, _ bool) <-chan *Object {
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
func (s *S3v2) listObjectsV2(ctx context.Context, u *url.URL) <-chan *Object {
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
				objCh <- &Object{Err: err}
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
func (s *S3v2) listObjects(ctx context.Context, u *url.URL) <-chan *Object {
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
				objCh <- &Object{Err: err}
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
func (s *S3v2) listObjectVersions(ctx context.Context, u *url.URL) <-chan *Object {
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
				objCh <- &Object{Err: err}
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

// MultiDelete asynchronously removes objects from the input channel,
// fan-out at most 10 in-flight DeleteObjects requests.
func (s *S3v2) MultiDelete(ctx context.Context, urlch <-chan *url.URL) <-chan *Object {
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
