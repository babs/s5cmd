package storage

// This file scaffolds the aws-sdk-go-v2 migration. Methods are ported from
// s3.go incrementally; once parity is reached, callers are switched and s3.go
// is removed. See migration plan for staging.

import (
	"context"
	"crypto/tls"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"sync"

	awsv2 "github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/config"
	credsv2 "github.com/aws/aws-sdk-go-v2/credentials"
	s3v2 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go/logging"

	"github.com/peak/s5cmd/v2/log"
)

// clientCacheV2 mirrors SessionCache for the v2 SDK; cached *s3.Client
// instances are reused per Options to avoid repeated config resolution.
var globalClientCacheV2 = &clientCacheV2{
	clients: map[Options]*s3v2.Client{},
}

type clientCacheV2 struct {
	sync.Mutex
	clients map[Options]*s3v2.Client
}

// newClient builds an *s3.Client for the given Options. Translation of v1
// behavior:
//   - NoSignRequest          -> aws.AnonymousCredentials
//   - Profile/CredentialFile -> shared config sources
//   - Endpoint               -> s3.Options.BaseEndpoint
//   - virtual-host detection -> s3.Options.UsePathStyle (inverted)
//   - transfer accel host    -> s3.Options.UseAccelerate (endpoint cleared)
//   - NoVerifySSL            -> custom http.Transport with InsecureSkipVerify
//   - LogLevel == Trace      -> aws.ClientLogMode + smithy logger
//   - MaxRetries             -> retry.Standard MaxAttempts; extended retryable
//     codes preserved from customRetryer.
//
// AWS_SDK_LOAD_CONFIG=0 disables shared config (legacy s5cmd behavior; v2
// loads shared config by default).
func (cc *clientCacheV2) newClient(ctx context.Context, opts Options) (*s3v2.Client, error) {
	cc.Lock()
	defer cc.Unlock()

	if c, ok := cc.clients[opts]; ok {
		return c, nil
	}

	endpointURL, err := parseEndpoint(opts.Endpoint)
	if err != nil {
		return nil, err
	}

	useAccelerate := supportsTransferAcceleration(endpointURL)
	if useAccelerate {
		// AWS handles accel via flag; explicit endpoint would break bucket ops.
		endpointURL = sentinelURL
	}
	usePathStyle := !isVirtualHostStyle(endpointURL)

	loadOpts := []func(*config.LoadOptions) error{}

	if v := os.Getenv("AWS_SDK_LOAD_CONFIG"); v != "" {
		if enable, _ := strconv.ParseBool(v); !enable {
			loadOpts = append(loadOpts,
				config.WithSharedConfigFiles(nil),
				config.WithSharedCredentialsFiles(nil),
			)
		}
	}

	if opts.Profile != "" {
		loadOpts = append(loadOpts, config.WithSharedConfigProfile(opts.Profile))
	}
	if opts.CredentialFile != "" {
		loadOpts = append(loadOpts,
			config.WithSharedCredentialsFiles([]string{opts.CredentialFile}),
		)
	}

	if opts.NoSignRequest {
		loadOpts = append(loadOpts,
			config.WithCredentialsProvider(awsv2.AnonymousCredentials{}),
		)
	}

	if opts.NoVerifySSL {
		httpClient := awshttp.NewBuildableClient().WithTransportOptions(func(t *http.Transport) {
			t.TLSClientConfig = &tls.Config{InsecureSkipVerify: true}
			t.Proxy = http.ProxyFromEnvironment
		})
		loadOpts = append(loadOpts, config.WithHTTPClient(httpClient))
	}

	if opts.LogLevel == log.LevelTrace {
		loadOpts = append(loadOpts,
			config.WithClientLogMode(awsv2.LogRequest|awsv2.LogResponse|awsv2.LogRetries),
			config.WithLogger(sdkV2Logger{}),
		)
	}

	loadOpts = append(loadOpts,
		config.WithRetryer(func() awsv2.Retryer {
			return retry.AddWithErrorCodes(
				retry.NewStandard(func(o *retry.StandardOptions) {
					if opts.MaxRetries > 0 {
						o.MaxAttempts = opts.MaxRetries + 1
					}
				}),
				// Preserved from v1 customRetryer.ShouldRetry.
				"InternalError",
				"RequestTimeTooSkewed",
				"SlowDown",
			)
		}),
	)

	cfg, err := config.LoadDefaultConfig(ctx, loadOpts...)
	if err != nil {
		return nil, fmt.Errorf("load aws config: %w", err)
	}

	if opts.region != "" {
		cfg.Region = opts.region
	} else if cfg.Region == "" {
		// Bucket region auto-detection happens at first call; default keeps
		// region-independent operations (ListBuckets, MakeBucket) working.
		cfg.Region = "us-east-1"
	}

	s3OptFns := []func(*s3v2.Options){
		func(o *s3v2.Options) {
			o.UsePathStyle = usePathStyle
			o.UseAccelerate = useAccelerate
		},
	}
	if endpointURL != sentinelURL {
		ep := endpointURL.String()
		s3OptFns = append(s3OptFns, func(o *s3v2.Options) {
			o.BaseEndpoint = awsv2.String(ep)
		})
	}

	client := s3v2.NewFromConfig(cfg, s3OptFns...)
	cc.clients[opts] = client
	return client, nil
}

func (cc *clientCacheV2) clear() {
	cc.Lock()
	defer cc.Unlock()
	cc.clients = map[Options]*s3v2.Client{}
}

// sdkV2Logger adapts smithy-go's logging.Logger to s5cmd's trace channel.
type sdkV2Logger struct{}

func (sdkV2Logger) Logf(_ logging.Classification, format string, v ...interface{}) {
	log.Trace(log.TraceMessage{Message: fmt.Sprintf(format, v...)})
}

// Compile-time assertion that the v2 credentials package is wired in; this
// keeps `go mod tidy` from stripping the dependency before any code uses it.
var _ = credsv2.NewStaticCredentialsProvider
