package blob

import (
	"context"
	"errors"
	"io"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// S3 is the S3-compatible driver (P-07): the same blob.Store seam as FS, but
// bytes live in a bucket reached through aws-sdk-go-v2. It works against AWS
// S3 and any S3-compatible endpoint (MinIO, Cloudflare R2, GCS interop) via a
// custom endpoint with path-style addressing. Keys are content-addressed by
// the domain layer, so PutObject is idempotent (rewriting a key with the same
// bytes), Open is GetObject, and Delete tolerates a missing key.
//
// Credentials come from the AWS SDK default chain (environment, IAM role,
// shared config) — NEVER from our own config keys, so secrets stay out of the
// server's configuration surface.
type S3 struct {
	client *s3.Client
	bucket string
	prefix string
}

// S3Config parameterizes the driver. Bucket and Region are required; Endpoint
// (MinIO/R2) and Prefix (a key namespace within the bucket) are optional.
type S3Config struct {
	Bucket   string
	Region   string
	Endpoint string
	Prefix   string
}

// NewS3 builds the driver. The SDK config load resolves credentials from the
// default chain; a custom endpoint switches on path-style addressing, which is
// what MinIO and R2 expect.
func NewS3(ctx context.Context, cfg S3Config) (*S3, error) {
	if cfg.Bucket == "" || cfg.Region == "" {
		return nil, errors.New("blob: s3 driver needs a bucket and region")
	}
	awsCfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(cfg.Region))
	if err != nil {
		return nil, err
	}
	client := s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		if cfg.Endpoint != "" {
			o.BaseEndpoint = aws.String(cfg.Endpoint)
			o.UsePathStyle = true
		}
	})
	return &S3{client: client, bucket: cfg.Bucket, prefix: cfg.Prefix}, nil
}

// key applies the optional bucket-relative prefix.
func (s *S3) key(k string) string {
	if s.prefix == "" {
		return k
	}
	return strings.TrimSuffix(s.prefix, "/") + "/" + k
}

func (s *S3) Put(ctx context.Context, key string, r io.Reader) error {
	_, err := s.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(s.key(key)),
		Body:   r,
	})
	return err
}

func (s *S3) Open(ctx context.Context, key string) (io.ReadCloser, error) {
	out, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(s.key(key)),
	})
	if err != nil {
		return nil, err
	}
	return out.Body, nil
}

func (s *S3) Delete(ctx context.Context, key string) error {
	_, err := s.client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(s.key(key)),
	})
	// S3 DeleteObject is already idempotent, but a stricter S3-compatible
	// backend may surface NoSuchKey — treat it as a successful no-op.
	var nsk *types.NoSuchKey
	if errors.As(err, &nsk) {
		return nil
	}
	return err
}
