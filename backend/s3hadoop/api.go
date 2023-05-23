package s3hadoop

import (
	"io"

	"github.com/gkalele/s3tohdfs"
)

type S3Hadoop struct{}

func (S3Hadoop) ListBuckets() ([]s3tohdfs.BucketInfo, error) {
	// TODO implement me
	panic("implement me")
}

func (S3Hadoop) ListBucket(name string, prefix *s3tohdfs.Prefix, page s3tohdfs.ListBucketPage) (*s3tohdfs.ObjectList, error) {
	// TODO implement me
	panic("implement me")
}

func (S3Hadoop) CreateBucket(name string) error {
	// TODO implement me
	panic("implement me")
}

func (S3Hadoop) BucketExists(name string) (exists bool, err error) {
	// TODO implement me
	panic("implement me")
}

func (S3Hadoop) DeleteBucket(name string) error {
	// TODO implement me
	panic("implement me")
}

func (S3Hadoop) GetObject(bucketName, objectName string, rangeRequest *s3tohdfs.ObjectRangeRequest) (*s3tohdfs.Object, error) {
	// TODO implement me
	panic("implement me")
}

func (S3Hadoop) HeadObject(bucketName, objectName string) (*s3tohdfs.Object, error) {
	// TODO implement me
	panic("implement me")
}

func (S3Hadoop) DeleteObject(bucketName, objectName string) (s3tohdfs.ObjectDeleteResult, error) {
	// TODO implement me
	panic("implement me")
}

func (S3Hadoop) PutObject(bucketName, key string, meta map[string]string, input io.Reader, size int64) (s3tohdfs.PutObjectResult, error) {
	// TODO implement me
	panic("implement me")
}

func (S3Hadoop) DeleteMulti(bucketName string, objects ...string) (s3tohdfs.MultiDeleteResult, error) {
	// TODO implement me
	panic("implement me")
}
