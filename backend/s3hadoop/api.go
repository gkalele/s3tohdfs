package s3hadoop

import (
	"fmt"
	"github.com/gkalele/s3tohdfs/backend/s3hadoop/metadata"
	"github.com/gkalele/s3tohdfs/backend/s3hadoop/metadata/backends/mem"
	"io"
	"log"
	"time"

	"github.com/gkalele/s3tohdfs"
	"github.com/vladimirvivien/gowfs"
)

type HdfsConfig struct {
	Namenode    string
	HdfsPort    uint16
	WebHdfsPort uint16
	WebHdfsUser string
}

type S3Hadoop struct {
	fs    *gowfs.FileSystem
	cache metadata.MetadataStore
}

var _ s3tohdfs.Backend = &S3Hadoop{}

const dbrStorageBucket = "dbrstoragev1"

func Factory(config HdfsConfig) *S3Hadoop {
	fs, err := gowfs.NewFileSystem(gowfs.Configuration{
		Addr: fmt.Sprintf("%s:%d", config.Namenode, config.WebHdfsPort),
		User: config.WebHdfsUser,
	})
	if err != nil {
		log.Fatal(err)
	}
	return &S3Hadoop{
		fs:    fs,
		cache: mem.Factory(), // TODO - allow user to pass in choice of metadata store
	}
}

func (s *S3Hadoop) ListBuckets() ([]s3tohdfs.BucketInfo, error) {
	return []s3tohdfs.BucketInfo{
		{
			Name: dbrStorageBucket,
			CreationDate: s3tohdfs.ContentTime{
				Time: time.Unix(1685059001, 0),
			},
		},
	}, nil
}

func (s *S3Hadoop) ListBucket(name string, prefix *s3tohdfs.Prefix, page s3tohdfs.ListBucketPage) (*s3tohdfs.ObjectList, error) {
	// TODO implement me
	panic("implement me")
}

func (s *S3Hadoop) CreateBucket(name string) error {
	return fmt.Errorf("Bucket creation is forbidden")
}

func (s *S3Hadoop) BucketExists(name string) (exists bool, err error) {
	if name == dbrStorageBucket {
		return true, nil
	}
	return false, nil
}

func (s *S3Hadoop) DeleteBucket(name string) error {
	return fmt.Errorf("Bucket deletion is forbidden")
}

func (s *S3Hadoop) GetObject(bucketName, objectName string, rangeRequest *s3tohdfs.ObjectRangeRequest) (*s3tohdfs.Object, error) {
	if bucketName != dbrStorageBucket {
		return nil, fmt.Errorf("invalid bucket name " + bucketName)
	}
	found, size, err := s.cache.Get(bucketName, objectName)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, fmt.Errorf("object not found")
	}
	return &s3tohdfs.Object{
		Name:           objectName,
		Metadata:       nil,
		Size:           int64(size),
		Contents:       nil, // TODO
		Hash:           nil,
		Range:          nil,
		VersionID:      "",
		IsDeleteMarker: false,
	}, nil
}

func (s *S3Hadoop) HeadObject(bucketName, objectName string) (*s3tohdfs.Object, error) {
	// TODO implement me
	panic("implement me")
}

func convertDeleteResult() s3tohdfs.ObjectDeleteResult {
	return s3tohdfs.ObjectDeleteResult{
		IsDeleteMarker: false,
	}
}

func (s *S3Hadoop) DeleteObject(bucketName, objectName string) (s3tohdfs.ObjectDeleteResult, error) {
	if bucketName != dbrStorageBucket {
		return s3tohdfs.ObjectDeleteResult{}, fmt.Errorf("invalid bucket name " + bucketName)
	}
	found, _, err := s.cache.Get(bucketName, objectName)
	if err != nil {
		return s3tohdfs.ObjectDeleteResult{}, err
	}
	if !found {
		return s3tohdfs.ObjectDeleteResult{}, fmt.Errorf("object %s not found", objectName)
	}
	err = s.cache.Delete(bucketName, objectName)
	if err != nil {
		return s3tohdfs.ObjectDeleteResult{}, err
	}
	return convertDeleteResult(), nil
}

func (s *S3Hadoop) PutObject(bucketName, key string, meta map[string]string, input io.Reader, size int64) (s3tohdfs.PutObjectResult, error) {
	if bucketName != dbrStorageBucket {
		return s3tohdfs.PutObjectResult{}, fmt.Errorf("invalid bucket name " + bucketName)
	}
	panic("IMPLEMENT ME")
}

func (s *S3Hadoop) DeleteMulti(bucketName string, objects ...string) (s3tohdfs.MultiDeleteResult, error) {
	result := s3tohdfs.MultiDeleteResult{
		Deleted: make([]s3tohdfs.ObjectID, 0, len(objects)),
		Error:   make([]s3tohdfs.ErrorResult, 0, len(objects)),
	}
	if bucketName != dbrStorageBucket {
		return result, fmt.Errorf("invalid bucket name " + bucketName)
	}
	for _, name := range objects {
		_, err := s.DeleteObject(bucketName, name)
		if err != nil {
			result.Error = append(result.Error, s3tohdfs.ErrorResultFromError(err))
			continue
		}
		result.Deleted = append(result.Deleted, s3tohdfs.ObjectID{Key: name, VersionID: ""})
	}
	return result, nil
}
