package s3bolt

import (
	"bytes"
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"io"
	"log"

	"github.com/gkalele/s3tohdfs"
	"github.com/gkalele/s3tohdfs/internal/s3io"
	bolt "go.etcd.io/bbolt"
	"gopkg.in/mgo.v2/bson"
)

var (
	emptyPrefix = &s3tohdfs.Prefix{}
)

type Backend struct {
	bolt           *bolt.DB
	timeSource     s3tohdfs.TimeSource
	metaBucketName []byte
}

var _ s3tohdfs.Backend = &Backend{}

type Option func(b *Backend)

func WithTimeSource(timeSource s3tohdfs.TimeSource) Option {
	return func(b *Backend) { b.timeSource = timeSource }
}

func NewFile(file string, opts ...Option) (*Backend, error) {
	if file == "" {
		return nil, fmt.Errorf("s3tohdfs: invalid bolt file name")
	}
	db, err := bolt.Open(file, 0600, nil)
	if err != nil {
		return nil, err
	}
	return New(db, opts...), nil
}

func New(bolt *bolt.DB, opts ...Option) *Backend {
	b := &Backend{
		bolt:           bolt,
		metaBucketName: []byte("_meta"), // Underscore guarantees no overlap with legal S3 bucket names
	}
	for _, opt := range opts {
		opt(b)
	}
	if b.timeSource == nil {
		b.timeSource = s3tohdfs.DefaultTimeSource()
	}
	return b
}

// metaBucket returns a utility that manages access to the metadata bucket.
// The returned struct is valid only for the lifetime of the bolt.Tx.
// The metadata bucket may not exist if this is an older database.
func (db *Backend) metaBucket(tx *bolt.Tx) (*metaBucket, error) {
	var bucket *bolt.Bucket
	var err error

	if tx.Writable() {
		bucket, err = tx.CreateBucketIfNotExists(db.metaBucketName)
		if err != nil {
			return nil, err
		}
	} else {
		bucket = tx.Bucket(db.metaBucketName)
		if bucket == nil {
			// FIXME: support legacy databases; remove when versioning is supported.
			return nil, nil
		}
	}

	return &metaBucket{
		Tx:       tx,
		bucket:   bucket,
		metaName: db.metaBucketName,
	}, nil
}

func (db *Backend) ListBuckets() ([]s3tohdfs.BucketInfo, error) {
	var buckets []s3tohdfs.BucketInfo

	err := db.bolt.View(func(tx *bolt.Tx) error {
		metaBucket, err := db.metaBucket(tx)
		if err != nil {
			return err
		}

		return tx.ForEach(func(name []byte, _ *bolt.Bucket) error {
			if bytes.Equal(name, db.metaBucketName) {
				return nil
			}

			nameStr := string(name)
			info := s3tohdfs.BucketInfo{Name: nameStr}

			// Attempt to assign metadata. If it isn't found, we will just
			// pretend that's fine for now. This is to support existing
			// databases that have buckets created without associated metadata.
			//
			// FIXME: clean this up when there is an upgrade script to expect
			// that it exists
			if metaBucket != nil {
				bucketInfo, err := metaBucket.s3Bucket(nameStr)
				if err != nil {
					return err
				}
				if bucketInfo != nil {
					info.CreationDate = s3tohdfs.NewContentTime(bucketInfo.CreationDate)
				}
			}

			// The AWS CLI will fail if there is no creation date:
			if info.CreationDate.IsZero() {
				info.CreationDate = s3tohdfs.NewContentTime(db.timeSource.Now())
			}

			buckets = append(buckets, info)
			return nil
		})
	})

	return buckets, err
}

func (db *Backend) ListBucket(name string, prefix *s3tohdfs.Prefix, page s3tohdfs.ListBucketPage) (*s3tohdfs.ObjectList, error) {
	if prefix == nil {
		prefix = emptyPrefix
	}
	if !page.IsEmpty() {
		return nil, s3tohdfs.ErrInternalPageNotImplemented
	}

	objects := s3tohdfs.NewObjectList()

	err := db.bolt.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(name))
		if b == nil {
			return s3tohdfs.BucketNotFound(name)
		}

		c := b.Cursor()
		var match s3tohdfs.PrefixMatch

		for k, v := c.First(); k != nil; k, v = c.Next() {
			key := string(k)
			if !prefix.Match(key, &match) {
				continue

			} else if match.CommonPrefix {
				objects.AddPrefix(match.MatchedPart)

			} else {
				var b boltObject
				err := bson.Unmarshal(v, &b)
				if err != nil {
					return fmt.Errorf("s3tohdfs: could not unmarshal object %q: %v", string(k[:]), err)
				}
				item := &s3tohdfs.Content{
					Key:          string(k[:]),
					ETag:         `"` + hex.EncodeToString(b.Hash[:]) + `"`,
					Size:         b.Size,
					LastModified: s3tohdfs.NewContentTime(b.LastModified.UTC()),
				}
				objects.Add(item)
			}
		}

		return nil
	})

	return objects, err
}

func (db *Backend) CreateBucket(name string) error {
	return db.bolt.Update(func(tx *bolt.Tx) error {
		{ // create bucket metadata
			metaBucket, err := db.metaBucket(tx)
			if err != nil {
				return err
			}
			if err := metaBucket.createS3Bucket(name, db.timeSource.Now()); err != nil {
				return err
			}
		}

		{ // create bucket
			nameBts := []byte(name)
			if tx.Bucket(nameBts) != nil {
				return s3tohdfs.ResourceError(s3tohdfs.ErrBucketAlreadyExists, name)
			}
			if _, err := tx.CreateBucket(nameBts); err != nil {
				return err
			}
		}
		return nil
	})
}

func (db *Backend) DeleteBucket(name string) error {
	nameBts := []byte(name)

	if bytes.Equal(nameBts, db.metaBucketName) {
		return s3tohdfs.ResourceError(s3tohdfs.ErrInvalidBucketName, name)
	}

	return db.bolt.Update(func(tx *bolt.Tx) error {
		{ // delete bucket
			b := tx.Bucket(nameBts)
			if b == nil {
				return s3tohdfs.ErrNoSuchBucket
			}
			c := b.Cursor()
			k, _ := c.First()
			if k != nil {
				return s3tohdfs.ResourceError(s3tohdfs.ErrBucketNotEmpty, name)
			}
		}

		{ // delete bucket metadata
			metaBucket, err := db.metaBucket(tx)
			if err != nil {
				return err
			}

			// FIXME: assumes a legacy database, where the bucket may not exist. Clean
			// this up when there is a DB upgrade script.
			if metaBucket != nil {
				if err := metaBucket.deleteS3Bucket(name); err != nil {
					return err
				}
			}
		}

		return tx.DeleteBucket(nameBts)
	})
}

func (db *Backend) BucketExists(name string) (exists bool, err error) {
	err = db.bolt.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(name))
		exists = b != nil
		return nil
	})
	return exists, err
}

func (db *Backend) HeadObject(bucketName, objectName string) (*s3tohdfs.Object, error) {
	obj, err := db.GetObject(bucketName, objectName, nil)
	if err != nil {
		return nil, err
	}
	obj.Contents = s3io.NoOpReadCloser{}
	return obj, nil
}

func (db *Backend) GetObject(bucketName, objectName string, rangeRequest *s3tohdfs.ObjectRangeRequest) (*s3tohdfs.Object, error) {
	var t boltObject

	err := db.bolt.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucketName))
		if b == nil {
			return s3tohdfs.BucketNotFound(bucketName)
		}

		v := b.Get([]byte(objectName))
		if v == nil {
			return s3tohdfs.KeyNotFound(objectName)
		}

		if err := bson.Unmarshal(v, &t); err != nil {
			return fmt.Errorf("s3tohdfs: could not unmarshal object at %q/%q: %v", bucketName, objectName, err)
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	// FIXME: objectName here is a bit of a hack; this can be cleaned up when we have a
	// database migration script.
	return t.Object(objectName, rangeRequest)
}

func (db *Backend) PutObject(
	bucketName, objectName string,
	meta map[string]string,
	input io.Reader, size int64,
) (result s3tohdfs.PutObjectResult, err error) {

	bts, err := s3tohdfs.ReadAll(input, size)
	if err != nil {
		return result, err
	}

	err = s3tohdfs.MergeMetadata(db, bucketName, objectName, meta)
	if err != nil {
		return result, err
	}

	mod := db.timeSource.Now()
	hash := md5.Sum(bts)

	return result, db.bolt.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucketName))
		if b == nil {
			return s3tohdfs.BucketNotFound(bucketName)
		}

		data, err := bson.Marshal(&boltObject{
			Name:         objectName,
			Metadata:     meta,
			Size:         int64(len(bts)),
			LastModified: mod,
			Contents:     bts,
			Hash:         hash[:],
		})
		if err != nil {
			return err
		}
		if err := b.Put([]byte(objectName), data); err != nil {
			return err
		}
		return nil
	})
}

func (db *Backend) DeleteObject(bucketName, objectName string) (result s3tohdfs.ObjectDeleteResult, rerr error) {
	return result, db.bolt.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucketName))
		if b == nil {
			return s3tohdfs.BucketNotFound(bucketName)
		}
		if err := b.Delete([]byte(objectName)); err != nil {
			return fmt.Errorf("s3tohdfs: delete failed for object %q in bucket %q", objectName, bucketName)
		}
		return nil
	})
}

func (db *Backend) DeleteMulti(bucketName string, objects ...string) (result s3tohdfs.MultiDeleteResult, err error) {
	err = db.bolt.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bucketName))
		if b == nil {
			return s3tohdfs.BucketNotFound(bucketName)
		}

		for _, object := range objects {
			if err := b.Delete([]byte(object)); err != nil {
				log.Println("delete object failed:", err)
				result.Error = append(result.Error, s3tohdfs.ErrorResult{
					Code:    s3tohdfs.ErrInternal,
					Message: s3tohdfs.ErrInternal.Message(),
					Key:     object,
				})

			} else {
				result.Deleted = append(result.Deleted, s3tohdfs.ObjectID{
					Key: object,
				})
			}
		}

		return nil
	})

	return result, err
}
