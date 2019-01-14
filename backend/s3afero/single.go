package s3afero

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/johannesboyne/gofakes3"
	"github.com/spf13/afero"
)

// SingleBucketBackend is a gofakes3.Backend that allows you to treat an existing
// filesystem as an S3 bucket directly. It does not support multiple buckets.
//
// A second afero.Fs, metaFs, may be passed; if this is nil,
// afero.NewMemMapFs() is used and the metadata will not persist between
// restarts of gofakes3.
//
// It is STRONGLY recommended that the metadata Fs is not contained within the
// `/buckets` subdirectory as that could make a significant mess, but this is
// infeasible to validate, so you're encouraged to be extremely careful!
//
type SingleBucketBackend struct {
	lock      sync.Mutex
	fs        afero.Fs
	metaStore *metaStore
	name      string
}

var _ gofakes3.Backend = &SingleBucketBackend{}

func SingleBucket(name string, fs afero.Fs, metaFs afero.Fs, opts ...SingleOption) (*SingleBucketBackend, error) {
	if err := ensureNoOsFs("fs", fs); err != nil {
		return nil, err
	}

	if metaFs == nil {
		metaFs = afero.NewMemMapFs()
	} else {
		if err := ensureNoOsFs("metaFs", metaFs); err != nil {
			return nil, err
		}
	}

	if err := gofakes3.ValidateBucketName(name); err != nil {
		return nil, err
	}

	b := &SingleBucketBackend{
		name:      name,
		fs:        fs,
		metaStore: newMetaStore(metaFs),
	}
	for _, opt := range opts {
		if err := opt(b); err != nil {
			return nil, err
		}
	}

	return b, nil
}

func (db *SingleBucketBackend) ListBuckets() ([]gofakes3.BucketInfo, error) {
	db.lock.Lock()
	defer db.lock.Unlock()

	var created time.Time

	stat, err := db.fs.Stat("")
	if os.IsNotExist(err) {
		created = time.Now()
	} else if err != nil {
		return nil, err
	} else {
		created = stat.ModTime()
	}

	// FIXME: "birth time" is not available cross-platform.
	// See MultiBucketBackend.ListBuckets for more details.
	return []gofakes3.BucketInfo{
		{Name: db.name, CreationDate: gofakes3.NewContentTime(created)},
	}, nil
}

func (db *SingleBucketBackend) GetBucket(bucket string, prefix gofakes3.Prefix) (*gofakes3.Bucket, error) {
	if bucket != db.name {
		return nil, gofakes3.BucketNotFound(bucket)
	}

	db.lock.Lock()
	defer db.lock.Unlock()

	path, part, ok := prefix.FilePrefix()
	if ok {
		return db.getBucketWithFilePrefixLocked(bucket, path, part)
	} else {
		return db.getBucketWithArbitraryPrefixLocked(bucket, prefix)
	}
}

func (db *SingleBucketBackend) getBucketWithFilePrefixLocked(bucket string, prefixPath, prefixPart string) (*gofakes3.Bucket, error) {
	dirEntries, err := afero.ReadDir(db.fs, filepath.FromSlash(prefixPath))
	if err != nil {
		return nil, err
	}

	response := gofakes3.NewBucket(bucket)

	for _, entry := range dirEntries {
		object := entry.Name()

		// Expected use of 'path'; see the "Path Handling" subheading in doc.go:
		objectPath := path.Join(prefixPath, object)

		if prefixPart != "" && !strings.HasPrefix(object, prefixPart) {
			continue
		}

		size := entry.Size()
		mtime := entry.ModTime()

		meta, err := db.metaStore.loadMeta(bucket, objectPath, size, mtime)
		if err != nil {
			return nil, err
		}

		if entry.IsDir() {
			response.AddPrefix(path.Join(prefixPath, prefixPart))
		} else {
			response.Add(&gofakes3.Content{
				Key:          objectPath,
				LastModified: gofakes3.NewContentTime(mtime),
				ETag:         `"` + hex.EncodeToString(meta.Hash) + `"`,
				Size:         size,
			})
		}
	}

	return response, nil
}

func (db *SingleBucketBackend) getBucketWithArbitraryPrefixLocked(bucket string, prefix gofakes3.Prefix) (*gofakes3.Bucket, error) {
	response := gofakes3.NewBucket(bucket)

	if err := afero.Walk(db.fs, filepath.FromSlash(bucket), func(path string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() {
			return err
		}

		objectPath := filepath.ToSlash(path)
		parts := strings.SplitN(objectPath, "/", 2)
		if len(parts) != 2 {
			panic(fmt.Errorf("unexpected path %q", path)) // should never happen
		}
		objectName := parts[1]

		match := prefix.Match(objectName)
		if match == nil {
			return nil
		}

		size := info.Size()
		mtime := info.ModTime()
		meta, err := db.metaStore.loadMeta(bucket, objectName, size, mtime)
		if err != nil {
			return err
		}

		response.Add(&gofakes3.Content{
			Key:          objectName,
			LastModified: gofakes3.NewContentTime(mtime),
			ETag:         `"` + hex.EncodeToString(meta.Hash) + `"`,
			Size:         size,
		})

		return nil

	}); err != nil {
		return nil, err
	}

	return response, nil
}

func (db *SingleBucketBackend) HeadObject(bucketName, objectName string) (*gofakes3.Object, error) {
	if bucketName != db.name {
		return nil, gofakes3.BucketNotFound(bucketName)
	}

	db.lock.Lock()
	defer db.lock.Unlock()

	stat, err := db.fs.Stat(filepath.FromSlash(objectName))
	if os.IsNotExist(err) {
		return nil, gofakes3.KeyNotFound(objectName)
	} else if err != nil {
		return nil, err
	}

	size, mtime := stat.Size(), stat.ModTime()

	meta, err := db.metaStore.loadMeta(bucketName, objectName, size, mtime)
	if err != nil {
		return nil, err
	}

	return &gofakes3.Object{
		Hash:     meta.Hash,
		Metadata: meta.Meta,
		Size:     size,
		Contents: noOpReadCloser{},
	}, nil
}

func (db *SingleBucketBackend) GetObject(bucketName, objectName string) (*gofakes3.Object, error) {
	if bucketName != db.name {
		return nil, gofakes3.BucketNotFound(bucketName)
	}

	db.lock.Lock()
	defer db.lock.Unlock()

	f, err := db.fs.Open(filepath.FromSlash(objectName))
	if os.IsNotExist(err) {
		return nil, gofakes3.KeyNotFound(objectName)
	} else if err != nil {
		return nil, err
	}

	stat, err := f.Stat()
	if err != nil {
		return nil, err
	}

	size, mtime := stat.Size(), stat.ModTime()

	meta, err := db.metaStore.loadMeta(bucketName, objectName, size, mtime)
	if err != nil {
		return nil, err
	}

	return &gofakes3.Object{
		Hash:     meta.Hash,
		Metadata: meta.Meta,
		Size:     size,
		Contents: f,
	}, nil
}

func (db *SingleBucketBackend) PutObject(bucketName, objectName string, meta map[string]string, input io.Reader) error {
	if bucketName != db.name {
		return gofakes3.BucketNotFound(bucketName)
	}

	db.lock.Lock()
	defer db.lock.Unlock()

	objectFilePath := filepath.FromSlash(objectName)
	objectDir := filepath.Dir(objectFilePath)

	if objectDir != "." {
		if err := db.fs.MkdirAll(objectDir, 0777); err != nil {
			return err
		}
	}

	f, err := db.fs.Create(objectFilePath)
	if err != nil {
		return err
	}

	var closed bool
	defer func() {
		// Unfortunately, afero's MemMapFs updates the mtime if you double-close, which
		// highlights that other afero.Fs implementations may have side effects here::
		if !closed {
			f.Close()
		}
	}()

	hasher := md5.New()
	w := io.MultiWriter(f, hasher)
	if _, err := io.Copy(w, input); err != nil {
		return err
	}

	// We have to close here before we stat the file as some filesystems don't update the
	// mtime until after close:
	if err := f.Close(); err != nil {
		return err
	}

	closed = true

	stat, err := db.fs.Stat(objectFilePath)
	if err != nil {
		return err
	}

	storedMeta := &Metadata{
		File:    objectName,
		Hash:    hasher.Sum(nil),
		Meta:    meta,
		Size:    stat.Size(),
		ModTime: stat.ModTime(),
	}
	if err := db.metaStore.saveMeta(db.metaStore.metaPath(bucketName, objectName), storedMeta); err != nil {
		return err
	}

	return nil
}

func (db *SingleBucketBackend) DeleteObject(bucketName, objectName string) error {
	if bucketName != db.name {
		return gofakes3.BucketNotFound(bucketName)
	}

	db.lock.Lock()
	defer db.lock.Unlock()

	// S3 does not report an error when attemping to delete a key that does not exist, so
	// we need to skip IsNotExist errors.
	if err := db.fs.Remove(filepath.FromSlash(objectName)); err != nil && !os.IsNotExist(err) {
		return err
	}
	if err := db.metaStore.deleteMeta(db.metaStore.metaPath(bucketName, objectName)); err != nil {
		return err
	}

	return nil
}

// CreateBucket cannot be implemented by this backend. See MultiBucketBackend if you
// need a backend that supports it.
func (db *SingleBucketBackend) CreateBucket(name string) error {
	return gofakes3.ErrNotImplemented
}

// DeleteBucket cannot be implemented by this backend. See MultiBucketBackend if you
// need a backend that supports it.
func (db *SingleBucketBackend) DeleteBucket(name string) error {
	return gofakes3.ErrNotImplemented
}

func (db *SingleBucketBackend) BucketExists(name string) (exists bool, err error) {
	return db.name == name, nil
}
