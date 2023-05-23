package s3mem

import (
	"crypto/md5"
	"encoding/hex"
	"io"
	"sync"

	"github.com/gkalele/s3tohdfs"
	"github.com/gkalele/s3tohdfs/internal/goskipiter"
)

var (
	emptyPrefix       = &s3tohdfs.Prefix{}
	emptyVersionsPage = &s3tohdfs.ListBucketVersionsPage{}
)

type Backend struct {
	buckets          map[string]*bucket
	timeSource       s3tohdfs.TimeSource
	versionGenerator *versionGenerator
	versionSeed      int64
	versionSeedSet   bool
	versionScratch   []byte
	lock             sync.RWMutex
}

var _ s3tohdfs.Backend = &Backend{}
var _ s3tohdfs.VersionedBackend = &Backend{}

type Option func(b *Backend)

func WithTimeSource(timeSource s3tohdfs.TimeSource) Option {
	return func(b *Backend) { b.timeSource = timeSource }
}

func WithVersionSeed(seed int64) Option {
	return func(b *Backend) { b.versionSeed = seed; b.versionSeedSet = true }
}

func New(opts ...Option) *Backend {
	b := &Backend{
		buckets: make(map[string]*bucket),
	}
	for _, opt := range opts {
		opt(b)
	}
	if b.timeSource == nil {
		b.timeSource = s3tohdfs.DefaultTimeSource()
	}
	if b.versionGenerator == nil {
		if b.versionSeedSet {
			b.versionGenerator = newVersionGenerator(uint64(b.versionSeed), 0)
		} else {
			b.versionGenerator = newVersionGenerator(uint64(b.timeSource.Now().UnixNano()), 0)
		}
	}
	return b
}

func (db *Backend) ListBuckets() ([]s3tohdfs.BucketInfo, error) {
	db.lock.RLock()
	defer db.lock.RUnlock()

	var buckets = make([]s3tohdfs.BucketInfo, 0, len(db.buckets))
	for _, bucket := range db.buckets {
		buckets = append(buckets, s3tohdfs.BucketInfo{
			Name:         bucket.name,
			CreationDate: bucket.creationDate,
		})
	}

	return buckets, nil
}

func (db *Backend) ListBucket(name string, prefix *s3tohdfs.Prefix, page s3tohdfs.ListBucketPage) (*s3tohdfs.ObjectList, error) {
	if prefix == nil {
		prefix = emptyPrefix
	}

	db.lock.RLock()
	defer db.lock.RUnlock()

	storedBucket := db.buckets[name]
	if storedBucket == nil {
		return nil, s3tohdfs.BucketNotFound(name)
	}

	var response = s3tohdfs.NewObjectList()
	var iter = goskipiter.New(storedBucket.objects.Iterator())
	var match s3tohdfs.PrefixMatch

	if page.Marker != "" {
		iter.Seek(page.Marker)
		iter.Next() // Move to the next item after the Marker
	}

	var cnt int64 = 0

	var lastMatchedPart string

	for iter.Next() {
		item := iter.Value().(*bucketObject)

		if !prefix.Match(item.data.name, &match) {
			continue
		} else if item.data.deleteMarker {
			continue
		} else if match.CommonPrefix {
			if match.MatchedPart == lastMatchedPart {
				continue // Should not count towards keys
			}
			response.AddPrefix(match.MatchedPart)
			lastMatchedPart = match.MatchedPart

		} else {
			response.Add(&s3tohdfs.Content{
				Key:          item.data.name,
				LastModified: s3tohdfs.NewContentTime(item.data.lastModified),
				ETag:         `"` + hex.EncodeToString(item.data.hash) + `"`,
				Size:         int64(len(item.data.body)),
			})
		}

		cnt++
		if page.MaxKeys > 0 && cnt >= page.MaxKeys {
			response.NextMarker = item.data.name
			response.IsTruncated = iter.Next()
			break
		}
	}

	return response, nil
}

func (db *Backend) CreateBucket(name string) error {
	db.lock.Lock()
	defer db.lock.Unlock()

	if db.buckets[name] != nil {
		return s3tohdfs.ResourceError(s3tohdfs.ErrBucketAlreadyExists, name)
	}

	db.buckets[name] = newBucket(name, db.timeSource.Now(), db.nextVersion)
	return nil
}

func (db *Backend) DeleteBucket(name string) error {
	db.lock.Lock()
	defer db.lock.Unlock()

	if db.buckets[name] == nil {
		return s3tohdfs.ErrNoSuchBucket
	}

	if db.buckets[name].objects.Len() > 0 {
		return s3tohdfs.ResourceError(s3tohdfs.ErrBucketNotEmpty, name)
	}

	delete(db.buckets, name)

	return nil
}

func (db *Backend) BucketExists(name string) (exists bool, err error) {
	db.lock.RLock()
	defer db.lock.RUnlock()
	return db.buckets[name] != nil, nil
}

func (db *Backend) HeadObject(bucketName, objectName string) (*s3tohdfs.Object, error) {
	db.lock.RLock()
	defer db.lock.RUnlock()

	bucket := db.buckets[bucketName]
	if bucket == nil {
		return nil, s3tohdfs.BucketNotFound(bucketName)
	}

	obj := bucket.object(objectName)
	if obj == nil || obj.data.deleteMarker {
		return nil, s3tohdfs.KeyNotFound(objectName)
	}

	return obj.data.toObject(nil, false)
}

func (db *Backend) GetObject(bucketName, objectName string, rangeRequest *s3tohdfs.ObjectRangeRequest) (*s3tohdfs.Object, error) {
	db.lock.RLock()
	defer db.lock.RUnlock()

	bucket := db.buckets[bucketName]
	if bucket == nil {
		return nil, s3tohdfs.BucketNotFound(bucketName)
	}

	obj := bucket.object(objectName)
	if obj == nil || obj.data.deleteMarker {
		// FIXME: If the current version of the object is a delete marker,
		// Amazon S3 behaves as if the object was deleted and includes
		// x-amz-delete-marker: true in the response.
		//
		// The solution may be to return an object but no error if the object is
		// a delete marker, and let the main GoFakeS3 class decide what to do.
		return nil, s3tohdfs.KeyNotFound(objectName)
	}

	result, err := obj.data.toObject(rangeRequest, true)
	if err != nil {
		return nil, err
	}

	if bucket.versioning != s3tohdfs.VersioningEnabled {
		result.VersionID = ""
	}

	return result, nil
}

func (db *Backend) PutObject(bucketName, objectName string, meta map[string]string, input io.Reader, size int64) (result s3tohdfs.PutObjectResult, err error) {
	// No need to lock the backend while we read the data into memory; it holds
	// the write lock open unnecessarily, and could be blocked for an unreasonably
	// long time by a connection timing out:
	bts, err := s3tohdfs.ReadAll(input, size)
	if err != nil {
		return result, err
	}

	err = s3tohdfs.MergeMetadata(db, bucketName, objectName, meta)
	if err != nil {
		return result, err
	}

	db.lock.Lock()
	defer db.lock.Unlock()

	bucket := db.buckets[bucketName]
	if bucket == nil {
		return result, s3tohdfs.BucketNotFound(bucketName)
	}

	hash := md5.Sum(bts)

	item := &bucketData{
		name:         objectName,
		body:         bts,
		hash:         hash[:],
		etag:         `"` + hex.EncodeToString(hash[:]) + `"`,
		metadata:     meta,
		lastModified: db.timeSource.Now(),
	}

	bucket.put(objectName, item)

	if bucket.versioning == s3tohdfs.VersioningEnabled {
		// versionID is assigned in bucket.put()
		result.VersionID = item.versionID
	}

	return result, nil
}

func (db *Backend) DeleteObject(bucketName, objectName string) (result s3tohdfs.ObjectDeleteResult, rerr error) {
	db.lock.Lock()
	defer db.lock.Unlock()

	bucket := db.buckets[bucketName]
	if bucket == nil {
		return result, s3tohdfs.BucketNotFound(bucketName)
	}

	return bucket.rm(objectName, db.timeSource.Now())
}

func (db *Backend) DeleteMulti(bucketName string, objects ...string) (result s3tohdfs.MultiDeleteResult, err error) {
	db.lock.Lock()
	defer db.lock.Unlock()

	bucket := db.buckets[bucketName]
	if bucket == nil {
		return result, s3tohdfs.BucketNotFound(bucketName)
	}

	now := db.timeSource.Now()

	for _, object := range objects {
		dresult, err := bucket.rm(object, now)
		_ = dresult // FIXME: what to do with rm result in multi delete?

		if err != nil {
			errres := s3tohdfs.ErrorResultFromError(err)
			if errres.Code == s3tohdfs.ErrInternal {
				// FIXME: log
			}

			result.Error = append(result.Error, errres)

		} else {
			result.Deleted = append(result.Deleted, s3tohdfs.ObjectID{
				Key: object,
			})
		}
	}

	return result, nil
}

func (db *Backend) DeleteMultiVersions(bucketName string, objects ...s3tohdfs.ObjectID) (result s3tohdfs.MultiDeleteResult, err error) {
	db.lock.Lock()
	defer db.lock.Unlock()

	bucket := db.buckets[bucketName]
	if bucket == nil {
		return result, s3tohdfs.BucketNotFound(bucketName)
	}

	now := db.timeSource.Now()

	for _, object := range objects {
		var dresult s3tohdfs.ObjectDeleteResult
		var err error
		if object.VersionID != "" {
			_, err = bucket.rmVersion(object.Key, s3tohdfs.VersionID(object.VersionID), now)
		} else {
			dresult, err = bucket.rm(object.Key, now)
			_ = dresult // FIXME: what to do with rm result in multi delete?
		}

		if err != nil {
			errres := s3tohdfs.ErrorResultFromError(err)
			if errres.Code == s3tohdfs.ErrInternal {
				// FIXME: log
			}

			result.Error = append(result.Error, errres)

		} else {
			result.Deleted = append(result.Deleted, object)
		}
	}

	return result, nil
}

func (db *Backend) VersioningConfiguration(bucketName string) (versioning s3tohdfs.VersioningConfiguration, rerr error) {
	db.lock.RLock()
	defer db.lock.RUnlock()

	bucket := db.buckets[bucketName]
	if bucket == nil {
		return versioning, s3tohdfs.BucketNotFound(bucketName)
	}

	versioning.Status = bucket.versioning

	return versioning, nil
}

func (db *Backend) SetVersioningConfiguration(bucketName string, v s3tohdfs.VersioningConfiguration) error {
	if v.MFADelete.Enabled() {
		return s3tohdfs.ErrNotImplemented
	}

	db.lock.Lock()
	defer db.lock.Unlock()

	bucket := db.buckets[bucketName]
	if bucket == nil {
		return s3tohdfs.BucketNotFound(bucketName)
	}

	bucket.setVersioning(v.Enabled())

	return nil
}

func (db *Backend) GetObjectVersion(
	bucketName, objectName string,
	versionID s3tohdfs.VersionID,
	rangeRequest *s3tohdfs.ObjectRangeRequest) (*s3tohdfs.Object, error) {
	if versionID == "" {
		return db.GetObject(bucketName, objectName, rangeRequest)
	}

	db.lock.RLock()
	defer db.lock.RUnlock()

	bucket := db.buckets[bucketName]
	if bucket == nil {
		return nil, s3tohdfs.BucketNotFound(bucketName)
	}

	ver, err := bucket.objectVersion(objectName, versionID)
	if err != nil {
		return nil, err
	}

	return ver.toObject(rangeRequest, true)
}

func (db *Backend) HeadObjectVersion(bucketName, objectName string, versionID s3tohdfs.VersionID) (*s3tohdfs.Object, error) {
	if versionID == "" {
		return db.HeadObject(bucketName, objectName)
	}

	db.lock.RLock()
	defer db.lock.RUnlock()

	bucket := db.buckets[bucketName]
	if bucket == nil {
		return nil, s3tohdfs.BucketNotFound(bucketName)
	}

	ver, err := bucket.objectVersion(objectName, versionID)
	if err != nil {
		return nil, err
	}

	return ver.toObject(nil, false)
}

func (db *Backend) DeleteObjectVersion(bucketName, objectName string, versionID s3tohdfs.VersionID) (result s3tohdfs.ObjectDeleteResult, rerr error) {
	db.lock.Lock()
	defer db.lock.Unlock()

	bucket := db.buckets[bucketName]
	if bucket == nil {
		return result, s3tohdfs.BucketNotFound(bucketName)
	}

	return bucket.rmVersion(objectName, versionID, db.timeSource.Now())
}

func (db *Backend) ListBucketVersions(
	bucketName string,
	prefix *s3tohdfs.Prefix,
	page *s3tohdfs.ListBucketVersionsPage,
) (*s3tohdfs.ListBucketVersionsResult, error) {
	if prefix == nil {
		prefix = emptyPrefix
	}
	if page == nil {
		page = emptyVersionsPage
	}

	db.lock.RLock()
	defer db.lock.RUnlock()

	result := s3tohdfs.NewListBucketVersionsResult(bucketName, prefix, page)

	bucket := db.buckets[bucketName]
	if bucket == nil {
		return result, s3tohdfs.BucketNotFound(bucketName)
	}

	var iter = goskipiter.New(bucket.objects.Iterator())
	var match s3tohdfs.PrefixMatch

	if page.KeyMarker != "" {
		if !prefix.Match(page.KeyMarker, &match) {
			// FIXME: NO idea what S3 would do here.
			return result, s3tohdfs.ErrInternal
		}
		iter.Seek(page.KeyMarker)
	}

	var truncated = false
	var first = true
	var cnt int64 = 0

	// FIXME: The S3 docs have this to say on the topic of result ordering:
	//   "The following request returns objects in the order they were stored,
	//   returning the most recently stored object first starting with the value
	//   for key-marker."
	//
	// OK so this method....
	// - Returns objects in the order they were stored
	// - Returning the most recently stored object first
	//
	// This makes no sense at all!

	for iter.Next() {
		object := iter.Value().(*bucketObject)

		if !prefix.Match(object.name, &match) {
			continue
		}

		if match.CommonPrefix {
			result.AddPrefix(match.MatchedPart)
			continue
		}

		versions := iter.Value().(*bucketObject).Iterator()
		if first {
			if page.VersionIDMarker != "" {
				if !versions.Seek(page.VersionIDMarker) {
					// FIXME: log
					return result, s3tohdfs.ErrInternal
				}
			}
			first = false
		}

		for versions.Next() {
			version := versions.Value()

			if version.deleteMarker {
				marker := &s3tohdfs.DeleteMarker{
					Key:          version.name,
					IsLatest:     version == object.data,
					LastModified: s3tohdfs.NewContentTime(version.lastModified),
				}
				if bucket.versioning != s3tohdfs.VersioningNone { // S300005
					marker.VersionID = version.versionID
				}
				result.Versions = append(result.Versions, marker)

			} else {
				resultVer := &s3tohdfs.Version{
					Key:          version.name,
					IsLatest:     version == object.data,
					LastModified: s3tohdfs.NewContentTime(version.lastModified),
					Size:         int64(len(version.body)),
					ETag:         version.etag,
				}
				if bucket.versioning != s3tohdfs.VersioningNone { // S300005
					resultVer.VersionID = version.versionID
				}
				result.Versions = append(result.Versions, resultVer)
			}

			cnt++
			if page.MaxKeys > 0 && cnt >= page.MaxKeys {
				truncated = versions.Next()
				goto done
			}
		}
	}

done:
	result.IsTruncated = truncated || iter.Next()

	return result, nil
}

// nextVersion assumes the backend's lock is acquired
func (db *Backend) nextVersion() s3tohdfs.VersionID {
	v, scr := db.versionGenerator.Next(db.versionScratch)
	db.versionScratch = scr
	return v
}
