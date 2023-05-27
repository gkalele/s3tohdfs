package mem

import "fmt"

type Info struct {
	Size uint64
}

type MemStore struct {
	Buckets map[string]*BucketInfo
}

type BucketInfo struct {
	LMap map[string]*Info
}

func Factory() *MemStore {
	return &MemStore{Buckets: map[string]*BucketInfo{}}
}

func (m *MemStore) CreateBucket(bucketname string) {
	if _, ok := m.Buckets[bucketname]; !ok {
		m.Buckets[bucketname] = &BucketInfo{LMap: map[string]*Info{}}
	}
}

func (m *MemStore) Create(bucket, path string, size uint64) error {
	binfo, ok := m.Buckets[bucket]
	if !ok {
		return fmt.Errorf("invalid bucket name " + bucket)
	}
	binfo.LMap[path] = &Info{size}
	return nil
}

func (m *MemStore) Get(bucket, path string) (bool, uint64, error) {
	binfo, ok := m.Buckets[bucket]
	if !ok {
		return false, 0, fmt.Errorf("invalid bucket name " + bucket)
	}
	info, ok := binfo.LMap[path]
	if !ok {
		return false, 0, nil
	}
	return true, info.Size, nil
}

func allKeys(m map[string]*Info) []string {
	s := make([]string, 0, len(m))
	for k := range m {
		s = append(s, k)
	}
	return s
}

func (m *MemStore) Index(bucket, path string) ([]string, error) {
	binfo, ok := m.Buckets[bucket]
	if !ok {
		return nil, fmt.Errorf("invalid bucket name " + bucket)
	}
	return allKeys(binfo.LMap), nil
}

func (m *MemStore) Delete(bucket, path string) error {
	binfo, ok := m.Buckets[bucket]
	if !ok {
		return fmt.Errorf("invalid bucket name " + bucket)
	}
	delete(binfo.LMap, path)
	return nil
}
