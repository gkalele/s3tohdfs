package metadata

// MetadataStore
type MetadataStore interface {
	CreateBucket(string)
	Create(string, string, uint64) error
	Get(string, string) (bool, uint64, error)
	Index(string, string) ([]string, error)
	Delete(string, string) error
}
