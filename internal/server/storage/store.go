package storage

type KVStore interface {
	Put(key []byte, value []byte) error
	Get(key []byte) ([]byte, error)
}
