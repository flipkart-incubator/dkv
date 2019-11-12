package redis

import (
	"fmt"
	"strings"

	"github.com/flipkart-incubator/dkv/internal/server/storage"
	"github.com/go-redis/redis"
)

type RedisDBStore struct {
	db *redis.Client
}

func OpenDB(dbPort, dbIndex int) storage.KVStore {
	if kvs, err := OpenStore(dbPort, dbIndex); err != nil {
		panic(err)
	} else {
		return kvs
	}
}

func OpenStore(dbPort, dbIndex int) (*RedisDBStore, error) {
	client := redis.NewClient(&redis.Options{
		Addr:     fmt.Sprintf("127.0.0.1:%d", dbPort),
		Password: "",
		DB:       dbIndex,
	})
	if _, err := client.Ping().Result(); err != nil {
		return nil, err
	}
	return &RedisDBStore{client}, nil
}

func (rdb *RedisDBStore) Close() error {
	return rdb.db.Close()
}

func (rdb *RedisDBStore) Put(key []byte, value []byte) *storage.Result {
	err := rdb.db.Set(string(key), value, 0).Err()
	return &storage.Result{err}
}

func (rdb *RedisDBStore) Get(keys ...[]byte) []*storage.ReadResult {
	var results []*storage.ReadResult
	numKeys := len(keys)

	switch {
	case numKeys == 1:
		results = append(results, rdb.getSingleKey(keys[0]))
	case numKeys > 1:
		results = rdb.getMultipleKeys(keys)
	default:
		results = nil
	}
	return results
}

func (rdb *RedisDBStore) getSingleKey(key []byte) *storage.ReadResult {
	if val, err := rdb.db.Get(string(key)).Result(); err != nil && !strings.HasSuffix(err.Error(), "nil") {
		return storage.NewReadResultWithError(err)
	} else {
		return storage.NewReadResultWithValue([]byte(val))
	}
}

func (rdb *RedisDBStore) getMultipleKeys(keys [][]byte) []*storage.ReadResult {
	var str_keys []string
	for _, key := range keys {
		str_keys = append(str_keys, string(key))
	}
	if vals, err := rdb.db.MGet(str_keys...).Result(); err != nil && !strings.HasSuffix(err.Error(), "nil") {
		return []*storage.ReadResult{storage.NewReadResultWithError(err)}
	} else {
		results := make([]*storage.ReadResult, len(vals))
		for i, val := range vals {
			results[i] = storage.NewReadResultWithValue([]byte(val.(string)))
		}
		return results
	}
}
