package redis

import (
	"fmt"
	"strings"

	"github.com/flipkart-incubator/dkv/internal/server/storage"
	"github.com/go-redis/redis"
)

type redisDBStore struct {
	db *redis.Client
}

// OpenDB initializes a new instance of RedisDB connecting
// to the Redis DB service running on the given port.
func OpenDB(dbPort, dbIndex int) storage.KVStore {
	if kvs, err := openStore(dbPort, dbIndex); err != nil {
		panic(err)
	} else {
		return kvs
	}
}

func openStore(dbPort, dbIndex int) (*redisDBStore, error) {
	client := redis.NewClient(&redis.Options{
		Addr:     fmt.Sprintf("127.0.0.1:%d", dbPort),
		Password: "",
		DB:       dbIndex,
	})
	if _, err := client.Ping().Result(); err != nil {
		return nil, err
	}
	return &redisDBStore{client}, nil
}

func (rdb *redisDBStore) Close() error {
	return rdb.db.Close()
}

func (rdb *redisDBStore) Put(key []byte, value []byte) error {
	return rdb.db.Set(string(key), value, 0).Err()
}

func (rdb *redisDBStore) Get(keys ...[]byte) ([][]byte, error) {
	switch numKeys := len(keys); {
	case numKeys == 1:
		val, err := rdb.getSingleKey(keys[0])
		return [][]byte{val}, err
	default:
		return rdb.getMultipleKeys(keys)
	}
}

func (rdb *redisDBStore) GetSnapshot() ([]byte, error) {
	return nil, nil
}

func (rdb *redisDBStore) PutSnapshot(snap []byte) error {
	return nil
}

func (rdb *redisDBStore) Iterate(iterOpts ...storage.IterationOption) (storage.Iterator, error) {
	return nil, nil
}

func (rdb *redisDBStore) getSingleKey(key []byte) ([]byte, error) {
	val, err := rdb.db.Get(string(key)).Result()
	if err != nil && !strings.HasSuffix(err.Error(), "nil") {
		return nil, err
	}
	return []byte(val), nil
}

func (rdb *redisDBStore) getMultipleKeys(keys [][]byte) ([][]byte, error) {
	var strKeys []string
	for _, key := range keys {
		strKeys = append(strKeys, string(key))
	}
	vals, err := rdb.db.MGet(strKeys...).Result()
	if err != nil && !strings.HasSuffix(err.Error(), "nil") {
		return nil, err
	}
	results := make([][]byte, len(vals))
	for i, val := range vals {
		results[i] = []byte(val.(string))
	}
	return results, nil
}
