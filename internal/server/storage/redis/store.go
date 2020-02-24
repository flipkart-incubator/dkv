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

func (rdb *RedisDBStore) Put(key []byte, value []byte) error {
	return rdb.db.Set(string(key), value, 0).Err()
}

func (rdb *RedisDBStore) Get(keys ...[]byte) ([][]byte, error) {
	switch numKeys := len(keys); {
	case numKeys == 1:
		val, err := rdb.getSingleKey(keys[0])
		return [][]byte{val}, err
	default:
		return rdb.getMultipleKeys(keys)
	}
}

func (rdb *RedisDBStore) getSingleKey(key []byte) ([]byte, error) {
	if val, err := rdb.db.Get(string(key)).Result(); err != nil && !strings.HasSuffix(err.Error(), "nil") {
		return nil, err
	} else {
		return []byte(val), nil
	}
}

func (rdb *RedisDBStore) getMultipleKeys(keys [][]byte) ([][]byte, error) {
	var str_keys []string
	for _, key := range keys {
		str_keys = append(str_keys, string(key))
	}
	if vals, err := rdb.db.MGet(str_keys...).Result(); err != nil && !strings.HasSuffix(err.Error(), "nil") {
		return nil, err
	} else {
		results := make([][]byte, len(vals))
		for i, val := range vals {
			results[i] = []byte(val.(string))
		}
		return results, nil
	}
}
