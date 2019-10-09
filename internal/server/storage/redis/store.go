package redis

import (
	"fmt"
	"strings"

	"github.com/go-redis/redis"
)

type RedisDBStore struct {
	db *redis.Client
}

func OpenStore(db_port, db_index int) (*RedisDBStore, error) {
	client := redis.NewClient(&redis.Options{
		Addr:     fmt.Sprintf("127.0.0.1:%d", db_port),
		Password: "",
		DB:       db_index,
	})
	if _, err := client.Ping().Result(); err != nil {
		return nil, err
	}
	return &RedisDBStore{client}, nil
}

func (rdb *RedisDBStore) Put(key []byte, value []byte) error {
	return rdb.db.Set(string(key), value, 0).Err()
}

func (rdb *RedisDBStore) Get(key []byte) ([]byte, error) {
	if val, err := rdb.db.Get(string(key)).Result(); err != nil && !strings.HasSuffix(err.Error(), "nil") {
		return nil, err
	} else {
		return []byte(val), nil
	}
}
