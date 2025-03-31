package rediscache

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/goware/cachestore"
	"github.com/redis/go-redis/v9"
)

const DefaultTTL = time.Second * 24 * 60 * 60 // 1 day in seconds

var _ cachestore.Store[any] = &RedisStore[any]{}

func NewBackend(cfg *Config, opts ...cachestore.StoreOptions) (cachestore.Backend, error) {
	return newRedisStore[any](cfg, opts...)
}

func NewCache[V any](cfg *Config, opts ...cachestore.StoreOptions) (*RedisStore[V], error) {
	return newRedisStore[V](cfg, opts...)
}

func newRedisStore[V any](cfg *Config, opts ...cachestore.StoreOptions) (*RedisStore[V], error) {
	if !cfg.Enabled {
		return nil, errors.New("cachestore-redis: attempting to create store while config.Enabled is false")
	}

	if cfg.Host == "" {
		return nil, errors.New("cachestore-redis: missing \"host\" parameter")
	}
	if cfg.Port < 1 {
		cfg.Port = 6379
	}
	if cfg.KeyTTL == 0 {
		cfg.KeyTTL = DefaultTTL // default setting
	}

	// Create store and connect to backend
	address := fmt.Sprintf("%s:%d", cfg.Host, cfg.Port)
	store := &RedisStore[V]{
		options: cachestore.ApplyOptions(opts...),
		client:  newRedisClient(cfg, address),
		random:  rand.Reader,
	}

	err := store.client.Ping(context.Background()).Err()
	if err != nil {
		return nil, fmt.Errorf("cachestore-redis: unable to dial redis host %v: %w", address, err)
	}

	// Apply store options, where value set by options will take precedence over the default
	store.options = cachestore.ApplyOptions(opts...)
	if store.options.DefaultKeyExpiry == 0 && cfg.KeyTTL > 0 {
		store.options.DefaultKeyExpiry = cfg.KeyTTL
	}

	// Set default key expiry for a long time on redis always. This is how we ensure
	// the cache will always function as a LRU.
	if store.options.DefaultKeyExpiry == 0 {
		store.options.DefaultKeyExpiry = DefaultTTL
	}

	return store, nil
}

type RedisStore[V any] struct {
	options cachestore.StoreOptions
	client  *redis.Client
	random  io.Reader
}

var _ cachestore.Store[any] = &RedisStore[any]{}

func newRedisClient(cfg *Config, address string) *redis.Client {
	var maxIdle, maxActive = cfg.MaxIdle, cfg.MaxActive
	if maxIdle <= 0 {
		maxIdle = 20
	}
	if maxActive <= 0 {
		maxActive = 50
	}

	return redis.NewClient(&redis.Options{
		Addr:         address,
		DB:           cfg.DBIndex,
		MinIdleConns: maxIdle,
		PoolSize:     maxActive,
	})
}

func (c *RedisStore[V]) Name() string {
	return "rediscache"
}

func (c *RedisStore[V]) Set(ctx context.Context, key string, value V) error {
	// call SetEx passing default keyTTL
	return c.SetEx(ctx, key, value, c.options.DefaultKeyExpiry)
}

func (c *RedisStore[V]) SetEx(ctx context.Context, key string, value V, ttl time.Duration) error {
	if len(key) > cachestore.MaxKeyLength {
		return cachestore.ErrKeyLengthTooLong
	}
	if len(key) == 0 {
		return cachestore.ErrInvalidKey
	}

	// TODO: handle timeout here, and return error if we hit it via ctx

	data, err := serialize(value)
	if err != nil {
		return fmt.Errorf("cachestore-redis: unable to serialize object: %w", err)
	}

	if ttl > 0 {
		_, err = c.client.SetEx(ctx, key, data, ttl).Result()
	} else {
		_, err = c.client.Set(ctx, key, data, 0).Result()
	}
	if err != nil {
		return fmt.Errorf("cachestore-redis: unable to set key %s: %w", key, err)
	}
	return nil
}

func (c *RedisStore[V]) BatchSet(ctx context.Context, keys []string, values []V) error {
	return c.BatchSetEx(ctx, keys, values, c.options.DefaultKeyExpiry)
}

func (c *RedisStore[V]) BatchSetEx(ctx context.Context, keys []string, values []V, ttl time.Duration) error {
	if len(keys) != len(values) {
		return errors.New("cachestore-redis: keys and values are not the same length")
	}
	if len(keys) == 0 {
		return errors.New("cachestore-redis: no keys are passed")
	}

	pipeline := c.client.Pipeline()

	// use pipelining to insert all keys. This ensures only one round-trip to
	// the server. We could use MSET but it doesn't support TTL so we'd need to
	// send one EXPIRE command per key anyway
	for i, key := range keys {
		data, err := serialize(values[i])
		if err != nil {
			return fmt.Errorf("cachestore-redis: unable to serialize object: %w", err)
		}

		if ttl > 0 {
			err = pipeline.SetEx(ctx, key, data, ttl).Err()
		} else {
			err = pipeline.Set(ctx, key, data, 0).Err()
		}
		if err != nil {
			return fmt.Errorf("cachestore-redis: failed writing key: %w", err)
		}
	}

	// send all commands
	_, err := pipeline.Exec(ctx)
	if err != nil {
		return fmt.Errorf("cachestore-redis: error encountered while batch-inserting value: %w", err)
	}

	return nil
}

func (c *RedisStore[V]) Get(ctx context.Context, key string) (V, bool, error) {
	var out V

	data, err := c.client.Get(ctx, key).Bytes()
	if err != nil && err != redis.Nil {
		return out, false, fmt.Errorf("cachestore-redis: GET command failed: %w", err)
	}
	if data == nil {
		return out, false, nil
	}

	out, err = deserialize[V](data)
	if err != nil {
		return out, false, fmt.Errorf("cachestore-redis: deserialize: %w", err)
	}

	return out, true, nil
}

func (c *RedisStore[V]) BatchGet(ctx context.Context, keys []string) ([]V, []bool, error) {
	// execute MGET and convert result to []V
	values, err := c.client.MGet(ctx, keys...).Result()
	if err != nil {
		return nil, nil, fmt.Errorf("cachestore-redis: failed assertion")
	}

	// we should always return the same number of values as keys requested,
	// in the same order
	if len(values) != len(keys) {
		return nil, nil, fmt.Errorf("cachestore/redis: failed assertion")
	}
	out := make([]V, len(values))
	oks := make([]bool, len(values))

	for i, value := range values {
		if value == nil {
			continue
		}
		v, _ := value.(string)

		out[i], err = deserialize[V]([]byte(v))
		if err != nil {
			return nil, nil, err
		}
		oks[i] = true
	}

	return out, oks, nil
}

func (c *RedisStore[V]) Exists(ctx context.Context, key string) (bool, error) {
	value, err := c.client.Exists(ctx, key).Result()
	if err != nil {
		return false, fmt.Errorf("cachestore-redis: EXISTS command failed: %w", err)
	}

	if value == 1 {
		return true, nil
	}
	return false, nil
}

func (c *RedisStore[V]) Delete(ctx context.Context, key string) error {
	return c.client.Del(ctx, key).Err()
}

func (c *RedisStore[V]) DeletePrefix(ctx context.Context, keyPrefix string) error {
	if len(keyPrefix) < 4 {
		return fmt.Errorf("cachestore-redis: DeletePrefix keyPrefix '%s' must be at least 4 characters long", keyPrefix)
	}

	keys := make([]string, 0, 1000)

	var results []string
	var cursor uint64 = 0
	var limit int64 = 2000
	var err error
	start := true

	for start || cursor != 0 {
		start = false
		results, cursor, err = c.client.Scan(ctx, cursor, fmt.Sprintf("%s*", keyPrefix), limit).Result()
		if err != nil {
			return fmt.Errorf("cachestore-redis: SCAN command returned unexpected result: %w", err)
		}
		keys = append(keys, results...)
	}

	if len(keys) == 0 {
		return nil
	}

	err = c.client.Unlink(ctx, keys...).Err()
	if err != nil {
		return fmt.Errorf("cachestore-redis: DeletePrefix UNLINK failed: %w", err)
	}

	return nil
}

func (c *RedisStore[V]) ClearAll(ctx context.Context) error {
	// With redis, we do not support ClearAll as its too destructive. For testing
	// use the memlru if you want to Clear All.
	return fmt.Errorf("cachestore-redis: unsupported")
}

func (c *RedisStore[V]) GetOrSetWithLock(
	ctx context.Context, key string, getter func(context.Context, string) (V, error),
) (V, error) {
	return c.GetOrSetWithLockEx(ctx, key, getter, c.options.DefaultKeyExpiry)
}

func (c *RedisStore[V]) GetOrSetWithLockEx(
	ctx context.Context, key string, getter func(context.Context, string) (V, error), ttl time.Duration,
) (V, error) {
	var out V

	mu, err := c.newMutex(ctx, key)
	if err != nil {
		return out, fmt.Errorf("cachestore-redis: creating mutex failed: %w", err)
	}
	defer mu.Unlock()

	for i := 0; ; i++ {
		// If there's a value in the cache, return it immediately
		out, found, err := c.Get(ctx, key)
		if err != nil {
			return out, err
		}
		if found {
			return out, nil
		}

		// Otherwise attempt to acquire a lock for writing
		acquired, err := mu.TryLock(ctx)
		if err != nil {
			return out, fmt.Errorf("cachestore-redis: try lock failed: %w", err)
		}
		if acquired {
			break
		}

		if err := mu.WaitForRetry(ctx, i); err != nil {
			return out, fmt.Errorf("cachestore-redis: timed out waiting for lock: %w", err)
		}
	}

	// We extend the lock in a goroutine for as long as GetOrSetWithLockEx runs.
	// If we're unable to extend the lock, the cancellation is propagated to the getter,
	// that is then expected to terminate.
	ctx, cancel := context.WithCancel(ctx) // TODO/NOTE: use WithCancelCause in the future to signal underlying err
	extendCtx, cancelExtending := context.WithCancel(ctx)
	defer cancelExtending()
	go func() {
		for {
			select {
			case <-extendCtx.Done():
				return
			case <-time.After(mu.lockExpiry / 2):
				if err := mu.Extend(extendCtx); err != nil {
					cancel()
				}
			}
		}
	}()

	// Retrieve a new value from the origin
	out, err = getter(ctx, key)
	if err != nil {
		return out, fmt.Errorf("cachestore-redis: getter function failed: %w", err)
	}

	// Store the retrieved value in the cache
	if err := c.SetEx(ctx, key, out, ttl); err != nil {
		return out, fmt.Errorf("cachestore-redis: SetEx: %w", err)
	}

	return out, nil
}

func (c *RedisStore[V]) RedisClient() *redis.Client {
	return c.client
}

func (c *RedisStore[V]) ByteStore() cachestore.Store[[]byte] {
	return &RedisStore[[]byte]{
		client: c.client,
		random: c.random,
	}
}

func serialize[V any](value V) ([]byte, error) {
	// return the value directly if the type is a []byte or string,
	// otherwise assume its json and unmarshal it
	switch v := any(value).(type) {
	case string:
		return []byte(v), nil
	case []byte:
		return v, nil
	default:
		out, err := json.Marshal(value)
		if err != nil {
			return nil, fmt.Errorf("cachestore-redis: failed to marshal data: %w", err)
		}
		return out, nil
	}
}

func deserialize[V any](data []byte) (V, error) {
	var out V
	switch any(out).(type) {
	case string:
		str := string(data)
		out = any(str).(V)
		return out, nil
	case []byte:
		out = any(data).(V)
		return out, nil
	default:
		err := json.Unmarshal(data, &out)
		if err != nil {
			return out, fmt.Errorf("cachestore-redis: failed to unmarshal data: %w", err)
		}
		return out, nil
	}
}
