package rediscache

import (
	"context"
	"fmt"
	"math/rand"
	"sync/atomic"
	"testing"
	"time"

	cachestore "github.com/goware/cachestore2"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

func TestBasicString(t *testing.T) {
	ctx := context.Background()

	cache, err := NewCache[string](&Config{Enabled: true, Host: "localhost"}, cachestore.WithDefaultKeyExpiry(10*time.Second))
	require.NoError(t, err)
	require.True(t, cache.options.DefaultKeyExpiry.Seconds() == 10)

	err = cache.Set(ctx, "hi", "bye")
	require.NoError(t, err)

	value, exists, err := cache.Get(ctx, "hi")
	require.NoError(t, err)
	require.True(t, exists)
	require.Equal(t, "bye", value)

	value, exists, err = cache.Get(ctx, "does-not-exist")
	require.NoError(t, err)
	require.False(t, exists)
	require.Equal(t, "", value)
}

func TestBasicBytes(t *testing.T) {
	ctx := context.Background()

	cache, err := NewCache[[]byte](&Config{Enabled: true, Host: "localhost"})
	require.NoError(t, err)

	err = cache.Set(ctx, "test-bytes", []byte{1, 2, 3, 4})
	require.NoError(t, err)

	value, exists, err := cache.Get(ctx, "test-bytes")
	require.NoError(t, err)
	require.True(t, exists)
	require.Equal(t, []byte{1, 2, 3, 4}, value)
}

type obj struct {
	A string
	B string
}

func TestBasicObject(t *testing.T) {
	ctx := context.Background()

	cache, err := NewCache[obj](&Config{Enabled: true, Host: "localhost"})
	require.NoError(t, err)

	var in = obj{A: "hi", B: "bye"}

	err = cache.Set(ctx, "test-obj", in)
	require.NoError(t, err)

	out, exists, err := cache.Get(ctx, "test-obj")
	require.True(t, exists)
	require.NoError(t, err)
	require.Equal(t, in, out)
}

func TestBasicObject2(t *testing.T) {
	ctx := context.Background()

	cache, err := NewCache[*obj](&Config{Enabled: true, Host: "localhost"})
	require.NoError(t, err)

	var in = &obj{A: "hi", B: "bye"}

	err = cache.Set(ctx, "test-obj2", in)
	require.NoError(t, err)

	out, exists, err := cache.Get(ctx, "test-obj2")
	require.True(t, exists)
	require.NoError(t, err)
	require.Equal(t, in, out)
}

func TestBasicBatchObjects(t *testing.T) {
	ctx := context.Background()

	cache, err := NewCache[*obj](&Config{Enabled: true, Host: "localhost"})
	require.NoError(t, err)

	var keys = []string{
		"test-obj3-a", "test-obj3-b",
	}

	var in = []*obj{
		{A: "3a", B: "3a"},
		{A: "3b", B: "3b"},
	}

	err = cache.BatchSet(ctx, keys, in)
	require.NoError(t, err)

	// adding some keys which will not exist
	fetchKeys := []string{"no1"}
	fetchKeys = append(fetchKeys, keys...)
	fetchKeys = append(fetchKeys, []string{"no2", "no3"}...)

	out, exists, err := cache.BatchGet(ctx, fetchKeys)
	require.NoError(t, err)
	require.Equal(t, []*obj{nil, in[0], in[1], nil, nil}, out)
	require.Equal(t, []bool{false, true, true, false, false}, exists)

	// another
	err = cache.Set(ctx, "yes", &obj{A: "yesA", B: "yesB"})
	require.NoError(t, err)

	vs, oks, err := cache.BatchGet(ctx, []string{"nil1", "yes", "nil2"})
	require.NoError(t, err)

	require.Nil(t, vs[0])
	require.NotNil(t, vs[1])
	require.Nil(t, vs[2])
	require.False(t, oks[0])
	require.True(t, oks[1])
	require.False(t, oks[2])
	require.NoError(t, err)
}

func TestBasicBatchObjectEmptyKeys(t *testing.T) {
	ctx := context.Background()

	cache, err := NewCache[*obj](&Config{Enabled: true, Host: "localhost"})
	require.NoError(t, err)

	var keys = []string{}

	var in = []*obj{}

	err = cache.BatchSet(ctx, keys, in)
	require.Error(t, err)
}

func TestExpiryOptions(t *testing.T) {
	ctx := context.Background()

	cache, err := NewCache[string](&Config{Enabled: true, Host: "localhost"}, cachestore.WithDefaultKeyExpiry(1*time.Second))
	// cache, err := New[string](&Config{Host: "localhost", KeyTTL: 1 * time.Second})
	require.NoError(t, err)
	require.True(t, cache.options.DefaultKeyExpiry.Seconds() == 1)

	err = cache.Set(ctx, "hi", "bye")
	require.NoError(t, err)

	err = cache.SetEx(ctx, "another", "longer", 10*time.Second)
	require.NoError(t, err)

	value, exists, err := cache.Get(ctx, "hi")
	require.NoError(t, err)
	require.True(t, exists)
	require.Equal(t, "bye", value)

	// pause to wait for expiry..
	time.Sleep(2 * time.Second)

	value, exists, err = cache.Get(ctx, "hi")
	require.NoError(t, err)
	require.False(t, exists)
	require.Equal(t, "", value)

	value, exists, err = cache.Get(ctx, "another")
	require.NoError(t, err)
	require.True(t, exists)
	require.Equal(t, "longer", value)
}

func TestDeletePrefix(t *testing.T) {
	ctx := context.Background()

	cache, err := NewCache[string](&Config{Enabled: true, Host: "localhost"})
	require.NoError(t, err)

	err = cache.Set(ctx, "test1", "1")
	require.NoError(t, err)
	err = cache.Set(ctx, "test2", "2")
	require.NoError(t, err)
	err = cache.Set(ctx, "test3", "3")
	require.NoError(t, err)
	err = cache.Set(ctx, "test4", "4")
	require.NoError(t, err)

	v, ok, err := cache.Get(ctx, "test3")
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, "3", v)

	err = cache.DeletePrefix(ctx, "test")
	require.NoError(t, err)

	v, ok, err = cache.Get(ctx, "test3")
	require.NoError(t, err)
	require.False(t, ok)
	require.Equal(t, "", v)
}

func TestGetOrSetWithLock(t *testing.T) {
	ctx := context.Background()

	cache, err := NewCache[string](&Config{Enabled: true, Host: "localhost"}, cachestore.WithLockExpiry(1*time.Second))
	require.NoError(t, err)

	var counter atomic.Uint32
	getter := func(ctx context.Context, key string) (string, error) {
		counter.Add(1)
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-time.After(3 * time.Second):
			return "result:" + key, nil
		}
	}

	concurrentCalls := 15
	results := make(chan string, concurrentCalls)
	key := fmt.Sprintf("concurrent-%d", rand.Uint64())

	var wg errgroup.Group
	for i := 0; i < concurrentCalls; i++ {
		wg.Go(func() error {
			v, err := cache.GetOrSetWithLock(ctx, key, getter)
			if err != nil {
				return err
			}
			results <- v
			return nil
		})
	}

	require.NoError(t, wg.Wait())
	require.Equalf(t, 1, int(counter.Load()), "getter should be called only once")

	for i := 0; i < concurrentCalls; i++ {
		select {
		case v := <-results:
			require.Equal(t, "result:"+key, v)
		default:
			t.Errorf("expected %d results but only got %d", concurrentCalls, i)
		}
	}
}

func TestBackend(t *testing.T) {
	backend, err := NewBackend(&Config{Enabled: true, Host: "localhost"})
	require.NoError(t, err)

	t.Run("basic", func(t *testing.T) {
		cache := cachestore.OpenStore[string](backend)

		{
			err = cache.Set(context.Background(), "key", "value")
			require.NoError(t, err)

			value, exists, err := cache.Get(context.Background(), "key")
			require.NoError(t, err)
			require.True(t, exists)
			require.Equal(t, "value", value)

			err = cache.Delete(context.Background(), "key")
			require.NoError(t, err)

			value, exists, err = cache.Get(context.Background(), "key")
			require.NoError(t, err)
			require.False(t, exists)
			require.Equal(t, "", value)
		}

		{
			keys := []string{"akey1", "akey2", "akey3"}
			values := []string{"avalue1", "avalue2", "avalue3"}
			err = cache.BatchSet(context.Background(), keys, values)
			require.NoError(t, err)

			batch, exists, err := cache.BatchGet(context.Background(), keys)
			require.NoError(t, err)
			require.Equal(t, values, batch)
			require.Equal(t, []bool{true, true, true}, exists)

			err = cache.DeletePrefix(context.Background(), "akey")
			require.NoError(t, err)

			batch, exists, err = cache.BatchGet(context.Background(), keys)
			require.NoError(t, err)
			require.Equal(t, []string{"", "", ""}, batch)
			require.Equal(t, []bool{false, false, false}, exists)
		}
	})

	t.Run("type: []byte", func(t *testing.T) {
		cache := cachestore.OpenStore[[]byte](backend)

		err = cache.Set(context.Background(), "bkey", []byte(`hello "world"`))
		require.NoError(t, err)

		value, exists, err := cache.Get(context.Background(), "bkey")
		require.NoError(t, err)
		require.True(t, exists)
		require.Equal(t, []byte(`hello "world"`), value)
	})
}

func TestBackendGetOrSetWithLock(t *testing.T) {
	backend, err := NewBackend(&Config{Enabled: true, Host: "localhost"})
	require.NoError(t, err)

	cache := cachestore.OpenStore[string](backend)

	ctx := context.Background()

	var counter atomic.Uint32
	getter := func(ctx context.Context, key string) (string, error) {
		counter.Add(1)
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-time.After(3 * time.Second):
			return "result:" + key, nil
		}
	}

	concurrentCalls := 15
	results := make(chan string, concurrentCalls)
	key := fmt.Sprintf("concurrent-%d", rand.Uint64())

	var wg errgroup.Group
	for i := 0; i < concurrentCalls; i++ {
		wg.Go(func() error {
			v, err := cache.GetOrSetWithLock(ctx, key, getter)
			if err != nil {
				return err
			}
			results <- v
			return nil
		})
	}

	require.NoError(t, wg.Wait())
	require.Equalf(t, 1, int(counter.Load()), "getter should be called only once")

	for i := 0; i < concurrentCalls; i++ {
		select {
		case v := <-results:
			require.Equal(t, "result:"+key, v)
		default:
			t.Errorf("expected %d results but only got %d", concurrentCalls, i)
		}
	}
}
