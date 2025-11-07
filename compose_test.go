package jiffy

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestCompositeCache_Get(t *testing.T) {
	local1, err := NewLocalCache[string](10, 2*time.Second)
	assert.Nil(t, err)
	local2, err := NewLocalCache[string](10, 5*time.Second)
	assert.Nil(t, err)
	cache := NewCompositeCache[string]([]Cache[string]{local1, local2})
	err = cache.Set(context.Background(), "k1", "v1")
	assert.Nil(t, err)
	for i := 0; i < 5; i++ {
		v, err := cache.Get(context.Background(), "k1")
		assert.Nil(t, err)
		assert.NotNil(t, v)
		time.Sleep(1 * time.Second)
	}
}
