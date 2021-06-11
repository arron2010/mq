package config

import "sync"

type memCache struct {
	sync.RWMutex
	cache map[string]interface{}
}

func newMemCache() *memCache {
	c := &memCache{}
	c.cache = make(map[string]interface{})
	return c
}
func (c *memCache) Put(k string, v interface{}) {
	c.Lock()
	defer c.Unlock()
	c.cache[k] = v
}

func (c *memCache) Get(k string) (interface{}, bool) {
	c.RLock()
	defer c.RUnlock()
	v, ok := c.cache[k]
	return v, ok
}
