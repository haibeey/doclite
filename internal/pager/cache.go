package pager

import (
	"container/list"
	"sync"
)

// cacheKey names one version of a page. frame is the WAL frame that holds
// the version, or 0 for the version in the main file.
type cacheKey struct {
	pg    PgNo
	frame uint64
}

type cacheEntry struct {
	key  cacheKey
	data []byte
}

// cache is an LRU cache of page versions. Cached buffers are shared and
// must not be changed.
type cache struct {
	mu    sync.Mutex
	limit int
	ll    *list.List
	items map[cacheKey]*list.Element
}

func newCache(limit int) *cache {
	return &cache{limit: limit, ll: list.New(), items: make(map[cacheKey]*list.Element)}
}

func (c *cache) get(k cacheKey) []byte {
	c.mu.Lock()
	defer c.mu.Unlock()
	if e, ok := c.items[k]; ok {
		c.ll.MoveToFront(e)
		return e.Value.(*cacheEntry).data
	}
	return nil
}

func (c *cache) put(k cacheKey, data []byte) {
	if c.limit <= 0 {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if e, ok := c.items[k]; ok {
		c.ll.MoveToFront(e)
		e.Value.(*cacheEntry).data = data
		return
	}
	c.items[k] = c.ll.PushFront(&cacheEntry{key: k, data: data})
	for c.ll.Len() > c.limit {
		e := c.ll.Back()
		c.ll.Remove(e)
		delete(c.items, e.Value.(*cacheEntry).key)
	}
}

func (c *cache) clear() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.ll.Init()
	clear(c.items)
}
