package datastructure

import (
	"easyRedis/util"
	"sync"
)

const MaxConSize = int(1<<31 - 1)

type shard struct {
	mp   map[string]any
	rwMu *sync.RWMutex
}

// ConcurrentMap manage a table slice with multiple hashmap shards.
// It is threads safe by using rwLock.
// it supports maximum table size = MaxConSize
type ConcurrentMap struct {
	table []*shard
	size  int // table size
	count int // total number of keys
}

func NewConcurrentMap(size int) *ConcurrentMap {
	if size <= 0 || size > MaxConSize {
		size = MaxConSize
	}
	m := &ConcurrentMap{
		table: make([]*shard, size),
		size:  size,
		count: 0,
	}
	for i := 0; i < size; i++ {
		m.table[i] = &shard{
			mp:   make(map[string]any),
			rwMu: &sync.RWMutex{},
		}
	}
	return m
}

func (m *ConcurrentMap) getKeyPos(key string) int {
	hash, err := util.HashKey(key)
	if err != nil {
		return -1
	}
	return hash % m.size
}

// Set 0：修改 1：添加
func (m *ConcurrentMap) Set(key string, val any) int {
	added := 0
	pos := m.getKeyPos(key)
	shard := m.table[pos]
	shard.rwMu.Lock()
	defer shard.rwMu.Unlock()

	if _, ok := shard.mp[key]; !ok {
		m.count++
		added = 1
	}
	shard.mp[key] = val
	return added
}

func (m *ConcurrentMap) SetIfExist(key string, value any) int {
	pos := m.getKeyPos(key)
	shard := m.table[pos]
	shard.rwMu.Lock()
	defer shard.rwMu.Unlock()

	if _, ok := shard.mp[key]; ok {
		shard.mp[key] = value
		return 1
	}
	return 0
}

func (m *ConcurrentMap) SetIfNotExist(key string, value any) int {
	pos := m.getKeyPos(key)
	shard := m.table[pos]
	shard.rwMu.Lock()
	defer shard.rwMu.Unlock()

	if _, ok := shard.mp[key]; !ok {
		m.count++
		shard.mp[key] = value
		return 1
	}
	return 0
}

func (m *ConcurrentMap) Get(key string) (any, bool) {
	pos := m.getKeyPos(key)
	shard := m.table[pos]
	shard.rwMu.RLock()
	defer shard.rwMu.RUnlock()

	val, ok := shard.mp[key]
	return val, ok
}

func (m *ConcurrentMap) Delete(key string) int {
	pos := m.getKeyPos(key)
	shard := m.table[pos]
	shard.rwMu.Lock()
	defer shard.rwMu.Unlock()

	if _, ok := shard.mp[key]; ok {
		delete(shard.mp, key)
		m.count--
		return 1
	}
	return 0
}

func (m *ConcurrentMap) Clear() {
	*m = *NewConcurrentMap(m.size)
}

func (m *ConcurrentMap) Keys() []string {
	keys := make([]string, m.count)
	i := 0
	for _, shard := range m.table {
		shard.rwMu.RLock()
		for key := range shard.mp {
			keys[i] = key
			i++
		}
		shard.rwMu.RUnlock()
	}
	return keys
}
