package datastructure

import (
	"easyRedis/logger"
	"easyRedis/util"
	"sort"
	"sync"
)

type Locks struct {
	locks []*sync.RWMutex
}

func NewLocks(size int) *Locks {
	locks := make([]*sync.RWMutex, size)
	for i := 0; i < size; i++ {
		locks[i] = &sync.RWMutex{}
	}
	return &Locks{locks: locks}
}

func (l *Locks) GetKeyPos(key string) int {
	pos, err := util.HashKey(key)
	if err != nil {
		logger.Error("Locks GetKeyPos error:%v", err)
		return -1
	}
	return pos % len(l.locks)
}

func (l *Locks) Lock(key string) {
	pos := l.GetKeyPos(key)
	if pos == -1 {
		logger.Error("Locks Lock key %s error: pos == -1", key)
		return
	}
	l.locks[pos].Lock()
}

func (l *Locks) Unlock(key string) {
	pos := l.GetKeyPos(key)
	if pos == -1 {
		logger.Error("Locks Unlock key %s error: pos == -1", key)
	}
	l.locks[pos].Unlock()
}

func (l *Locks) RLock(key string) {
	pos := l.GetKeyPos(key)
	if pos == -1 {
		logger.Error("Locks RLock key %s error: pos == -1", key)
	}
	l.locks[pos].RLock()
}

func (l *Locks) RUnlock(key string) {
	pos := l.GetKeyPos(key)
	if pos == -1 {
		logger.Error("Locks RUnLock key %s error: pos == -1", key)
	}
	l.locks[pos].RUnlock()
}

// 排序并去重
func (l *Locks) sortedLockPoses(keys []string) []int {
	set := make(map[int]struct{}) // 去重
	for _, k := range keys {
		pos := l.GetKeyPos(k)
		if pos == -1 {
			logger.Error("Locks Lock key %s error: pos == -1", k)
			return nil
		}
		set[pos] = struct{}{}
	}
	poses := make([]int, len(set))
	i := 0
	for pos, _ := range set {
		poses[i] = pos
		i++
	}
	sort.Ints(poses)
	return poses
}

func (l *Locks) LockMulti(keys []string) {
	// To avoid deadlock, we need to sort the locks
	poses := l.sortedLockPoses(keys)
	if poses == nil {
		return
	}
	for _, pos := range poses {
		l.locks[pos].Lock()
	}
}

func (l *Locks) UnlockMulti(keys []string) {
	poses := l.sortedLockPoses(keys)
	if poses == nil {
		return
	}
	for _, pos := range poses {
		l.locks[pos].Unlock()
	}
}

func (l *Locks) RLockMulti(keys []string) {
	poses := l.sortedLockPoses(keys)
	if poses == nil {
		return
	}
	for _, pos := range poses {
		l.locks[pos].RLock()
	}
}

func (l *Locks) RUnlockMulti(keys []string) {
	poses := l.sortedLockPoses(keys)
	if poses == nil {
		return
	}
	for _, pos := range poses {
		l.locks[pos].RUnlock()
	}
}
