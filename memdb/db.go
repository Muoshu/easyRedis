package memdb

import (
	"easyRedis/config"
	"easyRedis/datastructure"
	"easyRedis/logger"
	"easyRedis/resp"
	"easyRedis/timewheel"
	"strings"
	"time"
)

// MemDb is the memory cache database
// All key:value pairs are stored in db
// All ttl keys are stored in ttlKeys
// locks is used to lock a key for db to ensure some atomic operations
type MemDb struct {
	db      *datastructure.ConcurrentMap
	ttlKeys *datastructure.ConcurrentMap
	locks   *datastructure.Locks
	delay   *timewheel.Delay
}

func NewMemDb() *MemDb {
	return &MemDb{
		db:      datastructure.NewConcurrentMap(config.Configures.ShardNum),
		ttlKeys: datastructure.NewConcurrentMap(config.Configures.ShardNum),
		locks:   datastructure.NewLocks(config.Configures.ShardNum * 2),
		delay:   timewheel.NewDelay(),
	}
}

func (m *MemDb) ExecCommand(cmd [][]byte) resp.RedisData {
	if len(cmd) == 0 {
		return nil
	}

	var res resp.RedisData
	cmdName := strings.ToLower(string(cmd[0]))
	command, ok := CmdTable[cmdName]
	if !ok {
		res = resp.NewErrorData("error: unsupported command")
	} else {
		execFun := command.executor
		res = execFun(m, cmd)
	}
	return res
}

// CheckTTL check ttl keys and delete expired keys
// return false if key is expired, else true.
// Attention: Don't lock this function because it has called locks.Lock(key) for atomic deleting expired key.
// Otherwise, it will cause a deadlock.
func (m *MemDb) CheckTTL(key string) bool {
	ttl, ok := m.ttlKeys.Get(key)
	if !ok {
		return true
	}
	ttlTime := ttl.(int64)
	now := time.Now().Unix()
	if ttlTime > now {
		return true
	}
	m.locks.Lock(key)
	defer m.locks.Unlock(key)
	m.db.Delete(key)
	m.ttlKeys.Delete(key)
	return false
}

func (m *MemDb) SetTTL(key string, val int64) int {
	if _, ok := m.db.Get(key); !ok {
		logger.Debug("SetTTL: key not exists")
		return 0
	}

	m.ttlKeys.Set(key, val+time.Now().Unix())
	interval := time.Duration(val) * time.Second
	m.delay.Add(interval, key, func() {
		m.CheckTTL(key)
	})
	return 1
}

func (m *MemDb) DelTTL(key string) int {
	m.delay.Cancel(key)
	return m.ttlKeys.Delete(key)
}

func (m *MemDb) Stop() {
	m.delay.Stop()
}
