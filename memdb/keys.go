package memdb

import (
	"easyRedis/datastructure"
	"easyRedis/logger"
	"easyRedis/resp"
	"easyRedis/util"
	"fmt"
	"strconv"
	"strings"
	"time"
)

func delKey(m *MemDb, cmd [][]byte) resp.RedisData {
	cmdName := strings.ToLower(string(cmd[0]))
	if cmdName != "del" {
		logger.Error("delKey function: cmdName is not del")
		return resp.NewErrorData("protocol error: cmdName is not del")
	}
	if len(cmd) < 2 {
		return resp.NewErrorData("error: ERR wrong number of arguments for 'del' command")
	}

	dKey := 0
	for _, key := range cmd[1:] {
		k := string(key)
		m.locks.Lock(k)
		dKey += m.db.Delete(k)
		m.ttlKeys.Delete(k)
		m.locks.Unlock(k)
	}
	return resp.NewIntData(int64(dKey))
}

func existsKey(m *MemDb, cmd [][]byte) resp.RedisData {
	cmdName := strings.ToLower(string(cmd[0]))
	if cmdName != "exists" {
		logger.Error("existsKey Function: cmdName is not exists")
		return resp.NewErrorData("protocol error: cmdName is not exists")
	}
	if len(cmd) < 2 {
		return resp.NewErrorData("error: ERR wrong number of arguments for 'exists' command")
	}
	eKey := 0
	var key string
	for _, keyByte := range cmd[1:] {
		key = string(keyByte)
		if m.CheckTTL(key) {
			m.locks.RLock(key)
			if _, ok := m.db.Get(key); ok {
				eKey++
			}
			m.locks.RUnlock(key)
		}
	}
	return resp.NewIntData(int64(eKey))
}

func keysKey(m *MemDb, cmd [][]byte) resp.RedisData {
	if strings.ToLower(string(cmd[0])) != "keys" || len(cmd) != 2 {
		logger.Error("keysKey function: cmdName is not keys or cmd length is not 2")
		return resp.NewErrorData(fmt.Sprintf("error: keys function get invalid command %s %s", string(cmd[0]), string(cmd[1])))
	}
	res := make([]resp.RedisData, 0)
	allKeys := m.db.Keys()
	pattern := string(cmd[1])
	convertedPattern, err := util.CompilePattern(pattern)
	if err != nil {
		return resp.NewArrayData(res)
	}
	for _, key := range allKeys {
		if m.CheckTTL(key) {
			if ok := convertedPattern.IsMatch(key); ok {
				res = append(res, resp.NewBulkData([]byte(key)))
			}
		}
	}
	return resp.NewArrayData(res)
}

func expireKey(m *MemDb, cmd [][]byte) resp.RedisData {
	cmdName := strings.ToLower(string(cmd[0]))
	if cmdName != "expire" || len(cmd) < 3 || len(cmd) > 4 {
		logger.Error("expireKey Function: cmdName is not expire or command args number is invalid")
		return resp.NewErrorData("error: cmdName is not expire or command args number is invalid")
	}
	v, err := strconv.ParseInt(string(cmd[2]), 10, 64)
	if err != nil {
		return resp.NewErrorData("error: cmdName is not expire or command args number is invalid")
	}
	ttl := v
	var opt string
	if len(cmd) == 4 {
		opt = strings.ToLower(string(cmd[3]))
	}
	key := string(cmd[1])
	if !m.CheckTTL(key) {
		return resp.NewIntData(int64(0))
	}
	m.locks.Lock(key)
	defer m.locks.Unlock(key)
	var res int
	switch opt {
	case "nx":
		if _, ok := m.ttlKeys.Get(key); !ok {
			res = m.SetTTL(key, ttl)
		}
	case "xx":
		if _, ok := m.ttlKeys.Get(key); ok {
			res = m.SetTTL(key, ttl)
		}
	case "gt":
		if v, ok := m.ttlKeys.Get(key); ok && ttl > v.(int64) {
			res = m.SetTTL(key, ttl)
		}
	case "lt":
		if v, ok := m.ttlKeys.Get(key); ok && ttl < v.(int64) {
			res = m.SetTTL(key, ttl)
		}
	default:
		if opt != "" {
			logger.Error("expireKey Function: opt %s is not nx, xx, gt or lt", opt)
			return resp.NewErrorData(fmt.Sprintf("error: unsupport %s, except nx, xx, gt, lt", opt))
		}
		res = m.SetTTL(key, ttl)
	}
	return resp.NewIntData(int64(res))
}

func persistKey(m *MemDb, cmd [][]byte) resp.RedisData {
	cmdName := strings.ToLower(string(cmd[0]))
	if cmdName != "persist" || len(cmd) != 2 {
		logger.Error("persistKey function: cmdName is not persist or command args number is invalid")
		return resp.NewErrorData("error: cmdName is not persist or command args number is invalid")
	}
	key := string(cmd[1])
	if !m.CheckTTL(key) {
		return resp.NewIntData(int64(0))
	}
	m.locks.Lock(key)
	defer m.locks.Unlock(key)

	res := m.DelTTL(key)
	return resp.NewIntData(int64(res))
}

func ttlKey(m *MemDb, cmd [][]byte) resp.RedisData {
	cmdName := strings.ToLower(string(cmd[0]))
	if cmdName != "ttl" || len(cmd) != 2 {
		logger.Error("ttlKey error: cmdName is not ttl or command args number is not 2")
		return resp.NewErrorData("error: cmdName is not ttl or command args number is not 2")
	}
	key := string(cmd[1])
	//过期了
	if !m.CheckTTL(key) {
		return resp.NewIntData(int64(-2))
	}
	m.locks.RLock(key)
	defer m.locks.RUnlock(key)

	//不存在
	if _, ok := m.db.Get(key); !ok {
		return resp.NewIntData(int64(-2))
	}
	//在db数据库中，但是不在ttl数据库中
	ttl, ok := m.ttlKeys.Get(key)
	if !ok {
		return resp.NewIntData(int64(-1))
	}
	now := time.Now().Unix()
	return resp.NewIntData(ttl.(int64) - now)
}

func typeKey(m *MemDb, cmd [][]byte) resp.RedisData {
	cmdName := strings.ToLower(string(cmd[0]))
	if cmdName != "type" || len(cmd) != 2 {
		logger.Error("typeKey Function: cmdName is not type or command args number is invalid")
		return resp.NewErrorData("error: cmdName is not type or command args number is invalid")
	}
	key := string(cmd[1])
	if !m.CheckTTL(key) {
		return resp.NewStringData("none")
	}
	m.locks.RLock(key)
	defer m.locks.RUnlock(key)

	v, ok := m.db.Get(key)
	if !ok {
		resp.NewStringData("none")
	}
	switch v.(type) {
	case []byte:
		return resp.NewStringData("string")
	case *datastructure.List:
		return resp.NewStringData("list")
	case *datastructure.Hash:
		return resp.NewStringData("hash")
	case *datastructure.Set:
		return resp.NewStringData("set")
	case *datastructure.SortSet:
		return resp.NewStringData("sortSet")
	default:
		logger.Error("keyType function: type fun error, not in string|set|list|hash")
	}
	return resp.NewErrorData("unknown error: server error")
}

func renameKey(m *MemDb, cmd [][]byte) resp.RedisData {
	cmdName := strings.ToLower(string(cmd[0]))
	if cmdName != "rename" || len(cmd) != 3 {
		logger.Error("renameKey Function: cmdName is not rename or command args number is invalid")
		return resp.NewErrorData("error: cmdName is not rename or command args number is invalid")
	}
	oldName, newName := string(cmd[1]), string(cmd[2])
	if !m.CheckTTL(oldName) {
		return resp.NewErrorData(fmt.Sprintf("error: %s not exist", oldName))
	}

	m.locks.LockMulti([]string{oldName, newName})
	defer m.locks.UnlockMulti([]string{oldName, newName})
	oldVal, ok := m.db.Get(oldName)
	if !ok {
		return resp.NewErrorData(fmt.Sprintf("error: %s not exist", oldName))
	}
	m.db.Delete(oldName)
	m.ttlKeys.Delete(oldName)
	m.db.Delete(newName)
	m.ttlKeys.Delete(newName)
	m.db.Set(newName, oldVal)
	ttl, ok := m.ttlKeys.Get(oldName)
	if ok {
		m.ttlKeys.Set(newName, ttl.(int64))
	}

	return resp.NewStringData("OK")
}

func pingKeys(m *MemDb, cmd [][]byte) resp.RedisData {
	cmdName := string(cmd[0])
	if strings.ToLower(cmdName) != "ping" {
		logger.Error("pingKeys Function: cmdName is not ping")
		return resp.NewErrorData("server error")
	}

	if len(cmd) > 2 {
		return resp.NewErrorData("error: command args number is invalid")
	}

	if len(cmd) == 1 {
		return resp.NewStringData("PONG")
	}

	return resp.NewBulkData(cmd[1])
}

func RegisterKeyCommands() {
	RegisterCommand("ping", pingKeys)
	RegisterCommand("del", delKey)
	RegisterCommand("exists", existsKey)
	RegisterCommand("keys", keysKey)
	RegisterCommand("expire", expireKey)
	RegisterCommand("persist", persistKey)
	RegisterCommand("ttl", ttlKey)
	RegisterCommand("type", typeKey)
	RegisterCommand("rename", renameKey)
}
