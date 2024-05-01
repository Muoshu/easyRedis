package memdb

import (
	"easyRedis/logger"
	"easyRedis/resp"
	"fmt"
	"strconv"
	"strings"
)

// string.go file implements the string commands of redis

func setString(m *MemDb, cmd [][]byte) resp.RedisData {
	cmdName := strings.ToLower(string(cmd[0]))
	if cmdName != "set" {
		logger.Error("setString func: cmdName != set")
		return resp.NewErrorData("Server error")
	}
	if len(cmd) < 3 {
		return resp.NewErrorData("error: commands is invalid")
	}

	m.CheckTTL(string(cmd[1])) // check ttl first. if a key is expired, the key will be deleted.

	// check option params
	var err error
	var nx, xx, get, ex, keepttl bool
	var exVal int64
	for i := 3; i < len(cmd); i++ {
		switch strings.ToLower(string(cmd[i])) {
		case "nx":
			nx = true
		case "xx":
			xx = true
		case "get":
			get = true
		case "keepttl":
			keepttl = true
		case "ex":
			ex = true
			i++
			if i >= len(cmd) {
				return resp.NewErrorData("error: commands is invalid")
			}
			exVal, err = strconv.ParseInt(string(cmd[i]), 10, 64)
			if err != nil {
				return resp.NewErrorData(fmt.Sprintf("error: commands is invalid, %s is not interger", string(cmd[i])))
			}
		default:
			return resp.NewErrorData("Error unsupported option: " + string(cmd[i]))
		}
	}

	if (nx && xx) || (ex && keepttl) {
		return resp.NewErrorData("error: commands is invalid")
	}

	// set key based on options
	key := string(cmd[1])
	m.locks.Lock(key)
	defer m.locks.Unlock(key)

	var res resp.RedisData
	oldVal, oldOk := m.db.Get(key)
	// check if the value is string
	var oldTypeVal []byte
	var typeOk bool
	if oldOk {
		oldTypeVal, typeOk = oldVal.([]byte)
		if !typeOk {
			return resp.NewErrorData("WRONGTYPE Operation against a key holding the wrong kind of value")
		}
	}

	// set key and check if it satisfies nx or xx condition
	// return the set result if the get command is not given
	if nx || xx {
		if nx {
			if !oldOk {
				m.db.Set(string(cmd[1]), cmd[2])
			} else {
				res = resp.NewBulkData(nil)
			}
		} else {
			if oldOk {
				m.db.Set(string(cmd[1]), cmd[2])
				res = resp.NewStringData("OK")
			} else {
				res = resp.NewBulkData(nil)
			}
		}
	} else {
		m.db.Set(string(cmd[1]), cmd[2])
		res = resp.NewStringData("OK")
	}

	// If a get command offered, return GET result
	if get {
		if !oldOk {
			res = resp.NewBulkData(nil)
		} else {
			res = resp.NewBulkData(oldTypeVal)
		}
	}

	// set ttl after key is existed
	if !keepttl {
		m.DelTTL(string(cmd[1]))
	}

	if ex {
		m.SetTTL(string(cmd[1]), exVal)
	}

	return res
}

func getString(m *MemDb, cmd [][]byte) resp.RedisData {
	if strings.ToLower(string(cmd[0])) != "get" {
		logger.Error("getString func: cmdName is not get")
		return resp.NewErrorData("Server error")
	}
	if len(cmd) != 2 {
		return resp.NewErrorData("error : ERR wrong number of arguments for 'get' command")
	}
	key := string(cmd[1])
	m.locks.RLock(key)
	defer m.locks.RUnlock(key)

	val, ok := m.db.Get(key)
	if !ok {
		return resp.NewBulkData(nil)
	}
	byteVal, ok := val.([]byte)
	if !ok {
		return resp.NewErrorData("WRONGTYPE operation against a key holding the wrong kind of value")
	}
	return resp.NewBulkData(byteVal)
}

func getRangeString(m *MemDb, cmd [][]byte) resp.RedisData {
	if strings.ToLower(string(cmd[0])) != "getrange" {
		logger.Error("getRangeString function: cmdName is not getrange")
		return resp.NewErrorData("server error")
	}
	if len(cmd) != 4 {
		return resp.NewErrorData("error: command is invalid")
	}

	key := string(cmd[1])
	m.locks.RLock(key)
	defer m.locks.RUnlock(key)

	val, ok := m.db.Get(key)
	if !ok {
		return resp.NewBulkData(nil)
	}
	byteVal, ok := val.([]byte)
	if !ok {
		return resp.NewErrorData("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	start, err1 := strconv.Atoi(string(cmd[2]))
	end, err2 := strconv.Atoi(string(cmd[3]))
	if err1 != nil || err2 != nil {
		return resp.NewErrorData("error: ERR value is not an integer or out of range")
	}

	if start < 0 {
		start = len(byteVal) + start
	}
	if end < 0 {
		end = len(byteVal) + end
	}
	end = end + 1

	if start > end || start >= len(byteVal) || end < 0 {
		return resp.NewBulkData([]byte{})
	}
	if start < 0 {
		start = 0
	}
	return resp.NewBulkData(byteVal[start:end])
}

func setRangeString(m *MemDb, cmd [][]byte) resp.RedisData {
	if strings.ToLower(string(cmd[0])) != "setrange" {
		logger.Error("setRangeString func: cmdName is not setrange")
		return resp.NewErrorData("Server error")
	}
	if len(cmd) != 4 {
		return resp.NewErrorData("error: command is invalid")
	}

	offset, err := strconv.Atoi(string(cmd[2]))
	if err != nil || offset < 0 {
		return resp.NewErrorData("error: offset is not a integer or less than 0")
	}
	var oldVal, newVal []byte
	key := string(cmd[1])
	// check ttl first. if a key is expired, the key will be deleted.

	m.locks.Lock(key)
	defer m.locks.Unlock(key)

	val, ok := m.db.Get(key)
	if !ok {
		oldVal = make([]byte, 0)
	} else {
		oldVal, ok = val.([]byte)
		if !ok {
			return resp.NewErrorData("WRONGTYPE Operation against a key holding the wrong kind of value")
		}
	}
	if offset > len(oldVal) {
		newVal = oldVal
		for i := 0; i < offset-len(oldVal); i++ {
			newVal = append(newVal, byte(0))
		}
		newVal = append(newVal, cmd[3]...)
	} else {
		newVal = oldVal[:offset]
		newVal = append(newVal, cmd[3]...)
	}
	m.db.Set(key, newVal)
	return resp.NewIntData(int64(len(newVal)))
}

func mGetString(m *MemDb, cmd [][]byte) resp.RedisData {
	if strings.ToLower(string(cmd[0])) != "mget" {
		logger.Error("mGetString function: cmdName is not mget")
		return resp.NewErrorData("server error")
	}
	if len(cmd) < 2 {
		return resp.NewErrorData("error: command is invalid")
	}
	res := make([]resp.RedisData, 0)
	for i := 1; i < len(cmd); i++ {
		key := string(cmd[i])

		m.locks.RLock(key)
		val, ok := m.db.Get(key)
		m.locks.RUnlock(key)
		if !ok {
			res = append(res, resp.NewBulkData(nil))
		} else {
			byteVal, ok := val.([]byte)
			if !ok {
				res = append(res, resp.NewBulkData(nil))
			} else {
				res = append(res, resp.NewBulkData(byteVal))
			}
		}
	}
	return resp.NewArrayData(res)
}

func mSetString(m *MemDb, cmd [][]byte) resp.RedisData {
	if strings.ToLower(string(cmd[0])) != "mset" {
		logger.Error("mSetString func: cmdName != mset")
		return resp.NewErrorData("Server error")
	}
	if len(cmd) < 3 || len(cmd)&1 != 1 { //判断参数个数是否是奇数
		return resp.NewErrorData("error: command is invalid")
	}
	keys := make([]string, 0)
	vals := make([][]byte, 0)

	for i := 1; i < len(cmd); i += 2 {
		keys = append(keys, string(cmd[i]))
		vals = append(vals, cmd[i+1])
	}

	//lock all key for atomicity
	m.locks.LockMulti(keys)
	defer m.locks.UnlockMulti(keys)
	for i := 0; i < len(keys); i++ {
		m.DelTTL(keys[i])
		m.db.Set(keys[i], vals[i])
	}
	return resp.NewStringData("OK")
}

func setExString(m *MemDb, cmd [][]byte) resp.RedisData {
	if strings.ToLower(string(cmd[0])) != "setex" {
		logger.Error("setExString func: cmdName is not setex")
		return resp.NewErrorData("Server error")
	}

	if len(cmd) != 4 {
		return resp.NewErrorData("error: command is invalid")
	}

	ex, err := strconv.ParseInt(string(cmd[2]), 10, 64)
	if err != nil {
		return resp.NewErrorData(fmt.Sprintf("error: %s is not a integer", string(cmd[2])))
	}
	ttl := ex
	key := string(cmd[1])
	val := cmd[3]

	m.locks.Lock(key)
	defer m.locks.Unlock(key)
	m.db.Set(key, val)
	m.SetTTL(key, ttl)
	return resp.NewStringData("OK")
}

func setNxString(m *MemDb, cmd [][]byte) resp.RedisData {
	if strings.ToLower(string(cmd[0])) != "setnx" {
		logger.Error("setNxString func: cmdName != setnx")
		return resp.NewErrorData("Server error")
	}
	if len(cmd) != 3 {
		return resp.NewErrorData("error: command is invalid")
	}
	key := string(cmd[1])
	val := cmd[2]

	m.locks.Lock(key)
	defer m.locks.Unlock(key)
	res := m.db.SetIfNotExist(key, val)

	return resp.NewIntData(int64(res))
}

func strLenString(m *MemDb, cmd [][]byte) resp.RedisData {
	if strings.ToLower(string(cmd[0])) != "strlen" {
		logger.Error("strLenString func: cmdName is not strlen")
		return resp.NewErrorData("Server error")
	}
	if len(cmd) != 2 {
		return resp.NewErrorData("error: command is invalid")
	}
	key := string(cmd[1])
	m.CheckTTL(key)
	m.locks.RLock(key)
	defer m.locks.RUnlock(key)

	val, ok := m.db.Get(key)
	if !ok {
		return resp.NewIntData(0)
	}
	typeVal, ok := val.([]byte)
	if !ok {
		return resp.NewErrorData("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	return resp.NewIntData(int64(len(typeVal)))
}

func incrString(m *MemDb, cmd [][]byte) resp.RedisData {
	if strings.ToLower(string(cmd[0])) != "incr" {
		logger.Error("incrString func: cmdName != incr")
		return resp.NewErrorData("Server error")
	}
	if len(cmd) != 2 {
		return resp.NewErrorData("error: command is invalid")
	}

	key := string(cmd[1])
	m.locks.Lock(key)
	defer m.locks.Unlock(key)

	val, ok := m.db.Get(key)
	if !ok {
		m.db.Set(key, []byte("1"))
		return resp.NewIntData(1)
	}
	typeVal, ok := val.([]byte)
	if !ok {
		return resp.NewErrorData("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	intVal, err := strconv.ParseInt(string(typeVal), 10, 64)
	if err != nil {
		return resp.NewErrorData("value is not a integer")
	}
	intVal++
	m.db.Set(key, []byte(strconv.FormatInt(intVal, 10)))
	return resp.NewIntData(intVal)
}

func incrByString(m *MemDb, cmd [][]byte) resp.RedisData {
	if strings.ToLower(string(cmd[0])) != "incrby" {
		logger.Error("incrByString func: cmdName != incrby")
		return resp.NewErrorData("Server error")
	}
	if len(cmd) != 3 {
		return resp.NewErrorData("error: command is invalid")
	}

	inc, err := strconv.ParseInt(string(cmd[2]), 10, 64)
	if err != nil {
		return resp.NewErrorData("commands invalid: increment value is not an integer")
	}

	key := string(cmd[1])
	m.locks.Lock(key)
	defer m.locks.Unlock(key)

	val, ok := m.db.Get(key)
	if !ok {
		m.db.Set(key, []byte(strconv.FormatInt(inc, 10)))
		return resp.NewIntData(inc)
	}
	typeVal, ok := val.([]byte)
	if !ok {
		return resp.NewErrorData("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	invVal, err := strconv.ParseInt(string(typeVal), 10, 64)
	if err != nil {
		return resp.NewErrorData("value is not a integer")
	}
	invVal += inc
	m.db.Set(key, []byte(strconv.FormatInt(invVal, 10)))
	return resp.NewIntData(invVal)
}

func decrString(m *MemDb, cmd [][]byte) resp.RedisData {
	if strings.ToLower(string(cmd[0])) != "decr" {
		logger.Error("decrString func: cmdName != decr")
		return resp.NewErrorData("Server error")
	}
	if len(cmd) != 2 {
		return resp.NewErrorData("error: commands is invalid")
	}

	key := string(cmd[1])
	m.locks.Lock(key)
	defer m.locks.Unlock(key)

	val, ok := m.db.Get(key)
	if !ok {
		m.db.Set(key, []byte("-1"))
		return resp.NewIntData(-1)
	}
	typeVal, ok := val.([]byte)
	if !ok {
		return resp.NewErrorData("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	intVal, err := strconv.ParseInt(string(typeVal), 10, 64)
	if err != nil {
		return resp.NewErrorData("value is not an integer")
	}
	intVal--
	m.db.Set(key, []byte(strconv.FormatInt(intVal, 10)))
	return resp.NewIntData(intVal)
}

func decrByString(m *MemDb, cmd [][]byte) resp.RedisData {
	if strings.ToLower(string(cmd[0])) != "decrby" {
		logger.Error("decrByString func: cmdName != decrby")
		return resp.NewErrorData("Server error")
	}
	if len(cmd) != 3 {
		return resp.NewErrorData("error: commands is invalid")
	}

	dec, err := strconv.ParseInt(string(cmd[2]), 10, 64)
	if err != nil {
		return resp.NewErrorData("commands invalid: increment value is not an integer")
	}

	key := string(cmd[1])
	m.locks.Lock(key)
	defer m.locks.Unlock(key)

	val, ok := m.db.Get(key)
	if !ok {
		m.db.Set(key, []byte(strconv.FormatInt(-dec, 10)))
		return resp.NewIntData(-dec)
	}
	typeVal, ok := val.([]byte)
	if !ok {
		return resp.NewErrorData("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	intVal, err := strconv.ParseInt(string(typeVal), 10, 64)
	intVal -= dec
	m.db.Set(key, []byte(strconv.FormatInt(intVal, 10)))
	return resp.NewIntData(intVal)
}

func incrByFloatString(m *MemDb, cmd [][]byte) resp.RedisData {
	if strings.ToLower(string(cmd[0])) != "incrbyfloat" {
		logger.Error("incrByFloatString func: cmdName != incrbyfloat")
		return resp.NewErrorData("Server error")
	}
	if len(cmd) != 3 {
		return resp.NewErrorData("error: commands is invalid")
	}

	inc, err := strconv.ParseFloat(string(cmd[2]), 64)
	if err != nil {
		return resp.NewErrorData("commands invalid: increment value is not an float")
	}

	key := string(cmd[1])
	m.locks.Lock(key)
	defer m.locks.Unlock(key)

	val, ok := m.db.Get(key)
	if !ok {
		m.db.Set(key, []byte(strconv.FormatFloat(inc, 'f', -1, 64)))
		return resp.NewBulkData([]byte(strconv.FormatFloat(inc, 'f', -1, 64)))
	}
	typeVal, ok := val.([]byte)
	if !ok {
		return resp.NewErrorData("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	floatVal, err := strconv.ParseFloat(string(typeVal), 64)
	if err != nil {
		return resp.NewErrorData("value is not an float")
	}
	floatVal += inc
	m.db.Set(key, []byte(strconv.FormatFloat(floatVal, 'f', -1, 64)))
	return resp.NewBulkData([]byte(strconv.FormatFloat(floatVal, 'f', -1, 64)))
}

func appendString(m *MemDb, cmd [][]byte) resp.RedisData {
	if strings.ToLower(string(cmd[0])) != "append" {
		logger.Error("appendString func: cmdName != append")
		return resp.NewErrorData("Server error")
	}
	if len(cmd) != 3 {
		return resp.NewErrorData("error: command is invalid")
	}

	key := string(cmd[1])
	val := cmd[2]
	m.locks.Lock(key)
	defer m.locks.Unlock(key)

	oldVal, ok := m.db.Get(key)
	if !ok {
		m.db.Set(key, val)
		return resp.NewIntData(int64(len(val)))
	}
	typeVal, ok := oldVal.([]byte)
	if !ok {
		return resp.NewErrorData("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	newVal := append(typeVal, val...)
	m.db.Set(key, newVal)
	return resp.NewIntData(int64(len(newVal)))
}

func RegisterStringCommands() {
	RegisterCommand("set", setString)
	RegisterCommand("get", getString)
	RegisterCommand("getrange", getRangeString)
	RegisterCommand("setrange", setRangeString)
	RegisterCommand("mget", mGetString)
	RegisterCommand("mset", mSetString)
	RegisterCommand("setex", setExString)
	RegisterCommand("setnx", setNxString)
	RegisterCommand("strlen", strLenString)
	RegisterCommand("incr", incrString)
	RegisterCommand("incrby", incrByString)
	RegisterCommand("decr", decrString)
	RegisterCommand("decrby", decrByString)
	RegisterCommand("incrbyfloat", incrByFloatString)
	RegisterCommand("append", appendString)
}
