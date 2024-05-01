package memdb

import (
	"easyRedis/datastructure"
	"easyRedis/logger"
	"easyRedis/resp"
	"math"
	"strconv"
	"strings"
)

func sAddSet(m *MemDb, cmd [][]byte) resp.RedisData {
	if strings.ToLower(string(cmd[0])) != "sadd" {
		logger.Error("sAddSet Function: cmdName is not sadd")
		return resp.NewErrorData("server error")
	}

	if len(cmd) < 3 {
		return resp.NewErrorData("wrong number of arguments for 'sadd' command")
	}

	key := string(cmd[1])
	m.locks.Lock(key)
	defer m.locks.Unlock(key)

	temp, ok := m.db.Get(key)
	if !ok {
		temp = datastructure.NewSet()
		m.db.Set(key, temp)
	}
	sets, ok := temp.(*datastructure.Set)
	if !ok {
		return resp.NewErrorData("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	res := 0
	for i := 2; i < len(cmd); i++ {
		res += sets.Add(string(cmd[i]))
	}
	return resp.NewIntData(int64(res))
}

func sCardSet(m *MemDb, cmd [][]byte) resp.RedisData {
	if strings.ToLower(string(cmd[0])) != "scard" {
		logger.Error("sCardSet Function: cmdName is not scard")
		return resp.NewErrorData("server error")
	}

	if len(cmd) != 2 {
		return resp.NewErrorData("wrong number of arguments for 'scard' command")
	}
	key := string(cmd[1])
	m.locks.RLock(key)
	defer m.locks.RUnlock(key)
	temp, ok := m.db.Get(key)
	if !ok {
		return resp.NewIntData(0)
	}
	sets, ok := temp.(*datastructure.Set)
	if !ok {
		return resp.NewErrorData("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	res := sets.Len()
	return resp.NewIntData(int64(res))
}

func sDiffSet(m *MemDb, cmd [][]byte) resp.RedisData {
	if strings.ToLower(string(cmd[0])) != "sdiff" {
		logger.Error("sDiffSet Function: cmdName is not sdiff")
		return resp.NewErrorData("server error")
	}

	if len(cmd) < 2 {
		return resp.NewErrorData("wrong number of arguments for 'sdiff' command")
	}

	keys := make([]string, 0, len(cmd)-1)
	for i := 1; i < len(cmd); i++ {
		keys = append(keys, string(cmd[i]))
	}

	m.locks.RLockMulti(keys)
	defer m.locks.RUnlockMulti(keys)
	temp, ok := m.db.Get(keys[0])
	if !ok {
		return resp.NewArrayData(nil)
	}
	primSet, ok := temp.(*datastructure.Set)
	if !ok {
		return resp.NewErrorData("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	res := make([]resp.RedisData, 0)
	// if cmd has no other keys, return the first key member
	if len(keys) == 1 {
		members := primSet.Member()
		for _, member := range members {
			res = append(res, resp.NewBulkData([]byte(member)))
		}
		return resp.NewArrayData(res)
	}
	setSlice := make([]*datastructure.Set, 0)
	for i := 1; i < len(keys); i++ {
		temp, ok = m.db.Get(keys[i])
		if ok {
			set, ok := temp.(*datastructure.Set)
			if !ok {
				return resp.NewErrorData("WRONGTYPE Operation against a key holding the wrong kind of value")
			}
			setSlice = append(setSlice, set)
		}
	}
	diffSet := primSet.Difference(setSlice...)
	for _, member := range diffSet.Member() {
		res = append(res, resp.NewBulkData([]byte(member)))
	}
	return resp.NewArrayData(res)
}

func sDiffStoreSet(m *MemDb, cmd [][]byte) resp.RedisData {
	if strings.ToLower(string(cmd[0])) != "sdiffstore" {
		logger.Error("sDiffStoreSet Function: cmdName is not sdiffstore")
		return resp.NewErrorData("server error")
	}
	if len(cmd) < 3 {
		return resp.NewErrorData("wrong number of arguments for 'sdiffstore' command")
	}

	desKey := string(cmd[1])
	// first, get the difference set
	keys := make([]string, 0)
	for i := 2; i < len(cmd); i++ {
		key := string(cmd[i])
		keys = append(keys, key)

	}

	var diffRes *datastructure.Set
	// Don't forget Unlock,最好不用defer,因为defer最晚要到当前函数执行完之前才释放锁
	m.locks.RLockMulti(keys)
	temp, ok := m.db.Get(keys[0])

	if !ok {
		// 先释放keys，然后在对desKey加锁，因为keys可能包含desKey
		m.locks.RUnlockMulti(keys)
		diffRes = datastructure.NewSet()
		m.locks.Lock(desKey)
		m.db.Set(desKey, diffRes)
		m.locks.Unlock(desKey)
		return resp.NewIntData(0)
	} else {
		primSet, ok := temp.(*datastructure.Set)
		if !ok {
			m.locks.RUnlockMulti(keys)
			return resp.NewErrorData("WRONGTYPE Operation against a key holding the wrong kind of value")
		}
		setSlice := make([]*datastructure.Set, 0)
		for i := 1; i < len(keys); i++ {
			temp, ok = m.db.Get(keys[i])
			if ok {
				set, ok := temp.(*datastructure.Set)
				if !ok {
					m.locks.RUnlockMulti(keys)
					return resp.NewErrorData("WRONGTYPE Operation against a key holding the wrong kind of value")
				}
				setSlice = append(setSlice, set)
			}
		}
		diffRes = primSet.Difference(setSlice...)
	}
	m.locks.RUnlockMulti(keys)
	m.locks.Lock(desKey)
	defer m.locks.Unlock(desKey)
	// have to check again, because the key may be set by other goroutine
	temp, ok = m.db.Get(desKey)
	if ok {
		_, ok = temp.(*datastructure.Set)
		if !ok {
			return resp.NewErrorData("WRONGTYPE Operation against a key holding the wrong kind of value")
		}
	}
	if diffRes.Len() != 0 {
		m.db.Set(desKey, diffRes)
	}
	return resp.NewIntData(int64(diffRes.Len()))
}

func sInterSet(m *MemDb, cmd [][]byte) resp.RedisData {
	if strings.ToLower(string(cmd[0])) != "sinter" {
		logger.Error("sInterSet Function: cmdName is not sinter")
		return resp.NewErrorData("server error")
	}

	if len(cmd) < 2 {
		return resp.NewErrorData("wrong number of arguments for 'sinter' command")
	}

	keys := make([]string, 0, len(cmd)-1)
	for i := 1; i < len(cmd); i++ {
		keys = append(keys, string(cmd[i]))
	}

	m.locks.RLockMulti(keys)
	defer m.locks.RUnlockMulti(keys)

	// find the shortest set as the primary set to decrease the time complexity
	sets := make([]*datastructure.Set, 0)
	shortestSet := 0
	shortestLen := math.MaxInt

	for _, key := range keys {
		temp, ok := m.db.Get(key)
		if ok {
			set, ok := temp.(*datastructure.Set)
			if !ok {
				return resp.NewErrorData("WRONGTYPE Operation against a key holding the wrong kind of value")
			}
			sets = append(sets, set)
			if set.Len() < shortestLen {
				shortestLen = set.Len()
				shortestSet = len(sets) - 1
			}
		}
	}
	primSet := sets[shortestSet]
	sets = append(sets[:shortestSet], sets[shortestSet+1:]...)
	interSet := primSet.Intersect(sets...)
	res := make([]resp.RedisData, 0)
	for _, member := range interSet.Member() {
		res = append(res, resp.NewBulkData([]byte(member)))
	}
	return resp.NewArrayData(res)
}

func sInterStoreSet(m *MemDb, cmd [][]byte) resp.RedisData {
	if strings.ToLower(string(cmd[0])) != "sinterstore" {
		logger.Error("sInterStoreSet Function: cmdName is not sinterstore")
		return resp.NewErrorData("server error")
	}
	if len(cmd) < 3 {
		return resp.NewErrorData("wrong number of arguments for 'sinterstore' command")
	}

	desKey := string(cmd[1])
	keys := make([]string, 0, len(cmd)-2)
	for i := 2; i < len(cmd); i++ {
		key := string(cmd[i])
		keys = append(keys, key)
	}
	if len(keys) == 0 {
		return resp.NewIntData(0)
	}
	m.locks.RLockMulti(keys)

	// find the shortest set as primary set to decrease the time complexity
	sets := make([]*datastructure.Set, 0)
	shortestSet := 0
	shortestLen := math.MaxInt
	for _, key := range keys {
		temp, ok := m.db.Get(key)
		if ok {
			set, ok := temp.(*datastructure.Set)
			if !ok {
				m.locks.RUnlockMulti(keys)
				return resp.NewErrorData("WRONGTYPE Operation against a key holding the wrong kind of value")
			}
			sets = append(sets, set)
			if set.Len() < shortestLen {
				shortestLen = set.Len()
				shortestSet = len(sets) - 1
			}
		}
	}
	primSet := sets[shortestSet]
	sets = append(sets[:shortestSet], sets[shortestSet+1:]...)
	interSet := primSet.Intersect(sets...)
	m.locks.RUnlockMulti(keys)

	if interSet.Len() != 0 {
		m.locks.Lock(desKey)
		m.db.Set(desKey, interSet)
		m.locks.Unlock(desKey)
	}
	return resp.NewIntData(int64(interSet.Len()))
}

func sIsMemberSet(m *MemDb, cmd [][]byte) resp.RedisData {
	if strings.ToLower(string(cmd[0])) != "sismember" {
		logger.Error("sIsMemberSet Function: cmdName is not sismember")
		return resp.NewErrorData("server error")
	}

	if len(cmd) != 3 {
		return resp.NewErrorData("wrong number of arguments for 'sismember' command")
	}
	key := string(cmd[1])
	val := string(cmd[2])

	m.locks.RLock(key)
	defer m.locks.RUnlock(key)

	temp, ok := m.db.Get(key)
	if !ok {
		return resp.NewIntData(0)
	}
	set, ok := temp.(*datastructure.Set)
	if !ok {
		return resp.NewErrorData("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	if set.Has(val) {
		return resp.NewIntData(1)
	}
	return resp.NewIntData(0)
}

func sMembersSet(m *MemDb, cmd [][]byte) resp.RedisData {
	if strings.ToLower(string(cmd[0])) != "smembers" {
		logger.Error("sMembersSet Function: cmdName is not smembers")
		return resp.NewErrorData("server error")
	}
	if len(cmd) != 2 {
		return resp.NewErrorData("wrong number of arguments for 'smembers' command")
	}
	key := string(cmd[1])
	m.locks.RLock(key)
	defer m.locks.RUnlock(key)

	temp, ok := m.db.Get(key)
	if !ok {
		return resp.NewArrayData(nil)
	}
	set, ok := temp.(*datastructure.Set)
	if !ok {
		return resp.NewErrorData("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	members := set.Member()
	res := make([]resp.RedisData, len(members))
	for i := 0; i < len(members); i++ {
		res[i] = resp.NewBulkData([]byte(members[i]))
	}
	return resp.NewArrayData(res)
}

func sMoveSet(m *MemDb, cmd [][]byte) resp.RedisData {
	if strings.ToLower(string(cmd[0])) != "smove" {
		logger.Error("sMoveSet Function: cmdName is not smove")
		return resp.NewErrorData("server error")
	}

	if len(cmd) != 4 {
		return resp.NewErrorData("wrong number of arguments for 'smove' command")
	}
	srcKey := string(cmd[1])
	desKey := string(cmd[2])
	val := string(cmd[3])
	keys := []string{srcKey, desKey}

	m.locks.LockMulti(keys)
	defer m.locks.UnlockMulti(keys)

	temp, ok := m.db.Get(srcKey)
	if !ok {
		return resp.NewIntData(0)
	}
	srcSet, ok := temp.(*datastructure.Set)
	if !ok {
		return resp.NewErrorData("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	var desSet *datastructure.Set
	var desExist bool

	temp, ok = m.db.Get(desKey)
	if !ok {
		desSet = datastructure.NewSet()
		desExist = false
	} else {
		desSet, ok = temp.(*datastructure.Set)
		if !ok {
			return resp.NewErrorData("WRONGTYPE Operation against a key holding the wrong kind of value")
		}
		desExist = true
	}
	res := srcSet.Remove(val)
	if res == 0 {
		return resp.NewIntData(0)
	}
	desSet.Add(val)
	if !desExist {
		m.db.Set(desKey, desSet)
	}
	return resp.NewIntData(1)
}

func sPopSet(m *MemDb, cmd [][]byte) resp.RedisData {
	if strings.ToLower(string(cmd[0])) != "spop" {
		logger.Error("sPopSet Function: cmdName is not spop")
		return resp.NewErrorData("server error")
	}

	if len(cmd) != 2 && len(cmd) != 3 {
		return resp.NewErrorData("wrong number of arguments for 'spop' command")
	}
	var count int
	var err error
	if len(cmd) == 3 {
		count, err = strconv.Atoi(string(cmd[2]))
		if err != nil {
			return resp.NewErrorData("count value must be a positive integer")
		}
	} else {
		count = 1
	}

	key := string(cmd[1])
	m.locks.Lock(key)
	defer m.locks.Unlock(key)
	temp, ok := m.db.Get(key)

	if !ok {
		return resp.NewBulkData(nil)
	}
	set, ok := temp.(*datastructure.Set)
	if !ok {
		return resp.NewErrorData("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	if count <= 0 {
		return resp.NewArrayData(nil)
	}
	defer func() {
		if set.Len() == 0 {
			m.db.Delete(key)
			m.DelTTL(key)
		}
	}()

	res := make([]resp.RedisData, 0)
	if count == 1 {
		val := set.Pop()
		return resp.NewBulkData([]byte(val))
	} else {
		for i := 0; i < count; i++ {
			val := set.Pop()
			if val == "" {
				break
			}
			res = append(res, resp.NewBulkData([]byte(val)))
		}
	}
	return resp.NewArrayData(res)
}

func sRandMemberSet(m *MemDb, cmd [][]byte) resp.RedisData {
	if strings.ToLower(string(cmd[0])) != "srandmember" {
		logger.Error("sRandMemberSet Function: cmdName is not srandmember")
		return resp.NewErrorData("server error")
	}

	if len(cmd) != 2 && len(cmd) != 3 {
		return resp.NewErrorData("wrong number of arguments for 'srandmember' command")
	}

	var count int
	var err error
	if len(cmd) == 3 {
		count, err = strconv.Atoi(string(cmd[2]))
		if err != nil {
			return resp.NewErrorData("count must be an integer")
		}
	} else {
		count = 1
	}
	key := string(cmd[1])
	m.locks.RLock(key)
	defer m.locks.RUnlock(key)

	temp, ok := m.db.Get(key)
	if !ok {
		return resp.NewBulkData(nil)
	}
	set, ok := temp.(*datastructure.Set)
	if !ok {
		return resp.NewErrorData("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	resMembers := set.Random(count)
	if len(resMembers) == 0 {
		return resp.NewBulkData(nil)
	}
	res := make([]resp.RedisData, len(resMembers))
	for i := 0; i < len(resMembers); i++ {
		res[i] = resp.NewBulkData([]byte(resMembers[i]))
	}
	return resp.NewArrayData(res)

}

func sRemSet(m *MemDb, cmd [][]byte) resp.RedisData {
	if strings.ToLower(string(cmd[0])) != "srem" {
		logger.Error("sRemSet Function: cmdName is not srem")
		return resp.NewErrorData("server error")
	}

	if len(cmd) < 3 {
		return resp.NewErrorData("wrong number of arguments for 'srem' command")
	}
	key := string(cmd[1])
	m.locks.Lock(key)
	defer m.locks.Unlock(key)

	temp, ok := m.db.Get(key)
	if !ok {
		return resp.NewIntData(0)
	}
	set, ok := temp.(*datastructure.Set)
	if !ok {
		return resp.NewErrorData("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	defer func() {
		if set.Len() == 0 {
			m.db.Delete(key)
			m.DelTTL(key)
		}
	}()

	res := 0
	for i := 2; i < len(cmd); i++ {
		member := string(cmd[i])
		res += set.Remove(member)
	}
	return resp.NewIntData(int64(res))
}

func sUnionSet(m *MemDb, cmd [][]byte) resp.RedisData {
	if strings.ToLower(string(cmd[0])) != "sunion" {
		logger.Error("sUnionSet Function: cmdName is not sunion")
		return resp.NewErrorData("server error")
	}

	if len(cmd) < 2 {
		return resp.NewErrorData("wrong number of arguments for 'sunion' command")
	}
	keys := make([]string, 0)
	for i := 1; i < len(cmd); i++ {
		key := string(cmd[i])
		keys = append(keys, key)

	}
	m.locks.RLockMulti(keys)
	defer m.locks.RUnlockMulti(keys)

	sets := make([]*datastructure.Set, 0)
	for _, key := range keys {
		temp, ok := m.db.Get(key)
		if !ok {
			continue
		}
		set, ok := temp.(*datastructure.Set)
		if !ok {
			return resp.NewErrorData("WRONGTYPE Operation against a key holding the wrong kind of value")
		}
		sets = append(sets, set)
	}
	if len(sets) == 0 {
		return resp.NewArrayData(nil)
	}
	resSet := sets[0].Union(sets[1:]...)
	res := make([]resp.RedisData, 0, resSet.Len())
	for _, member := range resSet.Member() {
		res = append(res, resp.NewBulkData([]byte(member)))
	}
	return resp.NewArrayData(res)
}

func sUnionStoreSet(m *MemDb, cmd [][]byte) resp.RedisData {
	if strings.ToLower(string(cmd[0])) != "sunionstore" {
		logger.Error("sUnionStoreSet Function: cmdName is not sunionstore")
		return resp.NewErrorData("server error")
	}

	if len(cmd) < 3 {
		return resp.NewErrorData("wrong number of arguments for 'sunionstore' command")
	}

	desKey := string(cmd[1])
	keys := make([]string, 0, len(cmd)-2)
	for i := 2; i < len(cmd); i++ {
		key := string(cmd[i])
		keys = append(keys, key)

	}
	m.locks.RLockMulti(keys)
	sets := make([]*datastructure.Set, 0)
	for _, key := range keys {
		temp, ok := m.db.Get(key)
		if !ok {
			continue
		}
		set, ok := temp.(*datastructure.Set)
		if !ok {
			m.locks.RUnlockMulti(keys)
			return resp.NewErrorData("WRONGTYPE Operation against a key holding the wrong kind of value")
		}
		sets = append(sets, set)
	}
	if len(sets) == 0 {
		m.locks.RUnlockMulti(keys)
		return resp.NewArrayData(nil)
	}
	resSet := sets[0].Union(sets[1:]...)
	m.locks.RUnlockMulti(keys)

	if resSet.Len() != 0 {
		m.locks.Lock(desKey)
		m.db.Set(desKey, resSet)
		m.locks.Unlock(desKey)
	}
	return resp.NewIntData(int64(resSet.Len()))
}

func RegisterSetCommands() {
	RegisterCommand("sadd", sAddSet)
	RegisterCommand("scard", sCardSet)
	RegisterCommand("sdiff", sDiffSet)
	RegisterCommand("sdiffstore", sDiffStoreSet)
	RegisterCommand("sinter", sInterSet)
	RegisterCommand("sinterstore", sInterStoreSet)
	RegisterCommand("sismember", sIsMemberSet)
	RegisterCommand("smembers", sMembersSet)
	RegisterCommand("smove", sMoveSet)
	RegisterCommand("spop", sPopSet)
	RegisterCommand("srandmember", sRandMemberSet)
	RegisterCommand("srem", sRemSet)
	RegisterCommand("sunion", sUnionSet)
	RegisterCommand("sunionstore", sUnionStoreSet)

}
