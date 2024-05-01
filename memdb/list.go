package memdb

import (
	"bytes"
	"easyRedis/datastructure"
	"easyRedis/logger"
	"easyRedis/resp"
	"fmt"
	"strconv"
	"strings"
)

func lLenList(m *MemDb, cmd [][]byte) resp.RedisData {
	if strings.ToLower(string(cmd[0])) != "llen" {
		logger.Error("lLenList function: cmdName is not llen")
		return resp.NewErrorData("server error")
	}
	if len(cmd) != 2 {
		return resp.NewErrorData("wrong number of arguments for 'llen' command")
	}

	key := string(cmd[1])
	m.locks.RLock(key)
	defer m.locks.RUnlock(key)

	v, ok := m.db.Get(key)
	if !ok {
		return resp.NewIntData(0)
	}
	list, ok := v.(*datastructure.List)
	if !ok {
		return resp.NewErrorData("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	return resp.NewIntData(int64(list.Len))
}

func lIndexList(m *MemDb, cmd [][]byte) resp.RedisData {
	if strings.ToLower(string(cmd[0])) != "lindex" {
		logger.Error("lIndexList Function: cmdName is not lindex")
		return resp.NewErrorData("server error")
	}
	if len(cmd) != 3 {
		return resp.NewErrorData("wrong number of arguments for 'lindex' command")
	}

	index, err := strconv.Atoi(string(cmd[2]))
	if err != nil {
		return resp.NewErrorData("index is not an integer")
	}

	key := string(cmd[1])
	m.locks.RLock(key)
	defer m.locks.RUnlock(key)

	v, ok := m.db.Get(key)
	if !ok {
		return resp.NewBulkData(nil)
	}
	list, ok := v.(*datastructure.List)
	if !ok {
		return resp.NewErrorData("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	node := list.Index(index)
	if node == nil {
		return resp.NewBulkData(nil)
	}
	return resp.NewBulkData(node.Val)
}

func lPosList(m *MemDb, cmd [][]byte) resp.RedisData {
	if strings.ToLower(string(cmd[0])) != "lpos" {
		logger.Error("lPosList Function: cmdName is not lpos")
		return resp.NewErrorData("server error")
	}
	if len(cmd) < 3 || len(cmd)&1 != 1 {
		return resp.NewErrorData("wrong number of arguments for 'lpos' command")
	}

	var rank, count, maxLen, reverse bool
	var rankVal, countVal, maxLenVal int
	var key string
	var elem []byte
	var err error
	var pos int

	key = string(cmd[1])
	elem = cmd[2]

	// handle params
	for i := 3; i < len(cmd); i += 2 {
		switch strings.ToLower(string(cmd[i])) {
		case "rank":
			rank = true
			rankVal, err = strconv.Atoi(string(cmd[i+1]))
			if err != nil || rankVal == 0 {
				return resp.NewErrorData("rank value should 1,2,3... or -1,-2,-3...")
			}
		case "count":
			count = true
			countVal, err = strconv.Atoi(string(cmd[i+1]))
			if err != nil || countVal < 0 {
				return resp.NewErrorData("error: ERR COUNT can't be negative")
			}
		case "maxlen":
			maxLen = true
			maxLenVal, err = strconv.Atoi(string(cmd[i+1]))
			if err != nil || maxLenVal < 0 {
				return resp.NewErrorData("maxlen value is not an positive integer")
			}
		default:
			return resp.NewErrorData(fmt.Sprintf("unsupported option %s", string(cmd[i])))
		}
	}

	m.locks.RLock(key)
	defer m.locks.RUnlock(key)

	temp, ok := m.db.Get(key)
	if !ok {
		return resp.NewBulkData(nil)
	}
	list, ok := temp.(*datastructure.List)
	if !ok {
		return resp.NewErrorData("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	if list.Len == 0 {
		return resp.NewBulkData(nil)
	}
	if count && countVal == 0 {
		countVal = list.Len
	}
	if maxLen && maxLenVal == 0 {
		maxLenVal = list.Len
	}

	// normal pos without options
	if !rank && !count && !maxLen {
		pos := list.Pos(elem)
		if pos == -1 {
			return resp.NewBulkData(nil)
		} else {
			return resp.NewIntData(int64(pos))
		}
	}

	//handle options
	var cur *datastructure.ListNode
	if rank {
		if rankVal > 0 {
			pos = -1
			for cur = list.Head.Next; cur != list.Tail; cur = cur.Next {
				pos++
				if bytes.Equal(cur.Val, elem) {
					rankVal--
				}
				if maxLen {
					maxLenVal--
					if maxLenVal == 0 {
						break
					}
				}
				if rankVal == 0 {
					break
				}
			}
		} else {
			reverse = true
			pos = list.Len
			for cur = list.Tail.Prev; cur != list.Head; cur = cur.Prev {
				pos--
				if bytes.Equal(cur.Val, elem) {
					rankVal++
				}
				if maxLen {
					maxLenVal--
					if maxLenVal == 0 {
						break
					}
				}
				if rankVal == 0 {
					break
				}
			}
		}
	} else {
		cur = list.Head.Next
		pos = 0
		if maxLen {
			maxLenVal--
		}
	}

	// when rank is out of range, return nil
	if (rank && rankVal != 0) || cur == list.Tail || cur == list.Head {
		return resp.NewBulkData(nil)
	}
	res := make([]resp.RedisData, 0)
	if !count {
		// if count is not set, return first find pos inside maxLen range
		for ; cur != list.Tail; cur = cur.Next {
			if bytes.Equal(cur.Val, elem) {
				return resp.NewIntData(int64(pos))
			}
			pos++
			if maxLen {
				if maxLenVal <= 0 {
					break
				}
				maxLenVal--
			}
		}
		return resp.NewBulkData(nil)
	} else {
		if !reverse {
			for ; cur != list.Tail && countVal != 0; cur = cur.Next {
				if bytes.Equal(cur.Val, elem) {
					res = append(res, resp.NewIntData(int64(pos)))
					countVal--
				}
				pos++
				if maxLen {
					if maxLenVal <= 0 {
						break
					}
					maxLenVal--
				}
			}
		} else {
			for ; cur != list.Head && countVal != 0; cur = cur.Prev {
				if bytes.Equal(cur.Val, elem) {
					res = append(res, resp.NewIntData(int64(pos)))
					countVal--
				}
				pos--
				if maxLen {
					if maxLenVal <= 0 {
						break
					}
					maxLenVal--
				}
			}
		}
	}
	if len(res) == 0 {
		return resp.NewBulkData(nil)
	}
	return resp.NewArrayData(res)
}

func lPopList(m *MemDb, cmd [][]byte) resp.RedisData {
	if strings.ToLower(string(cmd[0])) != "lpop" {
		logger.Error("lPopList: command is not lpop")
		return resp.NewErrorData("server error")
	}
	if len(cmd) != 2 && len(cmd) != 3 {
		return resp.NewErrorData("wrong number of arguments for 'lpop' command")
	}
	var cnt int
	var err error
	if len(cmd) == 3 {
		cnt, err = strconv.Atoi(string(cmd[2]))
		if err != nil || cnt < 0 {
			return resp.NewErrorData("count value must be a positive integer")
		}
	}
	key := string(cmd[1])
	m.locks.Lock(key)
	defer m.locks.Unlock(key)

	temp, ok := m.db.Get(key)
	if !ok || (len(cmd) == 3 && cnt == 0) {
		return resp.NewBulkData(nil)
	}
	list, ok := temp.(*datastructure.List)
	if !ok {
		return resp.NewErrorData("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	// 当list的长度为0时删除，这是有必要的
	defer func() {
		if list.Len == 0 {
			m.db.Delete(key)
			m.DelTTL(key)
		}
	}()

	// if cnt is not set, return the first element
	if cnt == 0 && len(cmd) == 2 {
		e := list.LPop()
		if e == nil {
			return resp.NewBulkData(nil)
		}
		return resp.NewBulkData(e.Val)
	}
	// return cnt number elements as array
	res := make([]resp.RedisData, 0)
	for i := 0; i < cnt; i++ {
		e := list.LPop()
		if e == nil {
			break
		}
		res = append(res, resp.NewBulkData(e.Val))
	}
	return resp.NewArrayData(res)
}

func rPopList(m *MemDb, cmd [][]byte) resp.RedisData {
	if strings.ToLower(string(cmd[0])) != "rpop" {
		logger.Error("rPopList: command is not rpop")
		return resp.NewErrorData("server error")
	}
	if len(cmd) != 2 && len(cmd) != 3 {
		return resp.NewErrorData("wrong number of arguments for 'rpop' command")
	}

	var cnt int
	var err error

	if len(cmd) == 3 {
		cnt, err = strconv.Atoi(string(cmd[2]))
		if err != nil || cnt < 0 {
			return resp.NewErrorData("count value must be a positive integer")
		}
	}

	key := string(cmd[1])
	m.locks.Lock(key)
	defer m.locks.Unlock(key)

	temp, ok := m.db.Get(key)
	if !ok || (len(cmd) == 3 && cnt == 0) {
		return resp.NewBulkData(nil)
	}
	list, ok := temp.(*datastructure.List)
	// if cnt is not set, return last element
	if !ok {
		return resp.NewErrorData("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	defer func() {
		if list.Len == 0 {
			m.db.Delete(key)
			m.DelTTL(key)
		}
	}()
	if cnt == 0 {
		e := list.RPop()
		if e == nil {
			return resp.NewBulkData(nil)
		}
		return resp.NewBulkData(e.Val)
	}

	// return cnt number elements as array
	res := make([]resp.RedisData, 0)
	for i := 0; i < cnt; i++ {
		e := list.RPop()
		if e == nil {
			break
		}
		res = append(res, resp.NewBulkData(e.Val))
	}
	return resp.NewArrayData(res)
}

func lPushList(m *MemDb, cmd [][]byte) resp.RedisData {
	if strings.ToLower(string(cmd[0])) != "lpush" {
		logger.Error("lPushList Function : cmdName is not lpush")
		return resp.NewErrorData("Server Error")
	}
	if len(cmd) < 3 {
		return resp.NewErrorData("wrong number of arguments for 'lpush' command")
	}

	key := string(cmd[1])
	m.locks.Lock(key)
	defer m.locks.Unlock(key)

	var list *datastructure.List
	temp, ok := m.db.Get(key)
	if !ok {
		list = datastructure.NewList()
		m.db.Set(key, list)
	}
	list, ok = temp.(*datastructure.List)
	if !ok {
		return resp.NewErrorData("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	for i := 2; i < len(cmd); i++ {
		list.LPush(cmd[i])
	}
	return resp.NewIntData(int64(list.Len))
}

func lPushXList(m *MemDb, cmd [][]byte) resp.RedisData {
	if strings.ToLower(string(cmd[0])) != "lpushx" {
		logger.Error("lPushXList Function : cmdName is not lpushx")
		return resp.NewErrorData("Server Error")
	}
	if len(cmd) < 3 {
		return resp.NewErrorData("wrong number of arguments for 'lpushx' command")
	}
	key := string(cmd[1])
	m.locks.Lock(key)
	defer m.locks.Unlock(key)

	var list *datastructure.List
	temp, ok := m.db.Get(key)
	if !ok {
		return resp.NewIntData(0)
	}
	list, ok = temp.(*datastructure.List)
	if !ok {
		return resp.NewErrorData("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	for i := 2; i < len(cmd); i++ {
		list.LPush(cmd[i])
	}
	return resp.NewIntData(int64(list.Len))
}

func rPushList(m *MemDb, cmd [][]byte) resp.RedisData {
	if strings.ToLower(string(cmd[0])) != "rpush" {
		logger.Error("rPushList Function : cmdName is not rpush")
		return resp.NewErrorData("server error")
	}
	if len(cmd) < 3 {
		return resp.NewErrorData("wrong number of arguments for 'rpush' command")
	}

	key := string(cmd[1])
	m.locks.Lock(key)
	defer m.locks.Unlock(key)

	var list *datastructure.List
	tem, ok := m.db.Get(key)
	if !ok {
		list = datastructure.NewList()
		m.db.Set(key, list)
	}
	list, ok = tem.(*datastructure.List)
	if !ok {
		return resp.NewErrorData("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	for i := 2; i < len(cmd); i++ {
		list.RPush(cmd[i])
	}
	return resp.NewIntData(int64(list.Len))
}

func rPushXList(m *MemDb, cmd [][]byte) resp.RedisData {
	if strings.ToLower(string(cmd[0])) != "rpushx" {
		logger.Error("rPushXList Function : cmdName is not rpushx")
		return resp.NewErrorData("server error")
	}
	if len(cmd) < 3 {
		return resp.NewErrorData("wrong number of arguments for 'rpushX' command")
	}

	key := string(cmd[1])
	m.locks.Lock(key)
	defer m.locks.Unlock(key)

	var list *datastructure.List
	temp, ok := m.db.Get(key)
	if !ok {
		return resp.NewIntData(0)
	}
	list, ok = temp.(*datastructure.List)
	if !ok {
		return resp.NewErrorData("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	for i := 2; i < len(cmd); i++ {
		list.RPush(cmd[i])
	}
	return resp.NewIntData(int64(list.Len))
}

func lSetList(m *MemDb, cmd [][]byte) resp.RedisData {
	if strings.ToLower(string(cmd[0])) != "lset" {
		logger.Error("lSetList Function : cmdName is not lset")
		return resp.NewErrorData("server error")
	}
	if len(cmd) != 4 {
		return resp.NewErrorData("wrong number of arguments for 'lset' command")
	}
	index, err := strconv.Atoi(string(cmd[2]))
	if err != nil {
		return resp.NewErrorData("index must be an integer")
	}
	key := string(cmd[1])
	m.locks.Lock(key)
	defer m.locks.Unlock(key)

	temp, ok := m.db.Get(key)
	if !ok {
		return resp.NewErrorData("key not exist")
	}
	list, ok := temp.(*datastructure.List)
	if !ok {
		return resp.NewErrorData("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	ok = list.Set(index, cmd[3])
	if !ok {
		return resp.NewErrorData("index out of range")
	}
	return resp.NewStringData("OK")
}

func lRemList(m *MemDb, cmd [][]byte) resp.RedisData {
	if strings.ToLower(string(cmd[0])) != "lrem" {
		logger.Error("lRemList Function : cmdName is not lrem")
		return resp.NewErrorData("server error")
	}
	if len(cmd) != 4 {
		return resp.NewErrorData("wrong number of arguments for 'lrem' command")
	}
	count, err := strconv.Atoi(string(cmd[2]))
	if err != nil {
		return resp.NewErrorData("count must be an integer")
	}
	key := string(cmd[1])
	m.locks.Lock(key)
	defer m.locks.Unlock(key)

	temp, ok := m.db.Get(key)
	if !ok {
		return resp.NewIntData(0)
	}
	list, ok := temp.(*datastructure.List)
	if !ok {
		return resp.NewErrorData("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	defer func() {
		if list.Len == 0 {
			m.db.Delete(key)
			m.DelTTL(key)
		}
	}()

	res := list.RemoveElement(cmd[3], count)
	return resp.NewIntData(int64(res))
}

func lTrimList(m *MemDb, cmd [][]byte) resp.RedisData {
	if strings.ToLower(string(cmd[0])) != "ltrim" {
		logger.Error("lTrimList Function : cmdName is not ltrim")
		return resp.NewErrorData("server error")
	}
	if len(cmd) != 4 {
		return resp.NewErrorData("wrong number of argument of 'lrem' command")
	}
	start, err1 := strconv.Atoi(string(cmd[2]))
	end, err2 := strconv.Atoi(string(cmd[3]))
	if err1 != nil || err2 != nil {
		return resp.NewErrorData("start and end must be an integer")
	}
	key := string(cmd[1])
	m.locks.Lock(key)
	defer m.locks.Unlock(key)

	temp, ok := m.db.Get(key)
	if !ok {
		return resp.NewStringData("OK")
	}
	list, ok := temp.(*datastructure.List)
	if !ok {
		return resp.NewErrorData("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	defer func() {
		if list.Len == 0 {
			m.db.Delete(key)
			m.DelTTL(key)
		}
	}()

	list.Trim(start, end)
	return resp.NewStringData("OK")

}

func lRangeList(m *MemDb, cmd [][]byte) resp.RedisData {
	if strings.ToLower(string(cmd[0])) != "lrange" {
		logger.Error("lRangeList function: cmdName is not lrange")
		return resp.NewErrorData("server error")
	}
	if len(cmd) != 4 {
		return resp.NewErrorData("wrong number of arguments for 'lrange' command")
	}
	start, err1 := strconv.Atoi(string(cmd[2]))
	end, err2 := strconv.Atoi(string(cmd[3]))
	if err1 != nil || err2 != nil {
		return resp.NewErrorData("index must be an integer")
	}
	key := string(cmd[1])
	m.locks.RLock(key)
	defer m.locks.RUnlock(key)

	temp, ok := m.db.Get(key)
	if !ok {
		return resp.NewArrayData(nil)
	}
	list, ok := temp.(*datastructure.List)
	if !ok {
		return resp.NewErrorData("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	temRes := list.Range(start, end)
	if temRes == nil {
		return resp.NewArrayData(nil)
	}
	res := make([]resp.RedisData, len(temRes), len(temRes))
	for i := 0; i < len(temRes); i++ {
		res[i] = resp.NewBulkData(temRes[i])
	}
	return resp.NewArrayData(res)
}

func lMoveList(m *MemDb, cmd [][]byte) resp.RedisData {
	if strings.ToLower(string(cmd[0])) != "lmove" {
		logger.Error("lMoveList Function : cmdName is not lmove")
		return resp.NewErrorData("server error")
	}

	if len(cmd) != 5 {
		return resp.NewErrorData("wrong number of arguments for 'lmove' command")
	}
	src := string(cmd[1])
	des := string(cmd[2])
	srcDrc := strings.ToLower(string(cmd[3]))
	desDrc := strings.ToLower(string(cmd[4]))
	if (srcDrc != "left" && srcDrc != "right") || (desDrc != "left" && desDrc != "right") {
		return resp.NewErrorData("options must be left or right")
	}

	keys := []string{src, des}
	m.locks.LockMulti(keys)
	defer m.locks.UnlockMulti(keys)

	srcTemp, ok := m.db.Get(src)
	if !ok {
		return resp.NewBulkData(nil)
	}
	srcList, ok := srcTemp.(*datastructure.List)
	if !ok {
		return resp.NewErrorData("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	defer func() {
		if srcList.Len == 0 {
			m.db.Delete(src)
			m.DelTTL(src)
		}
	}()

	if srcList.Len == 0 {
		return resp.NewBulkData(nil)
	}
	desTemp, ok := m.db.Get(des)
	if !ok {
		desTemp = datastructure.NewList()
		m.db.Set(des, desTemp)
	}
	desList, ok := desTemp.(*datastructure.List)
	if !ok {
		return resp.NewErrorData("WRONGTYPE Operation against a key holding the wrong kind of value")
	}

	// pop from src
	var popElem *datastructure.ListNode
	if srcDrc == "left" {
		popElem = srcList.LPop()
	} else {
		popElem = srcList.RPop()
	}
	// insert to des
	if desDrc == "left" {
		desList.LPush(popElem.Val)
	} else {
		desList.RPush(popElem.Val)
	}
	return resp.NewBulkData(popElem.Val)
}

func RegisterListCommands() {
	RegisterCommand("llen", lLenList)
	RegisterCommand("lindex", lIndexList)
	RegisterCommand("lpos", lPosList)
	RegisterCommand("lpop", lPopList)
	RegisterCommand("rpop", rPopList)
	RegisterCommand("lpush", lPushList)
	RegisterCommand("lpushx", lPushXList)
	RegisterCommand("rpush", rPushList)
	RegisterCommand("rpushx", rPushXList)
	RegisterCommand("lindex", lIndexList)
	RegisterCommand("lset", lSetList)
	RegisterCommand("lrem", lRemList)
	RegisterCommand("ltrim", lTrimList)
	RegisterCommand("lrange", lRangeList)
	RegisterCommand("lmove", lMoveList)
}
