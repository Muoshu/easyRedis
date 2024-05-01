package memdb

import (
	"easyRedis/datastructure"
	"easyRedis/logger"
	"easyRedis/resp"
	"fmt"
	"math"
	"strconv"
	"strings"
)

func zAdd(m *MemDb, cmd [][]byte) resp.RedisData {
	if strings.ToLower(string(cmd[0])) != "zadd" {
		logger.Error("zAdd Function: cmdName is not zadd")
		return resp.NewErrorData("server error")
	}
	if len(cmd) < 4 {
		return resp.NewErrorData("error: commands is invalid")
	}

	var err error
	var nx, xx, ch, incr bool
	i := 2
	for i = 2; i < len(cmd); i++ {
		_, err = strconv.ParseFloat(string(cmd[i]), 10)
		if err == nil {
			break
		}
		switch strings.ToLower(string(cmd[i])) {
		case "nx":
			nx = true
		case "xx":
			xx = true
		case "ch":
			ch = true
		case "incr":
			incr = true
		default:
			break

		}
	}
	if nx && xx {
		return resp.NewErrorData("error: commands is invalid")
	}
	if (len(cmd)-i)&1 == 1 { //参数个数不对
		return resp.NewErrorData("error: commands is invalid")
	}
	var scores []float64
	var members []string
	for k := i; k < len(cmd); k += 2 {
		s, err := strconv.ParseFloat(string(cmd[k]), 10)
		if err != nil {
			return resp.NewErrorData("ERR value is not a valid float")
		}
		scores = append(scores, s)
		members = append(members, string(cmd[k+1]))
	}

	key := string(cmd[1])
	m.locks.Lock(key)
	defer m.locks.Unlock(key)
	temp, ok := m.db.Get(key)
	if !ok {
		temp = datastructure.NewDefaultSortSet()
		m.db.Set(key, temp)
	}
	sortSet, ok := temp.(*datastructure.SortSet)
	if !ok {
		return resp.NewErrorData("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	if incr {
		if len(scores) > 1 {
			return resp.NewErrorData("ERR INCR option supports a single increment-element pair")
		}
	}
	if nx {
		if incr {
			member := sortSet.GetMember(members[0])
			if member != nil {
				return resp.NewBulkData(nil)
			}
			item := &datastructure.StItem{
				F: scores[0],
				K: members[0],
			}
			sortSet.Add(item)
			return resp.NewFloat64Data(scores[0])
		}
		var result int64
		if ch {
			for i := 0; i < len(members); i++ {
				member := sortSet.GetMember(members[i])
				if member == nil {
					item := &datastructure.StItem{
						F: scores[i],
						K: members[i],
					}
					sortSet.Add(item)
					result++
				}
			}
			return resp.NewIntData(result)
		}

		for i := 0; i < len(members); i++ {
			member := sortSet.GetMember(members[i])
			if member == nil {
				item := &datastructure.StItem{
					F: scores[i],
					K: members[i],
				}
				sortSet.Add(item)
				result++
			}
		}
		return resp.NewIntData(result)
	}
	if xx {
		if incr {
			member := sortSet.GetMember(members[0])
			if member == nil {
				return resp.NewBulkData(nil)
			}
			item := &datastructure.StItem{
				F: scores[0] + member.Score(),
				K: members[0],
			}
			sortSet.Remove(members[0])
			sortSet.Add(item)
			return resp.NewFloat64Data(scores[0] + member.Score())
		}

		if ch {
			var result int64
			for i := 0; i < len(members); i++ {
				member := sortSet.GetMember(members[i])
				if member == nil {
					continue
				}
				member.SetScore(scores[i])
				result++
			}
			return resp.NewIntData(result)
		}

		for i := 0; i < len(members); i++ {
			member := sortSet.GetMember(members[i])
			if member != nil {
				item := &datastructure.StItem{
					F: scores[i],
					K: members[i],
				}
				sortSet.Remove(members[i])
				sortSet.Add(item)
			}
		}
		return resp.NewIntData(0)
	}
	if incr {
		member := sortSet.GetMember(members[0])
		item := &datastructure.StItem{
			F: scores[0],
			K: members[0],
		}
		if member == nil {
			sortSet.Add(item)
			return resp.NewFloat64Data(scores[0])
		}
		sortSet.Remove(members[0])
		sortSet.Add(item)
		return resp.NewFloat64Data(member.Score() + scores[0])
	}
	var result int64
	if ch {
		for i := 0; i < len(members); i++ {
			member := sortSet.GetMember(members[i])
			item := &datastructure.StItem{
				F: scores[i],
				K: members[i],
			}
			if member == nil {
				sortSet.Add(item)
				result++
			} else {
				if scores[i] != member.Score() {
					sortSet.Remove(members[i])
					sortSet.Add(item)
					result++
				}
			}
		}
		return resp.NewIntData(result)
	}
	for i := 0; i < len(members); i++ {
		member := sortSet.GetMember(members[i])
		if member == nil {
			item := &datastructure.StItem{
				F: scores[i],
				K: members[i],
			}
			sortSet.Add(item)
			result++
		}
	}
	return resp.NewIntData(result)
}

func zCard(m *MemDb, cmd [][]byte) resp.RedisData {
	if strings.ToLower(string(cmd[0])) != "zcard" {
		logger.Error("zCard Function: cmdName is not zcard")
		return resp.NewErrorData("server error")
	}
	if len(cmd) != 2 {
		return resp.NewErrorData("error: commands is invalid")
	}
	key := string(cmd[1])

	m.locks.RLock(key)
	defer m.locks.RUnlock(key)
	temp, ok := m.db.Get(key)
	if !ok {
		return resp.NewIntData(0)
	}
	sortSet, ok := temp.(*datastructure.SortSet)
	if !ok {
		return resp.NewErrorData("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	return resp.NewIntData(sortSet.Count())
}

func zDiff(m *MemDb, cmd [][]byte) resp.RedisData {
	if strings.ToLower(string(cmd[0])) != "zdiff" {
		logger.Error("zDiff Function: cmdName is not zdiff")
		return resp.NewErrorData("server error")
	}
	if len(cmd) < 3 {
		return resp.NewErrorData("ERR wrong number of arguments for 'zdiff' command")
	}
	numsKey, err := strconv.Atoi(string(cmd[1]))

	if err != nil {
		return resp.NewErrorData("ERR value is not an integer or out of range")
	}
	if cmd[1][0] == '0' || numsKey <= 0 { //前导0的情况，例如001
		return resp.NewErrorData("ERR value is not an integer or out of range")
	}
	var keys []string
	var withScores bool
	for i := 2; i < len(cmd); i++ {
		if strings.ToLower(string(cmd[i])) == "withscores" {
			if i == len(cmd)-1 {
				withScores = true
				break
			}
			return resp.NewErrorData("ERR syntax error")
		}
		keys = append(keys, string(cmd[i]))
	}

	if len(keys) != numsKey {
		return resp.NewErrorData("ERR syntax error")
	}
	m.locks.RLockMulti(keys)
	defer m.locks.RUnlockMulti(keys)

	temp, ok := m.db.Get(keys[0])
	if !ok {
		return resp.NewArrayData(nil)
	}

	firstSortSet, ok := temp.(*datastructure.SortSet)
	if !ok {
		return resp.NewErrorData("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	sortSetKeys := firstSortSet.GetAllKeys()

	for k := 1; k < len(keys); k++ {
		temp, ok := m.db.Get(keys[k])
		if !ok {
			continue
		}
		sortSet, ok := temp.(*datastructure.SortSet)
		if !ok {
			return resp.NewErrorData("WRONGTYPE Operation against a key holding the wrong kind of value")
		}
		if sortSetKeys == nil {
			return resp.NewArrayData(nil)
		}
		tempStr := make([]string, 0, len(sortSetKeys))
		for i := 0; i < len(sortSetKeys); i++ {
			members := sortSet.GetMember(sortSetKeys[i])
			if members == nil {
				tempStr = append(tempStr, sortSetKeys[i])
			}
		}
		sortSetKeys = tempStr
	}
	var result []resp.RedisData
	if withScores {
		for _, v := range sortSetKeys {
			f := firstSortSet.Score(v)
			sf := strconv.FormatFloat(f, 'f', -1, 64)
			result = append(result, resp.NewBulkData([]byte(v)), resp.NewBulkData([]byte(sf)))
		}
		return resp.NewArrayData(result)
	}
	for _, v := range sortSetKeys {
		result = append(result, resp.NewBulkData([]byte(v)))
	}

	return resp.NewArrayData(result)

}

func zCount(m *MemDb, cmd [][]byte) resp.RedisData {
	if strings.ToLower(string(cmd[0])) != "zcount" {
		logger.Error("zCount Function: cmdName is not zcount")
		return resp.NewErrorData("server error")
	}
	if len(cmd) != 4 {
		return resp.NewErrorData("error: commands is invalid")
	}
	key := string(cmd[1])
	var min, max float64
	var MinInf, MaxInf, MinBra, MaxBra bool
	var err error
	if strings.ToLower(string(cmd[2])) == "-inf" {
		MinInf = true
	} else {
		if cmd[2][0] == '(' {
			min, err = strconv.ParseFloat(string(cmd[2][1:]), 10)
			MinBra = true
			if err != nil {
				return resp.NewErrorData("ERR min or max is not a float")
			}
		} else {
			min, err = strconv.ParseFloat(string(cmd[2]), 10)
			if err != nil {
				return resp.NewErrorData("ERR min or max is not a float")
			}
		}
	}
	if strings.ToLower(string(cmd[3])) == "+inf" || strings.ToLower(string(cmd[3])) == "inf" {
		MaxInf = true
	} else {
		if cmd[3][0] == '(' {
			MaxBra = true
			max, err = strconv.ParseFloat(string(cmd[3][1:]), 10)
			if err != nil {
				return resp.NewErrorData("ERR min or max is not a float")
			}
		} else {
			max, err = strconv.ParseFloat(string(cmd[3]), 10)
			if err != nil {
				return resp.NewErrorData("ERR min or max is not a float")
			}
		}
	}
	fmt.Println(min, max, MinInf, MaxInf)
	if MinInf && MaxInf && strings.ToLower(string(cmd[2])) == strings.ToLower(string(cmd[3])) {
		MinInf = false
		MaxInf = false
	}
	findRange := &datastructure.SkipListFindRange{
		Min:    min,
		Max:    max,
		MinBra: MinBra,
		MaxBra: MaxBra,
		MinInf: MinInf,
		MaxInf: MaxInf,
	}

	m.locks.RLock(key)
	defer m.locks.RUnlock(key)

	temp, ok := m.db.Get(key)
	fmt.Println(ok)
	if !ok {
		return resp.NewIntData(0)
	}
	sortSet, ok := temp.(*datastructure.SortSet)
	if !ok {
		return resp.NewErrorData("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	s := sortSet.RangeByScore(findRange)
	for i := 0; i < len(s); i++ {
		fmt.Println(s[i].Score())
	}
	return resp.NewIntData(int64(len(s)))
}

func zDiffStore(m *MemDb, cmd [][]byte) resp.RedisData {
	if strings.ToLower(string(cmd[0])) != "zdiffstore" {
		logger.Error("zDiffStore Function: cmdName is not zdiffstore")
		return resp.NewErrorData("server error")
	}
	if len(cmd) < 4 {
		return resp.NewErrorData("error: commands is invalid")
	}

	numsKey, err := strconv.Atoi(string(cmd[2]))
	if err != nil || err == nil && numsKey <= 0 {
		return resp.NewErrorData("ERR at least 1 input key is needed for 'zdiffstore' command")
	}
	if numsKey != len(cmd)-3 {
		return resp.NewErrorData("ERR syntax error")
	}

	desKey := string(cmd[1])
	keys := make([]string, 0, len(cmd)-3)
	for i := 3; i < len(cmd); i++ {
		keys = append(keys, string(cmd[i]))
	}

	tempKeys := make([]string, 0, 1+len(keys))
	tempKeys = append(tempKeys, desKey)
	tempKeys = append(tempKeys, keys...)
	m.locks.LockMulti(tempKeys)
	defer m.locks.UnlockMulti(tempKeys)

	temp, ok := m.db.Get(desKey)
	if !ok {
		temp = datastructure.NewDefaultSortSet()

	}
	desSortSet, ok := temp.(*datastructure.SortSet)
	if !ok {
		return resp.NewErrorData("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	t, ok := m.db.Get(keys[0])
	if !ok {
		return resp.NewIntData(0)
	}
	sortSet1, ok := t.(*datastructure.SortSet)
	if !ok {
		return resp.NewErrorData("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	sortSetHelp := datastructure.NewDefaultSortSet()
	for key, score := range sortSet1.GetAllKeysAndScores() {
		item := &datastructure.StItem{
			F: score,
			K: key,
		}
		sortSetHelp.Add(item)
	}
	var count int64
	for i := 1; i < len(keys); i++ {
		temp, ok = m.db.Get(keys[i])
		if !ok {
			continue
		}
		sortSet, ok := temp.(*datastructure.SortSet)
		if !ok {
			return resp.NewErrorData("WRONGTYPE Operation against a key holding the wrong kind of value")
		}
		for key, score := range sortSet.GetAllKeysAndScores() {
			item := &datastructure.StItem{
				F: score,
				K: key,
			}
			if sortSetHelp.Add(item) > 0 {
				count++
			}
		}
	}
	m.db.Delete(desKey)
	m.db.Set(desKey, desSortSet)
	return resp.NewIntData(count)
}

func zIncrBy(m *MemDb, cmd [][]byte) resp.RedisData {
	if strings.ToLower(string(cmd[0])) != "zincrby" {
		logger.Error("zIncrby Function: cmdName is not zincrby")
		return resp.NewErrorData("server error")
	}
	if len(cmd) != 4 {
		return resp.NewErrorData("error: commands is invalid")
	}
	key := string(cmd[1])
	memberKey := string(cmd[3])
	incr, err := strconv.ParseFloat(string(cmd[2]), 10)
	if err != nil {
		return resp.NewErrorData("ERR value is not a valid float")
	}
	m.locks.Lock(key)
	defer m.locks.Unlock(key)
	temp, ok := m.db.Get(key)
	if !ok {
		temp = datastructure.NewDefaultSortSet()
		m.db.Set(key, temp)
	}
	sortSet, ok := temp.(*datastructure.SortSet)
	score := sortSet.Score(memberKey)
	sortSet.Remove(memberKey)
	item := &datastructure.StItem{
		F: score + incr,
		K: memberKey,
	}
	sortSet.Add(item)
	return resp.NewFloat64Data(score + incr)
}

func zInterStore(m *MemDb, cmd [][]byte) resp.RedisData {
	if strings.ToLower(string(cmd[0])) != "zinterstore" {
		logger.Error("zInterStore Function: cmdName is not zinterstore")
		return resp.NewErrorData("server error")
	}
	if len(cmd) < 4 {
		return resp.NewErrorData("error: commands is invalid")
	}
	desKey := string(cmd[1])
	numKeys, err := strconv.Atoi(string(cmd[2]))
	if err != nil || (err == nil && (numKeys <= 0 || cmd[2][0] == '0')) {
		return resp.NewErrorData("ERR at least 1 input key is needed for 'zinterstore' command")
	}
	var weights, aggregate, sum, min, max bool
	var keys []string
	var weight []float64
	i := 3
	for i = 3; i < len(cmd); i++ {
		tempKey := string(cmd[i])
		if strings.ToLower(tempKey) == "weights" {
			weights = true
			break
		}
		if strings.ToLower(tempKey) == "aggregate" {
			aggregate = true
			break
		}
		keys = append(keys, tempKey)
	}
	k := i + 1
	if weights {
		for ; k < len(cmd); k++ {
			if strings.ToLower(string(cmd[k])) == "aggregate" {
				break
			}
			tempWeight, err := strconv.ParseFloat(string(cmd[k]), 10)
			if err != nil {
				fmt.Println(string(cmd[k]))
				return resp.NewErrorData("ERR weight value is not a float")
			}
			weight = append(weight, tempWeight)
		}
	}
	var l int
	if weights {
		l = k + 1
	} else {
		l = k
	}
	if aggregate {
		for ; l < len(cmd); l++ {
			switch string(cmd[l]) {
			case "min":
				min = true
			case "max":
				max = true
			case "sum":
				sum = true
			default:
				return resp.NewErrorData("ERR syntax error")
			}
		}
	}
	if (min && max) || (min && sum) || (max && sum) || len(keys) != len(weight) || (aggregate && !min && !max && !sum) {
		return resp.NewErrorData("ERR syntax error")
	}

	tempKeys := []string{desKey}
	tempKeys = append(tempKeys, keys...)
	m.locks.LockMulti(tempKeys)
	m.locks.UnlockMulti(tempKeys)

	desSortSet := datastructure.NewDefaultSortSet()
	var count int64
	var minSortSet *datastructure.SortSet
	minSortSetSize := math.MaxInt64
	var minSortSetIndex int
	for i := 0; i < len(keys); i++ {
		temp, ok := m.db.Get(keys[i])
		if !ok {
			return resp.NewIntData(0)
		}
		tempSortSet, ok := temp.(*datastructure.SortSet)
		if !ok {
			return resp.NewErrorData("WRONGTYPE Operation against a key holding the wrong kind of value")
		}
		if tempSortSet.Count() < int64(minSortSetSize) {
			minSortSetIndex = i
			minSortSet = tempSortSet
		}
	}
	for k, s := range minSortSet.GetAllKeysAndScores() {
		var item *datastructure.StItem
		if weights {
			item = &datastructure.StItem{
				F: s * weight[minSortSetIndex],
				K: k,
			}
		} else {
			item = &datastructure.StItem{
				F: s,
				K: k,
			}
		}
		desSortSet.Add(item)
	}
	desSortSetKeys := desSortSet.GetAllKeys()
	if weights && aggregate {
		for i := 0; i < len(keys); i++ {
			temp, _ := m.db.Get(keys[i])
			tempSortSet, _ := temp.(*datastructure.SortSet)
			if tempSortSet == minSortSet {
				continue
			}
			for _, key := range desSortSetKeys {
				if desSortSet.GetMember(key) != nil && tempSortSet.GetMember(key) != nil {
					if min && desSortSet.Score(key) > tempSortSet.Score(key)*weight[i] {
						item := &datastructure.StItem{
							K: key,
							F: tempSortSet.Score(key) * weight[i],
						}
						desSortSet.Remove(key)
						desSortSet.Add(item)
					} else if max && desSortSet.Score(key) < tempSortSet.Score(key)*weight[i] {
						item := &datastructure.StItem{
							K: key,
							F: tempSortSet.Score(key) * weight[i],
						}
						desSortSet.Remove(key)
						desSortSet.Add(item)

					} else {
						item := &datastructure.StItem{
							K: key,
							F: tempSortSet.Score(key)*weight[i] + desSortSet.Score(key),
						}
						desSortSet.Remove(key)
						desSortSet.Add(item)
					}
					count++
				} else {
					desSortSet.Remove(key)
				}

			}
		}
	} else if weights {
		for i := 0; i < len(keys); i++ {
			temp, _ := m.db.Get(keys[i])
			tempSortSet, _ := temp.(*datastructure.SortSet)
			if tempSortSet == minSortSet {
				continue
			}
			for _, key := range desSortSetKeys {
				if desSortSet.GetMember(key) != nil && tempSortSet.GetMember(key) != nil {
					item := &datastructure.StItem{
						K: key,
						F: tempSortSet.Score(key)*weight[i] + desSortSet.Score(key),
					}
					desSortSet.Remove(key)
					desSortSet.Add(item)
					count++
				} else {
					desSortSet.Remove(key)
				}
			}
		}
	} else if aggregate {
		for i := 0; i < len(keys); i++ {
			temp, _ := m.db.Get(keys[i])
			tempSortSet, _ := temp.(*datastructure.SortSet)
			if tempSortSet == minSortSet {
				continue
			}
			for _, key := range desSortSetKeys {
				if desSortSet.GetMember(key) != nil && tempSortSet.GetMember(key) != nil {
					if min && desSortSet.Score(key) > tempSortSet.Score(key) {
						item := &datastructure.StItem{
							K: key,
							F: tempSortSet.Score(key),
						}
						desSortSet.Remove(key)
						desSortSet.Add(item)
					} else if max && desSortSet.Score(key) < tempSortSet.Score(key) {
						item := &datastructure.StItem{
							K: key,
							F: tempSortSet.Score(key),
						}
						desSortSet.Remove(key)
						desSortSet.Add(item)

					} else {
						item := &datastructure.StItem{
							K: key,
							F: tempSortSet.Score(key) + desSortSet.Score(key),
						}
						desSortSet.Remove(key)
						desSortSet.Add(item)
					}
					count++
				} else {
					desSortSet.Remove(key)
				}
			}
		}

	} else {
		for i := 0; i < len(keys); i++ {
			temp, _ := m.db.Get(keys[i])
			tempSortSet, _ := temp.(*datastructure.SortSet)
			if tempSortSet == minSortSet {
				continue
			}
			for _, key := range desSortSetKeys {
				if desSortSet.GetMember(key) != nil && tempSortSet.GetMember(key) != nil {
					item := &datastructure.StItem{
						K: key,
						F: tempSortSet.Score(key) + desSortSet.Score(key),
					}
					desSortSet.Remove(key)
					desSortSet.Add(item)
					count++
				} else {
					desSortSet.Remove(key)
				}
			}

		}
	}
	m.db.Delete(desKey)
	m.db.Set(desKey, desSortSet)
	return resp.NewIntData(count)
}

func zPopMax(m *MemDb, cmd [][]byte) resp.RedisData {
	if strings.ToLower(string(cmd[0])) != "zpopmax" {
		logger.Error("zPopMax Function: cmdName is not zpopmax")
		return resp.NewErrorData("server error")
	}
	if len(cmd) < 2 {
		return resp.NewErrorData("error: commands is invalid")
	}
	key := string(cmd[1])
	count := 1
	var err error
	if len(cmd) == 3 {
		count, err = strconv.Atoi(string(cmd[2]))
		if err != nil {
			return resp.NewErrorData("ERR value is out of range, must be positive")
		}
		if count == 0 {
			return resp.NewArrayData(nil)
		}
		if count < 0 {
			return resp.NewErrorData("ERR value is out of range, must be positive")
		}
	}
	m.locks.Lock(key)
	defer m.locks.Unlock(key)

	temp, ok := m.db.Get(key)
	if !ok {
		return resp.NewArrayData(nil)
	}
	sortSet, ok := temp.(*datastructure.SortSet)
	if !ok {
		return resp.NewErrorData("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	res := make([]resp.RedisData, 0)
	for i := 0; i < count; i++ {
		min := sortSet.Count() - 1
		max := min
		t := sortSet.Range(min, max)
		if t == nil {
			if len(res) == 0 {
				return resp.NewBulkData([]byte("empty array"))
			}
			break
		}
		for _, v := range t {
			score := strconv.FormatFloat(v.Score(), 'f', -1, 64)
			res = append(res, resp.NewBulkData([]byte(v.Key())), resp.NewBulkData([]byte(score)))
			sortSet.Remove(v.Key())
		}
	}

	defer func() {
		if sortSet.Count() == 0 {
			m.db.Delete(key)
			m.ttlKeys.Delete(key)
		}
	}()

	return resp.NewArrayData(res)
}

func zPopMin(m *MemDb, cmd [][]byte) resp.RedisData {
	if strings.ToLower(string(cmd[0])) != "zpopmin" {
		logger.Error("zPopMin Function: cmdName is not zpopmin")
		return resp.NewErrorData("server error")
	}
	if len(cmd) < 2 {
		return resp.NewErrorData("error: commands is invalid")
	}
	key := string(cmd[1])
	count := 1
	var err error

	if len(cmd) == 3 {
		count, err = strconv.Atoi(string(cmd[2]))
		if err != nil {
			return resp.NewErrorData("ERR value is out of range, must be positive")
		}
		if count == 0 {
			return resp.NewBulkData([]byte("empty array"))
		}
	}
	m.locks.Lock(key)
	m.locks.Unlock(key)
	temp, ok := m.db.Get(key)
	if !ok {
		return resp.NewBulkData([]byte("empty array"))
	}
	sortSet, ok := temp.(*datastructure.SortSet)
	if !ok {
		return resp.NewErrorData("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	res := make([]resp.RedisData, 0)
	for i := 0; i < count; i++ {
		min := int64(0)
		max := min
		t := sortSet.Range(min, max)
		if t == nil {
			if len(res) == 0 {
				return resp.NewBulkData([]byte("empty array"))
			}
			break
		}
		for _, v := range t {
			score := strconv.FormatFloat(v.Score(), 'f', -1, 64)
			res = append(res, resp.NewBulkData([]byte(v.Key())), resp.NewBulkData([]byte(score)))
			sortSet.Remove(v.Key())
		}
	}

	defer func() {
		if sortSet.Count() == 0 {
			m.db.Delete(key)
			m.ttlKeys.Delete(key)
		}
	}()

	return resp.NewArrayData(res)
}

func zRank(m *MemDb, cmd [][]byte) resp.RedisData {
	if strings.ToLower(string(cmd[0])) != "zrank" {
		logger.Error("zRank Function: cmdName is not zrank")
		return resp.NewErrorData("server error")
	}
	if len(cmd) < 3 || len(cmd) > 4 {
		return resp.NewErrorData("error: commands is invalid")
	}
	key := string(cmd[1])
	member := string(cmd[2])
	var withscore bool
	if len(cmd) == 4 {
		if strings.ToLower(string(cmd[3])) == "withscore" {
			withscore = true
		} else {
			return resp.NewErrorData("error: commands is invalid")
		}
	}
	m.locks.RLock(key)
	defer m.locks.RUnlock(key)
	temp, ok := m.db.Get(key)
	if !ok {
		return resp.NewBulkData(nil)
	}
	sortSet, ok := temp.(*datastructure.SortSet)
	if !ok {
		return resp.NewErrorData("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	if sortSet.GetMember(member) == nil {
		return resp.NewBulkData(nil)
	}
	rank := sortSet.Rank(member)
	if !withscore {
		return resp.NewIntData(rank)
	}
	score := strconv.FormatFloat(sortSet.Score(member), 'f', -1, 64)
	res := make([]resp.RedisData, 0)
	res = append(res, resp.NewIntData(rank), resp.NewBulkData([]byte(score)))
	return resp.NewArrayData(res)
}

func zRevRank(m *MemDb, cmd [][]byte) resp.RedisData {
	if strings.ToLower(string(cmd[0])) != "zrevrank" {
		logger.Error("zRevRank Function: cmdName is not zrevrank")
		return resp.NewErrorData("server error")
	}
	if len(cmd) < 3 || len(cmd) > 4 {
		return resp.NewErrorData("error: commands is invalid")
	}
	key := string(cmd[1])
	member := string(cmd[2])
	var withScore bool
	if len(cmd) == 4 {
		if strings.ToLower(string(cmd[3])) == "withscore" {
			withScore = true
		} else {
			return resp.NewErrorData("error: commands is invalid")
		}
	}
	m.locks.RLock(key)
	defer m.locks.RUnlock(key)
	temp, ok := m.db.Get(key)
	if !ok {
		return resp.NewBulkData(nil)
	}
	sortSet, ok := temp.(*datastructure.SortSet)
	if !ok {
		return resp.NewErrorData("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	if sortSet.GetMember(member) == nil {
		return resp.NewBulkData(nil)
	}

	rank := sortSet.RevRank(member)
	if !withScore {
		return resp.NewIntData(rank)
	}
	score := strconv.FormatFloat(sortSet.Score(member), 'f', -1, 64)
	res := make([]resp.RedisData, 0)
	res = append(res, resp.NewIntData(rank), resp.NewBulkData([]byte(score)))
	return resp.NewArrayData(res)
}

func zScore(m *MemDb, cmd [][]byte) resp.RedisData {
	if strings.ToLower(string(cmd[0])) != "zscore" {
		logger.Error("zScore Function: cmdName is not zscore")
		return resp.NewErrorData("server error")
	}
	if len(cmd) != 3 {
		return resp.NewErrorData("error: commands is invalid")
	}
	key := string(cmd[1])
	member := string(cmd[2])
	m.locks.RLock(key)
	defer m.locks.RUnlock(key)
	temp, ok := m.db.Get(key)
	if !ok {
		return resp.NewBulkData(nil)
	}
	sortSet, ok := temp.(*datastructure.SortSet)
	if !ok {
		return resp.NewErrorData("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	memberNode := sortSet.GetMember(member)
	if memberNode == nil {
		return resp.NewBulkData(nil)
	}
	score := sortSet.Score(member)
	return resp.NewFloat64Data(score)
}

func zRange(m *MemDb, cmd [][]byte) resp.RedisData {
	if strings.ToLower(string(cmd[0])) != "zrange" {
		logger.Error("zRange Function: cmdName is not zrange")
		return resp.NewErrorData("server error")
	}
	if len(cmd) < 4 || len(cmd) > 5 {
		return resp.NewErrorData("error: commands is invalid")
	}
	key := string(cmd[1])
	start, err := strconv.ParseInt(string(cmd[2]), 10, 64)
	if err != nil {
		return resp.NewErrorData("ERR value is not an integer or out of range")
	}
	stop, err := strconv.ParseInt(string(cmd[3]), 10, 64)
	if err != nil {
		return resp.NewErrorData("ERR value is not an integer or out of range")
	}
	var withScores bool
	if len(cmd) == 5 {
		if strings.ToLower(string(cmd[4])) == "withscores" {
			withScores = true
		} else {
			return resp.NewErrorData("ERR syntax error")
		}
	}
	m.locks.RLock(key)
	defer m.locks.RUnlock(key)

	temp, ok := m.db.Get(key)
	if !ok {
		return resp.NewBulkData([]byte("empty array"))
	}
	sortSet, ok := temp.(*datastructure.SortSet)
	if !ok {
		return resp.NewErrorData("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	result := sortSet.Range(start, stop)
	if len(result) == 0 {
		return resp.NewBulkData([]byte("empty array"))
	}
	var res []resp.RedisData
	if withScores {
		res = make([]resp.RedisData, 0, 2*len(result))
	} else {
		res = make([]resp.RedisData, 0, len(result))
	}
	for i := 0; i < len(result); i++ {
		if withScores {
			score := strconv.FormatFloat(result[i].Score(), 'f', -1, 64)
			key := result[i].Key()
			res = append(res, resp.NewBulkData([]byte(key)), resp.NewBulkData([]byte(score)))
		} else {
			res = append(res, resp.NewBulkData([]byte(result[i].Key())))
		}
	}
	return resp.NewArrayData(res)
}

func zRevRange(m *MemDb, cmd [][]byte) resp.RedisData {
	if strings.ToLower(string(cmd[0])) != "zrevrange" {
		logger.Error("zRevRange Function: cmdName is not zrevrange")
		return resp.NewErrorData("server error")
	}
	if len(cmd) < 4 || len(cmd) > 5 {
		return resp.NewErrorData("error: commands is invalid")
	}
	key := string(cmd[1])
	start, err := strconv.ParseInt(string(cmd[2]), 10, 64)
	if err != nil {
		return resp.NewErrorData("ERR value is not an integer or out of range")
	}
	stop, err := strconv.ParseInt(string(cmd[3]), 10, 64)
	if err != nil {
		return resp.NewErrorData("ERR value is not an integer or out of range")
	}
	var withScores bool
	if len(cmd) == 5 {
		if strings.ToLower(string(cmd[4])) == "withscores" {
			withScores = true
		} else {
			return resp.NewErrorData("ERR syntax error")
		}
	}
	m.locks.RLock(key)
	defer m.locks.RUnlock(key)

	temp, ok := m.db.Get(key)
	if !ok {
		return resp.NewBulkData([]byte("empty array"))
	}
	sortSet, ok := temp.(*datastructure.SortSet)
	if !ok {
		return resp.NewErrorData("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	result := sortSet.RevRange(start, stop)
	if len(result) == 0 {
		return resp.NewBulkData([]byte("empty array"))
	}
	var res []resp.RedisData
	if withScores {
		res = make([]resp.RedisData, 0, 2*len(result))
	} else {
		res = make([]resp.RedisData, 0, len(result))
	}
	for i := 0; i < len(result); i++ {
		if withScores {
			score := strconv.FormatFloat(result[i].Score(), 'f', -1, 64)
			key := result[i].Key()
			res = append(res, resp.NewBulkData([]byte(key)), resp.NewBulkData([]byte(score)))
		} else {
			res = append(res, resp.NewBulkData([]byte(result[i].Key())))
		}
	}
	return resp.NewArrayData(res)
}

func zRangeByScore(m *MemDb, cmd [][]byte) resp.RedisData {
	if strings.ToLower(string(cmd[0])) != "zrangebyscore" {
		logger.Error("zRangeByScore Function: cmdName is not zrangebyscore")
		return resp.NewErrorData("server error")
	}
	if len(cmd) < 4 || len(cmd) > 8 {
		return resp.NewErrorData("error: commands is invalid")
	}
	key := string(cmd[1])
	var min, max float64
	var offset, count int64
	var minBra, maxBra, withscores, limit, minInf, maxInf bool
	var err error

	if cmd[2][0] == '(' {
		min, err = strconv.ParseFloat(string(cmd[2][1:]), 64)
		minBra = true
	} else {
		min, err = strconv.ParseFloat(string(cmd[2]), 64)
	}
	if err != nil {
		if strings.ToLower(string(cmd[2])) == "-inf" {
			minInf = true
		} else {
			return resp.NewErrorData("ERR min or max is not a float")
		}
	}

	if cmd[3][0] == '(' {
		max, err = strconv.ParseFloat(string(cmd[3][1:]), 64)
		maxBra = true
	} else {
		max, err = strconv.ParseFloat(string(cmd[3]), 64)
	}
	if err != nil {
		if strings.ToLower(string(cmd[2])) == "+inf" {
			maxInf = true
		} else {
			return resp.NewErrorData("ERR min or max is not a float")
		}
	}

	for i := 4; i < len(cmd); i++ {
		if strings.ToLower(string(cmd[i])) == "withscores" {
			withscores = true
			continue
		} else if strings.ToLower(string(cmd[i])) == "limit" {
			limit = true
			if i+2 != len(cmd)-1 {
				return resp.NewErrorData("ERR syntax error")
			}
			offset, err = strconv.ParseInt(string(cmd[i+1]), 10, 64)
			if err != nil {
				return resp.NewErrorData("ERR value is not an integer or out of range")
			}
			count, err = strconv.ParseInt(string(cmd[i+2]), 10, 64)
			if err != nil {
				return resp.NewErrorData("ERR value is not an integer or out of range")
			}
			break
		} else {
			return resp.NewErrorData("ERR syntax error")
		}
	}

	if limit && (offset < 0 || count == 0) {
		return resp.NewBulkData([]byte("empty array"))
	}

	m.locks.RLock(key)
	defer m.locks.RUnlock(key)

	temp, ok := m.db.Get(key)
	if !ok {
		return resp.NewBulkData([]byte("empty array"))
	}
	sortSet, ok := temp.(*datastructure.SortSet)
	if !ok {
		return resp.NewErrorData("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	findRange := &datastructure.SkipListFindRange{
		Min:    min,
		Max:    max,
		MinBra: minBra,
		MaxBra: maxBra,
		MinInf: minInf,
		MaxInf: maxInf,
	}
	result := sortSet.RangeByScore(findRange)

	if int64(len(result)) <= offset {
		return resp.NewBulkData([]byte("empty array"))
	}
	var res []resp.RedisData
	if count < 0 {
		for i := offset; i < int64(len(result)); i++ {
			key := result[i].Key()
			if withscores {
				scoreStr := strconv.FormatFloat(result[i].Score(), 'f', -1, 64)
				res = append(res, resp.NewBulkData([]byte(key)), resp.NewBulkData([]byte(scoreStr)))
			} else {
				res = append(res, resp.NewBulkData([]byte(key)))
			}
		}
		return resp.NewArrayData(res)
	} else {
		if limit {
			for i := offset; i < int64(len(result)) && i < count+offset; i++ {
				key := result[i].Key()
				if withscores {
					scoreStr := strconv.FormatFloat(result[i].Score(), 'f', -1, 64)
					res = append(res, resp.NewBulkData([]byte(key)), resp.NewBulkData([]byte(scoreStr)))
				} else {
					res = append(res, resp.NewBulkData([]byte(key)))
				}
			}
		} else {
			for i := 0; i < len(result); i++ {
				key := result[i].Key()
				if withscores {
					scoreStr := strconv.FormatFloat(result[i].Score(), 'f', -1, 64)
					res = append(res, resp.NewBulkData([]byte(key)), resp.NewBulkData([]byte(scoreStr)))
				} else {
					res = append(res, resp.NewBulkData([]byte(key)))
				}
			}
		}

		return resp.NewArrayData(res)
	}
}

func zRem(m *MemDb, cmd [][]byte) resp.RedisData {
	if strings.ToLower(string(cmd[0])) != "zrem" {
		logger.Error("zRem Function: cmdName is not zrem")
		return resp.NewErrorData("server error")
	}
	if len(cmd) < 3 {
		return resp.NewErrorData("error: commands is invalid")
	}
	key := string(cmd[1])
	members := make([]string, 0, len(cmd)-2)
	for i := 2; i < len(cmd); i++ {
		members = append(members, string(cmd[i]))
	}

	m.locks.Lock(key)
	defer m.locks.Unlock(key)

	temp, ok := m.db.Get(key)
	if !ok {
		return resp.NewIntData(0)
	}
	sortSet, ok := temp.(*datastructure.SortSet)
	if !ok {
		return resp.NewErrorData("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	count := sortSet.Remove(members...)

	defer func() {
		if sortSet.Count() == 0 {
			m.db.Delete(key)
			m.ttlKeys.Delete(key)
		}
	}()

	return resp.NewIntData(int64(count))
}

func zRemRangeByRank(m *MemDb, cmd [][]byte) resp.RedisData {
	if strings.ToLower(string(cmd[0])) != "zremrangebyrank" {
		logger.Error("zRemRangeByRank Function: cmdName is not zremrangebyrank")
		return resp.NewErrorData("server error")
	}
	if len(cmd) != 4 {
		return resp.NewErrorData("error: commands is invalid")
	}

	key := string(cmd[1])
	start, err1 := strconv.ParseInt(string(cmd[2]), 10, 64)
	stop, err2 := strconv.ParseInt(string(cmd[3]), 10, 64)
	if err1 != nil || err2 != nil {
		return resp.NewErrorData("ERR value is not an integer or out of range")
	}
	m.locks.Lock(key)
	defer m.locks.Unlock(key)
	temp, ok := m.db.Get(key)
	if !ok {
		return resp.NewIntData(0)
	}
	sortSet, ok := temp.(*datastructure.SortSet)
	if !ok {
		return resp.NewErrorData("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	count := sortSet.RemoveRangeByRank(start, stop)
	defer func() {
		if sortSet.Count() == 0 {
			m.db.Delete(key)
			m.ttlKeys.Delete(key)
		}
	}()
	return resp.NewIntData(int64(count))
}

func zRemRangeByScore(m *MemDb, cmd [][]byte) resp.RedisData {
	if strings.ToLower(string(cmd[0])) != "zremrangebyscore" {
		logger.Error("zRemRangeByScore Function: cmdName is not zremrangebyscore")
		return resp.NewErrorData("server error")
	}
	if len(cmd) != 4 {
		return resp.NewErrorData("error: commands is invalid")
	}

	key := string(cmd[1])
	var start, stop float64
	var minBra, maxBra, minInf, maxInf bool
	var err1, err2 error

	if cmd[2][0] == '(' {
		minBra = true
		start, err1 = strconv.ParseFloat(string(cmd[2][1:]), 64)
	} else if strings.ToLower(string(cmd[2])) == "-inf" {
		minInf = true
	} else {
		start, err1 = strconv.ParseFloat(string(cmd[2]), 64)
	}

	if cmd[3][0] == '(' {
		maxBra = true
		stop, err2 = strconv.ParseFloat(string(cmd[3][1:]), 64)
	} else if strings.ToLower(string(cmd[3])) == "+inf" {
		maxInf = true
	} else {
		stop, err2 = strconv.ParseFloat(string(cmd[3]), 64)
	}

	if err1 != nil || err2 != nil {
		fmt.Println(err1.Error(), err2.Error())
		return resp.NewErrorData("ERR value is not a float or out of range")
	}
	m.locks.Lock(key)
	defer m.locks.Unlock(key)
	temp, ok := m.db.Get(key)
	if !ok {
		return resp.NewIntData(0)
	}
	sortSet, ok := temp.(*datastructure.SortSet)
	if !ok {
		return resp.NewErrorData("WRONGTYPE Operation against a key holding the wrong kind of value")
	}
	count := sortSet.RemoveRangeByScore(start, stop, minBra, maxBra, minInf, maxInf)
	defer func() {
		if sortSet.Count() == 0 {
			m.db.Delete(key)
			m.ttlKeys.Delete(key)
		}
	}()
	return resp.NewIntData(int64(count))
}

func zUnionStore(m *MemDb, cmd [][]byte) resp.RedisData {
	if strings.ToLower(string(cmd[0])) != "zunionstore" {
		logger.Error("zUnionStore Function: cmdName is not zunionstore")
		return resp.NewErrorData("server error")
	}
	if len(cmd) < 4 {
		return resp.NewErrorData("error: commands is invalid")
	}
	desKey := string(cmd[1])
	numKeys, err := strconv.Atoi(string(cmd[2]))
	if err != nil || err == nil && (numKeys <= 0 || cmd[2][0] == '0') {
		return resp.NewErrorData("ERR at least 1 input key is needed for 'zinterstore' command")
	}
	var weights, aggregate, sum, min, max bool
	var keys []string
	var weight []float64
	i := 3
	for i = 3; i < len(cmd); i++ {
		tempKey := string(cmd[i])
		if strings.ToLower(tempKey) == "weights" {
			weights = true
			break
		}
		if strings.ToLower(tempKey) == "aggregate" {
			aggregate = true
			break
		}
		keys = append(keys, tempKey)
	}
	k := i + 1
	if weights {
		for ; k < len(cmd); k++ {
			if strings.ToLower(string(cmd[k])) == "aggregate" {
				break
			}
			tempWeight, err := strconv.ParseFloat(string(cmd[k]), 10)
			if err != nil {
				fmt.Println(string(cmd[k]))
				return resp.NewErrorData("ERR weight value is not a float")
			}
			weight = append(weight, tempWeight)
		}
	}
	var l int
	if weights {
		l = k + 1
	} else {
		l = k
	}
	if aggregate {
		for ; l < len(cmd); l++ {
			switch string(cmd[l]) {
			case "min":
				min = true
			case "max":
				max = true
			case "sum":
				sum = true
			default:
				return resp.NewErrorData("ERR syntax error")
			}
		}
	}
	if (min && max) || (min && sum) || (max && sum) || len(keys) != len(weight) || (aggregate && !min && !max && !sum) {
		return resp.NewErrorData("ERR syntax error")
	}

	tempKeys := []string{desKey}
	tempKeys = append(tempKeys, keys...)
	m.locks.LockMulti(tempKeys)
	m.locks.UnlockMulti(tempKeys)

	desSortSet := datastructure.NewDefaultSortSet()
	var count int64
	for i := 0; i < len(keys); i++ {
		temp, ok := m.db.Get(keys[i])
		if !ok {
			continue
		}
		tempSortSet, ok := temp.(*datastructure.SortSet)
		if !ok {
			return resp.NewErrorData("WRONGTYPE Operation against a key holding the wrong kind of value")
		}
		tempKeyAndScore := tempSortSet.GetAllKeysAndScores()
		if weights && aggregate {
			if min {
				for k, v := range tempKeyAndScore {
					item := &datastructure.StItem{
						K: k,
						F: v * weight[i],
					}
					if desSortSet.GetMember(k) == nil {
						desSortSet.Add(item)
						count++
					} else {
						if v*weight[i] < desSortSet.Score(k) {
							desSortSet.Remove(k)
							desSortSet.Add(item)
							count++
						}
					}
				}

			} else if max {
				for k, v := range tempKeyAndScore {
					item := &datastructure.StItem{
						K: k,
						F: v * weight[i],
					}
					if desSortSet.GetMember(k) == nil {
						desSortSet.Add(item)
						count++
					} else {
						if v*weight[i] > desSortSet.Score(k) {
							desSortSet.Remove(k)
							desSortSet.Add(item)
							count++
						}
					}
				}
			} else {
				for k, v := range tempKeyAndScore {
					if desSortSet.GetMember(k) == nil {
						item := &datastructure.StItem{
							K: k,
							F: v * weight[i],
						}
						desSortSet.Add(item)
					} else {
						score := desSortSet.Score(k)
						item := &datastructure.StItem{
							K: k,
							F: score + v*weight[i],
						}
						desSortSet.Remove(k)
						desSortSet.Add(item)
					}
					count++
				}
			}
		} else if weights {
			for k, v := range tempKeyAndScore {
				item := &datastructure.StItem{
					K: k,
					F: v*weight[i] + desSortSet.Score(k),
				}
				if desSortSet.GetMember(k) != nil {
					desSortSet.Remove(k)
				}
				desSortSet.Add(item)
				count++
			}
		} else if aggregate {
			for k, v := range tempKeyAndScore {
				if min {
					item := &datastructure.StItem{
						K: k,
						F: v,
					}
					if desSortSet.GetMember(k) == nil {
						desSortSet.Add(item)
						count++
					} else {
						if desSortSet.Score(k) > v {
							desSortSet.Remove(k)
							desSortSet.Add(item)
							count++
						}
					}
				} else if max {
					item := &datastructure.StItem{
						K: k,
						F: v,
					}
					if desSortSet.GetMember(k) == nil {
						desSortSet.Add(item)
						count++
					} else {
						if desSortSet.Score(k) < v {
							desSortSet.Remove(k)
							desSortSet.Add(item)
							count++
						}
					}
				} else {
					item := &datastructure.StItem{
						K: k,
						F: v + desSortSet.Score(k),
					}
					if desSortSet.GetMember(k) != nil {
						desSortSet.Remove(k)
					}
					desSortSet.Add(item)
					count++
				}
			}
		} else {
			for k, v := range tempKeyAndScore {
				item := &datastructure.StItem{
					K: k,
					F: v + desSortSet.Score(k),
				}
				if desSortSet.GetMember(k) != nil {
					desSortSet.Remove(k)
				}
				desSortSet.Add(item)
				count++
			}
		}
	}
	m.db.Delete(desKey)
	m.db.Set(desKey, desSortSet)
	return resp.NewIntData(count)
}

func RegisterSortSetCommands() {
	RegisterCommand("zadd", zAdd)
	RegisterCommand("zcard", zCard)
	RegisterCommand("zdiff", zDiff)
	RegisterCommand("zcount", zCount)
	RegisterCommand("zdiffstore", zDiffStore)
	RegisterCommand("zincrby", zIncrBy)
	RegisterCommand("zinterstore", zInterStore)
	RegisterCommand("zpopmax", zPopMax)
	RegisterCommand("zpopmin", zPopMin)
	RegisterCommand("zrank", zRank)
	RegisterCommand("zrevrank", zRevRank)
	RegisterCommand("zscore", zScore)
	RegisterCommand("zrange", zRange)
	RegisterCommand("zrevrange", zRevRange)
	RegisterCommand("zrangebyscore", zRangeByScore)
	RegisterCommand("zrem", zRem)
	RegisterCommand("zremrangebyrank", zRemRangeByRank)
	RegisterCommand("zremrangebyscore", zRemRangeByScore)
	RegisterCommand("zunionstore", zUnionStore)
}
