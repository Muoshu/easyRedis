package datastructure

import (
	"strconv"
)

type Hash struct {
	table map[string][]byte
}

func NewHash() *Hash {
	return &Hash{make(map[string][]byte)}
}

func (h *Hash) Set(key string, val []byte) {
	h.table[key] = val
}

func (h *Hash) Get(key string) []byte {
	return h.table[key]
}

func (h *Hash) Del(key string) int {
	if h.Exist(key) {
		delete(h.table, key)
		return 1
	}
	return 0
}

func (h *Hash) Keys() []string {
	keys := make([]string, 0, len(h.table))
	for key, _ := range h.table {
		keys = append(keys, key)
	}
	return keys
}

func (h *Hash) Values() [][]byte {
	values := make([][]byte, 0, len(h.table))
	for _, v := range h.table {
		values = append(values, v)
	}
	return values
}

func (h *Hash) All() [][]byte {
	res := make([][]byte, 0, 2*len(h.table))
	for k, v := range h.table {
		res = append(res, []byte(k), v)
	}
	return res
}

func (h *Hash) Clear() {
	h.table = make(map[string][]byte)
}

func (h *Hash) Len() int {
	return len(h.table)
}

func (h *Hash) IsEmpty() bool {
	return len(h.table) == 0
}

func (h *Hash) Exist(key string) bool {
	_, ok := h.table[key]
	return ok
}
func (h *Hash) StrLen(key string) int {
	return len(h.table[key])
}

func (h *Hash) Random(count int) []string {
	var res []string
	if count == 0 || h.Len() == 0 {
		return res
	} else if count > 0 {
		if count > h.Len() {
			count = h.Len()
		}
		res = make([]string, count)
		for key := range h.table {
			res = append(res, key)
			if len(res) == count {
				break
			}
		}
	} else {
		res = make([]string, -count)
		for { // 当count<0,返回-count个元素（-count可能会大于h.Len()）
			for key := range h.table {
				res = append(res, key)
				if len(res) == -count {
					return res
				}
			}
		}
	}
	return res
}

func (h *Hash) RandomWithValue(count int) [][]byte {
	var res [][]byte
	if count == 0 || h.Len() == 0 {
		return res
	} else if count > 0 {
		if count >= h.Len() {
			count = h.Len()
		}
		count *= 2
		res = make([][]byte, count)
		for k, v := range h.table {
			res = append(res, []byte(k), v)
			if len(res) == count {
				break
			}
		}
	} else {
		count *= 2
		res = make([][]byte, -count)
		for {
			for key, val := range h.table {
				res = append(res, []byte(key), val)
				if len(res) == -count {
					return res
				}
			}
		}
	}
	return res
}

func (h *Hash) Table() map[string][]byte {
	return h.table
}

func (h *Hash) IncrBy(key string, incr int) (int, bool) {
	temp := h.Get(key)
	if len(temp) == 0 {
		h.Set(key, []byte(strconv.Itoa(incr)))
		return incr, true
	}
	val, err := strconv.Atoi(string(temp))
	if err != nil {
		return 0, false
	}
	val += incr
	h.Set(key, []byte(strconv.Itoa(val)))
	return val, true
}

func (h *Hash) IncrByFloat(key string, incr float64) (float64, bool) {
	temp := h.Get(key)
	if len(temp) == 0 {
		h.Set(key, []byte(strconv.FormatFloat(incr, 'f', -1, 64)))
		return incr, true
	}
	val, err := strconv.ParseFloat(string(temp), 64)
	if err != nil {
		return 0, false
	}
	val += incr
	h.Set(key, []byte(strconv.FormatFloat(val, 'f', -1, 64)))
	return val, true
}
