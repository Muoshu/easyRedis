package util

import (
	"easyRedis/logger"
	"hash/fnv"
)

func HashKey(key string) (int, error) {
	fnv32 := fnv.New32()
	key = key + "@#&"
	_, err := fnv32.Write([]byte(key))
	if err != nil {
		logger.Error("HashKey error: %v", err)
		return -1, err
	}
	return int(fnv32.Sum32()), nil
}
