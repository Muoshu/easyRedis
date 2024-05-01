package timewheel

import (
	"easyRedis/config"
	"easyRedis/logger"
	"fmt"
	"os"
	"testing"
	"time"
)

func TestAdd(t *testing.T) {
	cfg, err := config.Setup()
	if err != nil {
		fmt.Println(err.Error())
		os.Exit(1)
	}
	err = logger.Setup(cfg)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	ch := make(chan time.Time)
	beginTime := time.Now()
	Add(time.Second, "", func() {
		logger.Debug("exec task...")
		ch <- time.Now()
	})
	execAt := <-ch
	delayDuration := execAt.Sub(beginTime)
	// usually 1.0~2.0 s
	if delayDuration < time.Second || delayDuration > 3*time.Second {
		t.Error("wrong execute time")
	}
}

func TestAddTask(t *testing.T) {

	Add(0*time.Second, "test0", func() {
		logger.Info("0 time.Second running")
		time.Sleep(10 * time.Second)
	})

	time.Sleep(1500 * time.Millisecond)

	Add(9*time.Second, "testKey", func() {
		logger.Info("9 time.Second running")
		time.Sleep(5 * time.Second)
	})

	time.Sleep(14 * time.Second)
}

func TestCancelTask(t *testing.T) {
	Add(0*time.Second, "test0", func() {
		logger.Info("0 time.Second running")
		time.Sleep(10 * time.Second)
	})

	time.Sleep(1500 * time.Millisecond)

	Add(9*time.Second, "testKey", func() {
		logger.Info("9 time.Second running")
		time.Sleep(5 * time.Second)
	})

	Cancel("testKey")
	time.Sleep(14 * time.Second)
}
