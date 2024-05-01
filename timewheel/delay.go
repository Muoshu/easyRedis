package timewheel

import (
	"time"
)

type Delay struct {
	tw *TimeWheel
}

func NewDelay() *Delay {
	delay := &Delay{}
	delay.tw = New(100*time.Millisecond, 3600)
	delay.tw.Start()
	return delay
}

// Add 添加延迟任务 相对时间
func (d *Delay) Add(interval time.Duration, key string, callback func()) {
	d.tw.Add(interval, key, callback)
}

// Cancel 取消延迟任务
func (d *Delay) Cancel(key string) {
	d.tw.Cancel(key)
}

func (d *Delay) Stop() {
	d.tw.Stop()
}
