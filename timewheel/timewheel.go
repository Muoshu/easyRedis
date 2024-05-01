package timewheel

import (
	"container/list"
	"easyRedis/logger"
	"time"
)

// 记录任务task位于循环队列的哪一个链表上
type taskPos struct {
	pos int
	ele *list.Element
}

type task struct {
	delay    time.Duration
	key      string
	circle   int
	callback func()
}

// TimeWheel 循环队列+链表
type TimeWheel struct {
	// 间隔
	interval time.Duration
	// 定时器
	ticker *time.Ticker
	// 游标
	curSlotPos int
	// 循环队列大小
	slotNum int
	// 底层存储
	slots []*list.List
	m     map[string]*taskPos

	addChannel    chan *task
	cancelChannel chan string
	stopChannel   chan struct{}
}

func New(interval time.Duration, slotNum int) *TimeWheel {
	timeWheel := &TimeWheel{
		ticker:        nil,
		interval:      interval,
		slotNum:       slotNum,
		slots:         make([]*list.List, slotNum),
		m:             make(map[string]*taskPos),
		addChannel:    make(chan *task),
		cancelChannel: make(chan string),
		stopChannel:   make(chan struct{}),
	}
	for i := 0; i < slotNum; i++ {
		timeWheel.slots[i] = list.New()
	}
	return timeWheel
}

func (tw *TimeWheel) doTask() {
	for {
		select {
		case <-tw.ticker.C:
			tw.execTask()
		case t := <-tw.addChannel:
			tw.addTask(t)
		case key := <-tw.cancelChannel:
			tw.cancelTask(key)
		case <-tw.stopChannel:
			tw.Stop()
			return
		}
	}
}

func (tw *TimeWheel) execTask() {
	l := tw.slots[tw.curSlotPos]
	if tw.curSlotPos == tw.slotNum-1 {
		tw.curSlotPos = 0
	} else {
		tw.curSlotPos++
	}

	go tw.scanList(l)
}

func (tw *TimeWheel) scanList(l *list.List) {
	for e := l.Front(); e != nil; {
		t := e.Value.(*task)
		// 任务不在当前圈执行
		if t.circle > 0 {
			t.circle--
			continue
		}
		go func() {
			defer func() {
				if err := recover(); err != nil {
					logger.Error(err)
				}
			}()
			//协程中执行任务
			call := t.callback
			call()
		}()

		// 下一个记录
		next := e.Next()
		// 链表中删除
		l.Remove(e)
		if t.key != "" {
			delete(tw.m, t.key)
		}
		e = next
	}
}

func (tw *TimeWheel) posAndCircle(d time.Duration) (pos, circle int) {
	//延时（毫秒）
	delayMilliSecond := int(d.Milliseconds())
	//间隔（毫秒）
	intervalMilliSecond := int(tw.interval.Milliseconds())
	// delaySecond/intervalSecond 表示从curSlotPos位置偏移
	pos = (tw.curSlotPos + delayMilliSecond/intervalMilliSecond) % tw.slotNum
	circle = (delayMilliSecond / intervalMilliSecond) / tw.slotNum

	return
}

func (tw *TimeWheel) addTask(t *task) {
	pos, circle := tw.posAndCircle(t.delay)
	t.circle = circle
	//保存到循环队列pos位置
	ele := tw.slots[pos].PushBack(t)
	// 在map中记录 key -> { pos, ele } 的映射
	if t.key != "" {
		// 已经存在重复的key
		if _, ok := tw.m[t.key]; ok {
			tw.cancelTask(t.key)
		}
		tw.m[t.key] = &taskPos{
			pos: pos,
			ele: ele,
		}
	}
}

func (tw *TimeWheel) cancelTask(key string) {
	taskPos, ok := tw.m[key]
	if !ok {
		return
	}
	// 从循环队列链表中删除任务
	tw.slots[taskPos.pos].Remove(taskPos.ele)
	delete(tw.m, key)
}

/******外部调用*******/

func (tw *TimeWheel) Start() {
	tw.ticker = time.NewTicker(tw.interval)
	go tw.doTask()
}

func (tw *TimeWheel) Stop() {
	tw.stopChannel <- struct{}{}
}

func (tw *TimeWheel) Add(delay time.Duration, key string, callback func()) {
	if delay < 0 {
		return
	}
	t := &task{
		delay:    delay,
		key:      key,
		callback: callback,
	}
	//发送到channel中
	tw.addChannel <- t
}

func (tw *TimeWheel) Cancel(key string) {
	tw.cancelTask(key)
}
