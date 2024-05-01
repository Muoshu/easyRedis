package logger

import (
	"easyRedis/config"
	"fmt"
	"io"
	"log"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"sync"
)

type LogLevel int

type LogConfig struct {
	Path  string
	Name  string
	Level LogLevel
}

const (
	DEBUG LogLevel = iota
	INFO
	WARNING
	ERROR
	PANIC
)

var (
	logFile            *os.File
	logger             *log.Logger
	logMu              sync.Mutex
	levelLabels        = []string{"debug", "info", "warning", "error", "panic"}
	logcfg             *LogConfig
	defaultCallerDepth = 2  //追溯到调用函数的上一层的栈帧数，0：当前函数调用的行数，1：上一层函数的调用函数，2：上两层函数的调用位置
	logPrefix          = "" //日志的前缀
)

func Setup(cfg *config.Config) error {
	var err error
	logcfg = &LogConfig{
		Path:  cfg.LogDir,
		Name:  "redis.log",
		Level: INFO,
	}
	for i, v := range levelLabels {
		if v == cfg.LogLevel {
			logcfg.Level = LogLevel(i)
			break
		}
	}

	if _, err = os.Stat(logcfg.Path); err != nil {
		mkErr := os.Mkdir(logcfg.Path, 0755)
		if mkErr != nil {

			return mkErr
		}
	}
	logFile := path.Join(logcfg.Path, logcfg.Name)
	lofFile, err := os.OpenFile(logFile, os.O_RDWR|os.O_CREATE, 0755)
	if err != nil {
		return err
	}
	writer := io.MultiWriter(os.Stdout, lofFile)
	logger = log.New(writer, "", log.LstdFlags)
	return nil
}

func Debug(v ...any) {
	if logcfg.Level > DEBUG {
		return
	}
	logMu.Lock()
	defer logMu.Unlock()
	setPrefix(DEBUG)
	logger.Println(v)
}

func Info(v ...any) {
	if logcfg.Level > INFO {
		return
	}
	logMu.Lock()
	defer logMu.Unlock()
	setPrefix(INFO)
	logger.Println(v)
}

func Warning(v ...any) {
	if logcfg.Level > WARNING {
		return
	}
	logMu.Lock()
	defer logMu.Unlock()
	setPrefix(WARNING)
	logger.Println(v)
}

func Error(v ...any) {
	if logcfg.Level > ERROR {
		return
	}
	logMu.Lock()
	defer logMu.Unlock()
	setPrefix(ERROR)
	logger.Println(v)
}

func Panic(v ...any) {
	if logcfg.Level > PANIC {
		return
	}
	logMu.Lock()
	defer logMu.Unlock()
	setPrefix(PANIC)
	logger.Println(v)
}

func setPrefix(level LogLevel) {
	_, file, line, ok := runtime.Caller(defaultCallerDepth)
	if ok {
		logPrefix = fmt.Sprintf("[%s][%s:%d] ", levelLabels[level], filepath.Base(file), line)
	} else {
		logPrefix = fmt.Sprintf("[%s] ", levelLabels[level])
	}
	logger.SetPrefix(logPrefix)
}

func Disable() {
	logger.SetOutput(io.Discard)
}
