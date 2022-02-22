package redis

import (
	"context"
	"fmt"
	"os"

	"go.uber.org/zap"
)

var redisLogger Logger

type Logger interface {
	Debug(string, ...interface{})
	Error(string, ...interface{})
	Printf(ctx context.Context, format string, v ...interface{})
}

//Log 日志对象
type DefaultLog struct {
	*zap.Logger
}

//Printf  日志方法
func (d DefaultLog) Printf(ctx context.Context, format string, v ...interface{}) {
	d.Info(fmt.Sprintf(format, v...))
}

func (d DefaultLog) Debug(format string, v ...interface{}) {
	d.Logger.Debug(fmt.Sprintf(format, v...))
}

func (d DefaultLog) Error(format string, v ...interface{}) {
	d.Logger.Error(fmt.Sprintf(format, v...))
}
func GetLogger() Logger {
	return redisLogger
}

func SetterLogger(log Logger) {
	redisLogger = log
}
func init() {
	log, err := zap.NewDevelopment()
	if err != nil {
		fmt.Printf("init NewDevelopment log error:%v", err)
		os.Exit(1)
	}
	redisLogger = DefaultLog{log}
}

type LoggerInfo struct {
	Caller   string
	Line     int
	TimeCost int64
	Msg      string
}
