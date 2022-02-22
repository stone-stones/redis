package redis

import (
	"context"
	"errors"
	"runtime"
	"time"

	"go.uber.org/zap"

	redis "github.com/go-redis/redis/v8"
	"github.com/spf13/cast"
)

const (
	//RedisStartKey context redis cmd start key
	RedisStartKey = TimeKey("redisTimeStart")
	//DebugLogKey context debug key
	DebugLogKey = TimeKey("debugOn")
	//RedisTimeCost key in log for time cost
	RedisTimeCost = "redisTimeCost/us"
	//time unit,us
	nsToMsRate = 1000
)

var (
	//Op redis conn handle
	Op *GoRedisOp
	// Nil redis key not found error
	Nil = redis.Nil
	//ErrorNotAllow redis cmd bind error
	ErrorNotAllow = errors.New("error not allow ")
)

//contextToLog get logger from  context
type contextToLog func(context.Context) Logger

// inDisableCmd redis cmd filter
type inDisableCmd func(string) bool

//GoRedisOp the handle of redis handler
type GoRedisOp struct {
	Cli     *redis.Client
	Config  *Config
	bindCmd map[string]struct{}
}

// Config config store redis config
type Config struct {
	Options      *redis.Options
	Debug        bool
	ContextToLog contextToLog
	BindCmd      map[string]struct{}
}

//TimeKey context key type
type TimeKey string

//InitRedis init the redis handler
func InitRedis(config *Config) (*GoRedisOp, error) {
	var err error
	Op = new(GoRedisOp)
	Op.Cli = redis.NewClient(config.Options)
	Op.Config = config
	Op.bindCmd = make(map[string]struct{})

	for k := range config.BindCmd {
		Op.bindCmd[k] = struct{}{}
	}
	Op.bindCmd["flushdb"] = struct{}{}

	redis.SetLogger(GetLogger())
	hook := GoRedisHook{}
	hook.inDisable = Op.inDisableCmd
	if config.ContextToLog == nil {
		config.ContextToLog = DefaultContextToLogger()
	}
	hook.contextToLog = config.ContextToLog
	Op.Cli.AddHook(hook)

	s := Op.Cli.Ping(context.Background())
	if err = s.Err(); err != nil {
		GetLogger().Error("Redis tcp ping error:%v", err)
		return nil, err
	}

	return Op, nil
}

//SwitchDebug debug switch,if in debug mod ,
//the logger lever should also in debug level to output the debug log
func (op *GoRedisOp) SwitchDebug(on bool) {
	Op.Config.Debug = on
}

//GetDebugContext get a context with debug,use this context in redis cmd,it should output the debug log
func (op *GoRedisOp) GetDebugContext(ctx context.Context) context.Context {
	return context.WithValue(ctx, DebugLogKey, true)
}

func (op *GoRedisOp) inDisableCmd(name string) bool {
	if _, ok := op.bindCmd[name]; ok {
		return true
	}
	return false
}

//DefaultContextToLogger default context extract log function
func DefaultContextToLogger() contextToLog {
	return func(ctx context.Context) Logger {
		return GetLogger()
	}
}

//GoRedisHook hook struct
type GoRedisHook struct {
	inDisable    inDisableCmd
	contextToLog contextToLog
}

//BeforeProcess accomplish for hook interface,handler redis cmd bind and set start time
func (g GoRedisHook) BeforeProcess(ctx context.Context, cmd redis.Cmder) (context.Context, error) {
	ctx = context.WithValue(ctx, RedisStartKey, time.Now().UnixNano()/nsToMsRate)
	if g.inDisable(cmd.Name()) {
		return ctx, ErrorNotAllow
	}
	return ctx, nil

}

//AfterProcess accomplish for hook interface,check result and log debug info
func (g GoRedisHook) AfterProcess(ctx context.Context, cmd redis.Cmder) error {
	ctxDebug, ok := ctx.Value(DebugLogKey).(bool)
	//logger when cmd error or in debug mod
	if cmd.Err() != nil || (ok && ctxDebug) || Op.Config.Debug {
		newLog := g.contextToLog(ctx)
		timeStart := ctx.Value(RedisStartKey).(int64)
		_, file, no, _ := runtime.Caller(4)
		dLog, ok := newLog.(DefaultLog)
		if ok { //if use default zap logger, add zap.Filed
			dLog = DefaultLog{dLog.With(zap.Any("real_caller", file), zap.Any("line", no),
				zap.Any(RedisTimeCost, time.Now().UnixNano()/nsToMsRate-timeStart))}
			if cmd.Err() != nil {
				dLog.Error("redis error:cmd:%v,err:%v", cmd.Args(), cmd.Err())
				return nil
			}
			dLog.Debug("cmd:%v", cmd.Args())
			return nil
		}
		if cmd.Err() != nil {
			newLog.Error("real_caller:%s,line:%d,redisTimeCost/us:%d,redis error:cmd:%v,err:%v", file, no,
				time.Now().UnixNano()/nsToMsRate-timeStart, cmd.Args(), cmd.Err())
			return nil
		}
		newLog.Debug("real_caller:%s,line:%d,redisTimeCost/us:%d,cmd:%v", file, no,
			time.Now().UnixNano()/nsToMsRate-timeStart, cmd.Args())
	}
	return nil

}

//BeforeProcessPipeline accomplish for hook interface,handler redis cmd bind and set start time
func (g GoRedisHook) BeforeProcessPipeline(ctx context.Context, cmds []redis.Cmder) (context.Context, error) {
	ctx = context.WithValue(ctx, RedisStartKey, time.Now().UnixNano()/nsToMsRate)
	for _, cmd := range cmds {
		if Op.inDisableCmd(cmd.Name()) {
			return ctx, ErrorNotAllow
		}
	}
	return ctx, nil

}

//AfterProcessPipeline  accomplish for hook interface,check result and log debug info
func (g GoRedisHook) AfterProcessPipeline(ctx context.Context, cmds []redis.Cmder) error {
	ctxDebug, ok := ctx.Value(DebugLogKey).(bool)
	var cmdErr redis.Cmder
	for _, v := range cmds {
		if v.Err() != nil {
			cmdErr = v
			break
		}
	}
	//logger when cmd error or in debug mod
	if (cmdErr != nil && cmdErr.Err() != nil) || (ok && ctxDebug) || Op.Config.Debug {
		newLog := g.contextToLog(ctx)
		timeStart := ctx.Value(RedisStartKey).(int64)
		_, file, no, _ := runtime.Caller(4)
		var args string
		for _, v := range cmds {
			for _, a := range v.Args() {
				args += cast.ToString(a)
				args += " "
			}
			args += ";  "
		}
		dLog, ok := newLog.(DefaultLog)

		if ok { //if use default zap logger, add zap.Filed
			dLog = DefaultLog{dLog.With(zap.Any("real_caller", file), zap.Any("line", no),
				zap.Any(RedisTimeCost, time.Now().UnixNano()/nsToMsRate-timeStart))}
			if cmdErr != nil && cmdErr.Err() != nil {
				dLog.Error("redis error:cmd:%v,err:%v", cmdErr.Args(), cmdErr.Err())
				return nil
			}
			newLog.Debug("redis pipeline cmds:%s", args)
			return nil
		}
		if cmdErr != nil && cmdErr.Err() != nil {
			newLog.Error("real_caller:%s,line:%d,redisTimeCost/us:%d,redis error:cmd:%v,err:%v", file, no,
				time.Now().UnixNano()/nsToMsRate-timeStart, cmdErr.Args(), cmdErr.Err())
			return nil
		}

		if cmdErr != nil && cmdErr.Err() != nil {
			newLog.Error("real_caller:%s,line:%d,redisTimeCost/us:%d,redis error:cmd:%v,err:%v", file, no,
				time.Now().UnixNano()/nsToMsRate-timeStart, cmdErr.Args(), cmdErr.Err())
			return nil
		}
		newLog.Debug("real_caller:%s,line:%d,redisTimeCost/us:%d,cmd:%v", file, no,
			time.Now().UnixNano()/nsToMsRate-timeStart, args)
	}
	return nil
}

//IsRedisNilErr check the error is redis.Nil type
func IsRedisNilErr(err error) error {
	if err != nil && err == redis.Nil {
		return nil
	}

	return err
}
