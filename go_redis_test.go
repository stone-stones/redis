package redis

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/alicebob/miniredis"
	goRedis "github.com/go-redis/redis/v8"
	"go.uber.org/zap"
)

func TestExampleSetAndGet(t *testing.T) {
	initRedisByExample()
	//should not log when debug mod off
	RedisOp.Cli.Set(ctx, "aaa", "bbb", time.Second)
	//should log an error msg
	RedisOp.Cli.HGet(ctx, "aaa", "bbb")
}

func TestPipeline(t *testing.T) {
	initRedisByExample()
	p := RedisOp.Cli.Pipeline()
	//no log when debug mod off
	p.Set(ctx, "aaa", "aaa", time.Second)
	//no log when debug mod off
	p.Set(ctx, "bbb", "aaa", time.Second)
	_, err := p.Exec(ctx)
	if err != nil {
		GetLogger().Error("error: %v  ", err)
	}

	p1 := RedisOp.Cli.Pipeline()
	//not log when debug mod off
	p1.Set(ctx, "aaa", "aaa", time.Second)
	//log an error msg
	p1.HDel(ctx, "ccc", "aaa")
	_, err = p1.Exec(ctx)
	if err != nil {
		GetLogger().Error("error: %v  ", err)
	}
}

func BenchmarkGoRedis(b *testing.B) {
	initRedisByExample()
	for n := 0; n < b.N; n++ {
		RedisOp.Cli.Set(ctx, "aaaa", "bbbbb", time.Second)
		RedisOp.Cli.Get(ctx, "aaaa")
	}
}

func TestDebugContext(t *testing.T) {
	initRedisByExample()
	//no log msg
	RedisOp.Cli.Set(ctx, "shoulenotshow", "sdddd", time.Second)
	ctxDebug := RedisOp.GetDebugContext(ctx)
	//one log msg
	RedisOp.Cli.Set(ctxDebug, "shouldShow", "sdddd", time.Second)
	//no log msg
	RedisOp.Cli.Set(ctx, "willNoLog", "sdddd", time.Second)
}

func TestDebugSwitch(t *testing.T) {
	initRedisByExample()
	RedisOp.SwitchDebug(true)
	//one log msg
	RedisOp.Cli.Set(ctx, "shouldShow", "sdddd", time.Second)
	RedisOp.SwitchDebug(false)
	//no log msg
	RedisOp.Cli.Set(ctx, "shoulenotshow", "sdddd", time.Second)
}

func TestBindCmd(t *testing.T) {
	initRedisByExample()
	//one error msg
	err := RedisOp.Cli.FlushDB(ctx).Err()
	if err != nil && err.Error() != ErrorNotAllow.Error() {
		t.FailNow()
	}
}

type NewLogger struct {
	*zap.Logger
}

func (r NewLogger) Printf(ctx context.Context, format string, v ...interface{}) {
	r.Sugar().Infof(format, v...)
}

func (d NewLogger) Debug(format string, v ...interface{}) {
	d.Sugar().Infof(format, v...)
}

func (d NewLogger) Error(format string, v ...interface{}) {
	d.Sugar().Errorf(format, v...)
}

func TestSetLogger(t *testing.T) {
	initRedisByExample()
	config := zap.NewProductionConfig()
	config.DisableStacktrace = true
	config.OutputPaths = []string{"./tmp.log"}
	config.Level = zap.NewDevelopmentConfig().Level
	newLogger, err := config.Build()
	if err != nil {
		t.FailNow()
	}
	SetterLogger(NewLogger{newLogger})
	//no log output to file 111.log
	RedisOp.Cli.Set(ctx, "shoulenotshow", "sdddd", time.Second)
	//one error log output to file tmp.log
	RedisOp.Cli.HGet(ctx, "shoulenotshow", "sdddd")
	os.Remove("./tmp.log")

}

type ExampleKey string

var (
	//RedisOp global redis handler
	RedisOp *GoRedisOp
	//RedisStartKey context
	ExampleTimeKey = ExampleKey("exampleTimeKey")
	ctx            = context.Background()
)

//redis mock client
func initRedisByExample() {
	mr, err := miniredis.Run()
	if err != nil {
		GetLogger().Error("error: %v  ", err)
	}
	cfg := Config{}
	cfg.Options = new(goRedis.Options)
	cfg.Options.Addr = mr.Addr()

	RedisOp, err = InitRedis(&cfg)
	if err != nil {
		GetLogger().Error("InitRedis error ", err)
		return
	}
}
