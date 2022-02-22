# Redis tract log for go-redis

![build workflow](https://github.com/go-redis/redis/actions/workflows/build.yml/badge.svg)
[![PkgGoDev](https://pkg.go.dev/badge/github.com/go-redis/redis/v8)](https://pkg.go.dev/github.com/go-redis/redis/v8?tab=doc)
[![Documentation](https://img.shields.io/badge/redis-documentation-informational)](https://redis.uptrace.dev/)
[![Chat](https://discordapp.com/api/guilds/752070105847955518/widget.png)](https://discord.gg/rWtp5Aj)



## Ecosystem
- [Go Redis](https://github.com/go-redis/redis).
- [Redis Mock](https://github.com/go-redis/redismock).
- [Distributed Locks](https://github.com/bsm/redislock).
- [Redis Cache](https://github.com/go-redis/cache).
- [Rate limiting](https://github.com/go-redis/redis_rate).

## Installation

go-redis supports 2 last Go versions and requires a Go version with
[modules](https://github.com/golang/go/wiki/Modules) support. So make sure to initialize a Go
module:

```shell
go mod init github.com/my/repo
```

And then install go-redis/v8 (note _v8_ in the import; omitting it is a popular mistake):

```shell
go get github.com/go-redis/redis/v8
```

## Quickstart

```go
import (
    "context"
    goRedis "github.com/go-redis/redis/v8"
    "github.com/stone-stones/redis/"
)

var ctx = context.Background()

func ExampleClient() {
    opt := &goRedis.Options{
        Addr:     "localhost:6379",
        Password: "", // no password set
        DB:       0,  // use default DB
    })
    cfg := redis.Config{}
	cfg.Options = opt

	RedisOp, err = InitRedis(&cfg)
	if err != nil {
		GetLogger().Error("InitRedis error ", err)
		return
	}

    err := rdb.Set(ctx, "key", "value", 0).Err()
    if err != nil {
        panic(err)
    }

    val, err := rdb.Get(ctx, "key").Result()
    if err != nil {
        panic(err)
    }
    fmt.Println("key", val)

    val2, err := rdb.Get(ctx, "key2").Result()
    if err == redis.Nil {
        fmt.Println("key2 does not exist")
    } else if err != nil {
        panic(err)
    } else {
        fmt.Println("key2", val2)
    }
    // Output: key value
    // key2 does not exist
}
```

