// Package cachex 通用 Cache-Aside 缓存组件，内置三防：
// 防击穿（singleflight）、防雪崩（TTL 随机抖动）、防穿透（空值缓存）。
package cachex

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"time"

	"github.com/go-kratos/kratos/v2/log"
	"github.com/redis/go-redis/v9"
	"golang.org/x/sync/singleflight"
)

// NullMarker 穿透防护：查不到的数据写入这个标记（而不是不写），
// 下次同样的查询直接命中标记返回"不存在"，不再打到 DB。
const NullMarker = "\x00NULL\x00"

// Config 缓存策略配置，每个业务实体一份
type Config struct {
	Prefix  string        // key 前缀，如 paas:asset:
	TTL     time.Duration // 正常缓存时长
	Jitter  time.Duration // 雪崩防护：实际 TTL = TTL + rand(0, Jitter]，避免同时过期
	NullTTL time.Duration // 穿透防护：空值标记的缓存时长（要短，防止新数据写入后长时间不可见）
}

// Key 由业务 ID 构造缓存 key
func (c Config) Key(id int64) string {
	return fmt.Sprintf("%s%d", c.Prefix, id)
}

// jitterTTL 给 TTL 加随机抖动（防雪崩）
func jitterTTL(base, jitter time.Duration) time.Duration {
	if jitter <= 0 {
		return base
	}
	return base + time.Duration(rand.Int63n(int64(jitter)))
}

// Aside 通用读路径：Cache-Aside + 三防。
//  1. 防击穿：singleflight 把同一 key 的并发回源合并成一次 DB 查询
//  2. 防雪崩：回填 TTL 加随机抖动
//  3. 防穿透：数据不存在时写空值标记，短 TTL
//
// 缓存一致性采用双保险：本函数只管读；写路径由调用方"先更新 DB 再淘汰缓存"（Evict），
// 另有 canal 订阅 binlog 的兜底淘汰，多实例/淘汰失败也不会长期不一致。
//
// notFound: 判断 loader 的错误是否"数据不存在"；nullErr: 命中空值标记时返回的业务错误。
func Aside[T any](ctx context.Context, rdb *redis.Client, sf *singleflight.Group,
	cfg Config, key string,
	loader func(context.Context) (*T, error),
	notFound func(error) bool, nullErr error) (*T, error) {

	// 1. 读缓存：命中直接返回
	if raw, err := rdb.Get(ctx, key).Result(); err == nil {
		if raw == NullMarker {
			return nil, nullErr // 穿透防护命中
		}
		var v T
		if json.Unmarshal([]byte(raw), &v) == nil {
			return &v, nil
		}
		// 反序列化失败（如结构变更）：当作未命中继续回源
	} else if err != redis.Nil {
		// Redis 故障不阻塞业务：降级直查 DB
		log.Warnf("cache get %s failed, fallback to db: %v", key, err)
	}

	// 2. singleflight 合并同 key 的并发回源（防击穿）
	v, err, _ := sf.Do(key, func() (interface{}, error) {
		out, err := loader(ctx)
		if err != nil {
			if notFound(err) {
				if serr := rdb.Set(ctx, key, NullMarker, cfg.NullTTL).Err(); serr != nil {
					log.Warnf("cache set null %s failed: %v", key, serr)
				}
			}
			return nil, err
		}
		if data, merr := json.Marshal(out); merr == nil {
			if serr := rdb.Set(ctx, key, data, jitterTTL(cfg.TTL, cfg.Jitter)).Err(); serr != nil {
				log.Warnf("cache set %s failed: %v", key, serr)
			}
		}
		return out, nil
	})
	if err != nil {
		return nil, err
	}
	return v.(*T), nil
}

// Evict 写路径淘汰缓存（本地写后淘汰 + canal 订阅淘汰 双保险的本地侧）
func Evict(ctx context.Context, rdb *redis.Client, keys ...string) {
	if len(keys) == 0 {
		return
	}
	if err := rdb.Del(ctx, keys...).Err(); err != nil {
		log.Warnf("cache evict %v failed: %v", keys, err)
	}
}
