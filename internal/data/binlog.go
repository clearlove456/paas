package data

import (
	"context"

	"github.com/clearlove456/paas/internal/conf"
	"github.com/clearlove456/paas/pkg/cachex"
	"github.com/clearlove456/paas/pkg/canalx"

	"github.com/go-kratos/kratos/v2/log"
)

// evictDelayLevel 延迟双删的第二删档位（RocketMQ 固定级别 2 = 5s），
// 关掉「DB 已提交、淘汰未生效」窗口内的并发回填。
const evictDelayLevel = canalx.DelayLevel5s

// delayTopic 延迟双删主题（写死）
const delayTopic = "paas-cache-evict"

// binlogKeyCfgs binlog 变更表 -> 缓存配置的映射，新增带缓存的表在这里注册
var binlogKeyCfgs = map[string]cachex.Config{
	"assets": assetCacheCfg,
	"apps":   appCacheCfg,
}

// startBinlogSubscriber 订阅 canal 转发的 binlog 消息，淘汰对应缓存。
// 缓存一致性策略：写路径零侵入（业务代码完全不碰缓存），
// 淘汰全部由本订阅驱动，收到变更执行【立即删 + RocketMQ 延迟消息 5s 后二次删】。
func startBinlogSubscriber(c *conf.Data_Rocketmq, d *Data, logger log.Logger) (func(), error) {
	var sub *canalx.Sub
	sub, err := canalx.New(
		canalx.Options{
			NameServers: c.NameSrvAddrs,
			CanalTopic:  c.Topic,
			DelayTopic:  delayTopic,
			Group:       c.Group,
			PK:          "id",
		},
		// binlog 变更：第一删（立即）+ 发延迟消息（第二删）
		func(ctx context.Context, ev canalx.Event) {
			cfg, ok := binlogKeyCfgs[ev.Table]
			if !ok {
				return // 不关心的表（canal instance 也应配置白名单过滤，减少无效投递）
			}
			key := cfg.Key(ev.ID)
			cachex.Evict(ctx, d.rdb, key)
			if err := sub.PublishDelay(ctx, []string{key}, evictDelayLevel); err != nil {
				log.Warnf("binlog %s %s id=%d publish delay evict failed: %v", ev.Type, ev.Table, ev.ID, err)
			}
			log.Infof("binlog %s %s id=%d -> evict(+delay msg)", ev.Type, ev.Table, ev.ID)
		},
		// 延迟消息到期：第二删
		func(ctx context.Context, keys []string) {
			cachex.Evict(ctx, d.rdb, keys...)
			log.Infof("delayed evict done: %v", keys)
		},
		logger,
	)
	if err != nil {
		return nil, err
	}
	return sub.Stop, nil
}
