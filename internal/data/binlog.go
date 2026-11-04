package data

import (
	"context"
	"encoding/json"

	"github.com/clearlove456/paas/internal/conf"
	"github.com/clearlove456/paas/pkg/cachex"
	"github.com/clearlove456/paas/pkg/rmqx"

	"github.com/go-kratos/kratos/v2/log"
)

// 缓存一致性链路（写路径零侵入，业务代码完全不碰缓存）：
//
//	canal-server(订阅 binlog) ──生产──> RocketMQ[paas-canal] ──消费──> 本服务
//	    收到 binlog 变更: 立即删一次 + 发 5s 延迟消息
//	RocketMQ[paas-cache-evict] 5s 后投递 ──消费──> 本服务再删一次（延迟双删，
//	关掉「DB 已提交、淘汰未生效」窗口内并发回填旧值的竞争）

const (
	// evictDelayLevel 延迟双删第二删档位（RocketMQ 级别 2 = 5s）
	evictDelayLevel = rmqx.DelayLevel5s
	// delayTopic 延迟双删主题（写死）
	delayTopic = "paas-cache-evict"
)

// binlogKeyCfgs binlog 变更表 -> 缓存配置的映射，新增带缓存的表在这里注册
var binlogKeyCfgs = map[string]cachex.Config{
	"assets": assetCacheCfg,
	"apps":   appCacheCfg,
}

// delayMsg 延迟消息体
type delayMsg struct {
	Keys []string `json:"keys"`
}

// startBinlogSubscriber 监听 MQ 消息删除缓存。
// 主题 c.Topic 由 canal 投递 binlog 变更，主题 delayTopic 是自身的延迟双删消息。
func startBinlogSubscriber(c *conf.Data_Rocketmq, d *Data, logger log.Logger) (func(), error) {
	sender, err := rmqx.NewSender(c.NameSrvAddrs)
	if err != nil {
		return nil, err
	}

	// binlog 变更：第一删（立即）+ 发延迟消息（第二删）
	onBinlog := func(ctx context.Context, body []byte) error {
		for _, ev := range rmqx.ParseCanal(body, "id") {
			cfg, ok := binlogKeyCfgs[ev.Table]
			if !ok {
				continue // 不关心的表（canal instance 侧也应配置白名单过滤）
			}
			key := cfg.Key(ev.ID)
			cachex.Evict(ctx, d.rdb, key)
			if payload, err := json.Marshal(delayMsg{Keys: []string{key}}); err == nil {
				if err := sender.SendDelay(ctx, delayTopic, payload, evictDelayLevel); err != nil {
					log.Warnf("binlog %s %s id=%d send delay msg failed: %v", ev.Type, ev.Table, ev.ID, err)
				}
			}
			log.Infof("binlog %s %s id=%d -> evict(+delay msg)", ev.Type, ev.Table, ev.ID)
		}
		return nil
	}

	// 延迟消息到期：第二删
	onDelay := func(ctx context.Context, body []byte) error {
		var msg delayMsg
		if json.Unmarshal(body, &msg) == nil {
			cachex.Evict(ctx, d.rdb, msg.Keys...)
		}
		return nil
	}

	stop, err := rmqx.Listen(rmqx.Config{
		NameServers: c.NameSrvAddrs,
		Group:       c.Group,
		Subscriptions: map[string]rmqx.Handler{
			c.Topic:    onBinlog, // canal 投递 binlog 的主题
			delayTopic: onDelay,  // 延迟双删主题
		},
	}, logger)
	if err != nil {
		sender.Close()
		return nil, err
	}
	return func() {
		stop()
		sender.Close()
	}, nil
}
