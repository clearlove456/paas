package data

import (
	"context"
	"encoding/json"

	"github.com/clearlove456/paas/internal/conf"
	"github.com/clearlove456/paas/pkg/cachex"
	"github.com/clearlove456/paas/pkg/rmqx"
	"github.com/redis/go-redis/v9"

	"github.com/go-kratos/kratos/v2/log"
)

// 缓存一致性链路（写路径零侵入，业务代码完全不碰缓存）：
//
//	canal-server(订阅binlog) ──生产──> RocketMQ[paas-canal] ──消费──> 本服务
//	                                                      │
//	                                    ① 立即删一次（本地直接 DEL）
//	                                    ② 发一条 5s 延迟消息 ──> RocketMQ[paas-cache-evict]
//	                                                                      │ 5s 后投递
//	                                    ③ 消费到它，再删一次（延迟双删） <──┘
//
//	延迟双删防的是：读请求在「DB 已提交、淘汰未生效」的窗口内把旧值回填缓存。

const (
	// evictDelayLevel 延迟双删档位（RocketMQ 级别 2 = 5s）
	evictDelayLevel = rmqx.DelayLevel5s
	// delayTopic 延迟双删主题
	delayTopic = "paas-cache-evict"
)

// binlogKeyCfgs binlog 变更表 -> 缓存配置，新增带缓存的表在这里注册
var binlogKeyCfgs = map[string]cachex.Config{
	"assets": assetCacheCfg,
	"apps":   appCacheCfg,
}

// delayMsg 延迟消息体：5s 后要删的缓存 key
type delayMsg struct {
	Keys []string `json:"keys"`
}

// binlogEvictor 缓存淘汰消费者
type binlogEvictor struct {
	rdb    *redis.Client
	sender *rmqx.Sender // 延迟消息发送器
}

// handleBinlog 消费 paas-canal（canal 投递的 binlog 变更）：
// 每行变更立即删一次，并发一条 5s 延迟消息
func (e *binlogEvictor) handleBinlog(ctx context.Context, body []byte) error {
	for _, ev := range rmqx.ParseCanal(body, "id") {
		cfg, ok := binlogKeyCfgs[ev.Table]
		if !ok {
			continue // 不关心的表（canal instance 侧也应配置白名单过滤）
		}

		// ① 第一删：本地直接 DEL，图快
		key := cfg.Key(ev.ID)
		cachex.Evict(ctx, e.rdb, key)

		// ② 发一条延迟消息，5s 后由 handleDelay 执行第二删
		if payload, err := json.Marshal(delayMsg{Keys: []string{key}}); err == nil {
			if err := e.sender.SendDelay(ctx, delayTopic, payload, evictDelayLevel); err != nil {
				log.Warnf("binlog %s %s id=%d send delay msg failed: %v", ev.Type, ev.Table, ev.ID, err)
			}
		}
		log.Infof("binlog %s %s id=%d -> first evict + delay msg sent", ev.Type, ev.Table, ev.ID)
	}
	return nil
}

// handleDelay 消费 paas-cache-evict（自己发的延迟消息到期）：
// ③ 第二删
func (e *binlogEvictor) handleDelay(ctx context.Context, body []byte) error {
	var msg delayMsg
	if json.Unmarshal(body, &msg) == nil {
		cachex.Evict(ctx, e.rdb, msg.Keys...)
		log.Infof("delayed second evict: %v", msg.Keys)
	}
	return nil
}

// startBinlogSubscriber 装配并启动缓存淘汰消费者，返回停止函数
func startBinlogSubscriber(c *conf.Data_Rocketmq, rdb *redis.Client, logger log.Logger) (func(), error) {
	sender, err := rmqx.NewSender(c.NameSrvAddrs)
	if err != nil {
		return nil, err
	}

	e := &binlogEvictor{rdb: rdb, sender: sender}
	listener, err := rmqx.NewListener(rmqx.Config{
		NameServers: c.NameSrvAddrs,
		Group:       c.Group,
		Subscriptions: map[string]rmqx.Handler{
			c.Topic:    e.handleBinlog, // canal 投递 binlog 的主题
			delayTopic: e.handleDelay,  // 延迟双删主题
		},
	})
	if err != nil {
		return nil, err
	}

	if err := sender.Start(); err != nil {
		return nil, err
	}
	if err := listener.Start(); err != nil {
		return nil, err
	}
	log.Infof("cache evictor started: binlog topic=%s, delay topic=%s, group=%s", c.Topic, delayTopic, c.Group)

	return func() {
		listener.Stop()
		sender.Close()
	}, nil
}
