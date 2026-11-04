// Package consumer 统一管理本项目所有 MQ 消费者。
//
// 新增消费者只有两步：
//  1. 在本包写一个消费者文件（handler + 所需依赖），参考 cache_evict.go
//  2. 在下面的 registry() 登记一条 rmqx.Consumer
//
// 启动/停止/优雅退出全部由 Manager 托管，无需改其他任何代码。
package consumer

import (
	"context"

	"github.com/clearlove456/paas/internal/conf"
	"github.com/clearlove456/paas/internal/data"
	"github.com/clearlove456/paas/pkg/rmqx"

	"github.com/go-kratos/kratos/v2/log"
	"github.com/google/wire"
)

// ProviderSet is consumer providers.
var ProviderSet = wire.NewSet(NewManager)

// Manager MQ 消费者统一管理器，实现 kratos Server 接口，
// 在 cmd/server/main.go 的 kratos.Server(...) 中注册启停。
type Manager struct {
	rmq      *rmqx.Manager
	sender   *rmqx.Sender
	disabled bool // rocketmq 未启用时的 no-op 实例
}

// NewManager 构建全部消费者；rocketmq 未启用时返回空实现
func NewManager(c *conf.Data, d *data.Data) (*Manager, error) {
	if c.Rocketmq == nil || !c.Rocketmq.Enabled {
		log.Info("rocketmq disabled, mq consumers skipped")
		return &Manager{disabled: true}, nil
	}

	sender, err := rmqx.NewSender(c.Rocketmq.NameSrvAddrs)
	if err != nil {
		return nil, err
	}

	// ★ 全项目 MQ 消费者注册表——统一管理就在这里
	consumers := registry(c.Rocketmq, d, sender)

	m, err := rmqx.NewManager(c.Rocketmq.NameSrvAddrs, consumers)
	if err != nil {
		return nil, err
	}
	return &Manager{rmq: m, sender: sender}, nil
}

// registry 消费者注册表：一个消费者 = 一个消费组订阅一个 topic（Group 互不相同）。
// 新增消费者在这里加一条 rmqx.Consumer。
func registry(c *conf.Data_Rocketmq, d *data.Data, sender *rmqx.Sender) []rmqx.Consumer {
	cacheEvictor := newCacheEvictor(d.Redis(), sender)

	return []rmqx.Consumer{
		{
			Name:    "cache-evict-binlog", // 缓存淘汰①：binlog -> 第一删 + 发延迟消息
			Group:   c.Group,
			Topic:   c.Topic,
			Handler: cacheEvictor.handleBinlog,
		},
		{
			Name:    "cache-evict-delay", // 缓存淘汰②：5s 后的第二删
			Group:   delayGroup,
			Topic:   delayTopic,
			Handler: cacheEvictor.handleDelay,
		},
		// 以后的新消费者在这里追加一条，例如：
		// {Name: "notify", Group: "paas-notify", Topic: "paas-notify", Handler: ...},
	}
}

// Start 实现 kratos Server 接口
func (m *Manager) Start(_ context.Context) error {
	if m.disabled {
		return nil
	}
	if err := m.sender.Start(); err != nil {
		return err
	}
	return m.rmq.Start()
}

// Stop 实现 kratos Server 接口
func (m *Manager) Stop(_ context.Context) error {
	if m.disabled {
		return nil
	}
	m.rmq.Stop()
	m.sender.Close()
	return nil
}
