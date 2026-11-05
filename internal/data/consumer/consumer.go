// Package consumer 统一管理本项目所有 MQ 消费者。
//
// 新增消费者只有两步：
//  1. 在本包写一个消费者文件（handler + 所需依赖），参考 cache_evict.go
//  2. 在下面的 registry() 登记一条 rmqx.Consumer（name + handler）
//  3. 在 config.yaml 的 data.rocketmq.consumers 下按 name 配置 group/topic
//
// 启动/停止/优雅退出全部由 Manager 托管，无需改其他任何代码。
package consumer

import (
	"context"
	"fmt"

	"github.com/clearlove456/paas/internal/conf"
	"github.com/clearlove456/paas/internal/data"
	"github.com/clearlove456/paas/pkg/rmqx"

	"github.com/go-kratos/kratos/v2/log"
	"github.com/google/wire"
)

// ProviderSet is consumer providers.
var ProviderSet = wire.NewSet(NewManager)

// 消费者全局标识，与 config.yaml data.rocketmq.consumers 下的 key 一一对应
const (
	consumerBinlog = "cache-evict-binlog"
	consumerDelay  = "cache-evict-delay"
)

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

	// ★ 全项目 MQ 消费者注册表——统一管理就在这里。
	// 只登记 name + handler，订阅的 group/topic 按 name 从配置解析
	consumers, err := resolve(c.Rocketmq, registry(c.Rocketmq, d, sender))
	if err != nil {
		return nil, err
	}

	m, err := rmqx.NewManager(c.Rocketmq.NameSrvAddrs, consumers)
	if err != nil {
		return nil, err
	}
	return &Manager{rmq: m, sender: sender}, nil
}

// registry 消费者注册表：新增消费者在这里登记一条 {Name, Handler}。
// 对应的 group/topic 在 config.yaml 的 data.rocketmq.consumers.<name> 下配置。
func registry(c *conf.Data_Rocketmq, d *data.Data, sender *rmqx.Sender) []rmqx.Consumer {
	cacheEvictor := newCacheEvictor(d.Redis(), sender, c.Consumers[consumerDelay].GetTopic())

	return []rmqx.Consumer{
		{Name: consumerBinlog, Handler: cacheEvictor.handleBinlog},
		{Name: consumerDelay, Handler: cacheEvictor.handleDelay},
		// 以后的新消费者在这里追加一条，例如：
		// {Name: "notify", Handler: notifyHandler},
	}
}

// resolve 按 name 关联注册表与配置，补全每个消费者的 group/topic；
// 缺配置直接报错，避免消费者静默漏启
func resolve(c *conf.Data_Rocketmq, consumers []rmqx.Consumer) ([]rmqx.Consumer, error) {
	for i, cs := range consumers {
		cfg, ok := c.Consumers[cs.Name]
		if !ok || cfg.GetGroup() == "" || cfg.GetTopic() == "" {
			return nil, fmt.Errorf(
				"consumer %q: missing config, add it under data.rocketmq.consumers in config.yaml", cs.Name)
		}
		cs.Group = cfg.GetGroup()
		cs.Topic = cfg.GetTopic()
		consumers[i] = cs
	}
	return consumers, nil
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
