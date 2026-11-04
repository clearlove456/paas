package rmqx

import (
	"github.com/go-kratos/kratos/v2/log"
)

// Consumer 一个 MQ 消费者的定义：一个消费组订阅一个主题，对应一个处理函数。
//
// 注意：RocketMQ 要求同一消费组的订阅关系在所有实例上完全一致，
// 所以不同 topic 必须用不同的 Group——两个消费者若共用一个组却各订各的
// topic，broker 端订阅表会互相顶掉，直接丢消息。
type Consumer struct {
	Name    string  // 展示名（日志用）
	Group   string  // RocketMQ 消费组
	Topic   string  // 订阅的主题
	Handler Handler // 消息处理函数
}

// Manager 统一管理所有消费者的创建、启动与停止
type Manager struct {
	listeners []*Listener
}

// NewManager 创建全部消费者（未启动）
func NewManager(nameServers []string, consumers []Consumer) (*Manager, error) {
	m := &Manager{}
	for _, cs := range consumers {
		l, err := NewListener(ListenConfig{
			NameServers: nameServers,
			Group:       cs.Group,
			Topic:       cs.Topic,
			Handler:     cs.Handler,
		})
		if err != nil {
			m.Stop() // 已创建的先回收
			return nil, err
		}
		m.listeners = append(m.listeners, l)
	}
	return m, nil
}

// Start 启动全部消费者；中途失败则回滚已启动的
func (m *Manager) Start() error {
	started := 0
	for _, l := range m.listeners {
		if err := l.Start(); err != nil {
			for i := 0; i < started; i++ {
				m.listeners[i].Stop()
			}
			return err
		}
		started++
	}
	log.Infof("rmq manager started %d consumer(s)", len(m.listeners))
	return nil
}

// Stop 逆序停止全部消费者
func (m *Manager) Stop() {
	for i := len(m.listeners) - 1; i >= 0; i-- {
		m.listeners[i].Stop()
	}
}
