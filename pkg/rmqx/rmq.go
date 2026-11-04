// Package rmqx RocketMQ 通用收发组件：多主题监听 + 延迟消息发送。
// 只面向 topic，不关心消息是谁生产的（canal、其他服务...）。
package rmqx

import (
	"context"

	"github.com/go-kratos/kratos/v2/log"

	"github.com/apache/rocketmq-client-go/v2/consumer"
	"github.com/apache/rocketmq-client-go/v2/primitive"
	"github.com/apache/rocketmq-client-go/v2/producer"
)

// DelayLevel RocketMQ 4.x 延迟级别（服务端固定档位，非任意时长）:
// 1=1s 2=5s 3=10s 4=30s 5=1m 6=2m ... 18=2h
const (
	DelayLevel1s  = 1
	DelayLevel5s  = 2
	DelayLevel10s = 3
	DelayLevel30s = 4
)

// Handler 单条消息回调；返回 error 时 RocketMQ 按重试策略重投
type Handler func(ctx context.Context, body []byte) error

// pushConsumer / syncProducer：rocketmq-client-go v2.1.x 的构造函数返回
// 未导出类型，这里用接口把它们接住，使其可以作为结构体字段持有。
type pushConsumer interface {
	Subscribe(topic string, selector consumer.MessageSelector,
		f func(context.Context, ...*primitive.MessageExt) (consumer.ConsumeResult, error)) error
	Start() error
	Shutdown() error
}

type syncProducer interface {
	Start() error
	Shutdown() error
	SendSync(ctx context.Context, mq ...*primitive.Message) (*primitive.SendResult, error)
}

// Listener MQ 监听器：一个消费组，订阅多个主题
type Listener struct {
	c pushConsumer
}

// Config 监听配置
type Config struct {
	NameServers   []string
	Group         string
	Subscriptions map[string]Handler // topic -> 消息处理器
}

// NewListener 创建监听器（未启动）
func NewListener(cfg Config) (*Listener, error) {
	c, err := consumer.NewPushConsumer(
		consumer.WithGroupName(cfg.Group),
		consumer.WithNameServer(cfg.NameServers),
	)
	if err != nil {
		return nil, err
	}
	for topic, h := range cfg.Subscriptions {
		if err := subscribe(c, topic, h); err != nil {
			return nil, err
		}
	}
	return &Listener{c: c}, nil
}

// Start 开始消费
func (l *Listener) Start() error { return l.c.Start() }

// Stop 停止消费
func (l *Listener) Stop() {
	if err := l.c.Shutdown(); err != nil {
		log.Warnf("rmqx listener shutdown: %v", err)
	}
}

// subscribe 把 Handler 适配成 RocketMQ 的回调签名
func subscribe(c pushConsumer, topic string, h Handler) error {
	return c.Subscribe(topic, consumer.MessageSelector{},
		func(ctx context.Context, msgs ...*primitive.MessageExt) (consumer.ConsumeResult, error) {
			for _, m := range msgs {
				if err := h(ctx, m.Body); err != nil {
					return consumer.ConsumeRetryLater, err
				}
			}
			return consumer.ConsumeSuccess, nil
		})
}

// Sender 消息发送器（含延迟消息）
type Sender struct {
	p syncProducer
}

// NewSender 创建发送器（未启动）
func NewSender(nameServers []string) (*Sender, error) {
	p, err := producer.NewDefaultProducer(producer.WithNameServer(nameServers))
	if err != nil {
		return nil, err
	}
	return &Sender{p: p}, nil
}

// Start 启动发送器
func (s *Sender) Start() error { return s.p.Start() }

// Close 关闭发送器
func (s *Sender) Close() {
	if err := s.p.Shutdown(); err != nil {
		log.Warnf("rmqx sender shutdown: %v", err)
	}
}

// Send 普通发送
func (s *Sender) Send(ctx context.Context, topic string, body []byte) error {
	_, err := s.p.SendSync(ctx, primitive.NewMessage(topic, body))
	return err
}

// SendDelay 延迟发送，delayLevel 见 DelayLevelXXX 常量
func (s *Sender) SendDelay(ctx context.Context, topic string, body []byte, delayLevel int) error {
	msg := primitive.NewMessage(topic, body)
	msg.WithDelayTimeLevel(delayLevel)
	_, err := s.p.SendSync(ctx, msg)
	return err
}
