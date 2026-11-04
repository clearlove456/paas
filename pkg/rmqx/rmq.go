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

// Config 监听配置
type Config struct {
	NameServers   []string
	Group         string
	Subscriptions map[string]Handler // topic -> 消息处理器
}

// Listen 创建并启动一个多主题消费者，返回 stop。
// 注：rocketmq-client-go v2.1.x 的 NewPushConsumer 返回未导出类型，故用闭包捕获实例。
func Listen(cfg Config, logger log.Logger) (func(), error) {
	helper := log.NewHelper(log.With(logger, "module", "rmqx/listener"))

	c, err := consumer.NewPushConsumer(
		consumer.WithGroupName(cfg.Group),
		consumer.WithNameServer(cfg.NameServers),
	)
	if err != nil {
		return nil, err
	}
	for topic, h := range cfg.Subscriptions {
		handler := h
		if err := c.Subscribe(topic, consumer.MessageSelector{},
			func(ctx context.Context, msgs ...*primitive.MessageExt) (consumer.ConsumeResult, error) {
				for _, m := range msgs {
					if err := handler(ctx, m.Body); err != nil {
						return consumer.ConsumeRetryLater, err
					}
				}
				return consumer.ConsumeSuccess, nil
			}); err != nil {
			return nil, err
		}
	}
	if err := c.Start(); err != nil {
		return nil, err
	}

	topics := make([]string, 0, len(cfg.Subscriptions))
	for t := range cfg.Subscriptions {
		topics = append(topics, t)
	}
	helper.Infof("rmq listener started: group=%s topics=%v", cfg.Group, topics)

	return func() {
		if err := c.Shutdown(); err != nil {
			helper.Warnf("listener shutdown: %v", err)
		}
	}, nil
}

// Sender 消息发送器（含延迟消息）
type Sender struct {
	send  func(ctx context.Context, topic string, body []byte, delayLevel int) error
	close func()
}

// NewSender 创建并启动发送器
func NewSender(nameServers []string) (*Sender, error) {
	p, err := producer.NewDefaultProducer(producer.WithNameServer(nameServers))
	if err != nil {
		return nil, err
	}
	if err := p.Start(); err != nil {
		return nil, err
	}
	return &Sender{
		send: func(ctx context.Context, topic string, body []byte, delayLevel int) error {
			msg := primitive.NewMessage(topic, body)
			if delayLevel > 0 {
				msg.WithDelayTimeLevel(delayLevel)
			}
			_, err := p.SendSync(ctx, msg)
			return err
		},
		close: func() {
			if err := p.Shutdown(); err != nil {
				log.Warnf("rmqx sender shutdown: %v", err)
			}
		},
	}, nil
}

// Send 普通发送
func (s *Sender) Send(ctx context.Context, topic string, body []byte) error {
	return s.send(ctx, topic, body, 0)
}

// SendDelay 延迟发送，delayLevel 见 DelayLevelXXX 常量
func (s *Sender) SendDelay(ctx context.Context, topic string, body []byte, delayLevel int) error {
	return s.send(ctx, topic, body, delayLevel)
}

// Close 关闭发送器
func (s *Sender) Close() { s.close() }
