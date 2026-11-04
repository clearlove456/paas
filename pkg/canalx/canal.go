// Package canalx 订阅 canal-server 投递到 RocketMQ 的 MySQL binlog 变更消息，
// 并提供基于 RocketMQ 延迟消息的延迟任务能力（延迟双删等最终一致性场景）。
package canalx

import (
	"context"
	"encoding/json"
	"strconv"

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

// event canal-server 投递的扁平 JSON 格式（flatMessage，每行一条）
type event struct {
	Data     []map[string]string `json:"data"` // 受影响行的字段值（字符串化）
	Database string              `json:"database"`
	Table    string              `json:"table"`
	Type     string              `json:"type"` // INSERT / UPDATE / DELETE / ERASE ...
	IsDDL    bool                `json:"isDdl"`
}

// Event 解析后的业务事件（主键已提取）
type Event struct {
	Database string
	Table    string
	Type     string
	ID       int64
}

// RowHandler 每行变更回调一次。INSERT/UPDATE/DELETE/ERASE 才回调（DDL 忽略）。
// INSERT 也要回调：插入前可能被查过并缓存了空值标记（穿透防护），新数据必须让空值失效。
type RowHandler func(ctx context.Context, ev Event)

// KeyHandler 延迟消息到期回调，keys 为延迟任务携带的缓存 key
type KeyHandler func(ctx context.Context, keys []string)

// Options 订阅配置
type Options struct {
	NameServers []string // RocketMQ NameServer 地址
	CanalTopic  string   // canal 投递 binlog 的主题
	DelayTopic  string   // 延迟任务主题（延迟双删的第二删走这里）
	Group       string   // 消费组
	PK          string   // 主键列名，默认 id
}

// delayTask 延迟消息体
type delayTask struct {
	Keys []string `json:"keys"`
}

// Sub binlog 订阅器。
// 注：rocketmq-client-go v2.1.x 的 NewPushConsumer/NewProducer 返回未导出类型，
// 因此用闭包捕获实例，对外只暴露行为。
type Sub struct {
	publish func(ctx context.Context, keys []string, delayLevel int) error
	stop    func()
}

// New 创建订阅器：订阅 canal 主题与延迟主题（未启动）。
// rowHandler 处理 binlog 变更，keyHandler 处理延迟任务。
func New(opts Options, rowHandler RowHandler, keyHandler KeyHandler, logger log.Logger) (*Sub, error) {
	helper := log.NewHelper(log.With(logger, "module", "canalx"))
	if opts.PK == "" {
		opts.PK = "id"
	}

	c, err := consumer.NewPushConsumer(
		consumer.WithGroupName(opts.Group),
		consumer.WithNameServer(opts.NameServers),
	)
	if err != nil {
		return nil, err
	}

	// 订阅 canal binlog 主题：解析后逐行回调
	err = c.Subscribe(opts.CanalTopic, consumer.MessageSelector{},
		func(ctx context.Context, msgs ...*primitive.MessageExt) (consumer.ConsumeResult, error) {
			for _, m := range msgs {
				for _, ev := range parseCanal(m.Body, opts.PK) {
					rowHandler(ctx, ev)
				}
			}
			return consumer.ConsumeSuccess, nil
		})
	if err != nil {
		return nil, err
	}

	// 订阅延迟任务主题：到期后执行第二删
	err = c.Subscribe(opts.DelayTopic, consumer.MessageSelector{},
		func(ctx context.Context, msgs ...*primitive.MessageExt) (consumer.ConsumeResult, error) {
			for _, m := range msgs {
				var t delayTask
				if json.Unmarshal(m.Body, &t) == nil {
					keyHandler(ctx, t.Keys)
				}
			}
			return consumer.ConsumeSuccess, nil
		})
	if err != nil {
		return nil, err
	}

	p, err := producer.NewDefaultProducer(
		producer.WithNameServer(opts.NameServers),
	)
	if err != nil {
		return nil, err
	}

	sub := &Sub{
		publish: func(ctx context.Context, keys []string, delayLevel int) error {
			if len(keys) == 0 {
				return nil
			}
			body, err := json.Marshal(delayTask{Keys: keys})
			if err != nil {
				return err
			}
			msg := primitive.NewMessage(opts.DelayTopic, body)
			msg.WithDelayTimeLevel(delayLevel)
			_, err = p.SendSync(ctx, msg)
			return err
		},
		stop: func() {
			if err := c.Shutdown(); err != nil {
				helper.Warnf("consumer shutdown: %v", err)
			}
			if err := p.Shutdown(); err != nil {
				helper.Warnf("producer shutdown: %v", err)
			}
		},
	}

	if err := c.Start(); err != nil {
		return nil, err
	}
	if err := p.Start(); err != nil {
		return nil, err
	}
	helper.Infof("canal subscriber started: canal=%s delay=%s group=%s", opts.CanalTopic, opts.DelayTopic, opts.Group)
	return sub, nil
}

// Stop 优雅关闭（New 内部已自动启动，无需单独 Start）
func (s *Sub) Stop() { s.stop() }

// PublishDelay 发送延迟任务：delayLevel 见 DelayLevelXXX 常量，到期后 keyHandler 收到 keys
func (s *Sub) PublishDelay(ctx context.Context, keys []string, delayLevel int) error {
	return s.publish(ctx, keys, delayLevel)
}

// parseCanal 解析单条 canal 消息，提取所有需要回调的行事件
func parseCanal(payload []byte, pk string) []Event {
	var ev event
	if err := json.Unmarshal(payload, &ev); err != nil || ev.IsDDL || len(ev.Data) == 0 {
		return nil
	}
	switch ev.Type {
	case "INSERT", "UPDATE", "DELETE", "ERASE":
	default:
		return nil
	}
	out := make([]Event, 0, len(ev.Data))
	for _, row := range ev.Data {
		id, err := strconv.ParseInt(row[pk], 10, 64)
		if err != nil {
			continue
		}
		out = append(out, Event{Database: ev.Database, Table: ev.Table, Type: ev.Type, ID: id})
	}
	return out
}
