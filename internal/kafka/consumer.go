package kafka

import (
	"context"
	"fmt"
	"sync"
	"time"

	"xml2json-go/internal/config"

	"github.com/IBM/sarama"
	"go.uber.org/zap"
)

// Message 消费到的消息
type Message struct {
	Key       []byte
	Value     []byte
	Topic     string
	Partition int32
	Offset    int64
	Timestamp time.Time
	Headers   map[string]string
}

// Consumer Kafka 消费者封装
type Consumer struct {
	client      sarama.ConsumerGroup
	topics      []string
	cfg         *config.KafkaConsumerConfig
	workChan    chan<- *Message
	logger      *zap.Logger
	cancel      context.CancelFunc
	ready       chan struct{}
}

// ConsumerHandler 实现 sarama.ConsumerGroupHandler
type consumerHandler struct {
	workChan chan<- *Message
	logger   *zap.Logger
	ready    chan struct{}
	once     sync.Once
}

// NewConsumer 创建消费者
func NewConsumer(
	cfg *config.KafkaConsumerConfig,
	workChan chan<- *Message,
	logger *zap.Logger,
) (*Consumer, error) {
	saramaCfg := sarama.NewConfig()
	saramaCfg.Version = sarama.V3_6_0_0

	// 消费配置
	saramaCfg.Consumer.Return.Errors = true
	saramaCfg.Consumer.Offsets.Initial = sarama.OffsetOldest
	if cfg.AutoOffsetReset == "latest" {
		saramaCfg.Consumer.Offsets.Initial = sarama.OffsetNewest
	}
	saramaCfg.Consumer.Group.Session.Timeout = cfg.SessionTimeout
	saramaCfg.Consumer.Group.Heartbeat.Interval = cfg.HeartbeatInterval
	saramaCfg.Consumer.MaxProcessingTime = 30 * time.Second

	// 自动提交 offset
	saramaCfg.Consumer.Offsets.AutoCommit.Enable = cfg.EnableAutoCommit
	saramaCfg.Consumer.Offsets.AutoCommit.Interval = cfg.CommitInterval

	// 拉取配置
	if cfg.MaxPollRecords > 0 {
		saramaCfg.Consumer.MaxProcessingTime = time.Duration(cfg.MaxPollRecords) * time.Millisecond
	}
	if cfg.FetchMinBytes > 0 {
		saramaCfg.Consumer.Fetch.Min = cfg.FetchMinBytes
	}
	if cfg.FetchMaxBytes > 0 {
		saramaCfg.Consumer.Fetch.Default = cfg.FetchMaxBytes
	}
	if cfg.FetchMaxWait > 0 {
		saramaCfg.Consumer.MaxWaitTime = cfg.FetchMaxWait
	}

	// SASL 配置
	if cfg.SASL != nil && cfg.SASL.Enabled {
		// SASL 配置留空，由具体实现决定
	}

	// TLS 配置
	if cfg.TLS != nil && cfg.TLS.Enabled {
		// TLS 配置留空，由具体实现决定
	}

	client, err := sarama.NewConsumerGroup(cfg.Brokers, cfg.GroupID, saramaCfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create consumer group: %w", err)
	}

	// 确定 topic 列表
	topics := cfg.Topics

	return &Consumer{
		client:   client,
		topics:   topics,
		cfg:      cfg,
		workChan: workChan,
		logger:   logger,
		ready:    make(chan struct{}),
	}, nil
}

// Run 启动消费循环（阻塞）
func (c *Consumer) Run(ctx context.Context) error {
	handler := &consumerHandler{
		workChan: c.workChan,
		logger:   c.logger,
		ready:    c.ready,
	}

	ctx, c.cancel = context.WithCancel(ctx)
	defer c.cancel()

	// 错误处理
	go func() {
		for err := range c.client.Errors() {
			c.logger.Error("consumer group error", zap.Error(err))
		}
	}()

	for {
		// 消费循环
		if err := c.client.Consume(ctx, c.topics, handler); err != nil {
			if ctx.Err() != nil {
				return nil // 正常取消
			}
			c.logger.Error("consume error, retrying...", zap.Error(err))
			time.Sleep(time.Second)
			continue
		}

		// context 取消时退出
		if ctx.Err() != nil {
			return nil
		}
	}
}

// Close 关闭消费者
func (c *Consumer) Close() error {
	if c.cancel != nil {
		c.cancel()
	}
	return c.client.Close()
}

func (h *consumerHandler) Setup(sarama.ConsumerGroupSession) error {
	h.once.Do(func() {
		close(h.ready)
	})
	return nil
}

func (h *consumerHandler) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

func (h *consumerHandler) ConsumeClaim(
	session sarama.ConsumerGroupSession,
	claim sarama.ConsumerGroupClaim,
) error {
	for {
		select {
		case msg, ok := <-claim.Messages():
			if !ok {
				return nil
			}

			// 组装消息
			headers := make(map[string]string, len(msg.Headers))
			for _, hdr := range msg.Headers {
				headers[string(hdr.Key)] = string(hdr.Value)
			}

			kafkaMsg := &Message{
				Key:       msg.Key,
				Value:     msg.Value,
				Topic:     msg.Topic,
				Partition: msg.Partition,
				Offset:    msg.Offset,
				Timestamp: msg.Timestamp,
				Headers:   headers,
			}

			// 非阻塞发送到工作通道
			select {
			case h.workChan <- kafkaMsg:
				session.MarkMessage(msg, "")
			case <-session.Context().Done():
				return nil
			}

		case <-session.Context().Done():
			return nil
		}
	}
}
