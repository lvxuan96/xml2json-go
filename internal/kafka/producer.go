package kafka

import (
	"context"
	"fmt"
	"time"

	"xml2json-go/internal/config"

	"github.com/IBM/sarama"
	"go.uber.org/zap"
)

// Producer Kafka 生产者封装
type Producer struct {
	client  sarama.AsyncProducer
	cfg     *config.KafkaProducerConfig
	logger  *zap.Logger
}

// NewProducer 创建生产者
func NewProducer(cfg *config.KafkaProducerConfig, logger *zap.Logger) (*Producer, error) {
	saramaCfg := sarama.NewConfig()
	saramaCfg.Version = sarama.V3_6_0_0

	// 确认配置
	if cfg.Acks == -1 {
		saramaCfg.Producer.RequiredAcks = sarama.WaitForAll
	} else if cfg.Acks == 0 {
		saramaCfg.Producer.RequiredAcks = sarama.NoResponse
	} else {
		saramaCfg.Producer.RequiredAcks = sarama.WaitForLocal
	}

	// 幂等性
	saramaCfg.Producer.Idempotent = cfg.Idempotent
	if cfg.Idempotent {
		saramaCfg.Producer.RequiredAcks = sarama.WaitForAll
		saramaCfg.Producer.MaxMessageBytes = 10 * 1024 * 1024
		saramaCfg.Net.MaxOpenRequests = 1
	}

	// 重试配置
	if cfg.MaxRetries > 0 {
		saramaCfg.Producer.Retry.Max = cfg.MaxRetries
	}
	if cfg.RetryBackoff.ToStd() > 0 {
		saramaCfg.Producer.Retry.Backoff = time.Duration(cfg.RetryBackoff)
	}

	// 批量发送配置
	if cfg.BatchSize > 0 {
		saramaCfg.Producer.Flush.Bytes = cfg.BatchSize
	}
	if cfg.LingerMs > 0 {
		saramaCfg.Producer.Flush.Frequency = time.Duration(cfg.LingerMs) * time.Millisecond
	}
	if cfg.MaxInFlightRequests > 0 {
		saramaCfg.Net.MaxOpenRequests = cfg.MaxInFlightRequests
	}

	// 压缩
	switch cfg.Compression {
	case "gzip":
		saramaCfg.Producer.Compression = sarama.CompressionGZIP
	case "snappy":
		saramaCfg.Producer.Compression = sarama.CompressionSnappy
	case "lz4":
		saramaCfg.Producer.Compression = sarama.CompressionLZ4
	case "zstd":
		saramaCfg.Producer.Compression = sarama.CompressionZSTD
	default:
		saramaCfg.Producer.Compression = sarama.CompressionNone
	}

	// 分区策略
	switch cfg.Partitioner {
	case "roundrobin", "roundRobin":
		saramaCfg.Producer.Partitioner = sarama.NewRoundRobinPartitioner
	case "random":
		saramaCfg.Producer.Partitioner = sarama.NewRandomPartitioner
	case "manual":
		saramaCfg.Producer.Partitioner = sarama.NewManualPartitioner
	default:
		saramaCfg.Producer.Partitioner = sarama.NewHashPartitioner
	}

	saramaCfg.Producer.Return.Successes = true
	saramaCfg.Producer.Return.Errors = true

	client, err := sarama.NewAsyncProducer(cfg.Brokers, saramaCfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create producer: %w", err)
	}

	p := &Producer{
		client: client,
		cfg:    cfg,
		logger: logger,
	}

	// 后台处理结果
	go p.handleResults()

	return p, nil
}

// Send 发送消息（异步）
func (p *Producer) Send(msg *sarama.ProducerMessage) {
	p.client.Input() <- msg
}

// SendMessage 发送简单消息（key/value）
func (p *Producer) SendMessage(key, value []byte, headers map[string]string) {
	msg := &sarama.ProducerMessage{
		Topic: p.cfg.Topic,
		Key:   sarama.ByteEncoder(key),
		Value: sarama.ByteEncoder(value),
	}

	if len(headers) > 0 {
		hdr := make([]sarama.RecordHeader, 0, len(headers))
		for k, v := range headers {
			hdr = append(hdr, sarama.RecordHeader{
				Key:   []byte(k),
				Value: []byte(v),
			})
		}
		msg.Headers = hdr
	}

	p.client.Input() <- msg
}

// SendSync 同步发送消息（阻塞等待确认）
func (p *Producer) SendSync(ctx context.Context, key, value []byte) (partition int32, offset int64, err error) {
	msg := &sarama.ProducerMessage{
		Topic: p.cfg.Topic,
		Key:   sarama.ByteEncoder(key),
		Value: sarama.ByteEncoder(value),
	}

	select {
	case p.client.Input() <- msg:
		// 等待成功或错误
		select {
		case success := <-p.client.Successes():
			return success.Partition, success.Offset, nil
		case err := <-p.client.Errors():
			return 0, 0, fmt.Errorf("producer sync error: %w", err.Err)
		case <-ctx.Done():
			return 0, 0, ctx.Err()
		}
	case <-ctx.Done():
		return 0, 0, ctx.Err()
	}
}

// handleResults 处理异步生产结果
func (p *Producer) handleResults() {
	for {
		select {
		case success, ok := <-p.client.Successes():
			if !ok {
				return
			}
			p.logger.Debug("message produced",
				zap.String("topic", success.Topic),
				zap.Int32("partition", success.Partition),
				zap.Int64("offset", success.Offset),
			)
		case err, ok := <-p.client.Errors():
			if !ok {
				return
			}
			p.logger.Error("producer error",
				zap.String("topic", err.Msg.Topic),
				zap.Error(err.Err),
			)
		}
	}
}

// Close 关闭生产者
func (p *Producer) Close() error {
	return p.client.Close()
}
