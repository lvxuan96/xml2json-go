package pipeline

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"xml2json-go/internal/config"
	"xml2json-go/internal/converter"
	"xml2json-go/internal/kafka"

	"go.uber.org/zap"
)

// State 管道运行状态
type State int32

const (
	StateIdle    State = iota // 未启动
	StateRunning              // 运行中
	StateStopping             // 停止中
	StateError                // 错误
)

func (s State) String() string {
	switch s {
	case StateIdle:
		return "idle"
	case StateRunning:
		return "running"
	case StateStopping:
		return "stopping"
	case StateError:
		return "error"
	default:
		return "unknown"
	}
}

// Metrics 管道运行指标
type Metrics struct {
	Consumed  int64         `json:"consumed"`
	Produced  int64         `json:"produced"`
	Errors    int64         `json:"errors"`
	StartTime time.Time     `json:"startTime"`
	UpTime    time.Duration `json:"upTime"`
	State     string        `json:"state"`
}

// Pipeline 单个转换管道
type Pipeline struct {
	cfg       atomic.Pointer[config.PipelineCfg]
	converter *converter.XML2JSON
	consumer  *kafka.Consumer
	producer  *kafka.Producer

	workChan chan *kafka.Message
	ctx      context.Context
	cancel   context.CancelFunc
	wg       sync.WaitGroup

	state   atomic.Int32
	metrics Metrics
	mu      sync.RWMutex

	logger *zap.Logger
}

// New 创建管道
func New(cfg *config.PipelineCfg, logger *zap.Logger) (*Pipeline, error) {
	if cfg == nil {
		return nil, fmt.Errorf("pipeline config is nil")
	}

	conv := converter.New(&cfg.Transform, logger)

	p := &Pipeline{
		converter: conv,
		workChan:  make(chan *kafka.Message, cfg.BufferSize),
		logger:    logger.With(zap.String("pipeline", cfg.ID)),
	}

	p.cfg.Store(cfg.Clone())
	p.state.Store(int32(StateIdle))

	return p, nil
}

// Start 启动管道
func (p *Pipeline) Start() error {
	if !p.state.CompareAndSwap(int32(StateIdle), int32(StateRunning)) {
		return fmt.Errorf("pipeline is already running")
	}

	cfg := p.cfg.Load()

	// 创建 Kafka 生产者
	producer, err := kafka.NewProducer(&cfg.Sink, p.logger)
	if err != nil {
		p.state.Store(int32(StateError))
		return fmt.Errorf("failed to create producer: %w", err)
	}
	p.producer = producer

	// 创建 Kafka 消费者
	consumer, err := kafka.NewConsumer(&cfg.Source, p.workChan, p.logger)
	if err != nil {
		p.state.Store(int32(StateError))
		p.producer.Close()
		return fmt.Errorf("failed to create consumer: %w", err)
	}
	p.consumer = consumer

	// 启动上下文
	p.ctx, p.cancel = context.WithCancel(context.Background())

	// 记录启动时间
	p.mu.Lock()
	p.metrics.StartTime = time.Now()
	p.metrics.State = StateRunning.String()
	p.mu.Unlock()

	// 启动 worker 协程池
	workers := cfg.Workers
	if workers < 1 {
		workers = 4
	}
	for i := 0; i < workers; i++ {
		p.wg.Add(1)
		go p.worker(i)
	}

	// 启动消费循环（后台协程）
	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		if err := p.consumer.Run(p.ctx); err != nil {
			p.logger.Error("consumer run error", zap.Error(err))
		}
	}()

	p.logger.Info("pipeline started",
		zap.Int("workers", workers),
		zap.String("source_topics", fmt.Sprintf("%v", cfg.Source.Topics)),
		zap.String("sink_topic", cfg.Sink.Topic),
	)

	return nil
}

// Stop 停止管道
func (p *Pipeline) Stop() error {
	if !p.state.CompareAndSwap(int32(StateRunning), int32(StateStopping)) {
		return fmt.Errorf("pipeline is not running")
	}

	p.logger.Info("pipeline stopping...")

	// 取消上下文
	if p.cancel != nil {
		p.cancel()
	}

	// 等待所有 worker 完成
	p.wg.Wait()

	// 关闭消费者和生产者
	if p.consumer != nil {
		p.consumer.Close()
	}
	if p.producer != nil {
		p.producer.Close()
	}

	p.state.Store(int32(StateIdle))
	p.mu.Lock()
	p.metrics.State = StateIdle.String()
	p.mu.Unlock()

	p.logger.Info("pipeline stopped")
	return nil
}

// Reload 热重载配置
func (p *Pipeline) Reload(cfg *config.PipelineCfg) error {
	if p.state.Load() == int32(StateRunning) {
		if err := p.Stop(); err != nil {
			return fmt.Errorf("failed to stop pipeline for reload: %w", err)
		}
	}

	// 更新配置
	p.cfg.Store(cfg.Clone())
	p.converter = converter.New(&cfg.Transform, p.logger)
	p.workChan = make(chan *kafka.Message, cfg.BufferSize)

	// 清空指标
	p.mu.Lock()
	p.metrics = Metrics{}
	p.mu.Unlock()

	if cfg.Enabled {
		return p.Start()
	}
	return nil
}

// worker 工作协程
func (p *Pipeline) worker(id int) {
	defer p.wg.Done()
	logger := p.logger.With(zap.Int("worker", id))

	for {
		select {
		case msg, ok := <-p.workChan:
			if !ok {
				return
			}

			// 执行 XML → JSON 转换
			jsonData, err := p.converter.Convert(msg.Value)
			if err != nil {
				p.mu.Lock()
				p.metrics.Errors++
				p.mu.Unlock()

				logger.Error("conversion failed",
					zap.String("topic", msg.Topic),
					zap.Int32("partition", msg.Partition),
					zap.Int64("offset", msg.Offset),
					zap.Error(err),
				)
				continue
			}

			// 发送到目标 Kafka
			p.producer.SendMessage(msg.Key, jsonData, msg.Headers)

			// 更新指标
			atomic.AddInt64(&p.metrics.Consumed, 1)
			atomic.AddInt64(&p.metrics.Produced, 1)

			logger.Debug("message processed",
				zap.String("topic", msg.Topic),
				zap.Int32("partition", msg.Partition),
				zap.Int64("offset", msg.Offset),
				zap.Int("xml_len", len(msg.Value)),
				zap.Int("json_len", len(jsonData)),
			)

		case <-p.ctx.Done():
			logger.Debug("worker stopped")
			return
		}
	}
}

// GetMetrics 获取管道指标
func (p *Pipeline) GetMetrics() Metrics {
	p.mu.RLock()
	defer p.mu.RUnlock()

	m := p.metrics
	m.State = State(p.state.Load()).String()

	if m.StartTime.IsZero() {
		m.UpTime = 0
	} else if p.state.Load() == int32(StateRunning) {
		m.UpTime = time.Since(m.StartTime)
	}

	return m
}

// GetConfig 获取当前配置
func (p *Pipeline) GetConfig() *config.PipelineCfg {
	return p.cfg.Load()
}

// State 返回管道状态
func (p *Pipeline) State() State {
	return State(p.state.Load())
}
