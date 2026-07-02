package config

import (
	"fmt"
	"time"

	"github.com/spf13/viper"
)

// ServerConfig HTTP 服务配置
type ServerConfig struct {
	Host         string        `mapstructure:"host"`
	Port         int           `mapstructure:"port"`
	ReadTimeout  time.Duration `mapstructure:"readTimeout"`
	WriteTimeout time.Duration `mapstructure:"writeTimeout"`
}

// KafkaConfig Kafka 通用连接配置
type KafkaConfig struct {
	Brokers []string `mapstructure:"brokers"`
	Topic   string   `mapstructure:"topic"`
	SASL    *SASL    `mapstructure:"sasl,omitempty"`
	TLS     *TLS     `mapstructure:"tls,omitempty"`
}

// SASL SASL 认证配置
type SASL struct {
	Enabled   bool   `mapstructure:"enabled"`
	Mechanism string `mapstructure:"mechanism"`
	Username  string `mapstructure:"username"`
	Password  string `mapstructure:"password"`
}

// TLS TLS 配置
type TLS struct {
	Enabled    bool   `mapstructure:"enabled"`
	CAFile     string `mapstructure:"caFile"`
	CertFile   string `mapstructure:"certFile"`
	KeyFile    string `mapstructure:"keyFile"`
	SkipVerify bool   `mapstructure:"skipVerify"`
}

// KafkaConsumerConfig 消费者配置
type KafkaConsumerConfig struct {
	Brokers           []string      `mapstructure:"brokers"`
	Topics            []string      `mapstructure:"topics"`
	TopicPattern      string        `mapstructure:"topicPattern"`
	GroupID           string        `mapstructure:"groupId"`
	AutoOffsetReset   string        `mapstructure:"autoOffsetReset"`
	EnableAutoCommit  bool          `mapstructure:"enableAutoCommit"`
	CommitInterval    time.Duration `mapstructure:"commitInterval"`
	SessionTimeout    time.Duration `mapstructure:"sessionTimeout"`
	HeartbeatInterval time.Duration `mapstructure:"heartbeatInterval"`
	MaxPollRecords    int           `mapstructure:"maxPollRecords"`
	FetchMinBytes     int32         `mapstructure:"fetchMinBytes"`
	FetchMaxBytes     int32         `mapstructure:"fetchMaxBytes"`
	FetchMaxWait      time.Duration `mapstructure:"fetchMaxWait"`
	SASL              *SASL         `mapstructure:"sasl,omitempty"`
	TLS               *TLS          `mapstructure:"tls,omitempty"`
}

// KafkaProducerConfig 生产者配置
type KafkaProducerConfig struct {
	Brokers             []string      `mapstructure:"brokers"`
	Topic               string        `mapstructure:"topic"`
	Acks                int16         `mapstructure:"acks"`
	Compression         string        `mapstructure:"compression"`
	BatchSize           int           `mapstructure:"batchSize"`
	LingerMs            int           `mapstructure:"lingerMs"`
	MaxInFlightRequests int           `mapstructure:"maxInFlightRequests"`
	Idempotent          bool          `mapstructure:"idempotent"`
	MaxRetries          int           `mapstructure:"maxRetries"`
	RetryBackoff        time.Duration `mapstructure:"retryBackoff"`
	Partitioner         string        `mapstructure:"partitioner"`
	PartitionKeyField   string        `mapstructure:"partitionKeyField"`
	SASL                *SASL         `mapstructure:"sasl,omitempty"`
	TLS                 *TLS          `mapstructure:"tls,omitempty"`
}

// TransformConfig 转换配置
type TransformConfig struct {
	AttributePrefix string `mapstructure:"attributePrefix"`
	TextKey         string `mapstructure:"textKey"`
	CDataKey        string `mapstructure:"cdataKey"`
	NamespaceMode   string `mapstructure:"namespaceMode"`
	TrimElements    bool   `mapstructure:"trimElements"`
	SkipComments    bool   `mapstructure:"skipComments"`
	SkipProcInst    bool   `mapstructure:"skipProcInst"`
	StrictMode      bool   `mapstructure:"strictMode"`
	ErrorTopic      string `mapstructure:"errorTopic"`
}

// PipelineCfg 单条管道配置
type PipelineCfg struct {
	ID          string              `mapstructure:"id"`
	Name        string              `mapstructure:"name"`
	Description string              `mapstructure:"description"`
	Enabled     bool                `mapstructure:"enabled"`
	Workers     int                 `mapstructure:"workers"`
	BufferSize  int                 `mapstructure:"bufferSize"`
	Source      KafkaConsumerConfig `mapstructure:"source"`
	Transform   TransformConfig     `mapstructure:"transform"`
	Sink        KafkaProducerConfig `mapstructure:"sink"`
}

// AppConfig 应用总配置
type AppConfig struct {
	Server   ServerConfig `mapstructure:"server"`
	Pipeline PipelineCfg  `mapstructure:"pipeline"`
}

// DefaultConfig 返回带默认值的配置
func DefaultConfig() *AppConfig {
	return &AppConfig{
		Server: ServerConfig{
			Host:         "0.0.0.0",
			Port:         8080,
			ReadTimeout:  30 * time.Second,
			WriteTimeout: 30 * time.Second,
		},
		Pipeline: PipelineCfg{
			ID:      "default",
			Name:    "默认管道",
			Enabled: false,
			Workers: 4,
			BufferSize: 1024,
			Source: KafkaConsumerConfig{
				GroupID:         "xml2json-group",
				AutoOffsetReset: "latest",
				MaxPollRecords:  500,
				FetchMinBytes:   1,
				FetchMaxBytes:   10 * 1024 * 1024,
				FetchMaxWait:    5 * time.Second,
			},
			Transform: TransformConfig{
				AttributePrefix: "@",
				TextKey:         "#text",
				CDataKey:        "#cdata",
				NamespaceMode:   "strip",
				TrimElements:    true,
				SkipComments:    true,
				SkipProcInst:    true,
			},
			Sink: KafkaProducerConfig{
				Acks:                1,
				Compression:         "none",
				BatchSize:           16384,
				LingerMs:            10,
				MaxInFlightRequests: 5,
				MaxRetries:          3,
				RetryBackoff:        100 * time.Millisecond,
				Partitioner:         "hash",
			},
		},
	}
}

// Load 从配置文件加载配置
func Load(configPath string) (*AppConfig, error) {
	v := viper.New()

	if configPath != "" {
		v.SetConfigFile(configPath)
	} else {
		v.SetConfigName("config")
		v.SetConfigType("yaml")
		v.AddConfigPath("./configs")
		v.AddConfigPath(".")
	}

	// 环境变量覆盖
	v.AutomaticEnv()

	if err := v.ReadInConfig(); err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	cfg := DefaultConfig()
	if err := v.Unmarshal(cfg); err != nil {
		return nil, fmt.Errorf("failed to unmarshal config: %w", err)
	}

	// 校验必填项
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("config validation failed: %w", err)
	}

	return cfg, nil
}

// Validate 校验配置
func (c *AppConfig) Validate() error {
	if c.Pipeline.Enabled {
		if len(c.Pipeline.Source.Brokers) == 0 {
			return fmt.Errorf("pipeline.source.brokers is required")
		}
		if len(c.Pipeline.Source.Topics) == 0 && c.Pipeline.Source.TopicPattern == "" {
			return fmt.Errorf("pipeline.source.topics or topicPattern is required")
		}
		if c.Pipeline.Source.GroupID == "" {
			return fmt.Errorf("pipeline.source.groupId is required")
		}
		if len(c.Pipeline.Sink.Brokers) == 0 {
			return fmt.Errorf("pipeline.sink.brokers is required")
		}
		if c.Pipeline.Sink.Topic == "" {
			return fmt.Errorf("pipeline.sink.topic is required")
		}
		if c.Pipeline.Workers < 1 {
			c.Pipeline.Workers = 1
		}
		if c.Pipeline.BufferSize < 1 {
			c.Pipeline.BufferSize = 1024
		}
	}
	return nil
}

// Clone 深拷贝管道配置
func (p *PipelineCfg) Clone() *PipelineCfg {
	clone := *p
	clone.Source.Brokers = append([]string{}, p.Source.Brokers...)
	clone.Source.Topics = append([]string{}, p.Source.Topics...)
	clone.Sink.Brokers = append([]string{}, p.Sink.Brokers...)
	return &clone
}
