package config

import (
	"encoding/json"
	"fmt"
	"reflect"
	"time"

	"github.com/go-viper/mapstructure/v2"
	"github.com/spf13/viper"
)

// Duration 可解析字符串的时间间隔，同时兼容 JSON 字符串（"5s"）和整数（纳秒）
type Duration time.Duration

func (d Duration) MarshalJSON() ([]byte, error) {
	return json.Marshal(time.Duration(d).String())
}

func (d *Duration) UnmarshalJSON(b []byte) error {
	var v interface{}
	if err := json.Unmarshal(b, &v); err != nil {
		return err
	}
	switch val := v.(type) {
	case string:
		td, err := time.ParseDuration(val)
		if err != nil {
			return err
		}
		*d = Duration(td)
	case float64:
		*d = Duration(time.Duration(val))
	default:
		return fmt.Errorf("invalid duration: %v", v)
	}
	return nil
}

func (d Duration) String() string {
	return time.Duration(d).String()
}

// MarshalText 实现 encoding.TextMarshaler（供 Viper/YAML 序列化）
func (d Duration) MarshalText() ([]byte, error) {
	return []byte(time.Duration(d).String()), nil
}

// UnmarshalText 实现 encoding.TextUnmarshaler（供 Viper/YAML 反序列化）
// 支持 "5s", "10ms", "1m" 等格式
func (d *Duration) UnmarshalText(text []byte) error {
	td, err := time.ParseDuration(string(text))
	if err != nil {
		return err
	}
	*d = Duration(td)
	return nil
}

// ToStd 转为标准 time.Duration
func (d Duration) ToStd() time.Duration {
	return time.Duration(d)
}

// ServerConfig HTTP 服务配置
type ServerConfig struct {
	Host         string        `json:"host" mapstructure:"host"`
	Port         int           `json:"port" mapstructure:"port"`
	ReadTimeout  time.Duration `json:"readTimeout" mapstructure:"readTimeout"`
	WriteTimeout time.Duration `json:"writeTimeout" mapstructure:"writeTimeout"`
}

// KafkaConfig Kafka 通用连接配置
type KafkaConfig struct {
	Brokers []string `json:"brokers" mapstructure:"brokers"`
	Topic   string   `json:"topic" mapstructure:"topic"`
	SASL    *SASL    `json:"sasl,omitempty" mapstructure:"sasl,omitempty"`
	TLS     *TLS     `json:"tls,omitempty" mapstructure:"tls,omitempty"`
}

// SASL SASL 认证配置
type SASL struct {
	Enabled   bool   `json:"enabled" mapstructure:"enabled"`
	Mechanism string `json:"mechanism" mapstructure:"mechanism"`
	Username  string `json:"username" mapstructure:"username"`
	Password  string `json:"password" mapstructure:"password"`
}

// TLS TLS 配置
type TLS struct {
	Enabled    bool   `json:"enabled" mapstructure:"enabled"`
	CAFile     string `json:"caFile" mapstructure:"caFile"`
	CertFile   string `json:"certFile" mapstructure:"certFile"`
	KeyFile    string `json:"keyFile" mapstructure:"keyFile"`
	SkipVerify bool   `json:"skipVerify" mapstructure:"skipVerify"`
}

// KafkaConsumerConfig 消费者配置
type KafkaConsumerConfig struct {
	Brokers           []string `json:"brokers" mapstructure:"brokers"`
	Topics            []string `json:"topics" mapstructure:"topics"`
	TopicPattern      string   `json:"topicPattern" mapstructure:"topicPattern"`
	GroupID           string   `json:"groupId" mapstructure:"groupId"`
	AutoOffsetReset   string   `json:"autoOffsetReset" mapstructure:"autoOffsetReset"`
	EnableAutoCommit  bool     `json:"enableAutoCommit" mapstructure:"enableAutoCommit"`
	CommitInterval    Duration `json:"commitInterval" mapstructure:"commitInterval"`
	SessionTimeout    Duration `json:"sessionTimeout" mapstructure:"sessionTimeout"`
	HeartbeatInterval Duration `json:"heartbeatInterval" mapstructure:"heartbeatInterval"`
	MaxPollRecords    int      `json:"maxPollRecords" mapstructure:"maxPollRecords"`
	FetchMinBytes     int32    `json:"fetchMinBytes" mapstructure:"fetchMinBytes"`
	FetchMaxBytes     int32    `json:"fetchMaxBytes" mapstructure:"fetchMaxBytes"`
	FetchMaxWait      Duration `json:"fetchMaxWait" mapstructure:"fetchMaxWait"`
	SASL              *SASL    `json:"sasl,omitempty" mapstructure:"sasl,omitempty"`
	TLS               *TLS     `json:"tls,omitempty" mapstructure:"tls,omitempty"`
}

// KafkaProducerConfig 生产者配置
type KafkaProducerConfig struct {
	Brokers             []string `json:"brokers" mapstructure:"brokers"`
	Topic               string   `json:"topic" mapstructure:"topic"`
	Acks                int16    `json:"acks" mapstructure:"acks"`
	Compression         string   `json:"compression" mapstructure:"compression"`
	BatchSize           int      `json:"batchSize" mapstructure:"batchSize"`
	LingerMs            int      `json:"lingerMs" mapstructure:"lingerMs"`
	MaxInFlightRequests int      `json:"maxInFlightRequests" mapstructure:"maxInFlightRequests"`
	Idempotent          bool     `json:"idempotent" mapstructure:"idempotent"`
	MaxRetries          int      `json:"maxRetries" mapstructure:"maxRetries"`
	RetryBackoff        Duration `json:"retryBackoff" mapstructure:"retryBackoff"`
	Partitioner         string   `json:"partitioner" mapstructure:"partitioner"`
	PartitionKeyField   string   `json:"partitionKeyField" mapstructure:"partitionKeyField"`
	SASL                *SASL    `json:"sasl,omitempty" mapstructure:"sasl,omitempty"`
	TLS                 *TLS     `json:"tls,omitempty" mapstructure:"tls,omitempty"`
}

// TransformConfig 转换配置
type TransformConfig struct {
	AttributePrefix string `json:"attributePrefix" mapstructure:"attributePrefix"`
	TextKey         string `json:"textKey" mapstructure:"textKey"`
	CDataKey        string `json:"cdataKey" mapstructure:"cdataKey"`
	NamespaceMode   string `json:"namespaceMode" mapstructure:"namespaceMode"`
	TrimElements    bool   `json:"trimElements" mapstructure:"trimElements"`
	SkipComments    bool   `json:"skipComments" mapstructure:"skipComments"`
	SkipProcInst    bool   `json:"skipProcInst" mapstructure:"skipProcInst"`
	StrictMode      bool   `json:"strictMode" mapstructure:"strictMode"`
	ErrorTopic      string `json:"errorTopic" mapstructure:"errorTopic"`
	StripLevels     int    `json:"stripLevels" mapstructure:"stripLevels"`
}

// PipelineCfg 单条管道配置
type PipelineCfg struct {
	ID          string              `json:"id" mapstructure:"id"`
	Name        string              `json:"name" mapstructure:"name"`
	Description string              `json:"description" mapstructure:"description"`
	Enabled     bool                `json:"enabled" mapstructure:"enabled"`
	Workers     int                 `json:"workers" mapstructure:"workers"`
	BufferSize  int                 `json:"bufferSize" mapstructure:"bufferSize"`
	Source      KafkaConsumerConfig `json:"source" mapstructure:"source"`
	Transform   TransformConfig     `json:"transform" mapstructure:"transform"`
	Sink        KafkaProducerConfig `json:"sink" mapstructure:"sink"`
}

// AppConfig 应用总配置
type AppConfig struct {
	Server    ServerConfig  `json:"server" mapstructure:"server"`
	Pipelines []PipelineCfg `json:"pipelines" mapstructure:"pipelines"`
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
		Pipelines: []PipelineCfg{},
	}
}

// DefaultPipelineConfig 返回单条管道的默认配置模板
func DefaultPipelineConfig() PipelineCfg {
	return PipelineCfg{
		Enabled:    false,
		Workers:    4,
		BufferSize: 1024,
		Source: KafkaConsumerConfig{
			GroupID:           "xml2json-group",
			AutoOffsetReset:   "latest",
			MaxPollRecords:    500,
			FetchMinBytes:     1,
			FetchMaxBytes:     10 * 1024 * 1024,
			FetchMaxWait:      Duration(5 * time.Second),
			CommitInterval:    Duration(5 * time.Second),
			SessionTimeout:    Duration(10 * time.Second),
			HeartbeatInterval: Duration(3 * time.Second),
		},
		Transform: TransformConfig{
			AttributePrefix: "@",
			TextKey:         "#text",
			CDataKey:        "#cdata",
			NamespaceMode:   "strip",
			TrimElements:    true,
			SkipComments:    true,
			SkipProcInst:    true,
			StripLevels:     0,
		},
		Sink: KafkaProducerConfig{
			Acks:                1,
			Compression:         "none",
			BatchSize:           16384,
			LingerMs:            10,
			MaxInFlightRequests: 5,
			MaxRetries:          3,
			RetryBackoff:        Duration(100 * time.Millisecond),
			Partitioner:         "hash",
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

	v.AutomaticEnv()

	if err := v.ReadInConfig(); err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	cfg := DefaultConfig()
	if err := v.Unmarshal(cfg, viper.DecodeHook(
		mapstructure.ComposeDecodeHookFunc(
			mapstructure.StringToTimeDurationHookFunc(),
			mapstructure.StringToSliceHookFunc(","),
			durationDecodeHook,
		),
	)); err != nil {
		return nil, fmt.Errorf("failed to unmarshal config: %w", err)
	}

	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("config validation failed: %w", err)
	}

	return cfg, nil
}

// durationDecodeHook 将 YAML 中的字符串转为 Duration 类型
func durationDecodeHook(f reflect.Type, t reflect.Type, data interface{}) (interface{}, error) {
	if t == reflect.TypeOf(Duration(0)) {
		switch v := data.(type) {
		case string:
			d, err := time.ParseDuration(v)
			if err != nil {
				return nil, err
			}
			return Duration(d), nil
		case float64:
			return Duration(time.Duration(v)), nil
		case int64:
			return Duration(time.Duration(v)), nil
		}
	}
	return data, nil
}

// Validate 校验配置
func (c *AppConfig) Validate() error {
	for i := range c.Pipelines {
		p := &c.Pipelines[i]
		if !p.Enabled {
			continue
		}
		if p.ID == "" {
			return fmt.Errorf("pipelines[%d].id is required when enabled", i)
		}
		if len(p.Source.Brokers) == 0 {
			return fmt.Errorf("pipelines[%d].source.brokers is required", i)
		}
		if len(p.Source.Topics) == 0 && p.Source.TopicPattern == "" {
			return fmt.Errorf("pipelines[%d].source.topics or topicPattern is required", i)
		}
		if p.Source.GroupID == "" {
			return fmt.Errorf("pipelines[%d].source.groupId is required", i)
		}
		if len(p.Sink.Brokers) == 0 {
			return fmt.Errorf("pipelines[%d].sink.brokers is required", i)
		}
		if p.Sink.Topic == "" {
			return fmt.Errorf("pipelines[%d].sink.topic is required", i)
		}
		if p.Workers < 1 {
			p.Workers = 1
		}
		if p.BufferSize < 1 {
			p.BufferSize = 1024
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
