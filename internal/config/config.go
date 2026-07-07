package config

import (
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"time"

	"github.com/go-viper/mapstructure/v2"
	"github.com/spf13/viper"
	"gopkg.in/yaml.v3"
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
	Host         string        `json:"host" mapstructure:"host" yaml:"host"`
	Port         int           `json:"port" mapstructure:"port" yaml:"port"`
	ReadTimeout  time.Duration `json:"readTimeout" mapstructure:"readTimeout" yaml:"readTimeout"`
	WriteTimeout time.Duration `json:"writeTimeout" mapstructure:"writeTimeout" yaml:"writeTimeout"`
}

// KafkaConfig Kafka 通用连接配置
type KafkaConfig struct {
	Brokers []string `json:"brokers" mapstructure:"brokers" yaml:"brokers,flow"`
	Topic   string   `json:"topic" mapstructure:"topic" yaml:"topic"`
	SASL    *SASL    `json:"sasl,omitempty" mapstructure:"sasl,omitempty"`
	TLS     *TLS     `json:"tls,omitempty" mapstructure:"tls,omitempty"`
}

// SASL SASL 认证配置
type SASL struct {
	Enabled   bool   `json:"enabled" mapstructure:"enabled" yaml:"enabled"`
	Mechanism string `json:"mechanism" mapstructure:"mechanism"`
	Username  string `json:"username" mapstructure:"username"`
	Password  string `json:"password" mapstructure:"password"`
}

// TLS TLS 配置
type TLS struct {
	Enabled    bool   `json:"enabled" mapstructure:"enabled" yaml:"enabled"`
	CAFile     string `json:"caFile" mapstructure:"caFile"`
	CertFile   string `json:"certFile" mapstructure:"certFile"`
	KeyFile    string `json:"keyFile" mapstructure:"keyFile"`
	SkipVerify bool   `json:"skipVerify" mapstructure:"skipVerify"`
}

// KafkaConsumerConfig 消费者配置
type KafkaConsumerConfig struct {
	Brokers           []string `json:"brokers" mapstructure:"brokers" yaml:"brokers,flow"`
	Topics            []string `json:"topics" mapstructure:"topics" yaml:"topics,flow"`
	TopicPattern      string   `json:"topicPattern" mapstructure:"topicPattern" yaml:"topicPattern"`
	GroupID           string   `json:"groupId" mapstructure:"groupId" yaml:"groupId"`
	AutoOffsetReset   string   `json:"autoOffsetReset" mapstructure:"autoOffsetReset" yaml:"autoOffsetReset"`
	EnableAutoCommit  bool     `json:"enableAutoCommit" mapstructure:"enableAutoCommit" yaml:"enableAutoCommit"`
	CommitInterval    Duration `json:"commitInterval" mapstructure:"commitInterval" yaml:"commitInterval"`
	SessionTimeout    Duration `json:"sessionTimeout" mapstructure:"sessionTimeout" yaml:"sessionTimeout"`
	HeartbeatInterval Duration `json:"heartbeatInterval" mapstructure:"heartbeatInterval" yaml:"heartbeatInterval"`
	MaxPollRecords    int      `json:"maxPollRecords" mapstructure:"maxPollRecords" yaml:"maxPollRecords"`
	FetchMinBytes     int32    `json:"fetchMinBytes" mapstructure:"fetchMinBytes" yaml:"fetchMinBytes"`
	FetchMaxBytes     int32    `json:"fetchMaxBytes" mapstructure:"fetchMaxBytes" yaml:"fetchMaxBytes"`
	FetchMaxWait      Duration `json:"fetchMaxWait" mapstructure:"fetchMaxWait" yaml:"fetchMaxWait"`
	SASL              *SASL    `json:"sasl,omitempty" mapstructure:"sasl,omitempty"`
	TLS               *TLS     `json:"tls,omitempty" mapstructure:"tls,omitempty"`
}

// KafkaProducerConfig 生产者配置
type KafkaProducerConfig struct {
	Brokers             []string `json:"brokers" mapstructure:"brokers" yaml:"brokers,flow"`
	Topic               string   `json:"topic" mapstructure:"topic" yaml:"topic"`
	Acks                int16    `json:"acks" mapstructure:"acks" yaml:"acks"`
	Compression         string   `json:"compression" mapstructure:"compression" yaml:"compression"`
	BatchSize           int      `json:"batchSize" mapstructure:"batchSize" yaml:"batchSize"`
	LingerMs            int      `json:"lingerMs" mapstructure:"lingerMs" yaml:"lingerMs"`
	MaxInFlightRequests int      `json:"maxInFlightRequests" mapstructure:"maxInFlightRequests" yaml:"maxInFlightRequests"`
	Idempotent          bool     `json:"idempotent" mapstructure:"idempotent" yaml:"idempotent"`
	MaxRetries          int      `json:"maxRetries" mapstructure:"maxRetries" yaml:"maxRetries"`
	RetryBackoff        Duration `json:"retryBackoff" mapstructure:"retryBackoff" yaml:"retryBackoff"`
	Partitioner         string   `json:"partitioner" mapstructure:"partitioner" yaml:"partitioner"`
	PartitionKeyField   string   `json:"partitionKeyField" mapstructure:"partitionKeyField" yaml:"partitionKeyField"`
	SASL                *SASL    `json:"sasl,omitempty" mapstructure:"sasl,omitempty"`
	TLS                 *TLS     `json:"tls,omitempty" mapstructure:"tls,omitempty"`
}

// TransformConfig 转换配置
type TransformConfig struct {
	AttributePrefix string `json:"attributePrefix" mapstructure:"attributePrefix" yaml:"attributePrefix"`
	TextKey         string `json:"textKey" mapstructure:"textKey" yaml:"textKey"`
	CDataKey        string `json:"cdataKey" mapstructure:"cdataKey" yaml:"cdataKey"`
	NamespaceMode   string `json:"namespaceMode" mapstructure:"namespaceMode" yaml:"namespaceMode"`
	TrimElements    bool   `json:"trimElements" mapstructure:"trimElements" yaml:"trimElements"`
	SkipComments    bool   `json:"skipComments" mapstructure:"skipComments" yaml:"skipComments"`
	SkipProcInst    bool   `json:"skipProcInst" mapstructure:"skipProcInst" yaml:"skipProcInst"`
	StrictMode      bool   `json:"strictMode" mapstructure:"strictMode" yaml:"strictMode"`
	ErrorTopic      string `json:"errorTopic" mapstructure:"errorTopic" yaml:"errorTopic"`
	StripLevels     int    `json:"stripLevels" mapstructure:"stripLevels" yaml:"stripLevels"`
}

// PipelineCfg 单条管道配置
type PipelineCfg struct {
	ID          string              `json:"id" mapstructure:"id" yaml:"id"`
	Name        string              `json:"name" mapstructure:"name" yaml:"name"`
	Description string              `json:"description" mapstructure:"description" yaml:"description"`
	Enabled     bool                `json:"enabled" mapstructure:"enabled" yaml:"enabled"`
	Workers     int                 `json:"workers" mapstructure:"workers" yaml:"workers"`
	BufferSize  int                 `json:"bufferSize" mapstructure:"bufferSize" yaml:"bufferSize"`
	Source      KafkaConsumerConfig `json:"source" mapstructure:"source" yaml:"source"`
	Transform   TransformConfig     `json:"transform" mapstructure:"transform" yaml:"transform"`
	Sink        KafkaProducerConfig `json:"sink" mapstructure:"sink" yaml:"sink"`
}

// AppConfig 应用总配置
type AppConfig struct {
	Server    ServerConfig  `json:"server" mapstructure:"server" yaml:"server"`
	Pipelines []PipelineCfg `json:"pipelines" mapstructure:"pipelines" yaml:"pipelines"`
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

// Save 将配置写回 YAML 文件（持久化页面操作）
func Save(path string, cfg *AppConfig) error {
	tmpPath := path + ".tmp"

	data, err := yaml.Marshal(cfg)
	if err != nil {
		return fmt.Errorf("failed to marshal config: %w", err)
	}

	if err := os.WriteFile(tmpPath, data, 0644); err != nil {
		return fmt.Errorf("failed to write config: %w", err)
	}

	// Windows 上 os.Rename 无法覆盖已存在的文件，需要先删除
	_ = os.Remove(path)
	if err := os.Rename(tmpPath, path); err != nil {
		return fmt.Errorf("failed to rename config: %w", err)
	}
	return nil
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
