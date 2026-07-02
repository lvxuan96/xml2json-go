# XML-to-JSON 高性能转换器 开发文档

> 版本: v1.0 | 日期: 2026-07-02 | 作者: 开发团队

---

## 目录

1. [项目概述](#1-项目概述)
2. [需求分析](#2-需求分析)
3. [技术选型](#3-技术选型)
4. [系统架构](#4-系统架构)
5. [详细设计](#5-详细设计)
6. [Web 管理界面](#6-web-管理界面)
7. [扩展机制设计](#7-扩展机制设计)
8. [性能优化策略](#8-性能优化策略)
9. [接口规范](#9-接口规范)
10. [部署方案](#10-部署方案)
11. [测试方案](#11-测试方案)
12. [项目里程碑](#12-项目里程碑)
13. [附录](#13-附录)

---

## 1. 项目概述

### 1.1 项目背景

构建一个高性能的 XML 到 JSON 消息转换服务，通过 Kafka 作为消息中间件进行数据流转，支持灵活配置和水平扩展，满足高吞吐量场景下的实时数据转换需求。

### 1.2 项目目标

- 从 Kafka 消费 XML 格式消息，转换为 JSON 后投递到目标 Kafka 集群
- 提供 Web 管理界面，支持可视化配置与监控
- 支持多数据源（多 Kafka Topic）的动态扩展接入
- 单实例吞吐量目标：**≥ 50,000 msg/s**（消息大小 1KB 基准）

### 1.3 核心功能矩阵

| 功能模块 | 描述 | 优先级 |
|---------|------|--------|
| Kafka 消费 | 可配置的多 Topic 消费，支持多种消费策略 | P0 |
| XML→JSON 转换 | 高性能 XML 解析与 JSON 序列化 | P0 |
| Kafka 生产 | 可配置的 JSON 消息生产，支持分区策略 | P0 |
| Web 管理界面 | 管道配置、启停管理、状态监控 | P0 |
| 多管道扩展 | 动态新增/删除转换管道，无需重启 | P1 |
| 监控告警 | 吞吐量、延迟、错误率监控 | P1 |
| 数据校验 | XML Schema 校验、JSON Schema 校验 | P2 |
| 数据过滤/转换 | 自定义转换规则（XPath/模板） | P2 |

---

## 2. 需求分析

### 2.1 功能性需求

#### 2.1.1 Kafka 消费侧

```
┌──────────────────────────────────────────────────────────────┐
│ 消费配置项                                                     │
├──────────────────┬───────────────────────────────────────────┤
│ bootstrap.servers │ Kafka 集群地址（支持多个 broker）            │
│ group.id          │ 消费者组 ID                               │
│ topics            │ 订阅的 Topic 列表（支持正则匹配）            │
│ auto.offset.reset │ 消费策略: earliest / latest               │
│ enable.auto.commit │ 是否自动提交 offset                      │
│ max.poll.records  │ 单次拉取最大消息数                          │
│ fetch.min.bytes   │ 最小拉取字节数                             │
│ security.protocol │ 安全协议: PLAINTEXT / SASL_SSL            │
│ sasl.*            │ SASL 认证配置                              │
└──────────────────┴───────────────────────────────────────────┘
```

#### 2.1.2 转换规则

- **输入**: 任意合法 XML 文档
- **输出**: 等效 JSON 结构，遵循以下规则：

```
XML 元素 → JSON 对象
XML 属性 → JSON 对象的 "@attr" 前缀属性（可配置）
XML 文本内容 → JSON 对象的 "#text" 属性（可配置）
XML 重复子元素 → JSON 数组
XML 命名空间 → 保留前缀或展开（可配置）
```

转换示例:

```xml
<!-- 输入 -->
<order id="12345">
  <customer name="张三">
    <email>zhangsan@example.com</email>
  </customer>
  <items>
    <item sku="A001" qty="2">无线鼠标</item>
    <item sku="B002" qty="1">机械键盘</item>
  </items>
</order>
```

```json
// 输出
{
  "order": {
    "@id": "12345",
    "customer": {
      "@name": "张三",
      "email": "zhangsan@example.com"
    },
    "items": {
      "item": [
        { "@sku": "A001", "@qty": "2", "#text": "无线鼠标" },
        { "@sku": "B002", "@qty": "1", "#text": "机械键盘" }
      ]
    }
  }
}
```

#### 2.1.3 Kafka 生产侧

| 配置项 | 描述 |
|--------|------|
| bootstrap.servers | 目标 Kafka 集群地址 |
| topic | 输出 Topic 名称 |
| acks | 确认机制: 0 / 1 / all |
| compression.type | 压缩类型: none / gzip / snappy / lz4 / zstd |
| batch.size | 批量发送大小（字节） |
| linger.ms | 批量等待时间（毫秒） |
| max.in.flight.requests.per.connection | 最大未确认请求数 |
| partitioner | 分区策略: hash / round-robin / 自定义 |

### 2.2 非功能性需求

| 指标 | 目标值 | 说明 |
|------|--------|------|
| 吞吐量 | ≥ 50,000 msg/s | 单实例，1KB 消息基准 |
| 转换延迟 | P99 < 10ms | 端到端延迟 |
| 可用性 | 99.9% | 支持多实例部署 |
| 内存占用 | < 512MB | 稳态运行 |
| CPU 利用率 | < 70% | 稳态运行，4 核基准 |
| 启动时间 | < 30s | 冷启动至就绪 |

---

## 3. 技术选型

### 3.1 语言选型对比

| 维度 | Go | Java | Rust | Node.js | Python |
|------|-----|------|------|---------|--------|
| 原生并发模型 | ★★★★★ Goroutine | ★★★☆ 线程池 | ★★★★ async/await | ★★★ Event Loop | ★★ GIL |
| Kafka 生态 | ★★★★ sarama | ★★★★★ Kafka Client | ★★★ rdkafka | ★★★★ kafkajs | ★★★ confluent-kafka |
| XML 解析性能 | ★★★★ | ★★★★ | ★★★★★ | ★★★ | ★★ |
| JSON 序列化 | ★★★★★ | ★★★★ | ★★★★★ | ★★★★ | ★★★ |
| 内存管理 | ★★★★ GC | ★★★ GC | ★★★★★ 无GC | ★★★★ GC | ★★★ GC |
| Web 框架 | ★★★★ Gin/Fiber | ★★★★ Spring Boot | ★★★ Actix | ★★★★ Express/Fastify | ★★★ Flask/FastAPI |
| 部署便利性 | ★★★★★ 单二进制 | ★★ JRE 依赖 | ★★★★★ 单二进制 | ★★★ node_modules | ★★ 解释器依赖 |
| 学习曲线 | 低-中 | 中-高 | 高 | 低 | 低 |
| 团队招聘 | ★★★☆ | ★★★★★ | ★★ | ★★★★★ | ★★★★ |

### 3.2 结论：选择 Go 语言

**核心理由**:

1. **并发模型**: Goroutine 的轻量级协程（2KB 栈空间）天然适合 Kafka 多分区并发消费场景，与 Kafka 的分区并行消费模型完美匹配
2. **编译为单二进制**: 无需运行时依赖，Docker 镜像可压缩至 ~15MB
3. **GC 优化**: Go 1.21+ 的 GC 暂停 < 1ms，满足低延迟要求
4. **XML 库选择**: 标准库 `encoding/xml` 覆盖基础场景；高性能场景可选 `github.com/beevik/etree` 或流式解析器
5. **Kafka 库**: `github.com/IBM/sarama`（原 Shopify/sarama）是最成熟的 Go Kafka 客户端，纯 Go 实现，性能优秀

### 3.3 技术栈总览

| 层次 | 技术选型 | 版本要求 |
|------|---------|---------|
| 语言 | Go | ≥ 1.22 |
| Web 框架 | Gin | v1.10+ |
| Kafka 客户端 | IBM/sarama | v1.43+ |
| XML 解析 | encoding/xml + github.com/beevik/etree | — |
| JSON 序列化 | encoding/json + github.com/bytedance/sonic | v1.10+ |
| 配置管理 | Viper | v1.19+ |
| 日志 | Zap (Uber) | v1.27+ |
| 监控指标 | Prometheus client | v1.20+ |
| 前端框架 | Vue 3 + Element Plus | Vue 3.4+ |
| 内嵌资源 | embed (Go 标准库) | — |
| 数据库(可选) | SQLite (mattn/go-sqlite3) | — |

> **sonic 说明**: 字节跳动开源的 sonic 库在 JSON 序列化上比标准库快 3-5 倍，通过 SIMD 指令集加速，非常适合高吞吐量场景。仅在 `amd64` 架构可用，非 `amd64` 环境回退标准库。

---

## 4. 系统架构

### 4.1 整体架构图

```
                          ┌──────────────────────────────────────────────┐
                          │               Web 管理界面 (Vue 3)             │
                          │   ┌─────────┐ ┌─────────┐ ┌──────────────┐   │
                          │   │ 管道配置 │ │ 状态监控 │ │ 扩展开关      │   │
                          │   └─────────┘ └─────────┘ └──────────────┘   │
                          └──────────────────┬───────────────────────────┘
                                             │ HTTP REST API
                          ┌──────────────────▼───────────────────────────┐
                          │              API 层 (Gin Router)               │
                          │  /api/v1/pipelines  /api/v1/metrics  /health │
                          └──────────────────┬───────────────────────────┘
                                             │
                          ┌──────────────────▼───────────────────────────┐
                          │           管道管理器 (Pipeline Manager)        │
                          │  ┌─────────────────────────────────────────┐ │
                          │  │           Pipeline 生命周期管理           │ │
                          │  │   Create / Start / Stop / Delete / List  │ │
                          │  └─────────────────────────────────────────┘ │
                          └──────┬──────────────┬──────────────┬─────────┘
                                 │              │              │
                    ┌────────────▼─┐  ┌─────────▼──┐  ┌───────▼──────────┐
                    │   Pipeline 1  │  │ Pipeline 2  │  │  Pipeline N ...  │
                    │ ┌───────────┐ │  │ ┌──────────┐│  │ ┌──────────────┐ │
                    │ │ Consumer  │ │  │ │ Consumer  ││  │ │  Consumer    │ │
                    │ │ Goroutine │ │  │ │ Goroutine ││  │ │  Goroutine   │ │
                    │ └─────┬─────┘ │  │ └─────┬─────┘│  │ └──────┬───────┘ │
                    │       │       │  │       │      │  │        │         │
                    │ ┌─────▼─────┐ │  │ ┌─────▼─────┐│  │ ┌──────▼───────┐ │
                    │ │ XML→JSON  │ │  │ │ XML→JSON  ││  │ │  XML→JSON    │ │
                    │ │ Converter │ │  │ │ Converter ││  │ │  Converter   │ │
                    │ └─────┬─────┘ │  │ └─────┬─────┘│  │ └──────┬───────┘ │
                    │       │       │  │       │      │  │        │         │
                    │ ┌─────▼─────┐ │  │ ┌─────▼─────┐│  │ ┌──────▼───────┐ │
                    │ │ Producer  │ │  │ │ Producer  ││  │ │  Producer    │ │
                    │ │ Goroutine │ │  │ │ Goroutine ││  │ │  Goroutine   │ │
                    │ └───────────┘ │  │ └──────────┘│  │ └──────────────┘ │
                    └───────────────┘  └─────────────┘  └──────────────────┘
                          ▲                                        │
                          │         ┌──────────────┐               │
                          └─────────┤  Metrics 收集  │◄──────────────┘
                                    │  (Prometheus)  │
                                    └──────────────┘
```

### 4.2 数据流

```
Kafka Source ──XML──► Consumer ──[]byte──► Converter ──JSON──► Producer ──JSON──► Kafka Sink
                          │                    │                    │
                          ▼                    ▼                    ▼
                    消费 Offset 记录      XPath 过滤/转换       发送确认/重试
```

### 4.3 项目目录结构

```
xml2json-go/
├── cmd/
│   └── server/
│       └── main.go                    # 程序入口
├── internal/
│   ├── api/
│   │   ├── router.go                  # 路由注册
│   │   ├── handler/
│   │   │   ├── pipeline.go            # 管道管理 API
│   │   │   ├── metric.go             # 监控指标 API
│   │   │   └── health.go             # 健康检查 API
│   │   └── middleware/
│   │       ├── cors.go                # 跨域中间件
│   │       ├── recovery.go           # 崩溃恢复
│   │       └── logger.go             # 请求日志
│   ├── pipeline/
│   │   ├── manager.go                # 管道生命周期管理
│   │   ├── pipeline.go               # 管道核心逻辑
│   │   ├── registry.go               # 管道注册中心
│   │   └── config.go                 # 管道配置模型
│   ├── kafka/
│   │   ├── consumer.go               # Kafka 消费者封装
│   │   ├── consumer_group.go         # 消费者组管理
│   │   ├── producer.go               # Kafka 生产者封装
│   │   └── config.go                 # Kafka 配置模型
│   ├── converter/
│   │   ├── xml2json.go               # XML→JSON 核心转换
│   │   ├── options.go               # 转换选项配置
│   │   ├── stream.go                 # 流式转换（大文件）
│   │   └── transform.go             # 自定义转换规则
│   ├── config/
│   │   ├── config.go                 # 全局配置管理
│   │   └── defaults.go              # 默认配置值
│   ├── metrics/
│   │   ├── metrics.go                # Prometheus 指标定义
│   │   └── collector.go             # 指标采集器
│   └── extension/
│       ├── loader.go                 # 扩展加载器
│       └── interface.go             # 扩展接口定义
├── web/
│   ├── src/
│   │   ├── App.vue
│   │   ├── main.js
│   │   ├── views/
│   │   │   ├── Dashboard.vue         # 仪表盘
│   │   │   ├── PipelineList.vue      # 管道列表
│   │   │   ├── PipelineEditor.vue    # 管道编辑器
│   │   │   └── Monitor.vue           # 监控面板
│   │   ├── components/
│   │   │   ├── PipelineCard.vue
│   │   │   ├── ConfigForm.vue
│   │   │   ├── MetricsChart.vue
│   │   │   └── LogViewer.vue
│   │   ├── api/
│   │   │   └── index.js             # 前端 API 调用
│   │   └── router/
│   │       └── index.js
│   ├── index.html
│   ├── package.json
│   └── vite.config.js
├── pkg/
│   └── xjerror/
│       └── errors.go                 # 公共错误定义
├── configs/
│   └── config.yaml                   # 默认配置文件示例
├── deployments/
│   ├── Dockerfile
│   ├── docker-compose.yaml
│   └── k8s/
│       ├── deployment.yaml
│       ├── service.yaml
│       └── configmap.yaml
├── scripts/
│   ├── build.sh                      # 构建脚本
│   └── run.sh                        # 运行脚本
├── docs/
│   └── api.md                        # API 文档
├── go.mod
├── go.sum
├── Makefile
└── README.md
```

---

## 5. 详细设计

### 5.1 核心数据模型

#### 5.1.1 管道配置 (Pipeline Config)

```go
// PipelineConfig 定义一条完整的转换管道
type PipelineConfig struct {
    ID          string              `json:"id" yaml:"id"`                   // 唯一标识
    Name        string              `json:"name" yaml:"name"`               // 管道名称
    Description string              `json:"description" yaml:"description"`  // 描述
    Enabled     bool                `json:"enabled" yaml:"enabled"`          // 是否启用
    Source      KafkaConsumerConfig `json:"source" yaml:"source"`            // 消费配置
    Transform   TransformConfig     `json:"transform" yaml:"transform"`      // 转换配置
    Sink        KafkaProducerConfig `json:"sink" yaml:"sink"`                // 生产配置
    Workers     int                 `json:"workers" yaml:"workers"`          // 工作协程数
    BufferSize  int                 `json:"bufferSize" yaml:"bufferSize"`    // 内部通道缓冲区
    CreatedAt   time.Time           `json:"createdAt" yaml:"createdAt"`
    UpdatedAt   time.Time           `json:"updatedAt" yaml:"updatedAt"`
}
```

#### 5.1.2 Kafka 消费配置

```go
type KafkaConsumerConfig struct {
    Brokers           []string      `json:"brokers" yaml:"brokers"`                       // Kafka 集群地址
    Topics            []string      `json:"topics" yaml:"topics"`                         // 订阅主题
    TopicPattern      string        `json:"topicPattern,omitempty" yaml:"topicPattern"`    // 正则匹配主题
    GroupID           string        `json:"groupId" yaml:"groupId"`                       // 消费者组
    AutoOffsetReset   string        `json:"autoOffsetReset" yaml:"autoOffsetReset"`        // earliest/latest
    EnableAutoCommit  bool          `json:"enableAutoCommit" yaml:"enableAutoCommit"`      // 自动提交
    CommitInterval    time.Duration `json:"commitInterval" yaml:"commitInterval"`          // 提交间隔
    SessionTimeout    time.Duration `json:"sessionTimeout" yaml:"sessionTimeout"`
    HeartbeatInterval time.Duration `json:"heartbeatInterval" yaml:"heartbeatInterval"`
    MaxPollRecords    int           `json:"maxPollRecords" yaml:"maxPollRecords"`
    FetchMinBytes     int32         `json:"fetchMinBytes" yaml:"fetchMinBytes"`
    FetchMaxBytes     int32         `json:"fetchMaxBytes" yaml:"fetchMaxBytes"`
    FetchMaxWait      time.Duration `json:"fetchMaxWait" yaml:"fetchMaxWait"`
    SASL              *SASLConfig   `json:"sasl,omitempty" yaml:"sasl,omitempty"`
    TLS               *TLSConfig    `json:"tls,omitempty" yaml:"tls,omitempty"`
}

type SASLConfig struct {
    Enabled   bool   `json:"enabled"`
    Mechanism string `json:"mechanism"` // PLAIN, SCRAM-SHA-256, SCRAM-SHA-512
    Username  string `json:"username"`
    Password  string `json:"password"`
}

type TLSConfig struct {
    Enabled    bool   `json:"enabled"`
    CAFile     string `json:"caFile,omitempty"`
    CertFile   string `json:"certFile,omitempty"`
    KeyFile    string `json:"keyFile,omitempty"`
    SkipVerify bool   `json:"skipVerify"`
}
```

#### 5.1.3 Kafka 生产配置

```go
type KafkaProducerConfig struct {
    Brokers                []string      `json:"brokers" yaml:"brokers"`
    Topic                  string        `json:"topic" yaml:"topic"`
    Acks                   int16         `json:"acks" yaml:"acks"`                            // 0, 1, -1(all)
    Compression            string        `json:"compression" yaml:"compression"`              // none/gzip/snappy/lz4/zstd
    BatchSize              int           `json:"batchSize" yaml:"batchSize"`
    LingerMs               int           `json:"lingerMs" yaml:"lingerMs"`
    MaxInFlightRequests    int           `json:"maxInFlightRequests" yaml:"maxInFlightRequests"`
    Idempotent             bool          `json:"idempotent" yaml:"idempotent"`
    EnableIdempotence      bool          `json:"enableIdempotence" yaml:"enableIdempotence"`
    MaxRetries             int           `json:"maxRetries" yaml:"maxRetries"`
    RetryBackoff           time.Duration `json:"retryBackoff" yaml:"retryBackoff"`
    Partitioner            string        `json:"partitioner" yaml:"partitioner"`              // hash/roundRobin/manual
    PartitionKeyField      string        `json:"partitionKeyField,omitempty" yaml:"partitionKeyField"` // JSON 字段名，用于 hash 分区
    SASL                   *SASLConfig   `json:"sasl,omitempty" yaml:"sasl,omitempty"`
    TLS                    *TLSConfig    `json:"tls,omitempty" yaml:"tls,omitempty"`
}
```

#### 5.1.4 转换配置

```go
type TransformConfig struct {
    // 属性命名策略
    AttributePrefix string `json:"attributePrefix" yaml:"attributePrefix"` // 默认 "@"
    TextKey         string `json:"textKey" yaml:"textKey"`                 // 默认 "#text"
    CDataKey        string `json:"cdataKey" yaml:"cdataKey"`              // 默认 "#cdata"

    // 命名空间处理
    NamespaceMode  string `json:"namespaceMode" yaml:"namespaceMode"`      // keep/strip/expand

    // XML 解析选项
    TrimElements      bool `json:"trimElements" yaml:"trimElements"`       // 去除元素空白
    SkipComments      bool `json:"skipComments" yaml:"skipComments"`       // 跳过注释
    SkipProcInst      bool `json:"skipProcInst" yaml:"skipProcInst"`       // 跳过处理指令

    // 自定义转换规则（XPath 表达式 → JSON 结构映射）
    XPathMappings     []XPathMapping `json:"xpathMappings,omitempty" yaml:"xpathMappings"`

    // XSD Schema 校验（可选）
    XSDPath           string `json:"xsdPath,omitempty" yaml:"xsdPath"`

    // 验证模式
    StrictMode        bool `json:"strictMode" yaml:"strictMode"`           // 严格模式：格式错误则丢弃
    ErrorTopic        string `json:"errorTopic,omitempty" yaml:"errorTopic"` // 错误消息投递的 Topic
}

type XPathMapping struct {
    XPath      string `json:"xpath" yaml:"xpath"`            // XPath 表达式
    JSONPath   string `json:"jsonPath" yaml:"jsonPath"`      // 目标 JSON 路径
    TargetType string `json:"targetType" yaml:"targetType"`  // string/int/float/bool
    Default    string `json:"default,omitempty" yaml:"default"`
}
```

### 5.2 管道管理器设计

```go
// PipelineManager 管理所有管道的生命周期
type PipelineManager struct {
    mu        sync.RWMutex
    pipelines map[string]*Pipeline        // 运行中的管道
    configs   map[string]*PipelineConfig  // 管道配置
    registry  *PipelineRegistry
    metrics   *metrics.Collector
    logger    *zap.Logger
}

// 核心方法
func (pm *PipelineManager) CreatePipeline(cfg *PipelineConfig) error
func (pm *PipelineManager) StartPipeline(id string) error
func (pm *PipelineManager) StopPipeline(id string) error
func (pm *PipelineManager) DeletePipeline(id string) error
func (pm *PipelineManager) GetPipeline(id string) (*PipelineStatus, error)
func (pm *PipelineManager) ListPipelines() []*PipelineStatus
func (pm *PipelineManager) ReloadPipeline(id string, cfg *PipelineConfig) error
```

### 5.3 管道核心逻辑

```go
// Pipeline 单个转换管道的运行实例
type Pipeline struct {
    cfg       *PipelineConfig
    consumer  *kafka.Consumer
    converter *converter.XML2JSON
    producer  *kafka.Producer

    // 控制
    ctx       context.Context
    cancel    context.CancelFunc

    // Worker 管理
    workChan  chan *WorkItem
    wg        sync.WaitGroup

    // 状态
    state     atomic.Value  // PipelineState
    metrics   *PipelineMetrics

    logger    *zap.Logger
}

type WorkItem struct {
    Message   *sarama.ConsumerMessage
    StartTime time.Time
}

type PipelineState int32
const (
    StateCreated  PipelineState = iota
    StateRunning
    StatePaused
    StateStopping
    StateStopped
    StateError
)

// Run 启动管道的主循环
func (p *Pipeline) Run() error {
    p.setState(StateRunning)

    // 1. 启动 Worker 池
    for i := 0; i < p.cfg.Workers; i++ {
        p.wg.Add(1)
        go p.worker(i)
    }

    // 2. 启动消费循环
    go p.consumeLoop()

    // 3. 等待停止信号
    <-p.ctx.Done()
    return p.shutdown()
}
```

### 5.4 Kafka 消费者设计

```go
// Consumer 封装 Sarama 消费者组
type Consumer struct {
    client       sarama.ConsumerGroup
    topics       []string
    handler      *ConsumerHandler
    ready        chan bool
    logger       *zap.Logger
}

// ConsumerHandler 实现 sarama.ConsumerGroupHandler
type ConsumerHandler struct {
    workChan      chan<- *WorkItem
    metrics       *metrics.ConsumerMetrics
    logger        *zap.Logger
}

func (h *ConsumerHandler) Setup(sarama.ConsumerGroupSession) error {
    close(h.ready)
    return nil
}

func (h *ConsumerHandler) Cleanup(sarama.ConsumerGroupSession) error {
    return nil
}

func (h *ConsumerHandler) ConsumeClaim(
    session sarama.ConsumerGroupSession,
    claim sarama.ConsumerGroupClaim,
) error {
    // 遍历分区消息
    for {
        select {
        case msg, ok := <-claim.Messages():
            if !ok {
                return nil
            }
            // 非阻塞发送到 Worker 通道
            select {
            case h.workChan <- &WorkItem{
                Message:   msg,
                StartTime: time.Now(),
            }:
                session.MarkMessage(msg, "")
            case <-session.Context().Done():
                return nil
            }
        case <-session.Context().Done():
            return nil
        }
    }
}
```

### 5.5 XML→JSON 转换器设计

```go
// XML2JSON 转换器
type XML2JSON struct {
    opts     *TransformOptions
    decoder  *xml.Decoder
    metrics  *metrics.ConverterMetrics
}

// TransformOptions 转换选项
type TransformOptions struct {
    AttributePrefix string
    TextKey         string
    CDataKey        string
    NamespaceMode   string
    TrimElements    bool
    SkipComments    bool
    SkipProcInst    bool
    StrictMode      bool
    Mappings        []XPathMapping
}

// Convert 执行 XML → JSON 转换
func (c *XML2JSON) Convert(xmlData []byte) ([]byte, error) {
    // 1. 解析 XML 为中间 AST 节点树
    // 2. 应用 XPath 映射规则（如有）
    // 3. 序列化为 JSON
    // 4. 返回 JSON 字节
}

// intermediate node tree（中间表示）
type xmlNode struct {
    Name       string
    Attributes map[string]string
    Children   []*xmlNode
    Text       string
    Namespace  string
}
```

#### 转换算法伪代码

```
function xmlToJSON(xmlNode):
    if xmlNode has no children and no attributes:
        return xmlNode.text                    // 纯文本直接返回

    result = {}

    // 1. 处理命名空间
    if namespace mode == "keep":
        key = prefix + ":" + localName
    else if namespace mode == "expand":
        key = "{" + namespaceURI + "}" + localName

    // 2. 处理属性
    for each attr in xmlNode.attributes:
        result[attrPrefix + attr.name] = attr.value

    // 3. 处理子元素
    grouped = groupByName(xmlNode.children)    // 按元素名分组
    for each (name, elements) in grouped:
        if len(elements) == 1:
            result[name] = xmlToJSON(elements[0])
        else:
            result[name] = [xmlToJSON(e) for e in elements]

    // 4. 处理文本内容
    if xmlNode.text is not empty:
        result[textKey] = xmlNode.text

    return result
```

### 5.6 Kafka 生产者设计

```go
// Producer 封装 Sarama AsyncProducer
type Producer struct {
    producer    sarama.AsyncProducer
    topic       string
    partitioner Partitioner
    successChan chan *ProducerResult
    errorChan   chan *ProducerError
    metrics     *metrics.ProducerMetrics
    logger      *zap.Logger
}

// Send 异步发送消息
func (p *Producer) Send(ctx context.Context, key, value []byte, headers []sarama.RecordHeader) error {
    msg := &sarama.ProducerMessage{
        Topic:   p.topic,
        Key:     sarama.ByteEncoder(key),
        Value:   sarama.ByteEncoder(value),
        Headers: headers,
    }
    // 异步发送
    select {
    case p.producer.Input() <- msg:
        return nil
    case <-ctx.Done():
        return ctx.Err()
    }
}

// SendBatch 批量发送
func (p *Producer) SendBatch(ctx context.Context, messages []*ProducerBatchItem) error {
    for _, item := range messages {
        select {
        case p.producer.Input() <- item.Message:
        case <-ctx.Done():
            return ctx.Err()
        }
    }
    return nil
}

// 后台处理结果
func (p *Producer) handleResults() {
    for {
        select {
        case success := <-p.producer.Successes():
            p.metrics.SuccessCount.Inc()
            // 记录 offset
        case err := <-p.producer.Errors():
            p.metrics.ErrorCount.Inc()
            p.logger.Error("producer error",
                zap.Int32("partition", err.Msg.Partition),
                zap.Error(err.Err),
            )
        }
    }
}
```

### 5.7 配置管理

使用 Viper 实现多源配置：

```yaml
# configs/config.yaml 示例
server:
  host: "0.0.0.0"
  port: 8080
  readTimeout: 30s
  writeTimeout: 30s

# 默认管道配置
pipelines:
  - id: "order-pipeline"
    name: "订单数据转换"
    description: "将订单 XML 转为 JSON"
    enabled: true
    workers: 8
    bufferSize: 1024
    source:
      brokers:
        - "kafka-source-1:9092"
        - "kafka-source-2:9092"
      topics:
        - "order.xml"
      groupId: "xml2json-order-group"
      autoOffsetReset: "latest"
      maxPollRecords: 500
    transform:
      attributePrefix: "@"
      textKey: "#text"
      namespaceMode: "strip"
      strictMode: false
      errorTopic: "order.xml.error"
    sink:
      brokers:
        - "kafka-sink-1:9092"
      topic: "order.json"
      acks: -1
      compression: "zstd"
      batchSize: 16384
      lingerMs: 5
      partitioner: "hash"
      partitionKeyField: "order.@id"
      maxRetries: 3
```

---

## 6. Web 管理界面

### 6.1 页面结构

```
┌─────────────────────────────────────────────────────────────┐
│  XML2JSON Converter                           [系统状态 ●] │
├───────────┬─────────────────────────────────────────────────┤
│           │                                                  │
│  仪表盘   │   ┌──────────────────────────────────────────┐  │
│  管道管理 │   │                                              │
│  监控面板 │   │           页面内容区域                       │
│  系统设置 │   │                                              │
│           │   └──────────────────────────────────────────┘  │
│           │                                                  │
└───────────┴─────────────────────────────────────────────────┘
```

### 6.2 页面功能详述

#### 6.2.1 仪表盘 (Dashboard)

- 显示运行中/停止管道数量
- 总吞吐量（msg/s）实时折线图
- 各管道吞吐量排行
- 错误率概览
- 系统资源使用（CPU/内存）

#### 6.2.2 管道管理 (Pipeline List)

- 管道列表表格（名称、状态、吞吐量、延迟、操作）
- 新建管道 → 跳转到管道编辑器
- 启动/停止/删除操作
- 搜索/筛选管道

#### 6.2.3 管道编辑器 (Pipeline Editor)

分步骤表单：

```
─ 步骤1: 基本信息
  ├─ 管道名称
  ├─ 描述
  └─ 工作协程数

─ 步骤2: 数据源配置 (Kafka Consumer)
  ├─ Broker 地址列表
  ├─ Topic 列表 / Topic 正则
  ├─ Consumer Group ID
  ├─ offset 策略 (earliest / latest)
  ├─ 拉取参数 (max.poll.records / fetch.min.bytes)
  ├─ SASL 认证配置
  └─ TLS 配置

─ 步骤3: 转换规则配置
  ├─ XML → JSON 命名约定
  ├─ 命名空间处理方式
  ├─ XPath → JSON Path 映射规则
  └─ 校验模式 (严格/宽松)

─ 步骤4: 数据输出配置 (Kafka Producer)
  ├─ Broker 地址列表
  ├─ 输出 Topic
  ├─ ACK 策略 (0 / 1 / all)
  ├─ 压缩算法 (gzip / snappy / lz4 / zstd)
  ├─ 批量发送参数
  ├─ 分区策略 & 分区键字段
  ├─ SASL 认证配置
  └─ TLS 配置
```

#### 6.2.4 监控面板 (Monitor)

- 按管道筛选
- 实时消费速率图表
- 实时生产速率图表
- 转换延迟 P50/P90/P99
- 错误消息计数
- 消费者 Lag 监控

### 6.3 前端技术实现

```javascript
// web/src/api/index.js
const API_BASE = '/api/v1'

export const pipelineAPI = {
  list:       ()        => fetch(`${API_BASE}/pipelines`),
  get:        (id)      => fetch(`${API_BASE}/pipelines/${id}`),
  create:     (config)  => fetch(`${API_BASE}/pipelines`,       { method: 'POST', body: JSON.stringify(config) }),
  update:     (id, cfg) => fetch(`${API_BASE}/pipelines/${id}`, { method: 'PUT', body: JSON.stringify(cfg) }),
  delete:     (id)      => fetch(`${API_BASE}/pipelines/${id}`, { method: 'DELETE' }),
  start:      (id)      => fetch(`${API_BASE}/pipelines/${id}/start`, { method: 'POST' }),
  stop:       (id)      => fetch(`${API_BASE}/pipelines/${id}/stop`,  { method: 'POST' }),
}

export const metricsAPI = {
  overview:   ()        => fetch(`${API_BASE}/metrics/overview`),
  pipeline:   (id)      => fetch(`${API_BASE}/metrics/pipeline/${id}`),
  realtime:   ()        => fetch(`${API_BASE}/metrics/realtime`),
}
```

### 6.4 后端 API 设计

| 方法 | 路径 | 描述 |
|------|------|------|
| GET | `/api/v1/pipelines` | 获取管道列表 |
| POST | `/api/v1/pipelines` | 创建管道 |
| GET | `/api/v1/pipelines/:id` | 获取管道详情 |
| PUT | `/api/v1/pipelines/:id` | 更新管道配置 |
| DELETE | `/api/v1/pipelines/:id` | 删除管道 |
| POST | `/api/v1/pipelines/:id/start` | 启动管道 |
| POST | `/api/v1/pipelines/:id/stop` | 停止管道 |
| POST | `/api/v1/pipelines/:id/restart` | 重启管道 |
| GET | `/api/v1/pipelines/:id/metrics` | 管道指标 |
| GET | `/api/v1/metrics/overview` | 全局指标概览 |
| GET | `/api/v1/health` | 健康检查 |
| GET | `/ws/metrics` | 指标 WebSocket 推送 |

---

## 7. 扩展机制设计

### 7.1 设计原则

采用 **"热插拔管道 + 接口抽象"** 的设计模式，确保新增数据源和处理逻辑时无需修改核心代码。

### 7.2 扩展接口定义

```go
// internal/extension/interface.go

// PipelineFactory 管道工厂接口 — 用于自定义管道创建
type PipelineFactory interface {
    // Name 返回工厂名称
    Name() string
    // Create 基于配置创建管道运行实例
    Create(cfg *PipelineConfig) (RunnablePipeline, error)
    // Validate 校验配置是否合法
    Validate(cfg *PipelineConfig) error
}

// RunnablePipeline 可运行管道接口
type RunnablePipeline interface {
    // ID 管道唯一标识
    ID() string
    // Run 启动管道
    Run(ctx context.Context) error
    // Stop 停止管道（优雅关闭）
    Stop() error
    // Status 返回当前状态
    Status() PipelineStatus
    // Metrics 返回管道指标
    Metrics() *PipelineMetrics
}

// Converter 转换器接口 — 用于自定义转换逻辑
type Converter interface {
    // Convert 将 XML 数据转为 JSON
    Convert(xmlData []byte) ([]byte, error)
    // Name 转换器名称
    Name() string
    // ConfigSchema 返回配置 JSON Schema
    ConfigSchema() []byte
}

// PreProcessor 前置处理器 — 在 XML 解析前执行
type PreProcessor interface {
    Process(raw []byte) ([]byte, error)
}

// PostProcessor 后置处理器 — 在 JSON 序列化后执行
type PostProcessor interface {
    Process(jsonData []byte) ([]byte, error)
}
```

### 7.3 扩展注册机制

```go
// internal/extension/loader.go

var (
    converterFactories   = make(map[string]func() Converter)
    preProcessorFactories = make(map[string]func() PreProcessor)
    postProcessorFactories = make(map[string]func() PostProcessor)
    mu                   sync.RWMutex
)

// RegisterConverter 注册自定义转换器
func RegisterConverter(name string, factory func() Converter) {
    mu.Lock()
    defer mu.Unlock()
    converterFactories[name] = factory
}

// RegisterPreProcessor 注册前置处理器
func RegisterPreProcessor(name string, factory func() PreProcessor) {
    mu.Lock()
    defer mu.Unlock()
    preProcessorFactories[name] = factory
}

// RegisterPostProcessor 注册后置处理器
func RegisterPostProcessor(name string, factory func() PostProcessor) {
    mu.Lock()
    defer mu.Unlock()
    postProcessorFactories[name] = factory
}
```

### 7.4 新增数据源的方式

#### 方式一：YAML 配置文件（推荐，最简单）

在 `config.yaml` 中添加新的管道配置即可，支持热重载：

```yaml
pipelines:
  - id: "user-login-pipeline"
    name: "用户登录日志转换"
    source:
      topics: ["user.login.xml"]
      ...
    sink:
      topic: "user.login.json"
      ...
```

#### 方式二：Web 界面操作

在管理界面点击"新建管道"，填写表单配置后保存，系统自动创建并启动。

#### 方式三：API 调用（自动化对接）

```bash
curl -X POST http://localhost:8080/api/v1/pipelines \
  -H "Content-Type: application/json" \
  -d '{
    "name": "新数据源",
    "source": {
      "brokers": ["kafka:9092"],
      "topics": ["new.xml.topic"],
      "groupId": "xml2json-new-group"
    },
    "sink": {
      "brokers": ["kafka:9092"],
      "topic": "new.json.topic"
    }
  }'
```

#### 方式四：Go Plugin 扩展（高级场景）

对于需要自定义转换逻辑的场景，可以通过 Go 插件机制加载 `.so` 文件：

```go
// 自定义转换插件示例
package main

import "xml2json-go/internal/extension"

func init() {
    extension.RegisterConverter("custom-converter", func() extension.Converter {
        return &CustomConverter{}
    })
}

type CustomConverter struct{}

func (c *CustomConverter) Name() string { return "custom-converter" }

func (c *CustomConverter) Convert(xmlData []byte) ([]byte, error) {
    // 自定义转换逻辑
    // ...
}
```

编译为插件：
```bash
go build -buildmode=plugin -o custom.so custom_converter.go
```

### 7.5 管道并发隔离

```
┌──────────────────────────────────────────────────────┐
│                    Pipeline Manager                   │
│                                                      │
│  ┌─────────┐  ┌─────────┐  ┌─────────┐  ┌─────────┐ │
│  │Pipe A   │  │Pipe B   │  │Pipe C   │  │Pipe D   │ │
│  │ Goroutine│  │ Goroutine│  │ Goroutine│  │ Goroutine│ │
│  │ Pool    │  │ Pool    │  │ Pool    │  │ Pool    │ │
│  └─────────┘  └─────────┘  └─────────┘  └─────────┘ │
│                                                      │
│  每个管道独立的:                                      │
│   - Consumer Group                                   │
│   - Worker Pool                                      │
│   - Converter 实例                                    │
│   - Producer 实例                                     │
│   - Metrics 统计                                      │
└──────────────────────────────────────────────────────┘
```

每个管道运行在独立的 Goroutine 组中，互不干扰。一个管道故障不会影响其他管道。

---

## 8. 性能优化策略

### 8.1 内存优化

#### 8.1.1 对象池 (sync.Pool)

```go
// 复用频繁创建的对象
var (
    xmlNodePool = sync.Pool{
        New: func() interface{} { return &xmlNode{} },
    }
    bufferPool = sync.Pool{
        New: func() interface{} { return bytes.NewBuffer(make([]byte, 0, 4096)) },
    }
    jsonBufferPool = sync.Pool{
        New: func() interface{} { return bytes.NewBuffer(make([]byte, 0, 4096)) },
    }
)
```

#### 8.1.2 零拷贝传输

```go
// 消息在 Consumer → Converter → Producer 链路中尽量传递 []byte
// 减少不必要的 string 转换和内存分配
type WorkItem struct {
    Key       []byte
    Value     []byte       // 原始 XML
    Headers   []sarama.RecordHeader
    Metadata  MessageMeta
}
```

#### 8.1.3 预分配切片容量

```go
// 对于已知大小的集合，预分配容量避免动态扩容
children := make([]*xmlNode, 0, estimatedChildCount)
attrs := make(map[string]string, estimatedAttrCount)
```

### 8.2 并发优化

#### 8.2.1 分区级并发

```
Kafka Topic (12 partitions)
  │
  ├── Partition 0 ──► Consumer Goroutine ──► Worker 1
  ├── Partition 1 ──► Consumer Goroutine ──► Worker 2
  ├── Partition 2 ──► Consumer Goroutine ──► Worker 3
  ├── ...
  └── Partition N ──► Consumer Goroutine ──► Worker M

  每个分区由独立的 Goroutine 消费，天然支持并行
```

#### 8.2.2 Worker Pool 模式

```go
// 使用 Worker Pool 解耦消费与转换，平滑处理流量尖峰
type WorkerPool struct {
    workers    int
    workChan   chan *WorkItem      // 带缓冲的通道
    processor  func(*WorkItem) error
}

// Workers 数量建议: CPU 核数 × 2
// BufferSize 建议: max.poll.records × partitions × 2
```

#### 8.2.3 批量写入优化

```go
// Producer 端批量发送
// 使用 Sarama AsyncProducer + 合理配置
config := sarama.NewConfig()
config.Producer.Flush.Bytes = 32768       // 32KB 批量
config.Producer.Flush.Messages = 100       // 或 100 条消息
config.Producer.Flush.Frequency = 5 * time.Millisecond
config.Producer.Compression = sarama.CompressionZSTD  // 高压缩比
```

### 8.3 JSON 序列化优化

#### 使用 sonic 替换 encoding/json

```go
import "github.com/bytedance/sonic"

// sonic 通过 SIMD 指令集加速，比标准库快 3-5x
func serializeJSON(v interface{}) ([]byte, error) {
    return sonic.Marshal(v)
}

// sonic 提供了与 encoding/json 兼容的 API
// 在非 amd64 平台自动回退标准库
```

#### 考虑 sonic.Get / sonic.Search 用于部分提取

当只需要 JSON 中的特定字段时，无需反序列化整个文档：

```go
// 提取分区键字段，避免完整反序列化
key, _ := sonic.Get(jsonData, "order", "@id")
```

### 8.4 XML 解析优化

```go
// 使用流式解析器处理大 XML 文档
func (c *XML2JSON) ConvertStream(xmlData []byte) ([]byte, error) {
    decoder := xml.NewDecoder(bytes.NewReader(xmlData))
    decoder.Strict = false  // 非严格模式更快

    for {
        token, err := decoder.Token()
        if err == io.EOF { break }
        if err != nil { return nil, err }

        switch t := token.(type) {
        case xml.StartElement:
            // 处理开始标签
        case xml.EndElement:
            // 处理结束标签
        case xml.CharData:
            // 处理文本内容
        }
    }
    // ...
}
```

### 8.5 网络优化

```go
// 调整 Kafka 配置以最大化吞吐量
config := sarama.NewConfig()

// Consumer 侧
config.Consumer.Fetch.Min = 1024 * 1024       // 1MB 最小拉取
config.Consumer.Fetch.Default = 10 * 1024 * 1024  // 10MB 默认拉取
config.Consumer.MaxProcessingTime = 1 * time.Second

// Producer 侧
config.Producer.MaxMessageBytes = 10 * 1024 * 1024  // 10MB 最大消息
config.Producer.RequiredAcks = sarama.WaitForLocal  // 平衡性能与可靠性
config.Net.MaxOpenRequests = 10  // 提高并发请求数
```

### 8.6 性能基准测试 (Benchmark)

```go
// 基准测试用例
func BenchmarkXML2JSON_Small(b *testing.B)   // 1KB XML
func BenchmarkXML2JSON_Medium(b *testing.B)  // 10KB XML
func BenchmarkXML2JSON_Large(b *testing.B)   // 100KB XML

func BenchmarkPipeline_EndToEnd(b *testing.B)  // 端到端测试

// 预期性能目标（4 核 8G 环境）:
// - 小消息 (1KB):   ≥ 80,000 msg/s
// - 中消息 (10KB):  ≥ 20,000 msg/s
// - 大消息 (100KB): ≥  3,000 msg/s
```

---

## 9. 接口规范

### 9.1 统一响应格式

```json
{
  "code": 0,
  "message": "success",
  "data": {},
  "timestamp": "2026-07-02T10:00:00Z"
}
```

错误码定义：

| code | 含义 |
|------|------|
| 0 | 成功 |
| 1001 | 参数校验失败 |
| 1002 | 管道不存在 |
| 1003 | 管道状态不允许该操作 |
| 1004 | Kafka 连接失败 |
| 1005 | XML 格式错误 |
| 2001 | 内部错误 |

### 9.2 WebSocket 推送格式

```json
{
  "type": "metrics",
  "pipelineId": "order-pipeline",
  "data": {
    "timestamp": "2026-07-02T10:00:00Z",
    "consumeRate": 12500.5,
    "produceRate": 12498.3,
    "errorRate": 0.02,
    "lag": 150,
    "latencyP99": 3.2,
    "uptime": 86400
  }
}
```

---

## 10. 部署方案

### 10.1 Docker 部署

```dockerfile
# deployments/Dockerfile
FROM golang:1.22-alpine AS builder

RUN apk add --no-cache git ca-certificates

WORKDIR /build
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /app/xml2json ./cmd/server

# 前端构建
FROM node:20-alpine AS frontend-builder
WORKDIR /web
COPY web/package.json web/package-lock.json ./
RUN npm ci
COPY web/ ./
RUN npm run build

# 运行镜像
FROM alpine:3.20
RUN apk add --no-cache ca-certificates tzdata
ENV TZ=Asia/Shanghai

COPY --from=builder /app/xml2json /app/xml2json
COPY --from=frontend-builder /web/dist /app/web/dist
COPY configs/ /app/configs/

WORKDIR /app
EXPOSE 8080

ENTRYPOINT ["/app/xml2json", "--config", "/app/configs/config.yaml"]
```

```yaml
# deployments/docker-compose.yaml
version: '3.8'
services:
  xml2json:
    build:
      context: ..
      dockerfile: deployments/Dockerfile
    ports:
      - "8080:8080"
    volumes:
      - ./configs:/app/configs
    environment:
      - GOMAXPROCS=4
      - GOGC=100
    restart: unless-stopped
    depends_on:
      - kafka

  kafka:
    image: confluentinc/cp-kafka:7.6.0
    environment:
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:9092
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
    ports:
      - "9092:9092"

  zookeeper:
    image: confluentinc/cp-zookeeper:7.6.0
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
```

### 10.2 Kubernetes 部署

```yaml
# deployments/k8s/deployment.yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: xml2json
  labels:
    app: xml2json
spec:
  replicas: 3  # 多实例部署，Kafka 消费者组自动负载均衡
  selector:
    matchLabels:
      app: xml2json
  template:
    metadata:
      labels:
        app: xml2json
      annotations:
        prometheus.io/scrape: "true"
        prometheus.io/port: "8080"
        prometheus.io/path: "/metrics"
    spec:
      containers:
      - name: xml2json
        image: xml2json:latest
        ports:
        - containerPort: 8080
          name: http
        resources:
          requests:
            cpu: "2"
            memory: "256Mi"
          limits:
            cpu: "4"
            memory: "512Mi"
        livenessProbe:
          httpGet:
            path: /api/v1/health
            port: 8080
          initialDelaySeconds: 10
          periodSeconds: 10
        readinessProbe:
          httpGet:
            path: /api/v1/health
            port: 8080
          initialDelaySeconds: 5
          periodSeconds: 5
        volumeMounts:
        - name: config
          mountPath: /app/configs
      volumes:
      - name: config
        configMap:
          name: xml2json-config
```

### 10.3 水平扩展说明

- **消费侧扩展**: Kafka 消费者组机制天然支持水平扩展，增加实例数即可自动 rebalance 分区
- **生产侧扩展**: 每个实例独立生产，Kafka Broker 层面做负载均衡
- **限制**: 消费者组内实例数 ≤ Topic 分区数（多余的实例空闲）

---

## 11. 测试方案

### 11.1 测试金字塔

```
           ┌──────┐
           │ E2E  │  完整链路测试（Docker Compose 环境）
           ├──────┤
           │ 集成  │  Kafka 集成测试、API 集成测试
           ├──────────┤
           │   单元测试  │  各模块独立测试，覆盖率 ≥ 80%
           └──────────┘
```

### 11.2 单元测试

| 模块 | 测试内容 | 工具 |
|------|---------|------|
| converter | XML → JSON 转换正确性，边界条件 | go test |
| converter | 各种命名空间模式 | go test |
| converter | XPath 映射规则 | go test |
| kafka/consumer | Mock 消费者消息处理 | gomock |
| kafka/producer | Mock 生产者发送逻辑 | gomock |
| pipeline | 管道生命周期管理 | go test |
| api | HTTP API 处理逻辑 | httptest |

### 11.3 集成测试

```go
// 使用 testcontainers-go 启动真实 Kafka
func TestPipelineIntegration(t *testing.T) {
    // 1. 启动 Kafka 容器
    kafkaContainer, err := testcontainers.GenericContainer(...)

    // 2. 创建测试管道配置
    cfg := &PipelineConfig{...}

    // 3. 启动管道
    pipeline := NewPipeline(cfg)
    go pipeline.Run(ctx)

    // 4. 发送测试 XML 消息到源 Topic
    sendXMLMessage(t, kafkaContainer, "test.xml.topic", sampleXML)

    // 5. 消费目标 Topic 并验证 JSON 输出
    jsonMsg := consumeJSONMessage(t, kafkaContainer, "test.json.topic")

    // 6. 断言
    assert.JSONEq(t, expectedJSON, string(jsonMsg))
}
```

### 11.4 性能测试

```bash
# 使用 kafka-producer-perf-test 生产测试数据
kafka-producer-perf-test \
  --topic order.xml \
  --num-records 1000000 \
  --record-size 1024 \
  --throughput 100000 \
  --producer-props bootstrap.servers=localhost:9092

# 监控 xml2json 的吞吐量和资源使用
# 通过 Prometheus + Grafana 观察
```

---

## 12. 项目里程碑

| 阶段 | 周期 | 交付物 |
|------|------|--------|
| **Phase 1: 基础框架** | 第 1-2 周 | 项目骨架、配置管理、日志、API 框架 |
| **Phase 2: 核心转换** | 第 2-3 周 | XML→JSON 转换器、Kafka Consumer/Producer 封装 |
| **Phase 3: 管道管理** | 第 3-4 周 | Pipeline Manager、多管道支持、生命周期管理 |
| **Phase 4: Web 界面** | 第 4-5 周 | 管道管理页面、监控 Dashboard、管道编辑器 |
| **Phase 5: 性能优化** | 第 5-6 周 | 基准测试、内存优化、并发调优 |
| **Phase 6: 测试与文档** | 第 6-7 周 | 单元测试、集成测试、部署文档、API 文档 |
| **Phase 7: 上线与运维** | 第 7-8 周 | 灰度发布、监控告警接入、运维手册 |

---

## 13. 附录

### 13.1 依赖列表 (go.mod)

```
module xml2json-go

go 1.22

require (
    github.com/IBM/sarama          v1.43.2
    github.com/bytedance/sonic     v1.11.6
    github.com/gin-gonic/gin       v1.10.0
    github.com/prometheus/client_golang v1.19.1
    github.com/spf13/viper         v1.19.0
    go.uber.org/zap                v1.27.0
    github.com/google/uuid         v1.6.0
    github.com/stretchr/testify    v1.9.0
    github.com/beevik/etree        v1.3.0
)
```

### 13.2 运行命令

```bash
# 开发运行
go run ./cmd/server --config configs/config.yaml

# 构建
make build

# Docker 运行
docker-compose -f deployments/docker-compose.yaml up -d

# 运行测试
go test ./... -v -cover

# 运行基准测试
go test -bench=. -benchmem ./internal/converter/
```

### 13.3 环境变量

| 变量名 | 描述 | 默认值 |
|--------|------|--------|
| `CONFIG_PATH` | 配置文件路径 | `configs/config.yaml` |
| `SERVER_PORT` | API 服务端口 | `8080` |
| `LOG_LEVEL` | 日志级别 | `info` |
| `GOMAXPROCS` | Go 最大并行数 | CPU 核数 |
| `GOGC` | GC 触发百分比 | `100` |
| `METRICS_PORT` | Prometheus 指标端口 | `8080` |

### 13.4 参考资源

- [Sarama Kafka Client](https://github.com/IBM/sarama)
- [sonic JSON Library](https://github.com/bytedance/sonic)
- [Gin Web Framework](https://github.com/gin-gonic/gin)
- [Kafka 官方文档 - Consumer Configuration](https://kafka.apache.org/documentation/#consumerconfigs)
- [Kafka 官方文档 - Producer Configuration](https://kafka.apache.org/documentation/#producerconfigs)
- [Go sync.Pool 优化指南](https://go.dev/doc/effective_go#allocation_new)
