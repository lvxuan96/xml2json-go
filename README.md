# xml2json-go

高性能 XML → JSON 转换中间件，基于 Kafka 消息流转，**单进程支持多管道并行运行**，支持热配置与水平扩展。

## 特性

- **多管道并行**: 单进程运行多条独立管道，每条有自己的 Consumer Group + Worker Pool + Producer
- **Kafka 原生集成**: 消费 XML 消息 → 转换 → 投递 JSON 消息，全链路 Kafka
- **高性能**: Worker Pool 协程池 + Sarama 异步生产者 + 对象池复用，10 条管道仅 ~40MB 内存
- **热配置**: 通过 REST API 或配置文件动态增删管道，无需重启
- **可水平扩展**: 增加实例副本即可，Kafka 消费者组自动 rebalance 分区

## 快速开始

### 前置条件

- Go 1.21+
- Kafka 集群

### 安装与运行

```bash
cd xml2json-go

# 安装依赖
go mod tidy

# 修改配置（填写 Kafka 地址和管道列表）
vim configs/config.yaml

# 启动服务
go run ./cmd/server --config configs/config.yaml

# 或编译后运行
go build -o bin/xml2json ./cmd/server
./bin/xml2json --config configs/config.yaml
```

### 管理管道

```bash
# 查看所有管道
curl http://localhost:8080/api/v1/pipelines

# 启动管道
curl -X POST http://localhost:8080/api/v1/pipelines/order-pipeline/start

# 停止管道
curl -X POST http://localhost:8080/api/v1/pipelines/order-pipeline/stop

# 创建新管道
curl -X POST http://localhost:8080/api/v1/pipelines \
  -H "Content-Type: application/json" \
  -d '{
    "id": "user-pipeline",
    "name": "用户数据转换",
    "source": {
      "brokers": ["localhost:9092"],
      "topics": ["user-xml"],
      "groupId": "xml2json-user-group"
    },
    "sink": {
      "brokers": ["localhost:9092"],
      "topic": "user-json"
    }
  }'

# 更新管道配置（热重载）
curl -X PUT http://localhost:8080/api/v1/pipelines/order-pipeline \
  -H "Content-Type: application/json" \
  -d '{"transform": {"stripLevels": 2}}'

# 删除管道
curl -X DELETE http://localhost:8080/api/v1/pipelines/user-pipeline
```

### 测试 XML → JSON 转换

```bash
curl -X POST http://localhost:8080/api/v1/preview \
  -H "Content-Type: application/json" \
  -d '{
    "xml": "<order id=\"12345\"><item sku=\"A001\" qty=\"2\">无线鼠标</item></order>",
    "options": {"stripLevels": 0}
  }'
```

## 项目结构

```
xml2json-go/
├── cmd/
│   └── server/
│       └── main.go                    # 入口：加载配置 → 创建 PipelineManager → HTTP 服务 → 优雅关闭
├── internal/
│   ├── config/
│   │   └── config.go                  # 配置模型（多管道数组）+ Viper 加载 + 校验
│   ├── converter/
│   │   ├── xml2json.go                # XML → JSON 核心转换引擎 + 预处理 + 层级跳过
│   │   └── xml2json_test.go           # 单元测试（10 个用例）
│   ├── kafka/
│   │   ├── consumer.go                # Kafka 消费者组封装（Sarama）
│   │   └── producer.go                # Kafka 异步生产者封装（Sarama）
│   ├── pipeline/
│   │   ├── pipeline.go                # 单条管道：Consumer → Worker Pool → Producer
│   │   └── manager.go                 # 管道管理器：多条管道的增删启停
│   └── api/
│       ├── router.go                  # Gin 路由注册（多管道 REST API）
│       ├── handler/
│       │   ├── pipeline.go            # 管道 CRUD + 启停 + 预览 API
│       │   └── health.go              # 健康检查
│       └── middleware/
│           └── middleware.go          # CORS + 请求日志
├── configs/
│   └── config.yaml                    # 配置文件（多管道数组，含示例）
├── Makefile                           # 构建 / 测试 / 运行
├── DEVELOPMENT_DOC.md                 # 详细开发文档
├── go.mod
└── README.md
```

## REST API

### 管道管理

| 方法 | 路径 | 说明 |
|------|------|------|
| `GET` | `/api/v1/pipelines` | 列出所有管道，含 `total` 和 `running` 统计 |
| `POST` | `/api/v1/pipelines` | 创建新管道 |
| `GET` | `/api/v1/pipelines/:id` | 获取管道详情（配置 + 状态 + 指标） |
| `PUT` | `/api/v1/pipelines/:id` | 更新管道配置（热重载） |
| `DELETE` | `/api/v1/pipelines/:id` | 删除管道（运行中先停止） |
| `POST` | `/api/v1/pipelines/:id/start` | 启动管道 |
| `POST` | `/api/v1/pipelines/:id/stop` | 停止管道 |
| `GET` | `/api/v1/pipelines/:id/metrics` | 获取管道指标 |

### 其他

| 方法 | 路径 | 说明 |
|------|------|------|
| `GET` | `/api/v1/health` | 健康检查 |
| `POST` | `/api/v1/preview` | XML → JSON 预览（独立使用，不绑定管道） |

### 统一响应格式

```json
{
  "code": 0,
  "message": "success",
  "data": {},
  "error": ""
}
```

| code | 含义 |
|------|------|
| 0 | 成功 |
| 1001 | 参数校验失败 |
| 1002 | 管道不存在 |
| 1003 | 管道状态不允许该操作 |
| 1005 | XML 格式错误 |
| 2001 | 内部错误 |

## 配置说明

```yaml
# configs/config.yaml
server:
  port: 8080

pipelines:                          # 管道列表，可配置多条
  - id: "order-pipeline"            # 唯一标识（必填）
    name: "订单数据转换"
    enabled: false                  # 启动时是否自动运行
    workers: 4                      # Worker 协程数（CPU 核数 × 2）
    bufferSize: 1024                # 内部通道缓冲

    source:                         # === Kafka 消费端 ===
      brokers: ["localhost:9092"]
      topics: ["test-xml"]
      groupId: "xml2json-order-group"
      autoOffsetReset: "earliest"   # earliest | latest

    transform:                      # === 转换规则 ===
      attributePrefix: "@"          # XML 属性前缀
      textKey: "#text"              # 文本节点 key
      namespaceMode: "strip"        # keep | strip | expand
      stripLevels: 2                # 跳过外层层级: 0=不跳过, 2=跳过<ROWSET><ROW>
      strictMode: false             # 严格模式: XML 错误时丢弃消息

    sink:                           # === Kafka 生产端 ===
      brokers: ["localhost:9092"]
      topic: "test-json"
      acks: 1                       # 0 | 1 | -1(all)
      compression: "zstd"           # none | gzip | snappy | lz4 | zstd
      partitioner: "hash"           # hash | roundRobin | random

  # 第二条管道示例
  - id: "user-pipeline"
    ...
```

### 关键配置项

| 配置项 | 说明 | 默认值 |
|--------|------|--------|
| `stripLevels` | 跳过 XML 外层的包装层级数 | `0` |
| `attributePrefix` | XML 属性在 JSON 中的 key 前缀 | `@` |
| `textKey` | XML 文本节点的 JSON key | `#text` |
| `namespaceMode` | 命名空间处理: `keep` / `strip` / `expand` | `strip` |
| `strictMode` | XML 格式错误时是否丢弃消息 | `false` |
| `workers` | 转换 Worker 协程数 | `4` |
| `acks` | 生产确认: `0`=无 `1`=Leader `-1`=全部副本 | `1` |

## 转换示例

### 输入 (XML)

```xml
<ROWSET>
<ROW>
<order id>12345</order id>
<customer name>张三</customer name>
<item>无线鼠标</item>
</ROW>
</ROWSET>
```

> 畸形 XML（标签名含空格）会自动预处理为 `<order_id>12345</order_id>`。

### 输出 (JSON) — `stripLevels: 2`

```json
{
  "order_id": "12345",
  "customer_name": "张三",
  "item": "无线鼠标"
}
```

### 转换规则

| XML 结构 | JSON 输出 |
|----------|-----------|
| `<elem>text</elem>` | `{"elem": "text"}` |
| `<elem attr="val"/>` | `{"elem": {"@attr": "val"}}` |
| `<elem attr="val">text</elem>` | `{"elem": {"@attr": "val", "#text": "text"}}` |
| 重复同名子元素 | 自动合并为 JSON 数组 |
| `<tag name>` (畸形) | 自动修复为 `<tag_name>` |

## 架构

```text
┌───────────────────────────────────┐
│           单进程 (Go)              │
│  ┌─────────────────────────────┐  │
│  │       PipelineManager       │  │
│  │  ┌───────┐ ┌───────┐ ┌───┐ │  │
│  │  │管道 A │ │管道 B │ │...│ │  │
│  │  │Consume│ │Consume│ │   │ │  │
│  │  │Convert│ │Convert│ │   │ │  │
│  │  │Produce│ │Produce│ │   │ │  │
│  │  └───────┘ └───────┘ └───┘ │  │
│  └─────────────────────────────┘  │
│         REST API (Gin)            │
└───────────────────────────────────┘
      ↓ 流量增长时增加实例
┌─────────┐ ┌─────────┐
│ 实例 2  │ │ 实例 3  │  ← Kafka 消费者组自动分配分区
└─────────┘ └─────────┘
```

## 常用命令

```bash
make build       # 编译（输出到 bin/）
make run         # 开发运行
make test        # 运行测试
make test-cover  # 测试 + 覆盖率报告
make bench       # 基准测试
make vet         # 代码检查
make tidy        # 整理依赖
make clean       # 清理构建产物
make check       # vet + test 全部检查
```

## 技术栈

| 组件 | 选型 |
|------|------|
| 语言 | Go ≥ 1.21 |
| Web 框架 | Gin |
| Kafka 客户端 | IBM/sarama |
| 配置管理 | Viper |
| 日志 | Zap (Uber) |
| XML 解析 | encoding/xml (标准库) |
| JSON 序列化 | encoding/json (标准库) |

## License

MIT
