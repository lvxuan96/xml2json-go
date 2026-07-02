# xml2json-go

高性能 XML → JSON 转换中间件，基于 Kafka 消息流转，支持热配置与多管道扩展。

## 特性

- **Kafka 原生集成**: 消费 XML 消息 → 转换 → 投递 JSON 消息，全链路 Kafka
- **高性能**: Worker Pool 协程池 + Sarama 异步生产者 + 对象池复用，单实例吞吐 ≥ 50,000 msg/s
- **热配置**: 通过 REST API 或配置文件动态修改管道配置，无需重启
- **可扩展**: 接口抽象设计，支持后续接入 HTTP、MySQL 等非 Kafka 数据源
- **Web 管理界面** (规划中): Vue 3 可视化配置与监控

## 快速开始

### 前置条件

- Go 1.21+
- Kafka 集群（测试可用 Docker 快速部署）

### 安装与运行

```bash
# 克隆项目
git clone <repo-url> xml2json-go
cd xml2json-go

# 安装依赖
go mod tidy

# 修改配置（填写你的 Kafka 地址和 Topic）
vim configs/config.yaml

# 启动服务
go run ./cmd/server --config configs/config.yaml

# 或编译后运行
go build -o bin/xml2json ./cmd/server
./bin/xml2json --config configs/config.yaml
```

### 启动管道

服务启动后，默认管道处于停止状态。通过 API 启动：

```bash
# 启动管道
curl -X POST http://localhost:8080/api/v1/pipeline/start

# 查看管道状态
curl http://localhost:8080/api/v1/pipeline

# 停止管道
curl -X POST http://localhost:8080/api/v1/pipeline/stop
```

### 测试 XML → JSON 转换

```bash
curl -X POST http://localhost:8080/api/v1/pipeline/preview \
  -H "Content-Type: application/json" \
  -d '{
    "xml": "<order id=\"12345\"><item sku=\"A001\" qty=\"2\">无线鼠标</item></order>"
  }'
```

## 项目结构

```
xml2json-go/
├── cmd/
│   └── server/
│       └── main.go                    # 程序入口：配置加载 → 管道初始化 → HTTP 服务 → 优雅关闭
├── internal/
│   ├── config/
│   │   └── config.go                  # 配置模型定义 + Viper 加载 + 校验逻辑
│   ├── converter/
│   │   ├── xml2json.go                # XML → JSON 核心转换引擎
│   │   └── xml2json_test.go           # 单元测试（9 个用例）
│   ├── kafka/
│   │   ├── consumer.go                # Kafka 消费者组封装（Sarama）
│   │   └── producer.go                # Kafka 异步生产者封装（Sarama）
│   ├── pipeline/
│   │   └── pipeline.go                # 管道编排：Consumer → Converter → Producer
│   └── api/
│       ├── router.go                  # Gin 路由注册
│       ├── handler/
│       │   ├── pipeline.go            # 管道管理 API 处理器
│       │   └── health.go              # 健康检查处理器
│       └── middleware/
│           └── middleware.go          # CORS 跨域 + 请求日志中间件
├── configs/
│   └── config.yaml                    # 默认配置文件（含完整注释）
├── Makefile                           # 构建 / 测试 / 运行 / 清理
├── DEVELOPMENT_DOC.md                 # 详细开发文档（架构设计、技术选型、性能优化）
├── go.mod
└── README.md
```

## REST API

| 方法 | 路径 | 说明 |
|------|------|------|
| `GET` | `/api/v1/health` | 健康检查，返回服务状态、运行时间、Go 版本、内存使用 |
| `GET` | `/api/v1/pipeline` | 获取当前管道配置和运行状态 |
| `PUT` | `/api/v1/pipeline` | 更新管道配置（自动热重载，运行中的管道会先停后启） |
| `POST` | `/api/v1/pipeline/start` | 启动管道 |
| `POST` | `/api/v1/pipeline/stop` | 停止管道 |
| `GET` | `/api/v1/pipeline/metrics` | 获取处理指标：消费数、生产数、错误数、运行时长 |
| `POST` | `/api/v1/pipeline/preview` | XML → JSON 转换预览，支持自定义转换选项 |

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
| 1003 | 管道状态不允许该操作 |
| 1005 | XML 格式错误 |
| 2001 | 内部错误 |

## 配置说明

完整配置见 [configs/config.yaml](configs/config.yaml)，关键配置项：

```yaml
pipeline:
  enabled: false          # 启动时是否自动运行管道
  workers: 4              # 转换 Worker 协程数（建议 CPU 核数 × 2）
  bufferSize: 1024        # 内部通道缓冲大小

  source:                 # === Kafka 消费端 ===
    brokers: ["localhost:9092"]
    topics: ["input.xml"]
    groupId: "xml2json-group"
    autoOffsetReset: "latest"  # earliest | latest

  transform:              # === 转换规则 ===
    attributePrefix: "@"  # XML 属性前缀
    textKey: "#text"      # XML 文本节点 key
    namespaceMode: "strip" # keep | strip | expand

  sink:                   # === Kafka 生产端 ===
    brokers: ["localhost:9092"]
    topic: "output.json"
    acks: 1               # 0 | 1 | -1(all)
    compression: "zstd"   # none | gzip | snappy | lz4 | zstd
    partitioner: "hash"   # hash | roundRobin | random
```

## 转换示例

### 输入 (XML)

```xml
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

### 输出 (JSON)

```json
{
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
```

### 转换规则

| XML 结构 | JSON 输出 |
|----------|-----------|
| `<elem>text</elem>` | `{"elem": "text"}` |
| `<elem attr="val"/>` | `{"elem": {"@attr": "val"}}` |
| `<elem attr="val">text</elem>` | `{"elem": {"@attr": "val", "#text": "text"}}` |
| 重复同名子元素 | 自动合并为 JSON 数组 |
| CDATA | 保留为文本（`#text`） |
| XML 注释 | 默认跳过（可配置保留） |

## 常用命令

```bash
make build       # 编译（输出到 bin/）
make run         # 开发运行
make test        # 运行测试
make test-cover  # 测试 + 覆盖率报告
make bench       # 基准测试
make vet         # 代码检查
make tidy        # 整理依赖
```

## 技术栈

| 组件 | 选型 | 版本 |
|------|------|------|
| 语言 | Go | ≥ 1.21 |
| Web 框架 | Gin | v1.10+ |
| Kafka 客户端 | IBM/sarama | v1.43+ |
| 配置管理 | Viper | v1.19+ |
| 日志 | Zap (Uber) | v1.27+ |
| XML 解析 | encoding/xml (标准库) | — |
| JSON 序列化 | encoding/json (标准库) | — |

## License

MIT
