# 项目核心约束与开发指南 (Project Constitution)

**生效时间**: 2026-07-02
**适用范围**: 本项目所有代码生成、重构、代码审查

---

## 1. 项目目标
构建一个**高性能、可扩展**的 XML -> JSON 格式转换中间件。输入输出均对接 Kafka，且必须提供 Web 可视化配置界面。

## 2. 硬性技术栈约束 (Must Follow)
- **后端语言**: **Go (Golang) 1.21+**。理由：高并发下的卓越性能、低内存占用、原生支持协程，以应对高吞吐量场景。
- **Web 框架**: **Gin** 或 **Echo**（轻量级高性能 HTTP 服务）。
- **Kafka 客户端**: **IBM/sarama**（纯 Go 实现，性能优秀）或 **Confluent Kafka Go**。
- **前端 UI**: **Vue3 + TypeScript + Vite**（提供现代化配置页面）。
- **配置存储**: **SQLite**（单机版）或 **PostgreSQL**（集群版），用于持久化数据源、转换规则和 Pipeline 配置。
- **监控指标**: 必须暴露 `/metrics` 接口供 Prometheus 抓取。

## 3. 核心架构约束 (Design Rules)
- **微内核 + Pipeline 模式**：系统必须支持**多 Pipeline 并行运行**。每个 Pipeline 包含独立的 `Source(Consumer)` -> `Transformer` -> `Sink(Producer)`。
- **扩展性**：设计时必须考虑插件化。后续可能接入 MySQL、HTTP Webhook 等非 Kafka 数据源，因此 `Source` 和 `Sink` 必须通过 **Interface (接口)** 定义，禁止硬编码为单一 Kafka 实现。
- **高并发模型**：必须采用 **Worker Pool（协程池）** 模式。禁止为每条消息单独创建 Goroutine，必须使用 Channel 进行任务分发，控制并发数（可配置）。

## 4. 配置与业务规则
- **动态配置**：所有 Kafka 连接信息（Broker IP、Port、Topic、分区数、消费策略 `earliest/latest`）必须**存放于数据库/配置文件中**，允许通过 Web UI 动态增删改查，并**实时热生效**（无需重启服务）。
- **分区处理**：生产者配置必须支持指定 `partition` 或使用 `round-robin/hash` 路由策略。

## 5. 性能基准红线 (Non-negotiable)
- 单节点吞吐量目标: **≥ 10,000 条/秒**。
- 端到端延迟 (P99): **≤ 100ms**。
- 内存限制：堆内存使用 ≤ 2GB。
- **禁止**在转换逻辑中使用反射（`reflect`）处理高频数据路径，优先使用 `encoding/xml` 和 `encoding/json` 的结构体映射（Struct Tags）。

## 6. 代码生成规范
- **错误处理**：严禁忽略 `error`，必须使用 `fmt.Errorf` 或 `errors.Wrap` 包装上下文。
- **日志**：必须使用结构化日志（如 `logrus` 或 `zap`），包含 `trace_id` 以便链路追踪。
- **单元测试**：核心转换逻辑必须附带 `_test.go` 单元测试，覆盖率不低于 80%。

## 7. 项目目录结构约定 (Standard Layout)
生成代码时，请遵循以下标准 Go 项目布局：
- `/cmd/server/` : 主程序入口
- `/internal/core/` : 核心业务逻辑（Consumer, Transformer, Producer 实现）
- `/internal/config/` : 配置结构体定义与加载
- `/internal/api/` : RESTful API 路由和 Handler
- `/web/` : 前端 Vue3 项目源码
- `/docs/` : 人类阅读的详细设计文档