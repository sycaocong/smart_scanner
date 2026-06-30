# Smart Scanner - 多链区块链数据扫描系统

Smart Scanner 是一套企业级的多链区块链数据扫描系统，支持实时监控多个区块链网络上的交易、转账和合约事件。

## ✨ 核心特性

### 多链支持
- **EVM 兼容链**: Ethereum、BSC、Polygon、Arbitrum、Optimism、Avalanche 等
- **Solana**: 原生支持 Solana 生态链
- **Tron**: 支持 Tron 区块链
- **可扩展架构**: 通过插件机制轻松添加新链支持

### 高性能扫描
- **分片扫描**: 按区块范围分片，支持并行处理
- **Worker 池**: 每个分片独立的 Worker 池，提高吞吐量
- **水位线追踪**: 精确追踪各分片扫描进度
- **增量同步**: 支持断点续扫，无需从头开始

### Reorg 处理
- **分叉检测**: 实时检测区块链分叉
- **数据回滚**: 自动回滚受影响的分叉数据
- **状态恢复**: 从正确链重新处理数据
- **幂等消费**: 保证消息处理的精确一次语义

### 实时处理
- **Kafka 消息队列**: 异步事件流处理
- **Webhook 通知**: 实时推送扫描结果
- **状态机**: 支持复杂业务流程的状态管理
- **指标监控**: 完整的 Prometheus 指标暴露

## 🏗️ 架构设计

```
┌─────────────────────────────────────────────────────────────────────┐
│                         区块链节点层                                │
│  Ethereum | BSC | Polygon | Solana | Tron | Arbitrum | Optimism    │
│  (RPC节点 / 自建节点)                                                │
└─────────────────────────────────────────────────────────────────────┘
                              │ HTTP/WebSocket
                              ▼
┌─────────────────────────────────────────────────────────────────────┐
│                         扫描调度层                                  │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐                 │
│  │  Shard #1   │  │  Shard #2   │  │    ...      │                 │
│  │  Worker Pool│  │  Worker Pool│  │             │                 │
│  │  Watermark  │  │  Watermark  │  │             │                 │
│  └─────────────┘  └─────────────┘  └─────────────┘                 │
│  SchedulerManager | ChainScanner | ReorgHandler                    │
└─────────────────────────────────────────────────────────────────────┘
                              │ 原始区块数据
                              ▼
┌─────────────────────────────────────────────────────────────────────┐
│                         数据解析层                                  │
│  EVM Parser | Solana Parser | Tron Parser | Custom Parsers         │
│  - 交易解析 | - 事件日志解析 | - 合约调用解析                        │
└─────────────────────────────────────────────────────────────────────┘
                              │ 结构化事件
                              ▼
┌─────────────────────────────────────────────────────────────────────┐
│                         消息队列层                                  │
│                      Kafka / RocketMQ                              │
│  Topics: blocks, transactions, logs, transfers, events             │
└─────────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────────┐
│                         存储层                                      │
│  ┌──────────┐ ┌──────────┐ ┌──────────────┐ ┌──────────────┐       │
│  │  MySQL   │ │  Redis   │ │ Elasticsearch│ │ ClickHouse   │       │
│  │(业务数据) │ │(缓存/锁) │ │  (日志搜索)   │ │ (时序数据)   │       │
│  └──────────┘ └──────────┘ └──────────────┘ └──────────────┘       │
└─────────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────────┐
│                         业务消费层                                  │
│  ┌────────────┐ ┌──────────┐ ┌──────────────┐ ┌─────────────┐       │
│  │Consumer    │ │Webhook   │ │StateMachine  │ │Idempotency  │       │
│  │(事件消费)   │ │(通知推送) │ │(状态管理)    │ │(幂等处理)   │       │
│  └────────────┘ └──────────┘ └──────────────┘ └─────────────┘       │
│  ┌────────────┐ ┌──────────┐                                        │
│  │Wallet      │ │Plugin    │                                        │
│  │(钱包追踪)   │ │(插件管理) │                                        │
│  └────────────┘ └──────────┘                                        │
└─────────────────────────────────────────────────────────────────────┘
```

## 📁 项目结构

```
smart_scanner/
├── cmd/                    # 命令行入口
│   ├── mock-node/          # Mock 节点服务器
│   │   └── main.go
│   └── scanner/            # 扫描器主程序
│       └── main.go
├── internal/               # 内部模块
│   ├── consumer/           # 消息消费者
│   │   ├── consumer.go
│   │   ├── idempotency.go
│   │   ├── reorg_handler.go
│   │   ├── state_machine.go
│   │   └── webhook.go
│   ├── node/               # 节点客户端
│   │   ├── chain_factory.go
│   │   ├── evm_client.go
│   │   ├── http_client.go
│   │   ├── mock_evm_server.go
│   │   └── node_manager.go
│   ├── parser/             # 数据解析器
│   │   ├── evm_parser.go
│   │   ├── other_parsers.go
│   │   └── parser.go
│   ├── plugin/             # 插件系统
│   │   ├── adapter.go
│   │   ├── evm_adapter.go
│   │   ├── manager.go
│   │   ├── solana_adapter.go
│   │   └── tron_adapter.go
│   ├── queue/              # 消息队列
│   │   ├── kafka.go
│   │   ├── queue.go
│   │   └── rocketmq.go
│   ├── reorg/              # Reorg 处理
│   │   ├── detector.go
│   │   └── processor.go
│   ├── scheduler/          # 扫描调度
│   │   ├── chain_scanner.go
│   │   ├── reorg_handler.go
│   │   ├── scheduler_manager.go
│   │   ├── sharding.go
│   │   ├── watermark.go
│   │   └── worker.go
│   ├── storage/            # 存储层
│   │   ├── database.go
│   │   ├── elasticsearch.go
│   │   ├── redis.go
│   │   └── storage.go
│   └── wallet/             # 钱包服务
│       ├── handler.go
│       ├── service.go
│       └── types.go
├── pkg/                    # 公共包
│   ├── config/             # 配置管理
│   ├── lock/               # 分布式锁
│   ├── logger/             # 日志系统
│   ├── metrics/            # 指标监控
│   ├── types/              # 通用类型
│   └── util/               # 工具函数
├── docs/                   # 文档
│   ├── API.md
│   ├── DESIGN_SCANNER.md
│   └── node-deployment-guide.md
├── config.yaml             # 配置文件
├── docker-compose.yml      # Docker Compose 配置
├── Dockerfile              # Docker 镜像构建
└── go.mod                  # Go 依赖管理
```

## 🚀 快速开始

### 环境要求

- **Go**: 1.21+
- **Docker**: 24.0+
- **Docker Compose**: 2.20+
- **MySQL**: 8.0+
- **Redis**: 7.0+
- **Kafka**: 3.5+

### 使用 Docker 启动

```bash
# 进入项目目录
cd smart_scanner

# 启动所有服务
docker-compose up -d

# 查看服务状态
docker-compose ps

# 查看日志
docker-compose logs -f scanner
```

### 本地开发

```bash
# 下载依赖
go mod download

# 运行扫描器
go run cmd/scanner/main.go --config config.yaml

# 运行 Mock 节点（用于测试）
go run cmd/mock-node/main.go
```

### 配置说明

配置文件位于 `config.yaml`，主要配置项包括：

```yaml
# 服务器配置
server:
  port: 8080

# 区块链节点配置
chains:
  - name: ethereum
    type: evm
    endpoint: https://mainnet.infura.io/v3/your-project-id
    start_block: 0
    scan_interval: 1000

# Kafka 配置
kafka:
  brokers:
    - localhost:9092
  topic_prefix: smart_scanner

# 数据库配置
database:
  mysql:
    dsn: root:password@tcp(localhost:3306)/smart_scanner?charset=utf8mb4&parseTime=True&loc=Local

# Redis 配置
redis:
  addr: localhost:6379
  password: ""
  db: 0
```

## 🧪 测试

```bash
# 运行所有测试
go test ./...

# 运行特定模块测试
go test ./internal/scanner/...

# 生成测试覆盖率报告
go test -coverprofile=coverage.out ./...
go tool cover -html=coverage.out
```

## 📊 监控指标

系统暴露以下 Prometheus 指标：

| 指标名 | 类型 | 说明 |
|--------|------|------|
| `scanner_blocks_scanned_total` | Counter | 已扫描区块总数 |
| `scanner_transactions_scanned_total` | Counter | 已扫描交易总数 |
| `scanner_logs_scanned_total` | Counter | 已扫描日志总数 |
| `scanner_current_block` | Gauge | 当前扫描高度 |
| `scanner_latency_seconds` | Histogram | 扫描延迟 |
| `scanner_reorg_count_total` | Counter | Reorg 发生次数 |
| `scanner_queue_depth` | Gauge | 消息队列深度 |

## 🔧 API 接口

### 获取扫描状态

```bash
GET /api/v1/scanner/status
```

### 获取钱包交易

```bash
GET /api/v1/wallet/{address}/transactions?limit=100&offset=0
```

### 订阅钱包

```bash
POST /api/v1/wallet/subscribe
Content-Type: application/json

{
  "address": "0x1234...",
  "chain": "ethereum"
}
```

完整 API 文档请参考 [docs/API.md](docs/API.md)

## 📖 文档

- [设计文档](docs/DESIGN_SCANNER.md) - 系统架构和设计细节
- [API 文档](docs/API.md) - REST API 接口说明
- [节点部署指南](docs/node-deployment-guide.md) - 区块链节点部署说明

## 📝 贡献指南

欢迎提交 Issue 和 Pull Request！

1. Fork 本仓库
2. 创建特性分支 (`git checkout -b feature/AmazingFeature`)
3. 提交更改 (`git commit -m 'Add some AmazingFeature'`)
4. 推送到分支 (`git push origin feature/AmazingFeature`)
5. 打开 Pull Request

## 📄 许可证

本项目采用 MIT 许可证 - 详见 [LICENSE.txt](LICENSE.txt)

## 📞 联系方式

如有问题或建议，请通过以下方式联系：

<div align='center'>
    <img src="./微信图片_20260630212706_17_2.jpg" alt="作者二维码" width="30%">
    <p>发送邮件至 494919536@qq.com</p>
</div>