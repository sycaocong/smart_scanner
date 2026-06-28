# Smart Scanner 设计文档

## 1. 系统概述

Smart Scanner 是一个多链区块扫描系统，采用6层企业架构设计：

| 层级 | 名称 | 职责 | 代码位置 |
|------|------|------|----------|
| 1 | 链节点接入层 | 管理链节点连接、健康检查、故障转移 | [internal/node/](./internal/node/) |
| 2 | 链扫描调度层 | 任务调度、分片管理、水位控制 | [internal/scheduler/](./internal/scheduler/) |
| 3 | 数据解析层 | 区块/交易/日志解析、数据验证 | [internal/parser/](./internal/parser/) |
| 4 | 消息队列层 | 数据分发、异步处理 | [internal/queue/](./internal/queue/) |
| 5 | 存储层 | 持久化存储、缓存 | [internal/storage/](./internal/storage/) |
| 6 | 业务消费层 | 业务逻辑处理、幂等性保障、回滚处理 | [internal/consumer/](./internal/consumer/) |

## 2. 核心数据结构

### 2.1 扫描任务 (ScanTask)

```go
// ScanTask 扫描任务
type ScanTask struct {
    ChainID     string    // 链ID
    StartBlock  uint64    // 起始区块
    EndBlock    uint64    // 结束区块
    ShardID     int       // 分片ID
    RetryCount  int       // 重试次数
    Priority    int       // 优先级
    CreatedAt   time.Time // 创建时间
}
```

**实现代码**: [types.go](pkg/types/types.go)

### 2.2 扫描结果 (ScanResult)

```go
// ScanResult 扫描结果
type ScanResult struct {
    Task          *ScanTask // 关联任务
    ChainID       string    // 链ID
    StartBlock    uint64    // 起始区块
    EndBlock      uint64    // 结束区块
    Success       bool      // 是否成功
    BlocksScanned int       // 扫描的区块数
    TxsFound      int       // 发现的交易数
    Duration      time.Duration // 耗时
    Error         error     // 错误信息
}
```

**实现代码**: [types.go](pkg/types/types.go)

### 2.3 水位 (Watermark)

```go
// Watermark 水位信息
type Watermark struct {
    ChainID          string    // 链ID
    ScannedHeight    uint64    // 已扫描高度
    ConfirmedHeight  uint64    // 已确认高度
    FinalizedHeight  uint64    // 最终确认高度
    ReorgBoundary    uint64    // 回滚边界
    LastUpdateTime   time.Time // 最后更新时间
}
```

**实现代码**: [types.go](pkg/types/types.go)

### 2.4 回滚事件 (ReorgEvent)

```go
// ReorgEvent 回滚事件
type ReorgEvent struct {
    ChainID        string    // 链ID
    DetectedAt     time.Time // 检测时间
    OldBlockNumber uint64    // 旧区块高度
    OldBlockHash   string    // 旧区块哈希
    NewBlockNumber uint64    // 新区块高度
    NewBlockHash   string    // 新区块哈希
    Depth          uint64    // 回滚深度
    Processed      bool      // 是否已处理
}
```

**实现代码**: [types.go](pkg/types/types.go)

## 3. 链扫描调度层

### 3.1 ChainScanner 链扫描器

ChainScanner 是链扫描的核心组件，负责单链的区块扫描任务调度和执行。

**核心方法:**

| 方法 | 功能 | 代码位置 |
|------|------|----------|
| `Start()` | 启动扫描器 | [chain_scanner.go#L50](./internal/scheduler/chain_scanner.go#L50) |
| `Stop()` | 停止扫描器 | [chain_scanner.go#L100](./internal/scheduler/chain_scanner.go#L100) |
| `scheduleTasks()` | 调度扫描任务 | [chain_scanner.go#L150](./internal/scheduler/chain_scanner.go#L150) |
| `processResults()` | 处理扫描结果 | [chain_scanner.go#L200](./internal/scheduler/chain_scanner.go#L200) |
| `detectReorg()` | 检测回滚 | [chain_scanner.go#L250](./internal/scheduler/chain_scanner.go#L250) |

**架构特点:**
- 多工作线程并发扫描
- 基于分片的任务分配
- 分布式锁保证同一区块高度仅一个实例处理
- 水位管理追踪扫描进度

**实现代码**: [chain_scanner.go](internal/scheduler/chain_scanner.go)

### 3.2 SchedulerManager 调度器管理器

SchedulerManager 管理所有链的扫描器，提供统一的启动、停止和状态查询接口。

**核心方法:**

| 方法 | 功能 | 代码位置 |
|------|------|----------|
| `Start()` | 启动所有扫描器 | [scheduler_manager.go#L106](./internal/scheduler/scheduler_manager.go#L106) |
| `Stop()` | 停止所有扫描器 | [scheduler_manager.go#L133](./internal/scheduler/scheduler_manager.go#L133) |
| `AddScanner()` | 添加新扫描器 | [scheduler_manager.go#L262](./internal/scheduler/scheduler_manager.go#L262) |
| `RemoveScanner()` | 移除扫描器 | [scheduler_manager.go#L307](./internal/scheduler/scheduler_manager.go#L307) |
| `GetAllStatus()` | 获取所有扫描器状态 | [scheduler_manager.go#L173](./internal/scheduler/scheduler_manager.go#L173) |

**实现代码**: [scheduler_manager.go](internal/scheduler/scheduler_manager.go)

### 3.3 WatermarkManager 水位管理器

WatermarkManager 管理链扫描的水位信息，支持分布式锁保护的水位更新和回滚重置。

**核心方法:**

| 方法 | 功能 | 代码位置 |
|------|------|----------|
| `UpdateScannedHeight()` | 更新已扫描高度 | [watermark.go#L78](./internal/scheduler/watermark.go#L78) |
| `UpdateConfirmedHeight()` | 更新已确认高度 | [watermark.go#L142](./internal/scheduler/watermark.go#L142) |
| `UpdateFinalizedHeight()` | 更新最终确认高度 | [watermark.go#L185](./internal/scheduler/watermark.go#L185) |
| `ResetToHeight()` | 重置水位到指定高度 | [watermark.go#L241](./internal/scheduler/watermark.go#L241) |

**水位状态转换:**
```
ScannedHeight → ConfirmedHeight → FinalizedHeight
    ↓              ↓                  ↓
  扫描中        已确认             最终确认
    ↓              ↓                  ↓
  ReorgBoundary  可安全处理        不可回滚
```

**实现代码**: [watermark.go](internal/scheduler/watermark.go)

### 3.4 ShardingManager 分片管理器

ShardingManager 实现基于区块高度或地址的分片策略，支持水平扩展。

**分片策略:**

| 策略 | 计算方式 | 适用场景 |
|------|----------|----------|
| `block_number` | `blockNumber % shardCount` | 均匀分配区块扫描任务 |
| `address` | `hash(address) % shardCount` | 按地址路由交易处理 |

**核心方法:**

| 方法 | 功能 | 代码位置 |
|------|------|----------|
| `CalculateShard()` | 根据区块号计算分片 | [sharding.go#L37](./internal/scheduler/sharding.go#L37) |
| `CalculateAddressShard()` | 根据地址计算分片 | [sharding.go#L50](./internal/scheduler/sharding.go#L50) |
| `GetShardRange()` | 获取分片的区块范围 | [sharding.go#L64](./internal/scheduler/sharding.go#L64) |

**实现代码**: [sharding.go](internal/scheduler/sharding.go)

### 3.5 Worker 工作线程

Worker 负责执行具体的区块扫描任务，支持顺序和并行两种扫描模式。

**扫描模式选择:**

```
区块数 <= batchSize * 2 → 顺序扫描 (scanBlocksSequential)
区块数 > batchSize * 2  → 并行扫描 (scanBlocksParallel)
```

**核心方法:**

| 方法 | 功能 | 代码位置 |
|------|------|----------|
| `Start()` | 启动工作线程 | [worker.go#L48](./internal/scheduler/worker.go#L48) |
| `scanBlocks()` | 扫描区块（模式选择） | [worker.go#L128](./internal/scheduler/worker.go#L128) |
| `scanBlocksSequential()` | 顺序扫描 | [worker.go#L147](./internal/scheduler/worker.go#L147) |
| `scanBlocksParallel()` | 并行扫描 | [worker.go#L183](./internal/scheduler/worker.go#L183) |

**实现代码**: [worker.go](internal/scheduler/worker.go)

## 4. 链节点接入层

### 4.1 ChainClient 链客户端接口

ChainClient 定义了与链节点交互的标准接口，支持多种链类型的统一访问。

```go
type ChainClient interface {
    GetBlock(ctx context.Context, number uint64) (*types.Block, error)
    GetBlockByHash(ctx context.Context, hash string) (*types.Block, error)
    GetLatestBlock(ctx context.Context) (*types.Block, error)
    GetTransaction(ctx context.Context, hash string) (*types.Transaction, error)
    BatchGetBlocks(ctx context.Context, numbers []uint64) ([]*types.Block, error)
    HealthCheck(ctx context.Context) error
    DetectReorg(ctx context.Context, oldBlockNumber uint64, oldBlockHash string) (bool, uint64, error)
    // ... 其他方法
}
```

**实现代码**: [node_manager.go#L19](internal/node/node_manager.go#L19)

### 4.2 NodeManager 节点管理器

NodeManager 管理多个 RPC 节点，实现健康检查和故障转移机制。

**核心功能:**
- 多节点权重轮询选择
- 自动健康检查（链类型自适应）
- 故障节点自动隔离
- 自动恢复健康节点

**节点选择算法:**
```
totalWeight = sum(node.Weight for all healthy nodes)
random = crypto/rand(0, totalWeight)
for node in healthyNodes:
    random -= node.Weight
    if random < 0:
        return node
```

**核心方法:**

| 方法 | 功能 | 代码位置 |
|------|------|----------|
| `GetHealthyNode()` | 获取健康节点 | [node_manager.go#L133](./internal/node/node_manager.go#L133) |
| `Call()` | 带重试的 RPC 调用 | [node_manager.go#L182](./internal/node/node_manager.go#L182) |
| `BatchCall()` | 带重试的批量 RPC 调用 | [node_manager.go#L225](./internal/node/node_manager.go#L225) |
| `GetHealthStatus()` | 获取节点健康状态 | [node_manager.go#L267](./internal/node/node_manager.go#L267) |

**实现代码**: [node_manager.go](internal/node/node_manager.go)

### 4.3 ChainClientFactory 链客户端工厂

ChainClientFactory 根据链类型创建对应的链客户端实现。

**支持的链类型:**

| 链类型 | 客户端实现 | 代码位置 |
|--------|-----------|----------|
| EVM | EVMClient | [evm_client.go](./internal/node/evm_client.go) |
| Solana | SolanaClient | [chain_factory.go#L106](./internal/node/chain_factory.go#L106) |
| Tron | TronClient | [chain_factory.go#L184](./internal/node/chain_factory.go#L184) |

**核心方法:**

| 方法 | 功能 | 代码位置 |
|------|------|----------|
| `CreateClient()` | 创建链客户端 | [chain_factory.go#L26](./internal/node/chain_factory.go#L26) |
| `RegisterClient()` | 注册链客户端 | [chain_factory.go#L58](./internal/node/chain_factory.go#L58) |

**实现代码**: [chain_factory.go](internal/node/chain_factory.go)

### 4.4 HealthChecker 健康检查器

HealthChecker 定期检查节点健康状态，支持链类型自适应的健康检查方法。

**健康检查策略:**

| 链类型 | 检查方法 |
|--------|----------|
| Ethereum/Polygon/Arbitrum | `eth_blockNumber` |
| Solana | `getHealth` |
| Tron | `eth_blockNumber` |

**状态转换:**
```
健康 → 连续失败 >= FailureThreshold → 不健康
不健康 → 连续成功 >= SuccessThreshold → 健康
```

**实现代码**: [node_manager.go#L72](internal/node/node_manager.go#L72)

## 5. 数据解析层

### 5.1 Parser 解析器接口

Parser 定义了区块、交易和日志的解析接口，支持多种链类型的统一解析。

```go
type Parser interface {
    ParseBlock(ctx context.Context, rawData interface{}) (*types.Block, error)
    ParseTransaction(ctx context.Context, rawData interface{}) (*types.Transaction, error)
    ParseLog(ctx context.Context, rawData interface{}) (*types.Log, error)
    BatchParseTransactions(ctx context.Context, rawDataList []interface{}) ([]*types.Transaction, error)
    ValidateBlock(block *types.Block) error
    // ... 其他方法
}
```

**实现代码**: [parser.go#L10](internal/parser/parser.go#L10)

### 5.2 ParserManager 解析器管理器

ParserManager 管理不同链类型的解析器，提供统一的解析入口。

**核心方法:**

| 方法 | 功能 | 代码位置 |
|------|------|----------|
| `GetParser()` | 获取解析器 | [parser.go#L100](./internal/parser/parser.go#L100) |
| `ParseBlock()` | 解析区块 | [parser.go#L118](./internal/parser/parser.go#L118) |
| `ParseTransaction()` | 解析交易 | [parser.go#L138](./internal/parser/parser.go#L138) |
| `ParseLog()` | 解析日志 | [parser.go#L158](./internal/parser/parser.go#L158) |

**实现代码**: [parser.go](internal/parser/parser.go)

## 6. 业务消费层

### 6.1 BusinessConsumer 业务消费者

BusinessConsumer 是业务消费的核心组件，负责处理扫描到的交易和日志，支持幂等性保障和回滚处理。

**处理流程:**
```
1. 幂等性检查 → 已处理则跳过
2. 状态机处理 → 更新业务状态
3. 标记已处理 → 写入幂等性存储
4. 发送 Webhook → 通知外部系统
5. 记录指标 → 更新监控数据
```

**核心方法:**

| 方法 | 功能 | 代码位置 |
|------|------|----------|
| `Start()` | 启动消费者 | [consumer.go#L115](./internal/consumer/consumer.go#L115) |
| `Stop()` | 停止消费者 | [consumer.go#L146](./internal/consumer/consumer.go#L146) |
| `ProcessTransaction()` | 处理交易 | [consumer.go#L211](./internal/consumer/consumer.go#L211) |
| `ProcessLog()` | 处理日志 | [consumer.go#L272](./internal/consumer/consumer.go#L272) |
| `ProcessReorgEvent()` | 处理回滚事件 | [consumer.go#L376](./internal/consumer/consumer.go#L376) |

**实现代码**: [consumer.go](internal/consumer/consumer.go)

### 6.2 IdempotencyHandler 幂等性处理器

IdempotencyHandler 保证消息处理的幂等性，防止重复处理导致数据不一致。

**实现方式:**
- 使用 Redis 存储已处理的消息 ID
- 支持 TTL 自动过期
- 提供 `IsProcessed()` 和 `MarkProcessed()` 方法

**实现代码**: [idempotency.go](internal/consumer/idempotency.go)

### 6.3 StateMachine 状态机

StateMachine 管理业务状态转换，确保业务逻辑的正确性。

**实现代码**: [state_machine.go](internal/consumer/state_machine.go)

### 6.4 ReorgHandler 回滚处理器

ReorgHandler 处理链回滚事件，冲正受影响的业务数据。

**实现代码**: [reorg_handler.go](internal/consumer/reorg_handler.go)

## 6.5 存储层

存储层采用多层架构设计：

### 6.5.1 DatabaseStorage 数据库存储

数据库存储用于持久化区块、交易、日志等核心数据，支持 MySQL 和 PostgreSQL。

**核心方法:**

| 方法 | 功能 | 代码位置 |
|------|------|----------|
| `SaveBlock()` | 保存区块 | [database.go#L176](./internal/storage/database.go#L176) |
| `GetBlock()` | 获取区块 | [database.go#L206](./internal/storage/database.go#L206) |
| `SaveTransaction()` | 保存交易 | [database.go#L278](./internal/storage/database.go#L278) |
| `SaveWatermark()` | 保存水位 | [database.go#L551](./internal/storage/database.go#L551) |
| `SaveReorgEvent()` | 保存回滚事件 | [database.go#L615](./internal/storage/database.go#L615) |

**实现代码**: [database.go](internal/storage/database.go)

### 6.5.2 RedisStorage Redis 缓存

Redis 存储用于缓存热点数据，提供高性能读写。

**核心方法:**

| 方法 | 功能 | 代码位置 |
|------|------|----------|
| `SaveBlock()` | 缓存区块 | [redis.go#L65](./internal/storage/redis.go#L65) |
| `GetBlock()` | 获取缓存区块 | [redis.go#L83](./internal/storage/redis.go#L83) |
| `SaveWatermark()` | 缓存水位 | [redis.go#L279](./internal/storage/redis.go#L279) |
| `BatchSaveTransactions()` | 批量缓存交易 | [redis.go#L150](./internal/storage/redis.go#L150) |

**实现代码**: [redis.go](internal/storage/redis.go)

### 6.5.3 ElasticsearchStorage 搜索存储

Elasticsearch 用于全文搜索和复杂查询，支持按地址、区块范围等条件检索。

**核心方法:**

| 方法 | 功能 | 代码位置 |
|------|------|----------|
| `SaveBlock()` | 索引区块 | [elasticsearch.go#L220](./internal/storage/elasticsearch.go#L220) |
| `GetTransactionsByAddress()` | 按地址查询交易 | [elasticsearch.go#L734](./internal/storage/elasticsearch.go#L734) |
| `GetLogs()` | 查询日志 | [elasticsearch.go#L997](./internal/storage/elasticsearch.go#L997) |
| `MarkTransactionsAsReverted()` | 批量标记回滚交易 | [elasticsearch.go#L872](./internal/storage/elasticsearch.go#L872) |

**实现代码**: [elasticsearch.go](internal/storage/elasticsearch.go)

## 7. 回滚处理

### 7.1 ReorgDetector 回滚检测器

ReorgDetector 定期检测链回滚事件，支持自动处理和手动处理两种模式。

**检测流程:**
```
1. 获取链上最新区块高度
2. 获取本地存储的最新区块
3. 比较高度和哈希
4. 如不匹配，向下查找分叉点
5. 创建回滚事件并处理
```

**核心方法:**

| 方法 | 功能 | 代码位置 |
|------|------|----------|
| `Start()` | 启动检测器 | [detector.go#L131](./internal/reorg/detector.go#L131) |
| `DetectReorg()` | 检测回滚 | [detector.go#L262](./internal/reorg/detector.go#L262) |
| `findForkPoint()` | 查找分叉点 | [detector.go#L349](./internal/reorg/detector.go#L349) |
| `autoProcessReorg()` | 自动处理回滚 | [detector.go#L517](./internal/reorg/detector.go#L517) |

**实现代码**: [detector.go](internal/reorg/detector.go)

### 7.2 ReorgProcessor 回滚处理器

ReorgProcessor 处理回滚事件，执行数据清理和状态恢复。

**处理步骤:**
```
1. validate → 验证回滚事件
2. mark_transactions → 标记回滚交易
3. revert_business_data → 冲正业务数据
4. clean_data → 清理受影响数据
5. reset_watermark → 重置扫描器水位
6. mark_processed → 标记事件为已处理
```

**核心方法:**

| 方法 | 功能 | 代码位置 |
|------|------|----------|
| `Start()` | 启动处理器 | [processor.go#L138](./internal/reorg/processor.go#L138) |
| `ProcessReorg()` | 处理回滚事件 | [processor.go#L255](./internal/reorg/processor.go#L255) |
| `processEvent()` | 执行回滚处理 | [processor.go#L304](./internal/reorg/processor.go#L304) |
| `scheduleRescan()` | 安排重扫 | [processor.go#L578](./internal/reorg/processor.go#L578) |

**实现代码**: [processor.go](internal/reorg/processor.go)

## 8. 插件系统

### 8.1 PluginManager 插件管理器

PluginManager 管理链适配器的注册和创建，支持动态扩展链类型。

**核心方法:**

| 方法 | 功能 | 代码位置 |
|------|------|----------|
| `RegisterFactory()` | 注册适配器工厂 | [manager.go#L62](./internal/plugin/manager.go#L62) |
| `CreateAdapter()` | 创建适配器 | [manager.go#L118](./internal/plugin/manager.go#L118) |
| `RegisterAdapter()` | 注册适配器 | [manager.go#L157](./internal/plugin/manager.go#L157) |
| `GetAdapter()` | 获取适配器 | [manager.go#L209](./internal/plugin/manager.go#L209) |

**实现代码**: [manager.go](internal/plugin/manager.go)

### 8.2 ChainAdapter 链适配器

ChainAdapter 定义了链适配器的标准接口，支持链特定的业务逻辑扩展。

**实现代码**: [adapter.go](internal/plugin/adapter.go)

## 9. 配置管理

### 9.1 Config 配置结构

Config 定义了系统的完整配置结构，支持 YAML 文件加载和验证。

**配置层级:**
```
Config
├── App (应用配置)
├── Server (服务配置)
├── Database (数据库配置)
├── Redis (缓存配置)
├── Elasticsearch (搜索配置)
├── Kafka (消息队列配置)
├── DistributedLock (分布式锁配置)
│   ├── Redis
│   └── Etcd
├── Scanner (扫描器配置)
│   ├── Concurrency (并发配置)
│   ├── Watermark (水位配置)
│   └── Sharding (分片配置)
├── Chains (链配置)
│   └── ChainConfig (单链配置)
│       ├── RPCNodes
│       └── HealthCheck
├── Monitoring (监控配置)
├── Logging (日志配置)
└── Deployment (部署配置)
```

**核心方法:**

| 方法 | 功能 | 代码位置 |
|------|------|----------|
| `LoadConfig()` | 加载配置文件 | [config.go#L276](./pkg/config/config.go#L276) |
| `GetEnabledChains()` | 获取启用的链列表 | [config.go#L384](./pkg/config/config.go#L384) |
| `GetChainConfig()` | 获取链配置 | [config.go#L395](./pkg/config/config.go#L395) |

**实现代码**: [config.go](pkg/config/config.go)

## 10. 分布式锁

### 10.1 LockManager 锁管理器

LockManager 提供分布式锁服务，支持 Redis 和 Etcd 两种模式。

**锁类型:**
- 水位锁 (`watermark:{chainID}`) - 保护水位更新
- 扫描锁 (`scanner:{chainID}`) - 保护扫描任务调度

**实现代码**: [lock.go](pkg/lock/lock.go)

## 11. 监控指标

### 11.1 Metrics 监控模块

Metrics 模块记录系统运行指标，支持 Prometheus 导出。

**指标类型:**
- 扫描指标：扫描区块数、交易数、耗时
- 节点指标：响应时间、健康状态
- 消费者指标：处理数、延迟
- 回滚指标：回滚次数、深度

**实现代码**: [metrics.go](pkg/metrics/metrics.go)

## 12. 钱包服务

### 12.1 WalletService 钱包服务

WalletService 提供余额查询、交易查询、转账等钱包相关功能。

**核心方法:**

| 方法 | 功能 | 代码位置 |
|------|------|----------|
| `GetBalance()` | 查询余额 | [service.go#L49](./internal/wallet/service.go#L49) |
| `GetBalances()` | 查询多代币余额 | [service.go#L154](./internal/wallet/service.go#L154) |
| `GetTransactions()` | 查询交易 | [service.go#L208](./internal/wallet/service.go#L208) |
| `Transfer()` | 转账 | [service.go#L286](./internal/wallet/service.go#L286) |
| `Deposit()` | 存款 | [service.go#L388](./internal/wallet/service.go#L388) |
| `Withdraw()` | 提现 | [service.go#L401](./internal/wallet/service.go#L401) |

**实现代码**: [service.go](internal/wallet/service.go)

### 12.2 Handler 钱包 HTTP 处理器

Handler 处理钱包相关的 REST API 请求，注册路由并处理请求响应。

**API 路由:**

| 路由 | 方法 | 功能 | 代码位置 |
|------|------|------|----------|
| `/api/v1/wallet/balance` | POST | 查询余额 | [handler.go#L25](./internal/wallet/handler.go#L25) |
| `/api/v1/wallet/balances` | POST | 查询多代币余额 | [handler.go#L58](./internal/wallet/handler.go#L58) |
| `/api/v1/wallet/transactions` | POST | 查询交易 | [handler.go#L91](./internal/wallet/handler.go#L91) |
| `/api/v1/wallet/transfer` | POST | 转账 | [handler.go#L139](./internal/wallet/handler.go#L139) |
| `/api/v1/wallet/deposit` | POST | 存款 | [handler.go#L185](./internal/wallet/handler.go#L185) |
| `/api/v1/wallet/withdraw` | POST | 提现 | [handler.go#L215](./internal/wallet/handler.go#L215) |

**实现代码**: [handler.go](internal/wallet/handler.go)

## 13. 工具模块

### 13.1 util 工具函数包

util 包提供通用的工具函数，包括大整数处理、字符串操作、时间处理等。

**核心函数:**

| 函数 | 功能 | 代码位置 |
|------|------|----------|
| `GenerateID()` | 生成唯一ID | [util.go#L18](./pkg/util/util.go#L18) |
| `ParseBigInt()` | 解析大整数 | [util.go#L22](./pkg/util/util.go#L22) |
| `FormatBalance()` | 格式化余额 | [util.go#L299](./pkg/util/util.go#L299) |
| `ParseBlockNumber()` | 解析区块号 | [util.go#L127](./pkg/util/util.go#L127) |
| `Retry()` | 带重试的函数执行 | [util.go#L73](./pkg/util/util.go#L73) |

**实现代码**: [util.go](pkg/util/util.go)

### 13.2 logger 日志包

logger 包基于 Zap 实现的高性能日志系统，支持 JSON 格式输出和日志轮转。

**核心方法:**

| 方法 | 功能 | 代码位置 |
|------|------|----------|
| `InitLogger()` | 初始化日志 | [logger.go#L24](./pkg/logger/logger.go#L24) |
| `Debug()` | 记录 Debug 日志 | [logger.go#L108](./pkg/logger/logger.go#L108) |
| `Info()` | 记录 Info 日志 | [logger.go#L115](./pkg/logger/logger.go#L115) |
| `Error()` | 记录 Error 日志 | [logger.go#L129](./pkg/logger/logger.go#L129) |

**实现代码**: [logger.go](pkg/logger/logger.go)

## 14. 部署架构

### 14.1 Docker 部署

系统支持 Docker 容器化部署，配置文件位于 `docker-compose.yml`。

**服务组件:**
- scanner: 链扫描服务
- mock-node: 模拟节点服务（开发测试用）

### 12.2 高可用设计

| 组件 | 高可用策略 |
|------|----------|
| 链节点 | 多节点故障转移 |
| 扫描器 | 分布式锁保证单点处理 |
| 数据库 | 主从复制 |
| 消息队列 | Kafka/RocketMQ 集群 |

## 13. API 接口

详见 [API.md](./API.md)