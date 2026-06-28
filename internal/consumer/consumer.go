package consumer

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/smart-scanner/multi-chain-scanner/internal/storage"
	"github.com/smart-scanner/multi-chain-scanner/pkg/logger"
	"github.com/smart-scanner/multi-chain-scanner/pkg/metrics"
	"github.com/smart-scanner/multi-chain-scanner/pkg/types"
	"go.uber.org/zap"
)

// Consumer 业务消费者接口
// [Design: BusinessConsumer 业务消费者](../docs/DESIGN_SCANNER.md#61-businessconsumer-业务消费者)
type Consumer interface {
	Start(ctx context.Context) error
	Stop() error
	ProcessTransaction(ctx context.Context, tx *types.Transaction) error
	ProcessLog(ctx context.Context, log *types.Log) error
	ProcessReorgEvent(ctx context.Context, event *types.ReorgEvent) error
	HealthCheck(ctx context.Context) error
}

// BusinessConsumer 业务消费者实现
// 负责处理扫描到的交易和日志，支持幂等性保障和回滚处理
// [Design: BusinessConsumer 业务消费者](../docs/DESIGN_SCANNER.md#61-businessconsumer-业务消费者)
type BusinessConsumer struct {
	config        *ConsumerConfig         // 消费者配置
	storage       storage.Storage         // 存储层
	idempotency   IdempotencyHandler      // 幂等性处理器
	stateMachine  StateMachine            // 状态机
	reorgHandler  ReorgHandler            // 回滚处理器
	webhook       WebhookHandler          // Webhook 处理器
	
	running       bool                    // 是否运行中
	mu            sync.RWMutex            // 状态锁
	wg            sync.WaitGroup          // 等待组
}

// ConsumerConfig 消费者配置
type ConsumerConfig struct {
	// 消费者组ID
	ConsumerGroupID string
	
	// 消费者数量
	ConsumerCount int
	
	// 批处理大小
	BatchSize int
	
	// 批处理超时时间
	BatchTimeout time.Duration
	
	// 重试配置
	MaxRetries      int
	RetryInterval   time.Duration
	
	// 幂等性配置
	IdempotencyTTL time.Duration
	
	// Webhook 配置
	WebhookURL      string
	WebhookTimeout  time.Duration
	WebhookRetry    int
	
	// 监控配置
	MetricsEnabled bool
}

// NewBusinessConsumer 创建业务消费者
func NewBusinessConsumer(config *ConsumerConfig, storage storage.Storage) (*BusinessConsumer, error) {
	if config == nil {
		return nil, fmt.Errorf("config cannot be nil")
	}
	if storage == nil {
		return nil, fmt.Errorf("storage cannot be nil")
	}

	// 创建幂等性处理器
	idempotency, err := NewRedisIdempotencyHandler(storage, config.IdempotencyTTL)
	if err != nil {
		return nil, fmt.Errorf("failed to create idempotency handler: %w", err)
	}

	// 创建状态机
	stateMachine := NewDefaultStateMachine(storage)

	// 创建回滚处理器
	reorgHandler := NewDefaultReorgHandler(storage, stateMachine)

	// 创建 Webhook 处理器
	webhook := NewHTTPWebhookHandler(config.WebhookURL, config.WebhookTimeout, config.WebhookRetry)

	return &BusinessConsumer{
		config:       config,
		storage:      storage,
		idempotency:  idempotency,
		stateMachine: stateMachine,
		reorgHandler: reorgHandler,
		webhook:      webhook,
	}, nil
}

// Start 启动消费者
func (c *BusinessConsumer) Start(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.running {
		return fmt.Errorf("consumer is already running")
	}

	logger.Info("Starting business consumer",
		zap.String("consumer_group", c.config.ConsumerGroupID),
		zap.Int("consumer_count", c.config.ConsumerCount))

	c.running = true

	// 启动多个消费者协程
	for i := 0; i < c.config.ConsumerCount; i++ {
		c.wg.Add(1)
		go c.consumerWorker(ctx, i)
	}

	// 启动监控协程
	if c.config.MetricsEnabled {
		c.wg.Add(1)
		go c.metricsWorker(ctx)
	}

	logger.Info("Business consumer started successfully")
	return nil
}

// Stop 停止消费者
func (c *BusinessConsumer) Stop() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if !c.running {
		return nil
	}

	logger.Info("Stopping business consumer")

	c.running = false
	c.wg.Wait()

	logger.Info("Business consumer stopped successfully")
	return nil
}

// consumerWorker 消费者工作协程
func (c *BusinessConsumer) consumerWorker(ctx context.Context, workerID int) {
	defer c.wg.Done()

	logger.Info("Consumer worker started",
		zap.Int("worker_id", workerID),
		zap.String("consumer_group", c.config.ConsumerGroupID))

	for {
		select {
		case <-ctx.Done():
			logger.Info("Consumer worker stopped",
				zap.Int("worker_id", workerID))
			return
		default:
			// 这里应该从消息队列消费消息
			// 暂时使用模拟数据
			time.Sleep(1 * time.Second)
		}
	}
}

// metricsWorker 监控工作协程
func (c *BusinessConsumer) metricsWorker(ctx context.Context) {
	defer c.wg.Done()

	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// 收集和上报监控指标
			c.collectMetrics(ctx)
		}
	}
}

// collectMetrics 收集监控指标
func (c *BusinessConsumer) collectMetrics(ctx context.Context) {
	// 更新监控指标
	metrics.RecordConsumerStatus(c.config.ConsumerGroupID, "running")
	metrics.RecordConsumerLag(c.config.ConsumerGroupID, 0)
}

// ProcessTransaction 处理交易
func (c *BusinessConsumer) ProcessTransaction(ctx context.Context, tx *types.Transaction) error {
	startTime := time.Now()
	
	logger.Debug("Processing transaction",
		zap.String("chain_id", tx.ChainID),
		zap.String("tx_hash", tx.Hash),
		zap.Uint64("block_number", tx.BlockNumber))

	// 幂等性检查
	key := fmt.Sprintf("tx:%s:%s", tx.ChainID, tx.Hash)
	processed, err := c.idempotency.IsProcessed(ctx, key)
	if err != nil {
		logger.Error("Failed to check idempotency",
			zap.String("key", key),
			zap.Error(err))
		return fmt.Errorf("failed to check idempotency: %w", err)
	}

	if processed {
		logger.Debug("Transaction already processed, skipping",
			zap.String("tx_hash", tx.Hash))
		return nil
	}

	// 状态机处理
	if err := c.stateMachine.ProcessTransaction(ctx, tx); err != nil {
		logger.Error("Failed to process transaction in state machine",
			zap.String("tx_hash", tx.Hash),
			zap.Error(err))
		return fmt.Errorf("failed to process transaction: %w", err)
	}

	// 标记为已处理
	if err := c.idempotency.MarkProcessed(ctx, key); err != nil {
		logger.Error("Failed to mark transaction as processed",
			zap.String("tx_hash", tx.Hash),
			zap.Error(err))
		return fmt.Errorf("failed to mark as processed: %w", err)
	}

	// 发送 Webhook
	if err := c.webhook.SendTransaction(ctx, tx); err != nil {
		logger.Error("Failed to send webhook",
			zap.String("tx_hash", tx.Hash),
			zap.Error(err))
		// Webhook 失败不影响主流程
	}

	// 记录指标
	duration := time.Since(startTime)
	metrics.RecordTransactionProcessing(tx.ChainID)
	metrics.RecordScanDuration(tx.ChainID, duration.Seconds())

	logger.Info("Transaction processed successfully",
		zap.String("tx_hash", tx.Hash),
		zap.Duration("duration", duration))

	return nil
}

// ProcessLog 处理日志
func (c *BusinessConsumer) ProcessLog(ctx context.Context, log *types.Log) error {
	startTime := time.Now()
	
	logger.Debug("Processing log",
		zap.String("chain_id", log.ChainID),
		zap.String("tx_hash", log.TxHash),
		zap.Uint64("block_number", log.BlockNumber),
		zap.Uint64("log_index", log.LogIndex))

	// 幂等性检查
	key := fmt.Sprintf("log:%s:%s:%d", log.ChainID, log.TxHash, log.LogIndex)
	processed, err := c.idempotency.IsProcessed(ctx, key)
	if err != nil {
		logger.Error("Failed to check idempotency",
			zap.String("key", key),
			zap.Error(err))
		return fmt.Errorf("failed to check idempotency: %w", err)
	}

	if processed {
		logger.Debug("Log already processed, skipping",
			zap.String("tx_hash", log.TxHash),
			zap.Uint64("log_index", log.LogIndex))
		return nil
	}

	// 处理日志事件
	if err := c.processLogEvent(ctx, log); err != nil {
		logger.Error("Failed to process log event",
			zap.String("tx_hash", log.TxHash),
			zap.Uint64("log_index", log.LogIndex),
			zap.Error(err))
		return fmt.Errorf("failed to process log event: %w", err)
	}

	// 标记为已处理
	if err := c.idempotency.MarkProcessed(ctx, key); err != nil {
		logger.Error("Failed to mark log as processed",
			zap.String("tx_hash", log.TxHash),
			zap.Uint64("log_index", log.LogIndex),
			zap.Error(err))
		return fmt.Errorf("failed to mark as processed: %w", err)
	}

	// 发送 Webhook
	if err := c.webhook.SendLog(ctx, log); err != nil {
		logger.Error("Failed to send webhook",
			zap.String("tx_hash", log.TxHash),
			zap.Uint64("log_index", log.LogIndex),
			zap.Error(err))
		// Webhook 失败不影响主流程
	}

	// 记录指标
	duration := time.Since(startTime)
	metrics.RecordLogProcessing(log.ChainID)
	metrics.RecordScanDuration(log.ChainID, duration.Seconds())

	logger.Info("Log processed successfully",
		zap.String("tx_hash", log.TxHash),
		zap.Uint64("log_index", log.LogIndex),
		zap.Duration("duration", duration))

	return nil
}

// processLogEvent 处理日志事件
func (c *BusinessConsumer) processLogEvent(ctx context.Context, log *types.Log) error {
	// 根据日志内容处理不同的业务逻辑
	// 这里可以添加自定义的业务逻辑处理
	
	// 示例：处理 Transfer 事件
	if len(log.Topics) >= 3 {
		// 假设第一个 topic 是事件签名
		eventSignature := log.Topics[0]
		
		// 根据事件签名处理不同的业务逻辑
		switch eventSignature {
		case "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef":
			// ERC20 Transfer 事件
			return c.processTransferEvent(ctx, log)
		// 可以添加更多事件类型的处理
		default:
			logger.Debug("Unknown event signature",
				zap.String("signature", eventSignature))
		}
	}

	return nil
}

// processTransferEvent 处理 Transfer 事件
func (c *BusinessConsumer) processTransferEvent(ctx context.Context, log *types.Log) error {
	// 这里可以添加具体的 Transfer 事件处理逻辑
	// 例如：更新余额、记录流水等
	
	logger.Debug("Processing Transfer event",
		zap.String("address", log.Address),
		zap.String("tx_hash", log.TxHash))
	
	return nil
}

// ProcessReorgEvent 处理回滚事件
func (c *BusinessConsumer) ProcessReorgEvent(ctx context.Context, event *types.ReorgEvent) error {
	startTime := time.Now()
	
	logger.Warn("Processing reorg event",
		zap.String("chain_id", event.ChainID),
		zap.Uint64("old_block", event.OldBlockNumber),
		zap.Uint64("new_block", event.NewBlockNumber),
		zap.Uint64("depth", event.Depth))

	// 使用回滚处理器处理回滚事件
	if err := c.reorgHandler.HandleReorg(ctx, event); err != nil {
		logger.Error("Failed to handle reorg event",
			zap.String("chain_id", event.ChainID),
			zap.Error(err))
		return fmt.Errorf("failed to handle reorg: %w", err)
	}

	// 发送 Webhook 通知
	if err := c.webhook.SendReorgEvent(ctx, event); err != nil {
		logger.Error("Failed to send reorg webhook",
			zap.String("chain_id", event.ChainID),
			zap.Error(err))
		// Webhook 失败不影响主流程
	}

	// 记录指标
	duration := time.Since(startTime)
	metrics.RecordReorgProcessing(event.ChainID)
	metrics.RecordScanDuration(event.ChainID, duration.Seconds())

	logger.Warn("Reorg event processed successfully",
		zap.String("chain_id", event.ChainID),
		zap.Duration("duration", duration))

	return nil
}

// HealthCheck 健康检查
func (c *BusinessConsumer) HealthCheck(ctx context.Context) error {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if !c.running {
		return fmt.Errorf("consumer is not running")
	}

	// 检查存储连接
	if err := c.storage.HealthCheck(ctx); err != nil {
		return fmt.Errorf("storage health check failed: %w", err)
	}

	return nil
}

// GetStatus 获取消费者状态
func (c *BusinessConsumer) GetStatus() *ConsumerStatus {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return &ConsumerStatus{
		Running:       c.running,
		ConsumerGroup: c.config.ConsumerGroupID,
		ConsumerCount: c.config.ConsumerCount,
		StartTime:     time.Now(), // 这里应该记录实际启动时间
	}
}

// ConsumerStatus 消费者状态
type ConsumerStatus struct {
	Running       bool      `json:"running"`
	ConsumerGroup string    `json:"consumer_group"`
	ConsumerCount int       `json:"consumer_count"`
	StartTime     time.Time `json:"start_time"`
}