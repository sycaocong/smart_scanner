package queue

import (
	"context"
	"fmt"
	"time"

	"github.com/smart-scanner/multi-chain-scanner/pkg/types"
)

// MessageProducer 消息生产者接口
type MessageProducer interface {
	// 发送消息
	SendMessage(ctx context.Context, topic string, key string, value interface{}) error
	
	// 批量发送消息
	BatchSendMessages(ctx context.Context, topic string, messages []types.MessageQueueMessage) error
	
	// 发送原始区块
	SendRawBlock(ctx context.Context, block *types.Block) error
	
	// 发送标准交易
	SendNormalizedTransaction(ctx context.Context, tx *types.Transaction) error
	
	// 发送回滚事件
	SendReorgEvent(ctx context.Context, event *types.ReorgEvent) error
	
	// 发送到死信队列
	SendToDLQ(ctx context.Context, message interface{}, reason string) error
	
	// 关闭生产者
	Close() error
}

// MessageConsumer 消息消费者接口
type MessageConsumer interface {
	// 订阅主题
	Subscribe(ctx context.Context, topic string, groupID string, handler MessageHandler) error
	
	// 订阅原始区块
	SubscribeRawBlocks(ctx context.Context, groupID string, handler BlockHandler) error
	
	// 订阅标准交易
	SubscribeNormalizedTransactions(ctx context.Context, groupID string, handler TransactionHandler) error
	
	// 订阅回滚事件
	SubscribeReorgEvents(ctx context.Context, groupID string, handler ReorgEventHandler) error
	
	// 订阅死信队列
	SubscribeDLQ(ctx context.Context, groupID string, handler DLQHandler) error
	
	// 提交消息偏移量
	Commit(ctx context.Context) error
	
	// 关闭消费者
	Close() error
}

// MessageHandler 消息处理器
type MessageHandler func(ctx context.Context, message *types.MessageQueueMessage) error

// BlockHandler 区块处理器
type BlockHandler func(ctx context.Context, block *types.Block) error

// TransactionHandler 交易处理器
type TransactionHandler func(ctx context.Context, tx *types.Transaction) error

// ReorgEventHandler 回滚事件处理器
type ReorgEventHandler func(ctx context.Context, event *types.ReorgEvent) error

// DLQHandler 死信队列处理器
type DLQHandler func(ctx context.Context, message interface{}, reason string) error

// QueueManager 队列管理器
// [Design: 消息队列层](../docs/DESIGN_SCANNER.md#1-系统概述)
type QueueManager struct {
	producer MessageProducer
	consumer MessageConsumer
	config   *QueueConfig
}

// QueueConfig 队列配置
type QueueConfig struct {
	// Kafka 配置
	Kafka *KafkaConfig
	
	// RocketMQ 配置
	RocketMQ *RocketMQConfig
	
	// 通用配置
	BatchSize    int
	BatchTimeout time.Duration
	MaxRetries   int
	RetryDelay   time.Duration
}

// KafkaConfig Kafka 配置
type KafkaConfig struct {
	Brokers  []string
	ClientID string
	Topics   struct {
		RawBlock       string
		NormalizedTx   string
		ReorgEvent     string
		DLQ            string
	}
}

// RocketMQConfig RocketMQ 配置
type RocketMQConfig struct {
	NameServers []string
	GroupID     string
	Topics      struct {
		RawBlock       string
		NormalizedTx   string
		ReorgEvent     string
		DLQ            string
	}
}

// NewQueueManager 创建队列管理器
func NewQueueManager(config *QueueConfig) (*QueueManager, error) {
	if config == nil {
		return nil, fmt.Errorf("config cannot be nil")
	}

	manager := &QueueManager{
		config: config,
	}

	// 创建生产者
	if config.Kafka != nil {
		producer, err := NewKafkaProducer(config.Kafka)
		if err != nil {
			return nil, fmt.Errorf("failed to create Kafka producer: %w", err)
		}
		manager.producer = producer
	} else if config.RocketMQ != nil {
		producer, err := NewRocketMQProducer(config.RocketMQ)
		if err != nil {
			return nil, fmt.Errorf("failed to create RocketMQ producer: %w", err)
		}
		manager.producer = producer
	} else {
		return nil, fmt.Errorf("either Kafka or RocketMQ config must be provided")
	}

	// 创建消费者
	if config.Kafka != nil {
		consumer, err := NewKafkaConsumer(config.Kafka)
		if err != nil {
			return nil, fmt.Errorf("failed to create Kafka consumer: %w", err)
		}
		manager.consumer = consumer
	} else if config.RocketMQ != nil {
		consumer, err := NewRocketMQConsumer(config.RocketMQ)
		if err != nil {
			return nil, fmt.Errorf("failed to create RocketMQ consumer: %w", err)
		}
		manager.consumer = consumer
	}

	return manager, nil
}

// GetProducer 获取生产者
func (m *QueueManager) GetProducer() MessageProducer {
	return m.producer
}

// GetConsumer 获取消费者
func (m *QueueManager) GetConsumer() MessageConsumer {
	return m.consumer
}

// Close 关闭队列管理器
func (m *QueueManager) Close() error {
	var lastErr error

	if m.producer != nil {
		if err := m.producer.Close(); err != nil {
			lastErr = err
		}
	}

	if m.consumer != nil {
		if err := m.consumer.Close(); err != nil {
			lastErr = err
		}
	}

	return lastErr
}

// GenerateMessageKey 生成消息键（用于幂等性）
func GenerateMessageKey(chainID string, txHash string, logIndex uint64) string {
	return fmt.Sprintf("%s-%s-%d", chainID, txHash, logIndex)
}

// GenerateBlockMessageKey 生成区块消息键
func GenerateBlockMessageKey(chainID string, blockNumber uint64) string {
	return fmt.Sprintf("%s-%d", chainID, blockNumber)
}

// GenerateReorgEventKey 生成回滚事件键
func GenerateReorgEventKey(chainID string, timestamp time.Time) string {
	return fmt.Sprintf("%s-%d", chainID, timestamp.Unix())
}