package queue

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/smart-scanner/multi-chain-scanner/pkg/logger"
	"github.com/smart-scanner/multi-chain-scanner/pkg/metrics"
	"github.com/smart-scanner/multi-chain-scanner/pkg/types"
	"go.uber.org/zap"
)

// kafkaErrorLogger Kafka 错误日志包装器
func kafkaErrorLogger(msg string, args ...interface{}) {
	logger.Error(msg, zap.Any("args", args))
}

// KafkaProducer Kafka 生产者
// [Design: 消息队列层](../docs/DESIGN_SCANNER.md#1-系统概述)
type KafkaProducer struct {
	writer *kafka.Writer
	config *KafkaConfig
}

// NewKafkaProducer 创建 Kafka 生产者
func NewKafkaProducer(config *KafkaConfig) (*KafkaProducer, error) {
	if config == nil || len(config.Brokers) == 0 {
		return nil, fmt.Errorf("Kafka brokers cannot be empty")
	}

	writer := &kafka.Writer{
		Addr:         kafka.TCP(config.Brokers...),
		Balancer:     &kafka.LeastBytes{},
		BatchSize:    100,
		BatchTimeout: 10 * time.Millisecond,
		Compression:  kafka.Snappy,
		ErrorLogger:  kafka.LoggerFunc(kafkaErrorLogger),
	}

	return &KafkaProducer{
		writer: writer,
		config: config,
	}, nil
}

// SendMessage 发送消息
func (p *KafkaProducer) SendMessage(ctx context.Context, topic string, key string, value interface{}) error {
	startTime := time.Now()
	defer func() {
		duration := time.Since(startTime).Seconds()
		metrics.RecordMessageQueueOperationDuration("send", topic, duration)
	}()

	// 序列化消息
	data, err := json.Marshal(value)
	if err != nil {
		metrics.RecordMessageQueueOperation("send", topic, "error")
		return fmt.Errorf("failed to marshal message: %w", err)
	}

	// 创建 Kafka 消息
	message := kafka.Message{
		Topic: topic,
		Key:   []byte(key),
		Value: data,
		Time:  time.Now(),
	}

	// 发送消息
	if err := p.writer.WriteMessages(ctx, message); err != nil {
		metrics.RecordMessageQueueOperation("send", topic, "error")
		return fmt.Errorf("failed to send message: %w", err)
	}

	metrics.RecordMessageQueueOperation("send", topic, "success")
	logger.Debug("Message sent to Kafka",
		zap.String("topic", topic),
		zap.String("key", key))

	return nil
}

// BatchSendMessages 批量发送消息
func (p *KafkaProducer) BatchSendMessages(ctx context.Context, topic string, messages []types.MessageQueueMessage) error {
	if len(messages) == 0 {
		return nil
	}

	startTime := time.Now()
	defer func() {
		duration := time.Since(startTime).Seconds()
		metrics.RecordMessageQueueOperationDuration("batch_send", topic, duration)
	}()

	// 转换为 Kafka 消息
	kafkaMessages := make([]kafka.Message, 0, len(messages))
	for _, msg := range messages {
		data, err := json.Marshal(msg.Value)
		if err != nil {
			logger.Error("Failed to marshal message in batch",
				zap.String("topic", topic),
				zap.Error(err))
			continue
		}

		kafkaMessages = append(kafkaMessages, kafka.Message{
			Topic: topic,
			Key:   []byte(msg.Key),
			Value: data,
			Time:  msg.Timestamp,
		})
	}

	// 批量发送消息
	if err := p.writer.WriteMessages(ctx, kafkaMessages...); err != nil {
		metrics.RecordMessageQueueOperation("batch_send", topic, "error")
		return fmt.Errorf("failed to batch send messages: %w", err)
	}

	metrics.RecordMessageQueueOperation("batch_send", topic, "success")
	logger.Debug("Batch messages sent to Kafka",
		zap.String("topic", topic),
		zap.Int("count", len(kafkaMessages)))

	return nil
}

// SendRawBlock 发送原始区块
func (p *KafkaProducer) SendRawBlock(ctx context.Context, block *types.Block) error {
	key := GenerateBlockMessageKey(block.ChainID, block.Number)
	return p.SendMessage(ctx, p.config.Topics.RawBlock, key, block)
}

// SendNormalizedTransaction 发送标准交易
func (p *KafkaProducer) SendNormalizedTransaction(ctx context.Context, tx *types.Transaction) error {
	key := GenerateMessageKey(tx.ChainID, tx.Hash, 0)
	return p.SendMessage(ctx, p.config.Topics.NormalizedTx, key, tx)
}

// SendReorgEvent 发送回滚事件
func (p *KafkaProducer) SendReorgEvent(ctx context.Context, event *types.ReorgEvent) error {
	key := GenerateReorgEventKey(event.ChainID, event.DetectedAt)
	return p.SendMessage(ctx, p.config.Topics.ReorgEvent, key, event)
}

// SendToDLQ 发送到死信队列
func (p *KafkaProducer) SendToDLQ(ctx context.Context, message interface{}, reason string) error {
	dlqMessage := map[string]interface{}{
		"message": message,
		"reason":  reason,
		"time":    time.Now(),
	}
	
	key := fmt.Sprintf("dlq-%d", time.Now().UnixNano())
	return p.SendMessage(ctx, p.config.Topics.DLQ, key, dlqMessage)
}

// Close 关闭生产者
func (p *KafkaProducer) Close() error {
	if p.writer != nil {
		return p.writer.Close()
	}
	return nil
}

// KafkaConsumer Kafka 消费者
type KafkaConsumer struct {
	reader *kafka.Reader
	config *KafkaConfig
}

// NewKafkaConsumer 创建 Kafka 消费者
func NewKafkaConsumer(config *KafkaConfig) (*KafkaConsumer, error) {
	if config == nil || len(config.Brokers) == 0 {
		return nil, fmt.Errorf("Kafka brokers cannot be empty")
	}

	if config.ClientID == "" {
		config.ClientID = "multi-chain-scanner"
	}

	return &KafkaConsumer{
		config: config,
	}, nil
}

// createReader 创建 Kafka 读取器
func (c *KafkaConsumer) createReader(topic string, groupID string) *kafka.Reader {
	return kafka.NewReader(kafka.ReaderConfig{
		Brokers:     c.config.Brokers,
		GroupID:     groupID,
		Topic:       topic,
		MinBytes:    10e3,  // 10KB
		MaxBytes:    10e6,  // 10MB
		StartOffset: kafka.LastOffset,
	})
}

// Subscribe 订阅主题
func (c *KafkaConsumer) Subscribe(ctx context.Context, topic string, groupID string, handler MessageHandler) error {
	reader := c.createReader(topic, groupID)
	defer reader.Close()

	logger.Info("Starting to consume messages from Kafka",
		zap.String("topic", topic),
		zap.String("group_id", groupID))

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			message, err := reader.ReadMessage(ctx)
			if err != nil {
				logger.Error("Failed to read message from Kafka",
					zap.String("topic", topic),
					zap.Error(err))
				continue
			}

			// 解析消息
			var msgValue interface{}
			if err := json.Unmarshal(message.Value, &msgValue); err != nil {
				logger.Error("Failed to unmarshal message",
					zap.String("topic", topic),
					zap.Error(err))
				continue
			}

			// 创建消息队列消息
			queueMessage := &types.MessageQueueMessage{
				Topic:     topic,
				Key:       string(message.Key),
				Value:     msgValue,
				Timestamp: message.Time,
			}

			// 处理消息
			if err := handler(ctx, queueMessage); err != nil {
				logger.Error("Failed to handle message",
					zap.String("topic", topic),
					zap.String("key", string(message.Key)),
					zap.Error(err))
				
				// 发送到死信队列
				// TODO: 实现死信队列逻辑
				continue
			}

			// 记录指标
			metrics.RecordMessageQueueOperation("consume", topic, "success")
		}
	}
}

// SubscribeRawBlocks 订阅原始区块
func (c *KafkaConsumer) SubscribeRawBlocks(ctx context.Context, groupID string, handler BlockHandler) error {
	reader := c.createReader(c.config.Topics.RawBlock, groupID)
	defer reader.Close()

	logger.Info("Starting to consume raw blocks from Kafka",
		zap.String("topic", c.config.Topics.RawBlock),
		zap.String("group_id", groupID))

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			message, err := reader.ReadMessage(ctx)
			if err != nil {
				logger.Error("Failed to read raw block from Kafka",
					zap.Error(err))
				continue
			}

			// 解析区块
			var block types.Block
			if err := json.Unmarshal(message.Value, &block); err != nil {
				logger.Error("Failed to unmarshal raw block",
					zap.Error(err))
				continue
			}

			// 处理区块
			if err := handler(ctx, &block); err != nil {
				logger.Error("Failed to handle raw block",
					zap.String("chain_id", block.ChainID),
					zap.Uint64("block_number", block.Number),
					zap.Error(err))
				continue
			}

			// 记录指标
			metrics.RecordMessageQueueOperation("consume", c.config.Topics.RawBlock, "success")
		}
	}
}

// SubscribeNormalizedTransactions 订阅标准交易
func (c *KafkaConsumer) SubscribeNormalizedTransactions(ctx context.Context, groupID string, handler TransactionHandler) error {
	reader := c.createReader(c.config.Topics.NormalizedTx, groupID)
	defer reader.Close()

	logger.Info("Starting to consume normalized transactions from Kafka",
		zap.String("topic", c.config.Topics.NormalizedTx),
		zap.String("group_id", groupID))

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			message, err := reader.ReadMessage(ctx)
			if err != nil {
				logger.Error("Failed to read normalized transaction from Kafka",
					zap.Error(err))
				continue
			}

			// 解析交易
			var tx types.Transaction
			if err := json.Unmarshal(message.Value, &tx); err != nil {
				logger.Error("Failed to unmarshal normalized transaction",
					zap.Error(err))
				continue
			}

			// 处理交易
			if err := handler(ctx, &tx); err != nil {
				logger.Error("Failed to handle normalized transaction",
					zap.String("chain_id", tx.ChainID),
					zap.String("tx_hash", tx.Hash),
					zap.Error(err))
				continue
			}

			// 记录指标
			metrics.RecordMessageQueueOperation("consume", c.config.Topics.NormalizedTx, "success")
		}
	}
}

// SubscribeReorgEvents 订阅回滚事件
func (c *KafkaConsumer) SubscribeReorgEvents(ctx context.Context, groupID string, handler ReorgEventHandler) error {
	reader := c.createReader(c.config.Topics.ReorgEvent, groupID)
	defer reader.Close()

	logger.Info("Starting to consume reorg events from Kafka",
		zap.String("topic", c.config.Topics.ReorgEvent),
		zap.String("group_id", groupID))

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			message, err := reader.ReadMessage(ctx)
			if err != nil {
				logger.Error("Failed to read reorg event from Kafka",
					zap.Error(err))
				continue
			}

			// 解析回滚事件
			var event types.ReorgEvent
			if err := json.Unmarshal(message.Value, &event); err != nil {
				logger.Error("Failed to unmarshal reorg event",
					zap.Error(err))
				continue
			}

			// 处理回滚事件
			if err := handler(ctx, &event); err != nil {
				logger.Error("Failed to handle reorg event",
					zap.String("chain_id", event.ChainID),
					zap.Uint64("old_block_number", event.OldBlockNumber),
					zap.Error(err))
				continue
			}

			// 记录指标
			metrics.RecordMessageQueueOperation("consume", c.config.Topics.ReorgEvent, "success")
		}
	}
}

// SubscribeDLQ 订阅死信队列
func (c *KafkaConsumer) SubscribeDLQ(ctx context.Context, groupID string, handler DLQHandler) error {
	reader := c.createReader(c.config.Topics.DLQ, groupID)
	defer reader.Close()

	logger.Info("Starting to consume DLQ messages from Kafka",
		zap.String("topic", c.config.Topics.DLQ),
		zap.String("group_id", groupID))

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			message, err := reader.ReadMessage(ctx)
			if err != nil {
				logger.Error("Failed to read DLQ message from Kafka",
					zap.Error(err))
				continue
			}

			// 解析死信队列消息
			var dlqMessage map[string]interface{}
			if err := json.Unmarshal(message.Value, &dlqMessage); err != nil {
				logger.Error("Failed to unmarshal DLQ message",
					zap.Error(err))
				continue
			}

			// 提取原因和原始消息
			reason, _ := dlqMessage["reason"].(string)
			originalMessage := dlqMessage["message"]

			// 处理死信队列消息
			if err := handler(ctx, originalMessage, reason); err != nil {
				logger.Error("Failed to handle DLQ message",
					zap.String("reason", reason),
					zap.Error(err))
				continue
			}

			// 记录指标
			metrics.RecordMessageQueueOperation("consume", c.config.Topics.DLQ, "success")
		}
	}
}

// Commit 提交消息偏移量
func (c *KafkaConsumer) Commit(ctx context.Context) error {
	// Kafka Reader 会自动提交偏移量
	return nil
}

// Close 关闭消费者
func (c *KafkaConsumer) Close() error {
	// Reader 在每个订阅方法中单独创建和关闭
	return nil
}