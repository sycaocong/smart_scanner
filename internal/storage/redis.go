package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/smart-scanner/multi-chain-scanner/pkg/logger"
	"github.com/smart-scanner/multi-chain-scanner/pkg/types"
	"go.uber.org/zap"
)

// RedisStorage Redis 存储实现
// 用于缓存热点数据（区块、交易、水位），提供高性能读写
// [Design: 存储层](../docs/DESIGN_SCANNER.md#1-系统概述)
type RedisStorage struct {
	client *redis.Client
	config *RedisConfig
}

// NewRedisStorage 创建 Redis 存储
func NewRedisStorage(config *RedisConfig) (*RedisStorage, error) {
	if config == nil {
		return nil, fmt.Errorf("config cannot be nil")
	}

	client := redis.NewClient(&redis.Options{
		Addr:         config.Addr,
		Password:     config.Password,
		DB:           config.DB,
		PoolSize:     config.PoolSize,
		MinIdleConns: config.MinIdleConns,
	})

	// 测试连接
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := client.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("failed to connect to Redis: %w", err)
	}

	return &RedisStorage{
		client: client,
		config: config,
	}, nil
}

// generateBlockKey 生成区块缓存键
func (s *RedisStorage) generateBlockKey(chainID string, blockNumber uint64) string {
	return fmt.Sprintf("block:%s:%d", chainID, blockNumber)
}

// generateTransactionKey 生成交易缓存键
func (s *RedisStorage) generateTransactionKey(chainID string, txHash string) string {
	return fmt.Sprintf("tx:%s:%s", chainID, txHash)
}

// generateWatermarkKey 生成水位缓存键
func (s *RedisStorage) generateWatermarkKey(chainID string) string {
	return fmt.Sprintf("watermark:%s", chainID)
}

// SaveBlock 保存区块
func (s *RedisStorage) SaveBlock(ctx context.Context, block *types.Block) error {
	key := s.generateBlockKey(block.ChainID, block.Number)

	// 序列化区块数据
	data, err := json.Marshal(block)
	if err != nil {
		return fmt.Errorf("failed to marshal block: %w", err)
	}

	// 保存到 Redis，设置过期时间为 24 小时
	if err := s.client.Set(ctx, key, data, 24*time.Hour).Err(); err != nil {
		return fmt.Errorf("failed to save block to Redis: %w", err)
	}

	return nil
}

// GetBlock 获取区块
func (s *RedisStorage) GetBlock(ctx context.Context, chainID string, blockNumber uint64) (*types.Block, error) {
	key := s.generateBlockKey(chainID, blockNumber)

	// 从 Redis 获取
	data, err := s.client.Get(ctx, key).Bytes()
	if err != nil {
		if err == redis.Nil {
			return nil, fmt.Errorf("block not found in cache")
		}
		return nil, fmt.Errorf("failed to get block from Redis: %w", err)
	}

	// 反序列化区块数据
	var block types.Block
	if err := json.Unmarshal(data, &block); err != nil {
		return nil, fmt.Errorf("failed to unmarshal block: %w", err)
	}

	return &block, nil
}

// GetBlockByHash 根据哈希获取区块
func (s *RedisStorage) GetBlockByHash(ctx context.Context, chainID string, blockHash string) (*types.Block, error) {
	// Redis 不支持按哈希查询，返回错误
	return nil, fmt.Errorf("get block by hash not supported in Redis")
}

// GetLatestBlock 获取最新区块
func (s *RedisStorage) GetLatestBlock(ctx context.Context, chainID string) (*types.Block, error) {
	// Redis 不支持查询最新区块，返回错误
	return nil, fmt.Errorf("get latest block not supported in Redis")
}

// DeleteBlocks 删除区块
func (s *RedisStorage) DeleteBlocks(ctx context.Context, chainID string, fromBlock, toBlock uint64) error {
	// Redis 不支持范围删除，需要逐个删除
	for blockNumber := fromBlock; blockNumber <= toBlock; blockNumber++ {
		key := s.generateBlockKey(chainID, blockNumber)
		if err := s.client.Del(ctx, key).Err(); err != nil {
			logger.Error("Failed to delete block from Redis",
				zap.String("chain_id", chainID),
				zap.Uint64("block_number", blockNumber),
				zap.Error(err))
		}
	}
	return nil
}

// SaveTransaction 保存交易
func (s *RedisStorage) SaveTransaction(ctx context.Context, tx *types.Transaction) error {
	key := s.generateTransactionKey(tx.ChainID, tx.Hash)

	// 序列化交易数据
	data, err := json.Marshal(tx)
	if err != nil {
		return fmt.Errorf("failed to marshal transaction: %w", err)
	}

	// 保存到 Redis，设置过期时间为 24 小时
	if err := s.client.Set(ctx, key, data, 24*time.Hour).Err(); err != nil {
		return fmt.Errorf("failed to save transaction to Redis: %w", err)
	}

	return nil
}

// BatchSaveTransactions 批量保存交易
func (s *RedisStorage) BatchSaveTransactions(ctx context.Context, txs []*types.Transaction) error {
	// 使用 Pipeline 批量保存
	pipe := s.client.Pipeline()

	for _, tx := range txs {
		key := s.generateTransactionKey(tx.ChainID, tx.Hash)
		data, err := json.Marshal(tx)
		if err != nil {
			logger.Error("Failed to marshal transaction",
				zap.String("chain_id", tx.ChainID),
				zap.String("tx_hash", tx.Hash),
				zap.Error(err))
			continue
		}

		pipe.Set(ctx, key, data, 24*time.Hour)
	}

	// 执行批量操作
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("failed to batch save transactions: %w", err)
	}

	return nil
}

// GetTransaction 获取交易
func (s *RedisStorage) GetTransaction(ctx context.Context, chainID string, txHash string) (*types.Transaction, error) {
	key := s.generateTransactionKey(chainID, txHash)

	// 从 Redis 获取
	data, err := s.client.Get(ctx, key).Bytes()
	if err != nil {
		if err == redis.Nil {
			return nil, fmt.Errorf("transaction not found in cache")
		}
		return nil, fmt.Errorf("failed to get transaction from Redis: %w", err)
	}

	// 反序列化交易数据
	var tx types.Transaction
	if err := json.Unmarshal(data, &tx); err != nil {
		return nil, fmt.Errorf("failed to unmarshal transaction: %w", err)
	}

	return &tx, nil
}

// GetTransactionsByBlock 根据区块获取交易
func (s *RedisStorage) GetTransactionsByBlock(ctx context.Context, chainID string, blockNumber uint64) ([]*types.Transaction, error) {
	// Redis 不支持按区块查询交易，返回错误
	return nil, fmt.Errorf("get transactions by block not supported in Redis")
}

// GetTransactionsByAddress 根据地址获取交易
func (s *RedisStorage) GetTransactionsByAddress(ctx context.Context, chainID string, address string, limit, offset int) ([]*types.Transaction, error) {
	// Redis 不支持按地址查询交易，返回错误
	return nil, fmt.Errorf("get transactions by address not supported in Redis")
}

// UpdateTransactionStatus 更新交易状态
func (s *RedisStorage) UpdateTransactionStatus(ctx context.Context, chainID string, txHash string, status types.TransactionStatus) error {
	key := s.generateTransactionKey(chainID, txHash)

	// 获取现有交易数据
	data, err := s.client.Get(ctx, key).Bytes()
	if err != nil {
		if err == redis.Nil {
			return fmt.Errorf("transaction not found in cache")
		}
		return fmt.Errorf("failed to get transaction from Redis: %w", err)
	}

	// 反序列化交易数据
	var tx types.Transaction
	if err := json.Unmarshal(data, &tx); err != nil {
		return fmt.Errorf("failed to unmarshal transaction: %w", err)
	}

	// 更新状态
	tx.Status = status

	// 重新序列化
	updatedData, err := json.Marshal(tx)
	if err != nil {
		return fmt.Errorf("failed to marshal transaction: %w", err)
	}

	// 保存更新后的交易
	if err := s.client.Set(ctx, key, updatedData, 24*time.Hour).Err(); err != nil {
		return fmt.Errorf("failed to update transaction in Redis: %w", err)
	}

	return nil
}

// MarkTransactionsAsReverted 标记交易为已回滚
func (s *RedisStorage) MarkTransactionsAsReverted(ctx context.Context, chainID string, fromBlock, toBlock uint64) error {
	// Redis 不支持范围更新，需要逐个更新
	// 由于 Redis 不支持按区块查询交易，这里无法实现
	// 实际应用中，可以在删除区块时同时删除相关的交易缓存
	return nil
}

// SaveLog 保存日志
func (s *RedisStorage) SaveLog(ctx context.Context, log *types.Log) error {
	// Redis 不保存日志，日志数据量太大
	return nil
}

// BatchSaveLogs 批量保存日志
func (s *RedisStorage) BatchSaveLogs(ctx context.Context, logs []*types.Log) error {
	// Redis 不保存日志
	return nil
}

// GetLogs 获取日志
func (s *RedisStorage) GetLogs(ctx context.Context, chainID string, filter *LogFilter) ([]*types.Log, error) {
	// Redis 不保存日志
	return nil, fmt.Errorf("get logs not supported in Redis")
}

// DeleteLogs 删除日志
func (s *RedisStorage) DeleteLogs(ctx context.Context, chainID string, fromBlock, toBlock uint64) error {
	// Redis 不保存日志
	return nil
}

// SaveWatermark 保存水位
func (s *RedisStorage) SaveWatermark(ctx context.Context, watermark *types.Watermark) error {
	key := s.generateWatermarkKey(watermark.ChainID)

	// 序列化水位数据
	data, err := json.Marshal(watermark)
	if err != nil {
		return fmt.Errorf("failed to marshal watermark: %w", err)
	}

	// 保存到 Redis，不设置过期时间
	if err := s.client.Set(ctx, key, data, 0).Err(); err != nil {
		return fmt.Errorf("failed to save watermark to Redis: %w", err)
	}

	return nil
}

// GetWatermark 获取水位
func (s *RedisStorage) GetWatermark(ctx context.Context, chainID string) (*types.Watermark, error) {
	key := s.generateWatermarkKey(chainID)

	// 从 Redis 获取
	data, err := s.client.Get(ctx, key).Bytes()
	if err != nil {
		if err == redis.Nil {
			return nil, fmt.Errorf("watermark not found in cache")
		}
		return nil, fmt.Errorf("failed to get watermark from Redis: %w", err)
	}

	// 反序列化水位数据
	var watermark types.Watermark
	if err := json.Unmarshal(data, &watermark); err != nil {
		return nil, fmt.Errorf("failed to unmarshal watermark: %w", err)
	}

	return &watermark, nil
}

// UpdateWatermark 更新水位
func (s *RedisStorage) UpdateWatermark(ctx context.Context, chainID string, watermark *types.Watermark) error {
	return s.SaveWatermark(ctx, watermark)
}

// SaveReorgEvent 保存回滚事件
func (s *RedisStorage) SaveReorgEvent(ctx context.Context, event *types.ReorgEvent) error {
	// Redis 不保存回滚事件
	return nil
}

// GetReorgEvents 获取回滚事件
func (s *RedisStorage) GetReorgEvents(ctx context.Context, chainID string, limit, offset int) ([]*types.ReorgEvent, error) {
	// Redis 不保存回滚事件
	return nil, fmt.Errorf("get reorg events not supported in Redis")
}

// HealthCheck 健康检查
func (s *RedisStorage) HealthCheck(ctx context.Context) error {
	return s.client.Ping(ctx).Err()
}

// Close 关闭连接
func (s *RedisStorage) Close() error {
	return s.client.Close()
}