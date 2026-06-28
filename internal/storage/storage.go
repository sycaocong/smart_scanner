package storage

import (
	"context"
	"fmt"
	"time"

	"github.com/smart-scanner/multi-chain-scanner/pkg/logger"
	"github.com/smart-scanner/multi-chain-scanner/pkg/metrics"
	"github.com/smart-scanner/multi-chain-scanner/pkg/types"
	"go.uber.org/zap"
)

// Storage 存储接口
type Storage interface {
	// 区块操作
	SaveBlock(ctx context.Context, block *types.Block) error
	GetBlock(ctx context.Context, chainID string, blockNumber uint64) (*types.Block, error)
	GetBlockByHash(ctx context.Context, chainID string, blockHash string) (*types.Block, error)
	GetLatestBlock(ctx context.Context, chainID string) (*types.Block, error)
	DeleteBlocks(ctx context.Context, chainID string, fromBlock, toBlock uint64) error
	
	// 交易操作
	SaveTransaction(ctx context.Context, tx *types.Transaction) error
	BatchSaveTransactions(ctx context.Context, txs []*types.Transaction) error
	GetTransaction(ctx context.Context, chainID string, txHash string) (*types.Transaction, error)
	GetTransactionsByBlock(ctx context.Context, chainID string, blockNumber uint64) ([]*types.Transaction, error)
	GetTransactionsByAddress(ctx context.Context, chainID string, address string, limit, offset int) ([]*types.Transaction, error)
	UpdateTransactionStatus(ctx context.Context, chainID string, txHash string, status types.TransactionStatus) error
	MarkTransactionsAsReverted(ctx context.Context, chainID string, fromBlock, toBlock uint64) error
	
	// 日志操作
	SaveLog(ctx context.Context, log *types.Log) error
	BatchSaveLogs(ctx context.Context, logs []*types.Log) error
	GetLogs(ctx context.Context, chainID string, filter *LogFilter) ([]*types.Log, error)
	DeleteLogs(ctx context.Context, chainID string, fromBlock, toBlock uint64) error
	
	// 水位操作
	SaveWatermark(ctx context.Context, watermark *types.Watermark) error
	GetWatermark(ctx context.Context, chainID string) (*types.Watermark, error)
	UpdateWatermark(ctx context.Context, chainID string, watermark *types.Watermark) error
	
	// 回滚事件操作
	SaveReorgEvent(ctx context.Context, event *types.ReorgEvent) error
	GetReorgEvents(ctx context.Context, chainID string, limit, offset int) ([]*types.ReorgEvent, error)
	
	// 健康检查
	HealthCheck(ctx context.Context) error
	
	// 关闭连接
	Close() error
}

// LogFilter 日志过滤器
type LogFilter struct {
	ChainID     string
	BlockNumber *uint64
	FromBlock   *uint64
	ToBlock     *uint64
	Address     string
	Topics      []string
	TxHash      string
	Limit       int
	Offset      int
}

// StorageManager 存储管理器
type StorageManager struct {
	masterDB    Storage    // 主库（写）
	slaveDB     Storage    // 从库（读）
	redis       Storage    // Redis 缓存
	elasticsearch Storage // Elasticsearch 索引
	config      *StorageConfig
}

// StorageConfig 存储配置
type StorageConfig struct {
	// 数据库配置
	MasterDB *DatabaseConfig
	SlaveDB  *DatabaseConfig
	
	// Redis 配置
	Redis *RedisConfig
	
	// Elasticsearch 配置
	Elasticsearch *ElasticsearchConfig
	
	// 缓存配置
	Cache *CacheConfig
}

// DatabaseConfig 数据库配置
type DatabaseConfig struct {
	Driver          string
	Host            string
	Port            int
	Username        string
	Password        string
	Database        string
	MaxOpenConns    int
	MaxIdleConns    int
	ConnMaxLifetime time.Duration
}

// RedisConfig Redis 配置
type RedisConfig struct {
	Addr         string
	Password     string
	DB           int
	PoolSize     int
	MinIdleConns int
}

// ElasticsearchConfig Elasticsearch 配置
type ElasticsearchConfig struct {
	Addresses   []string
	Username    string
	Password    string
	IndexPrefix string
}

// CacheConfig 缓存配置
type CacheConfig struct {
	Enabled          bool
	DefaultTTL       time.Duration
	BlockTTL         time.Duration
	TransactionTTL   time.Duration
	WatermarkTTL     time.Duration
	MetadataTTL      time.Duration
}

// NewStorageManager 创建存储管理器
func NewStorageManager(config *StorageConfig) (*StorageManager, error) {
	if config == nil {
		return nil, fmt.Errorf("config cannot be nil")
	}

	manager := &StorageManager{
		config: config,
	}

	// 创建主库连接
	if config.MasterDB != nil {
		masterDB, err := NewDatabaseStorage(config.MasterDB)
		if err != nil {
			return nil, fmt.Errorf("failed to create master database: %w", err)
		}
		manager.masterDB = masterDB
	}

	// 创建从库连接
	if config.SlaveDB != nil {
		slaveDB, err := NewDatabaseStorage(config.SlaveDB)
		if err != nil {
			return nil, fmt.Errorf("failed to create slave database: %w", err)
		}
		manager.slaveDB = slaveDB
	} else {
		// 如果没有配置从库，使用主库进行读写
		manager.slaveDB = manager.masterDB
	}

	// 创建 Redis 连接
	if config.Redis != nil {
		redis, err := NewRedisStorage(config.Redis)
		if err != nil {
			return nil, fmt.Errorf("failed to create redis storage: %w", err)
		}
		manager.redis = redis
	}

	// 创建 Elasticsearch 连接
	if config.Elasticsearch != nil {
		elasticsearch, err := NewElasticsearchStorage(config.Elasticsearch)
		if err != nil {
			return nil, fmt.Errorf("failed to create elasticsearch storage: %w", err)
		}
		manager.elasticsearch = elasticsearch
	}

	return manager, nil
}

// GetMaster 获取主库（用于写操作）
func (m *StorageManager) GetMaster() Storage {
	return m.masterDB
}

// GetSlave 获取从库（用于读操作）
func (m *StorageManager) GetSlave() Storage {
	return m.slaveDB
}

// GetRedis 获取 Redis 缓存
func (m *StorageManager) GetRedis() Storage {
	return m.redis
}

// GetElasticsearch 获取 Elasticsearch 索引
func (m *StorageManager) GetElasticsearch() Storage {
	return m.elasticsearch
}

// SaveBlock 保存区块（写入主库、缓存、索引）
func (m *StorageManager) SaveBlock(ctx context.Context, block *types.Block) error {
	startTime := time.Now()
	defer func() {
		metrics.RecordStorageOperationDuration("save_block", time.Since(startTime).Seconds())
	}()

	// 写入主库
	if err := m.masterDB.SaveBlock(ctx, block); err != nil {
		metrics.RecordStorageOperation("save_block", "error")
		return fmt.Errorf("failed to save block to master: %w", err)
	}

	// 写入缓存
	if m.redis != nil && m.config.Cache.Enabled {
		if err := m.redis.SaveBlock(ctx, block); err != nil {
			// 缓存失败不影响主流程
			logger.Warn("Failed to save block to cache",
				zap.String("chain_id", block.ChainID),
				zap.Uint64("block_number", block.Number),
				zap.Error(err))
		}
	}

	// 写入索引
	if m.elasticsearch != nil {
		if err := m.elasticsearch.SaveBlock(ctx, block); err != nil {
			// 索引失败不影响主流程
			logger.Warn("Failed to save block to elasticsearch",
				zap.String("chain_id", block.ChainID),
				zap.Uint64("block_number", block.Number),
				zap.Error(err))
		}
	}

	metrics.RecordStorageOperation("save_block", "success")
	return nil
}

// GetBlock 获取区块（优先从缓存读取）
func (m *StorageManager) GetBlock(ctx context.Context, chainID string, blockNumber uint64) (*types.Block, error) {
	startTime := time.Now()
	defer func() {
		metrics.RecordStorageOperationDuration("get_block", time.Since(startTime).Seconds())
	}()

	// 先从缓存读取
	if m.redis != nil && m.config.Cache.Enabled {
		block, err := m.redis.GetBlock(ctx, chainID, blockNumber)
		if err == nil && block != nil {
			metrics.RecordStorageOperation("get_block", "cache_hit")
			return block, nil
		}
	}

	// 从从库读取
	block, err := m.slaveDB.GetBlock(ctx, chainID, blockNumber)
	if err != nil {
		metrics.RecordStorageOperation("get_block", "error")
		return nil, fmt.Errorf("failed to get block from slave: %w", err)
	}

	metrics.RecordStorageOperation("get_block", "success")
	return block, nil
}

// SaveTransaction 保存交易（写入主库、缓存、索引）
func (m *StorageManager) SaveTransaction(ctx context.Context, tx *types.Transaction) error {
	startTime := time.Now()
	defer func() {
		metrics.RecordStorageOperationDuration("save_transaction", time.Since(startTime).Seconds())
	}()

	// 写入主库
	if err := m.masterDB.SaveTransaction(ctx, tx); err != nil {
		metrics.RecordStorageOperation("save_transaction", "error")
		return fmt.Errorf("failed to save transaction to master: %w", err)
	}

	// 写入缓存
	if m.redis != nil && m.config.Cache.Enabled {
		if err := m.redis.SaveTransaction(ctx, tx); err != nil {
			// 缓存失败不影响主流程
			logger.Warn("Failed to save transaction to cache",
				zap.String("chain_id", tx.ChainID),
				zap.String("tx_hash", tx.Hash),
				zap.Error(err))
		}
	}

	// 写入索引
	if m.elasticsearch != nil {
		if err := m.elasticsearch.SaveTransaction(ctx, tx); err != nil {
			// 索引失败不影响主流程
			logger.Warn("Failed to save transaction to elasticsearch",
				zap.String("chain_id", tx.ChainID),
				zap.String("tx_hash", tx.Hash),
				zap.Error(err))
		}
	}

	metrics.RecordStorageOperation("save_transaction", "success")
	return nil
}

// BatchSaveTransactions 批量保存交易
func (m *StorageManager) BatchSaveTransactions(ctx context.Context, txs []*types.Transaction) error {
	startTime := time.Now()
	defer func() {
		metrics.RecordStorageOperationDuration("batch_save_transactions", time.Since(startTime).Seconds())
	}()

	// 写入主库
	if err := m.masterDB.BatchSaveTransactions(ctx, txs); err != nil {
		metrics.RecordStorageOperation("batch_save_transactions", "error")
		return fmt.Errorf("failed to batch save transactions to master: %w", err)
	}

	// 写入缓存
	if m.redis != nil && m.config.Cache.Enabled {
		for _, tx := range txs {
			if err := m.redis.SaveTransaction(ctx, tx); err != nil {
				logger.Warn("Failed to save transaction to cache",
					zap.String("chain_id", tx.ChainID),
					zap.String("tx_hash", tx.Hash),
					zap.Error(err))
			}
		}
	}

	// 写入索引
	if m.elasticsearch != nil {
		for _, tx := range txs {
			if err := m.elasticsearch.SaveTransaction(ctx, tx); err != nil {
				logger.Warn("Failed to save transaction to elasticsearch",
					zap.String("chain_id", tx.ChainID),
					zap.String("tx_hash", tx.Hash),
					zap.Error(err))
			}
		}
	}

	metrics.RecordStorageOperation("batch_save_transactions", "success")
	return nil
}

// GetTransaction 获取交易（优先从缓存读取）
func (m *StorageManager) GetTransaction(ctx context.Context, chainID string, txHash string) (*types.Transaction, error) {
	startTime := time.Now()
	defer func() {
		metrics.RecordStorageOperationDuration("get_transaction", time.Since(startTime).Seconds())
	}()

	// 先从缓存读取
	if m.redis != nil && m.config.Cache.Enabled {
		tx, err := m.redis.GetTransaction(ctx, chainID, txHash)
		if err == nil && tx != nil {
			metrics.RecordStorageOperation("get_transaction", "cache_hit")
			return tx, nil
		}
	}

	// 从从库读取
	tx, err := m.slaveDB.GetTransaction(ctx, chainID, txHash)
	if err != nil {
		metrics.RecordStorageOperation("get_transaction", "error")
		return nil, fmt.Errorf("failed to get transaction from slave: %w", err)
	}

	metrics.RecordStorageOperation("get_transaction", "success")
	return tx, nil
}

// MarkTransactionsAsReverted 标记交易为已回滚
func (m *StorageManager) MarkTransactionsAsReverted(ctx context.Context, chainID string, fromBlock, toBlock uint64) error {
	startTime := time.Now()
	defer func() {
		metrics.RecordStorageOperationDuration("mark_reverted", time.Since(startTime).Seconds())
	}()

	// 更新主库
	if err := m.masterDB.MarkTransactionsAsReverted(ctx, chainID, fromBlock, toBlock); err != nil {
		metrics.RecordStorageOperation("mark_reverted", "error")
		return fmt.Errorf("failed to mark transactions as reverted: %w", err)
	}

	// 清除缓存
	if m.redis != nil {
		// TODO: 清除相关交易的缓存
	}

	// 更新索引
	if m.elasticsearch != nil {
		// TODO: 更新索引中的交易状态
	}

	metrics.RecordStorageOperation("mark_reverted", "success")
	return nil
}

// HealthCheck 健康检查
func (m *StorageManager) HealthCheck(ctx context.Context) error {
	// 检查主库
	if m.masterDB != nil {
		if err := m.masterDB.HealthCheck(ctx); err != nil {
			return fmt.Errorf("master database health check failed: %w", err)
		}
	}

	// 检查从库
	if m.slaveDB != nil && m.slaveDB != m.masterDB {
		if err := m.slaveDB.HealthCheck(ctx); err != nil {
			return fmt.Errorf("slave database health check failed: %w", err)
		}
	}

	// 检查 Redis
	if m.redis != nil {
		if err := m.redis.HealthCheck(ctx); err != nil {
			return fmt.Errorf("redis health check failed: %w", err)
		}
	}

	// 检查 Elasticsearch
	if m.elasticsearch != nil {
		if err := m.elasticsearch.HealthCheck(ctx); err != nil {
			return fmt.Errorf("elasticsearch health check failed: %w", err)
		}
	}

	return nil
}

// Close 关闭所有连接
func (m *StorageManager) Close() error {
	var lastErr error

	if m.masterDB != nil {
		if err := m.masterDB.Close(); err != nil {
			lastErr = err
		}
	}

	if m.slaveDB != nil && m.slaveDB != m.masterDB {
		if err := m.slaveDB.Close(); err != nil {
			lastErr = err
		}
	}

	if m.redis != nil {
		if err := m.redis.Close(); err != nil {
			lastErr = err
		}
	}

	if m.elasticsearch != nil {
		if err := m.elasticsearch.Close(); err != nil {
			lastErr = err
		}
	}

	return lastErr
}