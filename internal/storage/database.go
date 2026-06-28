package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/smart-scanner/multi-chain-scanner/pkg/logger"
	"github.com/smart-scanner/multi-chain-scanner/pkg/types"
	"go.uber.org/zap"
	"gorm.io/driver/mysql"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

// DatabaseStorage 数据库存储实现
// [Design: 存储层](../docs/DESIGN_SCANNER.md#1-系统概述)
type DatabaseStorage struct {
	db     *gorm.DB
	config *DatabaseConfig
}

// NewDatabaseStorage 创建数据库存储
func NewDatabaseStorage(config *DatabaseConfig) (*DatabaseStorage, error) {
	if config == nil {
		return nil, fmt.Errorf("config cannot be nil")
	}

	var dialector gorm.Dialector
	var dsn string

	switch config.Driver {
	case "mysql":
		dsn = fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=utf8mb4&parseTime=True&loc=Local",
			config.Username,
			config.Password,
			config.Host,
			config.Port,
			config.Database)
		dialector = mysql.Open(dsn)
	case "postgres":
		dsn = fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
			config.Host,
			config.Port,
			config.Username,
			config.Password,
			config.Database)
		dialector = postgres.Open(dsn)
	default:
		return nil, fmt.Errorf("unsupported database driver: %s", config.Driver)
	}

	// 创建数据库连接
	db, err := gorm.Open(dialector, &gorm.Config{
		SkipDefaultTransaction: true,
		PrepareStmt:            true,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to connect to database: %w", err)
	}

	// 获取底层 SQL DB
	sqlDB, err := db.DB()
	if err != nil {
		return nil, fmt.Errorf("failed to get sql DB: %w", err)
	}

	// 设置连接池参数
	sqlDB.SetMaxOpenConns(config.MaxOpenConns)
	sqlDB.SetMaxIdleConns(config.MaxIdleConns)
	sqlDB.SetConnMaxLifetime(config.ConnMaxLifetime)

	// 自动迁移表结构
	if err := autoMigrate(db); err != nil {
		return nil, fmt.Errorf("failed to auto migrate: %w", err)
	}

	return &DatabaseStorage{
		db:     db,
		config: config,
	}, nil
}

// autoMigrate 自动迁移表结构
func autoMigrate(db *gorm.DB) error {
	return db.AutoMigrate(
		&BlockModel{},
		&TransactionModel{},
		&LogModel{},
		&WatermarkModel{},
		&ReorgEventModel{},
	)
}

// BlockModel 区块模型
type BlockModel struct {
	ID          uint      `gorm:"primaryKey"`
	ChainID     string    `gorm:"index:idx_chain_block;not null"`
	BlockNumber uint64    `gorm:"index:idx_chain_block;not null"`
	BlockHash   string    `gorm:"index;not null"`
	ParentHash  string    `gorm:"not null"`
	Timestamp   time.Time `gorm:"not null"`
	Data        string    `gorm:"type:text"` // JSON 格式的区块数据
	Version     int       `gorm:"default:0"`  // 版本号，用于回滚
	CreatedAt   time.Time `gorm:"autoCreateTime"`
	UpdatedAt   time.Time `gorm:"autoUpdateTime"`
}

// TransactionModel 交易模型
type TransactionModel struct {
	ID          uint      `gorm:"primaryKey"`
	ChainID     string    `gorm:"index:idx_chain_tx;not null"`
	TxHash      string    `gorm:"index:idx_chain_tx;not null"`
	BlockNumber uint64    `gorm:"index:idx_chain_block;not null"`
	BlockHash   string    `gorm:"not null"`
	FromAddress string    `gorm:"index;not null"`
	ToAddress   string    `gorm:"index"`
	Value       string    `gorm:"not null"` // 大整数，存储为字符串
	GasPrice    string    `gorm:"not null"`
	GasUsed     uint64    `gorm:"not null"`
	Status      string    `gorm:"not null"` // pending, confirmed, finalized, reverted
	InputData   string    `gorm:"type:text"`
	Data        string    `gorm:"type:text"` // JSON 格式的交易数据
	Version     int       `gorm:"default:0"`
	CreatedAt   time.Time `gorm:"autoCreateTime"`
	UpdatedAt   time.Time `gorm:"autoUpdateTime"`
}

// LogModel 日志模型
type LogModel struct {
	ID          uint      `gorm:"primaryKey"`
	ChainID     string    `gorm:"index:idx_chain_log;not null"`
	TxHash      string    `gorm:"index:idx_chain_tx;not null"`
	BlockNumber uint64    `gorm:"index:idx_chain_block;not null"`
	Address     string    `gorm:"index;not null"`
	Topics      string    `gorm:"type:text"` // JSON 数组
	Data        string    `gorm:"type:text"` // JSON 格式的日志数据
	LogIndex    uint64    `gorm:"not null"`
	TxIndex     uint64    `gorm:"not null"`
	Version     int       `gorm:"default:0"`
	CreatedAt   time.Time `gorm:"autoCreateTime"`
	UpdatedAt   time.Time `gorm:"autoUpdateTime"`
}

// WatermarkModel 水位模型
type WatermarkModel struct {
	ID             uint      `gorm:"primaryKey"`
	ChainID        string    `gorm:"uniqueIndex;not null"`
	ScannedHeight  uint64    `gorm:"not null"`
	ConfirmedHeight uint64   `gorm:"not null"`
	FinalizedHeight uint64   `gorm:"not null"`
	ReorgBoundary  uint64    `gorm:"not null"`
	LastUpdateTime time.Time `gorm:"not null"`
	CreatedAt      time.Time `gorm:"autoCreateTime"`
	UpdatedAt      time.Time `gorm:"autoUpdateTime"`
}

// ReorgEventModel 回滚事件模型
type ReorgEventModel struct {
	ID             uint      `gorm:"primaryKey"`
	ChainID        string    `gorm:"index;not null"`
	DetectedAt     time.Time `gorm:"not null"`
	OldBlockNumber uint64    `gorm:"not null"`
	OldBlockHash   string    `gorm:"not null"`
	NewBlockNumber uint64    `gorm:"not null"`
	NewBlockHash   string    `gorm:"not null"`
	Depth          uint64    `gorm:"not null"`
	AffectedTxs    string    `gorm:"type:text"` // JSON 数组
	Processed      bool      `gorm:"default:false"`
	Data           string    `gorm:"type:text"` // JSON 格式的事件数据
	CreatedAt      time.Time `gorm:"autoCreateTime"`
	UpdatedAt      time.Time `gorm:"autoUpdateTime"`
}

// SaveBlock 保存区块
func (s *DatabaseStorage) SaveBlock(ctx context.Context, block *types.Block) error {
	// 序列化区块数据
	data, err := json.Marshal(block)
	if err != nil {
		return fmt.Errorf("failed to marshal block: %w", err)
	}

	// 创建区块模型
	model := &BlockModel{
		ChainID:     block.ChainID,
		BlockNumber: block.Number,
		BlockHash:   block.Hash,
		ParentHash:  block.ParentHash,
		Timestamp:   block.Timestamp,
		Data:        string(data),
	}

	// 使用 ON DUPLICATE KEY UPDATE 处理重复插入
	result := s.db.WithContext(ctx).Where("chain_id = ? AND block_number = ?", block.ChainID, block.Number).
		Assign(model).
		FirstOrCreate(model)

	if result.Error != nil {
		return fmt.Errorf("failed to save block: %w", result.Error)
	}

	return nil
}

// GetBlock 获取区块
func (s *DatabaseStorage) GetBlock(ctx context.Context, chainID string, blockNumber uint64) (*types.Block, error) {
	var model BlockModel
	result := s.db.WithContext(ctx).Where("chain_id = ? AND block_number = ?", chainID, blockNumber).First(&model)

	if result.Error != nil {
		if result.Error == gorm.ErrRecordNotFound {
			return nil, fmt.Errorf("block not found")
		}
		return nil, fmt.Errorf("failed to get block: %w", result.Error)
	}

	// 反序列化区块数据
	var block types.Block
	if err := json.Unmarshal([]byte(model.Data), &block); err != nil {
		return nil, fmt.Errorf("failed to unmarshal block: %w", err)
	}

	return &block, nil
}

// GetBlockByHash 根据哈希获取区块
func (s *DatabaseStorage) GetBlockByHash(ctx context.Context, chainID string, blockHash string) (*types.Block, error) {
	var model BlockModel
	result := s.db.WithContext(ctx).Where("chain_id = ? AND block_hash = ?", chainID, blockHash).First(&model)

	if result.Error != nil {
		if result.Error == gorm.ErrRecordNotFound {
			return nil, fmt.Errorf("block not found")
		}
		return nil, fmt.Errorf("failed to get block by hash: %w", result.Error)
	}

	// 反序列化区块数据
	var block types.Block
	if err := json.Unmarshal([]byte(model.Data), &block); err != nil {
		return nil, fmt.Errorf("failed to unmarshal block: %w", err)
	}

	return &block, nil
}

// GetLatestBlock 获取最新区块
func (s *DatabaseStorage) GetLatestBlock(ctx context.Context, chainID string) (*types.Block, error) {
	var model BlockModel
	result := s.db.WithContext(ctx).Where("chain_id = ?", chainID).Order("block_number DESC").First(&model)

	if result.Error != nil {
		if result.Error == gorm.ErrRecordNotFound {
			return nil, fmt.Errorf("no blocks found for chain %s", chainID)
		}
		return nil, fmt.Errorf("failed to get latest block: %w", result.Error)
	}

	// 反序列化区块数据
	var block types.Block
	if err := json.Unmarshal([]byte(model.Data), &block); err != nil {
		return nil, fmt.Errorf("failed to unmarshal block: %w", err)
	}

	return &block, nil
}

// DeleteBlocks 删除区块
func (s *DatabaseStorage) DeleteBlocks(ctx context.Context, chainID string, fromBlock, toBlock uint64) error {
	result := s.db.WithContext(ctx).Where("chain_id = ? AND block_number >= ? AND block_number <= ?", chainID, fromBlock, toBlock).Delete(&BlockModel{})
	if result.Error != nil {
		return fmt.Errorf("failed to delete blocks: %w", result.Error)
	}
	return nil
}

// SaveTransaction 保存交易
func (s *DatabaseStorage) SaveTransaction(ctx context.Context, tx *types.Transaction) error {
	// 序列化交易数据
	data, err := json.Marshal(tx)
	if err != nil {
		return fmt.Errorf("failed to marshal transaction: %w", err)
	}

	// 转换大整数为字符串
	valueStr := "0"
	if tx.Value != nil {
		valueStr = tx.Value.String()
	}

	gasPriceStr := "0"
	if tx.GasPrice != nil {
		gasPriceStr = tx.GasPrice.String()
	}

	// 创建交易模型
	model := &TransactionModel{
		ChainID:     tx.ChainID,
		TxHash:      tx.Hash,
		BlockNumber: tx.BlockNumber,
		BlockHash:   tx.BlockHash,
		FromAddress: tx.From,
		ToAddress:   tx.To,
		Value:       valueStr,
		GasPrice:    gasPriceStr,
		GasUsed:     tx.GasUsed,
		Status:      string(tx.Status),
		InputData:   tx.InputData,
		Data:        string(data),
	}

	// 使用 ON DUPLICATE KEY UPDATE 处理重复插入
	result := s.db.WithContext(ctx).Where("chain_id = ? AND tx_hash = ?", tx.ChainID, tx.Hash).
		Assign(model).
		FirstOrCreate(model)

	if result.Error != nil {
		return fmt.Errorf("failed to save transaction: %w", result.Error)
	}

	return nil
}

// BatchSaveTransactions 批量保存交易
func (s *DatabaseStorage) BatchSaveTransactions(ctx context.Context, txs []*types.Transaction) error {
	if len(txs) == 0 {
		return nil
	}

	// 使用事务批量保存
	return s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		for _, transaction := range txs {
			if err := s.SaveTransaction(ctx, transaction); err != nil {
				return err
			}
		}
		return nil
	})
}

// GetTransaction 获取交易
func (s *DatabaseStorage) GetTransaction(ctx context.Context, chainID string, txHash string) (*types.Transaction, error) {
	var model TransactionModel
	result := s.db.WithContext(ctx).Where("chain_id = ? AND tx_hash = ?", chainID, txHash).First(&model)

	if result.Error != nil {
		if result.Error == gorm.ErrRecordNotFound {
			return nil, fmt.Errorf("transaction not found")
		}
		return nil, fmt.Errorf("failed to get transaction: %w", result.Error)
	}

	// 反序列化交易数据
	var transaction types.Transaction
	if err := json.Unmarshal([]byte(model.Data), &transaction); err != nil {
		return nil, fmt.Errorf("failed to unmarshal transaction: %w", err)
	}

	return &transaction, nil
}

// GetTransactionsByBlock 根据区块获取交易
func (s *DatabaseStorage) GetTransactionsByBlock(ctx context.Context, chainID string, blockNumber uint64) ([]*types.Transaction, error) {
	var models []TransactionModel
	result := s.db.WithContext(ctx).Where("chain_id = ? AND block_number = ?", chainID, blockNumber).Find(&models)

	if result.Error != nil {
		return nil, fmt.Errorf("failed to get transactions by block: %w", result.Error)
	}

	transactions := make([]*types.Transaction, 0, len(models))
	for _, model := range models {
		var transaction types.Transaction
		if err := json.Unmarshal([]byte(model.Data), &transaction); err != nil {
			logger.Error("Failed to unmarshal transaction",
				zap.String("chain_id", chainID),
				zap.String("tx_hash", model.TxHash),
				zap.Error(err))
			continue
		}
		transactions = append(transactions, &transaction)
	}

	return transactions, nil
}

// GetTransactionsByAddress 根据地址获取交易
func (s *DatabaseStorage) GetTransactionsByAddress(ctx context.Context, chainID string, address string, limit, offset int) ([]*types.Transaction, error) {
	var models []TransactionModel
	result := s.db.WithContext(ctx).Where("chain_id = ? AND (from_address = ? OR to_address = ?)", chainID, address, address).
		Order("block_number DESC").
		Limit(limit).
		Offset(offset).
		Find(&models)

	if result.Error != nil {
		return nil, fmt.Errorf("failed to get transactions by address: %w", result.Error)
	}

	transactions := make([]*types.Transaction, 0, len(models))
	for _, model := range models {
		var transaction types.Transaction
		if err := json.Unmarshal([]byte(model.Data), &transaction); err != nil {
			logger.Error("Failed to unmarshal transaction",
				zap.String("chain_id", chainID),
				zap.String("tx_hash", model.TxHash),
				zap.Error(err))
			continue
		}
		transactions = append(transactions, &transaction)
	}

	return transactions, nil
}

// UpdateTransactionStatus 更新交易状态
func (s *DatabaseStorage) UpdateTransactionStatus(ctx context.Context, chainID string, txHash string, status types.TransactionStatus) error {
	result := s.db.WithContext(ctx).Model(&TransactionModel{}).
		Where("chain_id = ? AND tx_hash = ?", chainID, txHash).
		Update("status", string(status))

	if result.Error != nil {
		return fmt.Errorf("failed to update transaction status: %w", result.Error)
	}

	return nil
}

// MarkTransactionsAsReverted 标记交易为已回滚
func (s *DatabaseStorage) MarkTransactionsAsReverted(ctx context.Context, chainID string, fromBlock, toBlock uint64) error {
	result := s.db.WithContext(ctx).Model(&TransactionModel{}).
		Where("chain_id = ? AND block_number >= ? AND block_number <= ?", chainID, fromBlock, toBlock).
		Update("status", string(types.TxStatusReverted))

	if result.Error != nil {
		return fmt.Errorf("failed to mark transactions as reverted: %w", result.Error)
	}

	return nil
}

// SaveLog 保存日志
func (s *DatabaseStorage) SaveLog(ctx context.Context, log *types.Log) error {
	// 序列化日志数据
	data, err := json.Marshal(log)
	if err != nil {
		return fmt.Errorf("failed to marshal log: %w", err)
	}

	// 序列化主题
	topics, err := json.Marshal(log.Topics)
	if err != nil {
		return fmt.Errorf("failed to marshal topics: %w", err)
	}

	// 创建日志模型
	model := &LogModel{
		ChainID:     log.ChainID,
		TxHash:      log.TxHash,
		BlockNumber: log.BlockNumber,
		Address:     log.Address,
		Topics:      string(topics),
		Data:        string(data),
		LogIndex:    log.LogIndex,
		TxIndex:     log.TxIndex,
	}

	// 使用 ON DUPLICATE KEY UPDATE 处理重复插入
	result := s.db.WithContext(ctx).Where("chain_id = ? AND tx_hash = ? AND log_index = ?", log.ChainID, log.TxHash, log.LogIndex).
		Assign(model).
		FirstOrCreate(model)

	if result.Error != nil {
		return fmt.Errorf("failed to save log: %w", result.Error)
	}

	return nil
}

// BatchSaveLogs 批量保存日志
func (s *DatabaseStorage) BatchSaveLogs(ctx context.Context, logs []*types.Log) error {
	if len(logs) == 0 {
		return nil
	}

	// 使用事务批量保存
	return s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		for _, log := range logs {
			if err := s.SaveLog(ctx, log); err != nil {
				return err
			}
		}
		return nil
	})
}

// GetLogs 获取日志
func (s *DatabaseStorage) GetLogs(ctx context.Context, chainID string, filter *LogFilter) ([]*types.Log, error) {
	query := s.db.WithContext(ctx).Model(&LogModel{}).Where("chain_id = ?", chainID)

	// 应用过滤器
	if filter.BlockNumber != nil {
		query = query.Where("block_number = ?", *filter.BlockNumber)
	}
	if filter.FromBlock != nil {
		query = query.Where("block_number >= ?", *filter.FromBlock)
	}
	if filter.ToBlock != nil {
		query = query.Where("block_number <= ?", *filter.ToBlock)
	}
	if filter.Address != "" {
		query = query.Where("address = ?", filter.Address)
	}
	if filter.TxHash != "" {
		query = query.Where("tx_hash = ?", filter.TxHash)
	}

	var models []LogModel
	result := query.Order("block_number ASC, log_index ASC").Limit(filter.Limit).Offset(filter.Offset).Find(&models)

	if result.Error != nil {
		return nil, fmt.Errorf("failed to get logs: %w", result.Error)
	}

	logs := make([]*types.Log, 0, len(models))
	for _, model := range models {
		var log types.Log
		if err := json.Unmarshal([]byte(model.Data), &log); err != nil {
			logger.Error("Failed to unmarshal log",
				zap.String("chain_id", chainID),
				zap.String("tx_hash", model.TxHash),
				zap.Error(err))
			continue
		}
		logs = append(logs, &log)
	}

	return logs, nil
}

// DeleteLogs 删除日志
func (s *DatabaseStorage) DeleteLogs(ctx context.Context, chainID string, fromBlock, toBlock uint64) error {
	result := s.db.WithContext(ctx).Where("chain_id = ? AND block_number >= ? AND block_number <= ?", chainID, fromBlock, toBlock).Delete(&LogModel{})
	if result.Error != nil {
		return fmt.Errorf("failed to delete logs: %w", result.Error)
	}
	return nil
}

// SaveWatermark 保存水位
func (s *DatabaseStorage) SaveWatermark(ctx context.Context, watermark *types.Watermark) error {
	model := &WatermarkModel{
		ChainID:         watermark.ChainID,
		ScannedHeight:   watermark.ScannedHeight,
		ConfirmedHeight: watermark.ConfirmedHeight,
		FinalizedHeight: watermark.FinalizedHeight,
		ReorgBoundary:   watermark.ReorgBoundary,
		LastUpdateTime:  watermark.LastUpdateTime,
	}

	// 使用 ON DUPLICATE KEY UPDATE 处理重复插入
	result := s.db.WithContext(ctx).Where("chain_id = ?", watermark.ChainID).
		Assign(model).
		FirstOrCreate(model)

	if result.Error != nil {
		return fmt.Errorf("failed to save watermark: %w", result.Error)
	}

	return nil
}

// GetWatermark 获取水位
func (s *DatabaseStorage) GetWatermark(ctx context.Context, chainID string) (*types.Watermark, error) {
	var model WatermarkModel
	result := s.db.WithContext(ctx).Where("chain_id = ?", chainID).First(&model)

	if result.Error != nil {
		if result.Error == gorm.ErrRecordNotFound {
			return nil, fmt.Errorf("watermark not found for chain %s", chainID)
		}
		return nil, fmt.Errorf("failed to get watermark: %w", result.Error)
	}

	return &types.Watermark{
		ChainID:         model.ChainID,
		ScannedHeight:   model.ScannedHeight,
		ConfirmedHeight: model.ConfirmedHeight,
		FinalizedHeight: model.FinalizedHeight,
		ReorgBoundary:   model.ReorgBoundary,
		LastUpdateTime:  model.LastUpdateTime,
	}, nil
}

// UpdateWatermark 更新水位
func (s *DatabaseStorage) UpdateWatermark(ctx context.Context, chainID string, watermark *types.Watermark) error {
	result := s.db.WithContext(ctx).Model(&WatermarkModel{}).
		Where("chain_id = ?", chainID).
		Updates(map[string]interface{}{
			"scanned_height":   watermark.ScannedHeight,
			"confirmed_height": watermark.ConfirmedHeight,
			"finalized_height": watermark.FinalizedHeight,
			"reorg_boundary":   watermark.ReorgBoundary,
			"last_update_time": watermark.LastUpdateTime,
		})

	if result.Error != nil {
		return fmt.Errorf("failed to update watermark: %w", result.Error)
	}

	return nil
}

// SaveReorgEvent 保存回滚事件
func (s *DatabaseStorage) SaveReorgEvent(ctx context.Context, event *types.ReorgEvent) error {
	// 序列化事件数据
	data, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("failed to marshal reorg event: %w", err)
	}

	// 序列化受影响的交易
	affectedTxs, err := json.Marshal(event.AffectedTxs)
	if err != nil {
		return fmt.Errorf("failed to marshal affected transactions: %w", err)
	}

	model := &ReorgEventModel{
		ChainID:        event.ChainID,
		DetectedAt:     event.DetectedAt,
		OldBlockNumber: event.OldBlockNumber,
		OldBlockHash:   event.OldBlockHash,
		NewBlockNumber: event.NewBlockNumber,
		NewBlockHash:   event.NewBlockHash,
		Depth:          event.Depth,
		AffectedTxs:    string(affectedTxs),
		Processed:      event.Processed,
		Data:           string(data),
	}

	result := s.db.WithContext(ctx).Create(model)
	if result.Error != nil {
		return fmt.Errorf("failed to save reorg event: %w", result.Error)
	}

	return nil
}

// GetReorgEvents 获取回滚事件
func (s *DatabaseStorage) GetReorgEvents(ctx context.Context, chainID string, limit, offset int) ([]*types.ReorgEvent, error) {
	var models []ReorgEventModel
	result := s.db.WithContext(ctx).Where("chain_id = ?", chainID).
		Order("detected_at DESC").
		Limit(limit).
		Offset(offset).
		Find(&models)

	if result.Error != nil {
		return nil, fmt.Errorf("failed to get reorg events: %w", result.Error)
	}

	events := make([]*types.ReorgEvent, 0, len(models))
	for _, model := range models {
		var event types.ReorgEvent
		if err := json.Unmarshal([]byte(model.Data), &event); err != nil {
			logger.Error("Failed to unmarshal reorg event",
				zap.String("chain_id", chainID),
				zap.Error(err))
			continue
		}
		events = append(events, &event)
	}

	return events, nil
}

// HealthCheck 健康检查
func (s *DatabaseStorage) HealthCheck(ctx context.Context) error {
	sqlDB, err := s.db.DB()
	if err != nil {
		return fmt.Errorf("failed to get sql DB: %w", err)
	}

	return sqlDB.PingContext(ctx)
}

// Close 关闭连接
func (s *DatabaseStorage) Close() error {
	sqlDB, err := s.db.DB()
	if err != nil {
		return fmt.Errorf("failed to get sql DB: %w", err)
	}

	return sqlDB.Close()
}