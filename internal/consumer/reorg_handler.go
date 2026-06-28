package consumer

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/smart-scanner/multi-chain-scanner/internal/storage"
	"github.com/smart-scanner/multi-chain-scanner/pkg/logger"
	"github.com/smart-scanner/multi-chain-scanner/pkg/types"
	"go.uber.org/zap"
)

// ReorgHandler 回滚处理器接口
// [Design: ReorgHandler 回滚处理器](../docs/DESIGN_SCANNER.md#64-reorghandler-回滚处理器)
type ReorgHandler interface {
	// HandleReorg 处理回滚事件
	HandleReorg(ctx context.Context, event *types.ReorgEvent) error
	
	// GetReorgHistory 获取回滚历史
	GetReorgHistory(ctx context.Context, chainID string, limit int) ([]*types.ReorgEvent, error)
	
	// GetReorgStatistics 获取回滚统计信息
	GetReorgStatistics(ctx context.Context, chainID string) (*ReorgStatistics, error)
}

// DefaultReorgHandler 默认回滚处理器实现
type DefaultReorgHandler struct {
	storage     storage.Storage
	stateMachine StateMachine
	mu          sync.RWMutex
	processing  map[string]bool // 正在处理的回滚事件
}

// NewDefaultReorgHandler 创建默认回滚处理器
func NewDefaultReorgHandler(storage storage.Storage, stateMachine StateMachine) *DefaultReorgHandler {
	return &DefaultReorgHandler{
		storage:     storage,
		stateMachine: stateMachine,
		processing:  make(map[string]bool),
	}
}

// HandleReorg 处理回滚事件
func (rh *DefaultReorgHandler) HandleReorg(ctx context.Context, event *types.ReorgEvent) error {
	eventKey := fmt.Sprintf("%s-%d-%d", event.ChainID, event.OldBlockNumber, event.NewBlockNumber)
	
	// 检查是否正在处理
	rh.mu.Lock()
	if rh.processing[eventKey] {
		rh.mu.Unlock()
		return fmt.Errorf("reorg event is already being processed: %s", eventKey)
	}
	rh.processing[eventKey] = true
	rh.mu.Unlock()
	
	defer func() {
		rh.mu.Lock()
		delete(rh.processing, eventKey)
		rh.mu.Unlock()
	}()

	logger.Warn("Handling reorg event",
		zap.String("chain_id", event.ChainID),
		zap.Uint64("old_block", event.OldBlockNumber),
		zap.Uint64("new_block", event.NewBlockNumber),
		zap.Uint64("depth", event.Depth))

	// 1. 验证回滚事件
	if err := rh.validateReorgEvent(ctx, event); err != nil {
		return fmt.Errorf("failed to validate reorg event: %w", err)
	}

	// 2. 标记受影响的交易为已回滚
	if err := rh.markTransactionsAsReverted(ctx, event); err != nil {
		return fmt.Errorf("failed to mark transactions as reverted: %w", err)
	}

	// 3. 冲正业务数据
	if err := rh.revertBusinessData(ctx, event); err != nil {
		return fmt.Errorf("failed to revert business data: %w", err)
	}

	// 4. 清理回滚区间的数据
	if err := rh.cleanupReorgData(ctx, event); err != nil {
		return fmt.Errorf("failed to cleanup reorg data: %w", err)
	}

	// 5. 标记回滚事件为已处理
	if err := rh.markReorgEventAsProcessed(ctx, event); err != nil {
		return fmt.Errorf("failed to mark reorg event as processed: %w", err)
	}

	logger.Warn("Reorg event handled successfully",
		zap.String("chain_id", event.ChainID),
		zap.Uint64("old_block", event.OldBlockNumber),
		zap.Uint64("new_block", event.NewBlockNumber),
		zap.Duration("duration", time.Since(event.DetectedAt)))

	return nil
}

// validateReorgEvent 验证回滚事件
func (rh *DefaultReorgHandler) validateReorgEvent(ctx context.Context, event *types.ReorgEvent) error {
	logger.Info("Validating reorg event",
		zap.String("chain_id", event.ChainID),
		zap.Uint64("old_block", event.OldBlockNumber),
		zap.Uint64("new_block", event.NewBlockNumber))

	// 验证回滚深度
	if event.Depth == 0 {
		return fmt.Errorf("invalid reorg depth: %d", event.Depth)
	}

	// 验证回滚深度是否过大（可能是攻击）
	if event.Depth > 1000 {
		logger.Error("Reorg depth too large, possible attack",
			zap.String("chain_id", event.ChainID),
			zap.Uint64("depth", event.Depth))
		return fmt.Errorf("reorg depth too large: %d", event.Depth)
	}

	// 验证区块哈希
	if event.OldBlockHash == "" || event.NewBlockHash == "" {
		return fmt.Errorf("invalid block hash")
	}

	// 验证新区块是否在链上
	newBlock, err := rh.storage.GetBlockByHash(ctx, event.ChainID, event.NewBlockHash)
	if err != nil {
		return fmt.Errorf("failed to get new block: %w", err)
	}

	if newBlock.Number != event.NewBlockNumber {
		return fmt.Errorf("new block number mismatch: expected %d, got %d",
			event.NewBlockNumber, newBlock.Number)
	}

	return nil
}

// markTransactionsAsReverted 标记受影响的交易为已回滚
func (rh *DefaultReorgHandler) markTransactionsAsReverted(ctx context.Context, event *types.ReorgEvent) error {
	logger.Info("Marking transactions as reverted",
		zap.String("chain_id", event.ChainID),
		zap.Uint64("from_block", event.NewBlockNumber),
		zap.Uint64("to_block", event.OldBlockNumber))

	// 计算回滚区间
	fromBlock := event.NewBlockNumber
	toBlock := event.OldBlockNumber

	// 标记交易为已回滚
	if err := rh.storage.MarkTransactionsAsReverted(ctx, event.ChainID, fromBlock, toBlock); err != nil {
		return fmt.Errorf("failed to mark transactions as reverted: %w", err)
	}

	// 获取受影响的交易列表
	// 这里应该从数据库查询受影响的交易
	// 由于我们的 storage 接口没有直接支持批量查询，
	// 这里使用一个简化的实现
	
	affectedTxs := make([]string, 0)
	for blockNumber := fromBlock; blockNumber <= toBlock; blockNumber++ {
		txs, err := rh.storage.GetTransactionsByBlock(ctx, event.ChainID, blockNumber)
		if err != nil {
			logger.Error("Failed to get transactions by block",
				zap.String("chain_id", event.ChainID),
				zap.Uint64("block_number", blockNumber),
				zap.Error(err))
			continue
		}

		for _, tx := range txs {
			affectedTxs = append(affectedTxs, tx.Hash)
			
			// 强制设置交易状态为已回滚
			if err := rh.stateMachine.ForceSetState(ctx, event.ChainID, tx.Hash, types.TxStatusReverted); err != nil {
				logger.Error("Failed to force set transaction state",
					zap.String("chain_id", event.ChainID),
					zap.String("tx_hash", tx.Hash),
					zap.Error(err))
			}
		}
	}

	logger.Info("Marked transactions as reverted",
		zap.String("chain_id", event.ChainID),
		zap.Int("count", len(affectedTxs)))

	return nil
}

// revertBusinessData 冲正业务数据
func (rh *DefaultReorgHandler) revertBusinessData(ctx context.Context, event *types.ReorgEvent) error {
	logger.Info("Reverting business data",
		zap.String("chain_id", event.ChainID),
		zap.Uint64("from_block", event.NewBlockNumber),
		zap.Uint64("to_block", event.OldBlockNumber))

	// 这里应该根据业务需求冲正相关的业务数据
	// 例如：
	// 1. 冲正余额变化
	// 2. 取消相关订单
	// 3. 撤销相关操作
	// 4. 发送通知

	// 获取受影响的交易
	fromBlock := event.NewBlockNumber
	toBlock := event.OldBlockNumber

	for blockNumber := fromBlock; blockNumber <= toBlock; blockNumber++ {
		txs, err := rh.storage.GetTransactionsByBlock(ctx, event.ChainID, blockNumber)
		if err != nil {
			logger.Error("Failed to get transactions by block",
				zap.String("chain_id", event.ChainID),
				zap.Uint64("block_number", blockNumber),
				zap.Error(err))
			continue
		}

		for _, tx := range txs {
			// 冲正单个交易的业务数据
			if err := rh.revertTransactionBusinessData(ctx, tx); err != nil {
				logger.Error("Failed to revert transaction business data",
					zap.String("chain_id", event.ChainID),
					zap.String("tx_hash", tx.Hash),
					zap.Error(err))
			}
		}
	}

	return nil
}

// revertTransactionBusinessData 冲正单个交易的业务数据
func (rh *DefaultReorgHandler) revertTransactionBusinessData(ctx context.Context, tx *types.Transaction) error {
	logger.Debug("Reverting transaction business data",
		zap.String("chain_id", tx.ChainID),
		zap.String("tx_hash", tx.Hash))

	// 这里应该根据业务需求冲正交易相关的业务数据
	// 示例：
	// 1. 如果是转账交易，冲正余额
	// 2. 如果是合约调用，撤销相关状态变化
	// 3. 如果是代币转账，冲正代币余额

	return nil
}

// cleanupReorgData 清理回滚区间的数据
func (rh *DefaultReorgHandler) cleanupReorgData(ctx context.Context, event *types.ReorgEvent) error {
	logger.Info("Cleaning up reorg data",
		zap.String("chain_id", event.ChainID),
		zap.Uint64("from_block", event.NewBlockNumber),
		zap.Uint64("to_block", event.OldBlockNumber))

	// 删除回滚区间的区块和交易
	fromBlock := event.NewBlockNumber
	toBlock := event.OldBlockNumber

	// 删除区块
	if err := rh.storage.DeleteBlocks(ctx, event.ChainID, fromBlock, toBlock); err != nil {
		return fmt.Errorf("failed to delete blocks: %w", err)
	}

	// 删除日志
	if err := rh.storage.DeleteLogs(ctx, event.ChainID, fromBlock, toBlock); err != nil {
		return fmt.Errorf("failed to delete logs: %w", err)
	}

	logger.Info("Cleaned up reorg data successfully",
		zap.String("chain_id", event.ChainID),
		zap.Uint64("from_block", fromBlock),
		zap.Uint64("to_block", toBlock))

	return nil
}

// markReorgEventAsProcessed 标记回滚事件为已处理
func (rh *DefaultReorgHandler) markReorgEventAsProcessed(ctx context.Context, event *types.ReorgEvent) error {
	logger.Info("Marking reorg event as processed",
		zap.String("chain_id", event.ChainID),
		zap.Uint64("old_block", event.OldBlockNumber),
		zap.Uint64("new_block", event.NewBlockNumber))

	// 这里应该更新数据库中的回滚事件状态
	// 由于我们的 storage 接口没有直接支持更新回滚事件，
	// 这里使用一个简化的实现

	event.Processed = true

	// 保存更新后的回滚事件
	if err := rh.storage.SaveReorgEvent(ctx, event); err != nil {
		return fmt.Errorf("failed to save reorg event: %w", err)
	}

	return nil
}

// GetReorgHistory 获取回滚历史
func (rh *DefaultReorgHandler) GetReorgHistory(ctx context.Context, chainID string, limit int) ([]*types.ReorgEvent, error) {
	events, err := rh.storage.GetReorgEvents(ctx, chainID, limit, 0)
	if err != nil {
		return nil, fmt.Errorf("failed to get reorg events: %w", err)
	}

	return events, nil
}

// GetReorgStatistics 获取回滚统计信息
func (rh *DefaultReorgHandler) GetReorgStatistics(ctx context.Context, chainID string) (*ReorgStatistics, error) {
	// 获取最近的回滚事件
	events, err := rh.GetReorgHistory(ctx, chainID, 100)
	if err != nil {
		return nil, fmt.Errorf("failed to get reorg history: %w", err)
	}

	// 计算统计信息
	stats := &ReorgStatistics{
		ChainID:         chainID,
		TotalCount:      int64(len(events)),
		MaxDepth:        0,
		AverageDepth:    0,
		LastReorgTime:   nil,
		LastReorgDepth:  0,
	}

	if len(events) > 0 {
		totalDepth := uint64(0)
		for _, event := range events {
			if event.Depth > stats.MaxDepth {
				stats.MaxDepth = event.Depth
			}
			totalDepth += event.Depth
		}

		stats.AverageDepth = float64(totalDepth) / float64(len(events))
		stats.LastReorgTime = &events[0].DetectedAt
		stats.LastReorgDepth = events[0].Depth
	}

	return stats, nil
}

// ReorgStatistics 回滚统计信息
type ReorgStatistics struct {
	ChainID        string     `json:"chain_id"`
	TotalCount     int64      `json:"total_count"`
	MaxDepth       uint64     `json:"max_depth"`
	AverageDepth   float64    `json:"average_depth"`
	LastReorgTime  *time.Time `json:"last_reorg_time,omitempty"`
	LastReorgDepth uint64     `json:"last_reorg_depth,omitempty"`
}

// AdvancedReorgHandler 高级回滚处理器实现
type AdvancedReorgHandler struct {
	storage     storage.Storage
	stateMachine StateMachine
	config      *ReorgHandlerConfig
	mu          sync.RWMutex
	processing  map[string]bool
}

// ReorgHandlerConfig 回滚处理器配置
type ReorgHandlerConfig struct {
	// 最大允许的回滚深度
	MaxAllowedDepth uint64
	
	// 回滚处理超时时间
	ReorgTimeout time.Duration
	
	// 是否启用业务数据冲正
	EnableBusinessDataReversion bool
	
	// 是否启用数据清理
	EnableDataCleanup bool
	
	// 回滚事件保留时间
	ReorgEventRetentionTime time.Duration
}

// NewAdvancedReorgHandler 创建高级回滚处理器
func NewAdvancedReorgHandler(
	storage storage.Storage,
	stateMachine StateMachine,
	config *ReorgHandlerConfig,
) *AdvancedReorgHandler {
	if config == nil {
		config = &ReorgHandlerConfig{
			MaxAllowedDepth:            1000,
			ReorgTimeout:               30 * time.Minute,
			EnableBusinessDataReversion: true,
			EnableDataCleanup:          true,
			ReorgEventRetentionTime:    7 * 24 * time.Hour,
		}
	}

	return &AdvancedReorgHandler{
		storage:     storage,
		stateMachine: stateMachine,
		config:      config,
		processing:  make(map[string]bool),
	}
}

// HandleReorg 处理回滚事件
func (rh *AdvancedReorgHandler) HandleReorg(ctx context.Context, event *types.ReorgEvent) error {
	eventKey := fmt.Sprintf("%s-%d-%d", event.ChainID, event.OldBlockNumber, event.NewBlockNumber)
	
	// 检查是否正在处理
	rh.mu.Lock()
	if rh.processing[eventKey] {
		rh.mu.Unlock()
		return fmt.Errorf("reorg event is already being processed: %s", eventKey)
	}
	rh.processing[eventKey] = true
	rh.mu.Unlock()
	
	defer func() {
		rh.mu.Lock()
		delete(rh.processing, eventKey)
		rh.mu.Unlock()
	}()

	// 设置超时上下文
	timeoutCtx, cancel := context.WithTimeout(ctx, rh.config.ReorgTimeout)
	defer cancel()

	logger.Warn("Handling reorg event with timeout",
		zap.String("chain_id", event.ChainID),
		zap.Uint64("old_block", event.OldBlockNumber),
		zap.Uint64("new_block", event.NewBlockNumber),
		zap.Duration("timeout", rh.config.ReorgTimeout))

	// 使用超时上下文处理回滚
	if err := rh.handleReorgWithTimeout(timeoutCtx, event); err != nil {
		return fmt.Errorf("failed to handle reorg: %w", err)
	}

	return nil
}

// handleReorgWithTimeout 使用超时上下文处理回滚
func (rh *AdvancedReorgHandler) handleReorgWithTimeout(ctx context.Context, event *types.ReorgEvent) error {
	// 1. 验证回滚事件
	if err := rh.validateReorgEvent(ctx, event); err != nil {
		return fmt.Errorf("failed to validate reorg event: %w", err)
	}

	// 2. 标记受影响的交易为已回滚
	if err := rh.markTransactionsAsReverted(ctx, event); err != nil {
		return fmt.Errorf("failed to mark transactions as reverted: %w", err)
	}

	// 3. 冲正业务数据（如果启用）
	if rh.config.EnableBusinessDataReversion {
		if err := rh.revertBusinessData(ctx, event); err != nil {
			return fmt.Errorf("failed to revert business data: %w", err)
		}
	}

	// 4. 清理回滚区间的数据（如果启用）
	if rh.config.EnableDataCleanup {
		if err := rh.cleanupReorgData(ctx, event); err != nil {
			return fmt.Errorf("failed to cleanup reorg data: %w", err)
		}
	}

	// 5. 标记回滚事件为已处理
	if err := rh.markReorgEventAsProcessed(ctx, event); err != nil {
		return fmt.Errorf("failed to mark reorg event as processed: %w", err)
	}

	return nil
}

// validateReorgEvent 验证回滚事件
func (rh *AdvancedReorgHandler) validateReorgEvent(ctx context.Context, event *types.ReorgEvent) error {
	// 验证回滚深度是否超过最大允许值
	if event.Depth > rh.config.MaxAllowedDepth {
		logger.Error("Reorg depth exceeds maximum allowed",
			zap.String("chain_id", event.ChainID),
			zap.Uint64("depth", event.Depth),
			zap.Uint64("max_allowed", rh.config.MaxAllowedDepth))
		return fmt.Errorf("reorg depth %d exceeds maximum allowed %d", event.Depth, rh.config.MaxAllowedDepth)
	}

	// 调用父类的验证逻辑
	defaultHandler := &DefaultReorgHandler{
		storage:     rh.storage,
		stateMachine: rh.stateMachine,
	}
	return defaultHandler.validateReorgEvent(ctx, event)
}

// markTransactionsAsReverted 标记受影响的交易为已回滚
func (rh *AdvancedReorgHandler) markTransactionsAsReverted(ctx context.Context, event *types.ReorgEvent) error {
	defaultHandler := &DefaultReorgHandler{
		storage:     rh.storage,
		stateMachine: rh.stateMachine,
	}
	return defaultHandler.markTransactionsAsReverted(ctx, event)
}

// revertBusinessData 冲正业务数据
func (rh *AdvancedReorgHandler) revertBusinessData(ctx context.Context, event *types.ReorgEvent) error {
	defaultHandler := &DefaultReorgHandler{
		storage:     rh.storage,
		stateMachine: rh.stateMachine,
	}
	return defaultHandler.revertBusinessData(ctx, event)
}

// revertTransactionBusinessData 冲正单个交易的业务数据
func (rh *AdvancedReorgHandler) revertTransactionBusinessData(ctx context.Context, tx *types.Transaction) error {
	defaultHandler := &DefaultReorgHandler{
		storage:     rh.storage,
		stateMachine: rh.stateMachine,
	}
	return defaultHandler.revertTransactionBusinessData(ctx, tx)
}

// cleanupReorgData 清理回滚区间的数据
func (rh *AdvancedReorgHandler) cleanupReorgData(ctx context.Context, event *types.ReorgEvent) error {
	defaultHandler := &DefaultReorgHandler{
		storage:     rh.storage,
		stateMachine: rh.stateMachine,
	}
	return defaultHandler.cleanupReorgData(ctx, event)
}

// markReorgEventAsProcessed 标记回滚事件为已处理
func (rh *AdvancedReorgHandler) markReorgEventAsProcessed(ctx context.Context, event *types.ReorgEvent) error {
	defaultHandler := &DefaultReorgHandler{
		storage:     rh.storage,
		stateMachine: rh.stateMachine,
	}
	return defaultHandler.markReorgEventAsProcessed(ctx, event)
}

// GetReorgHistory 获取回滚历史
func (rh *AdvancedReorgHandler) GetReorgHistory(ctx context.Context, chainID string, limit int) ([]*types.ReorgEvent, error) {
	defaultHandler := &DefaultReorgHandler{
		storage:     rh.storage,
		stateMachine: rh.stateMachine,
	}
	return defaultHandler.GetReorgHistory(ctx, chainID, limit)
}

// GetReorgStatistics 获取回滚统计信息
func (rh *AdvancedReorgHandler) GetReorgStatistics(ctx context.Context, chainID string) (*ReorgStatistics, error) {
	defaultHandler := &DefaultReorgHandler{
		storage:     rh.storage,
		stateMachine: rh.stateMachine,
	}
	return defaultHandler.GetReorgStatistics(ctx, chainID)
}