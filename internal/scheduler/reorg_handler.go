package scheduler

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/smart-scanner/multi-chain-scanner/internal/node"
	"github.com/smart-scanner/multi-chain-scanner/pkg/logger"
	"github.com/smart-scanner/multi-chain-scanner/pkg/lock"
	"github.com/smart-scanner/multi-chain-scanner/pkg/metrics"
	"github.com/smart-scanner/multi-chain-scanner/pkg/types"
	"go.uber.org/zap"
)

// ReorgHandler 回滚处理器
type ReorgHandler struct {
	chainID      string
	client       node.ChainClient
	watermark    *WatermarkManager
	lockManager  *lock.LockManager
	processing   bool
	mu           sync.Mutex
}

// NewReorgHandler 创建回滚处理器
func NewReorgHandler(
	chainID string,
	client node.ChainClient,
	watermark *WatermarkManager,
	lockManager *lock.LockManager,
) (*ReorgHandler, error) {
	if client == nil {
		return nil, fmt.Errorf("client cannot be nil")
	}
	if watermark == nil {
		return nil, fmt.Errorf("watermark manager cannot be nil")
	}
	if lockManager == nil {
		return nil, fmt.Errorf("lock manager cannot be nil")
	}

	return &ReorgHandler{
		chainID:     chainID,
		client:      client,
		watermark:   watermark,
		lockManager: lockManager,
	}, nil
}

// HandleReorg 处理回滚
func (h *ReorgHandler) HandleReorg(ctx context.Context, oldHeight, newHeight uint64) error {
	h.mu.Lock()
	if h.processing {
		h.mu.Unlock()
		return fmt.Errorf("reorg processing already in progress for chain %s", h.chainID)
	}
	h.processing = true
	h.mu.Unlock()

	defer func() {
		h.mu.Lock()
		h.processing = false
		h.mu.Unlock()
	}()

	logger.Warn("Starting reorg handling",
		zap.String("chain_id", h.chainID),
		zap.Uint64("old_height", oldHeight),
		zap.Uint64("new_height", newHeight))

	// 获取分布式锁
	reorgLock := h.lockManager.CreateLockWithTTL(
		lock.GenerateReorgLockKey(h.chainID),
		5*time.Minute,
	)

	if err := reorgLock.Lock(ctx); err != nil {
		return fmt.Errorf("failed to acquire reorg lock: %w", err)
	}
	defer reorgLock.Unlock(ctx)

	// 记录回滚事件
	reorgEvent := &types.ReorgEvent{
		ChainID:        h.chainID,
		DetectedAt:     time.Now(),
		OldBlockNumber: oldHeight,
		NewBlockNumber: newHeight,
		Depth:          oldHeight - newHeight,
		Processed:      false,
	}

	// 记录指标
	metrics.RecordReorgEventDetected(h.chainID)
	metrics.RecordReorgDepth(h.chainID, float64(reorgEvent.Depth))

	// 1. 标记回滚区间的交易为已回滚
	if err := h.markRevertedTransactions(ctx, newHeight, oldHeight); err != nil {
		logger.Error("Failed to mark reverted transactions",
			zap.String("chain_id", h.chainID),
			zap.Uint64("new_height", newHeight),
			zap.Uint64("old_height", oldHeight),
			zap.Error(err))
		return fmt.Errorf("failed to mark reverted transactions: %w", err)
	}

	// 2. 冲正业务数据
	if err := h.revertBusinessData(ctx, newHeight, oldHeight); err != nil {
		logger.Error("Failed to revert business data",
			zap.String("chain_id", h.chainID),
			zap.Uint64("new_height", newHeight),
			zap.Uint64("old_height", oldHeight),
			zap.Error(err))
		return fmt.Errorf("failed to revert business data: %w", err)
	}

	// 3. 重置扫链水位
	if err := h.watermark.ResetToHeight(newHeight); err != nil {
		logger.Error("Failed to reset watermark",
			zap.String("chain_id", h.chainID),
			zap.Uint64("new_height", newHeight),
			zap.Error(err))
		return fmt.Errorf("failed to reset watermark: %w", err)
	}

	// 4. 标记回滚事件为已处理
	reorgEvent.Processed = true

	// 5. 发送回滚事件到消息队列
	if err := h.publishReorgEvent(ctx, reorgEvent); err != nil {
		logger.Error("Failed to publish reorg event",
			zap.String("chain_id", h.chainID),
			zap.Error(err))
		// 不返回错误，因为回滚已经处理完成
	}

	// 记录指标
	metrics.RecordReorgEventProcessed(h.chainID)

	logger.Info("Reorg handling completed",
		zap.String("chain_id", h.chainID),
		zap.Uint64("old_height", oldHeight),
		zap.Uint64("new_height", newHeight),
		zap.Uint64("depth", reorgEvent.Depth))

	return nil
}

// markRevertedTransactions 标记回滚的交易
func (h *ReorgHandler) markRevertedTransactions(ctx context.Context, newHeight, oldHeight uint64) error {
	logger.Info("Marking reverted transactions",
		zap.String("chain_id", h.chainID),
		zap.Uint64("new_height", newHeight),
		zap.Uint64("old_height", oldHeight))

	// TODO: 从数据库查询回滚区间的交易
	// 这里需要实现数据库查询逻辑
	// 暂时返回 nil

	return nil
}

// revertBusinessData 冲正业务数据
func (h *ReorgHandler) revertBusinessData(ctx context.Context, newHeight, oldHeight uint64) error {
	logger.Info("Reverting business data",
		zap.String("chain_id", h.chainID),
		zap.Uint64("new_height", newHeight),
		zap.Uint64("old_height", oldHeight))

	// TODO: 冲正余额、流水、订单等业务数据
	// 这里需要实现业务数据冲正逻辑
	// 暂时返回 nil

	return nil
}

// publishReorgEvent 发布回滚事件
func (h *ReorgHandler) publishReorgEvent(ctx context.Context, event *types.ReorgEvent) error {
	logger.Info("Publishing reorg event",
		zap.String("chain_id", h.chainID),
		zap.Uint64("old_block_number", event.OldBlockNumber),
		zap.Uint64("new_block_number", event.NewBlockNumber))

	// TODO: 发送回滚事件到消息队列
	// 这里需要实现消息队列发送逻辑
	// 暂时返回 nil

	return nil
}

// IsProcessing 检查是否正在处理回滚
func (h *ReorgHandler) IsProcessing() bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.processing
}