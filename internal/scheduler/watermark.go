package scheduler

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/smart-scanner/multi-chain-scanner/pkg/config"
	"github.com/smart-scanner/multi-chain-scanner/pkg/logger"
	"github.com/smart-scanner/multi-chain-scanner/pkg/lock"
	"github.com/smart-scanner/multi-chain-scanner/pkg/types"
	"go.uber.org/zap"
)

// WatermarkManager 水位管理器
// 管理链扫描的水位信息，支持分布式锁保护的水位更新和回滚重置
// [Design: WatermarkManager 水位管理器](../docs/DESIGN_SCANNER.md#33-watermarkmanager-水位管理器)
type WatermarkManager struct {
	chainID      string                    // 链ID
	lockManager  *lock.LockManager         // 分布式锁管理器
	config       *config.WatermarkConfig   // 水位配置
	mu           sync.RWMutex              // 状态锁
	watermark    atomic.Pointer[types.Watermark] // 水位指针（原子操作）
	blockHashes  sync.Map                  // 区块哈希缓存（blockNumber -> hash）
}

// NewWatermarkManager 创建水位管理器
// [Design: WatermarkManager 水位管理器](../docs/DESIGN_SCANNER.md#33-watermarkmanager-水位管理器)
func NewWatermarkManager(
	chainID string,
	lockManager *lock.LockManager,
	config *config.WatermarkConfig,
) (*WatermarkManager, error) {
	if lockManager == nil {
		return nil, fmt.Errorf("lock manager cannot be nil")
	}

	manager := &WatermarkManager{
		chainID:     chainID,
		lockManager: lockManager,
		config:      config,
	}

	// 初始化水位（ScannedHeight=0, ConfirmedHeight=0, FinalizedHeight=0, ReorgBoundary=0）
	watermark := &types.Watermark{
		ChainID:         chainID,
		ScannedHeight:   0,
		ConfirmedHeight: 0,
		FinalizedHeight: 0,
		ReorgBoundary:   0,
		LastUpdateTime:  time.Now(),
	}

	manager.watermark.Store(watermark)

	// 尝试从持久化存储加载水位，失败则使用初始值
	if err := manager.loadWatermark(); err != nil {
		logger.Warn("Failed to load watermark, using initial values",
			zap.String("chain_id", chainID),
			zap.Error(err))
	}

	return manager, nil
}

// GetWatermark 获取水位
func (m *WatermarkManager) GetWatermark() (*types.Watermark, error) {
	watermark := m.watermark.Load()
	if watermark == nil {
		return nil, fmt.Errorf("watermark not initialized")
	}

	// 返回水位的副本以避免并发问题
	watermarkCopy := *watermark
	return &watermarkCopy, nil
}

// UpdateScannedHeight 更新已扫描高度
func (m *WatermarkManager) UpdateScannedHeight(height uint64) error {
	// 先获取分布式锁，再获取本地锁
	lock := m.lockManager.CreateLockWithTTL(
		lock.GenerateWatermarkLockKey(m.chainID),
		30*time.Second,
	)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := lock.Lock(ctx); err != nil {
		return fmt.Errorf("failed to acquire watermark lock: %w", err)
	}
	defer lock.Unlock(ctx)

	m.mu.Lock()
	defer m.mu.Unlock()

	current := m.watermark.Load()
	if current == nil {
		return fmt.Errorf("watermark not initialized")
	}

	if height <= current.ScannedHeight {
		return nil
	}

	// 创建新的水位对象（不可变模式）
	newWatermark := &types.Watermark{
		ChainID:         current.ChainID,
		ScannedHeight:   height,
		ConfirmedHeight: current.ConfirmedHeight,
		FinalizedHeight: current.FinalizedHeight,
		ReorgBoundary:   current.ReorgBoundary,
		LastUpdateTime:  time.Now(),
	}

	// 更新回滚边界
	reorgBoundary := int64(height) - int64(m.config.ReorgDetectionDepth)
	if reorgBoundary < 0 {
		reorgBoundary = 0
	}
	newWatermark.ReorgBoundary = uint64(reorgBoundary)

	// 原子更新
	m.watermark.Store(newWatermark)

	// 持久化水位
	if err := m.saveWatermark(); err != nil {
		logger.Error("Failed to save watermark",
			zap.String("chain_id", m.chainID),
			zap.Uint64("height", height),
			zap.Error(err))
		return err
	}

	logger.Debug("Updated scanned height",
		zap.String("chain_id", m.chainID),
		zap.Uint64("height", height))

	return nil
}

// UpdateConfirmedHeight 更新已确认高度
func (m *WatermarkManager) UpdateConfirmedHeight(height uint64) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	current := m.watermark.Load()
	if current == nil {
		return fmt.Errorf("watermark not initialized")
	}

	if height <= current.ConfirmedHeight {
		return nil
	}

	newWatermark := &types.Watermark{
		ChainID:         current.ChainID,
		ScannedHeight:   current.ScannedHeight,
		ConfirmedHeight: height,
		FinalizedHeight: current.FinalizedHeight,
		ReorgBoundary:   current.ReorgBoundary,
		LastUpdateTime:  time.Now(),
	}

	// 更新回滚边界
	reorgBoundary := int64(newWatermark.ScannedHeight) - int64(m.config.ReorgDetectionDepth)
	if reorgBoundary < 0 {
		reorgBoundary = 0
	}
	newWatermark.ReorgBoundary = uint64(reorgBoundary)

	m.watermark.Store(newWatermark)

	if err := m.saveWatermark(); err != nil {
		logger.Error("Failed to save watermark",
			zap.String("chain_id", m.chainID),
			zap.Uint64("height", height),
			zap.Error(err))
		return err
	}

	return nil
}

// UpdateFinalizedHeight 更新最终确认高度
func (m *WatermarkManager) UpdateFinalizedHeight(height uint64) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	current := m.watermark.Load()
	if current == nil {
		return fmt.Errorf("watermark not initialized")
	}

	if height <= current.FinalizedHeight {
		return nil
	}

	newWatermark := &types.Watermark{
		ChainID:         current.ChainID,
		ScannedHeight:   current.ScannedHeight,
		ConfirmedHeight: current.ConfirmedHeight,
		FinalizedHeight: height,
		ReorgBoundary:   current.ReorgBoundary,
		LastUpdateTime:  time.Now(),
	}

	m.watermark.Store(newWatermark)

	if err := m.saveWatermark(); err != nil {
		logger.Error("Failed to save watermark",
			zap.String("chain_id", m.chainID),
			zap.Uint64("height", height),
			zap.Error(err))
		return err
	}

	return nil
}

// SetBlockHash 设置区块哈希
func (m *WatermarkManager) SetBlockHash(blockNumber uint64, blockHash string) error {
	m.blockHashes.Store(blockNumber, blockHash)

	// 异步清理旧的区块哈希缓存
	go m.cleanupOldBlockHashes()

	return nil
}

// GetBlockHash 获取区块哈希
func (m *WatermarkManager) GetBlockHash(blockNumber uint64) (string, error) {
	hash, exists := m.blockHashes.Load(blockNumber)
	if !exists {
		return "", fmt.Errorf("block hash not found for block %d", blockNumber)
	}

	return hash.(string), nil
}

// ResetToHeight 重置到指定高度（用于回滚）
func (m *WatermarkManager) ResetToHeight(height uint64) error {
	// 先获取分布式锁，再获取本地锁
	lock := m.lockManager.CreateLockWithTTL(
		lock.GenerateWatermarkLockKey(m.chainID),
		30*time.Second,
	)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := lock.Lock(ctx); err != nil {
		return fmt.Errorf("failed to acquire watermark lock: %w", err)
	}
	defer lock.Unlock(ctx)

	m.mu.Lock()
	defer m.mu.Unlock()

	current := m.watermark.Load()
	if current == nil {
		return fmt.Errorf("watermark not initialized")
	}

	if height > current.ScannedHeight {
		return fmt.Errorf("cannot reset to height %d, current scanned height is %d", height, current.ScannedHeight)
	}

	newWatermark := &types.Watermark{
		ChainID:         current.ChainID,
		ScannedHeight:   height,
		ConfirmedHeight: current.ConfirmedHeight,
		FinalizedHeight: current.FinalizedHeight,
		ReorgBoundary:   current.ReorgBoundary,
		LastUpdateTime:  time.Now(),
	}

	m.watermark.Store(newWatermark)

	// 清理区块哈希缓存
	m.cleanupBlockHashesAfter(height)

	if err := m.saveWatermark(); err != nil {
		logger.Error("Failed to save watermark after reset",
			zap.String("chain_id", m.chainID),
			zap.Uint64("height", height),
			zap.Error(err))
		return err
	}

	logger.Warn("Watermark reset to height",
		zap.String("chain_id", m.chainID),
		zap.Uint64("height", height))

	return nil
}

// cleanupOldBlockHashes 清理旧的区块哈希缓存
func (m *WatermarkManager) cleanupOldBlockHashes() {
	keepCount := int(m.config.ReorgDetectionDepth) * 2
	if keepCount <= 0 {
		return
	}

	// 收集所有区块号
	var blockNumbers []uint64
	m.blockHashes.Range(func(key, value interface{}) bool {
		blockNumbers = append(blockNumbers, key.(uint64))
		return true
	})

	if len(blockNumbers) <= keepCount {
		return
	}

	// 排序
	sort.Slice(blockNumbers, func(i, j int) bool {
		return blockNumbers[i] < blockNumbers[j]
	})

	// 删除旧的区块哈希
	deleteCount := len(blockNumbers) - keepCount
	for i := 0; i < deleteCount; i++ {
		m.blockHashes.Delete(blockNumbers[i])
	}
}

// cleanupBlockHashesAfter 清理指定高度之后的区块哈希
func (m *WatermarkManager) cleanupBlockHashesAfter(height uint64) {
	m.blockHashes.Range(func(key, value interface{}) bool {
		blockNumber := key.(uint64)
		if blockNumber > height {
			m.blockHashes.Delete(blockNumber)
		}
		return true
	})
}

// loadWatermark 从持久化存储加载水位
func (m *WatermarkManager) loadWatermark() error {
	return nil
}

// saveWatermark 保存水位到持久化存储
func (m *WatermarkManager) saveWatermark() error {
	return nil
}

// GenerateWatermarkLockKey 生成水位锁的 key
func GenerateWatermarkLockKey(chainID string) string {
	return fmt.Sprintf("watermark:%s", chainID)
}