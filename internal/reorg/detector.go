package reorg

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/smart-scanner/multi-chain-scanner/internal/storage"
	"github.com/smart-scanner/multi-chain-scanner/internal/scheduler"
	"github.com/smart-scanner/multi-chain-scanner/pkg/logger"
	"github.com/smart-scanner/multi-chain-scanner/pkg/types"
	"go.uber.org/zap"
)

// Detector 回滚检测器接口
// [Design: ReorgDetector 回滚检测器](../docs/DESIGN_SCANNER.md#71-reorgdetector-回滚检测器)
type Detector interface {
	Start(ctx context.Context) error
	Stop() error
	DetectReorg(ctx context.Context, chainID string) (*types.ReorgEvent, error)
	GetReorgHistory(ctx context.Context, chainID string, limit int) ([]*types.ReorgEvent, error)
	GetStatistics(ctx context.Context, chainID string) (*DetectorStatistics, error)
}

// ReorgDetector 回滚检测器实现
// 定期检测链回滚事件，支持自动处理和手动处理两种模式
// [Design: ReorgDetector 回滚检测器](../docs/DESIGN_SCANNER.md#71-reorgdetector-回滚检测器)
type ReorgDetector struct {
	config          *DetectorConfig                    // 检测器配置
	storage         storage.Storage                    // 存储层
	scannerManager  *scheduler.SchedulerManager        // 调度器管理器
	
	running         bool                               // 是否运行中
	mu              sync.RWMutex                       // 状态锁
	wg              sync.WaitGroup                     // 等待组
	
	detectionState  map[string]*ChainDetectionState    // 检测状态（chainID -> ChainDetectionState）
	statistics      map[string]*DetectorStatistics     // 统计信息（chainID -> DetectorStatistics）
}

// DetectorConfig 检测器配置
type DetectorConfig struct {
	// 检测间隔
	CheckInterval time.Duration
	
	// 回滚深度阈值
	ReorgDepthThreshold uint64
	
	// 是否启用自动处理
	EnableAutoProcessing bool
	
	// 最大重试次数
	MaxRetries int
	
	// 重试间隔
	RetryInterval time.Duration
	
	// 检测超时时间
	DetectionTimeout time.Duration
}

// ChainDetectionState 链检测状态
type ChainDetectionState struct {
	ChainID             string
	LastCheckTime       time.Time
	LastCheckedHeight   uint64
	LastConfirmedHash   string
	ConsecutiveFailures int
	IsProcessingReorg   bool
}

// DetectorStatistics 检测器统计信息
type DetectorStatistics struct {
	ChainID              string     `json:"chain_id"`
	TotalChecks          int64      `json:"total_checks"`
	SuccessfulChecks     int64      `json:"successful_checks"`
	FailedChecks         int64      `json:"failed_checks"`
	ReorgsDetected       int64      `json:"reorgs_detected"`
	TotalReorgDepth      uint64     `json:"total_reorg_depth"`
	MaxReorgDepth        uint64     `json:"max_reorg_depth"`
	AverageReorgDepth    float64    `json:"average_reorg_depth"`
	LastCheckTime        time.Time  `json:"last_check_time"`
	LastReorgTime        *time.Time `json:"last_reorg_time,omitempty"`
	ConsecutiveFailures  int        `json:"consecutive_failures"`
}

// NewReorgDetector 创建回滚检测器
func NewReorgDetector(
	config *DetectorConfig,
	storage storage.Storage,
	scannerManager *scheduler.SchedulerManager,
) (*ReorgDetector, error) {
	if config == nil {
		config = &DetectorConfig{
			CheckInterval:        10 * time.Second,
			ReorgDepthThreshold:  1,
			EnableAutoProcessing: true,
			MaxRetries:           3,
			RetryInterval:        5 * time.Second,
			DetectionTimeout:     30 * time.Second,
		}
	}

	if storage == nil {
		return nil, fmt.Errorf("storage cannot be nil")
	}

	if scannerManager == nil {
		return nil, fmt.Errorf("scanner manager cannot be nil")
	}

	return &ReorgDetector{
		config:         config,
		storage:        storage,
		scannerManager: scannerManager,
		detectionState: make(map[string]*ChainDetectionState),
		statistics:     make(map[string]*DetectorStatistics),
	}, nil
}

// Start 启动检测器
func (d *ReorgDetector) Start(ctx context.Context) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.running {
		return fmt.Errorf("detector is already running")
	}

	logger.Info("Starting reorg detector")

	d.running = true

	// 获取所有活跃的链
	chains := d.scannerManager.GetActiveChains()

	// 为每个链初始化检测状态
	for _, chainID := range chains {
		d.detectionState[chainID] = &ChainDetectionState{
			ChainID:           chainID,
			LastCheckTime:     time.Now(),
			IsProcessingReorg: false,
		}
		d.statistics[chainID] = &DetectorStatistics{
			ChainID: chainID,
		}
	}

	// 启动检测协程
	d.wg.Add(1)
	go d.detectionLoop(ctx)

	logger.Info("Reorg detector started successfully")
	return nil
}

// Stop 停止检测器
func (d *ReorgDetector) Stop() error {
	d.mu.Lock()
	defer d.mu.Unlock()

	if !d.running {
		return nil
	}

	logger.Info("Stopping reorg detector")

	d.running = false
	d.wg.Wait()

	logger.Info("Reorg detector stopped successfully")
	return nil
}

// detectionLoop 检测循环
func (d *ReorgDetector) detectionLoop(ctx context.Context) {
	defer d.wg.Done()

	ticker := time.NewTicker(d.config.CheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			logger.Info("Detection loop stopped")
			return
		case <-ticker.C:
			d.checkAllChains(ctx)
		}
	}
}

// checkAllChains 检查所有链
func (d *ReorgDetector) checkAllChains(ctx context.Context) {
	d.mu.RLock()
	chains := make([]string, 0, len(d.detectionState))
	for chainID := range d.detectionState {
		chains = append(chains, chainID)
	}
	d.mu.RUnlock()

	// 并发检查每个链
	var wg sync.WaitGroup
	for _, chainID := range chains {
		wg.Add(1)
		go func(cid string) {
			defer wg.Done()
			d.checkChain(ctx, cid)
		}(chainID)
	}

	wg.Wait()
}

// checkChain 检查单个链
func (d *ReorgDetector) checkChain(ctx context.Context, chainID string) {
	d.mu.Lock()
	state, exists := d.detectionState[chainID]
	if !exists {
		d.mu.Unlock()
		logger.Warn("Chain not found in detection state",
			zap.String("chain_id", chainID))
		return
	}
	
	// 如果正在处理回滚，跳过检测
	if state.IsProcessingReorg {
		d.mu.Unlock()
		logger.Debug("Skipping reorg detection, already processing",
			zap.String("chain_id", chainID))
		return
	}
	d.mu.Unlock()

	// 设置检测超时
	timeoutCtx, cancel := context.WithTimeout(ctx, d.config.DetectionTimeout)
	defer cancel()

	// 执行检测
	reorgEvent, err := d.DetectReorg(timeoutCtx, chainID)
	if err != nil {
		d.handleDetectionError(chainID, err)
		return
	}

	// 如果检测到回滚，处理回滚事件
	if reorgEvent != nil {
		d.handleReorgEvent(ctx, reorgEvent)
	}
}

// DetectReorg 检测回滚
func (d *ReorgDetector) DetectReorg(ctx context.Context, chainID string) (*types.ReorgEvent, error) {
	d.mu.Lock()
	state := d.detectionState[chainID]
	stats := d.statistics[chainID]
	d.mu.Unlock()

	// 更新统计信息
	stats.TotalChecks++
	state.LastCheckTime = time.Now()

	logger.Debug("Detecting reorg",
		zap.String("chain_id", chainID),
		zap.Time("check_time", state.LastCheckTime))

	// 1. 获取当前链的最新区块
	scanner, err := d.scannerManager.GetScanner(chainID)
	if err != nil {
		return nil, fmt.Errorf("scanner not found for chain %s", chainID)
	}

	// 获取最新确认的区块高度
	latestHeight, err := scanner.GetLatestHeight(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get latest height: %w", err)
	}

	// 2. 获取本地存储的最新区块
	localBlock, err := d.storage.GetLatestBlock(ctx, chainID)
	if err != nil {
		// 如果没有本地区块，说明是首次运行，不算错误
		if err.Error() == "no blocks found for chain "+chainID {
			logger.Debug("No local blocks found, skipping reorg detection",
				zap.String("chain_id", chainID))
			stats.SuccessfulChecks++
			return nil, nil
		}
		return nil, fmt.Errorf("failed to get local block: %w", err)
	}

	// 3. 检查高度是否匹配
	if latestHeight < localBlock.Number {
		// 链高度低于本地高度，可能是节点同步问题
		logger.Warn("Chain height lower than local height",
			zap.String("chain_id", chainID),
			zap.Uint64("chain_height", latestHeight),
			zap.Uint64("local_height", localBlock.Number))
		stats.SuccessfulChecks++
		return nil, nil
	}

	// 4. 检查区块哈希是否匹配
	if latestHeight == localBlock.Number {
		// 高度相同，检查哈希
		chainBlock, err := scanner.GetBlock(ctx, latestHeight)
		if err != nil {
			return nil, fmt.Errorf("failed to get chain block: %w", err)
		}

		if chainBlock.Hash == localBlock.Hash {
			// 哈希匹配，没有回滚
			stats.SuccessfulChecks++
			state.ConsecutiveFailures = 0
			return nil, nil
		}

		// 哈希不匹配，检测到回滚
		return d.createReorgEvent(chainID, localBlock, chainBlock, 1)
	}

	// 5. 高度不同，需要找到分叉点
	reorgEvent, err := d.findForkPoint(ctx, chainID, localBlock, latestHeight)
	if err != nil {
		return nil, fmt.Errorf("failed to find fork point: %w", err)
	}

	if reorgEvent != nil {
		stats.ReorgsDetected++
		stats.LastReorgTime = &reorgEvent.DetectedAt
	}

	stats.SuccessfulChecks++
	state.ConsecutiveFailures = 0

	return reorgEvent, nil
}

// findForkPoint 查找分叉点
func (d *ReorgDetector) findForkPoint(
	ctx context.Context,
	chainID string,
	localBlock *types.Block,
	chainHeight uint64,
) (*types.ReorgEvent, error) {
	scanner, err := d.scannerManager.GetScanner(chainID)
	if err != nil {
		return nil, fmt.Errorf("scanner not found for chain %s", chainID)
	}

	// 从本地区块开始向下查找分叉点
	currentLocal := localBlock
	depth := uint64(0)
	maxDepth := uint64(100) // 最大查找深度

	for depth < maxDepth {
		// 获取链上对应高度的区块
		chainBlock, err := scanner.GetBlock(ctx, currentLocal.Number)
		if err != nil {
			return nil, fmt.Errorf("failed to get chain block at height %d: %w", currentLocal.Number, err)
		}

		// 检查哈希是否匹配
		if chainBlock.Hash == currentLocal.Hash {
			// 找到分叉点，当前高度的下一个区块就是分叉开始的地方
			if depth >= d.config.ReorgDepthThreshold {
				// 获取新的链上区块
				newChainBlock, err := scanner.GetBlock(ctx, currentLocal.Number+1)
				if err != nil {
					return nil, fmt.Errorf("failed to get new chain block: %w", err)
				}

				return d.createReorgEvent(chainID, currentLocal, newChainBlock, depth)
			}
			// 回滚深度小于阈值，忽略
			return nil, nil
		}

		// 哈希不匹配，继续向下查找
		if currentLocal.Number == 0 {
			// 已经到达创世区块，仍然不匹配，这是异常情况
			logger.Error("Genesis block hash mismatch",
				zap.String("chain_id", chainID),
				zap.String("local_hash", currentLocal.Hash),
				zap.String("chain_hash", chainBlock.Hash))
			return nil, fmt.Errorf("genesis block hash mismatch")
		}

		// 获取本地的前一个区块
		prevLocalBlock, err := d.storage.GetBlock(ctx, chainID, currentLocal.Number-1)
		if err != nil {
			return nil, fmt.Errorf("failed to get previous local block: %w", err)
		}

		currentLocal = prevLocalBlock
		depth++
	}

	// 超过最大查找深度
	logger.Warn("Exceeded maximum fork point search depth",
		zap.String("chain_id", chainID),
		zap.Uint64("max_depth", maxDepth))

	return nil, fmt.Errorf("exceeded maximum fork point search depth")
}

// createReorgEvent 创建回滚事件
func (d *ReorgDetector) createReorgEvent(
	chainID string,
	oldBlock *types.Block,
	newBlock *types.Block,
	depth uint64,
) (*types.ReorgEvent, error) {
	event := &types.ReorgEvent{
		ChainID:        chainID,
		DetectedAt:     time.Now(),
		OldBlockNumber: oldBlock.Number,
		OldBlockHash:   oldBlock.Hash,
		NewBlockNumber: newBlock.Number,
		NewBlockHash:   newBlock.Hash,
		Depth:          depth,
		Processed:      false,
	}

	logger.Warn("Reorg detected",
		zap.String("chain_id", chainID),
		zap.Uint64("old_block", oldBlock.Number),
		zap.Uint64("new_block", newBlock.Number),
		zap.Uint64("depth", depth),
		zap.String("old_hash", oldBlock.Hash),
		zap.String("new_hash", newBlock.Hash))

	// 更新统计信息
	d.mu.Lock()
	stats := d.statistics[chainID]
	stats.TotalReorgDepth += depth
	if depth > stats.MaxReorgDepth {
		stats.MaxReorgDepth = depth
	}
	stats.AverageReorgDepth = float64(stats.TotalReorgDepth) / float64(stats.ReorgsDetected)
	d.mu.Unlock()

	return event, nil
}

// handleDetectionError 处理检测错误
func (d *ReorgDetector) handleDetectionError(chainID string, err error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	state := d.detectionState[chainID]
	stats := d.statistics[chainID]

	stats.FailedChecks++
	state.ConsecutiveFailures++

	logger.Error("Reorg detection failed",
		zap.String("chain_id", chainID),
		zap.Int("consecutive_failures", state.ConsecutiveFailures),
		zap.Error(err))

	// 如果连续失败次数过多，发出警报
	if state.ConsecutiveFailures > 10 {
		logger.Error("Too many consecutive detection failures",
			zap.String("chain_id", chainID),
			zap.Int("failures", state.ConsecutiveFailures))
		// 这里可以添加警报逻辑
	}
}

// handleReorgEvent 处理回滚事件
func (d *ReorgDetector) handleReorgEvent(ctx context.Context, event *types.ReorgEvent) {
	logger.Warn("Handling reorg event",
		zap.String("chain_id", event.ChainID),
		zap.Uint64("depth", event.Depth))

	// 标记为正在处理回滚
	d.mu.Lock()
	state := d.detectionState[event.ChainID]
	state.IsProcessingReorg = true
	d.mu.Unlock()

	defer func() {
		d.mu.Lock()
		state.IsProcessingReorg = false
		d.mu.Unlock()
	}()

	// 保存回滚事件
	if err := d.storage.SaveReorgEvent(ctx, event); err != nil {
		logger.Error("Failed to save reorg event",
			zap.String("chain_id", event.ChainID),
			zap.Error(err))
		return
	}

	// 如果启用自动处理，执行自动处理逻辑
	if d.config.EnableAutoProcessing {
		if err := d.autoProcessReorg(ctx, event); err != nil {
			logger.Error("Failed to auto process reorg",
				zap.String("chain_id", event.ChainID),
				zap.Error(err))
		}
	}
}

// autoProcessReorg 自动处理回滚
func (d *ReorgDetector) autoProcessReorg(ctx context.Context, event *types.ReorgEvent) error {
	logger.Info("Auto processing reorg",
		zap.String("chain_id", event.ChainID),
		zap.Uint64("depth", event.Depth))

	// 1. 重置扫链水位到安全高度
	safeHeight := event.OldBlockNumber
	if safeHeight > 0 {
		safeHeight--
	}

	scanner, err := d.scannerManager.GetScanner(event.ChainID)
	if err != nil {
		return fmt.Errorf("scanner not found for chain %s", event.ChainID)
	}

	// 重置水位
	if err := scanner.ResetWatermark(ctx, safeHeight); err != nil {
		return fmt.Errorf("failed to reset watermark: %w", err)
	}

	// 2. 删除受影响的数据
	fromBlock := event.NewBlockNumber
	toBlock := event.OldBlockNumber

	if err := d.storage.DeleteBlocks(ctx, event.ChainID, fromBlock, toBlock); err != nil {
		return fmt.Errorf("failed to delete blocks: %w", err)
	}

	if err := d.storage.DeleteLogs(ctx, event.ChainID, fromBlock, toBlock); err != nil {
		return fmt.Errorf("failed to delete logs: %w", err)
	}

	// 3. 标记交易为已回滚
	if err := d.storage.MarkTransactionsAsReverted(ctx, event.ChainID, fromBlock, toBlock); err != nil {
		return fmt.Errorf("failed to mark transactions as reverted: %w", err)
	}

	// 4. 标记回滚事件为已处理
	event.Processed = true

	if err := d.storage.SaveReorgEvent(ctx, event); err != nil {
		return fmt.Errorf("failed to save reorg event: %w", err)
	}

	logger.Info("Auto processed reorg successfully",
		zap.String("chain_id", event.ChainID),
		zap.Uint64("safe_height", safeHeight))

	return nil
}

// GetReorgHistory 获取回滚历史
func (d *ReorgDetector) GetReorgHistory(ctx context.Context, chainID string, limit int) ([]*types.ReorgEvent, error) {
	return d.storage.GetReorgEvents(ctx, chainID, limit, 0)
}

// GetStatistics 获取统计信息
func (d *ReorgDetector) GetStatistics(ctx context.Context, chainID string) (*DetectorStatistics, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	stats, exists := d.statistics[chainID]
	if !exists {
		return nil, fmt.Errorf("statistics not found for chain %s", chainID)
	}

	// 返回统计信息的副本
	statsCopy := *stats
	return &statsCopy, nil
}

// GetAllStatistics 获取所有链的统计信息
func (d *ReorgDetector) GetAllStatistics(ctx context.Context) (map[string]*DetectorStatistics, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	result := make(map[string]*DetectorStatistics)
	for chainID, stats := range d.statistics {
		statsCopy := *stats
		result[chainID] = &statsCopy
	}

	return result, nil
}