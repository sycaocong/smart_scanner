package scheduler

import (
	"context"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/smart-scanner/multi-chain-scanner/internal/node"
	"github.com/smart-scanner/multi-chain-scanner/pkg/config"
	"github.com/smart-scanner/multi-chain-scanner/pkg/lock"
	"github.com/smart-scanner/multi-chain-scanner/pkg/logger"
	"github.com/smart-scanner/multi-chain-scanner/pkg/metrics"
	"github.com/smart-scanner/multi-chain-scanner/pkg/types"
	"github.com/smart-scanner/multi-chain-scanner/pkg/util"
	"go.uber.org/zap"
)

// Scanner 扫链器接口
// [Design: ChainScanner 链扫描器](../docs/DESIGN_SCANNER.md#31-chainscanner-链扫描器)
type Scanner interface {
	Start(ctx context.Context) error
	Stop() error
	GetStatus(chainID string) (*ScannerStatus, error)
	GetAllStatus() map[string]*ScannerStatus
}

// ScannerStatus 扫链器状态
// [Design: ChainScanner 链扫描器](../docs/DESIGN_SCANNER.md#31-chainscanner-链扫描器)
type ScannerStatus struct {
	ChainID           string    `json:"chain_id"`             // 链ID
	Running           bool      `json:"running"`             // 是否运行中
	CurrentHeight     uint64    `json:"current_height"`      // 当前链高度
	ScannedHeight     uint64    `json:"scanned_height"`      // 已扫描高度
	ConfirmedHeight   uint64    `json:"confirmed_height"`    // 已确认高度
	FinalizedHeight   uint64    `json:"finalized_height"`    // 最终确认高度
	ReorgBoundary     uint64    `json:"reorg_boundary"`      // 回滚边界
	BlockLag          uint64    `json:"block_lag"`           // 区块滞后数
	BlocksPerSecond   float64   `json:"blocks_per_second"`   // 扫描速度(块/秒)
	ActiveWorkers     int       `json:"active_workers"`      // 活跃工作线程数
	PendingTasks      int       `json:"pending_tasks"`       // 待处理任务数
	ErrorCount        uint64    `json:"error_count"`         // 错误计数
	LastScanTime      time.Time `json:"last_scan_time"`      // 最后扫描时间
	LastReorgTime     time.Time `json:"last_reorg_time"`     // 最后回滚时间
	ReorgCount        uint64    `json:"reorg_count"`         // 回滚计数
}

// ChainScanner 链扫描器
// 负责单链的区块扫描任务调度和执行，是链扫描的核心组件
// [Design: ChainScanner 链扫描器](../docs/DESIGN_SCANNER.md#31-chainscanner-链扫描器)
type ChainScanner struct {
	chainID        string                // 链ID
	client         node.ChainClient      // 链客户端
	config         *config.ChainConfig   // 链配置
	scannerConfig  *config.ScannerConfig // 扫描器配置
	lockManager    *lock.LockManager     // 分布式锁管理器
	watermark      *WatermarkManager     // 水位管理器
	taskQueue      chan *types.ScanTask  // 任务队列
	resultQueue    chan *types.ScanResult // 结果队列
	workers        []*Worker             // 工作线程列表
	sharding       *ShardingManager      // 分片管理器
	reorgHandler   *ReorgHandler         // 回滚处理器
	
	mu             sync.RWMutex          // 状态锁
	status         *ScannerStatus        // 扫描器状态
	ctx            context.Context       // 上下文
	cancel         context.CancelFunc    // 取消函数
	wg             sync.WaitGroup        // 等待组
	
	lastScanHeight uint64                // 上次扫描高度（用于计算扫描速度）
	scanRate       float64               // 当前扫描速度（指数移动平均）
}

// NewChainScanner 创建链扫描器
// [Design: ChainScanner 链扫描器](../docs/DESIGN_SCANNER.md#31-chainscanner-链扫描器)
func NewChainScanner(
	chainID string,
	client node.ChainClient,
	cfg *config.ChainConfig,
	scannerCfg *config.ScannerConfig,
	lockManager *lock.LockManager,
) (*ChainScanner, error) {
	if client == nil {
		return nil, fmt.Errorf("client cannot be nil")
	}

	ctx, cancel := context.WithCancel(context.Background())

	scanner := &ChainScanner{
		chainID:       chainID,
		client:        client,
		config:        cfg,
		scannerConfig: scannerCfg,
		lockManager:   lockManager,
		taskQueue:     make(chan *types.ScanTask, scannerCfg.Concurrency.MaxWorkersPerChain*10),
		resultQueue:   make(chan *types.ScanResult, scannerCfg.Concurrency.MaxWorkersPerChain*10),
		ctx:           ctx,
		cancel:        cancel,
		status: &ScannerStatus{
			ChainID: chainID,
			Running: false,
		},
	}

	// 初始化水位管理器 [Design: WatermarkManager 水位管理器](../docs/DESIGN_SCANNER.md#33-watermarkmanager-水位管理器)
	watermark, err := NewWatermarkManager(chainID, lockManager, &scannerCfg.Watermark)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("failed to create watermark manager: %w", err)
	}
	scanner.watermark = watermark

	// 初始化分片管理器 [Design: ShardingManager 分片管理器](../docs/DESIGN_SCANNER.md#34-shardingmanager-分片管理器)
	if scannerCfg.Sharding.Enabled {
		sharding, err := NewShardingManager(chainID, &scannerCfg.Sharding)
		if err != nil {
			cancel()
			return nil, fmt.Errorf("failed to create sharding manager: %w", err)
		}
		scanner.sharding = sharding
	}

	// 初始化回滚处理器 [Design: ReorgHandler](../docs/DESIGN_SCANNER.md#71-reorgdetector-回滚检测器)
	reorgHandler, err := NewReorgHandler(chainID, client, watermark, lockManager)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("failed to create reorg handler: %w", err)
	}
	scanner.reorgHandler = reorgHandler

	// 初始化工作线程 [Design: Worker 工作线程](../docs/DESIGN_SCANNER.md#35-worker-工作线程)
	scanner.initWorkers()

	return scanner, nil
}

// initWorkers 初始化工作线程
func (s *ChainScanner) initWorkers() {
	workerCount := s.scannerConfig.Concurrency.MaxWorkersPerChain
	s.workers = make([]*Worker, 0, workerCount)

	for i := 0; i < workerCount; i++ {
		worker := NewWorker(
			fmt.Sprintf("%s-worker-%d", s.chainID, i),
			s.client,
			s.taskQueue,
			s.resultQueue,
			&s.scannerConfig.Concurrency,
		)
		s.workers = append(s.workers, worker)
	}
}

// Start 启动扫描器
// 启动工作线程、结果处理器、任务调度器、水位监控和回滚检测
// [Design: ChainScanner 链扫描器](../docs/DESIGN_SCANNER.md#31-chainscanner-链扫描器)
func (s *ChainScanner) Start(ctx context.Context) error {
	s.mu.Lock()
	if s.status.Running {
		s.mu.Unlock()
		return fmt.Errorf("scanner is already running for chain %s", s.chainID)
	}
	s.status.Running = true
	s.mu.Unlock()

	logger.Info("Starting chain scanner",
		zap.String("chain_id", s.chainID),
		zap.Int("workers", len(s.workers)))

	// 启动工作线程 [Design: Worker 工作线程](../docs/DESIGN_SCANNER.md#35-worker-工作线程)
	for _, worker := range s.workers {
		s.wg.Add(1)
		go func(w *Worker) {
			defer s.wg.Done()
			w.Start(s.ctx)
		}(worker)
	}

	// 启动多个结果处理器（并发处理）
	resultWorkerCount := util.MinInt(len(s.workers), 4)
	for i := 0; i < resultWorkerCount; i++ {
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			s.processResults()
		}()
	}

	// 启动任务调度器 [Design: scheduleTasks](../docs/DESIGN_SCANNER.md#31-chainscanner-链扫描器)
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		s.scheduleTasks()
	}()

	// 启动水位监控 [Design: WatermarkManager 水位管理器](../docs/DESIGN_SCANNER.md#33-watermarkmanager-水位管理器)
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		s.monitorWatermark()
	}()

	// 启动回滚检测 [Design: ReorgDetector 回滚检测器](../docs/DESIGN_SCANNER.md#71-reorgdetector-回滚检测器)
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		s.detectReorg()
	}()

	return nil
}

// Stop 停止扫描器
// [Design: ChainScanner 链扫描器](../docs/DESIGN_SCANNER.md#31-chainscanner-链扫描器)
func (s *ChainScanner) Stop() error {
	s.mu.Lock()
	if !s.status.Running {
		s.mu.Unlock()
		return nil
	}
	s.status.Running = false
	s.mu.Unlock()

	logger.Info("Stopping chain scanner", zap.String("chain_id", s.chainID))

	// 取消上下文，通知所有协程停止
	s.cancel()

	// 关闭任务队列，阻止新任务入队
	close(s.taskQueue)

	// 等待所有工作线程完成
	s.wg.Wait()

	return nil
}

// GetStatus 获取状态
func (s *ChainScanner) GetStatus(chainID string) (*ScannerStatus, error) {
	if chainID != s.chainID {
		return nil, fmt.Errorf("chain ID mismatch: expected %s, got %s", s.chainID, chainID)
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	statusCopy := *s.status
	return &statusCopy, nil
}

// scheduleTasks 调度任务
func (s *ChainScanner) scheduleTasks() {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
			s.generateTasks()
		}
	}
}

// generateTasks 生成扫描任务
func (s *ChainScanner) generateTasks() {
	// 获取当前水位
	watermark, err := s.watermark.GetWatermark()
	if err != nil {
		logger.Error("Failed to get watermark",
			zap.String("chain_id", s.chainID),
			zap.Error(err))
		return
	}

	// 获取当前链高度
	currentHeight, err := s.client.GetCurrentHeight(s.ctx)
	if err != nil {
		logger.Error("Failed to get current height",
			zap.String("chain_id", s.chainID),
			zap.Error(err))
		return
	}

	// 获取最终确认高度
	finalizedHeight, err := s.client.GetFinalizedHeight(s.ctx)
	if err != nil {
		logger.Error("Failed to get finalized height",
			zap.String("chain_id", s.chainID),
			zap.Error(err))
		return
	}

	// 更新状态
	s.updateStatus(currentHeight, finalizedHeight, watermark)

	// 计算需要扫描的区块范围
	startBlock := watermark.ScannedHeight + 1
	endBlock := util.MinUint64(finalizedHeight, currentHeight)

	if startBlock > endBlock {
		return
	}

	// 背压控制：如果队列中任务太多，减少生成数量
	maxQueueSize := cap(s.taskQueue)
	queuePressure := float64(len(s.taskQueue)) / float64(maxQueueSize)
	
	var batchSize uint64
	if queuePressure > 0.8 {
		batchSize = uint64(s.scannerConfig.Concurrency.BatchSize / 4)
	} else if queuePressure > 0.5 {
		batchSize = uint64(s.scannerConfig.Concurrency.BatchSize / 2)
	} else {
		batchSize = uint64(s.scannerConfig.Concurrency.BatchSize)
	}

	if batchSize < 1 {
		batchSize = 1
	}

	// 生成任务
	for blockNumber := startBlock; blockNumber <= endBlock; blockNumber += batchSize {
		taskEndBlock := util.MinUint64(blockNumber+batchSize-1, endBlock)

		task := &types.ScanTask{
			ChainID:    s.chainID,
			StartBlock: blockNumber,
			EndBlock:   taskEndBlock,
			Priority:   1,
		}

		// 如果启用了分片，计算分片ID
		if s.sharding != nil {
			task.ShardID = s.sharding.CalculateShard(blockNumber)
		}

		// 带超时的入队，避免阻塞
		select {
		case s.taskQueue <- task:
			metrics.SetPendingTasks(s.chainID, float64(len(s.taskQueue)))
		case <-time.After(100 * time.Millisecond):
			logger.Warn("Task queue is full, skipping task",
				zap.String("chain_id", s.chainID),
				zap.Uint64("start_block", blockNumber),
				zap.Uint64("end_block", taskEndBlock),
				zap.Float64("queue_pressure", queuePressure))
			return
		case <-s.ctx.Done():
			return
		}
	}
}

// processResults 处理扫描结果
func (s *ChainScanner) processResults() {
	for result := range s.resultQueue {
		if result.Success {
			s.handleSuccessResult(result)
		} else {
			s.handleFailureResult(result)
		}
	}
}

// handleSuccessResult 处理成功结果
func (s *ChainScanner) handleSuccessResult(result *types.ScanResult) {
	logger.Info("Scan task completed successfully",
		zap.String("chain_id", s.chainID),
		zap.Uint64("start_block", result.StartBlock),
		zap.Uint64("end_block", result.EndBlock),
		zap.Int("blocks_scanned", result.BlocksScanned),
		zap.Duration("duration", result.Duration))

	// 更新水位
	if err := s.watermark.UpdateScannedHeight(result.EndBlock); err != nil {
		logger.Error("Failed to update scanned height",
			zap.String("chain_id", s.chainID),
			zap.Uint64("height", result.EndBlock),
			zap.Error(err))
	}

	// 记录指标
	metrics.RecordBlockScanned(s.chainID)
	for i := 0; i < result.TxsFound; i++ {
		metrics.RecordTransactionProcessed(s.chainID, "success")
	}

	// 更新状态
	s.mu.Lock()
	s.status.ScannedHeight = result.EndBlock
	s.status.LastScanTime = time.Now()
	s.mu.Unlock()
}

// handleFailureResult 处理失败结果
func (s *ChainScanner) handleFailureResult(result *types.ScanResult) {
	logger.Error("Scan task failed",
		zap.String("chain_id", s.chainID),
		zap.Uint64("start_block", result.StartBlock),
		zap.Uint64("end_block", result.EndBlock),
		zap.Error(result.Error))

	// 记录错误指标
	metrics.RecordScanError(s.chainID, "task_failure")

	// 更新状态
	s.mu.Lock()
	s.status.ErrorCount++
	s.mu.Unlock()

	// 如果重试次数未达到上限，重新入队
	if result.Task.RetryCount < s.scannerConfig.Concurrency.MaxRetries {
		result.Task.RetryCount++
		
		// 指数退避重试
		retryDelay := s.exponentialBackoff(result.Task.RetryCount)
		
		// 异步重试，避免阻塞结果处理
		go func(task *types.ScanTask, delay time.Duration) {
			time.Sleep(delay)
			select {
			case s.taskQueue <- task:
				logger.Info("Retry task scheduled",
					zap.String("chain_id", s.chainID),
					zap.Uint64("start_block", task.StartBlock),
					zap.Int("retry_count", task.RetryCount),
					zap.Duration("delay", delay))
			case <-s.ctx.Done():
				logger.Debug("Retry canceled due to context done",
					zap.String("chain_id", s.chainID),
					zap.Uint64("start_block", task.StartBlock))
			}
		}(result.Task, retryDelay)
	} else {
		// 超过重试次数，记录到死信队列
		logger.Error("Task exceeded max retries, sending to DLQ",
			zap.String("chain_id", s.chainID),
			zap.Uint64("start_block", result.StartBlock),
			zap.Int("retry_count", result.Task.RetryCount))
		// TODO: 发送到死信队列
	}
}

// exponentialBackoff 指数退避算法
func (s *ChainScanner) exponentialBackoff(retryCount int) time.Duration {
	baseDelay := s.scannerConfig.Concurrency.RetryDelay
	maxDelay := 5 * time.Minute
	
	delay := baseDelay * time.Duration(math.Pow(2, float64(retryCount-1)))
	if delay > maxDelay {
		delay = maxDelay
	}
	
	return delay
}

// monitorWatermark 监控水位
func (s *ChainScanner) monitorWatermark() {
	ticker := time.NewTicker(s.scannerConfig.Watermark.CheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
			s.checkWatermark()
		}
	}
}

// checkWatermark 检查水位状态
func (s *ChainScanner) checkWatermark() {
	watermark, err := s.watermark.GetWatermark()
	if err != nil {
		logger.Error("Failed to get watermark",
			zap.String("chain_id", s.chainID),
			zap.Error(err))
		return
	}

	// 检查是否有回滚风险
	if watermark.ScannedHeight > watermark.ReorgBoundary {
		logger.Warn("Scanned height exceeds reorg boundary",
			zap.String("chain_id", s.chainID),
			zap.Uint64("scanned_height", watermark.ScannedHeight),
			zap.Uint64("reorg_boundary", watermark.ReorgBoundary))
	}
}

// detectReorg 检测回滚
func (s *ChainScanner) detectReorg() {
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
			s.checkReorg()
		}
	}
}

// checkReorg 检查回滚
func (s *ChainScanner) checkReorg() {
	watermark, err := s.watermark.GetWatermark()
	if err != nil {
		logger.Error("Failed to get watermark for reorg check",
			zap.String("chain_id", s.chainID),
			zap.Error(err))
		return
	}

	// 检查已扫描的最新区块
	if watermark.ScannedHeight == 0 {
		return
	}

	// 获取该区块的哈希
	blockHash, err := s.watermark.GetBlockHash(watermark.ScannedHeight)
	if err != nil {
		logger.Error("Failed to get block hash for reorg check",
			zap.String("chain_id", s.chainID),
			zap.Uint64("block_number", watermark.ScannedHeight),
			zap.Error(err))
		return
	}

	// 检测回滚
	hasReorg, commonAncestor, err := s.client.DetectReorg(s.ctx, watermark.ScannedHeight, blockHash)
	if err != nil {
		logger.Error("Failed to detect reorg",
			zap.String("chain_id", s.chainID),
			zap.Uint64("block_number", watermark.ScannedHeight),
			zap.Error(err))
		return
	}

	if hasReorg {
		logger.Warn("Reorg detected",
			zap.String("chain_id", s.chainID),
			zap.Uint64("old_height", watermark.ScannedHeight),
			zap.Uint64("common_ancestor", commonAncestor))

		// 触发回滚处理
		if err := s.reorgHandler.HandleReorg(s.ctx, watermark.ScannedHeight, commonAncestor); err != nil {
			logger.Error("Failed to handle reorg",
				zap.String("chain_id", s.chainID),
				zap.Error(err))
		}

		// 更新状态
		s.mu.Lock()
		s.status.ReorgCount++
		s.status.LastReorgTime = time.Now()
		s.mu.Unlock()
	}
}

// GetLatestHeight 获取最新高度
func (s *ChainScanner) GetLatestHeight(ctx context.Context) (uint64, error) {
	return s.client.GetCurrentHeight(ctx)
}

// GetBlock 获取区块
func (s *ChainScanner) GetBlock(ctx context.Context, blockNumber uint64) (*types.Block, error) {
	return s.client.GetBlock(ctx, blockNumber)
}

// ResetWatermark 重置水位
func (s *ChainScanner) ResetWatermark(ctx context.Context, height uint64) error {
	return s.watermark.ResetToHeight(height)
}

// StartScan 启动扫描
func (s *ChainScanner) StartScan(ctx context.Context) error {
	return s.Start(ctx)
}

// updateStatus 更新状态
func (s *ChainScanner) updateStatus(currentHeight, finalizedHeight uint64, watermark *types.Watermark) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.status.CurrentHeight = currentHeight
	s.status.ConfirmedHeight = watermark.ConfirmedHeight
	s.status.FinalizedHeight = finalizedHeight
	s.status.ReorgBoundary = watermark.ReorgBoundary
	s.status.BlockLag = currentHeight - watermark.ScannedHeight
	s.status.ActiveWorkers = len(s.workers)
	s.status.PendingTasks = len(s.taskQueue)

	// 计算扫描速度（使用指数移动平均）
	if !s.status.LastScanTime.IsZero() {
		duration := time.Since(s.status.LastScanTime).Seconds()
		if duration > 0 {
			blocksScanned := watermark.ScannedHeight - s.lastScanHeight
			currentRate := float64(blocksScanned) / duration
			
			// 指数移动平均，alpha=0.3
			s.scanRate = 0.3*currentRate + 0.7*s.scanRate
			s.status.BlocksPerSecond = s.scanRate
		}
	}
	s.lastScanHeight = watermark.ScannedHeight

	// 更新指标
	metrics.SetCurrentHeight(s.chainID, float64(currentHeight))
	metrics.SetScannedHeight(s.chainID, float64(watermark.ScannedHeight))
	metrics.SetBlockLag(s.chainID, float64(s.status.BlockLag))
	metrics.SetActiveWorkers(s.chainID, float64(s.status.ActiveWorkers))
	metrics.SetPendingTasks(s.chainID, float64(s.status.PendingTasks))
}