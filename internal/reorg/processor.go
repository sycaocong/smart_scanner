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

// Processor 回滚处理器接口
// [Design: ReorgProcessor 回滚处理器](../docs/DESIGN_SCANNER.md#72-reorgprocessor-回滚处理器)
type Processor interface {
	Start(ctx context.Context) error
	Stop() error
	ProcessReorg(ctx context.Context, event *types.ReorgEvent) error
	GetProcessingStatus(chainID string) (*ProcessingStatus, error)
	GetStatistics(chainID string) (*ProcessorStatistics, error)
}

// ReorgProcessor 回滚处理器实现
// 处理回滚事件，执行数据清理和状态恢复
// [Design: ReorgProcessor 回滚处理器](../docs/DESIGN_SCANNER.md#72-reorgprocessor-回滚处理器)
type ReorgProcessor struct {
	config           *ProcessorConfig                  // 处理器配置
	storage          storage.Storage                   // 存储层
	scannerManager   *scheduler.SchedulerManager       // 调度器管理器
	
	running          bool                              // 是否运行中
	mu               sync.RWMutex                      // 状态锁
	wg               sync.WaitGroup                    // 等待组
	
	processingStatus map[string]*ProcessingStatus      // 处理状态（chainID -> ProcessingStatus）
	statistics       map[string]*ProcessorStatistics   // 统计信息（chainID -> ProcessorStatistics）
	eventQueue       chan *types.ReorgEvent            // 待处理事件队列
}

// ProcessorConfig 处理器配置
type ProcessorConfig struct {
	// 队列大小
	QueueSize int
	
	// 工作协程数量
	WorkerCount int
	
	// 处理超时时间
	ProcessingTimeout time.Duration
	
	// 最大重试次数
	MaxRetries int
	
	// 重试间隔
	RetryInterval time.Duration
	
	// 是否启用自动重扫
	EnableAutoRescan bool
	
	// 重扫延迟时间
	RescanDelay time.Duration
}

// ProcessingStatus 处理状态
type ProcessingStatus struct {
	ChainID              string     `json:"chain_id"`
	IsProcessing         bool       `json:"is_processing"`
	CurrentEvent         *types.ReorgEvent `json:"current_event,omitempty"`
	StartTime            time.Time  `json:"start_time,omitempty"`
	ProcessedEvents      int64      `json:"processed_events"`
	FailedEvents         int64      `json:"failed_events"`
	LastProcessTime      time.Time  `json:"last_process_time,omitempty"`
	CurrentStep          string     `json:"current_step,omitempty"`
	CurrentStepProgress  float64    `json:"current_step_progress,omitempty"`
}

// ProcessorStatistics 处理器统计信息
type ProcessorStatistics struct {
	ChainID              string     `json:"chain_id"`
	TotalProcessed       int64      `json:"total_processed"`
	TotalFailed          int64      `json:"total_failed"`
	AverageProcessingTime time.Duration `json:"average_processing_time"`
	LastProcessTime      time.Time  `json:"last_process_time,omitempty"`
	TotalDataCleaned     int64      `json:"total_data_cleaned"`
	TotalTransactionsReverted int64 `json:"total_transactions_reverted"`
}

// NewReorgProcessor 创建回滚处理器
func NewReorgProcessor(
	config *ProcessorConfig,
	storage storage.Storage,
	scannerManager *scheduler.SchedulerManager,
) (*ReorgProcessor, error) {
	if config == nil {
		config = &ProcessorConfig{
			QueueSize:          1000,
			WorkerCount:        5,
			ProcessingTimeout:  30 * time.Minute,
			MaxRetries:         3,
			RetryInterval:      1 * time.Minute,
			EnableAutoRescan:   true,
			RescanDelay:        5 * time.Minute,
		}
	}

	if storage == nil {
		return nil, fmt.Errorf("storage cannot be nil")
	}

	if scannerManager == nil {
		return nil, fmt.Errorf("scanner manager cannot be nil")
	}

	return &ReorgProcessor{
		config:           config,
		storage:          storage,
		scannerManager:   scannerManager,
		processingStatus: make(map[string]*ProcessingStatus),
		statistics:       make(map[string]*ProcessorStatistics),
		eventQueue:       make(chan *types.ReorgEvent, config.QueueSize),
	}, nil
}

// Start 启动处理器
func (p *ReorgProcessor) Start(ctx context.Context) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.running {
		return fmt.Errorf("processor is already running")
	}

	logger.Info("Starting reorg processor")

	p.running = true

	// 启动工作协程
	for i := 0; i < p.config.WorkerCount; i++ {
		p.wg.Add(1)
		go p.worker(ctx, i)
	}

	// 启动监控协程
	p.wg.Add(1)
	go p.monitor(ctx)

	logger.Info("Reorg processor started successfully")
	return nil
}

// Stop 停止处理器
func (p *ReorgProcessor) Stop() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if !p.running {
		return nil
	}

	logger.Info("Stopping reorg processor")

	p.running = false
	close(p.eventQueue)
	p.wg.Wait()

	logger.Info("Reorg processor stopped successfully")
	return nil
}

// worker 工作协程
func (p *ReorgProcessor) worker(ctx context.Context, workerID int) {
	defer p.wg.Done()

	logger.Info("Reorg processor worker started",
		zap.Int("worker_id", workerID))

	for {
		select {
		case <-ctx.Done():
			logger.Info("Reorg processor worker stopped",
				zap.Int("worker_id", workerID))
			return
		case event, ok := <-p.eventQueue:
			if !ok {
				logger.Info("Event queue closed, worker stopping",
					zap.Int("worker_id", workerID))
				return
			}

			// 处理回滚事件
			if err := p.processEventWithRetry(ctx, event); err != nil {
				logger.Error("Failed to process reorg event",
					zap.Int("worker_id", workerID),
					zap.String("chain_id", event.ChainID),
					zap.Uint64("old_block", event.OldBlockNumber),
					zap.Uint64("new_block", event.NewBlockNumber),
					zap.Error(err))
			}
		}
	}
}

// monitor 监控协程
func (p *ReorgProcessor) monitor(ctx context.Context) {
	defer p.wg.Done()

	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			p.checkStuckProcessing(ctx)
		}
	}
}

// checkStuckProcessing 检查卡住的处理
func (p *ReorgProcessor) checkStuckProcessing(ctx context.Context) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	now := time.Now()
	for chainID, status := range p.processingStatus {
		if status.IsProcessing {
			// 检查处理时间是否过长
			if now.Sub(status.StartTime) > p.config.ProcessingTimeout {
				logger.Warn("Reorg processing stuck",
					zap.String("chain_id", chainID),
					zap.Duration("processing_time", now.Sub(status.StartTime)),
					zap.String("current_step", status.CurrentStep))
				
				// 这里可以添加警报或自动恢复逻辑
			}
		}
	}
}

// ProcessReorg 处理回滚事件
func (p *ReorgProcessor) ProcessReorg(ctx context.Context, event *types.ReorgEvent) error {
	// 将事件放入队列
	select {
	case p.eventQueue <- event:
		logger.Info("Reorg event queued for processing",
			zap.String("chain_id", event.ChainID),
			zap.Uint64("depth", event.Depth))
		return nil
	default:
		return fmt.Errorf("event queue is full")
	}
}

// processEventWithRetry 带重试的事件处理
func (p *ReorgProcessor) processEventWithRetry(ctx context.Context, event *types.ReorgEvent) error {
	var lastErr error

	for attempt := 0; attempt <= p.config.MaxRetries; attempt++ {
		if attempt > 0 {
			logger.Info("Retrying reorg event processing",
				zap.String("chain_id", event.ChainID),
				zap.Int("attempt", attempt),
				zap.Int("max_retries", p.config.MaxRetries),
				zap.Error(lastErr))

			// 等待重试间隔
			select {
			case <-time.After(p.config.RetryInterval):
			case <-ctx.Done():
				return ctx.Err()
			}
		}

		err := p.processEvent(ctx, event)
		if err == nil {
			return nil
		}

		lastErr = err
		logger.Warn("Reorg event processing failed",
			zap.String("chain_id", event.ChainID),
			zap.Int("attempt", attempt+1),
			zap.Error(err))
	}

	return fmt.Errorf("failed to process reorg event after %d attempts: %w", p.config.MaxRetries+1, lastErr)
}

// processEvent 处理单个回滚事件
func (p *ReorgProcessor) processEvent(ctx context.Context, event *types.ReorgEvent) error {
	startTime := time.Now()

	// 更新处理状态
	p.updateProcessingStatus(event.ChainID, &ProcessingStatus{
		ChainID:         event.ChainID,
		IsProcessing:    true,
		CurrentEvent:    event,
		StartTime:       startTime,
		CurrentStep:     "starting",
	})

	defer func() {
		p.clearProcessingStatus(event.ChainID)
	}()

	logger.Warn("Processing reorg event",
		zap.String("chain_id", event.ChainID),
		zap.Uint64("old_block", event.OldBlockNumber),
		zap.Uint64("new_block", event.NewBlockNumber),
		zap.Uint64("depth", event.Depth))

	// 设置处理超时
	timeoutCtx, cancel := context.WithTimeout(ctx, p.config.ProcessingTimeout)
	defer cancel()

	// 执行处理步骤
	steps := []struct {
		name string
		fn   func(context.Context, *types.ReorgEvent) error
	}{
		{"validate", p.validateEvent},
		{"mark_transactions", p.markRevertedTransactions},
		{"revert_business_data", p.revertBusinessData},
		{"clean_data", p.cleanAffectedData},
		{"reset_watermark", p.resetScannerWatermark},
		{"mark_processed", p.markEventAsProcessed},
	}

	for i, step := range steps {
		p.updateProcessingStep(event.ChainID, step.name, float64(i)/float64(len(steps)))

		logger.Info("Executing reorg processing step",
			zap.String("chain_id", event.ChainID),
			zap.String("step", step.name),
			zap.Int("step_number", i+1),
			zap.Int("total_steps", len(steps)))

		if err := step.fn(timeoutCtx, event); err != nil {
			p.updateProcessingStep(event.ChainID, step.name+" (failed)", float64(i)/float64(len(steps)))
			return fmt.Errorf("failed to execute step %s: %w", step.name, err)
		}

		logger.Info("Reorg processing step completed",
			zap.String("chain_id", event.ChainID),
			zap.String("step", step.name))
	}

	p.updateProcessingStep(event.ChainID, "completed", 1.0)

	// 更新统计信息
	duration := time.Since(startTime)
	p.updateStatistics(event.ChainID, duration, true)

	logger.Warn("Reorg event processed successfully",
		zap.String("chain_id", event.ChainID),
		zap.Duration("duration", duration))

	// 如果启用自动重扫，启动重扫
	if p.config.EnableAutoRescan {
		go p.scheduleRescan(context.Background(), event)
	}

	return nil
}

// validateEvent 验证事件
func (p *ReorgProcessor) validateEvent(ctx context.Context, event *types.ReorgEvent) error {
	logger.Info("Validating reorg event",
		zap.String("chain_id", event.ChainID))

	// 验证基本字段
	if event.ChainID == "" {
		return fmt.Errorf("chain_id is empty")
	}
	if event.OldBlockNumber == 0 {
		return fmt.Errorf("old_block_number is zero")
	}
	if event.NewBlockNumber == 0 {
		return fmt.Errorf("new_block_number is zero")
	}
	if event.Depth == 0 {
		return fmt.Errorf("depth is zero")
	}

	// 验证区块哈希
	if event.OldBlockHash == "" || event.NewBlockHash == "" {
		return fmt.Errorf("block hash is empty")
	}

	// 验证新区块是否存在
	newBlock, err := p.storage.GetBlockByHash(ctx, event.ChainID, event.NewBlockHash)
	if err != nil {
		return fmt.Errorf("failed to get new block: %w", err)
	}

	if newBlock.Number != event.NewBlockNumber {
		return fmt.Errorf("new block number mismatch")
	}

	return nil
}

// markRevertedTransactions 标记回滚交易
func (p *ReorgProcessor) markRevertedTransactions(ctx context.Context, event *types.ReorgEvent) error {
	logger.Info("Marking reverted transactions",
		zap.String("chain_id", event.ChainID),
		zap.Uint64("from_block", event.NewBlockNumber),
		zap.Uint64("to_block", event.OldBlockNumber))

	// 计算回滚区间
	fromBlock := event.NewBlockNumber
	toBlock := event.OldBlockNumber

	// 标记交易为已回滚
	if err := p.storage.MarkTransactionsAsReverted(ctx, event.ChainID, fromBlock, toBlock); err != nil {
		return fmt.Errorf("failed to mark transactions as reverted: %w", err)
	}

	// 统计回滚的交易数量
	var totalTxs int64
	for blockNumber := fromBlock; blockNumber <= toBlock; blockNumber++ {
		txs, err := p.storage.GetTransactionsByBlock(ctx, event.ChainID, blockNumber)
		if err != nil {
			logger.Error("Failed to get transactions by block",
				zap.String("chain_id", event.ChainID),
				zap.Uint64("block_number", blockNumber),
				zap.Error(err))
			continue
		}
		totalTxs += int64(len(txs))
	}

	logger.Info("Marked transactions as reverted",
		zap.String("chain_id", event.ChainID),
		zap.Int64("total_transactions", totalTxs))

	return nil
}

// revertBusinessData 冲正业务数据
func (p *ReorgProcessor) revertBusinessData(ctx context.Context, event *types.ReorgEvent) error {
	logger.Info("Reverting business data",
		zap.String("chain_id", event.ChainID),
		zap.Uint64("from_block", event.NewBlockNumber),
		zap.Uint64("to_block", event.OldBlockNumber))

	// 这里应该根据业务需求冲正相关的业务数据
	// 示例：
	// 1. 冲正余额变化
	// 2. 取消相关订单
	// 3. 撤销相关操作

	// 获取受影响的交易
	fromBlock := event.NewBlockNumber
	toBlock := event.OldBlockNumber

	for blockNumber := fromBlock; blockNumber <= toBlock; blockNumber++ {
		txs, err := p.storage.GetTransactionsByBlock(ctx, event.ChainID, blockNumber)
		if err != nil {
			logger.Error("Failed to get transactions by block",
				zap.String("chain_id", event.ChainID),
				zap.Uint64("block_number", blockNumber),
				zap.Error(err))
			continue
		}

		for _, tx := range txs {
			// 冲正单个交易的业务数据
			if err := p.revertTransactionBusinessData(ctx, tx); err != nil {
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
func (p *ReorgProcessor) revertTransactionBusinessData(ctx context.Context, tx *types.Transaction) error {
	logger.Debug("Reverting transaction business data",
		zap.String("chain_id", tx.ChainID),
		zap.String("tx_hash", tx.Hash))

	// 这里应该根据业务需求冲正交易相关的业务数据
	// 这个函数应该由具体的业务实现来重写

	return nil
}

// cleanAffectedData 清理受影响的数据
func (p *ReorgProcessor) cleanAffectedData(ctx context.Context, event *types.ReorgEvent) error {
	logger.Info("Cleaning affected data",
		zap.String("chain_id", event.ChainID),
		zap.Uint64("from_block", event.NewBlockNumber),
		zap.Uint64("to_block", event.OldBlockNumber))

	fromBlock := event.NewBlockNumber
	toBlock := event.OldBlockNumber

	// 删除区块
	if err := p.storage.DeleteBlocks(ctx, event.ChainID, fromBlock, toBlock); err != nil {
		return fmt.Errorf("failed to delete blocks: %w", err)
	}

	// 删除日志
	if err := p.storage.DeleteLogs(ctx, event.ChainID, fromBlock, toBlock); err != nil {
		return fmt.Errorf("failed to delete logs: %w", err)
	}

	logger.Info("Cleaned affected data successfully",
		zap.String("chain_id", event.ChainID),
		zap.Uint64("from_block", fromBlock),
		zap.Uint64("to_block", toBlock))

	return nil
}

// resetScannerWatermark 重置扫描器水位
func (p *ReorgProcessor) resetScannerWatermark(ctx context.Context, event *types.ReorgEvent) error {
	logger.Info("Resetting scanner watermark",
		zap.String("chain_id", event.ChainID))

	scanner, err := p.scannerManager.GetScanner(event.ChainID)
	if err != nil {
		return fmt.Errorf("scanner not found for chain %s", event.ChainID)
	}

	// 计算安全高度
	safeHeight := event.OldBlockNumber
	if safeHeight > 0 {
		safeHeight--
	}

	// 重置水位
	if err := scanner.ResetWatermark(ctx, safeHeight); err != nil {
		return fmt.Errorf("failed to reset watermark: %w", err)
	}

	logger.Info("Reset scanner watermark successfully",
		zap.String("chain_id", event.ChainID),
		zap.Uint64("safe_height", safeHeight))

	return nil
}

// markEventAsProcessed 标记事件为已处理
func (p *ReorgProcessor) markEventAsProcessed(ctx context.Context, event *types.ReorgEvent) error {
	logger.Info("Marking reorg event as processed",
		zap.String("chain_id", event.ChainID))

	event.Processed = true

	if err := p.storage.SaveReorgEvent(ctx, event); err != nil {
		return fmt.Errorf("failed to save reorg event: %w", err)
	}

	return nil
}

// scheduleRescan 安排重扫
func (p *ReorgProcessor) scheduleRescan(ctx context.Context, event *types.ReorgEvent) {
	logger.Info("Scheduling rescan",
		zap.String("chain_id", event.ChainID),
		zap.Duration("delay", p.config.RescanDelay))

	// 等待延迟时间
	select {
	case <-time.After(p.config.RescanDelay):
	case <-ctx.Done():
		return
	}

	// 启动重扫
	scanner, err := p.scannerManager.GetScanner(event.ChainID)
	if err != nil {
		logger.Error("Scanner not found for rescan",
			zap.String("chain_id", event.ChainID))
		return
	}

	if err := scanner.StartScan(ctx); err != nil {
		logger.Error("Failed to start rescan",
			zap.String("chain_id", event.ChainID),
			zap.Error(err))
	}
}

// updateProcessingStatus 更新处理状态
func (p *ReorgProcessor) updateProcessingStatus(chainID string, status *ProcessingStatus) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.processingStatus[chainID] = status
}

// updateProcessingStep 更新处理步骤
func (p *ReorgProcessor) updateProcessingStep(chainID string, step string, progress float64) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if status, exists := p.processingStatus[chainID]; exists {
		status.CurrentStep = step
		status.CurrentStepProgress = progress
	}
}

// clearProcessingStatus 清除处理状态
func (p *ReorgProcessor) clearProcessingStatus(chainID string) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if status, exists := p.processingStatus[chainID]; exists {
		status.IsProcessing = false
		status.LastProcessTime = time.Now()
		status.CurrentStep = ""
		status.CurrentStepProgress = 0
	}
}

// updateStatistics 更新统计信息
func (p *ReorgProcessor) updateStatistics(chainID string, duration time.Duration, success bool) {
	p.mu.Lock()
	defer p.mu.Unlock()

	stats, exists := p.statistics[chainID]
	if !exists {
		stats = &ProcessorStatistics{
			ChainID: chainID,
		}
		p.statistics[chainID] = stats
	}

	if success {
		stats.TotalProcessed++
	} else {
		stats.TotalFailed++
	}

	stats.LastProcessTime = time.Now()

	// 计算平均处理时间
	totalProcessed := stats.TotalProcessed + stats.TotalFailed
	if totalProcessed > 0 {
		totalTime := stats.AverageProcessingTime * time.Duration(totalProcessed-1)
		stats.AverageProcessingTime = (totalTime + duration) / time.Duration(totalProcessed)
	}
}

// GetProcessingStatus 获取处理状态
func (p *ReorgProcessor) GetProcessingStatus(chainID string) (*ProcessingStatus, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	status, exists := p.processingStatus[chainID]
	if !exists {
		return &ProcessingStatus{
			ChainID:      chainID,
			IsProcessing: false,
		}, nil
	}

	// 返回状态的副本
	statusCopy := *status
	return &statusCopy, nil
}

// GetAllProcessingStatus 获取所有处理状态
func (p *ReorgProcessor) GetAllProcessingStatus() map[string]*ProcessingStatus {
	p.mu.RLock()
	defer p.mu.RUnlock()

	result := make(map[string]*ProcessingStatus)
	for chainID, status := range p.processingStatus {
		statusCopy := *status
		result[chainID] = &statusCopy
	}

	return result
}

// GetStatistics 获取统计信息
func (p *ReorgProcessor) GetStatistics(chainID string) (*ProcessorStatistics, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	stats, exists := p.statistics[chainID]
	if !exists {
		return nil, fmt.Errorf("statistics not found for chain %s", chainID)
	}

	// 返回统计信息的副本
	statsCopy := *stats
	return &statsCopy, nil
}

// GetAllStatistics 获取所有统计信息
func (p *ReorgProcessor) GetAllStatistics() map[string]*ProcessorStatistics {
	p.mu.RLock()
	defer p.mu.RUnlock()

	result := make(map[string]*ProcessorStatistics)
	for chainID, stats := range p.statistics {
		statsCopy := *stats
		result[chainID] = &statsCopy
	}

	return result
}