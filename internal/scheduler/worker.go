package scheduler

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/smart-scanner/multi-chain-scanner/internal/node"
	"github.com/smart-scanner/multi-chain-scanner/pkg/config"
	"github.com/smart-scanner/multi-chain-scanner/pkg/logger"
	"github.com/smart-scanner/multi-chain-scanner/pkg/metrics"
	"github.com/smart-scanner/multi-chain-scanner/pkg/types"
	"github.com/smart-scanner/multi-chain-scanner/pkg/util"
	"go.uber.org/zap"
)

// Worker 工作线程
// 负责执行具体的区块扫描任务，支持顺序和并行两种扫描模式
// [Design: Worker 工作线程](../docs/DESIGN_SCANNER.md#35-worker-工作线程)
type Worker struct {
	workerID       string                          // 工作线程ID
	client         node.ChainClient                // 链客户端
	taskQueue      <-chan *types.ScanTask          // 任务队列（只读）
	resultQueue    chan<- *types.ScanResult        // 结果队列（只写）
	concurrency    *config.ConcurrencyConfig       // 并发配置
	currentTask    atomic.Pointer[types.ScanTask]  // 当前执行的任务（原子操作）
	tasksCompleted uint64                          // 已完成任务数
	tasksFailed    uint64                          // 失败任务数
}

// NewWorker 创建工作线程
func NewWorker(
	workerID string,
	client node.ChainClient,
	taskQueue <-chan *types.ScanTask,
	resultQueue chan<- *types.ScanResult,
	concurrency *config.ConcurrencyConfig,
) *Worker {
	return &Worker{
		workerID:    workerID,
		client:      client,
		taskQueue:   taskQueue,
		resultQueue: resultQueue,
		concurrency: concurrency,
	}
}

// Start 启动工作线程
func (w *Worker) Start(ctx context.Context) {
	logger.Info("Worker started", zap.String("worker_id", w.workerID))

	for {
		select {
		case <-ctx.Done():
			logger.Info("Worker stopped", zap.String("worker_id", w.workerID))
			return
		case task, ok := <-w.taskQueue:
			if !ok {
				logger.Info("Task queue closed, worker stopping", zap.String("worker_id", w.workerID))
				return
			}
			w.processTask(ctx, task)
		}
	}
}

// processTask 处理任务
func (w *Worker) processTask(ctx context.Context, task *types.ScanTask) {
	w.currentTask.Store(task)
	startTime := time.Now()

	logger.Info("Processing scan task",
		zap.String("worker_id", w.workerID),
		zap.String("chain_id", task.ChainID),
		zap.Uint64("start_block", task.StartBlock),
		zap.Uint64("end_block", task.EndBlock),
		zap.Int("retry_count", task.RetryCount))

	result := &types.ScanResult{
		Task:        task,
		ChainID:     task.ChainID,
		StartBlock:  task.StartBlock,
		EndBlock:    task.EndBlock,
		Success:     false,
		Duration:    0,
	}

	// 执行扫描
	blocksScanned, txsFound, err := w.scanBlocks(ctx, task.StartBlock, task.EndBlock)
	if err != nil {
		result.Error = err
		atomic.AddUint64(&w.tasksFailed, 1)
		logger.Error("Scan task failed",
			zap.String("worker_id", w.workerID),
			zap.String("chain_id", task.ChainID),
			zap.Uint64("start_block", task.StartBlock),
			zap.Uint64("end_block", task.EndBlock),
			zap.Error(err))
	} else {
		result.Success = true
		result.BlocksScanned = blocksScanned
		result.TxsFound = txsFound
		result.Duration = time.Since(startTime)
		atomic.AddUint64(&w.tasksCompleted, 1)

		logger.Info("Scan task completed",
			zap.String("worker_id", w.workerID),
			zap.String("chain_id", task.ChainID),
			zap.Uint64("start_block", task.StartBlock),
			zap.Uint64("end_block", task.EndBlock),
			zap.Int("blocks_scanned", blocksScanned),
			zap.Int("txs_found", txsFound),
			zap.Duration("duration", result.Duration))
	}

	// 发送结果（带超时）
	select {
	case w.resultQueue <- result:
	default:
		logger.Error("Result queue is full, dropping result",
			zap.String("worker_id", w.workerID),
			zap.String("chain_id", task.ChainID))
	}

	w.currentTask.Store(nil)
}

// scanBlocks 扫描区块
func (w *Worker) scanBlocks(ctx context.Context, startBlock, endBlock uint64) (int, int, error) {
	batchSize := w.concurrency.BatchSize

	// 计算需要扫描的区块数量
	totalBlocks := int(endBlock - startBlock + 1)
	if totalBlocks <= 0 {
		return 0, 0, fmt.Errorf("invalid block range: %d to %d", startBlock, endBlock)
	}

	// 如果区块数量较少，使用单线程处理
	if totalBlocks <= batchSize*2 {
		return w.scanBlocksSequential(ctx, startBlock, endBlock, batchSize)
	}

	// 并行扫描
	return w.scanBlocksParallel(ctx, startBlock, endBlock, batchSize)
}

// scanBlocksSequential 顺序扫描区块
func (w *Worker) scanBlocksSequential(ctx context.Context, startBlock, endBlock uint64, batchSize int) (int, int, error) {
	blocksScanned := 0
	txsFound := 0

	for batchStart := startBlock; batchStart <= endBlock; batchStart += uint64(batchSize) {
		batchEnd := util.MinUint64(batchStart+uint64(batchSize)-1, endBlock)

		blockNumbers := make([]uint64, 0, int(batchEnd-batchStart+1))
		for num := batchStart; num <= batchEnd; num++ {
			blockNumbers = append(blockNumbers, num)
		}

		blocks, err := w.client.BatchGetBlocks(ctx, blockNumbers)
		if err != nil {
			metrics.RecordScanError(w.getCurrentTaskChainID(), "batch_get_blocks")
			return blocksScanned, txsFound, fmt.Errorf("failed to batch get blocks %d to %d: %w", batchStart, batchEnd, err)
		}

		for _, block := range blocks {
			if block == nil {
				continue
			}
			blocksScanned++
			txsFound += len(block.Transactions)
		}

		select {
		case <-ctx.Done():
			return blocksScanned, txsFound, ctx.Err()
		default:
		}
	}

	return blocksScanned, txsFound, nil
}

// scanBlocksParallel 并行扫描区块
func (w *Worker) scanBlocksParallel(ctx context.Context, startBlock, endBlock uint64, batchSize int) (int, int, error) {
	parallelBatches := w.concurrency.ParallelRequests
	if parallelBatches < 2 {
		parallelBatches = 2
	}

	// 计算每个批次的区块数
	batches := make([]struct {
		start uint64
		end   uint64
	}, 0, parallelBatches)

	for i := 0; i < parallelBatches; i++ {
		batchStart := startBlock + uint64(i)*uint64(batchSize)
		if batchStart > endBlock {
			break
		}
		batchEnd := util.MinUint64(batchStart+uint64(batchSize)-1, endBlock)
		batches = append(batches, struct {
			start uint64
			end   uint64
		}{batchStart, batchEnd})
	}

	// 使用 channel 收集结果
	type batchResult struct {
		blocksScanned int
		txsFound      int
		err           error
	}

	resultChan := make(chan batchResult, len(batches))

	// 并行执行
	for _, batch := range batches {
		go func(batchStart, batchEnd uint64) {
			blockNumbers := make([]uint64, 0, int(batchEnd-batchStart+1))
			for num := batchStart; num <= batchEnd; num++ {
				blockNumbers = append(blockNumbers, num)
			}

			blocks, err := w.client.BatchGetBlocks(ctx, blockNumbers)
			if err != nil {
				resultChan <- batchResult{0, 0, fmt.Errorf("failed to batch get blocks %d to %d: %w", batchStart, batchEnd, err)}
				return
			}

			blocksScanned := 0
			txsFound := 0
			for _, block := range blocks {
				if block == nil {
					continue
				}
				blocksScanned++
				txsFound += len(block.Transactions)
			}

			resultChan <- batchResult{blocksScanned, txsFound, nil}
		}(batch.start, batch.end)
	}

	// 收集结果
	totalBlocksScanned := 0
	totalTxsFound := 0
	for i := 0; i < len(batches); i++ {
		select {
		case result := <-resultChan:
			if result.err != nil {
				return totalBlocksScanned, totalTxsFound, result.err
			}
			totalBlocksScanned += result.blocksScanned
			totalTxsFound += result.txsFound
		case <-ctx.Done():
			return totalBlocksScanned, totalTxsFound, ctx.Err()
		}
	}

	return totalBlocksScanned, totalTxsFound, nil
}

// getCurrentTaskChainID 获取当前任务的链ID（用于指标记录）
func (w *Worker) getCurrentTaskChainID() string {
	task := w.currentTask.Load()
	if task == nil {
		return "unknown"
	}
	return task.ChainID
}

// GetCurrentTask 获取当前任务
func (w *Worker) GetCurrentTask() *types.ScanTask {
	return w.currentTask.Load()
}

// GetWorkerID 获取工作线程ID
func (w *Worker) GetWorkerID() string {
	return w.workerID
}

// GetStats 获取工作线程统计信息
func (w *Worker) GetStats() (uint64, uint64) {
	return atomic.LoadUint64(&w.tasksCompleted), atomic.LoadUint64(&w.tasksFailed)
}