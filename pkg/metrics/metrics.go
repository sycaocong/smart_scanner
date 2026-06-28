package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// metrics 指标包
// 基于 Prometheus 的监控指标系统，提供扫描、解析、队列、存储等模块的指标收集
// [Design: 监控配置](../docs/DESIGN_SCANNER.md#1-系统概述)

var (
	// 扫描相关指标
	BlocksScanned = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "scanner_blocks_scanned_total",
			Help: "Total number of blocks scanned",
		},
		[]string{"chain_id"},
	)

	TransactionsProcessed = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "scanner_transactions_processed_total",
			Help: "Total number of transactions processed",
		},
		[]string{"chain_id", "status"},
	)

	LogsProcessed = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "scanner_logs_processed_total",
			Help: "Total number of logs processed",
		},
		[]string{"chain_id"},
	)

	// 性能指标
	ScanDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "scanner_scan_duration_seconds",
			Help:    "Time taken to scan blocks",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"chain_id"},
	)

	ParseDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "scanner_parse_duration_seconds",
			Help:    "Time taken to parse transactions",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"chain_id"},
	)

	// 错误指标
	ScanErrors = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "scanner_scan_errors_total",
			Help: "Total number of scan errors",
		},
		[]string{"chain_id", "error_type"},
	)

	ParseErrors = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "scanner_parse_errors_total",
			Help: "Total number of parse errors",
		},
		[]string{"chain_id", "error_type"},
	)

	// 队列指标
	QueueDepth = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "scanner_queue_depth",
			Help: "Current queue depth",
		},
		[]string{"chain_id", "queue_type"},
	)

	// 水位指标
	CurrentHeight = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "scanner_current_height",
			Help: "Current blockchain height",
		},
		[]string{"chain_id"},
	)

	ScannedHeight = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "scanner_scanned_height",
			Help: "Scanned block height",
		},
		[]string{"chain_id"},
	)

	BlockLag = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "scanner_block_lag",
			Help: "Block lag behind current height",
		},
		[]string{"chain_id"},
	)

	// 节点健康指标
	NodeHealthStatus = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "scanner_node_health_status",
			Help: "Node health status (1=healthy, 0=unhealthy)",
		},
		[]string{"chain_id", "node_url"},
	)

	NodeResponseTime = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "scanner_node_response_time_seconds",
			Help:    "Node response time",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"chain_id", "node_url"},
	)

	// 回滚指标
	ReorgEventsDetected = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "scanner_reorg_events_detected_total",
			Help: "Total number of reorg events detected",
		},
		[]string{"chain_id"},
	)

	ReorgEventsProcessed = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "scanner_reorg_events_processed_total",
			Help: "Total number of reorg events processed",
		},
		[]string{"chain_id"},
	)

	ReorgDepth = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "scanner_reorg_depth",
			Help:    "Reorg depth",
			Buckets: []float64{1, 2, 4, 8, 16, 32, 64, 128, 256},
		},
		[]string{"chain_id"},
	)

	// 并发指标
	ActiveWorkers = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "scanner_active_workers",
			Help: "Number of active workers",
		},
		[]string{"chain_id"},
	)

	PendingTasks = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "scanner_pending_tasks",
			Help: "Number of pending tasks",
		},
		[]string{"chain_id"},
	)

	// 存储指标
	StorageOperations = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "scanner_storage_operations_total",
			Help: "Total number of storage operations",
		},
		[]string{"operation_type", "status"},
	)

	StorageOperationDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "scanner_storage_operation_duration_seconds",
			Help:    "Storage operation duration",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"operation_type"},
	)

	// 消息队列指标
	MessageQueueOperations = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "scanner_message_queue_operations_total",
			Help: "Total number of message queue operations",
		},
		[]string{"operation_type", "topic", "status"},
	)

	MessageQueueOperationDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "scanner_message_queue_operation_duration_seconds",
			Help:    "Message queue operation duration",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"operation_type", "topic"},
	)

	// 消费者指标
	ConsumerStatus = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "scanner_consumer_status_total",
			Help: "Consumer status changes",
		},
		[]string{"chain_id", "status"},
	)

	ConsumerLag = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "scanner_consumer_lag_seconds",
			Help:    "Consumer lag in seconds",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"chain_id"},
	)
)

// RecordBlockScanned 记录扫描的区块
func RecordBlockScanned(chainID string) {
	BlocksScanned.WithLabelValues(chainID).Inc()
}

// RecordTransactionProcessed 记录处理的交易
func RecordTransactionProcessed(chainID string, status string) {
	TransactionsProcessed.WithLabelValues(chainID, status).Inc()
}

// RecordLogProcessed 记录处理的日志
func RecordLogProcessed(chainID string) {
	LogsProcessed.WithLabelValues(chainID).Inc()
}

// RecordScanDuration 记录扫描持续时间
func RecordScanDuration(chainID string, duration float64) {
	ScanDuration.WithLabelValues(chainID).Observe(duration)
}

// RecordParseDuration 记录解析持续时间
func RecordParseDuration(chainID string, duration float64) {
	ParseDuration.WithLabelValues(chainID).Observe(duration)
}

// RecordScanError 记录扫描错误
func RecordScanError(chainID string, errorType string) {
	ScanErrors.WithLabelValues(chainID, errorType).Inc()
}

// RecordParseError 记录解析错误
func RecordParseError(chainID string, errorType string) {
	ParseErrors.WithLabelValues(chainID, errorType).Inc()
}

// SetQueueDepth 设置队列深度
func SetQueueDepth(chainID string, queueType string, depth float64) {
	QueueDepth.WithLabelValues(chainID, queueType).Set(depth)
}

// SetCurrentHeight 设置当前高度
func SetCurrentHeight(chainID string, height float64) {
	CurrentHeight.WithLabelValues(chainID).Set(height)
}

// SetScannedHeight 设置已扫描高度
func SetScannedHeight(chainID string, height float64) {
	ScannedHeight.WithLabelValues(chainID).Set(height)
}

// SetBlockLag 设置区块延迟
func SetBlockLag(chainID string, lag float64) {
	BlockLag.WithLabelValues(chainID).Set(lag)
}

// SetNodeHealthStatus 设置节点健康状态
func SetNodeHealthStatus(chainID string, nodeURL string, healthy float64) {
	NodeHealthStatus.WithLabelValues(chainID, nodeURL).Set(healthy)
}

// RecordNodeResponseTime 记录节点响应时间
func RecordNodeResponseTime(chainID string, nodeURL string, duration float64) {
	NodeResponseTime.WithLabelValues(chainID, nodeURL).Observe(duration)
}

// RecordReorgEventDetected 记录检测到的回滚事件
func RecordReorgEventDetected(chainID string) {
	ReorgEventsDetected.WithLabelValues(chainID).Inc()
}

// RecordReorgEventProcessed 记录处理的回滚事件
func RecordReorgEventProcessed(chainID string) {
	ReorgEventsProcessed.WithLabelValues(chainID).Inc()
}

// RecordReorgDepth 记录回滚深度
func RecordReorgDepth(chainID string, depth float64) {
	ReorgDepth.WithLabelValues(chainID).Observe(depth)
}

// SetActiveWorkers 设置活跃工作线程数
func SetActiveWorkers(chainID string, count float64) {
	ActiveWorkers.WithLabelValues(chainID).Set(count)
}

// SetPendingTasks 设置待处理任务数
func SetPendingTasks(chainID string, count float64) {
	PendingTasks.WithLabelValues(chainID).Set(count)
}

// RecordStorageOperation 记录存储操作
func RecordStorageOperation(operationType string, status string) {
	StorageOperations.WithLabelValues(operationType, status).Inc()
}

// RecordStorageOperationDuration 记录存储操作持续时间
func RecordStorageOperationDuration(operationType string, duration float64) {
	StorageOperationDuration.WithLabelValues(operationType).Observe(duration)
}

// RecordMessageQueueOperation 记录消息队列操作
func RecordMessageQueueOperation(operationType string, topic string, status string) {
	MessageQueueOperations.WithLabelValues(operationType, topic, status).Inc()
}

// RecordMessageQueueOperationDuration 记录消息队列操作持续时间
func RecordMessageQueueOperationDuration(operationType string, topic string, duration float64) {
	MessageQueueOperationDuration.WithLabelValues(operationType, topic).Observe(duration)
}

// RecordConsumerStatus 记录消费者状态
func RecordConsumerStatus(chainID string, status string) {
	ConsumerStatus.WithLabelValues(chainID, status).Inc()
}

// RecordConsumerLag 记录消费者延迟
func RecordConsumerLag(chainID string, lag float64) {
	ConsumerLag.WithLabelValues(chainID).Observe(lag)
}

// RecordTransactionProcessing 记录交易处理
func RecordTransactionProcessing(chainID string) {
	TransactionsProcessed.WithLabelValues(chainID).Inc()
}

// RecordLogProcessing 记录日志处理
func RecordLogProcessing(chainID string) {
	LogsProcessed.WithLabelValues(chainID).Inc()
}

// RecordReorgProcessing 记录回滚处理
func RecordReorgProcessing(chainID string) {
	ReorgEventsProcessed.WithLabelValues(chainID).Inc()
}

// ResetMetrics 重置指标（主要用于测试）
func ResetMetrics() {
	BlocksScanned.Reset()
	TransactionsProcessed.Reset()
	LogsProcessed.Reset()
	ScanDuration.Reset()
	ParseDuration.Reset()
	ScanErrors.Reset()
	ParseErrors.Reset()
	QueueDepth.Reset()
	CurrentHeight.Reset()
	ScannedHeight.Reset()
	BlockLag.Reset()
	NodeHealthStatus.Reset()
	NodeResponseTime.Reset()
	ReorgEventsDetected.Reset()
	ReorgEventsProcessed.Reset()
	ReorgDepth.Reset()
	ActiveWorkers.Reset()
	PendingTasks.Reset()
	StorageOperations.Reset()
	StorageOperationDuration.Reset()
	MessageQueueOperations.Reset()
	MessageQueueOperationDuration.Reset()
}