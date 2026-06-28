package types

import (
	"math/big"
	"time"
)

// ChainType 链类型
type ChainType string

const (
	ChainTypeEVM    ChainType = "evm"
	ChainTypeSolana ChainType = "solana"
	ChainTypeTron   ChainType = "tron"
)

// FinalizationType 最终性类型
type FinalizationType string

const (
	FinalizationTypeFinalized    FinalizationType = "finalized"
	FinalizationTypeConfirmed    FinalizationType = "confirmed"
	FinalizationTypeIrreversible FinalizationType = "irreversible"
)

// TransactionStatus 交易状态
type TransactionStatus string

const (
	TxStatusPending   TransactionStatus = "pending"
	TxStatusConfirmed TransactionStatus = "confirmed"
	TxStatusFinalized TransactionStatus = "finalized"
	TxStatusReverted  TransactionStatus = "reverted"
)

// ChainConfig 链配置
type ChainConfig struct {
	ChainID             string            `yaml:"chain_id" json:"chain_id"`
	ChainType           ChainType         `yaml:"chain_type" json:"chain_type"`
	BlockTime           time.Duration     `yaml:"block_time" json:"block_time"`
	FinalizationType    FinalizationType  `yaml:"finalization_type" json:"finalization_type"`
	RPCNodes            []RPCNodeConfig   `yaml:"rpc_nodes" json:"rpc_nodes"`
	HealthCheck         HealthCheckConfig `yaml:"health_check" json:"health_check"`
	StartBlock          uint64            `yaml:"start_block" json:"start_block"`
	Confirmations       uint64            `yaml:"confirmations" json:"confirmations"`
	Contracts           []string          `yaml:"contracts" json:"contracts"`
	Enabled             bool              `yaml:"enabled" json:"enabled"`
}

// RPCNodeConfig RPC 节点配置
type RPCNodeConfig struct {
	URL      string        `yaml:"url" json:"url"`
	Weight   int           `yaml:"weight" json:"weight"`
	Timeout  time.Duration `yaml:"timeout" json:"timeout"`
	MaxConns int           `yaml:"max_conns" json:"max_conns"`
}

// HealthCheckConfig 健康检查配置
type HealthCheckConfig struct {
	Enabled          bool          `yaml:"enabled" json:"enabled"`
	Interval         time.Duration `yaml:"interval" json:"interval"`
	Timeout          time.Duration `yaml:"timeout" json:"timeout"`
	FailureThreshold int           `yaml:"failure_threshold" json:"failure_threshold"`
	SuccessThreshold int           `yaml:"success_threshold" json:"success_threshold"`
}

// Block 区块数据（通用结构）
type Block struct {
	ChainID      string          `json:"chain_id"`
	Number       uint64          `json:"number"`
	Hash         string          `json:"hash"`
	ParentHash   string          `json:"parent_hash"`
	Timestamp    time.Time       `json:"timestamp"`
	Transactions []Transaction   `json:"transactions"`
	RawData      interface{}     `json:"raw_data"` // 原始区块数据，用于调试
}

// Transaction 交易数据（通用结构）
type Transaction struct {
	ChainID       string            `json:"chain_id"`
	Hash          string            `json:"hash"`
	BlockNumber   uint64            `json:"block_number"`
	BlockHash     string            `json:"block_hash"`
	From          string            `json:"from"`
	To            string            `json:"to"`
	Value         *big.Int          `json:"value"`
	GasPrice      *big.Int          `json:"gas_price"`
	GasUsed       uint64            `json:"gas_used"`
	Status        TransactionStatus `json:"status"`
	Timestamp     time.Time         `json:"timestamp"`
	Index         uint64            `json:"index"`
	InputData     string            `json:"input_data"`
	Logs          []Log             `json:"logs"`
	RawData       interface{}       `json:"raw_data"` // 原始交易数据
}

// Log 日志数据（通用结构）
type Log struct {
	ChainID     string   `json:"chain_id"`
	TxHash      string   `json:"tx_hash"`
	BlockNumber uint64   `json:"block_number"`
	Address     string   `json:"address"`
	Topics      []string `json:"topics"`
	Data        string   `json:"data"`
	LogIndex    uint64   `json:"log_index"`
	TxIndex     uint64   `json:"tx_index"`
}

// Watermark 水位信息
type Watermark struct {
	ChainID           string `json:"chain_id"`
	ScannedHeight     uint64 `json:"scanned_height"`     // 已扫描高度
	ConfirmedHeight   uint64 `json:"confirmed_height"`   // 已确认高度
	FinalizedHeight   uint64 `json:"finalized_height"`   // 已最终确认高度
	ReorgBoundary     uint64 `json:"reorg_boundary"`     // 回滚边界
	LastUpdateTime    time.Time `json:"last_update_time"`
}

// ReorgEvent 回滚事件
type ReorgEvent struct {
	ChainID         string    `json:"chain_id"`
	DetectedAt      time.Time `json:"detected_at"`
	OldBlockNumber  uint64    `json:"old_block_number"`
	OldBlockHash    string    `json:"old_block_hash"`
	NewBlockNumber  uint64    `json:"new_block_number"`
	NewBlockHash    string    `json:"new_block_hash"`
	Depth           uint64    `json:"depth"`
	AffectedTxs     []string  `json:"affected_txs"`     // 受影响的交易哈希列表
	Processed       bool      `json:"processed"`        // 是否已处理
}

// NodeHealth 节点健康状态
type NodeHealth struct {
	URL             string    `json:"url"`
	Healthy         bool      `json:"healthy"`
	LastCheckTime   time.Time `json:"last_check_time"`
	FailureCount    int       `json:"failure_count"`
	SuccessCount    int       `json:"success_count"`
	AvgResponseTime float64   `json:"avg_response_time"` // 毫秒
}

// ScanTask 扫描任务
type ScanTask struct {
	ChainID       string `json:"chain_id"`
	StartBlock    uint64 `json:"start_block"`
	EndBlock      uint64 `json:"end_block"`
	ShardID       int    `json:"shard_id"`       // 分片ID
	RetryCount    int    `json:"retry_count"`    // 重试次数
	Priority      int    `json:"priority"`       // 优先级
}

// ScanResult 扫描结果
type ScanResult struct {
	Task          *ScanTask     `json:"task"`
	TaskID        string        `json:"task_id"`
	ChainID       string        `json:"chain_id"`
	StartBlock    uint64        `json:"start_block"`
	EndBlock      uint64        `json:"end_block"`
	BlocksScanned int           `json:"blocks_scanned"`
	TxsFound      int           `json:"txs_found"`
	Success       bool          `json:"success"`
	Error         error         `json:"error,omitempty"`
	Duration      time.Duration `json:"duration"`
}

// FilterConfig 过滤配置
type FilterConfig struct {
	Addresses    []string `json:"addresses"`    // 地址白名单
	Contracts    []string `json:"contracts"`    // 合约白名单
	Events       []string `json:"events"`       // 事件白名单
	MinValue     *big.Int `json:"min_value"`    // 最小金额过滤
	ExcludeEmpty bool     `json:"exclude_empty"` // 是否排除空交易
}

// MessageQueueMessage 消息队列消息
type MessageQueueMessage struct {
	Topic     string      `json:"topic"`
	Key       string      `json:"key"`
	Value     interface{} `json:"value"`
	Timestamp time.Time   `json:"timestamp"`
	Retries   int         `json:"retries"`
}

// StorageRecord 存储记录
type StorageRecord struct {
	ID        string      `json:"id"`
	ChainID   string      `json:"chain_id"`
	Type      string      `json:"type"`      // block, transaction, log
	Data      interface{} `json:"data"`
	Version   int         `json:"version"`   // 版本号，用于回滚
	Status    string      `json:"status"`    // pending, confirmed, finalized, reverted
	CreatedAt time.Time   `json:"created_at"`
	UpdatedAt time.Time   `json:"updated_at"`
}

// Metrics 指标数据
type Metrics struct {
	ChainID            string  `json:"chain_id"`
	BlocksScanned      uint64  `json:"blocks_scanned"`
	TxsProcessed       uint64  `json:"txs_processed"`
	CurrentHeight      uint64  `json:"current_height"`
	ScannedHeight      uint64  `json:"scanned_height"`
	BlockLag           uint64  `json:"block_lag"`
	ErrorRate          float64 `json:"error_rate"`
	AvgScanTime        float64 `json:"avg_scan_time"`    // 毫秒
	QueueDepth         int     `json:"queue_depth"`
	ActiveConnections  int     `json:"active_connections"`
	HealthyNodes       int     `json:"healthy_nodes"`
	TotalNodes         int     `json:"total_nodes"`
}

// Alert 告警信息
type Alert struct {
	Type         string    `json:"type"`
	ChainID      string    `json:"chain_id"`
	NodeURL      string    `json:"node_url"`
	Message      string    `json:"message"`
	Timestamp    time.Time `json:"timestamp"`
	FailureCount int       `json:"failure_count"`
	SuccessCount int       `json:"success_count"`
}