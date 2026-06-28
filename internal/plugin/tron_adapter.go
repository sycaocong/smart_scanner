package plugin

import (
	"context"
	"fmt"
	"math/big"
	"time"

	"github.com/smart-scanner/multi-chain-scanner/pkg/types"
)

// TronAdapter Tron 链适配器
// [Design: ChainAdapter 链适配器](../docs/DESIGN_SCANNER.md#82-chainadapter-链适配器)
type TronAdapter struct {
	*BaseAdapter
	client TronClient
	config *TronConfig
}

// TronClient Tron 客户端接口
type TronClient interface {
	// GetBlock 获取区块
	GetBlock(ctx context.Context, blockNumber uint64) (*TronBlock, error)
	
	// GetBlockByHash 根据哈希获取区块
	GetBlockByHash(ctx context.Context, blockHash string) (*TronBlock, error)
	
	// GetTransaction 获取交易
	GetTransaction(ctx context.Context, txHash string) (*TronTransaction, error)
	
	// GetLatestHeight 获取最新高度
	GetLatestHeight(ctx context.Context) (uint64, error)
	
	// GetNowBlock 获取最新区块
	GetNowBlock(ctx context.Context) (*TronBlock, error)
}

// TronConfig Tron 配置
type TronConfig struct {
	ChainID          string `json:"chain_id"`
	ChainName        string `json:"chain_name"`
	RPCURL           string `json:"rpc_url"`
	GRPCURL          string `json:"grpc_url,omitempty"`
	FinalityBlocks   uint64 `json:"finality_blocks"`
	BlockTime        time.Duration `json:"block_time"`
}

// TronBlock Tron 区块
type TronBlock struct {
	BlockID       string `json:"blockID"`
	BlockNumber   uint64 `json:"blockNumber"`
	BlockWitness  string `json:"blockWitness"`
	ParentHash    string `json:"parentHash"`
	Timestamp     int64  `json:"timestamp"`
	Transactions  []*TronTransaction `json:"transactions"`
}

// TronTransaction Tron 交易
type TronTransaction struct {
	TxID          string `json:"txID"`
	BlockNumber   uint64 `json:"blockNumber"`
	BlockHash     string `json:"blockHash"`
	FromAddress   string `json:"fromAddress"`
	ToAddress     string `json:"toAddress"`
	Value         *big.Int `json:"value"`
	GasPrice      *big.Int `json:"gasPrice"`
	GasUsed       uint64 `json:"gasUsed"`
	Status        bool   `json:"status"`
	ContractData  string `json:"contractData"`
	Logs          []*TronLog `json:"logs"`
}

// TronLog Tron 日志
type TronLog struct {
	Address string   `json:"address"`
	Topics  []string `json:"topics"`
	Data    string   `json:"data"`
}

// TronAdapterFactory Tron 适配器工厂
type TronAdapterFactory struct {
	*BaseAdapterFactory
}

// NewTronAdapterFactory 创建 Tron 适配器工厂
func NewTronAdapterFactory() *TronAdapterFactory {
	return &TronAdapterFactory{
		BaseAdapterFactory: NewBaseAdapterFactory("tron"),
	}
}

// Create 创建 Tron 适配器
func (f *TronAdapterFactory) Create(config interface{}) (ChainAdapter, error) {
	tronConfig, ok := config.(*TronConfig)
	if !ok {
		return nil, fmt.Errorf("invalid config type, expected *TronConfig")
	}

	adapter := &TronAdapter{
		BaseAdapter: NewBaseAdapter(tronConfig.ChainID, tronConfig.ChainName),
		config:      tronConfig,
	}

	return adapter, nil
}

// ValidateConfig 验证配置
func (f *TronAdapterFactory) ValidateConfig(config interface{}) error {
	tronConfig, ok := config.(*TronConfig)
	if !ok {
		return fmt.Errorf("invalid config type, expected *TronConfig")
	}

	if tronConfig.ChainID == "" {
		return fmt.Errorf("chain_id cannot be empty")
	}
	if tronConfig.ChainName == "" {
		return fmt.Errorf("chain_name cannot be empty")
	}
	if tronConfig.RPCURL == "" {
		return fmt.Errorf("rpc_url cannot be empty")
	}
	if tronConfig.FinalityBlocks == 0 {
		return fmt.Errorf("finality_blocks cannot be zero")
	}
	if tronConfig.BlockTime == 0 {
		return fmt.Errorf("block_time cannot be zero")
	}

	return nil
}

// Initialize 初始化 Tron 适配器
func (a *TronAdapter) Initialize(ctx context.Context, config interface{}) error {
	tronConfig, ok := config.(*TronConfig)
	if !ok {
		return fmt.Errorf("invalid config type, expected *TronConfig")
	}

	a.config = tronConfig

	// 创建 Tron 客户端
	client, err := NewTronHTTPClient(tronConfig.RPCURL)
	if err != nil {
		return fmt.Errorf("failed to create Tron client: %w", err)
	}

	a.client = client

	// 验证连接
	if _, err := a.client.GetLatestHeight(ctx); err != nil {
		return fmt.Errorf("failed to verify connection: %w", err)
	}

	return nil
}

// GetBlock 获取区块
func (a *TronAdapter) GetBlock(ctx context.Context, blockNumber uint64) (*types.Block, error) {
	block, err := a.client.GetBlock(ctx, blockNumber)
	if err != nil {
		return nil, fmt.Errorf("failed to get block: %w", err)
	}

	return a.convertBlock(block)
}

// GetBlockByHash 根据哈希获取区块
func (a *TronAdapter) GetBlockByHash(ctx context.Context, blockHash string) (*types.Block, error) {
	block, err := a.client.GetBlockByHash(ctx, blockHash)
	if err != nil {
		return nil, fmt.Errorf("failed to get block by hash: %w", err)
	}

	return a.convertBlock(block)
}

// GetTransaction 获取交易
func (a *TronAdapter) GetTransaction(ctx context.Context, txHash string) (*types.Transaction, error) {
	tx, err := a.client.GetTransaction(ctx, txHash)
	if err != nil {
		return nil, fmt.Errorf("failed to get transaction: %w", err)
	}

	return a.convertTransaction(tx)
}

// GetLatestHeight 获取最新高度
func (a *TronAdapter) GetLatestHeight(ctx context.Context) (uint64, error) {
	return a.client.GetLatestHeight(ctx)
}

// GetFinalizedHeight 获取最终确认高度
func (a *TronAdapter) GetFinalizedHeight(ctx context.Context) (uint64, error) {
	latestHeight, err := a.GetLatestHeight(ctx)
	if err != nil {
		return 0, err
	}

	// Tron 使用区块数作为最终性确认
	if latestHeight > a.config.FinalityBlocks {
		return latestHeight - a.config.FinalityBlocks, nil
	}

	return 0, nil
}

// ParseBlock 解析区块
func (a *TronAdapter) ParseBlock(ctx context.Context, rawBlock interface{}) (*types.Block, error) {
	block, ok := rawBlock.(*types.Block)
	if !ok {
		return nil, fmt.Errorf("invalid block type")
	}

	return block, nil
}

// ParseTransaction 解析交易
func (a *TronAdapter) ParseTransaction(ctx context.Context, rawTx interface{}) (*types.Transaction, error) {
	tx, ok := rawTx.(*types.Transaction)
	if !ok {
		return nil, fmt.Errorf("invalid transaction type")
	}

	return tx, nil
}

// ParseLog 解析日志
func (a *TronAdapter) ParseLog(ctx context.Context, rawLog interface{}) (*types.Log, error) {
	log, ok := rawLog.(*types.Log)
	if !ok {
		return nil, fmt.Errorf("invalid log type")
	}

	return log, nil
}

// DetectReorg 检测回滚
func (a *TronAdapter) DetectReorg(ctx context.Context, localBlock *types.Block) (bool, *types.ReorgEvent, error) {
	// 获取链上当前高度的区块
	chainBlock, err := a.GetBlock(ctx, localBlock.Number)
	if err != nil {
		return false, nil, fmt.Errorf("failed to get chain block: %w", err)
	}

	// 比较哈希
	if chainBlock.Hash != localBlock.Hash {
		// 检测到回滚
		event := &types.ReorgEvent{
			ChainID:        a.chainID,
			DetectedAt:     time.Now(),
			OldBlockNumber: localBlock.Number,
			OldBlockHash:   localBlock.Hash,
			NewBlockNumber: chainBlock.Number,
			NewBlockHash:   chainBlock.Hash,
			Depth:          1, // 简化实现，实际需要计算深度
			Processed:      false,
		}

		return true, event, nil
	}

	return false, nil, nil
}

// Close 关闭适配器
func (a *TronAdapter) Close() error {
	// Tron HTTP 客户端不需要显式关闭
	return nil
}

// HealthCheck 健康检查
func (a *TronAdapter) HealthCheck(ctx context.Context) error {
	if a.client == nil {
		return fmt.Errorf("client not initialized")
	}

	// 尝试获取最新区块号
	_, err := a.client.GetLatestHeight(ctx)
	if err != nil {
		return fmt.Errorf("health check failed: %w", err)
	}

	return nil
}

// convertBlock 转换区块
func (a *TronAdapter) convertBlock(block *TronBlock) (*types.Block, error) {
	if block == nil {
		return nil, fmt.Errorf("block is nil")
	}

	result := &types.Block{
		ChainID:     a.chainID,
		Number:      block.BlockNumber,
		Hash:        block.BlockID,
		ParentHash:  block.ParentHash,
		Timestamp:   time.Unix(block.Timestamp/1000, 0), // Tron 使用毫秒时间戳
		Transactions: make([]types.Transaction, 0, len(block.Transactions)),
	}

	// 转换交易
	for _, tx := range block.Transactions {
		convertedTx, err := a.convertTransaction(tx)
		if err != nil {
			return nil, fmt.Errorf("failed to convert transaction: %w", err)
		}
		result.Transactions = append(result.Transactions, *convertedTx)
	}

	return result, nil
}

// convertTransaction 转换交易
func (a *TronAdapter) convertTransaction(tx *TronTransaction) (*types.Transaction, error) {
	if tx == nil {
		return nil, fmt.Errorf("transaction is nil")
	}

	// 确定交易状态
	status := types.TxStatusPending
	if tx.Status {
		status = types.TxStatusConfirmed
	} else {
		status = types.TxStatusReverted
	}

	result := &types.Transaction{
		ChainID:     a.chainID,
		Hash:        tx.TxID,
		BlockNumber: tx.BlockNumber,
		BlockHash:   tx.BlockHash,
		From:        tx.FromAddress,
		To:          tx.ToAddress,
		Value:       tx.Value,
		GasPrice:    tx.GasPrice,
		GasUsed:     tx.GasUsed,
		Status:      status,
		InputData:   tx.ContractData,
		Logs:        make([]types.Log, 0, len(tx.Logs)),
	}

	// 转换日志
	for i, log := range tx.Logs {
		convertedLog := a.convertLog(log, tx.TxID, tx.BlockNumber, uint64(i))
		result.Logs = append(result.Logs, *convertedLog)
	}

	return result, nil
}

// convertLog 转换日志
func (a *TronAdapter) convertLog(log *TronLog, txHash string, blockNumber uint64, logIndex uint64) *types.Log {
	return &types.Log{
		ChainID:     a.chainID,
		TxHash:      txHash,
		BlockNumber: blockNumber,
		Address:     log.Address,
		Topics:      log.Topics,
		Data:        log.Data,
		LogIndex:    logIndex,
		TxIndex:     0, // Tron 可能没有交易索引
	}
}

// TronHTTPClient Tron HTTP 客户端实现
type TronHTTPClient struct {
	rpcURL string
}

// NewTronHTTPClient 创建 Tron HTTP 客户端
func NewTronHTTPClient(rpcURL string) (*TronHTTPClient, error) {
	if rpcURL == "" {
		return nil, fmt.Errorf("RPC URL cannot be empty")
	}

	return &TronHTTPClient{
		rpcURL: rpcURL,
	}, nil
}

// GetBlock 获取区块
func (c *TronHTTPClient) GetBlock(ctx context.Context, blockNumber uint64) (*TronBlock, error) {
	// 这里应该实现实际的 HTTP 调用
	// 简化实现，返回空区块
	return &TronBlock{
		BlockID:      "",
		BlockNumber:  blockNumber,
		ParentHash:   "",
		Timestamp:    time.Now().Unix() * 1000,
		Transactions: []*TronTransaction{},
	}, nil
}

// GetBlockByHash 根据哈希获取区块
func (c *TronHTTPClient) GetBlockByHash(ctx context.Context, blockHash string) (*TronBlock, error) {
	// 这里应该实现实际的 HTTP 调用
	// 简化实现，返回空区块
	return &TronBlock{
		BlockID:      blockHash,
		BlockNumber:  0,
		ParentHash:   "",
		Timestamp:    time.Now().Unix() * 1000,
		Transactions: []*TronTransaction{},
	}, nil
}

// GetTransaction 获取交易
func (c *TronHTTPClient) GetTransaction(ctx context.Context, txHash string) (*TronTransaction, error) {
	// 这里应该实现实际的 HTTP 调用
	// 简化实现，返回空交易
	return &TronTransaction{
		TxID:         txHash,
		BlockNumber:  0,
		BlockHash:    "",
		FromAddress:  "",
		ToAddress:    "",
		Value:        big.NewInt(0),
		GasPrice:     big.NewInt(0),
		GasUsed:      0,
		Status:       true,
		ContractData: "",
		Logs:         []*TronLog{},
	}, nil
}

// GetLatestHeight 获取最新高度
func (c *TronHTTPClient) GetLatestHeight(ctx context.Context) (uint64, error) {
	// 这里应该实现实际的 HTTP 调用
	// 简化实现，返回 0
	return 0, nil
}

// GetNowBlock 获取最新区块
func (c *TronHTTPClient) GetNowBlock(ctx context.Context) (*TronBlock, error) {
	// 这里应该实现实际的 HTTP 调用
	// 简化实现，返回空区块
	return &TronBlock{
		BlockID:      "",
		BlockNumber:  0,
		ParentHash:   "",
		Timestamp:    time.Now().Unix() * 1000,
		Transactions: []*TronTransaction{},
	}, nil
}