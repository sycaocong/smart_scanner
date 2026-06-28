package plugin

import (
	"context"
	"fmt"
	"math/big"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	ethtypes "github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/smart-scanner/multi-chain-scanner/pkg/types"
)

// EVMAdapter EVM 链适配器
// [Design: ChainAdapter 链适配器](../docs/DESIGN_SCANNER.md#82-chainadapter-链适配器)
type EVMAdapter struct {
	*BaseAdapter
	client *ethclient.Client
	config *EVMConfig
}

// EVMConfig EVM 配置
type EVMConfig struct {
	ChainID          string `json:"chain_id"`
	ChainName        string `json:"chain_name"`
	RPCURL           string `json:"rpc_url"`
	WSURL            string `json:"ws_url,omitempty"`
	FinalityBlocks   uint64 `json:"finality_blocks"`
	BlockTime        time.Duration `json:"block_time"`
}

// EVMAdapterFactory EVM 适配器工厂
type EVMAdapterFactory struct {
	*BaseAdapterFactory
}

// NewEVMAdapterFactory 创建 EVM 适配器工厂
func NewEVMAdapterFactory() *EVMAdapterFactory {
	return &EVMAdapterFactory{
		BaseAdapterFactory: NewBaseAdapterFactory("evm"),
	}
}

// Create 创建 EVM 适配器
func (f *EVMAdapterFactory) Create(config interface{}) (ChainAdapter, error) {
	evmConfig, ok := config.(*EVMConfig)
	if !ok {
		return nil, fmt.Errorf("invalid config type, expected *EVMConfig")
	}

	adapter := &EVMAdapter{
		BaseAdapter: NewBaseAdapter(evmConfig.ChainID, evmConfig.ChainName),
		config:      evmConfig,
	}

	return adapter, nil
}

// ValidateConfig 验证配置
func (f *EVMAdapterFactory) ValidateConfig(config interface{}) error {
	evmConfig, ok := config.(*EVMConfig)
	if !ok {
		return fmt.Errorf("invalid config type, expected *EVMConfig")
	}

	if evmConfig.ChainID == "" {
		return fmt.Errorf("chain_id cannot be empty")
	}
	if evmConfig.ChainName == "" {
		return fmt.Errorf("chain_name cannot be empty")
	}
	if evmConfig.RPCURL == "" {
		return fmt.Errorf("rpc_url cannot be empty")
	}
	if evmConfig.FinalityBlocks == 0 {
		return fmt.Errorf("finality_blocks cannot be zero")
	}
	if evmConfig.BlockTime == 0 {
		return fmt.Errorf("block_time cannot be zero")
	}

	return nil
}

// Initialize 初始化 EVM 适配器
func (a *EVMAdapter) Initialize(ctx context.Context, config interface{}) error {
	evmConfig, ok := config.(*EVMConfig)
	if !ok {
		return fmt.Errorf("invalid config type, expected *EVMConfig")
	}

	a.config = evmConfig

	// 创建以太坊客户端
	client, err := ethclient.DialContext(ctx, evmConfig.RPCURL)
	if err != nil {
		return fmt.Errorf("failed to connect to RPC: %w", err)
	}

	a.client = client

	// 验证连接
	if _, err := a.client.ChainID(ctx); err != nil {
		return fmt.Errorf("failed to verify connection: %w", err)
	}

	return nil
}

// GetBlock 获取区块
func (a *EVMAdapter) GetBlock(ctx context.Context, blockNumber uint64) (*types.Block, error) {
	block, err := a.client.BlockByNumber(ctx, new(big.Int).SetUint64(blockNumber))
	if err != nil {
		return nil, fmt.Errorf("failed to get block: %w", err)
	}

	return a.convertBlock(block)
}

// GetBlockByHash 根据哈希获取区块
func (a *EVMAdapter) GetBlockByHash(ctx context.Context, blockHash string) (*types.Block, error) {
	hash := common.HexToHash(blockHash)
	block, err := a.client.BlockByHash(ctx, hash)
	if err != nil {
		return nil, fmt.Errorf("failed to get block by hash: %w", err)
	}

	return a.convertBlock(block)
}

// GetTransaction 获取交易
func (a *EVMAdapter) GetTransaction(ctx context.Context, txHash string) (*types.Transaction, error) {
	hash := common.HexToHash(txHash)
	tx, _, err := a.client.TransactionByHash(ctx, hash)
	if err != nil {
		return nil, fmt.Errorf("failed to get transaction: %w", err)
	}

	// 获取交易回执
	receipt, err := a.client.TransactionReceipt(ctx, hash)
	if err != nil {
		return nil, fmt.Errorf("failed to get transaction receipt: %w", err)
	}

	return a.convertTransaction(tx, receipt)
}

// GetLatestHeight 获取最新高度
func (a *EVMAdapter) GetLatestHeight(ctx context.Context) (uint64, error) {
	header, err := a.client.HeaderByNumber(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("failed to get latest header: %w", err)
	}

	return header.Number.Uint64(), nil
}

// GetFinalizedHeight 获取最终确认高度
func (a *EVMAdapter) GetFinalizedHeight(ctx context.Context) (uint64, error) {
	latestHeight, err := a.GetLatestHeight(ctx)
	if err != nil {
		return 0, err
	}

	// EVM 链通常使用区块数作为最终性确认
	if latestHeight > a.config.FinalityBlocks {
		return latestHeight - a.config.FinalityBlocks, nil
	}

	return 0, nil
}

// ParseBlock 解析区块
func (a *EVMAdapter) ParseBlock(ctx context.Context, rawBlock interface{}) (*types.Block, error) {
	block, ok := rawBlock.(*types.Block)
	if !ok {
		return nil, fmt.Errorf("invalid block type")
	}

	return block, nil
}

// ParseTransaction 解析交易
func (a *EVMAdapter) ParseTransaction(ctx context.Context, rawTx interface{}) (*types.Transaction, error) {
	tx, ok := rawTx.(*types.Transaction)
	if !ok {
		return nil, fmt.Errorf("invalid transaction type")
	}

	return tx, nil
}

// ParseLog 解析日志
func (a *EVMAdapter) ParseLog(ctx context.Context, rawLog interface{}) (*types.Log, error) {
	log, ok := rawLog.(*types.Log)
	if !ok {
		return nil, fmt.Errorf("invalid log type")
	}

	return log, nil
}

// DetectReorg 检测回滚
func (a *EVMAdapter) DetectReorg(ctx context.Context, localBlock *types.Block) (bool, *types.ReorgEvent, error) {
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
func (a *EVMAdapter) Close() error {
	if a.client != nil {
		a.client.Close()
	}
	return nil
}

// HealthCheck 健康检查
func (a *EVMAdapter) HealthCheck(ctx context.Context) error {
	if a.client == nil {
		return fmt.Errorf("client not initialized")
	}

	// 尝试获取最新区块号
	_, err := a.client.HeaderByNumber(ctx, nil)
	if err != nil {
		return fmt.Errorf("health check failed: %w", err)
	}

	return nil
}

// convertBlock 转换以太坊区块
func (a *EVMAdapter) convertBlock(block *ethtypes.Block) (*types.Block, error) {
	number := block.Number().Uint64()
	hash := block.Hash().Hex()
	parentHash := block.ParentHash().Hex()
	timestamp := time.Unix(int64(block.Time()), 0)

	result := &types.Block{
		ChainID:    a.chainID,
		Number:     number,
		Hash:       hash,
		ParentHash: parentHash,
		Timestamp:  timestamp,
	}

	return result, nil
}

// convertTransaction 转换以太坊交易
func (a *EVMAdapter) convertTransaction(tx *ethtypes.Transaction, receipt *ethtypes.Receipt) (*types.Transaction, error) {
	status := types.TxStatusPending
	if receipt != nil {
		if receipt.Status == 1 {
			status = types.TxStatusConfirmed
		} else {
			status = types.TxStatusReverted
		}
	}

	result := &types.Transaction{
		ChainID:     a.chainID,
		Hash:        tx.Hash().Hex(),
		BlockHash:   receipt.BlockHash.Hex(),
		BlockNumber: receipt.BlockNumber.Uint64(),
		To:          "",
		Value:       tx.Value(),
		GasPrice:    tx.GasPrice(),
		GasUsed:     receipt.GasUsed,
		Status:      status,
		Index:       uint64(receipt.TransactionIndex),
	}

	return result, nil
}

// convertEVMBlock 转换 EVM 区块 (使用 raw interface{})
func (a *EVMAdapter) convertEVMBlock(rawBlock interface{}) (*types.Block, error) {
	// 尝试解析为原始 map (来自 eth_getBlockByNumber)
	blockData, ok := rawBlock.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("invalid block data format")
	}

	// 解析区块号
	numberHex, ok := blockData["number"].(string)
	if !ok {
		return nil, fmt.Errorf("invalid block number format")
	}
	number, err := hexutil.DecodeUint64(numberHex)
	if err != nil {
		return nil, fmt.Errorf("failed to decode block number: %w", err)
	}

	// 解析区块哈希
	hash, ok := blockData["hash"].(string)
	if !ok {
		return nil, fmt.Errorf("invalid block hash format")
	}

	// 解析父区块哈希
	parentHash, ok := blockData["parentHash"].(string)
	if !ok {
		return nil, fmt.Errorf("invalid parent hash format")
	}

	// 解析时间戳
	timestampHex, ok := blockData["timestamp"].(string)
	if !ok {
		return nil, fmt.Errorf("invalid timestamp format")
	}
	timestampBig, err := hexutil.DecodeBig(timestampHex)
	if err != nil {
		return nil, fmt.Errorf("failed to decode timestamp: %w", err)
	}
	timestamp := time.Unix(int64(timestampBig.Int64()), 0)

	result := &types.Block{
		ChainID:    a.chainID,
		Number:     number,
		Hash:       hash,
		ParentHash: parentHash,
		Timestamp:  timestamp,
		RawData:   blockData,
	}

	return result, nil
}

// convertEVMTransaction 转换 EVM 交易 (从原始 map)
func (a *EVMAdapter) convertEVMTransaction(txData map[string]interface{}, receiptData map[string]interface{}) (*types.Transaction, error) {
	status := types.TxStatusPending
	if receiptData != nil {
		if statusRaw, ok := receiptData["status"].(string); ok {
			if statusRaw == "0x1" {
				status = types.TxStatusConfirmed
			} else {
				status = types.TxStatusReverted
			}
		}
	}

	// 解析交易哈希
	txHash := ""
	if hash, ok := txData["hash"].(string); ok {
		txHash = hash
	}

	// 解析区块信息
	blockNumber := uint64(0)
	blockHash := ""
	if numberHex, ok := txData["blockNumber"].(string); ok {
		if n, err := hexutil.DecodeUint64(numberHex); err == nil {
			blockNumber = n
		}
	}
	if hash, ok := txData["blockHash"].(string); ok {
		blockHash = hash
	}

	// 解析发送方和接收方
	from := ""
	to := ""
	if f, ok := txData["from"].(string); ok {
		from = f
	}
	if t, ok := txData["to"].(string); ok {
		to = t
	}

	// 解析值
	value := "0"
	if v, ok := txData["value"].(string); ok {
		value = v
	}

	// 解析 gas price
	gasPrice := "0"
	if gp, ok := txData["gasPrice"].(string); ok {
		gasPrice = gp
	}

	// 解析数值为 *big.Int
	valueBig, err := hexutil.DecodeBig(value)
	if err != nil {
		valueBig = big.NewInt(0)
	}
	gasPriceBig, err := hexutil.DecodeBig(gasPrice)
	if err != nil {
		gasPriceBig = big.NewInt(0)
	}

	result := &types.Transaction{
		ChainID:     a.chainID,
		Hash:        txHash,
		BlockNumber: blockNumber,
		BlockHash:   blockHash,
		From:        from,
		To:          to,
		Value:       valueBig,
		GasPrice:    gasPriceBig,
		Status:      status,
	}

	return result, nil
}

// convertEVMLog 转换 EVM 日志 (从原始 map)
func (a *EVMAdapter) convertEVMLog(logData map[string]interface{}, txHash string, blockNumber uint64) (*types.Log, error) {
	// 解析地址
	address := ""
	if addr, ok := logData["address"].(string); ok {
		address = addr
	}

	// 解析主题
	topics := make([]string, 0)
	if t, ok := logData["topics"].([]interface{}); ok {
		for _, topic := range t {
			if s, ok := topic.(string); ok {
				topics = append(topics, s)
			}
		}
	}

	// 解析数据
	data := ""
	if d, ok := logData["data"].(string); ok {
		data = d
	}

	// 解析日志索引
	logIndex := uint64(0)
	if idx, ok := logData["logIndex"].(string); ok {
		if n, err := hexutil.DecodeUint64(idx); err == nil {
			logIndex = n
		}
	}

	result := &types.Log{
		ChainID:     a.chainID,
		TxHash:      txHash,
		BlockNumber: blockNumber,
		Address:     address,
		Topics:      topics,
		Data:        data,
		LogIndex:    logIndex,
		TxIndex:     0,
	}

	return result, nil
}

// GetBlockWithTransactions 获取包含完整交易的区块
func (a *EVMAdapter) GetBlockWithTransactions(ctx context.Context, blockNumber uint64) (*types.Block, error) {
	block, err := a.client.BlockByNumber(ctx, new(big.Int).SetUint64(blockNumber))
	if err != nil {
		return nil, fmt.Errorf("failed to get block: %w", err)
	}

	// 转换区块
	result := &types.Block{
		ChainID:    a.chainID,
		Number:     block.NumberU64(),
		Hash:       block.Hash().Hex(),
		ParentHash: block.ParentHash().Hex(),
		Timestamp:  time.Unix(int64(block.Time()), 0),
	}

	// 转换交易
	transactions := make([]types.Transaction, 0, len(block.Transactions()))
	for i, tx := range block.Transactions() {
		// 获取交易回执
		receipt, err := a.client.TransactionReceipt(ctx, tx.Hash())
		if err != nil {
			return nil, fmt.Errorf("failed to get receipt for tx %s: %w", tx.Hash().Hex(), err)
		}

		// 转换交易
		convertedTx, err := a.convertRawTransaction(tx, receipt, block.NumberU64(), uint64(i))
		if err != nil {
			return nil, fmt.Errorf("failed to convert transaction: %w", err)
		}

		transactions = append(transactions, *convertedTx)
	}

	result.Transactions = transactions

	return result, nil
}

// convertRawTransaction 转换原始交易 (使用以太坊类型)
func (a *EVMAdapter) convertRawTransaction(tx *ethtypes.Transaction, receipt *ethtypes.Receipt, blockNumber uint64, txIndex uint64) (*types.Transaction, error) {
	status := types.TxStatusPending
	if receipt != nil {
		if receipt.Status == 1 {
			status = types.TxStatusConfirmed
		} else {
			status = types.TxStatusReverted
		}
	}

	var to string
	if tx.To() != nil {
		to = tx.To().Hex()
	}

	result := &types.Transaction{
		ChainID:     a.chainID,
		Hash:        tx.Hash().Hex(),
		BlockNumber: blockNumber,
		BlockHash:   "",
		To:          to,
		Value:       tx.Value(),
		GasPrice:    tx.GasPrice(),
		GasUsed:     receipt.GasUsed,
		Status:      status,
		Index:       txIndex,
		InputData:   hexutil.Encode(tx.Data()),
	}

	// 转换日志
	logs := make([]types.Log, 0, len(receipt.Logs))
	for i, log := range receipt.Logs {
		convertedLog := a.convertRawLog(log, tx.Hash().Hex(), blockNumber, uint64(i), txIndex)
		logs = append(logs, *convertedLog)
	}
	result.Logs = logs

	return result, nil
}

// convertRawLog 转换原始日志 (使用以太坊类型)
func (a *EVMAdapter) convertRawLog(log *ethtypes.Log, txHash string, blockNumber uint64, logIndex uint64, txIndex uint64) *types.Log {
	topics := make([]string, 0, len(log.Topics))
	for _, topic := range log.Topics {
		topics = append(topics, topic.Hex())
	}

	return &types.Log{
		ChainID:     a.chainID,
		TxHash:      txHash,
		BlockNumber: blockNumber,
		Address:     log.Address.Hex(),
		Topics:      topics,
		Data:        hexutil.Encode(log.Data),
		LogIndex:    logIndex,
		TxIndex:     txIndex,
	}
}