package node

import (
	"context"
	"fmt"
	"time"

	"github.com/smart-scanner/multi-chain-scanner/pkg/config"
	"github.com/smart-scanner/multi-chain-scanner/pkg/logger"
	"github.com/smart-scanner/multi-chain-scanner/pkg/metrics"
	"github.com/smart-scanner/multi-chain-scanner/pkg/types"
	"github.com/smart-scanner/multi-chain-scanner/pkg/util"
	"go.uber.org/zap"
)

// EVMClient EVM 链客户端
// [Design: ChainClient 链客户端接口](../docs/DESIGN_SCANNER.md#41-chainclient-链客户端接口)
type EVMClient struct {
	nodeManager *NodeManager
	chainID     string
	config      *config.ChainConfig
}

// NewEVMClient 创建 EVM 客户端
func NewEVMClient(chainID string, cfg *config.ChainConfig) (*EVMClient, error) {
	nodeManager, err := NewNodeManager(chainID, cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create node manager: %w", err)
	}

	return &EVMClient{
		nodeManager: nodeManager,
		chainID:     chainID,
		config:      cfg,
	}, nil
}

// GetBlock 获取区块
func (c *EVMClient) GetBlock(ctx context.Context, number uint64) (*types.Block, error) {
	startTime := time.Now()
	defer func() {
		metrics.RecordScanDuration(c.chainID, time.Since(startTime).Seconds())
	}()

	result, err := c.nodeManager.Call(ctx, "eth_getBlockByNumber", 
		fmt.Sprintf("0x%x", number), true)
	if err != nil {
		metrics.RecordScanError(c.chainID, "get_block")
		return nil, fmt.Errorf("failed to get block %d: %w", number, err)
	}

	return c.parseBlock(result)
}

// GetBlockByHash 根据哈希获取区块
func (c *EVMClient) GetBlockByHash(ctx context.Context, hash string) (*types.Block, error) {
	startTime := time.Now()
	defer func() {
		metrics.RecordScanDuration(c.chainID, time.Since(startTime).Seconds())
	}()

	result, err := c.nodeManager.Call(ctx, "eth_getBlockByHash", hash, true)
	if err != nil {
		metrics.RecordScanError(c.chainID, "get_block_by_hash")
		return nil, fmt.Errorf("failed to get block by hash %s: %w", hash, err)
	}

	return c.parseBlock(result)
}

// GetLatestBlock 获取最新区块
func (c *EVMClient) GetLatestBlock(ctx context.Context) (*types.Block, error) {
	startTime := time.Now()
	defer func() {
		metrics.RecordScanDuration(c.chainID, time.Since(startTime).Seconds())
	}()

	result, err := c.nodeManager.Call(ctx, "eth_getBlockByNumber", "latest", true)
	if err != nil {
		metrics.RecordScanError(c.chainID, "get_latest_block")
		return nil, fmt.Errorf("failed to get latest block: %w", err)
	}

	return c.parseBlock(result)
}

// GetTransaction 获取交易
func (c *EVMClient) GetTransaction(ctx context.Context, hash string) (*types.Transaction, error) {
	startTime := time.Now()
	defer func() {
		metrics.RecordParseDuration(c.chainID, time.Since(startTime).Seconds())
	}()

	result, err := c.nodeManager.Call(ctx, "eth_getTransactionByHash", hash)
	if err != nil {
		metrics.RecordParseError(c.chainID, "get_transaction")
		return nil, fmt.Errorf("failed to get transaction %s: %w", hash, err)
	}

	return c.parseTransaction(result)
}

// GetTransactionReceipt 获取交易回执
func (c *EVMClient) GetTransactionReceipt(ctx context.Context, hash string) (*types.Transaction, error) {
	startTime := time.Now()
	defer func() {
		metrics.RecordParseDuration(c.chainID, time.Since(startTime).Seconds())
	}()

	result, err := c.nodeManager.Call(ctx, "eth_getTransactionReceipt", hash)
	if err != nil {
		metrics.RecordParseError(c.chainID, "get_transaction_receipt")
		return nil, fmt.Errorf("failed to get transaction receipt %s: %w", hash, err)
	}

	return c.parseTransactionReceipt(result)
}

// GetFinalizedHeight 获取最终确认高度
func (c *EVMClient) GetFinalizedHeight(ctx context.Context) (uint64, error) {
	var blockTag string
	switch c.config.FinalizationType {
	case "finalized":
		blockTag = "finalized"
	case "safe":
		blockTag = "safe"
	default:
		blockTag = "latest"
	}

	result, err := c.nodeManager.Call(ctx, "eth_getBlockByNumber", blockTag, false)
	if err != nil {
		return 0, fmt.Errorf("failed to get finalized block: %w", err)
	}

	blockData, ok := result.(map[string]interface{})
	if !ok {
		return 0, fmt.Errorf("invalid block data format")
	}

	numberHex, ok := blockData["number"].(string)
	if !ok {
		return 0, fmt.Errorf("invalid block number format")
	}

	return util.ParseBlockNumber(numberHex)
}

// GetCurrentHeight 获取当前高度
func (c *EVMClient) GetCurrentHeight(ctx context.Context) (uint64, error) {
	result, err := c.nodeManager.Call(ctx, "eth_blockNumber")
	if err != nil {
		return 0, fmt.Errorf("failed to get current block number: %w", err)
	}

	numberHex, ok := result.(string)
	if !ok {
		return 0, fmt.Errorf("invalid block number format")
	}

	return util.ParseBlockNumber(numberHex)
}

// BatchGetBlocks 批量获取区块
func (c *EVMClient) BatchGetBlocks(ctx context.Context, numbers []uint64) ([]*types.Block, error) {
	if len(numbers) == 0 {
		return []*types.Block{}, nil
	}

	startTime := time.Now()
	defer func() {
		metrics.RecordScanDuration(c.chainID, time.Since(startTime).Seconds())
	}()

	// 构建批量调用
	calls := make([]RPCCall, 0, len(numbers))
	for _, number := range numbers {
		calls = append(calls, RPCCall{
			Method: "eth_getBlockByNumber",
			Params: []interface{}{fmt.Sprintf("0x%x", number), true},
		})
	}

	results, err := c.nodeManager.BatchCall(ctx, calls)
	if err != nil {
		metrics.RecordScanError(c.chainID, "batch_get_blocks")
		return nil, fmt.Errorf("failed to batch get blocks: %w", err)
	}

	blocks := make([]*types.Block, 0, len(results))
	for i, result := range results {
		block, err := c.parseBlock(result)
		if err != nil {
			logger.Error("Failed to parse block in batch",
				zap.String("chain_id", c.chainID),
				zap.Uint64("block_number", numbers[i]),
				zap.Error(err))
			continue
		}
		blocks = append(blocks, block)
	}

	return blocks, nil
}

// BatchGetTransactions 批量获取交易
func (c *EVMClient) BatchGetTransactions(ctx context.Context, hashes []string) ([]*types.Transaction, error) {
	if len(hashes) == 0 {
		return []*types.Transaction{}, nil
	}

	startTime := time.Now()
	defer func() {
		metrics.RecordParseDuration(c.chainID, time.Since(startTime).Seconds())
	}()

	// 构建批量调用
	calls := make([]RPCCall, 0, len(hashes))
	for _, hash := range hashes {
		calls = append(calls, RPCCall{
			Method: "eth_getTransactionByHash",
			Params: []interface{}{hash},
		})
	}

	results, err := c.nodeManager.BatchCall(ctx, calls)
	if err != nil {
		metrics.RecordParseError(c.chainID, "batch_get_transactions")
		return nil, fmt.Errorf("failed to batch get transactions: %w", err)
	}

	transactions := make([]*types.Transaction, 0, len(results))
	for i, result := range results {
		tx, err := c.parseTransaction(result)
		if err != nil {
			logger.Error("Failed to parse transaction in batch",
				zap.String("chain_id", c.chainID),
				zap.String("tx_hash", hashes[i]),
				zap.Error(err))
			continue
		}
		transactions = append(transactions, tx)
	}

	return transactions, nil
}

// HealthCheck 健康检查
func (c *EVMClient) HealthCheck(ctx context.Context) error {
	_, err := c.GetCurrentHeight(ctx)
	return err
}

// GetChainID 获取链 ID
func (c *EVMClient) GetChainID(ctx context.Context) (string, error) {
	result, err := c.nodeManager.Call(ctx, "eth_chainId")
	if err != nil {
		return "", fmt.Errorf("failed to get chain ID: %w", err)
	}

	chainIDHex, ok := result.(string)
	if !ok {
		return "", fmt.Errorf("invalid chain ID format")
	}

	chainID, err := util.ParseBigInt(chainIDHex)
	if err != nil {
		return "", fmt.Errorf("failed to parse chain ID: %w", err)
	}

	return chainID.String(), nil
}

// GetBlockTime 获取区块时间
func (c *EVMClient) GetBlockTime(ctx context.Context) (time.Duration, error) {
	return c.config.BlockTime, nil
}

// DetectReorg 检测回滚
func (c *EVMClient) DetectReorg(ctx context.Context, oldBlockNumber uint64, oldBlockHash string) (bool, uint64, error) {
	currentBlock, err := c.GetBlock(ctx, oldBlockNumber)
	if err != nil {
		return false, 0, fmt.Errorf("failed to get current block: %w", err)
	}

	if currentBlock.Hash != oldBlockHash {
		// 发生回滚，需要找到共同祖先
		commonAncestor, err := c.findCommonAncestor(ctx, oldBlockNumber)
		if err != nil {
			return false, 0, fmt.Errorf("failed to find common ancestor: %w", err)
		}

		reorgDepth := oldBlockNumber - commonAncestor
		logger.Warn("Reorg detected",
			zap.String("chain_id", c.chainID),
			zap.Uint64("old_block_number", oldBlockNumber),
			zap.String("old_block_hash", oldBlockHash),
			zap.String("new_block_hash", currentBlock.Hash),
			zap.Uint64("reorg_depth", reorgDepth))

		return true, commonAncestor, nil
	}

	return false, oldBlockNumber, nil
}

// findCommonAncestor 找到共同祖先
func (c *EVMClient) findCommonAncestor(ctx context.Context, startBlock uint64) (uint64, error) {
	// 从当前区块向下查找，直到找到共同祖先
	for blockNumber := startBlock; blockNumber > 0; blockNumber-- {
		_, err := c.GetBlock(ctx, blockNumber)
		if err != nil {
			continue
		}

		// 检查这个区块是否在我们的数据库中
		// 这里需要查询数据库来验证
		// 暂时返回当前区块号
		return blockNumber, nil
	}

	return 0, fmt.Errorf("no common ancestor found")
}

// parseBlock 解析区块数据
func (c *EVMClient) parseBlock(result interface{}) (*types.Block, error) {
	blockData, ok := result.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("invalid block data format")
	}

	// 解析区块号
	numberHex, ok := blockData["number"].(string)
	if !ok {
		return nil, fmt.Errorf("invalid block number format")
	}
	number, err := util.ParseBlockNumber(numberHex)
	if err != nil {
		return nil, fmt.Errorf("failed to parse block number: %w", err)
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
	timestamp, err := util.ParseBigInt(timestampHex)
	if err != nil {
		return nil, fmt.Errorf("failed to parse timestamp: %w", err)
	}

	block := &types.Block{
		ChainID:    c.chainID,
		Number:     number,
		Hash:       hash,
		ParentHash: parentHash,
		Timestamp:  util.TimestampToTime(timestamp.Int64()),
		RawData:    blockData,
	}

	// 解析交易
	transactions, ok := blockData["transactions"].([]interface{})
	if ok {
		block.Transactions = make([]types.Transaction, 0, len(transactions))
		for i, txData := range transactions {
			tx, err := c.parseTransaction(txData)
			if err != nil {
				logger.Error("Failed to parse transaction",
					zap.String("chain_id", c.chainID),
					zap.Uint64("block_number", number),
					zap.Int("tx_index", i),
					zap.Error(err))
				continue
			}
			tx.BlockNumber = number
			tx.BlockHash = hash
			tx.Index = uint64(i)
			block.Transactions = append(block.Transactions, *tx)
		}
	}

	return block, nil
}

// parseTransaction 解析交易数据
func (c *EVMClient) parseTransaction(result interface{}) (*types.Transaction, error) {
	txData, ok := result.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("invalid transaction data format")
	}

	// 解析交易哈希
	hash, ok := txData["hash"].(string)
	if !ok {
		return nil, fmt.Errorf("invalid transaction hash format")
	}

	// 解析发送方地址
	from, ok := txData["from"].(string)
	if !ok {
		return nil, fmt.Errorf("invalid from address format")
	}

	// 解析接收方地址
	to, _ := txData["to"].(string) // to 可能为空（合约创建）

	// 解析值
	valueHex, ok := txData["value"].(string)
	if !ok {
		return nil, fmt.Errorf("invalid value format")
	}
	value, err := util.ParseBigInt(valueHex)
	if err != nil {
		return nil, fmt.Errorf("failed to parse value: %w", err)
	}

	// 解析 Gas 价格
	gasPriceHex, ok := txData["gasPrice"].(string)
	if !ok {
		return nil, fmt.Errorf("invalid gas price format")
	}
	gasPrice, err := util.ParseBigInt(gasPriceHex)
	if err != nil {
		return nil, fmt.Errorf("failed to parse gas price: %w", err)
	}

	// 解析输入数据
	inputData, _ := txData["input"].(string)

	transaction := &types.Transaction{
		ChainID:   c.chainID,
		Hash:      hash,
		From:      from,
		To:        to,
		Value:     value,
		GasPrice:  gasPrice,
		InputData: inputData,
		Status:    types.TxStatusPending,
		RawData:   txData,
	}

	return transaction, nil
}

// parseTransactionReceipt 解析交易回执
func (c *EVMClient) parseTransactionReceipt(result interface{}) (*types.Transaction, error) {
	receiptData, ok := result.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("invalid receipt data format")
	}

	// 解析交易哈希
	hash, ok := receiptData["transactionHash"].(string)
	if !ok {
		return nil, fmt.Errorf("invalid transaction hash format")
	}

	// 解析区块号
	blockNumberHex, ok := receiptData["blockNumber"].(string)
	if !ok {
		return nil, fmt.Errorf("invalid block number format")
	}
	blockNumber, err := util.ParseBlockNumber(blockNumberHex)
	if err != nil {
		return nil, fmt.Errorf("failed to parse block number: %w", err)
	}

	// 解析区块哈希
	blockHash, ok := receiptData["blockHash"].(string)
	if !ok {
		return nil, fmt.Errorf("invalid block hash format")
	}

	// 解析状态
	statusHex, ok := receiptData["status"].(string)
	if !ok {
		return nil, fmt.Errorf("invalid status format")
	}
	status, err := util.ParseBigInt(statusHex)
	if err != nil {
		return nil, fmt.Errorf("failed to parse status: %w", err)
	}

	var txStatus types.TransactionStatus
	if status.Int64() == 1 {
		txStatus = types.TxStatusConfirmed
	} else {
		txStatus = types.TxStatusReverted
	}

	// 解析 Gas 使用量
	gasUsedHex, ok := receiptData["gasUsed"].(string)
	if !ok {
		return nil, fmt.Errorf("invalid gas used format")
	}
	gasUsed, err := util.ParseBigInt(gasUsedHex)
	if err != nil {
		return nil, fmt.Errorf("failed to parse gas used: %w", err)
	}

	// 解析交易索引
	txIndexHex, ok := receiptData["transactionIndex"].(string)
	if !ok {
		return nil, fmt.Errorf("invalid transaction index format")
	}
	txIndex, err := util.ParseBigInt(txIndexHex)
	if err != nil {
		return nil, fmt.Errorf("failed to parse transaction index: %w", err)
	}

	transaction := &types.Transaction{
		ChainID:     c.chainID,
		Hash:        hash,
		BlockNumber: blockNumber,
		BlockHash:   blockHash,
		Status:      txStatus,
		GasUsed:     gasUsed.Uint64(),
		Index:       txIndex.Uint64(),
		RawData:     receiptData,
	}

	// 解析日志
	logs, ok := receiptData["logs"].([]interface{})
	if ok {
		transaction.Logs = make([]types.Log, 0, len(logs))
		for _, logData := range logs {
			log, err := c.parseLog(logData)
			if err != nil {
				logger.Error("Failed to parse log",
					zap.String("chain_id", c.chainID),
					zap.String("tx_hash", hash),
					zap.Error(err))
				continue
			}
			transaction.Logs = append(transaction.Logs, *log)
		}
	}

	return transaction, nil
}

// parseLog 解析日志数据
func (c *EVMClient) parseLog(result interface{}) (*types.Log, error) {
	logData, ok := result.(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("invalid log data format")
	}

	// 解析交易哈希
	txHash, ok := logData["transactionHash"].(string)
	if !ok {
		return nil, fmt.Errorf("invalid transaction hash format")
	}

	// 解析区块号
	blockNumberHex, ok := logData["blockNumber"].(string)
	if !ok {
		return nil, fmt.Errorf("invalid block number format")
	}
	blockNumber, err := util.ParseBlockNumber(blockNumberHex)
	if err != nil {
		return nil, fmt.Errorf("failed to parse block number: %w", err)
	}

	// 解析地址
	address, ok := logData["address"].(string)
	if !ok {
		return nil, fmt.Errorf("invalid address format")
	}

	// 解析主题
	topics, ok := logData["topics"].([]interface{})
	if !ok {
		return nil, fmt.Errorf("invalid topics format")
	}
	topicStrings := make([]string, 0, len(topics))
	for _, topic := range topics {
		topicStr, ok := topic.(string)
		if !ok {
			continue
		}
		topicStrings = append(topicStrings, topicStr)
	}

	// 解析数据
	data, _ := logData["data"].(string)

	// 解析日志索引
	logIndexHex, ok := logData["logIndex"].(string)
	if !ok {
		return nil, fmt.Errorf("invalid log index format")
	}
	logIndex, err := util.ParseBigInt(logIndexHex)
	if err != nil {
		return nil, fmt.Errorf("failed to parse log index: %w", err)
	}

	// 解析交易索引
	txIndexHex, ok := logData["transactionIndex"].(string)
	if !ok {
		return nil, fmt.Errorf("invalid transaction index format")
	}
	txIndex, err := util.ParseBigInt(txIndexHex)
	if err != nil {
		return nil, fmt.Errorf("failed to parse transaction index: %w", err)
	}

	log := &types.Log{
		ChainID:     c.chainID,
		TxHash:      txHash,
		BlockNumber: blockNumber,
		Address:     address,
		Topics:      topicStrings,
		Data:        data,
		LogIndex:    logIndex.Uint64(),
		TxIndex:     txIndex.Uint64(),
	}

	return log, nil
}

// Close 关闭客户端
func (c *EVMClient) Close() error {
	return c.nodeManager.Close()
}

// NodeManager 返回节点管理器
func (c *EVMClient) NodeManager() *NodeManager {
	return c.nodeManager
}