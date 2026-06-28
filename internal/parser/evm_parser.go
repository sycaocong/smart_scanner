package parser

import (
	"context"
	"fmt"
	"time"

	"github.com/smart-scanner/multi-chain-scanner/pkg/logger"
	"github.com/smart-scanner/multi-chain-scanner/pkg/metrics"
	"github.com/smart-scanner/multi-chain-scanner/pkg/types"
	"github.com/smart-scanner/multi-chain-scanner/pkg/util"
	"go.uber.org/zap"
)

// EVMParser EVM 解析器
// [Design: Parser 解析器接口](../docs/DESIGN_SCANNER.md#51-parser-解析器接口)
type EVMParser struct {
	abiCache     map[string]interface{}
	filterConfig *types.FilterConfig
}

// NewEVMParser 创建 EVM 解析器
func NewEVMParser() *EVMParser {
	return &EVMParser{
		abiCache:     make(map[string]interface{}),
		filterConfig: &types.FilterConfig{},
	}
}

// SetFilterConfig 设置过滤配置
func (p *EVMParser) SetFilterConfig(config *types.FilterConfig) {
	p.filterConfig = config
}

// GetType 获取解析器类型
func (p *EVMParser) GetType() string {
	return string(types.ChainTypeEVM)
}

// ParseBlock 解析区块
func (p *EVMParser) ParseBlock(ctx context.Context, rawData interface{}) (*types.Block, error) {
	startTime := time.Now()
	defer func() {
		metrics.RecordParseDuration("evm", time.Since(startTime).Seconds())
	}()

	blockData, ok := rawData.(map[string]interface{})
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
		ChainID:    "", // 将由调用者设置
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
			tx, err := p.ParseTransaction(ctx, txData)
			if err != nil {
				logger.Error("Failed to parse transaction in block",
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

// ParseTransaction 解析交易
func (p *EVMParser) ParseTransaction(ctx context.Context, rawData interface{}) (*types.Transaction, error) {
	startTime := time.Now()
	defer func() {
		metrics.RecordParseDuration("evm", time.Since(startTime).Seconds())
	}()

	txData, ok := rawData.(map[string]interface{})
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
		ChainID:    "", // 将由调用者设置
		Hash:       hash,
		From:       from,
		To:         to,
		Value:      value,
		GasPrice:   gasPrice,
		InputData:  inputData,
		Status:     types.TxStatusPending,
		RawData:    txData,
	}

	// 应用过滤
	if !p.shouldIncludeTransaction(transaction) {
		return nil, fmt.Errorf("transaction filtered out")
	}

	return transaction, nil
}

// ParseLog 解析日志
func (p *EVMParser) ParseLog(ctx context.Context, rawData interface{}) (*types.Log, error) {
	logData, ok := rawData.(map[string]interface{})
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
		ChainID:     "", // 将由调用者设置
		TxHash:      txHash,
		BlockNumber: blockNumber,
		Address:     address,
		Topics:      topicStrings,
		Data:        data,
		LogIndex:    logIndex.Uint64(),
		TxIndex:     txIndex.Uint64(),
	}

	// 应用过滤
	if !p.shouldIncludeLog(log) {
		return nil, fmt.Errorf("log filtered out")
	}

	return log, nil
}

// BatchParseTransactions 批量解析交易
func (p *EVMParser) BatchParseTransactions(ctx context.Context, rawDataList []interface{}) ([]*types.Transaction, error) {
	transactions := make([]*types.Transaction, 0, len(rawDataList))

	for i, rawData := range rawDataList {
		tx, err := p.ParseTransaction(ctx, rawData)
		if err != nil {
			logger.Error("Failed to parse transaction in batch",
				zap.Int("index", i),
				zap.Error(err))
			continue
		}
		transactions = append(transactions, tx)
	}

	return transactions, nil
}

// BatchParseLogs 批量解析日志
func (p *EVMParser) BatchParseLogs(ctx context.Context, rawDataList []interface{}) ([]*types.Log, error) {
	logs := make([]*types.Log, 0, len(rawDataList))

	for i, rawData := range rawDataList {
		log, err := p.ParseLog(ctx, rawData)
		if err != nil {
			logger.Error("Failed to parse log in batch",
				zap.Int("index", i),
				zap.Error(err))
			continue
		}
		logs = append(logs, log)
	}

	return logs, nil
}

// ValidateBlock 验证区块
func (p *EVMParser) ValidateBlock(block *types.Block) error {
	if block.Number == 0 {
		return fmt.Errorf("block number cannot be zero")
	}
	if block.Hash == "" {
		return fmt.Errorf("block hash cannot be empty")
	}
	if block.ParentHash == "" {
		return fmt.Errorf("parent hash cannot be empty")
	}
	if block.Timestamp.IsZero() {
		return fmt.Errorf("block timestamp cannot be zero")
	}
	return nil
}

// ValidateTransaction 验证交易
func (p *EVMParser) ValidateTransaction(tx *types.Transaction) error {
	if tx.Hash == "" {
		return fmt.Errorf("transaction hash cannot be empty")
	}
	if tx.From == "" {
		return fmt.Errorf("from address cannot be empty")
	}
	if !util.IsValidAddress(tx.From) {
		return fmt.Errorf("invalid from address format: %s", tx.From)
	}
	if tx.To != "" && !util.IsValidAddress(tx.To) {
		return fmt.Errorf("invalid to address format: %s", tx.To)
	}
	if tx.Value == nil {
		return fmt.Errorf("value cannot be nil")
	}
	return nil
}

// ValidateLog 验证日志
func (p *EVMParser) ValidateLog(log *types.Log) error {
	if log.TxHash == "" {
		return fmt.Errorf("transaction hash cannot be empty")
	}
	if log.BlockNumber == 0 {
		return fmt.Errorf("block number cannot be zero")
	}
	if log.Address == "" {
		return fmt.Errorf("address cannot be empty")
	}
	if !util.IsValidAddress(log.Address) {
		return fmt.Errorf("invalid address format: %s", log.Address)
	}
	if len(log.Topics) == 0 {
		return fmt.Errorf("topics cannot be empty")
	}
	return nil
}

// shouldIncludeTransaction 判断是否应该包含该交易
func (p *EVMParser) shouldIncludeTransaction(tx *types.Transaction) bool {
	// 检查地址过滤
	if len(p.filterConfig.Addresses) > 0 {
		fromIncluded := util.ContainsString(p.filterConfig.Addresses, tx.From)
		toIncluded := tx.To != "" && util.ContainsString(p.filterConfig.Addresses, tx.To)
		if !fromIncluded && !toIncluded {
			return false
		}
	}

	// 检查合约过滤
	if len(p.filterConfig.Contracts) > 0 && tx.To != "" {
		if !util.ContainsString(p.filterConfig.Contracts, tx.To) {
			return false
		}
	}

	// 检查最小金额过滤
	if p.filterConfig.MinValue != nil && tx.Value != nil {
		if tx.Value.Cmp(p.filterConfig.MinValue) < 0 {
			return false
		}
	}

	// 检查是否排除空交易
	if p.filterConfig.ExcludeEmpty {
		if tx.To == "" || (tx.Value != nil && tx.Value.Sign() == 0) {
			return false
		}
	}

	return true
}

// shouldIncludeLog 判断是否应该包含该日志
func (p *EVMParser) shouldIncludeLog(log *types.Log) bool {
	// 检查地址过滤
	if len(p.filterConfig.Addresses) > 0 {
		if !util.ContainsString(p.filterConfig.Addresses, log.Address) {
			return false
		}
	}

	// 检查合约过滤
	if len(p.filterConfig.Contracts) > 0 {
		if !util.ContainsString(p.filterConfig.Contracts, log.Address) {
			return false
		}
	}

	// 检查事件过滤（通过主题签名）
	if len(p.filterConfig.Events) > 0 && len(log.Topics) > 0 {
		eventSignature := log.Topics[0]
		if !util.ContainsString(p.filterConfig.Events, eventSignature) {
			return false
		}
	}

	return true
}

// ParseEvent 解析事件
func (p *EVMParser) ParseEvent(ctx context.Context, log *types.Log, abi interface{}) (map[string]interface{}, error) {
	// TODO: 实现 ABI 解析逻辑
	// 这里需要根据 ABI 解析事件的参数
	return nil, fmt.Errorf("not implemented")
}

// ParseFunctionCall 解析函数调用
func (p *EVMParser) ParseFunctionCall(ctx context.Context, inputData string, abi interface{}) (map[string]interface{}, error) {
	// TODO: 实现 ABI 解析逻辑
	// 这里需要根据 ABI 解析函数调用的参数
	return nil, fmt.Errorf("not implemented")
}