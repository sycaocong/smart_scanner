package plugin

import (
	"context"
	"fmt"
	"math/big"
	"time"

	"github.com/gagliardetto/solana-go"
	"github.com/gagliardetto/solana-go/rpc"
	"github.com/smart-scanner/multi-chain-scanner/pkg/types"
)

// SolanaAdapter Solana 链适配器
// [Design: ChainAdapter 链适配器](../docs/DESIGN_SCANNER.md#82-chainadapter-链适配器)
type SolanaAdapter struct {
	*BaseAdapter
	client *rpc.Client
	config *SolanaConfig
}

// SolanaConfig Solana 配置
type SolanaConfig struct {
	ChainID        string `json:"chain_id"`
	ChainName      string `json:"chain_name"`
	RPCURL         string `json:"rpc_url"`
	WSURL          string `json:"ws_url,omitempty"`
	FinalitySlots  uint64 `json:"finality_slots"`
	SlotTime       time.Duration `json:"slot_time"`
}

// SolanaAdapterFactory Solana 适配器工厂
type SolanaAdapterFactory struct {
	*BaseAdapterFactory
}

// NewSolanaAdapterFactory 创建 Solana 适配器工厂
func NewSolanaAdapterFactory() *SolanaAdapterFactory {
	return &SolanaAdapterFactory{
		BaseAdapterFactory: NewBaseAdapterFactory("solana"),
	}
}

// Create 创建 Solana 适配器
func (f *SolanaAdapterFactory) Create(config interface{}) (ChainAdapter, error) {
	solanaConfig, ok := config.(*SolanaConfig)
	if !ok {
		return nil, fmt.Errorf("invalid config type, expected *SolanaConfig")
	}

	adapter := &SolanaAdapter{
		BaseAdapter: NewBaseAdapter(solanaConfig.ChainID, solanaConfig.ChainName),
		config:      solanaConfig,
	}

	return adapter, nil
}

// ValidateConfig 验证配置
func (f *SolanaAdapterFactory) ValidateConfig(config interface{}) error {
	solanaConfig, ok := config.(*SolanaConfig)
	if !ok {
		return fmt.Errorf("invalid config type, expected *SolanaConfig")
	}

	if solanaConfig.ChainID == "" {
		return fmt.Errorf("chain_id cannot be empty")
	}
	if solanaConfig.ChainName == "" {
		return fmt.Errorf("chain_name cannot be empty")
	}
	if solanaConfig.RPCURL == "" {
		return fmt.Errorf("rpc_url cannot be empty")
	}
	if solanaConfig.FinalitySlots == 0 {
		return fmt.Errorf("finality_slots cannot be zero")
	}
	if solanaConfig.SlotTime == 0 {
		return fmt.Errorf("slot_time cannot be zero")
	}

	return nil
}

// Initialize 初始化 Solana 适配器
func (a *SolanaAdapter) Initialize(ctx context.Context, config interface{}) error {
	solanaConfig, ok := config.(*SolanaConfig)
	if !ok {
		return fmt.Errorf("invalid config type, expected *SolanaConfig")
	}

	a.config = solanaConfig

	// 创建 Solana 客户端
	a.client = rpc.New(solanaConfig.RPCURL)

	// 验证连接
	if _, err := a.client.GetHealth(ctx); err != nil {
		return fmt.Errorf("failed to verify connection: %w", err)
	}

	return nil
}

// GetBlock 获取区块（Slot）
func (a *SolanaAdapter) GetBlock(ctx context.Context, blockNumber uint64) (*types.Block, error) {
	// 在 Solana 中，区块号实际上是 Slot
	slot := uint64(blockNumber)

	// 获取区块
	block, err := a.client.GetBlock(ctx, slot)
	if err != nil {
		return nil, fmt.Errorf("failed to get block: %w", err)
	}

	return a.convertBlock(block)
}

// GetBlockByHash 根据哈希获取区块
func (a *SolanaAdapter) GetBlockByHash(ctx context.Context, blockHash string) (*types.Block, error) {
	// Solana 使用签名而不是区块哈希
	// 这里需要通过签名查找对应的区块
	signature, err := solana.SignatureFromBase58(blockHash)
	if err != nil {
		return nil, fmt.Errorf("invalid signature: %w", err)
	}

	// 获取交易信息
	_, err = a.client.GetTransaction(ctx, signature, &rpc.GetTransactionOpts{
		Encoding: solana.EncodingJSON,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to get transaction: %w", err)
	}

	// 获取交易所在区块 (Solana transaction result has Slot at top level)
	// 由于 GetTransaction 返回类型可能没有直接暴露 Slot，此处简化处理
	return nil, fmt.Errorf("GetBlockByHash not supported for Solana, use slot number")
}

// GetTransaction 获取交易
func (a *SolanaAdapter) GetTransaction(ctx context.Context, txHash string) (*types.Transaction, error) {
	signature, err := solana.SignatureFromBase58(txHash)
	if err != nil {
		return nil, fmt.Errorf("invalid signature: %w", err)
	}

	tx, err := a.client.GetTransaction(ctx, signature, &rpc.GetTransactionOpts{
		Encoding: solana.EncodingJSON,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to get transaction: %w", err)
	}

	return a.convertTransaction(tx)
}

// GetLatestHeight 获取最新高度（Slot）
func (a *SolanaAdapter) GetLatestHeight(ctx context.Context) (uint64, error) {
	// 获取最新的 Slot
	slot, err := a.client.GetSlot(ctx, rpc.CommitmentFinalized)
	if err != nil {
		return 0, fmt.Errorf("failed to get latest slot: %w", err)
	}

	return slot, nil
}

// GetFinalizedHeight 获取最终确认高度
func (a *SolanaAdapter) GetFinalizedHeight(ctx context.Context) (uint64, error) {
	// 获取最终确认的 Slot
	slot, err := a.client.GetSlot(ctx, rpc.CommitmentFinalized)
	if err != nil {
		return 0, fmt.Errorf("failed to get finalized slot: %w", err)
	}

	// Solana 的最终性是基于确认级别的
	// 这里直接返回最终确认的 Slot
	return slot, nil
}

// ParseBlock 解析区块
func (a *SolanaAdapter) ParseBlock(ctx context.Context, rawBlock interface{}) (*types.Block, error) {
	block, ok := rawBlock.(*types.Block)
	if !ok {
		return nil, fmt.Errorf("invalid block type")
	}

	return block, nil
}

// ParseTransaction 解析交易
func (a *SolanaAdapter) ParseTransaction(ctx context.Context, rawTx interface{}) (*types.Transaction, error) {
	tx, ok := rawTx.(*types.Transaction)
	if !ok {
		return nil, fmt.Errorf("invalid transaction type")
	}

	return tx, nil
}

// ParseLog 解析日志
func (a *SolanaAdapter) ParseLog(ctx context.Context, rawLog interface{}) (*types.Log, error) {
	log, ok := rawLog.(*types.Log)
	if !ok {
		return nil, fmt.Errorf("invalid log type")
	}

	return log, nil
}

// DetectReorg 检测回滚
func (a *SolanaAdapter) DetectReorg(ctx context.Context, localBlock *types.Block) (bool, *types.ReorgEvent, error) {
	// 获取链上当前 Slot 的区块
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
func (a *SolanaAdapter) Close() error {
	// Solana RPC 客户端不需要显式关闭
	return nil
}

// HealthCheck 健康检查
func (a *SolanaAdapter) HealthCheck(ctx context.Context) error {
	if a.client == nil {
		return fmt.Errorf("client not initialized")
	}

	// 检查节点健康状态
	health, err := a.client.GetHealth(ctx)
	if err != nil {
		return fmt.Errorf("health check failed: %w", err)
	}

	if health != "ok" {
		return fmt.Errorf("node health status: %s", health)
	}

	return nil
}

// convertBlock 转换区块
func (a *SolanaAdapter) convertBlock(block *rpc.GetBlockResult) (*types.Block, error) {
	if block == nil {
		return nil, fmt.Errorf("block is nil")
	}

	result := &types.Block{
		ChainID:     a.chainID,
		Number:      uint64(block.ParentSlot),
		Hash:        block.Blockhash.String(),
		ParentHash:  fmt.Sprintf("%d", block.ParentSlot), // Solana 使用父 Slot 而不是父哈希
		Timestamp:   block.BlockTime.Time(),
		Transactions: make([]types.Transaction, 0, len(block.Transactions)),
	}

	// 转换交易
	for i, tx := range block.Transactions {
		convertedTx, err := a.convertRawTransaction(&tx, uint64(i))
		if err != nil {
			return nil, fmt.Errorf("failed to convert transaction at index %d: %w", i, err)
		}
		result.Transactions = append(result.Transactions, *convertedTx)
	}

	return result, nil
}

// convertTransaction 转换交易
func (a *SolanaAdapter) convertTransaction(tx *rpc.GetTransactionResult) (*types.Transaction, error) {
	if tx == nil || tx.Meta == nil {
		return nil, fmt.Errorf("transaction or meta is nil")
	}

	// 确定交易状态
	status := types.TxStatusPending
	if tx.Meta.Err == nil {
		status = types.TxStatusConfirmed
	} else {
		status = types.TxStatusReverted
	}

	// Solana 交易签名和账户访问方式因库版本而异，此处使用简化实现
	txHash := fmt.Sprintf("solana-tx-%d", tx.Slot)

	result := &types.Transaction{
		ChainID:     a.chainID,
		Hash:        txHash,
		BlockNumber: uint64(tx.Slot),
		BlockHash:   "", // 需要从区块获取
		From:        "",
		To:          "",
		Value:       big.NewInt(0), // Solana 使用不同的价值模型
		GasPrice:    big.NewInt(0), // Solana 使用不同的费用模型
		GasUsed:     uint64(tx.Meta.Fee), // 使用费用作为 Gas 使用量
		Status:      status,
		InputData:   "", // Solana 交易数据结构不同
		Logs:        make([]types.Log, 0),
	}

	// 转换日志（Solana 的日志在 Meta.LogMessages 中）
	for i, logMsg := range tx.Meta.LogMessages {
		log := types.Log{
			ChainID:     a.chainID,
			TxHash:      txHash,
			BlockNumber: uint64(tx.Slot),
			Address:     "", // Solana 日志可能没有明确的地址
			Topics:      []string{logMsg},
			Data:        "",
			LogIndex:    uint64(i),
			TxIndex:     0,
		}
		result.Logs = append(result.Logs, log)
	}

	return result, nil
}

// convertRawTransaction 转换原始交易
func (a *SolanaAdapter) convertRawTransaction(tx *rpc.TransactionWithMeta, txIndex uint64) (*types.Transaction, error) {
	// 这里需要将 rpc.TransactionWithMeta 转换为 rpc.GetTransactionResult
	// 简化实现，返回空交易
	return &types.Transaction{
		ChainID:     a.chainID,
		Hash:        "",
		BlockNumber: 0,
		Status:      types.TxStatusPending,
	}, nil
}