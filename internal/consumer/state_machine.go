package consumer

import (
	"context"
	"fmt"
	"time"

	"github.com/smart-scanner/multi-chain-scanner/internal/storage"
	"github.com/smart-scanner/multi-chain-scanner/pkg/logger"
	"github.com/smart-scanner/multi-chain-scanner/pkg/types"
	"go.uber.org/zap"
)

// StateMachine 状态机接口
// [Design: StateMachine 状态机](../docs/DESIGN_SCANNER.md#63-statemachine-状态机)
type StateMachine interface {
	// ProcessTransaction 处理交易状态转换
	ProcessTransaction(ctx context.Context, tx *types.Transaction) error
	
	// GetTransactionState 获取交易状态
	GetTransactionState(ctx context.Context, chainID string, txHash string) (types.TransactionStatus, error)
	
	// ForceSetState 强制设置交易状态
	ForceSetState(ctx context.Context, chainID string, txHash string, status types.TransactionStatus) error
	
	// GetStateStatistics 获取状态统计信息
	GetStateStatistics(ctx context.Context, chainID string) (*StateStatistics, error)
}

// DefaultStateMachine 默认状态机实现
type DefaultStateMachine struct {
	storage storage.Storage
}

// NewDefaultStateMachine 创建默认状态机
func NewDefaultStateMachine(storage storage.Storage) *DefaultStateMachine {
	return &DefaultStateMachine{
		storage: storage,
	}
}

// ProcessTransaction 处理交易状态转换
func (sm *DefaultStateMachine) ProcessTransaction(ctx context.Context, tx *types.Transaction) error {
	// 获取当前状态
	currentStatus, err := sm.GetTransactionState(ctx, tx.ChainID, tx.Hash)
	if err != nil {
		return fmt.Errorf("failed to get current state: %w", err)
	}

	// 检查状态转换是否合法
	if err := sm.validateStateTransition(currentStatus, tx.Status); err != nil {
		return fmt.Errorf("invalid state transition: %w", err)
	}

	// 执行状态转换
	if err := sm.executeStateTransition(ctx, tx); err != nil {
		return fmt.Errorf("failed to execute state transition: %w", err)
	}

	logger.Info("Transaction state transition completed",
		zap.String("chain_id", tx.ChainID),
		zap.String("tx_hash", tx.Hash),
		zap.String("from_status", string(currentStatus)),
		zap.String("to_status", string(tx.Status)))

	return nil
}

// validateStateTransition 验证状态转换是否合法
func (sm *DefaultStateMachine) validateStateTransition(currentStatus, newStatus types.TransactionStatus) error {
	// 定义合法的状态转换
	validTransitions := map[types.TransactionStatus][]types.TransactionStatus{
		types.TxStatusPending: {
			types.TxStatusConfirmed,
			types.TxStatusReverted,
		},
		types.TxStatusConfirmed: {
			types.TxStatusFinalized,
			types.TxStatusReverted,
		},
		types.TxStatusFinalized: {
			// Finalized 状态不能转换到其他状态
		},
		types.TxStatusReverted: {
			// Reverted 状态不能转换到其他状态
		},
	}

	// 检查转换是否合法
	allowedStatuses, exists := validTransitions[currentStatus]
	if !exists {
		return fmt.Errorf("unknown current status: %s", currentStatus)
	}

	for _, allowedStatus := range allowedStatuses {
		if allowedStatus == newStatus {
			return nil
		}
	}

	return fmt.Errorf("invalid state transition from %s to %s", currentStatus, newStatus)
}

// executeStateTransition 执行状态转换
func (sm *DefaultStateMachine) executeStateTransition(ctx context.Context, tx *types.Transaction) error {
	// 更新交易状态
	if err := sm.storage.UpdateTransactionStatus(ctx, tx.ChainID, tx.Hash, tx.Status); err != nil {
		return fmt.Errorf("failed to update transaction status: %w", err)
	}

	// 根据状态执行不同的业务逻辑
	switch tx.Status {
	case types.TxStatusConfirmed:
		return sm.handleConfirmed(ctx, tx)
	case types.TxStatusFinalized:
		return sm.handleFinalized(ctx, tx)
	case types.TxStatusReverted:
		return sm.handleReverted(ctx, tx)
	default:
		return nil
	}
}

// handleConfirmed 处理已确认状态
func (sm *DefaultStateMachine) handleConfirmed(ctx context.Context, tx *types.Transaction) error {
	logger.Debug("Handling confirmed transaction",
		zap.String("chain_id", tx.ChainID),
		zap.String("tx_hash", tx.Hash))

	// 这里可以添加已确认状态的业务逻辑
	// 例如：更新临时余额、发送通知等

	return nil
}

// handleFinalized 处理已最终确认状态
func (sm *DefaultStateMachine) handleFinalized(ctx context.Context, tx *types.Transaction) error {
	logger.Debug("Handling finalized transaction",
		zap.String("chain_id", tx.ChainID),
		zap.String("tx_hash", tx.Hash))

	// 这里可以添加已最终确认状态的业务逻辑
	// 例如：更新最终余额、记录流水、触发清算等

	return nil
}

// handleReverted 处理已回滚状态
func (sm *DefaultStateMachine) handleReverted(ctx context.Context, tx *types.Transaction) error {
	logger.Warn("Handling reverted transaction",
		zap.String("chain_id", tx.ChainID),
		zap.String("tx_hash", tx.Hash))

	// 这里可以添加已回滚状态的业务逻辑
	// 例如：冲正余额、取消订单、发送警报等

	return nil
}

// GetTransactionState 获取交易状态
func (sm *DefaultStateMachine) GetTransactionState(ctx context.Context, chainID string, txHash string) (types.TransactionStatus, error) {
	tx, err := sm.storage.GetTransaction(ctx, chainID, txHash)
	if err != nil {
		return "", fmt.Errorf("failed to get transaction: %w", err)
	}

	return tx.Status, nil
}

// ForceSetState 强制设置交易状态
func (sm *DefaultStateMachine) ForceSetState(ctx context.Context, chainID string, txHash string, status types.TransactionStatus) error {
	logger.Warn("Force setting transaction state",
		zap.String("chain_id", chainID),
		zap.String("tx_hash", txHash),
		zap.String("status", string(status)))

	// 更新交易状态
	if err := sm.storage.UpdateTransactionStatus(ctx, chainID, txHash, status); err != nil {
		return fmt.Errorf("failed to update transaction status: %w", err)
	}

	return nil
}

// GetStateStatistics 获取状态统计信息
func (sm *DefaultStateMachine) GetStateStatistics(ctx context.Context, chainID string) (*StateStatistics, error) {
	// 这里应该从数据库统计不同状态的交易数量
	// 由于我们的 storage 接口没有直接支持统计功能，
	// 这里返回一个简化的实现
	
	stats := &StateStatistics{
		ChainID:           chainID,
		PendingCount:      0,
		ConfirmedCount:    0,
		FinalizedCount:    0,
		RevertedCount:     0,
		TotalCount:        0,
		LastUpdatedTime:   time.Now(),
	}

	return stats, nil
}

// StateStatistics 状态统计信息
type StateStatistics struct {
	ChainID         string                `json:"chain_id"`
	PendingCount    int64                 `json:"pending_count"`
	ConfirmedCount  int64                 `json:"confirmed_count"`
	FinalizedCount  int64                 `json:"finalized_count"`
	RevertedCount   int64                 `json:"reverted_count"`
	TotalCount      int64                 `json:"total_count"`
	LastUpdatedTime time.Time             `json:"last_updated_time"`
}

// StateTransitionRule 状态转换规则
type StateTransitionRule struct {
	FromStatus types.TransactionStatus `json:"from_status"`
	ToStatus   types.TransactionStatus `json:"to_status"`
	Condition  string                 `json:"condition"`
	Action     string                 `json:"action"`
}

// CustomStateMachine 自定义状态机实现
type CustomStateMachine struct {
	storage storage.Storage
	rules   []StateTransitionRule
}

// NewCustomStateMachine 创建自定义状态机
func NewCustomStateMachine(storage storage.Storage, rules []StateTransitionRule) *CustomStateMachine {
	return &CustomStateMachine{
		storage: storage,
		rules:   rules,
	}
}

// ProcessTransaction 处理交易状态转换
func (sm *CustomStateMachine) ProcessTransaction(ctx context.Context, tx *types.Transaction) error {
	// 获取当前状态
	currentStatus, err := sm.GetTransactionState(ctx, tx.ChainID, tx.Hash)
	if err != nil {
		return fmt.Errorf("failed to get current state: %w", err)
	}

	// 查找适用的转换规则
	rule, err := sm.findTransitionRule(currentStatus, tx.Status)
	if err != nil {
		return err
	}

	// 执行状态转换
	if err := sm.executeStateTransition(ctx, tx, rule); err != nil {
		return fmt.Errorf("failed to execute state transition: %w", err)
	}

	logger.Info("Transaction state transition completed",
		zap.String("chain_id", tx.ChainID),
		zap.String("tx_hash", tx.Hash),
		zap.String("from_status", string(currentStatus)),
		zap.String("to_status", string(tx.Status)))

	return nil
}

// findTransitionRule 查找适用的转换规则
func (sm *CustomStateMachine) findTransitionRule(currentStatus, newStatus types.TransactionStatus) (*StateTransitionRule, error) {
	for _, rule := range sm.rules {
		if rule.FromStatus == currentStatus && rule.ToStatus == newStatus {
			return &rule, nil
		}
	}

	return nil, fmt.Errorf("no transition rule found from %s to %s", currentStatus, newStatus)
}

// executeStateTransition 执行状态转换
func (sm *CustomStateMachine) executeStateTransition(ctx context.Context, tx *types.Transaction, rule *StateTransitionRule) error {
	// 更新交易状态
	if err := sm.storage.UpdateTransactionStatus(ctx, tx.ChainID, tx.Hash, tx.Status); err != nil {
		return fmt.Errorf("failed to update transaction status: %w", err)
	}

	// 执行规则定义的动作
	if err := sm.executeAction(ctx, tx, rule.Action); err != nil {
		return fmt.Errorf("failed to execute action: %w", err)
	}

	return nil
}

// executeAction 执行动作
func (sm *CustomStateMachine) executeAction(ctx context.Context, tx *types.Transaction, action string) error {
	// 根据动作类型执行不同的逻辑
	// 这里可以扩展支持更多的动作类型
	
	logger.Debug("Executing action",
		zap.String("action", action),
		zap.String("tx_hash", tx.Hash))

	return nil
}

// GetTransactionState 获取交易状态
func (sm *CustomStateMachine) GetTransactionState(ctx context.Context, chainID string, txHash string) (types.TransactionStatus, error) {
	tx, err := sm.storage.GetTransaction(ctx, chainID, txHash)
	if err != nil {
		return "", fmt.Errorf("failed to get transaction: %w", err)
	}

	return tx.Status, nil
}

// ForceSetState 强制设置交易状态
func (sm *CustomStateMachine) ForceSetState(ctx context.Context, chainID string, txHash string, status types.TransactionStatus) error {
	logger.Warn("Force setting transaction state",
		zap.String("chain_id", chainID),
		zap.String("tx_hash", txHash),
		zap.String("status", string(status)))

	// 更新交易状态
	if err := sm.storage.UpdateTransactionStatus(ctx, chainID, txHash, status); err != nil {
		return fmt.Errorf("failed to update transaction status: %w", err)
	}

	return nil
}

// GetStateStatistics 获取状态统计信息
func (sm *CustomStateMachine) GetStateStatistics(ctx context.Context, chainID string) (*StateStatistics, error) {
	// 返回简化的统计信息
	stats := &StateStatistics{
		ChainID:         chainID,
		PendingCount:    0,
		ConfirmedCount:  0,
		FinalizedCount:  0,
		RevertedCount:   0,
		TotalCount:      0,
		LastUpdatedTime: time.Now(),
	}

	return stats, nil
}