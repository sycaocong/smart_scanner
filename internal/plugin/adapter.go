package plugin

import (
	"context"
	"fmt"

	"github.com/smart-scanner/multi-chain-scanner/pkg/types"
)

// ChainAdapter 链适配器接口
// [Design: ChainAdapter 链适配器](../docs/DESIGN_SCANNER.md#82-chainadapter-链适配器)
type ChainAdapter interface {
	// GetChainID 获取链ID
	GetChainID() string
	
	// GetChainName 获取链名称
	GetChainName() string
	
	// GetBlock 获取区块
	GetBlock(ctx context.Context, blockNumber uint64) (*types.Block, error)
	
	// GetBlockByHash 根据哈希获取区块
	GetBlockByHash(ctx context.Context, blockHash string) (*types.Block, error)
	
	// GetTransaction 获取交易
	GetTransaction(ctx context.Context, txHash string) (*types.Transaction, error)
	
	// GetLatestHeight 获取最新高度
	GetLatestHeight(ctx context.Context) (uint64, error)
	
	// GetFinalizedHeight 获取最终确认高度
	GetFinalizedHeight(ctx context.Context) (uint64, error)
	
	// ParseBlock 解析区块
	ParseBlock(ctx context.Context, rawBlock interface{}) (*types.Block, error)
	
	// ParseTransaction 解析交易
	ParseTransaction(ctx context.Context, rawTx interface{}) (*types.Transaction, error)
	
	// ParseLog 解析日志
	ParseLog(ctx context.Context, rawLog interface{}) (*types.Log, error)
	
	// DetectReorg 检测回滚
	DetectReorg(ctx context.Context, localBlock *types.Block) (bool, *types.ReorgEvent, error)
	
	// ValidateConfig 验证配置
	ValidateConfig(config interface{}) error
	
	// Initialize 初始化适配器
	Initialize(ctx context.Context, config interface{}) error
	
	// Close 关闭适配器
	Close() error
	
	// HealthCheck 健康检查
	HealthCheck(ctx context.Context) error
}

// BaseAdapter 基础适配器实现
type BaseAdapter struct {
	chainID   string
	chainName string
	config    interface{}
}

// NewBaseAdapter 创建基础适配器
func NewBaseAdapter(chainID, chainName string) *BaseAdapter {
	return &BaseAdapter{
		chainID:   chainID,
		chainName: chainName,
	}
}

// GetChainID 获取链ID
func (a *BaseAdapter) GetChainID() string {
	return a.chainID
}

// GetChainName 获取链名称
func (a *BaseAdapter) GetChainName() string {
	return a.chainName
}

// Initialize 初始化适配器
func (a *BaseAdapter) Initialize(ctx context.Context, config interface{}) error {
	a.config = config
	return nil
}

// Close 关闭适配器
func (a *BaseAdapter) Close() error {
	return nil
}

// HealthCheck 健康检查
func (a *BaseAdapter) HealthCheck(ctx context.Context) error {
	return nil
}

// ValidateConfig 验证配置
func (a *BaseAdapter) ValidateConfig(config interface{}) error {
	return nil
}

// GetBlock 获取区块（基础实现，需要子类重写）
func (a *BaseAdapter) GetBlock(ctx context.Context, blockNumber uint64) (*types.Block, error) {
	return nil, fmt.Errorf("GetBlock not implemented")
}

// GetBlockByHash 根据哈希获取区块（基础实现，需要子类重写）
func (a *BaseAdapter) GetBlockByHash(ctx context.Context, blockHash string) (*types.Block, error) {
	return nil, fmt.Errorf("GetBlockByHash not implemented")
}

// GetTransaction 获取交易（基础实现，需要子类重写）
func (a *BaseAdapter) GetTransaction(ctx context.Context, txHash string) (*types.Transaction, error) {
	return nil, fmt.Errorf("GetTransaction not implemented")
}

// GetLatestHeight 获取最新高度（基础实现，需要子类重写）
func (a *BaseAdapter) GetLatestHeight(ctx context.Context) (uint64, error) {
	return 0, fmt.Errorf("GetLatestHeight not implemented")
}

// GetFinalizedHeight 获取最终确认高度（基础实现，需要子类重写）
func (a *BaseAdapter) GetFinalizedHeight(ctx context.Context) (uint64, error) {
	return 0, fmt.Errorf("GetFinalizedHeight not implemented")
}

// ParseBlock 解析区块（基础实现，需要子类重写）
func (a *BaseAdapter) ParseBlock(ctx context.Context, rawBlock interface{}) (*types.Block, error) {
	return nil, fmt.Errorf("ParseBlock not implemented")
}

// ParseTransaction 解析交易（基础实现，需要子类重写）
func (a *BaseAdapter) ParseTransaction(ctx context.Context, rawTx interface{}) (*types.Transaction, error) {
	return nil, fmt.Errorf("ParseTransaction not implemented")
}

// ParseLog 解析日志（基础实现，需要子类重写）
func (a *BaseAdapter) ParseLog(ctx context.Context, rawLog interface{}) (*types.Log, error) {
	return nil, fmt.Errorf("ParseLog not implemented")
}

// DetectReorg 检测回滚（基础实现，需要子类重写）
func (a *BaseAdapter) DetectReorg(ctx context.Context, localBlock *types.Block) (bool, *types.ReorgEvent, error) {
	return false, nil, fmt.Errorf("DetectReorg not implemented")
}

// AdapterFactory 适配器工厂接口
type AdapterFactory interface {
	// Create 创建适配器
	Create(config interface{}) (ChainAdapter, error)
	
	// GetChainType 获取链类型
	GetChainType() string
	
	// ValidateConfig 验证配置
	ValidateConfig(config interface{}) error
}

// BaseAdapterFactory 基础适配器工厂
type BaseAdapterFactory struct {
	chainType string
}

// NewBaseAdapterFactory 创建基础适配器工厂
func NewBaseAdapterFactory(chainType string) *BaseAdapterFactory {
	return &BaseAdapterFactory{
		chainType: chainType,
	}
}

// GetChainType 获取链类型
func (f *BaseAdapterFactory) GetChainType() string {
	return f.chainType
}

// ValidateConfig 验证配置
func (f *BaseAdapterFactory) ValidateConfig(config interface{}) error {
	return nil
}

// Create 创建适配器（基础实现，需要子类重写）
func (f *BaseAdapterFactory) Create(config interface{}) (ChainAdapter, error) {
	return nil, fmt.Errorf("Create not implemented")
}