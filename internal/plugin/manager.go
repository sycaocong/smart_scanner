package plugin

import (
	"context"
	"fmt"
	"sync"

	"github.com/smart-scanner/multi-chain-scanner/pkg/logger"
	"go.uber.org/zap"
)

// Manager 插件管理器接口
// [Design: PluginManager 插件管理器](../docs/DESIGN_SCANNER.md#81-pluginmanager-插件管理器)
type Manager interface {
	RegisterFactory(factory AdapterFactory) error
	UnregisterFactory(chainType string) error
	GetFactory(chainType string) (AdapterFactory, error)
	CreateAdapter(chainType string, config interface{}) (ChainAdapter, error)
	GetRegisteredFactories() []string
	GetAdapter(chainID string) (ChainAdapter, error)
	RegisterAdapter(chainID string, adapter ChainAdapter) error
	UnregisterAdapter(chainID string) error
	Close() error
	HealthCheck(ctx context.Context) error
}

// PluginManager 插件管理器实现
// 管理链适配器的注册和创建，支持动态扩展链类型
// [Design: PluginManager 插件管理器](../docs/DESIGN_SCANNER.md#81-pluginmanager-插件管理器)
type PluginManager struct {
	mu               sync.RWMutex                     // 状态锁
	factories        map[string]AdapterFactory        // 适配器工厂（chainType -> AdapterFactory）
	adapters         map[string]ChainAdapter         // 适配器实例（chainID -> ChainAdapter）
	adapterFactories map[string]string               // 适配器与工厂映射（chainID -> chainType）
}

// NewPluginManager 创建插件管理器
func NewPluginManager() *PluginManager {
	return &PluginManager{
		factories:        make(map[string]AdapterFactory),
		adapters:         make(map[string]ChainAdapter),
		adapterFactories: make(map[string]string),
	}
}

// RegisterFactory 注册适配器工厂
func (pm *PluginManager) RegisterFactory(factory AdapterFactory) error {
	if factory == nil {
		return fmt.Errorf("factory cannot be nil")
	}

	chainType := factory.GetChainType()
	if chainType == "" {
		return fmt.Errorf("chain type cannot be empty")
	}

	pm.mu.Lock()
	defer pm.mu.Unlock()

	if _, exists := pm.factories[chainType]; exists {
		return fmt.Errorf("factory for chain type %s already registered", chainType)
	}

	pm.factories[chainType] = factory

	logger.Info("Registered adapter factory",
		zap.String("chain_type", chainType))

	return nil
}

// UnregisterFactory 注销适配器工厂
func (pm *PluginManager) UnregisterFactory(chainType string) error {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	if _, exists := pm.factories[chainType]; !exists {
		return fmt.Errorf("factory for chain type %s not found", chainType)
	}

	delete(pm.factories, chainType)

	logger.Info("Unregistered adapter factory",
		zap.String("chain_type", chainType))

	return nil
}

// GetFactory 获取适配器工厂
func (pm *PluginManager) GetFactory(chainType string) (AdapterFactory, error) {
	pm.mu.RLock()
	defer pm.mu.RUnlock()

	factory, exists := pm.factories[chainType]
	if !exists {
		return nil, fmt.Errorf("factory for chain type %s not found", chainType)
	}

	return factory, nil
}

// CreateAdapter 创建适配器
func (pm *PluginManager) CreateAdapter(chainType string, config interface{}) (ChainAdapter, error) {
	// 获取工厂
	factory, err := pm.GetFactory(chainType)
	if err != nil {
		return nil, fmt.Errorf("failed to get factory: %w", err)
	}

	// 验证配置
	if err := factory.ValidateConfig(config); err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}

	// 创建适配器
	adapter, err := factory.Create(config)
	if err != nil {
		return nil, fmt.Errorf("failed to create adapter: %w", err)
	}

	logger.Info("Created adapter",
		zap.String("chain_type", chainType),
		zap.String("chain_id", adapter.GetChainID()))

	return adapter, nil
}

// GetRegisteredFactories 获取已注册的工厂列表
func (pm *PluginManager) GetRegisteredFactories() []string {
	pm.mu.RLock()
	defer pm.mu.RUnlock()

	factories := make([]string, 0, len(pm.factories))
	for chainType := range pm.factories {
		factories = append(factories, chainType)
	}

	return factories
}

// RegisterAdapter 注册适配器
func (pm *PluginManager) RegisterAdapter(chainID string, adapter ChainAdapter) error {
	if adapter == nil {
		return fmt.Errorf("adapter cannot be nil")
	}

	if chainID == "" {
		return fmt.Errorf("chain ID cannot be empty")
	}

	pm.mu.Lock()
	defer pm.mu.Unlock()

	if _, exists := pm.adapters[chainID]; exists {
		return fmt.Errorf("adapter for chain %s already registered", chainID)
	}

	pm.adapters[chainID] = adapter

	logger.Info("Registered adapter",
		zap.String("chain_id", chainID),
		zap.String("chain_name", adapter.GetChainName()))

	return nil
}

// UnregisterAdapter 注销适配器
func (pm *PluginManager) UnregisterAdapter(chainID string) error {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	adapter, exists := pm.adapters[chainID]
	if !exists {
		return fmt.Errorf("adapter for chain %s not found", chainID)
	}

	// 关闭适配器
	if err := adapter.Close(); err != nil {
		logger.Error("Failed to close adapter",
			zap.String("chain_id", chainID),
			zap.Error(err))
	}

	delete(pm.adapters, chainID)
	delete(pm.adapterFactories, chainID)

	logger.Info("Unregistered adapter",
		zap.String("chain_id", chainID))

	return nil
}

// GetAdapter 获取适配器
func (pm *PluginManager) GetAdapter(chainID string) (ChainAdapter, error) {
	pm.mu.RLock()
	defer pm.mu.RUnlock()

	adapter, exists := pm.adapters[chainID]
	if !exists {
		return nil, fmt.Errorf("adapter for chain %s not found", chainID)
	}

	return adapter, nil
}

// GetAllAdapters 获取所有适配器
func (pm *PluginManager) GetAllAdapters() map[string]ChainAdapter {
	pm.mu.RLock()
	defer pm.mu.RUnlock()

	adapters := make(map[string]ChainAdapter)
	for chainID, adapter := range pm.adapters {
		adapters[chainID] = adapter
	}

	return adapters
}

// Close 关闭管理器
func (pm *PluginManager) Close() error {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	var errors []error

	// 关闭所有适配器
	for chainID, adapter := range pm.adapters {
		if err := adapter.Close(); err != nil {
			errors = append(errors, fmt.Errorf("failed to close adapter %s: %w", chainID, err))
		}
	}

	// 清空所有映射
	pm.adapters = make(map[string]ChainAdapter)
	pm.adapterFactories = make(map[string]string)
	pm.factories = make(map[string]AdapterFactory)

	if len(errors) > 0 {
		return fmt.Errorf("errors occurred while closing: %v", errors)
	}

	logger.Info("Plugin manager closed")

	return nil
}

// HealthCheck 健康检查
func (pm *PluginManager) HealthCheck(ctx context.Context) error {
	pm.mu.RLock()
	defer pm.mu.RUnlock()

	// 检查所有适配器的健康状态
	for chainID, adapter := range pm.adapters {
		if err := adapter.HealthCheck(ctx); err != nil {
			return fmt.Errorf("adapter %s health check failed: %w", chainID, err)
		}
	}

	return nil
}

// GetStatistics 获取统计信息
func (pm *PluginManager) GetStatistics() *ManagerStatistics {
	pm.mu.RLock()
	defer pm.mu.RUnlock()

	stats := &ManagerStatistics{
		RegisteredFactories: len(pm.factories),
		RegisteredAdapters:  len(pm.adapters),
		FactoryTypes:        make([]string, 0, len(pm.factories)),
		AdapterChains:       make([]string, 0, len(pm.adapters)),
	}

	for chainType := range pm.factories {
		stats.FactoryTypes = append(stats.FactoryTypes, chainType)
	}

	for chainID := range pm.adapters {
		stats.AdapterChains = append(stats.AdapterChains, chainID)
	}

	return stats
}

// ManagerStatistics 管理器统计信息
type ManagerStatistics struct {
	RegisteredFactories int      `json:"registered_factories"`
	RegisteredAdapters  int      `json:"registered_adapters"`
	FactoryTypes        []string `json:"factory_types"`
	AdapterChains       []string `json:"adapter_chains"`
}

// GlobalPluginManager 全局插件管理器实例
var GlobalPluginManager = NewPluginManager()

// RegisterGlobalFactory 注册全局适配器工厂
func RegisterGlobalFactory(factory AdapterFactory) error {
	return GlobalPluginManager.RegisterFactory(factory)
}

// GetGlobalFactory 获取全局适配器工厂
func GetGlobalFactory(chainType string) (AdapterFactory, error) {
	return GlobalPluginManager.GetFactory(chainType)
}

// CreateGlobalAdapter 创建全局适配器
func CreateGlobalAdapter(chainType string, config interface{}) (ChainAdapter, error) {
	return GlobalPluginManager.CreateAdapter(chainType, config)
}

// RegisterGlobalAdapter 注册全局适配器
func RegisterGlobalAdapter(chainID string, adapter ChainAdapter) error {
	return GlobalPluginManager.RegisterAdapter(chainID, adapter)
}

// GetGlobalAdapter 获取全局适配器
func GetGlobalAdapter(chainID string) (ChainAdapter, error) {
	return GlobalPluginManager.GetAdapter(chainID)
}

// UnregisterGlobalAdapter 注销全局适配器
func UnregisterGlobalAdapter(chainID string) error {
	return GlobalPluginManager.UnregisterAdapter(chainID)
}