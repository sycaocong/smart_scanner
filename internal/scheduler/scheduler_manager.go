package scheduler

import (
	"context"
	"fmt"
	"sync"

	"github.com/smart-scanner/multi-chain-scanner/internal/node"
	"github.com/smart-scanner/multi-chain-scanner/pkg/config"
	"github.com/smart-scanner/multi-chain-scanner/pkg/lock"
	"github.com/smart-scanner/multi-chain-scanner/pkg/logger"
	"go.uber.org/zap"
)

// SchedulerManager 调度器管理器
// 管理所有链的扫描器，提供统一的启动、停止和状态查询接口
// [Design: SchedulerManager 调度器管理器](../docs/DESIGN_SCANNER.md#32-schedulermanager-调度器管理器)
type SchedulerManager struct {
	scanners        map[string]*ChainScanner              // 扫描器映射（chainID -> ChainScanner）
	clientRegistry  *node.ChainClientRegistry             // 链客户端注册表
	config          *config.Config                        // 全局配置
	lockManager     *lock.LockManager                     // 分布式锁管理器
	mu              sync.RWMutex                          // 状态锁
}

// NewSchedulerManager 创建调度器管理器
// [Design: SchedulerManager 调度器管理器](../docs/DESIGN_SCANNER.md#32-schedulermanager-调度器管理器)
func NewSchedulerManager(
	cfg *config.Config,
	clientRegistry *node.ChainClientRegistry,
	lockManager *lock.LockManager,
) (*SchedulerManager, error) {
	if cfg == nil {
		return nil, fmt.Errorf("config cannot be nil")
	}
	if clientRegistry == nil {
		return nil, fmt.Errorf("client registry cannot be nil")
	}
	if lockManager == nil {
		return nil, fmt.Errorf("lock manager cannot be nil")
	}

	manager := &SchedulerManager{
		scanners:       make(map[string]*ChainScanner),
		clientRegistry: clientRegistry,
		config:         cfg,
		lockManager:    lockManager,
	}

	// 初始化启用的链扫描器
	if err := manager.initializeScanners(); err != nil {
		return nil, fmt.Errorf("failed to initialize scanners: %w", err)
	}

	return manager, nil
}

// initializeScanners 初始化扫描器
func (m *SchedulerManager) initializeScanners() error {
	enabledChains := m.config.GetEnabledChains()
	logger.Info("Initializing scanners for enabled chains",
		zap.Strings("chains", enabledChains))

	for _, chainID := range enabledChains {
		chainConfig, err := m.config.GetChainConfig(chainID)
		if err != nil {
			logger.Error("Failed to get chain config",
				zap.String("chain_id", chainID),
				zap.Error(err))
			continue
		}

		// 获取链客户端
		client, err := m.clientRegistry.GetClient(chainID)
		if err != nil {
			logger.Error("Failed to get chain client",
				zap.String("chain_id", chainID),
				zap.Error(err))
			continue
		}

		// 创建链扫描器
		scanner, err := NewChainScanner(
			chainID,
			client,
			chainConfig,
			&m.config.Scanner,
			m.lockManager,
		)
		if err != nil {
			logger.Error("Failed to create chain scanner",
				zap.String("chain_id", chainID),
				zap.Error(err))
			continue
		}

		m.scanners[chainID] = scanner
		logger.Info("Scanner initialized successfully",
			zap.String("chain_id", chainID))
	}

	if len(m.scanners) == 0 {
		return fmt.Errorf("no scanners initialized")
	}

	return nil
}

// Start 启动所有扫描器
// [Design: SchedulerManager 调度器管理器](../docs/DESIGN_SCANNER.md#32-schedulermanager-调度器管理器)
func (m *SchedulerManager) Start(ctx context.Context) error {
	m.mu.RLock()
	scanners := make([]*ChainScanner, 0, len(m.scanners))
	for _, scanner := range m.scanners {
		scanners = append(scanners, scanner)
	}
	m.mu.RUnlock()

	logger.Info("Starting all scanners", zap.Int("count", len(scanners)))

	// 启动所有扫描器，单个失败不影响其他扫描器启动
	for _, scanner := range scanners {
		if err := scanner.Start(ctx); err != nil {
			logger.Error("Failed to start scanner",
				zap.String("chain_id", scanner.chainID),
				zap.Error(err))
			// 继续启动其他扫描器
		} else {
			logger.Info("Scanner started successfully",
				zap.String("chain_id", scanner.chainID))
		}
	}

	return nil
}

// Stop 停止所有扫描器
func (m *SchedulerManager) Stop() error {
	m.mu.RLock()
	scanners := make([]*ChainScanner, 0, len(m.scanners))
	for _, scanner := range m.scanners {
		scanners = append(scanners, scanner)
	}
	m.mu.RUnlock()

	logger.Info("Stopping all scanners", zap.Int("count", len(scanners)))

	var lastErr error
	for _, scanner := range scanners {
		if err := scanner.Stop(); err != nil {
			logger.Error("Failed to stop scanner",
				zap.String("chain_id", scanner.chainID),
				zap.Error(err))
			lastErr = err
		} else {
			logger.Info("Scanner stopped successfully",
				zap.String("chain_id", scanner.chainID))
		}
	}

	return lastErr
}

// GetStatus 获取指定链的状态
func (m *SchedulerManager) GetStatus(chainID string) (*ScannerStatus, error) {
	m.mu.RLock()
	scanner, exists := m.scanners[chainID]
	m.mu.RUnlock()

	if !exists {
		return nil, fmt.Errorf("scanner not found for chain %s", chainID)
	}

	return scanner.GetStatus(chainID)
}

// GetAllStatus 获取所有链的状态
func (m *SchedulerManager) GetAllStatus() map[string]*ScannerStatus {
	m.mu.RLock()
	defer m.mu.RUnlock()

	statuses := make(map[string]*ScannerStatus, len(m.scanners))
	for chainID, scanner := range m.scanners {
		status, err := scanner.GetStatus(chainID)
		if err != nil {
			logger.Error("Failed to get scanner status",
				zap.String("chain_id", chainID),
				zap.Error(err))
			continue
		}
		statuses[chainID] = status
	}

	return statuses
}

// GetScanner 获取指定链的扫描器
func (m *SchedulerManager) GetScanner(chainID string) (*ChainScanner, error) {
	m.mu.RLock()
	scanner, exists := m.scanners[chainID]
	m.mu.RUnlock()

	if !exists {
		return nil, fmt.Errorf("scanner not found for chain %s", chainID)
	}

	return scanner, nil
}

// GetActiveChains 获取所有活跃的链ID
func (m *SchedulerManager) GetActiveChains() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	chains := make([]string, 0, len(m.scanners))
	for chainID := range m.scanners {
		chains = append(chains, chainID)
	}
	return chains
}

// GetAllScanners 获取所有扫描器
func (m *SchedulerManager) GetAllScanners() map[string]*ChainScanner {
	m.mu.RLock()
	defer m.mu.RUnlock()

	scanners := make(map[string]*ChainScanner, len(m.scanners))
	for k, v := range m.scanners {
		scanners[k] = v
	}
	return scanners
}

// RestartScanner 重启指定链的扫描器
func (m *SchedulerManager) RestartScanner(ctx context.Context, chainID string) error {
	m.mu.Lock()
	scanner, exists := m.scanners[chainID]
	m.mu.Unlock()

	if !exists {
		return fmt.Errorf("scanner not found for chain %s", chainID)
	}

	logger.Info("Restarting scanner", zap.String("chain_id", chainID))

	// 停止扫描器
	if err := scanner.Stop(); err != nil {
		logger.Error("Failed to stop scanner during restart",
			zap.String("chain_id", chainID),
			zap.Error(err))
		return fmt.Errorf("failed to stop scanner: %w", err)
	}

	// 启动扫描器
	if err := scanner.Start(ctx); err != nil {
		logger.Error("Failed to start scanner during restart",
			zap.String("chain_id", chainID),
			zap.Error(err))
		return fmt.Errorf("failed to start scanner: %w", err)
	}

	logger.Info("Scanner restarted successfully", zap.String("chain_id", chainID))
	return nil
}

// AddScanner 添加新的链扫描器
func (m *SchedulerManager) AddScanner(ctx context.Context, chainID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// 检查是否已存在
	if _, exists := m.scanners[chainID]; exists {
		return fmt.Errorf("scanner already exists for chain %s", chainID)
	}

	// 获取链配置
	chainConfig, err := m.config.GetChainConfig(chainID)
	if err != nil {
		return fmt.Errorf("failed to get chain config: %w", err)
	}

	// 获取链客户端
	client, err := m.clientRegistry.GetClient(chainID)
	if err != nil {
		return fmt.Errorf("failed to get chain client: %w", err)
	}

	// 创建链扫描器
	scanner, err := NewChainScanner(
		chainID,
		client,
		chainConfig,
		&m.config.Scanner,
		m.lockManager,
	)
	if err != nil {
		return fmt.Errorf("failed to create chain scanner: %w", err)
	}

	// 启动扫描器
	if err := scanner.Start(ctx); err != nil {
		return fmt.Errorf("failed to start scanner: %w", err)
	}

	m.scanners[chainID] = scanner
	logger.Info("Scanner added and started successfully", zap.String("chain_id", chainID))

	return nil
}

// RemoveScanner 移除链扫描器
func (m *SchedulerManager) RemoveScanner(chainID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	scanner, exists := m.scanners[chainID]
	if !exists {
		return fmt.Errorf("scanner not found for chain %s", chainID)
	}

	// 停止扫描器
	if err := scanner.Stop(); err != nil {
		logger.Error("Failed to stop scanner during removal",
			zap.String("chain_id", chainID),
			zap.Error(err))
		return fmt.Errorf("failed to stop scanner: %w", err)
	}

	// 移除扫描器
	delete(m.scanners, chainID)
	logger.Info("Scanner removed successfully", zap.String("chain_id", chainID))

	return nil
}