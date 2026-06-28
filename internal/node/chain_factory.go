package node

import (
	"context"
	"fmt"
	"time"

	"github.com/smart-scanner/multi-chain-scanner/pkg/config"
	"github.com/smart-scanner/multi-chain-scanner/pkg/types"
)

// ChainClientFactory 链客户端工厂
// [Design: ChainClientFactory 链客户端工厂](../docs/DESIGN_SCANNER.md#43-chainclientfactory-链客户端工厂)
type ChainClientFactory interface {
	CreateClient(chainID string, cfg *config.ChainConfig) (ChainClient, error)
}

// DefaultChainClientFactory 默认链客户端工厂
type DefaultChainClientFactory struct{}

// NewDefaultChainClientFactory 创建默认链客户端工厂
func NewDefaultChainClientFactory() *DefaultChainClientFactory {
	return &DefaultChainClientFactory{}
}

// CreateClient 创建链客户端
func (f *DefaultChainClientFactory) CreateClient(chainID string, cfg *config.ChainConfig) (ChainClient, error) {
	switch cfg.ChainType {
	case string(types.ChainTypeEVM):
		return NewEVMClient(chainID, cfg)
	case string(types.ChainTypeSolana):
		return NewSolanaClient(chainID, cfg)
	case string(types.ChainTypeTron):
		return NewTronClient(chainID, cfg)
	default:
		return nil, fmt.Errorf("unsupported chain type: %s", cfg.ChainType)
	}
}

// ChainClientRegistry 链客户端注册表
type ChainClientRegistry struct {
	factory ChainClientFactory
	clients map[string]ChainClient
}

// NewChainClientRegistry 创建链客户端注册表
func NewChainClientRegistry(factory ChainClientFactory) *ChainClientRegistry {
	if factory == nil {
		factory = NewDefaultChainClientFactory()
	}

	return &ChainClientRegistry{
		factory: factory,
		clients: make(map[string]ChainClient),
	}
}

// RegisterClient 注册链客户端
func (r *ChainClientRegistry) RegisterClient(chainID string, cfg *config.ChainConfig) error {
	client, err := r.factory.CreateClient(chainID, cfg)
	if err != nil {
		return fmt.Errorf("failed to create client for chain %s: %w", chainID, err)
	}

	r.clients[chainID] = client
	return nil
}

// GetClient 获取链客户端
func (r *ChainClientRegistry) GetClient(chainID string) (ChainClient, error) {
	client, exists := r.clients[chainID]
	if !exists {
		return nil, fmt.Errorf("client not found for chain %s", chainID)
	}
	return client, nil
}

// GetAllClients 获取所有链客户端
func (r *ChainClientRegistry) GetAllClients() map[string]ChainClient {
	clients := make(map[string]ChainClient, len(r.clients))
	for k, v := range r.clients {
		clients[k] = v
	}
	return clients
}

// CloseAll 关闭所有客户端
func (r *ChainClientRegistry) CloseAll() error {
	var lastErr error
	for chainID, client := range r.clients {
		if err := client.Close(); err != nil {
			lastErr = fmt.Errorf("failed to close client for chain %s: %w", chainID, err)
		}
	}
	return lastErr
}

// HealthCheckAll 对所有客户端进行健康检查
func (r *ChainClientRegistry) HealthCheckAll(ctx context.Context) map[string]error {
	results := make(map[string]error)
	for chainID, client := range r.clients {
		results[chainID] = client.HealthCheck(ctx)
	}
	return results
}

// SolanaClient Solana 链客户端（占位实现）
type SolanaClient struct {
	nodeManager *NodeManager
	chainID     string
	config      *config.ChainConfig
}

// NewSolanaClient 创建 Solana 客户端
func NewSolanaClient(chainID string, cfg *config.ChainConfig) (*SolanaClient, error) {
	nodeManager, err := NewNodeManager(chainID, cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create node manager: %w", err)
	}

	return &SolanaClient{
		nodeManager: nodeManager,
		chainID:     chainID,
		config:      cfg,
	}, nil
}

// 实现 ChainClient 接口的方法（占位实现）
func (c *SolanaClient) GetBlock(ctx context.Context, number uint64) (*types.Block, error) {
	return nil, fmt.Errorf("not implemented for Solana")
}

func (c *SolanaClient) GetBlockByHash(ctx context.Context, hash string) (*types.Block, error) {
	return nil, fmt.Errorf("not implemented for Solana")
}

func (c *SolanaClient) GetLatestBlock(ctx context.Context) (*types.Block, error) {
	return nil, fmt.Errorf("not implemented for Solana")
}

func (c *SolanaClient) GetTransaction(ctx context.Context, hash string) (*types.Transaction, error) {
	return nil, fmt.Errorf("not implemented for Solana")
}

func (c *SolanaClient) GetTransactionReceipt(ctx context.Context, hash string) (*types.Transaction, error) {
	return nil, fmt.Errorf("not implemented for Solana")
}

func (c *SolanaClient) GetFinalizedHeight(ctx context.Context) (uint64, error) {
	return 0, fmt.Errorf("not implemented for Solana")
}

func (c *SolanaClient) GetCurrentHeight(ctx context.Context) (uint64, error) {
	return 0, fmt.Errorf("not implemented for Solana")
}

func (c *SolanaClient) BatchGetBlocks(ctx context.Context, numbers []uint64) ([]*types.Block, error) {
	return nil, fmt.Errorf("not implemented for Solana")
}

func (c *SolanaClient) BatchGetTransactions(ctx context.Context, hashes []string) ([]*types.Transaction, error) {
	return nil, fmt.Errorf("not implemented for Solana")
}

func (c *SolanaClient) HealthCheck(ctx context.Context) error {
	return fmt.Errorf("not implemented for Solana")
}

func (c *SolanaClient) GetChainID(ctx context.Context) (string, error) {
	return c.chainID, nil
}

func (c *SolanaClient) GetBlockTime(ctx context.Context) (time.Duration, error) {
	return c.config.BlockTime, nil
}

func (c *SolanaClient) DetectReorg(ctx context.Context, oldBlockNumber uint64, oldBlockHash string) (bool, uint64, error) {
	return false, oldBlockNumber, fmt.Errorf("not implemented for Solana")
}

func (c *SolanaClient) Close() error {
	return c.nodeManager.Close()
}

// TronClient Tron 链客户端（占位实现）
type TronClient struct {
	nodeManager *NodeManager
	chainID     string
	config      *config.ChainConfig
}

// NewTronClient 创建 Tron 客户端
func NewTronClient(chainID string, cfg *config.ChainConfig) (*TronClient, error) {
	nodeManager, err := NewNodeManager(chainID, cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create node manager: %w", err)
	}

	return &TronClient{
		nodeManager: nodeManager,
		chainID:     chainID,
		config:      cfg,
	}, nil
}

// 实现 ChainClient 接口的方法（占位实现）
func (c *TronClient) GetBlock(ctx context.Context, number uint64) (*types.Block, error) {
	return nil, fmt.Errorf("not implemented for Tron")
}

func (c *TronClient) GetBlockByHash(ctx context.Context, hash string) (*types.Block, error) {
	return nil, fmt.Errorf("not implemented for Tron")
}

func (c *TronClient) GetLatestBlock(ctx context.Context) (*types.Block, error) {
	return nil, fmt.Errorf("not implemented for Tron")
}

func (c *TronClient) GetTransaction(ctx context.Context, hash string) (*types.Transaction, error) {
	return nil, fmt.Errorf("not implemented for Tron")
}

func (c *TronClient) GetTransactionReceipt(ctx context.Context, hash string) (*types.Transaction, error) {
	return nil, fmt.Errorf("not implemented for Tron")
}

func (c *TronClient) GetFinalizedHeight(ctx context.Context) (uint64, error) {
	return 0, fmt.Errorf("not implemented for Tron")
}

func (c *TronClient) GetCurrentHeight(ctx context.Context) (uint64, error) {
	return 0, fmt.Errorf("not implemented for Tron")
}

func (c *TronClient) BatchGetBlocks(ctx context.Context, numbers []uint64) ([]*types.Block, error) {
	return nil, fmt.Errorf("not implemented for Tron")
}

func (c *TronClient) BatchGetTransactions(ctx context.Context, hashes []string) ([]*types.Transaction, error) {
	return nil, fmt.Errorf("not implemented for Tron")
}

func (c *TronClient) HealthCheck(ctx context.Context) error {
	return fmt.Errorf("not implemented for Tron")
}

func (c *TronClient) GetChainID(ctx context.Context) (string, error) {
	return c.chainID, nil
}

func (c *TronClient) GetBlockTime(ctx context.Context) (time.Duration, error) {
	return c.config.BlockTime, nil
}

func (c *TronClient) DetectReorg(ctx context.Context, oldBlockNumber uint64, oldBlockHash string) (bool, uint64, error) {
	return false, oldBlockNumber, fmt.Errorf("not implemented for Tron")
}

func (c *TronClient) Close() error {
	return c.nodeManager.Close()
}