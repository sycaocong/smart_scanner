package node

import (
	"context"
	"crypto/rand"
	"fmt"
	"math/big"
	"sync"
	"sync/atomic"
	"time"

	"github.com/smart-scanner/multi-chain-scanner/pkg/config"
	"github.com/smart-scanner/multi-chain-scanner/pkg/logger"
	"github.com/smart-scanner/multi-chain-scanner/pkg/metrics"
	"github.com/smart-scanner/multi-chain-scanner/pkg/types"
	"go.uber.org/zap"
)

// ChainClient 链客户端接口
// 定义了与链节点交互的标准接口，支持多种链类型的统一访问
// [Design: ChainClient 链客户端接口](../docs/DESIGN_SCANNER.md#41-chainclient-链客户端接口)
type ChainClient interface {
	GetBlock(ctx context.Context, number uint64) (*types.Block, error)
	GetBlockByHash(ctx context.Context, hash string) (*types.Block, error)
	GetLatestBlock(ctx context.Context) (*types.Block, error)
	GetTransaction(ctx context.Context, hash string) (*types.Transaction, error)
	GetTransactionReceipt(ctx context.Context, hash string) (*types.Transaction, error)
	GetFinalizedHeight(ctx context.Context) (uint64, error)
	GetCurrentHeight(ctx context.Context) (uint64, error)
	BatchGetBlocks(ctx context.Context, numbers []uint64) ([]*types.Block, error)
	BatchGetTransactions(ctx context.Context, hashes []string) ([]*types.Transaction, error)
	HealthCheck(ctx context.Context) error
	GetChainID(ctx context.Context) (string, error)
	GetBlockTime(ctx context.Context) (time.Duration, error)
	DetectReorg(ctx context.Context, oldBlockNumber uint64, oldBlockHash string) (bool, uint64, error)
	Close() error
}

// RPCClient RPC 客户端接口
type RPCClient interface {
	Call(ctx context.Context, method string, params ...interface{}) (interface{}, error)
	BatchCall(ctx context.Context, calls []RPCCall) ([]interface{}, error)
	Close() error
}

// RPCCall RPC 调用
type RPCCall struct {
	Method string
	Params []interface{}
}

// NodeManager 节点管理器
// 管理多个 RPC 节点，实现健康检查和故障转移机制
// [Design: NodeManager 节点管理器](../docs/DESIGN_SCANNER.md#42-nodemanager-节点管理器)
type NodeManager struct {
	chainID     string           // 链ID
	nodes       []*NodePool      // 节点池列表
	healthCheck *HealthChecker   // 健康检查器
	config      *config.ChainConfig // 链配置
	mu          sync.RWMutex     // 状态锁
	roundRobin  int              // 轮询索引
}

// NodePool 节点池
// 管理单个 RPC 节点的连接和状态
type NodePool struct {
	config          config.RPCNodeConfig // 节点配置
	client          RPCClient           // RPC 客户端
	healthy         bool                // 是否健康
	lastCheck       time.Time           // 最后检查时间
	failureCount    int32               // 连续失败次数（原子操作）
	successCount    int32               // 连续成功次数（原子操作）
	avgResponseTime float64             // 平均响应时间
	mu              sync.RWMutex        // 状态锁
}

// HealthChecker 健康检查器
// 定期检查节点健康状态，支持链类型自适应的健康检查方法
// [Design: HealthChecker 健康检查器](../docs/DESIGN_SCANNER.md#44-healthchecker-健康检查器)
type HealthChecker struct {
	nodes    []*NodePool               // 节点池列表
	config   config.HealthCheckConfig  // 健康检查配置
	chainID  string                    // 链ID
	stopChan chan struct{}             // 停止信号通道
	mu       sync.RWMutex              // 状态锁
}

// NewNodeManager 创建节点管理器
func NewNodeManager(chainID string, cfg *config.ChainConfig) (*NodeManager, error) {
	if len(cfg.RPCNodes) == 0 {
		return nil, fmt.Errorf("no RPC nodes configured for chain %s", chainID)
	}

	manager := &NodeManager{
		chainID:    chainID,
		config:     cfg,
		nodes:      make([]*NodePool, 0, len(cfg.RPCNodes)),
		roundRobin: 0,
	}

	for _, nodeConfig := range cfg.RPCNodes {
		node, err := NewNodePool(nodeConfig)
		if err != nil {
			logger.Error("Failed to create node pool",
				zap.String("chain_id", chainID),
				zap.String("node_url", nodeConfig.URL),
				zap.Error(err))
			continue
		}
		manager.nodes = append(manager.nodes, node)
	}

	if len(manager.nodes) == 0 {
		return nil, fmt.Errorf("no valid RPC nodes for chain %s", chainID)
	}

	if cfg.HealthCheck.Enabled {
		manager.healthCheck = NewHealthChecker(chainID, manager.nodes, cfg.HealthCheck)
		go manager.healthCheck.Start()
	}

	return manager, nil
}

// NewNodePool 创建节点池
func NewNodePool(cfg config.RPCNodeConfig) (*NodePool, error) {
	client, err := NewRPCClient(cfg.URL, cfg.Timeout)
	if err != nil {
		return nil, fmt.Errorf("failed to create RPC client: %w", err)
	}

	return &NodePool{
		config:    cfg,
		client:    client,
		healthy:   true,
		lastCheck: time.Now(),
	}, nil
}

// GetHealthyNode 获取健康的节点
func (m *NodeManager) GetHealthyNode() (*NodePool, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var healthyNodes []*NodePool
	for _, node := range m.nodes {
		if node.IsHealthy() {
			healthyNodes = append(healthyNodes, node)
		}
	}

	if len(healthyNodes) == 0 {
		return nil, fmt.Errorf("no healthy nodes available for chain %s", m.chainID)
	}

	return selectNodeByWeight(healthyNodes), nil
}

// selectNodeByWeight 按权重选择节点
func selectNodeByWeight(nodes []*NodePool) *NodePool {
	totalWeight := 0
	for _, node := range nodes {
		totalWeight += node.config.Weight
	}

	if totalWeight == 0 {
		return nodes[0]
	}

	// 使用 crypto/rand 获取真正的随机数
	randomNum, err := rand.Int(rand.Reader, big.NewInt(int64(totalWeight)))
	if err != nil {
		// 降级使用时间戳
		randomNum = big.NewInt(time.Now().UnixNano() % int64(totalWeight))
	}

	weight := randomNum.Int64()
	currentWeight := 0
	for _, node := range nodes {
		currentWeight += node.config.Weight
		if weight < int64(currentWeight) {
			return node
		}
	}

	return nodes[0]
}

// Call 调用 RPC 方法（带故障转移）
func (m *NodeManager) Call(ctx context.Context, method string, params ...interface{}) (interface{}, error) {
	return m.callWithRetry(ctx, method, params...)
}

// callWithRetry 带重试和故障转移的 RPC 调用
func (m *NodeManager) callWithRetry(ctx context.Context, method string, params ...interface{}) (interface{}, error) {
	maxRetries := 3
	var lastErr error

	for retry := 0; retry < maxRetries; retry++ {
		node, err := m.GetHealthyNode()
		if err != nil {
			return nil, err
		}

		startTime := time.Now()
		result, err := node.Call(ctx, method, params...)
		duration := time.Since(startTime)

		metrics.RecordNodeResponseTime(m.chainID, node.config.URL, duration.Seconds())

		if err != nil {
			node.RecordFailure()
			metrics.SetNodeHealthStatus(m.chainID, node.config.URL, 0)
			lastErr = fmt.Errorf("RPC call failed on node %s: %w", node.config.URL, err)
			logger.Warn("RPC call failed, retrying",
				zap.String("chain_id", m.chainID),
				zap.String("node_url", node.config.URL),
				zap.String("method", method),
				zap.Int("retry", retry),
				zap.Error(err))
			continue
		}

		node.RecordSuccess()
		metrics.SetNodeHealthStatus(m.chainID, node.config.URL, 1)
		return result, nil
	}

	return nil, fmt.Errorf("all nodes failed after %d retries: %w", maxRetries, lastErr)
}

// BatchCall 批量调用 RPC 方法
func (m *NodeManager) BatchCall(ctx context.Context, calls []RPCCall) ([]interface{}, error) {
	return m.batchCallWithRetry(ctx, calls)
}

// batchCallWithRetry 带重试和故障转移的批量 RPC 调用
func (m *NodeManager) batchCallWithRetry(ctx context.Context, calls []RPCCall) ([]interface{}, error) {
	maxRetries := 3
	var lastErr error

	for retry := 0; retry < maxRetries; retry++ {
		node, err := m.GetHealthyNode()
		if err != nil {
			return nil, err
		}

		startTime := time.Now()
		results, err := node.BatchCall(ctx, calls)
		duration := time.Since(startTime)

		metrics.RecordNodeResponseTime(m.chainID, node.config.URL, duration.Seconds())

		if err != nil {
			node.RecordFailure()
			metrics.SetNodeHealthStatus(m.chainID, node.config.URL, 0)
			lastErr = fmt.Errorf("batch RPC call failed on node %s: %w", node.config.URL, err)
			logger.Warn("Batch RPC call failed, retrying",
				zap.String("chain_id", m.chainID),
				zap.String("node_url", node.config.URL),
				zap.Int("retry", retry),
				zap.Error(err))
			continue
		}

		node.RecordSuccess()
		metrics.SetNodeHealthStatus(m.chainID, node.config.URL, 1)
		return results, nil
	}

	return nil, fmt.Errorf("all nodes failed after %d retries: %w", maxRetries, lastErr)
}

// GetHealthStatus 获取健康状态
func (m *NodeManager) GetHealthStatus() []types.NodeHealth {
	m.mu.RLock()
	defer m.mu.RUnlock()

	status := make([]types.NodeHealth, 0, len(m.nodes))
	for _, node := range m.nodes {
		node.mu.RLock()
		status = append(status, types.NodeHealth{
			URL:             node.config.URL,
			Healthy:         node.healthy,
			LastCheckTime:   node.lastCheck,
			FailureCount:    int(node.failureCount),
			SuccessCount:    int(node.successCount),
			AvgResponseTime: node.avgResponseTime,
		})
		node.mu.RUnlock()
	}

	return status
}

// Close 关闭节点管理器
func (m *NodeManager) Close() error {
	var lastErr error

	if m.healthCheck != nil {
		m.healthCheck.Stop()
	}

	for _, node := range m.nodes {
		if err := node.Close(); err != nil {
			lastErr = err
		}
	}

	return lastErr
}

// IsHealthy 检查节点是否健康
func (n *NodePool) IsHealthy() bool {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.healthy
}

// Call 调用 RPC 方法
func (n *NodePool) Call(ctx context.Context, method string, params ...interface{}) (interface{}, error) {
	return n.client.Call(ctx, method, params...)
}

// BatchCall 批量调用 RPC 方法
func (n *NodePool) BatchCall(ctx context.Context, calls []RPCCall) ([]interface{}, error) {
	return n.client.BatchCall(ctx, calls)
}

// RecordFailure 记录失败（使用 atomic）
func (n *NodePool) RecordFailure() {
	atomic.AddInt32(&n.failureCount, 1)
	n.mu.Lock()
	n.lastCheck = time.Now()
	n.mu.Unlock()
}

// RecordSuccess 记录成功（使用 atomic）
func (n *NodePool) RecordSuccess() {
	atomic.AddInt32(&n.successCount, 1)
	n.mu.Lock()
	n.lastCheck = time.Now()
	n.mu.Unlock()
}

// updateAvgResponseTime 更新平均响应时间
func (n *NodePool) updateAvgResponseTime(responseTime float64) {
	n.mu.Lock()
	defer n.mu.Unlock()
	
	count := float64(n.successCount + n.failureCount)
	if count == 0 {
		n.avgResponseTime = responseTime
		return
	}
	n.avgResponseTime = (n.avgResponseTime*(count-1) + responseTime) / count
}

// Close 关闭节点连接
func (n *NodePool) Close() error {
	return n.client.Close()
}

// NewHealthChecker 创建健康检查器
func NewHealthChecker(chainID string, nodes []*NodePool, cfg config.HealthCheckConfig) *HealthChecker {
	return &HealthChecker{
		nodes:   nodes,
		config:  cfg,
		chainID: chainID,
		stopChan: make(chan struct{}),
	}
}

// Start 启动健康检查
func (h *HealthChecker) Start() {
	ticker := time.NewTicker(h.config.Interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			h.checkAllNodes()
		case <-h.stopChan:
			return
		}
	}
}

// Stop 停止健康检查
func (h *HealthChecker) Stop() {
	close(h.stopChan)
}

// checkAllNodes 检查所有节点
func (h *HealthChecker) checkAllNodes() {
	for _, node := range h.nodes {
		go h.checkNode(node)
	}
}

// checkNode 检查单个节点
func (h *HealthChecker) checkNode(node *NodePool) {
	ctx, cancel := context.WithTimeout(context.Background(), h.config.Timeout)
	defer cancel()

	var method string
	var params []interface{}

	switch h.chainID {
	case "ethereum", "goerli", "sepolia", "polygon", "arbitrum", "optimism":
		method = "eth_blockNumber"
		params = nil
	case "solana":
		method = "getHealth"
		params = nil
	case "tron":
		method = "eth_blockNumber"
		params = nil
	default:
		method = "eth_blockNumber"
		params = nil
	}

	startTime := time.Now()
	_, err := node.client.Call(ctx, method, params...)
	duration := time.Since(startTime)

	node.mu.Lock()
	defer node.mu.Unlock()

	node.lastCheck = time.Now()

	if err != nil {
		node.failureCount++
		node.updateAvgResponseTime(0)

		if node.failureCount >= int32(h.config.FailureThreshold) {
			if node.healthy {
				logger.Warn("Node marked as unhealthy",
					zap.String("chain_id", h.chainID),
					zap.String("node_url", node.config.URL),
					zap.Int32("failure_count", node.failureCount))
				node.healthy = false

				if h.config.AlertEnabled {
					h.sendAlert("NODE_UNHEALTHY", node)
				}
			}
		}
	} else {
		node.successCount++
		node.failureCount = 0
		node.updateAvgResponseTime(duration.Seconds())

		if !node.healthy && node.successCount >= int32(h.config.SuccessThreshold) {
			logger.Info("Node marked as healthy",
				zap.String("chain_id", h.chainID),
				zap.String("node_url", node.config.URL),
				zap.Int32("success_count", node.successCount))
			node.healthy = true

			if h.config.AlertEnabled {
				h.sendAlert("NODE_RECOVERED", node)
			}
		}
	}
}

// sendAlert 发送告警
func (h *HealthChecker) sendAlert(alertType string, node *NodePool) {
	alert := types.Alert{
		Type:        alertType,
		ChainID:     h.chainID,
		NodeURL:     node.config.URL,
		Message:     fmt.Sprintf("%s: Node %s is %s", alertType, node.config.URL, alertType),
		Timestamp:   time.Now(),
		FailureCount: int(node.failureCount),
		SuccessCount: int(node.successCount),
	}

	logger.Error("Sending alert",
		zap.String("type", alert.Type),
		zap.String("chain_id", alert.ChainID),
		zap.String("node_url", alert.NodeURL),
		zap.String("message", alert.Message))

	go func() {
		if err := h.publishAlert(alert); err != nil {
			logger.Error("Failed to publish alert",
				zap.String("type", alert.Type),
				zap.Error(err))
		}
	}()
}

// publishAlert 发布告警
func (h *HealthChecker) publishAlert(alert types.Alert) error {
	return nil
}

// NewRPCClient 创建 RPC 客户端
func NewRPCClient(url string, timeout time.Duration) (RPCClient, error) {
	return NewHTTPClient(url, timeout)
}