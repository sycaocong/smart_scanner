package scheduler

import (
	"fmt"

	"github.com/smart-scanner/multi-chain-scanner/pkg/config"
	"github.com/smart-scanner/multi-chain-scanner/pkg/util"
)

// ShardingManager 分片管理器
// 实现基于区块高度或地址的分片策略，支持水平扩展
// [Design: ShardingManager 分片管理器](../docs/DESIGN_SCANNER.md#34-shardingmanager-分片管理器)
type ShardingManager struct {
	chainID    string                // 链ID
	config     *config.ShardingConfig // 分片配置
	shardCount int                   // 分片数量
	strategy   string                // 分片策略（block_number | address）
}

// NewShardingManager 创建分片管理器
// [Design: ShardingManager 分片管理器](../docs/DESIGN_SCANNER.md#34-shardingmanager-分片管理器)
func NewShardingManager(chainID string, config *config.ShardingConfig) (*ShardingManager, error) {
	if !config.Enabled {
		return nil, fmt.Errorf("sharding is not enabled")
	}

	if config.ShardCount <= 0 {
		return nil, fmt.Errorf("shard count must be positive")
	}

	return &ShardingManager{
		chainID:    chainID,
		config:     config,
		shardCount: config.ShardCount,
		strategy:   config.Strategy,
	}, nil
}

// CalculateShard 计算分片ID
func (m *ShardingManager) CalculateShard(blockNumber uint64) int {
	switch m.strategy {
	case "block_number":
		return util.CalculateShard(blockNumber, m.shardCount)
	case "address":
		// 地址分片需要额外的地址信息
		return 0
	default:
		return util.CalculateShard(blockNumber, m.shardCount)
	}
}

// CalculateAddressShard 根据地址计算分片ID
func (m *ShardingManager) CalculateAddressShard(address string) int {
	return util.CalculateAddressShard(address, m.shardCount)
}

// GetShardCount 获取分片数量
func (m *ShardingManager) GetShardCount() int {
	return m.shardCount
}

// GetStrategy 获取分片策略
func (m *ShardingManager) GetStrategy() string {
	return m.strategy
}

// GetShardRange 获取分片的区块范围
func (m *ShardingManager) GetShardRange(shardID int, startBlock, endBlock uint64) (uint64, uint64) {
	if shardID < 0 || shardID >= m.shardCount {
		return 0, 0
	}

	// 计算每个分片的区块数量
	totalBlocks := endBlock - startBlock + 1
	blocksPerShard := totalBlocks / uint64(m.shardCount)
	remainder := totalBlocks % uint64(m.shardCount)

	// 计算分片的起始和结束区块
	shardStart := startBlock + uint64(shardID)*blocksPerShard
	if uint64(shardID) < remainder {
		shardStart += uint64(shardID)
	} else {
		shardStart += remainder
	}

	shardEnd := shardStart + blocksPerShard - 1
	if uint64(shardID) < remainder {
		shardEnd++
	}

	// 确保不超过结束区块
	if shardEnd > endBlock {
		shardEnd = endBlock
	}

	return shardStart, shardEnd
}

// GetAllShardRanges 获取所有分片的区块范围
func (m *ShardingManager) GetAllShardRanges(startBlock, endBlock uint64) []struct {
	ShardID    int
	StartBlock uint64
	EndBlock   uint64
} {
	ranges := make([]struct {
		ShardID    int
		StartBlock uint64
		EndBlock   uint64
	}, m.shardCount)

	for i := 0; i < m.shardCount; i++ {
		start, end := m.GetShardRange(i, startBlock, endBlock)
		ranges[i] = struct {
			ShardID    int
			StartBlock uint64
			EndBlock   uint64
		}{
			ShardID:    i,
			StartBlock: start,
			EndBlock:   end,
		}
	}

	return ranges
}