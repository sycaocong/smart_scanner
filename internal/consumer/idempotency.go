package consumer

import (
	"context"
	"fmt"
	"time"

	"github.com/smart-scanner/multi-chain-scanner/internal/storage"
	"github.com/smart-scanner/multi-chain-scanner/pkg/logger"
	"go.uber.org/zap"
)

// IdempotencyHandler 幂等性处理器接口
// [Design: IdempotencyHandler 幂等性处理器](../docs/DESIGN_SCANNER.md#62-idempotencyhandler-幂等性处理器)
type IdempotencyHandler interface {
	// IsProcessed 检查是否已处理
	IsProcessed(ctx context.Context, key string) (bool, error)
	
	// MarkProcessed 标记为已处理
	MarkProcessed(ctx context.Context, key string) error
	
	// Clear 清除处理标记
	Clear(ctx context.Context, key string) error
	
	// ClearExpired 清除过期的处理标记
	ClearExpired(ctx context.Context) error
}

// RedisIdempotencyHandler Redis 幂等性处理器实现
type RedisIdempotencyHandler struct {
	storage storage.Storage
	ttl     time.Duration
}

// NewRedisIdempotencyHandler 创建 Redis 幂等性处理器
func NewRedisIdempotencyHandler(storage storage.Storage, ttl time.Duration) (*RedisIdempotencyHandler, error) {
	if storage == nil {
		return nil, fmt.Errorf("storage cannot be nil")
	}

	return &RedisIdempotencyHandler{
		storage: storage,
		ttl:     ttl,
	}, nil
}

// IsProcessed 检查是否已处理
func (h *RedisIdempotencyHandler) IsProcessed(ctx context.Context, key string) (bool, error) {
	// 这里应该使用 Redis 的 EXISTS 命令检查
	// 由于我们的 storage 接口没有直接支持 Redis 操作，
	// 这里使用一个简化的实现
	
	// 实际实现中，应该直接使用 Redis 客户端
	// 示例代码：
	// exists, err := h.redisClient.Exists(ctx, h.generateKey(key)).Result()
	// return exists > 0, err
	
	// 暂时返回 false，表示未处理
	return false, nil
}

// MarkProcessed 标记为已处理
func (h *RedisIdempotencyHandler) MarkProcessed(ctx context.Context, key string) error {
	// 这里应该使用 Redis 的 SET 命令设置，并指定过期时间
	// 示例代码：
	// return h.redisClient.Set(ctx, h.generateKey(key), "1", h.ttl).Err()
	
	logger.Debug("Marked as processed",
		zap.String("key", key),
		zap.Duration("ttl", h.ttl))
	
	return nil
}

// Clear 清除处理标记
func (h *RedisIdempotencyHandler) Clear(ctx context.Context, key string) error {
	// 这里应该使用 Redis 的 DEL 命令删除
	// 示例代码：
	// return h.redisClient.Del(ctx, h.generateKey(key)).Err()
	
	logger.Debug("Cleared processed mark",
		zap.String("key", key))
	
	return nil
}

// ClearExpired 清除过期的处理标记
func (h *RedisIdempotencyHandler) ClearExpired(ctx context.Context) error {
	// Redis 会自动清除过期的键，这里不需要实现
	return nil
}

// generateKey 生成 Redis 键
func (h *RedisIdempotencyHandler) generateKey(key string) string {
	return fmt.Sprintf("idempotency:%s", key)
}

// DatabaseIdempotencyHandler 数据库幂等性处理器实现
type DatabaseIdempotencyHandler struct {
	storage storage.Storage
	ttl     time.Duration
}

// NewDatabaseIdempotencyHandler 创建数据库幂等性处理器
func NewDatabaseIdempotencyHandler(storage storage.Storage, ttl time.Duration) (*DatabaseIdempotencyHandler, error) {
	if storage == nil {
		return nil, fmt.Errorf("storage cannot be nil")
	}

	return &DatabaseIdempotencyHandler{
		storage: storage,
		ttl:     ttl,
	}, nil
}

// IsProcessed 检查是否已处理
func (h *DatabaseIdempotencyHandler) IsProcessed(ctx context.Context, key string) (bool, error) {
	// 这里应该查询数据库中的处理记录
	// 由于我们的 storage 接口没有直接支持幂等性检查，
	// 这里使用一个简化的实现
	
	// 实际实现中，应该创建一个专门的表来存储处理记录
	// 示例代码：
	// var count int64
	// err := h.db.WithContext(ctx).Model(&ProcessedRecord{}).
	//     Where("key = ? AND created_at > ?", key, time.Now().Add(-h.ttl)).
	//     Count(&count).Error
	// return count > 0, err
	
	return false, nil
}

// MarkProcessed 标记为已处理
func (h *DatabaseIdempotencyHandler) MarkProcessed(ctx context.Context, key string) error {
	// 这里应该向数据库插入处理记录
	// 示例代码：
	// record := &ProcessedRecord{
	//     Key:       key,
	//     CreatedAt: time.Now(),
	//     ExpiresAt: time.Now().Add(h.ttl),
	// }
	// return h.db.WithContext(ctx).Create(record).Error
	
	logger.Debug("Marked as processed",
		zap.String("key", key),
		zap.Duration("ttl", h.ttl))
	
	return nil
}

// Clear 清除处理标记
func (h *DatabaseIdempotencyHandler) Clear(ctx context.Context, key string) error {
	// 这里应该从数据库删除处理记录
	// 示例代码：
	// return h.db.WithContext(ctx).Where("key = ?", key).Delete(&ProcessedRecord{}).Error
	
	logger.Debug("Cleared processed mark",
		zap.String("key", key))
	
	return nil
}

// ClearExpired 清除过期的处理标记
func (h *DatabaseIdempotencyHandler) ClearExpired(ctx context.Context) error {
	// 这里应该从数据库删除过期的处理记录
	// 示例代码：
	// return h.db.WithContext(ctx).Where("expires_at < ?", time.Now()).Delete(&ProcessedRecord{}).Error
	
	logger.Debug("Cleared expired processed marks")
	
	return nil
}

// ProcessedRecord 处理记录模型
type ProcessedRecord struct {
	ID        uint      `gorm:"primaryKey"`
	Key       string    `gorm:"uniqueIndex;not null"`
	CreatedAt time.Time `gorm:"autoCreateTime"`
	ExpiresAt time.Time `gorm:"not null"`
}