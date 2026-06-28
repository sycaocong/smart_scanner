package lock

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/go-redis/redis/v8"
)

// lock 分布式锁包
// 提供 Redis 分布式锁实现，支持锁的获取、释放、延长和状态检查
// [Design: 分布式锁](../docs/DESIGN_SCANNER.md#1-系统概述)

var (
	ErrLockNotHeld   = errors.New("lock not held")
	ErrLockExpired   = errors.New("lock expired")
	ErrLockConflict  = errors.New("lock conflict")
)

// Lock 分布式锁接口
type Lock interface {
	Lock(ctx context.Context) error
	Unlock(ctx context.Context) error
	Extend(ctx context.Context, ttl time.Duration) error
	IsLocked(ctx context.Context) (bool, error)
}

// RedisLock Redis 分布式锁实现
type RedisLock struct {
	client    *redis.Client
	key       string
	value     string
	ttl       time.Duration
	locked    bool
}

// NewRedisLock 创建 Redis 分布式锁
func NewRedisLock(client *redis.Client, key string, ttl time.Duration) *RedisLock {
	return &RedisLock{
		client: client,
		key:    key,
		value:  generateLockValue(),
		ttl:    ttl,
		locked: false,
	}
}

// Lock 获取锁
func (l *RedisLock) Lock(ctx context.Context) error {
	// 使用 SET NX EX 命令实现分布式锁
	result, err := l.client.SetNX(ctx, l.key, l.value, l.ttl).Result()
	if err != nil {
		return fmt.Errorf("failed to acquire lock: %w", err)
	}
	if !result {
		return ErrLockConflict
	}
	l.locked = true
	return nil
}

// Unlock 释放锁
func (l *RedisLock) Unlock(ctx context.Context) error {
	if !l.locked {
		return ErrLockNotHeld
	}

	// 使用 Lua 脚本确保只有锁的持有者才能释放锁
	script := `
		if redis.call("get", KEYS[1]) == ARGV[1] then
			return redis.call("del", KEYS[1])
		else
			return 0
		end
	`
	result, err := l.client.Eval(ctx, script, []string{l.key}, l.value).Result()
	if err != nil {
		return fmt.Errorf("failed to release lock: %w", err)
	}
	if result.(int64) == 0 {
		return ErrLockNotHeld
	}
	l.locked = false
	return nil
}

// Extend 延长锁的过期时间
func (l *RedisLock) Extend(ctx context.Context, ttl time.Duration) error {
	if !l.locked {
		return ErrLockNotHeld
	}

	// 使用 Lua 脚本确保只有锁的持有者才能延长锁
	script := `
		if redis.call("get", KEYS[1]) == ARGV[1] then
			return redis.call("expire", KEYS[1], ARGV[2])
		else
			return 0
		end
	`
	result, err := l.client.Eval(ctx, script, []string{l.key}, l.value, int(ttl.Seconds())).Result()
	if err != nil {
		return fmt.Errorf("failed to extend lock: %w", err)
	}
	if result.(int64) == 0 {
		return ErrLockNotHeld
	}
	l.ttl = ttl
	return nil
}

// IsLocked 检查锁是否被持有
func (l *RedisLock) IsLocked(ctx context.Context) (bool, error) {
	result, err := l.client.Exists(ctx, l.key).Result()
	if err != nil {
		return false, fmt.Errorf("failed to check lock status: %w", err)
	}
	return result > 0, nil
}

// generateLockValue 生成锁的唯一值
func generateLockValue() string {
	return fmt.Sprintf("%d", time.Now().UnixNano())
}

// LockManager 锁管理器
type LockManager struct {
	redisClient *redis.Client
	defaultTTL  time.Duration
}

// NewLockManager 创建锁管理器
func NewLockManager(redisClient *redis.Client, defaultTTL time.Duration) *LockManager {
	return &LockManager{
		redisClient: redisClient,
		defaultTTL:  defaultTTL,
	}
}

// CreateLock 创建锁
func (m *LockManager) CreateLock(key string) Lock {
	return NewRedisLock(m.redisClient, key, m.defaultTTL)
}

// CreateLockWithTTL 创建带自定义 TTL 的锁
func (m *LockManager) CreateLockWithTTL(key string, ttl time.Duration) Lock {
	return NewRedisLock(m.redisClient, key, ttl)
}

// TryLock 尝试获取锁，如果失败立即返回
func (m *LockManager) TryLock(ctx context.Context, key string) (Lock, error) {
	lock := m.CreateLock(key)
	if err := lock.Lock(ctx); err != nil {
		return nil, err
	}
	return lock, nil
}

// LockWithRetry 带重试的获取锁
func (m *LockManager) LockWithRetry(ctx context.Context, key string, maxRetries int, retryInterval time.Duration) (Lock, error) {
	var lastErr error
	for i := 0; i < maxRetries; i++ {
		lock := m.CreateLock(key)
		if err := lock.Lock(ctx); err == nil {
			return lock, nil
		} else {
			lastErr = err
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(retryInterval):
				continue
			}
		}
	}
	return nil, lastErr
}

// GenerateBlockLockKey 生成区块锁的 key
func GenerateBlockLockKey(chainID string, blockNumber uint64) string {
	return fmt.Sprintf("lock:block:%s:%d", chainID, blockNumber)
}

// GenerateChainLockKey 生成链锁的 key
func GenerateChainLockKey(chainID string) string {
	return fmt.Sprintf("lock:chain:%s", chainID)
}

// GenerateShardLockKey 生成分片锁的 key
func GenerateShardLockKey(chainID string, shardID int) string {
	return fmt.Sprintf("lock:shard:%s:%d", chainID, shardID)
}

// GenerateReorgLockKey 生成回滚锁的 key
func GenerateReorgLockKey(chainID string) string {
	return fmt.Sprintf("lock:reorg:%s", chainID)
}

// GenerateWatermarkLockKey 生成水位锁的 key
func GenerateWatermarkLockKey(chainID string) string {
	return fmt.Sprintf("lock:watermark:%s", chainID)
}