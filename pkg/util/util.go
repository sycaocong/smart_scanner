package util

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"math/big"
	"runtime"
	"strconv"
	"strings"
	"time"
)

// util 工具函数包
// 提供通用的工具函数，包括大整数处理、字符串操作、时间处理等
// [Design: 工具模块](../docs/DESIGN_SCANNER.md#1-系统概述)

// GenerateID 生成唯一ID
func GenerateID(chainID string, txHash string, logIndex uint64) string {
	data := fmt.Sprintf("%s-%s-%d", chainID, txHash, logIndex)
	hash := sha256.Sum256([]byte(data))
	return hex.EncodeToString(hash[:])
}

// ParseBigInt 解析大整数
func ParseBigInt(value string) (*big.Int, error) {
	if strings.HasPrefix(value, "0x") {
		value = value[2:]
	}
	i := new(big.Int)
	_, success := i.SetString(value, 16)
	if !success {
		return nil, fmt.Errorf("failed to parse big int: %s", value)
	}
	return i, nil
}

// FormatWeiToEther 将 Wei 转换为 Ether
func FormatWeiToEther(wei *big.Int) *big.Float {
	if wei == nil {
		return big.NewFloat(0)
	}
	weiFloat := new(big.Float).SetInt(wei)
	ether := new(big.Float).Quo(weiFloat, big.NewFloat(1e18))
	return ether
}

// FormatEtherToWei 将 Ether 转换为 Wei
func FormatEtherToWei(ether *big.Float) *big.Int {
	if ether == nil {
		return big.NewInt(0)
	}
	wei := new(big.Float).Mul(ether, big.NewFloat(1e18))
	result := new(big.Int)
	wei.Int(result)
	return result
}

// CalculateShard 计算分片ID
func CalculateShard(value uint64, shardCount int) int {
	return int(value % uint64(shardCount))
}

// CalculateAddressShard 根据地址计算分片ID
func CalculateAddressShard(address string, shardCount int) int {
	if len(address) < 2 {
		return 0
	}
	// 使用地址的最后几位字符计算分片
	suffix := address[len(address)-2:]
	hash := sha256.Sum256([]byte(suffix))
	shard := int(hash[0]) % shardCount
	return shard
}

// Retry 带重试的函数执行
func Retry(fn func() error, maxRetries int, delay time.Duration) error {
	var lastErr error
	for i := 0; i < maxRetries; i++ {
		if err := fn(); err != nil {
			lastErr = err
			time.Sleep(delay)
			continue
		}
		return nil
	}
	return lastErr
}

// SafeGo 安全启动 goroutine
func SafeGo(fn func()) {
	go func() {
		defer func() {
			if r := recover(); r != nil {
				fmt.Printf("Recovered from panic: %v\n", r)
			}
		}()
		fn()
	}()
}

// GetGoroutineCount 获取当前 goroutine 数量
func GetGoroutineCount() int {
	return runtime.NumGoroutine()
}

// TruncateString 截断字符串
func TruncateString(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen] + "..."
}

// IsValidAddress 验证地址格式
func IsValidAddress(address string) bool {
	if len(address) < 2 {
		return false
	}
	if strings.HasPrefix(address, "0x") {
		if len(address) != 42 {
			return false
		}
		_, err := hex.DecodeString(address[2:])
		return err == nil
	}
	return true
}

// ParseBlockNumber 解析区块号
func ParseBlockNumber(blockNumber string) (uint64, error) {
	if strings.HasPrefix(blockNumber, "0x") {
		// 十六进制
		i := new(big.Int)
		_, success := i.SetString(blockNumber[2:], 16)
		if !success {
			return 0, fmt.Errorf("invalid hex block number: %s", blockNumber)
		}
		if !i.IsUint64() {
			return 0, fmt.Errorf("block number too large: %s", blockNumber)
		}
		return i.Uint64(), nil
	}
	// 十进制
	return strconv.ParseUint(blockNumber, 10, 64)
}

// FormatBlockNumber 格式化区块号
func FormatBlockNumber(blockNumber uint64) string {
	return fmt.Sprintf("0x%x", blockNumber)
}

// GetCurrentTimestamp 获取当前时间戳（秒）
func GetCurrentTimestamp() int64 {
	return time.Now().Unix()
}

// GetCurrentTimestampMs 获取当前时间戳（毫秒）
func GetCurrentTimestampMs() int64 {
	return time.Now().UnixMilli()
}

// TimestampToTime 时间戳转时间
func TimestampToTime(timestamp int64) time.Time {
	return time.Unix(timestamp, 0)
}

// TimeToTimestamp 时间转时间戳
func TimeToTimestamp(t time.Time) int64 {
	return t.Unix()
}

// CalculateDuration 计算持续时间
func CalculateDuration(start time.Time) time.Duration {
	return time.Since(start)
}

// FormatDuration 格式化持续时间
func FormatDuration(d time.Duration) string {
	if d < time.Second {
		return fmt.Sprintf("%dms", d.Milliseconds())
	} else if d < time.Minute {
		return fmt.Sprintf("%.2fs", d.Seconds())
	} else if d < time.Hour {
		return fmt.Sprintf("%.2fm", d.Minutes())
	}
	return fmt.Sprintf("%.2fh", d.Hours())
}

// MinInt 取最小整数
func MinInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// MaxInt 取最大整数
func MaxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// MinUint64 取最小 uint64
func MinUint64(a, b uint64) uint64 {
	if a < b {
		return a
	}
	return b
}

// MaxUint64 取最大 uint64
func MaxUint64(a, b uint64) uint64 {
	if a > b {
		return a
	}
	return b
}

// ContainsString 检查字符串切片是否包含某个字符串
func ContainsString(slice []string, item string) bool {
	for _, s := range slice {
		if s == item {
			return true
		}
	}
	return false
}

// RemoveDuplicateStrings 去除字符串切片中的重复项
func RemoveDuplicateStrings(slice []string) []string {
	keys := make(map[string]bool)
	var result []string
	for _, item := range slice {
		if !keys[item] {
			keys[item] = true
			result = append(result, item)
		}
	}
	return result
}

// ChunkStringSlice 将字符串切片分块
func ChunkStringSlice(slice []string, chunkSize int) [][]string {
	var chunks [][]string
	for i := 0; i < len(slice); i += chunkSize {
		end := i + chunkSize
		if end > len(slice) {
			end = len(slice)
		}
		chunks = append(chunks, slice[i:end])
	}
	return chunks
}

// IsZeroAddress 检查是否为零地址
func IsZeroAddress(address string) bool {
	return address == "0x0000000000000000000000000000000000000000" || address == ""
}

// SanitizeString 清理字符串
func SanitizeString(s string) string {
	// 去除首尾空格
	s = strings.TrimSpace(s)
	// 去除控制字符
	var result strings.Builder
	for _, r := range s {
		if r >= 32 && r != 127 {
			result.WriteRune(r)
		}
	}
	return result.String()
}

// FormatBytes 格式化字节数
func FormatBytes(bytes uint64) string {
	const unit = 1024
	if bytes < unit {
		return fmt.Sprintf("%d B", bytes)
	}
	div, exp := uint64(unit), 0
	for n := bytes / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %ciB", float64(bytes)/float64(div), "KMGTPE"[exp])
}

// FormatNumber 格式化数字
func FormatNumber(num int64) string {
	str := strconv.FormatInt(num, 10)
	var result []string
	for i := len(str); i > 0; i -= 3 {
		start := MaxInt(0, i-3)
		result = append([]string{str[start:i]}, result...)
	}
	return strings.Join(result, ",")
}

// FormatBalance 格式化余额（将 wei 转换为可读格式）
func FormatBalance(balance *big.Int, decimals int) string {
	if balance == nil {
		return "0"
	}

	decimal := big.NewInt(1)
	for i := 0; i < decimals; i++ {
		decimal.Mul(decimal, big.NewInt(10))
	}

	div := new(big.Int)
	mod := new(big.Int)
	div.DivMod(balance, decimal, mod)

	modStr := fmt.Sprintf("%0*d", decimals, mod)
	if decimals > 0 {
		modStr = strings.TrimRight(modStr, "0")
		if modStr == "" {
			return div.String()
		}
		return fmt.Sprintf("%s.%s", div.String(), modStr)
	}

	return div.String()
}

// ParseStringResult 解析 EVM 字符串返回结果
func ParseStringResult(result string) string {
	if !strings.HasPrefix(result, "0x") {
		return result
	}

	data := result[2:]
	if len(data) < 64 {
		return ""
	}

	lengthHex := data[:64]
	length, err := ParseBigInt(lengthHex)
	if err != nil {
		return ""
	}

	strData := data[64 : 64+length.Int64()*2]
	bytes, err := hex.DecodeString(strData)
	if err != nil {
		return ""
	}

	return string(bytes)
}