package consumer

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/smart-scanner/multi-chain-scanner/pkg/logger"
	"github.com/smart-scanner/multi-chain-scanner/pkg/types"
	"go.uber.org/zap"
)

// WebhookHandler Webhook 处理器接口
// [Design: BusinessConsumer 业务消费者](../docs/DESIGN_SCANNER.md#61-businessconsumer-业务消费者)
type WebhookHandler interface {
	// SendTransaction 发送交易通知
	SendTransaction(ctx context.Context, tx *types.Transaction) error
	
	// SendLog 发送日志通知
	SendLog(ctx context.Context, log *types.Log) error
	
	// SendReorgEvent 发送回滚事件通知
	SendReorgEvent(ctx context.Context, event *types.ReorgEvent) error
	
	// SendCustomEvent 发送自定义事件通知
	SendCustomEvent(ctx context.Context, eventType string, data interface{}) error
	
	// HealthCheck 健康检查
	HealthCheck(ctx context.Context) error
}

// HTTPWebhookHandler HTTP Webhook 处理器实现
type HTTPWebhookHandler struct {
	config      *WebhookConfig
	httpClient  *http.Client
	mu          sync.RWMutex
	statistics  *WebhookStatistics
}

// WebhookConfig Webhook 配置
type WebhookConfig struct {
	// Webhook URL
	URL string
	
	// 超时时间
	Timeout time.Duration
	
	// 重试次数
	RetryCount int
	
	// 重试间隔
	RetryInterval time.Duration
	
	// 是否启用签名
	EnableSignature bool
	
	// 签名密钥
	SecretKey string
	
	// 请求头
	Headers map[string]string
	
	// 是否启用压缩
	EnableCompression bool
}

// WebhookStatistics Webhook 统计信息
type WebhookStatistics struct {
	TotalSent      int64     `json:"total_sent"`
	SuccessCount   int64     `json:"success_count"`
	FailureCount   int64     `json:"failure_count"`
	LastSuccessTime time.Time `json:"last_success_time,omitempty"`
	LastFailureTime time.Time `json:"last_failure_time,omitempty"`
	AverageLatency  time.Duration `json:"average_latency"`
}

// NewHTTPWebhookHandler 创建 HTTP Webhook 处理器
func NewHTTPWebhookHandler(url string, timeout time.Duration, retryCount int) *HTTPWebhookHandler {
	config := &WebhookConfig{
		URL:           url,
		Timeout:       timeout,
		RetryCount:    retryCount,
		RetryInterval: 1 * time.Second,
		Headers:       make(map[string]string),
	}

	// 设置默认请求头
	config.Headers["Content-Type"] = "application/json"
	config.Headers["User-Agent"] = "SmartScanner/1.0"

	return &HTTPWebhookHandler{
		config: config,
		httpClient: &http.Client{
			Timeout: timeout,
		},
		statistics: &WebhookStatistics{},
	}
}

// SendTransaction 发送交易通知
func (wh *HTTPWebhookHandler) SendTransaction(ctx context.Context, tx *types.Transaction) error {
	payload := map[string]interface{}{
		"event_type": "transaction",
		"timestamp":  time.Now().Unix(),
		"data":       tx,
	}

	return wh.sendWithRetry(ctx, payload)
}

// SendLog 发送日志通知
func (wh *HTTPWebhookHandler) SendLog(ctx context.Context, log *types.Log) error {
	payload := map[string]interface{}{
		"event_type": "log",
		"timestamp":  time.Now().Unix(),
		"data":       log,
	}

	return wh.sendWithRetry(ctx, payload)
}

// SendReorgEvent 发送回滚事件通知
func (wh *HTTPWebhookHandler) SendReorgEvent(ctx context.Context, event *types.ReorgEvent) error {
	payload := map[string]interface{}{
		"event_type": "reorg",
		"timestamp":  time.Now().Unix(),
		"data":       event,
	}

	return wh.sendWithRetry(ctx, payload)
}

// SendCustomEvent 发送自定义事件通知
func (wh *HTTPWebhookHandler) SendCustomEvent(ctx context.Context, eventType string, data interface{}) error {
	payload := map[string]interface{}{
		"event_type": eventType,
		"timestamp":  time.Now().Unix(),
		"data":       data,
	}

	return wh.sendWithRetry(ctx, payload)
}

// sendWithRetry 发送请求并重试
func (wh *HTTPWebhookHandler) sendWithRetry(ctx context.Context, payload interface{}) error {
	var lastErr error

	for attempt := 0; attempt <= wh.config.RetryCount; attempt++ {
		if attempt > 0 {
			logger.Debug("Retrying webhook request",
				zap.Int("attempt", attempt),
				zap.Int("max_retries", wh.config.RetryCount),
				zap.Error(lastErr))

			// 等待重试间隔
			select {
			case <-time.After(wh.config.RetryInterval):
			case <-ctx.Done():
				return ctx.Err()
			}
		}

		err := wh.sendRequest(ctx, payload)
		if err == nil {
			return nil
		}

		lastErr = err
		logger.Warn("Webhook request failed",
			zap.Int("attempt", attempt+1),
			zap.Error(err))
	}

	return fmt.Errorf("webhook request failed after %d attempts: %w", wh.config.RetryCount+1, lastErr)
}

// sendRequest 发送 HTTP 请求
func (wh *HTTPWebhookHandler) sendRequest(ctx context.Context, payload interface{}) error {
	startTime := time.Now()

	// 序列化 payload
	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("failed to marshal payload: %w", err)
	}

	// 创建请求
	req, err := http.NewRequestWithContext(ctx, "POST", wh.config.URL, bytes.NewBuffer(body))
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	// 设置请求头
	for key, value := range wh.config.Headers {
		req.Header.Set(key, value)
	}

	// 如果启用签名，添加签名头
	if wh.config.EnableSignature && wh.config.SecretKey != "" {
		signature := wh.generateSignature(body)
		req.Header.Set("X-Signature", signature)
	}

	// 发送请求
	resp, err := wh.httpClient.Do(req)
	if err != nil {
		wh.recordFailure(time.Since(startTime))
		return fmt.Errorf("failed to send request: %w", err)
	}
	defer resp.Body.Close()

	// 读取响应
	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		wh.recordFailure(time.Since(startTime))
		return fmt.Errorf("failed to read response: %w", err)
	}

	// 检查响应状态码
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		wh.recordFailure(time.Since(startTime))
		return fmt.Errorf("webhook returned error status: %d, body: %s", resp.StatusCode, string(respBody))
	}

	wh.recordSuccess(time.Since(startTime))

	logger.Debug("Webhook request succeeded",
		zap.Int("status_code", resp.StatusCode),
		zap.Duration("latency", time.Since(startTime)))

	return nil
}

// generateSignature 生成签名
func (wh *HTTPWebhookHandler) generateSignature(body []byte) string {
	// 这里应该实现 HMAC-SHA256 签名
	// 简化实现：直接返回 body 的 base64 编码
	return fmt.Sprintf("%x", body)
}

// recordSuccess 记录成功
func (wh *HTTPWebhookHandler) recordSuccess(latency time.Duration) {
	wh.mu.Lock()
	defer wh.mu.Unlock()

	wh.statistics.TotalSent++
	wh.statistics.SuccessCount++
	wh.statistics.LastSuccessTime = time.Now()

	// 计算平均延迟
	totalLatency := wh.statistics.AverageLatency * time.Duration(wh.statistics.SuccessCount-1)
	wh.statistics.AverageLatency = (totalLatency + latency) / time.Duration(wh.statistics.SuccessCount)
}

// recordFailure 记录失败
func (wh *HTTPWebhookHandler) recordFailure(latency time.Duration) {
	wh.mu.Lock()
	defer wh.mu.Unlock()

	wh.statistics.TotalSent++
	wh.statistics.FailureCount++
	wh.statistics.LastFailureTime = time.Now()
}

// HealthCheck 健康检查
func (wh *HTTPWebhookHandler) HealthCheck(ctx context.Context) error {
	if wh.config.URL == "" {
		return fmt.Errorf("webhook URL is not configured")
	}

	// 发送一个简单的健康检查请求
	payload := map[string]interface{}{
		"event_type": "health_check",
		"timestamp":  time.Now().Unix(),
	}

	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	return wh.sendRequest(ctx, payload)
}

// GetStatistics 获取统计信息
func (wh *HTTPWebhookHandler) GetStatistics() *WebhookStatistics {
	wh.mu.RLock()
	defer wh.mu.RUnlock()

	// 返回统计信息的副本
	stats := *wh.statistics
	return &stats
}

// BatchWebhookHandler 批量 Webhook 处理器实现
type BatchWebhookHandler struct {
	webhookHandler WebhookHandler
	config         *BatchWebhookConfig
	buffer         []interface{}
	mu             sync.Mutex
	flushTicker    *time.Ticker
	done           chan struct{}
}

// BatchWebhookConfig 批量 Webhook 配置
type BatchWebhookConfig struct {
	// 批量大小
	BatchSize int
	
	// 刷新间隔
	FlushInterval time.Duration
	
	// 最大缓冲区大小
	MaxBufferSize int
}

// NewBatchWebhookHandler 创建批量 Webhook 处理器
func NewBatchWebhookHandler(
	webhookHandler WebhookHandler,
	config *BatchWebhookConfig,
) *BatchWebhookHandler {
	if config == nil {
		config = &BatchWebhookConfig{
			BatchSize:      100,
			FlushInterval: 5 * time.Second,
			MaxBufferSize: 1000,
		}
	}

	handler := &BatchWebhookHandler{
		webhookHandler: webhookHandler,
		config:         config,
		buffer:         make([]interface{}, 0, config.BatchSize),
		flushTicker:    time.NewTicker(config.FlushInterval),
		done:           make(chan struct{}),
	}

	// 启动后台刷新协程
	go handler.flushLoop()

	return handler
}

// SendTransaction 发送交易通知
func (bwh *BatchWebhookHandler) SendTransaction(ctx context.Context, tx *types.Transaction) error {
	payload := map[string]interface{}{
		"event_type": "transaction",
		"timestamp":  time.Now().Unix(),
		"data":       tx,
	}

	return bwh.addToBuffer(payload)
}

// SendLog 发送日志通知
func (bwh *BatchWebhookHandler) SendLog(ctx context.Context, log *types.Log) error {
	payload := map[string]interface{}{
		"event_type": "log",
		"timestamp":  time.Now().Unix(),
		"data":       log,
	}

	return bwh.addToBuffer(payload)
}

// SendReorgEvent 发送回滚事件通知
func (bwh *BatchWebhookHandler) SendReorgEvent(ctx context.Context, event *types.ReorgEvent) error {
	// 回滚事件需要立即发送
	if err := bwh.flush(); err != nil {
		return err
	}

	return bwh.webhookHandler.SendReorgEvent(ctx, event)
}

// SendCustomEvent 发送自定义事件通知
func (bwh *BatchWebhookHandler) SendCustomEvent(ctx context.Context, eventType string, data interface{}) error {
	payload := map[string]interface{}{
		"event_type": eventType,
		"timestamp":  time.Now().Unix(),
		"data":       data,
	}

	return bwh.addToBuffer(payload)
}

// addToBuffer 添加到缓冲区
func (bwh *BatchWebhookHandler) addToBuffer(payload interface{}) error {
	bwh.mu.Lock()
	defer bwh.mu.Unlock()

	bwh.buffer = append(bwh.buffer, payload)

	// 检查是否达到批量大小
	if len(bwh.buffer) >= bwh.config.BatchSize {
		return bwh.flush()
	}

	// 检查是否超过最大缓冲区大小
	if len(bwh.buffer) >= bwh.config.MaxBufferSize {
		logger.Warn("Buffer size exceeds maximum, forcing flush",
			zap.Int("buffer_size", len(bwh.buffer)),
			zap.Int("max_buffer_size", bwh.config.MaxBufferSize))
		return bwh.flush()
	}

	return nil
}

// flush 刷新缓冲区
func (bwh *BatchWebhookHandler) flush() error {
	bwh.mu.Lock()
	defer bwh.mu.Unlock()

	if len(bwh.buffer) == 0 {
		return nil
	}

	// 创建批量 payload
	batchPayload := map[string]interface{}{
		"event_type": "batch",
		"timestamp":  time.Now().Unix(),
		"batch_size": len(bwh.buffer),
		"events":     bwh.buffer,
	}

	// 清空缓冲区
	bwh.buffer = make([]interface{}, 0, bwh.config.BatchSize)

	// 发送批量请求
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// 使用底层 webhook 处理器发送
	// 这里需要将批量 payload 转换为适当的格式
	// 简化实现：直接调用底层处理器
	return bwh.webhookHandler.SendCustomEvent(ctx, "batch", batchPayload)
}

// flushLoop 刷新循环
func (bwh *BatchWebhookHandler) flushLoop() {
	for {
		select {
		case <-bwh.flushTicker.C:
			if err := bwh.flush(); err != nil {
				logger.Error("Failed to flush buffer", zap.Error(err))
			}
		case <-bwh.done:
			return
		}
	}
}

// Stop 停止批量处理器
func (bwh *BatchWebhookHandler) Stop() error {
	// 停止刷新循环
	close(bwh.done)
	bwh.flushTicker.Stop()

	// 刷新剩余数据
	return bwh.flush()
}

// HealthCheck 健康检查
func (bwh *BatchWebhookHandler) HealthCheck(ctx context.Context) error {
	return bwh.webhookHandler.HealthCheck(ctx)
}

// GetStatistics 获取统计信息
func (bwh *BatchWebhookHandler) GetStatistics() interface{} {
	if httpHandler, ok := bwh.webhookHandler.(*HTTPWebhookHandler); ok {
		return httpHandler.GetStatistics()
	}
	return nil
}