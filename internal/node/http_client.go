package node

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"
)

// JSONRPCRequest JSON-RPC 请求
type JSONRPCRequest struct {
	JSONRPC string        `json:"jsonrpc"`
	Method  string        `json:"method"`
	Params  []interface{} `json:"params"`
	ID      int           `json:"id"`
}

// JSONRPCResponse JSON-RPC 响应
type JSONRPCResponse struct {
	JSONRPC string          `json:"jsonrpc"`
	Result  json.RawMessage `json:"result"`
	Error   *JSONRPCError   `json:"error"`
	ID      int             `json:"id"`
}

// JSONRPCError JSON-RPC 错误
type JSONRPCError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

// HTTPClient HTTP RPC 客户端实现
// [Design: NodeManager 节点管理器](../docs/DESIGN_SCANNER.md#42-nodemanager-节点管理器)
type HTTPClient struct {
	url        string
	httpClient *http.Client
	timeout    time.Duration
	requestID  int
	mu         sync.Mutex
}

// NewHTTPClient 创建 HTTP RPC 客户端
func NewHTTPClient(url string, timeout time.Duration) (*HTTPClient, error) {
	if url == "" {
		return nil, fmt.Errorf("URL cannot be empty")
	}

	return &HTTPClient{
		url: url,
		httpClient: &http.Client{
			Timeout: timeout,
			Transport: &http.Transport{
				MaxIdleConns:        100,
				MaxIdleConnsPerHost: 100,
				IdleConnTimeout:     90 * time.Second,
			},
		},
		timeout: timeout,
	}, nil
}

// Call 调用 RPC 方法
func (c *HTTPClient) Call(ctx context.Context, method string, params ...interface{}) (interface{}, error) {
	c.mu.Lock()
	c.requestID++
	id := c.requestID
	c.mu.Unlock()

	// 构建请求
	request := JSONRPCRequest{
		JSONRPC: "2.0",
		Method:  method,
		Params:  params,
		ID:      id,
	}

	requestBody, err := json.Marshal(request)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request: %w", err)
	}

	// 创建 HTTP 请求
	httpReq, err := http.NewRequestWithContext(ctx, "POST", c.url, bytes.NewBuffer(requestBody))
	if err != nil {
		return nil, fmt.Errorf("failed to create HTTP request: %w", err)
	}

	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Accept", "application/json")

	// 发送请求
	resp, err := c.httpClient.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("HTTP request failed: %w", err)
	}
	defer resp.Body.Close()

	// 检查响应状态
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("HTTP error: status=%d, body=%s", resp.StatusCode, string(body))
	}

	// 解析响应
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response body: %w", err)
	}

	var rpcResp JSONRPCResponse
	if err := json.Unmarshal(body, &rpcResp); err != nil {
		return nil, fmt.Errorf("failed to unmarshal response: %w", err)
	}

	// 检查 RPC 错误
	if rpcResp.Error != nil {
		return nil, fmt.Errorf("RPC error: code=%d, message=%s", rpcResp.Error.Code, rpcResp.Error.Message)
	}

	// 检查响应 ID
	if rpcResp.ID != id {
		return nil, fmt.Errorf("response ID mismatch: expected=%d, got=%d", id, rpcResp.ID)
	}

	// 解析结果
	var result interface{}
	if err := json.Unmarshal(rpcResp.Result, &result); err != nil {
		// 如果无法解析为通用接口，返回原始字节数据
		return rpcResp.Result, nil
	}

	return result, nil
}

// BatchCall 批量调用 RPC 方法
func (c *HTTPClient) BatchCall(ctx context.Context, calls []RPCCall) ([]interface{}, error) {
	if len(calls) == 0 {
		return []interface{}{}, nil
	}

	c.mu.Lock()
	baseID := c.requestID + 1
	c.requestID += len(calls)
	c.mu.Unlock()

	// 构建批量请求
	requests := make([]JSONRPCRequest, 0, len(calls))
	for i, call := range calls {
		requests = append(requests, JSONRPCRequest{
			JSONRPC: "2.0",
			Method:  call.Method,
			Params:  call.Params,
			ID:      baseID + i,
		})
	}

	requestBody, err := json.Marshal(requests)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal batch request: %w", err)
	}

	// 创建 HTTP 请求
	httpReq, err := http.NewRequestWithContext(ctx, "POST", c.url, bytes.NewBuffer(requestBody))
	if err != nil {
		return nil, fmt.Errorf("failed to create HTTP request: %w", err)
	}

	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Accept", "application/json")

	// 发送请求
	resp, err := c.httpClient.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("HTTP request failed: %w", err)
	}
	defer resp.Body.Close()

	// 检查响应状态
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("HTTP error: status=%d, body=%s", resp.StatusCode, string(body))
	}

	// 解析响应
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response body: %w", err)
	}

	var rpcResponses []JSONRPCResponse
	if err := json.Unmarshal(body, &rpcResponses); err != nil {
		return nil, fmt.Errorf("failed to unmarshal batch response: %w", err)
	}

	// 检查响应数量
	if len(rpcResponses) != len(calls) {
		return nil, fmt.Errorf("response count mismatch: expected=%d, got=%d", len(calls), len(rpcResponses))
	}

	// 解析结果
	results := make([]interface{}, 0, len(rpcResponses))
	for i, rpcResp := range rpcResponses {
		// 检查 RPC 错误
		if rpcResp.Error != nil {
			results = append(results, fmt.Errorf("RPC error in call %d: code=%d, message=%s", i, rpcResp.Error.Code, rpcResp.Error.Message))
			continue
		}

		// 检查响应 ID
		expectedID := baseID + i
		if rpcResp.ID != expectedID {
			results = append(results, fmt.Errorf("response ID mismatch in call %d: expected=%d, got=%d", i, expectedID, rpcResp.ID))
			continue
		}

		// 解析结果
		var result interface{}
		if err := json.Unmarshal(rpcResp.Result, &result); err != nil {
			// 如果无法解析为通用接口，返回原始字节数据
			results = append(results, rpcResp.Result)
			continue
		}

		results = append(results, result)
	}

	return results, nil
}

// Close 关闭客户端
func (c *HTTPClient) Close() error {
	c.httpClient.CloseIdleConnections()
	return nil
}

// GetURL 获取客户端 URL
func (c *HTTPClient) GetURL() string {
	return c.url
}

// GetTimeout 获取超时时间
func (c *HTTPClient) GetTimeout() time.Duration {
	return c.timeout
}