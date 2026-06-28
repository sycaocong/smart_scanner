package node

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/common/hexutil"
)

// MockEVMNode 模拟 EVM 节点（开发测试用）
// [Design: ChainClient 链客户端接口](../docs/DESIGN_SCANNER.md#41-chainclient-链客户端接口)
type MockEVMNode struct {
	chainID       uint64
	port          int
	server        *http.Server
	currentBlock  uint64
	mu            sync.RWMutex
}

func NewMockEVMNode(chainID uint64, port int) *MockEVMNode {
	return &MockEVMNode{
		chainID:      chainID,
		port:         port,
		currentBlock: 19000000,
	}
}

func (n *MockEVMNode) Start() error {
	mux := http.NewServeMux()
	
	mux.HandleFunc("/", n.handleRequest)
	
	n.server = &http.Server{
		Addr:    fmt.Sprintf(":%d", n.port),
		Handler: mux,
	}
	
	go func() {
		if err := n.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			fmt.Printf("Mock node server error: %v\n", err)
		}
	}()
	
	n.startBlockGenerator()
	fmt.Printf("Mock EVM node started on http://localhost:%d (chain ID: %d)\n", n.port, n.chainID)
	return nil
}

func (n *MockEVMNode) startBlockGenerator() {
	go func() {
		for {
			time.Sleep(12 * time.Second)
			n.mu.Lock()
			n.currentBlock++
			n.mu.Unlock()
		}
	}()
}

func (n *MockEVMNode) Stop() error {
	return n.server.Close()
}

func (n *MockEVMNode) handleRequest(w http.ResponseWriter, r *http.Request) {
	var req struct {
		JSONRPC string        `json:"jsonrpc"`
		Method  string        `json:"method"`
		Params  []interface{} `json:"params"`
		ID      int           `json:"id"`
	}
	
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request", http.StatusBadRequest)
		return
	}
	
	w.Header().Set("Content-Type", "application/json")
	
	var result interface{}
	var err error
	
	switch req.Method {
	case "eth_blockNumber":
		result, err = n.handleBlockNumber()
	case "eth_getBlockByNumber":
		result, err = n.handleGetBlockByNumber(req.Params)
	case "eth_getBlockByHash":
		result, err = n.handleGetBlockByHash(req.Params)
	case "eth_getTransactionByHash":
		result, err = n.handleGetTransactionByHash(req.Params)
	case "eth_getTransactionReceipt":
		result, err = n.handleGetTransactionReceipt(req.Params)
	case "eth_getLogs":
		result, err = n.handleGetLogs(req.Params)
	case "eth_chainId":
		result, err = n.handleChainID()
	case "net_version":
		result, err = n.handleNetVersion()
	default:
		err = fmt.Errorf("method not found: %s", req.Method)
	}
	
	if err != nil {
		json.NewEncoder(w).Encode(map[string]interface{}{
			"jsonrpc": "2.0",
			"error": map[string]interface{}{
				"code":    -32601,
				"message": err.Error(),
			},
			"id": req.ID,
		})
		return
	}
	
	json.NewEncoder(w).Encode(map[string]interface{}{
		"jsonrpc": "2.0",
		"result":  result,
		"id":      req.ID,
	})
}

func (n *MockEVMNode) handleBlockNumber() (interface{}, error) {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return hexutil.EncodeUint64(n.currentBlock), nil
}

func (n *MockEVMNode) handleGetBlockByNumber(params []interface{}) (interface{}, error) {
	if len(params) == 0 {
		return nil, fmt.Errorf("missing block number")
	}
	
	blockNumStr := params[0].(string)
	var blockNum uint64
	
	if blockNumStr == "latest" {
		n.mu.RLock()
		blockNum = n.currentBlock
		n.mu.RUnlock()
	} else {
		num, err := hexutil.DecodeUint64(blockNumStr)
		if err != nil {
			return nil, err
		}
		blockNum = num
	}
	
	return n.createMockBlockResponse(blockNum), nil
}

func (n *MockEVMNode) handleGetBlockByHash(params []interface{}) (interface{}, error) {
	if len(params) == 0 {
		return nil, fmt.Errorf("missing block hash")
	}
	
	hash := params[0].(string)
	
	if hash == "0x0000000000000000000000000000000000000000000000000000000000000000" {
		return nil, nil
	}
	
	n.mu.RLock()
	defer n.mu.RUnlock()
	
	for i := int64(n.currentBlock) - 1000; i <= int64(n.currentBlock); i++ {
		if i <= 0 {
			continue
		}
		if fmt.Sprintf("0x%064x", uint64(i)) == hash {
			return n.createMockBlockResponse(uint64(i)), nil
		}
	}
	
	return nil, nil
}

func (n *MockEVMNode) handleGetTransactionByHash(params []interface{}) (interface{}, error) {
	if len(params) == 0 {
		return nil, fmt.Errorf("missing transaction hash")
	}
	
	n.mu.RLock()
	current := n.currentBlock
	n.mu.RUnlock()
	
	value, _ := hexutil.DecodeBig("0xDE0B6B3A7640000")
	
	return map[string]interface{}{
		"hash":             params[0].(string),
		"blockNumber":      "0x" + strconv.FormatUint(current, 16),
		"from":             "0x742d35Cc6634C0532925a3b844Bc9e7595f7AAA",
		"to":               "0xdAC17F958D2ee523a2206206994597C13D831ec7",
		"value":            hexutil.EncodeBig(value),
		"gas":              "0x5208",
		"gasPrice":         "0x3B9ACA00",
		"input":            "0x",
		"nonce":            "0x123",
		"transactionIndex": "0x0",
	}, nil
}

func (n *MockEVMNode) handleGetTransactionReceipt(params []interface{}) (interface{}, error) {
	if len(params) == 0 {
		return nil, fmt.Errorf("missing transaction hash")
	}
	
	n.mu.RLock()
	current := n.currentBlock
	n.mu.RUnlock()
	
	return map[string]interface{}{
		"transactionHash":   params[0].(string),
		"blockNumber":      "0x" + strconv.FormatUint(current, 16),
		"from":             "0x742d35Cc6634C0532925a3b844Bc9e7595f7AAA",
		"to":               "0xdAC17F958D2ee523a2206206994597C13D831ec7",
		"cumulativeGasUsed": "0x2710",
		"gasUsed":          "0x2710",
		"status":           "0x1",
		"logs": []interface{}{
			map[string]interface{}{
				"address":          "0xdAC17F958D2ee523a2206206994597C13D831ec7",
				"topics":           []string{"0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"},
				"data":             "0x00000000000000000000000000000000000000000000000000000000DE0B6B3A7640000",
				"blockNumber":      "0x" + strconv.FormatUint(current, 16),
				"transactionHash":  params[0].(string),
				"logIndex":         "0x0",
			},
		},
	}, nil
}

func (n *MockEVMNode) handleGetLogs(params []interface{}) (interface{}, error) {
	if len(params) == 0 {
		return nil, fmt.Errorf("missing filter")
	}
	
	filter := params[0].(map[string]interface{})
	fromBlockStr := filter["fromBlock"].(string)
	toBlockStr := filter["toBlock"].(string)
	
	var fromBlock, toBlock uint64
	if fromBlockStr == "latest" {
		n.mu.RLock()
		fromBlock = n.currentBlock
		n.mu.RUnlock()
	} else {
		num, err := hexutil.DecodeUint64(fromBlockStr)
		if err != nil {
			return nil, err
		}
		fromBlock = num
	}
	
	if toBlockStr == "latest" {
		n.mu.RLock()
		toBlock = n.currentBlock
		n.mu.RUnlock()
	} else {
		num, err := hexutil.DecodeUint64(toBlockStr)
		if err != nil {
			return nil, err
		}
		toBlock = num
	}
	
	var logs []interface{}
	for i := fromBlock; i <= toBlock; i++ {
		logs = append(logs, map[string]interface{}{
			"address":         "0xdAC17F958D2ee523a2206206994597C13D831ec7",
			"topics":          []string{"0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"},
			"data":            "0x00000000000000000000000000000000000000000000000000000000DE0B6B3A7640000",
			"blockNumber":     "0x" + strconv.FormatUint(i, 16),
			"blockHash":       "0x" + fmt.Sprintf("%064x", i),
			"transactionHash": "0x" + fmt.Sprintf("%064x", i*1000),
			"logIndex":        "0x0",
		})
	}
	
	return logs, nil
}

func (n *MockEVMNode) handleChainID() (interface{}, error) {
	return hexutil.EncodeUint64(n.chainID), nil
}

func (n *MockEVMNode) handleNetVersion() (interface{}, error) {
	return strconv.FormatUint(n.chainID, 10), nil
}

func (n *MockEVMNode) createMockBlockResponse(number uint64) interface{} {
	n.mu.RLock()
	current := n.currentBlock
	n.mu.RUnlock()
	
	var txs []interface{}
	if number == current {
		txs = []interface{}{
			"0x" + fmt.Sprintf("%064x", number*1000),
			"0x" + fmt.Sprintf("%064x", number*1001),
		}
	} else {
		txs = []interface{}{
			"0x" + fmt.Sprintf("%064x", number*1000),
		}
	}
	
	return map[string]interface{}{
		"number":         "0x" + strconv.FormatUint(number, 16),
		"hash":           "0x" + fmt.Sprintf("%064x", number),
		"parentHash":     "0x" + fmt.Sprintf("%064x", number-1),
		"timestamp":      "0x" + strconv.FormatUint(uint64(time.Now().Unix()), 16),
		"transactions":   txs,
		"logsBloom":      "0x0",
		"difficulty":     "0x0",
		"gasLimit":       "0x1C9C380",
		"gasUsed":        "0x0",
		"miner":          "0x0000000000000000000000000000000000000000",
		"nonce":          "0x0000000000000000",
		"size":           "0x200",
		"stateRoot":      "0x0000000000000000000000000000000000000000000000000000000000000000",
		"transactionsRoot": "0x0000000000000000000000000000000000000000000000000000000000000000",
		"receiptsRoot":     "0x0000000000000000000000000000000000000000000000000000000000000000",
	}
}

func (n *MockEVMNode) GetCurrentHeight() uint64 {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.currentBlock
}

func (n *MockEVMNode) GetChainID() uint64 {
	return n.chainID
}

func (n *MockEVMNode) GetPort() int {
	return n.port
}