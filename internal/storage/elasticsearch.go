package storage

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esapi"
	"github.com/smart-scanner/multi-chain-scanner/pkg/logger"
	"github.com/smart-scanner/multi-chain-scanner/pkg/types"
	"go.uber.org/zap"
)

// ElasticsearchStorage Elasticsearch 存储实现
// 用于全文搜索和复杂查询，支持按地址、区块范围等条件检索
// [Design: 存储层](../docs/DESIGN_SCANNER.md#1-系统概述)
type ElasticsearchStorage struct {
	client       *elasticsearch.Client
	config       *ElasticsearchConfig
	indexPrefix  string
}

// NewElasticsearchStorage 创建 Elasticsearch 存储
func NewElasticsearchStorage(config *ElasticsearchConfig) (*ElasticsearchStorage, error) {
	if config == nil {
		return nil, fmt.Errorf("config cannot be nil")
	}

	cfg := elasticsearch.Config{
		Addresses: config.Addresses,
		Username:  config.Username,
		Password:  config.Password,
	}

	client, err := elasticsearch.NewClient(cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create Elasticsearch client: %w", err)
	}

	// 测试连接
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	res, err := client.Info()
	if err != nil {
		return nil, fmt.Errorf("failed to connect to Elasticsearch: %w", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		return nil, fmt.Errorf("Elasticsearch returned error: %s", res.Status())
	}

	storage := &ElasticsearchStorage{
		client:      client,
		config:      config,
		indexPrefix: config.IndexPrefix,
	}

	// 创建索引模板
	if err := storage.createIndexTemplates(ctx); err != nil {
		logger.Warn("Failed to create index templates", zap.Error(err))
	}

	return storage, nil
}

// createIndexTemplates 创建索引模板
func (s *ElasticsearchStorage) createIndexTemplates(ctx context.Context) error {
	// 创建区块索引模板
	blockTemplate := map[string]interface{}{
		"index_patterns": []string{fmt.Sprintf("%s-blocks-*", s.indexPrefix)},
		"template": map[string]interface{}{
			"settings": map[string]interface{}{
				"number_of_shards":   3,
				"number_of_replicas": 1,
				"refresh_interval":   "1s",
			},
			"mappings": map[string]interface{}{
				"properties": map[string]interface{}{
					"chain_id":      map[string]interface{}{"type": "keyword"},
					"block_number":  map[string]interface{}{"type": "long"},
					"block_hash":    map[string]interface{}{"type": "keyword"},
					"parent_hash":   map[string]interface{}{"type": "keyword"},
					"timestamp":     map[string]interface{}{"type": "date"},
					"transactions": map[string]interface{}{
						"type": "nested",
						"properties": map[string]interface{}{
							"hash":        map[string]interface{}{"type": "keyword"},
							"from":        map[string]interface{}{"type": "keyword"},
							"to":          map[string]interface{}{"type": "keyword"},
							"value":       map[string]interface{}{"type": "keyword"},
							"status":      map[string]interface{}{"type": "keyword"},
							"block_number": map[string]interface{}{"type": "long"},
						},
					},
				},
			},
		},
	}

	// 创建交易索引模板
	txTemplate := map[string]interface{}{
		"index_patterns": []string{fmt.Sprintf("%s-transactions-*", s.indexPrefix)},
		"template": map[string]interface{}{
			"settings": map[string]interface{}{
				"number_of_shards":   3,
				"number_of_replicas": 1,
				"refresh_interval":   "1s",
			},
			"mappings": map[string]interface{}{
				"properties": map[string]interface{}{
					"chain_id":     map[string]interface{}{"type": "keyword"},
					"tx_hash":      map[string]interface{}{"type": "keyword"},
					"block_number": map[string]interface{}{"type": "long"},
					"block_hash":   map[string]interface{}{"type": "keyword"},
					"from":         map[string]interface{}{"type": "keyword"},
					"to":           map[string]interface{}{"type": "keyword"},
					"value":        map[string]interface{}{"type": "keyword"},
					"gas_price":    map[string]interface{}{"type": "keyword"},
					"gas_used":     map[string]interface{}{"type": "long"},
					"status":       map[string]interface{}{"type": "keyword"},
					"timestamp":    map[string]interface{}{"type": "date"},
					"logs": map[string]interface{}{
						"type": "nested",
						"properties": map[string]interface{}{
							"address":  map[string]interface{}{"type": "keyword"},
							"topics":   map[string]interface{}{"type": "keyword"},
							"log_index": map[string]interface{}{"type": "long"},
						},
					},
				},
			},
		},
	}

	// 创建日志索引模板
	logTemplate := map[string]interface{}{
		"index_patterns": []string{fmt.Sprintf("%s-logs-*", s.indexPrefix)},
		"template": map[string]interface{}{
			"settings": map[string]interface{}{
				"number_of_shards":   3,
				"number_of_replicas": 1,
				"refresh_interval":   "1s",
			},
			"mappings": map[string]interface{}{
				"properties": map[string]interface{}{
					"chain_id":     map[string]interface{}{"type": "keyword"},
					"tx_hash":      map[string]interface{}{"type": "keyword"},
					"block_number": map[string]interface{}{"type": "long"},
					"address":      map[string]interface{}{"type": "keyword"},
					"topics":       map[string]interface{}{"type": "keyword"},
					"log_index":    map[string]interface{}{"type": "long"},
					"tx_index":     map[string]interface{}{"type": "long"},
					"timestamp":    map[string]interface{}{"type": "date"},
				},
			},
		},
	}

	// 应用索引模板
	templates := []map[string]interface{}{
		{"name": fmt.Sprintf("%s-blocks-template", s.indexPrefix), "body": blockTemplate},
		{"name": fmt.Sprintf("%s-transactions-template", s.indexPrefix), "body": txTemplate},
		{"name": fmt.Sprintf("%s-logs-template", s.indexPrefix), "body": logTemplate},
	}

	for _, tmpl := range templates {
		if err := s.putIndexTemplate(ctx, tmpl["name"].(string), tmpl["body"]); err != nil {
			logger.Warn("Failed to put index template",
				zap.String("name", tmpl["name"].(string)),
				zap.Error(err))
		}
	}

	return nil
}

// putIndexTemplate 应用索引模板
func (s *ElasticsearchStorage) putIndexTemplate(ctx context.Context, name string, body interface{}) error {
	data, err := json.Marshal(body)
	if err != nil {
		return fmt.Errorf("failed to marshal template: %w", err)
	}

	res, err := s.client.Indices.PutIndexTemplate(
		name,
		strings.NewReader(string(data)),
	)
	if err != nil {
		return fmt.Errorf("failed to put index template: %w", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		return fmt.Errorf("Elasticsearch returned error: %s", res.Status())
	}

	return nil
}

// generateBlockIndex 生成区块索引名称
func (s *ElasticsearchStorage) generateBlockIndex(chainID string) string {
	return fmt.Sprintf("%s-blocks-%s", s.indexPrefix, chainID)
}

// generateTransactionIndex 生成交易索引名称
func (s *ElasticsearchStorage) generateTransactionIndex(chainID string) string {
	return fmt.Sprintf("%s-transactions-%s", s.indexPrefix, chainID)
}

// generateLogIndex 生成日志索引名称
func (s *ElasticsearchStorage) generateLogIndex(chainID string) string {
	return fmt.Sprintf("%s-logs-%s", s.indexPrefix, chainID)
}

// SaveBlock 保存区块
func (s *ElasticsearchStorage) SaveBlock(ctx context.Context, block *types.Block) error {
	index := s.generateBlockIndex(block.ChainID)
	id := fmt.Sprintf("%d", block.Number)

	// 序列化区块数据
	data, err := json.Marshal(block)
	if err != nil {
		return fmt.Errorf("failed to marshal block: %w", err)
	}

	// 索引区块
	res, err := s.client.Index(
		index,
		strings.NewReader(string(data)),
		s.client.Index.WithDocumentID(id),
		s.client.Index.WithContext(ctx),
	)
	if err != nil {
		return fmt.Errorf("failed to index block: %w", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		return fmt.Errorf("Elasticsearch returned error: %s", res.Status())
	}

	return nil
}

// GetBlock 获取区块
func (s *ElasticsearchStorage) GetBlock(ctx context.Context, chainID string, blockNumber uint64) (*types.Block, error) {
	index := s.generateBlockIndex(chainID)
	id := fmt.Sprintf("%d", blockNumber)

	// 获取区块
	res, err := s.client.Get(
		index,
		id,
		s.client.Get.WithContext(ctx),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to get block: %w", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		if res.StatusCode == 404 {
			return nil, fmt.Errorf("block not found")
		}
		return nil, fmt.Errorf("Elasticsearch returned error: %s", res.Status())
	}

	// 解析响应
	var result map[string]interface{}
	if err := json.NewDecoder(res.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("failed to parse response: %w", err)
	}

	// 提取源数据
	source, ok := result["_source"].(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("invalid source data format")
	}

	// 反序列化区块数据
	data, err := json.Marshal(source)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal source: %w", err)
	}

	var block types.Block
	if err := json.Unmarshal(data, &block); err != nil {
		return nil, fmt.Errorf("failed to unmarshal block: %w", err)
	}

	return &block, nil
}

// GetBlockByHash 根据哈希获取区块
func (s *ElasticsearchStorage) GetBlockByHash(ctx context.Context, chainID string, blockHash string) (*types.Block, error) {
	index := s.generateBlockIndex(chainID)

	// 构建查询
	query := map[string]interface{}{
		"query": map[string]interface{}{
			"term": map[string]interface{}{
				"block_hash": blockHash,
			},
		},
	}

	queryData, err := json.Marshal(query)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal query: %w", err)
	}

	// 执行搜索
	req := esapi.SearchRequest{
		Index: []string{index},
		Body:  bytes.NewReader(queryData),
	}
	res, err := req.Do(ctx, s.client)
	if err != nil {
		return nil, fmt.Errorf("failed to search block: %w", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		return nil, fmt.Errorf("Elasticsearch returned error: %s", res.Status())
	}

	// 解析响应
	var result map[string]interface{}
	if err := json.NewDecoder(res.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("failed to parse response: %w", err)
	}

	// 检查是否找到结果
	hits, ok := result["hits"].(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("invalid hits format")
	}

	total, ok := hits["total"].(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("invalid total format")
	}

	value, ok := total["value"].(float64)
	if !ok || value == 0 {
		return nil, fmt.Errorf("block not found")
	}

	// 提取第一个结果
	hitsList, ok := hits["hits"].([]interface{})
	if !ok || len(hitsList) == 0 {
		return nil, fmt.Errorf("no hits found")
	}

	hit, ok := hitsList[0].(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("invalid hit format")
	}

	source, ok := hit["_source"].(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("invalid source data format")
	}

	// 反序列化区块数据
	data, err := json.Marshal(source)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal source: %w", err)
	}

	var block types.Block
	if err := json.Unmarshal(data, &block); err != nil {
		return nil, fmt.Errorf("failed to unmarshal block: %w", err)
	}

	return &block, nil
}

// GetLatestBlock 获取最新区块
func (s *ElasticsearchStorage) GetLatestBlock(ctx context.Context, chainID string) (*types.Block, error) {
	index := s.generateBlockIndex(chainID)

	// 构建查询
	query := map[string]interface{}{
		"query": map[string]interface{}{
			"match_all": map[string]interface{}{},
		},
		"sort": []map[string]interface{}{
			{
				"block_number": map[string]interface{}{
					"order": "desc",
				},
			},
		},
		"size": 1,
	}

	queryData, err := json.Marshal(query)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal query: %w", err)
	}

	// 执行搜索
	req := esapi.SearchRequest{
		Index: []string{index},
		Body:  bytes.NewReader(queryData),
	}
	res, err := req.Do(ctx, s.client)
	if err != nil {
		return nil, fmt.Errorf("failed to search latest block: %w", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		return nil, fmt.Errorf("Elasticsearch returned error: %s", res.Status())
	}

	// 解析响应
	var result map[string]interface{}
	if err := json.NewDecoder(res.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("failed to parse response: %w", err)
	}

	// 检查是否找到结果
	hits, ok := result["hits"].(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("invalid hits format")
	}

	total, ok := hits["total"].(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("invalid total format")
	}

	value, ok := total["value"].(float64)
	if !ok || value == 0 {
		return nil, fmt.Errorf("no blocks found")
	}

	// 提取第一个结果
	hitsList, ok := hits["hits"].([]interface{})
	if !ok || len(hitsList) == 0 {
		return nil, fmt.Errorf("no hits found")
	}

	hit, ok := hitsList[0].(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("invalid hit format")
	}

	source, ok := hit["_source"].(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("invalid source data format")
	}

	// 反序列化区块数据
	data, err := json.Marshal(source)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal source: %w", err)
	}

	var block types.Block
	if err := json.Unmarshal(data, &block); err != nil {
		return nil, fmt.Errorf("failed to unmarshal block: %w", err)
	}

	return &block, nil
}

// DeleteBlocks 删除区块
func (s *ElasticsearchStorage) DeleteBlocks(ctx context.Context, chainID string, fromBlock, toBlock uint64) error {
	index := s.generateBlockIndex(chainID)

	// 构建删除查询
	query := map[string]interface{}{
		"query": map[string]interface{}{
			"range": map[string]interface{}{
				"block_number": map[string]interface{}{
					"gte": fromBlock,
					"lte": toBlock,
				},
			},
		},
	}

	queryData, err := json.Marshal(query)
	if err != nil {
		return fmt.Errorf("failed to marshal delete query: %w", err)
	}

	// 执行删除
	res, err := s.client.DeleteByQuery(
		[]string{index},
		strings.NewReader(string(queryData)),
		s.client.DeleteByQuery.WithContext(ctx),
	)
	if err != nil {
		return fmt.Errorf("failed to delete blocks: %w", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		return fmt.Errorf("Elasticsearch returned error: %s", res.Status())
	}

	return nil
}

// SaveTransaction 保存交易
func (s *ElasticsearchStorage) SaveTransaction(ctx context.Context, tx *types.Transaction) error {
	index := s.generateTransactionIndex(tx.ChainID)
	id := tx.Hash

	// 序列化交易数据
	data, err := json.Marshal(tx)
	if err != nil {
		return fmt.Errorf("failed to marshal transaction: %w", err)
	}

	// 索引交易
	res, err := s.client.Index(
		index,
		strings.NewReader(string(data)),
		s.client.Index.WithDocumentID(id),
		s.client.Index.WithContext(ctx),
	)
	if err != nil {
		return fmt.Errorf("failed to index transaction: %w", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		return fmt.Errorf("Elasticsearch returned error: %s", res.Status())
	}

	return nil
}

// BatchSaveTransactions 批量保存交易
func (s *ElasticsearchStorage) BatchSaveTransactions(ctx context.Context, txs []*types.Transaction) error {
	if len(txs) == 0 {
		return nil
	}

	// 按链分组
	chainTxs := make(map[string][]*types.Transaction)
	for _, tx := range txs {
		chainTxs[tx.ChainID] = append(chainTxs[tx.ChainID], tx)
	}

	// 为每个链批量索引
	for chainID, transactions := range chainTxs {
		index := s.generateTransactionIndex(chainID)

		// 构建批量请求
		var bulkBody strings.Builder
		for _, tx := range transactions {
			// 添加索引操作
			bulkBody.WriteString(fmt.Sprintf(`{"index":{"_index":"%s","_id":"%s"}}`+"\n", index, tx.Hash))
			
			// 添加文档数据
			data, err := json.Marshal(tx)
			if err != nil {
				logger.Error("Failed to marshal transaction",
					zap.String("chain_id", tx.ChainID),
					zap.String("tx_hash", tx.Hash),
					zap.Error(err))
				continue
			}
			bulkBody.WriteString(string(data) + "\n")
		}

		// 执行批量请求
		res, err := s.client.Bulk(
			strings.NewReader(bulkBody.String()),
			s.client.Bulk.WithContext(ctx),
		)
		if err != nil {
			return fmt.Errorf("failed to bulk index transactions: %w", err)
		}
		defer res.Body.Close()

		if res.IsError() {
			return fmt.Errorf("Elasticsearch returned error: %s", res.Status())
		}
	}

	return nil
}

// GetTransaction 获取交易
func (s *ElasticsearchStorage) GetTransaction(ctx context.Context, chainID string, txHash string) (*types.Transaction, error) {
	index := s.generateTransactionIndex(chainID)
	id := txHash

	// 获取交易
	res, err := s.client.Get(
		index,
		id,
		s.client.Get.WithContext(ctx),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to get transaction: %w", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		if res.StatusCode == 404 {
			return nil, fmt.Errorf("transaction not found")
		}
		return nil, fmt.Errorf("Elasticsearch returned error: %s", res.Status())
	}

	// 解析响应
	var result map[string]interface{}
	if err := json.NewDecoder(res.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("failed to parse response: %w", err)
	}

	// 提取源数据
	source, ok := result["_source"].(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("invalid source data format")
	}

	// 反序列化交易数据
	data, err := json.Marshal(source)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal source: %w", err)
	}

	var tx types.Transaction
	if err := json.Unmarshal(data, &tx); err != nil {
		return nil, fmt.Errorf("failed to unmarshal transaction: %w", err)
	}

	return &tx, nil
}

// GetTransactionsByBlock 根据区块获取交易
func (s *ElasticsearchStorage) GetTransactionsByBlock(ctx context.Context, chainID string, blockNumber uint64) ([]*types.Transaction, error) {
	index := s.generateTransactionIndex(chainID)

	// 构建查询
	query := map[string]interface{}{
		"query": map[string]interface{}{
			"term": map[string]interface{}{
				"block_number": blockNumber,
			},
		},
		"sort": []map[string]interface{}{
			{
				"index": map[string]interface{}{
					"order": "asc",
				},
			},
		},
		"size": 10000, // 最大返回数量
	}

	queryData, err := json.Marshal(query)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal query: %w", err)
	}

	// 执行搜索
	req := esapi.SearchRequest{
		Index: []string{index},
		Body:  bytes.NewReader(queryData),
	}
	res, err := req.Do(ctx, s.client)
	if err != nil {
		return nil, fmt.Errorf("failed to search transactions: %w", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		return nil, fmt.Errorf("Elasticsearch returned error: %s", res.Status())
	}

	// 解析响应
	var result map[string]interface{}
	if err := json.NewDecoder(res.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("failed to parse response: %w", err)
	}

	// 提取结果
	hits, ok := result["hits"].(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("invalid hits format")
	}

	hitsList, ok := hits["hits"].([]interface{})
	if !ok {
		return nil, fmt.Errorf("invalid hits list format")
	}

	transactions := make([]*types.Transaction, 0, len(hitsList))
	for _, hit := range hitsList {
		hitMap, ok := hit.(map[string]interface{})
		if !ok {
			continue
		}

		source, ok := hitMap["_source"].(map[string]interface{})
		if !ok {
			continue
		}

		// 反序列化交易数据
		data, err := json.Marshal(source)
		if err != nil {
			logger.Error("Failed to marshal source", zap.Error(err))
			continue
		}

		var tx types.Transaction
		if err := json.Unmarshal(data, &tx); err != nil {
			logger.Error("Failed to unmarshal transaction", zap.Error(err))
			continue
		}

		transactions = append(transactions, &tx)
	}

	return transactions, nil
}

// GetTransactionsByAddress 根据地址获取交易
func (s *ElasticsearchStorage) GetTransactionsByAddress(ctx context.Context, chainID string, address string, limit, offset int) ([]*types.Transaction, error) {
	index := s.generateTransactionIndex(chainID)

	// 构建查询
	query := map[string]interface{}{
		"query": map[string]interface{}{
			"bool": map[string]interface{}{
				"should": []map[string]interface{}{
					{
						"term": map[string]interface{}{
							"from": address,
						},
					},
					{
						"term": map[string]interface{}{
							"to": address,
						},
					},
				},
				"minimum_should_match": 1,
			},
		},
		"sort": []map[string]interface{}{
			{
				"block_number": map[string]interface{}{
					"order": "desc",
				},
			},
		},
		"from": offset,
		"size": limit,
	}

	queryData, err := json.Marshal(query)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal query: %w", err)
	}

	// 执行搜索
	req := esapi.SearchRequest{
		Index: []string{index},
		Body:  bytes.NewReader(queryData),
	}
	res, err := req.Do(ctx, s.client)
	if err != nil {
		return nil, fmt.Errorf("failed to search transactions by address: %w", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		return nil, fmt.Errorf("Elasticsearch returned error: %s", res.Status())
	}

	// 解析响应
	var result map[string]interface{}
	if err := json.NewDecoder(res.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("failed to parse response: %w", err)
	}

	// 提取结果
	hits, ok := result["hits"].(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("invalid hits format")
	}

	hitsList, ok := hits["hits"].([]interface{})
	if !ok {
		return nil, fmt.Errorf("invalid hits list format")
	}

	transactions := make([]*types.Transaction, 0, len(hitsList))
	for _, hit := range hitsList {
		hitMap, ok := hit.(map[string]interface{})
		if !ok {
			continue
		}

		source, ok := hitMap["_source"].(map[string]interface{})
		if !ok {
			continue
		}

		// 反序列化交易数据
		data, err := json.Marshal(source)
		if err != nil {
			logger.Error("Failed to marshal source", zap.Error(err))
			continue
		}

		var tx types.Transaction
		if err := json.Unmarshal(data, &tx); err != nil {
			logger.Error("Failed to unmarshal transaction", zap.Error(err))
			continue
		}

		transactions = append(transactions, &tx)
	}

	return transactions, nil
}

// UpdateTransactionStatus 更新交易状态
func (s *ElasticsearchStorage) UpdateTransactionStatus(ctx context.Context, chainID string, txHash string, status types.TransactionStatus) error {
	index := s.generateTransactionIndex(chainID)
	id := txHash

	// 构建更新文档
	updateDoc := map[string]interface{}{
		"doc": map[string]interface{}{
			"status": string(status),
		},
	}

	data, err := json.Marshal(updateDoc)
	if err != nil {
		return fmt.Errorf("failed to marshal update document: %w", err)
	}

	// 执行更新
	res, err := s.client.Update(
		index,
		id,
		strings.NewReader(string(data)),
		s.client.Update.WithContext(ctx),
	)
	if err != nil {
		return fmt.Errorf("failed to update transaction: %w", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		return fmt.Errorf("Elasticsearch returned error: %s", res.Status())
	}

	return nil
}

// MarkTransactionsAsReverted 标记交易为已回滚
func (s *ElasticsearchStorage) MarkTransactionsAsReverted(ctx context.Context, chainID string, fromBlock, toBlock uint64) error {
	index := s.generateTransactionIndex(chainID)

	// 构建更新查询
	updateQuery := map[string]interface{}{
		"query": map[string]interface{}{
			"range": map[string]interface{}{
				"block_number": map[string]interface{}{
					"gte": fromBlock,
					"lte": toBlock,
				},
			},
		},
		"script": map[string]interface{}{
			"source": "ctx._source.status = 'reverted'",
		},
	}

	queryData, err := json.Marshal(updateQuery)
	if err != nil {
		return fmt.Errorf("failed to marshal update query: %w", err)
	}

	// 执行批量更新
	req := esapi.UpdateByQueryRequest{
		Index: []string{index},
		Body:  bytes.NewReader(queryData),
	}
	res, err := req.Do(ctx, s.client)
	if err != nil {
		return fmt.Errorf("failed to update transactions: %w", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		return fmt.Errorf("Elasticsearch returned error: %s", res.Status())
	}

	return nil
}

// SaveLog 保存日志
func (s *ElasticsearchStorage) SaveLog(ctx context.Context, log *types.Log) error {
	index := s.generateLogIndex(log.ChainID)
	id := fmt.Sprintf("%s-%d", log.TxHash, log.LogIndex)

	// 序列化日志数据
	data, err := json.Marshal(log)
	if err != nil {
		return fmt.Errorf("failed to marshal log: %w", err)
	}

	// 索引日志
	res, err := s.client.Index(
		index,
		strings.NewReader(string(data)),
		s.client.Index.WithDocumentID(id),
		s.client.Index.WithContext(ctx),
	)
	if err != nil {
		return fmt.Errorf("failed to index log: %w", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		return fmt.Errorf("Elasticsearch returned error: %s", res.Status())
	}

	return nil
}

// BatchSaveLogs 批量保存日志
func (s *ElasticsearchStorage) BatchSaveLogs(ctx context.Context, logs []*types.Log) error {
	if len(logs) == 0 {
		return nil
	}

	// 按链分组
	chainLogs := make(map[string][]*types.Log)
	for _, log := range logs {
		chainLogs[log.ChainID] = append(chainLogs[log.ChainID], log)
	}

	// 为每个链批量索引
	for chainID, logList := range chainLogs {
		index := s.generateLogIndex(chainID)

		// 构建批量请求
		var bulkBody strings.Builder
		for _, log := range logList {
			// 添加索引操作
			id := fmt.Sprintf("%s-%d", log.TxHash, log.LogIndex)
			bulkBody.WriteString(fmt.Sprintf(`{"index":{"_index":"%s","_id":"%s"}}`+"\n", index, id))
			
			// 添加文档数据
			data, err := json.Marshal(log)
			if err != nil {
				logger.Error("Failed to marshal log",
					zap.String("chain_id", log.ChainID),
					zap.String("tx_hash", log.TxHash),
					zap.Error(err))
				continue
			}
			bulkBody.WriteString(string(data) + "\n")
		}

		// 执行批量请求
		res, err := s.client.Bulk(
			strings.NewReader(bulkBody.String()),
			s.client.Bulk.WithContext(ctx),
		)
		if err != nil {
			return fmt.Errorf("failed to bulk index logs: %w", err)
		}
		defer res.Body.Close()

		if res.IsError() {
			return fmt.Errorf("Elasticsearch returned error: %s", res.Status())
		}
	}

	return nil
}

// GetLogs 获取日志
func (s *ElasticsearchStorage) GetLogs(ctx context.Context, chainID string, filter *LogFilter) ([]*types.Log, error) {
	index := s.generateLogIndex(chainID)

	// 构建查询
	query := map[string]interface{}{
		"query": map[string]interface{}{
			"bool": map[string]interface{}{
				"must": []map[string]interface{}{},
			},
		},
	}

	// 添加过滤条件
	must := query["query"].(map[string]interface{})["bool"].(map[string]interface{})["must"].([]map[string]interface{})

	if filter.BlockNumber != nil {
		must = append(must, map[string]interface{}{
			"term": map[string]interface{}{
				"block_number": *filter.BlockNumber,
			},
		})
	}

	if filter.FromBlock != nil {
		must = append(must, map[string]interface{}{
			"range": map[string]interface{}{
				"block_number": map[string]interface{}{
					"gte": *filter.FromBlock,
				},
			},
		})
	}

	if filter.ToBlock != nil {
		must = append(must, map[string]interface{}{
			"range": map[string]interface{}{
				"block_number": map[string]interface{}{
					"lte": *filter.ToBlock,
				},
			},
		})
	}

	if filter.Address != "" {
		must = append(must, map[string]interface{}{
			"term": map[string]interface{}{
				"address": filter.Address,
			},
		})
	}

	if filter.TxHash != "" {
		must = append(must, map[string]interface{}{
			"term": map[string]interface{}{
				"tx_hash": filter.TxHash,
			},
		})
	}

	if len(filter.Topics) > 0 {
		for _, topic := range filter.Topics {
			must = append(must, map[string]interface{}{
				"term": map[string]interface{}{
					"topics": topic,
				},
			})
		}
	}

	// 添加排序和分页
	query["sort"] = []map[string]interface{}{
		{
			"block_number": map[string]interface{}{
				"order": "asc",
			},
		},
		{
			"log_index": map[string]interface{}{
				"order": "asc",
			},
		},
	}
	query["from"] = filter.Offset
	query["size"] = filter.Limit

	queryData, err := json.Marshal(query)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal query: %w", err)
	}

	// 执行搜索
	req := esapi.SearchRequest{
		Index: []string{index},
		Body:  bytes.NewReader(queryData),
	}
	res, err := req.Do(ctx, s.client)
	if err != nil {
		return nil, fmt.Errorf("failed to search logs: %w", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		return nil, fmt.Errorf("Elasticsearch returned error: %s", res.Status())
	}

	// 解析响应
	var result map[string]interface{}
	if err := json.NewDecoder(res.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("failed to parse response: %w", err)
	}

	// 提取结果
	hits, ok := result["hits"].(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("invalid hits format")
	}

	hitsList, ok := hits["hits"].([]interface{})
	if !ok {
		return nil, fmt.Errorf("invalid hits list format")
	}

	logs := make([]*types.Log, 0, len(hitsList))
	for _, hit := range hitsList {
		hitMap, ok := hit.(map[string]interface{})
		if !ok {
			continue
		}

		source, ok := hitMap["_source"].(map[string]interface{})
		if !ok {
			continue
		}

		// 反序列化日志数据
		data, err := json.Marshal(source)
		if err != nil {
			logger.Error("Failed to marshal source", zap.Error(err))
			continue
		}

		var log types.Log
		if err := json.Unmarshal(data, &log); err != nil {
			logger.Error("Failed to unmarshal log", zap.Error(err))
			continue
		}

		logs = append(logs, &log)
	}

	return logs, nil
}

// DeleteLogs 删除日志
func (s *ElasticsearchStorage) DeleteLogs(ctx context.Context, chainID string, fromBlock, toBlock uint64) error {
	index := s.generateLogIndex(chainID)

	// 构建删除查询
	query := map[string]interface{}{
		"query": map[string]interface{}{
			"range": map[string]interface{}{
				"block_number": map[string]interface{}{
					"gte": fromBlock,
					"lte": toBlock,
				},
			},
		},
	}

	queryData, err := json.Marshal(query)
	if err != nil {
		return fmt.Errorf("failed to marshal delete query: %w", err)
	}

	// 执行删除
	res, err := s.client.DeleteByQuery(
		[]string{index},
		strings.NewReader(string(queryData)),
		s.client.DeleteByQuery.WithContext(ctx),
	)
	if err != nil {
		return fmt.Errorf("failed to delete logs: %w", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		return fmt.Errorf("Elasticsearch returned error: %s", res.Status())
	}

	return nil
}

// SaveWatermark 保存水位
func (s *ElasticsearchStorage) SaveWatermark(ctx context.Context, watermark *types.Watermark) error {
	// Elasticsearch 不保存水位，水位由 Redis 或数据库保存
	return nil
}

// GetWatermark 获取水位
func (s *ElasticsearchStorage) GetWatermark(ctx context.Context, chainID string) (*types.Watermark, error) {
	// Elasticsearch 不保存水位
	return nil, fmt.Errorf("get watermark not supported in Elasticsearch")
}

// UpdateWatermark 更新水位
func (s *ElasticsearchStorage) UpdateWatermark(ctx context.Context, chainID string, watermark *types.Watermark) error {
	// Elasticsearch 不保存水位
	return nil
}

// SaveReorgEvent 保存回滚事件
func (s *ElasticsearchStorage) SaveReorgEvent(ctx context.Context, event *types.ReorgEvent) error {
	// Elasticsearch 可以保存回滚事件用于分析
	index := fmt.Sprintf("%s-reorg-events", s.indexPrefix)
	id := fmt.Sprintf("%s-%d", event.ChainID, event.DetectedAt.Unix())

	// 序列化事件数据
	data, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("failed to marshal reorg event: %w", err)
	}

	// 索引事件
	res, err := s.client.Index(
		index,
		strings.NewReader(string(data)),
		s.client.Index.WithDocumentID(id),
		s.client.Index.WithContext(ctx),
	)
	if err != nil {
		return fmt.Errorf("failed to index reorg event: %w", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		return fmt.Errorf("Elasticsearch returned error: %s", res.Status())
	}

	return nil
}

// GetReorgEvents 获取回滚事件
func (s *ElasticsearchStorage) GetReorgEvents(ctx context.Context, chainID string, limit, offset int) ([]*types.ReorgEvent, error) {
	index := fmt.Sprintf("%s-reorg-events", s.indexPrefix)

	// 构建查询
	query := map[string]interface{}{
		"query": map[string]interface{}{
			"term": map[string]interface{}{
				"chain_id": chainID,
			},
		},
		"sort": []map[string]interface{}{
			{
				"detected_at": map[string]interface{}{
					"order": "desc",
				},
			},
		},
		"from": offset,
		"size": limit,
	}

	queryData, err := json.Marshal(query)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal query: %w", err)
	}

	// 执行搜索
	req := esapi.SearchRequest{
		Index: []string{index},
		Body:  bytes.NewReader(queryData),
	}
	res, err := req.Do(ctx, s.client)
	if err != nil {
		return nil, fmt.Errorf("failed to search reorg events: %w", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		return nil, fmt.Errorf("Elasticsearch returned error: %s", res.Status())
	}

	// 解析响应
	var result map[string]interface{}
	if err := json.NewDecoder(res.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("failed to parse response: %w", err)
	}

	// 提取结果
	hits, ok := result["hits"].(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("invalid hits format")
	}

	hitsList, ok := hits["hits"].([]interface{})
	if !ok {
		return nil, fmt.Errorf("invalid hits list format")
	}

	events := make([]*types.ReorgEvent, 0, len(hitsList))
	for _, hit := range hitsList {
		hitMap, ok := hit.(map[string]interface{})
		if !ok {
			continue
		}

		source, ok := hitMap["_source"].(map[string]interface{})
		if !ok {
			continue
		}

		// 反序列化事件数据
		data, err := json.Marshal(source)
		if err != nil {
			logger.Error("Failed to marshal source", zap.Error(err))
			continue
		}

		var event types.ReorgEvent
		if err := json.Unmarshal(data, &event); err != nil {
			logger.Error("Failed to unmarshal reorg event", zap.Error(err))
			continue
		}

		events = append(events, &event)
	}

	return events, nil
}

// HealthCheck 健康检查
func (s *ElasticsearchStorage) HealthCheck(ctx context.Context) error {
	res, err := s.client.Cluster.Health(s.client.Cluster.Health.WithContext(ctx))
	if err != nil {
		return fmt.Errorf("failed to check Elasticsearch health: %w", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		return fmt.Errorf("Elasticsearch returned error: %s", res.Status())
	}

	return nil
}

// Close 关闭连接
func (s *ElasticsearchStorage) Close() error {
	// Elasticsearch client 不需要显式关闭
	return nil
}