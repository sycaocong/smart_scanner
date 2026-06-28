package parser

import (
	"context"
	"fmt"

	"github.com/smart-scanner/multi-chain-scanner/pkg/types"
)

// Parser 解析器接口
// 定义了区块、交易和日志的解析接口，支持多种链类型的统一解析
// [Design: Parser 解析器接口](../docs/DESIGN_SCANNER.md#51-parser-解析器接口)
type Parser interface {
	ParseBlock(ctx context.Context, rawData interface{}) (*types.Block, error)
	ParseTransaction(ctx context.Context, rawData interface{}) (*types.Transaction, error)
	ParseLog(ctx context.Context, rawData interface{}) (*types.Log, error)
	BatchParseTransactions(ctx context.Context, rawDataList []interface{}) ([]*types.Transaction, error)
	BatchParseLogs(ctx context.Context, rawDataList []interface{}) ([]*types.Log, error)
	ValidateBlock(block *types.Block) error
	ValidateTransaction(tx *types.Transaction) error
	ValidateLog(log *types.Log) error
	GetType() string
}

// ParserFactory 解析器工厂
type ParserFactory interface {
	CreateParser(chainType string) (Parser, error)
	GetSupportedTypes() []string
}

// DefaultParserFactory 默认解析器工厂
type DefaultParserFactory struct {
	parsers map[string]Parser
}

// NewDefaultParserFactory 创建默认解析器工厂
func NewDefaultParserFactory() *DefaultParserFactory {
	factory := &DefaultParserFactory{
		parsers: make(map[string]Parser),
	}

	// 注册默认解析器
	factory.RegisterParser(string(types.ChainTypeEVM), NewEVMParser())
	factory.RegisterParser(string(types.ChainTypeSolana), NewSolanaParser())
	factory.RegisterParser(string(types.ChainTypeTron), NewTronParser())

	return factory
}

// RegisterParser 注册解析器
func (f *DefaultParserFactory) RegisterParser(chainType string, parser Parser) {
	f.parsers[chainType] = parser
}

// CreateParser 创建解析器
func (f *DefaultParserFactory) CreateParser(chainType string) (Parser, error) {
	parser, exists := f.parsers[chainType]
	if !exists {
		return nil, fmt.Errorf("unsupported chain type: %s", chainType)
	}
	return parser, nil
}

// GetSupportedTypes 获取支持的链类型
func (f *DefaultParserFactory) GetSupportedTypes() []string {
	types := make([]string, 0, len(f.parsers))
	for chainType := range f.parsers {
		types = append(types, chainType)
	}
	return types
}

// ParserManager 解析器管理器
// 管理不同链类型的解析器，提供统一的解析入口
// [Design: ParserManager 解析器管理器](../docs/DESIGN_SCANNER.md#52-parsermanager-解析器管理器)
type ParserManager struct {
	factory ParserFactory          // 解析器工厂
	parsers map[string]Parser      // 解析器缓存（chainType -> Parser）
}

// NewParserManager 创建解析器管理器
func NewParserManager(factory ParserFactory) *ParserManager {
	if factory == nil {
		factory = NewDefaultParserFactory()
	}

	return &ParserManager{
		factory: factory,
		parsers: make(map[string]Parser),
	}
}

// GetParser 获取解析器
func (m *ParserManager) GetParser(chainType string) (Parser, error) {
	// 检查缓存
	if parser, exists := m.parsers[chainType]; exists {
		return parser, nil
	}

	// 创建新的解析器
	parser, err := m.factory.CreateParser(chainType)
	if err != nil {
		return nil, err
	}

	// 缓存解析器
	m.parsers[chainType] = parser
	return parser, nil
}

// ParseBlock 解析区块
func (m *ParserManager) ParseBlock(ctx context.Context, chainType string, rawData interface{}) (*types.Block, error) {
	parser, err := m.GetParser(chainType)
	if err != nil {
		return nil, fmt.Errorf("failed to get parser: %w", err)
	}

	block, err := parser.ParseBlock(ctx, rawData)
	if err != nil {
		return nil, fmt.Errorf("failed to parse block: %w", err)
	}

	// 验证区块
	if err := parser.ValidateBlock(block); err != nil {
		return nil, fmt.Errorf("block validation failed: %w", err)
	}

	return block, nil
}

// ParseTransaction 解析交易
func (m *ParserManager) ParseTransaction(ctx context.Context, chainType string, rawData interface{}) (*types.Transaction, error) {
	parser, err := m.GetParser(chainType)
	if err != nil {
		return nil, fmt.Errorf("failed to get parser: %w", err)
	}

	tx, err := parser.ParseTransaction(ctx, rawData)
	if err != nil {
		return nil, fmt.Errorf("failed to parse transaction: %w", err)
	}

	// 验证交易
	if err := parser.ValidateTransaction(tx); err != nil {
		return nil, fmt.Errorf("transaction validation failed: %w", err)
	}

	return tx, nil
}

// ParseLog 解析日志
func (m *ParserManager) ParseLog(ctx context.Context, chainType string, rawData interface{}) (*types.Log, error) {
	parser, err := m.GetParser(chainType)
	if err != nil {
		return nil, fmt.Errorf("failed to get parser: %w", err)
	}

	log, err := parser.ParseLog(ctx, rawData)
	if err != nil {
		return nil, fmt.Errorf("failed to parse log: %w", err)
	}

	// 验证日志
	if err := parser.ValidateLog(log); err != nil {
		return nil, fmt.Errorf("log validation failed: %w", err)
	}

	return log, nil
}

// BatchParseTransactions 批量解析交易
func (m *ParserManager) BatchParseTransactions(ctx context.Context, chainType string, rawDataList []interface{}) ([]*types.Transaction, error) {
	parser, err := m.GetParser(chainType)
	if err != nil {
		return nil, fmt.Errorf("failed to get parser: %w", err)
	}

	transactions, err := parser.BatchParseTransactions(ctx, rawDataList)
	if err != nil {
		return nil, fmt.Errorf("failed to batch parse transactions: %w", err)
	}

	// 验证所有交易
	for _, tx := range transactions {
		if err := parser.ValidateTransaction(tx); err != nil {
			return nil, fmt.Errorf("transaction validation failed: %w", err)
		}
	}

	return transactions, nil
}

// BatchParseLogs 批量解析日志
func (m *ParserManager) BatchParseLogs(ctx context.Context, chainType string, rawDataList []interface{}) ([]*types.Log, error) {
	parser, err := m.GetParser(chainType)
	if err != nil {
		return nil, fmt.Errorf("failed to get parser: %w", err)
	}

	logs, err := parser.BatchParseLogs(ctx, rawDataList)
	if err != nil {
		return nil, fmt.Errorf("failed to batch parse logs: %w", err)
	}

	// 验证所有日志
	for _, log := range logs {
		if err := parser.ValidateLog(log); err != nil {
			return nil, fmt.Errorf("log validation failed: %w", err)
		}
	}

	return logs, nil
}