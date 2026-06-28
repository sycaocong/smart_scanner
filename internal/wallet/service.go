package wallet

import (
	"context"
	"fmt"
	"math/big"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/smart-scanner/multi-chain-scanner/internal/node"
	"github.com/smart-scanner/multi-chain-scanner/pkg/config"
	"github.com/smart-scanner/multi-chain-scanner/pkg/logger"
	"github.com/smart-scanner/multi-chain-scanner/pkg/util"
	"go.uber.org/zap"
)

// WalletService 钱包服务
// 提供余额查询、交易查询、转账等钱包相关功能
// [Design: 钱包服务](../docs/DESIGN_SCANNER.md#1-系统概述)
type WalletService struct {
	clients map[ChainID]*node.EVMClient
}

// NewWalletService 创建钱包服务
func NewWalletService(cfg *config.Config) (*WalletService, error) {
	service := &WalletService{
		clients: make(map[ChainID]*node.EVMClient),
	}

	for chainID, chainConfig := range cfg.Chains {
		if !chainConfig.Enabled || chainConfig.ChainType != "evm" {
			continue
		}

		client, err := node.NewEVMClient(chainID, &chainConfig)
		if err != nil {
			logger.Warn("Failed to create EVM client for chain",
				zap.String("chain_id", chainID),
				zap.Error(err))
			continue
		}

		service.clients[ChainID(chainID)] = client
		logger.Info("Wallet service initialized for chain",
			zap.String("chain_id", chainID))
	}

	return service, nil
}

func (s *WalletService) GetBalance(ctx context.Context, req GetBalanceRequest) (*Balance, error) {
	client, err := s.getClient(req.ChainID)
	if err != nil {
		return nil, err
	}

	var balance *big.Int
	var blockNumber uint64

	if req.Contract == "" {
		result, err := client.NodeManager().Call(ctx, "eth_getBalance", req.Address, "latest")
		if err != nil {
			return nil, fmt.Errorf("failed to get balance: %w", err)
		}

		balanceHex, ok := result.(string)
		if !ok {
			return nil, fmt.Errorf("invalid balance format")
		}

		balance, err = util.ParseBigInt(balanceHex)
		if err != nil {
			return nil, fmt.Errorf("failed to parse balance: %w", err)
		}

		currentHeight, err := client.GetCurrentHeight(ctx)
		if err != nil {
			return nil, fmt.Errorf("failed to get current height: %w", err)
		}
		blockNumber = currentHeight

		token := Token{
			ChainID:  req.ChainID,
			Contract: "",
			Symbol:   s.getNativeSymbol(req.ChainID),
			Name:     s.getNativeName(req.ChainID),
			Decimals: 18,
			Type:     TokenType_Native,
		}

		display := util.FormatBalance(balance, token.Decimals)

		return &Balance{
			ChainID:     req.ChainID,
			Address:     req.Address,
			Token:       token,
			Balance:     balance,
			Display:     display,
			BlockNumber: blockNumber,
			UpdatedAt:   time.Now(),
		}, nil
	}

	result, err := client.NodeManager().Call(ctx, "eth_call", map[string]interface{}{
		"to":   req.Contract,
		"data": "0x70a08231000000000000000000000000" + common.HexToAddress(req.Address).String()[2:],
	}, "latest")
	if err != nil {
		return nil, fmt.Errorf("failed to get ERC20 balance: %w", err)
	}

	balanceHex, ok := result.(string)
	if !ok {
		return nil, fmt.Errorf("invalid balance format")
	}

	balance, err = util.ParseBigInt(balanceHex)
	if err != nil {
		return nil, fmt.Errorf("failed to parse balance: %w", err)
	}

	currentHeight, err := client.GetCurrentHeight(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get current height: %w", err)
	}
	blockNumber = currentHeight

	tokenInfo, err := s.getTokenInfo(ctx, req.ChainID, req.Contract)
	if err != nil {
		logger.Warn("Failed to get token info, using default",
			zap.String("contract", req.Contract),
			zap.Error(err))
		tokenInfo = Token{
			ChainID:  req.ChainID,
			Contract: req.Contract,
			Symbol:   "Unknown",
			Name:     "Unknown Token",
			Decimals: 18,
			Type:     TokenType_ERC20,
		}
	}

	display := util.FormatBalance(balance, tokenInfo.Decimals)

	return &Balance{
		ChainID:     req.ChainID,
		Address:     req.Address,
		Token:       tokenInfo,
		Balance:     balance,
		Display:     display,
		BlockNumber: blockNumber,
		UpdatedAt:   time.Now(),
	}, nil
}

func (s *WalletService) GetBalances(ctx context.Context, req GetBalancesRequest) ([]Balance, error) {
	var balances []Balance

	if len(req.Tokens) == 0 {
		nativeBalance, err := s.GetBalance(ctx, GetBalanceRequest{
			ChainID: req.ChainID,
			Address: req.Address,
		})
		if err != nil {
			logger.Warn("Failed to get native balance",
				zap.String("chain_id", string(req.ChainID)),
				zap.String("address", req.Address),
				zap.Error(err))
		} else {
			balances = append(balances, *nativeBalance)
		}

		defaultTokens := s.getDefaultTokens(req.ChainID)
		for _, token := range defaultTokens {
			balance, err := s.GetBalance(ctx, GetBalanceRequest{
				ChainID:  req.ChainID,
				Address:  req.Address,
				Contract: token.Contract,
			})
			if err != nil {
				logger.Warn("Failed to get token balance",
					zap.String("chain_id", string(req.ChainID)),
					zap.String("contract", token.Contract),
					zap.Error(err))
				continue
			}
			balances = append(balances, *balance)
		}
	} else {
		for _, contract := range req.Tokens {
			balance, err := s.GetBalance(ctx, GetBalanceRequest{
				ChainID:  req.ChainID,
				Address:  req.Address,
				Contract: contract,
			})
			if err != nil {
				logger.Warn("Failed to get token balance",
					zap.String("chain_id", string(req.ChainID)),
					zap.String("contract", contract),
					zap.Error(err))
				continue
			}
			balances = append(balances, *balance)
		}
	}

	return balances, nil
}

func (s *WalletService) GetTransactions(ctx context.Context, req GetTransactionsRequest) ([]Transaction, error) {
	var transactions []Transaction

	client, err := s.getClient(req.ChainID)
	if err != nil {
		return nil, err
	}

	currentHeight, err := client.GetCurrentHeight(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get current height: %w", err)
	}

	fromBlock := currentHeight - 1000
	if fromBlock < 0 {
		fromBlock = 0
	}

	filter := map[string]interface{}{
		"fromBlock": fmt.Sprintf("0x%x", fromBlock),
		"toBlock":   "latest",
		"address":   req.Address,
	}

	if req.Token != "" {
		filter["address"] = req.Token
	}

	result, err := client.NodeManager().Call(ctx, "eth_getLogs", filter)
	if err != nil {
		return nil, fmt.Errorf("failed to get logs: %w", err)
	}

	logs, ok := result.([]interface{})
	if !ok {
		return transactions, nil
	}

	for _, logData := range logs {
		txHash, _ := logData.(map[string]interface{})["transactionHash"].(string)
		if txHash == "" {
			continue
		}

		tx, err := client.GetTransaction(ctx, txHash)
		if err != nil {
			logger.Warn("Failed to get transaction",
				zap.String("tx_hash", txHash),
				zap.Error(err))
			continue
		}

		tokenInfo := s.getTokenByContract(req.ChainID, req.Token)

		transaction := Transaction{
			ID:          txHash,
			ChainID:     req.ChainID,
			TxHash:      tx.Hash,
			From:        tx.From,
			To:          tx.To,
			Token:       tokenInfo,
			Value:       tx.Value,
			Display:     util.FormatBalance(tx.Value, tokenInfo.Decimals),
			Fee:         new(big.Int).Mul(tx.GasPrice, big.NewInt(int64(tx.GasUsed))),
			FeeDisplay:  util.FormatBalance(new(big.Int).Mul(tx.GasPrice, big.NewInt(int64(tx.GasUsed))), 18),
			BlockNumber: tx.BlockNumber,
			Status:      string(tx.Status),
			TxType:      TxType_Transfer,
			CreatedAt:   tx.Timestamp,
			ConfirmedAt: tx.Timestamp,
		}

		transactions = append(transactions, transaction)
	}

	return transactions, nil
}

func (s *WalletService) Transfer(ctx context.Context, req TransferRequest) (*TransferResponse, error) {
	client, err := s.getClient(req.ChainID)
	if err != nil {
		return nil, err
	}

	privateKey, err := crypto.HexToECDSA(req.PrivateKey)
	if err != nil {
		return nil, fmt.Errorf("invalid private key: %w", err)
	}

	fromAddr := crypto.PubkeyToAddress(privateKey.PublicKey)

	chainID, err := client.GetChainID(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get chain ID: %w", err)
	}

	chainIDBig, _ := big.NewInt(0).SetString(chainID, 10)

	nonce, err := client.NodeManager().Call(ctx, "eth_getTransactionCount", fromAddr.Hex(), "pending")
	if err != nil {
		return nil, fmt.Errorf("failed to get nonce: %w", err)
	}

	nonceHex, _ := nonce.(string)
	nonceNum, err := util.ParseBigInt(nonceHex)
	if err != nil {
		return nil, fmt.Errorf("failed to parse nonce: %w", err)
	}

	gasLimit := uint64(21000)
	if req.GasLimit > 0 {
		gasLimit = req.GasLimit
	}

	gasPrice := big.NewInt(10000000000)
	if req.GasPrice != nil {
		gasPrice = req.GasPrice
	}

	var tx *types.Transaction

	if req.Contract == "" {
		tx = types.NewTransaction(
			nonceNum.Uint64(),
			common.HexToAddress(req.To),
			req.Value,
			gasLimit,
			gasPrice,
			nil,
		)
	} else {
		transferData := []byte{0xa9, 0x05, 0x9c, 0xbb}
		toBytes := common.LeftPadBytes(common.HexToAddress(req.To).Bytes(), 32)
		valueBytes := common.LeftPadBytes(req.Value.Bytes(), 32)
		data := append(transferData, toBytes...)
		data = append(data, valueBytes...)

		tx = types.NewTransaction(
			nonceNum.Uint64(),
			common.HexToAddress(req.Contract),
			big.NewInt(0),
			gasLimit,
			gasPrice,
			data,
		)
	}

	signedTx, err := types.SignTx(tx, types.NewEIP155Signer(chainIDBig), privateKey)
	if err != nil {
		return nil, fmt.Errorf("failed to sign transaction: %w", err)
	}

	txData, err := signedTx.MarshalJSON()
	if err != nil {
		return nil, fmt.Errorf("failed to marshal transaction: %w", err)
	}

	result, err := client.NodeManager().Call(ctx, "eth_sendRawTransaction", "0x"+common.Bytes2Hex(txData))
	if err != nil {
		return nil, fmt.Errorf("failed to send transaction: %w", err)
	}

	txHash, ok := result.(string)
	if !ok {
		return nil, fmt.Errorf("invalid transaction hash format")
	}

	logger.Info("Transfer successful",
		zap.String("chain_id", string(req.ChainID)),
		zap.String("from", fromAddr.Hex()),
		zap.String("to", req.To),
		zap.String("tx_hash", txHash))

	return &TransferResponse{
		Success: true,
		TxHash:  txHash,
		ChainID: req.ChainID,
	}, nil
}

func (s *WalletService) Deposit(ctx context.Context, req DepositRequest) (*DepositResponse, error) {
	_, err := s.getClient(req.ChainID)
	if err != nil {
		return nil, err
	}

	return &DepositResponse{
		Success: true,
		Address: req.Address,
		ChainID: req.ChainID,
	}, nil
}

func (s *WalletService) Withdraw(ctx context.Context, req WithdrawRequest) (*WithdrawResponse, error) {
	transferReq := TransferRequest{
		ChainID:    req.ChainID,
		From:       req.Address,
		To:         req.To,
		Contract:   req.Contract,
		Value:      req.Value,
		PrivateKey: req.PrivateKey,
	}

	response, err := s.Transfer(ctx, transferReq)
	if err != nil {
		return nil, err
	}

	return &WithdrawResponse{
		Success: response.Success,
		TxHash:  response.TxHash,
		ChainID: response.ChainID,
	}, nil
}

func (s *WalletService) getClient(chainID ChainID) (*node.EVMClient, error) {
	client, exists := s.clients[chainID]
	if !exists {
		return nil, fmt.Errorf("client not found for chain %s", chainID)
	}
	return client, nil
}

func (s *WalletService) getNativeSymbol(chainID ChainID) string {
	switch chainID {
	case ChainID_Ethereum:
		return "ETH"
	case ChainID_BSC:
		return "BNB"
	case ChainID_Polygon:
		return "MATIC"
	default:
		return "ETH"
	}
}

func (s *WalletService) getNativeName(chainID ChainID) string {
	switch chainID {
	case ChainID_Ethereum:
		return "Ethereum"
	case ChainID_BSC:
		return "Binance Coin"
	case ChainID_Polygon:
		return "Polygon"
	default:
		return "Native Token"
	}
}

func (s *WalletService) getDefaultTokens(chainID ChainID) []Token {
	switch chainID {
	case ChainID_Ethereum:
		return []Token{
			{ChainID: chainID, Contract: "0xdAC17F958D2ee523a2206206994597C13D831ec7", Symbol: "USDT", Name: "Tether", Decimals: 6, Type: TokenType_ERC20},
			{ChainID: chainID, Contract: "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48", Symbol: "USDC", Name: "USD Coin", Decimals: 6, Type: TokenType_ERC20},
			{ChainID: chainID, Contract: "0x2260FAC5E5542a773Aa44fBCfeDf7C193bc2C599", Symbol: "WBTC", Name: "Wrapped BTC", Decimals: 8, Type: TokenType_ERC20},
		}
	case ChainID_BSC:
		return []Token{
			{ChainID: chainID, Contract: "0x55d398326f99059fF775485246999027B3197955", Symbol: "USDT", Name: "Tether", Decimals: 18, Type: TokenType_ERC20},
			{ChainID: chainID, Contract: "0x8AC76a51cc950d9822D68b83fE1Ad97B32Cd580d", Symbol: "USDC", Name: "USD Coin", Decimals: 18, Type: TokenType_ERC20},
		}
	case ChainID_Polygon:
		return []Token{
			{ChainID: chainID, Contract: "0xc2132D05D31c914a87C6611C10748AEb04B58e8F", Symbol: "USDT", Name: "Tether", Decimals: 6, Type: TokenType_ERC20},
			{ChainID: chainID, Contract: "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174", Symbol: "USDC", Name: "USD Coin", Decimals: 6, Type: TokenType_ERC20},
		}
	default:
		return []Token{}
	}
}

func (s *WalletService) getTokenByContract(chainID ChainID, contract string) Token {
	if contract == "" {
		return Token{
			ChainID:  chainID,
			Contract: "",
			Symbol:   s.getNativeSymbol(chainID),
			Name:     s.getNativeName(chainID),
			Decimals: 18,
			Type:     TokenType_Native,
		}
	}

	tokens := s.getDefaultTokens(chainID)
	for _, token := range tokens {
		if token.Contract == contract {
			return token
		}
	}

	return Token{
		ChainID:  chainID,
		Contract: contract,
		Symbol:   "Unknown",
		Name:     "Unknown Token",
		Decimals: 18,
		Type:     TokenType_ERC20,
	}
}

func (s *WalletService) getTokenInfo(ctx context.Context, chainID ChainID, contract string) (Token, error) {
	client, err := s.getClient(chainID)
	if err != nil {
		return Token{}, err
	}

	symbolResult, err := client.NodeManager().Call(ctx, "eth_call", map[string]interface{}{
		"to":   contract,
		"data": "0x95d89b41",
	}, "latest")
	if err != nil {
		return Token{}, fmt.Errorf("failed to get symbol: %w", err)
	}

	symbol := util.ParseStringResult(symbolResult.(string))

	nameResult, err := client.NodeManager().Call(ctx, "eth_call", map[string]interface{}{
		"to":   contract,
		"data": "0x06fdde03",
	}, "latest")
	if err != nil {
		return Token{}, fmt.Errorf("failed to get name: %w", err)
	}

	name := util.ParseStringResult(nameResult.(string))

	decimalsResult, err := client.NodeManager().Call(ctx, "eth_call", map[string]interface{}{
		"to":   contract,
		"data": "0x313ce567",
	}, "latest")
	if err != nil {
		return Token{}, fmt.Errorf("failed to get decimals: %w", err)
	}

	decimalsHex, _ := decimalsResult.(string)
	decimalsBig, _ := util.ParseBigInt(decimalsHex)

	return Token{
		ChainID:  chainID,
		Contract: contract,
		Symbol:   symbol,
		Name:     name,
		Decimals: int(decimalsBig.Int64()),
		Type:     TokenType_ERC20,
	}, nil
}

func (s *WalletService) Close() {
	for chainID, client := range s.clients {
		if err := client.Close(); err != nil {
			logger.Warn("Failed to close client",
				zap.String("chain_id", string(chainID)),
				zap.Error(err))
		}
	}
}