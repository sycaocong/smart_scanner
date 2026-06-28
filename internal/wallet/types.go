package wallet

import (
	"math/big"
	"time"
)

// ChainID 链 ID 类型
type ChainID string

const (
	ChainID_Ethereum ChainID = "1"
	ChainID_BSC      ChainID = "56"
	ChainID_Polygon  ChainID = "137"
)

// TokenType 代币类型
type TokenType string

const (
	TokenType_Native TokenType = "native"
	TokenType_ERC20  TokenType = "erc20"
)

// Token 代币信息
type Token struct {
	ChainID     ChainID   `json:"chain_id"`
	Contract    string    `json:"contract"`
	Symbol      string    `json:"symbol"`
	Name        string    `json:"name"`
	Decimals    int       `json:"decimals"`
	Type        TokenType `json:"type"`
}

// Balance 余额信息
type Balance struct {
	ChainID     ChainID   `json:"chain_id"`
	Address     string    `json:"address"`
	Token       Token     `json:"token"`
	Balance     *big.Int  `json:"balance"`
	Display     string    `json:"display"`
	BlockNumber uint64    `json:"block_number"`
	UpdatedAt   time.Time `json:"updated_at"`
}

// Transaction 交易信息
type Transaction struct {
	ID            string          `json:"id"`
	ChainID       ChainID         `json:"chain_id"`
	TxHash        string          `json:"tx_hash"`
	From          string          `json:"from"`
	To            string          `json:"to"`
	Token         Token           `json:"token"`
	Value         *big.Int        `json:"value"`
	Display       string          `json:"display"`
	Fee           *big.Int        `json:"fee"`
	FeeDisplay    string          `json:"fee_display"`
	BlockNumber   uint64          `json:"block_number"`
	Status        string          `json:"status"`
	TxType        TransactionType `json:"tx_type"`
	CreatedAt     time.Time       `json:"created_at"`
	ConfirmedAt   time.Time       `json:"confirmed_at"`
}

// TransactionType 交易类型
type TransactionType string

const (
	TxType_Transfer     TransactionType = "transfer"
	TxType_Deposit      TransactionType = "deposit"
	TxType_Withdraw     TransactionType = "withdraw"
	TxType_Swap         TransactionType = "swap"
)

type GetBalanceRequest struct {
	ChainID  ChainID `json:"chain_id"`
	Address  string  `json:"address"`
	Contract string  `json:"contract,omitempty"`
}

type GetBalanceResponse struct {
	Success bool      `json:"success"`
	Data    Balance   `json:"data"`
	Error   string    `json:"error,omitempty"`
}

type GetBalancesRequest struct {
	ChainID ChainID   `json:"chain_id"`
	Address string    `json:"address"`
	Tokens  []string  `json:"tokens,omitempty"`
}

type GetBalancesResponse struct {
	Success bool      `json:"success"`
	Data    []Balance `json:"data"`
	Error   string    `json:"error,omitempty"`
}

type GetTransactionsRequest struct {
	ChainID      ChainID   `json:"chain_id"`
	Address      string    `json:"address"`
	Token        string    `json:"token,omitempty"`
	TxType       string    `json:"tx_type,omitempty"`
	StartTime    time.Time `json:"start_time,omitempty"`
	EndTime      time.Time `json:"end_time,omitempty"`
	Page         int       `json:"page"`
	PageSize     int       `json:"page_size"`
}

type GetTransactionsResponse struct {
	Success       bool          `json:"success"`
	Data          []Transaction `json:"data"`
	Total         int           `json:"total"`
	Page          int           `json:"page"`
	PageSize      int           `json:"page_size"`
	HasMore       bool          `json:"has_more"`
	Error         string        `json:"error,omitempty"`
}

type TransferRequest struct {
	ChainID     ChainID  `json:"chain_id"`
	From        string   `json:"from"`
	To          string   `json:"to"`
	Contract    string   `json:"contract,omitempty"`
	Value       *big.Int `json:"value"`
	PrivateKey  string   `json:"private_key"`
	GasLimit    uint64   `json:"gas_limit,omitempty"`
	GasPrice    *big.Int `json:"gas_price,omitempty"`
}

type TransferResponse struct {
	Success   bool   `json:"success"`
	TxHash    string `json:"tx_hash"`
	ChainID   ChainID `json:"chain_id"`
	Error     string `json:"error,omitempty"`
}

type DepositRequest struct {
	ChainID  ChainID `json:"chain_id"`
	Address  string  `json:"address"`
	Token    string  `json:"token,omitempty"`
}

type DepositResponse struct {
	Success   bool   `json:"success"`
	Address   string `json:"address"`
	ChainID   ChainID `json:"chain_id"`
	Error     string `json:"error,omitempty"`
}

type WithdrawRequest struct {
	ChainID     ChainID  `json:"chain_id"`
	Address     string   `json:"address"`
	To          string   `json:"to"`
	Contract    string   `json:"contract,omitempty"`
	Value       *big.Int `json:"value"`
	PrivateKey  string   `json:"private_key"`
}

type WithdrawResponse struct {
	Success   bool   `json:"success"`
	TxHash    string `json:"tx_hash"`
	ChainID   ChainID `json:"chain_id"`
	Error     string `json:"error,omitempty"`
}