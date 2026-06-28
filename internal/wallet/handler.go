package wallet

import (
	"context"
	"encoding/json"
	"math/big"
	"net/http"
	"strconv"
	"strings"

	"github.com/smart-scanner/multi-chain-scanner/pkg/logger"
	"go.uber.org/zap"
)

// Handler 钱包服务 HTTP 处理器
// 处理钱包相关的 REST API 请求
// [Design: 钱包服务](../docs/DESIGN_SCANNER.md#1-系统概述)
type Handler struct {
	service *WalletService
}

// NewHandler 创建钱包处理器
func NewHandler(service *WalletService) *Handler {
	return &Handler{
		service: service,
	}
}

func (h *Handler) GetBalance(w http.ResponseWriter, r *http.Request) {
	var req GetBalanceRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.sendError(w, http.StatusBadRequest, "Invalid request body: "+err.Error())
		return
	}

	if req.Address == "" {
		h.sendError(w, http.StatusBadRequest, "Address is required")
		return
	}

	if req.ChainID == "" {
		req.ChainID = ChainID_Ethereum
	}

	ctx := r.Context()
	balance, err := h.service.GetBalance(ctx, req)
	if err != nil {
		logger.Error("GetBalance failed",
			zap.String("chain_id", string(req.ChainID)),
			zap.String("address", req.Address),
			zap.Error(err))
		h.sendError(w, http.StatusInternalServerError, err.Error())
		return
	}

	h.sendSuccess(w, GetBalanceResponse{
		Success: true,
		Data:    *balance,
	})
}

func (h *Handler) GetBalances(w http.ResponseWriter, r *http.Request) {
	var req GetBalancesRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.sendError(w, http.StatusBadRequest, "Invalid request body: "+err.Error())
		return
	}

	if req.Address == "" {
		h.sendError(w, http.StatusBadRequest, "Address is required")
		return
	}

	if req.ChainID == "" {
		req.ChainID = ChainID_Ethereum
	}

	ctx := r.Context()
	balances, err := h.service.GetBalances(ctx, req)
	if err != nil {
		logger.Error("GetBalances failed",
			zap.String("chain_id", string(req.ChainID)),
			zap.String("address", req.Address),
			zap.Error(err))
		h.sendError(w, http.StatusInternalServerError, err.Error())
		return
	}

	h.sendSuccess(w, GetBalancesResponse{
		Success: true,
		Data:    balances,
	})
}

func (h *Handler) GetTransactions(w http.ResponseWriter, r *http.Request) {
	var req GetTransactionsRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.sendError(w, http.StatusBadRequest, "Invalid request body: "+err.Error())
		return
	}

	if req.Address == "" {
		h.sendError(w, http.StatusBadRequest, "Address is required")
		return
	}

	if req.ChainID == "" {
		req.ChainID = ChainID_Ethereum
	}

	if req.Page == 0 {
		req.Page = 1
	}

	if req.PageSize == 0 {
		req.PageSize = 20
	}

	ctx := r.Context()
	transactions, err := h.service.GetTransactions(ctx, req)
	if err != nil {
		logger.Error("GetTransactions failed",
			zap.String("chain_id", string(req.ChainID)),
			zap.String("address", req.Address),
			zap.Error(err))
		h.sendError(w, http.StatusInternalServerError, err.Error())
		return
	}

	total := len(transactions)
	hasMore := total > req.Page*req.PageSize

	h.sendSuccess(w, GetTransactionsResponse{
		Success: true,
		Data:    transactions,
		Total:   total,
		Page:    req.Page,
		PageSize: req.PageSize,
		HasMore: hasMore,
	})
}

func (h *Handler) Transfer(w http.ResponseWriter, r *http.Request) {
	var req TransferRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.sendError(w, http.StatusBadRequest, "Invalid request body: "+err.Error())
		return
	}

	if req.From == "" {
		h.sendError(w, http.StatusBadRequest, "From address is required")
		return
	}

	if req.To == "" {
		h.sendError(w, http.StatusBadRequest, "To address is required")
		return
	}

	if req.Value == nil || req.Value.Sign() <= 0 {
		h.sendError(w, http.StatusBadRequest, "Value must be positive")
		return
	}

	if req.PrivateKey == "" {
		h.sendError(w, http.StatusBadRequest, "Private key is required")
		return
	}

	if req.ChainID == "" {
		req.ChainID = ChainID_Ethereum
	}

	ctx := r.Context()
	response, err := h.service.Transfer(ctx, req)
	if err != nil {
		logger.Error("Transfer failed",
			zap.String("chain_id", string(req.ChainID)),
			zap.String("from", req.From),
			zap.String("to", req.To),
			zap.Error(err))
		h.sendError(w, http.StatusInternalServerError, err.Error())
		return
	}

	h.sendSuccess(w, response)
}

func (h *Handler) Deposit(w http.ResponseWriter, r *http.Request) {
	var req DepositRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.sendError(w, http.StatusBadRequest, "Invalid request body: "+err.Error())
		return
	}

	if req.Address == "" {
		h.sendError(w, http.StatusBadRequest, "Address is required")
		return
	}

	if req.ChainID == "" {
		req.ChainID = ChainID_Ethereum
	}

	ctx := r.Context()
	response, err := h.service.Deposit(ctx, req)
	if err != nil {
		logger.Error("Deposit failed",
			zap.String("chain_id", string(req.ChainID)),
			zap.String("address", req.Address),
			zap.Error(err))
		h.sendError(w, http.StatusInternalServerError, err.Error())
		return
	}

	h.sendSuccess(w, response)
}

func (h *Handler) Withdraw(w http.ResponseWriter, r *http.Request) {
	var req WithdrawRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.sendError(w, http.StatusBadRequest, "Invalid request body: "+err.Error())
		return
	}

	if req.Address == "" {
		h.sendError(w, http.StatusBadRequest, "Address is required")
		return
	}

	if req.To == "" {
		h.sendError(w, http.StatusBadRequest, "To address is required")
		return
	}

	if req.Value == nil || req.Value.Sign() <= 0 {
		h.sendError(w, http.StatusBadRequest, "Value must be positive")
		return
	}

	if req.PrivateKey == "" {
		h.sendError(w, http.StatusBadRequest, "Private key is required")
		return
	}

	if req.ChainID == "" {
		req.ChainID = ChainID_Ethereum
	}

	ctx := r.Context()
	response, err := h.service.Withdraw(ctx, req)
	if err != nil {
		logger.Error("Withdraw failed",
			zap.String("chain_id", string(req.ChainID)),
			zap.String("address", req.Address),
			zap.String("to", req.To),
			zap.Error(err))
		h.sendError(w, http.StatusInternalServerError, err.Error())
		return
	}

	h.sendSuccess(w, response)
}

func (h *Handler) sendSuccess(w http.ResponseWriter, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(data)
}

func (h *Handler) sendError(w http.ResponseWriter, statusCode int, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"success": false,
		"error":   message,
	})
}

func (h *Handler) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("/api/v1/wallet/balance", h.GetBalance)
	mux.HandleFunc("/api/v1/wallet/balances", h.GetBalances)
	mux.HandleFunc("/api/v1/wallet/transactions", h.GetTransactions)
	mux.HandleFunc("/api/v1/wallet/transfer", h.Transfer)
	mux.HandleFunc("/api/v1/wallet/deposit", h.Deposit)
	mux.HandleFunc("/api/v1/wallet/withdraw", h.Withdraw)
}

func ParseBigInt(s string) *big.Int {
	if strings.HasPrefix(s, "0x") {
		s = s[2:]
	}

	var result big.Int
	result.SetString(s, 16)
	return &result
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")

	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusOK)
		return
	}

	ctx := context.WithValue(r.Context(), "request_id", r.Header.Get("X-Request-ID"))
	r = r.WithContext(ctx)
}

func GetPaginationParams(r *http.Request) (page, pageSize int) {
	pageStr := r.URL.Query().Get("page")
	pageSizeStr := r.URL.Query().Get("page_size")

	if pageStr != "" {
		if p, err := strconv.Atoi(pageStr); err == nil && p > 0 {
			page = p
		}
	}

	if pageSizeStr != "" {
		if ps, err := strconv.Atoi(pageSizeStr); err == nil && ps > 0 {
			pageSize = ps
		}
	}

	if page == 0 {
		page = 1
	}

	if pageSize == 0 {
		pageSize = 20
	}

	return
}