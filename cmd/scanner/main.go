package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/smart-scanner/multi-chain-scanner/internal/node"
	"github.com/smart-scanner/multi-chain-scanner/internal/scheduler"
	"github.com/smart-scanner/multi-chain-scanner/internal/wallet"
	"github.com/smart-scanner/multi-chain-scanner/pkg/config"
	"github.com/smart-scanner/multi-chain-scanner/pkg/lock"
	"github.com/smart-scanner/multi-chain-scanner/pkg/logger"
	"go.uber.org/zap"
)

// main 智能链扫描器入口函数
// 负责初始化配置、日志、Redis、分布式锁、链客户端、调度器管理器和钱包服务
// [Design: 系统概述](../docs/DESIGN_SCANNER.md#1-系统概述)
func main() {
	cfg, err := config.LoadConfig("config.yaml")
	if err != nil {
		panic(fmt.Sprintf("Failed to load config: %v", err))
	}

	if err := logger.InitLogger(&cfg.Logging); err != nil {
		panic(fmt.Sprintf("Failed to init logger: %v", err))
	}
	defer logger.Sync()

	logger.Info("Starting Smart Scanner",
		zap.String("version", "1.0.0"),
		zap.String("environment", cfg.App.Environment))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	redisClient := redis.NewClient(&redis.Options{
		Addr:     cfg.Redis.Addr,
		Password: cfg.Redis.Password,
		DB:       cfg.Redis.DB,
	})

	lockManager := lock.NewLockManager(redisClient, 30*time.Second)

	clientRegistry := node.NewChainClientRegistry(nil)
	for chainID, chainConfig := range cfg.Chains {
		if err := clientRegistry.RegisterClient(chainID, &chainConfig); err != nil {
			logger.Error("Failed to register chain client",
				zap.String("chain_id", chainID),
				zap.Error(err))
		}
	}

	scannerManager, err := scheduler.NewSchedulerManager(cfg, clientRegistry, lockManager)
	if err != nil {
		logger.Fatal("Failed to create scheduler manager", zap.Error(err))
	}

	walletService, err := wallet.NewWalletService(cfg)
	if err != nil {
		logger.Warn("Failed to create wallet service", zap.Error(err))
	}

	go startMonitoringServer(&cfg.Server)
	go startAPIServer(&cfg.Server, walletService)

	if err := scannerManager.Start(ctx); err != nil {
		logger.Fatal("Failed to start scanner manager", zap.Error(err))
	}

	logger.Info("Smart Scanner started successfully")

	waitForShutdown()

	if err := scannerManager.Stop(); err != nil {
		logger.Error("Failed to stop scanner manager", zap.Error(err))
	}

	if err := redisClient.Close(); err != nil {
		logger.Error("Failed to close redis client", zap.Error(err))
	}

	logger.Info("Smart Scanner stopped gracefully")
}

// startMonitoringServer 启动监控服务
// 暴露 Prometheus 指标端点和健康检查端点
// [Design: 监控配置](../docs/DESIGN_SCANNER.md#1-系统概述)
func startMonitoringServer(cfg *config.ServerConfig) error {
	http.Handle("/metrics", promhttp.Handler())

	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	})

	go func() {
		addr := fmt.Sprintf(":%d", cfg.MetricsPort)
		logger.Info("Starting monitoring server", zap.String("addr", addr))
		if err := http.ListenAndServe(addr, nil); err != nil {
			logger.Error("Monitoring server failed", zap.Error(err))
		}
	}()

	return nil
}

// startAPIServer 启动 API 服务
// 注册钱包服务路由、指标端点和健康检查端点
// [Design: 钱包服务](../docs/DESIGN_SCANNER.md#1-系统概述)
func startAPIServer(cfg *config.ServerConfig, walletService *wallet.WalletService) error {
	mux := http.NewServeMux()

	if walletService != nil {
		handler := wallet.NewHandler(walletService)
		handler.RegisterRoutes(mux)
	}

	mux.Handle("/metrics", promhttp.Handler())

	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	})

	go func() {
		addr := fmt.Sprintf(":%d", cfg.HTTPPort)
		logger.Info("Starting API server", zap.String("addr", addr))
		if err := http.ListenAndServe(addr, mux); err != nil {
			logger.Error("API server failed", zap.Error(err))
		}
	}()

	return nil
}

// waitForShutdown 等待系统关闭信号
// 监听 SIGINT 和 SIGTERM 信号，用于优雅关闭
func waitForShutdown() {
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	sig := <-sigChan
	logger.Info("Received shutdown signal", zap.String("signal", sig.String()))
}
