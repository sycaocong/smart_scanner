package logger

import (
	"os"
	"path/filepath"

	"github.com/smart-scanner/multi-chain-scanner/pkg/config"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gopkg.in/natefinch/lumberjack.v2"
)

// logger 日志包
// 基于 Zap 实现的高性能日志系统，支持 JSON 格式输出和日志轮转
// [Design: 日志配置](../docs/DESIGN_SCANNER.md#1-系统概述)

var (
	// Logger 全局日志实例
	Logger *zap.Logger
	// Sugar 全局 SugaredLogger 实例
	Sugar *zap.SugaredLogger
)

// InitLogger 初始化日志
func InitLogger(cfg *config.LoggingConfig) error {
	// 解析日志级别
	level, err := zapcore.ParseLevel(cfg.Level)
	if err != nil {
		level = zapcore.InfoLevel
	}

	// 编码器配置
	encoderConfig := zapcore.EncoderConfig{
		TimeKey:        "timestamp",
		LevelKey:       "level",
		NameKey:        "logger",
		CallerKey:      "caller",
		MessageKey:     "message",
		StacktraceKey:  "stacktrace",
		LineEnding:     zapcore.DefaultLineEnding,
		EncodeLevel:    zapcore.LowercaseLevelEncoder,
		EncodeTime:     zapcore.ISO8601TimeEncoder,
		EncodeDuration: zapcore.SecondsDurationEncoder,
		EncodeCaller:   zapcore.ShortCallerEncoder,
	}

	// 根据格式选择编码器
	var encoder zapcore.Encoder
	if cfg.Format == "json" {
		encoder = zapcore.NewJSONEncoder(encoderConfig)
	} else {
		encoder = zapcore.NewConsoleEncoder(encoderConfig)
	}

	// 写入器配置
	var writeSyncer zapcore.WriteSyncer
	if cfg.File.Enabled {
		// 确保日志目录存在
		logDir := filepath.Dir(cfg.File.Path)
		if err := os.MkdirAll(logDir, 0755); err != nil {
			return err
		}

		// 使用 lumberjack 进行日志轮转
		fileWriter := &lumberjack.Logger{
			Filename:   cfg.File.Path,
			MaxSize:    cfg.File.MaxSize,
			MaxBackups: cfg.File.MaxBackups,
			MaxAge:     cfg.File.MaxAge,
			Compress:   cfg.File.Compress,
		}
		writeSyncer = zapcore.AddSync(fileWriter)
	} else {
		writeSyncer = zapcore.AddSync(os.Stdout)
	}

	// 创建 Core
	core := zapcore.NewCore(encoder, writeSyncer, level)

	// 创建 Logger
	Logger = zap.New(core, zap.AddCaller(), zap.AddStacktrace(zapcore.ErrorLevel))
	Sugar = Logger.Sugar()

	return nil
}

// Sync 同步日志缓冲区
func Sync() error {
	if Logger != nil {
		return Logger.Sync()
	}
	return nil
}

// WithFields 创建带字段的日志记录器
func WithFields(fields ...zap.Field) *zap.Logger {
	if Logger == nil {
		return zap.NewNop()
	}
	return Logger.With(fields...)
}

// With 创建带字段的 SugaredLogger
func With(args ...interface{}) *zap.SugaredLogger {
	if Sugar == nil {
		return zap.NewNop().Sugar()
	}
	return Sugar.With(args...)
}

// Debug 记录 Debug 级别日志
func Debug(msg string, fields ...zap.Field) {
	if Logger != nil {
		Logger.Debug(msg, fields...)
	}
}

// Info 记录 Info 级别日志
func Info(msg string, fields ...zap.Field) {
	if Logger != nil {
		Logger.Info(msg, fields...)
	}
}

// Warn 记录 Warn 级别日志
func Warn(msg string, fields ...zap.Field) {
	if Logger != nil {
		Logger.Warn(msg, fields...)
	}
}

// Error 记录 Error 级别日志
func Error(msg string, fields ...zap.Field) {
	if Logger != nil {
		Logger.Error(msg, fields...)
	}
}

// Fatal 记录 Fatal 级别日志并退出
func Fatal(msg string, fields ...zap.Field) {
	if Logger != nil {
		Logger.Fatal(msg, fields...)
	}
	os.Exit(1)
}

// Panic 记录 Panic 级别日志并 panic
func Panic(msg string, fields ...zap.Field) {
	if Logger != nil {
		Logger.Panic(msg, fields...)
	}
	panic(msg)
}