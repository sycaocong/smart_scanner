package config

import (
	"fmt"
	"time"

	"github.com/spf13/viper"
)

// Config 应用配置
type Config struct {
	App         AppConfig         `yaml:"app"`
	Server      ServerConfig      `yaml:"server"`
	Database    DatabaseConfig    `yaml:"database"`
	Redis       RedisConfig       `yaml:"redis"`
	Elasticsearch ElasticsearchConfig `yaml:"elasticsearch"`
	Kafka       KafkaConfig       `yaml:"kafka"`
	RocketMQ    RocketMQConfig    `yaml:"rocketmq"`
	DistributedLock DistributedLockConfig `yaml:"distributed_lock"`
	Scanner     ScannerConfig     `yaml:"scanner"`
	Chains      map[string]ChainConfig `yaml:"chains"`
	Monitoring  MonitoringConfig  `yaml:"monitoring"`
	Logging     LoggingConfig     `yaml:"logging"`
	Deployment  DeploymentConfig  `yaml:"deployment"`
}

// AppConfig 应用配置
type AppConfig struct {
	Name        string `yaml:"name"`
	Version     string `yaml:"version"`
	Environment string `yaml:"environment"`
	LogLevel    string `yaml:"log_level"`
}

// ServerConfig 服务配置
type ServerConfig struct {
	HTTPPort    int `yaml:"http_port"`
	GRPCPort    int `yaml:"grpc_port"`
	MetricsPort int `yaml:"metrics_port"`
}

// DatabaseConfig 数据库配置
type DatabaseConfig struct {
	Master   DBInstanceConfig `yaml:"master"`
	Slave    DBInstanceConfig `yaml:"slave"`
	Postgres PostgresConfig   `yaml:"postgres"`
}

// DBInstanceConfig 数据库实例配置
type DBInstanceConfig struct {
	Driver          string        `yaml:"driver"`
	Host            string        `yaml:"host"`
	Port            int           `yaml:"port"`
	Username        string        `yaml:"username"`
	Password        string        `yaml:"password"`
	Database        string        `yaml:"database"`
	MaxOpenConns    int           `yaml:"max_open_conns"`
	MaxIdleConns    int           `yaml:"max_idle_conns"`
	ConnMaxLifetime time.Duration `yaml:"conn_max_lifetime"`
}

// PostgresConfig PostgreSQL 配置
type PostgresConfig struct {
	Enabled  bool   `yaml:"enabled"`
	Host     string `yaml:"host"`
	Port     int    `yaml:"port"`
	Username string `yaml:"username"`
	Password string `yaml:"password"`
	Database string `yaml:"database"`
	SSLMode  string `yaml:"ssl_mode"`
}

// RedisConfig Redis 配置
type RedisConfig struct {
	Addr         string `yaml:"addr"`
	Password     string `yaml:"password"`
	DB           int    `yaml:"db"`
	PoolSize     int    `yaml:"pool_size"`
	MinIdleConns int    `yaml:"min_idle_conns"`
}

// ElasticsearchConfig Elasticsearch 配置
type ElasticsearchConfig struct {
	Enabled     bool     `yaml:"enabled"`
	Addresses   []string `yaml:"addresses"`
	Username    string   `yaml:"username"`
	Password    string   `yaml:"password"`
	IndexPrefix string   `yaml:"index_prefix"`
}

// KafkaConfig Kafka 配置
type KafkaConfig struct {
	Brokers  []string       `yaml:"brokers"`
	ClientID string         `yaml:"client_id"`
	GroupID  string         `yaml:"group_id"`
	Topics   KafkaTopics    `yaml:"topics"`
}

// KafkaTopics Kafka 主题配置
type KafkaTopics struct {
	RawBlock   string `yaml:"raw_block"`
	NormalizedTx string `yaml:"normalized_tx"`
	ReorgEvent string `yaml:"reorg_event"`
	DLQ        string `yaml:"dlq"`
}

// RocketMQConfig RocketMQ 配置
type RocketMQConfig struct {
	Enabled    bool   `yaml:"enabled"`
	NameServers string `yaml:"name_servers"`
	GroupID    string `yaml:"group_id"`
}

// DistributedLockConfig 分布式锁配置
type DistributedLockConfig struct {
	Provider string           `yaml:"provider"`
	Redis    RedisLockConfig  `yaml:"redis"`
	Etcd     EtcdLockConfig   `yaml:"etcd"`
}

// RedisLockConfig Redis 锁配置
type RedisLockConfig struct {
	Addr     string `yaml:"addr"`
	Password string `yaml:"password"`
	DB       int    `yaml:"db"`
}

// EtcdLockConfig Etcd 锁配置
type EtcdLockConfig struct {
	Endpoints    []string      `yaml:"endpoints"`
	DialTimeout time.Duration `yaml:"dial_timeout"`
}

// ScannerConfig 扫链器配置
type ScannerConfig struct {
	Concurrency ConcurrencyConfig `yaml:"concurrency"`
	Watermark   WatermarkConfig   `yaml:"watermark"`
	Sharding    ShardingConfig    `yaml:"sharding"`
}

// ConcurrencyConfig 并发配置
type ConcurrencyConfig struct {
	MaxWorkersPerChain int           `yaml:"max_workers_per_chain"`
	BatchSize          int           `yaml:"batch_size"`
	MaxRetries         int           `yaml:"max_retries"`
	RetryDelay         time.Duration `yaml:"retry_delay"`
	ParallelRequests   int           `yaml:"parallel_requests"`
}

// WatermarkConfig 水位控制配置
type WatermarkConfig struct {
	CheckInterval            time.Duration `yaml:"check_interval"`
	ReorgDetectionDepth      uint64        `yaml:"reorg_detection_depth"`
	FinalizationConfirmations uint64       `yaml:"finalization_confirmations"`
}

// ShardingConfig 分片配置
type ShardingConfig struct {
	Enabled     bool   `yaml:"enabled"`
	ShardCount  int    `yaml:"shard_count"`
	Strategy    string `yaml:"strategy"`
}

// ChainConfig 链配置（在 types.go 中定义详细结构）
type ChainConfig struct {
	Enabled             bool              `yaml:"enabled"`
	ChainID             string            `yaml:"chain_id"`
	ChainType           string            `yaml:"chain_type"`
	BlockTime           time.Duration     `yaml:"block_time"`
	FinalizationType    string            `yaml:"finalization_type"`
	RPCNodes            []RPCNodeConfig   `yaml:"rpc_nodes"`
	HealthCheck         HealthCheckConfig `yaml:"health_check"`
	StartBlock          uint64            `yaml:"start_block"`
	Confirmations       uint64            `yaml:"confirmations"`
	Contracts           []string          `yaml:"contracts"`
}

// RPCNodeConfig RPC 节点配置
type RPCNodeConfig struct {
	URL      string        `yaml:"url"`
	Weight   int           `yaml:"weight"`
	Timeout  time.Duration `yaml:"timeout"`
	MaxConns int           `yaml:"max_conns"`
}

// HealthCheckConfig 健康检查配置
type HealthCheckConfig struct {
	Enabled          bool          `yaml:"enabled"`
	Interval         time.Duration `yaml:"interval"`
	Timeout          time.Duration `yaml:"timeout"`
	FailureThreshold int           `yaml:"failure_threshold"`
	SuccessThreshold int           `yaml:"success_threshold"`
	AlertEnabled     bool          `yaml:"alert_enabled"`
}

// MonitoringConfig 监控配置
type MonitoringConfig struct {
	Prometheus PrometheusConfig `yaml:"prometheus"`
	Health     HealthConfig     `yaml:"health"`
	Alerts     AlertsConfig     `yaml:"alerts"`
}

// PrometheusConfig Prometheus 配置
type PrometheusConfig struct {
	Enabled bool   `yaml:"enabled"`
	Path    string `yaml:"path"`
}

// HealthConfig 健康检查配置
type HealthConfig struct {
	Enabled  bool `yaml:"enabled"`
	Path     string `yaml:"path"`
	Detailed bool `yaml:"detailed"`
}

// AlertsConfig 告警配置
type AlertsConfig struct {
	Enabled    bool            `yaml:"enabled"`
	WebhookURL string          `yaml:"webhook_url"`
	Thresholds AlertsThreshold `yaml:"thresholds"`
}

// AlertsThreshold 告警阈值
type AlertsThreshold struct {
	BlockLag   uint64  `yaml:"block_lag"`
	ErrorRate  float64 `yaml:"error_rate"`
	QueueDepth int     `yaml:"queue_depth"`
}

// LoggingConfig 日志配置
type LoggingConfig struct {
	Level  string         `yaml:"level"`
	Format string         `yaml:"format"`
	Output string         `yaml:"output"`
	File   FileLogConfig  `yaml:"file"`
}

// FileLogConfig 文件日志配置
type FileLogConfig struct {
	Enabled     bool   `yaml:"enabled"`
	Path        string `yaml:"path"`
	MaxSize     int    `yaml:"max_size"`
	MaxBackups  int    `yaml:"max_backups"`
	MaxAge      int    `yaml:"max_age"`
	Compress    bool   `yaml:"compress"`
}

// DeploymentConfig 部署配置
type DeploymentConfig struct {
	Replicas    int             `yaml:"replicas"`
	Resources   ResourcesConfig `yaml:"resources"`
	Autoscaling AutoscalingConfig `yaml:"autoscaling"`
}

// ResourcesConfig 资源配置
type ResourcesConfig struct {
	Requests ResourceLimit `yaml:"requests"`
	Limits   ResourceLimit `yaml:"limits"`
}

// ResourceLimit 资源限制
type ResourceLimit struct {
	CPU    string `yaml:"cpu"`
	Memory string `yaml:"memory"`
}

// AutoscalingConfig 自动扩缩容配置
type AutoscalingConfig struct {
	Enabled                   bool    `yaml:"enabled"`
	MinReplicas               int     `yaml:"min_replicas"`
	MaxReplicas               int     `yaml:"max_replicas"`
	TargetCPUUtilization      int     `yaml:"target_cpu_utilization"`
	TargetMemoryUtilization   int     `yaml:"target_memory_utilization"`
}

// LoadConfig 加载配置文件
func LoadConfig(configPath string) (*Config, error) {
	viper.SetConfigFile(configPath)
	viper.SetConfigType("yaml")

	// 设置默认值
	setDefaults()

	// 读取配置文件
	if err := viper.ReadInConfig(); err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	// 解析配置
	var config Config
	if err := viper.Unmarshal(&config); err != nil {
		return nil, fmt.Errorf("failed to unmarshal config: %w", err)
	}

	// 验证配置
	if err := validateConfig(&config); err != nil {
		return nil, fmt.Errorf("config validation failed: %w", err)
	}

	return &config, nil
}

// setDefaults 设置默认值
func setDefaults() {
	viper.SetDefault("app.name", "multi-chain-scanner")
	viper.SetDefault("app.version", "1.0.0")
	viper.SetDefault("app.environment", "development")
	viper.SetDefault("app.log_level", "info")

	viper.SetDefault("server.http_port", 8080)
	viper.SetDefault("server.grpc_port", 9090)
	viper.SetDefault("server.metrics_port", 9091)

	viper.SetDefault("database.master.driver", "mysql")
	viper.SetDefault("database.master.max_open_conns", 100)
	viper.SetDefault("database.master.max_idle_conns", 20)
	viper.SetDefault("database.master.conn_max_lifetime", "1h")

	viper.SetDefault("redis.pool_size", 100)
	viper.SetDefault("redis.min_idle_conns", 10)

	viper.SetDefault("scanner.concurrency.max_workers_per_chain", 10)
	viper.SetDefault("scanner.concurrency.batch_size", 100)
	viper.SetDefault("scanner.concurrency.max_retries", 3)
	viper.SetDefault("scanner.concurrency.retry_delay", "1s")

	viper.SetDefault("scanner.watermark.check_interval", "10s")
	viper.SetDefault("scanner.watermark.reorg_detection_depth", 128)
	viper.SetDefault("scanner.watermark.finalization_confirmations", 32)

	viper.SetDefault("scanner.sharding.enabled", true)
	viper.SetDefault("scanner.sharding.shard_count", 16)
	viper.SetDefault("scanner.sharding.strategy", "block_number")

	viper.SetDefault("monitoring.prometheus.enabled", true)
	viper.SetDefault("monitoring.prometheus.path", "/metrics")
	viper.SetDefault("monitoring.health.enabled", true)
	viper.SetDefault("monitoring.health.path", "/health")

	viper.SetDefault("logging.level", "info")
	viper.SetDefault("logging.format", "json")
	viper.SetDefault("logging.output", "stdout")
}

// validateConfig 验证配置
func validateConfig(config *Config) error {
	// 验证必需的配置项
	if config.App.Name == "" {
		return fmt.Errorf("app.name is required")
	}

	if len(config.Chains) == 0 {
		return fmt.Errorf("at least one chain must be configured")
	}

	// 验证每个链的配置
	for chainID, chain := range config.Chains {
		if !chain.Enabled {
			continue
		}

		if chain.ChainID == "" {
			return fmt.Errorf("chain.%s.chain_id is required", chainID)
		}

		if len(chain.RPCNodes) == 0 {
			return fmt.Errorf("chain.%s must have at least one RPC node", chainID)
		}

		// 验证 RPC 节点配置
		for i, node := range chain.RPCNodes {
			if node.URL == "" {
				return fmt.Errorf("chain.%s.rpc_nodes[%d].url is required", chainID, i)
			}
			if node.Weight <= 0 {
				return fmt.Errorf("chain.%s.rpc_nodes[%d].weight must be positive", chainID, i)
			}
		}
	}

	return nil
}

// GetEnabledChains 获取启用的链列表
func (c *Config) GetEnabledChains() []string {
	var enabledChains []string
	for chainID, chain := range c.Chains {
		if chain.Enabled {
			enabledChains = append(enabledChains, chainID)
		}
	}
	return enabledChains
}

// GetChainConfig 获取指定链的配置
func (c *Config) GetChainConfig(chainID string) (*ChainConfig, error) {
	chain, exists := c.Chains[chainID]
	if !exists {
		return nil, fmt.Errorf("chain %s not found", chainID)
	}
	if !chain.Enabled {
		return nil, fmt.Errorf("chain %s is disabled", chainID)
	}
	return &chain, nil
}