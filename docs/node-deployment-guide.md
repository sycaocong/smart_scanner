# 自建区块链归档节点部署指南

## 一、硬件配置要求

### Ethereum 归档节点
- **CPU**: 16+ 核心（推荐 AMD Ryzen 9 或 Intel Xeon）
- **内存**: 64GB+ RAM（推荐 128GB）
- **存储**: 12TB+ NVMe SSD（完整归档需要约 12TB）
- **网络**: 1Gbps+ 带宽，低延迟连接
- **同步时间**: 3-7 天（使用 Erigon 快速同步）

### BSC 归档节点
- **CPU**: 16+ 核心
- **内存**: 64GB+ RAM
- **存储**: 4TB+ NVMe SSD
- **网络**: 1Gbps+ 带宽
- **同步时间**: 2-5 天

### Polygon 归档节点
- **CPU**: 16+ 核心
- **内存**: 64GB+ RAM
- **存储**: 5TB+ NVMe SSD
- **网络**: 1Gbps+ 带宽
- **同步时间**: 3-6 天

## 二、节点软件选择与部署

### 1. Ethereum - Erigon（推荐）

**优势**:
- 快速同步（使用 P2P torrent 方式）
- 低资源消耗（压缩存储）
- 支持完整归档数据
- 内置追溯查询功能

**部署步骤**:

```bash
# 使用 Docker 部署
docker run -d \
  --name erigon-archive \
  -v /data/erigon:/data \
  -p 8545:8545 \
  -p 8546:8546 \
  -p 30303:30303 \
  thorax/erigon:latest \
  --chain=mainnet \
  --datadir=/data \
  --sync=archive \
  --http \
  --http.addr=0.0.0.0 \
  --http.port=8545 \
  --http.api=eth,web3,net,debug,trace,txpool \
  --ws \
  --ws.addr=0.0.0.0 \
  --ws.port=8546 \
  --torrent.port=42069 \
  --prune=none
```

**配置文件** (`erigon.yaml`):

```yaml
chain: mainnet
datadir: /data/erigon
sync: archive

http:
  enabled: true
  addr: 0.0.0.0
  port: 8545
  api:
    - eth
    - web3
    - net
    - debug
    - trace
    - txpool
    - admin

ws:
  enabled: true
  addr: 0.0.0.0
  port: 8546

torrent:
  enabled: true
  port: 42069
  download_rate: 100MB

prune:
  mode: none  # 归档模式不裁剪
```

### 2. Ethereum - Geth（备选）

**优势**:
- 官方实现，稳定可靠
- 社区支持广泛
- 适合长期维护

**部署步骤**:

```bash
# 下载归档快照（加速同步）
wget https://snapshots.mainnet.ethereum.tech/geth-archival/latest.tar

# 解压快照
tar -xf latest.tar -C /data/geth

# 启动 Geth
docker run -d \
  --name geth-archive \
  -v /data/geth:/data \
  -p 8547:8545 \
  -p 8548:8546 \
  ethereum/client-go:latest \
  --datadir=/data \
  --syncmode=full \
  --gcmode=archive \
  --http \
  --http.addr=0.0.0.0 \
  --http.port=8545 \
  --http.api=eth,web3,net,debug,trace,admin \
  --ws \
  --ws.addr=0.0.0.0 \
  --ws.port=8546 \
  --cache=8000 \
  --maxpeers=100
```

### 3. BSC - Erigon-BSC

```bash
docker run -d \
  --name bsc-archive \
  -v /data/bsc:/data \
  -p 8545:8545 \
  thorax/erigon:latest \
  --chain=bsc \
  --datadir=/data \
  --sync=archive \
  --http \
  --http.addr=0.0.0.0 \
  --http.port=8545 \
  --http.api=eth,web3,net,debug,trace \
  --prune=none
```

### 4. Polygon - Bor + Heimdall

```bash
# Heimdall（共识层）
docker run -d \
  --name heimdall \
  -v /data/heimdall:/data \
  polygon-heimdall:latest \
  --home=/data \
  --chain=mainnet

# Bor（执行层）
docker run -d \
  --name bor-archive \
  -v /data/bor:/data \
  -p 8545:8545 \
  polygon-bor:latest \
  --chain=mainnet \
  --datadir=/data \
  --sync=full \
  --archive \
  --http \
  --http.addr=0.0.0.0 \
  --http.port=8545 \
  --http.api=eth,web3,net,debug,trace
```

## 三、节点监控与维护

### 1. Prometheus 监控配置

**Erigon 指标端点**: `http://localhost:6060/debug/metrics/prometheus`

**Geth 指标端点**: `http://localhost:6061`

**Prometheus 配置** (`prometheus.yml`):

```yaml
scrape_configs:
  - job_name: 'ethereum-nodes'
    static_configs:
      - targets:
        - 'localhost:6060'  # Erigon metrics
        - 'localhost:6061'  # Geth metrics
    metrics_path: /debug/metrics/prometheus
```

### 2. 健康检查脚本

```bash
#!/bin/bash
# health_check.sh

ERIGON_URL="http://localhost:8545"
GETH_URL="http://localhost:8547"

check_node() {
    NODE_URL=$1
    NODE_NAME=$2
    
    RESPONSE=$(curl -s -X POST \
        -H "Content-Type: application/json" \
        --data '{"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":1}' \
        $NODE_URL)
    
    if [ $? -eq 0 ]; then
        BLOCK=$(echo $RESPONSE | jq -r '.result')
        BLOCK_NUM=$(echo $BLOCK | xargs printf "%d")
        echo "[$NODE_NAME] Current block: $BLOCK_NUM"
        return 0
    else
        echo "[$NODE_NAME] Health check failed!"
        return 1
    fi
}

check_node $ERIGON_URL "Erigon"
check_node $GETH_URL "Geth"
```

### 3. 自动重启脚本

```bash
#!/bin/bash
# auto_restart.sh

while true; do
    # 检查 Erigon
    if ! docker ps | grep -q erigon-archive; then
        echo "Erigon stopped, restarting..."
        docker start erigon-archive
    fi
    
    # 检查 Geth
    if ! docker ps | grep -q geth-archive; then
        echo "Geth stopped, restarting..."
        docker start geth-archive
    fi
    
    sleep 60
done
```

## 四、性能优化配置

### 1. Erigon 性能优化

```yaml
# 高性能配置
erigon:
  cache:
    state: 8GB
    block: 4GB
    txpool: 2GB
  
  database:
    compression: true
    mmap: true
    
  network:
    max_peers: 100
    discovery: true
    
  sync:
    batch_size: 1000
    parallel_requests: 10
```

### 2. Geth 性能优化

```bash
# 启动参数优化
geth \
  --cache=8000 \
  --cache.database=4000 \
  --cache.gc=2000 \
  --cache.trie=2000 \
  --maxpeers=100 \
  --light.maxpeers=50 \
  --txpool.nolocals \
  --txpool.accountslots=16 \
  --txpool.globalslots=4096
```

### 3. 系统优化

```bash
# 系统参数优化（添加到 /etc/sysctl.conf）
net.core.somaxconn = 65535
net.ipv4.tcp_max_syn_backlog = 65535
net.ipv4.tcp_tw_reuse = 1
net.ipv4.tcp_fin_timeout = 30
vm.swappiness = 10
vm.dirty_ratio = 40
vm.dirty_background_ratio = 10

# 应用优化
sudo sysctl -p
```

## 五、数据同步策略

### 1. 快照同步（推荐）

```bash
# 使用官方快照加速同步
# Ethereum
wget https://snapshots.mainnet.ethereum.tech/geth-archival/latest.tar

# BSC
wget https://snapshots.bscscan.com/latest.tar.gz

# Polygon
wget https://snapshots.matic.network/latest.tar.gz
```

### 2. 增量同步

```bash
# 定期同步最新区块
# 每天同步一次
0 2 * * * wget https://snapshots.mainnet.ethereum.tech/daily.tar -O /tmp/daily.tar
0 3 * * * tar -xf /tmp/daily.tar -C /data/erigon/snapshots
```

### 3. 数据验证

```bash
# 验证区块哈希一致性
curl -X POST -H "Content-Type: application/json" \
  --data '{"jsonrpc":"2.0","method":"eth_getBlockByNumber","params":["0x1",false],"id":1}' \
  http://localhost:8545 | jq -r '.result.hash'
```

## 六、故障排查

### 1. 同步卡住

```bash
# 查看同步进度
curl -X POST -H "Content-Type: application/json" \
  --data '{"jsonrpc":"2.0","method":"eth_syncing","params":[],"id":1}' \
  http://localhost:8545

# 查看节点日志
docker logs erigon-archive --tail 100
```

### 2. 内存不足

```bash
# 监控内存使用
docker stats erigon-archive

# 调整缓存配置
docker update --memory=128g --memory-swap=256g erigon-archive
```

### 3. 网络问题

```bash
# 检查网络连接
curl -I http://localhost:8545

# 检查节点发现
curl -X POST -H "Content-Type: application/json" \
  --data '{"jsonrpc":"2.0","method":"admin_peers","params":[],"id":1}' \
  http://localhost:8545
```

## 七、安全配置

### 1. 访问控制

```yaml
# 仅允许内网访问
http:
  addr: 192.168.1.100  # 内网 IP
  port: 8545
  cors:
    allowed_origins:
      - "http://192.168.1.*"
  auth:
    enabled: true
    secret: "your-secret-key"
```

### 2. 防火墙配置

```bash
# 仅开放必要端口
sudo ufw allow 8545/tcp  # RPC
sudo ufw allow 8546/tcp  # WebSocket
sudo ufw allow 30303/tcp # P2P
sudo ufw enable
```

### 3. SSL/TLS 加密

```bash
# 使用 Nginx 反向代理添加 SSL
server {
    listen 443 ssl;
    ssl_certificate /path/to/cert.pem;
    ssl_certificate_key /path/to/key.pem;
    
    location / {
        proxy_pass http://localhost:8545;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
    }
}
```

## 八、成本分析

### 1. 硬件成本

- **服务器**: $5000-$10000（高性能服务器）
- **存储**: $2000-$3000（12TB NVMe SSD）
- **网络**: $200-$500/月（带宽费用）

### 2. 运维成本

- **电力**: $200-$400/月
- **维护**: $500-$1000/月（人工成本）
- **备份**: $100-$200/月（备份存储）

### 3. 收益分析

- **节省 API 费用**: 相比第三方 RPC（$1000-$5000/月）
- **提高可靠性**: 自建节点可控性更高
- **增强隐私**: 数据不经过第三方
- **性能优势**: 本地节点延迟更低（<10ms）

## 九、最佳实践

1. **多节点部署**: 至少部署 2 个节点（主备）
2. **自动监控**: Prometheus + Grafana 监控
3. **定期备份**: 每周备份节点数据
4. **版本升级**: 定期升级节点软件
5. **日志分析**: 集中日志管理（ELK）
6. **性能测试**: 定期性能测试和优化
7. **故障演练**: 定期故障演练和恢复测试

## 十、与扫链系统集成

在 `config.yaml` 中配置自建节点：

```yaml
chains:
  ethereum:
    rpc_nodes:
      # 自建归档节点（最高权重）
      - url: "http://192.168.1.100:8545"
        weight: 200
        timeout: "5s"
        max_conns: 100
        
      # 自建备用节点
      - url: "http://192.168.1.101:8545"
        weight: 150
        timeout: "5s"
        max_conns: 50
        
      # 第三方 RPC（作为备份）
      - url: "https://eth-mainnet.alchemyapi.io/v2/YOUR_KEY"
        weight: 80
        timeout: "10s"
        max_conns: 30
```

---

**部署建议**:
1. 先部署单个链的节点（如 Ethereum）
2. 观察同步进度和性能表现
3. 逐步添加其他链的节点
4. 配置监控和告警系统
5. 与扫链系统集成测试

**注意事项**:
1. 确保硬件资源充足
2. 网络连接稳定
3. 定期检查同步进度
4. 及时处理告警信息
5. 保持节点软件更新