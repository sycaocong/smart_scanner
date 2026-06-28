# Wallet API 文档

## 概述

本 API 提供钱包相关功能，包括余额查询、交易记录查询、转账、充值和提现等操作。

## 基础信息

- **基础 URL**: `http://localhost:8080/api/v1/wallet`
- **内容类型**: `application/json`
- **认证方式**: JWT Token (Header: `Authorization: Bearer <token>`)

## 错误响应格式

```json
{
  "success": false,
  "error": "错误描述信息"
}
```

## 接口列表

### 1. 查询单个余额

**URL**: `/api/v1/wallet/balance`

**方法**: `POST`

**代码位置**: [handler.go#L25](../internal/wallet/handler.go#L25)

**请求体**:

| 字段 | 类型 | 必填 | 说明 |
|------|------|------|------|
| chain_id | string | 否 | 链 ID，默认 "1" (Ethereum) |
| address | string | 是 | 钱包地址 |
| contract | string | 否 | 代币合约地址，不传则查询原生代币余额 |

**请求示例**:

```json
{
  "chain_id": "1",
  "address": "0x742d35Cc6634C0532925a3b844Bc9e7595f7AAA",
  "contract": ""
}
```

**成功响应**:

```json
{
  "success": true,
  "data": {
    "chain_id": "1",
    "address": "0x742d35Cc6634C0532925a3b844Bc9e7595f7AAA",
    "token": {
      "chain_id": "1",
      "contract": "",
      "symbol": "ETH",
      "name": "Ethereum",
      "decimals": 18,
      "type": "native"
    },
    "balance": "1000000000000000000",
    "display": "1.0",
    "block_number": 19000000,
    "updated_at": "2024-01-15T10:30:00Z"
  }
}
```

### 2. 查询多个余额

**URL**: `/api/v1/wallet/balances`

**方法**: `POST`

**代码位置**: [handler.go#L58](../internal/wallet/handler.go#L58)

**请求体**:

| 字段 | 类型 | 必填 | 说明 |
|------|------|------|------|
| chain_id | string | 否 | 链 ID，默认 "1" (Ethereum) |
| address | string | 是 | 钱包地址 |
| tokens | array | 否 | 代币合约地址列表，不传则查询默认代币 |

**请求示例**:

```json
{
  "chain_id": "1",
  "address": "0x742d35Cc6634C0532925a3b844Bc9e7595f7AAA",
  "tokens": [
    "0xdAC17F958D2ee523a2206206994597C13D831ec7",
    "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"
  ]
}
```

**成功响应**:

```json
{
  "success": true,
  "data": [
    {
      "chain_id": "1",
      "address": "0x742d35Cc6634C0532925a3b844Bc9e7595f7AAA",
      "token": {
        "chain_id": "1",
        "contract": "",
        "symbol": "ETH",
        "name": "Ethereum",
        "decimals": 18,
        "type": "native"
      },
      "balance": "1000000000000000000",
      "display": "1.0",
      "block_number": 19000000,
      "updated_at": "2024-01-15T10:30:00Z"
    },
    {
      "chain_id": "1",
      "address": "0x742d35Cc6634C0532925a3b844Bc9e7595f7AAA",
      "token": {
        "chain_id": "1",
        "contract": "0xdAC17F958D2ee523a2206206994597C13D831ec7",
        "symbol": "USDT",
        "name": "Tether",
        "decimals": 6,
        "type": "erc20"
      },
      "balance": "100000000",
      "display": "100.0",
      "block_number": 19000000,
      "updated_at": "2024-01-15T10:30:00Z"
    }
  ]
}
```

### 3. 查询交易记录

**URL**: `/api/v1/wallet/transactions`

**方法**: `POST`

**代码位置**: [handler.go#L91](../internal/wallet/handler.go#L91)

**请求体**:

| 字段 | 类型 | 必填 | 说明 |
|------|------|------|------|
| chain_id | string | 否 | 链 ID，默认 "1" (Ethereum) |
| address | string | 是 | 钱包地址 |
| token | string | 否 | 代币合约地址 |
| tx_type | string | 否 | 交易类型：transfer, deposit, withdraw, swap |
| start_time | string | 否 | 开始时间 (ISO 8601) |
| end_time | string | 否 | 结束时间 (ISO 8601) |
| page | int | 否 | 页码，默认 1 |
| page_size | int | 否 | 每页数量，默认 20 |

**请求示例**:

```json
{
  "chain_id": "1",
  "address": "0x742d35Cc6634C0532925a3b844Bc9e7595f7AAA",
  "token": "0xdAC17F958D2ee523a2206206994597C13D831ec7",
  "page": 1,
  "page_size": 20
}
```

**成功响应**:

```json
{
  "success": true,
  "data": [
    {
      "id": "0xabc123...",
      "chain_id": "1",
      "tx_hash": "0xabc123...",
      "from": "0x742d35Cc6634C0532925a3b844Bc9e7595f7AAA",
      "to": "0xdAC17F958D2ee523a2206206994597C13D831ec7",
      "token": {
        "chain_id": "1",
        "contract": "0xdAC17F958D2ee523a2206206994597C13D831ec7",
        "symbol": "USDT",
        "name": "Tether",
        "decimals": 6,
        "type": "erc20"
      },
      "value": "100000000",
      "display": "100.0",
      "fee": "21000000000000",
      "fee_display": "0.000021",
      "block_number": 19000000,
      "status": "confirmed",
      "tx_type": "transfer",
      "created_at": "2024-01-15T10:30:00Z",
      "confirmed_at": "2024-01-15T10:30:12Z"
    }
  ],
  "total": 100,
  "page": 1,
  "page_size": 20,
  "has_more": true
}
```

### 4. 转账

**URL**: `/api/v1/wallet/transfer`

**方法**: `POST`

**代码位置**: [handler.go#L139](../internal/wallet/handler.go#L139)

**请求体**:

| 字段 | 类型 | 必填 | 说明 |
|------|------|------|------|
| chain_id | string | 否 | 链 ID，默认 "1" (Ethereum) |
| from | string | 是 | 发送方地址 |
| to | string | 是 | 接收方地址 |
| contract | string | 否 | 代币合约地址，不传则转账原生代币 |
| value | string | 是 | 转账金额 (wei) |
| private_key | string | 是 | 发送方私钥 |
| gas_limit | uint64 | 否 | Gas 限制，默认 21000 |
| gas_price | string | 否 | Gas 价格 (wei)，默认 10gwei |

**请求示例**:

```json
{
  "chain_id": "1",
  "from": "0x742d35Cc6634C0532925a3b844Bc9e7595f7AAA",
  "to": "0xdAC17F958D2ee523a2206206994597C13D831ec7",
  "contract": "",
  "value": "1000000000000000000",
  "private_key": "0xYourPrivateKey",
  "gas_limit": 21000,
  "gas_price": "10000000000"
}
```

**成功响应**:

```json
{
  "success": true,
  "tx_hash": "0xabc123...",
  "chain_id": "1"
}
```

### 5. 充值

**URL**: `/api/v1/wallet/deposit`

**方法**: `POST`

**代码位置**: [handler.go#L185](../internal/wallet/handler.go#L185)

**请求体**:

| 字段 | 类型 | 必填 | 说明 |
|------|------|------|------|
| chain_id | string | 否 | 链 ID，默认 "1" (Ethereum) |
| address | string | 是 | 充值地址 |
| token | string | 否 | 代币合约地址 |

**请求示例**:

```json
{
  "chain_id": "1",
  "address": "0x742d35Cc6634C0532925a3b844Bc9e7595f7AAA",
  "token": "0xdAC17F958D2ee523a2206206994597C13D831ec7"
}
```

**成功响应**:

```json
{
  "success": true,
  "address": "0x742d35Cc6634C0532925a3b844Bc9e7595f7AAA",
  "chain_id": "1"
}
```

### 6. 提现

**URL**: `/api/v1/wallet/withdraw`

**方法**: `POST`

**代码位置**: [handler.go#L215](../internal/wallet/handler.go#L215)

**请求体**:

| 字段 | 类型 | 必填 | 说明 |
|------|------|------|------|
| chain_id | string | 否 | 链 ID，默认 "1" (Ethereum) |
| address | string | 是 | 提现地址 |
| to | string | 是 | 目标地址 |
| contract | string | 否 | 代币合约地址，不传则提现原生代币 |
| value | string | 是 | 提现金额 (wei) |
| private_key | string | 是 | 提现地址私钥 |

**请求示例**:

```json
{
  "chain_id": "1",
  "address": "0x742d35Cc6634C0532925a3b844Bc9e7595f7AAA",
  "to": "0x1234567890abcdef1234567890abcdef12345678",
  "contract": "0xdAC17F958D2ee523a2206206994597C13D831ec7",
  "value": "100000000",
  "private_key": "0xYourPrivateKey"
}
```

**成功响应**:

```json
{
  "success": true,
  "tx_hash": "0xabc123...",
  "chain_id": "1"
}
```

## 支持的链

| 链 ID | 链名称 | 原生代币 |
|-------|--------|----------|
| 1 | Ethereum | ETH |
| 56 | BSC | BNB |
| 137 | Polygon | MATIC |

## 默认代币列表

### Ethereum (Chain ID: 1)

| 合约地址 | 代币符号 | 名称 | 精度 |
|----------|----------|------|------|
| 0xdAC17F958D2ee523a2206206994597C13D831ec7 | USDT | Tether | 6 |
| 0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48 | USDC | USD Coin | 6 |
| 0x2260FAC5E5542a773Aa44fBCfeDf7C193bc2C599 | WBTC | Wrapped BTC | 8 |

### BSC (Chain ID: 56)

| 合约地址 | 代币符号 | 名称 | 精度 |
|----------|----------|------|------|
| 0x55d398326f99059fF775485246999027B3197955 | USDT | Tether | 18 |
| 0x8AC76a51cc950d9822D68b83fE1Ad97B32Cd580d | USDC | USD Coin | 18 |

### Polygon (Chain ID: 137)

| 合约地址 | 代币符号 | 名称 | 精度 |
|----------|----------|------|------|
| 0xc2132D05D31c914a87C6611C10748AEb04B58e8F | USDT | Tether | 6 |
| 0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174 | USDC | USD Coin | 6 |

## 响应状态码

| 状态码 | 说明 |
|--------|------|
| 200 | 成功 |
| 400 | 请求参数错误 |
| 500 | 服务器内部错误 |

## 注意事项

1. **私钥安全**: 私钥仅用于签名交易，不会被存储或传输
2. **网络费用**: 转账和提现操作需要支付 Gas 费用
3. **确认时间**: 交易确认时间取决于链的区块时间
4. **金额格式**: 所有金额以 wei 为单位（最小单位）
5. **地址格式**: 所有地址需要以 `0x` 开头