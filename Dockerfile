# 构建阶段
FROM golang:1.21-alpine AS builder

# 安装必要的工具
RUN apk add --no-cache git make

# 设置工作目录
WORKDIR /app

# 复制 go mod 文件
COPY go.mod go.sum ./

# 下载依赖
RUN go mod download

# 复制源代码
COPY . .

# 构建应用
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o smart-scanner ./cmd/scanner

# 运行阶段
FROM alpine:latest

# 安装必要的工具
RUN apk --no-cache add ca-certificates tzdata

# 设置时区
ENV TZ=Asia/Shanghai

# 创建非 root 用户
RUN addgroup -g 1000 scanner && \
    adduser -D -u 1000 -G scanner scanner

# 设置工作目录
WORKDIR /app

# 从构建阶段复制二进制文件
COPY --from=builder /app/smart-scanner .

# 复制配置文件
COPY config.yaml .

# 创建日志目录
RUN mkdir -p /app/logs && \
    chown -R scanner:scanner /app

# 切换到非 root 用户
USER scanner

# 暴露端口
EXPOSE 8080 9090

# 健康检查
HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
    CMD wget --quiet --tries=1 --spider http://localhost:8080/health || exit 1

# 启动应用
CMD ["./smart-scanner"]