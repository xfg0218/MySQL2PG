#!/bin/bash
# =============================================================================
# wait-for-greenplum.sh
# 等待 Greenplum 集群就绪的脚本
# 
# 用法:
#   ./wait-for-greenplum.sh [MAX_RETRIES] [RETRY_INTERVAL]
#
# 参数:
#   MAX_RETRIES:     最大重试次数 (默认：36，约 3 分钟)
#   RETRY_INTERVAL:  重试间隔秒数 (默认：5)
#
# 退出码:
#   0: Greenplum 集群就绪
#   1: 超时或失败
# =============================================================================

set -e

# 配置参数
MAX_RETRIES=${1:-36}
RETRY_INTERVAL=${2:-5}
CONTAINER_NAME=${GP_MASTER_CONTAINER:-gp_master}
GPADMIN_USER=${GPADMIN_USER:-gpadmin}

# 颜色输出
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# 检查 Master 节点是否可连接
check_master() {
    docker exec "$CONTAINER_NAME" su - "$GPADMIN_USER" -c "psql -U $GPADMIN_USER -d postgres -c 'SELECT 1' > /dev/null 2>&1" 2>/dev/null
    return $?
}

# 检查 Segment 节点状态
check_segments() {
    # 查询 gp_segment_configuration 表，检查所有 segment 状态是否为 'u' (up)
    local result
    result=$(docker exec "$CONTAINER_NAME" su - "$GPADMIN_USER" -c "psql -U $GPADMIN_USER -d postgres -t -c \"SELECT COUNT(*) FROM gp_segment_configuration WHERE status = 'u' AND content >= 0\"" 2>/dev/null)
    
    if [ -z "$result" ]; then
        return 1
    fi
    
    # 清理空白字符
    result=$(echo "$result" | tr -d '[:space:]')
    
    # 至少有 1 个 segment 在线
    if [ "$result" -ge 1 ] 2>/dev/null; then
        return 0
    fi
    
    return 1
}

# 显示集群状态
show_cluster_status() {
    log_info "Greenplum 集群状态:"
    docker exec "$CONTAINER_NAME" su - "$GPADMIN_USER" -c "psql -U $GPADMIN_USER -d postgres -c \"
        SELECT 
            role,
            preferred_name as hostname,
            address,
            port,
            CASE WHEN status = 'u' THEN 'UP' 
                 WHEN status = 'd' THEN 'DOWN' 
                 ELSE 'UNKNOWN' 
            END as status
        FROM gp_segment_configuration 
        ORDER BY role, content
    \"" 2>/dev/null || true
}

# 主循环
main() {
    log_info "开始等待 Greenplum 集群就绪..."
    log_info "最大重试次数：$MAX_RETRIES, 间隔：${RETRY_INTERVAL}s, 容器：$CONTAINER_NAME"
    
    local attempt=1
    local master_ready=false
    
    while [ $attempt -le $MAX_RETRIES ]; do
        # 检查 Master 节点
        if [ "$master_ready" = false ]; then
            if check_master; then
                master_ready=true
                log_info "Master 节点已就绪 (尝试 $attempt/$MAX_RETRIES)"
            else
                log_warn "等待 Master 节点... (尝试 $attempt/$MAX_RETRIES)"
                sleep $RETRY_INTERVAL
                ((attempt++))
                continue
            fi
        fi
        
        # 检查 Segment 节点
        if check_segments; then
            log_info "所有 Segment 节点已就绪!"
            show_cluster_status
            log_info "Greenplum 集群已就绪，总耗时：$(( (attempt - 1) * RETRY_INTERVAL ))s"
            exit 0
        fi
        
        log_warn "等待 Segment 节点... (尝试 $attempt/$MAX_RETRIES)"
        sleep $RETRY_INTERVAL
        ((attempt++))
    done
    
    log_error "Greenplum 集群未能在超时时间内就绪"
    show_cluster_status
    exit 1
}

# 检查 Docker 是否可用
if ! command -v docker &> /dev/null; then
    log_error "Docker 未安装或不在 PATH 中"
    exit 1
fi

# 检查容器是否存在
if ! docker ps -a --format '{{.Names}}' | grep -q "^${CONTAINER_NAME}$"; then
    log_error "Greenplum 容器 '$CONTAINER_NAME' 不存在"
    log_error "请先启动容器：docker-compose up -d"
    exit 1
fi

main
