# Greenplum CI Testing

本目录包含在 GitHub Actions CI 环境中部署和测试 Greenplum 数据库的所有配置文件和脚本。

## 目录结构

```
scripts/greenplum/
├── docker-compose.yml          # Greenplum 集群 Docker Compose 配置
├── gpinitsystem_config         # Greenplum 初始化配置文件
├── wait-for-greenplum.sh       # 等待 Greenplum 就绪脚本
└── README.md                   # 本文件
```

## 集群架构

```
┌─────────────────────────────────────────┐
│  GitHub Actions Runner (Ubuntu 22.04)  │
│                                          │
│  ┌──────────────┐                       │
│  │   Master     │                       │
│  │  Port: 5432  │                       │
│  │  Memory: 2GB │                       │
│  └──────────────┘                       │
│         │                                │
│  ┌──────────────┐                       │
│  │  Segment 1   │                       │
│  │  Port: 6000  │                       │
│  │  Memory: 2GB │                       │
│  └──────────────┘                       │
└─────────────────────────────────────────┘
```

## 本地测试

### 1. 启动 Greenplum 集群

```bash
cd scripts/greenplum
docker-compose up -d
```

### 2. 等待集群就绪

```bash
./wait-for-greenplum.sh
```

### 3. 验证集群状态

```bash
# 连接 Greenplum
docker exec gp_master su - gpadmin -c "psql -U gpadmin -d postgres"

# 查看集群配置
docker exec gp_master su - gpadmin -c "psql -U gpadmin -d postgres -c 'SELECT * FROM gp_segment_configuration'"

# 查看版本
docker exec gp_master su - gpadmin -c "psql -U gpadmin -d postgres -c 'SELECT version()'"
```

### 4. 创建测试数据库

```bash
docker exec gp_master su - gpadmin -c "psql -U gpadmin -d postgres -c 'CREATE DATABASE test_db'"
```

### 5. 停止集群

```bash
docker-compose down -v
```

## CI 使用

### 自动触发

Greenplum 测试在以下情况下自动运行：

1. **main 分支推送** 且修改了相关文件：
   - `internal/converter/mpp/**`
   - `internal/converter/postgres/sync_tableddl.go`
   - `internal/converter/postgres/manager.go`
   - `scripts/greenplum/**`
   - `.github/workflows/greenplum-test.yml`

2. **Pull Request** 且修改了上述文件

### 手动触发

在 GitHub Actions 页面点击 "Run workflow" 按钮，可以选择：

- **MySQL 版本**: 5.7 或 8.0
- **调试模式**: 启用后测试完成不删除容器（便于调试）

## 环境变量

| 变量名 | 默认值 | 说明 |
|--------|--------|------|
| `GP_MASTER_CONTAINER` | `gp_master` | Master 节点容器名 |
| `GPADMIN_USER` | `gpadmin` | Greenplum 管理员用户名 |
| `GREENPLUM_IMAGE` | `andrewjw/docker-greenplum:latest` | Greenplum 镜像 |

## 故障排查

### 1. 集群启动失败

```bash
# 查看容器日志
docker logs gp_master
docker logs gp_segment1

# 查看容器状态
docker-compose ps

# 重启集群
docker-compose down -v
docker-compose up -d
```

### 2. Segment 节点未就绪

```bash
# 检查 segment 状态
docker exec gp_master su - gpadmin -c "psql -U gpadmin -d postgres -c 'SELECT * FROM gp_segment_configuration'"

# 查看 segment 日志
docker exec gp_master cat /data/master/pg_log/gpdb-*.csv
```

### 3. 内存不足

如果 CI runner 内存不足，可以调整配置：

```yaml
# docker-compose.yml
environment:
  - GP_MEMORY=1GB  # 降低内存配置
```

## 参考资料

- [Greenplum 官方文档](https://greenplum.org/docs/)
- [Docker Greenplum](https://github.com/andrewjw/docker-greenplum)
- [Greenplum 段配置表](https://greenplum.org/docs/admin_guide/system_tables/gp_segment_configuration.html)
