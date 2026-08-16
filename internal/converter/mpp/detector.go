package mpp

import (
	"context"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
)

// DetectDatabaseType 自动检测 MPP 数据库类型
// 优先使用版本号检测（更可靠），备用扩展检查
// ctx 用于取消控制
func DetectDatabaseType(ctx context.Context, pool *pgxpool.Pool) string {
	// 优先：通过版本号检测（最可靠，不依赖扩展是否安装）
	if dbType := detectFromVersion(ctx, pool); dbType != "unknown" {
		return dbType
	}

	// 备用：检查扩展（某些发行版版本号可能不包含 MPP 标识）
	if isGreenplumByExtension(ctx, pool) {
		return "greenplum"
	}
	if isYugabyteByExtension(ctx, pool) {
		return "yugabyte"
	}

	return "unknown"
}

// detectFromVersion 通过 SELECT version() 检测数据库类型
func detectFromVersion(ctx context.Context, pool *pgxpool.Pool) string {
	var version string
	err := pool.QueryRow(ctx, "SELECT version()").Scan(&version)
	if err != nil {
		return "unknown"
	}

	versionLower := strings.ToLower(version)
	if strings.Contains(versionLower, "greenplum") {
		return "greenplum"
	}
	if strings.Contains(versionLower, "yugabyte") {
		return "yugabyte"
	}
	if strings.Contains(versionLower, "cockroachdb") {
		return "cockroachdb"
	}

	return "unknown"
}

func isGreenplumByExtension(ctx context.Context, pool *pgxpool.Pool) bool {
	var exists bool
	err := pool.QueryRow(ctx,
		"SELECT EXISTS(SELECT 1 FROM pg_catalog.pg_extension WHERE extname = 'gp_kmeans')",
	).Scan(&exists)
	return err == nil && exists
}

func isYugabyteByExtension(ctx context.Context, pool *pgxpool.Pool) bool {
	var exists bool
	err := pool.QueryRow(ctx,
		"SELECT EXISTS(SELECT 1 FROM pg_catalog.pg_extension WHERE extname = 'yb_pg_metrics')",
	).Scan(&exists)
	return err == nil && exists
}

// IsSupportedMPP 检查是否为支持的 MPP 数据库
// 注意：目前仅支持 Greenplum，YugabyteDB 不支持 ALTER TABLE SET DISTRIBUTED BY 语法
func IsSupportedMPP(dbType string) bool {
	return dbType == "greenplum"
}
