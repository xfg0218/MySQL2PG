package mpp

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// DistributionKeyInfo 表的分布键信息
type DistributionKeyInfo struct {
	TableName string
	Columns   []string
	IsRandom  bool // 是否随机分布（无明确分布键）
}

// GetCurrentDistributionKey 获取表的当前分布键（Greenplum/Yugabyte）
func GetCurrentDistributionKey(pool *pgxpool.Pool, tableName string, schemaName string, lowercaseColumns bool) (*DistributionKeyInfo, error) {
	ctx := context.Background()

	// Greenplum 查询分布键
	query := `
		SELECT a.attname
		FROM pg_catalog.gp_distribution_policy p
		JOIN pg_catalog.pg_attribute a ON a.attrelid = p.localoid AND a.attnum = ANY(p.attrnums)
		JOIN pg_catalog.pg_class c ON c.oid = p.localoid
		JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
		WHERE n.nspname = $2 AND c.relname = $1
		ORDER BY array_position(p.attrnums, a.attnum)
	`

	var columns []string
	rows, err := pool.Query(ctx, query, tableName, schemaName)
	if err != nil {
		// 区分"功能不支持"（非 MPP 数据库）和"真正的错误"
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) {
			// PostgreSQL 错误码 42P01 = undefined_table
			// 42883 = undefined_function
			// 3F000 = invalid_schema_name
			if pgErr.Code == "42P01" || pgErr.Code == "42883" || pgErr.Code == "3F000" {
				// 系统表不存在 → 非 MPP 数据库，返回空结果（不上报错误）
				return &DistributionKeyInfo{
					TableName: tableName,
					Columns:   []string{},
					IsRandom:  true,
				}, nil
			}
		}
		// 其他错误（连接超时、权限不足等）应返回 error
		return nil, fmt.Errorf("查询分布键失败: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var col string
		if err := rows.Scan(&col); err != nil {
			return nil, err
		}
		// 如果启用 lowercaseColumns，将列名转换为小写
		if lowercaseColumns {
			col = strings.ToLower(col)
		}
		columns = append(columns, col)
	}

	return &DistributionKeyInfo{
		TableName: tableName,
		Columns:   columns,
		IsRandom:  len(columns) == 0,
	}, nil
}

// CalculateNewDistributionKey 计算新的分布键 = UNIQUE列 + 当前分布键（去重）
// 规则：UNIQUE 列优先，然后补充当前分布键的剩余列
func CalculateNewDistributionKey(currentDistKey []string, uniqueColumns []string) []string {
	seen := make(map[string]bool)
	newKey := []string{}

	// UNIQUE 列优先
	for _, col := range uniqueColumns {
		if !seen[col] {
			seen[col] = true
			newKey = append(newKey, col)
		}
	}

	// 补充当前分布键的剩余列
	for _, col := range currentDistKey {
		if !seen[col] {
			seen[col] = true
			newKey = append(newKey, col)
		}
	}

	return newKey
}

// quoteIdentifier 安全地转义 SQL 标识符（表名、列名等）
// 使用 PostgreSQL 的双引号转义规则：双引号内部的双引号需要写成两个双引号
func quoteIdentifier(name string) string {
	if name == "" {
		return ""
	}
	safe := strings.ReplaceAll(name, "\"", "\"\"")
	return fmt.Sprintf("\"%s\"", safe)
}

// GenerateAlterDistributionKeySQL 生成调整分布键的 SQL
func GenerateAlterDistributionKeySQL(tableName string, schemaName string, newDistKey []string) string {
	quotedSchema := quoteIdentifier(schemaName)
	quotedTable := quoteIdentifier(tableName)
	quotedCols := make([]string, len(newDistKey))
	for i, col := range newDistKey {
		quotedCols[i] = quoteIdentifier(col)
	}
	distKeySQL := strings.Join(quotedCols, ", ")
	return fmt.Sprintf("ALTER TABLE %s.%s SET DISTRIBUTED BY (%s)", quotedSchema, quotedTable, distKeySQL)
}

// AdjustDistributionKey 调整表的分布键
func AdjustDistributionKey(pool *pgxpool.Pool, tableName string, schemaName string, uniqueColumns []string, lowercaseColumns bool, logFunc func(string, ...interface{})) (bool, error) {
	// 处理默认 Schema
	if schemaName == "" {
		schemaName = "public" // 默认 schema
	}

	// 1. 查询当前分布键
	currentDistKey, err := GetCurrentDistributionKey(pool, tableName, schemaName, lowercaseColumns)
	if err != nil {
		return false, fmt.Errorf("查询当前分布键失败: %w", err)
	}

	// 2. 计算新分布键
	newDistKey := CalculateNewDistributionKey(currentDistKey.Columns, uniqueColumns)

	// 3. 如果分布键未变化，跳过
	if len(newDistKey) == len(currentDistKey.Columns) {
		same := true
		for i := range newDistKey {
			if !strings.EqualFold(newDistKey[i], currentDistKey.Columns[i]) {
				same = false
				break
			}
		}
		if same {
			logFunc("表 %s 分布键无需调整: (%s)", tableName, strings.Join(newDistKey, ", "))
			return false, nil
		}
	}

	// 4. 生成并执行 ALTER TABLE
	alterSQL := GenerateAlterDistributionKeySQL(tableName, schemaName, newDistKey)
	logFunc("调整表 %s 分布键: (%s) → (%s)", tableName,
		strings.Join(currentDistKey.Columns, ", "), strings.Join(newDistKey, ", "))

	ctx := context.Background()
	_, err = pool.Exec(ctx, alterSQL)
	if err != nil {
		return false, fmt.Errorf("执行分布键调整失败: %w", err)
	}

	logFunc("表 %s 分布键调整成功", tableName)
	return true, nil
}
