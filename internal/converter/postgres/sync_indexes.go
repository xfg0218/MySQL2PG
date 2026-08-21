package postgres

import (
	"fmt"
	"hash/fnv"
	"strings"

	"github.com/yourusername/mysql2pg/internal/mysql"
)

// ConvertIndexDDL 将MySQL索引信息转换为PostgreSQL索引DDL。
// 返回 PG DDL、降级/跳过告警列表与错误；DDL 为空字符串表示该索引被跳过
// （调用方仅记日志不视为失败）。tableName 参数已移除（P3-07：
// 从未使用，表名始终取自 index.Table）。
//
// 语义处理（P2-18/P2-19/P2-01）：
//   - FULLTEXT：按普通 B-tree 索引创建（保留索引可用性），全文检索语义未迁移，记入告警
//   - SPATIAL：跳过创建（空间索引需 PostGIS 的 GIST 索引），记入告警
//   - 函数索引部件：跳过创建（表达式索引需人工还原表达式），记入告警
//   - 前缀索引：前缀长度丢弃、按整列建索引，记入告警
//   - 降序索引：输出列级 DESC
//   - USING HASH：映射为 PG 的 USING HASH
func ConvertIndexDDL(index mysql.IndexInfo, lowercaseColumns bool, columnNamesMap map[string]string) (string, []string, error) {
	// 检查索引名称是否有效
	if index.Name == "" {
		return "", nil, fmt.Errorf("索引名称为空，表：%s", index.Table)
	}

	// 检查表名是否有效
	if index.Table == "" {
		return "", nil, fmt.Errorf("索引所属表名为空，索引：%s", index.Name)
	}

	var warnings []string

	// P2-01：函数索引部件的表达式未被采集，无法还原为 PG 表达式索引，跳过创建
	if index.IsFunctional {
		warnings = append(warnings, "包含函数索引部件，已跳过创建；PostgreSQL 表达式索引需人工还原表达式后创建")
		return "", warnings, nil
	}

	// P2-18 相关：SPATIAL 索引在 PG 需 PostGIS 的 GIST 索引，按普通索引创建无意义，跳过
	if index.IndexType == "SPATIAL" {
		warnings = append(warnings, "SPATIAL 索引已跳过创建；需安装 PostGIS 并人工创建 GIST 索引")
		return "", warnings, nil
	}

	var uniqueClause string
	if index.IsUnique {
		uniqueClause = "UNIQUE "
	}

	// 为列名添加双引号，保持大小写一致
	var quotedColumns []string
	var prefixColumns []string
	for i, column := range index.Columns {
		// 处理pri_key特殊情况
		if strings.ToLower(column) == "pri_key" {
			continue
		}

		// 检查列名是否有效
		if column == "" {
			return "", warnings, fmt.Errorf("索引列名为空，索引：%s，表：%s", index.Name, index.Table)
		}

		colName := column
		// 使用列名映射获取转换后的列名
		if convertedColumn, ok := columnNamesMap[column]; ok {
			colName = convertedColumn
			// 移除双引号，因为后面会重新添加
			colName = strings.Trim(colName, `"`)
		}

		// 处理列名大小写
		if lowercaseColumns {
			colName = strings.ToLower(colName)
		}

		colExpr := fmt.Sprintf(`"%s"`, colName)
		// P2-19：降序索引部件输出列级 DESC
		if i < len(index.ColumnDescs) && index.ColumnDescs[i] {
			colExpr += " DESC"
		}
		quotedColumns = append(quotedColumns, colExpr)

		// P2-19：前缀长度无 PG 等价物，记录后按整列建索引
		if i < len(index.ColumnSubParts) && index.ColumnSubParts[i] > 0 {
			prefixColumns = append(prefixColumns, fmt.Sprintf("%s(%d)", column, index.ColumnSubParts[i]))
		}
	}

	if len(prefixColumns) > 0 {
		warnings = append(warnings, fmt.Sprintf("前缀索引列 %s 的前缀长度已丢弃，转为整列索引", strings.Join(prefixColumns, ", ")))
	}

	// P2-18：FULLTEXT 按普通 B-tree 索引创建（用户选择保留索引可用性），
	// 全文检索语义未迁移，需人工决策 GIN(to_tsvector) 方案
	if index.IndexType == "FULLTEXT" {
		warnings = append(warnings, "FULLTEXT 索引已转为普通 B-tree 索引，全文检索语义未迁移；如需全文检索请人工创建 GIN(to_tsvector(...)) 索引")
	}

	// 如果没有有效的列名，则跳过这个索引的创建，这通常是因为索引只包含pri_key，而PostgreSQL会自动为主键创建索引
	if len(quotedColumns) == 0 {
		return "", warnings, nil
	}

	columns := strings.Join(quotedColumns, ", ")

	// 将索引名转换为小写，以匹配PostgreSQL的默认行为
	// 为了避免不同表有相同索引名导致的冲突（PostgreSQL中索引名在Schema级别必须唯一），
	// 我们将表名作为前缀添加到索引名中
	lowercaseIndexName := strings.ToLower(fmt.Sprintf("%s_%s", index.Table, index.Name))

	// 截断索引名以符合PostgreSQL的长度限制（63字节）
	// P1-17：截断后拼接原名哈希后缀，避免仅尾部不同的长索引名截断后撞名
	// （撞名会导致第二个索引被 IF NOT EXISTS 静默跳过）
	if len(lowercaseIndexName) > 63 {
		sum := fnv.New32a()
		sum.Write([]byte(lowercaseIndexName))
		lowercaseIndexName = fmt.Sprintf("%s_%08x", lowercaseIndexName[:54], sum.Sum32())
	}

	// P2-19：USING HASH 映射为 PG 的 HASH 索引访问方法
	var usingClause string
	if index.IndexType == "HASH" {
		usingClause = "USING HASH "
	}

	// 为表名和索引名添加双引号，以处理特殊字符和关键字
	pgDDL := fmt.Sprintf("CREATE %sINDEX IF NOT EXISTS \"%s\" ON \"%s\" %s(%s);",
		uniqueClause, lowercaseIndexName, index.Table, usingClause, columns)

	return pgDDL, warnings, nil
}
