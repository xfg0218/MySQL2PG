package postgres

import (
	"fmt"
	"strings"

	"github.com/yourusername/mysql2pg/internal/mysql"
)

// ConvertIndexDDL 将MySQL索引DDL转换为PostgreSQL索引DDL
func ConvertIndexDDL(tableName string, index mysql.IndexInfo, lowercaseColumns bool, columnNamesMap map[string]string) (string, error) {
	// 检查索引名称是否有效
	if index.Name == "" {
		return "", fmt.Errorf("索引名称为空，表：%s", index.Table)
	}

	// 检查表名是否有效
	if index.Table == "" {
		return "", fmt.Errorf("索引所属表名为空，索引：%s", index.Name)
	}

	var uniqueClause string
	if index.IsUnique {
		uniqueClause = "UNIQUE "
	}

	// 为列名添加双引号，保持大小写一致
	var quotedColumns []string
	for _, column := range index.Columns {
		// 处理pri_key特殊情况
		if strings.ToLower(column) == "pri_key" {
			continue
		}

		// 检查列名是否有效
		if column == "" {
			return "", fmt.Errorf("索引列名为空，索引：%s，表：%s", index.Name, index.Table)
		}

		// 使用列名映射获取转换后的列名
		if convertedColumn, ok := columnNamesMap[column]; ok {
			column = convertedColumn
			// 移除双引号，因为后面会重新添加
			column = strings.Trim(column, `"`)
		}

		// 处理列名大小写
		if lowercaseColumns {
			column = strings.ToLower(column)
		}

		quotedColumns = append(quotedColumns, fmt.Sprintf(`"%s"`, column))
	}

	// 如果没有有效的列名，则跳过这个索引的创建，这通常是因为索引只包含pri_key，而PostgreSQL会自动为主键创建索引
	if len(quotedColumns) == 0 {
		return "", nil
	}

	columns := strings.Join(quotedColumns, ", ")

	// 将索引名转换为小写，以匹配PostgreSQL的默认行为
	// 为了避免不同表有相同索引名导致的冲突（PostgreSQL中索引名在Schema级别必须唯一），
	// 我们将表名作为前缀添加到索引名中
	lowercaseIndexName := strings.ToLower(fmt.Sprintf("%s_%s", index.Table, index.Name))

	// 截断索引名以符合PostgreSQL的长度限制（63字节）
	if len(lowercaseIndexName) > 63 {
		// 保留唯一的后缀（通常包含列名信息）和表名的一部分
		// 简单的截断策略
		lowercaseIndexName = lowercaseIndexName[:63]
		// 确保截断后不会以特殊字符结尾
		lowercaseIndexName = strings.TrimRight(lowercaseIndexName, "_")
	}

	// 为表名和索引名添加双引号，以处理特殊字符和关键字
	// 使用index.Table而不是传入的tableName参数，确保索引创建在正确的表上
	pgDDL := fmt.Sprintf("CREATE %sINDEX IF NOT EXISTS \"%s\" ON \"%s\" (%s);",
		uniqueClause, lowercaseIndexName, index.Table, columns)

	return pgDDL, nil
}
