package postgres

import (
	"fmt"
	"strings"

	"github.com/yourusername/mysql2pg/internal/mysql"
)

// ConvertIndexDDL 将MySQL索引DDL转换为PostgreSQL索引DDL
func ConvertIndexDDL(_ string, index mysql.IndexInfo, lowercaseColumns bool) (string, error) {
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
	lowercaseIndexName := strings.ToLower(index.Name)

	// 为表名和索引名添加双引号，以处理特殊字符和关键字
	// 使用index.Table而不是传入的tableName参数，确保索引创建在正确的表上
	pgDDL := fmt.Sprintf("CREATE %sINDEX IF NOT EXISTS \"%s\" ON \"%s\" (%s);",
		uniqueClause, lowercaseIndexName, index.Table, columns)

	return pgDDL, nil
}
