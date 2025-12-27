package converter

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/yourusername/mysql2pg/internal/mysql"
)

// ConvertTableDDL 转换MySQL表DDL到PostgreSQL
func ConvertTableDDL(mysqlDDL string, lowercaseColumns bool) (string, error) {
	// 移除MySQL反引号
	mysqlDDL = strings.ReplaceAll(mysqlDDL, "`", "")

	// 解析表名
	tableNameStart := strings.Index(strings.ToUpper(mysqlDDL), "CREATE TABLE")
	if tableNameStart == -1 {
		return "", fmt.Errorf("无效的CREATE TABLE语句")
	}

	tableNameStart += len("CREATE TABLE")
	tableNameEnd := strings.Index(mysqlDDL[tableNameStart:], "(")
	if tableNameEnd == -1 {
		return "", fmt.Errorf("无效的CREATE TABLE语句，缺少左括号")
	}

	tableName := strings.TrimSpace(mysqlDDL[tableNameStart : tableNameStart+tableNameEnd])

	// 提取列定义部分
	columnsStart := tableNameStart + tableNameEnd + 1
	columnsEnd := strings.LastIndex(mysqlDDL, ")")
	if columnsEnd == -1 {
		return "", fmt.Errorf("无效的CREATE TABLE语句，缺少右括号")
	}

	// 检查是否有引擎和字符集设置
	// 在MySQL中，表定义的结尾通常会有 ) engine=innodb default charset=utf8mb4 collate=utf8mb4_bin;
	// 我们需要确保在提取列定义时，正确地处理这部分内容
	columnsDefinition := mysqlDDL[columnsStart:columnsEnd]

	// 移除任何可能的表级别的引擎和字符集设置
	// 这些设置可能会被错误地包含在列定义中
	columnsDefinition = strings.ReplaceAll(columnsDefinition, " engine=innodb", "")
	columnsDefinition = strings.ReplaceAll(columnsDefinition, " ENGINE=InnoDB", "")
	columnsDefinition = strings.ReplaceAll(columnsDefinition, " default charset=utf8mb4", "")
	columnsDefinition = strings.ReplaceAll(columnsDefinition, " DEFAULT CHARSET=utf8mb4", "")
	columnsDefinition = strings.ReplaceAll(columnsDefinition, " collate=utf8mb4_bin", "")
	columnsDefinition = strings.ReplaceAll(columnsDefinition, " COLLATE=utf8mb4_bin", "")
	columnsDefinition = strings.ReplaceAll(columnsDefinition, " collate=utf8mb4_unicode_ci", "")
	columnsDefinition = strings.ReplaceAll(columnsDefinition, " COLLATE=utf8mb4_unicode_ci", "")
	columnsDefinition = strings.ReplaceAll(columnsDefinition, " collate=utf8mb4_general_ci", "")
	columnsDefinition = strings.ReplaceAll(columnsDefinition, " COLLATE=utf8mb4_general_ci", "")

	// 按行分割列定义
	lines := strings.Split(columnsDefinition, "\n")

	// 存储列定义和主键信息
	var columnDefinitions []string
	var primaryKeyColumn string
	var re *regexp.Regexp

	// 存储列名映射，用于保持大小写一致
	columnNames := make(map[string]string)

	// 处理每一行
	for _, line := range lines {
		trimmedLine := strings.TrimSpace(line)

		// 跳过空行、索引定义和外键约束
		if trimmedLine == "" ||
			strings.HasPrefix(trimmedLine, "KEY ") ||
			strings.HasPrefix(trimmedLine, "INDEX ") ||
			strings.HasPrefix(trimmedLine, "UNIQUE KEY ") ||
			strings.HasPrefix(trimmedLine, "UNIQUE INDEX ") ||
			strings.Contains(trimmedLine, "FOREIGN KEY") ||
			strings.Contains(trimmedLine, "USING BTREE") ||
			strings.Contains(trimmedLine, "USING HASH") ||
			// 跳过表级别的引擎和字符集设置
			strings.Contains(trimmedLine, "engine=") ||
			strings.Contains(trimmedLine, "ENGINE=") ||
			strings.Contains(trimmedLine, "charset=") ||
			strings.Contains(trimmedLine, "CHARSET=") ||
			strings.Contains(trimmedLine, "collate=") ||
			strings.Contains(trimmedLine, "COLLATE=") ||
			// 跳过只有右括号的行
			trimmedLine == ")" {
			continue
		}

		// 处理PRIMARY KEY
		if strings.HasPrefix(strings.ToUpper(trimmedLine), "PRIMARY KEY") {
			// 提取主键列名
			pkMatch := regexp.MustCompile(`PRIMARY KEY\s*\(\s*(\w+)\s*\)`).FindStringSubmatch(trimmedLine)
			if len(pkMatch) > 1 {
				primaryKeyColumn = pkMatch[1]
			}
			continue
		}

		// 移除字符集和排序规则声明
		trimmedLine = strings.ReplaceAll(trimmedLine, " COLLATE utf8mb4_unicode_ci", "")
		trimmedLine = strings.ReplaceAll(trimmedLine, " COLLATE utf8_unicode_ci", "")
		trimmedLine = strings.ReplaceAll(trimmedLine, " COLLATE utf8_general_ci", "")
		trimmedLine = strings.ReplaceAll(trimmedLine, " COLLATE utf8mb4_bin", "")
		trimmedLine = strings.ReplaceAll(trimmedLine, " COLLATE utf8_bin", "")
		trimmedLine = strings.ReplaceAll(trimmedLine, " COLLATE utf32_bin", "")
		trimmedLine = strings.ReplaceAll(trimmedLine, " COLLATE latin1_bin", "")
		trimmedLine = strings.ReplaceAll(trimmedLine, " COLLATE latin1_swedish_ci", "")
		trimmedLine = strings.ReplaceAll(trimmedLine, " character set utf8", "")
		trimmedLine = strings.ReplaceAll(trimmedLine, " CHARACTER SET utf8", "")
		trimmedLine = strings.ReplaceAll(trimmedLine, " character set latin1", "")
		trimmedLine = strings.ReplaceAll(trimmedLine, " CHARACTER SET latin1", "")
		trimmedLine = strings.ReplaceAll(trimmedLine, " character set utf16", "")
		trimmedLine = strings.ReplaceAll(trimmedLine, " CHARACTER SET utf16", "")
		trimmedLine = strings.ReplaceAll(trimmedLine, " charset=latin1", "")
		trimmedLine = strings.ReplaceAll(trimmedLine, " CHARSET=latin1", "")
		trimmedLine = strings.ReplaceAll(trimmedLine, " charset=utf16", "")
		trimmedLine = strings.ReplaceAll(trimmedLine, " CHARSET=utf16", "")

		// 移除ON UPDATE CURRENT_TIMESTAMP
		trimmedLine = strings.ReplaceAll(trimmedLine, " ON UPDATE CURRENT_TIMESTAMP", "")

		// 移除unsigned关键字
		trimmedLine = strings.ReplaceAll(trimmedLine, " unsigned", "")
		trimmedLine = strings.ReplaceAll(trimmedLine, " UNSIGNED", "")

		// 处理列注释（PostgreSQL的注释语法不同，暂时移除注释）
		trimmedLine = strings.Split(trimmedLine, "COMMENT")[0]
		trimmedLine = strings.TrimSpace(trimmedLine)

		// 分离列名和类型定义
		// 使用正则表达式来处理包含空格的列名
		var columnName string
		var typeDefinition string

		// 检查是否有引号包围的列名
		if strings.HasPrefix(trimmedLine, `"`) {
			// 找到第一个引号结束的位置
			quoteEnd := strings.Index(trimmedLine[1:], `"`)
			if quoteEnd != -1 {
				columnName = trimmedLine[1 : quoteEnd+1]
				typeDefinition = strings.TrimSpace(trimmedLine[quoteEnd+2:])
			}
		} else {
			// 没有引号包围的列名，使用正则表达式来解析
			// 匹配列名（可能包含空格）和类型定义
			// 假设类型定义以以下关键字之一开头：varchar, char, int, bigint, decimal, timestamp, date, time, text, blob, json, enum
			re := regexp.MustCompile(`^(.+?)\s+(varchar|char|int|bigint|decimal|timestamp|date|time|text|blob|json|enum|boolean|smallint|double|float|real|bytea|serial|bigserial|integer|smallserial|jsonb|uuid|inet|cidr|macaddr|xml|bit|money|interval|numeric)(.*)$`)
			match := re.FindStringSubmatch(trimmedLine)
			if len(match) == 4 {
				columnName = strings.TrimSpace(match[1])
				typeDefinition = match[2] + match[3]
			} else {
				// 如果正则表达式匹配失败，使用传统的字段分割作为后备方案
				parts := strings.Fields(trimmedLine)
				if len(parts) < 2 {
					continue
				}
				columnName = parts[0]
				typeDefinition = strings.Join(parts[1:], " ")
			}
		}

		// 跳过没有类型定义的列
		if typeDefinition == "" {
			continue
		}

		// 存储列名，保持原始大小写
		columnNames[strings.ToLower(columnName)] = columnName

		// 处理自增主键（在替换数据类型之前）
		if strings.Contains(typeDefinition, "AUTO_INCREMENT") {
			if strings.Contains(strings.ToLower(typeDefinition), "bigint") {
				typeDefinition = strings.ReplaceAll(typeDefinition, "AUTO_INCREMENT", "")
				typeDefinition = strings.ReplaceAll(typeDefinition, "bigint(20)", "BIGSERIAL")
				typeDefinition = strings.ReplaceAll(typeDefinition, "BIGINT(20)", "BIGSERIAL")
				typeDefinition = strings.ReplaceAll(typeDefinition, "bigint(11)", "BIGSERIAL")
				typeDefinition = strings.ReplaceAll(typeDefinition, "BIGINT(11)", "BIGSERIAL")
				typeDefinition = strings.ReplaceAll(typeDefinition, "bigint(32)", "BIGSERIAL")
				typeDefinition = strings.ReplaceAll(typeDefinition, "BIGINT(32)", "BIGSERIAL")
				typeDefinition = strings.ReplaceAll(typeDefinition, "bigint(24)", "BIGSERIAL")
				typeDefinition = strings.ReplaceAll(typeDefinition, "BIGINT(24)", "BIGSERIAL")
				typeDefinition = strings.ReplaceAll(typeDefinition, "bigint(128)", "BIGSERIAL")
				typeDefinition = strings.ReplaceAll(typeDefinition, "BIGINT(128)", "BIGSERIAL")
				typeDefinition = strings.ReplaceAll(typeDefinition, "BIGINT", "BIGSERIAL")
				typeDefinition = strings.ReplaceAll(typeDefinition, "bigint", "BIGSERIAL")
			} else {
				typeDefinition = strings.ReplaceAll(typeDefinition, "AUTO_INCREMENT", "")
				typeDefinition = strings.ReplaceAll(typeDefinition, "int(11)", "SERIAL")
				typeDefinition = strings.ReplaceAll(typeDefinition, "INT(11)", "SERIAL")
				typeDefinition = strings.ReplaceAll(typeDefinition, "int(4)", "SERIAL")
				typeDefinition = strings.ReplaceAll(typeDefinition, "INT(4)", "SERIAL")
				typeDefinition = strings.ReplaceAll(typeDefinition, "int(10)", "SERIAL")
				typeDefinition = strings.ReplaceAll(typeDefinition, "INT(10)", "SERIAL")
				typeDefinition = strings.ReplaceAll(typeDefinition, "int(32)", "SERIAL")
				typeDefinition = strings.ReplaceAll(typeDefinition, "INT(32)", "SERIAL")
				typeDefinition = strings.ReplaceAll(typeDefinition, "int(25)", "SERIAL")
				typeDefinition = strings.ReplaceAll(typeDefinition, "INT(25)", "SERIAL")
				typeDefinition = strings.ReplaceAll(typeDefinition, "INTEGER", "SERIAL")
				typeDefinition = strings.ReplaceAll(typeDefinition, "int", "SERIAL")
			}
		}

		// 先转为小写，确保类型映射能正确工作
		lowerTypeDef := strings.ToLower(typeDefinition)

		// 处理字符集和排序规则
		lowerTypeDef = strings.ReplaceAll(lowerTypeDef, " character set utf8mb4", "")
		lowerTypeDef = strings.ReplaceAll(lowerTypeDef, " character set utf8", "")
		lowerTypeDef = strings.ReplaceAll(lowerTypeDef, " character set utf32", "")
		lowerTypeDef = strings.ReplaceAll(lowerTypeDef, " collate utf8mb4_unicode_ci", "")
		lowerTypeDef = strings.ReplaceAll(lowerTypeDef, " collate utf8mb4_general_ci", "")
		lowerTypeDef = strings.ReplaceAll(lowerTypeDef, " collate utf8_unicode_ci", "")
		lowerTypeDef = strings.ReplaceAll(lowerTypeDef, " collate utf8_general_ci", "")
		lowerTypeDef = strings.ReplaceAll(lowerTypeDef, " collate utf32_bin", "")
		lowerTypeDef = strings.ReplaceAll(lowerTypeDef, " CHARACTER SET utf8mb4", "")
		lowerTypeDef = strings.ReplaceAll(lowerTypeDef, " CHARACTER SET utf8", "")
		lowerTypeDef = strings.ReplaceAll(lowerTypeDef, " CHARACTER SET utf32", "")
		lowerTypeDef = strings.ReplaceAll(lowerTypeDef, " COLLATE utf8mb4_unicode_ci", "")
		lowerTypeDef = strings.ReplaceAll(lowerTypeDef, " COLLATE utf8mb4_general_ci", "")
		lowerTypeDef = strings.ReplaceAll(lowerTypeDef, " COLLATE utf8_unicode_ci", "")
		lowerTypeDef = strings.ReplaceAll(lowerTypeDef, " COLLATE utf8_general_ci", "")
		lowerTypeDef = strings.ReplaceAll(lowerTypeDef, " COLLATE utf32_bin", "")

		// 处理默认值中的等号语法错误
		// 移除默认值中的多余等号
		re = regexp.MustCompile(`default\s*=\s*`)
		lowerTypeDef = re.ReplaceAllString(lowerTypeDef, "default ")

		// 处理默认值中的current_timestamp(3)(3)错误
		re = regexp.MustCompile(`current_timestamp\(\d+\)\(\d+\)`)
		lowerTypeDef = re.ReplaceAllStringFunc(lowerTypeDef, func(m string) string {
			// 提取第一个括号中的数字
			re = regexp.MustCompile(`current_timestamp\((\d+)\)`)
			match := re.FindStringSubmatch(m)
			if len(match) > 1 {
				return "CURRENT_TIMESTAMP(" + match[1] + ")"
			}
			return "CURRENT_TIMESTAMP"
		})

		// 移除mb4后缀（如varchar(50)mb4、longtextmb4、char(1)mb4等）
		re = regexp.MustCompile(`(?i)(text|longtext|mediumtext|tinytext|blob|longblob|mediumblob|tinyblob|binary|varbinary|varchar\(\d+\)|char\(\d+\))mb4`)
		lowerTypeDef = re.ReplaceAllString(lowerTypeDef, "$1")

		// 移除unsigned关键字
		lowerTypeDef = strings.ReplaceAll(lowerTypeDef, " unsigned", "")

		// 移除zerofill关键字
		lowerTypeDef = strings.ReplaceAll(lowerTypeDef, " zerofill", "")

		// 替换数据类型
		// 使用一个映射表来处理所有数据类型转换，确保一次性完成所有替换
		typeMap := map[string]string{
			// 整数类型
			"bigint(20)":      "BIGINT",
			"bigint(11)":      "BIGINT",
			"bigint(32)":      "BIGINT",
			"bigint(24)":      "BIGINT",
			"bigint(128)":     "BIGINT",
			"bigint(10)":      "BIGINT",
			"bigint(19)":      "BIGINT",
			"bigint":          "BIGINT",
			"biginteger(20)":  "BIGINT",
			"biginteger(255)": "BIGINT",
			"biginteger(19)":  "BIGINT",
			"biginteger":      "BIGINT",
			"int(11)":         "INTEGER",
			"int(4)":          "INTEGER",
			"int(2)":          "INTEGER",
			"int(5)":          "INTEGER",
			"int(10)":         "INTEGER",
			"int(20)":         "INTEGER",
			"int(255)":        "INTEGER",
			"int(32)":         "INTEGER",
			"int(8)":          "INTEGER",
			"int(60)":         "INTEGER",
			"int(3)":          "INTEGER",
			"int(25)":         "INTEGER",
			"int(22)":         "INTEGER",
			"int":             "INTEGER",
			"integer(4)":      "INTEGER",
			"integer(2)":      "INTEGER",
			"integer(10)":     "INTEGER",
			"integer(20)":     "INTEGER",
			"integer(11)":     "INTEGER",
			"integer(22)":     "INTEGER",
			"integer":         "INTEGER",
			"smallinteger(1)": "SMALLINT",
			"smallinteger":    "SMALLINT",
			"tinyinteger(1)":  "BOOLEAN",
			"tinyinteger":     "SMALLINT",
			"tinyint(1)":      "BOOLEAN",
			"tinyint(4)":      "SMALLINT",
			"tinyint(255)":    "SMALLINT",
			"tinyint":         "SMALLINT",
			"smallint(6)":     "SMALLINT",
			"smallint(1)":     "SMALLINT",
			"smallint":        "SMALLINT",
			"mediumint(9)":    "INTEGER",
			"mediumint":       "INTEGER",
			// 浮点数类型
			"decimal(10,0)":    "INTEGER",
			"decimal(10,2)":    "DECIMAL(10,2)",
			"decimal":          "DECIMAL",
			"double":           "DOUBLE PRECISION",
			"double precision": "DOUBLE PRECISION",
			"float":            "REAL",
			// 字符串类型
			"char(1)":      "CHAR(1)",
			"varchar(255)": "VARCHAR(255)",
			"varchar(256)": "VARCHAR(256)",
			"varchar(64)":  "VARCHAR(64)",
			"varchar(20)":  "VARCHAR(20)",
			"varchar(100)": "VARCHAR(100)",
			"varchar(50)":  "VARCHAR(50)",
			"varchar(128)": "VARCHAR(128)",
			"varchar(500)": "VARCHAR(500)",
			"varchar(200)": "VARCHAR(200)",
			"varchar":      "VARCHAR",
			"text":         "TEXT",
			"longtext":     "TEXT",
			"mediumtext":   "TEXT",
			"tinytext":     "TEXT",
			// 二进制类型
			"blob":          "BYTEA",
			"longblob":      "BYTEA",
			"mediumblob":    "BYTEA",
			"tinyblob":      "BYTEA",
			"binary":        "BYTEA",
			"varbinary":     "BYTEA",
			"varbinary(64)": "BYTEA",
			// 日期时间类型
			"datetime":     "TIMESTAMP",
			"datetime(6)":  "TIMESTAMP",
			"datetime(3)":  "TIMESTAMP",
			"timestamp":    "TIMESTAMP",
			"timestamp(6)": "TIMESTAMP",
			"timestamp(3)": "TIMESTAMP",
			"date":         "DATE",
			"time":         "TIME",
			"year":         "INTEGER",
			// 其他类型
			"json":  "JSONB",
			"jsonb": "JSONB",
			// 处理ENUM类型，转换为VARCHAR
			"enum": "VARCHAR(255)",
		}

		// 应用类型映射
		for mysqlType, pgType := range typeMap {
			if strings.Contains(lowerTypeDef, mysqlType) {
				// 使用正则表达式确保只替换完整的类型名称
				re = regexp.MustCompile(`\b` + regexp.QuoteMeta(mysqlType) + `\b`)
				lowerTypeDef = re.ReplaceAllString(lowerTypeDef, pgType)
			}
		}

		// 处理ENUM类型的特殊情况（带枚举值的情况）
		re = regexp.MustCompile(`(?i)enum\(([^)]+)\)`)
		lowerTypeDef = re.ReplaceAllString(lowerTypeDef, "VARCHAR(255)")

		// 处理VARCHAR(255)后面的枚举值列表
		re = regexp.MustCompile(`(?i)varchar\(\d+\)\(([^)]+)\)`)
		lowerTypeDef = re.ReplaceAllString(lowerTypeDef, "VARCHAR(255)")

		// 处理VARCHAR(0)长度错误，将其转换为VARCHAR(1)
		re = regexp.MustCompile(`(?i)varchar\(0\)`)
		lowerTypeDef = re.ReplaceAllString(lowerTypeDef, "VARCHAR(1)")

		// 移除double precision后的精度指定
		re = regexp.MustCompile(`(?i)double precision\(\d+,\d+\)`)
		lowerTypeDef = re.ReplaceAllString(lowerTypeDef, "DOUBLE PRECISION")

		// 处理所有整数类型后的精度指定
		re = regexp.MustCompile(`(?i)(bigint|integer|smallint|int)\(\d+\)`)
		lowerTypeDef = re.ReplaceAllStringFunc(lowerTypeDef, func(m string) string {
			return strings.ToUpper(strings.Split(m, "(")[0])
		})

		// 处理bigserial后的精度指定
		re = regexp.MustCompile(`(?i)bigserial\(\d+\)`)
		lowerTypeDef = re.ReplaceAllString(lowerTypeDef, "BIGSERIAL")

		// 处理serial后的精度指定
		re = regexp.MustCompile(`(?i)serial\(\d+\)`)
		lowerTypeDef = re.ReplaceAllString(lowerTypeDef, "SERIAL")

		// 移除bytea类型后的修饰符（PostgreSQL不允许bytea有修饰符）
		re = regexp.MustCompile(`(?i)bytea\(\d+\)`)
		lowerTypeDef = re.ReplaceAllString(lowerTypeDef, "BYTEA")

		// 移除默认值为NULL的显式声明
		lowerTypeDef = strings.ReplaceAll(lowerTypeDef, " default null", "")

		// 清理多余的逗号
		if strings.HasSuffix(lowerTypeDef, ",") {
			lowerTypeDef = strings.TrimSuffix(lowerTypeDef, ",")
		}

		// 确保所有类型名称大写
		re = regexp.MustCompile(`(?i)\b(bigint|integer|smallint|int|bigserial|serial|boolean|text|bytea|timestamp|date|time|decimal|double precision|real)\b`)
		typeDefinition = re.ReplaceAllStringFunc(lowerTypeDef, strings.ToUpper)

		// 处理列名大小写
		if lowercaseColumns {
			columnName = strings.ToLower(columnName)
		}

		// 重新组合列定义
		newColumnDefinition := fmt.Sprintf(`"%s" %s`, columnName, typeDefinition)
		columnDefinitions = append(columnDefinitions, newColumnDefinition)
	}

	// 构建PostgreSQL DDL
	var result strings.Builder
	result.WriteString(fmt.Sprintf(`CREATE TABLE "%s" (`, tableName))

	// 添加列定义
	for i, columnDef := range columnDefinitions {
		if i > 0 {
			result.WriteString(",")
		}
		result.WriteString(fmt.Sprintf(`
  %s`, columnDef))
	}

	// 添加主键约束，使用正确大小写的列名
	if primaryKeyColumn != "" {
		// 查找原始大小写的列名
		if originalColumnName, ok := columnNames[strings.ToLower(primaryKeyColumn)]; ok {
			primaryKeyColumn = originalColumnName
			// 如果需要转小写，确保主键列名也是小写
			if lowercaseColumns {
				primaryKeyColumn = strings.ToLower(primaryKeyColumn)
			}
		}
		result.WriteString(fmt.Sprintf(`,
  PRIMARY KEY ("%s")`, primaryKeyColumn))
	}

	result.WriteString(`
)`)

	finalDDL := result.String()

	// 检查生成的DDL是否有效
	if !strings.Contains(finalDDL, "CREATE TABLE") || !strings.Contains(finalDDL, "(") || !strings.Contains(finalDDL, ")") {
		return "", fmt.Errorf("生成的DDL无效: %s", finalDDL)
	}

	return finalDDL, nil
}

// ConvertIndexDDL 将MySQL索引DDL转换为PostgreSQL索引DDL
func ConvertIndexDDL(tableName string, index mysql.IndexInfo, lowercaseColumns bool) (string, error) {
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

		// 处理列名大小写
		if lowercaseColumns {
			column = strings.ToLower(column)
		}

		quotedColumns = append(quotedColumns, fmt.Sprintf(`"%s"`, column))
	}

	// 如果没有有效的列名，则跳过这个索引的创建
	// 这通常是因为索引只包含pri_key，而PostgreSQL会自动为主键创建索引
	if len(quotedColumns) == 0 {
		return "", nil
	}

	columns := strings.Join(quotedColumns, ", ")

	// 为表名和索引名添加双引号，以处理特殊字符和关键字
	pgDDL := fmt.Sprintf("CREATE %sINDEX IF NOT EXISTS \"%s\" ON \"%s\" (%s);",
		uniqueClause, index.Name, tableName, columns)

	return pgDDL, nil
}

// ConvertFunctionDDL 将MySQL函数转换为PostgreSQL函数
func ConvertFunctionDDL(mysqlFunc mysql.FunctionInfo) (string, error) {
	// 这里需要根据具体的函数语法进行转换
	// 这是一个简化版本，实际转换可能需要更复杂的逻辑
	pgDDL := fmt.Sprintf(`
	CREATE OR REPLACE FUNCTION %s()
	RETURNS VOID AS $$
	%s
	$$ LANGUAGE plpgsql;
	`, mysqlFunc.Name, mysqlFunc.DDL)

	return pgDDL, nil
}

// ConvertUserDDL 将MySQL用户权限转换为PostgreSQL用户权限
func ConvertUserDDL(user mysql.UserInfo) ([]string, error) {
	var pgDDLs []string

	// 提取用户名（去掉主机部分）
	userParts := strings.Split(user.Name, "@")
	if len(userParts) != 2 {
		return nil, fmt.Errorf("无效的用户名格式: %s", user.Name)
	}
	userName := userParts[0]

	// 创建用户 - PostgreSQL 不支持 IF NOT EXISTS，所以先检查用户是否存在
	pgDDLs = append(pgDDLs, fmt.Sprintf("DO $$ BEGIN IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = '%s') THEN CREATE USER %s; END IF; END $$;", userName, userName))

	// 转换权限
	for _, grant := range user.Grants {
		// 处理数据库级别的权限
		if strings.Contains(grant, "ALL PRIVILEGES ON") {
			// 提取数据库名
			dbStart := strings.Index(grant, "ON ") + 3
			dbEnd := strings.Index(grant[dbStart:], " TO")
			if dbEnd == -1 {
				continue
			}
			dbSpec := grant[dbStart : dbStart+dbEnd]

			// 处理通配符数据库
			if dbSpec == "*.*" {
				pgDDLs = append(pgDDLs, fmt.Sprintf("GRANT ALL PRIVILEGES ON DATABASE postgres TO %s;", userName))
				pgDDLs = append(pgDDLs, fmt.Sprintf("GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO %s;", userName))
				pgDDLs = append(pgDDLs, fmt.Sprintf("GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO %s;", userName))
			} else {
				// 处理特定数据库
				dbName := strings.Split(dbSpec, ".")[0]
				pgDDLs = append(pgDDLs, fmt.Sprintf("GRANT ALL PRIVILEGES ON DATABASE %s TO %s;", dbName, userName))
				pgDDLs = append(pgDDLs, fmt.Sprintf("GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO %s;", userName))
				pgDDLs = append(pgDDLs, fmt.Sprintf("GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO %s;", userName))
			}
		} else if strings.Contains(grant, "SELECT ON") {
			// 处理SELECT权限
			pgDDLs = append(pgDDLs, fmt.Sprintf("GRANT SELECT ON ALL TABLES IN SCHEMA public TO %s;", userName))
		} else if strings.Contains(grant, "INSERT ON") {
			// 处理INSERT权限
			pgDDLs = append(pgDDLs, fmt.Sprintf("GRANT INSERT ON ALL TABLES IN SCHEMA public TO %s;", userName))
		} else if strings.Contains(grant, "UPDATE ON") {
			// 处理UPDATE权限
			pgDDLs = append(pgDDLs, fmt.Sprintf("GRANT UPDATE ON ALL TABLES IN SCHEMA public TO %s;", userName))
		} else if strings.Contains(grant, "DELETE ON") {
			// 处理DELETE权限
			pgDDLs = append(pgDDLs, fmt.Sprintf("GRANT DELETE ON ALL TABLES IN SCHEMA public TO %s;", userName))
		}
	}

	return pgDDLs, nil
}

// ConvertTablePrivilegeDDL 将MySQL表权限转换为PostgreSQL表权限
func ConvertTablePrivilegeDDL(tablePriv mysql.TablePrivInfo) ([]string, error) {
	var pgDDLs []string

	// 提取用户名（处理带主机和不带主机的情况）
	var userName string
	userParts := strings.Split(tablePriv.User, "@")
	if len(userParts) == 2 {
		userName = userParts[0]
	} else if len(userParts) == 1 {
		// 没有主机部分的情况
		userName = userParts[0]
	} else {
		return nil, fmt.Errorf("无效的用户名格式: %s", tablePriv.User)
	}

	// 处理表级别的权限
	tableName := tablePriv.TableName

	// 转换权限（忽略大小写）
	tablePrivStr := strings.ToUpper(tablePriv.TablePriv)

	if strings.Contains(tablePrivStr, "SELECT") {
		pgDDLs = append(pgDDLs, fmt.Sprintf("GRANT SELECT ON \"%s\" TO \"%s\";", tableName, userName))
	}
	if strings.Contains(tablePrivStr, "INSERT") {
		pgDDLs = append(pgDDLs, fmt.Sprintf("GRANT INSERT ON \"%s\" TO \"%s\";", tableName, userName))
	}
	if strings.Contains(tablePrivStr, "UPDATE") {
		pgDDLs = append(pgDDLs, fmt.Sprintf("GRANT UPDATE ON \"%s\" TO \"%s\";", tableName, userName))
	}
	if strings.Contains(tablePrivStr, "DELETE") {
		pgDDLs = append(pgDDLs, fmt.Sprintf("GRANT DELETE ON \"%s\" TO \"%s\";", tableName, userName))
	}
	if strings.Contains(tablePrivStr, "ALL PRIVILEGES") {
		pgDDLs = append(pgDDLs, fmt.Sprintf("GRANT ALL PRIVILEGES ON \"%s\" TO \"%s\";", tableName, userName))
	}

	return pgDDLs, nil
}
