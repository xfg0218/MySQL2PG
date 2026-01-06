package postgres

import (
	"fmt"
	"regexp"
	"strings"
)

type ConvertTableDDLResult struct {
	DDL          string
	TableComment string
}

// ConvertTableDDL 转换MySQL表DDL到PostgreSQL
func ConvertTableDDL(mysqlDDL string, lowercaseColumns bool) (*ConvertTableDDLResult, error) {
	// 移除MySQL反引号
	mysqlDDL = strings.ReplaceAll(mysqlDDL, "`", "")

	// 解析表名，先检查是否是临时表
	var tableNameStart int
	var isTemporary bool

	// 检查是否是临时表
	tableNameStart = strings.Index(strings.ToUpper(mysqlDDL), "CREATE TEMPORARY TABLE")
	if tableNameStart != -1 {
		isTemporary = true
		tableNameStart += len("CREATE TEMPORARY TABLE")
	} else {
		// 不是临时表，检查普通表
		tableNameStart = strings.Index(strings.ToUpper(mysqlDDL), "CREATE TABLE")
		if tableNameStart == -1 {
			return nil, fmt.Errorf("无效的CREATE TABLE语句")
		}
		tableNameStart += len("CREATE TABLE")
	}

	tableNameEnd := strings.Index(mysqlDDL[tableNameStart:], "(")
	if tableNameEnd == -1 {
		return nil, fmt.Errorf("无效的CREATE TABLE语句，缺少左括号")
	}

	// 提取表名，处理引号包围的情况
	tableName := strings.TrimSpace(mysqlDDL[tableNameStart : tableNameStart+tableNameEnd])
	// 移除表名周围的引号
	if strings.HasPrefix(tableName, "'") && strings.HasSuffix(tableName, "'") {
		tableName = tableName[1 : len(tableName)-1]
	} else if strings.HasPrefix(tableName, `"`) && strings.HasSuffix(tableName, `"`) {
		tableName = tableName[1 : len(tableName)-1]
	}

	// 提取列定义部分
	columnsStart := tableNameStart + tableNameEnd + 1
	columnsEnd := 0

	// 提取表注释（在找到右括号之前先处理表注释，避免括号匹配问题）
	// 先提取表注释，然后再处理列定义
	tableComment := ""
	reTableComment := regexp.MustCompile(`(?i)\s+COMMENT\s*=\s*'([^']*)'`)
	tableCommentMatch := reTableComment.FindStringSubmatch(mysqlDDL)
	if tableCommentMatch != nil {
		tableComment = tableCommentMatch[1]
	}

	// 使用正则表达式来正确找到表定义的结束位置
	// 匹配 ) 后跟表级选项（ENGINE, CHARSET, COLLATE, ROW_FORMAT, COMMENT等）的模式
	// 注意：表注释中的右括号不应该被匹配，所以我们需要更精确的匹配
	reTableEnd := regexp.MustCompile(`(?i)\)\s*(?:ENGINE\s*=\s*\w+\s*)?(?:DEFAULT\s+(?:CHARSET|CHARACTER\s+SET)\s*=\w+\s*)?(?:COLLATE\s*=\w+\s*)?(?:ROW_FORMAT\s*=\w+\s*)?(?:COMMENT\s*=\s*'[^']*'\s*)?;?\s*$`)
	tableEndMatch := reTableEnd.FindStringIndex(mysqlDDL)
	if tableEndMatch != nil {
		// 找到表定义结束位置
		columnsEnd = tableEndMatch[0]
	} else {
		// 如果没有匹配到表级选项，使用原来的方法（找最后一个右括号）
		columnsEnd = strings.LastIndex(mysqlDDL, ")")
		if columnsEnd == -1 {
			return nil, fmt.Errorf("无效的CREATE TABLE语句，缺少右括号")
		}
	}

	// 检查是否有表级选项（如engine、charset、row_format等）
	columnsDefinition := mysqlDDL[columnsStart:columnsEnd]

	// 移除任何可能的表级别的引擎、字符集和行格式设置
	columnsDefinition = strings.ReplaceAll(columnsDefinition, " engine=innodb", "")
	columnsDefinition = strings.ReplaceAll(columnsDefinition, " ENGINE=InnoDB", "")
	columnsDefinition = strings.ReplaceAll(columnsDefinition, " engine=myisam", "")
	columnsDefinition = strings.ReplaceAll(columnsDefinition, " ENGINE=MyISAM", "")
	columnsDefinition = strings.ReplaceAll(columnsDefinition, " engine=memory", "")
	columnsDefinition = strings.ReplaceAll(columnsDefinition, " ENGINE=MEMORY", "")
	columnsDefinition = strings.ReplaceAll(columnsDefinition, " default charset=utf8mb4", "")
	columnsDefinition = strings.ReplaceAll(columnsDefinition, " DEFAULT CHARSET=utf8mb4", "")
	columnsDefinition = strings.ReplaceAll(columnsDefinition, " default charset=utf8", "")
	columnsDefinition = strings.ReplaceAll(columnsDefinition, " DEFAULT CHARSET=utf8", "")
	columnsDefinition = strings.ReplaceAll(columnsDefinition, " collate=utf8mb4_bin", "")
	columnsDefinition = strings.ReplaceAll(columnsDefinition, " COLLATE=utf8mb4_bin", "")
	columnsDefinition = strings.ReplaceAll(columnsDefinition, " collate=utf8mb4_unicode_ci", "")
	columnsDefinition = strings.ReplaceAll(columnsDefinition, " COLLATE=utf8mb4_unicode_ci", "")
	columnsDefinition = strings.ReplaceAll(columnsDefinition, " collate=utf8mb4_general_ci", "")
	columnsDefinition = strings.ReplaceAll(columnsDefinition, " COLLATE=utf8mb4_general_ci", "")
	columnsDefinition = strings.ReplaceAll(columnsDefinition, " row_format=compact", "")
	columnsDefinition = strings.ReplaceAll(columnsDefinition, " ROW_FORMAT=COMPACT", "")
	columnsDefinition = strings.ReplaceAll(columnsDefinition, " row_format=dynamic", "")
	columnsDefinition = strings.ReplaceAll(columnsDefinition, " ROW_FORMAT=DYNAMIC", "")

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
		upperTrimmedLine := strings.ToUpper(trimmedLine)
		// 使用正则表达式更精确地匹配索引定义
		// 索引定义必须以 KEY/INDEX 开头，索引名后面必须有左括号，且左括号后紧跟列名
		// 列名必须以字母或下划线开头（不能是类型参数如 20、255 等）
		indexPattern := regexp.MustCompile(`^(KEY|INDEX|UNIQUE KEY|UNIQUE INDEX)\s+[a-zA-Z_][a-zA-Z0-9_]*\s*\([a-zA-Z_]`)
		if trimmedLine == "" ||
			indexPattern.MatchString(upperTrimmedLine) ||
			strings.Contains(upperTrimmedLine, "FOREIGN KEY") ||
			strings.Contains(upperTrimmedLine, "USING BTREE") ||
			strings.Contains(upperTrimmedLine, "USING HASH") ||
			// 跳过表级别的引擎和字符集设置
			strings.Contains(trimmedLine, "engine=") ||
			strings.Contains(trimmedLine, "ENGINE=") ||
			strings.Contains(trimmedLine, "charset=") ||
			strings.Contains(trimmedLine, "CHARSET=") ||
			strings.Contains(trimmedLine, "collate=") ||
			strings.Contains(trimmedLine, "COLLATE=") ||
			// 跳过行格式设置
			strings.Contains(trimmedLine, "row_format=") ||
			strings.Contains(trimmedLine, "ROW_FORMAT=") ||
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

		// 解决mysql字段中有commitinfo字段无法转移的问题
		// 移除COMMENT子句，支持任意位置的COMMENT和转义的单引号
		reComment := regexp.MustCompile(`(?i)\s+comment\s+'((?:[^']|'')*)'\s*,?\s*|\s+comment\s+"([^"]*)"\s*,?\s*`)
		trimmedLine = reComment.ReplaceAllString(trimmedLine, "")
		trimmedLine = strings.TrimSpace(trimmedLine)

		// 移除末尾的逗号
		trimmedLine = strings.TrimSuffix(trimmedLine, ",")
		trimmedLine = strings.TrimSpace(trimmedLine)

		// 如果处理后行内容为空，跳过
		if trimmedLine == "" {
			continue
		}

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
			"decimal(10,0)":    "DECIMAL",
			"decimal(10,2)":    "DECIMAL",
			"decimal(64,0)":    "DECIMAL",
			"decimal(65,0)":    "DECIMAL",
			"decimal(41,0)":    "DECIMAL",
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
			"json":  "JSON",
			"jsonb": "JSONB",
			// 处理ENUM类型，转换为VARCHAR
			"enum": "VARCHAR(255)",
			// 处理SET类型，转换为VARCHAR
			"set": "VARCHAR(255)",
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

		// 处理SET类型的特殊情况（带枚举值的情况）
		re = regexp.MustCompile(`(?i)set\(([^)]+)\)`)
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

		// 处理零日期格式 "0000-00-00 00:00:00"，PostgreSQL不支持这种格式
		// 将其转换为NULL
		lowerTypeDef = strings.ReplaceAll(lowerTypeDef, " default '0000-00-00 00:00:00'", "")
		lowerTypeDef = strings.ReplaceAll(lowerTypeDef, " default '0000-00-00 00:00:00.000000'", "")
		lowerTypeDef = strings.ReplaceAll(lowerTypeDef, " default '0000-00-00 00:00:00.000'", "")
		lowerTypeDef = strings.ReplaceAll(lowerTypeDef, " default '0000-00-00'", "")

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

	// 根据是否是临时表生成不同的CREATE语句
	if isTemporary {
		result.WriteString(fmt.Sprintf(`CREATE TEMPORARY TABLE "%s" (`, tableName))
	} else {
		result.WriteString(fmt.Sprintf(`CREATE TABLE "%s" (`, tableName))
	}

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
	if (!strings.Contains(finalDDL, "CREATE TABLE") && !strings.Contains(finalDDL, "CREATE TEMPORARY TABLE")) || !strings.Contains(finalDDL, "(") || !strings.Contains(finalDDL, ")") {
		return nil, fmt.Errorf("生成的DDL无效: %s", finalDDL)
	}

	// 返回结果，包含DDL和表注释
	return &ConvertTableDDLResult{
		DDL:          finalDDL,
		TableComment: tableComment,
	}, nil
}
