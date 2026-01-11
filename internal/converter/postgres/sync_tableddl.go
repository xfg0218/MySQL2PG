package postgres

import (
	"fmt"
	"regexp"
	"strings"
)

// 包级预编译正则表达式，提高性能
var (
	// 字符集处理相关正则
	reTypeMb3Direct          = regexp.MustCompile(`(?i)(VARCHAR\(\d+\)|CHAR\(\d+\)|TEXT)mb3`)
	reTypeMb3Any             = regexp.MustCompile(`(?i)(VARCHAR\(\d+\)|CHAR\(\d+\)|TEXT)[\s\S]*?mb3`)
	reMb3Suffix              = regexp.MustCompile(`(?i)mb3`)
	reCharsetFull            = regexp.MustCompile(`(?i)(VARCHAR\(\d+\)|CHAR\(\d+\)|TEXT)\s*CHARACTER\s*SET\s*(?:utf8mb3|ascii)`)
	reCharsetSimple          = regexp.MustCompile(`(?i)(VARCHAR\(\d+\)|CHAR\(\d+\)|TEXT)\s*CHARACTER\s*(?:utf8mb3|ascii)`)
	reCollate                = regexp.MustCompile(`(?i)(VARCHAR\(\d+\)|CHAR\(\d+\)|TEXT)\s*COLLATE\s*(?:utf8mb3|ascii)_[\w-]+`)
	reComplexCharsetSpecific = regexp.MustCompile(`(?i)(char\(\d+\))\s*character\s+varchar\(\d+\)\s*ascii`)
	reComplexCharsetVarchar  = regexp.MustCompile(`(?i)(varchar\(\d+\))\s*character\s+char\(\d+\)\s*ascii`)
	reComplexCharset         = regexp.MustCompile(`(?i)(char\(\d+\)|varchar\(\d+\)|text)\s*character\s+(char\(\d+\)|varchar\(\d+\))`)
	reMb4Suffix              = regexp.MustCompile(`(?i)(text|longtext|mediumtext|tinytext|blob|longblob|mediumblob|tinyblob|binary|varbinary|varchar\(\d+\)|char\(\d+\))mb4`)

	// 默认值处理相关正则
	reDefaultEqual     = regexp.MustCompile(`default\s*=\s*`)
	reCurrentTimestamp = regexp.MustCompile(`current_timestamp\(\d+\)\(\d+\)`)

	// 类型映射相关正则
	reTinyInt1   = regexp.MustCompile(`(?i)\btinyint\(1\)\b`)
	reJsonLength = regexp.MustCompile(`(?i)\bjson\((\d+)\)\b`)

	// 类型清理相关正则
	reVarcharMissingParen  = regexp.MustCompile(`(?i)varchar\(\d+`)
	reExtraParens          = regexp.MustCompile(`([a-zA-Z]+)\((\s*\d+\s*)\)\s*\)`)
	reVarchar              = regexp.MustCompile(`(?i)varchar\(\d+\)`)
	reEnum                 = regexp.MustCompile(`(?i)enum\(([^)]+)\)`)
	reSet                  = regexp.MustCompile(`(?i)set\(([^)]+)\)`)
	reVarcharEnum          = regexp.MustCompile(`(?i)varchar\(\d+\)\(([^)]+)\)`)
	reVarcharZero          = regexp.MustCompile(`(?i)varchar\(0\)`)
	reDoublePrecision      = regexp.MustCompile(`(?i)double precision\(\d+,\d+\)`)
	reReal                 = regexp.MustCompile(`(?i)real\(\d+,\d+\)`)
	reIntegerWithPrecision = regexp.MustCompile(`(?i)(bigint|integer|smallint|int)\(\d+\)`)
	reBigSerial            = regexp.MustCompile(`(?i)bigserial\(\d+\)`)
	reSerial               = regexp.MustCompile(`(?i)serial\(\d+\)`)
	reBytea                = regexp.MustCompile(`(?i)bytea\(\d+\)`)
	reBasicTypes           = regexp.MustCompile(`(?i)\b(bigint|integer|smallint|int|bigserial|serial|boolean|text|bytea|timestamp|date|time|decimal|double precision|real)\b`)

	// 表相关正则
	reComment      = regexp.MustCompile(`(?i)\s+comment\s+'((?:[^']|'')*)'\s*,?\s*|\s+comment\s+"([^"]*)"\s*,?\s*`)
	reTableComment = regexp.MustCompile(`(?i)\s+COMMENT\s*=\s*'([^']*)'`)

	// 索引相关正则
	reIndexPattern = regexp.MustCompile(`^(KEY|INDEX|UNIQUE KEY|UNIQUE INDEX|"KEY"|"INDEX"|"UNIQUE KEY"|"UNIQUE INDEX")\s+(["a-zA-Z_]["a-zA-Z0-9_"]*)\s*\(["a-zA-Z_]`)

	// mb3相关正则
	reTypeMb3Generic = regexp.MustCompile(`(?i)(varchar\((\d+)\)|char\((\d+)\)|text)[^\w]*mb3`)
)

// 类型映射表 - 简化版，包含所有MySQL数据类型的基本类型映射

// 应用类型映射的顺序
var typeMappingOrder = []string{
	// 特殊处理的类型放在前面
	"tinyint(1)",
	// 整数类型
	"bigint", "biginteger", "int", "integer", "smallinteger", "tinyinteger", "tinyint", "smallint", "mediumint",
	// 浮点数类型
	"decimal", "double", "double precision", "float", "numeric",
	// 字符串类型
	"char", "varchar", "text", "longtext", "mediumtext", "tinytext",
	// 二进制类型
	"blob", "longblob", "mediumblob", "tinyblob", "binary", "varbinary",
	// 日期时间类型
	"datetime", "timestamp", "date", "time", "year",
	// JSON类型
	"json", "jsonb",
	// 空间类型
	"geometry", "point", "linestring", "polygon", "multipoint", "multilinestring", "multipolygon", "geometrycollection",
	// 特殊类型
	"enum", "set",
}

// 定义需要保留精度的类型模式
var typePatterns = map[string]*regexp.Regexp{
	"decimal":   regexp.MustCompile(`(?i)\bdecimal\((\d+)(?:,(\d+))?\)\b`),
	"numeric":   regexp.MustCompile(`(?i)\bnumeric\((\d+)(?:,(\d+))?\)\b`),
	"datetime":  regexp.MustCompile(`(?i)\bdatetime\((\d+)\)\b`),
	"timestamp": regexp.MustCompile(`(?i)\btimestamp\((\d+)\)\b`),
	"char":      regexp.MustCompile(`(?i)\bchar\((\d+)\)\b`),
	"varchar":   regexp.MustCompile(`(?i)\bvarchar\((\d+)\)\b`),
	"double":    regexp.MustCompile(`(?i)\bdouble\((\d+)(?:,(\d+))?\)\b`),
	"float":     regexp.MustCompile(`(?i)\bfloat\((\d+)(?:,(\d+))?\)\b`),
	"time":      regexp.MustCompile(`(?i)\btime\((\d+)\)\b`),
}

// 预编译常用正则表达式，提高性能

// 类型映射表 - 简化版，包含所有MySQL数据类型的基本类型映射
var typeMap = map[string]string{
	// 整数类型
	"bigint":       "BIGINT",
	"biginteger":   "BIGINT",
	"int":          "INTEGER",
	"integer":      "INTEGER",
	"smallinteger": "SMALLINT",
	"tinyinteger":  "SMALLINT",
	"tinyint(1)":   "BOOLEAN",
	"tinyint":      "SMALLINT",
	"smallint":     "SMALLINT",
	"mediumint":    "INTEGER",
	// 浮点数类型
	"decimal":          "DECIMAL",
	"double":           "DOUBLE PRECISION",
	"double precision": "DOUBLE PRECISION",
	"float":            "REAL",
	"numeric":          "NUMERIC",
	// 字符串类型
	"char":       "CHAR",
	"varchar":    "VARCHAR",
	"text":       "TEXT",
	"longtext":   "TEXT",
	"mediumtext": "TEXT",
	"tinytext":   "TEXT",
	// 二进制类型
	"blob":       "BYTEA",
	"longblob":   "BYTEA",
	"mediumblob": "BYTEA",
	"tinyblob":   "BYTEA",
	"binary":     "BYTEA",
	"varbinary":  "BYTEA",
	// 日期时间类型
	"datetime":  "TIMESTAMP",
	"timestamp": "TIMESTAMP",
	"date":      "DATE",
	"time":      "TIME",
	"year":      "INTEGER",
	// JSON类型
	"json":  "JSON",
	"jsonb": "JSONB",
	// 空间类型
	"geometry":           "GEOMETRY",
	"point":              "POINT",
	"linestring":         "LINESTRING",
	"polygon":            "POLYGON",
	"multipoint":         "MULTIPOINT",
	"multilinestring":    "MULTILINESTRING",
	"multipolygon":       "MULTIPOLYGON",
	"geometrycollection": "GEOMETRYCOLLECTION",
	// 特殊类型
	"enum": "VARCHAR(255)",
	"set":  "VARCHAR(255)",
}

type ConvertTableDDLResult struct {
	DDL            string
	TableComment   string
	ColumnNames    map[string]string // 键：原始列名，值：转换后的列名（带双引号格式）
	ColumnComments map[string]string // 键：原始列名，值：列注释
}

// parseTableInfo 解析表名和是否为临时表
func parseTableInfo(mysqlDDL string) (tableName string, isTemporary bool, tableComment string, columnsStart int, columnsEnd int, err error) {
	// 将MySQL DDL中的反引号替换为双引号
	mysqlDDL = strings.ReplaceAll(mysqlDDL, "`", "")

	// 解析表名，先检查是否是临时表
	var tableNameStart int

	// 检查是否是临时表
	tableNameStart = strings.Index(strings.ToUpper(mysqlDDL), "CREATE TEMPORARY TABLE")
	if tableNameStart != -1 {
		isTemporary = true
		tableNameStart += len("CREATE TEMPORARY TABLE")
	} else {
		// 不是临时表，检查普通表
		tableNameStart = strings.Index(strings.ToUpper(mysqlDDL), "CREATE TABLE")
		if tableNameStart == -1 {
			return "", false, "", 0, 0, fmt.Errorf("无效的CREATE TABLE语句")
		}
		tableNameStart += len("CREATE TABLE")
	}

	tableNameEnd := strings.Index(mysqlDDL[tableNameStart:], "(")
	if tableNameEnd == -1 {
		return "", false, "", 0, 0, fmt.Errorf("无效的CREATE TABLE语句，缺少左括号")
	}

	// 提取表名，处理引号包围的情况
	tableName = strings.TrimSpace(mysqlDDL[tableNameStart : tableNameStart+tableNameEnd])
	// 移除表名周围的引号
	if strings.HasPrefix(tableName, "'") && strings.HasSuffix(tableName, "'") {
		tableName = tableName[1 : len(tableName)-1]
	} else if strings.HasPrefix(tableName, `"`) && strings.HasSuffix(tableName, `"`) {
		tableName = tableName[1 : len(tableName)-1]
	}

	// 先提取表注释，然后再处理列定义
	tableComment = ""
	tableCommentMatch := reTableComment.FindStringSubmatch(mysqlDDL)
	if tableCommentMatch != nil {
		tableComment = tableCommentMatch[1]
	}

	// 使用平衡括号算法来找到表定义的结束位置，这样可以正确处理注释中的括号和嵌套括号
	var bracketCount int
	var inSingleQuote bool
	var inDoubleQuote bool
	var escapeNext bool

	// 将MySQL DDL转换为rune数组，以便正确处理中文字符
	mysqlDDLRunes := []rune(mysqlDDL)

	// 初始括号计数应为1，因为我们已经跳过了表定义的左括号
	columnsStart = tableNameStart + tableNameEnd + 1
	bracketCount = 1

	for i := columnsStart; i < len(mysqlDDLRunes); i++ {
		char := mysqlDDLRunes[i]

		if escapeNext {
			escapeNext = false
			continue
		}

		switch char {
		case '\\':
			escapeNext = true
		case '\'':
			if !inDoubleQuote {
				inSingleQuote = !inSingleQuote
			}
		case '"':
			if !inSingleQuote {
				inDoubleQuote = !inDoubleQuote
			}
		case '(':
			if !inSingleQuote && !inDoubleQuote {
				bracketCount++
			}
		case ')':
			if !inSingleQuote && !inDoubleQuote {
				bracketCount--
				if bracketCount == 0 {
					// 找到表定义的结束位置，将rune索引转换为字节索引
					columnsEnd = len([]byte(string(mysqlDDLRunes[:i+1])))
					break
				}
			}
		}
	}

	if columnsEnd == 0 {
		// 如果平衡括号算法失败，回退到原来的方法（找最后一个右括号）
		columnsEnd = strings.LastIndex(mysqlDDL, ")")
		if columnsEnd == -1 {
			// 如果找不到右括号，返回错误
			return "", false, "", 0, 0, fmt.Errorf("无法解析表DDL: 找不到右括号")
		}
	}

	return tableName, isTemporary, tableComment, columnsStart, columnsEnd, nil
}

// cleanTableLevelSettings 清理表级别的引擎、字符集和行格式设置
func cleanTableLevelSettings(columnsDefinition string) string {
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
	columnsDefinition = strings.ReplaceAll(columnsDefinition, " default charset=utf8mb3", "")
	columnsDefinition = strings.ReplaceAll(columnsDefinition, " DEFAULT CHARSET=utf8mb3", "")
	columnsDefinition = strings.ReplaceAll(columnsDefinition, " collate=utf8mb4_bin", "")
	columnsDefinition = strings.ReplaceAll(columnsDefinition, " COLLATE=utf8mb4_bin", "")
	columnsDefinition = strings.ReplaceAll(columnsDefinition, " collate=utf8mb3_bin", "")
	columnsDefinition = strings.ReplaceAll(columnsDefinition, " COLLATE=utf8mb3_bin", "")
	columnsDefinition = strings.ReplaceAll(columnsDefinition, " collate=utf8mb3_general_ci", "")
	columnsDefinition = strings.ReplaceAll(columnsDefinition, " COLLATE=utf8mb3_general_ci", "")
	columnsDefinition = strings.ReplaceAll(columnsDefinition, " collate=utf8mb4_unicode_ci", "")
	columnsDefinition = strings.ReplaceAll(columnsDefinition, " COLLATE=utf8mb4_unicode_ci", "")
	columnsDefinition = strings.ReplaceAll(columnsDefinition, " collate=utf8mb4_general_ci", "")
	columnsDefinition = strings.ReplaceAll(columnsDefinition, " COLLATE=utf8mb4_general_ci", "")
	columnsDefinition = strings.ReplaceAll(columnsDefinition, " collate=utf8mb4_0900_ai_ci", "")
	columnsDefinition = strings.ReplaceAll(columnsDefinition, " COLLATE=utf8mb4_0900_ai_ci", "")
	columnsDefinition = strings.ReplaceAll(columnsDefinition, " row_format=compact", "")
	columnsDefinition = strings.ReplaceAll(columnsDefinition, " ROW_FORMAT=COMPACT", "")
	columnsDefinition = strings.ReplaceAll(columnsDefinition, " row_format=dynamic", "")
	columnsDefinition = strings.ReplaceAll(columnsDefinition, " ROW_FORMAT=DYNAMIC", "")
	return columnsDefinition
}

// convertDataType 将MySQL数据类型转换为PostgreSQL数据类型
func convertDataType(mysqlType string) (postgresType string, isAutoIncrement bool, err error) {
	// 初始化返回值
	postgresType = mysqlType
	isAutoIncrement = false

	// 移除AUTO_INCREMENT关键字，并标记是否为自增字段
	if strings.Contains(strings.ToLower(mysqlType), "auto_increment") {
		isAutoIncrement = true
		mysqlType = strings.ReplaceAll(strings.ToLower(mysqlType), "auto_increment", "")
		mysqlType = strings.TrimSpace(mysqlType)
	}

	// 处理tinyint(1)转换为BOOLEAN的特殊情况
	if reTinyInt1.MatchString(mysqlType) {
		postgresType = "BOOLEAN"
		return postgresType, isAutoIncrement, nil
	}

	// 处理JSON带长度的情况，如json(500)
	if reJsonLength.MatchString(mysqlType) {
		postgresType = "JSON"
		return postgresType, isAutoIncrement, nil
	}

	// 处理mb3相关的字符集问题
	mysqlType = reTypeMb3Direct.ReplaceAllString(mysqlType, "$1")
	mysqlType = reTypeMb3Any.ReplaceAllString(mysqlType, "$1")
	mysqlType = reTypeMb3Generic.ReplaceAllString(mysqlType, "$1")
	mysqlType = reMb3Suffix.ReplaceAllString(mysqlType, "")

	// 处理字符集相关的语法
	mysqlType = reCharsetFull.ReplaceAllString(mysqlType, "$1")
	mysqlType = reCharsetSimple.ReplaceAllString(mysqlType, "$1")
	mysqlType = reCollate.ReplaceAllString(mysqlType, "$1")
	mysqlType = reComplexCharset.ReplaceAllString(mysqlType, "$1")
	mysqlType = reComplexCharsetSpecific.ReplaceAllString(mysqlType, "$1")
	mysqlType = reComplexCharsetVarchar.ReplaceAllString(mysqlType, "$1")

	// 处理mb4后缀
	mysqlType = reMb4Suffix.ReplaceAllString(mysqlType, "$1")

	// 移除多余的空格
	mysqlType = strings.TrimSpace(mysqlType)

	// 检查是否为基本类型，直接映射
	for _, mysqlTypeKey := range typeMappingOrder {
		if strings.Contains(strings.ToLower(mysqlType), strings.ToLower(mysqlTypeKey)) {
			// 检查是否需要保留精度
			if pattern, exists := typePatterns[strings.ToLower(mysqlTypeKey)]; exists && pattern.MatchString(mysqlType) {
				// 对于需要保留精度的类型，保持原始格式
				postgresType = mysqlType
			} else {
				// 使用映射表转换类型
				postgresType = typeMap[mysqlTypeKey]
			}
			break
		}
	}

	// 处理自增字段的类型映射
	if isAutoIncrement {
		if postgresType == "BIGINT" {
			postgresType = "BIGSERIAL"
		} else {
			postgresType = "SERIAL"
		}
	}

	return postgresType, isAutoIncrement, nil
}

// processColumnDefinition 处理列定义，提取列名、类型定义和注释
func processColumnDefinition(line string, lowercaseColumns bool) (columnName string, typeDefinition string, columnComment string, isConstraint bool, isIncompleteType bool, err error) {
	// 移除ON UPDATE CURRENT_TIMESTAMP
	line = strings.ReplaceAll(line, " ON UPDATE CURRENT_TIMESTAMP", "")

	// 移除unsigned关键字
	line = strings.ReplaceAll(line, " unsigned", "")
	line = strings.ReplaceAll(line, " UNSIGNED", "")

	// 移除字符集和排序规则声明
	line = strings.ReplaceAll(line, " COLLATE utf8mb4_unicode_ci", "")
	line = strings.ReplaceAll(line, " COLLATE utf8_unicode_ci", "")
	line = strings.ReplaceAll(line, " COLLATE utf8_general_ci", "")
	line = strings.ReplaceAll(line, " COLLATE utf8mb4_bin", "")
	line = strings.ReplaceAll(line, " COLLATE utf8_bin", "")
	line = strings.ReplaceAll(line, " COLLATE utf8mb3_bin", "")
	line = strings.ReplaceAll(line, " COLLATE utf8mb3_general_ci", "")
	line = strings.ReplaceAll(line, " COLLATE utf32_bin", "")
	line = strings.ReplaceAll(line, " COLLATE latin1_bin", "")
	line = strings.ReplaceAll(line, " COLLATE latin1_swedish_ci", "")
	line = strings.ReplaceAll(line, " COLLATE utf8mb4_0900_ai_ci", "")
	line = strings.ReplaceAll(line, " character set utf8", "")
	line = strings.ReplaceAll(line, " CHARACTER SET utf8", "")
	line = strings.ReplaceAll(line, " character set utf8mb3", "")
	line = strings.ReplaceAll(line, " CHARACTER SET utf8mb3", "")
	line = strings.ReplaceAll(line, " character set latin1", "")
	line = strings.ReplaceAll(line, " CHARACTER SET latin1", "")
	line = strings.ReplaceAll(line, " character set utf16", "")
	line = strings.ReplaceAll(line, " CHARACTER SET utf16", "")
	line = strings.ReplaceAll(line, " charset=latin1", "")
	line = strings.ReplaceAll(line, " CHARSET=latin1", "")
	line = strings.ReplaceAll(line, " charset=utf16", "")
	line = strings.ReplaceAll(line, " CHARSET=utf16", "")
	line = strings.ReplaceAll(line, " charset=utf8mb3", "")
	line = strings.ReplaceAll(line, " CHARSET=utf8mb3", "")

	// 移除COMMENT子句，支持任意位置的COMMENT和转义的单引号
	commentMatch := reComment.FindStringSubmatch(line)
	if commentMatch != nil {
		// 捕获单引号或双引号中的注释内容
		if commentMatch[1] != "" {
			columnComment = commentMatch[1]
		} else {
			columnComment = commentMatch[2]
		}
	}
	line = reComment.ReplaceAllString(line, "")
	line = strings.TrimSpace(line)

	// 移除末尾的逗号
	line = strings.TrimSuffix(line, ",")
	line = strings.TrimSpace(line)

	// 如果处理后行内容为空或者只是右括号，跳过
	if line == "" || line == ")" {
		isConstraint = true
		return
	}

	// 检查是否是约束定义（包含CONSTRAINT关键字或其他约束类型）
	upperLine := strings.ToUpper(line)
	// 特殊处理CONSTRAINT, KEY, INDEX开头的情况
	if strings.HasPrefix(upperLine, "CONSTRAINT") || strings.HasPrefix(upperLine, "KEY") || strings.HasPrefix(upperLine, "INDEX") {
		// 检查后面是否跟着数据类型（表示这是一个列名）
		// 先分割行，查看第二个单词是否是数据类型
		parts := strings.Fields(line)
		if len(parts) < 2 {
			// 如果只有一个单词，跳过
			isConstraint = true
			return
		}
		// 检查第二个单词是否是常见的数据类型
		upperSecondPart := strings.ToUpper(parts[1])
		isDataType := strings.Contains(upperSecondPart, "INT") ||
			strings.Contains(upperSecondPart, "TEXT") ||
			strings.Contains(upperSecondPart, "VARCHAR") ||
			strings.Contains(upperSecondPart, "CHAR") ||
			strings.Contains(upperSecondPart, "BOOLEAN") ||
			strings.Contains(upperSecondPart, "DATE") ||
			strings.Contains(upperSecondPart, "TIME") ||
			strings.Contains(upperSecondPart, "TIMESTAMP") ||
			strings.Contains(upperSecondPart, "DECIMAL") ||
			strings.Contains(upperSecondPart, "DOUBLE") ||
			strings.Contains(upperSecondPart, "FLOAT") ||
			strings.Contains(upperSecondPart, "BLOB") ||
			strings.Contains(upperSecondPart, "BYTEA") ||
			strings.Contains(upperSecondPart, "JSON")
		if !isDataType {
			// 如果不是数据类型，说明这是一个约束定义，跳过
			isConstraint = true
			return
		}
		// 如果是数据类型，说明这是一个列名，继续处理
	}

	// 分离列名和类型定义
	if strings.HasPrefix(line, `"`) {
		// 找到第一个引号结束的位置
		quoteEnd := strings.Index(line[1:], `"`)
		if quoteEnd != -1 {
			columnName = line[1 : quoteEnd+1]
			typeDefinition = strings.TrimSpace(line[quoteEnd+2:])
			// 检查类型定义是否完整（括号是否匹配）
			if strings.Count(typeDefinition, "(") > strings.Count(typeDefinition, ")") {
				// 类型定义不完整，需要继续处理下一行
				isIncompleteType = true
				return
			}
			// 确保列名是正确的大小写（根据lowercaseColumns参数）
			if lowercaseColumns {
				columnName = strings.ToLower(columnName)
			}
		}
	} else {
		// 处理没有引号的列名，使用第一个空格分割
		parts := strings.Fields(line)
		if len(parts) < 2 {
			// 如果只有一个单词，可能是约束定义
			isConstraint = true
			return
		}
		columnName = parts[0]
		typeDefinition = strings.Join(parts[1:], " ")
		// 检查类型定义是否完整（括号是否匹配）
		if strings.Count(typeDefinition, "(") > strings.Count(typeDefinition, ")") {
			// 类型定义不完整，需要继续处理下一行
			isIncompleteType = true
			return
		}
		// 确保列名是正确的大小写（根据lowercaseColumns参数）
		if lowercaseColumns {
			columnName = strings.ToLower(columnName)
		}
	}

	return
}

// ConvertTableDDL 转换MySQL表DDL到PostgreSQL
func ConvertTableDDL(mysqlDDL string, lowercaseColumns bool) (*ConvertTableDDLResult, error) {
	// 将MySQL DDL中的反引号替换为双引号
	mysqlDDL = strings.ReplaceAll(mysqlDDL, "`", "\"")

	// 创建列名映射：原始列名 → 转换后的列名
	columnNamesMap := make(map[string]string)
	// 创建列注释映射：原始列名 → 列注释
	columnCommentsMap := make(map[string]string)

	// 解析表信息
	tableName, isTemporary, tableComment, columnsStart, columnsEnd, err := parseTableInfo(mysqlDDL)
	if err != nil {
		return nil, err
	}

	// 检查是否有表级选项（如engine、charset、row_format等）
	columnsDefinition := mysqlDDL[columnsStart:columnsEnd]

	// 移除任何可能的表级别的引擎、字符集和行格式设置
	columnsDefinition = cleanTableLevelSettings(columnsDefinition)

	// 按行分割列定义
	lines := strings.Split(columnsDefinition, "\n")

	// 存储列定义和主键信息
	var columnDefinitions []string
	var primaryKeyColumn string
	var re *regexp.Regexp

	// 存储列名映射，用于保持大小写一致
	columnNames := make(map[string]string)

	// 处理每一行
	var incompleteTypeDef bool
	var partialTypeDef string
	var partialColumnName string
	for _, line := range lines {
		trimmedLine := strings.TrimSpace(line)

		// 处理跨多行的类型定义
		if incompleteTypeDef {
			// 继续添加到之前的部分类型定义，避免在括号内添加多余空格
			if strings.HasPrefix(trimmedLine, ")") && strings.HasSuffix(partialTypeDef, "(") {
				partialTypeDef += trimmedLine // No space between ( and )
			} else {
				partialTypeDef += " " + trimmedLine
			}
			// 检查是否完成了类型定义（包含了右括号）
			if strings.Count(partialTypeDef, "(") == strings.Count(partialTypeDef, ")") {
				// 完整的类型定义已经形成，使用这个完整的定义继续处理，确保partialColumnName是正确的大小写（根据lowercaseColumns参数）
				if lowercaseColumns {
					partialColumnName = strings.ToLower(partialColumnName)
				}
				trimmedLine = partialColumnName + " " + partialTypeDef
				incompleteTypeDef = false
				partialTypeDef = ""
				partialColumnName = ""
			} else {
				// 类型定义仍然不完整，继续等待下一行
				continue
			}
		}

		// 检查是否需要跳过当前行
		if trimmedLine == "" {
			continue
		}

		// 首先检查是否是约束定义行，约束定义以 CONSTRAINT 开头，或者是 FOREIGN KEY 约束，必须在所有处理之前检查并跳过
		upperTrimmedLine := strings.ToUpper(trimmedLine)
		// 跳过以CONSTRAINT开头的行，只跳过真正的约束定义（以"CONSTRAINT "开头，注意空格）
		// 避免跳过列名为"constraints"的列定义
		if strings.HasPrefix(strings.TrimSpace(upperTrimmedLine), "CONSTRAINT ") {
			continue
		}
		// 跳过包含CONSTRAINT的外键约束定义（需要包含括号结构，且不是列定义）
		// 只有当行以CONSTRAINT或FOREIGN KEY开头时才跳过，避免跳过列名为constraint/constraints的列定义
		if strings.HasPrefix(upperTrimmedLine, "CONSTRAINT") || strings.HasPrefix(upperTrimmedLine, "FOREIGN KEY") {
			continue
		}

		// 使用正则表达式更精确地匹配索引定义
		// 索引定义必须以 KEY/INDEX 开头，索引名后面必须有左括号，且左括号后紧跟列名
		// 列名必须以字母或下划线开头（不能是类型参数如 20、255 等）
		// 支持带有双引号的KEY/INDEX定义（转换后的格式）

		// 只跳过明确是索引、外键或表级设置的行
		// 避免误判列定义为需要跳过的内容
		if reIndexPattern.MatchString(upperTrimmedLine) ||
			strings.Contains(upperTrimmedLine, "FOREIGN KEY") ||
			strings.Contains(upperTrimmedLine, "USING BTREE") ||
			strings.Contains(upperTrimmedLine, "USING HASH") ||
			// 跳过表级别的引擎和字符集设置（注意：不要跳过列级别的字符集设置）
			(strings.Contains(trimmedLine, "engine=") && !strings.Contains(trimmedLine, "`") && !strings.Contains(trimmedLine, " ")) ||
			(strings.Contains(trimmedLine, "ENGINE=") && !strings.Contains(trimmedLine, "`") && !strings.Contains(trimmedLine, " ")) ||
			// 列级别的charset和collate会在后面的处理中被移除，所以不要跳过
			(strings.Contains(trimmedLine, "row_format=") && !strings.Contains(trimmedLine, "`") && !strings.Contains(trimmedLine, " ")) ||
			(strings.Contains(trimmedLine, "ROW_FORMAT=") && !strings.Contains(trimmedLine, "`") && !strings.Contains(trimmedLine, " ")) {
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

		// 处理CONSTRAINT关键字，只跳过真正的约束定义（以"CONSTRAINT "开头，注意空格）
		// 避免跳过列名为"constraints"的列定义
		if strings.HasPrefix(strings.ToUpper(trimmedLine), "CONSTRAINT ") {
			continue
		}

		// 使用辅助函数处理列定义
		columnName, typeDefinition, columnComment, isConstraint, isIncompleteType, err := processColumnDefinition(trimmedLine, lowercaseColumns)
		if err != nil {
			return nil, err
		}

		// 如果是约束定义或空行，跳过
		if isConstraint {
			continue
		}

		// 如果类型定义不完整，需要继续处理下一行
		if isIncompleteType {
			incompleteTypeDef = true
			partialTypeDef = typeDefinition
			partialColumnName = columnName
			continue
		}

		// 处理没有类型定义的列
		if typeDefinition == "" {
			// 无法确定列类型，返回错误
			return nil, fmt.Errorf("为表 %s 的列 %s 无法确定类型定义", tableName, columnName)
		}

		// 保存原始列名（在处理大小写之前）
		originalColumnName := columnName

		// 处理列名大小写（提前处理，确保保存到columnNames映射表的是正确的大小写）
		if lowercaseColumns {
			columnName = strings.ToLower(columnName)
		}

		// 将原始列名和转换后的列名保存到映射中（转换后的列名带有双引号格式）
		columnNamesMap[originalColumnName] = fmt.Sprintf(`"%s"`, columnName)

		// 如果有列注释，保存到映射中
		if columnComment != "" {
			columnCommentsMap[originalColumnName] = columnComment
		}

		// 存储列名，保持正确的大小写
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

		// 先清理原始类型定义，移除所有可能的字符集信息
		// 使用更强大的正则表达式处理所有mb3后缀情况
		// 匹配类型后面直接跟mb3的情况，如VARCHAR(255)mb3、char(10)mb3、TEXTmb3等
		typeDefinition = reTypeMb3Direct.ReplaceAllString(typeDefinition, "$1")

		// 匹配类型后面可能有空格或其他字符再跟mb3的情况
		typeDefinition = reTypeMb3Any.ReplaceAllString(typeDefinition, "$1")

		// 全局替换所有mb3后缀，确保没有遗漏
		typeDefinition = reMb3Suffix.ReplaceAllString(typeDefinition, "")

		// 处理完整的CHARACTER SET语法
		typeDefinition = reCharsetFull.ReplaceAllString(typeDefinition, "$1")

		// 处理简化的CHARACTER语法
		typeDefinition = reCharsetSimple.ReplaceAllString(typeDefinition, "$1")

		// 处理COLLATE语法
		typeDefinition = reCollate.ReplaceAllString(typeDefinition, "$1")

		// 处理复杂的ascii字符集问题，如：char(255) character VARCHAR(255) ascii
		typeDefinition = reComplexCharsetSpecific.ReplaceAllString(typeDefinition, "$1")
		typeDefinition = reComplexCharsetVarchar.ReplaceAllString(typeDefinition, "$1")
		typeDefinition = reComplexCharset.ReplaceAllString(typeDefinition, "$1")

		// 处理单独的character ascii和collate ascii_general_ci
		typeDefinition = strings.ReplaceAll(typeDefinition, " character ascii", "")
		typeDefinition = strings.ReplaceAll(typeDefinition, " CHARACTER ASCII", "")
		typeDefinition = strings.ReplaceAll(typeDefinition, " collate ascii_general_ci", "")
		typeDefinition = strings.ReplaceAll(typeDefinition, " COLLATE ASCII_GENERAL_CI", "")

		// 现在转为小写，确保类型映射能正确工作
		lowerTypeDef := strings.ToLower(typeDefinition)

		// 继续处理其他字符集和排序规则
		lowerTypeDef = strings.ReplaceAll(lowerTypeDef, " character set utf8mb4", "")
		lowerTypeDef = strings.ReplaceAll(lowerTypeDef, " character set utf8", "")
		lowerTypeDef = strings.ReplaceAll(lowerTypeDef, " character set utf32", "")
		lowerTypeDef = strings.ReplaceAll(lowerTypeDef, " character set utf8mb3", "")
		lowerTypeDef = strings.ReplaceAll(lowerTypeDef, " character set gb2312", "")
		lowerTypeDef = strings.ReplaceAll(lowerTypeDef, " collate utf8mb4_unicode_ci", "")
		lowerTypeDef = strings.ReplaceAll(lowerTypeDef, " collate utf8mb4_general_ci", "")
		lowerTypeDef = strings.ReplaceAll(lowerTypeDef, " collate utf8_unicode_ci", "")
		lowerTypeDef = strings.ReplaceAll(lowerTypeDef, " collate utf8_general_ci", "")
		lowerTypeDef = strings.ReplaceAll(lowerTypeDef, " collate utf32_bin", "")
		lowerTypeDef = strings.ReplaceAll(lowerTypeDef, " collate utf8mb3_bin", "")
		lowerTypeDef = strings.ReplaceAll(lowerTypeDef, " collate utf8mb3_general_ci", "")
		lowerTypeDef = strings.ReplaceAll(lowerTypeDef, " collate utf8mb3_unicode_ci", "")
		lowerTypeDef = strings.ReplaceAll(lowerTypeDef, " collate utf8mb4_0900_ai_ci", "")
		lowerTypeDef = strings.ReplaceAll(lowerTypeDef, " collate gb2312_chinese_ci", "")
		// 处理没有set关键字的情况
		lowerTypeDef = strings.ReplaceAll(lowerTypeDef, " character utf8mb4", "")
		lowerTypeDef = strings.ReplaceAll(lowerTypeDef, " character utf8", "")
		lowerTypeDef = strings.ReplaceAll(lowerTypeDef, " character utf8mb3", "")
		lowerTypeDef = strings.ReplaceAll(lowerTypeDef, " character gb2312", "")
		// 最后再次检查并移除可能遗留的mb3后缀
		lowerTypeDef = regexp.MustCompile(`(?i)(varchar\(\d+\)|char\(\d+\)|text)[^\w]*mb3`).ReplaceAllString(lowerTypeDef, "$1")
		lowerTypeDef = regexp.MustCompile(`(?i)(varchar\(\d+\)|char\(\d+\)|text)mb3`).ReplaceAllString(lowerTypeDef, "$1")

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

		// 应用类型映射
		for _, mysqlType := range typeMappingOrder {
			pgType, exists := typeMap[mysqlType]
			if !exists {
				continue
			}

			// 特殊处理tinyint(1)映射为BOOLEAN
			if mysqlType == "tinyint(1)" {
				re = regexp.MustCompile(`(?i)\btinyint\(1\)\b`)
				lowerTypeDef = re.ReplaceAllString(lowerTypeDef, pgType)
				continue
			}

			// 检查是否需要保留精度信息
			if pattern, ok := typePatterns[mysqlType]; ok {
				// 处理带精度的类型
				lowerTypeDef = pattern.ReplaceAllStringFunc(lowerTypeDef, func(m string) string {
					match := pattern.FindStringSubmatch(m)
					if len(match) >= 2 {
						// 根据不同类型构建带精度的PostgreSQL类型
						switch mysqlType {
						case "decimal", "numeric":
							if len(match) == 3 && match[2] != "" {
								return fmt.Sprintf("%s(%s,%s)", strings.ToUpper(mysqlType), match[1], match[2])
							}
							return fmt.Sprintf("%s(%s)", strings.ToUpper(mysqlType), match[1])
						case "datetime", "timestamp":
							return fmt.Sprintf("TIMESTAMP(%s)", match[1])
						case "time":
							return fmt.Sprintf("TIME(%s)", match[1])
						case "char":
							return fmt.Sprintf("CHAR(%s)", match[1])
						case "varchar":
							return fmt.Sprintf("VARCHAR(%s)", match[1])
						case "double":
							if len(match) == 3 && match[2] != "" {
								return fmt.Sprintf("DOUBLE PRECISION(%s,%s)", match[1], match[2])
							}
							return fmt.Sprintf("DOUBLE PRECISION(%s)", match[1])
						case "float":
							if len(match) == 3 && match[2] != "" {
								return fmt.Sprintf("REAL(%s,%s)", match[1], match[2])
							}
							return fmt.Sprintf("REAL(%s)", match[1])
						default:
							return pgType
						}
					}
					return pgType
				})
			}

			// 处理不带精度的基本类型
			re = regexp.MustCompile(`(?i)\b` + regexp.QuoteMeta(mysqlType) + `\b`)
			lowerTypeDef = re.ReplaceAllString(lowerTypeDef, pgType)

			// 处理带精度的JSON类型变体（如json(1024)）
			if mysqlType == "json" {
				re = regexp.MustCompile(`(?i)\bjson\((\d+)\)\b`)
				lowerTypeDef = re.ReplaceAllString(lowerTypeDef, "JSON")
			}
		}

		// 修复VARCHAR类型缺少右括号的情况 - 只在确实缺少时添加
		re = regexp.MustCompile(`(?i)varchar\(\d+`)
		lowerTypeDef = re.ReplaceAllStringFunc(lowerTypeDef, func(m string) string {
			// 查找匹配字符串在原始字符串中的位置
			index := strings.Index(lowerTypeDef, m)
			if index == -1 {
				return m
			}
			// 检查匹配字符串后面是否紧跟着右括号
			nextIndex := index + len(m)
			if nextIndex < len(lowerTypeDef) && string(lowerTypeDef[nextIndex]) == ")" {
				// 已经有右括号了，直接返回大写形式
				return strings.ToUpper(m)
			}
			// 没有右括号，添加一个
			return strings.ToUpper(m) + ")"
		})

		// 清理类型定义中可能出现的多余空格和括号
		re = regexp.MustCompile(`([a-zA-Z]+)\((\s*\d+\s*)\)\s*\)`)
		lowerTypeDef = re.ReplaceAllStringFunc(lowerTypeDef, func(m string) string {
			match := re.FindStringSubmatch(m)
			if len(match) == 3 {
				return strings.ToUpper(match[1]) + "(" + strings.TrimSpace(match[2]) + ")"
			}
			return strings.ToUpper(m)
		})

		// 处理任意长度的VARCHAR类型（确保所有VARCHAR类型都有正确的闭合括号）
		re = regexp.MustCompile(`(?i)varchar\(\d+\)`)
		lowerTypeDef = re.ReplaceAllStringFunc(lowerTypeDef, func(m string) string {
			// 将匹配到的varchar(数字)转换为大写的VARCHAR(数字)
			return strings.ToUpper(m)
		})

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

		// 移除REAL类型后的精度指定
		re = regexp.MustCompile(`(?i)real\(\d+,\d+\)`)
		lowerTypeDef = re.ReplaceAllString(lowerTypeDef, "REAL")

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

		// 移除JSON类型后的长度参数（PostgreSQL不允许JSON有长度参数）
		re = regexp.MustCompile(`(?i)json\(\d+\)`)
		lowerTypeDef = re.ReplaceAllString(lowerTypeDef, "JSON")

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
		result.WriteString(fmt.Sprintf(`%s`, columnDef))
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
		result.WriteString(fmt.Sprintf(`,  PRIMARY KEY ("%s")`, primaryKeyColumn))
	}

	result.WriteString(`)`)

	finalDDL := result.String()

	// 检查生成的DDL是否有效
	if (!strings.Contains(finalDDL, "CREATE TABLE") && !strings.Contains(finalDDL, "CREATE TEMPORARY TABLE")) || !strings.Contains(finalDDL, "(") || !strings.Contains(finalDDL, ")") {
		return nil, fmt.Errorf("生成的DDL无效: %s", finalDDL)
	}

	// 返回结果，包含DDL、表注释、列名映射和列注释映射
	return &ConvertTableDDLResult{
		DDL:            finalDDL,
		TableComment:   tableComment,
		ColumnNames:    columnNamesMap,
		ColumnComments: columnCommentsMap,
	}, nil
}

// GenerateColumnCommentsSQL 生成PostgreSQL列注释SQL,PostgreSQL表名（带双引号）
func GenerateColumnCommentsSQL(tableName string, columnNamesMap, columnCommentsMap map[string]string) []string {
	var comments []string

	for originalColName, comment := range columnCommentsMap {
		// 处理注释中的单引号
		processedComment := strings.ReplaceAll(comment, "'", "''")
		// 清除注释中的回车换行等特殊字符
		processedComment = strings.ReplaceAll(processedComment, "\r", "")
		processedComment = strings.ReplaceAll(processedComment, "\n", "")
		processedComment = strings.ReplaceAll(processedComment, "\t", "")
		// 清除注释中的转义换行符\n
		processedComment = strings.ReplaceAll(processedComment, "\\n", "")

		// 根据映射表获取正确的列名
		if convertedColName, exists := columnNamesMap[originalColName]; exists {
			var commentSQL string
			if strings.HasPrefix(convertedColName, `"`) && strings.HasSuffix(convertedColName, `"`) {
				// 列名已经包含双引号，直接使用
				commentSQL = fmt.Sprintf("COMMENT ON COLUMN %s.%s IS '%s';", tableName, convertedColName, processedComment)
			} else {
				commentSQL = fmt.Sprintf("COMMENT ON COLUMN %s.\"%s\" IS '%s';", tableName, convertedColName, processedComment)
			}
			comments = append(comments, commentSQL)
		}
	}

	return comments
}
