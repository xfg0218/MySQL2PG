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
	reDefaultEqual            = regexp.MustCompile(`default\s*=\s*`)
	reCurrentTimestamp        = regexp.MustCompile(`current_timestamp\(\d+\)\(\d+\)`)
	reCurrentTimestampExtract = regexp.MustCompile(`current_timestamp\((\d+)\)`)

	// 类型映射相关正则
	reTinyInt1       = regexp.MustCompile(`(?i)\btinyint\(1\)\b`)
	reJsonLength     = regexp.MustCompile(`(?i)\bjson\((\d+)\)\b`)
	reJsonWithLength = regexp.MustCompile(`(?i)json\(\d+\)`)

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
	rePrimaryKey   = regexp.MustCompile(`PRIMARY KEY\s*\(\s*(\w+)\s*\)`)

	// mb3相关正则
	reTypeMb3Generic = regexp.MustCompile(`(?i)(varchar\((\d+)\)|char\((\d+)\)|text)[^\w]*mb3`)

	// 其他杂项正则
	reCharsetPrefix = regexp.MustCompile(`(?i)\b_\w+(['"])`)
	reVirtual       = regexp.MustCompile(`(?i)\s+VIRTUAL`)
)

// 基本类型正则缓存
var basicTypeRegexes = make(map[string]*regexp.Regexp)

// 初始化基本类型正则
func init() {
	for _, mysqlType := range typeMappingOrder {
		basicTypeRegexes[mysqlType] = regexp.MustCompile(`(?i)\b` + regexp.QuoteMeta(mysqlType) + `\b`)
	}
}

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

// 类型映射表
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

// ConvertTableDDLResult 存储DDL转换结果
type ConvertTableDDLResult struct {
	DDL            string
	TableComment   string
	ColumnNames    map[string]string // 键：原始列名，值：转换后的列名（带双引号格式）
	ColumnComments map[string]string // 键：原始列名，值：列注释
}

// parseTableInfo 解析表名和是否为临时表
func parseTableInfo(mysqlDDL string) (tableName string, isTemporary bool, tableComment string, columnsStart int, columnsEnd int, err error) {
	mysqlDDL = strings.ReplaceAll(mysqlDDL, "`", "")

	var tableNameStart int
	tableNameStart = strings.Index(strings.ToUpper(mysqlDDL), "CREATE TEMPORARY TABLE")
	if tableNameStart != -1 {
		isTemporary = true
		tableNameStart += len("CREATE TEMPORARY TABLE")
	} else {
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

	tableName = strings.TrimSpace(mysqlDDL[tableNameStart : tableNameStart+tableNameEnd])
	if strings.HasPrefix(tableName, "'") && strings.HasSuffix(tableName, "'") {
		tableName = tableName[1 : len(tableName)-1]
	} else if strings.HasPrefix(tableName, `"`) && strings.HasSuffix(tableName, `"`) {
		tableName = tableName[1 : len(tableName)-1]
	}

	tableComment = ""
	tableCommentMatch := reTableComment.FindStringSubmatch(mysqlDDL)
	if tableCommentMatch != nil {
		tableComment = tableCommentMatch[1]
	}

	var bracketCount int
	var inSingleQuote bool
	var inDoubleQuote bool
	var escapeNext bool

	mysqlDDLRunes := []rune(mysqlDDL)
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
					columnsEnd = len([]byte(string(mysqlDDLRunes[:i+1])))
					break
				}
			}
		}
	}

	if columnsEnd == 0 {
		columnsEnd = strings.LastIndex(mysqlDDL, ")")
		if columnsEnd == -1 {
			return "", false, "", 0, 0, fmt.Errorf("无法解析表DDL: 找不到右括号")
		}
	}

	return tableName, isTemporary, tableComment, columnsStart, columnsEnd, nil
}

// cleanTableLevelSettings 清理表级别的引擎、字符集和行格式设置
func cleanTableLevelSettings(columnsDefinition string) string {
	// 首先处理分区语法（最长匹配优先）
	columnsDefinition = rePartitionComment.ReplaceAllString(columnsDefinition, "")
	columnsDefinition = rePartitionSimple.ReplaceAllString(columnsDefinition, "")
	columnsDefinition = rePartitionComplex.ReplaceAllString(columnsDefinition, "")
	columnsDefinition = rePartition.ReplaceAllString(columnsDefinition, "")

	replacements := []struct {
		old string
		new string
	}{
		{" engine=innodb", ""}, {" ENGINE=InnoDB", ""},
		{" engine=myisam", ""}, {" ENGINE=MyISAM", ""},
		{" engine=memory", ""}, {" ENGINE=MEMORY", ""},
		{" default charset=utf8mb4", ""}, {" DEFAULT CHARSET=utf8mb4", ""},
		{" default charset=utf8", ""}, {" DEFAULT CHARSET=utf8", ""},
		{" default charset=utf8mb3", ""}, {" DEFAULT CHARSET=utf8mb3", ""},
		{" collate=utf8mb4_bin", ""}, {" COLLATE=utf8mb4_bin", ""},
		{" collate=utf8mb3_bin", ""}, {" COLLATE=utf8mb3_bin", ""},
		{" collate=utf8mb3_general_ci", ""}, {" COLLATE=utf8mb3_general_ci", ""},
		{" collate=utf8mb4_unicode_ci", ""}, {" COLLATE=utf8mb4_unicode_ci", ""},
		{" collate=utf8mb4_general_ci", ""}, {" COLLATE=utf8mb4_general_ci", ""},
		{" collate=utf8mb4_0900_ai_ci", ""}, {" COLLATE=utf8mb4_0900_ai_ci", ""},
		{" row_format=compact", ""}, {" ROW_FORMAT=COMPACT", ""},
		{" row_format=dynamic", ""}, {" ROW_FORMAT=DYNAMIC", ""},
	}

	for _, r := range replacements {
		columnsDefinition = strings.ReplaceAll(columnsDefinition, r.old, r.new)
	}
	return columnsDefinition
}

// convertDataType 将MySQL数据类型转换为PostgreSQL数据类型
func convertDataType(mysqlType string) (postgresType string, isAutoIncrement bool, err error) {
	postgresType = mysqlType
	isAutoIncrement = false

	if strings.Contains(strings.ToLower(mysqlType), "auto_increment") {
		isAutoIncrement = true
		mysqlType = strings.ReplaceAll(strings.ToLower(mysqlType), "auto_increment", "")
		mysqlType = strings.TrimSpace(mysqlType)
	}

	if reTinyInt1.MatchString(mysqlType) {
		postgresType = "BOOLEAN"
		return postgresType, isAutoIncrement, nil
	}

	if reJsonLength.MatchString(mysqlType) {
		postgresType = "JSON"
		return postgresType, isAutoIncrement, nil
	}

	mysqlType = reTypeMb3Direct.ReplaceAllString(mysqlType, "$1")
	mysqlType = reTypeMb3Any.ReplaceAllString(mysqlType, "$1")
	mysqlType = reTypeMb3Generic.ReplaceAllString(mysqlType, "$1")
	mysqlType = reMb3Suffix.ReplaceAllString(mysqlType, "")

	mysqlType = reCharsetFull.ReplaceAllString(mysqlType, "$1")
	mysqlType = reCharsetSimple.ReplaceAllString(mysqlType, "$1")
	mysqlType = reCollate.ReplaceAllString(mysqlType, "$1")
	mysqlType = reComplexCharset.ReplaceAllString(mysqlType, "$1")
	mysqlType = reComplexCharsetSpecific.ReplaceAllString(mysqlType, "$1")
	mysqlType = reComplexCharsetVarchar.ReplaceAllString(mysqlType, "$1")

	mysqlType = reMb4Suffix.ReplaceAllString(mysqlType, "$1")
	mysqlType = strings.TrimSpace(mysqlType)

	for _, mysqlTypeKey := range typeMappingOrder {
		if strings.Contains(strings.ToLower(mysqlType), strings.ToLower(mysqlTypeKey)) {
			if pattern, exists := typePatterns[strings.ToLower(mysqlTypeKey)]; exists && pattern.MatchString(mysqlType) {
				postgresType = mysqlType
			} else {
				postgresType = typeMap[mysqlTypeKey]
			}
			break
		}
	}

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
	line = strings.ReplaceAll(line, " ON UPDATE CURRENT_TIMESTAMP", "")
	line = strings.ReplaceAll(line, " unsigned", "")
	line = strings.ReplaceAll(line, " UNSIGNED", "")

	// 批量清理字符集和Collate
	replacements := []string{
		" COLLATE utf8mb4_unicode_ci", "", " COLLATE utf8_unicode_ci", "",
		" COLLATE utf8_general_ci", "", " COLLATE utf8mb4_bin", "",
		" COLLATE utf8_bin", "", " COLLATE utf8mb3_bin", "",
		" COLLATE utf8mb3_general_ci", "", " COLLATE utf32_bin", "",
		" COLLATE latin1_bin", "", " COLLATE latin1_swedish_ci", "",
		" COLLATE utf8mb4_0900_ai_ci", "",
		" character set utf8", "", " CHARACTER SET utf8", "",
		" character set utf8mb3", "", " CHARACTER SET utf8mb3", "",
		" character set latin1", "", " CHARACTER SET latin1", "",
		" character set utf16", "", " CHARACTER SET utf16", "",
		" charset=latin1", "", " CHARSET=latin1", "",
		" charset=utf16", "", " CHARSET=utf16", "",
		" charset=utf8mb3", "", " CHARSET=utf8mb3", "",
	}
	for i := 0; i < len(replacements); i += 2 {
		line = strings.ReplaceAll(line, replacements[i], replacements[i+1])
	}

	commentMatch := reComment.FindStringSubmatch(line)
	if commentMatch != nil {
		if commentMatch[1] != "" {
			columnComment = commentMatch[1]
		} else {
			columnComment = commentMatch[2]
		}
	}
	line = reComment.ReplaceAllString(line, "")
	line = strings.TrimSpace(line)
	line = strings.TrimSuffix(line, ",")
	line = strings.TrimSpace(line)

	if line == "" || line == ")" {
		isConstraint = true
		return
	}

	upperLine := strings.ToUpper(line)
	if strings.HasPrefix(upperLine, "CONSTRAINT") || strings.HasPrefix(upperLine, "KEY") || strings.HasPrefix(upperLine, "INDEX") {
		parts := strings.Fields(line)
		if len(parts) < 2 {
			isConstraint = true
			return
		}
		upperSecondPart := strings.ToUpper(parts[1])
		isDataType := false
		for _, t := range []string{"INT", "TEXT", "VARCHAR", "CHAR", "BOOLEAN", "DATE", "TIME", "TIMESTAMP", "DECIMAL", "DOUBLE", "FLOAT", "BLOB", "BYTEA", "JSON"} {
			if strings.Contains(upperSecondPart, t) {
				isDataType = true
				break
			}
		}
		if !isDataType {
			isConstraint = true
			return
		}
	}

	if strings.HasPrefix(line, `"`) {
		quoteEnd := strings.Index(line[1:], `"`)
		if quoteEnd != -1 {
			columnName = line[1 : quoteEnd+1]
			typeDefinition = strings.TrimSpace(line[quoteEnd+2:])
			if strings.Count(typeDefinition, "(") > strings.Count(typeDefinition, ")") {
				isIncompleteType = true
				return
			}
			if lowercaseColumns {
				columnName = strings.ToLower(columnName)
			}
		}
	} else {
		parts := strings.Fields(line)
		if len(parts) < 2 {
			isConstraint = true
			return
		}
		columnName = parts[0]
		typeDefinition = strings.Join(parts[1:], " ")
		if strings.Count(typeDefinition, "(") > strings.Count(typeDefinition, ")") {
			isIncompleteType = true
			return
		}
		if lowercaseColumns {
			columnName = strings.ToLower(columnName)
		}
	}

	return
}

// cleanTypeDefinition 清理和规范化类型定义
func cleanTypeDefinition(typeDefinition string) string {
	typeDefinition = reTypeMb3Direct.ReplaceAllString(typeDefinition, "$1")
	typeDefinition = reTypeMb3Any.ReplaceAllString(typeDefinition, "$1")
	typeDefinition = reMb3Suffix.ReplaceAllString(typeDefinition, "")
	typeDefinition = reCharsetFull.ReplaceAllString(typeDefinition, "$1")
	typeDefinition = reCharsetSimple.ReplaceAllString(typeDefinition, "$1")
	typeDefinition = reCollate.ReplaceAllString(typeDefinition, "$1")
	typeDefinition = reComplexCharsetSpecific.ReplaceAllString(typeDefinition, "$1")
	typeDefinition = reComplexCharsetVarchar.ReplaceAllString(typeDefinition, "$1")
	typeDefinition = reComplexCharset.ReplaceAllString(typeDefinition, "$1")

	replacements := []string{
		" character ascii", "", " CHARACTER ASCII", "",
		" collate ascii_general_ci", "", " COLLATE ASCII_GENERAL_CI", "",
	}
	for i := 0; i < len(replacements); i += 2 {
		typeDefinition = strings.ReplaceAll(typeDefinition, replacements[i], replacements[i+1])
	}

	lowerTypeDef := strings.ToLower(typeDefinition)

	// 批量移除字符集相关字符串
	charsetRemovals := []string{
		" character set utf8mb4", " character set utf8", " character set utf32",
		" character set utf8mb3", " character set gb2312",
		" collate utf8mb4_unicode_ci", " collate utf8mb4_general_ci",
		" collate utf8_unicode_ci", " collate utf8_general_ci",
		" collate utf32_bin", " collate utf8mb3_bin",
		" collate utf8mb3_general_ci", " collate utf8mb3_unicode_ci",
		" collate utf8mb4_0900_ai_ci", " collate gb2312_chinese_ci",
		" character utf8mb4", " character utf8",
		" character utf8mb3", " character gb2312",
	}
	for _, s := range charsetRemovals {
		lowerTypeDef = strings.ReplaceAll(lowerTypeDef, s, "")
	}

	lowerTypeDef = reTypeMb3Generic.ReplaceAllString(lowerTypeDef, "$1")
	lowerTypeDef = reTypeMb3Direct.ReplaceAllString(lowerTypeDef, "$1")
	lowerTypeDef = reDefaultEqual.ReplaceAllString(lowerTypeDef, "default ")

	lowerTypeDef = reCurrentTimestamp.ReplaceAllStringFunc(lowerTypeDef, func(m string) string {
		match := reCurrentTimestampExtract.FindStringSubmatch(m)
		if len(match) > 1 {
			return "CURRENT_TIMESTAMP(" + match[1] + ")"
		}
		return "CURRENT_TIMESTAMP"
	})

	lowerTypeDef = reMb4Suffix.ReplaceAllString(lowerTypeDef, "$1")
	lowerTypeDef = strings.ReplaceAll(lowerTypeDef, " unsigned", "")
	lowerTypeDef = strings.ReplaceAll(lowerTypeDef, " zerofill", "")

	// 应用类型映射
	for _, mysqlType := range typeMappingOrder {
		pgType, exists := typeMap[mysqlType]
		if !exists {
			continue
		}

		if mysqlType == "tinyint(1)" {
			lowerTypeDef = reTinyInt1.ReplaceAllString(lowerTypeDef, pgType)
			continue
		}

		if pattern, ok := typePatterns[mysqlType]; ok {
			lowerTypeDef = pattern.ReplaceAllStringFunc(lowerTypeDef, func(m string) string {
				match := pattern.FindStringSubmatch(m)
				if len(match) >= 2 {
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

		// 使用预编译的正则进行替换
		if re, ok := basicTypeRegexes[mysqlType]; ok {
			lowerTypeDef = re.ReplaceAllString(lowerTypeDef, pgType)
		}

		if mysqlType == "json" {
			lowerTypeDef = reJsonLength.ReplaceAllString(lowerTypeDef, "JSON")
		}
	}

	lowerTypeDef = reVarcharMissingParen.ReplaceAllStringFunc(lowerTypeDef, func(m string) string {
		if strings.Contains(lowerTypeDef, m+")") {
			return strings.ToUpper(m)
		}
		return strings.ToUpper(m) + ")"
	})

	lowerTypeDef = reExtraParens.ReplaceAllStringFunc(lowerTypeDef, func(m string) string {
		match := reExtraParens.FindStringSubmatch(m)
		if len(match) == 3 {
			return strings.ToUpper(match[1]) + "(" + strings.TrimSpace(match[2]) + ")"
		}
		return strings.ToUpper(m)
	})

	lowerTypeDef = reVarchar.ReplaceAllStringFunc(lowerTypeDef, func(m string) string { return strings.ToUpper(m) })
	lowerTypeDef = reEnum.ReplaceAllString(lowerTypeDef, "VARCHAR(255)")
	lowerTypeDef = reSet.ReplaceAllString(lowerTypeDef, "VARCHAR(255)")
	lowerTypeDef = reVarcharEnum.ReplaceAllString(lowerTypeDef, "VARCHAR(255)")
	lowerTypeDef = reVarcharZero.ReplaceAllString(lowerTypeDef, "VARCHAR(1)")
	lowerTypeDef = reDoublePrecision.ReplaceAllString(lowerTypeDef, "DOUBLE PRECISION")
	lowerTypeDef = reReal.ReplaceAllString(lowerTypeDef, "REAL")
	lowerTypeDef = reIntegerWithPrecision.ReplaceAllStringFunc(lowerTypeDef, func(m string) string {
		return strings.ToUpper(strings.Split(m, "(")[0])
	})
	lowerTypeDef = reBigSerial.ReplaceAllString(lowerTypeDef, "BIGSERIAL")
	lowerTypeDef = reSerial.ReplaceAllString(lowerTypeDef, "SERIAL")
	lowerTypeDef = reBytea.ReplaceAllString(lowerTypeDef, "BYTEA")
	lowerTypeDef = reJsonWithLength.ReplaceAllString(lowerTypeDef, "JSON")

	lowerTypeDef = strings.ReplaceAll(lowerTypeDef, " default null", "")
	lowerTypeDef = strings.ReplaceAll(lowerTypeDef, " default '0000-00-00 00:00:00'", "")
	lowerTypeDef = strings.ReplaceAll(lowerTypeDef, " default '0000-00-00 00:00:00.000000'", "")
	lowerTypeDef = strings.ReplaceAll(lowerTypeDef, " default '0000-00-00 00:00:00.000'", "")
	lowerTypeDef = strings.ReplaceAll(lowerTypeDef, " default '0000-00-00'", "")

	if strings.Contains(strings.ToUpper(lowerTypeDef), "GENERATED ALWAYS AS") {
		lowerTypeDef = reCharsetPrefix.ReplaceAllString(lowerTypeDef, "$1")
		lowerTypeDef = reVirtual.ReplaceAllString(lowerTypeDef, " STORED")
	}

	if strings.HasSuffix(lowerTypeDef, ",") {
		lowerTypeDef = strings.TrimSuffix(lowerTypeDef, ",")
	}

	lowerTypeDef = reBasicTypes.ReplaceAllStringFunc(lowerTypeDef, strings.ToUpper)
	return lowerTypeDef
}

// ConvertTableDDL 转换MySQL表DDL到PostgreSQL
func ConvertTableDDL(mysqlDDL string, lowercaseColumns bool) (*ConvertTableDDLResult, error) {
	mysqlDDL = strings.ReplaceAll(mysqlDDL, "`", "\"")

	columnNamesMap := make(map[string]string)
	columnCommentsMap := make(map[string]string)

	tableName, isTemporary, tableComment, columnsStart, columnsEnd, err := parseTableInfo(mysqlDDL)
	if err != nil {
		return nil, err
	}

	columnsDefinition := cleanTableLevelSettings(mysqlDDL[columnsStart:columnsEnd])
	lines := strings.Split(columnsDefinition, "\n")

	var columnDefinitions []string
	var primaryKeyColumn string
	columnNames := make(map[string]string)

	var incompleteTypeDef bool
	var partialTypeDef string
	var partialColumnName string

	for _, line := range lines {
		trimmedLine := strings.TrimSpace(line)

		if incompleteTypeDef {
			if strings.HasPrefix(trimmedLine, ")") && strings.HasSuffix(partialTypeDef, "(") {
				partialTypeDef += trimmedLine
			} else {
				partialTypeDef += " " + trimmedLine
			}
			if strings.Count(partialTypeDef, "(") == strings.Count(partialTypeDef, ")") {
				if lowercaseColumns {
					partialColumnName = strings.ToLower(partialColumnName)
				}
				trimmedLine = partialColumnName + " " + partialTypeDef
				incompleteTypeDef = false
				partialTypeDef = ""
				partialColumnName = ""
			} else {
				continue
			}
		}

		if trimmedLine == "" {
			continue
		}

		upperTrimmedLine := strings.ToUpper(trimmedLine)
		if strings.HasPrefix(strings.TrimSpace(upperTrimmedLine), "CONSTRAINT ") {
			continue
		}
		if strings.HasPrefix(upperTrimmedLine, "CONSTRAINT") || strings.HasPrefix(upperTrimmedLine, "FOREIGN KEY") {
			continue
		}

		if reIndexPattern.MatchString(upperTrimmedLine) ||
			strings.Contains(upperTrimmedLine, "FOREIGN KEY") ||
			strings.Contains(upperTrimmedLine, "USING BTREE") ||
			strings.Contains(upperTrimmedLine, "USING HASH") ||
			(strings.Contains(trimmedLine, "engine=") && !strings.Contains(trimmedLine, "`") && !strings.Contains(trimmedLine, " ")) ||
			(strings.Contains(trimmedLine, "ENGINE=") && !strings.Contains(trimmedLine, "`") && !strings.Contains(trimmedLine, " ")) ||
			(strings.Contains(trimmedLine, "row_format=") && !strings.Contains(trimmedLine, "`") && !strings.Contains(trimmedLine, " ")) ||
			(strings.Contains(trimmedLine, "ROW_FORMAT=") && !strings.Contains(trimmedLine, "`") && !strings.Contains(trimmedLine, " ")) {
			continue
		}

		if strings.HasPrefix(strings.ToUpper(trimmedLine), "PRIMARY KEY") {
			pkMatch := rePrimaryKey.FindStringSubmatch(trimmedLine)
			if len(pkMatch) > 1 {
				primaryKeyColumn = pkMatch[1]
			}
			continue
		}

		if strings.HasPrefix(strings.ToUpper(trimmedLine), "CONSTRAINT ") {
			continue
		}

		columnName, typeDefinition, columnComment, isConstraint, isIncompleteType, err := processColumnDefinition(trimmedLine, lowercaseColumns)
		if err != nil {
			return nil, err
		}

		if isConstraint {
			continue
		}

		if isIncompleteType {
			incompleteTypeDef = true
			partialTypeDef = typeDefinition
			partialColumnName = columnName
			continue
		}

		if typeDefinition == "" {
			return nil, fmt.Errorf("为表 %s 的列 %s 无法确定类型定义", tableName, columnName)
		}

		originalColumnName := columnName
		if lowercaseColumns {
			columnName = strings.ToLower(columnName)
		}

		columnNamesMap[originalColumnName] = fmt.Sprintf(`"%s"`, columnName)
		if columnComment != "" {
			columnCommentsMap[originalColumnName] = columnComment
		}
		columnNames[strings.ToLower(columnName)] = columnName

		if strings.Contains(typeDefinition, "AUTO_INCREMENT") {
			typeDefinition = strings.ReplaceAll(typeDefinition, "AUTO_INCREMENT", "")
			lowerTypeDef := strings.ToLower(typeDefinition)
			if strings.Contains(lowerTypeDef, "bigint") {
				replacements := []string{
					"bigint(20)", "BIGSERIAL", "BIGINT(20)", "BIGSERIAL",
					"bigint(11)", "BIGSERIAL", "BIGINT(11)", "BIGSERIAL",
					"bigint(32)", "BIGSERIAL", "BIGINT(32)", "BIGSERIAL",
					"bigint(24)", "BIGSERIAL", "BIGINT(24)", "BIGSERIAL",
					"bigint(128)", "BIGSERIAL", "BIGINT(128)", "BIGSERIAL",
					"BIGINT", "BIGSERIAL", "bigint", "BIGSERIAL",
				}
				for i := 0; i < len(replacements); i += 2 {
					typeDefinition = strings.ReplaceAll(typeDefinition, replacements[i], replacements[i+1])
				}
			} else {
				replacements := []string{
					"int(11)", "SERIAL", "INT(11)", "SERIAL",
					"int(4)", "SERIAL", "INT(4)", "SERIAL",
					"int(10)", "SERIAL", "INT(10)", "SERIAL",
					"int(32)", "SERIAL", "INT(32)", "SERIAL",
					"int(25)", "SERIAL", "INT(25)", "SERIAL",
					"INTEGER", "SERIAL", "int", "SERIAL",
				}
				for i := 0; i < len(replacements); i += 2 {
					typeDefinition = strings.ReplaceAll(typeDefinition, replacements[i], replacements[i+1])
				}
			}
		}

		typeDefinition = cleanTypeDefinition(typeDefinition)
		newColumnDefinition := fmt.Sprintf(`"%s" %s`, columnName, typeDefinition)
		columnDefinitions = append(columnDefinitions, newColumnDefinition)
	}

	var result strings.Builder
	if isTemporary {
		result.WriteString(fmt.Sprintf(`CREATE TEMPORARY TABLE "%s" (`, tableName))
	} else {
		result.WriteString(fmt.Sprintf(`CREATE TABLE "%s" (`, tableName))
	}

	for i, columnDef := range columnDefinitions {
		if i > 0 {
			result.WriteString(",")
		}
		result.WriteString(fmt.Sprintf(`%s`, columnDef))
	}

	if primaryKeyColumn != "" {
		if originalColumnName, ok := columnNames[strings.ToLower(primaryKeyColumn)]; ok {
			primaryKeyColumn = originalColumnName
			if lowercaseColumns {
				primaryKeyColumn = strings.ToLower(primaryKeyColumn)
			}
		}
		result.WriteString(fmt.Sprintf(`,  PRIMARY KEY ("%s")`, primaryKeyColumn))
	}

	result.WriteString(`)`)
	finalDDL := result.String()

	if (!strings.Contains(finalDDL, "CREATE TABLE") && !strings.Contains(finalDDL, "CREATE TEMPORARY TABLE")) || !strings.Contains(finalDDL, "(") || !strings.Contains(finalDDL, ")") {
		return nil, fmt.Errorf("生成的DDL无效: %s", finalDDL)
	}

	return &ConvertTableDDLResult{
		DDL:            finalDDL,
		TableComment:   tableComment,
		ColumnNames:    columnNamesMap,
		ColumnComments: columnCommentsMap,
	}, nil
}

// GenerateColumnCommentsSQL 生成PostgreSQL列注释SQL
func GenerateColumnCommentsSQL(tableName string, columnNamesMap, columnCommentsMap map[string]string) []string {
	var comments []string

	for originalColName, comment := range columnCommentsMap {
		processedComment := strings.ReplaceAll(comment, "'", "''")
		processedComment = strings.ReplaceAll(processedComment, "\r", "")
		processedComment = strings.ReplaceAll(processedComment, "\n", "")
		processedComment = strings.ReplaceAll(processedComment, "\t", "")
		processedComment = strings.ReplaceAll(processedComment, "\\n", "")

		if convertedColName, exists := columnNamesMap[originalColName]; exists {
			var commentSQL string
			if strings.HasPrefix(convertedColName, `"`) && strings.HasSuffix(convertedColName, `"`) {
				commentSQL = fmt.Sprintf("COMMENT ON COLUMN %s.%s IS '%s';", tableName, convertedColName, processedComment)
			} else {
				commentSQL = fmt.Sprintf("COMMENT ON COLUMN %s.\"%s\" IS '%s';", tableName, convertedColName, processedComment)
			}
			comments = append(comments, commentSQL)
		}
	}

	return comments
}
