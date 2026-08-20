package postgres

import (
	"fmt"
	"regexp"
	"strconv"
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
	reMySQLCharsetClause     = regexp.MustCompile(`(?i)\s+(?:character\s+set|charset)\s*=?\s*[\w]+`)
	reMySQLCollateClause     = regexp.MustCompile(`(?i)\s+collate\s+[\w]+`)

	// 默认值处理相关正则
	reDefaultEqual            = regexp.MustCompile(`default\s*=\s*`)
	reCurrentTimestamp        = regexp.MustCompile(`current_timestamp\(\d+\)\(\d+\)`)
	reCurrentTimestampExtract = regexp.MustCompile(`current_timestamp\((\d+)\)`)

	// 类型映射相关正则
	reTinyInt1       = regexp.MustCompile(`(?i)\btinyint\(1\)\b`)
	reJsonLength     = regexp.MustCompile(`(?i)\bjson\((\d+)\)\b`)
	reJsonWithLength = regexp.MustCompile(`(?i)json\(\d+\)`)

	// 无符号类型提升相关正则：MySQL 无符号整数需提升为能容纳完整无符号范围的 PG 类型
	// bigint unsigned -> NUMERIC(20,0)，int unsigned -> BIGINT，smallint unsigned -> INTEGER
	// MySQL 中 ZEROFILL 隐含 UNSIGNED 语义，因此同样按无符号处理
	reBigintUnsigned   = regexp.MustCompile(`(?i)\bbigint(\(\d+\))?\s+(?:unsigned(?:\s+zerofill)?|zerofill)\b`)
	reSmallintUnsigned = regexp.MustCompile(`(?i)\bsmallint(\(\d+\))?\s+(?:unsigned(?:\s+zerofill)?|zerofill)\b`)
	reIntUnsigned      = regexp.MustCompile(`(?i)\b(?:int|integer)(\(\d+\))?\s+(?:unsigned(?:\s+zerofill)?|zerofill)\b`)
	// BIT(64) 最大值 18446744073709551615 超出 BIGINT 上限，需映射为 NUMERIC(20,0)
	// （MySQL BIT 宽度上限为 64，BIT(n<=63) 走标准 bit -> BIGINT 映射）
	reBit64 = regexp.MustCompile(`(?i)\bbit\(64\)`)

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
	reIndexPattern = regexp.MustCompile(`(?i)^(UNIQUE\s+)?(FULLTEXT\s+)?(KEY|INDEX)\s+`)
	rePrimaryKey   = regexp.MustCompile(`PRIMARY KEY\s*\(\s*(\w+)\s*\)`)

	// mb3相关正则
	reTypeMb3Generic = regexp.MustCompile(`(?i)(varchar\((\d+)\)|char\((\d+)\)|text)[^\w]*mb3`)

	// 其他杂项正则
	reCharsetPrefix = regexp.MustCompile(`(?i)\b_\w+(['"])`)
	reVirtual       = regexp.MustCompile(`(?i)\s+VIRTUAL`)
)

func extractPrimaryKeyColumns(line string) []string {
	openIdx := strings.Index(line, "(")
	closeIdx := strings.LastIndex(line, ")")
	if openIdx == -1 || closeIdx == -1 || closeIdx <= openIdx {
		return []string{}
	}

	content := strings.TrimSpace(line[openIdx+1 : closeIdx])
	if content == "" {
		return []string{}
	}

	parts := strings.Split(content, ",")
	var columns []string
	for _, part := range parts {
		col := strings.TrimSpace(part)
		// 先移除引号
		col = strings.Trim(col, `"`)
		col = strings.Trim(col, "`")
		// 移除列名后面的排序方向关键字 (ASC/DESC) 和右括号
		colParts := strings.Fields(col)
		if len(colParts) > 0 {
			col = colParts[0]
		}
		// 再次清理引号和右括号
		col = strings.Trim(col, `"`)
		col = strings.Trim(col, "`")
		col = strings.TrimRight(col, ")")
		if col != "" {
			columns = append(columns, col)
		}
	}
	return columns
}

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
	// 位字段类型（按无符号整数语义转换）
	"bit",
	// 日期时间类型
	// timestamp 必须先于 datetime 替换：datetime 生成的 TIMESTAMP/TIMESTAMP(n)
	// 否则会被 timestamp 的 (?i) 模式二次匹配误转为 TIMESTAMPTZ
	"timestamp", "datetime", "date", "time", "year",
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
	// 位字段类型（本质是无符号整数：BIT(n) 取值 0 ~ 2^n-1）
	// BIT(n<=63) 可由 BIGINT 容纳；BIT(64) 最大值 18446744073709551615 超出 BIGINT，
	// 由 cleanTypeDefinition 中的 reBit64 先行转为 NUMERIC(20,0)
	"bit": "BIGINT",
	// 日期时间类型
	// MySQL TIMESTAMP 内部按 UTC 存储、存取经会话时区转换，是带时区语义的类型 -> TIMESTAMPTZ；
	// DATETIME 为朴素时间（无时区）-> TIMESTAMP
	"datetime":  "TIMESTAMP",
	"timestamp": "TIMESTAMPTZ",
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
	PartitionDDLs  []string
	// Warnings 记录转换中发生的语义降级/丢弃说明（P1-20），
	// 由调用方汇入迁移报告的转换降级清单
	Warnings []string
	// CheckConstraints 记录 MySQL CHECK 约束对应的 PG ALTER TABLE ADD CHECK 语句（P1-02），
	// 由调用方在建表成功后执行
	CheckConstraints []string
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

	foundColumnsEnd := false
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
					foundColumnsEnd = true
					break
				}
			}
		}
		if foundColumnsEnd {
			break
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

type partitionRangeDefinition struct {
	name       string
	lessThan   string // RANGE 分区：VALUES LESS THAN (...)
	valuesIn   string // LIST 分区：VALUES IN (...)
	partitions int    // HASH/KEY 分区：PARTITIONS N
}

// PartitionInfo 存储完整的分区信息
type PartitionInfo struct {
	PartitionType  string                     // RANGE, LIST, HASH, KEY
	Expression     string                     // 分区表达式
	RangeDefs      []partitionRangeDefinition // RANGE 分区定义
	ListDefs       []partitionRangeDefinition // LIST 分区定义
	PartitionCount int                        // HASH/KEY 分区的分区数量

	// 子分区信息（PostgreSQL 不支持，降级处理）
	HasSubPartition   bool
	SubPartitionType  string // HASH 或 KEY
	SubPartitionExpr  string // 子分区表达式
	SubPartitionCount int    // 子分区数量
}

// findMatchingParen 查找与 openIdx 处左括号匹配的右括号位置
// 引号感知：字符串字面量（单/双引号，支持反斜杠与双写转义）内的括号不参与匹配
func findMatchingParen(input string, openIdx int) int {
	depth := 0
	inSingle := false
	inDouble := false
	for i := openIdx; i < len(input); i++ {
		ch := input[i]
		if inSingle {
			if ch == '\\' {
				i++
				continue
			}
			if ch == '\'' {
				if i+1 < len(input) && input[i+1] == '\'' {
					i++
					continue
				}
				inSingle = false
			}
			continue
		}
		if inDouble {
			if ch == '\\' {
				i++
				continue
			}
			if ch == '"' {
				if i+1 < len(input) && input[i+1] == '"' {
					i++
					continue
				}
				inDouble = false
			}
			continue
		}
		switch ch {
		case '\'':
			inSingle = true
		case '"':
			inDouble = true
		case '(':
			depth++
		case ')':
			depth--
			if depth == 0 {
				return i
			}
		}
	}
	return -1
}

// parsePartitionInfo 解析 MySQL 分区信息（支持 RANGE、LIST、HASH、KEY、SUBPARTITION）
func parsePartitionInfo(mysqlDDL string) *PartitionInfo {
	// 0. 首先尝试匹配 RANGE + SUBPARTITION 子分区语法
	subMatch := rePartitionCommentRangeSub.FindStringSubmatch(mysqlDDL)
	if len(subMatch) >= 6 {
		// 直接从正则表达式捕获组获取数据
		expr := strings.TrimSpace(strings.ReplaceAll(strings.TrimSpace(subMatch[1]), "\n", " "))
		subPartType := strings.ToUpper(strings.TrimSpace(subMatch[2]))
		subPartExpr := strings.TrimSpace(subMatch[3])
		subPartCount, _ := strconv.Atoi(strings.TrimSpace(subMatch[4]))
		defSection := strings.TrimSpace(subMatch[5])

		// 使用新的解析函数（支持 MAXVALUE）
		partitionDefs := parseRangePartitionDefinitions(defSection)
		if len(partitionDefs) > 0 {
			return &PartitionInfo{
				PartitionType:     "RANGE",
				Expression:        expr,
				RangeDefs:         partitionDefs,
				HasSubPartition:   true,
				SubPartitionType:  subPartType,
				SubPartitionExpr:  subPartExpr,
				SubPartitionCount: subPartCount,
			}
		}
		return &PartitionInfo{
			PartitionType:     "RANGE",
			Expression:        expr,
			HasSubPartition:   true,
			SubPartitionType:  subPartType,
			SubPartitionExpr:  subPartExpr,
			SubPartitionCount: subPartCount,
		}
	}

	// 1. 尝试从 /*!XXXXX ... */ 注释中提取 RANGE 分区信息
	rangeMatch := rePartitionCommentRange.FindStringSubmatch(mysqlDDL)
	if len(rangeMatch) >= 3 {
		// 直接从正则表达式捕获组获取表达式和分区定义部分
		// 表达式需要清理前后空白和换行
		expr := strings.TrimSpace(strings.ReplaceAll(strings.TrimSpace(rangeMatch[1]), "\n", " "))
		defSection := strings.TrimSpace(rangeMatch[2])

		// 解析 RANGE 分区定义（支持多行和 MAXVALUE）
		partitionDefs := parseRangePartitionDefinitions(defSection)
		if len(partitionDefs) > 0 {
			return &PartitionInfo{
				PartitionType: "RANGE",
				Expression:    expr,
				RangeDefs:     partitionDefs,
			}
		}
		return &PartitionInfo{
			PartitionType: "RANGE",
			Expression:    expr,
		}
	}

	// 2. 尝试从 /*!XXXXX ... */ 注释中提取 LIST 分区信息
	listMatch := rePartitionCommentList.FindStringSubmatch(mysqlDDL)
	if len(listMatch) >= 3 {
		// 重新提取表达式
		listByIdx := strings.Index(strings.ToUpper(mysqlDDL), "PARTITION BY LIST")
		if listByIdx != -1 {
			openParen := strings.Index(mysqlDDL[listByIdx:], "(")
			if openParen != -1 {
				openParen += listByIdx
				closeParen := findMatchingParen(mysqlDDL, openParen)
				if closeParen != -1 {
					expr := strings.TrimSpace(mysqlDDL[openParen+1 : closeParen])

					// 提取分区定义部分
					defStart := closeParen + 1
					defEnd := strings.LastIndex(mysqlDDL, ")")
					if defEnd > defStart {
						defSection := mysqlDDL[defStart:defEnd]

						// 解析 LIST 分区定义（支持多值列表）
						rePartitionDef := regexp.MustCompile(`(?is)PARTITION\s+"?([a-zA-Z0-9_]+)"?\s+VALUES\s+IN\s*\(([\s\S]+?)\)`)
						defMatches := rePartitionDef.FindAllStringSubmatch(defSection, -1)
						if len(defMatches) > 0 {
							partitionDefs := make([]partitionRangeDefinition, 0, len(defMatches))
							for _, defMatch := range defMatches {
								if len(defMatch) < 3 {
									continue
								}
								partitionDefs = append(partitionDefs, partitionRangeDefinition{
									name:     strings.TrimSpace(defMatch[1]),
									valuesIn: strings.TrimSpace(defMatch[2]),
								})
							}
							return &PartitionInfo{
								PartitionType: "LIST",
								Expression:    expr,
								ListDefs:      partitionDefs,
							}
						}
					}
					return &PartitionInfo{
						PartitionType: "LIST",
						Expression:    expr,
					}
				}
			}
		}
	}

	// 3. 尝试从 /*!XXXXX ... */ 注释中提取 HASH 分区信息
	hashMatches := rePartitionCommentHash.FindStringSubmatch(mysqlDDL)
	if len(hashMatches) >= 2 {
		expr := strings.TrimSpace(hashMatches[1])
		partitionCount := 0
		if len(hashMatches) >= 3 {
			partitionCount, _ = strconv.Atoi(strings.TrimSpace(hashMatches[2]))
		}
		return &PartitionInfo{
			PartitionType:  "HASH",
			Expression:     expr,
			PartitionCount: partitionCount,
		}
	}

	// 4. 尝试从 /*!XXXXX ... */ 注释中提取 KEY 分区信息
	keyMatches := rePartitionCommentKey.FindStringSubmatch(mysqlDDL)
	if len(keyMatches) >= 2 {
		expr := strings.TrimSpace(keyMatches[1])
		partitionCount := 0
		if len(keyMatches) >= 3 {
			partitionCount, _ = strconv.Atoi(strings.TrimSpace(keyMatches[2]))
		}
		return &PartitionInfo{
			PartitionType:  "KEY",
			Expression:     expr,
			PartitionCount: partitionCount,
		}
	}

	// 5. 注释中提取失败，使用原有逻辑搜索标准 PARTITION BY RANGE
	return parseRangePartitionInfoLegacy(mysqlDDL)
}

// parseRangePartitionDefinitions 解析 RANGE 分区定义（支持多行和 MAXVALUE）
// 支持两种语法：
// 1. VALUES LESS THAN (expr) - 有括号
// 2. VALUES LESS THAN MAXVALUE - 无括号
func parseRangePartitionDefinitions(defSection string) []partitionRangeDefinition {
	partitionDefs := []partitionRangeDefinition{}

	// 使用正则表达式查找所有 PARTITION 定义
	// 修改：\s* 允许零个或多个空白字符，支持 VALUES LESS THANMAXVALUE 的边界情况
	rePartitionStart := regexp.MustCompile(`(?i)PARTITION\s+"?([a-zA-Z0-9_]+)"?\s+VALUES\s+LESS\s+THAN\s*`)
	matches := rePartitionStart.FindAllStringSubmatchIndex(defSection, -1)

	if len(matches) == 0 {
		return nil
	}

	for i, match := range matches {
		name := defSection[match[2]:match[3]]
		startIdx := match[1] // VALUES LESS THAN 后的位置（可能包含空白）

		// 跳过可能的空白字符
		for startIdx < len(defSection) && (defSection[startIdx] == ' ' || defSection[startIdx] == '\n' || defSection[startIdx] == '\r' || defSection[startIdx] == '\t') {
			startIdx++
		}

		// 找到下一个 PARTITION 的位置或结尾
		endIdx := len(defSection)
		if i+1 < len(matches) {
			endIdx = matches[i+1][0]
		}

		// 提取 VALUES LESS THAN 后的内容
		valueStr := strings.TrimSpace(defSection[startIdx:endIdx])

		// 移除尾随的逗号
		valueStr = strings.TrimSuffix(valueStr, ",")
		valueStr = strings.TrimSpace(valueStr)

		// 处理括号或 MAXVALUE
		lessThan := valueStr
		if strings.HasPrefix(valueStr, "(") {
			// 左括号位置就是 startIdx
			openParenIdx := startIdx

			// 使用括号匹配找到闭合括号
			closeIdx := findMatchingParen(defSection, openParenIdx)
			if closeIdx != -1 {
				// 提取括号内的内容
				lessThan = strings.TrimSpace(defSection[openParenIdx+1 : closeIdx])
			}
		}
		// 否则就是 MAXVALUE，直接使用

		// 移除 ENGINE = xxx, DATA DIRECTORY = xxx, INDEX DIRECTORY = xxx, MAX_ROWS = xxx 等子句
		// 这些是 MySQL 特有的分区存储子句，PostgreSQL 不支持
		lessThan = stripPartitionStorageClause(lessThan)

		partitionDefs = append(partitionDefs, partitionRangeDefinition{
			name:     strings.TrimSpace(name),
			lessThan: strings.TrimSpace(lessThan),
		})
	}

	return partitionDefs
}

// stripPartitionStorageClause 移除 MySQL 分区定义中的存储子句
// 例如：ENGINE = InnoDB, DATA DIRECTORY = '/path', INDEX DIRECTORY = '/path', MAX_ROWS = 1000
func stripPartitionStorageClause(input string) string {
	// 定义需要移除的子句模式（不区分大小写）
	storagePatterns := []string{
		`(?i)\s+ENGINE\s*=\s*\w+`,
		`(?i)\s+DATA\s+DIRECTORY\s*=\s*'[^']*'`,
		`(?i)\s+INDEX\s+DIRECTORY\s*=\s*'[^']*'`,
		`(?i)\s+MAX_ROWS\s*=\s*\d+`,
		`(?i)\s+MIN_ROWS\s*=\s*\d+`,
		`(?i)\s+TABLESPACE\s*=\s*\w+`,
		`(?i)\s+STORAGE\s+DISK`,
		`(?i)\s+STORAGE\s+MEMORY`,
	}

	result := input
	for _, pattern := range storagePatterns {
		re := regexp.MustCompile(pattern)
		result = re.ReplaceAllString(result, "")
	}

	return strings.TrimSpace(result)
}

// parseRangePartitionInfoLegacy 旧版解析函数（保持向后兼容）
func parseRangePartitionInfoLegacy(mysqlDDL string) *PartitionInfo {
	upperDDL := strings.ToUpper(mysqlDDL)
	rangeIdx := strings.Index(upperDDL, "PARTITION BY RANGE")
	if rangeIdx == -1 {
		return nil
	}

	rangeSegment := mysqlDDL[rangeIdx:]
	rangeUpperSegment := upperDDL[rangeIdx:]

	rangeTokenIdx := strings.Index(rangeUpperSegment, "RANGE")
	if rangeTokenIdx == -1 {
		return nil
	}

	exprOpenIdx := strings.Index(rangeSegment[rangeTokenIdx+len("RANGE"):], "(")
	if exprOpenIdx == -1 {
		return nil
	}
	exprOpenIdx += rangeTokenIdx + len("RANGE")

	exprCloseIdx := findMatchingParen(rangeSegment, exprOpenIdx)
	if exprCloseIdx == -1 {
		return nil
	}
	expr := strings.TrimSpace(rangeSegment[exprOpenIdx+1 : exprCloseIdx])

	defOpenRel := strings.Index(rangeSegment[exprCloseIdx+1:], "(")
	if defOpenRel == -1 {
		return &PartitionInfo{
			PartitionType: "RANGE",
			Expression:    expr,
		}
	}
	defOpenIdx := exprCloseIdx + 1 + defOpenRel
	defCloseIdx := findMatchingParen(rangeSegment, defOpenIdx)
	if defCloseIdx == -1 {
		return &PartitionInfo{
			PartitionType: "RANGE",
			Expression:    expr,
		}
	}

	defSection := rangeSegment[defOpenIdx+1 : defCloseIdx]
	rePartitionDef := regexp.MustCompile(`(?is)PARTITION\s+"?([a-zA-Z0-9_]+)"?\s+VALUES\s+LESS\s+THAN\s*\(\s*([^)]+)\s*\)`)
	matches := rePartitionDef.FindAllStringSubmatch(defSection, -1)
	if len(matches) == 0 {
		return &PartitionInfo{
			PartitionType: "RANGE",
			Expression:    expr,
		}
	}

	partitions := make([]partitionRangeDefinition, 0, len(matches))
	for _, match := range matches {
		if len(match) < 3 {
			continue
		}
		// 移除 ENGINE = xxx 等存储子句
		lessThan := stripPartitionStorageClause(strings.TrimSpace(match[2]))
		partitions = append(partitions, partitionRangeDefinition{
			name:     strings.TrimSpace(match[1]),
			lessThan: lessThan,
		})
	}

	return &PartitionInfo{
		PartitionType: "RANGE",
		Expression:    expr,
		RangeDefs:     partitions,
	}
}

// 旧版 parseRangePartitionInfo 函数保留用于向后兼容
func parseRangePartitionInfo(mysqlDDL string) (string, []partitionRangeDefinition) {
	info := parsePartitionInfo(mysqlDDL)
	if info == nil {
		return "", nil
	}
	if info.PartitionType == "RANGE" {
		return info.Expression, info.RangeDefs
	}
	return "", nil
}

func convertPartitionExpression(mysqlExpr string) string {
	reYearExpr := regexp.MustCompile(`(?is)^\s*YEAR\s*\(\s*"?([a-zA-Z0-9_]+)"?\s*\)\s*$`)
	if match := reYearExpr.FindStringSubmatch(mysqlExpr); len(match) == 2 {
		return fmt.Sprintf(`EXTRACT(YEAR FROM "%s")`, match[1])
	}
	return strings.TrimSpace(mysqlExpr)
}

func normalizePartitionBound(bound string) string {
	trimmed := strings.TrimSpace(bound)
	if strings.EqualFold(trimmed, "MAXVALUE") {
		return "MAXVALUE"
	}
	return trimmed
}

// toLowerOutsideQuotes 将字符串中非引号包裹内容转换为小写
func toLowerOutsideQuotes(input string) string {
	var builder strings.Builder
	builder.Grow(len(input))

	inSingleQuote := false
	inDoubleQuote := false
	escapeNext := false

	for _, char := range input {
		if escapeNext {
			builder.WriteRune(char)
			escapeNext = false
			continue
		}

		switch char {
		case '\\':
			builder.WriteRune(char)
			escapeNext = true
		case '\'':
			if !inDoubleQuote {
				inSingleQuote = !inSingleQuote
			}
			builder.WriteRune(char)
		case '"':
			if !inSingleQuote {
				inDoubleQuote = !inDoubleQuote
			}
			builder.WriteRune(char)
		default:
			if inSingleQuote || inDoubleQuote {
				builder.WriteRune(char)
			} else {
				builder.WriteRune([]rune(strings.ToLower(string(char)))[0])
			}
		}
	}

	return builder.String()
}

// convertMySQLDateFormatToPostgres 生成列管道使用的日期格式转换：返回不带引号的格式串
// P1-12：内部复用统一实现 convertMySQLDateFormatToPG（sync_sql_literals.go）
func convertMySQLDateFormatToPostgres(mysqlFormat string) string {
	return strings.Trim(convertMySQLDateFormatToPG(mysqlFormat), "'")
}

// convertGeneratedFunctionsToPostgres 将生成列中的MySQL函数转换为PostgreSQL表达式
func convertGeneratedFunctionsToPostgres(typeDefinition string) string {
	reJSONUnquoteExtract := regexp.MustCompile(`(?is)json_unquote\s*\(\s*json_extract\s*\(\s*([^,]+?)\s*,\s*'\s*\$\.([^']+)\s*'\s*\)\s*\)`)
	typeDefinition = reJSONUnquoteExtract.ReplaceAllStringFunc(typeDefinition, func(m string) string {
		match := reJSONUnquoteExtract.FindStringSubmatch(m)
		if len(match) < 3 {
			return m
		}
		return fmt.Sprintf("(%s ->> '%s')", strings.TrimSpace(match[1]), strings.TrimSpace(match[2]))
	})

	reJSONExtract := regexp.MustCompile(`(?is)json_extract\s*\(\s*([^,]+?)\s*,\s*'\s*\$\.([^']+)\s*'\s*\)`)
	typeDefinition = reJSONExtract.ReplaceAllStringFunc(typeDefinition, func(m string) string {
		match := reJSONExtract.FindStringSubmatch(m)
		if len(match) < 3 {
			return m
		}
		return fmt.Sprintf("(%s -> '%s')", strings.TrimSpace(match[1]), strings.TrimSpace(match[2]))
	})

	reStrToDate := regexp.MustCompile(`(?is)str_to_date\s*\(\s*([^,]+?)\s*,\s*'([^']+)'\s*\)`)
	typeDefinition = reStrToDate.ReplaceAllStringFunc(typeDefinition, func(m string) string {
		match := reStrToDate.FindStringSubmatch(m)
		if len(match) < 3 {
			return m
		}
		pgFormat := convertMySQLDateFormatToPostgres(strings.TrimSpace(match[2]))
		return fmt.Sprintf("to_timestamp(%s, '%s')::timestamp", strings.TrimSpace(match[1]), pgFormat)
	})

	return typeDefinition
}

// shouldFallbackGeneratedToPlainColumn 判断是否需要将生成列降级为普通列
func shouldFallbackGeneratedToPlainColumn(typeDefinition string) bool {
	lowerType := strings.ToLower(typeDefinition)
	if !strings.Contains(lowerType, "generated always as") {
		return false
	}
	return strings.Contains(lowerType, "to_timestamp(")
}

// stripGeneratedClause 移除生成列子句，仅保留基础类型定义
func stripGeneratedClause(typeDefinition string) string {
	lowerType := strings.ToLower(typeDefinition)
	generatedIdx := strings.Index(lowerType, " generated always as")
	if generatedIdx == -1 {
		return strings.TrimSpace(typeDefinition)
	}
	return strings.TrimSpace(typeDefinition[:generatedIdx])
}

// isGeneratedColumnDefinition 判断字段定义是否为生成列定义
func isGeneratedColumnDefinition(typeDefinition string) bool {
	return strings.Contains(strings.ToLower(typeDefinition), "generated always as")
}

// extractGeneratedExpression 提取生成列表达式中的核心表达式
func extractGeneratedExpression(typeDefinition string) (string, bool) {
	lowerType := strings.ToLower(typeDefinition)
	generatedIdx := strings.Index(lowerType, "generated always as")
	if generatedIdx == -1 {
		return "", false
	}

	openRelIdx := strings.Index(typeDefinition[generatedIdx:], "(")
	if openRelIdx == -1 {
		return "", false
	}
	openIdx := generatedIdx + openRelIdx
	closeIdx := findMatchingParen(typeDefinition, openIdx)
	if closeIdx == -1 || closeIdx <= openIdx {
		return "", false
	}

	return strings.TrimSpace(typeDefinition[openIdx+1 : closeIdx]), true
}

// expandGeneratedExpressionDependencies 展开生成列表达式中对已生成列的依赖引用
func expandGeneratedExpressionDependencies(expression string, generatedExpressionMap map[string]string) string {
	expanded := expression
	changed := true

	for changed {
		changed = false
		for generatedColumn, generatedExpression := range generatedExpressionMap {
			quotedPattern := regexp.MustCompile(`(?i)"` + regexp.QuoteMeta(generatedColumn) + `"`)
			unquotedPattern := regexp.MustCompile(`(?i)\b` + regexp.QuoteMeta(generatedColumn) + `\b`)

			replacement := "(" + generatedExpression + ")"
			afterQuoted := quotedPattern.ReplaceAllString(expanded, replacement)
			if afterQuoted != expanded {
				expanded = afterQuoted
				changed = true
			}

			afterUnquoted := unquotedPattern.ReplaceAllString(expanded, replacement)
			if afterUnquoted != expanded {
				expanded = afterUnquoted
				changed = true
			}
		}
	}

	return expanded
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
		{" default charset=latin1", ""}, {" DEFAULT CHARSET=latin1", ""},
		{" default charset=gbk", ""}, {" DEFAULT CHARSET=gbk", ""},
		{" default charset=gb18030", ""}, {" DEFAULT CHARSET=gb18030", ""},
		{" default charset=big5", ""}, {" DEFAULT CHARSET=big5", ""},
		{" default charset=binary", ""}, {" DEFAULT CHARSET=binary", ""},
		{" default charset=ascii", ""}, {" DEFAULT CHARSET=ascii", ""},
		{" default charset=utf16", ""}, {" DEFAULT CHARSET=utf16", ""},
		{" default charset=utf32", ""}, {" DEFAULT CHARSET=utf32", ""},
		{" collate=utf8mb4_bin", ""}, {" COLLATE=utf8mb4_bin", ""},
		{" collate=utf8mb3_bin", ""}, {" COLLATE=utf8mb3_bin", ""},
		{" collate=utf8mb3_general_ci", ""}, {" COLLATE=utf8mb3_general_ci", ""},
		{" collate=utf8mb4_unicode_ci", ""}, {" COLLATE=utf8mb4_unicode_ci", ""},
		{" collate=utf8mb4_general_ci", ""}, {" COLLATE=utf8mb4_general_ci", ""},
		{" collate=utf8mb4_0900_ai_ci", ""}, {" COLLATE=utf8mb4_0900_ai_ci", ""},
		{" collate=utf8mb4_0900_as_cs", ""}, {" COLLATE=utf8mb4_0900_as_cs", ""},
		{" collate=latin1_swedish_ci", ""}, {" COLLATE=latin1_swedish_ci", ""},
		{" collate=latin1_general_ci", ""}, {" COLLATE=latin1_general_ci", ""},
		{" collate=gbk_chinese_ci", ""}, {" COLLATE=gbk_chinese_ci", ""},
		{" collate=gb18030_chinese_ci", ""}, {" COLLATE=gb18030_chinese_ci", ""},
		{" collate=big5_chinese_ci", ""}, {" COLLATE=big5_chinese_ci", ""},
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

// promoteUnsignedTypes 将 MySQL 无符号整数类型提升为能容纳完整无符号范围的等价类型，
// 替换结果仍走标准类型映射链（bigint -> BIGINT、numeric(20,0) -> NUMERIC(20,0) 等）
// bigint unsigned（0~18446744073709551615）超出 BIGINT 上限，提升为 numeric(20,0)
// int unsigned（0~4294967295）超出 INTEGER 上限，提升为 bigint
// smallint unsigned（0~65535）超出 SMALLINT 上限，提升为 int（映射为 INTEGER）
// tinyint unsigned（0~255）可由 SMALLINT 容纳、mediumint unsigned（0~16777215）可由 INTEGER 容纳，无需提升
func promoteUnsignedTypes(s string) string {
	s = reBigintUnsigned.ReplaceAllString(s, "numeric(20,0)")
	s = reSmallintUnsigned.ReplaceAllString(s, "int")
	s = reIntUnsigned.ReplaceAllString(s, "bigint")
	return s
}

// reDecimalUnsignedColumn 匹配 DECIMAL 系类型（decimal/numeric/float/double/real）
// 带 UNSIGNED/ZEROFILL 修饰的列定义；列名支持反引号、双引号（ConvertTableDDL
// 会将反引号统一替换为双引号）与无引号三种形式
var reDecimalUnsignedColumn = regexp.MustCompile("(?i)^\\s*[`\"]?[A-Za-z_$][A-Za-z0-9_$]*[`\"]?\\s+(?:double precision|decimal|numeric|float|double|real)\\s*(?:\\(\\d+(?:,\\s*\\d+)?\\))?[^,\\n]*\\b(?:unsigned|zerofill)\\b")

// isUnsignedDecimalLikeColumn 判断 MySQL 列定义是否为带 UNSIGNED/ZEROFILL 修饰的
// DECIMAL 系类型（decimal/numeric/float/double/real）
// PostgreSQL 没有无符号类型，整数系的 UNSIGNED 已由 promoteUnsignedTypes 提升类型覆盖，
// DECIMAL 系的非负语义无法通过类型表达，需在转换后补充 CHECK (col >= 0) 约束
func isUnsignedDecimalLikeColumn(columnLine string) bool {
	return reDecimalUnsignedColumn.MatchString(columnLine)
}

// reCheckIfnull CHECK 表达式中的 IFNULL 函数
var reCheckIfnull = regexp.MustCompile(`(?i)\bIFNULL\s*\(`)

// parseCheckConstraint 解析 MySQL CHECK 约束行，生成 PG ALTER TABLE ADD CONSTRAINT CHECK DDL（P1-02）
// 支持两种形式：CONSTRAINT `name` CHECK (expr) 与 CHECK (expr)
// 以独立 ALTER 语句形式执行（建表后追加），失败时仅告警不阻断建表
func parseCheckConstraint(line string, tableName string, lowercaseColumns bool) (string, bool) {
	upper := strings.ToUpper(line)
	checkIdx := strings.Index(upper, "CHECK")
	if checkIdx == -1 {
		return "", false
	}

	// 提取约束名（如有）
	var constraintName string
	prefix := strings.TrimSpace(line[:checkIdx])
	if prefix != "" {
		if !strings.HasPrefix(strings.ToUpper(prefix), "CONSTRAINT") {
			return "", false // 前缀不是 CONSTRAINT，不是合法的 CHECK 约束行
		}
		constraintName = strings.Trim(strings.TrimSpace(prefix[len("CONSTRAINT"):]), "`\" ")
	}

	// 定位 CHECK 表达式括号范围（引号感知的括号匹配）
	relOpen := strings.Index(line[checkIdx:], "(")
	if relOpen == -1 {
		return "", false
	}
	openIdx := checkIdx + relOpen
	endIdx := findMatchingParen(line, openIdx)
	if endIdx == -1 {
		return "", false
	}

	expr := convertCheckExpression(line[openIdx+1:endIdx], lowercaseColumns)
	quotedTable := quotePGIdentifier(tableName)
	if constraintName != "" {
		if lowercaseColumns {
			constraintName = strings.ToLower(constraintName)
		}
		return fmt.Sprintf("ALTER TABLE %s ADD CONSTRAINT %s CHECK (%s);", quotedTable, quotePGIdentifier(constraintName), expr), true
	}
	return fmt.Sprintf("ALTER TABLE %s ADD CHECK (%s);", quotedTable, expr), true
}

// reCheckBacktickIdent CHECK 表达式中的反引号标识符
var reCheckBacktickIdent = regexp.MustCompile("`([^`]+)`")

// reCheckQuotedIdent CHECK 表达式中的双引号标识符（入口已将反引号替换为双引号）
var reCheckQuotedIdent = regexp.MustCompile(`"([^"]+)"`)

// convertCheckExpression 转换 CHECK 表达式：反引号标识符转双引号、
// 双引号标识符按配置小写化（与建表列名保持一致）、IFNULL→COALESCE
func convertCheckExpression(expr string, lowercaseColumns bool) string {
	expr = reCheckBacktickIdent.ReplaceAllStringFunc(expr, func(m string) string {
		name := m[1 : len(m)-1]
		if lowercaseColumns {
			name = strings.ToLower(name)
		}
		return quotePGIdentifier(name)
	})
	if lowercaseColumns {
		expr = reCheckQuotedIdent.ReplaceAllStringFunc(expr, func(m string) string {
			return quotePGIdentifier(strings.ToLower(m[1 : len(m)-1]))
		})
	}
	return reCheckIfnull.ReplaceAllString(expr, "COALESCE(")
}

// reDefaultExpr MySQL 8.0 表达式默认值 DEFAULT (expr)
var reDefaultExpr = regexp.MustCompile(`(?i)\bDEFAULT\s*\(`)

// reUnsupportedDefaultExpr 表达式默认值中无法转换的 MySQL 专有语法
var reUnsupportedDefaultExpr = regexp.MustCompile(`(?i)\b(IF|ISNULL|CONCAT|DATE_FORMAT|STR_TO_DATE|YEAR|MONTH|DAY)\s*\(|@`)

// reDefaultUUID/ reDefaultNow 可映射的默认值函数
var reDefaultUUID = regexp.MustCompile(`(?i)\bUUID\s*\(\s*\)`)
var reDefaultNow = regexp.MustCompile(`(?i)\b(NOW|CURRENT_TIMESTAMP|LOCALTIME|LOCALTIMESTAMP)\s*\(\s*\)`)
var reDefaultCurDate = regexp.MustCompile(`(?i)\b(CURDATE|CURRENT_DATE)\s*\(\s*\)?`)

// convertExpressionDefault 处理 MySQL 8.0 表达式默认值 DEFAULT (expr)（P1-04）
// 可转换时保留 DEFAULT (PG 形式)；不可转换时剥离默认值并返回告警说明
func convertExpressionDefault(line string, lowercaseColumns bool) (string, string) {
	loc := reDefaultExpr.FindStringIndex(line)
	if loc == nil {
		return line, ""
	}
	relOpen := strings.Index(line[loc[0]:], "(")
	if relOpen == -1 {
		return line, ""
	}
	openIdx := loc[0] + relOpen
	endIdx := findMatchingParen(line, openIdx)
	if endIdx == -1 {
		return line, ""
	}

	expr := line[openIdx+1 : endIdx]
	prefix := strings.TrimSpace(line[:loc[0]])
	suffix := strings.TrimSpace(line[endIdx+1:])

	if reUnsupportedDefaultExpr.MatchString(expr) {
		rest := strings.TrimSpace(prefix + " " + suffix)
		return rest, fmt.Sprintf("表达式默认值 DEFAULT (%s) 含不可转换的 MySQL 语法，已剥离该默认值", expr)
	}

	converted := convertCheckExpression(expr, lowercaseColumns)
	converted = reDefaultUUID.ReplaceAllString(converted, "gen_random_uuid()")
	converted = reDefaultNow.ReplaceAllString(converted, "CURRENT_TIMESTAMP")
	converted = reDefaultCurDate.ReplaceAllString(converted, "CURRENT_DATE")

	rest := prefix + " DEFAULT (" + converted + ")"
	if suffix != "" {
		rest += " " + suffix
	}
	return rest, ""
}

// reSpatialType 空间类型列（P1-05 告警检测）
var reSpatialType = regexp.MustCompile(`(?i)\b(geometrycollection|geometry|linestring|multilinestring|multipoint|multipolygon|point|polygon)\b`)

// processColumnDefinition 处理列定义，提取列名、类型定义和注释
// 返回值 defaultWarning 非空表示表达式默认值被剥离（P1-04），由调用方记入转换警告
func processColumnDefinition(line string, lowercaseColumns bool) (columnName string, typeDefinition string, columnComment string, isConstraint bool, isIncompleteType bool, defaultWarning string, err error) {
	line = strings.ReplaceAll(line, " ON UPDATE CURRENT_TIMESTAMP", "")
	// 先提升无符号整数类型，再剥离残留的 unsigned/zerofill 修饰
	line = promoteUnsignedTypes(line)
	line = strings.ReplaceAll(line, " unsigned", "")
	line = strings.ReplaceAll(line, " UNSIGNED", "")

	// P1-04：处理 MySQL 8.0 表达式默认值 DEFAULT (expr)
	line, defaultWarning = convertExpressionDefault(line, lowercaseColumns)

	// 批量清理字符集和Collate
	replacements := []string{
		" COLLATE utf8mb4_unicode_ci", "", " COLLATE utf8_unicode_ci", "",
		" COLLATE utf8_general_ci", "", " COLLATE utf8mb4_bin", "",
		" COLLATE utf8mb4_general_ci", "",
		" COLLATE utf8_bin", "", " COLLATE utf8mb3_bin", "",
		" COLLATE utf8mb3_general_ci", "", " COLLATE utf32_bin", "",
		" COLLATE latin1_bin", "", " COLLATE latin1_swedish_ci", "",
		" COLLATE latin1_general_ci", "",
		" COLLATE utf8mb4_0900_ai_ci", "", " COLLATE utf8mb4_0900_as_cs", "",
		" COLLATE gbk_chinese_ci", "", " COLLATE gb18030_chinese_ci", "",
		" COLLATE big5_chinese_ci", "",
		" character set utf8", "", " CHARACTER SET utf8", "",
		" character set utf8mb4", "", " CHARACTER SET utf8mb4", "",
		" character set utf8mb3", "", " CHARACTER SET utf8mb3", "",
		" character set latin1", "", " CHARACTER SET latin1", "",
		" character set utf16", "", " CHARACTER SET utf16", "",
		" character set gbk", "", " CHARACTER SET gbk", "",
		" character set gb18030", "", " CHARACTER SET gb18030", "",
		" character set big5", "", " CHARACTER SET big5", "",
		" character set binary", "", " CHARACTER SET binary", "",
		" character set ascii", "", " CHARACTER SET ascii", "",
		" character set utf32", "", " CHARACTER SET utf32", "",
		" charset=utf8mb4", "", " CHARSET=utf8mb4", "",
		" charset=latin1", "", " CHARSET=latin1", "",
		" charset=utf16", "", " CHARSET=utf16", "",
		" charset=utf8mb3", "", " CHARSET=utf8mb3", "",
		" charset=gbk", "", " CHARSET=gbk", "",
		" charset=gb18030", "", " CHARSET=gb18030", "",
		" charset=big5", "", " CHARSET=big5", "",
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
	isKeyword := false
	if strings.HasPrefix(upperLine, "CONSTRAINT ") || strings.HasPrefix(upperLine, "CONSTRAINT(") {
		isKeyword = true
	} else if strings.HasPrefix(upperLine, "KEY ") || strings.HasPrefix(upperLine, "KEY(") {
		isKeyword = true
	} else if strings.HasPrefix(upperLine, "INDEX ") || strings.HasPrefix(upperLine, "INDEX(") {
		isKeyword = true
	} else if strings.HasPrefix(upperLine, "FULLTEXT KEY ") || strings.HasPrefix(upperLine, "FULLTEXT KEY(") || strings.HasPrefix(upperLine, "FULLTEXT INDEX ") || strings.HasPrefix(upperLine, "FULLTEXT INDEX(") {
		isKeyword = true
	}

	if isKeyword {
		parts := strings.Fields(line)
		if len(parts) < 2 {
			isConstraint = true
			return
		}
		upperSecondPart := strings.ToUpper(parts[1])
		isDataType := false
		for _, t := range []string{"BIGINT", "SMALLINT", "MEDIUMINT", "TINYINT", "INTEGER", "INT", "TEXT", "LONGTEXT", "MEDIUMTEXT", "TINYTEXT", "VARCHAR", "CHAR", "BOOLEAN", "DATE", "TIME", "TIMESTAMP", "DECIMAL", "DOUBLE", "FLOAT", "NUMERIC", "REAL", "BLOB", "BYTEA", "BINARY", "VARBINARY", "JSON", "ENUM", "SET"} {
			if strings.HasPrefix(upperSecondPart, t) {
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
	if strings.Contains(strings.ToLower(typeDefinition), "generated always as") {
		typeDefinition = reCharsetPrefix.ReplaceAllString(typeDefinition, "$1")
		typeDefinition = convertGeneratedFunctionsToPostgres(typeDefinition)
	}

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

	lowerTypeDef := toLowerOutsideQuotes(typeDefinition)
	lowerTypeDef = reMySQLCharsetClause.ReplaceAllString(lowerTypeDef, "")
	lowerTypeDef = reMySQLCollateClause.ReplaceAllString(lowerTypeDef, "")

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
	// 先提升无符号整数类型，再剥离残留的 unsigned/zerofill 修饰（兜底：确保任意调用路径都正确）
	lowerTypeDef = promoteUnsignedTypes(lowerTypeDef)
	lowerTypeDef = strings.ReplaceAll(lowerTypeDef, " unsigned", "")
	lowerTypeDef = strings.ReplaceAll(lowerTypeDef, " zerofill", "")
	// BIT(64) 超出 BIGINT 上限，先于标准 bit -> BIGINT 映射处理；
	// BIT(n<=63) 走标准映射后残留的 (n) 由 reIntegerWithPrecision 清理
	lowerTypeDef = reBit64.ReplaceAllString(lowerTypeDef, "NUMERIC(20,0)")

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
					case "datetime":
						return fmt.Sprintf("TIMESTAMP(%s)", match[1])
					case "timestamp":
						return fmt.Sprintf("TIMESTAMPTZ(%s)", match[1])
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

	// 类型映射后再次清理字符集残留（enum/set→VARCHAR 转换后可能暴露出 mb4 后缀）
	lowerTypeDef = reMySQLCharsetClause.ReplaceAllString(lowerTypeDef, "")
	lowerTypeDef = reMySQLCollateClause.ReplaceAllString(lowerTypeDef, "")
	lowerTypeDef = reMb4Suffix.ReplaceAllString(lowerTypeDef, "$1")

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
func ConvertTableDDL(mysqlDDL string, lowercaseColumns bool, distributedByColumns ...string) (*ConvertTableDDLResult, error) {
	mysqlDDL = strings.ReplaceAll(mysqlDDL, "`", "\"")

	columnNamesMap := make(map[string]string)
	columnCommentsMap := make(map[string]string)

	// 使用新的 parsePartitionInfo 函数，支持多种分区类型
	partitionInfo := parsePartitionInfo(mysqlDDL)

	tableName, isTemporary, tableComment, columnsStart, columnsEnd, err := parseTableInfo(mysqlDDL)
	if err != nil {
		return nil, err
	}

	columnsDefinition := cleanTableLevelSettings(mysqlDDL[columnsStart:columnsEnd])
	lines := strings.Split(columnsDefinition, "\n")

	var columnDefinitions []string
	var primaryKeyColumns []string
	columnNames := make(map[string]string)
	generatedExpressionMap := make(map[string]string)

	// P1-02：CHECK 约束收集为独立 ALTER 语句；warnings 收集语义降级/丢弃说明（P1-20）
	var checkConstraints []string
	var warnings []string

	// P1-05：空间类型列检测（PG 需 PostGIS 扩展，未安装时建表失败，提前告警）
	if reSpatialType.MatchString(mysqlDDL) {
		warnings = append(warnings, "表包含空间类型列（GEOMETRY 系）：PostgreSQL 需安装 PostGIS 扩展，否则建表失败")
	}

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
		// P1-02：CHECK 约束解析为独立的 ALTER TABLE ADD CONSTRAINT CHECK 语句
		// （外键约束仍然跳过，见分级清单 P1-01 后续排期）
		if strings.Contains(upperTrimmedLine, "CHECK") && !strings.Contains(upperTrimmedLine, "FOREIGN KEY") &&
			(strings.HasPrefix(strings.TrimSpace(upperTrimmedLine), "CONSTRAINT") || strings.HasPrefix(strings.TrimSpace(upperTrimmedLine), "CHECK")) {
			if ddl, ok := parseCheckConstraint(trimmedLine, tableName, lowercaseColumns); ok {
				checkConstraints = append(checkConstraints, ddl)
			}
			continue
		}
		if strings.HasPrefix(strings.TrimSpace(upperTrimmedLine), "CONSTRAINT ") {
			continue
		}
		if strings.HasPrefix(upperTrimmedLine, "CONSTRAINT") || strings.HasPrefix(upperTrimmedLine, "FOREIGN KEY") {
			continue
		}

		skipIndexLine := false
		if reIndexPattern.MatchString(upperTrimmedLine) {
			parts := strings.Fields(trimmedLine)
			if len(parts) < 2 {
				skipIndexLine = true
			} else {
				upperSecondPart := strings.ToUpper(parts[1])
				isDataType := false
				for _, t := range []string{"BIGINT", "SMALLINT", "MEDIUMINT", "TINYINT", "INTEGER", "INT", "TEXT", "LONGTEXT", "MEDIUMTEXT", "TINYTEXT", "VARCHAR", "CHAR", "BOOLEAN", "DATE", "TIME", "TIMESTAMP", "DECIMAL", "DOUBLE", "FLOAT", "NUMERIC", "REAL", "BLOB", "BYTEA", "BINARY", "VARBINARY", "JSON", "ENUM", "SET"} {
					if strings.HasPrefix(upperSecondPart, t) {
						isDataType = true
						break
					}
				}
				if !isDataType {
					skipIndexLine = true
				}
			}
		}

		// 先处理 PRIMARY KEY（即使包含 USING BTREE/HASH 也要提取主键列名）
		if strings.HasPrefix(strings.ToUpper(trimmedLine), "PRIMARY KEY") {
			pkColumns := extractPrimaryKeyColumns(trimmedLine)
			if len(pkColumns) == 0 {
				pkMatch := rePrimaryKey.FindStringSubmatch(trimmedLine)
				if len(pkMatch) > 1 {
					primaryKeyColumns = []string{pkMatch[1]}
				}
			} else {
				primaryKeyColumns = pkColumns
			}
			continue
		}

		if skipIndexLine ||
			strings.Contains(upperTrimmedLine, "FOREIGN KEY") ||
			strings.Contains(upperTrimmedLine, "USING BTREE") ||
			strings.Contains(upperTrimmedLine, "USING HASH") ||
			(strings.Contains(trimmedLine, "engine=") && !strings.Contains(trimmedLine, "`") && !strings.Contains(trimmedLine, " ")) ||
			(strings.Contains(trimmedLine, "ENGINE=") && !strings.Contains(trimmedLine, "`") && !strings.Contains(trimmedLine, " ")) ||
			(strings.Contains(trimmedLine, "row_format=") && !strings.Contains(trimmedLine, "`") && !strings.Contains(trimmedLine, " ")) ||
			(strings.Contains(trimmedLine, "ROW_FORMAT=") && !strings.Contains(trimmedLine, "`") && !strings.Contains(trimmedLine, " ")) {
			continue
		}

		if strings.HasPrefix(strings.ToUpper(trimmedLine), "CONSTRAINT ") {
			continue
		}

		// 在剥离 UNSIGNED 修饰前记录符号性：
		// DECIMAL 系 UNSIGNED 需在转换后补 CHECK (col >= 0)（PG 无无符号类型）
		isUnsignedDecimalLike := isUnsignedDecimalLikeColumn(trimmedLine)

		columnName, typeDefinition, columnComment, isConstraint, isIncompleteType, defaultWarning, err := processColumnDefinition(trimmedLine, lowercaseColumns)
		if defaultWarning != "" {
			warnings = append(warnings, fmt.Sprintf("列 %s: %s", columnName, defaultWarning))
		}
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
			if strings.Contains(lowerTypeDef, "numeric(20,0)") {
				// bigint unsigned 已提升为 NUMERIC(20,0)，自增列使用 BIGSERIAL
				// （已知限制：序列本身为 BIGINT 范围）
				typeDefinition = strings.ReplaceAll(typeDefinition, "NUMERIC(20,0)", "BIGSERIAL")
				typeDefinition = strings.ReplaceAll(typeDefinition, "numeric(20,0)", "BIGSERIAL")
			} else if strings.Contains(lowerTypeDef, "bigint") {
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
		if shouldFallbackGeneratedToPlainColumn(typeDefinition) {
			typeDefinition = stripGeneratedClause(typeDefinition)
		}
		if isGeneratedColumnDefinition(typeDefinition) {
			if rawExpression, ok := extractGeneratedExpression(typeDefinition); ok {
				expandedExpression := expandGeneratedExpressionDependencies(rawExpression, generatedExpressionMap)
				if expandedExpression != rawExpression {
					typeDefinition = strings.Replace(typeDefinition, rawExpression, expandedExpression, 1)
				}
				generatedExpressionMap[strings.ToLower(columnName)] = expandedExpression
			}
		}
		newColumnDefinition := fmt.Sprintf(`"%s" %s`, columnName, typeDefinition)
		if isUnsignedDecimalLike {
			// MySQL DECIMAL 系 UNSIGNED 的非负语义在 PostgreSQL 中以 CHECK 约束表达
			newColumnDefinition += fmt.Sprintf(` CHECK ("%s" >= 0)`, columnName)
		}
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

	if len(primaryKeyColumns) > 0 {
		var quotedCols []string
		for _, pkCol := range primaryKeyColumns {
			col := pkCol
			if originalColumnName, ok := columnNames[strings.ToLower(pkCol)]; ok {
				col = originalColumnName
			}
			if lowercaseColumns {
				col = strings.ToLower(col)
			}
			quotedCols = append(quotedCols, fmt.Sprintf(`"%s"`, col))
		}
		cols := strings.Join(quotedCols, ", ")
		result.WriteString(fmt.Sprintf(`,  PRIMARY KEY (%s)`, cols))
	}

	result.WriteString(`)`)

	// MPP 模式：添加 DISTRIBUTED BY 子句
	if len(distributedByColumns) > 0 {
		var quotedCols []string
		for _, col := range distributedByColumns {
			finalCol := col
			if lowercaseColumns {
				finalCol = strings.ToLower(finalCol)
			}
			quotedCols = append(quotedCols, fmt.Sprintf(`"%s"`, finalCol))
		}
		cols := strings.Join(quotedCols, ", ")
		result.WriteString(fmt.Sprintf(` DISTRIBUTED BY (%s)`, cols))
	}

	var partitionDDLs []string
	if !isTemporary && partitionInfo != nil && partitionInfo.Expression != "" {
		pgPartitionExpr := convertPartitionExpression(partitionInfo.Expression)

		// 根据分区类型生成不同的 PostgreSQL DDL
		switch partitionInfo.PartitionType {
		case "RANGE":
			// PostgreSQL 支持 RANGE 分区
			if len(partitionInfo.RangeDefs) > 0 {
				result.WriteString(fmt.Sprintf(` PARTITION BY RANGE (%s)`, pgPartitionExpr))

				// 处理子分区：PostgreSQL 不支持，记录警告并降级为普通分区
				if partitionInfo.HasSubPartition {
					// 子分区信息已记录，这里只生成主分区 DDL
					// 警告信息会在日志中输出
				}

				prevUpper := "MINVALUE"
				for _, partitionDef := range partitionInfo.RangeDefs {
					currentUpper := normalizePartitionBound(partitionDef.lessThan)
					partitionTableName := fmt.Sprintf(`"%s_%s"`, tableName, partitionDef.name)
					partitionDDL := fmt.Sprintf(`CREATE TABLE %s PARTITION OF "%s" FOR VALUES FROM (%s) TO (%s)`,
						partitionTableName, tableName, prevUpper, currentUpper)
					partitionDDLs = append(partitionDDLs, partitionDDL)
					prevUpper = currentUpper
				}
			}

		case "LIST":
			// PostgreSQL 支持 LIST 分区
			if len(partitionInfo.ListDefs) > 0 {
				result.WriteString(fmt.Sprintf(` PARTITION BY LIST (%s)`, pgPartitionExpr))
				for _, partitionDef := range partitionInfo.ListDefs {
					partitionTableName := fmt.Sprintf(`"%s_%s"`, tableName, partitionDef.name)
					partitionDDL := fmt.Sprintf(`CREATE TABLE %s PARTITION OF "%s" FOR VALUES IN (%s)`,
						partitionTableName, tableName, partitionDef.valuesIn)
					partitionDDLs = append(partitionDDLs, partitionDDL)
				}
			}

		case "HASH":
			// PostgreSQL 11+ 支持 HASH 分区，但语法不同：PARTITION BY HASH (expr)
			// 移除 PARTITIONS N 子句，只保留分区表达式
			result.WriteString(fmt.Sprintf(` PARTITION BY HASH (%s)`, pgPartitionExpr))
			// 注意：PostgreSQL 需要手动创建分区表，这里只转换主表语法

		case "KEY":
			// PostgreSQL 不支持 KEY 分区
			// KEY 分区是 MySQL 特有的哈希分区变体
			// 记录警告，不生成 PARTITION BY 子句
		}
	}
	finalDDL := result.String()

	if (!strings.Contains(finalDDL, "CREATE TABLE") && !strings.Contains(finalDDL, "CREATE TEMPORARY TABLE")) || !strings.Contains(finalDDL, "(") || !strings.Contains(finalDDL, ")") {
		return nil, fmt.Errorf("生成的DDL无效: %s", finalDDL)
	}

	return &ConvertTableDDLResult{
		DDL:              finalDDL,
		TableComment:     tableComment,
		ColumnNames:      columnNamesMap,
		ColumnComments:   columnCommentsMap,
		PartitionDDLs:    partitionDDLs,
		Warnings:         warnings,
		CheckConstraints: checkConstraints,
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
