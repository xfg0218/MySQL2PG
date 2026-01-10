package postgres

import (
	"fmt"
	"regexp"
	"strings"
)

// postgresReservedKeywords PostgreSQL保留关键字列表
var postgresReservedKeywords = map[string]bool{
	// 常用数据类型相关
	"type":        true,
	"class":       true,
	"select":      true,
	"insert":      true,
	"update":      true,
	"delete":      true,
	"from":        true,
	"where":       true,
	"and":         true,
	"or":          true,
	"not":         true,
	"in":          true,
	"is":          true,
	"null":        true,
	"default":     true,
	"primary":     true,
	"key":         true,
	"unique":      true,
	"foreign":     true,
	"references":  true,
	"check":       true,
	"constraint":  true,
	"table":       true,
	"column":      true,
	"view":        true,
	"index":       true,
	"sequence":    true,
	"function":    true,
	"procedure":   true,
	"trigger":     true,
	"schema":      true,
	"database":    true,
	"user":        true,
	"group":       true,
	"role":        true,
	"grant":       true,
	"revoke":      true,
	"create":      true,
	"alter":       true,
	"drop":        true,
	"truncate":    true,
	"begin":       true,
	"commit":      true,
	"rollback":    true,
	"savepoint":   true,
	"transaction": true,
	"lock":        true,
	"unlock":      true,
	"set":         true,
	"reset":       true,
	"show":        true,
	"describe":    true,
	"explain":     true,
	"analyze":     true,
	"vacuum":      true,
	"cluster":     true,
	"reindex":     true,
}

// isPostgresReservedKeyword 检查是否为PostgreSQL保留关键字
func isPostgresReservedKeyword(keyword string) bool {
	_, exists := postgresReservedKeywords[strings.ToLower(keyword)]
	return exists
}

type ConvertTableDDLResult struct {
	DDL            string
	TableComment   string
	ColumnNames    map[string]string // 键：原始列名，值：转换后的列名（带双引号格式）
	ColumnComments map[string]string // 键：原始列名，值：列注释
}

// ConvertTableDDL 转换MySQL表DDL到PostgreSQL
func ConvertTableDDL(mysqlDDL string, lowercaseColumns bool) (*ConvertTableDDLResult, error) {
	// 将MySQL DDL中的反引号替换为双引号
	mysqlDDL = strings.ReplaceAll(mysqlDDL, "`", "\"")

	// 创建列名映射：原始列名 → 转换后的列名
	columnNamesMap := make(map[string]string)
	// 创建列注释映射：原始列名 → 列注释
	columnCommentsMap := make(map[string]string)

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

	// 使用平衡括号算法来找到表定义的结束位置
	// 这样可以正确处理注释中的括号和嵌套括号
	var bracketCount int
	var inSingleQuote bool
	var inDoubleQuote bool
	var escapeNext bool

	// 将MySQL DDL转换为rune数组，以便正确处理中文字符
	mysqlDDLRunes := []rune(mysqlDDL)

	// 初始括号计数应为1，因为我们已经跳过了表定义的左括号
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
					// 找到表定义的结束位置
					// 将rune索引转换为字节索引
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
			return nil, fmt.Errorf("无法解析表DDL: 找不到右括号")
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
				// 完整的类型定义已经形成，使用这个完整的定义继续处理
				// 确保partialColumnName是正确的大小写（根据lowercaseColumns参数）
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

		// 首先检查是否是约束定义行
		// 约束定义以 CONSTRAINT 开头，或者是 FOREIGN KEY 约束
		// 必须在所有处理之前检查并跳过
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
		indexPattern := regexp.MustCompile(`^(KEY|INDEX|UNIQUE KEY|UNIQUE INDEX|"KEY"|"INDEX"|"UNIQUE KEY"|"UNIQUE INDEX")\s+(["a-zA-Z_]["a-zA-Z0-9_"]*)\s*\(["a-zA-Z_]`)
		// 只跳过明确是索引、外键或表级设置的行
		// 避免误判列定义为需要跳过的内容
		if indexPattern.MatchString(upperTrimmedLine) ||
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

		// 移除字符集和排序规则声明
		trimmedLine = strings.ReplaceAll(trimmedLine, " COLLATE utf8mb4_unicode_ci", "")
		trimmedLine = strings.ReplaceAll(trimmedLine, " COLLATE utf8_unicode_ci", "")
		trimmedLine = strings.ReplaceAll(trimmedLine, " COLLATE utf8_general_ci", "")
		trimmedLine = strings.ReplaceAll(trimmedLine, " COLLATE utf8mb4_bin", "")
		trimmedLine = strings.ReplaceAll(trimmedLine, " COLLATE utf8_bin", "")
		trimmedLine = strings.ReplaceAll(trimmedLine, " COLLATE utf8mb3_bin", "")
		trimmedLine = strings.ReplaceAll(trimmedLine, " COLLATE utf8mb3_general_ci", "")
		trimmedLine = strings.ReplaceAll(trimmedLine, " COLLATE utf32_bin", "")
		trimmedLine = strings.ReplaceAll(trimmedLine, " COLLATE latin1_bin", "")
		trimmedLine = strings.ReplaceAll(trimmedLine, " COLLATE latin1_swedish_ci", "")
		trimmedLine = strings.ReplaceAll(trimmedLine, " COLLATE utf8mb4_0900_ai_ci", "")
		trimmedLine = strings.ReplaceAll(trimmedLine, " character set utf8", "")
		trimmedLine = strings.ReplaceAll(trimmedLine, " CHARACTER SET utf8", "")
		trimmedLine = strings.ReplaceAll(trimmedLine, " character set utf8mb3", "")
		trimmedLine = strings.ReplaceAll(trimmedLine, " CHARACTER SET utf8mb3", "")
		trimmedLine = strings.ReplaceAll(trimmedLine, " character set latin1", "")
		trimmedLine = strings.ReplaceAll(trimmedLine, " CHARACTER SET latin1", "")
		trimmedLine = strings.ReplaceAll(trimmedLine, " character set utf16", "")
		trimmedLine = strings.ReplaceAll(trimmedLine, " CHARACTER SET utf16", "")
		trimmedLine = strings.ReplaceAll(trimmedLine, " charset=latin1", "")
		trimmedLine = strings.ReplaceAll(trimmedLine, " CHARSET=latin1", "")
		trimmedLine = strings.ReplaceAll(trimmedLine, " charset=utf16", "")
		trimmedLine = strings.ReplaceAll(trimmedLine, " CHARSET=utf16", "")
		trimmedLine = strings.ReplaceAll(trimmedLine, " charset=utf8mb3", "")
		trimmedLine = strings.ReplaceAll(trimmedLine, " CHARSET=utf8mb3", "")

		// 移除ON UPDATE CURRENT_TIMESTAMP
		trimmedLine = strings.ReplaceAll(trimmedLine, " ON UPDATE CURRENT_TIMESTAMP", "")

		// 移除unsigned关键字
		trimmedLine = strings.ReplaceAll(trimmedLine, " unsigned", "")
		trimmedLine = strings.ReplaceAll(trimmedLine, " UNSIGNED", "")

		// 解决mysql字段中有commitinfo字段无法转移的问题
		// 移除COMMENT子句，支持任意位置的COMMENT和转义的单引号
		reComment := regexp.MustCompile(`(?i)\s+comment\s+'((?:[^']|'')*)'\s*,?\s*|\s+comment\s+"([^"]*)"\s*,?\s*`)
		commentMatch := reComment.FindStringSubmatch(trimmedLine)
		var columnComment string
		if commentMatch != nil {
			// 捕获单引号或双引号中的注释内容
			if commentMatch[1] != "" {
				columnComment = commentMatch[1]
			} else {
				columnComment = commentMatch[2]
			}
		}
		trimmedLine = reComment.ReplaceAllString(trimmedLine, "")
		trimmedLine = strings.TrimSpace(trimmedLine)

		// 移除末尾的逗号
		trimmedLine = strings.TrimSuffix(trimmedLine, ",")
		trimmedLine = strings.TrimSpace(trimmedLine)

		// 如果处理后行内容为空或者只是右括号，跳过
		if trimmedLine == "" || trimmedLine == ")" {
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
				// 检查类型定义是否完整（括号是否匹配）
				if strings.Count(typeDefinition, "(") > strings.Count(typeDefinition, ")") {
					// 类型定义不完整，需要继续处理下一行
					incompleteTypeDef = true
					partialTypeDef = typeDefinition
					// 确保partialColumnName是正确的大小写（根据lowercaseColumns参数）
					if lowercaseColumns {
						partialColumnName = strings.ToLower(columnName)
					} else {
						partialColumnName = columnName
					}
					continue
				}
			}
		} else {
			// 检查是否是约束定义（包含CONSTRAINT关键字或其他约束类型）
			upperLine := strings.ToUpper(trimmedLine)
			// 检查是否以约束类型关键字开头
			isConstraintStart := strings.HasPrefix(upperLine, "PRIMARY KEY") ||
				strings.HasPrefix(upperLine, "FOREIGN KEY") ||
				(strings.HasPrefix(upperLine, "UNIQUE") && strings.Contains(trimmedLine, "("))

			// 特殊处理CONSTRAINT, KEY, INDEX开头的情况
			if strings.HasPrefix(upperLine, "CONSTRAINT") || strings.HasPrefix(upperLine, "KEY") || strings.HasPrefix(upperLine, "INDEX") {
				// 检查后面是否跟着数据类型（表示这是一个列名）
				// 先分割行，查看第二个单词是否是数据类型
				parts := strings.Fields(trimmedLine)
				if len(parts) < 2 {
					// 如果只有一个单词，跳过
					continue
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
					continue
				}
				// 如果是数据类型，说明这是一个列名，不跳过
			}

			// 检查是否是包含CONSTRAINT的约束定义（需要包含括号结构）
			hasConstraintWord := strings.Contains(upperLine, " CONSTRAINT ")
			isConstraintDefinition := hasConstraintWord && (strings.Contains(trimmedLine, "(") && strings.Contains(trimmedLine, ")"))

			// 如果是约束定义则跳过
			if isConstraintStart || isConstraintDefinition {
				continue
			}

			// 没有引号包围的列名，使用更健壮的正则表达式来解析
			// 匹配模式：列名 + 空格 + 类型定义（支持复杂的括号结构）
			// 这个正则表达式会尽可能精确地分离列名和类型定义
			re := regexp.MustCompile(`^([a-zA-Z0-9_]+)\s+(.+)$`)
			match := re.FindStringSubmatch(trimmedLine)
			if len(match) == 3 {
				columnName = strings.TrimSpace(match[1])
				typeDefinition = match[2]
				// 检查类型定义是否完整（括号是否匹配）
				if strings.Count(typeDefinition, "(") > strings.Count(typeDefinition, ")") {
					// 类型定义不完整，需要继续处理下一行
					incompleteTypeDef = true
					partialTypeDef = typeDefinition
					// 确保partialColumnName是正确的大小写（根据lowercaseColumns参数）
					if lowercaseColumns {
						partialColumnName = strings.ToLower(columnName)
					} else {
						partialColumnName = columnName
					}
					continue
				}
			} else {
				// 如果正则表达式匹配失败，使用传统的字段分割作为后备方案
				parts := strings.Fields(trimmedLine)
				if len(parts) < 2 {
					// 检查是否是PRIMARY KEY定义或其他特殊情况
					if strings.HasPrefix(strings.ToUpper(trimmedLine), "PRIMARY KEY") {
						// 处理PRIMARY KEY定义
						pkMatch := regexp.MustCompile(`PRIMARY KEY\s*\(\s*(\w+)\s*\)`).FindStringSubmatch(trimmedLine)
						if len(pkMatch) > 1 {
							primaryKeyColumn = pkMatch[1]
						}
						continue
					} else if (strings.HasPrefix(strings.ToUpper(trimmedLine), "KEY") || strings.HasPrefix(strings.ToUpper(trimmedLine), "INDEX")) && strings.Contains(trimmedLine, "(") {
						// 处理索引定义
						continue
					} else {
						// 无法解析行，返回错误
						return nil, fmt.Errorf("无法解析DDL行的列信息: %s", trimmedLine)
					}
				}
				// 检查第一个部分是否是约束关键字
				firstPartUpper := strings.ToUpper(parts[0])
				isConstraintKeyWord := firstPartUpper == "PRIMARY" ||
					firstPartUpper == "FOREIGN" || firstPartUpper == "UNIQUE"
				// 检查是否是PRIMARY KEY, FOREIGN KEY等约束定义
				if isConstraintKeyWord {
					continue
				}

				// 特殊处理CONSTRAINT, KEY和INDEX作为列名的情况
				if firstPartUpper == "CONSTRAINT" || firstPartUpper == "KEY" || firstPartUpper == "INDEX" {
					// 检查第二个单词是否是数据类型（表示这是一个列名）
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
						continue
					}
					// 如果是数据类型，说明这是一个列名，不跳过
				}
				columnName = parts[0]
				typeDefinition = strings.Join(parts[1:], " ")
				// 检查类型定义是否完整（括号是否匹配）
				if strings.Count(typeDefinition, "(") > strings.Count(typeDefinition, ")") {
					// 类型定义不完整，需要继续处理下一行
					incompleteTypeDef = true
					partialTypeDef = typeDefinition
					// 确保partialColumnName是正确的大小写（根据lowercaseColumns参数）
					if lowercaseColumns {
						partialColumnName = strings.ToLower(columnName)
					} else {
						partialColumnName = columnName
					}
					continue
				}
			}
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
		typeDefinition = regexp.MustCompile(`(?i)(VARCHAR\(\d+\)|CHAR\(\d+\)|TEXT)mb3`).ReplaceAllString(typeDefinition, "$1")

		// 匹配类型后面可能有空格或其他字符再跟mb3的情况
		typeDefinition = regexp.MustCompile(`(?i)(VARCHAR\(\d+\)|CHAR\(\d+\)|TEXT)[\s\S]*?mb3`).ReplaceAllString(typeDefinition, "$1")

		// 全局替换所有mb3后缀，确保没有遗漏
		typeDefinition = regexp.MustCompile(`(?i)mb3`).ReplaceAllString(typeDefinition, "")

		// 处理完整的CHARACTER SET语法
		typeDefinition = regexp.MustCompile(`(?i)(VARCHAR\(\d+\)|CHAR\(\d+\)|TEXT)\s*CHARACTER\s*SET\s*utf8mb3`).ReplaceAllString(typeDefinition, "$1")
		typeDefinition = regexp.MustCompile(`(?i)(VARCHAR\(\d+\)|CHAR\(\d+\)|TEXT)\s*CHARACTER\s*SET\s*ascii`).ReplaceAllString(typeDefinition, "$1")

		// 处理简化的CHARACTER语法
		typeDefinition = regexp.MustCompile(`(?i)(VARCHAR\(\d+\)|CHAR\(\d+\)|TEXT)\s*CHARACTER\s*utf8mb3`).ReplaceAllString(typeDefinition, "$1")
		typeDefinition = regexp.MustCompile(`(?i)(VARCHAR\(\d+\)|CHAR\(\d+\)|TEXT)\s*CHARACTER\s*ascii`).ReplaceAllString(typeDefinition, "$1")

		// 处理COLLATE语法
		typeDefinition = regexp.MustCompile(`(?i)(VARCHAR\(\d+\)|CHAR\(\d+\)|TEXT)\s*COLLATE\s*utf8mb3_\w+`).ReplaceAllString(typeDefinition, "$1")
		typeDefinition = regexp.MustCompile(`(?i)(VARCHAR\(\d+\)|CHAR\(\d+\)|TEXT)\s*COLLATE\s*ascii_\w+`).ReplaceAllString(typeDefinition, "$1")

		// 处理复杂的ascii字符集问题，如：char(255) character VARCHAR(255) ascii
		typeDefinition = regexp.MustCompile(`(?i)(char\(\d+\))\s*character\s+varchar\(\d+\)\s*ascii`).ReplaceAllString(typeDefinition, "$1")
		typeDefinition = regexp.MustCompile(`(?i)(varchar\(\d+\))\s*character\s+char\(\d+\)\s*ascii`).ReplaceAllString(typeDefinition, "$1")
		typeDefinition = regexp.MustCompile(`(?i)(char\(\d+\)|varchar\(\d+\)|text)\s*character\s+(char\(\d+\)|varchar\(\d+\))`).ReplaceAllString(typeDefinition, "$1")

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
			"varchar(30)":  "VARCHAR(30)",
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
			// JSON类型 - 确保正确转换
			"json":       "JSON",
			"json(1024)": "JSON",
			"json(2048)": "JSON",
			"json(4096)": "JSON",
			"json(8192)": "JSON",
			"jsonb":      "JSONB",
			// 处理ENUM类型，转换为VARCHAR
			"enum": "VARCHAR(255)",
			// 处理SET类型，转换为VARCHAR
			"set": "VARCHAR(255)",
			// 处理BINARY类型
			"binary(1)":  "BYTEA",
			"binary(2)":  "BYTEA",
			"binary(4)":  "BYTEA",
			"binary(16)": "BYTEA",
			"binary(32)": "BYTEA",
			"binary(64)": "BYTEA",
			// 处理VARBINARY类型
			"varbinary(1)":    "BYTEA",
			"varbinary(2)":    "BYTEA",
			"varbinary(4)":    "BYTEA",
			"varbinary(8)":    "BYTEA",
			"varbinary(16)":   "BYTEA",
			"varbinary(32)":   "BYTEA",
			"varbinary(6)":    "BYTEA",
			"varbinary(128)":  "BYTEA",
			"varbinary(255)":  "BYTEA",
			"varbinary(512)":  "BYTEA",
			"varbinary(1024)": "BYTEA",
			// 处理TEXT类型变体
			"text(1024)": "TEXT",
			"text(2048)": "TEXT",
			"text(4096)": "TEXT",
			"text(8192)": "TEXT",
		}

		// 应用类型映射 - 使用有序切片确保处理顺序，先处理更具体的类型
		typeMappingOrder := []string{
			// 整数类型 - 先处理带长度的具体类型
			"bigint(20)", "bigint(11)", "bigint(32)", "bigint(24)", "bigint(128)", "bigint(10)", "bigint(19)", "bigint",
			"biginteger(20)", "biginteger(255)", "biginteger(19)", "biginteger",
			"int(11)", "int(4)", "int(2)", "int(5)", "int(10)", "int(20)", "int(255)", "int(32)", "int(8)", "int(60)", "int(3)", "int(25)", "int(22)", "int",
			"integer(4)", "integer(2)", "integer(10)", "integer(20)", "integer(11)", "integer(22)", "integer",
			"smallinteger(1)", "smallinteger",
			"tinyinteger(1)", "tinyinteger",
			"tinyint(1)", "tinyint(4)", "tinyint(255)", "tinyint",
			"smallint(6)", "smallint(1)", "smallint",
			"mediumint(9)", "mediumint",
			// 浮点数类型 - 保留精度信息
			"decimal", "double precision", "double", "float",
			// 字符串类型 - 保留长度信息
			"char(1)", "varchar(255)", "varchar(256)", "varchar(64)", "varchar(20)", "varchar(30)", "varchar(100)", "varchar(50)", "varchar(128)", "varchar(500)", "varchar(200)", "varchar",
			"text", "longtext", "mediumtext", "tinytext",
			// 二进制类型
			"blob", "longblob", "mediumblob", "tinyblob", "binary", "varbinary", "varbinary(64)",
			// 日期时间类型 - 保留精度信息
			"datetime(6)", "datetime(3)", "datetime",
			"timestamp(6)", "timestamp(3)", "timestamp",
			"date", "time", "year",
			// JSON类型 - 包含变体
			"json(1024)", "json(2048)", "json(512)", "json", "jsonb",
			// 处理ENUM类型，转换为VARCHAR
			"enum",
			// 处理SET类型，转换为VARCHAR
			"set",
		}

		// 应用类型映射
		for _, mysqlType := range typeMappingOrder {
			pgType, exists := typeMap[mysqlType]
			if !exists {
				continue
			}

			// 特殊处理JSON类型变体（如json(1024)）
			if strings.HasPrefix(mysqlType, "json") {
				// 处理带长度的JSON类型变体
				if strings.Contains(mysqlType, "(") {
					// 对于具体的json(n)变体，使用精确匹配
					re = regexp.MustCompile(`(?i)\b` + regexp.QuoteMeta(mysqlType) + `\b`)
					lowerTypeDef = re.ReplaceAllString(lowerTypeDef, pgType)
				} else {
					// 对于json和jsonb，处理所有变体
					re = regexp.MustCompile(`(?i)\b` + regexp.QuoteMeta(mysqlType) + `\((\d+)\)\b`)
					lowerTypeDef = re.ReplaceAllString(lowerTypeDef, pgType)
					// 处理不带精度的JSON类型
					re = regexp.MustCompile(`(?i)\b` + regexp.QuoteMeta(mysqlType) + `\b`)
					lowerTypeDef = re.ReplaceAllString(lowerTypeDef, pgType)
				}
				// 特殊处理DECIMAL和其他需要保留精度的类型
			} else if strings.HasPrefix(mysqlType, "decimal") && mysqlType == "decimal" {
				// 匹配DECIMAL(precision, scale)格式
				re = regexp.MustCompile(`(?i)\bdecimal\((\d+)(?:,(\d+))?\)\b`)
				lowerTypeDef = re.ReplaceAllStringFunc(lowerTypeDef, func(m string) string {
					match := re.FindStringSubmatch(m)
					if len(match) >= 2 {
						if len(match) == 3 && match[2] != "" {
							// 保留精度和小数位数
							return fmt.Sprintf("DECIMAL(%s,%s)", match[1], match[2])
						} else {
							// 只保留精度
							return fmt.Sprintf("DECIMAL(%s)", match[1])
						}
					}
					return strings.ToUpper(m)
				})
			} else if strings.HasPrefix(mysqlType, "datetime") && mysqlType == "datetime" {
				// 匹配DATETIME(precision)格式
				re = regexp.MustCompile(`(?i)\bdatetime\((\d+)\)\b`)
				lowerTypeDef = re.ReplaceAllStringFunc(lowerTypeDef, func(m string) string {
					match := re.FindStringSubmatch(m)
					if len(match) >= 2 {
						// 转换为带精度的TIMESTAMP
						return fmt.Sprintf("TIMESTAMP(%s)", match[1])
					}
					return "TIMESTAMP"
				})
				// 处理不带精度的DATETIME类型
				re = regexp.MustCompile(`(?i)\bdatetime\b`)
				lowerTypeDef = re.ReplaceAllString(lowerTypeDef, "TIMESTAMP")
			} else if strings.HasPrefix(mysqlType, "timestamp") && mysqlType == "timestamp" {
				// 匹配TIMESTAMP(precision)格式
				re = regexp.MustCompile(`(?i)\btimestamp\((\d+)\)\b`)
				lowerTypeDef = re.ReplaceAllStringFunc(lowerTypeDef, func(m string) string {
					match := re.FindStringSubmatch(m)
					if len(match) >= 2 {
						// 保留精度
						return fmt.Sprintf("TIMESTAMP(%s)", match[1])
					}
					return "TIMESTAMP"
				})
			} else if strings.HasPrefix(mysqlType, "double") || strings.HasPrefix(mysqlType, "float") {
				// 保留DOUBLE和FLOAT类型的精度信息
				re = regexp.MustCompile(`(?i)\b` + regexp.QuoteMeta(mysqlType) + `\((\d+)(?:,(\d+))?\)\b`)
				if re.MatchString(lowerTypeDef) {
					lowerTypeDef = re.ReplaceAllStringFunc(lowerTypeDef, func(m string) string {
						match := re.FindStringSubmatch(m)
						if len(match) >= 2 {
							if len(match) == 3 && match[2] != "" {
								// 保留精度和小数位数
								if mysqlType == "double" || mysqlType == "double precision" {
									return fmt.Sprintf("DOUBLE PRECISION(%s,%s)", match[1], match[2])
								} else {
									return fmt.Sprintf("REAL(%s,%s)", match[1], match[2])
								}
							} else {
								// 只保留精度
								if mysqlType == "double" || mysqlType == "double precision" {
									return fmt.Sprintf("DOUBLE PRECISION(%s)", match[1])
								} else {
									return fmt.Sprintf("REAL(%s)", match[1])
								}
							}
						}
						// 如果没有匹配到精度信息，使用标准类型
						if mysqlType == "double" || mysqlType == "double precision" {
							return "DOUBLE PRECISION"
						} else {
							return "REAL"
						}
					})
				} else {
					// 没有精度信息，使用标准类型
					re = regexp.MustCompile(`(?i)\b` + regexp.QuoteMeta(mysqlType) + `\b`)
					if mysqlType == "double" || mysqlType == "double precision" {
						lowerTypeDef = re.ReplaceAllString(lowerTypeDef, "DOUBLE PRECISION")
					} else {
						lowerTypeDef = re.ReplaceAllString(lowerTypeDef, "REAL")
					}
				}
			} else if strings.HasPrefix(mysqlType, "varchar") || strings.HasPrefix(mysqlType, "char") {
				// 保留字符串类型的长度信息
				re = regexp.MustCompile(`(?i)\b` + regexp.QuoteMeta(mysqlType) + `\b`)
				lowerTypeDef = re.ReplaceAllStringFunc(lowerTypeDef, strings.ToUpper)
			} else {
				// 其他类型使用普通替换
				re = regexp.MustCompile(`(?i)\b` + regexp.QuoteMeta(mysqlType) + `\b`)
				lowerTypeDef = re.ReplaceAllString(lowerTypeDef, pgType)
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
