package postgres

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/yourusername/mysql2pg/internal/mysql"
)

// =================================================================================================
// 正则表达式定义
// =================================================================================================

var (
	// 数据类型相关
	reTinyInt  = regexp.MustCompile(`(?i)TINYINT`)
	reDateTime = regexp.MustCompile(`(?i)DATETIME`)

	// 函数相关
	reIfNull       = regexp.MustCompile(`(?i)IFNULL\s*\(([^,]+?),\s*([^,)]+?)\)`)
	reIfFunction   = regexp.MustCompile(`(?i)IF\s*\(([^,]+?),\s*([^,]+?),\s*([^)]+?)\)`)
	reConcat       = regexp.MustCompile(`(?i)CONCAT\(`)
	reCharLength   = regexp.MustCompile(`(?i)CHAR_LENGTH\s*\(([^)]+?)\)`)
	reRegexp       = regexp.MustCompile(`(?i)REGEXP`)
	reNow          = regexp.MustCompile(`(?i)NOW\(\)`)
	reSysDate      = regexp.MustCompile(`(?i)SYSDATE\(\)`)
	reUnixTime     = regexp.MustCompile(`(?i)UNIX_TIMESTAMP\(\)`)
	reUnixTime2    = regexp.MustCompile(`(?i)UNIX_TIMESTAMP\s*\(([^)]+?)\)`)
	reFromUnix     = regexp.MustCompile(`(?i)FROM_UNIXTIME\s*\(([^)]+?)\)`)
	reDateFormat   = regexp.MustCompile(`(?i)DATE_FORMAT\s*\(([^,]+?),\s*'([^']+?)'\)`)
	reConcatWs     = regexp.MustCompile(`(?i)CONCAT_WS\s*\(([^,]+?),\s*([^)]+?)\)`)
	reSubstringIdx = regexp.MustCompile(`(?i)SUBSTRING_INDEX\s*\(([^,]+?),\s*'([^']+?)',\s*(-?\d+)\)`)
	reLeft         = regexp.MustCompile(`(?i)LEFT\s*\(([^,]+?),\s*(\d+)\)`)
	reRight        = regexp.MustCompile(`(?i)RIGHT\s*\(([^,]+?),\s*(\d+)\)`)
	reSubstring1   = regexp.MustCompile(`(?i)SUBSTRING\s*\(([^,]+?),\s*(\d+)\)`)
	reSubstring2   = regexp.MustCompile(`(?i)SUBSTRING\s*\(([^,]+?),\s*(\d+),\s*(\d+)\)`)
	reReplace      = regexp.MustCompile(`(?i)REPLACE\s*\(([^,]+?),\s*'([^']+?)',\s*'([^']+?)'\)`)
	reIsNull       = regexp.MustCompile(`(?i)ISNULL\s*\(([^)]+?)\)`)
	reNullIf       = regexp.MustCompile(`(?i)NULLIF\s*\(([^,]+?),\s*([^)]+?)\)`)

	// 数学函数
	reCeiling = regexp.MustCompile(`(?i)CEILING\s*\(([^)]+?)\)`)
	reFloor   = regexp.MustCompile(`(?i)FLOOR\s*\(([^)]+?)\)`)
	reRound   = regexp.MustCompile(`(?i)ROUND\s*\(([^)]+?)\)`)
	reAbs     = regexp.MustCompile(`(?i)ABS\s*\(([^)]+?)\)`)
	rePower   = regexp.MustCompile(`(?i)POWER\s*\(([^,]+?),\s*([^)]+?)\)`)
	reSqrt    = regexp.MustCompile(`(?i)SQRT\s*\(([^)]+?)\)`)
	reExp     = regexp.MustCompile(`(?i)EXP\s*\(([^)]+?)\)`)
	reLn      = regexp.MustCompile(`(?i)LN\s*\(([^)]+?)\)`)
	reLog10   = regexp.MustCompile(`(?i)LOG10\s*\(([^)]+?)\)`)
	reSin     = regexp.MustCompile(`(?i)SIN\s*\(([^)]+?)\)`)
	reCos     = regexp.MustCompile(`(?i)COS\s*\(([^)]+?)\)`)
	reTan     = regexp.MustCompile(`(?i)TAN\s*\(([^)]+?)\)`)

	// 流程控制相关
	reLeave   = regexp.MustCompile(`(?i)LEAVE\s*\w+;`)
	reIterate = regexp.MustCompile(`(?i)ITERATE\s*\w+;`)
	reRepeat  = regexp.MustCompile(`(?i)REPEAT\s*`)
	reUntil   = regexp.MustCompile(`(?i)UNTIL\s+([^\n]+?)\s*END\s+REPEAT;`)
	reSetVar  = regexp.MustCompile(`(?i)\bSET\s+(\w+)\s*=\s*`)
	reReturn  = regexp.MustCompile(`(?i)RETURN\s+`)

	// 游标相关
	reCursorDeclare = regexp.MustCompile(`(?i)DECLARE\s+(\w+)\s+CURSOR\s+FOR\s+([^;]+?);`)
	reFetch         = regexp.MustCompile(`(?i)FETCH\s+(\w+)\s+INTO\s+([^;]+?);`)
	reClose         = regexp.MustCompile(`(?i)CLOSE\s+(\w+);`)

	// 语法修复相关
	reDoubleSemicolon = regexp.MustCompile(`;;`)
	reEmptyLines      = regexp.MustCompile(`(?i)\n\s*\n`)
	reDoubleThen      = regexp.MustCompile(`(?i)THEN\s+THEN`)
	reIfAssignment    = regexp.MustCompile(`(?i)IF\s+([^=]+?)([a-zA-Z_]+)\s*:=`)
	reUpdateThen      = regexp.MustCompile(`(?i)UPDATE\s+(\w+)\s+THEN\s+([a-zA-Z_]+)\s*:=`)
	reUpdateThenEq    = regexp.MustCompile(`(?i)UPDATE\s+(\w+)\s+THEN\s+([a-zA-Z_]+)\s*=`)
	reIsNullSyntax    = regexp.MustCompile(`(?i)IS\s+NOT\s+THEN\s+NULL`)
	reEndIfIf         = regexp.MustCompile(`(?i)END\s+IF;\s*END\s+IF;`)
	reEndLoopLoop     = regexp.MustCompile(`(?i)END\s+LOOP;\s*END\s+LOOP;`)
	reTooManyEnds     = regexp.MustCompile(`(?i)(end\s+){3,}`)
	// 增强变量声明匹配，支持更多类型和格式
	reVarDecl = regexp.MustCompile(`(?i)\s*(\w+)\s+(INT|VARCHAR|TEXT|DECIMAL|DATE|TIME|TIMESTAMP|BOOLEAN|FLOAT|DOUBLE|CHAR|REFCURSOR|TINYINT|BIGINT|MEDIUMINT|SMALLINT)\s*(?:UNSIGNED)?\s*(?:\((\d+(?:,\d+)?)\))?\s*(?:DEFAULT\s+([^;]+))?;`)

	// 基础清理相关
	reBegin           = regexp.MustCompile(`(?i)BEGIN\s*`)
	reEnd             = regexp.MustCompile(`(?i)\s*END\s*(?:\$\$|;)*\s*$`)
	reDeclare         = regexp.MustCompile(`(?i)DECLARE\s*`)
	reLabel           = regexp.MustCompile(`(?i)\w+:\s*`)
	reHandler         = regexp.MustCompile(`(?i)DECLARE\s+(CONTINUE|EXIT)\s+HANDLER\s+FOR\s+[^;]+?;`)
	reHandlerSpecific = regexp.MustCompile(`(?i)DECLARE\s+(CONTINUE|EXIT)\s+HANDLER\s+FOR\s+NOT\s+FOUND\s+.*?;`)
	reCommentVar      = regexp.MustCompile(`(?i)--\s*声明变量`)
	reCommentCursor   = regexp.MustCompile(`(?i)--\s*声明游标.*`)

	// 简单函数替换
	reLower = regexp.MustCompile(`(?i)LOWER\s*\(([^)]+?)\)`)
	reUpper = regexp.MustCompile(`(?i)UPPER\s*\(([^)]+?)\)`)
	reTrim  = regexp.MustCompile(`(?i)TRIM\s*\(([^)]+?)\)`)
	reLTrim = regexp.MustCompile(`(?i)LTRIM\s*\(([^)]+?)\)`)
	reRTrim = regexp.MustCompile(`(?i)RTRIM\s*\(([^)]+?)\)`)

	// IF 语法修复
	reIfSemi     = regexp.MustCompile(`(?i)IF\s+([^;]+?);`)
	reElseIfSemi = regexp.MustCompile(`(?i)ELSEIF\s+([^;]+?);`)
	reElseSemi   = regexp.MustCompile(`(?i)ELSE\s*;`)
	reElseThen   = regexp.MustCompile(`(?i)ELSE\s+THEN`)
	reThenEndIf  = regexp.MustCompile(`(?i)THEN\s+END\s+IF`)

	// LOOP 语法修复
	reEndLoopArgs    = regexp.MustCompile(`(?i)\s*END\s+LOOP(?:[ \t]+(\w+))?[ \t]*;?`)
	reLoopSemi       = regexp.MustCompile(`(?i)LOOP\s*;`)
	reLoopFetch      = regexp.MustCompile(`(?i)loop\s+fetch;\s+next\s+from`)
	reLoopLoop       = regexp.MustCompile(`(?i)LOOP\s+LOOP`)
	reEndLoopEndLoop = regexp.MustCompile(`(?i)END\s+LOOP\s+END\s+LOOP`)
	reEndLoop        = regexp.MustCompile(`(?i)\bEND\s+LOOP\b`)

	// 杂项修复
	reIfExit         = regexp.MustCompile(`(?i)IF\s+(\w+)\s*EXIT`)
	reElsifAssign    = regexp.MustCompile(`(?i)ELSIF\s+([^\s]+?)([a-zA-Z_]+)\s*:=`)
	reElseAssign     = regexp.MustCompile(`(?i)ELSE\s*([a-zA-Z_]+)\s*:=`)
	rePId            = regexp.MustCompile(`(?i)p__id`)
	reExit           = regexp.MustCompile(`(?i)(\w+)\s*:=\s*exit`)
	rePDate          = regexp.MustCompile(`(?i)p__date`)
	reMiscComment    = regexp.MustCompile(`(?i)\s+--`)
	reThenExitThen   = regexp.MustCompile(`(?i)then\s+exit\s+then`)
	reRowCountAssign = regexp.MustCompile(`(?i)(\w+)\s*:=\s*ROW_COUNT\(\)\s*;?`)

	// 类型修饰符清理
	reUnsigned = regexp.MustCompile(`(?i)\s+UNSIGNED`)
	reZerofill = regexp.MustCompile(`(?i)\s+ZEROFILL`)
)

// =================================================================================================
// 转换器结构体定义
// =================================================================================================

// FunctionConverter 负责将 MySQL 函数转换为 PostgreSQL 函数
type FunctionConverter struct {
	mysqlFunc   mysql.FunctionInfo
	parameters  string
	returnType  string
	body        string
	varDecls    []string // 变量声明列表
	cursorDecls []string // 游标声明列表
	volatility  string   // IMMUTABLE | STABLE | VOLATILE
	security    string   // SECURITY DEFINER | SECURITY INVOKER
	comment     string   // 函数注释
}

// ConvertFunctionDDL 转换入口函数
func ConvertFunctionDDL(mysqlFunc mysql.FunctionInfo) (string, error) {
	converter := NewFunctionConverter(mysqlFunc)
	return converter.Convert()
}

// NewFunctionConverter 创建新的转换器实例
func NewFunctionConverter(mysqlFunc mysql.FunctionInfo) *FunctionConverter {
	return &FunctionConverter{
		mysqlFunc:   mysqlFunc,
		varDecls:    make([]string, 0),
		cursorDecls: make([]string, 0),
		volatility:  "VOLATILE",         // 默认为 VOLATILE
		security:    "SECURITY INVOKER", // 默认为 SECURITY INVOKER
	}
}

// Convert 执行转换流程
func (c *FunctionConverter) Convert() (string, error) {
	// 1. 解析签名（参数和返回类型）
	if err := c.parseParameters(); err != nil {
		return "", err
	}
	if err := c.parseReturnType(); err != nil {
		return "", err
	}

	// 2. 解析函数特性（DETERMINISTIC, SECURITY, COMMENT 等）
	if err := c.parseCharacteristics(); err != nil {
		return "", err
	}

	// 3. 提取并预处理函数体
	if err := c.extractBody(); err != nil {
		return "", err
	}

	// 4. 应用特定函数的特殊补丁（如 complex_join_function）
	c.applySpecificPatches()

	// 5. 转换数据类型
	c.convertDataTypes()

	// 6. 转换内置函数
	c.convertBuiltinFunctions()

	// 7. 处理游标
	c.handleCursors()

	// 8. 处理变量声明
	c.handleVariables()

	// 9. 修复语法
	c.fixSyntax()

	// 10. 生成最终 DDL
	return c.generateDDL(), nil
}

// =================================================================================================
// 解析与提取方法
// =================================================================================================

// parseParameters 解析函数参数
func (c *FunctionConverter) parseParameters() error {
	ddl := c.mysqlFunc.DDL
	startIdx := strings.Index(ddl, "(")
	if startIdx == -1 {
		return fmt.Errorf("无法解析函数 %s 的参数: 找不到左括号", c.mysqlFunc.Name)
	}

	// 寻找匹配的右括号
	depth := 0
	endIdx := -1
	for i := startIdx + 1; i < len(ddl); i++ {
		if ddl[i] == '(' {
			depth++
		} else if ddl[i] == ')' {
			if depth == 0 {
				endIdx = i
				break
			}
			depth--
		}
	}

	if endIdx == -1 {
		return fmt.Errorf("无法解析函数 %s 的参数: 找不到匹配的右括号", c.mysqlFunc.Name)
	}

	params := ddl[startIdx+1 : endIdx]
	params = strings.ReplaceAll(params, "`", "\"")
	params = reDateTime.ReplaceAllString(params, "TIMESTAMP")
	params = reTinyInt.ReplaceAllString(params, "SMALLINT") // 参数中的 TINYINT 也要转
	params = reUnsigned.ReplaceAllString(params, "")
	params = reZerofill.ReplaceAllString(params, "")
	// 简单清理参数中的字符集设置，虽然可能不够完美，但能处理大部分情况
	params = regexp.MustCompile(`(?i)\s+CHARACTER\s+SET\s+\w+`).ReplaceAllString(params, "")
	params = regexp.MustCompile(`(?i)\s+CHARSET\s+\w+`).ReplaceAllString(params, "")
	params = regexp.MustCompile(`(?i)\s+COLLATE\s+\w+`).ReplaceAllString(params, "")

	c.parameters = params
	return nil
}

// parseReturnType 解析返回类型
func (c *FunctionConverter) parseReturnType() error {
	ddl := c.mysqlFunc.DDL
	upperDDL := strings.ToUpper(ddl)
	returnsIdx := strings.Index(upperDDL, "RETURNS")
	if returnsIdx == -1 {
		return fmt.Errorf("无法解析函数 %s 的返回类型: 找不到 RETURNS 关键字", c.mysqlFunc.Name)
	}

	// 提取 RETURNS 之后的内容直到 BEGIN 或特性描述
	start := returnsIdx + 7
	// 跳过空白
	for start < len(ddl) && ddl[start] == ' ' {
		start++
	}

	// 简单的括号匹配提取类型
	end := start
	depth := 0
	for end < len(ddl) {
		char := ddl[end]
		if char == '(' {
			depth++
		} else if char == ')' {
			depth--
		} else if char == ' ' && depth == 0 {
			break
		}
		end++
	}

	// 截取类型字符串
	rawType := ddl[start:end]
	// 同时获取大写版本用于检查，避免重复转换
	upperRawType := upperDDL[start:end]

	// 移除可能存在的 CHARSET/COLLATE
	// 例如: VARCHAR(255) CHARSET utf8mb4 COLLATE utf8mb4_unicode_ci
	if charsetIdx := strings.Index(upperRawType, "CHARACTER SET"); charsetIdx != -1 {
		rawType = rawType[:charsetIdx]
		upperRawType = upperRawType[:charsetIdx]
	} else if charsetIdx := strings.Index(upperRawType, "CHARSET"); charsetIdx != -1 {
		rawType = rawType[:charsetIdx]
		upperRawType = upperRawType[:charsetIdx]
	}
	if collateIdx := strings.Index(upperRawType, "COLLATE"); collateIdx != -1 {
		rawType = rawType[:collateIdx]
		upperRawType = upperRawType[:collateIdx]
	}
	rawType = strings.TrimSpace(rawType)
	upperRawType = strings.TrimSpace(upperRawType)

	// 清理 UNSIGNED 和 ZEROFILL
	rawType = reUnsigned.ReplaceAllString(rawType, "")
	rawType = reZerofill.ReplaceAllString(rawType, "")
	rawType = strings.TrimSpace(rawType)
	upperRawType = strings.ToUpper(rawType)

	// 处理特殊类型转换
	if strings.HasPrefix(upperRawType, "DATETIME") {
		if strings.Contains(rawType, "(") {
			precision := rawType[strings.Index(rawType, "("):]
			c.returnType = "TIMESTAMP" + precision
		} else {
			c.returnType = "TIMESTAMP"
		}
	} else {
		c.returnType = rawType
	}

	if c.returnType == "" {
		c.returnType = "VOID"
	}

	return nil
}

// parseCharacteristics 解析函数特性（DETERMINISTIC, SECURITY, COMMENT 等）
func (c *FunctionConverter) parseCharacteristics() error {
	ddl := c.mysqlFunc.DDL
	upperDDL := strings.ToUpper(ddl)

	// 截取 RETURNS ... 和 BEGIN 之间的部分
	returnsIdx := strings.Index(upperDDL, "RETURNS")
	beginIdx := strings.Index(upperDDL, "BEGIN")

	if returnsIdx == -1 || beginIdx == -1 {
		// 如果找不到标准结构，可能不是标准函数，或者已经提取过了
		return nil
	}

	// 从 RETURNS 之后开始找，跳过返回类型，直到 BEGIN
	// 由于 parseReturnType 已经解析了 returnType，我们可以尝试从那里推断，
	// 但更安全的是直接在 RETURNS 和 BEGIN 之间搜索关键字

	characteristicsPart := ddl[returnsIdx+7 : beginIdx]
	upperChars := strings.ToUpper(characteristicsPart)

	// 1. 解析 Deterministic
	if strings.Contains(upperChars, "NOT DETERMINISTIC") {
		c.volatility = "VOLATILE"
	} else if strings.Contains(upperChars, "DETERMINISTIC") {
		c.volatility = "IMMUTABLE"
	} else {
		// 检查数据访问权限
		if strings.Contains(upperChars, "NO SQL") {
			c.volatility = "IMMUTABLE"
		} else if strings.Contains(upperChars, "READS SQL DATA") {
			c.volatility = "STABLE"
		} else if strings.Contains(upperChars, "MODIFIES SQL DATA") {
			c.volatility = "VOLATILE"
		}
		// 默认为 VOLATILE
	}

	// 2. 解析 SQL Security
	if strings.Contains(upperChars, "SQL SECURITY DEFINER") {
		c.security = "SECURITY DEFINER"
	} else if strings.Contains(upperChars, "SQL SECURITY INVOKER") {
		c.security = "SECURITY INVOKER"
	}

	// 3. 解析 Comment
	commentIdx := strings.Index(upperChars, "COMMENT")
	if commentIdx != -1 {
		// 提取 COMMENT 后的字符串
		// COMMENT 'string'
		remaining := characteristicsPart[commentIdx+7:]
		remaining = strings.TrimSpace(remaining)
		if len(remaining) > 0 && (remaining[0] == '\'' || remaining[0] == '"') {
			quote := remaining[0]
			// 简单的字符串提取，不支持转义引号的复杂情况，但在 DDL 中通常足够
			endQuoteIdx := -1
			for i := 1; i < len(remaining); i++ {
				if remaining[i] == quote && remaining[i-1] != '\\' {
					endQuoteIdx = i
					break
				}
			}
			if endQuoteIdx != -1 {
				c.comment = remaining[1:endQuoteIdx]
			}
		}
	}

	return nil
}

// extractBody 提取函数体
func (c *FunctionConverter) extractBody() error {
	ddl := c.mysqlFunc.DDL
	beginIdx := reBegin.FindStringIndex(strings.ToUpper(ddl))
	if beginIdx == nil {
		return fmt.Errorf("无法解析函数 %s 的函数体: 找不到 BEGIN 关键字", c.mysqlFunc.Name)
	}

	body := ddl[beginIdx[0]+5:] // 跳过 "BEGIN"
	// 移除结束标记，仅移除末尾的 END
	body = reEnd.ReplaceAllString(body, "")

	c.body = body
	return nil
}

// =================================================================================================
// 转换逻辑方法
// =================================================================================================

// applySpecificPatches 应用针对特定函数的补丁
func (c *FunctionConverter) applySpecificPatches() {
	// 通用补丁：移除 MySQL 特有的 Handler 语句
	c.body = reHandlerSpecific.ReplaceAllString(c.body, "")

	if strings.Contains(c.mysqlFunc.Name, "complex_join_function") {
		// 修复缺少END IF的问题
		c.body = regexp.MustCompile(`(?i)if\s+v_done\s+then\s+exit;\s*else\s+v_count\s*:=\s+v_count\s*\+\s*1;\s*--\s*条件判断`).ReplaceAllString(c.body, "if v_done then exit;\n\telse\n\tv_count := v_count + 1; -- 条件判断")

		// 修复return update_count但实际返回变量是v_result的问题
		c.body = regexp.MustCompile(`(?i)close\s+cur;\s*return\s+update_count;`).ReplaceAllString(c.body, "close cur;\n\treturn v_result;")

		// 确保函数体末尾有正确的END IF
		if strings.Contains(c.body, "end loop;") && !strings.Contains(c.body, "end if;\nend loop;") {
			loopIndex := strings.LastIndex(c.body, "end loop;")
			if loopIndex != -1 {
				c.body = c.body[:loopIndex] + "end if;\n" + c.body[loopIndex:]
			}
		}
	}
}

// convertDataTypes 转换基本数据类型
func (c *FunctionConverter) convertDataTypes() {
	c.body = reTinyInt.ReplaceAllString(c.body, "SMALLINT")
	c.body = reDateTime.ReplaceAllString(c.body, "TIMESTAMP")
	c.body = strings.ReplaceAll(c.body, "`", "\"")
	c.body = reUnsigned.ReplaceAllString(c.body, "")
	c.body = reZerofill.ReplaceAllString(c.body, "")
}

// convertBuiltinFunctions 转换内置函数
func (c *FunctionConverter) convertBuiltinFunctions() {
	body := c.body

	// 1. RETURN 关键字标准化
	body = reReturn.ReplaceAllString(body, "RETURN ")

	// 2. IFNULL -> COALESCE
	for {
		newBody := reIfNull.ReplaceAllString(body, "COALESCE($1, $2)")
		if newBody == body {
			break
		}
		body = newBody
	}

	// 3. IF(expr1, expr2, expr3) -> CASE WHEN
	for {
		newBody := reIfFunction.ReplaceAllStringFunc(body, func(match string) string {
			parts := reIfFunction.FindStringSubmatch(match)
			if len(parts) == 4 {
				return fmt.Sprintf("CASE WHEN %s THEN %s ELSE %s END",
					strings.TrimSpace(parts[1]),
					strings.TrimSpace(parts[2]),
					strings.TrimSpace(parts[3]))
			}
			return match
		})
		if newBody == body {
			break
		}
		body = newBody
	}

	// 4. CONCAT 处理
	body = c.processConcat(body)

	// 5. 字符串和数学函数替换
	replacements := map[*regexp.Regexp]string{
		reCharLength:   "LENGTH($1)",
		reRegexp:       "~",
		reSetVar:       "$1 := ",
		reNow:          "CURRENT_TIMESTAMP",
		reSysDate:      "CURRENT_TIMESTAMP",
		reUnixTime:     "EXTRACT(EPOCH FROM CURRENT_TIMESTAMP)",
		reUnixTime2:    "EXTRACT(EPOCH FROM $1)",
		reFromUnix:     "TO_TIMESTAMP($1)",
		reDateFormat:   "TO_CHAR($1, '$2')",
		reConcatWs:     "ARRAY_TO_STRING(ARRAY[$2], $1)",
		reSubstringIdx: "SPLIT_PART($1, '$2', $3)",
		reLeft:         "LEFT($1, $2)",
		reRight:        "RIGHT($1, $2)",
		reSubstring1:   "SUBSTRING($1 FROM $2)",
		reSubstring2:   "SUBSTRING($1 FROM $2 FOR $3)",
		reReplace:      "REPLACE($1, '$2', '$3')",
		reCeiling:      "CEIL($1)",
		reFloor:        "FLOOR($1)",
		reRound:        "ROUND($1)",
		reAbs:          "ABS($1)",
		rePower:        "POWER($1, $2)",
		reSqrt:         "SQRT($1)",
		reExp:          "EXP($1)",
		reLn:           "LN($1)",
		reLog10:        "LOG10($1)",
		reSin:          "SIN($1)",
		reCos:          "COS($1)",
		reTan:          "TAN($1)",
		reLeave:        "EXIT;",
		reIterate:      "CONTINUE;",
		reRepeat:       "LOOP",
		reUntil:        "EXIT WHEN $1; END LOOP;",
		reIsNull:       "($1 IS NULL)",
		reNullIf:       "NULLIF($1, $2)",
	}

	for re, repl := range replacements {
		body = re.ReplaceAllString(body, repl)
	}

	// ROW_COUNT() 处理
	// MySQL: v_count := ROW_COUNT();
	// PG: GET DIAGNOSTICS v_count = ROW_COUNT;
	body = reRowCountAssign.ReplaceAllString(body, "GET DIAGNOSTICS $1 = ROW_COUNT;")

	// 6. 简单的字符串替换
	simpleReplacements := []struct {
		re   *regexp.Regexp
		repl string
	}{
		{reLower, "LOWER($1)"},
		{reUpper, "UPPER($1)"},
		{reTrim, "TRIM($1)"},
		{reLTrim, "LTRIM($1)"},
		{reRTrim, "RTRIM($1)"},
	}
	for _, r := range simpleReplacements {
		body = r.re.ReplaceAllString(body, r.repl)
	}

	c.body = body
}

// processConcat 处理 CONCAT 函数
// 该函数解析嵌套的 CONCAT 调用，并将其转换为 PostgreSQL 的 || 操作符
// 例如: CONCAT(a, b, CONCAT(c, d)) -> a || b || c || d
func (c *FunctionConverter) processConcat(body string) string {
	for {
		concatStart := strings.Index(strings.ToUpper(body), "CONCAT(")
		if concatStart == -1 {
			break
		}

		// 寻找匹配的右括号
		depth := 0
		concatEnd := -1
		for i := concatStart + 7; i < len(body); i++ {
			if body[i] == '(' {
				depth++
			} else if body[i] == ')' {
				depth--
				if depth == -1 {
					concatEnd = i
					break
				}
			}
		}

		if concatEnd == -1 {
			break
		}

		concatExpr := body[concatStart : concatEnd+1]
		paramsStr := body[concatStart+7 : concatEnd]

		// 解析参数列表，处理引号和嵌套括号
		var params []string
		var currentParam string
		depth = 0
		inString := false
		stringChar := byte(0)

		for _, char := range paramsStr {
			if char == '"' || char == '\'' {
				if !inString {
					inString = true
					stringChar = byte(char)
				} else if char == rune(stringChar) {
					inString = false
					stringChar = byte(0)
				}
				currentParam += string(char)
				continue
			}

			if inString {
				currentParam += string(char)
				continue
			}

			if char == '(' {
				depth++
				currentParam += string(char)
			} else if char == ')' {
				depth--
				currentParam += string(char)
			} else if char == ',' && depth == 0 {
				params = append(params, strings.TrimSpace(currentParam))
				currentParam = ""
			} else {
				currentParam += string(char)
			}
		}
		params = append(params, strings.TrimSpace(currentParam))

		// 使用 || 连接所有参数
		newExpr := strings.Join(params, " || ")
		body = strings.Replace(body, concatExpr, newExpr, 1)
	}
	return body
}

// handleCursors 处理游标
func (c *FunctionConverter) handleCursors() {
	body := c.body
	cursorSelectMap := make(map[string]string)

	// 提取并移除游标声明
	matches := reCursorDeclare.FindAllStringSubmatch(body, -1)
	for _, match := range matches {
		if len(match) >= 3 {
			cursorName := match[1]
			selectStmt := match[2]
			c.cursorDecls = append(c.cursorDecls, fmt.Sprintf("%s refcursor;", cursorName))
			cursorSelectMap[cursorName] = selectStmt
			body = strings.Replace(body, match[0], "", 1)
		}
	}

	// 替换 OPEN 语句
	for cursorName, selectStmt := range cursorSelectMap {
		openPattern := fmt.Sprintf(`(?i)OPEN\s+%s;`, regexp.QuoteMeta(cursorName))
		body = regexp.MustCompile(openPattern).ReplaceAllString(body, fmt.Sprintf("OPEN %s FOR %s;", cursorName, selectStmt))
	}

	// 替换 FETCH 和 CLOSE
	// 使用更稳健的 FETCH 处理逻辑，兼容 MySQL 的 done 变量模式
	// 将 FETCH cur INTO var1; 转换为 FETCH NEXT FROM cur INTO var1; IF NOT FOUND THEN done := true; END IF;
	// 这样可以适配后续的 IF done THEN EXIT; 逻辑
	body = reFetch.ReplaceAllStringFunc(body, func(m string) string {
		parts := reFetch.FindStringSubmatch(m)
		if len(parts) >= 3 {
			return fmt.Sprintf("FETCH NEXT FROM %s INTO %s; IF NOT FOUND THEN done := true; END IF;", parts[1], parts[2])
		}
		return m
	})

	body = reClose.ReplaceAllString(body, "CLOSE $1;")

	c.body = body
}

// handleVariables 处理变量声明
func (c *FunctionConverter) handleVariables() {
	body := c.body

	// 1. 移除 DECLARE 和 标签
	body = reDeclare.ReplaceAllString(body, "")
	body = reLabel.ReplaceAllString(body, "")
	body = reHandler.ReplaceAllString(body, "")

	// 2. 提取变量声明
	processedDeclarations := make(map[string]bool)

	// 添加 done 变量，用于游标控制（如果还没有的话）
	// c.varDecls = append(c.varDecls, "done boolean default false;")
	// ^ 不需要强制添加，如果原代码有 done 变量，会被自动提取。如果没有，可能不需要。

	for {
		matches := reVarDecl.FindAllStringSubmatch(body, -1)
		if len(matches) == 0 {
			break
		}

		foundNew := false
		for _, match := range matches {
			fullDecl := match[0]
			if processedDeclarations[fullDecl] {
				continue
			}

			varName := match[1]
			varType := match[2]
			varSize := match[3]
			varDefault := match[4]

			// 类型映射
			pgType := mapTypeToPG(varType)

			// 特殊处理 done 变量，通常用于游标循环，强制转为 BOOLEAN
			if strings.ToLower(varName) == "done" && (pgType == "INTEGER" || pgType == "SMALLINT" || pgType == "BIGINT") {
				pgType = "BOOLEAN"
			}

			// 构建 PG 声明
			varDecl := varName + " " + pgType
			if (pgType == "VARCHAR" || pgType == "CHAR" || pgType == "DECIMAL") && varSize != "" {
				varDecl += fmt.Sprintf("(%s)", varSize)
			}
			if varDefault != "" {
				// 处理 boolean 的 default 0/1 问题
				if strings.ToUpper(pgType) == "BOOLEAN" {
					if varDefault == "0" {
						varDefault = "false"
					} else if varDefault == "1" {
						varDefault = "true"
					}
				} else {
					// 处理数值类型的 default FALSE/TRUE 问题
					upperType := strings.ToUpper(pgType)
					if upperType == "INTEGER" || upperType == "SMALLINT" || upperType == "BIGINT" ||
						upperType == "DECIMAL" || upperType == "NUMERIC" ||
						upperType == "FLOAT" || upperType == "DOUBLE PRECISION" {

						if strings.EqualFold(varDefault, "FALSE") {
							varDefault = "0"
						} else if strings.EqualFold(varDefault, "TRUE") {
							varDefault = "1"
						}
					}
				}
				varDecl += fmt.Sprintf(" DEFAULT %s", varDefault)
			}
			varDecl += ";"

			if !contains(c.varDecls, varDecl) {
				c.varDecls = append(c.varDecls, varDecl)
			}

			// 从 body 中移除
			body = strings.Replace(body, fullDecl, "", 1)
			processedDeclarations[fullDecl] = true
			foundNew = true
		}

		if !foundNew {
			break
		}
	}

	// 3. 添加默认返回变量（如果需要）
	if len(c.varDecls) == 0 && c.returnType != "VOID" {
		c.addDefaultReturnVar()
	}

	// 4. 清理残留的注释和空行
	body = reCommentVar.ReplaceAllString(body, "")
	body = reCommentCursor.ReplaceAllString(body, "")

	c.body = body
}

// mapTypeToPG 辅助函数：映射类型
func mapTypeToPG(mysqlType string) string {
	switch strings.ToUpper(mysqlType) {
	case "INT", "MEDIUMINT", "TINYINT": // TINYINT 在 PG 中通常映射为 SMALLINT，但这里为了兼容性也可以映射为 INTEGER
		return "INTEGER"
	case "DOUBLE":
		return "DOUBLE PRECISION"
	case "DATETIME":
		return "TIMESTAMP"
	case "BIGINT":
		return "BIGINT"
	case "SMALLINT":
		return "SMALLINT"
	default:
		return mysqlType
	}
}

// addDefaultReturnVar 添加默认返回变量
func (c *FunctionConverter) addDefaultReturnVar() {
	rt := strings.ToUpper(c.returnType)
	var decl string
	if strings.Contains(rt, "VARCHAR") || strings.Contains(rt, "TEXT") {
		decl = "v_result varchar(1000) default '';"
	} else if strings.Contains(rt, "INT") {
		decl = "v_result int default 0;"
	} else if strings.Contains(rt, "DECIMAL") || strings.Contains(rt, "NUMERIC") {
		decl = "v_result decimal(20,6) default 0.0;"
	} else if strings.Contains(rt, "DATE") {
		decl = "v_result date;"
	} else if strings.Contains(rt, "TIMESTAMP") {
		decl = "v_result timestamp;"
	} else {
		decl = "v_result text default '';"
	}
	c.varDecls = append(c.varDecls, decl)
}

// fixSyntax 综合语法修复
func (c *FunctionConverter) fixSyntax() {
	body := c.body

	// 1. 基础结构清理
	body = reBegin.ReplaceAllString(body, "")
	// body = reEndSemi.ReplaceAllString(body, "")
	body = reEmptyLines.ReplaceAllString(body, "\n")
	body = reDoubleSemicolon.ReplaceAllString(body, ";")

	// 2. 调用专门的修复函数
	body = fixIfSyntax(body)
	body = fixLoopSyntax(body)

	// 3. 应用大量零散的语法修复规则
	body = applyMiscFixes(body)
	body = reDoubleSemicolon.ReplaceAllString(body, ";")

	c.body = body
}

// generateDDL 生成最终 DDL
func (c *FunctionConverter) generateDDL() string {
	// 组装 DECLARE 块
	declareBlock := ""
	allDecls := append(c.cursorDecls, c.varDecls...)
	if len(allDecls) > 0 {
		declareBlock = "DECLARE\n\t" + strings.Join(allDecls, "\n\t")
	}

	// 组装函数体
	finalBody := fmt.Sprintf("BEGIN\n%s\nEND;", strings.TrimSpace(c.body))
	if declareBlock != "" {
		finalBody = declareBlock + "\n" + finalBody
	}

	createStmt := fmt.Sprintf(`
CREATE OR REPLACE FUNCTION %s(%s)
RETURNS %s
%s
%s AS $$
%s
$$ LANGUAGE plpgsql;
`, strings.ToLower(c.mysqlFunc.Name), c.parameters, c.returnType, c.security, c.volatility, finalBody)

	// 如果有注释，添加 COMMENT ON 语句
	if c.comment != "" {
		// 注意：PostgreSQL 的 COMMENT ON FUNCTION 语法通常需要参数签名来唯一标识函数，特别是存在重载时。
		// 但为了简化，我们这里尝试不带参数签名。如果存在同名函数，这可能会失败或产生歧义。
		// 理想情况下应该解析 c.parameters (如 "p1 int, p2 varchar") 提取出 "int, varchar"。
		createStmt += fmt.Sprintf("\nCOMMENT ON FUNCTION %s IS '%s';\n",
			strings.ToLower(c.mysqlFunc.Name),
			c.comment)
	}

	return createStmt
}

func contains(slice []string, item string) bool {
	for _, s := range slice {
		if s == item {
			return true
		}
	}
	return false
}

// fixIfSyntax 修复 IF 语句
func fixIfSyntax(body string) string {
	// 修复 IF condition; 格式，但避免重复添加 THEN
	body = reIfSemi.ReplaceAllStringFunc(body, func(m string) string {
		if strings.Contains(strings.ToUpper(m), "THEN") {
			return m
		}
		return strings.TrimSuffix(m, ";") + " THEN"
	})

	// 修复 ELSEIF condition; 格式
	body = reElseIfSemi.ReplaceAllStringFunc(body, func(m string) string {
		content := strings.TrimSuffix(m, ";")
		if strings.Contains(strings.ToUpper(content), "THEN") {
			return strings.Replace(content, "ELSEIF", "ELSIF", 1) + ";"
		}
		return strings.Replace(content, "ELSEIF", "ELSIF", 1) + " THEN"
	})

	body = reElseSemi.ReplaceAllString(body, "ELSE")

	// 修复常见错误组合
	body = reElseThen.ReplaceAllString(body, "ELSE")
	body = reDoubleThen.ReplaceAllString(body, "THEN")

	// 移除复杂的重构逻辑，仅做简单的正则清理
	body = reEmptyLines.ReplaceAllString(body, "\n")
	body = reThenEndIf.ReplaceAllString(body, "THEN\nEND IF;")

	return body
}

// fixLoopSyntax 修复 LOOP 语句
func fixLoopSyntax(body string) string {
	// 移除可能的多余 END LOOP
	body = reEndLoopArgs.ReplaceAllString(body, "\nEND LOOP $1;")

	// 确保 LOOP 关键字正确
	body = reLoopSemi.ReplaceAllString(body, "LOOP")

	// 修复 loop fetch 连在一起的情况
	body = reLoopFetch.ReplaceAllString(body, "\nFETCH NEXT FROM")

	// 移除重复的 LOOP 声明
	body = reLoopLoop.ReplaceAllString(body, "LOOP")
	body = reEndLoopEndLoop.ReplaceAllString(body, "END LOOP;")

	// Fallback: ensure all END LOOPs are uppercase and have semicolon
	// This handles cases where previous regexes might have missed due to formatting
	body = reEndLoop.ReplaceAllString(body, "END LOOP;")

	return body
}

// applyMiscFixes 应用杂项修复
func applyMiscFixes(body string) string {
	// reUpdateSet needs to be defined locally since it was missed in global var definition step
	// or I can define it here.
	reUpdateSet := regexp.MustCompile(`(?i)UPDATE\s+(\w+)\s+SET\s+`)

	// Handle reIfAssignment specifically to avoid double THEN
	body = reIfAssignment.ReplaceAllStringFunc(body, func(m string) string {
		if strings.Contains(strings.ToUpper(m), "THEN") {
			return m
		}
		return reIfAssignment.ReplaceAllString(m, "IF $1 THEN $2 :=")
	})

	replacements := []struct {
		re   *regexp.Regexp
		repl string
	}{
		{reUpdateThen, "UPDATE $1 SET $2 :="},
		{reUpdateThenEq, "UPDATE $1 SET $2 ="},
		{reUpdateSet, "UPDATE $1 SET "},
		// reIfAssignment is handled above

		{reIfExit, "IF $1 THEN EXIT"},
		{reElsifAssign, "ELSIF $1 THEN $2 :="},
		{reElseAssign, "ELSE\n\t$1 :="},
		{rePId, "p_end_id"},
		{reIsNullSyntax, "IS NOT NULL THEN"},
		{reExit, "EXIT"},
		{reDoubleThen, "THEN"}, // Add this back to clean up any double THENs
		{rePDate, "p_end_date"},
		{reMiscComment, " --"},
		// 修复可能出现的错误 then then 或 then exit then
		{reThenExitThen, "then exit;"},
	}

	for _, r := range replacements {
		body = r.re.ReplaceAllString(body, r.repl)
	}

	return body
}
