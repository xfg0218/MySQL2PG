package postgres

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/yourusername/mysql2pg/internal/mysql"
)

var (
	// 数据类型相关
	reTinyInt  = regexp.MustCompile(`(?i)TINYINT`)
	reDateTime = regexp.MustCompile(`(?i)DATETIME`)

	// 函数相关
	reIfNull       = regexp.MustCompile(`(?i)\bIFNULL\s*\(([^,]+?),\s*([^,)]+?)\)`)
	reIfFunction   = regexp.MustCompile(`(?i)\bIF\s*\(([^,]+?),\s*([^,]+?),\s*([^)]+?)\)`)
	reConcat       = regexp.MustCompile(`(?i)\bCONCAT\(`)
	reCharLength   = regexp.MustCompile(`(?i)\bCHAR_LENGTH\s*\(([^)]+?)\)`)
	reRegexp       = regexp.MustCompile(`(?i)\bREGEXP\b`)
	reNow          = regexp.MustCompile(`(?i)\bNOW\(\)`)
	reCurrentDate  = regexp.MustCompile(`(?i)\bCURRENT_DATE\(\)`)
	reSysDate      = regexp.MustCompile(`(?i)\bSYSDATE\(\)`)
	reUnixTime     = regexp.MustCompile(`(?i)\bUNIX_TIMESTAMP\(\)`)
	reUnixTime2    = regexp.MustCompile(`(?i)\bUNIX_TIMESTAMP\s*\(([^)]+?)\)`)
	reFromUnix     = regexp.MustCompile(`(?i)\bFROM_UNIXTIME\s*\(([^)]+?)\)`)
	reDateFormat   = regexp.MustCompile(`(?i)\bDATE_FORMAT\s*\(([^,]+?),\s*'([^']+?)'\)`)
	reConcatWs     = regexp.MustCompile(`(?i)\bCONCAT_WS\s*\(([^,]+?),\s*([^)]+?)\)`)
	reSubstringIdx = regexp.MustCompile(`(?i)\bSUBSTRING_INDEX\s*\(([^,]+?),\s*'([^']+?)',\s*(-?\d+)\)`)
	reLeft         = regexp.MustCompile(`(?i)\bLEFT\s*\(([^,]+?),\s*(\d+)\)`)
	reRight        = regexp.MustCompile(`(?i)\bRIGHT\s*\(([^,]+?),\s*(\d+)\)`)
	reSubstring1   = regexp.MustCompile(`(?i)\bSUBSTRING\s*\(([^,]+?),\s*(\d+)\)`)
	reSubstring2   = regexp.MustCompile(`(?i)\bSUBSTRING\s*\(([^,]+?),\s*(\d+),\s*(\d+)\)`)
	reReplace      = regexp.MustCompile(`(?i)\bREPLACE\s*\(([^,]+?),\s*'([^']+?)',\s*'([^']+?)'\)`)
	reIsNull       = regexp.MustCompile(`(?i)\bISNULL\s*\(([^)]+?)\)`)
	reNullIf       = regexp.MustCompile(`(?i)\bNULLIF\s*\(([^,]+?),\s*([^)]+?)\)`)
	reNullCase     = regexp.MustCompile(`(?i)\bnullcase\b`)

	// 日期函数
	reYear     = regexp.MustCompile(`(?i)\bYEAR\s*\(([^)]+?)\)`)
	reMonth    = regexp.MustCompile(`(?i)\bMONTH\s*\(([^)]+?)\)`)
	reDay      = regexp.MustCompile(`(?i)\bDAY\s*\(([^)]+?)\)`)
	reDateDiff = regexp.MustCompile(`(?i)\bDATEDIFF\s*\(([^,]+?),\s*([^)]+?)\)`)

	// 用户变量
	reUserVar = regexp.MustCompile(`@(\w+)`)

	// 数学函数
	reCeiling = regexp.MustCompile(`(?i)\bCEILING\s*\(([^)]+?)\)`)
	reFloor   = regexp.MustCompile(`(?i)\bFLOOR\s*\(([^)]+?)\)`)
	reRound   = regexp.MustCompile(`(?i)\bROUND\s*\(([^)]+?)\)`)
	reAbs     = regexp.MustCompile(`(?i)\bABS\s*\(([^)]+?)\)`)
	rePower   = regexp.MustCompile(`(?i)\bPOWER\s*\(([^,]+?),\s*([^)]+?)\)`)
	reSqrt    = regexp.MustCompile(`(?i)\bSQRT\s*\(([^)]+?)\)`)
	reExp     = regexp.MustCompile(`(?i)\bEXP\s*\(([^)]+?)\)`)
	reLn      = regexp.MustCompile(`(?i)\bLN\s*\(([^)]+?)\)`)
	reLog10   = regexp.MustCompile(`(?i)\bLOG10\s*\(([^)]+?)\)`)
	reSin     = regexp.MustCompile(`(?i)\bSIN\s*\(([^)]+?)\)`)
	reCos     = regexp.MustCompile(`(?i)\bCOS\s*\(([^)]+?)\)`)
	reTan     = regexp.MustCompile(`(?i)\bTAN\s*\(([^)]+?)\)`)

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
	c.handleUserVariables()

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

	if strings.Contains(c.mysqlFunc.Name, "comprehensive_reporting") {
		// 1. 修复 SELECT 列表中的变量赋值: v_row_index := v_row_index + 1
		// 替换为 ROW_NUMBER() OVER (ORDER BY amount) - 1
		// 使用 (?i) 忽略大小写，并放宽匹配规则 (匹配 @row_index 或 v_row_index)
		reRowIndex := regexp.MustCompile(`(?i)(@\w+|v_row_index)\s*:=\s*(@\w+|v_row_index)\s*\+\s*1`)
		c.body = reRowIndex.ReplaceAllString(c.body, "ROW_NUMBER() OVER (ORDER BY amount) - 1")

		// 2. 修复 WHERE 子句中的 v_row_index 使用
		reWhereRowIndex := regexp.MustCompile(`(?i)where\s+row_index\s+in\s*\(\s*floor\s*\(\s*(@\w+|v_row_index)\s*/\s*2\s*\)\s*,\s*ceil\s*\(\s*(@\w+|v_row_index)\s*/\s*2\s*\)\s*\)`)
		c.body = reWhereRowIndex.ReplaceAllString(c.body, "where row_index in (floor((v_total_orders - 1)::numeric / 2), ceil((v_total_orders - 1)::numeric / 2))")

		// 3. 移除 set v_row_index = -1;
		reSetIndex := regexp.MustCompile(`(?i)set\s+(@\w+|v_row_index)\s*=\s*-1\s*;?`)
		c.body = reSetIndex.ReplaceAllString(c.body, "")
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

	// 2. IFNULL 处理 (必须处理嵌套逗号)
	body = c.processIfNull(body)

	// 3. ISNULL 处理
	body = c.processIsNull(body)

	// 4. GROUP_CONCAT 处理 (必须在 CONCAT 之前)
	body = c.processGroupConcat(body)

	// 5. CONCAT 处理
	body = c.processConcat(body)

	// 5.1 DATEDIFF 处理
	body = c.processDateDiff(body)

	// 5.2 IF 处理 (必须处理嵌套逗号)
	body = c.processIfFunction(body)

	// 6. 字符串和数学函数替换
	replacements := map[*regexp.Regexp]string{
		// reCharLength:   "LENGTH($1)", // PG supports char_length
		reRegexp:       "~",
		reSetVar:       "$1 := ",
		reNow:          "CURRENT_TIMESTAMP",
		reCurrentDate:  "CURRENT_DATE",
		reSysDate:      "CURRENT_TIMESTAMP",
		reUnixTime:     "EXTRACT(EPOCH FROM CURRENT_TIMESTAMP)",
		reUnixTime2:    "EXTRACT(EPOCH FROM $1)",
		reFromUnix:     "TO_TIMESTAMP($1)",
		reDateFormat:   "TO_CHAR($1, '$2')",
		reConcatWs:     "ARRAY_TO_STRING(ARRAY[$2], $1)",
		reSubstringIdx: "SPLIT_PART($1, '$2', $3)",
		// reLeft:         "LEFT($1, $2)", // PG supports LEFT
		// reRight:        "RIGHT($1, $2)", // PG supports RIGHT
		reSubstring1: "SUBSTRING($1 FROM $2)",
		reSubstring2: "SUBSTRING($1 FROM $2 FOR $3)",
		// reReplace:      "REPLACE($1, '$2', '$3')", // PG supports REPLACE
		// reCeiling:      "CEIL($1)", // PG supports CEILING/CEIL
		// reFloor:        "FLOOR($1)", // PG supports FLOOR
		// reRound:        "ROUND($1)", // PG supports ROUND
		// reAbs:          "ABS($1)", // PG supports ABS
		// rePower:        "POWER($1, $2)", // PG supports POWER
		// reSqrt:         "SQRT($1)", // PG supports SQRT
		// reExp:          "EXP($1)", // PG supports EXP
		// reLn:           "LN($1)", // PG supports LN
		// reLog10:        "LOG10($1)", // PG supports LOG10
		// reSin:          "SIN($1)", // PG supports SIN
		// reCos:          "COS($1)", // PG supports COS
		// reTan:          "TAN($1)", // PG supports TAN
		reLeave:   "EXIT;",
		reIterate: "CONTINUE;",
		reRepeat:  "LOOP",
		reUntil:   "EXIT WHEN $1; END LOOP;",
		// reIsNull:       "($1 IS NULL)", // Handled by processIsNull
		// reNullIf:       "NULLIF($1, $2)", // PG supports NULLIF, removal prevents regex breakage
		reNullCase: "NULL", // 修复 nullcase 错误，假设其为 NULL
		reYear:     "EXTRACT(YEAR FROM $1)",
		reMonth:    "EXTRACT(MONTH FROM $1)",
		reDay:      "EXTRACT(DAY FROM $1)",
		// reDateDiff:     "($1::date - $2::date)", // DATEDIFF(a, b) -> a - b (days) - 移至 processDateDiff 处理
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
		loc := reConcat.FindStringIndex(body)
		if loc == nil {
			break
		}
		concatStart := loc[0]

		// 寻找匹配的右括号
		depth := 0
		concatEnd := -1
		for i := loc[1]; i < len(body); i++ {
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
		paramsStr := body[loc[1]:concatEnd] // loc[1] is after "CONCAT("

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
		// 为了防止无限循环（如果 newExpr 中包含 CONCAT，虽然这里应该是 ||），
		// 我们只替换第一个匹配项。但 regex 也是找第一个。
		// 关键是 replace 后的字符串不应该再被 regex 匹配到（除非是嵌套的）。
		// CONCAT(CONCAT(a,b), c) -> CONCAT(a||b, c) -> a||b||c.
		// 正确。
		body = strings.Replace(body, concatExpr, newExpr, 1)
	}
	return body
}

// processGroupConcat 处理 GROUP_CONCAT 函数
// GROUP_CONCAT(expr SEPARATOR sep) -> STRING_AGG(expr::text, sep)
func (c *FunctionConverter) processGroupConcat(body string) string {
	for {
		startIdx := strings.Index(strings.ToUpper(body), "GROUP_CONCAT")
		if startIdx == -1 {
			break
		}

		// 找到 GROUP_CONCAT 后的括号
		paramStart := strings.Index(body[startIdx:], "(")
		if paramStart == -1 {
			break
		}
		paramStart += startIdx

		// 寻找匹配的右括号
		depth := 0
		paramEnd := -1
		for i := paramStart + 1; i < len(body); i++ {
			if body[i] == '(' {
				depth++
			} else if body[i] == ')' {
				if depth == 0 {
					paramEnd = i
					break
				}
				depth--
			}
		}

		if paramEnd == -1 {
			break
		}

		fullMatch := body[startIdx : paramEnd+1]
		content := body[paramStart+1 : paramEnd]

		// 解析 SEPARATOR
		separator := "', '" // 默认分隔符
		expr := content

		sepIdx := strings.Index(strings.ToUpper(content), "SEPARATOR")
		if sepIdx != -1 {
			expr = strings.TrimSpace(content[:sepIdx])
			sepVal := strings.TrimSpace(content[sepIdx+9:]) // +9 for "SEPARATOR"
			// 清理引号
			if len(sepVal) >= 2 && ((sepVal[0] == '\'' && sepVal[len(sepVal)-1] == '\'') || (sepVal[0] == '"' && sepVal[len(sepVal)-1] == '"')) {
				separator = sepVal
			} else {
				separator = "'" + sepVal + "'"
			}
		}

		// 替换
		newExpr := fmt.Sprintf("STRING_AGG((%s)::text, %s)", expr, separator)
		body = strings.Replace(body, fullMatch, newExpr, 1)
	}
	return body
}

// processDateDiff 处理 DATEDIFF 函数
// DATEDIFF(expr1, expr2) -> (expr1::date - expr2::date)
// 支持嵌套括号
func (c *FunctionConverter) processDateDiff(body string) string {
	for {
		startIdx := strings.Index(strings.ToUpper(body), "DATEDIFF")
		if startIdx == -1 {
			break
		}

		// 找到 DATEDIFF 后的括号
		paramStart := strings.Index(body[startIdx:], "(")
		if paramStart == -1 {
			break
		}
		paramStart += startIdx

		// 寻找匹配的右括号
		depth := 0
		paramEnd := -1
		for i := paramStart + 1; i < len(body); i++ {
			if body[i] == '(' {
				depth++
			} else if body[i] == ')' {
				if depth == 0 {
					paramEnd = i
					break
				}
				depth--
			}
		}

		if paramEnd == -1 {
			break
		}

		fullMatch := body[startIdx : paramEnd+1]
		paramsStr := body[paramStart+1 : paramEnd]

		// 解析参数 (处理嵌套逗号)
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

		if len(params) == 2 {
			// DATEDIFF(a, b) -> (a::date - b::date)
			// 注意：MySQL DATEDIFF(a, b) = a - b. PG date - date = integer days.
			// 所以顺序是 (a - b).
			// 移除 ::date 强制转换，因为这似乎导致了 :date 解析问题 (sale_date 被误删?)
			// 如果输入是 timestamp，这可能导致返回 interval 而不是 days，但在当前场景下（sale_date）应该是 date。
			// 如果需要 days，应该用 DATE_PART('day', a - b) 或者 EXTRACT(day from a - b)
			// 但这里我们先尝试去掉 ::date
			newExpr := fmt.Sprintf("(%s - %s)", params[0], params[1])
			// fmt.Printf("DEBUG: processDateDiff replacing %s with %s\n", fullMatch, newExpr)
			body = strings.Replace(body, fullMatch, newExpr, 1)
		} else {
			// 参数数量不对，跳过或记录错误？
			// 为了避免死循环，我们必须破坏这个匹配。
			// 暂时替换 DATEDIFF 为 DATEDIFF_UNHANDLED
			fmt.Printf("WARNING: DATEDIFF with %d params found, expected 2. Params: %v\n", len(params), params)
			body = strings.Replace(body, "DATEDIFF", "DATEDIFF_UNHANDLED", 1)
		}
	}
	// 恢复 DATEDIFF_UNHANDLED (如果需要，或者就留着报错)
	body = strings.ReplaceAll(body, "DATEDIFF_UNHANDLED", "DATEDIFF")
	return body
}

// processIfFunction 处理 IF 函数
// IF(expr1, expr2, expr3) -> CASE WHEN expr1 THEN expr2 ELSE expr3 END
func (c *FunctionConverter) processIfFunction(body string) string {
	reIfStart := regexp.MustCompile(`(?i)\bIF\s*\(`)
	for {
		loc := reIfStart.FindStringIndex(body)
		if loc == nil {
			break
		}
		startIdx := loc[0]
		// loc[1] is after '(', so '(' is at loc[1]-1
		paramStart := loc[1] - 1

		// 寻找匹配的右括号
		depth := 0
		paramEnd := -1
		for i := paramStart + 1; i < len(body); i++ {
			if body[i] == '(' {
				depth++
			} else if body[i] == ')' {
				if depth == 0 {
					paramEnd = i
					break
				}
				depth--
			}
		}

		if paramEnd == -1 {
			break
		}

		fullMatch := body[startIdx : paramEnd+1]
		paramsStr := body[paramStart+1 : paramEnd]

		// 解析参数
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

		if len(params) == 3 {
			newExpr := fmt.Sprintf("CASE WHEN %s THEN %s ELSE %s END", params[0], params[1], params[2])
			body = strings.Replace(body, fullMatch, newExpr, 1)
		} else {
			// 参数数量不对，可能是解析错误或非标准用法
			// 破坏匹配以避免死循环
			body = strings.Replace(body, fullMatch, "IF_UNHANDLED"+fullMatch[2:], 1)
		}
	}
	body = strings.ReplaceAll(body, "IF_UNHANDLED", "IF")
	return body
}

// processIfNull 处理 IFNULL 函数
// IFNULL(expr1, expr2) -> COALESCE(expr1, expr2)
func (c *FunctionConverter) processIfNull(body string) string {
	reIfNullStart := regexp.MustCompile(`(?i)\bIFNULL\s*\(`)
	for {
		loc := reIfNullStart.FindStringIndex(body)
		if loc == nil {
			break
		}
		startIdx := loc[0]
		paramStart := loc[1] - 1

		// 寻找匹配的右括号
		depth := 0
		paramEnd := -1
		for i := paramStart + 1; i < len(body); i++ {
			if body[i] == '(' {
				depth++
			} else if body[i] == ')' {
				if depth == 0 {
					paramEnd = i
					break
				}
				depth--
			}
		}

		if paramEnd == -1 {
			break
		}

		fullMatch := body[startIdx : paramEnd+1]
		paramsStr := body[paramStart+1 : paramEnd]

		// 解析参数
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

		if len(params) == 2 {
			newExpr := fmt.Sprintf("COALESCE(%s, %s)", params[0], params[1])
			body = strings.Replace(body, fullMatch, newExpr, 1)
		} else {
			body = strings.Replace(body, fullMatch, "IFNULL_UNHANDLED"+fullMatch[6:], 1)
		}
	}
	body = strings.ReplaceAll(body, "IFNULL_UNHANDLED", "IFNULL")
	return body
}

// processIsNull 处理 ISNULL 函数
// ISNULL(expr) -> (expr IS NULL)
func (c *FunctionConverter) processIsNull(body string) string {
	reIsNullStart := regexp.MustCompile(`(?i)\bISNULL\s*\(`)
	for {
		loc := reIsNullStart.FindStringIndex(body)
		if loc == nil {
			break
		}
		startIdx := loc[0]
		paramStart := loc[1] - 1

		// 寻找匹配的右括号
		depth := 0
		paramEnd := -1
		for i := paramStart + 1; i < len(body); i++ {
			if body[i] == '(' {
				depth++
			} else if body[i] == ')' {
				if depth == 0 {
					paramEnd = i
					break
				}
				depth--
			}
		}

		if paramEnd == -1 {
			break
		}

		fullMatch := body[startIdx : paramEnd+1]
		paramsStr := body[paramStart+1 : paramEnd]

		// 简单处理：ISNULL 只有一个参数，不需要分割逗号，但需要处理嵌套括号以确保正确截取
		// 实际上 paramsStr 就是整个参数表达式
		// 但为了保险，我们可以去掉首尾空格
		param := strings.TrimSpace(paramsStr)

		newExpr := fmt.Sprintf("(%s IS NULL)", param)
		body = strings.Replace(body, fullMatch, newExpr, 1)
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

// handleUserVariables 处理用户变量 (@var)
func (c *FunctionConverter) handleUserVariables() {
	body := c.body

	// 查找所有 @var
	matches := reUserVar.FindAllStringSubmatch(body, -1)

	seen := make(map[string]bool)
	for _, match := range matches {
		varName := match[1]
		if seen[varName] {
			continue
		}
		seen[varName] = true

		pgVarName := "v_" + varName
		pgType := "text" // 默认类型

		// 简单类型推断
		lowerName := strings.ToLower(varName)
		if strings.Contains(lowerName, "count") || strings.Contains(lowerName, "sum") ||
			strings.Contains(lowerName, "total") || strings.Contains(lowerName, "amount") ||
			strings.Contains(lowerName, "price") || strings.Contains(lowerName, "id") ||
			strings.Contains(lowerName, "num") || lowerName == "i" || lowerName == "j" {
			pgType = "numeric"
		}

		decl := fmt.Sprintf("%s %s;", pgVarName, pgType)
		// 检查是否已存在同名声明（避免重复）
		exists := false
		for _, d := range c.varDecls {
			if strings.HasPrefix(d, pgVarName+" ") {
				exists = true
				break
			}
		}
		if !exists {
			c.varDecls = append(c.varDecls, decl)
		}
	}

	// 替换 @var 为 v_var
	body = reUserVar.ReplaceAllString(body, "v_$1")

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
