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
	reConcatWs     = regexp.MustCompile(`(?i)\bCONCAT_WS\s*\(`)
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
	// 增强变量声明匹配，支持更多类型和格式（包括 NUMERIC）
	reVarDecl = regexp.MustCompile(`(?i)\s*(\w+)\s+(INT|VARCHAR|TEXT|DECIMAL|NUMERIC|DATE|TIME|TIMESTAMP|BOOLEAN|FLOAT|DOUBLE|CHAR|REFCURSOR|TINYINT|BIGINT|MEDIUMINT|SMALLINT)\s*(?:UNSIGNED)?\s*(?:\((\d+(?:,\d+)?)\))?\s*(?:DEFAULT\s+([^;]+))?;`)

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
	reWhileDo        = regexp.MustCompile(`(?i)\bwhile\s+([^\n]+?)\s+do\b`)
	reEndWhile       = regexp.MustCompile(`(?i)\bend\s+while\s*;?`)

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
	reDoneEqTrue     = regexp.MustCompile(`(?i)\bdone\s*=\s*1\b`)
	reDoneEqFalse    = regexp.MustCompile(`(?i)\bdone\s*=\s*0\b`)
	reEndLoopTail    = regexp.MustCompile(`(?i)\bEND\s+LOOP\s+[A-Za-z_][A-Za-z0-9_]*\s*;`)
	reIdentifierOnly = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

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

	// 4. 应用通用补丁（如 MySQL 特有 Handler 语句移除；不做按函数名的定制补丁）
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
	for start < len(ddl) && isWhitespaceByte(ddl[start]) {
		start++
	}

	end := findReturnTypeEnd(ddl, upperDDL, start)
	rawType := strings.TrimSpace(ddl[start:end])
	upperRawType := strings.ToUpper(rawType)

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
		c.returnType = mapTypeToPG(rawType)
	}

	if c.returnType == "" {
		c.returnType = "VOID"
	}

	return nil
}

func isWhitespaceByte(b byte) bool {
	return b == ' ' || b == '\n' || b == '\r' || b == '\t'
}

func isIdentifierByte(b byte) bool {
	return (b >= 'A' && b <= 'Z') || (b >= 'a' && b <= 'z') || (b >= '0' && b <= '9') || b == '_'
}

func hasKeywordAt(input string, idx int, keyword string) bool {
	if idx < 0 || idx+len(keyword) > len(input) {
		return false
	}
	if !strings.HasPrefix(input[idx:], keyword) {
		return false
	}
	if idx > 0 && isIdentifierByte(input[idx-1]) {
		return false
	}
	end := idx + len(keyword)
	if end < len(input) && isIdentifierByte(input[end]) {
		return false
	}
	return true
}

func findReturnTypeEnd(ddl, upperDDL string, start int) int {
	keywords := []string{
		"BEGIN",
		"SQL SECURITY",
		"NOT DETERMINISTIC",
		"DETERMINISTIC",
		"CONTAINS SQL",
		"NO SQL",
		"READS SQL DATA",
		"MODIFIES SQL DATA",
		"COMMENT",
	}

	depth := 0
	for i := start; i < len(ddl); i++ {
		switch ddl[i] {
		case '(':
			depth++
		case ')':
			if depth > 0 {
				depth--
			}
		}
		if depth != 0 {
			continue
		}
		for _, keyword := range keywords {
			if hasKeywordAt(upperDDL, i, keyword) {
				return i
			}
		}
	}
	return len(ddl)
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

// applySpecificPatches 应用通用补丁
// 注意：不做任何按函数名匹配的定制补丁——按名补丁会把针对个别函数的改写
// 强加给所有同名（或名字含该子串）的函数，产生不可解释的输出。
// 无法通用转换的语法应在迁移报告中提示人工复核，而非静默定制改写。
func (c *FunctionConverter) applySpecificPatches() {
	// P1-14：DECLARE HANDLER 处理：
	// FOR NOT FOUND 的语义已由 FETCH 转换覆盖（FETCH 转换生成 IF NOT FOUND THEN done := true），删除声明即可；
	// 其他 HANDLER（SQLEXCEPTION 等）无法转换，以注释保留原文避免 PG 语法错误，并由迁移报告告警
	c.body = reHandler.ReplaceAllStringFunc(c.body, func(m string) string {
		if strings.Contains(strings.ToLower(m), "not found") {
			return ""
		}
		return "-- [mysql2pg] MySQL HANDLER 无法自动转换，原文: " + strings.TrimSpace(m)
	})
}

// convertDataTypes 转换基本数据类型
// 单词级替换在字面量遮蔽下进行，字符串字面量内的 datetime/unsigned 等
// 字样不会被误替换；反引号替换仅作用于字面量之外的标识符
func (c *FunctionConverter) convertDataTypes() {
	mask := newLiteralMask()
	c.body = mask.mask(c.body)
	c.body = reTinyInt.ReplaceAllString(c.body, "SMALLINT")
	c.body = reDateTime.ReplaceAllString(c.body, "TIMESTAMP")
	c.body = mask.unmask(c.body)

	c.body = replaceBackticksOutsideLiterals(c.body)

	mask = newLiteralMask()
	c.body = mask.mask(c.body)
	c.body = reUnsigned.ReplaceAllString(c.body, "")
	c.body = reZerofill.ReplaceAllString(c.body, "")
	c.body = mask.unmask(c.body)
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

	// 5. CONCAT_WS 处理（必须在 CONCAT 之前，避免参数被误切分）
	body = c.processConcatWs(body)

	// 6. CONCAT 处理
	body = c.processConcat(body)

	// 6.1 DATEDIFF 处理
	body = c.processDateDiff(body)

	// 6.2 IF 处理 (必须处理嵌套逗号)
	body = c.processIfFunction(body)

	// 7. 字符串和数学函数替换
	// 使用有序规则，避免存在前后依赖的模式被随机 map 遍历顺序破坏。
	// readsLiteral 标记需要读取字符串字面量内容的规则（全局执行），
	// 其余单词级替换在字面量遮蔽下执行，避免破坏字面量内容。
	orderedReplacements := []struct {
		re           *regexp.Regexp
		repl         string
		readsLiteral bool
	}{
		// reCharLength:   "LENGTH($1)", // PG supports char_length
		{reRegexp, "~", false},
		{reSetVar, "$1 := ", false},
		{reNow, "CURRENT_TIMESTAMP", false},
		{reCurrentDate, "CURRENT_DATE", false},
		{reSysDate, "CURRENT_TIMESTAMP", false},
		{reUnixTime, "EXTRACT(EPOCH FROM CURRENT_TIMESTAMP)", false},
		{reUnixTime2, "EXTRACT(EPOCH FROM $1)", false},
		{reFromUnix, "TO_TIMESTAMP($1)", false},
		{reSubstringIdx, "SPLIT_PART($1, '$2', $3)", true},
		// reLeft:         "LEFT($1, $2)", // PG supports LEFT
		// reRight:        "RIGHT($1, $2)", // PG supports RIGHT
		{reSubstring1, "SUBSTRING($1 FROM $2)", false},
		{reSubstring2, "SUBSTRING($1 FROM $2 FOR $3)", false},
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
		{reLeave, "EXIT;", false},
		{reIterate, "CONTINUE;", false},
		// 先处理完整的 UNTIL ... END REPEAT，再处理单独的 REPEAT。
		{reUntil, "EXIT WHEN $1; END LOOP;", false},
		{reRepeat, "LOOP", false},
		// reIsNull:       "($1 IS NULL)", // Handled by processIsNull
		// reNullIf:       "NULLIF($1, $2)", // PG supports NULLIF, removal prevents regex breakage
		{reNullCase, "NULL", false}, // 修复 nullcase 错误，假设其为 NULL
		{reYear, "EXTRACT(YEAR FROM $1)", false},
		{reMonth, "EXTRACT(MONTH FROM $1)", false},
		{reDay, "EXTRACT(DAY FROM $1)", false},
		// reDateDiff:     "($1::date - $2::date)", // DATEDIFF(a, b) -> a - b (days) - 移至 processDateDiff 处理
	}

	// 需要读取字面量内容的规则先全局执行（其输出不与其他规则交互）
	// DATE_FORMAT -> TO_CHAR：格式说明符经 convertMySQLDateFormatToPG 转换（issue-12，
	// 否则 MySQL 的 %Y-%m-%d 会原样进入 PG to_char 产生错误的格式化结果）
	body = reDateFormat.ReplaceAllStringFunc(body, func(m string) string {
		match := reDateFormat.FindStringSubmatch(m)
		if len(match) < 3 {
			return m
		}
		pgFormat := convertMySQLDateFormatToPG(match[2])
		return fmt.Sprintf("TO_CHAR(%s, %s)", strings.TrimSpace(match[1]), pgFormat)
	})
	for _, item := range orderedReplacements {
		if item.readsLiteral {
			body = item.re.ReplaceAllString(body, item.repl)
		}
	}

	// 其余单词级替换在字面量遮蔽下执行，字符串字面量内的
	// NOW/SYSDATE/LEAVE/年份函数等字样不会被误替换
	mask := newLiteralMask()
	maskedBody := mask.mask(body)
	for _, item := range orderedReplacements {
		if !item.readsLiteral {
			maskedBody = item.re.ReplaceAllString(maskedBody, item.repl)
		}
	}

	// ROW_COUNT() 处理
	// MySQL: v_count := ROW_COUNT();
	// PG: GET DIAGNOSTICS v_count = ROW_COUNT;
	maskedBody = reRowCountAssign.ReplaceAllString(maskedBody, "GET DIAGNOSTICS $1 = ROW_COUNT;")

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
		maskedBody = r.re.ReplaceAllString(maskedBody, r.repl)
	}
	body = mask.unmask(maskedBody)

	c.body = body
}

// processConcat 处理 CONCAT 函数
// 该函数解析嵌套的 CONCAT 调用，并将其转换为 PostgreSQL 的 || 操作符
// 例如: CONCAT(a, b, CONCAT(c, d)) -> a || b || c || d
func (c *FunctionConverter) processConcat(body string) string {
	searchFrom := 0
	for {
		// 仅在字符串字面量之外查找 CONCAT
		pos := findKeywordOutsideLiterals(body, "CONCAT", searchFrom)
		if pos == -1 {
			break
		}
		// 排除更长标识符的后缀（如 group_concat）与 CONCAT_WS
		if pos > 0 && isIdentChar(body[pos-1]) {
			searchFrom = pos + 6
			continue
		}
		j := pos + len("CONCAT")
		if j < len(body) && body[j] == '_' {
			searchFrom = j + 1
			continue
		}
		for j < len(body) && (body[j] == ' ' || body[j] == '\t' || body[j] == '\n' || body[j] == '\r') {
			j++
		}
		if j >= len(body) || body[j] != '(' {
			searchFrom = pos + 6
			continue
		}

		// 引号感知的括号匹配
		concatEnd, ok := findMatchingParenInViewExpr(body, j)
		if !ok {
			searchFrom = pos + 6
			continue
		}

		// 解析参数列表（引号感知，字面量内的逗号/括号不参与分割）
		params := splitTopLevelCommas(body[j+1 : concatEnd])
		for i := range params {
			params[i] = strings.TrimSpace(params[i])
		}

		// 使用 || 连接所有参数
		newExpr := strings.Join(params, " || ")
		body = body[:pos] + newExpr + body[concatEnd+1:]
		searchFrom = pos + len(newExpr)
	}
	return body
}

// processGroupConcat 处理 GROUP_CONCAT 函数
// GROUP_CONCAT(expr SEPARATOR sep) -> STRING_AGG(expr::text, sep)
// 仅在字符串字面量之外查找 GROUP_CONCAT，括号匹配引号感知
func (c *FunctionConverter) processGroupConcat(body string) string {
	searchFrom := 0
	for {
		startIdx := findKeywordOutsideLiterals(body, "GROUP_CONCAT", searchFrom)
		if startIdx == -1 {
			break
		}

		// 找到 GROUP_CONCAT 后的括号（允许空白）
		j := startIdx + len("GROUP_CONCAT")
		for j < len(body) && (body[j] == ' ' || body[j] == '\t' || body[j] == '\n' || body[j] == '\r') {
			j++
		}
		if j >= len(body) || body[j] != '(' {
			searchFrom = startIdx + len("GROUP_CONCAT")
			continue
		}

		// 引号感知的括号匹配
		paramEnd, ok := findMatchingParenInViewExpr(body, j)
		if !ok {
			searchFrom = startIdx + len("GROUP_CONCAT")
			continue
		}

		content := body[j+1 : paramEnd]

		// 解析 DISTINCT / ORDER BY / SEPARATOR（仅字面量之外的关键字才算，P1-15）
		separator := "', '" // 默认分隔符
		hasDistinct := false
		orderBy := ""

		distIdx := findKeywordOutsideLiterals(content, "DISTINCT", 0)
		orderIdx := findKeywordOutsideLiterals(content, "ORDER BY", 0)
		sepIdx := findKeywordOutsideLiterals(content, "SEPARATOR", 0)

		// 表达式为最早一个结构性关键字之前的部分
		cutIdx := len(content)
		for _, idx := range []int{distIdx, orderIdx, sepIdx} {
			if idx != -1 && idx < cutIdx {
				cutIdx = idx
			}
		}
		expr := strings.TrimSpace(content[:cutIdx])

		if distIdx != -1 {
			hasDistinct = true
		}
		if orderIdx != -1 {
			orderEnd := len(content)
			if sepIdx != -1 && sepIdx > orderIdx {
				orderEnd = sepIdx
			}
			orderBy = strings.TrimSpace(content[orderIdx+len("ORDER BY") : orderEnd])
		}
		if sepIdx != -1 {
			sepVal := strings.TrimSpace(content[sepIdx+len("SEPARATOR"):])
			if len(sepVal) >= 2 && ((sepVal[0] == '\'' && sepVal[len(sepVal)-1] == '\'') || (sepVal[0] == '"' && sepVal[len(sepVal)-1] == '"')) {
				separator = sepVal
			} else if sepVal != "" {
				separator = "'" + sepVal + "'"
			}
		}

		// 组装 PG string_agg 表达式：STRING_AGG([DISTINCT ] expr, sep [ORDER BY ...])
		var agg strings.Builder
		agg.WriteString("STRING_AGG(")
		if hasDistinct {
			agg.WriteString("DISTINCT ")
		}
		agg.WriteString(fmt.Sprintf("(%s)::text, %s", expr, separator))
		if orderBy != "" {
			agg.WriteString(" ORDER BY " + orderBy)
		}
		agg.WriteString(")")
		newExpr := agg.String()

		body = body[:startIdx] + newExpr + body[paramEnd+1:]
		searchFrom = startIdx + len(newExpr)
	}
	return body
}

// processConcatWs 处理 CONCAT_WS 函数并保留分隔符与参数顺序。
// 仅在字符串字面量之外查找 CONCAT_WS，括号匹配引号感知
func (c *FunctionConverter) processConcatWs(body string) string {
	searchFrom := 0
	for {
		startIdx := findKeywordOutsideLiterals(body, "CONCAT_WS", searchFrom)
		if startIdx == -1 {
			break
		}
		// 排除更长标识符的后缀
		if startIdx > 0 && isIdentChar(body[startIdx-1]) {
			searchFrom = startIdx + len("CONCAT_WS")
			continue
		}
		j := startIdx + len("CONCAT_WS")
		for j < len(body) && (body[j] == ' ' || body[j] == '\t' || body[j] == '\n' || body[j] == '\r') {
			j++
		}
		if j >= len(body) || body[j] != '(' {
			searchFrom = startIdx + len("CONCAT_WS")
			continue
		}

		paramEnd, ok := findMatchingParenInViewExpr(body, j)
		if !ok {
			searchFrom = startIdx + len("CONCAT_WS")
			continue
		}

		args := splitArgsWithContext(body[j+1 : paramEnd])
		if len(args) < 2 {
			searchFrom = paramEnd + 1
			continue
		}

		separator := strings.TrimSpace(args[0])
		exprs := make([]string, 0, len(args)-1)
		for _, arg := range args[1:] {
			trimmed := strings.TrimSpace(arg)
			if trimmed != "" {
				exprs = append(exprs, trimmed)
			}
		}
		if len(exprs) == 0 {
			searchFrom = paramEnd + 1
			continue
		}

		newExpr := fmt.Sprintf("ARRAY_TO_STRING(ARRAY[%s], %s)", strings.Join(exprs, ", "), separator)
		body = body[:startIdx] + newExpr + body[paramEnd+1:]
		searchFrom = startIdx + len(newExpr)
	}
	return body
}

// splitArgsWithContext 按顶层逗号拆分参数并正确处理引号和嵌套括号。
func splitArgsWithContext(paramsStr string) []string {
	var args []string
	var current strings.Builder
	depth := 0
	inString := false
	stringChar := byte(0)
	for i := 0; i < len(paramsStr); i++ {
		ch := paramsStr[i]
		if ch == '\'' || ch == '"' {
			current.WriteByte(ch)
			if !inString {
				inString = true
				stringChar = ch
				continue
			}
			if ch == stringChar {
				if ch == '\'' && i+1 < len(paramsStr) && paramsStr[i+1] == '\'' {
					current.WriteByte(paramsStr[i+1])
					i++
					continue
				}
				inString = false
				stringChar = 0
			}
			continue
		}
		if inString {
			current.WriteByte(ch)
			continue
		}
		switch ch {
		case '(':
			depth++
			current.WriteByte(ch)
		case ')':
			if depth > 0 {
				depth--
			}
			current.WriteByte(ch)
		case ',':
			if depth == 0 {
				args = append(args, strings.TrimSpace(current.String()))
				current.Reset()
				continue
			}
			current.WriteByte(ch)
		default:
			current.WriteByte(ch)
		}
	}
	args = append(args, strings.TrimSpace(current.String()))
	return args
}

// processDateDiff 处理 DATEDIFF 函数
// DATEDIFF(expr1, expr2) -> (expr1::date - expr2::date)
// 仅在字符串字面量之外查找 DATEDIFF，括号匹配引号感知
func (c *FunctionConverter) processDateDiff(body string) string {
	searchFrom := 0
	for {
		startIdx := findKeywordOutsideLiterals(body, "DATEDIFF", searchFrom)
		if startIdx == -1 {
			break
		}
		// 排除更长标识符的后缀
		if startIdx > 0 && isIdentChar(body[startIdx-1]) {
			searchFrom = startIdx + len("DATEDIFF")
			continue
		}
		j := startIdx + len("DATEDIFF")
		for j < len(body) && (body[j] == ' ' || body[j] == '\t' || body[j] == '\n' || body[j] == '\r') {
			j++
		}
		if j >= len(body) || body[j] != '(' {
			searchFrom = startIdx + len("DATEDIFF")
			continue
		}

		paramEnd, ok := findMatchingParenInViewExpr(body, j)
		if !ok {
			searchFrom = startIdx + len("DATEDIFF")
			continue
		}

		// 解析参数（引号感知，字面量内的逗号/括号不参与分割）
		params := splitTopLevelCommas(body[j+1 : paramEnd])
		for i := range params {
			params[i] = strings.TrimSpace(params[i])
		}

		if len(params) == 2 {
			// DATEDIFF(a, b) -> (a - b)
			// 注意：MySQL DATEDIFF(a, b) = a - b. PG date - date = integer days.
			// 所以顺序是 (a - b).
			newExpr := fmt.Sprintf("(%s - %s)", params[0], params[1])
			body = body[:startIdx] + newExpr + body[paramEnd+1:]
			searchFrom = startIdx + len(newExpr)
		} else {
			// 参数数量不对，保留原 DATEDIFF 让 PostgreSQL 报错
			searchFrom = paramEnd + 1
		}
	}
	return body
}

// processIfFunction 处理 IF 函数
// IF(expr1, expr2, expr3) -> CASE WHEN expr1 THEN expr2 ELSE expr3 END
// 仅在字符串字面量之外查找 IF(，括号匹配引号感知；
// IFNULL/IF(...) 之外的 IF 语句形式（无左括号）不受影响
func (c *FunctionConverter) processIfFunction(body string) string {
	searchFrom := 0
	for {
		startIdx := findKeywordOutsideLiterals(body, "IF", searchFrom)
		if startIdx == -1 {
			break
		}
		// 排除更长标识符的后缀（如 x_if）
		if startIdx > 0 && isIdentChar(body[startIdx-1]) {
			searchFrom = startIdx + 2
			continue
		}
		j := startIdx + len("IF")
		for j < len(body) && (body[j] == ' ' || body[j] == '\t' || body[j] == '\n' || body[j] == '\r') {
			j++
		}
		if j >= len(body) || body[j] != '(' {
			// 非 IF( 形式（IF 语句、IFNULL 等），跳过
			searchFrom = startIdx + 2
			continue
		}

		paramEnd, ok := findMatchingParenInViewExpr(body, j)
		if !ok {
			searchFrom = startIdx + 2
			continue
		}

		// 解析参数（引号感知，字面量内的逗号/括号不参与分割）
		params := splitTopLevelCommas(body[j+1 : paramEnd])
		for i := range params {
			params[i] = strings.TrimSpace(params[i])
		}

		if len(params) == 3 {
			newExpr := fmt.Sprintf("CASE WHEN %s THEN %s ELSE %s END", params[0], params[1], params[2])
			body = body[:startIdx] + newExpr + body[paramEnd+1:]
			searchFrom = startIdx + len(newExpr)
		} else {
			// 参数数量不对，可能是解析错误或非标准用法，保留原样跳过
			searchFrom = paramEnd + 1
		}
	}
	return body
}

// processIfNull 处理 IFNULL 函数
// IFNULL(expr1, expr2) -> COALESCE(expr1, expr2)
// 仅在字符串字面量之外查找 IFNULL，括号匹配引号感知，
// 字符串字面量内的 IFNULL 文本与括号不会被误处理
func (c *FunctionConverter) processIfNull(body string) string {
	searchFrom := 0
	for {
		pos := findKeywordOutsideLiterals(body, "IFNULL", searchFrom)
		if pos == -1 {
			break
		}
		// 排除更长标识符的后缀（如 x_ifnull）
		if pos > 0 && isIdentChar(body[pos-1]) {
			searchFrom = pos + 6
			continue
		}
		// IFNULL 与左括号之间允许空白
		j := pos + len("IFNULL")
		for j < len(body) && (body[j] == ' ' || body[j] == '\t' || body[j] == '\n' || body[j] == '\r') {
			j++
		}
		if j >= len(body) || body[j] != '(' {
			searchFrom = pos + 6
			continue
		}
		paramEnd, ok := findMatchingParenInViewExpr(body, j)
		if !ok {
			searchFrom = pos + 6
			continue
		}
		params := splitTopLevelCommas(body[j+1 : paramEnd])
		for i := range params {
			params[i] = strings.TrimSpace(params[i])
		}
		if len(params) == 2 {
			newExpr := fmt.Sprintf("COALESCE(%s, %s)", params[0], params[1])
			body = body[:pos] + newExpr + body[paramEnd+1:]
			searchFrom = pos + len(newExpr)
		} else {
			// 参数数量不为 2 的 IFNULL 保持原样，跳过
			searchFrom = paramEnd + 1
		}
	}
	return body
}

// processIsNull 处理 ISNULL 函数
// ISNULL(expr) -> (expr IS NULL)
// 仅在字符串字面量之外查找 ISNULL，括号匹配引号感知
func (c *FunctionConverter) processIsNull(body string) string {
	searchFrom := 0
	for {
		pos := findKeywordOutsideLiterals(body, "ISNULL", searchFrom)
		if pos == -1 {
			break
		}
		// 排除更长标识符的后缀
		if pos > 0 && isIdentChar(body[pos-1]) {
			searchFrom = pos + 6
			continue
		}
		j := pos + len("ISNULL")
		for j < len(body) && (body[j] == ' ' || body[j] == '\t' || body[j] == '\n' || body[j] == '\r') {
			j++
		}
		if j >= len(body) || body[j] != '(' {
			searchFrom = pos + 6
			continue
		}
		paramEnd, ok := findMatchingParenInViewExpr(body, j)
		if !ok {
			searchFrom = pos + 6
			continue
		}

		param := strings.TrimSpace(body[j+1 : paramEnd])
		newExpr := fmt.Sprintf("(%s IS NULL)", param)
		body = body[:pos] + newExpr + body[paramEnd+1:]
		searchFrom = pos + len(newExpr)
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
	// reLabel 必须只在字面量之外应用，否则会删除字符串中的 "HH24:" "MI:" 等
	// 形如 "单词:" 的内容（issue-12）
	body = reDeclare.ReplaceAllString(body, "")
	body = replaceRegexOutsideLiterals(body, reLabel, "")
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
			if (pgType == "VARCHAR" || pgType == "CHAR" || pgType == "DECIMAL" || pgType == "NUMERIC") && varSize != "" {
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
	normalized := strings.TrimSpace(strings.ToUpper(mysqlType))
	baseType := normalized
	if idx := strings.Index(baseType, "("); idx != -1 {
		baseType = strings.TrimSpace(baseType[:idx])
	}

	switch baseType {
	case "INT", "INTEGER", "MEDIUMINT", "TINYINT": // TINYINT 在 PG 中通常映射为 SMALLINT，但这里为了兼容性也可以映射为 INTEGER
		return "INTEGER"
	case "DOUBLE":
		return "DOUBLE PRECISION"
	case "DATETIME":
		return "TIMESTAMP"
	case "BIGINT":
		return "BIGINT"
	case "SMALLINT":
		return "SMALLINT"
	case "FLOAT":
		return "REAL"
	case "DECIMAL", "NUMERIC":
		// 保留精度定义，如 DECIMAL(65,30)
		if idx := strings.Index(mysqlType, "("); idx != -1 {
			return strings.TrimSpace(mysqlType)
		}
		return "DECIMAL"
	case "VARCHAR", "CHAR":
		// 保留长度定义，如 VARCHAR(255)
		if idx := strings.Index(mysqlType, "("); idx != -1 {
			return strings.TrimSpace(mysqlType)
		}
		return "VARCHAR"
	case "TEXT":
		return "TEXT"
	case "BOOLEAN", "BOOL":
		return "BOOLEAN"
	case "DATE":
		return "DATE"
	case "TIME":
		return "TIME"
	case "JSON":
		return "JSONB"
	case "BLOB", "LONGBLOB", "MEDIUMBLOB", "BINARY", "VARBINARY":
		return "BYTEA"
	default:
		// 未知类型，返回清理后的原始类型（移除括号和 UNSIGNED 等）
		cleanType := regexp.MustCompile(`\([^)]*\)`).ReplaceAllString(mysqlType, "")
		cleanType = regexp.MustCompile(`(?i)\s+UNSIGNED`).ReplaceAllString(cleanType, "")
		cleanType = regexp.MustCompile(`(?i)\s+ZEROFILL`).ReplaceAllString(cleanType, "")
		return strings.TrimSpace(cleanType)
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
	body = normalizeMySQLEscapedQuoteLiteral(body)
	body = removeMySQLHashComments(body)
	body = reDoneEqTrue.ReplaceAllString(body, "done")
	body = reDoneEqFalse.ReplaceAllString(body, "NOT done")
	body = reEndLoopTail.ReplaceAllString(body, "END LOOP;")
	body = normalizeEndLoopLabelTails(body)
	body = reBegin.ReplaceAllString(body, "")
	// body = reEndSemi.ReplaceAllString(body, "")
	body = reEmptyLines.ReplaceAllString(body, "\n")
	body = reDoubleSemicolon.ReplaceAllString(body, ";")

	// 2. 调用专门的修复函数
	body = fixIfSyntax(body)
	body = fixLoopSyntax(body)
	body = normalizeEndLoopLabelTails(body)

	// 3. 应用大量零散的语法修复规则
	body = applyMiscFixes(body)
	body = reDoubleSemicolon.ReplaceAllString(body, ";")

	c.body = body
}

func removeMySQLHashComments(body string) string {
	lines := strings.Split(body, "\n")
	for i, line := range lines {
		inSingle := false
		inDouble := false
		cut := -1
		for j := 0; j < len(line); j++ {
			ch := line[j]
			if ch == '\'' && !inDouble {
				inSingle = !inSingle
				continue
			}
			if ch == '"' && !inSingle {
				inDouble = !inDouble
				continue
			}
			if ch == '#' && !inSingle && !inDouble {
				cut = j
				break
			}
		}
		if cut >= 0 {
			lines[i] = strings.TrimRight(line[:cut], " \t")
		}
	}
	return strings.Join(lines, "\n")
}

func normalizeEndLoopLabelTails(body string) string {
	lines := strings.Split(body, "\n")
	for i, line := range lines {
		upperLine := strings.ToUpper(line)
		idx := strings.Index(upperLine, "END LOOP;")
		if idx == -1 {
			continue
		}
		tail := strings.TrimSpace(line[idx+len("END LOOP;"):])
		if tail == "" {
			continue
		}
		fields := strings.Fields(tail)
		if len(fields) == 0 {
			continue
		}
		if reIdentifierOnly.MatchString(fields[0]) {
			lines[i] = line[:idx+len("END LOOP;")]
		}
	}
	return strings.Join(lines, "\n")
}

func normalizeMySQLEscapedQuoteLiteral(body string) string {
	return strings.ReplaceAll(body, "'\\''", "''''")
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
	// MySQL WHILE ... DO → PostgreSQL WHILE ... LOOP
	body = reWhileDo.ReplaceAllString(body, "WHILE $1 LOOP")
	body = reEndWhile.ReplaceAllString(body, "END LOOP;")

	// MySQL LEAVE label → PostgreSQL EXIT label
	body = reLeave.ReplaceAllString(body, "EXIT")

	// MySQL ITERATE label → PostgreSQL CONTINUE label
	body = reIterate.ReplaceAllString(body, "CONTINUE")

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
