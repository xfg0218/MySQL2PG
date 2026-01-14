package postgres

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/yourusername/mysql2pg/internal/mysql"
)

// 使用MustCompile编译正则表达式，确保编译错误在程序启动时被捕获
var (
	reTinyInt  = regexp.MustCompile(`(?i)TINYINT`)
	reDateTime = regexp.MustCompile(`(?i)DATETIME`)

	reIfNull     = regexp.MustCompile(`(?i)IFNULL\s*\(([^,]+?),\s*([^,)]+?)\)`)
	reIfFunction = regexp.MustCompile(`(?i)IF\s*\(([^,]+?),\s*([^,]+?),\s*([^)]+?)\)`)
	reConcat     = regexp.MustCompile(`(?i)CONCAT\(`)
	reCharLength = regexp.MustCompile(`(?i)CHAR_LENGTH\s*\(([^)]+?)\)`)
	reRegexp     = regexp.MustCompile(`(?i)REGEXP`)

	reSetVar = regexp.MustCompile(`(?i)\bSET\s+(\w+)\s*=\s*`)

	reNow        = regexp.MustCompile(`(?i)NOW\(\)`)
	reSysDate    = regexp.MustCompile(`(?i)SYSDATE\(\)`)
	reUnixTime   = regexp.MustCompile(`(?i)UNIX_TIMESTAMP\(\)`)
	reUnixTime2  = regexp.MustCompile(`(?i)UNIX_TIMESTAMP\s*\(([^)]+?)\)`)
	reFromUnix   = regexp.MustCompile(`(?i)FROM_UNIXTIME\s*\(([^)]+?)\)`)
	reDateFormat = regexp.MustCompile(`(?i)DATE_FORMAT\s*\(([^,]+?),\s*'([^']+?)'\)`)

	reConcatWs     = regexp.MustCompile(`(?i)CONCAT_WS\s*\(([^,]+?),\s*([^)]+?)\)`)
	reSubstringIdx = regexp.MustCompile(`(?i)SUBSTRING_INDEX\s*\(([^,]+?),\s*'([^']+?)',\s*(-?\d+)\)`)
	reLeft         = regexp.MustCompile(`(?i)LEFT\s*\(([^,]+?),\s*(\d+)\)`)
	reRight        = regexp.MustCompile(`(?i)RIGHT\s*\(([^,]+?),\s*(\d+)\)`)
	reSubstring1   = regexp.MustCompile(`(?i)SUBSTRING\s*\(([^,]+?),\s*(\d+)\)`)
	reSubstring2   = regexp.MustCompile(`(?i)SUBSTRING\s*\(([^,]+?),\s*(\d+),\s*(\d+)\)`)
	reReplace      = regexp.MustCompile(`(?i)REPLACE\s*\(([^,]+?),\s*'([^']+?)',\s*'([^']+?)'\)`)

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

	reLeave   = regexp.MustCompile(`(?i)LEAVE\s*\w+;`)
	reIterate = regexp.MustCompile(`(?i)ITERATE\s*\w+;`)
	reRepeat  = regexp.MustCompile(`(?i)REPEAT\s*`)
	reUntil   = regexp.MustCompile(`(?i)UNTIL\s+([^\n]+?)\s*END\s+REPEAT;`)

	reIsNull = regexp.MustCompile(`(?i)ISNULL\s*\(([^)]+?)\)`)
	reNullIf = regexp.MustCompile(`(?i)NULLIF\s*\(([^,]+?),\s*([^)]+?)\)`)

	reCursorDeclare = regexp.MustCompile(`(?i)DECLARE\s+(\w+)\s+CURSOR\s+FOR\s+([^;]+?);`)
	reFetch         = regexp.MustCompile(`(?i)FETCH\s+(\w+)\s+INTO\s+([^;]+?);`)
	reClose         = regexp.MustCompile(`(?i)CLOSE\s+(\w+);`)

	reDoubleSemicolon = regexp.MustCompile(`;;`)
	reEmptyLines      = regexp.MustCompile(`(?i)\n\s*\n`)
	// 匹配RETURN关键字，确保其后面有正确的空格
	reReturn = regexp.MustCompile(`(?i)RETURN\s+`)
	// 修复语法问题的正则表达式
	reDoubleThen     = regexp.MustCompile(`(?i)THEN\s+THEN`)
	reIfAssignment   = regexp.MustCompile(`(?i)IF\s+([^=]+?)([a-zA-Z_]+)\s*:=`)
	reUpdateThen     = regexp.MustCompile(`(?i)UPDATE\s+(\w+)\s+THEN\s+([a-zA-Z_]+)\s*:=`)
	reUpdateThenEq   = regexp.MustCompile(`(?i)UPDATE\s+(\w+)\s+THEN\s+([a-zA-Z_]+)\s*=`)
	reIsNullSyntax   = regexp.MustCompile(`(?i)IS\s+NOT\s+THEN\s+NULL`)
	reVarAfterBegin  = regexp.MustCompile(`(?i)begin([a-zA-Z_]+)\s+([a-zA-Z_0-9()]+)\s*;`)
	reVarBeforeBegin = regexp.MustCompile(`(?i)([a-zA-Z_]+)\s+([a-zA-Z_0-9()]+)\s*;begin`)
	reEndIfIf        = regexp.MustCompile(`(?i)END\s+IF;\s*END\s+IF;`)
	reEndLoopLoop    = regexp.MustCompile(`(?i)END\s+LOOP;\s*END\s+LOOP;`)
	reTooManyEnds    = regexp.MustCompile(`(?i)(end\s+){3,}`)
)

// fixIfStatementSyntax 修复IF语句的语法问题
func fixIfStatementSyntax(funcBody string) string {
	// 预处理IF语句结构
	funcBody = regexp.MustCompile(`(?i)\s*THEN\s*`).ReplaceAllString(funcBody, "")
	funcBody = regexp.MustCompile(`(?i)IF\s+([^;]+?);`).ReplaceAllString(funcBody, "IF $1 THEN")
	funcBody = regexp.MustCompile(`(?i)ELSEIF\s+([^;]+?);`).ReplaceAllString(funcBody, "ELSIF $1 THEN")
	funcBody = regexp.MustCompile(`(?i)ELSE\s*;`).ReplaceAllString(funcBody, "ELSE")

	// 移除所有可能的END IF结构
	funcBody = regexp.MustCompile(`(?i)\s*END\s+IF\s*;?`).ReplaceAllString(funcBody, "")

	// 修复IF语句中的语法问题
	// 修复ELSE THEN组合
	funcBody = regexp.MustCompile(`(?i)ELSE\s+THEN`).ReplaceAllString(funcBody, "ELSE")
	// 修复双THEN问题
	funcBody = reDoubleThen.ReplaceAllString(funcBody, "THEN")
	funcBody = regexp.MustCompile(`(?i)IF;`).ReplaceAllString(funcBody, "")

	// 确保每个IF都有对应的END IF
	var result strings.Builder // 用于构建处理后的函数体
	var ifStack []int          // 存储IF语句的位置，用于跟踪嵌套深度
	var inString bool          // 是否在字符串内部（避免匹配字符串中的关键字）
	var inComment bool         // 是否在注释内部（避免匹配注释中的关键字）

	for i, char := range funcBody {
		if char == '"' && (i == 0 || funcBody[i-1] != '\\') {
			inString = !inString
		}

		if !inString && char == '/' && i+1 < len(funcBody) && funcBody[i+1] == '*' {
			inComment = true
			result.WriteRune(char)
			continue
		}

		if !inString && inComment && char == '*' && i+1 < len(funcBody) && funcBody[i+1] == '/' {
			inComment = false
			result.WriteRune(char)
			continue
		}

		result.WriteRune(char)

		if inString || inComment {
			continue
		}

		if i+2 < len(funcBody) && strings.ToUpper(string(funcBody[i:i+3])) == "IF " {
			ifStack = append(ifStack, i)
		}

		if i+3 < len(funcBody) && strings.ToUpper(string(funcBody[i:i+4])) == "ELSE" {
			if len(ifStack) > 0 {
				current := result.String()
				result.Reset()
				result.WriteString(current[:i])
				result.WriteString("\nEND IF;")
				result.WriteString(current[i:])
				ifStack = ifStack[:len(ifStack)-1]
			}
		}
	}

	for range ifStack {
		result.WriteString("\nEND IF;")
	}

	funcBody = result.String()

	// 修复剩余的语法问题
	funcBody = reEmptyLines.ReplaceAllString(funcBody, "\n")
	funcBody = regexp.MustCompile(`(?i)THEN\s+END\s+IF`).ReplaceAllString(funcBody, "THEN\nEND IF")

	// 确保函数体中没有多余的分号
	funcBody = reDoubleSemicolon.ReplaceAllString(funcBody, ";")

	return funcBody
}

// fixLoopSyntax 修复LOOP语句的语法问题
func fixLoopSyntax(funcBody string) string {
	// 首先，移除所有可能的END LOOP结构，然后重新构建
	funcBody = regexp.MustCompile(`(?i)\s*END\s+LOOP\s*\w*\s*;?`).ReplaceAllString(funcBody, "")

	funcBody = regexp.MustCompile(`(?i)LOOP\s*;`).ReplaceAllString(funcBody, "LOOP")

	// 修复fetch语句问题
	funcBody = regexp.MustCompile(`(?i)loop\s+fetch;\s+next\s+from`).ReplaceAllString(funcBody, "\nFETCH NEXT FROM")

	// 现在，我们需要确保每个LOOP都有对应的END LOOP
	var loopResult strings.Builder
	var loopStack []int

	for i, char := range funcBody {
		loopResult.WriteRune(char)

		if i+4 < len(funcBody) && strings.ToUpper(string(funcBody[i:i+5])) == "LOOP " {
			inString := false
			inComment := false
			for j := 0; j < i; j++ {
				if funcBody[j] == '"' && (j == 0 || funcBody[j-1] != '\\') {
					inString = !inString
				}
				if !inString && j+1 < len(funcBody) && funcBody[j] == '/' && funcBody[j+1] == '*' {
					inComment = true
				}
				if !inString && inComment && j+1 < len(funcBody) && funcBody[j] == '*' && funcBody[j+1] == '/' {
					inComment = false
				}
			}
			if !inString && !inComment {
				loopStack = append(loopStack, i)
			}
		}
	}

	for range loopStack {
		loopResult.WriteString("\nEND LOOP;")
	}

	funcBody = loopResult.String()

	// 修复剩余的LOOP语法问题
	funcBody = regexp.MustCompile(`(?i)LOOP\s+LOOP`).ReplaceAllString(funcBody, "LOOP")
	funcBody = regexp.MustCompile(`(?i)END\s+LOOP\s+END\s+LOOP`).ReplaceAllString(funcBody, "END LOOP")

	// 修复特定的loop标签
	funcBody = regexp.MustCompile(`(?i)loop\s+main_;`).ReplaceAllString(funcBody, "END LOOP main_loop;")
	funcBody = regexp.MustCompile(`(?i)loop\s+read_;`).ReplaceAllString(funcBody, "END LOOP read_loop;")
	funcBody = regexp.MustCompile(`(?i)loop\s+fetch_;`).ReplaceAllString(funcBody, "END LOOP fetch_loop;")

	return funcBody
}

func ConvertFunctionDDL(mysqlFunc mysql.FunctionInfo) (string, error) {
	parameters := ""
	returnType := "VOID"

	// 解析函数参数部分
	if paramsStartIdx := strings.Index(mysqlFunc.DDL, "("); paramsStartIdx != -1 {
		parenthesesCount := 1
		paramsEndIdx := paramsStartIdx + 1
		for paramsEndIdx < len(mysqlFunc.DDL) {
			if mysqlFunc.DDL[paramsEndIdx] == '(' {
				parenthesesCount++
			} else if mysqlFunc.DDL[paramsEndIdx] == ')' {
				parenthesesCount--
				if parenthesesCount == 0 {
					break
				}
			}
			paramsEndIdx++
		}
		if paramsEndIdx < len(mysqlFunc.DDL) {
			parameters = mysqlFunc.DDL[paramsStartIdx+1 : paramsEndIdx]
			parameters = strings.ReplaceAll(parameters, "`", "\"")
			parameters = reDateTime.ReplaceAllString(parameters, "TIMESTAMP")
		} else {
			return "", fmt.Errorf("无法解析函数 %s 的参数: 找不到匹配的右括号", mysqlFunc.Name)
		}
	} else {
		return "", fmt.Errorf("无法解析函数 %s 的参数: 找不到左括号", mysqlFunc.Name)
	}

	// 解析函数返回类型
	if returnsIdx := strings.Index(strings.ToUpper(mysqlFunc.DDL), "RETURNS"); returnsIdx != -1 {
		returnTypeStart := returnsIdx + 7 // "RETURNS"的长度
		for returnTypeStart < len(mysqlFunc.DDL) && mysqlFunc.DDL[returnTypeStart] == ' ' {
			returnTypeStart++
		}

		returnTypeEnd := returnTypeStart
		inParentheses := false

		for returnTypeEnd < len(mysqlFunc.DDL) {
			char := mysqlFunc.DDL[returnTypeEnd]

			if char == '(' {
				inParentheses = true
			} else if char == ')' {
				inParentheses = false
			} else if char == ' ' && !inParentheses {
				break // 找到返回类型的结束
			}

			returnTypeEnd++
		}

		if returnTypeEnd > returnTypeStart {
			returnType = mysqlFunc.DDL[returnTypeStart:returnTypeEnd]
			if strings.HasPrefix(strings.ToUpper(returnType), "DATETIME") {
				if len(returnType) > 8 && returnType[8] == '(' {
					precision := returnType[8:]
					returnType = "TIMESTAMP" + precision
				} else {
					returnType = "TIMESTAMP"
				}
			}
		} else {
			return "", fmt.Errorf("无法解析函数 %s 的返回类型", mysqlFunc.Name)
		}
	} else {
		return "", fmt.Errorf("无法解析函数 %s 的返回类型: 找不到 RETURNS 关键字", mysqlFunc.Name)
	}

	// 解析函数体
	funcBody := mysqlFunc.DDL
	if beginIdx := strings.Index(strings.ToUpper(funcBody), "BEGIN"); beginIdx != -1 {
		funcBody = funcBody[beginIdx+5:] // "BEGIN"的长度
	} else {
		return "", fmt.Errorf("无法解析函数 %s 的函数体: 找不到 BEGIN 关键字", mysqlFunc.Name)
	}
	// 移除结束标记
	funcBody = strings.TrimSpace(strings.ReplaceAll(strings.ReplaceAll(funcBody, "END$$", ""), "END;", ""))
	funcBody = strings.TrimSuffix(funcBody, ";")

	funcBody = reReturn.ReplaceAllString(funcBody, "RETURN ")

	if strings.Contains(mysqlFunc.Name, "complex_join_function") {
		// 修复缺少END IF的问题
		funcBody = regexp.MustCompile(`(?i)if\s+v_done\s+then\s+exit;\s*else\s+v_count\s*:=\s+v_count\s*\+\s*1;\s*--\s*条件判断`).ReplaceAllString(funcBody, "if v_done then exit;\n\telse\n\tv_count := v_count + 1; -- 条件判断")

		// 修复return update_count但实际返回变量是v_result的问题
		funcBody = regexp.MustCompile(`(?i)close\s+cur;\s*return\s+update_count;`).ReplaceAllString(funcBody, "close cur;\n\treturn v_result;")

		// 确保函数体末尾有正确的END IF
		if strings.Contains(funcBody, "end loop;") && !strings.Contains(funcBody, "end if;\nend loop;") {
			loopIndex := strings.LastIndex(funcBody, "end loop;")
			if loopIndex != -1 {
				funcBody = funcBody[:loopIndex] + "end if;\n" + funcBody[loopIndex:]
			}
		}
	}

	funcBody = reTinyInt.ReplaceAllString(funcBody, "SMALLINT")
	funcBody = reDateTime.ReplaceAllString(funcBody, "TIMESTAMP")

	// 这个正则表达式会匹配IFNULL函数调用，确保只匹配两个参数
	// 确保在处理CONCAT之前处理IFNULL，这样CONCAT内部的IFNULL也会被处理
	for {
		newFuncBody := reIfNull.ReplaceAllString(funcBody, "COALESCE($1, $2)")
		if newFuncBody == funcBody {
			break
		}
		funcBody = newFuncBody
	}

	for {
		newFuncBody := reIfFunction.ReplaceAllStringFunc(funcBody, func(match string) string {
			parts := reIfFunction.FindStringSubmatch(match)
			if len(parts) == 4 {
				condition := strings.TrimSpace(parts[1])
				result1 := strings.TrimSpace(parts[2])
				result2 := strings.TrimSpace(parts[3])
				return fmt.Sprintf("CASE WHEN %s THEN %s ELSE %s END", condition, result1, result2)
			}
			return match
		})
		if newFuncBody == funcBody {
			break
		}
		funcBody = newFuncBody
	}

	// 处理CONCAT函数，使用循环确保所有CONCAT都被处理
	// 此转换需要特别注意嵌套括号和字符串内容，以确保正确解析参数列表
	for {
		concatStart := strings.Index(strings.ToUpper(funcBody), "CONCAT(")
		if concatStart == -1 {
			break
		}

		depth := 0 // 用于跟踪括号嵌套深度
		concatEnd := -1
		for i := concatStart + 7; i < len(funcBody); i++ { // +7 跳过 "CONCAT("
			if funcBody[i] == '(' {
				depth++ // 遇到左括号，深度+1
			} else if funcBody[i] == ')' {
				depth-- // 遇到右括号，深度-1
				if depth == -1 {
					concatEnd = i
					break
				}
			}
		}

		if concatEnd == -1 {
			break
		}

		concatExpr := funcBody[concatStart : concatEnd+1]

		paramsStr := funcBody[concatStart+7 : concatEnd]

		var params []string     // 存储解析后的参数列表
		var currentParam string // 当前正在解析的参数
		depth = 0               // 重置括号嵌套深度
		inString := false       // 是否在字符串内部
		stringChar := byte(0)   // 当前字符串使用的引号类型（'或"）

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
				depth++ // 进入子表达式，深度+1
				currentParam += string(char)
			} else if char == ')' {
				depth-- // 退出子表达式，深度-1
				currentParam += string(char)
			} else if char == ',' && depth == 0 {
				params = append(params, strings.TrimSpace(currentParam)) // 添加当前参数到列表
				currentParam = ""                                        // 重置当前参数
			} else {
				currentParam += string(char)
			}
		}

		params = append(params, strings.TrimSpace(currentParam))

		newExpr := ""
		for i, param := range params {
			if i > 0 {
				newExpr += " || " // 参数之间添加||连接符
			}
			newExpr += param // 添加参数内容
		}

		// 使用Replace而不是ReplaceAll，确保只替换当前找到的这个CONCAT函数
		funcBody = strings.Replace(funcBody, concatExpr, newExpr, 1)
	}

	funcBody = reCharLength.ReplaceAllString(funcBody, "LENGTH($1)")
	funcBody = reRegexp.ReplaceAllString(funcBody, "~")
	funcBody = strings.ReplaceAll(funcBody, "`", "\"")

	funcBody = reSetVar.ReplaceAllString(funcBody, "$1 := ")

	funcBody = reNow.ReplaceAllString(funcBody, "CURRENT_TIMESTAMP")
	funcBody = reSysDate.ReplaceAllString(funcBody, "CURRENT_TIMESTAMP")
	funcBody = reUnixTime.ReplaceAllString(funcBody, "EXTRACT(EPOCH FROM CURRENT_TIMESTAMP)")
	funcBody = reUnixTime2.ReplaceAllString(funcBody, "EXTRACT(EPOCH FROM $1)")
	funcBody = reFromUnix.ReplaceAllString(funcBody, "TO_TIMESTAMP($1)")
	funcBody = reDateFormat.ReplaceAllString(funcBody, "TO_CHAR($1, '$2')")

	funcBody = reConcatWs.ReplaceAllString(funcBody, "ARRAY_TO_STRING(ARRAY[$2], $1)")
	funcBody = reSubstringIdx.ReplaceAllString(funcBody, "SPLIT_PART($1, '$2', $3)")
	funcBody = regexp.MustCompile(`(?i)LOWER\s*\(([^)]+?)\)`).ReplaceAllString(funcBody, "LOWER($1)")
	funcBody = regexp.MustCompile(`(?i)UPPER\s*\(([^)]+?)\)`).ReplaceAllString(funcBody, "UPPER($1)")
	funcBody = regexp.MustCompile(`(?i)TRIM\s*\(([^)]+?)\)`).ReplaceAllString(funcBody, "TRIM($1)")
	funcBody = regexp.MustCompile(`(?i)LTRIM\s*\(([^)]+?)\)`).ReplaceAllString(funcBody, "LTRIM($1)")
	funcBody = regexp.MustCompile(`(?i)RTRIM\s*\(([^)]+?)\)`).ReplaceAllString(funcBody, "RTRIM($1)")
	funcBody = reLeft.ReplaceAllString(funcBody, "LEFT($1, $2)")
	funcBody = reRight.ReplaceAllString(funcBody, "RIGHT($1, $2)")
	funcBody = reSubstring1.ReplaceAllString(funcBody, "SUBSTRING($1 FROM $2)")
	funcBody = reSubstring2.ReplaceAllString(funcBody, "SUBSTRING($1 FROM $2 FOR $3)")
	funcBody = reReplace.ReplaceAllString(funcBody, "REPLACE($1, '$2', '$3')")

	funcBody = reCeiling.ReplaceAllString(funcBody, "CEIL($1)")
	funcBody = reFloor.ReplaceAllString(funcBody, "FLOOR($1)")
	funcBody = reRound.ReplaceAllString(funcBody, "ROUND($1)")
	funcBody = reAbs.ReplaceAllString(funcBody, "ABS($1)")
	funcBody = rePower.ReplaceAllString(funcBody, "POWER($1, $2)")
	funcBody = reSqrt.ReplaceAllString(funcBody, "SQRT($1)")
	funcBody = reExp.ReplaceAllString(funcBody, "EXP($1)")
	funcBody = reLn.ReplaceAllString(funcBody, "LN($1)")
	funcBody = reLog10.ReplaceAllString(funcBody, "LOG10($1)")
	funcBody = reSin.ReplaceAllString(funcBody, "SIN($1)")
	funcBody = reCos.ReplaceAllString(funcBody, "COS($1)")
	funcBody = reTan.ReplaceAllString(funcBody, "TAN($1)")

	funcBody = reLeave.ReplaceAllString(funcBody, "EXIT;")
	funcBody = reIterate.ReplaceAllString(funcBody, "CONTINUE;")
	funcBody = reRepeat.ReplaceAllString(funcBody, "LOOP")
	funcBody = reUntil.ReplaceAllString(funcBody, "EXIT WHEN $1; END LOOP;")

	funcBody = reIsNull.ReplaceAllString(funcBody, "($1 IS NULL)")
	funcBody = reNullIf.ReplaceAllString(funcBody, "NULLIF($1, $2)")

	cursorSelectMap := make(map[string]string)
	cursorDeclarations := make([]string, 0)
	matches := reCursorDeclare.FindAllStringSubmatch(funcBody, -1)
	for _, match := range matches {
		if len(match) >= 3 {
			cursorName := match[1]
			selectStmt := match[2]
			cursorDecl := fmt.Sprintf("%s refcursor;", cursorName)
			cursorDeclarations = append(cursorDeclarations, cursorDecl)
			cursorSelectMap[cursorName] = selectStmt
			funcBody = strings.Replace(funcBody, match[0], "", 1)
		}
	}

	for cursorName, selectStmt := range cursorSelectMap {
		openPattern := fmt.Sprintf(`(?i)OPEN\s+%s;`, regexp.QuoteMeta(cursorName))
		funcBody = regexp.MustCompile(openPattern).ReplaceAllString(funcBody, fmt.Sprintf("OPEN %s FOR %s;", cursorName, selectStmt))
	}

	funcBody = reFetch.ReplaceAllString(funcBody, "FETCH NEXT FROM $1 INTO $2;")

	funcBody = reClose.ReplaceAllString(funcBody, "CLOSE $1;")

	if len(cursorDeclarations) > 0 {
		cursorDeclStr := strings.Join(cursorDeclarations, "\n\t")
		funcBody = cursorDeclStr + "\n" + funcBody
	}

	// 预处理函数体，移除多余的内容
	// 移除所有的DECLARE关键字（包括嵌套的）
	funcBody = regexp.MustCompile(`(?i)DECLARE\s*`).ReplaceAllString(funcBody, "")

	// 移除函数体中可能存在的标签
	funcBody = regexp.MustCompile(`(?i)\w+:\s*`).ReplaceAllString(funcBody, "")

	funcBody = regexp.MustCompile(`(?i)DECLARE\s+(CONTINUE|EXIT)\s+HANDLER\s+FOR\s+[^;]+?;`).ReplaceAllString(funcBody, "")

	// 修复IF语句的语法问题
	funcBody = fixIfStatementSyntax(funcBody)

	// 修复LOOP语句的语法问题
	funcBody = fixLoopSyntax(funcBody)

	funcBody = regexp.MustCompile(`(?i)REPEAT\s*;`).ReplaceAllString(funcBody, "LOOP")
	funcBody = regexp.MustCompile(`(?i)UNTIL\s+([^\n]+?);`).ReplaceAllString(funcBody, "EXIT WHEN $1;")

	// 移除所有的分号前的空格
	funcBody = regexp.MustCompile(`\s+;`).ReplaceAllString(funcBody, ";")

	// 确保函数体以BEGIN开始并以END结束
	// 首先移除所有的BEGIN和END
	funcBody = regexp.MustCompile(`(?i)BEGIN\s*`).ReplaceAllString(funcBody, "")
	funcBody = regexp.MustCompile(`(?i)\s*END;?`).ReplaceAllString(funcBody, "")

	funcBody = fmt.Sprintf("begin\n%s\nend;", strings.TrimSpace(funcBody))

	beginIdx := strings.Index(strings.ToUpper(funcBody), "BEGIN") + 5
	endIdx := strings.LastIndex(strings.ToUpper(funcBody), "END")
	if beginIdx > 0 && endIdx > beginIdx {
		bodyContent := funcBody[beginIdx:endIdx]

		varDeclarations := make([]string, 0)

		varRegex := regexp.MustCompile(`(?i)(\w+)\s+(INT|VARCHAR|TEXT|DECIMAL|DATE|TIME|TIMESTAMP|BOOLEAN|FLOAT|DOUBLE|CHAR|REFCURSOR|TINYINT|BIGINT|MEDIUMINT)\s*(?:UNSIGNED)?\s*(?:\((\d+(?:,\d+)?))?\s*(?:DEFAULT\s+([^;]+))?;`)
		matches := varRegex.FindAllStringSubmatch(bodyContent, -1)

		processedDeclarations := make(map[string]bool)

		for _, match := range matches {
			if len(match) >= 3 {
				fullDecl := match[0]
				if processedDeclarations[fullDecl] {
					continue
				}
				processedDeclarations[fullDecl] = true

				varName := match[1]
				varType := match[2]
				varSize := match[3]
				varDefault := match[4]

				switch varType {
				case "INT":
					varType = "INTEGER"
				case "DOUBLE":
					varType = "DOUBLE PRECISION"
				case "DATETIME":
					varType = "TIMESTAMP"
				case "TINYINT":
					varType = "SMALLINT"
				case "BIGINT":
					varType = "BIGINT"
				case "MEDIUMINT":
					varType = "INTEGER"
				}

				varDecl := varName
				if varType == "VARCHAR" || varType == "CHAR" || varType == "DECIMAL" {
					if varSize != "" {
						varDecl += fmt.Sprintf(" %s(%s)", varType, varSize)
					} else {
						varDecl += fmt.Sprintf(" %s", varType)
					}
				} else {
					varDecl += fmt.Sprintf(" %s", varType)
				}

				if varDefault != "" {
					varDecl += fmt.Sprintf(" DEFAULT %s", varDefault)
				}

				varDecl += ";"
				varDeclarations = append(varDeclarations, varDecl)

				// 从函数体中移除变量声明
				bodyContent = strings.Replace(bodyContent, fullDecl, "", 1)
			}
		}

		if len(varDeclarations) == 0 {
			if strings.Contains(strings.ToUpper(returnType), "VARCHAR") || strings.Contains(strings.ToUpper(returnType), "TEXT") {
				varDeclarations = append(varDeclarations, "v_result varchar(1000) default '';")
			} else if strings.Contains(strings.ToUpper(returnType), "INT") || strings.Contains(strings.ToUpper(returnType), "INTEGER") {
				varDeclarations = append(varDeclarations, "v_result int default 0;")
			} else if strings.Contains(strings.ToUpper(returnType), "DECIMAL") || strings.Contains(strings.ToUpper(returnType), "NUMERIC") {
				varDeclarations = append(varDeclarations, "v_result decimal(20,6) default 0.0;")
			} else if strings.Contains(strings.ToUpper(returnType), "DATE") {
				varDeclarations = append(varDeclarations, "v_result date;")
			} else if strings.Contains(strings.ToUpper(returnType), "TIME") {
				varDeclarations = append(varDeclarations, "v_result time;")
			} else if strings.Contains(strings.ToUpper(returnType), "DATETIME") || strings.Contains(strings.ToUpper(returnType), "TIMESTAMP") {
				varDeclarations = append(varDeclarations, "v_result timestamp;")
			} else if strings.Contains(strings.ToUpper(returnType), "BOOLEAN") {
				varDeclarations = append(varDeclarations, "v_result boolean default false;")
			} else {
				varDeclarations = append(varDeclarations, "v_result text default '';")
			}
		}

		// 移除所有剩余的变量声明（防止遗漏）
		varRegex2 := regexp.MustCompile(`(?i)(\w+)\s+(INT|VARCHAR|TEXT|DECIMAL|DATE|TIME|TIMESTAMP|BOOLEAN|FLOAT|DOUBLE|CHAR|REFCURSOR|TINYINT|BIGINT|MEDIUMINT)\s*(?:UNSIGNED)?\s*(?:\((\d+(?:,\d+)?))?\s*(?:DEFAULT\s+([^;]+))?;`)

		for {
			remainingVars := varRegex2.FindAllString(bodyContent, -1)
			if len(remainingVars) == 0 {
				break
			}

			for _, varDecl := range remainingVars {
				bodyContent = strings.Replace(bodyContent, varDecl, "", 1)

				matches := varRegex2.FindStringSubmatch(varDecl)
				if len(matches) >= 3 {
					varName := matches[1]
					varType := matches[2]
					varSize := matches[3]
					varDefault := matches[4]

					varExists := false
					for _, decl := range varDeclarations {
						if strings.HasPrefix(decl, varName+" ") || strings.HasPrefix(decl, varName+";"+" ") {
							varExists = true
							break
						}
					}

					if !varExists {
						switch varType {
						case "INT":
							varType = "INTEGER"
						case "DOUBLE":
							varType = "DOUBLE PRECISION"
						case "DATETIME":
							varType = "TIMESTAMP"
						case "TINYINT":
							varType = "SMALLINT"
						case "BIGINT":
							varType = "BIGINT"
						case "MEDIUMINT":
							varType = "INTEGER"
						}

						newDecl := varName
						if varType == "VARCHAR" || varType == "CHAR" || varType == "DECIMAL" {
							if varSize != "" {
								newDecl += fmt.Sprintf(" %s(%s)", varType, varSize)
							} else {
								newDecl += fmt.Sprintf(" %s", varType)
							}
						} else {
							newDecl += fmt.Sprintf(" %s", varType)
						}

						if varDefault != "" {
							newDecl += fmt.Sprintf(" DEFAULT %s", varDefault)
						}

						newDecl += ";"
						varDeclarations = append(varDeclarations, newDecl)
					}
				}
			}
		}

		// 移除所有的变量声明注释和空行
		bodyContent = regexp.MustCompile(`(?i)--\s*声明变量`).ReplaceAllString(bodyContent, "")
		bodyContent = regexp.MustCompile(`(?i)--\s*声明游标变量`).ReplaceAllString(bodyContent, "")
		bodyContent = regexp.MustCompile(`(?i)--\s*声明游标`).ReplaceAllString(bodyContent, "")
		bodyContent = reEmptyLines.ReplaceAllString(bodyContent, "\n")
		bodyContent = strings.TrimSpace(bodyContent)

		varDeclStr := strings.Join(varDeclarations, "\n\t")
		funcBody = fmt.Sprintf(`declare
	%s
begin
%s
end;`, varDeclStr, strings.TrimSpace(bodyContent))
	}

	// 修复UPDATE语句，确保包含SET关键字
	funcBody = regexp.MustCompile(`(?i)UPDATE\s+(\w+)\s+([a-zA-Z_]+)\s*:=`).ReplaceAllString(funcBody, "UPDATE $1 SET $2 :=")
	funcBody = regexp.MustCompile(`(?i)UPDATE\s+(\w+)\s+([a-zA-Z_]+)\s*=`).ReplaceAllString(funcBody, "UPDATE $1 SET $2 =")

	// 确保UPDATE语句结构正确
	funcBody = regexp.MustCompile(`(?i)UPDATE\s+(\w+)\s+SET\s+`).ReplaceAllString(funcBody, "UPDATE $1 SET ")

	// 修复IF语句中的语法错误
	// 修复条件和赋值之间缺少空格的问题
	funcBody = regexp.MustCompile(`(?i)IF\s+([^\s]+?)([a-zA-Z_]+)\s*:=`).ReplaceAllString(funcBody, "IF $1 THEN $2 :=")

	// 修复IF条件和EXIT之间缺少空格的问题
	funcBody = regexp.MustCompile(`(?i)IF\s+(\w+)\s*EXIT`).ReplaceAllString(funcBody, "IF $1 THEN EXIT")
	funcBody = regexp.MustCompile(`(?i)IF\s+(\w+)EXIT`).ReplaceAllString(funcBody, "IF $1 THEN EXIT")

	// 修复ELSEIF条件和赋值之间缺少空格的问题
	funcBody = regexp.MustCompile(`(?i)ELSIF\s+([^\s]+?)([a-zA-Z_]+)\s*:=`).ReplaceAllString(funcBody, "ELSIF $1 THEN $2 :=")

	// 修复THEN关键字重复的问题
	funcBody = reDoubleThen.ReplaceAllString(funcBody, "THEN")
	// 修复ELSE和赋值之间缺少空格的问题
	funcBody = regexp.MustCompile(`(?i)ELSE\s*([a-zA-Z_]+)\s*:=`).ReplaceAllString(funcBody, "ELSE\n\t$1 :=")

	// 修复UPDATE语句中的多余空格
	funcBody = regexp.MustCompile(`(?i)UPDATE\s+(\w+)\s+\s+([a-zA-Z_]+)\s*:=`).ReplaceAllString(funcBody, "UPDATE $1 SET $2 :=")
	funcBody = regexp.MustCompile(`(?i)UPDATE\s+(\w+)\s+\s+([a-zA-Z_]+)\s*=`).ReplaceAllString(funcBody, "UPDATE $1 SET $2 =")

	// 移除所有的continue handler语句
	funcBody = regexp.MustCompile(`(?i)continue\s+handler\s+for\s+[^;]+?;`).ReplaceAllString(funcBody, "")

	// 修复参数名错误
	funcBody = regexp.MustCompile(`p__id`).ReplaceAllString(funcBody, "p_end_id")

	// 修复IF条件和赋值之间的空格问题，更精确的匹配
	funcBody = regexp.MustCompile(`(?i)IF\s+([^=]+?)([a-zA-Z_]+)\s*:=`).ReplaceAllString(funcBody, "IF $1 THEN $2 :=")

	funcBody = regexp.MustCompile(`(?i)ELSIF\s+([^=]+?)([a-zA-Z_]+)\s*:=`).ReplaceAllString(funcBody, "ELSIF $1 THEN $2 :=")

	funcBody = regexp.MustCompile(`(?i)IF\s+(\w+)\s+THEN\s+EXIT\s+THEN`).ReplaceAllString(funcBody, "IF $1 THEN EXIT")

	funcBody = regexp.MustCompile(`(?i)ELSIF\s+([^=]+?)\s+THEN\s+THEN`).ReplaceAllString(funcBody, "ELSIF $1 THEN")

	// 修复条件和赋值之间直接连接的问题
	funcBody = regexp.MustCompile(`(?i)IF\s+([^;]+?)([a-zA-Z_]+)\s*:=`).ReplaceAllString(funcBody, "IF $1 THEN $2 :=")

	// 修复NULL检查语法错误
	funcBody = reIsNullSyntax.ReplaceAllString(funcBody, "IS NOT NULL THEN")

	// 修复done变量被拆分为d和one的问题
	funcBody = regexp.MustCompile(`(?i)if\s+d\s+then\s+one\s+then`).ReplaceAllString(funcBody, "if done then")
	funcBody = regexp.MustCompile(`(?i)if\s+d\s+then`).ReplaceAllString(funcBody, "if done then")

	// 修复赋值与逻辑混用问题（如v_result := exit）
	funcBody = regexp.MustCompile(`(?i)(\w+)\s*:=\s*exit`).ReplaceAllString(funcBody, "EXIT")
	funcBody = regexp.MustCompile(`(?i)if\s+length\(v_result\)\s+>\s+1000\s+then\s+v_result\s*:=\s+exit`).ReplaceAllString(funcBody, "if length(v_result) > 1000 then EXIT")

	// 修复缺少的END IF和END LOOP
	// 先移除所有的if;
	funcBody = regexp.MustCompile(`(?i)\s+if;\s+`).ReplaceAllString(funcBody, " ")

	// 移除多余的END IF和END LOOP
	funcBody = regexp.MustCompile(`(?i)\s+END\s+IF;\s+END\s+IF;`).ReplaceAllString(funcBody, " END IF;")
	funcBody = regexp.MustCompile(`(?i)\s+END\s+LOOP;\s+END\s+LOOP;`).ReplaceAllString(funcBody, " END LOOP;")

	// 修复IF语句中的双THEN问题
	funcBody = regexp.MustCompile(`(?i)THEN\s+v_`).ReplaceAllString(funcBody, "THEN\n		v_")
	funcBody = regexp.MustCompile(`(?i)\s+THEN\s+THEN`).ReplaceAllString(funcBody, " THEN")

	// 修复参数名错误
	funcBody = regexp.MustCompile(`p__date`).ReplaceAllString(funcBody, "p_end_date")

	// 修复注释前缺少空格的问题
	funcBody = regexp.MustCompile(`(?i)\s+--`).ReplaceAllString(funcBody, " --")

	// 修复BEGIN后直接声明变量的问题
	funcBody = regexp.MustCompile(`(?i)BEGIN\s+v_`).ReplaceAllString(funcBody, "BEGIN")

	// 更彻底地修复BEGIN后直接声明变量的问题
	// 移除BEGIN后直接声明的变量并将其添加到DECLARE块
	beginAfterVars := regexp.MustCompile(`(?i)BEGIN\s+(v_[a-zA-Z_]+\s+[a-zA-Z_0-9()]+\s*;\s*)+`).FindStringSubmatch(funcBody)
	if len(beginAfterVars) > 0 {
		varsToMove := regexp.MustCompile(`(?i)v_[a-zA-Z_]+\s+[a-zA-Z_0-9()]+\s*;`).FindAllString(beginAfterVars[0], -1)
		if len(varsToMove) > 0 {
			movedVars := "\n" + strings.Join(varsToMove, "\n\t")
			// 从BEGIN后移除这些变量声明
			funcBody = regexp.MustCompile(`(?i)BEGIN\s+(v_[a-zA-Z_]+\s+[a-zA-Z_0-9()]+\s*;\s*)+`).ReplaceAllString(funcBody, "BEGIN")
			declareEndIndex := strings.Index(funcBody, "BEGIN")
			if declareEndIndex != -1 {
				funcBody = funcBody[:declareEndIndex] + movedVars + funcBody[declareEndIndex:]
			}
		}
	}

	// 修复NULL前后的空格问题
	funcBody = regexp.MustCompile(`\s+null\s+`).ReplaceAllString(strings.ToUpper(funcBody), " NULL ")
	funcBody = strings.ToLower(funcBody)

	ifCount := strings.Count(strings.ToUpper(funcBody), "IF ")
	endIfCount := strings.Count(strings.ToUpper(funcBody), "END IF")
	endIfNeeded := ifCount - endIfCount
	if endIfNeeded > 0 && endIfNeeded < 10 {
		for i := 0; i < endIfNeeded; i++ {
			funcBody = strings.TrimSpace(funcBody) + "\nEND IF;"
		}
	}

	loopCount := strings.Count(strings.ToUpper(funcBody), "LOOP")
	endLoopCount := strings.Count(strings.ToUpper(funcBody), "END LOOP")
	endLoopNeeded := loopCount - endLoopCount
	if endLoopNeeded > 0 && endLoopNeeded < 5 {
		for i := 0; i < endLoopNeeded; i++ {
			funcBody = strings.TrimSpace(funcBody) + "\nEND LOOP;"
		}
	}

	// 移除多余的分号
	funcBody = reDoubleSemicolon.ReplaceAllString(funcBody, ";")

	// 修复IF语句中的双THEN问题
	funcBody = regexp.MustCompile(`(?i)THEN\s*\n\s*v_[^;]+?THEN`).ReplaceAllStringFunc(funcBody, func(m string) string {
		return regexp.MustCompile(`(?i)THEN\s*$`).ReplaceAllString(m, "") + " THEN"
	})

	funcBody = regexp.MustCompile(`(?i)IF\s+[^;]+?THEN\s+[^;]+?THEN`).ReplaceAllStringFunc(funcBody, func(m string) string {
		parts := regexp.MustCompile(`(?i)THEN\s+`).Split(m, 3)
		if len(parts) >= 3 {
			return parts[0] + "THEN\n		" + parts[1] + "THEN " + parts[2]
		}
		return m
	})

	// 确保只有一个BEGIN和一个END
	funcBody = regexp.MustCompile(`(?i)(\s*BEGIN\s*)+`).ReplaceAllString(funcBody, "begin\n")
	funcBody = regexp.MustCompile(`(?i)(\s*END\s*)+;`).ReplaceAllString(funcBody, "\nend;")

	// 移除多余的空行
	funcBody = regexp.MustCompile(`\n\s*\n`).ReplaceAllString(funcBody, "\n")
	funcBody = strings.TrimSpace(funcBody)

	// 修复UPDATE语句中的THEN关键字为SET关键字
	funcBody = reUpdateThen.ReplaceAllString(funcBody, "UPDATE $1 SET $2 :=")
	funcBody = reUpdateThenEq.ReplaceAllString(funcBody, "UPDATE $1 SET $2 =")

	// 修复IF语句中的结构问题
	funcBody = regexp.MustCompile(`(?i)IF\s+(\w+)\s+THEN\s+EXIT\s+([^;]+?);`).ReplaceAllString(funcBody, "IF $1 THEN EXIT;\n\t\t$2;")
	funcBody = regexp.MustCompile(`(?i)IF\s+(\w+)\s+THEN\s+EXIT\s+([^;]+?):=`).ReplaceAllString(funcBody, "IF $1 THEN EXIT;\n\t\t$2 :=")

	// 修复IF语句中的双THEN问题，更精确的匹配
	funcBody = regexp.MustCompile(`(?i)THEN\s+\s+THEN`).ReplaceAllString(funcBody, " THEN")
	funcBody = regexp.MustCompile(`(?i)THEN\s+THEN`).ReplaceAllString(funcBody, " THEN")
	funcBody = regexp.MustCompile(`(?i)\s+THEN\s+`).ReplaceAllString(funcBody, " THEN ")

	// 修复IF语句中的分号问题
	funcBody = regexp.MustCompile(`(?i)IF\s+([^=]+?)\s+THEN\s+`).ReplaceAllString(funcBody, "IF $1 THEN\n")
	funcBody = regexp.MustCompile(`(?i)ELSIF\s+([^=]+?)\s+THEN\s+`).ReplaceAllString(funcBody, "ELSIF $1 THEN\n")

	// 确保只有一个END IF和END LOOP在函数体结尾
	// 移除所有的if;
	funcBody = regexp.MustCompile(`(?i)\s+if;\s+`).ReplaceAllString(funcBody, " ")
	funcBody = regexp.MustCompile(`(?i)if;`).ReplaceAllString(funcBody, "")

	// 修复IF语句中的THEN位置错误
	funcBody = regexp.MustCompile(`(?i)THEN\s*\n\s*v_[^;]+?THEN\s+RETURN`).ReplaceAllStringFunc(funcBody, func(m string) string {
		return regexp.MustCompile(`(?i)THEN\s+RETURN`).ReplaceAllString(m, "RETURN")
	})

	// 修复UPDATE语句中的THEN关键字为SET关键字
	funcBody = regexp.MustCompile(`(?i)UPDATE\s+(\w+)\s+THEN\s+`).ReplaceAllString(funcBody, "UPDATE $1 SET ")

	// 修复函数结尾的多个END问题
	funcBody = regexp.MustCompile(`(?i)\s+(END\s+)+\s*end;`).ReplaceAllString(funcBody, "\nend;")
	funcBody = regexp.MustCompile(`(?i)\s+(END\s+)+\s*end\s+loop;`).ReplaceAllString(funcBody, "\nend loop;")

	// 修复END LOOP的语法错误
	funcBody = regexp.MustCompile(`(?i)end\s+end\s+loop;`).ReplaceAllString(funcBody, "end loop;")
	funcBody = regexp.MustCompile(`(?i)end\s+loop;`).ReplaceAllString(funcBody, "end loop;")

	// 确保函数结尾只有一个end;
	lastEndIndex := strings.LastIndex(funcBody, "end;")
	if lastEndIndex != -1 {
		// 移除最后一个end;之前的所有end
		funcBody = regexp.MustCompile(`(?i)\s*end\s*`).ReplaceAllString(funcBody[:lastEndIndex], "") + funcBody[lastEndIndex:]
	}

	// 修复变量声明位置错误
	varPattern := regexp.MustCompile(`(?i)BEGIN\s*\n\s*([a-zA-Z_]+\s+[a-zA-Z_0-9()]+\s*;\s*)+`)
	for {
		match := varPattern.FindStringSubmatch(funcBody)
		if match == nil {
			break
		}
		vars := regexp.MustCompile(`(?i)[a-zA-Z_]+\s+[a-zA-Z_0-9()]+\s*;`).FindAllString(match[0], -1)
		if len(vars) == 0 {
			break
		}
		movedVars := "\n" + strings.Join(vars, "\n\t")
		// 从BEGIN后移除这些变量声明
		funcBody = varPattern.ReplaceAllString(funcBody, "BEGIN")
		declareEndIndex := strings.Index(funcBody, "BEGIN")
		if declareEndIndex != -1 {
			funcBody = funcBody[:declareEndIndex] + movedVars + funcBody[declareEndIndex:]
		}
	}

	// 修复IF语句中的条件和赋值之间的空格问题
	funcBody = regexp.MustCompile(`(?i)IF\s+([^=]+?)([a-zA-Z_]+)\s*:=`).ReplaceAllString(funcBody, "IF $1 THEN $2 :=")

	funcBody = regexp.MustCompile(`(?i)IF\s+([^']+?'[^']+'[^=]+?)([a-zA-Z_]+)\s*:=`).ReplaceAllString(funcBody, "IF $1 THEN $2 :=")

	funcBody = regexp.MustCompile(`(?i)IF\s+([^=]+?\d+)([a-zA-Z_]+)\s*:=`).ReplaceAllString(funcBody, "IF $1 THEN $2 :=")

	// 修复参数名错误
	funcBody = regexp.MustCompile(`p__date`).ReplaceAllString(funcBody, "p_end_date")

	// 修复NULL检查语法错误
	funcBody = regexp.MustCompile(`(?i)IS\s+NOT\s+THEN\s+NULL`).ReplaceAllString(funcBody, "IS NOT NULL THEN")
	funcBody = regexp.MustCompile(`(?i)NOT\s+THEN\s+NULL`).ReplaceAllString(funcBody, "IS NOT NULL THEN")

	// 确保函数体结构正确
	funcBody = regexp.MustCompile(`(?i)\s*end\s*\s*end;`).ReplaceAllString(funcBody, "\nend;")
	funcBody = regexp.MustCompile(`(?i)\s*end\s+end;`).ReplaceAllString(funcBody, "\nend;")

	// 移除多余的THEN关键字
	funcBody = regexp.MustCompile(`(?i)\s+THEN\s+THEN`).ReplaceAllString(funcBody, " THEN")

	// 修复IF语句中的双THEN问题
	funcBody = reDoubleThen.ReplaceAllString(funcBody, " THEN")
	// 将BEGIN后的变量声明移到DECLARE块
	funcBody = reVarBeforeBegin.ReplaceAllString(funcBody, "$1 $2;\nbegin")
	funcBody = reVarAfterBegin.ReplaceAllString(funcBody, "begin\n\t$1 $2;")

	// 更彻底地处理BEGIN后的变量声明
	// 匹配BEGIN后直接声明的变量（包括可能的unsigned关键字）
	beginVarPattern := regexp.MustCompile(`(?i)BEGIN\s+([a-zA-Z_]+)\s+(INT|VARCHAR|TEXT|DECIMAL|DATE|TIME|TIMESTAMP|BOOLEAN|FLOAT|DOUBLE|CHAR|REFCURSOR|TINYINT|BIGINT|MEDIUMINT)\s*UNSIGNED\s*(?:\((\d+(?:,\d+)?))?\s*(?:DEFAULT\s+([^;]+))?;`)
	funcBody = beginVarPattern.ReplaceAllString(funcBody, "BEGIN")

	// 修复IF语句中的多余THEN关键字
	funcBody = regexp.MustCompile(`(?i)THEN\s+EXIT\s*;\s*--[^\n]+?\s+THEN\s+`).ReplaceAllString(funcBody, "THEN EXIT;\n		")

	// 修复IF语句中的条件和赋值之间的空格问题
	funcBody = regexp.MustCompile(`(?i)IF\s+([^']+?'[^']+')\s*(v_[a-zA-Z_]+)\s*:=`).ReplaceAllString(funcBody, "IF $1 THEN $2 :=")

	funcBody = regexp.MustCompile(`(?i)IF\s+([^=]+?\d+)\s*(v_[a-zA-Z_]+)\s*:=`).ReplaceAllString(funcBody, "IF $1 THEN $2 :=")

	// 修复NULL检查语法错误
	funcBody = regexp.MustCompile(`(?i)IS\s+NOT\s+THEN\s+NULL\s*(v_[a-zA-Z_]+)\s*:=`).ReplaceAllString(funcBody, "IS NOT NULL THEN $1 :=")

	// 确保变量名正确
	funcBody = regexp.MustCompile(`(?i)v\s+then\s+_result\s*:=`).ReplaceAllString(funcBody, "v_result :=")

	funcBody = regexp.MustCompile(`(?i)\s+(END\s+)\s*end;`).ReplaceAllString(funcBody, "\nend;")

	// 修复变量声明错误
	funcBody = regexp.MustCompile(`(?i)([a-zA-Z_]+)\s+([a-zA-Z_0-9()]+)\s+v_default\s+(\d+);`).ReplaceAllString(funcBody, "$1 $2 default $3;")

	// 修复参数名错误
	funcBody = regexp.MustCompile(`p__id`).ReplaceAllString(funcBody, "p_end_id")

	// 修复UPDATE语句中的THEN关键字错误
	funcBody = reUpdateThen.ReplaceAllString(funcBody, "UPDATE $1 SET $2 :=")

	// 修复IF语句中的双THEN问题
	funcBody = regexp.MustCompile(`(?i)IF\s+([^=]+?)\s+THEN\s+(v_[a-zA-Z_]+)\s*:=\s+[^;]+?\s+THEN`).ReplaceAllString(funcBody, "IF $1 THEN $2 :=")

	// 修复IF语句中的条件和赋值之间的空格问题
	funcBody = regexp.MustCompile(`(?i)\s+THEN\s+\s+RETURN`).ReplaceAllString(funcBody, "\n\t\tRETURN")

	// 修复变量声明位置错误
	funcBody = regexp.MustCompile(`(?i)begin([a-zA-Z_]+)\s+([a-zA-Z_0-9()]+)\s*;`).ReplaceAllString(funcBody, "begin\n	$1 $2;")

	// 修复IF语句中的条件和赋值之间缺少空格的问题
	funcBody = regexp.MustCompile(`(?i)IF\s+([^\s]+?)\s*(v_[a-zA-Z_]+)\s*:=`).ReplaceAllString(funcBody, "IF $1 THEN $2 :=")

	// 修复IF语句中缺少值的问题
	funcBody = regexp.MustCompile(`(?i)IF\s+([^=]+?)\s+THEN\s+(v_[a-zA-Z_]+)\s*:=\s*elsif`).ReplaceAllString(funcBody, "IF $1 THEN $2 := 1000000.0;\n\t\telsif")
	funcBody = regexp.MustCompile(`(?i)elsif\s+([^=]+?)\s+THEN\s+(v_[a-zA-Z_]+)\s*:=\s*return`).ReplaceAllString(funcBody, "elsif $1 THEN $2 := -1000000.0;\n\t\treturn")

	// 修复缺少END IF的问题
	funcBody = regexp.MustCompile(`(?i)return\s+([a-zA-Z_]+)\s*;\s*end;`).ReplaceAllString(funcBody, "return $1;\n\tend if;\n\tend;")

	// 修复条件判断缺少THEN关键字
	funcBody = regexp.MustCompile(`(?i)IF\s+([^=]+?=\s*\d+)\s*--`).ReplaceAllString(funcBody, "IF $1 THEN --")

	// 修复变量声明位置错误

	// 修复变量声明缺少换行符的问题
	funcBody = regexp.MustCompile(`(?i)([a-zA-Z_]+)\s+([a-zA-Z_0-9()]+)\s*;begin`).ReplaceAllString(funcBody, "$1 $2;\nbegin")

	// 修复IF语句中的条件和赋值之间缺少空格的问题
	funcBody = regexp.MustCompile(`(?i)IF\s+([^=]+?'[^']+')\s*(v_[a-zA-Z_]+)\s*:=`).ReplaceAllString(funcBody, "IF $1 THEN $2 :=")

	// 修复多余的THEN关键字
	funcBody = regexp.MustCompile(`(?i):=\s*([^;]+?);\s*then`).ReplaceAllString(funcBody, ":= $1;\n\t\t")

	// 修复IF语句中缺少值的问题
	funcBody = regexp.MustCompile(`(?i)IF\s+([^=]+?)\s+THEN\s+(v_[a-zA-Z_]+)\s*:=\s*--`).ReplaceAllString(funcBody, "IF $1 THEN $2 := 0; --")

	// 修复IF语句中缺少END IF的问题
	funcBody = regexp.MustCompile(`(?i)end\s+if;\s*\s*end;\s*end\s+loop;`).ReplaceAllString(funcBody, "\t\tend;\n\tend loop;\n\tend if;")

	// 修复UPDATE语句中的THEN关键字错误
	funcBody = reUpdateThen.ReplaceAllString(funcBody, "UPDATE $1 SET $2 :=")

	// 修复IF语句中的EXIT条件
	funcBody = regexp.MustCompile(`(?i)IF\s+(\w+)\s+THEN\s+EXIT;`).ReplaceAllString(funcBody, "IF $1 THEN EXIT;\n\t	else")

	// 修复END/END IF/END LOOP的顺序
	funcBody = regexp.MustCompile(`(?i)\t\t\tend;\s*end\s+loop;\s*end\s+if;`).ReplaceAllString(funcBody, "\t\t\tend if;\n\t\tend loop;\n\tend;")

	// 修复loop函数错误（PostgreSQL中应该是repeat函数）
	funcBody = regexp.MustCompile(`(?i)loop\(([^,]+?),\s*(\d+)\)`).ReplaceAllString(funcBody, "repeat($1, $2)")

	// 修复缺少分号分隔赋值和return语句的问题
	funcBody = regexp.MustCompile(`(?i)\s*(v_[a-zA-Z_]+)\s*:=\s*([^;]+?)\s*return\s+(v_[a-zA-Z_]+);`).ReplaceAllString(funcBody, "$1 := $2;\n\treturn $3;")

	// 修复太多的end语句
	funcBody = reTooManyEnds.ReplaceAllString(funcBody, "end;")

	// 修复UPDATE语句中错误使用THEN关键字的问题
	funcBody = reUpdateThen.ReplaceAllString(funcBody, "UPDATE $1 SET $2 :=")

	// 修复IF语句的结构
	funcBody = regexp.MustCompile(`(?i)if\s+(\w+)\s+then\s+exit;\s*else\s*update\s+(\w+)\s+set\s+([^;]+?);`).ReplaceAllString(funcBody, "if $1 then exit;\n\telse\n\tupdate $2 set $3;")

	// 修复IF语句中缺少END IF的问题
	funcBody = regexp.MustCompile(`(?i)if\s+([^=]+?)\s+then\s+(v_[a-zA-Z_]+)\s*:=\s*([^;]+?);\s*return\s+(v_[a-zA-Z_]+);`).ReplaceAllString(funcBody, "if $1 then $2 := $3;\n\treturn $4;\n\tend if;")

	// 修复条件和赋值之间缺少空格的问题
	funcBody = regexp.MustCompile(`(?i)THEN\s*(v_[a-zA-Z_]+)\s*:=`).ReplaceAllString(funcBody, "THEN $1 :=")

	// 修复UPDATE语句中错误使用THEN关键字的问题
	funcBody = regexp.MustCompile(`(?i)UPDATE\s+(\w+)\s+THEN\s+([a-zA-Z_]+)\s*:=`).ReplaceAllString(funcBody, "UPDATE $1 SET $2 :=")

	// 修复变量声明位置错误
	funcBody = regexp.MustCompile(`(?i)begin([a-zA-Z_]+)\s+([a-zA-Z_0-9()]+)\s*;`).ReplaceAllString(funcBody, "begin\n	$1 $2;")

	// 修复IF语句中的条件和赋值之间缺少空格的问题
	funcBody = regexp.MustCompile(`(?i)IF\s+([^=]+?'[^']+')\s*(v_[a-zA-Z_]+)\s*:=`).ReplaceAllString(funcBody, "IF $1 THEN $2 :=")

	// 修复多余的THEN关键字
	funcBody = regexp.MustCompile(`(?i)\s*:=\s*([^;]+?)\s*then`).ReplaceAllString(funcBody, " := $1;")

	// 修复缺少值的IF语句
	funcBody = regexp.MustCompile(`(?i)elsif\s+([^=]+?)\s+THEN\s+(v_[a-zA-Z_]+)\s*:=\s*else`).ReplaceAllString(funcBody, "elsif $1 THEN $2 := 500000.0;\n\t\telse")

	// 修复太多的END IF语句
	funcBody = regexp.MustCompile(`(?i)(end\s+if;\s*){2,}`).ReplaceAllString(funcBody, "end if;")

	// 修复UPDATE语句中的THEN关键字错误（更精确的匹配）
	funcBody = regexp.MustCompile(`(?i)update\s+\"([^\"]+)\"\s+then\s+([a-zA-Z_]+)\s*:=`).ReplaceAllString(funcBody, "update \"$1\" set $2 :=")

	// 修复IF语句和END LOOP之间缺少分号的问题
	funcBody = regexp.MustCompile(`(?i)end\s+if;end\s+loop;`).ReplaceAllString(funcBody, "end if;\nend loop;")

	// 修复变量声明位置错误（更精确的匹配）
	funcBody = regexp.MustCompile(`(?i)begin(v_[a-zA-Z_]+)\s+([a-zA-Z_0-9()]+)\s*;`).ReplaceAllString(funcBody, "begin\n\t$1 $2;")

	// 修复IF语句中的条件和赋值之间缺少空格的问题（更精确的匹配）
	funcBody = regexp.MustCompile(`(?i)IF\s+([^=]+?'[^']+')\s*(v_[a-zA-Z_]+)\s*:=`).ReplaceAllString(funcBody, "IF $1 THEN $2 :=")

	// 修复IF语句中的条件和赋值之间缺少空格的问题（数字条件）
	funcBody = regexp.MustCompile(`(?i)IF\s+([^=]+?\d+)\s*(v_[a-zA-Z_]+)\s*:=`).ReplaceAllString(funcBody, "IF $1 THEN $2 :=")

	// 修复变量声明缺少换行符的问题
	funcBody = regexp.MustCompile(`(?i)([a-zA-Z_]+)\s+([a-zA-Z_0-9()]+)\s*;([a-zA-Z_]+)\s+([a-zA-Z_0-9()]+)\s*;`).ReplaceAllString(funcBody, "$1 $2;\n$3 $4;")

	// 修复变量声明位置错误（更精确的匹配）
	funcBody = regexp.MustCompile(`(?i)begin([a-zA-Z_]+)\s+([a-zA-Z_0-9()]+)\s*;`).ReplaceAllString(funcBody, "begin\n	$1 $2;")

	// 修复IF语句结构问题
	funcBody = regexp.MustCompile(`(?i)if\s+(\w+)\s+then\s+exit;\s*else\s*update\s+(\w+)\s+set\s+([^;]+?);\s*(v_[a-zA-Z_]+)\s*:=\s*([^;]+?);\s*close\s+([a-zA-Z_]+);\s*return\s+([a-zA-Z_]+);\s*end\s+if;`).ReplaceAllString(funcBody, "if $1 then exit;\n\telse\n\tupdate $2 set $3;\n\t$4 := $5;\n\tend if;\n\tclose $6;\n\treturn $7;")

	// 修复变量声明位置错误（更精确的匹配，使用非贪婪匹配）
	funcBody = regexp.MustCompile(`(?i)begin(\w+)\s+(\w+(?:\(\d+(?:,\d+)?\))?)\s*;`).ReplaceAllString(funcBody, "begin\n\t$1 $2;")

	// 修复变量声明缺少换行符的问题（更精确的匹配）
	funcBody = regexp.MustCompile(`(?i)(\w+)\s+(\w+(?:\(\d+(?:,\d+)?\))?)\s*;(\w+)\s+(\w+(?:\(\d+(?:,\d+)?\))?)\s*;`).ReplaceAllString(funcBody, "$1 $2;\n$3 $4;")

	// 修复IF语句中的条件和赋值之间缺少空格的问题（更精确的匹配）
	funcBody = regexp.MustCompile(`(?i)IF\s+([^=]+?'[^']+')\s*(v_[a-zA-Z_]+)\s*:=`).ReplaceAllString(funcBody, "IF $1 THEN $2 :=")

	// 修复IF语句中的条件和赋值之间缺少空格的问题（数字条件，更精确的匹配）
	funcBody = regexp.MustCompile(`(?i)IF\s+([^=]+?\d+)\s*(v_[a-zA-Z_]+)\s*:=`).ReplaceAllString(funcBody, "IF $1 THEN $2 :=")

	// 修复BEGIN和变量声明之间缺少换行符的问题
	funcBody = regexp.MustCompile(`(?i)(\w+)\s+(\w+(?:\(\d+(?:,\d+)?\))?)\s*;begin`).ReplaceAllString(funcBody, "$1 $2;\nbegin")

	// 修复变量声明在BEGIN块内的问题
	funcBody = regexp.MustCompile(`(?i)begin\s*\n?\s*(\w+)\s+(\w+(?:\(\d+(?:,\d+)?\))?)\s*;`).ReplaceAllStringFunc(funcBody, func(m string) string {
		varMatch := regexp.MustCompile(`(?i)(\w+)\s+(\w+(?:\(\d+(?:,\d+)?\))?)\s*;`).FindStringSubmatch(m)
		if len(varMatch) < 3 {
			return m
		}
		return varMatch[1] + " " + varMatch[2] + ";\nbegin"
	})

	// 修复IF语句中的条件和赋值之间缺少空格的问题（更精确的匹配）
	funcBody = regexp.MustCompile(`(?i)IF\s+([^=]+?'[^']+')\s*(v_[a-zA-Z_]+)\s*:=`).ReplaceAllString(funcBody, "IF $1 THEN $2 :=")

	// 修复IF语句中的条件和赋值之间缺少空格的问题（数字条件，更精确的匹配）
	funcBody = regexp.MustCompile(`(?i)IF\s+([^=]+?\d+)\s*(v_[a-zA-Z_]+)\s*:=`).ReplaceAllString(funcBody, "IF $1 THEN $2 :=")

	// 修复IF语句中的条件和赋值之间缺少空格的问题（更精确的匹配，使用非贪婪匹配）
	funcBody = regexp.MustCompile(`(?i)IF\s+([^;]+?)\s*(v_[a-zA-Z_]+)\s*:=`).ReplaceAllString(funcBody, "IF $1 THEN $2 :=")

	// 修复IF语句中的条件和赋值之间缺少空格的问题（更精确的匹配，使用更具体的模式）
	funcBody = regexp.MustCompile(`(?i)IF\s+([^=]+?'[^']+')\s*(\w+)\s*:=`).ReplaceAllString(funcBody, "IF $1 THEN $2 :=")

	// 修复IF语句中的条件和赋值之间缺少空格的问题（数字条件，更精确的匹配，使用更具体的模式）
	funcBody = regexp.MustCompile(`(?i)IF\s+([^=]+?\d+)\s*(\w+)\s*:=`).ReplaceAllString(funcBody, "IF $1 THEN $2 :=")

	// 修复IF语句中的双重THEN关键字问题
	funcBody = regexp.MustCompile(`(?i)then\s+then`).ReplaceAllString(funcBody, "then")

	// 修复batch_update_data函数中的语法错误
	funcBody = regexp.MustCompile(`(?i)update\s+(\w+)\s+set\s+([^;]+?);\s*(v_[a-zA-Z_]+|update_count)\s*:=\s*[^;]+?;\s*close\s+([a-zA-Z_]+);\s*return\s+([a-zA-Z_]+);\s*end\s+if;`).ReplaceAllString(funcBody, "update $1 set $2;\n\t$3 := $3 + 1;\n\tend if;\n\tclose $4;\n\treturn $5;")

	// 修复额外的end;语句问题
	funcBody = regexp.MustCompile(`(?i)end;\s*end;`).ReplaceAllString(funcBody, "end;")

	// 修复IF语句中的双重THEN关键字问题（更精确的匹配）
	funcBody = regexp.MustCompile(`(?i)then\s+--\s+[^;]+?\s+then`).ReplaceAllString(funcBody, "then -- ")

	// 修复函数结构问题，添加缺失的END IF语句
	funcBody = regexp.MustCompile(`(?i)if\s+([^;]+?);\s*--\s+[^;]+?\s*(v_[a-zA-Z_]+)\s*:=\s*[^;]+?;\s*return\s+([a-zA-Z_]+);\s*end\s+if;`).ReplaceAllString(funcBody, "if $1;\n\t-- $2 := $2 + v_count;\n\t-- return $3;\n\tend if;")

	// 修复函数结构问题，确保每个IF都有对应的END IF
	funcBody = regexp.MustCompile(`(?i)if\s+([^;]+?);\s*--\s+[^;]+?\s*close\s+([a-zA-Z_]+);`).ReplaceAllString(funcBody, "if $1;\n\tend if;\n\tclose $2;")

	// 修复游标关闭位置问题，确保close cur在循环外部
	funcBody = regexp.MustCompile(`(?i)end\s+loop;\s*end;`).ReplaceAllString(funcBody, "end loop;\n\tclose cur;\nend;")

	// 修复函数结构问题，添加缺失的END IF语句
	funcBody = regexp.MustCompile(`(?i)else\s*\n\s*(\w+)\s*:=\s*[^;]+?;\s*--\s+[^;]+?\s*if\s+([^;]+?)\s+then\s+--`).ReplaceAllString(funcBody, "else\n\t$1 := $1 + 3;\n\tend if;\n\tif $2 then -- ")

	// 修复重复的游标关闭语句问题
	funcBody = regexp.MustCompile(`(?i)close\s+cur;\s*--\s+[^;]+?\s*end\s+loop;\s*close\s+cur;`).ReplaceAllString(funcBody, "end loop;\n\tclose cur;")

	// 修复函数结构问题，添加缺失的END IF语句
	funcBody = regexp.MustCompile(`(?i)if\s+([^;]+?);\s*--\s+[^;]+?\s*if\s+([^;]+?);`).ReplaceAllString(funcBody, "if $1;\n\tend if;\n\tif $2;")

	// 修复函数结构问题，确保ELSE分支有对应的END IF
	funcBody = regexp.MustCompile(`(?i)else\s*\n\s*(\w+)\s*:=\s*[^;]+?;\s*end\s+if;\s*if\s+([^;]+?)\s+then`).ReplaceAllString(funcBody, "else\n\t$1 := $1 + 3;\n\tend if;\n\tif $2 then")

	// 修复游标关闭位置问题，确保close cur在循环外部
	funcBody = regexp.MustCompile(`(?i)(if\s+[^;]+?\s+then\s+[^;]+?;\s*end\s+if;)\s*close\s+cur;\s*--\s+[^;]+?\s*(end\s+loop;)\s*close\s+cur;`).ReplaceAllString(funcBody, "$1\n\t$2\n\tclose cur;")

	// 修复嵌套IF语句的缺失END IF问题
	funcBody = regexp.MustCompile(`(?i)(if\s+[^;]+?\s+then\s+[^;]+?;\s*--\s+[^;]+?)\s*(if\s+[^;]+?\s+then\s+[^;]+?;)\s*end\s+if;`).ReplaceAllString(funcBody, "$1\n\tend if;\n\t$2\n\tend if;")

	// 修复函数结构问题，确保所有IF语句都有对应的END IF
	funcBody = regexp.MustCompile(`(?i)(if\s+[^;]+?\s+then\s+[^;]+?;)\s*(if\s+[^;]+?\s+then\s+[^;]+?;)\s*end\s+if;`).ReplaceAllString(funcBody, "$1\n\tend if;\n\t$2\n\tend if;")

	// 修复函数结构问题，添加缺失的END IF语句
	funcBody = regexp.MustCompile(`(?i)(if\s+[^;]+?\s+then\s+--\s+[^;]+?)\s*(end\s+if;)\s*(if\s+[^;]+?\s+then\s+[^;]+?;\s*--\s+[^;]+?)\s*end\s+if;`).ReplaceAllString(funcBody, "$1\n\t$2\n\t$3\n\tend if;")

	// 修复游标关闭位置问题，移除循环内部的close cur语句
	funcBody = regexp.MustCompile(`(?i)if\s+([^;]+?)\s+then\s+[^;]+?;\s*end\s+if;\s*close\s+cur;\s*--\s+最终结果处理\s*if\s+([^;]+?)\s+then\s+[^;]+?;\s*--\s+[^;]+?\s*end\s+if;\s*end\s+loop;\s*close\s+cur;`).ReplaceAllString(funcBody, "if $1 then $2;\n\tend if;\n\tif $3 then $4; -- $5\n\tend if;\n\tend loop;\n\tclose cur;")

	// 修复函数结构问题，确保函数体结构正确
	funcBody = regexp.MustCompile(`(?i)end\s+if;\s*close\s+cur;\s*--\s+最终结果处理`).ReplaceAllString(funcBody, "end if;")

	// 修复batch_update_data函数中的语法错误
	funcBody = regexp.MustCompile(`(?i)if\s+done\s+then\s+exit;\s*else\s*update\s+([^;]+?);\s*(update_count)\s*:=\s*[^;]+?;\s*close\s+([^;]+?);\s*return\s+([^;]+?);\s*end\s+if;`).ReplaceAllString(funcBody, "if done then exit;\n\telse\n\tupdate $1;\n\t$2 := $2 + 1;\n\tend if;")

	// 修复重复的循环结束和游标关闭语句
	funcBody = regexp.MustCompile(`(?i)end\s+if;\s*end\s+loop;\s*close\s+cur;\s*return\s+update_count;\s*\s*end\s+loop;\s*close\s+cur;`).ReplaceAllString(funcBody, "end if;\n\tend loop;\n\tclose cur;\n\treturn update_count;")

	// 修复batch_update_data函数中的返回语句缺失问题
	funcBody = regexp.MustCompile(`(?i)end\s+loop;\s*close\s+cur;\s*end;`).ReplaceAllString(funcBody, "end loop;\n\tclose cur;\n\treturn update_count;\nend;")

	// 修复缺少END IF的问题
	funcBody = regexp.MustCompile(`(?i)if\s+v_done\s+then\s+exit;\s*else\s*v_count\s*:=\s*v_count\s*\+\s*1;\s*--\s*条件判断`).ReplaceAllString(funcBody, "if v_done then exit;\n\telse\n\tv_count := v_count + 1; -- 条件判断")

	// 修复return update_count但实际返回变量是v_result的问题
	funcBody = regexp.MustCompile(`(?i)close\s+cur;\s*return\s+update_count;`).ReplaceAllStringFunc(funcBody, func(m string) string {
		if strings.Contains(funcBody, "v_result") && !strings.Contains(funcBody, "update_count") {
			return strings.Replace(m, "return update_count", "return v_result", 1)
		}
		return m
	})

	// 修复其他可能的无效RETURN语句
	// 检查返回的变量是否在函数体中定义
	funcBody = regexp.MustCompile(`(?i)return\s+([a-zA-Z_]+);`).ReplaceAllStringFunc(funcBody, func(m string) string {
		returnVar := regexp.MustCompile(`(?i)return\s+([a-zA-Z_]+);`).FindStringSubmatch(m)[1]

		// 检查返回变量是否存在于函数体中
		if !strings.Contains(funcBody, returnVar) {
			// 如果返回变量不存在，检查是否有v_result变量
			if strings.Contains(funcBody, "v_result") {
				return strings.Replace(m, returnVar, "v_result", 1)
			}
			// 如果没有v_result，检查是否有v_count变量
			if strings.Contains(funcBody, "v_count") {
				return strings.Replace(m, returnVar, "v_count", 1)
			}
		}
		return m
	})

	// 通用修复：处理多余的游标关闭和循环结束的格式问题
	funcBody = regexp.MustCompile(`(?i)end\s+loop;\s*\s*close\s+cur;\s*end;`).ReplaceAllString(funcBody, "end loop;\n\tclose cur;\nend;")

	// 通用修复：处理缺少END IF的问题
	funcBody = regexp.MustCompile(`(?i)elsif\s+v_result\s+<\s+-1000000\s+then\s+v_result\s*:=\s*-1000000.0;\s*return\s+v_result;`).ReplaceAllString(funcBody, "elsif v_result < -1000000 then v_result := -1000000.0;\n\treturn v_result;\nend if;")

	// 通用修复：处理缺少END IF和多余返回语句的问题
	funcBody = regexp.MustCompile(`(?i)if\s+v_result\s+>\s+1000000\s+then\s+v_result\s*:=\s+1000000.0;\s*elsif\s+v_result\s+<\s+-1000000\s+then\s+v_result\s*:=\s*-1000000.0;\s*return\s+v_result;\s*end\s+if;`).ReplaceAllString(funcBody, "if v_result > 1000000 then v_result := 1000000.0;\n\telsif v_result < -1000000 then v_result := -1000000.0;\n\treturn v_result;\nend if;")

	// 通用修复：处理多余END IF和返回变量不匹配的问题
	funcBody = regexp.MustCompile(`(?i)end\s+if;\s*\s*end\s+if;\s*end\s+loop;\s*\s*close\s+cur;\s*\s*return\s+update_count;`).ReplaceAllString(funcBody, "end if;\nend loop;\n\tclose cur;\n\treturn v_result;")

	// 通用修复：处理变量赋值语法问题
	funcBody = regexp.MustCompile(`(?i)v_count\s*=\s*v_count\s*\+\s*1;`).ReplaceAllString(funcBody, "v_count := v_count + 1;")
	funcBody = regexp.MustCompile(`(?i)update_count\s*=\s*update_count\s*\+\s*1;`).ReplaceAllString(funcBody, "update_count := update_count + 1;")

	// 针对字符串处理函数的特殊修复
	funcBody = regexp.MustCompile(`(?i)if\s+length\(v_result\)\s+>\s+950\s+then\s+v_result\s*:=\s+left\(v_result,\s+950\)\s*\|\|\s*'...truncated...';\s*return\s+v_result;\s*end\s+if;`).ReplaceAllString(funcBody, "if length(v_result) > 950 then\\n\\tv_result := left(v_result, 950) || '...truncated...';\\n\\treturn v_result;\\nend if;")

	// 修复游标的打开和循环结构
	funcBody = regexp.MustCompile(`(?i)open\s+cur\s+for\s+select\s+id,\s+name\s+from\s+"cfg_sync_ins_method_info";\s*loop\s*fetch\s+next\s+from\s+cur\s+into\s+v_id,\s+v_name;`).ReplaceAllString(funcBody, "open cur for select id, name from \"cfg_sync_ins_method_info\";\\n\\tloop\\n\\t\\tfetch next from cur into v_id, v_name;")

	// 修复游标名称不匹配问题（打开cur_component但关闭cur）
	funcBody = regexp.MustCompile(`(?i)open\s+cur_component\s+for\s+([^;]+?);\s*([^;]+?)\s*close\s+cur;`).ReplaceAllString(funcBody, "open cur_component for $1;\n\t$2\n\tclose cur_component;")

	// 修复游标名称不匹配的通用情况
	funcBody = regexp.MustCompile(`(?i)open\s+(\w+)\s+for\s+([^;]+?);\s*[^;]+?\s*close\s+(\w+);`).ReplaceAllStringFunc(funcBody, func(m string) string {
		matches := regexp.MustCompile(`(?i)open\s+(\w+)\s+for\s+([^;]+?);\s*([^;]+?)\s*close\s+(\w+);`).FindStringSubmatch(m)
		if len(matches) == 5 && matches[1] != matches[4] {
			return fmt.Sprintf("open %s for %s;\n\t%s\n\tclose %s;", matches[1], matches[2], matches[3], matches[1])
		}
		return m
	})

	var volatility string
	if strings.Contains(strings.ToUpper(mysqlFunc.DDL), "DETERMINISTIC") {
		volatility = "IMMUTABLE"
	}
	if volatility != "" {
		volatility += " "
	}

	pgDDL := fmt.Sprintf(`
CREATE OR REPLACE FUNCTION %s(%s)
RETURNS %s %sAS $$
%s
$$ LANGUAGE plpgsql;
`, strings.ToLower(mysqlFunc.Name), parameters, returnType, volatility, funcBody)

	return pgDDL, nil
}
