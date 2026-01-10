package postgres

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/yourusername/mysql2pg/internal/mysql"
)

// ConvertFunctionDDL 将MySQL函数转换为PostgreSQL函数
func ConvertFunctionDDL(mysqlFunc mysql.FunctionInfo) (string, error) {
	// 从DDL中解析参数列表
	parameters := ""
	returnType := "VOID"

	// 解析函数参数
	if idx := strings.Index(mysqlFunc.DDL, "("); idx != -1 {
		// 寻找匹配的右括号
		count := 1
		endIdx := idx + 1
		for endIdx < len(mysqlFunc.DDL) {
			if mysqlFunc.DDL[endIdx] == '(' {
				count++
			} else if mysqlFunc.DDL[endIdx] == ')' {
				count--
				if count == 0 {
					break
				}
			}
			endIdx++
		}
		if endIdx < len(mysqlFunc.DDL) {
			parameters = mysqlFunc.DDL[idx+1 : endIdx]
		} else {
			return "", fmt.Errorf("无法解析函数 %s 的参数: 找不到匹配的右括号", mysqlFunc.Name)
		}
	} else {
		return "", fmt.Errorf("无法解析函数 %s 的参数: 找不到左括号", mysqlFunc.Name)
	}

	// 解析返回类型
	if idx := strings.Index(mysqlFunc.DDL, "RETURNS"); idx != -1 {
		// 寻找RETURNS后面的内容，直到遇到下一个关键字
		startIdx := idx + 7 // "RETURNS"的长度
		// 跳过空格
		for startIdx < len(mysqlFunc.DDL) && mysqlFunc.DDL[startIdx] == ' ' {
			startIdx++
		}
		// 寻找下一个空格或左括号
		endIdx := startIdx
		for endIdx < len(mysqlFunc.DDL) && mysqlFunc.DDL[endIdx] != ' ' && mysqlFunc.DDL[endIdx] != '(' {
			endIdx++
		}
		if endIdx > startIdx {
			returnType = mysqlFunc.DDL[startIdx:endIdx]
		} else {
			return "", fmt.Errorf("无法解析函数 %s 的返回类型", mysqlFunc.Name)
		}
	} else {
		return "", fmt.Errorf("无法解析函数 %s 的返回类型: 找不到 RETURNS 关键字", mysqlFunc.Name)
	}

	// 解析函数体
	funcBody := mysqlFunc.DDL
	// 找到BEGIN关键字
	if idx := strings.Index(strings.ToUpper(funcBody), "BEGIN"); idx != -1 {
		funcBody = funcBody[idx+5:] // "BEGIN"的长度
	} else {
		return "", fmt.Errorf("无法解析函数 %s 的函数体: 找不到 BEGIN 关键字", mysqlFunc.Name)
	}
	// 移除END关键字
	funcBody = strings.TrimSpace(strings.ReplaceAll(strings.ReplaceAll(funcBody, "END$$", ""), "END;", ""))
	funcBody = strings.TrimSuffix(funcBody, ";")

	// 处理RETURN语句
	returnStmt := regexp.MustCompile(`(?i)RETURN\s+`)
	funcBody = returnStmt.ReplaceAllString(funcBody, "RETURN ")

	// 处理函数转换逻辑
	// 1. 处理类型转换
	funcBody = regexp.MustCompile(`(?i)TINYINT`).ReplaceAllString(funcBody, "SMALLINT")
	funcBody = regexp.MustCompile(`(?i)DATETIME`).ReplaceAllString(funcBody, "TIMESTAMP")

	// 2. 处理IFNULL函数，使用精确的正则表达式
	// 这个正则表达式会匹配IFNULL函数调用，确保只匹配两个参数
	// 确保在处理CONCAT之前处理IFNULL，这样CONCAT内部的IFNULL也会被处理
	for {
		// 使用正则表达式匹配IFNULL函数调用
		ifnullRegex := regexp.MustCompile(`(?i)IFNULL\s*\(([^,]+?),\s*([^,)]+?)\)`)
		newFuncBody := ifnullRegex.ReplaceAllString(funcBody, "COALESCE($1, $2)")

		if newFuncBody == funcBody {
			break
		}
		funcBody = newFuncBody
	}

	// 3. 处理IF函数，转换为CASE语句
	for {
		// 使用正则表达式匹配IF函数调用
		ifRegex := regexp.MustCompile(`(?i)IF\s*\(([^,]+?),\s*([^,]+?),\s*([^)]+?)\)`)
		newFuncBody := ifRegex.ReplaceAllStringFunc(funcBody, func(match string) string {
			parts := ifRegex.FindStringSubmatch(match)
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

	// 4. 处理CONCAT函数，使用循环确保所有CONCAT都被处理
	for {
		// 查找CONCAT函数的位置
		concatStart := strings.Index(strings.ToUpper(funcBody), "CONCAT(")
		if concatStart == -1 {
			break
		}

		// 查找对应的右括号
		depth := 0
		concatEnd := -1
		for i := concatStart + 7; i < len(funcBody); i++ {
			if funcBody[i] == '(' {
				depth++
			} else if funcBody[i] == ')' {
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

		// 提取整个CONCAT表达式
		concatExpr := funcBody[concatStart : concatEnd+1]

		// 提取参数部分
		paramsStr := funcBody[concatStart+7 : concatEnd]

		// 解析参数列表
		var params []string
		var currentParam string
		depth = 0
		inString := false
		stringChar := byte(0)

		for _, char := range paramsStr {
			// 处理字符串
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

			// 如果在字符串中，直接添加字符
			if inString {
				currentParam += string(char)
				continue
			}

			// 处理括号
			if char == '(' {
				depth++
				currentParam += string(char)
			} else if char == ')' {
				depth--
				currentParam += string(char)
			} else if char == ',' && depth == 0 {
				// 这是一个参数分隔符
				params = append(params, strings.TrimSpace(currentParam))
				currentParam = ""
			} else {
				currentParam += string(char)
			}
		}

		// 添加最后一个参数
		params = append(params, strings.TrimSpace(currentParam))

		// 构建新的表达式，用||连接参数
		newExpr := ""
		for i, param := range params {
			if i > 0 {
				newExpr += " || "
			}
			newExpr += param
		}

		// 替换CONCAT函数
		funcBody = strings.Replace(funcBody, concatExpr, newExpr, 1)
	}

	// 5. 处理其他函数转换
	funcBody = regexp.MustCompile(`(?i)CHAR_LENGTH\s*\(([^)]+?)\)`).ReplaceAllString(funcBody, "LENGTH($1)")
	funcBody = regexp.MustCompile(`(?i)REGEXP`).ReplaceAllString(funcBody, "~")

	// 4. 处理DETERMINISTIC关键字
	var volatility string
	if strings.Contains(strings.ToUpper(mysqlFunc.DDL), "DETERMINISTIC") {
		volatility = "IMMUTABLE"
	}
	if volatility != "" {
		volatility += " "
	}

	// 构建PostgreSQL函数DDL，将函数名转换为小写
	pgDDL := fmt.Sprintf(`
CREATE OR REPLACE FUNCTION %s(%s)
RETURNS %s %sAS $$
%s
$$ LANGUAGE plpgsql;
`, strings.ToLower(mysqlFunc.Name), parameters, returnType, volatility, funcBody)

	return pgDDL, nil
}
