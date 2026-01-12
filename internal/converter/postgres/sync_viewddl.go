package postgres

import (
	"fmt"
	"regexp"
	"strings"
)

// 正则表达式预编译，提高性能
var (
	// 匹配数据库名前缀，如 "db"."table" - 使用Go支持的语法
	reDBPrefix = regexp.MustCompile(`(?i)"[^"]+"\.("[^"]+")`)
	// 匹配 IFNULL 函数
	reIfnull = regexp.MustCompile(`(?i)ifnull\s*\(`)
	// 匹配 GROUP_CONCAT 函数
	reGroupConcat = regexp.MustCompile(`(?i)group_concat\s*\(\s*(?:distinct\s+)?([^)]*)\)`)
	// 匹配 ORDER BY 子句
	reOrder = regexp.MustCompile(`(?i)\s+order\s+by\s+[^,]*`)
	// 匹配 SEPARATOR 关键字
	reSep = regexp.MustCompile(`(?i)\s*separator\s*['"]([^'"]+)['"]`)
	// 匹配 IF 函数
	reIf = regexp.MustCompile(`(?i)\bif\s*\(\s*([^,()]+)\s*,\s*([^,()]+)\s*,\s*([^)]+)\)`)
	// 匹配 CONVERT 函数
	reConvert = regexp.MustCompile(`(?i)\bconvert\s*\(\s*([^,]+)\s*,\s*([^)]+)\)`)
	// 匹配 LIMIT a,b 语法
	reLimitOffset = regexp.MustCompile(`(?i)\blimit\s+(\d+)\s*,\s*(\d+)`)
	// 匹配 JSON_OBJECT 函数
	reJSONObject = regexp.MustCompile(`(?i)json_object\s*\(`)
	// 匹配 JSON_ARRAY 函数
	reJSONArray = regexp.MustCompile(`(?i)json_array\s*\(`)
	// 匹配 JSON_QUOTE 函数
	reJSONQuote = regexp.MustCompile(`(?i)json_quote\s*\(`)
	// 匹配 JSON_UNQUOTE 函数
	reJSONUnquote = regexp.MustCompile(`(?i)json_unquote\s*\(`)
	// 匹配 JSON_EXTRACT 函数
	reJSONExtract = regexp.MustCompile(`(?i)json_extract\s*\(\s*([^,]+)\s*,\s*([^)]+)\)`)
	// 匹配 JSON_KEYS 函数
	reJSONKeys = regexp.MustCompile(`(?i)json_keys\s*\(`)
	// 匹配 JSON_LENGTH 函数
	reJSONLength = regexp.MustCompile(`(?i)json_length\s*\(`)
	// 匹配 JSON_TYPE 函数
	reJSONType = regexp.MustCompile(`(?i)json_type\s*\(`)
	// 匹配 JSON_VALID 函数
	reJSONValid = regexp.MustCompile(`(?i)json_valid\s*\([^)]*\)`)
	// 匹配 JSON_VALUE 函数
	reJSONValue = regexp.MustCompile(`(?i)json_value\s*\(\s*([^,]+)\s*,\s*([^)]+)\)`)
	// 匹配 JSON_INSERT 函数
	reJSONInsert = regexp.MustCompile(`(?i)json_insert\s*\(`)
	// 匹配 JSON_SET 函数
	reJSONSet = regexp.MustCompile(`(?i)json_set\s*\(`)
	// 匹配 JSON_REPLACE 函数
	reJSONReplace = regexp.MustCompile(`(?i)json_replace\s*\(`)
	// 匹配 JSON_REMOVE 函数
	reJSONRemove = regexp.MustCompile(`(?i)json_remove\s*\(`)
	// 匹配 JSON_ARRAY_APPEND 函数
	reJSONArrayAppend = regexp.MustCompile(`(?i)json_array_append\s*\(`)
	// 匹配 JSON_ARRAY_INSERT 函数
	reJSONArrayInsert = regexp.MustCompile(`(?i)json_array_insert\s*\(`)
	// 匹配 JSON_MERGE 函数
	reJSONMerge = regexp.MustCompile(`(?i)json_merge\s*\(`)
	// 匹配 JSON_MERGE_PATCH 函数
	reJSONMergePatch = regexp.MustCompile(`(?i)json_merge_patch\s*\(`)
	// 匹配 JSON_MERGE_PRESERVE 函数
	reJSONMergePreserve = regexp.MustCompile(`(?i)json_merge_preserve\s*\(`)
	// 匹配 DATE_ADD 函数
	reDATE_ADD = regexp.MustCompile(`(?i)date_add\s*\(\s*([^,]+)\s*,\s*interval\s+([^)]+)\)`)
	// 匹配 DATE_SUB 函数
	reDATE_SUB = regexp.MustCompile(`(?i)date_sub\s*\(\s*([^,]+)\s*,\s*interval\s+([^)]+)\)`)
	// 匹配 ADDDATE 函数
	reADDDATE = regexp.MustCompile(`(?i)adddate\s*\(\s*([^,]+)\s*,\s*([^)]+)\)`)
	// 匹配 SUBDATE 函数
	reSUBDATE = regexp.MustCompile(`(?i)subdate\s*\(\s*([^,]+)\s*,\s*([^)]+)\)`)
	// 匹配 ADDTIME 函数
	reADDTIME = regexp.MustCompile(`(?i)addtime\s*\(`)
	// 匹配 SUBTIME 函数
	reSUBTIME = regexp.MustCompile(`(?i)subtime\s*\(`)
	// 匹配 DATABASE 函数
	reDATABASE = regexp.MustCompile(`(?i)database\s*\(`)
	// 匹配 USER 函数
	reUSER = regexp.MustCompile(`(?i)user\s*\(`)
	// 匹配 VERSION 函数
	reVERSION = regexp.MustCompile(`(?i)version\s*\(`)
	// 匹配 MD5 函数
	reMD5 = regexp.MustCompile(`(?i)md5\s*\(`)
	// 匹配 SHA1 函数
	reSHA1 = regexp.MustCompile(`(?i)sha1\s*\(`)
	// 匹配 SHA2 函数
	reSHA2 = regexp.MustCompile(`(?i)sha2\s*\(`)
	// 匹配 UUID 函数
	reUUID = regexp.MustCompile(`(?i)uuid\s*\(`)
	// 匹配 INET_ATON 函数
	reINET_ATON = regexp.MustCompile(`(?i)inet_aton\s*\(`)
	// 匹配 INET_NTOA 函数
	reINET_NTOA = regexp.MustCompile(`(?i)inet_ntoa\s*\(`)
	// 匹配 UNIX_TIMESTAMP 函数
	reUNIX_TIMESTAMP = regexp.MustCompile(`(?i)unix_timestamp\s*\(`)
	// 匹配 FROM_UNIXTIME 函数
	reFROM_UNIXTIME = regexp.MustCompile(`(?i)from_unixtime\s*\(`)
	// 匹配 DATE_FORMAT 函数
	reDATE_FORMAT = regexp.MustCompile(`(?i)date_format\s*\(\s*([^,]+)\s*,\s*([^)]+)\)`)
	// 匹配 STR_TO_DATE 函数
	reSTR_TO_DATE = regexp.MustCompile(`(?i)str_to_date\s*\(\s*([^,]+)\s*,\s*([^)]+)\)`)
	// 匹配 DATEDIFF 函数
	reDATEDIFF = regexp.MustCompile(`(?i)datediff\s*\(\s*([^,]+)\s*,\s*([^)]+)\)`)
	// 匹配 TIMEDIFF 函数
	reTIMEDIFF = regexp.MustCompile(`(?i)timediff\s*\(\s*([^,]+)\s*,\s*([^)]+)\)`)
	// 匹配 LAST_INSERT_ID 函数
	reLAST_INSERT_ID = regexp.MustCompile(`(?i)last_insert_id\s*\(`)
	// 匹配 CONNECTION_ID 函数
	reCONNECTION_ID = regexp.MustCompile(`(?i)connection_id\s*\(`)
	// 匹配 CURRENT_USER 函数
	reCURRENT_USER = regexp.MustCompile(`(?i)current_user\s*\(`)
	// 匹配 SESSION_USER 函数
	reSESSION_USER = regexp.MustCompile(`(?i)session_user\s*\(`)
	// 匹配 SYSTEM_USER 函数
	reSYSTEM_USER = regexp.MustCompile(`(?i)system_user\s*\(`)
	// 匹配 SCHEMA 函数
	reSCHEMA = regexp.MustCompile(`(?i)schema\s*\(`)
	// 匹配 UUID_SHORT 函数
	reUUID_SHORT = regexp.MustCompile(`(?i)uuid_short\s*\(`)
)

// ConvertViewDDL 将MySQL的VIEW_DEFINITION转换为PostgreSQL的CREATE VIEW语句
// - viewName: 视图名（不带schema）
// - viewDefinition: 从information_schema.VIEWS中读取的VIEW_DEFINITION字段内容
func ConvertViewDDL(viewName string, viewDefinition string) (string, error) {
	if strings.TrimSpace(viewName) == "" {
		return "", fmt.Errorf("empty view name")
	}
	if strings.TrimSpace(viewDefinition) == "" {
		return "", fmt.Errorf("empty view definition for view '%s'", viewName)
	}

	// 1) 首先将反引号替换为双引号（标识符引用），确保所有后续正则表达式处理正确
	processed := strings.ReplaceAll(viewDefinition, "`", `"`)
	if processed == "" {
		return "", fmt.Errorf("failed to process backticks in view definition for view '%s'", viewName)
	}

	// 2) 移除数据库名前缀（例如 "db"."table" -> 只保留 "table"）
	// 仅在出现 "db"."table" 或 "db"."table"."col" 的情况下移除 db 前缀
	processed = reDBPrefix.ReplaceAllString(processed, "$1")
	if processed == "" {
		return "", fmt.Errorf("failed to remove database prefix in view definition for view '%s'", viewName)
	}

	// 3) 将IFNULL/ifnull替换为COALESCE
	processed = reIfnull.ReplaceAllString(processed, "COALESCE(")
	if processed == "" {
		return "", fmt.Errorf("failed to replace IFNULL with COALESCE in view definition for view '%s'", viewName)
	}

	// 4) GROUP_CONCAT -> string_agg 的简单转换，保留 SEPARATOR 和 ORDER BY 的常见用法
	processed = reGroupConcat.ReplaceAllStringFunc(processed, func(s string) string {
		m := reGroupConcat.FindStringSubmatch(s)
		if len(m) < 2 {
			return s
		}
		inner := m[1]
		// 移除 ORDER BY 子句（简单处理）
		innerClean := reOrder.ReplaceAllString(inner, "")
		// 解析 SEPARATOR
		sepM := reSep.FindStringSubmatch(inner)
		sep := ","
		if len(sepM) >= 2 {
			sep = sepM[1]
			innerClean = reSep.ReplaceAllString(innerClean, "")
		}
		return fmt.Sprintf("string_agg(CAST(%s AS text), '%s')", strings.TrimSpace(innerClean), sep)
	})
	if processed == "" {
		return "", fmt.Errorf("failed to convert GROUP_CONCAT to string_agg in view definition for view '%s'", viewName)
	}

	// 5) 将IF(expr, then, else)转换为CASE WHEN ... THEN ... ELSE ... END（简单版，不处理嵌套逗号）
	processed = reIf.ReplaceAllString(processed, "CASE WHEN $1 THEN $2 ELSE $3 END")
	if processed == "" {
		return "", fmt.Errorf("failed to replace IF with CASE WHEN in view definition for view '%s'", viewName)
	}

	// 6) 将CONVERT(x, TYPE)转换为CAST(x AS TYPE)（简单替换）
	processed = reConvert.ReplaceAllString(processed, "CAST($1 AS $2)")
	if processed == "" {
		return "", fmt.Errorf("failed to replace CONVERT with CAST in view definition for view '%s'", viewName)
	}

	// 7) 将LIMIT a,b转换为LIMIT b OFFSET a
	processed = reLimitOffset.ReplaceAllString(processed, "LIMIT $2 OFFSET $1")
	if processed == "" {
		return "", fmt.Errorf("failed to adjust LIMIT syntax in view definition for view '%s'", viewName)
	}

	// 8) 处理表连接条件中的列名歧义，为连接条件中的列添加表别名
	// 对于视图中常见的连接模式：(table1 alias1 join table2 alias2 on(...))
	// 为on子句中的列添加表别名
	reJoinPattern := regexp.MustCompile(`(?i)\(([^\s]+)\s+([^\s]+)\s+(?:left|inner|right|full)?\s*join\s+([^\s]+)\s+([^\s]+)\s+on\s*\(+([^)]+)\s*\)+\)`)
	processed = reJoinPattern.ReplaceAllStringFunc(processed, func(joinExpr string) string {
		matches := reJoinPattern.FindStringSubmatch(joinExpr)
		if len(matches) < 6 {
			return joinExpr
		}

		// matches[1]: 第一个表名
		// matches[2]: 第一个表别名
		// matches[3]: 第二个表名
		// matches[4]: 第二个表别名
		// matches[5]: 连接条件

		alias1 := matches[2]
		alias2 := matches[4]
		condition := matches[5]

		// 处理条件中的列名，为没有表别名的列添加表别名
		reColumns := regexp.MustCompile(`(?i)(["\w]+)\s*=\s*(["\w]+)`)
		processedCondition := reColumns.ReplaceAllStringFunc(condition, func(colMatch string) string {
			parts := strings.SplitN(colMatch, "=", 2)
			if len(parts) != 2 {
				return colMatch
			}

			col1 := strings.TrimSpace(parts[0])
			col2 := strings.TrimSpace(parts[1])

			// 为没有表别名的列添加表别名
			if !strings.Contains(col1, ".") {
				col1 = fmt.Sprintf("%s.%s", alias1, col1)
			}
			if !strings.Contains(col2, ".") {
				col2 = fmt.Sprintf("%s.%s", alias2, col2)
			}

			// 添加类型转换以解决PostgreSQL中的类型不匹配问题
			return fmt.Sprintf("%s::text = %s::text", col1, col2)
		})

		// 重新构建连接表达式
		return fmt.Sprintf("(%s %s join %s %s on((%s)))",
			matches[1], alias1, matches[3], alias2, processedCondition)
	})

	// 9) 将简单的CONCAT(a,b,...)转换为 a || b || ... （保留原始行为，对于复杂表达式会尽量处理）
	processed = replaceConcatExpressions(processed)
	if processed == "" {
		return "", fmt.Errorf("failed to replace CONCAT with || in view definition for view '%s'", viewName)
	}

	// 9.1) 为SUM函数添加类型转换，解决sum(character varying)不存在的问题
	reSum := regexp.MustCompile(`(?i)sum\s*\(\s*(["\w\.]+)\s*\)`)
	processed = reSum.ReplaceAllStringFunc(processed, func(m string) string {
		match := reSum.FindStringSubmatch(m)
		if len(match) < 2 {
			return m
		}
		column := match[1]
		var sb strings.Builder
		sb.WriteString("sum(")
		sb.WriteString(column)
		sb.WriteString("::numeric)")
		return sb.String()
	})
	if processed == "" {
		return "", fmt.Errorf("failed to add type conversion for SUM function in view definition for view '%s'", viewName)
	}

	// 9.2) 处理COALESCE函数的参数类型不匹配问题
	reCoalesce := regexp.MustCompile(`(?i)coalesce\s*\(\s*(["\w\.]+)\s*,\s*(\d+)\s*\)`)
	processed = reCoalesce.ReplaceAllStringFunc(processed, func(m string) string {
		match := reCoalesce.FindStringSubmatch(m)
		if len(match) < 3 {
			return m
		}
		column := match[1]
		defaultVal := match[2]
		var sb strings.Builder
		sb.WriteString("coalesce(")
		sb.WriteString(column)
		sb.WriteString("::numeric, ")
		sb.WriteString(defaultVal)
		sb.WriteString("::numeric)")
		return sb.String()
	})
	if processed == "" {
		return "", fmt.Errorf("failed to fix COALESCE parameter types in view definition for view '%s'", viewName)
	}

	// 10) 修正常见MySQL函数差异/关键字（可扩展）
	// JSON函数转换
	processed = reJSONObject.ReplaceAllString(processed, "json_build_object(")
	processed = reJSONArray.ReplaceAllString(processed, "json_build_array(")
	processed = reJSONQuote.ReplaceAllString(processed, "jsonb_quote(")
	processed = reJSONUnquote.ReplaceAllString(processed, "jsonb_unquote(")
	// JSON_EXTRACT(json_column, '$.key') -> json_column -> 'key'
	processed = reJSONExtract.ReplaceAllString(processed, "$1 -> $2")
	processed = reJSONKeys.ReplaceAllString(processed, "json_object_keys(")
	processed = reJSONLength.ReplaceAllString(processed, "json_array_length(")
	processed = reJSONType.ReplaceAllString(processed, "jsonb_typeof(")
	processed = reJSONValid.ReplaceAllStringFunc(processed, func(m string) string {
		// 匹配JSON_VALID(expr) -> (expr IS NOT NULL AND jsonb_typeof(expr::jsonb) IS NOT NULL)
		return "(" + m[10:len(m)-1] + " IS NOT NULL AND jsonb_typeof(" + m[10:len(m)-1] + "::jsonb) IS NOT NULL)"
	})
	// JSON_VALUE(json_column, '$.key') -> json_column ->> 'key'
	processed = reJSONValue.ReplaceAllString(processed, "$1 ->> $2")
	processed = reJSONInsert.ReplaceAllString(processed, "jsonb_insert(")
	processed = reJSONSet.ReplaceAllString(processed, "jsonb_set(")
	processed = reJSONReplace.ReplaceAllString(processed, "jsonb_set(")
	processed = reJSONRemove.ReplaceAllString(processed, "jsonb_delete(")
	// JSON_ARRAY_APPEND(arr, path, value) -> arr || json_build_array(value)
	processed = reJSONArrayAppend.ReplaceAllStringFunc(processed, func(m string) string {
		// 匹配JSON_ARRAY_APPEND(arr, path, value)，简单处理为数组拼接
		// 注意：这是简化版本，不处理复杂路径
		parts := strings.SplitN(m[17:len(m)-1], ",", 3)
		if len(parts) < 3 {
			return m // 格式不正确，返回原始字符串
		}
		arr := strings.TrimSpace(parts[0])
		value := strings.TrimSpace(parts[2])
		return fmt.Sprintf("%s || json_build_array(%s)", arr, value)
	})
	// JSON_ARRAY_INSERT(arr, path, value) -> jsonb_insert
	processed = reJSONArrayInsert.ReplaceAllString(processed, "jsonb_insert(")
	// JSON_MERGE -> jsonb_concat
	processed = reJSONMerge.ReplaceAllString(processed, "jsonb_concat(")
	// JSON_MERGE_PATCH -> jsonb_merge_patch
	processed = reJSONMergePatch.ReplaceAllString(processed, "jsonb_merge_patch(")
	// JSON_MERGE_PRESERVE -> jsonb_concat
	processed = reJSONMergePreserve.ReplaceAllString(processed, "jsonb_concat(")
	if processed == "" {
		return "", fmt.Errorf("failed to convert JSON functions in view definition for view '%s'", viewName)
	}

	// 加密函数转换
	processed = reMD5.ReplaceAllString(processed, "md5(")
	processed = reSHA1.ReplaceAllString(processed, "sha1(")
	processed = reSHA2.ReplaceAllString(processed, "sha2(")
	if processed == "" {
		return "", fmt.Errorf("failed to convert encryption functions in view definition for view '%s'", viewName)
	}

	// UUID函数转换
	processed = reUUID.ReplaceAllString(processed, "uuid_generate_v4(")
	processed = reUUID_SHORT.ReplaceAllString(processed, "(extract(epoch from now()) * 1000000)::bigint")
	if processed == "" {
		return "", fmt.Errorf("failed to convert UUID functions in view definition for view '%s'", viewName)
	}

	// 网络函数转换
	processed = reINET_ATON.ReplaceAllStringFunc(processed, func(m string) string {
		// INET_ATON('192.168.1.1') -> (CAST('192.168.1.1' AS inet) - CAST('0.0.0.0' AS inet))::bigint
		// 安全提取参数
		params := strings.TrimPrefix(m, "INET_ATON(")
		params = strings.TrimSuffix(params, ")")
		var sb strings.Builder
		sb.WriteString("(CAST(")
		sb.WriteString(params)
		sb.WriteString(" AS inet) - CAST('0.0.0.0' AS inet))::bigint")
		return sb.String()
	})
	processed = reINET_NTOA.ReplaceAllStringFunc(processed, func(m string) string {
		// INET_NTOA(3232235777) -> CAST((CAST('0.0.0.0' AS inet) + $1::bigint) AS text)
		// 安全提取参数
		params := strings.TrimPrefix(m, "INET_NTOA(")
		params = strings.TrimSuffix(params, ")")
		var sb strings.Builder
		sb.WriteString("CAST((CAST('0.0.0.0' AS inet) + ")
		sb.WriteString(params)
		sb.WriteString("::bigint) AS text)")
		return sb.String()
	})
	if processed == "" {
		return "", fmt.Errorf("failed to convert network functions in view definition for view '%s'", viewName)
	}

	// 时间函数转换
	processed = reUNIX_TIMESTAMP.ReplaceAllStringFunc(processed, func(m string) string {
		if len(m) == 15 { // UNIX_TIMESTAMP() 不带参数
			return "extract(epoch from now())"
		}
		// UNIX_TIMESTAMP(expr) -> extract(epoch from $1)
		return "extract(epoch from " + m[16:len(m)-1] + ")"
	})
	processed = reFROM_UNIXTIME.ReplaceAllString(processed, "to_timestamp(")
	processed = reDATE_FORMAT.ReplaceAllString(processed, "to_char($1, $2)")
	processed = reSTR_TO_DATE.ReplaceAllString(processed, "to_date($1, $2)")
	processed = reDATEDIFF.ReplaceAllString(processed, "date_part('day', $1 - $2)")
	processed = reTIMEDIFF.ReplaceAllString(processed, "($1 - $2)")
	if processed == "" {
		return "", fmt.Errorf("failed to convert basic time functions in view definition for view '%s'", viewName)
	}

	// 时间函数转换 - DATE_ADD/DATE_SUB
	processed = reDATE_ADD.ReplaceAllStringFunc(processed, func(m string) string {
		match := reDATE_ADD.FindStringSubmatch(m)
		if len(match) < 3 {
			return m
		}
		// 匹配 DATE_ADD(date, INTERVAL expr unit) -> date + expr * interval '1 unit'
		datePart := strings.TrimSpace(match[1])
		intervalPart := strings.TrimSpace(match[2])
		// 简单处理，假设格式为 '1 day' 或 '2 hours'
		parts := strings.SplitN(intervalPart, " ", 2)
		var sb strings.Builder
		if len(parts) < 2 {
			sb.WriteString(datePart)
			sb.WriteString(" + ")
			sb.WriteString(intervalPart)
			sb.WriteString("::interval")
			return sb.String()
		}
		num := strings.TrimSpace(parts[0])
		unit := strings.TrimSpace(parts[1])
		sb.WriteString(datePart)
		sb.WriteString(" + ")
		sb.WriteString(num)
		sb.WriteString("::interval '1 ")
		sb.WriteString(unit)
		sb.WriteString("'")
		return sb.String()
	})
	processed = reDATE_SUB.ReplaceAllStringFunc(processed, func(m string) string {
		match := reDATE_SUB.FindStringSubmatch(m)
		if len(match) < 3 {
			return m
		}
		// 匹配 DATE_SUB(date, INTERVAL expr unit) -> date - expr * interval '1 unit'
		datePart := strings.TrimSpace(match[1])
		intervalPart := strings.TrimSpace(match[2])
		// 简单处理，假设格式为 '1 day' 或 '2 hours'
		parts := strings.SplitN(intervalPart, " ", 2)
		var sb strings.Builder
		if len(parts) < 2 {
			sb.WriteString(datePart)
			sb.WriteString(" - ")
			sb.WriteString(intervalPart)
			sb.WriteString("::interval")
			return sb.String()
		}
		num := strings.TrimSpace(parts[0])
		unit := strings.TrimSpace(parts[1])
		sb.WriteString(datePart)
		sb.WriteString(" - ")
		sb.WriteString(num)
		sb.WriteString("::interval '1 ")
		sb.WriteString(unit)
		sb.WriteString("'")
		return sb.String()
	})
	if processed == "" {
		return "", fmt.Errorf("failed to process DATE_ADD/DATE_SUB functions in view definition for view '%s'", viewName)
	}

	// ADDDATE/SUBDATE -> + / -
	processed = reADDDATE.ReplaceAllStringFunc(processed, func(m string) string {
		// 匹配 ADDDATE(date, days) -> date + days * interval '1 day'
		parts := strings.SplitN(m[8:len(m)-1], ",", 2)
		if len(parts) < 2 {
			return m
		}
		date := strings.TrimSpace(parts[0])
		days := strings.TrimSpace(parts[1])
		var sb strings.Builder
		sb.WriteString(date)
		sb.WriteString(" + ")
		sb.WriteString(days)
		sb.WriteString("::interval '1 day'")
		return sb.String()
	})
	processed = reSUBDATE.ReplaceAllStringFunc(processed, func(m string) string {
		// 匹配 SUBDATE(date, days) -> date - days * interval '1 day'
		parts := strings.SplitN(m[8:len(m)-1], ",", 2)
		if len(parts) < 2 {
			return m
		}
		date := strings.TrimSpace(parts[0])
		days := strings.TrimSpace(parts[1])
		var sb strings.Builder
		sb.WriteString(date)
		sb.WriteString(" - ")
		sb.WriteString(days)
		sb.WriteString("::interval '1 day'")
		return sb.String()
	})
	if processed == "" {
		return "", fmt.Errorf("failed to process ADDDATE/SUBDATE functions in view definition for view '%s'", viewName)
	}

	// ADDTIME/SUBTIME -> + / -
	// 使用更精确的方式处理ADDTIME和SUBTIME函数，避免影响其他表达式
	processed = reADDTIME.ReplaceAllStringFunc(processed, func(m string) string {
		// 匹配 ADDTIME(expr1, expr2) -> expr1 + expr2
		parts := strings.SplitN(m[8:len(m)-1], ",", 2)
		if len(parts) < 2 {
			return m
		}
		expr1 := strings.TrimSpace(parts[0])
		expr2 := strings.TrimSpace(parts[1])
		var sb strings.Builder
		sb.WriteString("(")
		sb.WriteString(expr1)
		sb.WriteString(" + ")
		sb.WriteString(expr2)
		sb.WriteString(")")
		return sb.String()
	})
	processed = reSUBTIME.ReplaceAllStringFunc(processed, func(m string) string {
		// 匹配 SUBTIME(expr1, expr2) -> expr1 - expr2
		parts := strings.SplitN(m[8:len(m)-1], ",", 2)
		if len(parts) < 2 {
			return m
		}
		expr1 := strings.TrimSpace(parts[0])
		expr2 := strings.TrimSpace(parts[1])
		var sb strings.Builder
		sb.WriteString("(")
		sb.WriteString(expr1)
		sb.WriteString(" - ")
		sb.WriteString(expr2)
		sb.WriteString(")")
		return sb.String()
	})
	if processed == "" {
		return "", fmt.Errorf("failed to process ADDTIME/SUBTIME functions in view definition for view '%s'", viewName)
	}

	// 系统函数转换
	processed = reLAST_INSERT_ID.ReplaceAllString(processed, "lastval()")
	processed = reCONNECTION_ID.ReplaceAllString(processed, "pg_backend_pid()")
	processed = reCURRENT_USER.ReplaceAllString(processed, "current_user")
	processed = reSESSION_USER.ReplaceAllString(processed, "session_user")
	processed = reSYSTEM_USER.ReplaceAllString(processed, "system_user")
	processed = reSCHEMA.ReplaceAllString(processed, "current_schema")
	processed = reDATABASE.ReplaceAllString(processed, "current_database()")
	processed = reUSER.ReplaceAllString(processed, "current_user")
	processed = reVERSION.ReplaceAllString(processed, "version()")
	if processed == "" {
		return "", fmt.Errorf("failed to convert system functions in view definition for view '%s'", viewName)
	}

	processed = strings.TrimSpace(processed)
	if processed == "" {
		return "", fmt.Errorf("processed view definition is empty after trimming for view '%s'", viewName)
	}

	// 如果定义末尾有分号，去掉它（我们将在CREATE VIEW语句后追加分号）
	if strings.HasSuffix(processed, ";") {
		processed = strings.TrimSuffix(processed, ";")
		processed = strings.TrimSpace(processed)
		if processed == "" {
			return "", fmt.Errorf("view definition became empty after removing trailing semicolon for view '%s'", viewName)
		}
	}

	// 11) 包装成CREATE OR REPLACE VIEW语句
	quotedViewName := quoteIdentifier(viewName)
	if quotedViewName == "" {
		return "", fmt.Errorf("failed to quote view name '%s'", viewName)
	}
	createStmt := fmt.Sprintf("CREATE OR REPLACE VIEW %s AS %s;", quotedViewName, processed)
	if createStmt == "" {
		return "", fmt.Errorf("failed to generate CREATE VIEW statement for view '%s'", viewName)
	}

	// 12) 将整个语句转换为小写，确保符合要求
	createStmt = strings.ToLower(createStmt)
	if createStmt == "" {
		return "", fmt.Errorf("failed to convert CREATE VIEW statement to lowercase for view '%s'", viewName)
	}

	return createStmt, nil
}

// quoteIdentifier 始终用双引号引用标识符，且对内部双引号做转义
func quoteIdentifier(s string) string {
	if s == "" {
		return s
	}
	// 如果已经被双引号包围，直接返回
	if strings.HasPrefix(s, `"`) && strings.HasSuffix(s, `"`) {
		return s
	}
	// 双倍内部双引号
	s = strings.ReplaceAll(s, `"`, `""`)
	return fmt.Sprintf(`"%s"`, s)
}

// splitTopLevelCommas 将字符串按顶层逗号分割（忽略括号内的逗号）
func splitTopLevelCommas(s string) []string {
	var parts []string
	var buf strings.Builder
	depth := 0
	inSingle := false
	inDouble := false
	for i := 0; i < len(s); i++ {
		r := s[i]
		switch r {
		case '\'':
			if !inDouble {
				inSingle = !inSingle
			}
		case '"':
			if !inSingle {
				inDouble = !inDouble
			}
		case '(':
			if !inSingle && !inDouble {
				depth++
			}
		case ')':
			if !inSingle && !inDouble {
				if depth > 0 {
					depth--
				}
			}
		case ',':
			if depth == 0 && !inSingle && !inDouble {
				parts = append(parts, strings.TrimSpace(buf.String()))
				buf.Reset()
				continue
			}
		}
		buf.WriteByte(r)
	}
	if buf.Len() > 0 {
		parts = append(parts, strings.TrimSpace(buf.String()))
	}
	return parts
}

// replaceConcatExpressions 将 concat(a,b,c) 转成 a || b || c（尽量处理嵌套）
func replaceConcatExpressions(s string) string {
	lower := strings.ToLower(s)
	out := s
	idx := 0
	for {
		pos := strings.Index(lower[idx:], "concat(")
		if pos == -1 {
			break
		}
		pos += idx
		// 找到括号开始
		start := pos + len("concat(")
		depth := 1
		end := start
		for i := start; i < len(s); i++ {
			if s[i] == '(' {
				depth++
			} else if s[i] == ')' {
				depth--
				if depth == 0 {
					end = i
					break
				}
			}
		}
		if end <= start {
			// 无法匹配，退出
			break
		}
		argsStr := s[start:end]
		args := splitTopLevelCommas(argsStr)
		var sb strings.Builder
		sb.WriteString("(")
		for i, a := range args {
			if i > 0 {
				sb.WriteString(" || ")
			}
			sb.WriteString(strings.TrimSpace(a))
		}
		sb.WriteString(")")
		out = out[:pos] + sb.String() + out[end+1:]
		// move index forward
		idx = pos + len(sb.String())
		lower = strings.ToLower(out)
	}
	return out
}
