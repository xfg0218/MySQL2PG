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
	// 匹配 JSON_EXTRACT 函数
	reJSONExtract = regexp.MustCompile(`(?i)json_extract\s*\(\s*([^,]+)\s*,\s*([^)]+)\)`)
	// 匹配 JSON_KEYS 函数 (Removed)
	// 匹配 JSON_LENGTH 函数 (Removed)
	// 匹配 JSON_TYPE 函数 (Removed)
	// 匹配 JSON_VALID 函数
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
	reADDTIME = regexp.MustCompile(`(?i)addtime\s*\(\s*([^,]+)\s*,\s*([^)]+)\)`)
	// 匹配 SUBTIME 函数
	reSUBTIME = regexp.MustCompile(`(?i)subtime\s*\(\s*([^,]+)\s*,\s*([^)]+)\)`)
	// 匹配 DATABASE 函数
	reDATABASE = regexp.MustCompile(`(?i)database\s*\([^)]*\)`)
	// 匹配 USER 函数
	reUSER = regexp.MustCompile(`(?i)user\s*\([^)]*\)`)
	// 匹配 VERSION 函数
	reVERSION = regexp.MustCompile(`(?i)version\s*\([^)]*\)`)
	// 匹配 MD5 函数
	reMD5 = regexp.MustCompile(`(?i)md5\s*\([^)]*\)`)
	// 匹配 SHA1 函数
	reSHA1 = regexp.MustCompile(`(?i)sha1\s*\([^)]*\)`)
	// 匹配 SHA2 函数
	reSHA2 = regexp.MustCompile(`(?i)sha2\s*\([^)]*\)`)
	// 匹配 UUID 函数
	reUUID = regexp.MustCompile(`(?i)uuid\s*\([^)]*\)`)
	// 匹配 INET_ATON 函数
	reINET_ATON = regexp.MustCompile(`(?i)inet_aton\s*\([^)]*\)`)
	// 匹配 INET_NTOA 函数
	reINET_NTOA = regexp.MustCompile(`(?i)inet_ntoa\s*\([^)]*\)`)
	// 匹配 UNIX_TIMESTAMP 函数
	reUNIX_TIMESTAMP = regexp.MustCompile(`(?i)unix_timestamp\s*\(\s*([^)]*)\s*\)`)
	// 匹配 FROM_UNIXTIME 函数
	reFROM_UNIXTIME = regexp.MustCompile(`(?i)from_unixtime\s*\(\s*([^)]*)\s*\)`)
	// 匹配 DATE_FORMAT 函数
	reDATE_FORMAT = regexp.MustCompile(`(?i)date_format\s*\(\s*([^,]+)\s*,\s*([^)]+)\)`)
	// 匹配 STR_TO_DATE 函数
	reSTR_TO_DATE = regexp.MustCompile(`(?i)str_to_date\s*\(\s*([^,]+)\s*,\s*([^)]+)\)`)
	// 匹配 DATEDIFF 函数
	reDATEDIFF = regexp.MustCompile(`(?i)datediff\s*\(\s*([^,]+)\s*,\s*([^)]+)\)`)
	// 匹配 TIMEDIFF 函数
	reTIMEDIFF = regexp.MustCompile(`(?i)timediff\s*\(\s*([^,]+)\s*,\s*([^)]+)\)`)
	// 匹配 MySQL INSERT 函数 (字符串插入)
	reINSERT = regexp.MustCompile(`(?i)insert\s*\(\s*([^,]+)\s*,\s*([^,]+)\s*,\s*([^,]+)\s*,\s*([^)]+)\)`)
	// 匹配 LAST_INSERT_ID 函数
	reLAST_INSERT_ID = regexp.MustCompile(`(?i)last_insert_id\s*\([^)]*\)`)
	// 匹配 CONNECTION_ID 函数
	reCONNECTION_ID = regexp.MustCompile(`(?i)connection_id\s*\([^)]*\)`)
	// 匹配 CURRENT_USER 函数
	reCURRENT_USER = regexp.MustCompile(`(?i)current_user\s*\([^)]*\)`)
	// 匹配 SESSION_USER 函数
	reSESSION_USER = regexp.MustCompile(`(?i)session_user\s*\([^)]*\)`)
	// 匹配 SYSTEM_USER 函数
	reSYSTEM_USER = regexp.MustCompile(`(?i)system_user\s*\([^)]*\)`)
	// 匹配 SCHEMA 函数
	reSCHEMA = regexp.MustCompile(`(?i)schema\s*\([^)]*\)`)
	// 匹配 UUID_SHORT 函数
	reUUID_SHORT = regexp.MustCompile(`(?i)uuid_short\s*\([^)]*\)`)
	// 匹配 RAND 函数 (包括带参数的情况)
	reRAND = regexp.MustCompile(`(?i)rand\s*\([^)]*\)`)
	// 匹配表连接模式
	reJoinPattern = regexp.MustCompile(`(?i)\(([^\s]+)\s+([^\s]+)\s+(?:left|inner|right|full)?\s*join\s+([^\s]+)\s+([^\s]+)\s+on\s*\(+([^)]+)\s*\)+\)`)
	// 匹配连接条件中的列名 - 更新以支持点号周围的空格
	// reColumns = regexp.MustCompile(`(?i)(["\w\.]+)\s*=\s*("["\w\.]+)`)
	// 新正则支持 "alias" . "column" 格式
	reColumns = regexp.MustCompile(`(?i)(["\w]+(?:\s*\.\s*["\w]+)?)\s*=\s*(["\w]+(?:\s*\.\s*["\w]+)?)`)
	// 匹配SUM函数的正则
	reSum = regexp.MustCompile(`(?i)sum\s*\(\s*(["\w\.]+)\s*\)`)
	// 匹配 interval 语法 (如 now() + interval 1 day)
	reInterval = regexp.MustCompile(`(?i)(\S[^+\-]*\S)\s*([+\-])\s*interval\s+([+\-]?\d+)\s+([\w_]+)`)
	// 匹配 CAST AS SIGNED
	reCastSigned = regexp.MustCompile(`(?i)\bcast\s*\(\s*(.*?)\s+as\s+signed\s*\)`)
	// 匹配 CAST AS UNSIGNED
	reCastUnsigned = regexp.MustCompile(`(?i)\bcast\s*\(\s*(.*?)\s+as\s+unsigned\s*\)`)
	// 匹配取模运算 %
	reModulo = regexp.MustCompile(`(?i)(["\w\.]+)\s*%\s*(["\w\.]+)`)
	// 匹配 JOIN 开始部分
	reJoinStart = regexp.MustCompile(`(?i)\(\s*("[^"]+"|\w+)\s+("[^"]+"|\w+)\s+(?:left|inner|right|full)?\s*join\s+("[^"]+"|\w+)\s+("[^"]+"|\w+)\s+on`)
	// 匹配 CAST(... USING ...)
	reCastUsing = regexp.MustCompile(`(?i)\bcast\s*\(\s*(.*?)\s+using\s+[\w]+\s*\)`)
	// 匹配 CONVERT(... USING ...)
	reConvertUsing = regexp.MustCompile(`(?i)\bconvert\s*\(\s*(.*?)\s+using\s+[\w]+\s*\)`)
)

// ConvertViewDDL 将MySQL的VIEW_DEFINITION转换为PostgreSQL的CREATE VIEW语句,从information_schema.VIEWS中读取的VIEW_DEFINITION字段内容
func ConvertViewDDL(viewName string, viewDefinition string, dbName string) (string, error) {
	if strings.TrimSpace(viewName) == "" {
		return "", fmt.Errorf("empty view name")
	}
	if strings.TrimSpace(viewDefinition) == "" {
		return "", fmt.Errorf("empty view definition for view '%s'", viewName)
	}

	//  首先将反引号替换为双引号（标识符引用），确保所有后续正则表达式处理正确
	processed := strings.ReplaceAll(viewDefinition, "`", `"`)
	if processed == "" {
		return "", fmt.Errorf("failed to process backticks in view definition for view '%s'", viewName)
	}

	processed, literals := maskStringLiterals(processed)

	// 移除数据库名前缀（例如 "db"."table" -> 只保留 "table"）
	// 只移除匹配当前数据库名的前缀，避免误伤其他标识符
	if dbName != "" {
		// 1. 处理带引号的数据库名: "dbName".
		quotedDB := fmt.Sprintf(`"%s".`, dbName)
		// 使用不区分大小写的替换
		processed = replaceCaseInsensitive(processed, quotedDB, "")
	}

	if processed == "" {
		return "", fmt.Errorf("failed to remove database prefix in view definition for view '%s'", viewName)
	}

	// 将IFNULL/ifnull替换为COALESCE
	processed = reIfnull.ReplaceAllString(processed, "COALESCE(")
	if processed == "" {
		return "", fmt.Errorf("failed to replace IFNULL with COALESCE in view definition for view '%s'", viewName)
	}

	// GROUP_CONCAT -> string_agg 的简单转换，保留 SEPARATOR 和 ORDER BY 的常见用法
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

	//  将IF(expr, then, else)转换为CASE WHEN ... THEN ... ELSE ... END（简单版，不处理嵌套逗号）
	processed = reIf.ReplaceAllString(processed, "CASE WHEN $1 THEN $2 ELSE $3 END")
	if processed == "" {
		return "", fmt.Errorf("failed to replace IF with CASE WHEN in view definition for view '%s'", viewName)
	}

	processed = processUsingClause(processed)

	// Remove COLLATE clause
	processed = reCollateSuffix.ReplaceAllString(processed, "")

	// 处理 CAST AS SIGNED/UNSIGNED
	processed = reCastSigned.ReplaceAllString(processed, "CAST($1 AS INTEGER)")
	processed = reCastUnsigned.ReplaceAllString(processed, "CAST($1 AS BIGINT)")

	// Handle Modulo operator % -> MOD(a::numeric, b::numeric)
	processed = reModulo.ReplaceAllString(processed, "MOD(CAST($1 AS numeric), CAST($2 AS numeric))")

	// 将LIMIT a,b转换为LIMIT b OFFSET a
	processed = reLimitOffset.ReplaceAllString(processed, "LIMIT $2 OFFSET $1")
	if processed == "" {
		return "", fmt.Errorf("failed to adjust LIMIT syntax in view definition for view '%s'", viewName)
	}

	processed = processFunctionCall(processed, "length", func(args []string) string {
		if len(args) == 1 {
			return fmt.Sprintf("length(CAST(%s AS TEXT))", args[0])
		}
		return fmt.Sprintf("length(%s)", strings.Join(args, ","))
	})

	processed = processFunctionCall(processed, "substr", func(args []string) string {
		if len(args) >= 1 {
			args[0] = fmt.Sprintf("CAST(%s AS TEXT)", args[0])
		}
		return fmt.Sprintf("substr(%s)", strings.Join(args, ","))
	})
	processed = processFunctionCall(processed, "substring", func(args []string) string {
		if len(args) >= 1 {
			args[0] = fmt.Sprintf("CAST(%s AS TEXT)", args[0])
		}
		return fmt.Sprintf("substring(%s)", strings.Join(args, ","))
	})

	// Locate (MySQL: locate(substr, str) -> PG: strpos(str, substr))
	processed = processFunctionCall(processed, "locate", func(args []string) string {
		if len(args) == 2 {
			return fmt.Sprintf("strpos(CAST(%s AS TEXT), %s)", args[1], args[0])
		}
		return fmt.Sprintf("locate(%s)", strings.Join(args, ","))
	})

	processed = processFunctionCall(processed, "replace", func(args []string) string {
		if len(args) >= 1 {
			args[0] = fmt.Sprintf("CAST(%s AS TEXT)", args[0])
		}
		return fmt.Sprintf("replace(%s)", strings.Join(args, ","))
	})

	processed = processFunctionCall(processed, "trim", func(args []string) string {
		if len(args) == 1 {
			return fmt.Sprintf("trim(CAST(%s AS TEXT))", args[0])
		}
		return fmt.Sprintf("trim(%s)", strings.Join(args, ","))
	})

	processed = processFunctionCall(processed, "upper", func(args []string) string {
		if len(args) == 1 {
			return fmt.Sprintf("upper(CAST(%s AS TEXT))", args[0])
		}
		return fmt.Sprintf("upper(%s)", strings.Join(args, ","))
	})

	processed = processFunctionCall(processed, "lower", func(args []string) string {
		if len(args) == 1 {
			return fmt.Sprintf("lower(CAST(%s AS TEXT))", args[0])
		}
		return fmt.Sprintf("lower(%s)", strings.Join(args, ","))
	})

	processed = processFunctionCall(processed, "reverse", func(args []string) string {
		if len(args) == 1 {
			return fmt.Sprintf("reverse(CAST(%s AS TEXT))", args[0])
		}
		return fmt.Sprintf("reverse(%s)", strings.Join(args, ","))
	})

	processed = processFunctionCall(processed, "left", func(args []string) string {
		if len(args) >= 1 {
			args[0] = fmt.Sprintf("CAST(%s AS TEXT)", args[0])
		}
		return fmt.Sprintf("left(%s)", strings.Join(args, ","))
	})

	processed = processFunctionCall(processed, "right", func(args []string) string {
		if len(args) >= 1 {
			args[0] = fmt.Sprintf("CAST(%s AS TEXT)", args[0])
		}
		return fmt.Sprintf("right(%s)", strings.Join(args, ","))
	})

	processed = processFunctionCall(processed, "round", func(args []string) string {
		if len(args) == 2 {
			return fmt.Sprintf("round(CAST(%s AS numeric), %s)", args[0], args[1])
		}
		return fmt.Sprintf("round(%s)", strings.Join(args, ","))
	})

	processed = processFunctionCall(processed, "substring_index", func(args []string) string {
		if len(args) == 3 {
			// substring_index(str, delim, count) -> split_part(str, delim, count)
			return fmt.Sprintf("split_part(%s, %s, %s)", args[0], args[1], args[2])
		}
		return fmt.Sprintf("substring_index(%s)", strings.Join(args, ","))
	})

	processed = processFunctionCall(processed, "space", func(args []string) string {
		if len(args) == 1 {
			return fmt.Sprintf("repeat(' ', %s::integer)", args[0])
		}
		return fmt.Sprintf("space(%s)", strings.Join(args, ","))
	})

	processed = processFunctionCall(processed, "hex", func(args []string) string {
		if len(args) == 1 {
			// Heuristic: if arg involves ascii, use to_hex (for int), else encode (for string)
			if strings.Contains(strings.ToLower(args[0]), "ascii") {
				return fmt.Sprintf("to_hex(%s)", args[0])
			}
			return fmt.Sprintf("encode(CAST(%s AS bytea), 'hex')", args[0])
		}
		return fmt.Sprintf("hex(%s)", strings.Join(args, ","))
	})

	processed = processFunctionCall(processed, "conv", func(args []string) string {
		if len(args) == 3 {
			// conv(N, 10, 2) -> trim leading 0s from bit string
			if strings.TrimSpace(args[1]) == "10" && strings.TrimSpace(args[2]) == "2" {
				return fmt.Sprintf("trim(leading '0' from CAST(%s AS BIT(64))::text)", args[0])
			}
		}
		return fmt.Sprintf("conv(%s)", strings.Join(args, ","))
	})

	processed = processFunctionCall(processed, "strcmp", func(args []string) string {
		if len(args) == 2 {
			return fmt.Sprintf("(CASE WHEN %s = %s THEN 0 WHEN %s < %s THEN -1 ELSE 1 END)", args[0], args[1], args[0], args[1])
		}
		return fmt.Sprintf("strcmp(%s)", strings.Join(args, ","))
	})

	// 处理表连接条件中的列名歧义
	processed = replaceJoinAliases(processed)

	// 9) 将简单的CONCAT(a,b,...)转换为 a || b || ... （保留原始行为，对于复杂表达式会尽量处理）
	processed = replaceConcatExpressions(processed)
	if processed == "" {
		return "", fmt.Errorf("failed to replace CONCAT with || in view definition for view '%s'", viewName)
	}

	// 9.1) 为SUM函数添加类型转换，解决sum(character varying)不存在的问题
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
	// 使用 processFunctionCall 处理任意数量参数，并统一转换为 text 以避免类型不匹配
	processed = processFunctionCall(processed, "coalesce", func(args []string) string {
		castedArgs := make([]string, len(args))
		for i, arg := range args {
			castedArgs[i] = fmt.Sprintf("CAST(%s AS text)", arg)
		}
		return fmt.Sprintf("coalesce(%s)", strings.Join(castedArgs, ","))
	})
	if processed == "" {
		return "", fmt.Errorf("failed to fix COALESCE parameter types in view definition for view '%s'", viewName)
	}

	// 修正常见MySQL函数差异/关键字，JSON函数转换
	processed = reJSONObject.ReplaceAllString(processed, "json_build_object(")
	processed = reJSONArray.ReplaceAllString(processed, "json_build_array(")
	processed = processFunctionCall(processed, "json_quote", func(args []string) string {
		if len(args) == 1 {
			return fmt.Sprintf("to_jsonb((%s)::text)", args[0])
		}
		return fmt.Sprintf("json_quote(%s)", strings.Join(args, ","))
	})
	processed = processFunctionCall(processed, "json_unquote", func(args []string) string {
		if len(args) == 1 {
			return fmt.Sprintf("((%s)::jsonb #>> '{}')", args[0])
		}
		return fmt.Sprintf("json_unquote(%s)", strings.Join(args, ","))
	})
	// JSON_EXTRACT(json_column, '$.key') -> json_column -> 'key'
	processed = reJSONExtract.ReplaceAllString(processed, "$1 -> $2")
	processed = processFunctionCall(processed, "json_keys", func(args []string) string {
		if len(args) == 1 {
			return fmt.Sprintf("(SELECT jsonb_agg(keys) FROM jsonb_object_keys((%s)::jsonb) keys)", args[0])
		}
		return fmt.Sprintf("json_keys(%s)", strings.Join(args, ","))
	})
	processed = processFunctionCall(processed, "json_pretty", func(args []string) string {
		if len(args) == 1 {
			return fmt.Sprintf("jsonb_pretty((%s)::jsonb)", args[0])
		}
		return fmt.Sprintf("json_pretty(%s)", strings.Join(args, ","))
	})
	processed = processFunctionCall(processed, "json_length", func(args []string) string {
		if len(args) == 1 {
			// MySQL JSON_LENGTH behavior: array->len, object->keys count, scalar->1
			return fmt.Sprintf("(CASE WHEN jsonb_typeof((%s)::jsonb) = 'array' THEN jsonb_array_length((%s)::jsonb) WHEN jsonb_typeof((%s)::jsonb) = 'object' THEN (SELECT count(*) FROM jsonb_object_keys((%s)::jsonb)) ELSE 1 END)", args[0], args[0], args[0], args[0])
		}
		return fmt.Sprintf("json_length(%s)", strings.Join(args, ","))
	})
	processed = processFunctionCall(processed, "json_contains_path", func(args []string) string {
		if len(args) >= 3 {
			// json_contains_path(json, 'one', path) -> jsonb_path_exists(json, path)
			// Ignoring 'one'/'all' distinction for multiple paths for now, assuming simple case
			return fmt.Sprintf("jsonb_path_exists((%s)::jsonb, (%s)::jsonpath)", args[0], args[2])
		}
		return fmt.Sprintf("json_contains_path(%s)", strings.Join(args, ","))
	})
	processed = processFunctionCall(processed, "json_depth", func(args []string) string {
		return "NULL::integer" // json_depth not supported directly in PostgreSQL
	})
	processed = processFunctionCall(processed, "json_overlaps", func(args []string) string {
		return "NULL::boolean" // json_overlaps not supported directly in PostgreSQL
	})
	processed = processFunctionCall(processed, "json_type", func(args []string) string {
		if len(args) == 1 {
			return fmt.Sprintf("jsonb_typeof((%s)::jsonb)", args[0])
		}
		return fmt.Sprintf("json_type(%s)", strings.Join(args, ","))
	})
	processed = processFunctionCall(processed, "json_valid", func(args []string) string {
		if len(args) == 1 {
			return fmt.Sprintf("(%s IS NOT NULL AND jsonb_typeof((%s)::jsonb) IS NOT NULL)", args[0], args[0])
		}
		return fmt.Sprintf("json_valid(%s)", strings.Join(args, ","))
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

	// MySQL INSERT(str, pos, len, newstr) -> PostgreSQL OVERLAY(str PLACING newstr FROM pos FOR len)
	processed = reINSERT.ReplaceAllStringFunc(processed, func(m string) string {
		// 去掉函数名和括号，只保留参数部分，找到第一个'('和最后一个')'的位置
		openParen := strings.Index(m, "(")
		closeParen := strings.LastIndex(m, ")")
		if openParen == -1 || closeParen == -1 || openParen >= closeParen {
			return m // 格式不正确，返回原始字符串
		}

		// 提取参数部分
		paramStr := m[openParen+1 : closeParen]

		// 解析参数，处理嵌套括号（使用已有的splitTopLevelCommas函数）
		params := splitTopLevelCommas(paramStr)
		if len(params) != 4 {
			return m // 参数数量不正确，返回原始字符串
		}

		// 提取并修剪每个参数
		str := strings.TrimSpace(params[0])
		pos := strings.TrimSpace(params[1])
		len := strings.TrimSpace(params[2])
		newstr := strings.TrimSpace(params[3])

		// 构建OVERLAY函数调用（PLACING关键字必须大写）
		return fmt.Sprintf("OVERLAY(%s PLACING %s FROM %s FOR %s)", str, newstr, pos, len)
	})

	if processed == "" {
		return "", fmt.Errorf("failed to convert JSON functions in view definition for view '%s'", viewName)
	}

	// 加密函数转换
	processed = reMD5.ReplaceAllStringFunc(processed, func(m string) string {
		// 提取参数部分
		params := m[4 : len(m)-1] // 去掉 "md5(" 和 ")"
		return fmt.Sprintf("md5(%s)", params)
	})
	processed = reSHA1.ReplaceAllStringFunc(processed, func(m string) string {
		// 提取参数部分
		params := m[5 : len(m)-1] // 去掉 "sha1(" 和 ")"
		return fmt.Sprintf("sha1(%s)", params)
	})
	processed = reSHA2.ReplaceAllStringFunc(processed, func(m string) string {
		// 提取参数部分
		params := m[5 : len(m)-1] // 去掉 "sha2(" 和 ")"
		return fmt.Sprintf("sha2(%s)", params)
	})
	if processed == "" {
		return "", fmt.Errorf("failed to convert encryption functions in view definition for view '%s'", viewName)
	}

	// UUID函数转换
	processed = reUUID.ReplaceAllStringFunc(processed, func(m string) string {
		return "uuid_generate_v4()"
	})
	processed = reUUID_SHORT.ReplaceAllStringFunc(processed, func(m string) string {
		return "(extract(epoch from now()) * 1000000)::bigint"
	})
	if processed == "" {
		return "", fmt.Errorf("failed to convert UUID functions in view definition for view '%s'", viewName)
	}

	// 网络函数转换
	processed = reINET_ATON.ReplaceAllStringFunc(processed, func(m string) string {
		// 安全提取参数，找到左括号的位置
		parenIndex := strings.Index(m, "(")
		if parenIndex == -1 {
			return m // 无效格式，返回原始值
		}
		params := m[parenIndex+1 : len(m)-1] // 提取括号内的参数
		var sb strings.Builder
		sb.WriteString("(CAST(")
		sb.WriteString(params)
		sb.WriteString(" AS inet) - CAST('0.0.0.0' AS inet))::bigint")
		return sb.String()
	})
	processed = reINET_NTOA.ReplaceAllStringFunc(processed, func(m string) string {
		// 安全提取参数，找到左括号的位置
		parenIndex := strings.Index(m, "(")
		if parenIndex == -1 {
			return m // 无效格式，返回原始值
		}
		params := m[parenIndex+1 : len(m)-1] // 提取括号内的参数
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
		// 提取参数部分
		args := m[15 : len(m)-1] // 去掉 "UNIX_TIMESTAMP(" 和 ")"
		args = strings.TrimSpace(args)
		if args == "" { // UNIX_TIMESTAMP() 不带参数
			return "extract(epoch from now())"
		}
		// UNIX_TIMESTAMP(expr) -> extract(epoch from expr)
		return "extract(epoch from " + args + ")"
	})
	// FROM_UNIXTIME(expr) -> to_timestamp(expr)
	processed = reFROM_UNIXTIME.ReplaceAllStringFunc(processed, func(m string) string {
		// 提取参数部分
		args := m[14 : len(m)-1] // 去掉 "FROM_UNIXTIME(" 和 ")"
		args = strings.TrimSpace(args)
		if args == "" { // FROM_UNIXTIME() 不带参数
			return "to_timestamp(extract(epoch from now()))"
		}
		// FROM_UNIXTIME(expr) -> to_timestamp(expr)
		return "to_timestamp(" + args + ")"
	})
	processed = reDATE_FORMAT.ReplaceAllString(processed, "to_char($1, $2)")
	processed = reSTR_TO_DATE.ReplaceAllString(processed, "to_date($1, $2)")
	processed = reDATEDIFF.ReplaceAllString(processed, "date_part('day', $1 - $2)")
	processed = reTIMEDIFF.ReplaceAllString(processed, "($1 - $2)")

	processed = processFunctionCall(processed, "timestampdiff", func(args []string) string {
		if len(args) != 3 {
			return fmt.Sprintf("timestampdiff(%s)", strings.Join(args, ","))
		}
		unit := strings.ToLower(strings.TrimSpace(args[0]))
		start := args[1]
		end := args[2]

		switch unit {
		case "second":
			return fmt.Sprintf("trunc(extract(epoch from (%s - %s)))::bigint", end, start)
		case "minute":
			return fmt.Sprintf("trunc(extract(epoch from (%s - %s)) / 60)::bigint", end, start)
		case "hour":
			return fmt.Sprintf("trunc(extract(epoch from (%s - %s)) / 3600)::bigint", end, start)
		case "day":
			// extract(day from interval) only returns the 'days' part, not total days if > 30 days?
			// No, interval subtraction result 'just works' for days if we extract epoch / 86400?
			// Actually date_part('day', end - start) for timestamps returns number of days.
			// Let's use extract(epoch)/86400 for safety across date/timestamp types.
			return fmt.Sprintf("trunc(extract(epoch from (%s - %s)) / 86400)::bigint", end, start)
		case "week":
			return fmt.Sprintf("trunc(extract(epoch from (%s - %s)) / 604800)::bigint", end, start)
		case "month":
			// (year(end) - year(start)) * 12 + (month(end) - month(start))
			// But we need to account for day of month?
			// MySQL TIMESTAMPDIFF(MONTH, '2012-02-01', '2012-03-01') -> 1
			// PostgreSQL age() handles this well.
			return fmt.Sprintf("((extract(year from age(%s, %s)) * 12 + extract(month from age(%s, %s))))::bigint", end, start, end, start)
		case "quarter":
			return fmt.Sprintf("trunc(((extract(year from age(%s, %s)) * 12 + extract(month from age(%s, %s))) / 3))::bigint", end, start, end, start)
		case "year":
			return fmt.Sprintf("trunc(extract(year from age(%s, %s)))::bigint", end, start)
		default:
			return fmt.Sprintf("timestampdiff(%s, %s, %s)", unit, start, end)
		}
	})

	// Date/Time parts extraction
	timeParts := map[string]string{
		"year":       "year",
		"month":      "month",
		"day":        "day",
		"dayofmonth": "day",
		"hour":       "hour",
		"minute":     "minute",
		"second":     "second",
		"dayofyear":  "doy",
		"quarter":    "quarter",
		"week":       "week",
	}
	for funcName, part := range timeParts {
		processed = processFunctionCall(processed, funcName, func(args []string) string {
			if len(args) >= 1 {
				return fmt.Sprintf("extract(%s from %s)::integer", part, args[0])
			}
			return fmt.Sprintf("%s(%s)", funcName, strings.Join(args, ","))
		})
	}

	processed = processFunctionCall(processed, "dayofweek", func(args []string) string {
		if len(args) == 1 {
			// MySQL: 1=Sun...7=Sat. PG: 0=Sun...6=Sat.
			return fmt.Sprintf("(extract(dow from %s)::integer + 1)", args[0])
		}
		return fmt.Sprintf("dayofweek(%s)", strings.Join(args, ","))
	})

	processed = processFunctionCall(processed, "last_day", func(args []string) string {
		if len(args) == 1 {
			return fmt.Sprintf("(date_trunc('month', %s::date) + interval '1 month - 1 day')::date", args[0])
		}
		return fmt.Sprintf("last_day(%s)", strings.Join(args, ","))
	})

	processed = processFunctionCall(processed, "to_days", func(args []string) string {
		if len(args) == 1 {
			// Using Julian date for approximation suitable for diffs
			return fmt.Sprintf("extract(julian from %s)::integer", args[0])
		}
		return fmt.Sprintf("to_days(%s)", strings.Join(args, ","))
	})

	processed = processFunctionCall(processed, "curdate", func(args []string) string {
		return "current_date"
	})
	processed = processFunctionCall(processed, "curtime", func(args []string) string {
		return "current_time"
	})

	processed = processFunctionCall(processed, "time_to_sec", func(args []string) string {
		if len(args) == 1 {
			return fmt.Sprintf("extract(epoch from (%s)::time)::integer", args[0])
		}
		return fmt.Sprintf("time_to_sec(%s)", strings.Join(args, ","))
	})

	processed = processFunctionCall(processed, "sec_to_time", func(args []string) string {
		if len(args) == 1 {
			return fmt.Sprintf("(interval '1 second' * (%s))::time", args[0])
		}
		return fmt.Sprintf("sec_to_time(%s)", strings.Join(args, ","))
	})

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

	// 使用更精确的方式处理ADDTIME和SUBTIME函数，避免影响其他表达式
	processed = reADDTIME.ReplaceAllString(processed, "($1 + $2)")
	processed = reSUBTIME.ReplaceAllString(processed, "($1 - $2)")
	if processed == "" {
		return "", fmt.Errorf("failed to process ADDTIME/SUBTIME functions in view definition for view '%s'", viewName)
	}

	// 系统函数转换
	processed = reLAST_INSERT_ID.ReplaceAllStringFunc(processed, func(m string) string {
		return "lastval()"
	})
	processed = reCONNECTION_ID.ReplaceAllStringFunc(processed, func(m string) string {
		return "pg_backend_pid()"
	})
	processed = reCURRENT_USER.ReplaceAllStringFunc(processed, func(m string) string {
		return "current_user"
	})
	processed = reSESSION_USER.ReplaceAllStringFunc(processed, func(m string) string {
		return "session_user"
	})
	processed = reSYSTEM_USER.ReplaceAllStringFunc(processed, func(m string) string {
		return "system_user"
	})
	processed = reSCHEMA.ReplaceAllStringFunc(processed, func(m string) string {
		return "current_schema"
	})
	processed = reDATABASE.ReplaceAllStringFunc(processed, func(m string) string {
		return "current_database()"
	})
	processed = reUSER.ReplaceAllStringFunc(processed, func(m string) string {
		return "current_user"
	})
	processed = reVERSION.ReplaceAllStringFunc(processed, func(m string) string {
		return "version()"
	})
	// 转换 RAND 函数 (MySQL) 为 random() (PostgreSQL)
	// 处理 RAND() 和 RAND(seed) 两种情况
	// PostgreSQL的random()不支持种子参数，所以直接替换整个函数调用
	processed = reRAND.ReplaceAllString(processed, "random()")
	if processed == "" {
		return "", fmt.Errorf("failed to convert system functions in view definition for view '%s'", viewName)
	}

	// 处理 interval 语法 (如 now() + interval 1 day → now() + interval '1 day')
	processed = reInterval.ReplaceAllStringFunc(processed, func(m string) string {
		// 提取捕获组
		matches := reInterval.FindStringSubmatch(m)
		if len(matches) != 5 {
			return m
		}

		dateExpr := strings.TrimSpace(matches[1])
		operator := matches[2]
		number := matches[3]
		unit := matches[4]

		// 处理负数值的情况
		var processedOperator string
		var processedNumber string

		if strings.HasPrefix(number, "-") {
			// 如果数值是负数，运算符保持正号，数值变为正数
			processedOperator = "+"
			processedNumber = strings.TrimPrefix(number, "-")
		} else {
			processedOperator = operator
			processedNumber = number
		}

		var sb strings.Builder
		sb.WriteString(dateExpr)
		sb.WriteString(" ")
		sb.WriteString(processedOperator)
		sb.WriteString(" interval '")
		sb.WriteString(processedNumber)
		sb.WriteString(" ")
		sb.WriteString(unit)
		sb.WriteString("'")
		return sb.String()
	})
	if processed == "" {
		return "", fmt.Errorf("failed to process interval syntax in view definition for view '%s'", viewName)
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

	// Unmask string literals
	processed = unmaskStringLiterals(processed, literals)

	// 包装成CREATE OR REPLACE VIEW语句
	quotedViewName := quoteIdentifier(viewName)
	if quotedViewName == "" {
		return "", fmt.Errorf("failed to quote view name '%s'", viewName)
	}
	// Use DROP VIEW IF EXISTS ... CASCADE to allow type changes in columns
	createStmt := fmt.Sprintf("DROP VIEW IF EXISTS %s CASCADE; CREATE OR REPLACE VIEW %s AS %s;", quotedViewName, quotedViewName, processed)
	if createStmt == "" {
		return "", fmt.Errorf("failed to generate CREATE VIEW statement for view '%s'", viewName)
	}

	// 将整个语句转换为小写，确保符合要求
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
	out := s
	idx := 0
	for {
		// 直接在原字符串中查找 "concat("，不区分大小写
		pos := -1
		for i := idx; i <= len(out)-6; i++ {
			if strings.ToLower(out[i:i+6]) == "concat(" {
				pos = i
				break
			}
		}
		if pos == -1 {
			break
		}
		// 找到括号开始
		start := pos + 6
		depth := 1
		end := start
		// 找到匹配的右括号
		for i := start; i < len(out); i++ {
			switch out[i] {
			case '(':
				depth++
			case ')':
				depth--
				if depth == 0 {
					end = i
					break
				}
			}
		}
		// 如果找不到匹配的右括号，跳过这个函数调用
		if depth > 0 {
			idx = pos + 6
			continue
		}
		// 分割参数
		argsStr := out[start:end]
		args := splitTopLevelCommas(argsStr)
		// 构建替换后的字符串
		var sb strings.Builder
		sb.WriteString("(")
		for i, a := range args {
			if i > 0 {
				sb.WriteString(" || ")
			}
			sb.WriteString(strings.TrimSpace(a))
		}
		sb.WriteString(")")
		// 替换原字符串中的concat函数调用
		replacement := sb.String()
		out = out[:pos] + replacement + out[end+1:]
		// 更新索引位置
		idx = pos + len(replacement)
	}
	return out
}

// processFunctionCall 处理嵌套函数调用，找到函数名及其参数，然后对参数应用转换函数。
func processFunctionCall(s string, funcName string, transformer func([]string) string) string {
	reStart := regexp.MustCompile(`(?i)\b` + regexp.QuoteMeta(funcName) + `\s*\(`)
	matches := reStart.FindAllStringIndex(s, -1)

	for i := len(matches) - 1; i >= 0; i-- {
		pos := matches[i][0]

		openParenPos := -1
		for k := pos; k < len(s); k++ {
			if s[k] == '(' {
				openParenPos = k
				break
			}
		}
		if openParenPos == -1 {
			continue
		}

		depth := 1
		end := -1
		for k := openParenPos + 1; k < len(s); k++ {
			switch s[k] {
			case '(':
				depth++
			case ')':
				depth--
			}
			if depth == 0 {
				end = k
				break
			}
		}

		if end == -1 {
			continue
		}

		argsStr := s[openParenPos+1 : end]
		args := splitTopLevelCommas(argsStr)
		replacement := transformer(args)

		s = s[:pos] + replacement + s[end+1:]
	}
	return s
}

// maskStringLiterals 用占位符替换单引号字符串字面量。
func maskStringLiterals(s string) (string, map[string]string) {
	literals := make(map[string]string)
	var sb strings.Builder

	lastPos := 0
	inQuote := false
	quoteStart := 0
	litCount := 0

	for i := 0; i < len(s); i++ {
		if inQuote {
			if s[i] == '\\' {
				i++
				continue
			}
			if s[i] == '\'' {
				if i+1 < len(s) && s[i+1] == '\'' {
					i++
					continue
				}
				inQuote = false
				literal := s[quoteStart : i+1]
				placeholder := fmt.Sprintf("___STR-LIT-%d___", litCount)
				litCount++
				literals[placeholder] = literal

				sb.WriteString(s[lastPos:quoteStart])
				sb.WriteString(placeholder)
				lastPos = i + 1
			}
		} else {
			if s[i] == '\'' {
				inQuote = true
				quoteStart = i
			}
		}
	}
	sb.WriteString(s[lastPos:])

	return sb.String(), literals
}

// unmaskStringLiterals 从占位符恢复字符串字面量。
func unmaskStringLiterals(s string, literals map[string]string) string {
	for placeholder, val := range literals {
		s = strings.ReplaceAll(s, placeholder, val)
	}
	return s
}

// replaceCaseInsensitive 执行不区分大小写的字符串替换。
func replaceCaseInsensitive(s, oldStr, newStr string) string {
	// Escape special regex chars in 'oldStr'
	re := regexp.MustCompile("(?i)" + regexp.QuoteMeta(oldStr))
	return re.ReplaceAllString(s, newStr)
}

// processUsingClause 处理 CONVERT(expr USING charset) 和 CAST(expr USING charset)，将它们替换为 CAST(expr AS TEXT)。
func processUsingClause(s string) string {
	reStart := regexp.MustCompile(`(?i)\b(convert|cast)\s*\(`)

	matches := reStart.FindAllStringIndex(s, -1)

	for i := len(matches) - 1; i >= 0; i-- {
		pos := matches[i][0]

		openParenPos := -1
		for k := pos; k < len(s); k++ {
			if s[k] == '(' {
				openParenPos = k
				break
			}
		}
		if openParenPos == -1 {
			continue
		}

		depth := 1
		end := -1
		for k := openParenPos + 1; k < len(s); k++ {
			if s[k] == '(' {
				depth++
			} else if s[k] == ')' {
				depth--
				if depth == 0 {
					end = k
					break
				}
			}
		}

		if end == -1 {
			continue
		}

		content := s[openParenPos+1 : end]
		reUsing := regexp.MustCompile(`(?i)\s+USING\s+[\w]+`)
		loc := reUsing.FindStringIndex(content)

		if loc != nil {
			expr := content[:loc[0]]
			replacement := fmt.Sprintf("CAST(%s AS TEXT)", expr)
			s = s[:pos] + replacement + s[end+1:]
		} else {
			funcName := s[pos:openParenPos]
			if strings.ToLower(funcName) == "convert" {
				parts := splitTopLevelCommas(content)
				if len(parts) == 2 {
					replacement := fmt.Sprintf("CAST(%s AS %s)", parts[0], parts[1])
					s = s[:pos] + replacement + s[end+1:]
				}
			}
		}
	}
	return s
}

func replaceJoinAliases(s string) string {
	matches := reJoinStart.FindAllStringIndex(s, -1)

	for i := len(matches) - 1; i >= 0; i-- {
		start := matches[i][0]
		endStart := matches[i][1]
		current := endStart
		for current < len(s) && (s[current] == ' ' || s[current] == '\t' || s[current] == '\n') {
			current++
		}

		if current >= len(s) || s[current] != '(' {
			continue
		}
		onStart := current
		depth := 0
		condEnd := -1

		for k := current; k < len(s); k++ {
			if s[k] == '(' {
				depth++
			} else if s[k] == ')' {
				depth--
				if depth == 0 {
					condEnd = k
					break
				}
			}
		}

		if condEnd == -1 {
			continue
		}

		// 从开始正则中提取捕获
		submatch := reJoinStart.FindStringSubmatch(s[start:endStart])
		if len(submatch) < 5 {
			continue
		}
		// t1 := submatch[1]
		a1 := submatch[2]
		// t2 := submatch[3]
		a2 := submatch[4]

		// ON 子句内容（外层括号内）
		condition := s[onStart+1 : condEnd]

		// 处理条件以添加别名
		processedCondition := reColumns.ReplaceAllStringFunc(condition, func(colMatch string) string {
			// 确保匹配逻辑安全：仅当匹配后不是 '('
			// 我们检查此匹配的原始条件字符串上下文
			idx := strings.Index(condition, colMatch)
			if idx == -1 {
				return colMatch
			}

			// 检查匹配后的字符
			afterMatchIdx := idx + len(colMatch)
			if afterMatchIdx < len(condition) {
				nextChar := condition[afterMatchIdx]
				if nextChar == '(' {
					// 可能是函数调用，如 length(...)
					return colMatch
				}
			}

			parts := strings.SplitN(colMatch, "=", 2)
			if len(parts) != 2 {
				return colMatch
			}

			col1 := strings.TrimSpace(parts[0])
			col2 := strings.TrimSpace(parts[1])

			// 缺失时添加别名
			if !strings.Contains(col1, ".") {
				col1 = fmt.Sprintf("%s.%s", a1, col1)
			}
			if !strings.Contains(col2, ".") {
				// 特殊检查：如果 col2 与 col1 相同，两个表都有则可能歧义。
				// 但我们假设 col2 属于 a2。
				// 例如，如果 col2 是 "c1"，且 a1 和 a2 都有 "c1"。
				// 连接条件 "c1" = "c1" 通常意味着 a1.c1 = a2.c1
				col2 = fmt.Sprintf("%s.%s", a2, col2)
			}

			// 添加类型转换以避免类型不匹配
			return fmt.Sprintf("%s::text = %s::text", col1, col2)
		})

		// 替换 ON 括号内的条件。
		s = s[:onStart+1] + processedCondition + s[condEnd:]
	}
	return s
}
