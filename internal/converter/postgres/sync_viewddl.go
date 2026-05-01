package postgres

import (
	"fmt"
	"regexp"
	"strings"
)

// 正则表达式预编译，提高性能
var (
	// 匹配三段式数据库名前缀，如 "db"."table"."column"
	reDBPrefix = regexp.MustCompile(`(?i)"[^"]+"\.("[^"]+"\."[^"]+")`)
	// 匹配带别名的二段式表引用，如 "db"."table" "t"
	reDBTableWithAlias = regexp.MustCompile(`(?i)"[^"]+"\.("[^"]+")(\s+"[^"]+")`)
	// 匹配 FROM/JOIN 中不带别名的二段式表引用，如 FROM "db"."table"
	reDBTableInFromJoin = regexp.MustCompile(`(?i)\b(from|join)\s+"[^"]+"\.("[^"]+")`)
	// 匹配 IFNULL 函数
	reIfnull = regexp.MustCompile(`(?i)ifnull\s*\(`)
	// 匹配 GROUP_CONCAT 函数
	reGroupConcat = regexp.MustCompile(`(?i)group_concat\s*\(\s*(?:distinct\s+)?([^)]*)\)`)
	// 匹配 ORDER BY 子句
	reOrder = regexp.MustCompile(`(?i)\s+order\s+by\s+[^,]*`)
	// 匹配 DISTINCT 关键字
	reDistinct = regexp.MustCompile(`(?i)\bdistinct\s+`)
	// 匹配 SEPARATOR 关键字
	reSep = regexp.MustCompile(`(?i)\s*separator\s*['"]([^'"]+)['"]`)
	// 匹配 CONVERT 函数
	reConvert = regexp.MustCompile(`(?i)\bconvert\s*\(\s*([^,]+)\s*,\s*([^)]+)\)`)
	reCast    = regexp.MustCompile(`(?i)\bcast\s*\(\s*(.+?)\s+as\s+([a-z_][a-z0-9_]*(?:\s*\(\s*[^)]+\s*\))?)\s*\)`)
	// 匹配 CAST(x USING charset) 语法（MySQL 特有，PostgreSQL 不支持）
	reCastUsing = regexp.MustCompile(`(?i)\bcast\s*\(\s*([^)]+)\s+using\s+[a-z0-9_]+\s*\)(?:\s+as\s+'[^']*')?`)
	// 匹配 LIMIT a,b 语法
	reLimitOffset = regexp.MustCompile(`(?i)\blimit\s+(\d+)\s*,\s*(\d+)`)
	// 匹配 JSON_OBJECT 函数
	reJSONObject = regexp.MustCompile(`(?i)json_object\s*\(`)
	// 匹配 JSON_ARRAY 函数
	reJSONArray = regexp.MustCompile(`(?i)json_array\s*\(`)
	// 匹配 JSON_QUOTE 函数
	reJSONQuote = regexp.MustCompile(`(?i)json_quote\s*\(\s*([^)]+)\s*\)`)
	// 匹配 JSON_UNQUOTE 函数
	reJSONUnquote        = regexp.MustCompile(`(?i)json_unquote\s*\(\s*([^)]+)\s*\)`)
	reJSONUnquoteExtract = regexp.MustCompile(`(?i)json_unquote\s*\(\s*json_extract\s*\(\s*([^,]+)\s*,\s*([^)]+)\)\s*\)`)
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
	reYEAR_FUNC   = regexp.MustCompile(`(?i)\byear\s*\(\s*([^)]+)\)`)
	reMONTH_FUNC  = regexp.MustCompile(`(?i)\bmonth\s*\(\s*([^)]+)\)`)
	reDAY_FUNC    = regexp.MustCompile(`(?i)\bdayofmonth\s*\(\s*([^)]+)\)`)
	reHOUR_FUNC   = regexp.MustCompile(`(?i)\bhour\s*\(\s*([^)]+)\)`)
	reMINUTE_FUNC = regexp.MustCompile(`(?i)\bminute\s*\(\s*([^)]+)\)`)
	reSECOND_FUNC = regexp.MustCompile(`(?i)\bsecond\s*\(\s*([^)]+)\)`)
	// 匹配 YEARWEEK 函数
	reYEARWEEK = regexp.MustCompile(`(?i)\byearweek\s*\(\s*([^)]+)\)`)
	// 匹配 DAYNAME 函数
	reDAYNAME = regexp.MustCompile(`(?i)\bdayname\s*\(\s*([^)]+)\)`)
	// 匹配 MONTHNAME 函数
	reMONTHNAME = regexp.MustCompile(`(?i)\bmonthname\s*\(\s*([^)]+)\)`)
	// 匹配 QUARTER 函数
	reQUARTER = regexp.MustCompile(`(?i)\bquarter\s*\(\s*([^)]+)\)`)
	// 匹配 WEEK 函数
	reWEEK = regexp.MustCompile(`(?i)\bweek\s*\(\s*([^)]+)\)`)
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
	// 匹配连接条件中的列名
	reColumns = regexp.MustCompile(`(?i)(["\w]+)\s*=\s*("[\w]+")`)
	// 匹配SUM函数的正则
	reSum = regexp.MustCompile(`(?i)sum\s*\(\s*(["\w\.]+)\s*\)`)
	// 匹配COALESCE函数的正则
	reCoalesce = regexp.MustCompile(`(?i)coalesce\s*\(\s*("[\w\.]+)\s*,\s*(\d+)\s*\)`)
	// 匹配 interval 语法 (如 now() + interval 1 day)
	reInterval  = regexp.MustCompile(`(?i)(\S[^+\-]*\S)\s*([+\-])\s*interval\s+([+\-]?\d+)\s+([\w_]+)`)
	reIndexHint = regexp.MustCompile(`(?i)\b(?:force|use|ignore)\s+index\s*(?:for\s+(?:join|order\s+by|group\s+by)\s*)?\([^)]+\)`)
	reISNULL    = regexp.MustCompile(`(?i)\bisnull\s*\(\s*([^)]+)\s*\)`)
	// 匹配 REGEXP_LIKE 函数 (MySQL 8.0+)
	reRegexpLike = regexp.MustCompile(`(?i)\bregexp_like\s*\(\s*([^,]+)\s*,\s*([^)]+)\)`)
	// 匹配 LOCATE 函数 (MySQL)
	reLocate = regexp.MustCompile(`(?i)\blocate\s*\(\s*([^,]+)\s*,\s*([^)]+)\)`)
	// 匹配 JSON_ARRAYAGG 函数 (MySQL 8.0+)
	reJsonArrayagg = regexp.MustCompile(`(?i)\bjson_arrayagg\s*\(\s*([^)]+)\)`)
	// 匹配 JSON_OBJECTAGG 函数 (MySQL 8.0+)
	reJsonObjectagg = regexp.MustCompile(`(?i)\bjson_objectagg\s*\(\s*([^,]+)\s*,\s*([^)]+)\)`)
	// 匹配 JSON_INSERT 函数 (MySQL 8.0+) - 用于视图
	reJSONInsertView = regexp.MustCompile(`(?i)\bjson_insert\s*\(\s*([^,]+)\s*,\s*'([^']+?)'\s*,\s*([^)]+)\)`)
	// 匹配 JSON_REPLACE 函数 (MySQL 8.0+) - 用于视图
	reJSONReplaceView = regexp.MustCompile(`(?i)\bjson_replace\s*\(\s*([^,]+)\s*,\s*'([^']+?)'\s*,\s*([^)]+)\)`)
	// 匹配 JSON_SET 函数 (MySQL 8.0+) - 用于视图
	reJSONSetView = regexp.MustCompile(`(?i)\bjson_set\s*\(\s*([^,]+)\s*,\s*(.+)\)`)
	// 匹配 JSON_REMOVE 函数 (MySQL 8.0+) - 用于视图
	reJSONRemoveView = regexp.MustCompile(`(?i)\bjson_remove\s*\(\s*([^,]+)\s*,\s*'([^']+?)'\)`)
	// 匹配 JSON_MERGE_PATCH 函数 (MySQL 8.0+) - 用于视图
	reJSONMergePatchView = regexp.MustCompile(`(?i)\bjson_merge_patch\s*\(\s*([^,]+)\s*,\s*([^)]+)\)`)
	// 匹配 JSON_KEYS 函数 (MySQL 8.0+) - 用于视图
	reJSONKeysView = regexp.MustCompile(`(?i)\bjson_keys\s*\(\s*([^)]+)\)`)
	// 匹配 JSON_LENGTH 函数 (MySQL 8.0+) - 用于视图
	reJSONLengthView = regexp.MustCompile(`(?i)\bjson_length\s*\(\s*([^)]+)\)`)
	// 匹配 JSON_ARRAY_INSERT 函数 (MySQL 9.0+) - 用于视图
	reJSONArrayInsert = regexp.MustCompile(`(?i)\bjson_array_insert\s*\(\s*([^,]+)\s*,\s*'([^']+?)'\s*,\s*([^)]+)\)`)
	// 匹配 INSTR 函数 (MySQL)
	reInstr = regexp.MustCompile(`(?i)\binstr\s*\(\s*([^,]+)\s*,\s*([^)]+)\)`)
	// 匹配 RLIKE 操作符 (MySQL 8.0+) - 支持括号内的情况
	reRLike = regexp.MustCompile(`(?i)(\([^)]+)\s+rlike\s+'([^']+)'`)
	// 匹配 REGEXP_INSTR 函数 (MySQL 8.0+) - 2 参数版本
	reRegexpInstr = regexp.MustCompile(`(?i)\bregexp_instr\s*\(\s*([^,]+)\s*,\s*([^)]+)\)`)
	// 匹配 REGEXP_INSTR 函数 (MySQL 8.0.17+/9.0+) - 6 参数完整版本
	reRegexpInstrFull = regexp.MustCompile(`(?i)\bregexp_instr\s*\(\s*([^,]+)\s*,\s*([^,]+)\s*,\s*([^,]+)\s*,\s*([^,]+)\s*,\s*([^,]+)\s*,\s*([^)]+)\)`)
	// 匹配 REGEXP_SUBSTR 函数 (MySQL 8.0+) - 2 参数版本
	reRegexpSubstr = regexp.MustCompile(`(?i)\bregexp_substr\s*\(\s*([^,]+)\s*,\s*([^)]+)\)`)
	// 匹配 REGEXP_SUBSTR 函数 (MySQL 8.0.17+/9.0+) - 4 参数版本（带 occurrence）
	reRegexpSubstrFull = regexp.MustCompile(`(?i)\bregexp_substr\s*\(\s*([^,]+)\s*,\s*([^,]+)\s*,\s*([^,]+)\s*,\s*([^)]+)\)`)
	// 匹配 REGEXP_REPLACE 函数 (MySQL 8.0+) - 三个参数
	reRegexpReplace = regexp.MustCompile(`(?i)\bregexp_replace\s*\(\s*([^,]+)\s*,\s*([^,]+)\s*,\s*([^)]+)\)`)
	// 匹配 CAST(x AS SIGNED) 函数
	reCastSigned = regexp.MustCompile(`(?i)\bcast\s*\(\s*([^)]+)\s+as\s+signed\)`)
	// 匹配 CAST(x AS CHAR) 函数
	reCastChar = regexp.MustCompile(`(?i)\bcast\s*\(\s*([^)]+)\s+as\s+char(?:\(\d+\))?\)`)
	// 匹配 FORCE INDEX 提示
	reForceIndex = regexp.MustCompile(`(?i)\bforce\s+index\s*\([^)]*\)`)
)

// ConvertViewDDL 将MySQL的VIEW_DEFINITION转换为PostgreSQL的CREATE VIEW语句,从information_schema.VIEWS中读取的VIEW_DEFINITION字段内容
func ConvertViewDDL(viewName string, viewDefinition string) (string, error) {
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

	processed = reIndexHint.ReplaceAllString(processed, "")
	processed = strings.Join(strings.Fields(processed), " ")
	if processed == "" {
		return "", fmt.Errorf("failed to remove mysql index hints in view definition for view '%s'", viewName)
	}

	processed = reISNULL.ReplaceAllString(processed, "($1 IS NULL)")
	if processed == "" {
		return "", fmt.Errorf("failed to replace isnull in view definition for view '%s'", viewName)
	}

	processed = replaceToDaysExpressions(processed)
	if processed == "" {
		return "", fmt.Errorf("failed to replace to_days in view definition for view '%s'", viewName)
	}

	// 将 REGEXP_LIKE(expr, pattern) 转换为 expr ~ pattern (PostgreSQL 正则匹配)
	processed = replaceRegexpLikeExpressions(processed)
	if processed == "" {
		return "", fmt.Errorf("failed to replace REGEXP_LIKE in view definition for view '%s'", viewName)
	}

	// 将 REGEXP_INSTR 完整版本 (6 参数) 转换为 PostgreSQL (MySQL 8.0.17+/9.0+)
	processed = replaceRegexpInstrFullExpressions(processed)
	if processed == "" {
		return "", fmt.Errorf("failed to replace REGEXP_INSTR full in view definition for view '%s'", viewName)
	}

	// 将 REGEXP_INSTR(str, pattern) 转换为 POSITION(substring in str) 或 regexp_match (PostgreSQL)
	processed = replaceRegexpInstrExpressions(processed)
	if processed == "" {
		return "", fmt.Errorf("failed to replace REGEXP_INSTR in view definition for view '%s'", viewName)
	}

	// 将 REGEXP_SUBSTR 完整版本 (4 参数) 转换为 PostgreSQL (MySQL 8.0.17+/9.0+)
	processed = replaceRegexpSubstrFullExpressions(processed)
	if processed == "" {
		return "", fmt.Errorf("failed to replace REGEXP_SUBSTR full in view definition for view '%s'", viewName)
	}

	// 将 REGEXP_SUBSTR(str, pattern) 转换为 SUBSTRING(str from pattern) (PostgreSQL)
	processed = replaceRegexpSubstrExpressions(processed)
	if processed == "" {
		return "", fmt.Errorf("failed to replace REGEXP_SUBSTR in view definition for view '%s'", viewName)
	}

	// 将 REGEXP_REPLACE(str, pattern, repl) 转换为 regexp_replace(str, pattern, repl) (PostgreSQL)
	processed = replaceRegexpReplaceExpressions(processed)
	if processed == "" {
		return "", fmt.Errorf("failed to replace REGEXP_REPLACE in view definition for view '%s'", viewName)
	}

	// 将 LOCATE(substr, str) 转换为 STRPOS(str, substr) (PostgreSQL)
	processed = replaceLocateExpressions(processed)
	if processed == "" {
		return "", fmt.Errorf("failed to replace LOCATE in view definition for view '%s'", viewName)
	}

	// 将 JSON_ARRAYAGG(expr) 转换为 JSON_AGG(expr) (PostgreSQL)
	processed = replaceJsonAggExpressions(processed)
	if processed == "" {
		return "", fmt.Errorf("failed to replace JSON_ARRAYAGG in view definition for view '%s'", viewName)
	}

	// 将 JSON_OBJECTAGG(key, value) 转换为 JSON_OBJECT_AGG(key, value) (PostgreSQL)
	processed = replaceJsonObjectAggExpressions(processed)
	if processed == "" {
		return "", fmt.Errorf("failed to replace JSON_OBJECTAGG in view definition for view '%s'", viewName)
	}

	// 将 JSON_INSERT/JSON_REPLACE/JSON_SET 转换为 JSONB_SET (PostgreSQL)
	processed = replaceJSONInsertView(processed)
	processed = replaceJSONReplaceView(processed)
	processed = replaceJSONSetView(processed)

	// 将 JSON_REMOVE 转换为 JSONB_DELETE_PATH (PostgreSQL)
	processed = replaceJSONRemoveView(processed)

	// 将 JSON_MERGE_PATCH 转换为 JSONB 连接操作符 || (PostgreSQL)
	processed = replaceJSONMergePatchView(processed)

	// 将 JSON_KEYS 转换为 JSONB_OBJECT_KEYS (PostgreSQL)
	processed = replaceJSONKeysView(processed)

	// 将 JSON_LENGTH 转换为 JSONB_ARRAY_LENGTH (PostgreSQL)
	processed = replaceJSONLengthView(processed)

	// 将 JSON_ARRAY_INSERT 转换为 JSONB_INSERT (PostgreSQL, MySQL 9.0+)
	processed = replaceJSONArrayInsert(processed)

	// 将 INSTR(str, substr) 转换为 STRPOS(str, substr) (PostgreSQL)
	processed = replaceInstrExpressions(processed)

	// 将 RLIKE 转换为 ~ (PostgreSQL 正则匹配)
	processed = replaceRLikeExpressions(processed)

	// 将 CAST(x AS SIGNED) 转换为 CAST(x AS INTEGER) (PostgreSQL)
	processed = replaceCastSignedExpressions(processed)

	// 将 CAST(x AS CHAR) 转换为 CAST(x AS TEXT) (PostgreSQL)
	processed = replaceCastCharExpressions(processed)

	// 移除 FORCE INDEX 提示 (PostgreSQL 不支持)
	processed = reForceIndex.ReplaceAllString(processed, "")

	// 移除三段式数据库名前缀（例如 "db"."table"."col" -> "table"."col"）
	processed = reDBPrefix.ReplaceAllString(processed, "$1")
	processed = reDBTableWithAlias.ReplaceAllString(processed, "$1$2")
	processed = reDBTableInFromJoin.ReplaceAllString(processed, "$1 $2")
	if processed == "" {
		return "", fmt.Errorf("failed to remove database prefix in view definition for view '%s'", viewName)
	}

	// 将IFNULL/ifnull替换为COALESCE
	processed = reIfnull.ReplaceAllString(processed, "COALESCE(")
	if processed == "" {
		return "", fmt.Errorf("failed to replace IFNULL with COALESCE in view definition for view '%s'", viewName)
	}

	// GROUP_CONCAT -> string_agg 的增强转换，支持 DISTINCT、ORDER BY 和 SEPARATOR
	processed = reGroupConcat.ReplaceAllStringFunc(processed, func(s string) string {
		m := reGroupConcat.FindStringSubmatch(s)
		if len(m) < 2 {
			return s
		}
		inner := m[1]

		// 检查是否有 DISTINCT
		hasDistinct := strings.Contains(strings.ToUpper(inner), "DISTINCT")
		innerNoDistinct := reDistinct.ReplaceAllString(inner, "")

		// 提取 ORDER BY 子句（如果有）
		var orderBy string
		orderByMatch := reOrder.FindStringSubmatch(innerNoDistinct)
		if len(orderByMatch) > 0 {
			orderBy = strings.TrimSpace(strings.TrimPrefix(orderByMatch[0], " "))
			innerNoDistinct = reOrder.ReplaceAllString(innerNoDistinct, "")
		}

		// 解析 SEPARATOR
		sepM := reSep.FindStringSubmatch(innerNoDistinct)
		sep := ","
		if len(sepM) >= 2 {
			sep = sepM[1]
			innerNoDistinct = reSep.ReplaceAllString(innerNoDistinct, "")
		}

		expr := strings.TrimSpace(innerNoDistinct)

		// 构建 PostgreSQL string_agg 表达式（ORDER BY 需位于聚合函数内部）
		var sb strings.Builder
		sb.WriteString("string_agg(")
		if hasDistinct {
			sb.WriteString("DISTINCT ")
		}
		sb.WriteString("CAST(")
		sb.WriteString(expr)
		sb.WriteString(" AS text)")
		sb.WriteString(", '")
		sb.WriteString(strings.ReplaceAll(sep, `'`, `''`))
		sb.WriteString("')")
		if orderBy != "" {
			// PostgreSQL 语法：string_agg(expr, sep ORDER BY ...)
			pgOrderBy := convertMySQLOrderByToPG(orderBy)
			sb.WriteString(" ORDER BY ")
			sb.WriteString(stripOrderByKeyword(pgOrderBy))
		}

		return sb.String()
	})
	if processed == "" {
		return "", fmt.Errorf("failed to convert GROUP_CONCAT to string_agg in view definition for view '%s'", viewName)
	}

	//  将IF(expr, then, else)转换为CASE WHEN ... THEN ... ELSE ... END（简单版，不处理嵌套逗号）
	processed = replaceIfExpressions(processed)
	if processed == "" {
		return "", fmt.Errorf("failed to replace IF with CASE WHEN in view definition for view '%s'", viewName)
	}

	// 处理 CAST(x USING charset) 语法（MySQL 特有，PostgreSQL 不支持）
	// MySQL: CAST(x USING utf8mb4) - 字符集转换
	// PostgreSQL: 直接使用 x（PostgreSQL 默认使用 UTF-8）
	processed = reCastUsing.ReplaceAllStringFunc(processed, func(m string) string {
		match := reCastUsing.FindStringSubmatch(m)
		if len(match) < 2 {
			return m
		}
		expr := strings.TrimSpace(match[1])
		// 直接返回表达式，移除 CAST ... USING 语法
		return expr
	})

	processed = reConvert.ReplaceAllStringFunc(processed, func(m string) string {
		match := reConvert.FindStringSubmatch(m)
		if len(match) < 3 {
			return m
		}
		expr := strings.TrimSpace(match[1])
		targetType := normalizeCastTypeForPG(strings.TrimSpace(match[2]))
		return fmt.Sprintf("CAST(%s AS %s)", expr, targetType)
	})
	processed = reCast.ReplaceAllStringFunc(processed, func(m string) string {
		match := reCast.FindStringSubmatch(m)
		if len(match) < 3 {
			return m
		}
		expr := strings.TrimSpace(match[1])
		targetType := normalizeCastTypeForPG(strings.TrimSpace(match[2]))
		return fmt.Sprintf("CAST(%s AS %s)", expr, targetType)
	})
	if processed == "" {
		return "", fmt.Errorf("failed to replace CONVERT with CAST in view definition for view '%s'", viewName)
	}

	// 将LIMIT a,b转换为LIMIT b OFFSET a
	processed = reLimitOffset.ReplaceAllString(processed, "LIMIT $2 OFFSET $1")
	if processed == "" {
		return "", fmt.Errorf("failed to adjust LIMIT syntax in view definition for view '%s'", viewName)
	}

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

	// 修正常见MySQL函数差异/关键字，JSON函数转换
	processed = reJSONObject.ReplaceAllString(processed, "json_build_object(")
	processed = reJSONArray.ReplaceAllString(processed, "json_build_array(")
	processed = replaceJSONQuoteExpressions(processed)
	processed = reJSONUnquoteExtract.ReplaceAllStringFunc(processed, func(m string) string {
		match := reJSONUnquoteExtract.FindStringSubmatch(m)
		if len(match) < 3 {
			return m
		}
		return buildJSONPathExpr(strings.TrimSpace(match[1]), strings.TrimSpace(match[2]), true)
	})
	processed = reJSONUnquote.ReplaceAllStringFunc(processed, func(m string) string {
		match := reJSONUnquote.FindStringSubmatch(m)
		if len(match) < 2 {
			return m
		}
		arg := strings.TrimSpace(match[1])
		return fmt.Sprintf("trim(both '\"' from (%s)::text)", arg)
	})
	processed = reJSONExtract.ReplaceAllStringFunc(processed, func(m string) string {
		match := reJSONExtract.FindStringSubmatch(m)
		if len(match) < 3 {
			return m
		}
		return buildJSONPathExpr(strings.TrimSpace(match[1]), strings.TrimSpace(match[2]), false)
	})
	processed = reJSONKeys.ReplaceAllString(processed, "json_object_keys(")
	processed = reJSONLength.ReplaceAllString(processed, "json_array_length(")
	processed = reJSONType.ReplaceAllString(processed, "jsonb_typeof(")
	processed = reJSONValid.ReplaceAllStringFunc(processed, func(m string) string {
		// 匹配JSON_VALID(expr) -> (expr IS NOT NULL AND jsonb_typeof(expr::jsonb) IS NOT NULL)
		return "(" + m[10:len(m)-1] + " IS NOT NULL AND jsonb_typeof(" + m[10:len(m)-1] + "::jsonb) IS NOT NULL)"
	})
	processed = reJSONValue.ReplaceAllStringFunc(processed, func(m string) string {
		match := reJSONValue.FindStringSubmatch(m)
		if len(match) < 3 {
			return m
		}
		return buildJSONPathExpr(strings.TrimSpace(match[1]), strings.TrimSpace(match[2]), true)
	})
	// 使用可执行的 PostgreSQL JSONB 语法替换（避免仅替换函数名导致语法错误）
	processed = replaceJSONSetCompatible(processed)
	processed = replaceJSONInsertCompatible(processed)
	processed = replaceJSONReplaceCompatible(processed)
	processed = replaceJSONRemoveCompatible(processed)
	processed = replaceJSONArrayAppendCompatible(processed)
	processed = replaceJSONArrayInsertCompatible(processed)
	processed = replaceJSONMergeCompatible(processed)
	processed = replaceJSONMergePatchCompatible(processed)
	processed = replaceJSONMergePreserveCompatible(processed)

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
	processed = reYEAR_FUNC.ReplaceAllString(processed, "extract(year from $1)::int")
	processed = reMONTH_FUNC.ReplaceAllString(processed, "extract(month from $1)::int")
	processed = reDAY_FUNC.ReplaceAllString(processed, "extract(day from $1)::int")
	processed = reHOUR_FUNC.ReplaceAllString(processed, "extract(hour from $1)::int")
	processed = reMINUTE_FUNC.ReplaceAllString(processed, "extract(minute from $1)::int")
	processed = reSECOND_FUNC.ReplaceAllString(processed, "extract(second from $1)::int")
	// 新增日期时间函数转换
	processed = reYEARWEEK.ReplaceAllString(processed, "(extract(year from $1)::int * 100 + extract(week from $1)::int)")
	processed = reDAYNAME.ReplaceAllString(processed, "to_char($1, 'Day')")
	processed = reMONTHNAME.ReplaceAllString(processed, "to_char($1, 'Month')")
	processed = reQUARTER.ReplaceAllString(processed, "extract(quarter from $1)::int")
	processed = reWEEK.ReplaceAllString(processed, "extract(week from $1)::int")
	processed = reDATE_FORMAT.ReplaceAllStringFunc(processed, func(m string) string {
		match := reDATE_FORMAT.FindStringSubmatch(m)
		if len(match) < 3 {
			return m
		}
		return fmt.Sprintf("to_char(%s, %s)", strings.TrimSpace(match[1]), convertMySQLDateFormatToPG(strings.TrimSpace(match[2])))
	})
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
		// 匹配 DATE_ADD(date, INTERVAL expr unit) -> date + (expr * interval '1 unit')
		datePart := strings.TrimSpace(match[1])
		intervalPart := strings.TrimSpace(match[2])
		parts := strings.SplitN(intervalPart, " ", 2)
		if len(parts) < 2 {
			return fmt.Sprintf("(%s + (%s)::interval)", datePart, intervalPart)
		}
		num := strings.TrimSpace(parts[0])
		unit := normalizeIntervalUnit(strings.TrimSpace(parts[1]))
		return fmt.Sprintf("(%s + ((%s) * interval '1 %s'))", datePart, num, unit)
	})
	processed = reDATE_SUB.ReplaceAllStringFunc(processed, func(m string) string {
		match := reDATE_SUB.FindStringSubmatch(m)
		if len(match) < 3 {
			return m
		}
		// 匹配 DATE_SUB(date, INTERVAL expr unit) -> date - (expr * interval '1 unit')
		datePart := strings.TrimSpace(match[1])
		intervalPart := strings.TrimSpace(match[2])
		parts := strings.SplitN(intervalPart, " ", 2)
		if len(parts) < 2 {
			return fmt.Sprintf("(%s - (%s)::interval)", datePart, intervalPart)
		}
		num := strings.TrimSpace(parts[0])
		unit := normalizeIntervalUnit(strings.TrimSpace(parts[1]))
		return fmt.Sprintf("(%s - ((%s) * interval '1 %s'))", datePart, num, unit)
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

	// 包装成CREATE OR REPLACE VIEW语句
	quotedViewName := quoteIdentifier(viewName)
	if quotedViewName == "" {
		return "", fmt.Errorf("failed to quote view name '%s'", viewName)
	}
	createStmt := fmt.Sprintf("CREATE OR REPLACE VIEW %s AS %s;", quotedViewName, processed)
	if createStmt == "" {
		return "", fmt.Errorf("failed to generate CREATE VIEW statement for view '%s'", viewName)
	}

	// 将 SQL 关键字和标识符规范化为小写，但保留单引号内字面量内容
	createStmt = lowerSQLPreservingSingleQuotedLiterals(createStmt)
	if createStmt == "" {
		return "", fmt.Errorf("failed to convert CREATE VIEW statement to lowercase for view '%s'", viewName)
	}

	return createStmt, nil
}

// lowerSQLPreservingSingleQuotedLiterals 将 SQL 在单引号字面量外转为小写。
func lowerSQLPreservingSingleQuotedLiterals(sql string) string {
	if sql == "" {
		return ""
	}
	var out strings.Builder
	out.Grow(len(sql))
	inSingleQuoted := false
	for i := 0; i < len(sql); i++ {
		ch := sql[i]
		if ch == '\'' {
			out.WriteByte(ch)
			if inSingleQuoted && i+1 < len(sql) && sql[i+1] == '\'' {
				out.WriteByte(sql[i+1])
				i++
				continue
			}
			inSingleQuoted = !inSingleQuoted
			continue
		}
		if inSingleQuoted {
			out.WriteByte(ch)
			continue
		}
		if ch >= 'A' && ch <= 'Z' {
			ch = ch + ('a' - 'A')
		}
		out.WriteByte(ch)
	}
	return out.String()
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

func replaceIfExpressions(s string) string {
	out := s
	idx := 0
	for {
		pos, openParen := findNextIfCall(out, idx)
		if pos == -1 {
			break
		}
		endParen, ok := findMatchingParenInViewExpr(out, openParen)
		if !ok {
			idx = openParen + 1
			continue
		}
		args := splitTopLevelCommas(out[openParen+1 : endParen])
		if len(args) != 3 {
			idx = endParen + 1
			continue
		}
		replacement := fmt.Sprintf("CASE WHEN %s THEN %s ELSE %s END",
			strings.TrimSpace(args[0]),
			strings.TrimSpace(args[1]),
			strings.TrimSpace(args[2]),
		)
		out = out[:pos] + replacement + out[endParen+1:]
		idx = pos + len(replacement)
	}
	return out
}

func findNextIfCall(s string, start int) (int, int) {
	for i := start; i < len(s)-1; i++ {
		if !((s[i] == 'i' || s[i] == 'I') && (s[i+1] == 'f' || s[i+1] == 'F')) {
			continue
		}
		if i > 0 && isIdentChar(s[i-1]) {
			continue
		}
		j := i + 2
		if j < len(s) && isIdentChar(s[j]) {
			continue
		}
		for j < len(s) && (s[j] == ' ' || s[j] == '\t' || s[j] == '\n' || s[j] == '\r') {
			j++
		}
		if j < len(s) && s[j] == '(' {
			return i, j
		}
	}
	return -1, -1
}

func findMatchingParenInViewExpr(s string, openIdx int) (int, bool) {
	depth := 1
	inSingle := false
	inDouble := false
	for i := openIdx + 1; i < len(s); i++ {
		switch s[i] {
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
				depth--
				if depth == 0 {
					return i, true
				}
			}
		}
	}
	return -1, false
}

func isIdentChar(b byte) bool {
	return (b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z') || (b >= '0' && b <= '9') || b == '_'
}

func normalizeCastTypeForPG(t string) string {
	normalized := strings.TrimSpace(t)
	upper := strings.ToUpper(normalized)
	switch {
	case upper == "SIGNED", upper == "UNSIGNED":
		return "BIGINT"
	case upper == "DATETIME":
		return "TIMESTAMP"
	case upper == "CHAR":
		return "TEXT"
	case strings.HasPrefix(upper, "DECIMAL"):
		return "NUMERIC" + normalized[len("DECIMAL"):]
	default:
		return normalized
	}
}

func convertMySQLDateFormatToPG(raw string) string {
	format := strings.TrimSpace(raw)
	if len(format) >= 2 && ((format[0] == '\'' && format[len(format)-1] == '\'') || (format[0] == '"' && format[len(format)-1] == '"')) {
		format = format[1 : len(format)-1]
	}
	replacer := strings.NewReplacer(
		"%Y", "YYYY",
		"%y", "YY",
		"%m", "MM",
		"%c", "FMMM",
		"%d", "DD",
		"%e", "FMDD",
		"%H", "HH24",
		"%h", "HH12",
		"%I", "HH12",
		"%i", "MI",
		"%s", "SS",
		"%S", "SS",
	)
	format = replacer.Replace(format)
	return "'" + format + "'"
}

// convertMySQLOrderByToPG 将 MySQL ORDER BY 子句转换为 PostgreSQL 格式
// 例如："col ASC" -> "col ASC", "col DESC" -> "col DESC"
func convertMySQLOrderByToPG(orderBy string) string {
	// 简单处理：移除可能的引号，保持原有顺序
	result := strings.TrimSpace(orderBy)
	// 如果包含引号，替换为双引号
	result = strings.ReplaceAll(result, "`", `"`)
	return result
}

// stripOrderByKeyword 移除 ORDER BY 前缀，保留排序表达式。
func stripOrderByKeyword(orderBy string) string {
	trimmed := strings.TrimSpace(orderBy)
	lower := strings.ToLower(trimmed)
	if strings.HasPrefix(lower, "order by ") {
		return strings.TrimSpace(trimmed[len("order by "):])
	}
	return trimmed
}

// normalizeIntervalUnit 规范化 interval 单位，确保输出可被 PostgreSQL 解析。
func normalizeIntervalUnit(unit string) string {
	upper := strings.ToUpper(strings.TrimSpace(unit))
	switch upper {
	case "MICROSECOND", "MICROSECONDS":
		return "microsecond"
	case "SECOND", "SECONDS":
		return "second"
	case "MINUTE", "MINUTES":
		return "minute"
	case "HOUR", "HOURS":
		return "hour"
	case "DAY", "DAYS":
		return "day"
	case "WEEK", "WEEKS":
		return "week"
	case "MONTH", "MONTHS":
		return "month"
	case "YEAR", "YEARS":
		return "year"
	default:
		// 兜底处理，避免注入特殊字符导致 SQL 非法
		cleaned := strings.ToLower(strings.TrimSpace(unit))
		cleaned = strings.ReplaceAll(cleaned, "_", " ")
		return cleaned
	}
}

// replaceJSONQuoteExpressions 将 JSON_QUOTE(expr) 转换为 to_jsonb(expr)::text。
func replaceJSONQuoteExpressions(s string) string {
	return reJSONQuote.ReplaceAllString(s, "to_jsonb($1)::text")
}

// replaceJSONSetCompatible 将 JSON_SET 转换为 PostgreSQL 可执行 JSONB_SET 调用。
func replaceJSONSetCompatible(s string) string {
	return rewriteFunctionCalls(s, "json_set", func(args []string) (string, bool) {
		return buildJSONSetExpression(args, true)
	})
}

// replaceJSONInsertCompatible 将 JSON_INSERT 转换为 PostgreSQL 可执行 JSONB_SET 调用。
func replaceJSONInsertCompatible(s string) string {
	return rewriteFunctionCalls(s, "json_insert", func(args []string) (string, bool) {
		return buildJSONSetExpression(args, true)
	})
}

// replaceJSONReplaceCompatible 将 JSON_REPLACE 转换为 PostgreSQL 可执行 JSONB_SET 调用。
func replaceJSONReplaceCompatible(s string) string {
	return rewriteFunctionCalls(s, "json_replace", func(args []string) (string, bool) {
		return buildJSONSetExpression(args, false)
	})
}

// replaceJSONRemoveCompatible 将 JSON_REMOVE 转换为 PostgreSQL 可执行 #- 链式删除表达式。
func replaceJSONRemoveCompatible(s string) string {
	return rewriteFunctionCalls(s, "json_remove", func(args []string) (string, bool) {
		if len(args) < 2 {
			return "", false
		}
		result := fmt.Sprintf("(%s)::jsonb", strings.TrimSpace(args[0]))
		for i := 1; i < len(args); i++ {
			path, ok := mysqlJSONPathArgToPGTextArray(strings.TrimSpace(args[i]))
			if !ok {
				return "", false
			}
			result = fmt.Sprintf("(%s #- %s)", result, path)
		}
		return result, true
	})
}

// replaceJSONArrayAppendCompatible 将 JSON_ARRAY_APPEND 转为可执行 JSONB 数组拼接。
func replaceJSONArrayAppendCompatible(s string) string {
	return rewriteFunctionCalls(s, "json_array_append", func(args []string) (string, bool) {
		if len(args) < 3 || len(args)%2 == 0 {
			return "", false
		}
		result := fmt.Sprintf("(%s)::jsonb", strings.TrimSpace(args[0]))
		for i := 2; i < len(args); i += 2 {
			value := strings.TrimSpace(args[i])
			result = fmt.Sprintf("(%s || jsonb_build_array(%s))", result, value)
		}
		return result, true
	})
}

// replaceJSONArrayInsertCompatible 将 JSON_ARRAY_INSERT 转为可执行 JSONB_INSERT 调用。
func replaceJSONArrayInsertCompatible(s string) string {
	return rewriteFunctionCalls(s, "json_array_insert", func(args []string) (string, bool) {
		if len(args) < 3 || len(args)%2 == 0 {
			return "", false
		}
		result := fmt.Sprintf("(%s)::jsonb", strings.TrimSpace(args[0]))
		for i := 1; i+1 < len(args); i += 2 {
			path, ok := mysqlJSONPathArgToPGTextArray(strings.TrimSpace(args[i]))
			if !ok {
				return "", false
			}
			value := strings.TrimSpace(args[i+1])
			result = fmt.Sprintf("jsonb_insert(%s, %s, to_jsonb(%s), true)", result, path, value)
		}
		return result, true
	})
}

// replaceJSONMergeCompatible 将 JSON_MERGE 转换为 JSONB 连接表达式。
func replaceJSONMergeCompatible(s string) string {
	return rewriteFunctionCalls(s, "json_merge", buildJSONMergeExpression)
}

// replaceJSONMergePatchCompatible 将 JSON_MERGE_PATCH 转换为 JSONB 连接表达式。
func replaceJSONMergePatchCompatible(s string) string {
	return rewriteFunctionCalls(s, "json_merge_patch", buildJSONMergeExpression)
}

// replaceJSONMergePreserveCompatible 将 JSON_MERGE_PRESERVE 转换为 JSONB 连接表达式。
func replaceJSONMergePreserveCompatible(s string) string {
	return rewriteFunctionCalls(s, "json_merge_preserve", buildJSONMergeExpression)
}

// buildJSONSetExpression 构建 JSON_SET/INSERT/REPLACE 对应的嵌套 JSONB_SET 表达式。
func buildJSONSetExpression(args []string, createIfMissing bool) (string, bool) {
	if len(args) < 3 || len(args)%2 == 0 {
		return "", false
	}
	result := fmt.Sprintf("(%s)::jsonb", strings.TrimSpace(args[0]))
	for i := 1; i+1 < len(args); i += 2 {
		path, ok := mysqlJSONPathArgToPGTextArray(strings.TrimSpace(args[i]))
		if !ok {
			return "", false
		}
		value := strings.TrimSpace(args[i+1])
		result = fmt.Sprintf("jsonb_set(%s, %s, to_jsonb(%s), %t)", result, path, value, createIfMissing)
	}
	return result, true
}

// buildJSONMergeExpression 构建 JSON_MERGE* 的 JSONB 连接表达式。
func buildJSONMergeExpression(args []string) (string, bool) {
	if len(args) < 2 {
		return "", false
	}
	result := fmt.Sprintf("(%s)::jsonb", strings.TrimSpace(args[0]))
	for i := 1; i < len(args); i++ {
		result = fmt.Sprintf("(%s || (%s)::jsonb)", result, strings.TrimSpace(args[i]))
	}
	return result, true
}

// rewriteFunctionCalls 按函数名重写调用，支持嵌套参数和引号场景。
func rewriteFunctionCalls(s string, functionName string, rewriter func(args []string) (string, bool)) string {
	out := s
	idx := 0
	for {
		pos, openParen := findFunctionCall(out, functionName, idx)
		if pos == -1 {
			break
		}
		endParen, ok := findMatchingParenInViewExpr(out, openParen)
		if !ok {
			idx = openParen + 1
			continue
		}
		args := splitTopLevelCommas(out[openParen+1 : endParen])
		replacement, rewritten := rewriter(args)
		if !rewritten {
			idx = endParen + 1
			continue
		}
		out = out[:pos] + replacement + out[endParen+1:]
		idx = pos + len(replacement)
	}
	return out
}

// findFunctionCall 查找下一个函数调用起点和左括号位置。
func findFunctionCall(s string, functionName string, start int) (int, int) {
	target := strings.ToLower(functionName)
	if target == "" {
		return -1, -1
	}
	lower := strings.ToLower(s)
	for i := start; i < len(s); i++ {
		if !strings.HasPrefix(lower[i:], target) {
			continue
		}
		if i > 0 && isIdentChar(s[i-1]) {
			continue
		}
		j := i + len(target)
		if j < len(s) && isIdentChar(s[j]) {
			continue
		}
		for j < len(s) && (s[j] == ' ' || s[j] == '\t' || s[j] == '\n' || s[j] == '\r') {
			j++
		}
		if j < len(s) && s[j] == '(' {
			return i, j
		}
	}
	return -1, -1
}

// mysqlJSONPathArgToPGTextArray 将 MySQL JSON 路径参数转换为 PostgreSQL text[] 字面量。
func mysqlJSONPathArgToPGTextArray(pathArg string) (string, bool) {
	trimmed := strings.TrimSpace(pathArg)
	if len(trimmed) < 2 {
		return "", false
	}
	quote := trimmed[0]
	if (quote != '\'' && quote != '"') || trimmed[len(trimmed)-1] != quote {
		return "", false
	}
	path := trimmed[1 : len(trimmed)-1]
	return mysqlJSONPathToPGTextArray(path)
}

// mysqlJSONPathToPGTextArray 将 MySQL JSON 路径字符串转换为 PostgreSQL text[] 字面量。
func mysqlJSONPathToPGTextArray(path string) (string, bool) {
	trimmed := strings.TrimSpace(path)
	if trimmed == "" {
		return "", false
	}
	if trimmed == "$" {
		return "'{}'", true
	}
	trimmed = strings.TrimPrefix(trimmed, "$")
	trimmed = strings.TrimPrefix(trimmed, ".")

	var segments []string
	var token strings.Builder
	for i := 0; i < len(trimmed); i++ {
		ch := trimmed[i]
		switch ch {
		case '.':
			if token.Len() > 0 {
				segments = append(segments, token.String())
				token.Reset()
			}
		case '[':
			if token.Len() > 0 {
				segments = append(segments, token.String())
				token.Reset()
			}
			closeIdx := strings.IndexByte(trimmed[i+1:], ']')
			if closeIdx < 0 {
				return "", false
			}
			seg := strings.TrimSpace(trimmed[i+1 : i+1+closeIdx])
			if seg == "" {
				return "", false
			}
			segments = append(segments, strings.Trim(seg, `"'`))
			i = i + closeIdx + 1
		default:
			token.WriteByte(ch)
		}
	}
	if token.Len() > 0 {
		segments = append(segments, token.String())
	}
	if len(segments) == 0 {
		return "'{}'", true
	}
	for i := range segments {
		seg := strings.TrimSpace(strings.Trim(segments[i], `"'`))
		seg = strings.ReplaceAll(seg, `"`, `\"`)
		seg = strings.ReplaceAll(seg, `'`, `''`)
		segments[i] = seg
	}
	return fmt.Sprintf("'{%s}'", strings.Join(segments, ",")), true
}

func buildJSONPathExpr(base string, rawPath string, textResult bool) string {
	path := strings.TrimSpace(rawPath)
	if len(path) >= 2 && ((path[0] == '\'' && path[len(path)-1] == '\'') || (path[0] == '"' && path[len(path)-1] == '"')) {
		path = path[1 : len(path)-1]
	}
	if !strings.HasPrefix(path, "$.") {
		if textResult {
			return fmt.Sprintf("%s ->> '%s'", base, strings.Trim(path, `"'`))
		}
		return fmt.Sprintf("%s -> '%s'", base, strings.Trim(path, `"'`))
	}
	segments := strings.Split(path[2:], ".")
	cleanSegments := make([]string, 0, len(segments))
	for _, seg := range segments {
		seg = strings.TrimSpace(seg)
		seg = strings.Trim(seg, `"'`)
		if seg != "" {
			cleanSegments = append(cleanSegments, seg)
		}
	}
	if len(cleanSegments) == 0 {
		if textResult {
			return fmt.Sprintf("%s::text", base)
		}
		return base
	}
	if len(cleanSegments) == 1 {
		if textResult {
			return fmt.Sprintf("%s ->> '%s'", base, cleanSegments[0])
		}
		return fmt.Sprintf("%s -> '%s'", base, cleanSegments[0])
	}
	if textResult {
		return fmt.Sprintf("%s #>> '{%s}'", base, strings.Join(cleanSegments, ","))
	}
	return fmt.Sprintf("%s #> '{%s}'", base, strings.Join(cleanSegments, ","))
}

// replaceToDaysExpressions 将 to_days(expr) 转成 floor(extract(epoch from (expr)::timestamp) / 86400)
func replaceToDaysExpressions(s string) string {
	out := s
	idx := 0
	for {
		pos := -1
		for i := idx; i <= len(out)-8; i++ {
			if strings.ToLower(out[i:i+8]) == "to_days(" {
				pos = i
				break
			}
		}
		if pos == -1 {
			break
		}

		openParen := pos + 7
		depth := 1
		end := openParen + 1
		for i := openParen + 1; i < len(out); i++ {
			switch out[i] {
			case '(':
				depth++
			case ')':
				depth--
				if depth == 0 {
					end = i
					i = len(out)
				}
			}
		}

		if depth > 0 {
			idx = pos + 8
			continue
		}

		expr := strings.TrimSpace(out[openParen+1 : end])
		replacement := fmt.Sprintf("(floor(extract(epoch from (%s)::timestamp) / 86400))", expr)
		out = out[:pos] + replacement + out[end+1:]
		idx = pos + len(replacement)
	}
	return out
}

// replaceRegexpLikeExpressions 将 REGEXP_LIKE(expr, pattern) 转成 expr ~ pattern
// MySQL 8.0+ 的 REGEXP_LIKE 函数在 PostgreSQL 中对应 ~ 操作符（区分大小写）
func replaceRegexpLikeExpressions(s string) string {
	return reRegexpLike.ReplaceAllStringFunc(s, func(match string) string {
		submatch := reRegexpLike.FindStringSubmatch(match)
		if len(submatch) < 3 {
			return match
		}
		expr := strings.TrimSpace(submatch[1])
		pattern := strings.TrimSpace(submatch[2])
		return fmt.Sprintf("%s ~ %s", expr, pattern)
	})
}

// replaceLocateExpressions 将 LOCATE(substr, str) 转成 STRPOS(str, substr)
// MySQL LOCATE(substr, str) 返回 substr 在 str 中首次出现的位置（从 1 开始）
// PostgreSQL STRPOS(str, substr) 功能相同
func replaceLocateExpressions(s string) string {
	return reLocate.ReplaceAllStringFunc(s, func(match string) string {
		submatch := reLocate.FindStringSubmatch(match)
		if len(submatch) < 3 {
			return match
		}
		substr := strings.TrimSpace(submatch[1])
		str := strings.TrimSpace(submatch[2])
		return fmt.Sprintf("STRPOS(%s, %s)", str, substr)
	})
}

// replaceJsonAggExpressions 将 JSON_ARRAYAGG(expr) 转成 JSON_AGG(expr)
// MySQL 8.0+ 的 JSON_ARRAYAGG 在 PostgreSQL 中对应 JSON_AGG
func replaceJsonAggExpressions(s string) string {
	return reJsonArrayagg.ReplaceAllStringFunc(s, func(match string) string {
		submatch := reJsonArrayagg.FindStringSubmatch(match)
		if len(submatch) < 2 {
			return match
		}
		expr := strings.TrimSpace(submatch[1])
		return fmt.Sprintf("JSON_AGG(%s)", expr)
	})
}

// replaceJsonObjectAggExpressions 将 JSON_OBJECTAGG(key, value) 转成 JSON_OBJECT_AGG(key, value)
// MySQL 8.0+ 的 JSON_OBJECTAGG 在 PostgreSQL 中对应 JSON_OBJECT_AGG
func replaceJsonObjectAggExpressions(s string) string {
	return reJsonObjectagg.ReplaceAllStringFunc(s, func(match string) string {
		submatch := reJsonObjectagg.FindStringSubmatch(match)
		if len(submatch) < 3 {
			return match
		}
		key := strings.TrimSpace(submatch[1])
		value := strings.TrimSpace(submatch[2])
		return fmt.Sprintf("JSON_OBJECT_AGG(%s, %s)", key, value)
	})
}

// replaceJSONInsertView 将 JSON_INSERT(doc, path, val) 转换为 JSONB_SET(doc::jsonb, path, to_jsonb(val), true)
// MySQL JSON_INSERT: 只在路径不存在时插入
// PostgreSQL JSONB_SET: 第四个参数为 true 时表示不存在则创建
// 注意：需要将 json 类型显式转换为 jsonb，值需要用 to_jsonb() 包裹
// PostgreSQL 路径格式：'{key}' 或 '{key,nested}'（数组格式）
func replaceJSONInsertView(s string) string {
	return reJSONInsertView.ReplaceAllStringFunc(s, func(match string) string {
		submatch := reJSONInsertView.FindStringSubmatch(match)
		if len(submatch) < 4 {
			return match
		}
		doc := strings.TrimSpace(submatch[1])
		path := strings.TrimSpace(submatch[2])
		val := strings.TrimSpace(submatch[3])
		// PostgreSQL 路径格式：'{key}' 或 '{key,nested}'（数组格式）
		// 值需要用 to_jsonb() 包裹
		pgPath := fmt.Sprintf("'{%s}'", strings.TrimPrefix(path, "$."))
		return fmt.Sprintf("JSONB_SET(%s::jsonb, %s, to_jsonb(%s), true)", doc, pgPath, val)
	})
}

// replaceJSONReplaceView 将 JSON_REPLACE(doc, path, val) 转换为 JSONB_SET(doc::jsonb, path, to_jsonb(val), false)
// MySQL JSON_REPLACE: 只在路径存在时替换
// PostgreSQL JSONB_SET: 第四个参数为 false 时表示仅当存在时替换
// 注意：需要将 json 类型显式转换为 jsonb，值需要用 to_jsonb() 包裹
// PostgreSQL 路径格式：'{key}' 或 '{key,nested}'（数组格式）
func replaceJSONReplaceView(s string) string {
	return reJSONReplaceView.ReplaceAllStringFunc(s, func(match string) string {
		submatch := reJSONReplaceView.FindStringSubmatch(match)
		if len(submatch) < 4 {
			return match
		}
		doc := strings.TrimSpace(submatch[1])
		path := strings.TrimSpace(submatch[2])
		val := strings.TrimSpace(submatch[3])
		// PostgreSQL 路径格式：'{key}' 或 '{key,nested}'（数组格式）
		// 值需要用 to_jsonb() 包裹
		pgPath := fmt.Sprintf("'{%s}'", strings.TrimPrefix(path, "$."))
		return fmt.Sprintf("JSONB_SET(%s::jsonb, %s, to_jsonb(%s), false)", doc, pgPath, val)
	})
}

// replaceJSONSetView 将 JSON_SET(doc, path1, val1, path2, val2, ...) 转换为嵌套的 JSONB_SET
// MySQL JSON_SET: 替换或插入（默认行为），支持多个路径 - 值对
// PostgreSQL JSONB_SET: 默认替换或插入，只支持单个路径 - 值对，需要嵌套调用
// 注意：需要将 json 类型显式转换为 jsonb，值需要用 to_jsonb() 包裹
// PostgreSQL 路径格式：'{key}' 或 '{key,nested}'（数组格式）
func replaceJSONSetView(s string) string {
	return reJSONSetView.ReplaceAllStringFunc(s, func(match string) string {
		submatch := reJSONSetView.FindStringSubmatch(match)
		if len(submatch) < 3 {
			return match
		}
		doc := strings.TrimSpace(submatch[1])
		argsStr := strings.TrimSpace(submatch[2])

		// 解析多个路径 - 值对：'path1', val1, 'path2', val2, ...
		// 路径总是以引号开始，所以可以用引号来分割
		var paths []string
		var vals []string

		// 使用正则表达式提取所有的 'path' 和对应的值
		rePathVal := regexp.MustCompile(`'([^']+?)'\s*,\s*([^,]+?)(?:\s*,\s*'|$)`)
		matches := rePathVal.FindAllStringSubmatch(argsStr, -1)

		for _, m := range matches {
			if len(m) >= 3 {
				paths = append(paths, strings.TrimSpace(m[1]))
				vals = append(vals, strings.TrimSpace(m[2]))
			}
		}

		if len(paths) == 0 || len(paths) != len(vals) {
			return match
		}

		// 构建嵌套的 JSONB_SET 调用
		result := fmt.Sprintf("%s::jsonb", doc)
		for i := range paths {
			path := paths[i]
			val := vals[i]
			// 移除 $. 前缀并转换为 PostgreSQL 数组格式
			path = strings.TrimPrefix(path, "$.")
			pgPath := fmt.Sprintf("'{%s}'", path)
			result = fmt.Sprintf("JSONB_SET(%s, %s, to_jsonb(%s))", result, pgPath, val)
		}

		return result
	})
}

// replaceJSONRemoveView 将 JSON_REMOVE(doc, path) 转换为 doc - path 或 JSONB_DELETE_PATH(doc, path)
// MySQL JSON_REMOVE: 从 JSON 文档中删除指定路径的数据
// PostgreSQL: 使用 - 操作符或 JSONB_DELETE_PATH 函数
func replaceJSONRemoveView(s string) string {
	return reJSONRemoveView.ReplaceAllStringFunc(s, func(match string) string {
		submatch := reJSONRemoveView.FindStringSubmatch(match)
		if len(submatch) < 3 {
			return match
		}
		doc := strings.TrimSpace(submatch[1])
		path := strings.TrimSpace(submatch[2])
		// 移除 $.前缀，只保留键名
		key := strings.TrimPrefix(path, "$.")
		// 使用 - 操作符删除键
		return fmt.Sprintf("%s - '%s'", doc, key)
	})
}

// replaceJSONMergePatchView 将 JSON_MERGE_PATCH(doc1, doc2) 转换为 (doc1::jsonb || doc2::jsonb)
// MySQL JSON_MERGE_PATCH: JSON 文档的 RFC 7396 合并
// PostgreSQL: 使用 || 操作符进行 JSONB 连接
// 注意：需要将 json 类型显式转换为 jsonb
func replaceJSONMergePatchView(s string) string {
	return reJSONMergePatchView.ReplaceAllStringFunc(s, func(match string) string {
		submatch := reJSONMergePatchView.FindStringSubmatch(match)
		if len(submatch) < 3 {
			return match
		}
		doc1 := strings.TrimSpace(submatch[1])
		doc2 := strings.TrimSpace(submatch[2])
		return fmt.Sprintf("(%s::jsonb || %s::jsonb)", doc1, doc2)
	})
}

// replaceJSONKeysView 将 JSON_KEYS(doc) 转换为 JSONB_OBJECT_KEYS(doc)
// MySQL JSON_KEYS: 返回 JSON 对象的键名数组
// PostgreSQL JSONB_OBJECT_KEYS: 返回键名集合（需要配合 ARRAY 使用）
// 注意：需要将 json 类型显式转换为 jsonb
func replaceJSONKeysView(s string) string {
	return reJSONKeysView.ReplaceAllStringFunc(s, func(match string) string {
		submatch := reJSONKeysView.FindStringSubmatch(match)
		if len(submatch) < 2 {
			return match
		}
		doc := strings.TrimSpace(submatch[1])
		return fmt.Sprintf("ARRAY(SELECT * FROM JSONB_OBJECT_KEYS(%s::jsonb))", doc)
	})
}

// replaceJSONLengthView 将 JSON_LENGTH(doc) 转换为 JSONB_ARRAY_LENGTH(doc) 或 JSONB_EACH_TEXT(doc)
// MySQL JSON_LENGTH: 返回 JSON 数组的长度或对象键数
// PostgreSQL: 对于数组使用 JSONB_ARRAY_LENGTH，对于对象使用 JSONB_EACH_TEXT
// 这里简化处理，假设为数组
// 注意：需要将 json 类型显式转换为 jsonb
func replaceJSONLengthView(s string) string {
	return reJSONLengthView.ReplaceAllStringFunc(s, func(match string) string {
		submatch := reJSONLengthView.FindStringSubmatch(match)
		if len(submatch) < 2 {
			return match
		}
		doc := strings.TrimSpace(submatch[1])
		return fmt.Sprintf("JSONB_ARRAY_LENGTH(%s::jsonb)", doc)
	})
}

// replaceJSONArrayInsert 将 JSON_ARRAY_INSERT(doc, path, val) 转换为 JSONB_INSERT
// MySQL 9.0+: JSON_ARRAY_INSERT(doc, '$[0]', val)
// PostgreSQL: JSONB_INSERT(doc::jsonb, '{0}', val::jsonb)
func replaceJSONArrayInsert(s string) string {
	return reJSONArrayInsert.ReplaceAllStringFunc(s, func(match string) string {
		submatch := reJSONArrayInsert.FindStringSubmatch(match)
		if len(submatch) < 4 {
			return match
		}
		doc := strings.TrimSpace(submatch[1])
		path := strings.TrimSpace(submatch[2])
		val := strings.TrimSpace(submatch[3])

		// 转换 MySQL JSON 路径 '$[0]' 为 PostgreSQL 路径 '{0}'
		pgPath := mysqlJsonPathToPostgresPath(path)

		return fmt.Sprintf("JSONB_INSERT(%s::jsonb, '%s', %s::jsonb)", doc, pgPath, val)
	})
}

// mysqlJsonPathToPostgresPath 将 MySQL JSON 路径转换为 PostgreSQL 路径
// '$[0]' -> '{0}'
// '$.array[0]' -> '{array,0}'
func mysqlJsonPathToPostgresPath(path string) string {
	// 移除前缀 $
	p := strings.TrimPrefix(path, "$")
	// 替换 . 为 ,
	p = strings.ReplaceAll(p, ".", ",")
	// 替换 [ 为 ,
	p = strings.ReplaceAll(p, "[", ",")
	// 替换 ] 为空
	p = strings.ReplaceAll(p, "]", "")
	// 包裹在 {} 中
	return "{" + p + "}"
}

// replaceInstrExpressions 将 INSTR(str, substr) 转换为 STRPOS(str, substr)
// MySQL INSTR: 返回子串首次出现的位置（从 1 开始）
// PostgreSQL STRPOS: 功能相同
func replaceInstrExpressions(s string) string {
	return reInstr.ReplaceAllStringFunc(s, func(match string) string {
		submatch := reInstr.FindStringSubmatch(match)
		if len(submatch) < 3 {
			return match
		}
		str := strings.TrimSpace(submatch[1])
		substr := strings.TrimSpace(submatch[2])
		return fmt.Sprintf("STRPOS(%s, %s)", str, substr)
	})
}

// replaceRLikeExpressions 将 RLIKE 转换为 ~ (PostgreSQL 正则匹配操作符)
// MySQL RLIKE: 正则表达式匹配
// PostgreSQL ~: 区分大小写的正则匹配
func replaceRLikeExpressions(s string) string {
	return reRLike.ReplaceAllString(s, "($1 ~ '$2')")
}

// replaceRegexpInstrFullExpressions 将 REGEXP_INSTR 完整版本 (6 参数) 转换为 PostgreSQL
// MySQL 8.0.17+/9.0+: REGEXP_INSTR(expr, pat[, pos[, occurrence[, return_option[, match_type]]]])
// PostgreSQL: 使用 regexp_matches 数组索引
func replaceRegexpInstrFullExpressions(s string) string {
	return reRegexpInstrFull.ReplaceAllStringFunc(s, func(match string) string {
		submatch := reRegexpInstrFull.FindStringSubmatch(match)
		if len(submatch) < 7 {
			return match
		}
		str := strings.TrimSpace(submatch[1])
		pattern := strings.TrimSpace(submatch[2])
		// pos := strings.TrimSpace(submatch[3])  // 起始位置，暂不处理
		occurrence := strings.TrimSpace(submatch[4]) // 第 N 个匹配
		// returnOpt := strings.TrimSpace(submatch[5])  // 0=返回开始位置，1=返回结束位置
		// matchType := strings.TrimSpace(submatch[6])  // 'c'=区分大小写，'i'=不区分

		// PostgreSQL 方案：(regexp_matches(str, pattern, 'g'))[occurrence]
		return fmt.Sprintf("(regexp_matches(%s, %s, 'g'))[%s]", str, pattern, occurrence)
	})
}

// replaceRegexpInstrExpressions 将 REGEXP_INSTR(str, pattern[, pos]) 转换为 STRPOS 或 regexp_instr
// MySQL REGEXP_INSTR: 返回正则表达式匹配的位置（从 1 开始）
// PostgreSQL: 使用 STRPOS + SUBSTRING 或自定义函数
// 注意：PostgreSQL 没有直接等价的函数，这里使用 CASE WHEN ~ THEN 1 ELSE 0 模拟
func replaceRegexpInstrExpressions(s string) string {
	return reRegexpInstr.ReplaceAllStringFunc(s, func(match string) string {
		submatch := reRegexpInstr.FindStringSubmatch(match)
		if len(submatch) < 3 {
			return match
		}
		str := strings.TrimSpace(submatch[1])
		pattern := strings.TrimSpace(submatch[2])

		// PostgreSQL 方案：使用正则匹配返回布尔值，转换为 1/0
		// pattern 已经包含引号，不需要额外添加
		return fmt.Sprintf("(CASE WHEN %s ~ %s THEN 1 ELSE 0 END)", str, pattern)
	})
}

// replaceRegexpSubstrFullExpressions 将 REGEXP_SUBSTR 完整版本 (4 参数) 转换为 PostgreSQL
// MySQL 8.0.17+/9.0+: REGEXP_SUBSTR(str, pattern[, pos[, occurrence]])
// PostgreSQL: 使用 regexp_matches 数组索引
func replaceRegexpSubstrFullExpressions(s string) string {
	return reRegexpSubstrFull.ReplaceAllStringFunc(s, func(match string) string {
		submatch := reRegexpSubstrFull.FindStringSubmatch(match)
		if len(submatch) < 5 {
			return match
		}
		str := strings.TrimSpace(submatch[1])
		pattern := strings.TrimSpace(submatch[2])
		// pos := strings.TrimSpace(submatch[3])  // 起始位置，暂不处理
		occurrence := strings.TrimSpace(submatch[4]) // 第 N 个匹配

		// PostgreSQL 方案：(regexp_matches(str, pattern, 'g'))[occurrence]
		return fmt.Sprintf("(regexp_matches(%s, %s, 'g'))[%s]", str, pattern, occurrence)
	})
}

// replaceRegexpSubstrExpressions 将 REGEXP_SUBSTR(str, pattern[, pos]) 转换为 SUBSTRING
// MySQL REGEXP_SUBSTR: 返回正则表达式匹配的子串
// PostgreSQL SUBSTRING: SUBSTRING(str FROM pattern)
func replaceRegexpSubstrExpressions(s string) string {
	return reRegexpSubstr.ReplaceAllStringFunc(s, func(match string) string {
		submatch := reRegexpSubstr.FindStringSubmatch(match)
		if len(submatch) < 3 {
			return match
		}
		str := strings.TrimSpace(submatch[1])
		pattern := strings.TrimSpace(submatch[2])

		// PostgreSQL: SUBSTRING(str FROM pattern)
		// pattern 已经包含引号，不需要额外添加
		return fmt.Sprintf("SUBSTRING(%s FROM %s)", str, pattern)
	})
}

// replaceRegexpReplaceExpressions 将 REGEXP_REPLACE(str, pattern, repl) 转换为 regexp_replace
// MySQL REGEXP_REPLACE: 使用正则表达式替换
// PostgreSQL regexp_replace: 语法相同，直接转换
func replaceRegexpReplaceExpressions(s string) string {
	return reRegexpReplace.ReplaceAllString(s, "regexp_replace($1, $2, $3)")
}

// replaceCastSignedExpressions 将 CAST(x AS SIGNED) 转换为 CAST(x AS INTEGER)
// MySQL SIGNED: 有符号整数
// PostgreSQL INTEGER: 32 位有符号整数
func replaceCastSignedExpressions(s string) string {
	return reCastSigned.ReplaceAllString(s, "CAST($1 AS INTEGER)")
}

// replaceCastCharExpressions 将 CAST(x AS CHAR) 转换为 CAST(x AS TEXT)
// MySQL CHAR: 字符类型
// PostgreSQL TEXT: 可变长度字符串
func replaceCastCharExpressions(s string) string {
	return reCastChar.ReplaceAllString(s, "CAST($1 AS TEXT)")
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
		start := pos + 6 // len("concat(")
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
