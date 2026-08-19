package postgres

import (
	"strings"
	"testing"
)

func TestConvertViewDDL_MapsJSONUnquoteAndExtract(t *testing.T) {
	viewSQL := `SELECT
JSON_EXTRACT(case_08_json.data, '$.name') AS json_name,
JSON_UNQUOTE(JSON_EXTRACT(case_08_json.data, '$.name')) AS json_name_unquoted
FROM case_08_json`

	ddl, err := ConvertViewDDL("v_json_map", viewSQL)
	if err != nil {
		t.Fatalf("ConvertViewDDL 返回错误：%v", err)
	}

	lowerDDL := strings.ToLower(ddl)
	if strings.Contains(lowerDDL, "jsonb_unquote(") {
		t.Fatalf("不应包含不存在的 jsonb_unquote 函数：%s", ddl)
	}
	if !strings.Contains(lowerDDL, "-> 'name'") {
		t.Fatalf("JSON_EXTRACT 未转换为 -> 'name': %s", ddl)
	}
	if !strings.Contains(lowerDDL, "->> 'name'") {
		t.Fatalf("JSON_UNQUOTE(JSON_EXTRACT(...)) 未转换为 ->> 'name': %s", ddl)
	}
}

func TestConvertViewDDL_MapsDatetimeExtractFunctions(t *testing.T) {
	viewSQL := `SELECT
YEAR(case_09_datetime.d1) AS year_only,
MONTH(case_09_datetime.d1) AS month_only,
DAYOFMONTH(case_09_datetime.d1) AS day_only,
HOUR(case_09_datetime.t1) AS hour_only,
MINUTE(case_09_datetime.t1) AS minute_only,
SECOND(case_09_datetime.t1) AS second_only,
DATE_FORMAT(case_09_datetime.d1, '%Y-%m-%d') AS fmt_date,
DATE_FORMAT(case_09_datetime.dt1, '%Y-%m-%d %H:%i:%s') AS fmt_datetime
FROM case_09_datetime`

	ddl, err := ConvertViewDDL("v_datetime_map", viewSQL)
	if err != nil {
		t.Fatalf("ConvertViewDDL 返回错误：%v", err)
	}

	lowerDDL := strings.ToLower(ddl)
	if strings.Contains(lowerDDL, "year(") || strings.Contains(lowerDDL, "month(") ||
		strings.Contains(lowerDDL, "dayofmonth(") || strings.Contains(lowerDDL, "hour(") ||
		strings.Contains(lowerDDL, "minute(") || strings.Contains(lowerDDL, "second(") {
		t.Fatalf("日期时间提取函数未完整转换：%s", ddl)
	}
	if !strings.Contains(lowerDDL, "extract(year from") ||
		!strings.Contains(lowerDDL, "extract(month from") ||
		!strings.Contains(lowerDDL, "extract(day from") ||
		!strings.Contains(lowerDDL, "extract(hour from") ||
		!strings.Contains(lowerDDL, "extract(minute from") ||
		!strings.Contains(lowerDDL, "extract(second from") {
		t.Fatalf("extract 映射不完整：%s", ddl)
	}
	if !strings.Contains(lowerDDL, "to_char(case_09_datetime.d1, 'yyyy-mm-dd')") {
		t.Fatalf("DATE_FORMAT 日期模板未转换：%s", ddl)
	}
	if !strings.Contains(lowerDDL, "to_char(case_09_datetime.dt1, 'yyyy-mm-dd hh24:mi:ss')") {
		t.Fatalf("DATE_FORMAT 日期时间模板未转换：%s", ddl)
	}
}

// TestConvertViewDDL_RegexpLike 测试 REGEXP_LIKE 函数转换 (MySQL 8.0+)
func TestConvertViewDDL_RegexpLike(t *testing.T) {
	viewSQL := `SELECT
    case_05_charsets.c1,
    case_05_charsets.c2,
    REGEXP_LIKE(case_05_charsets.c1, '^[a-zA-Z]+$') AS is_alpha_c1,
    REGEXP_LIKE(case_05_charsets.c2, '^[0-9]+$') AS is_numeric_c2,
    REGEXP_LIKE(c3, 'test') AS has_test
FROM case_05_charsets`

	ddl, err := ConvertViewDDL("view_case25_mysql8_regexp", viewSQL)
	if err != nil {
		t.Fatalf("ConvertViewDDL 返回错误：%v", err)
	}

	t.Logf("转换结果：%s", ddl)

	// 检查转换结果：操作符要转换且正则字面量应保持原语义
	if !strings.Contains(ddl, "~ '^[a-zA-Z]+$'") {
		t.Errorf("REGEXP_LIKE(c1, '^[a-zA-Z]+$') 未正确转换为 ~ 操作符：%s", ddl)
	}
	if !strings.Contains(ddl, "~ '^[0-9]+$'") {
		t.Errorf("REGEXP_LIKE(c2, '^[0-9]+$') 未正确转换为 ~ 操作符：%s", ddl)
	}
	if !strings.Contains(ddl, "~ 'test'") {
		t.Errorf("REGEXP_LIKE(c3, 'test') 未正确转换为 ~ 操作符：%s", ddl)
	}

	// 检查不再包含 REGEXP_LIKE 函数调用
	lowerDDL := strings.ToLower(ddl)
	if strings.Contains(lowerDDL, "regexp_like(") {
		t.Errorf("转换后仍包含 regexp_like 函数：%s", ddl)
	}
}

// TestConvertViewDDL_RegexpLikeWithQuotes 测试带引号的 REGEXP_LIKE 转换
func TestConvertViewDDL_RegexpLikeWithQuotes(t *testing.T) {
	viewSQL := `SELECT 
    REGEXP_LIKE(name, '^[A-Z][a-z]+') AS valid_name,
    REGEXP_LIKE(email, '^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$') AS valid_email
FROM users`

	ddl, err := ConvertViewDDL("v_users_regexp", viewSQL)
	if err != nil {
		t.Fatalf("ConvertViewDDL 返回错误：%v", err)
	}

	t.Logf("转换结果：%s", ddl)

	// 检查正则内容保持不变，避免大小写语义损坏
	if !strings.Contains(ddl, "name ~ '^[A-Z][a-z]+'") {
		t.Errorf("REGEXP_LIKE(name, ...) 转换失败：%s", ddl)
	}
	if !strings.Contains(ddl, "email ~ '^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+") {
		t.Errorf("REGEXP_LIKE(email, ...) 转换失败：%s", ddl)
	}
}

// TestConvertViewDDL_RegexpLikeWithColumnRef 测试列引用的 REGEXP_LIKE 转换
func TestConvertViewDDL_RegexpLikeWithColumnRef(t *testing.T) {
	viewSQL := `SELECT 
    REGEXP_LIKE(t1.c1, t2.pattern) AS matches
FROM table1 t1, table2 t2`

	ddl, err := ConvertViewDDL("v_cross_regexp", viewSQL)
	if err != nil {
		t.Fatalf("ConvertViewDDL 返回错误：%v", err)
	}

	if !strings.Contains(ddl, "t1.c1 ~ t2.pattern") {
		t.Errorf("REGEXP_LIKE(t1.c1, t2.pattern) 转换失败：%s", ddl)
	}
}

// TestConvertViewDDL_Locate 测试 LOCATE 函数转换
func TestConvertViewDDL_Locate(t *testing.T) {
	viewSQL := `SELECT
    LOCATE('test', case_05_charsets.c4) AS test_pos_c4,
    LOCATE('abc', name) AS pos_name,
    LOCATE(sub, str) AS pos_var
FROM case_05_charsets`

	ddl, err := ConvertViewDDL("view_case25_locate", viewSQL)
	if err != nil {
		t.Fatalf("ConvertViewDDL 返回错误：%v", err)
	}

	t.Logf("转换结果：%s", ddl)

	// 检查转换结果（LOCATE('test', c4) -> strpos(c4, 'test')）
	// SQL 会被转为小写
	if !strings.Contains(ddl, "strpos(case_05_charsets.c4, 'test')") {
		t.Errorf("LOCATE 未正确转换为 strpos：%s", ddl)
	}

	// 检查不再包含 LOCATE 函数调用
	lowerDDL := strings.ToLower(ddl)
	if strings.Contains(lowerDDL, "locate(") {
		t.Errorf("转换后仍包含 locate 函数：%s", ddl)
	}

	// 检查参数顺序正确（substr 和 str 位置交换）
	if !strings.Contains(ddl, "strpos(name, 'abc')") {
		t.Errorf("LOCATE 参数顺序错误，应该是 strpos(str, substr)：%s", ddl)
	}
}

// TestConvertViewDDL_JsonAgg 测试 JSON_ARRAYAGG 和 JSON_OBJECTAGG 函数转换
func TestConvertViewDDL_JsonAgg(t *testing.T) {
	viewSQL := `SELECT
    b.status AS status,
    JSON_ARRAYAGG(JSON_BUILD_OBJECT('tiny', i.col_tiny)) AS int_data,
    JSON_OBJECTAGG(b.status, JSON_BUILD_ARRAY(i.col_tiny, i.col_small)) AS status_map,
    JSON_ARRAYAGG(i.col_tiny) AS unique_tiny
FROM case_01_integers i
JOIN case_02_boolean b ON i.col_tiny = b.status
GROUP BY b.status`

	ddl, err := ConvertViewDDL("view_case27_mysql8_json_agg", viewSQL)
	if err != nil {
		t.Fatalf("ConvertViewDDL 返回错误：%v", err)
	}

	t.Logf("转换结果：%s", ddl)

	// SQL 会被转为小写，检查小写形式
	// 检查 JSON_ARRAYAGG 转换为 JSON_AGG
	if !strings.Contains(ddl, "json_agg(") {
		t.Errorf("JSON_ARRAYAGG 未转换为 json_agg：%s", ddl)
	}

	// 检查 JSON_OBJECTAGG 转换为 JSON_OBJECT_AGG
	if !strings.Contains(ddl, "json_object_agg(") {
		t.Errorf("JSON_OBJECTAGG 未转换为 json_object_agg：%s", ddl)
	}

	// 检查不再包含 MySQL 函数名
	lowerDDL := strings.ToLower(ddl)
	if strings.Contains(lowerDDL, "json_arrayagg(") {
		t.Errorf("转换后仍包含 json_arrayagg 函数：%s", ddl)
	}
	if strings.Contains(lowerDDL, "json_objectagg(") {
		t.Errorf("转换后仍包含 json_objectagg 函数：%s", ddl)
	}
}

// TestConvertViewDDL_JSONModifyFunctions 测试 JSON 修改函数转换
func TestConvertViewDDL_JSONModifyFunctions(t *testing.T) {
	viewSQL := `SELECT
    JSON_INSERT(data, '$.new_key', 'new_value') AS json_inserted,
    JSON_REPLACE(data, '$.id', 999) AS json_replaced,
    JSON_SET(data, '$.id', 123) AS json_set,
    JSON_REMOVE(data, '$.old_key') AS json_removed,
    JSON_MERGE_PATCH(data, '{"status": "active"}') AS json_merged
FROM case_08_json`

	ddl, err := ConvertViewDDL("view_case39_mysql8_json_modify", viewSQL)
	if err != nil {
		t.Fatalf("ConvertViewDDL 返回错误：%v", err)
	}

	t.Logf("转换结果：%s", ddl)

	// 检查 JSON_INSERT/REPLACE/SET 转换（SQL 会被转为小写）
	if !strings.Contains(ddl, "jsonb_set(") {
		t.Errorf("JSON_INSERT/REPLACE/SET 未转换为 jsonb_set：%s", ddl)
	}
	if !strings.Contains(ddl, "jsonb_set((data)::jsonb, '{id}', to_jsonb(123), true)") {
		t.Errorf("JSON_SET 未转换为合法的 jsonb_set 签名：%s", ddl)
	}
	if strings.Contains(ddl, "jsonb_set(data, '$.id', 123)") {
		t.Errorf("JSON_SET 仍为不兼容语法：%s", ddl)
	}
	// 检查 JSON_REMOVE 转换
	if !strings.Contains(ddl, " - 'old_key'") {
		t.Errorf("JSON_REMOVE 未正确转换：%s", ddl)
	}
	// 检查 JSON_MERGE_PATCH 转换
	if !strings.Contains(ddl, "||") {
		t.Errorf("JSON_MERGE_PATCH 未转换为 || 操作符：%s", ddl)
	}
}

// TestConvertViewDDL_JSONKeysLength 测试 JSON_KEYS 和 JSON_LENGTH 转换
func TestConvertViewDDL_JSONKeysLength(t *testing.T) {
	viewSQL := `SELECT
    JSON_KEYS(data) AS json_keys,
    JSON_LENGTH(data) AS json_length
FROM case_08_json`

	ddl, err := ConvertViewDDL("view_case17_advanced_json", viewSQL)
	if err != nil {
		t.Fatalf("ConvertViewDDL 返回错误：%v", err)
	}

	t.Logf("转换结果：%s", ddl)

	// 检查 JSON_KEYS 转换（SQL 会被转为小写）
	if !strings.Contains(ddl, "jsonb_object_keys(") {
		t.Errorf("JSON_KEYS 未转换为 JSONB_OBJECT_KEYS：%s", ddl)
	}
	// 检查 JSON_LENGTH 转换
	if !strings.Contains(ddl, "jsonb_array_length(") {
		t.Errorf("JSON_LENGTH 未转换为 JSONB_ARRAY_LENGTH：%s", ddl)
	}
}

// TestConvertViewDDL_InstrRLike 测试 INSTR 和 RLIKE 转换
func TestConvertViewDDL_InstrRLike(t *testing.T) {
	viewSQL := `SELECT
    INSTR(c4, 'test') AS test_pos_c4,
    (c1 RLIKE '^[A-Za-z]+$') AS is_alpha_c1,
    (c2 RLIKE '^[0-9]+$') AS is_numeric_c2
FROM case_05_charsets`

	ddl, err := ConvertViewDDL("view_case25_mysql8_regexp", viewSQL)
	if err != nil {
		t.Fatalf("ConvertViewDDL 返回错误：%v", err)
	}

	t.Logf("转换结果：%s", ddl)

	// 检查 INSTR 转换
	if !strings.Contains(ddl, "strpos(") {
		t.Errorf("INSTR 未转换为 STRPOS：%s", ddl)
	}
	// 检查 RLIKE 转换
	if !strings.Contains(ddl, " ~ '") {
		t.Errorf("RLIKE 未转换为 ~ 操作符：%s", ddl)
	}
	// 检查正则字面量内容保持不变，避免大小写语义被破坏
	if !strings.Contains(ddl, "'^[A-Za-z]+$'") {
		t.Errorf("RLIKE 正则字面量被错误改写：%s", ddl)
	}
}

// TestConvertViewDDL_CastTypes 测试 CAST 类型转换
func TestConvertViewDDL_CastTypes(t *testing.T) {
	viewSQL := `SELECT
    CAST(col_float AS SIGNED) AS float_as_int,
    CAST(col_tiny AS CHAR) AS tiny_as_string,
    CAST(col_medium AS CHAR(10)) AS medium_as_string
FROM case_03_floats`

	ddl, err := ConvertViewDDL("view_cast_types", viewSQL)
	if err != nil {
		t.Fatalf("ConvertViewDDL 返回错误：%v", err)
	}

	t.Logf("转换结果：%s", ddl)

	// 检查 CAST(x AS SIGNED) 转换
	if !strings.Contains(ddl, "as integer") {
		t.Errorf("CAST(x AS SIGNED) 未转换为 CAST(x AS INTEGER)：%s", ddl)
	}
	// 检查 CAST(x AS CHAR) 转换
	if !strings.Contains(ddl, "as text") {
		t.Errorf("CAST(x AS CHAR) 未转换为 CAST(x AS TEXT)：%s", ddl)
	}
}

// TestConvertViewDDL_CastUsingInConcat 测试 CAST(x USING charset) 在 CONCAT 中的转换
func TestConvertViewDDL_CastUsingInConcat(t *testing.T) {
	viewSQL := `SELECT
    CONCAT(CAST(case_05_charsets.c1 USING utf8mb4), ' ', case_05_charsets.c2) AS concatenated
FROM case_05_charsets`

	ddl, err := ConvertViewDDL("view_cast_using_concat", viewSQL)
	if err != nil {
		t.Fatalf("ConvertViewDDL 返回错误：%v", err)
	}

	t.Logf("转换结果：%s", ddl)

	lowerDDL := strings.ToLower(ddl)
	if strings.Contains(lowerDDL, " using ") {
		t.Errorf("CAST(... USING ...) 未被移除：%s", ddl)
	}
	if strings.Contains(lowerDDL, " as ' '") {
		t.Errorf("CAST 误匹配导致别名被破坏：%s", ddl)
	}
	if !strings.Contains(lowerDDL, "as concatenated") {
		t.Errorf("列别名 concatenated 丢失：%s", ddl)
	}
}

// TestConvertViewDDL_CastUsingInQuotedConcat 测试带双引号标识符的 CAST(x USING charset) 场景
func TestConvertViewDDL_CastUsingInQuotedConcat(t *testing.T) {
	viewSQL := `select "case_05_charsets"."c1" as "utf8_col",
"case_05_charsets"."c2" as "utf8mb4_col",
concat(cast("case_05_charsets"."c1" using utf8mb4), ' ',"case_05_charsets"."c2") as "concatenated"
from "case_05_charsets"`

	ddl, err := ConvertViewDDL("view_case19_advanced_string", viewSQL)
	if err != nil {
		t.Fatalf("ConvertViewDDL 返回错误：%v", err)
	}

	t.Logf("转换结果：%s", ddl)
	lowerDDL := strings.ToLower(ddl)
	if strings.Contains(lowerDDL, " using ") {
		t.Errorf("仍包含 using 语法：%s", ddl)
	}
	if strings.Contains(lowerDDL, "as ' '") {
		t.Errorf("出现错误的 as ' ' 片段：%s", ddl)
	}
}

// TestConvertViewDDL_ForceIndex 测试 FORCE INDEX 移除
func TestConvertViewDDL_ForceIndex(t *testing.T) {
	viewSQL := `SELECT COUNT(i.col_tiny) AS total_rows
FROM case_01_integers i FORCE INDEX (PRIMARY)
LEFT JOIN case_02_boolean b ON i.col_tiny = b.id`

	ddl, err := ConvertViewDDL("view_case42_compat_optimizer_hint", viewSQL)
	if err != nil {
		t.Fatalf("ConvertViewDDL 返回错误：%v", err)
	}

	t.Logf("转换结果：%s", ddl)

	// 检查 FORCE INDEX 已被移除
	lowerDDL := strings.ToLower(ddl)
	if strings.Contains(lowerDDL, "force index") {
		t.Errorf("FORCE INDEX 未被移除：%s", ddl)
	}
}

// TestConvertViewDDL_JSONObjectArray 测试 JSON_OBJECT 和 JSON_ARRAY 转换
func TestConvertViewDDL_JSONObjectArray(t *testing.T) {
	viewSQL := `SELECT
		JSON_OBJECT('tiny', col_tiny, 'small', col_small) AS json_data,
		JSON_ARRAY(col_tiny, col_small) AS json_array
	FROM test_table`

	ddl, err := ConvertViewDDL("test_json", viewSQL)
	if err != nil {
		t.Fatalf("ConvertViewDDL 返回错误：%v", err)
	}

	t.Logf("转换结果：%s", ddl)

	// 检查 JSON_OBJECT 转换为 json_build_object
	if !strings.Contains(ddl, "json_build_object(") {
		t.Errorf("JSON_OBJECT 未转换为 json_build_object：%s", ddl)
	}
	// 检查 JSON_ARRAY 转换为 json_build_array
	if !strings.Contains(ddl, "json_build_array(") {
		t.Errorf("JSON_ARRAY 未转换为 json_build_array：%s", ddl)
	}
}

// TestConvertViewDDL_DateTimeFunctions 测试日期时间函数转换
func TestConvertViewDDL_DateTimeFunctions(t *testing.T) {
	viewSQL := `SELECT
    DATE_ADD(d1, INTERVAL 1 WEEK) AS next_week,
    DATE_SUB(d1, INTERVAL 1 MONTH) AS last_month,
    TIMEDIFF(NOW(), dt1) AS time_since,
    TO_DAYS(NOW()) AS days_since_epoch
FROM case_09_datetime`

	ddl, err := ConvertViewDDL("view_datetime_functions", viewSQL)
	if err != nil {
		t.Fatalf("ConvertViewDDL 返回错误：%v", err)
	}

	t.Logf("转换结果：%s", ddl)

	// 检查 DATE_ADD 转换
	if !strings.Contains(ddl, "+") {
		t.Errorf("DATE_ADD 未转换为 + 操作符：%s", ddl)
	}
	if !strings.Contains(ddl, "interval '1 week'") {
		t.Errorf("DATE_ADD 未转换为 PostgreSQL interval 语法：%s", ddl)
	}
	// 检查 DATE_SUB 转换
	if !strings.Contains(ddl, "-") {
		t.Errorf("DATE_SUB 未转换为 - 操作符：%s", ddl)
	}
	if !strings.Contains(ddl, "interval '1 month'") {
		t.Errorf("DATE_SUB 未转换为 PostgreSQL interval 语法：%s", ddl)
	}
	if strings.Contains(ddl, "::interval '1 week'") || strings.Contains(ddl, "::interval '1 month'") {
		t.Errorf("仍包含不兼容 interval 强制转换语法：%s", ddl)
	}
	// 检查 TIMEDIFF 转换
	if !strings.Contains(ddl, " - ") {
		t.Errorf("TIMEDIFF 未转换为时间减法：%s", ddl)
	}
	// 检查 TO_DAYS 转换
	if !strings.Contains(ddl, "extract(epoch from") {
		t.Errorf("TO_DAYS 未转换为 extract epoch：%s", ddl)
	}
}

// TestConvertViewDDL_FullView19WithCastUsing 测试完整的 view_case19 转换（包含 MySQL 可能返回的 CAST USING 语法）
func TestConvertViewDDL_FullView19WithCastUsing(t *testing.T) {
	// 模拟 MySQL information_schema.views.view_definition 可能返回的内容
	// MySQL 可能会在视图定义中自动添加 CAST(x USING charset) 语法
	mysqlViewDefinition := `select 
    c1 as utf8_col,
    c2 as utf8mb4_col,
    c3 as latin1_col,
    c4 as utf16_col,
    c5 as charset_utf8mb4,
    c6 as charset_latin1,
    upper(c1) as upper_utf8,
    lower(c2) as lower_utf8mb4,
    trim(c3) as trimmed_latin1,
    length(c1) as length_utf8,
    char_length(c2) as char_length_utf8mb4,
    concat(cast(c1 using utf8mb4) as ' ',c2) as concatenated
from case_05_charsets`

	ddl, err := ConvertViewDDL("view_case19_advanced_string", mysqlViewDefinition)
	if err != nil {
		t.Fatalf("ConvertViewDDL 返回错误：%v", err)
	}

	t.Logf("转换结果：%s", ddl)

	lowerDDL := strings.ToLower(ddl)

	// 检查不包含 USING 语法
	if strings.Contains(lowerDDL, " using ") {
		t.Errorf("仍包含 using 语法：%s", ddl)
	}

	// 检查不包含 as ' ' 语法（这是错误的语法）
	if strings.Contains(lowerDDL, "as ' '") {
		t.Errorf("包含错误的 as ' ' 语法：%s", ddl)
	}

	// 检查 concat 被转换为 || 或者至少不包含 cast
	if strings.Contains(lowerDDL, "concat(") && strings.Contains(lowerDDL, "using") {
		t.Errorf("concat 中包含 using：%s", ddl)
	}
}

// ==================== TDD: 提高 convertMySQLOrderByToPG 覆盖率 ====================

// Test_convertMySQLOrderByToPG_Comprehensive 测试 ORDER BY 转换的全面场景
func Test_convertMySQLOrderByToPG_Comprehensive(t *testing.T) {
	tests := []struct {
		name   string
		input  string
		output string
	}{
		{"simple_asc", "col ASC", "col ASC"},
		{"simple_desc", "col DESC", "col DESC"},
		{"backtick", "`col`", "\"col\""},
		{"leading_spaces", "  col ASC", "col ASC"},
		{"trailing_spaces", "col ASC  ", "col ASC"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := convertMySQLOrderByToPG(tt.input)
			if result != tt.output {
				t.Errorf("convertMySQLOrderByToPG(%q) = %q, want %q", tt.input, result, tt.output)
			}
		})
	}
}

// ==================== TDD: 提高 replaceCastCharExpressions 覆盖率 ====================

// Test_replaceCastCharExpressions 测试 CAST(x AS CHAR) 转换
func Test_replaceCastCharExpressions(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{"simple", "SELECT CAST(col AS CHAR)", "CAST(col AS TEXT)"},
		{"with_length", "SELECT CAST(col AS CHAR(100))", "CAST(col AS TEXT)"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := replaceCastCharExpressions(tt.input)
			if !strings.Contains(result, tt.expected) {
				t.Errorf("replaceCastCharExpressions(%q) = %q, want to contain %q", tt.input, result, tt.expected)
			}
		})
	}
}

// ==================== TDD: 提高 replaceCastSignedExpressions 覆盖率 ====================

// Test_replaceCastSignedExpressions 测试 CAST(x AS SIGNED) 转换
func Test_replaceCastSignedExpressions(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{"simple", "SELECT CAST(col AS SIGNED)", "CAST(col AS INTEGER)"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := replaceCastSignedExpressions(tt.input)
			if !strings.Contains(result, tt.expected) {
				t.Errorf("replaceCastSignedExpressions(%q) = %q, want to contain %q", tt.input, result, tt.expected)
			}
		})
	}
}

// ==================== TDD: 提高 replaceJSON* 函数覆盖率 ====================

// Test_replaceJSONInsertView 测试 JSON_INSERT 转换
func Test_replaceJSONInsertView(t *testing.T) {
	result := replaceJSONInsertView("SELECT JSON_INSERT(data, '$.key', 'value')")
	if !strings.Contains(result, "JSONB_SET") {
		t.Errorf("replaceJSONInsertView() = %q, want to contain JSONB_SET", result)
	}
}

// Test_replaceJSONReplaceView 测试 JSON_REPLACE 转换
func Test_replaceJSONReplaceView(t *testing.T) {
	result := replaceJSONReplaceView("SELECT JSON_REPLACE(data, '$.key', 'value')")
	if !strings.Contains(result, "JSONB_SET") {
		t.Errorf("replaceJSONReplaceView() = %q, want to contain JSONB_SET", result)
	}
}

// Test_replaceJSONSetView 测试 JSON_SET 转换
func Test_replaceJSONSetView(t *testing.T) {
	result := replaceJSONSetView("SELECT JSON_SET(data, '$.key', 'value')")
	if !strings.Contains(result, "JSONB_SET") {
		t.Errorf("replaceJSONSetView() = %q, want to contain JSONB_SET", result)
	}
}

// Test_replaceJSONRemoveView 测试 JSON_REMOVE 转换
func Test_replaceJSONRemoveView(t *testing.T) {
	result := replaceJSONRemoveView("SELECT JSON_REMOVE(data, '$.key')")
	if !strings.Contains(result, "-") {
		t.Errorf("replaceJSONRemoveView() = %q, want to contain -", result)
	}
}

// Test_replaceJSONMergePatchView 测试 JSON_MERGE_PATCH 转换
func Test_replaceJSONMergePatchView(t *testing.T) {
	result := replaceJSONMergePatchView("SELECT JSON_MERGE_PATCH(data1, data2)")
	if !strings.Contains(result, "||") {
		t.Errorf("replaceJSONMergePatchView() = %q, want to contain ||", result)
	}
}

// Test_replaceJSONKeysView 测试 JSON_KEYS 转换
func Test_replaceJSONKeysView(t *testing.T) {
	result := replaceJSONKeysView("SELECT JSON_KEYS(data)")
	if !strings.Contains(result, "JSONB_OBJECT_KEYS") {
		t.Errorf("replaceJSONKeysView() = %q, want to contain JSONB_OBJECT_KEYS", result)
	}
}

// Test_replaceJSONLengthView 测试 JSON_LENGTH 转换
func Test_replaceJSONLengthView(t *testing.T) {
	result := replaceJSONLengthView("SELECT JSON_LENGTH(arr)")
	if !strings.Contains(result, "JSONB_ARRAY_LENGTH") {
		t.Errorf("replaceJSONLengthView() = %q, want to contain JSONB_ARRAY_LENGTH", result)
	}
}

// TestConvertViewDDL_GroupConcatWithNestedCast 测试 GROUP_CONCAT 嵌套 CAST 的转换
func TestConvertViewDDL_GroupConcatWithNestedCast(t *testing.T) {
	// 模拟错误日志中的视图定义
	viewSQL := `select "b"."status" AS "status",group_concat(distinct "i"."col_tiny" order by "i"."col_tiny" ASC separator ', ') AS "tiny_values",group_concat("i"."col_small" separator '|') AS "small_values",group_concat(cast("i"."col_medium" as char charset utf8) separator ';') AS "medium_values" from ("case_01_integers" "i" join "case_02_boolean" "b" on(("i"."col_tiny" = "b"."status"))) group by "b"."status"`

	ddl, err := ConvertViewDDL("view_case33_mysql8_string_agg", viewSQL)
	if err != nil {
		t.Fatalf("ConvertViewDDL 返回错误：%v", err)
	}

	t.Logf("转换结果：%s", ddl)

	// 验证 string_agg 函数存在
	lowerDDL := strings.ToLower(ddl)
	if !strings.Contains(lowerDDL, "string_agg(") {
		t.Errorf("GROUP_CONCAT 未转换为 string_agg：%s", ddl)
	}

	// 验证 DISTINCT 保留
	if !strings.Contains(lowerDDL, "distinct") {
		t.Errorf("DISTINCT 丢失：%s", ddl)
	}

	// 验证 ORDER BY 在 string_agg 内部
	if !strings.Contains(lowerDDL, "order by") {
		t.Errorf("ORDER BY 丢失：%s", ddl)
	}

	// 验证分隔符正确
	if !strings.Contains(ddl, "', '") || !strings.Contains(ddl, "'|'") || !strings.Contains(ddl, "';'") {
		t.Errorf("分隔符转换错误：%s", ddl)
	}

	// 验证 CAST 嵌套正确(不应出现语法错误如 "cast(..., ' AS text)")
	if strings.Contains(ddl, ", ' AS text)") {
		t.Errorf("CAST 语法错误：%s", ddl)
	}

	// 验证不包含 GROUP_CONCAT
	if strings.Contains(lowerDDL, "group_concat(") {
		t.Errorf("仍包含 GROUP_CONCAT：%s", ddl)
	}
}

// TestConvertViewDDL_GroupConcatSimple 测试简单 GROUP_CONCAT 转换
func TestConvertViewDDL_GroupConcatSimple(t *testing.T) {
	tests := []struct {
		name          string
		input         string
		expectedLower string
	}{
		{
			name:          "basic",
			input:         "SELECT GROUP_CONCAT(col) FROM t",
			expectedLower: "string_agg(cast(col as text), ',')",
		},
		{
			name:          "with_separator",
			input:         "SELECT GROUP_CONCAT(col SEPARATOR ', ') FROM t",
			expectedLower: "string_agg(cast(col as text), ', ')",
		},
		{
			name:          "with_distinct",
			input:         "SELECT GROUP_CONCAT(DISTINCT col) FROM t",
			expectedLower: "string_agg(distinct cast(col as text), ',')",
		},
		{
			name:          "with_order_by",
			input:         "SELECT GROUP_CONCAT(col ORDER BY col ASC) FROM t",
			expectedLower: "string_agg(cast(col as text), ',') order by",
		},
		{
			name:          "nested_cast",
			input:         "SELECT GROUP_CONCAT(CAST(col AS CHAR) SEPARATOR ';') FROM t",
			expectedLower: "string_agg(cast(cast(col as text) as text), ';')",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ddl, err := ConvertViewDDL("test_view", tt.input)
			if err != nil {
				t.Fatalf("ConvertViewDDL 返回错误：%v", err)
			}

			lowerDDL := strings.ToLower(ddl)
			if !strings.Contains(lowerDDL, tt.expectedLower) {
				t.Errorf("转换结果不包含期望字符串\n输入: %s\n输出: %s\n期望包含: %s", tt.input, ddl, tt.expectedLower)
			}
		})
	}
}

// TestConvertViewDDL_RoundMod 测试 ROUND 和 MOD 函数转换
func TestConvertViewDDL_RoundMod(t *testing.T) {
	viewSQL := `SELECT
    ROUND(price, 2) AS rounded_price,
    MOD(quantity, 10) AS quantity_mod,
    ROUND(score, 0) AS rounded_score
FROM case_10_numbers`

	ddl, err := ConvertViewDDL("view_round_mod_test", viewSQL)
	if err != nil {
		t.Fatalf("ConvertViewDDL 返回错误：%v", err)
	}

	t.Logf("转换结果：%s", ddl)

	lowerDDL := strings.ToLower(ddl)

	// 检查 ROUND 函数转换：ROUND(column, n) -> ROUND(column::NUMERIC, n)
	if !strings.Contains(lowerDDL, "round(price::numeric, 2)") {
		t.Errorf("ROUND(price, 2) 未正确转换为 ROUND(price::NUMERIC, 2)：%s", ddl)
	}
	if !strings.Contains(lowerDDL, "round(score::numeric, 0)") {
		t.Errorf("ROUND(score, 0) 未正确转换为 ROUND(score::NUMERIC, 0)：%s", ddl)
	}

	// 检查 MOD 函数转换：MOD(column, n) -> MOD(column::NUMERIC, n)
	if !strings.Contains(lowerDDL, "mod(quantity::numeric, 10)") {
		t.Errorf("MOD(quantity, 10) 未正确转换为 MOD(quantity::NUMERIC, 10)：%s", ddl)
	}

	// 检查不再包含原始的 MySQL 函数调用（未转换的）
	if strings.Contains(lowerDDL, "round(price,") && !strings.Contains(lowerDDL, "round(price::numeric,") {
		t.Errorf("ROUND(price, 2) 仍为 MySQL 格式，未添加 ::NUMERIC：%s", ddl)
	}
	if strings.Contains(lowerDDL, "mod(quantity,") && !strings.Contains(lowerDDL, "mod(quantity::numeric,") {
		t.Errorf("MOD(quantity, 10) 仍为 MySQL 格式，未添加 ::NUMERIC：%s", ddl)
	}
}

// Test_normalizeCastTypeForPG 测试 normalizeCastTypeForPG 的所有类型映射
func Test_normalizeCastTypeForPG(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		// 整数类型
		{"signed", "SIGNED", "BIGINT"},
		{"unsigned", "UNSIGNED", "BIGINT"},
		{"signed integer", "SIGNED INTEGER", "BIGINT"},
		{"unsigned integer", "UNSIGNED INTEGER", "BIGINT"},
		{"year", "YEAR", "INTEGER"},

		// 日期时间类型
		{"datetime", "DATETIME", "TIMESTAMP"},
		{"datetime with precision", "DATETIME(6)", "TIMESTAMP(6)"},
		{"date", "DATE", "DATE"},
		{"time", "TIME", "TIME"},
		{"time with precision", "TIME(3)", "TIME(3)"},

		// 字符串类型
		{"char", "CHAR", "TEXT"},
		{"char with length", "CHAR(20)", "TEXT"},
		{"nchar", "NCHAR", "TEXT"},
		{"nchar with length", "NCHAR(10)", "TEXT"},

		// 二进制类型
		{"binary", "BINARY", "BYTEA"},
		{"binary with length", "BINARY(16)", "BYTEA"},
		{"varbinary", "VARBINARY", "BYTEA"},
		{"varbinary with length", "VARBINARY(255)", "BYTEA"},

		// 浮点数类型
		{"double", "DOUBLE", "DOUBLE PRECISION"},
		{"float", "FLOAT", "REAL"},
		{"float with precision", "FLOAT(10)", "REAL"},
		{"real", "REAL", "DOUBLE PRECISION"},

		// JSON 类型
		{"json", "JSON", "JSONB"},

		// DECIMAL 保留精度
		{"decimal", "DECIMAL", "NUMERIC"},
		{"decimal with precision", "DECIMAL(10,2)", "NUMERIC(10,2)"},

		// 不需要转换的类型
		{"integer passthrough", "INTEGER", "INTEGER"},
		{"bigint passthrough", "BIGINT", "BIGINT"},
		{"text passthrough", "TEXT", "TEXT"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := normalizeCastTypeForPG(tt.input)
			if result != tt.expected {
				t.Errorf("normalizeCastTypeForPG(%q) = %q, want %q", tt.input, result, tt.expected)
			}
		})
	}
}

// TestConvertViewDDL_CastAllTypes 测试视图中所有 MySQL CAST 类型的端到端转换
func TestConvertViewDDL_CastAllTypes(t *testing.T) {
	viewSQL := `SELECT
    CAST(col1 AS BINARY) AS c_binary,
    CAST(col2 AS BINARY(16)) AS c_binary_n,
    CAST(col3 AS NCHAR) AS c_nchar,
    CAST(col4 AS NCHAR(10)) AS c_nchar_n,
    CAST(col5 AS DOUBLE) AS c_double,
    CAST(col6 AS FLOAT) AS c_float,
    CAST(col7 AS FLOAT(10)) AS c_float_n,
    CAST(col8 AS REAL) AS c_real,
    CAST(col9 AS JSON) AS c_json,
    CAST(col10 AS YEAR) AS c_year,
    CAST(col11 AS DATETIME(6)) AS c_datetime_n,
    CAST(col12 AS SIGNED INTEGER) AS c_signed_int,
    CAST(col13 AS UNSIGNED INTEGER) AS c_unsigned_int
FROM test_table`

	ddl, err := ConvertViewDDL("view_cast_all_types", viewSQL)
	if err != nil {
		t.Fatalf("ConvertViewDDL 返回错误：%v", err)
	}

	t.Logf("转换结果：%s", ddl)
	lowerDDL := strings.ToLower(ddl)

	// 验证所有类型转换
	checks := []struct {
		contains    string
		description string
	}{
		{"as bytea", "BINARY → BYTEA"},
		{"as double precision", "DOUBLE → DOUBLE PRECISION"},
		{"as real", "FLOAT → REAL"},
		{"as jsonb", "JSON → JSONB"},
		{"as integer", "YEAR → INTEGER"},
		{"as bigint", "SIGNED/UNSIGNED INTEGER → BIGINT"},
	}

	for _, check := range checks {
		if !strings.Contains(lowerDDL, check.contains) {
			t.Errorf("%s 转换失败，未找到 %q：%s", check.description, check.contains, ddl)
		}
	}

	// 验证 DATETIME(6) → TIMESTAMP(6)
	if !strings.Contains(lowerDDL, "timestamp(6)") {
		t.Errorf("DATETIME(6) 未转换为 TIMESTAMP(6)：%s", ddl)
	}

	// 验证不应存在的 MySQL 类型（排除列别名干扰，仅检查 CAST 目标类型）
	notWant := []string{
		"as binary)", "as nchar", "as float)", "as json)", "as year)", "as datetime",
	}
	for _, nw := range notWant {
		if strings.Contains(lowerDDL, nw) {
			t.Errorf("输出中仍包含 MySQL 类型 %q：%s", nw, ddl)
		}
	}
}
