package postgres

import (
	"os"
	"reflect"
	"regexp"
	"strings"
	"testing"

	"github.com/yourusername/mysql2pg/internal/mysql"
)

// splitSQLFunctions 将 SQL 文件内容拆分为独立的函数 DDL 块
func splitSQLFunctions(content string) []string {
	var blocks []string
	for _, block := range strings.Split(content, "DELIMITER //") {
		block = strings.TrimSpace(block)
		if block == "" {
			continue
		}
		for _, fb := range strings.Split(block, "END //") {
			fb = strings.TrimSpace(fb)
			if fb == "" {
				continue
			}
			var lines []string
			for _, line := range strings.Split(fb, "\n") {
				upper := strings.ToUpper(strings.TrimSpace(line))
				if strings.HasPrefix(upper, "DROP FUNCTION") ||
					strings.HasPrefix(upper, "DELIMITER") ||
					strings.TrimSpace(line) == "" {
					continue
				}
				lines = append(lines, line)
			}
			ddl := strings.TrimSpace(strings.Join(lines, "\n"))
			if strings.Contains(strings.ToUpper(ddl), "CREATE FUNCTION") {
				blocks = append(blocks, ddl)
			}
		}
	}
	return blocks
}

// TestCreateFunctionSQL_AllFunctions 测试 create_function.sql 中所有函数的转换
func TestCreateFunctionSQL_AllFunctions(t *testing.T) {
	sqlPath := "../../../scripts/mysql/create_function.sql"
	content, err := os.ReadFile(sqlPath)
	if err != nil {
		t.Skipf("无法读取 create_function.sql: %v", err)
	}

	blocks := splitSQLFunctions(string(content))
	if len(blocks) == 0 {
		t.Fatal("未从 create_function.sql 中解析到任何函数")
	}

	t.Logf("共解析到 %d 个函数", len(blocks))

	var successCount, failCount int
	var failedFunctions []string

	for _, ddl := range blocks {
		re := regexp.MustCompile(`(?i)CREATE\s+FUNCTION\s+(\w+)`)
		matches := re.FindStringSubmatch(ddl)
		if matches == nil {
			continue
		}
		funcName := matches[1]

		t.Run(funcName, func(t *testing.T) {
			// 直接调用 ConvertFunctionDDL 入口函数
			result, err := ConvertFunctionDDL(mysql.FunctionInfo{
				Name: funcName,
				DDL:  ddl,
			})
			if err != nil {
				t.Errorf("转换失败: %v", err)
				return
			}

			if !strings.Contains(result, "CREATE OR REPLACE FUNCTION") {
				t.Error("缺少 CREATE OR REPLACE FUNCTION")
			}
			if !strings.Contains(result, "RETURNS") {
				t.Error("缺少 RETURNS")
			}
			if !strings.Contains(result, "LANGUAGE plpgsql") {
				t.Error("缺少 LANGUAGE plpgsql")
			}
		})
	}

	t.Run("__summary__", func(t *testing.T) {
		for _, ddl := range blocks {
			re := regexp.MustCompile(`(?i)CREATE\s+FUNCTION\s+(\w+)`)
			matches := re.FindStringSubmatch(ddl)
			if matches == nil {
				continue
			}
			funcName := matches[1]

			_, err := ConvertFunctionDDL(mysql.FunctionInfo{
				Name: funcName,
				DDL:  ddl,
			})
			if err != nil {
				failCount++
				failedFunctions = append(failedFunctions, funcName+" ("+err.Error()+")")
			} else {
				successCount++
			}
		}

		t.Logf("========== 转换汇总 ==========")
		t.Logf("总数: %d | 成功: %d | 失败: %d", len(blocks), successCount, failCount)
		if failCount > 0 {
			t.Logf("失败列表:")
			for _, f := range failedFunctions {
				t.Logf("  - %s", f)
			}
		}
	})
}

// TestCreateFunctionSQL_CompatibilityFunctions 测试兼容性函数
func TestCreateFunctionSQL_CompatibilityFunctions(t *testing.T) {
	sqlPath := "../../../scripts/mysql/create_function.sql"
	content, err := os.ReadFile(sqlPath)
	if err != nil {
		t.Skipf("无法读取 create_function.sql: %v", err)
	}

	blocks := splitSQLFunctions(string(content))

	for _, ddl := range blocks {
		re := regexp.MustCompile(`(?i)CREATE\s+FUNCTION\s+(fn_case_compat_\w+)`)
		matches := re.FindStringSubmatch(ddl)
		if matches == nil {
			continue
		}
		funcName := matches[1]

		t.Run(funcName, func(t *testing.T) {
			result, err := ConvertFunctionDDL(mysql.FunctionInfo{
				Name: funcName,
				DDL:  ddl,
			})
			if err != nil {
				t.Fatalf("转换失败: %v", err)
			}

			if !strings.Contains(result, "CREATE OR REPLACE FUNCTION") {
				t.Error("缺少 CREATE OR REPLACE FUNCTION")
			}
			t.Logf("转换后的 DDL:\n%s", result)
		})
	}
}

// TestCreateFunctionSQL_ComplexFunctions 测试复杂函数转换
func TestCreateFunctionSQL_ComplexFunctions(t *testing.T) {
	sqlPath := "../../../scripts/mysql/create_function.sql"
	content, err := os.ReadFile(sqlPath)
	if err != nil {
		t.Skipf("无法读取 create_function.sql: %v", err)
	}

	blocks := splitSQLFunctions(string(content))
	complexNames := []string{
		"func_001_complex_analysis",
		"func_008_complex_analysis",
		"func_020_complex_analysis",
		"func_084_complex_analysis",
		"func_092_complex_analysis",
		"func_095_complex_analysis",
	}

	for _, name := range complexNames {
		var ddl string
		for _, block := range blocks {
			if strings.Contains(block, "CREATE FUNCTION "+name) {
				ddl = block
				break
			}
		}
		if ddl == "" {
			t.Skipf("未找到函数 %s", name)
			continue
		}

		t.Run(name, func(t *testing.T) {
			result, err := ConvertFunctionDDL(mysql.FunctionInfo{
				Name: name,
				DDL:  ddl,
			})
			if err != nil {
				t.Fatalf("转换失败: %v", err)
			}

			if !strings.Contains(result, "CREATE OR REPLACE FUNCTION") {
				t.Error("缺少 CREATE OR REPLACE FUNCTION")
			}

			if strings.Contains(strings.ToLower(ddl), "cursor") {
				if !strings.Contains(result, "refcursor") {
					t.Error("游标声明未转换为 refcursor")
				}
				if !strings.Contains(result, "FETCH NEXT FROM") {
					t.Error("FETCH 未转换")
				}
			}

			if strings.Contains(strings.ToLower(ddl), "concat_ws") {
				if strings.Contains(result, "CONCAT_WS(") {
					t.Error("CONCAT_WS 未转换")
				}
			}

			t.Logf("转换成功，DDL 长度: %d 字符", len(result))
		})
	}
}

// TestCreateFunctionSQL_FeaturesCoverage 测试特定功能覆盖
func TestCreateFunctionSQL_FeaturesCoverage(t *testing.T) {
	sqlPath := "../../../scripts/mysql/create_function.sql"
	content, err := os.ReadFile(sqlPath)
	if err != nil {
		t.Skipf("无法读取 create_function.sql: %v", err)
	}

	blocks := splitSQLFunctions(string(content))

	features := map[string]string{
		"JSON_UNQUOTE+JSON_EXTRACT": "func_102_case_157_extract_bizid",
		"DATE_FORMAT":               "func_103_case_158_period_key",
		"LENGTH+BLOB":               "func_104_case_159_attachment_size",
		"DECIMAL 高精度":             "func_105_case_160_numeric_score",
		"AVG 聚合":                   "func_107_case_daily_order_avg_price",
		"STR_TO_DATE":               "func_108_case_daily_payload_event_time",
		"ELSEIF 链":                 "func_110_case_daily_numeric_risk_tag",
		"done=1 兼容":               "fn_case_compat_memberratio",
		"WHILE 循环":                 "fn_case_compat_unspecial",
		"REPLACE 函数":               "fn_case_compat_unspecial",
	}

	for feature, funcName := range features {
		t.Run(feature, func(t *testing.T) {
			var ddl string
			for _, block := range blocks {
				if strings.Contains(block, "CREATE FUNCTION "+funcName) {
					ddl = block
					break
				}
			}
			if ddl == "" {
				t.Skipf("未找到函数 %s", funcName)
			}

			result, err := ConvertFunctionDDL(mysql.FunctionInfo{
				Name: funcName,
				DDL:  ddl,
			})
			if err != nil {
				t.Fatalf("转换失败: %v", err)
			}

			t.Logf("函数 %s 转换成功，DDL:\n%s", funcName, result)
		})
	}
}

// TestCreateFunctionSQL_DailyFunctions 测试日常业务函数转换
func TestCreateFunctionSQL_DailyFunctions(t *testing.T) {
	sqlPath := "../../../scripts/mysql/create_function.sql"
	content, err := os.ReadFile(sqlPath)
	if err != nil {
		t.Skipf("无法读取 create_function.sql: %v", err)
	}

	blocks := splitSQLFunctions(string(content))

	dailyFuncs := []string{
		"func_106_case_daily_order_item_count",
		"func_107_case_daily_order_avg_price",
		"func_108_case_daily_payload_event_time",
		"func_109_case_daily_deleted_title",
		"func_110_case_daily_numeric_risk_tag",
	}

	for _, funcName := range dailyFuncs {
		var ddl string
		for _, block := range blocks {
			if strings.Contains(block, "CREATE FUNCTION "+funcName) {
				ddl = block
				break
			}
		}
		if ddl == "" {
			t.Skipf("未找到函数 %s", funcName)
		}

		t.Run(funcName, func(t *testing.T) {
			result, err := ConvertFunctionDDL(mysql.FunctionInfo{
				Name: funcName,
				DDL:  ddl,
			})
			if err != nil {
				t.Fatalf("转换失败：%v", err)
			}

			if !strings.Contains(result, "CREATE OR REPLACE FUNCTION") {
				t.Error("缺少 CREATE OR REPLACE FUNCTION")
			}
			if !strings.Contains(result, "RETURNS") {
				t.Error("缺少 RETURNS")
			}
			if !strings.Contains(result, "LANGUAGE plpgsql") {
				t.Error("缺少 LANGUAGE plpgsql")
			}

			t.Logf("函数 %s 转换成功", funcName)
		})
	}
}

// TestCreateFunctionSQL_TypeCoverage 测试类型覆盖
func TestCreateFunctionSQL_TypeCoverage(t *testing.T) {
	sqlPath := "../../../scripts/mysql/create_function.sql"
	content, err := os.ReadFile(sqlPath)
	if err != nil {
		t.Skipf("无法读取 create_function.sql: %v", err)
	}

	blocks := splitSQLFunctions(string(content))

	typeTests := map[string]string{
		"DECIMAL":  "func_101_case_156_order_amount",
		"VARCHAR":  "func_102_case_157_extract_bizid",
		"CHAR":     "func_103_case_158_period_key",
		"BIGINT":   "func_104_case_159_attachment_size",
		"INT":      "func_106_case_daily_order_item_count",
		"DATETIME": "func_108_case_daily_payload_event_time",
	}

	for typeName, funcName := range typeTests {
		var ddl string
		for _, block := range blocks {
			if strings.Contains(block, "CREATE FUNCTION "+funcName) {
				ddl = block
				break
			}
		}
		if ddl == "" {
			t.Skipf("未找到函数 %s", funcName)
		}

		t.Run(typeName, func(t *testing.T) {
			result, err := ConvertFunctionDDL(mysql.FunctionInfo{
				Name: funcName,
				DDL:  ddl,
			})
			if err != nil {
				t.Fatalf("转换失败：%v", err)
			}

			if !strings.Contains(result, "RETURNS") {
				t.Error("缺少 RETURNS 子句")
			}

			t.Logf("类型 %s 的函数 %s 转换成功", typeName, funcName)
		})
	}
}

// TestCreateFunctionSQL_ControlStructures 测试控制结构覆盖
func TestCreateFunctionSQL_ControlStructures(t *testing.T) {
	sqlPath := "../../../scripts/mysql/create_function.sql"
	content, err := os.ReadFile(sqlPath)
	if err != nil {
		t.Skipf("无法读取 create_function.sql: %v", err)
	}

	blocks := splitSQLFunctions(string(content))

	structTests := map[string]string{
		"CURSOR+LOOP": "func_001_complex_analysis",
		"WHILE":       "fn_case_compat_unspecial",
		"CASE":        "func_001_complex_analysis",
		"IF+ELSEIF":   "func_110_case_daily_numeric_risk_tag",
		"DECLARE":     "func_001_complex_analysis",
		"HANDLER":     "func_001_complex_analysis",
	}

	for structName, funcName := range structTests {
		var ddl string
		for _, block := range blocks {
			if strings.Contains(block, "CREATE FUNCTION "+funcName) {
				ddl = block
				break
			}
		}
		if ddl == "" {
			t.Skipf("未找到函数 %s", funcName)
		}

		t.Run(structName, func(t *testing.T) {
			result, err := ConvertFunctionDDL(mysql.FunctionInfo{
				Name: funcName,
				DDL:  ddl,
			})
			if err != nil {
				t.Fatalf("转换失败：%v", err)
			}

			switch structName {
			case "CURSOR+LOOP":
				if strings.Contains(strings.ToLower(ddl), "cursor") {
					if !strings.Contains(result, "refcursor") {
						t.Error("游标未转换为 refcursor")
					}
				}
			case "WHILE":
				if strings.Contains(strings.ToLower(ddl), "while") {
					if !strings.Contains(result, "while") && !strings.Contains(result, "LOOP") {
						t.Error("WHILE 未正确转换")
					}
				}
			case "CASE":
				if strings.Contains(strings.ToUpper(ddl), "CASE") {
					if !strings.Contains(result, "CASE") && !strings.Contains(result, "case") {
						t.Error("CASE 结构丢失")
					}
				}
			}

			t.Logf("控制结构 %s 的函数 %s 转换成功", structName, funcName)
		})
	}
}

// TestCreateFunctionSQL_JSONFunctions 测试 JSON 函数转换
func TestCreateFunctionSQL_JSONFunctions(t *testing.T) {
	sqlPath := "../../../scripts/mysql/create_function.sql"
	content, err := os.ReadFile(sqlPath)
	if err != nil {
		t.Skipf("无法读取 create_function.sql: %v", err)
	}

	blocks := splitSQLFunctions(string(content))

	jsonFuncs := []string{
		"func_102_case_157_extract_bizid",
		"func_108_case_daily_payload_event_time",
	}

	for _, funcName := range jsonFuncs {
		var ddl string
		for _, block := range blocks {
			if strings.Contains(block, "CREATE FUNCTION "+funcName) {
				ddl = block
				break
			}
		}
		if ddl == "" {
			t.Skipf("未找到函数 %s", funcName)
		}

		t.Run(funcName, func(t *testing.T) {
			result, err := ConvertFunctionDDL(mysql.FunctionInfo{
				Name: funcName,
				DDL:  ddl,
			})
			if err != nil {
				t.Fatalf("转换失败：%v", err)
			}

			if !strings.Contains(result, "CREATE OR REPLACE FUNCTION") {
				t.Error("缺少 CREATE OR REPLACE FUNCTION")
			}

			t.Logf("JSON 函数 %s 转换成功", funcName)
		})
	}
}

// TestCreateFunctionSQL_DateTimeFunctions 测试日期时间函数转换
func TestCreateFunctionSQL_DateTimeFunctions(t *testing.T) {
	sqlPath := "../../../scripts/mysql/create_function.sql"
	content, err := os.ReadFile(sqlPath)
	if err != nil {
		t.Skipf("无法读取 create_function.sql: %v", err)
	}

	blocks := splitSQLFunctions(string(content))

	dateTimeFuncs := map[string]string{
		"DATE_FORMAT": "func_103_case_158_period_key",
		"STR_TO_DATE": "func_108_case_daily_payload_event_time",
	}

	for funcType, funcName := range dateTimeFuncs {
		var ddl string
		for _, block := range blocks {
			if strings.Contains(block, "CREATE FUNCTION "+funcName) {
				ddl = block
				break
			}
		}
		if ddl == "" {
			t.Skipf("未找到函数 %s", funcName)
		}

		t.Run(funcType, func(t *testing.T) {
			result, err := ConvertFunctionDDL(mysql.FunctionInfo{
				Name: funcName,
				DDL:  ddl,
			})
			if err != nil {
				t.Fatalf("转换失败：%v", err)
			}

			if !strings.Contains(result, "CREATE OR REPLACE FUNCTION") {
				t.Error("缺少 CREATE OR REPLACE FUNCTION")
			}

			t.Logf("日期时间函数 %s 转换成功", funcType)
		})
	}
}

// TestFunctionConverter_processGroupConcat 测试 GROUP_CONCAT 处理
func TestFunctionConverter_processGroupConcat(t *testing.T) {
	converter := &FunctionConverter{}
	
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "simple",
			input:    "SELECT GROUP_CONCAT(col) FROM t",
			expected: "STRING_AGG",
		},
		{
			name:     "with_separator",
			input:    "SELECT GROUP_CONCAT(col SEPARATOR ', ') FROM t",
			expected: "STRING_AGG",
		},
		{
			name:     "with_distinct",
			input:    "SELECT GROUP_CONCAT(DISTINCT col) FROM t",
			expected: "STRING_AGG",
		},
		{
			name:     "with_order_by",
			input:    "SELECT GROUP_CONCAT(col ORDER BY col) FROM t",
			expected: "STRING_AGG",
		},
		{
			name:     "nested_expression",
			input:    "SELECT GROUP_CONCAT(CONCAT(a, b)) FROM t",
			expected: "STRING_AGG",
		},
	}
	
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := converter.processGroupConcat(tt.input)
			if !strings.Contains(result, tt.expected) {
				t.Errorf("processGroupConcat(%q) = %q, want to contain %q", tt.input, result, tt.expected)
			}
		})
	}
}

// TestFunctionConverter_processDateDiff 测试 DATEDIFF 处理
func TestFunctionConverter_processDateDiff(t *testing.T) {
	converter := &FunctionConverter{}
	
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "simple",
			input:    "SELECT DATEDIFF(a, b)",
			expected: "-",
		},
		{
			name:     "with_columns",
			input:    "SELECT DATEDIFF(end_date, start_date)",
			expected: "-",
		},
		{
			name:     "in_where",
			input:    "WHERE DATEDIFF(NOW(), created_at) > 7",
			expected: "-",
		},
		{
			name:     "multiple",
			input:    "SELECT DATEDIFF(a, b), DATEDIFF(c, d)",
			expected: "-",
		},
	}
	
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := converter.processDateDiff(tt.input)
			if !strings.Contains(result, tt.expected) {
				t.Errorf("processDateDiff(%q) = %q, want to contain %q", tt.input, result, tt.expected)
			}
		})
	}
}

// TestFunctionConverter_processIfFunction 测试 IF 函数处理
func TestFunctionConverter_processIfFunction(t *testing.T) {
	converter := &FunctionConverter{}
	
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "simple",
			input:    "SELECT IF(a > b, 'yes', 'no')",
			expected: "CASE WHEN",
		},
		{
			name:     "with_columns",
			input:    "SELECT IF(col1 > 100, col2, col3)",
			expected: "CASE WHEN",
		},
		{
			name:     "nested",
			input:    "SELECT IF(a > b, IF(c > d, 'x', 'y'), 'z')",
			expected: "CASE WHEN",
		},
		{
			name:     "in_where",
			input:    "WHERE IF(status = 1, active, inactive) = 1",
			expected: "CASE WHEN",
		},
		{
			name:     "with_null",
			input:    "SELECT IF(col IS NULL, 0, col)",
			expected: "CASE WHEN",
		},
	}
	
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := converter.processIfFunction(tt.input)
			if !strings.Contains(result, tt.expected) {
				t.Errorf("processIfFunction(%q) = %q, want to contain %q", tt.input, result, tt.expected)
			}
		})
	}
}

// TestFunctionConverter_processIsNull 测试 ISNULL 处理
func TestFunctionConverter_processIsNull(t *testing.T) {
	converter := &FunctionConverter{}
	
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "simple",
			input:    "SELECT ISNULL(col)",
			expected: "IS NULL",
		},
		{
			name:     "in_where",
			input:    "WHERE ISNULL(email)",
			expected: "IS NULL",
		},
		{
			name:     "with_expression",
			input:    "SELECT ISNULL(a + b)",
			expected: "IS NULL",
		},
	}
	
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := converter.processIsNull(tt.input)
			if !strings.Contains(result, tt.expected) {
				t.Errorf("processIsNull(%q) = %q, want to contain %q", tt.input, result, tt.expected)
			}
		})
	}
}

// TestFunctionConverter_handleUserVariables 测试用户变量处理
func TestFunctionConverter_handleUserVariables(t *testing.T) {
	converter := &FunctionConverter{}
	
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "simple_var",
			input:    "SET @var = 1",
			expected: "v_var",
		},
		{
			name:     "multiple_vars",
			input:    "SET @a = 1, @b = 2",
			expected: "v_a",
		},
		{
			name:     "in_select",
			input:    "SELECT @var := col FROM t",
			expected: ":=",
		},
	}
	
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			converter.body = tt.input
			converter.handleUserVariables()
			if !strings.Contains(converter.body, tt.expected) {
				t.Errorf("handleUserVariables(%q) = %q, want to contain %q", tt.input, converter.body, tt.expected)
			}
		})
	}
}

// TestFunctionConverter_applySpecificPatches 测试特定补丁应用
func TestFunctionConverter_applySpecificPatches(t *testing.T) {
	converter := &FunctionConverter{}
	
	tests := []struct {
		name     string
		funcName string
		input    string
		check    func(string) bool
	}{
		{
			name:     "complex_join_function",
			funcName: "complex_join_function",
			input:    "if v_done then exit; else v_count := v_count + 1; -- 条件判断",
			check: func(s string) bool {
				return strings.Contains(s, "if v_done then exit;") && 
				       strings.Contains(s, "else") &&
				       strings.Contains(s, "v_count := v_count + 1")
			},
		},
		{
			name:     "comprehensive_reporting",
			funcName: "comprehensive_reporting",
			input:    "SET v_row_index = -1;",
			check: func(s string) bool {
				return !strings.Contains(s, "SET v_row_index = -1;")
			},
		},
	}
	
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			converter.mysqlFunc = mysql.FunctionInfo{Name: tt.funcName}
			converter.body = tt.input
			converter.applySpecificPatches()
			if !tt.check(converter.body) {
				t.Errorf("applySpecificPatches(%q) check failed, got: %q", tt.name, converter.body)
			}
		})
	}
}

// TestFunctionConverter_parseCharacteristics 测试函数特性解析
func TestFunctionConverter_parseCharacteristics(t *testing.T) {
	tests := []struct {
		name           string
		ddl            string
		wantVolatility string
		wantSecurity   string
		wantComment    string
	}{
		{
			name:           "deterministic",
			ddl:            "CREATE FUNCTION f() RETURNS INT DETERMINISTIC BEGIN RETURN 1; END",
			wantVolatility: "IMMUTABLE",
			wantSecurity:   "SECURITY INVOKER",
		},
		{
			name:           "not_deterministic",
			ddl:            "CREATE FUNCTION f() RETURNS INT NOT DETERMINISTIC BEGIN RETURN 1; END",
			wantVolatility: "VOLATILE",
			wantSecurity:   "SECURITY INVOKER",
		},
		{
			name:           "reads_sql_data",
			ddl:            "CREATE FUNCTION f() RETURNS INT READS SQL DATA BEGIN RETURN 1; END",
			wantVolatility: "STABLE",
			wantSecurity:   "SECURITY INVOKER",
		},
		{
			name:           "sql_security_definer",
			ddl:            "CREATE FUNCTION f() RETURNS INT SQL SECURITY DEFINER BEGIN RETURN 1; END",
			wantVolatility: "VOLATILE",
			wantSecurity:   "SECURITY DEFINER",
		},
		{
			name:        "with_comment",
			ddl:         "CREATE FUNCTION f() RETURNS INT COMMENT 'test comment' BEGIN RETURN 1; END",
			wantComment: "test comment",
		},
	}
	
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			converter := NewFunctionConverter(mysql.FunctionInfo{
				Name: "f",
				DDL:  tt.ddl,
			})
			
			err := converter.parseCharacteristics()
			if err != nil {
				t.Fatalf("parseCharacteristics() error = %v", err)
			}
			
			if tt.wantVolatility != "" && converter.volatility != tt.wantVolatility {
				t.Errorf("volatility = %q, want %q", converter.volatility, tt.wantVolatility)
			}
			if tt.wantSecurity != "" && converter.security != tt.wantSecurity {
				t.Errorf("security = %q, want %q", converter.security, tt.wantSecurity)
			}
			if tt.wantComment != "" && converter.comment != tt.wantComment {
				t.Errorf("comment = %q, want %q", converter.comment, tt.wantComment)
			}
		})
	}
}

// TestFunctionConverter_extractBody 测试函数体提取
func TestFunctionConverter_extractBody(t *testing.T) {
	tests := []struct {
		name    string
		ddl     string
		wantErr bool
	}{
		{
			name:    "with_begin_end",
			ddl:     "CREATE FUNCTION f() RETURNS INT BEGIN RETURN 1; END",
			wantErr: false,
		},
		{
			name:    "without_begin",
			ddl:     "CREATE FUNCTION f() RETURNS INT RETURN 1",
			wantErr: true,
		},
		{
			name:    "complex_body",
			ddl:     "CREATE FUNCTION f() RETURNS INT BEGIN DECLARE x INT; SET x = 1; RETURN x; END",
			wantErr: false,
		},
	}
	
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			converter := NewFunctionConverter(mysql.FunctionInfo{
				Name: "f",
				DDL:  tt.ddl,
			})
			
			err := converter.extractBody()
			if (err != nil) != tt.wantErr {
				t.Errorf("extractBody() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

// ==================== TDD: 提高 Convert 方法覆盖率 ====================

// TestFunctionConverter_Convert_Integration 测试完整转换流程
func TestFunctionConverter_Convert_Integration(t *testing.T) {
	tests := []struct {
		name        string
		funcInfo    mysql.FunctionInfo
		wantContain []string
		wantErr     bool
	}{
		{
			name: "complete_function_with_deterministic",
			funcInfo: mysql.FunctionInfo{
				Name: "test_func",
				DDL:  "CREATE FUNCTION test_func(a INT) RETURNS INT DETERMINISTIC BEGIN RETURN a; END",
			},
			wantContain: []string{
				"CREATE OR REPLACE FUNCTION",
				"RETURNS INTEGER",
				"LANGUAGE plpgsql",
				"IMMUTABLE",
			},
			wantErr: false,
		},
		{
			name: "function_reads_sql_data",
			funcInfo: mysql.FunctionInfo{
				Name: "test_func",
				DDL:  "CREATE FUNCTION test_func(a INT) RETURNS INT READS SQL DATA BEGIN RETURN a; END",
			},
			wantContain: []string{
				"CREATE OR REPLACE FUNCTION",
				"RETURNS INTEGER",
				"STABLE",
			},
			wantErr: false,
		},
		{
			name: "function_with_sql_security_definer",
			funcInfo: mysql.FunctionInfo{
				Name: "test_func",
				DDL:  "CREATE FUNCTION test_func() RETURNS INT SQL SECURITY DEFINER BEGIN RETURN 1; END",
			},
			wantContain: []string{
				"SECURITY DEFINER",
			},
			wantErr: false,
		},
		{
			name: "function_with_cursor",
			funcInfo: mysql.FunctionInfo{
				Name: "test_func",
				DDL:  "CREATE FUNCTION test_func() RETURNS INT BEGIN DECLARE cur CURSOR FOR SELECT 1; OPEN cur; CLOSE cur; RETURN 1; END",
			},
			wantContain: []string{"refcursor", "OPEN", "CLOSE"},
			wantErr:     false,
		},
		{
			name: "function_with_variables",
			funcInfo: mysql.FunctionInfo{
				Name: "test_func",
				DDL:  "CREATE FUNCTION test_func() RETURNS INT BEGIN DECLARE x INT DEFAULT 0; SET x = 1; RETURN x; END",
			},
			wantContain: []string{"DECLARE", "x INTEGER", ":="},
			wantErr:     false,
		},
		{
			name: "function_with_json_functions",
			funcInfo: mysql.FunctionInfo{
				Name: "test_func",
				DDL:  "CREATE FUNCTION test_func(doc JSON) RETURNS JSON BEGIN DECLARE result JSON; SET result = JSON_EXTRACT(doc, '$.key'); RETURN result; END",
			},
			wantContain: []string{
				"CREATE OR REPLACE FUNCTION",
				"RETURNS JSONB",
			},
			wantErr: false,
		},
		{
			name: "function_with_date_format",
			funcInfo: mysql.FunctionInfo{
				Name: "test_func",
				DDL:  "CREATE FUNCTION test_func() RETURNS VARCHAR(50) BEGIN RETURN DATE_FORMAT(NOW(), '%Y-%m-%d'); END",
			},
			wantContain: []string{"TO_CHAR", "CURRENT_TIMESTAMP"},
			wantErr:     false,
		},
		{
			name: "function_with_ifnull",
			funcInfo: mysql.FunctionInfo{
				Name: "test_func",
				DDL:  "CREATE FUNCTION test_func(a INT) RETURNS INT BEGIN RETURN IFNULL(a, 0); END",
			},
			wantContain: []string{"COALESCE"},
			wantErr:     false,
		},
		{
			name: "function_with_concat",
			funcInfo: mysql.FunctionInfo{
				Name: "test_func",
				DDL:  "CREATE FUNCTION test_func(a VARCHAR(50), b VARCHAR(50)) RETURNS VARCHAR(100) BEGIN RETURN CONCAT(a, b); END",
			},
			wantContain: []string{"||"},
			wantErr:     false,
		},
		{
			name: "function_with_group_concat",
			funcInfo: mysql.FunctionInfo{
				Name: "test_func",
				DDL:  "CREATE FUNCTION test_func() RETURNS TEXT BEGIN RETURN GROUP_CONCAT(col SEPARATOR ', '); END",
			},
			wantContain: []string{"STRING_AGG"},
			wantErr:     false,
		},
		{
			name: "function_with_if_function",
			funcInfo: mysql.FunctionInfo{
				Name: "test_func",
				DDL:  "CREATE FUNCTION test_func(a INT, b INT) RETURNS INT BEGIN RETURN IF(a > b, a, b); END",
			},
			wantContain: []string{"CASE WHEN", "THEN", "ELSE", "END"},
			wantErr:     false,
		},
		{
			name: "function_missing_returns",
			funcInfo: mysql.FunctionInfo{
				Name: "test_func",
				DDL:  "CREATE FUNCTION test_func(a INT) BEGIN RETURN a; END",
			},
			wantErr: true,
		},
	}
	
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := ConvertFunctionDDL(tt.funcInfo)
			if (err != nil) != tt.wantErr {
				t.Errorf("ConvertFunctionDDL() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr {
				for _, want := range tt.wantContain {
					if !strings.Contains(result, want) {
						t.Errorf("ConvertFunctionDDL() = %q, want to contain %q", result, want)
					}
				}
			}
		})
	}
}

// ==================== TDD: 提高 parseReturnType 覆盖率 ====================

// TestFunctionConverter_parseReturnType 测试返回类型解析
func TestFunctionConverter_parseReturnType(t *testing.T) {
	tests := []struct {
		name        string
		ddl         string
		wantType    string
		wantErr     bool
	}{
		{
			name:     "simple_int",
			ddl:      "CREATE FUNCTION f() RETURNS INT BEGIN RETURN 1; END",
			wantType: "INTEGER",
			wantErr:  false,
		},
		{
			name:     "varchar_with_length",
			ddl:      "CREATE FUNCTION f() RETURNS VARCHAR(255) BEGIN RETURN 'a'; END",
			wantType: "VARCHAR(255)",
			wantErr:  false,
		},
		{
			name:     "varchar_with_charset",
			ddl:      "CREATE FUNCTION f() RETURNS VARCHAR(255) CHARSET utf8mb4 BEGIN RETURN 'a'; END",
			wantType: "VARCHAR(255)",
			wantErr:  false,
		},
		{
			name:     "varchar_with_collate",
			ddl:      "CREATE FUNCTION f() RETURNS VARCHAR(255) COLLATE utf8mb4_unicode_ci BEGIN RETURN 'a'; END",
			wantType: "VARCHAR(255)",
			wantErr:  false,
		},
		{
			name:     "datetime_with_precision",
			ddl:      "CREATE FUNCTION f() RETURNS DATETIME(6) BEGIN RETURN NOW(); END",
			wantType: "TIMESTAMP(6)",
			wantErr:  false,
		},
		{
			name:     "datetime_without_precision",
			ddl:      "CREATE FUNCTION f() RETURNS DATETIME BEGIN RETURN NOW(); END",
			wantType: "TIMESTAMP",
			wantErr:  false,
		},
		{
			name:     "decimal_with_precision",
			ddl:      "CREATE FUNCTION f() RETURNS DECIMAL(65,30) BEGIN RETURN 1.0; END",
			wantType: "DECIMAL(65,30)",
			wantErr:  false,
		},
		{
			name:     "numeric_with_precision",
			ddl:      "CREATE FUNCTION f() RETURNS NUMERIC(10,2) BEGIN RETURN 1.0; END",
			wantType: "NUMERIC(10,2)",
			wantErr:  false,
		},
		{
			name:     "unsigned_int",
			ddl:      "CREATE FUNCTION f() RETURNS INT UNSIGNED BEGIN RETURN 1; END",
			wantType: "INTEGER",
			wantErr:  false,
		},
		{
			name:     "unsigned_bigint",
			ddl:      "CREATE FUNCTION f() RETURNS BIGINT UNSIGNED BEGIN RETURN 1; END",
			wantType: "BIGINT",
			wantErr:  false,
		},
		{
			name:     "text_with_collation",
			ddl:      "CREATE FUNCTION f() RETURNS TEXT COLLATE utf8mb4_unicode_ci BEGIN RETURN 'a'; END",
			wantType: "TEXT",
			wantErr:  false,
		},
		{
			name:     "json_type",
			ddl:      "CREATE FUNCTION f() RETURNS JSON BEGIN RETURN '{}'; END",
			wantType: "JSONB",
			wantErr:  false,
		},
		{
			name:     "tinyint_boolean",
			ddl:      "CREATE FUNCTION f() RETURNS TINYINT(1) BEGIN RETURN 1; END",
			wantType: "INTEGER",  // TINYINT 统一转换为 INTEGER
			wantErr:  false,
		},
		{
			name:     "tinyint_not_boolean",
			ddl:      "CREATE FUNCTION f() RETURNS TINYINT(4) BEGIN RETURN 1; END",
			wantType: "INTEGER",  // TINYINT 统一转换为 INTEGER
			wantErr:  false,
		},
		{
			name:     "zerofill_int",
			ddl:      "CREATE FUNCTION f() RETURNS INT ZEROFILL BEGIN RETURN 1; END",
			wantType: "INTEGER",
			wantErr:  false,
		},
		{
			name:     "missing_returns",
			ddl:      "CREATE FUNCTION f() BEGIN RETURN 1; END",
			wantErr:  true,
		},
		{
			name:     "with_character_set",
			ddl:      "CREATE FUNCTION f() RETURNS VARCHAR(100) CHARACTER SET utf8 BEGIN RETURN 'a'; END",
			wantType: "VARCHAR(100)",
			wantErr:  false,
		},
		{
			name:     "with_deterministic",
			ddl:      "CREATE FUNCTION f() RETURNS INT DETERMINISTIC BEGIN RETURN 1; END",
			wantType: "INTEGER",
			wantErr:  false,
		},
		{
			name:     "with_reads_sql_data",
			ddl:      "CREATE FUNCTION f() RETURNS INT READS SQL DATA BEGIN RETURN 1; END",
			wantType: "INTEGER",
			wantErr:  false,
		},
		{
			name:     "complex_return_type",
			ddl:      "CREATE FUNCTION f() RETURNS DECIMAL(65,30) UNSIGNED BEGIN RETURN 1.0; END",
			wantType: "DECIMAL(65,30)",
			wantErr:  false,
		},
	}
	
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			converter := NewFunctionConverter(mysql.FunctionInfo{
				Name: "f",
				DDL:  tt.ddl,
			})
			
			err := converter.parseReturnType()
			if (err != nil) != tt.wantErr {
				t.Errorf("parseReturnType() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && converter.returnType != tt.wantType {
				t.Errorf("parseReturnType() returnType = %q, want %q", converter.returnType, tt.wantType)
			}
		})
	}
}

// ==================== TDD: 辅助函数测试 ====================

// TestHelperFunctions 测试辅助函数
func TestHelperFunctions(t *testing.T) {
	t.Run("isWhitespaceByte", func(t *testing.T) {
		tests := []struct {
			input byte
			want  bool
		}{
			{' ', true},
			{'\n', true},
			{'\r', true},
			{'\t', true},
			{'a', false},
			{'1', false},
			{'_', false},
			{'(', false},
		}
		for _, tt := range tests {
			if got := isWhitespaceByte(tt.input); got != tt.want {
				t.Errorf("isWhitespaceByte(%q) = %v, want %v", tt.input, got, tt.want)
			}
		}
	})
	
	t.Run("isIdentifierByte", func(t *testing.T) {
		tests := []struct {
			input byte
			want  bool
		}{
			{'a', true},
			{'Z', true},
			{'_', true},
			{'1', true},
			{'0', true},
			{' ', false},
			{'(', false},
			{')', false},
			{',', false},
		}
		for _, tt := range tests {
			if got := isIdentifierByte(tt.input); got != tt.want {
				t.Errorf("isIdentifierByte(%q) = %v, want %v", tt.input, got, tt.want)
			}
		}
	})
	
	t.Run("hasKeywordAt", func(t *testing.T) {
		tests := []struct {
			input   string
			idx     int
			keyword string
			want    bool
		}{
			{"BEGIN", 0, "BEGIN", true},
			{"BEGIN", 0, "BE", false}, // BE 不是独立关键字
			{"  BEGIN", 2, "BEGIN", true},
			{"BEGIN ", 0, "BEGIN", true},
			{"BEGINNER", 0, "BEGIN", false}, // BEGINNER 不是 BEGIN
			{"", 0, "BEGIN", false},
			{"BEGIN", 10, "BEGIN", false}, // 索引越界
			{"CREATE FUNCTION", 7, "FUNCTION", true},
			{"RETURNS INT", 8, "INT", true},
			{"INTEGER", 0, "INT", false}, // INT 不是 INTEGER
		}
		for _, tt := range tests {
			if got := hasKeywordAt(tt.input, tt.idx, tt.keyword); got != tt.want {
				t.Errorf("hasKeywordAt(%q, %d, %q) = %v, want %v", tt.input, tt.idx, tt.keyword, got, tt.want)
			}
		}
	})
	
	t.Run("findReturnTypeEnd", func(t *testing.T) {
		tests := []struct {
			name    string
			ddl     string
			start   int
			wantEnd int
		}{
			{
				name:    "simple_int",
				ddl:     "RETURNS INT BEGIN",
				start:   8,
				wantEnd: 12,  // INT 后面一个字符
			},
			{
				name:    "varchar_with_precision",
				ddl:     "RETURNS VARCHAR(255) BEGIN",
				start:   8,
				wantEnd: 21,  // VARCHAR(255) 后面一个字符
			},
			{
				name:    "with_deterministic",
				ddl:     "RETURNS INT DETERMINISTIC BEGIN",
				start:   8,
				wantEnd: 12,  // 遇到 DETERMINISTIC 停止
			},
			{
				name:    "with_reads_sql_data",
				ddl:     "RETURNS INT READS SQL DATA BEGIN",
				start:   8,
				wantEnd: 12,  // 遇到 READS SQL DATA 停止
			},
			{
				name:    "with_not_deterministic",
				ddl:     "RETURNS INT NOT DETERMINISTIC BEGIN",
				start:   8,
				wantEnd: 12,  // 遇到 NOT DETERMINISTIC 停止
			},
			{
				name:    "with_comment",
				ddl:     "RETURNS INT COMMENT 'test' BEGIN",
				start:   8,
				wantEnd: 12,  // 遇到 COMMENT 停止
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				if got := findReturnTypeEnd(tt.ddl, strings.ToUpper(tt.ddl), tt.start); got != tt.wantEnd {
					t.Errorf("findReturnTypeEnd() = %v, want %v", got, tt.wantEnd)
				}
			})
		}
	})
	
	t.Run("splitArgsWithContext", func(t *testing.T) {
		tests := []struct {
			input string
			want  []string
		}{
			{"a, b, c", []string{"a", "b", "c"}},
			{"'a, b', c", []string{"'a, b'", "c"}}, // 引号内的逗号不分割
			{"CONCAT(a, b), c", []string{"CONCAT(a, b)", "c"}}, // 括号内的逗号不分割
			{"", []string{""}},
			{"a", []string{"a"}},
			{"a, 'b, c', d", []string{"a", "'b, c'", "d"}},
			{"JSON_EXTRACT(doc, '$.key'), val", []string{"JSON_EXTRACT(doc, '$.key')", "val"}},
		}
		for _, tt := range tests {
			if got := splitArgsWithContext(tt.input); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("splitArgsWithContext(%q) = %v, want %v", tt.input, got, tt.want)
			}
		}
	})
	
	t.Run("normalizeMySQLEscapedQuoteLiteral", func(t *testing.T) {
		tests := []struct {
			input string
			want  string
		}{
			{"'normal string'", "'normal string'"},
			{"'test'", "'test'"},
		}
		for _, tt := range tests {
			if got := normalizeMySQLEscapedQuoteLiteral(tt.input); got != tt.want {
				t.Errorf("normalizeMySQLEscapedQuoteLiteral(%q) = %q, want %q", tt.input, got, tt.want)
			}
		}
	})
	
	t.Run("removeMySQLHashComments", func(t *testing.T) {
		tests := []struct {
			input string
			want  string
		}{
			{"SELECT 1 # comment", "SELECT 1"},
			{"SELECT 1 -- comment", "SELECT 1 -- comment"}, // 不处理 -- 注释
			{"SELECT 'test # not comment'", "SELECT 'test # not comment'"}, // 引号内的 # 不移除
			{"SELECT 1", "SELECT 1"},
		}
		for _, tt := range tests {
			if got := removeMySQLHashComments(tt.input); got != tt.want {
				t.Errorf("removeMySQLHashComments(%q) = %q, want %q", tt.input, got, tt.want)
			}
		}
	})
	
	t.Run("normalizeEndLoopLabelTails", func(t *testing.T) {
		tests := []struct {
			input string
			want  string
		}{
			// 注意：这个函数只处理 END LOOP 后面紧跟标签的情况
			{"END LOOP;", "END LOOP;"},
			{"END LOOP", "END LOOP"},
		}
		for _, tt := range tests {
			if got := normalizeEndLoopLabelTails(tt.input); got != tt.want {
				t.Errorf("normalizeEndLoopLabelTails(%q) = %q, want %q", tt.input, got, tt.want)
			}
		}
	})
}

// ==================== TDD: 提高 processDateDiff 覆盖率 ====================

// TestFunctionConverter_processDateDiff_Comprehensive 测试 DATEDIFF 处理的全面场景
func TestFunctionConverter_processDateDiff_Comprehensive(t *testing.T) {
	converter := &FunctionConverter{}
	
	tests := []struct {
		name     string
		input    string
		expected string
		notExpected string
	}{
		{
			name:     "simple_datediff",
			input:    "SELECT DATEDIFF(end_date, start_date)",
			expected: "(end_date - start_date)",
		},
		{
			name:     "datediff_with_now",
			input:    "SELECT DATEDIFF(NOW(), created_at)",
			expected: "(NOW() - created_at)",
		},
		{
			name:     "datediff_in_where",
			input:    "WHERE DATEDIFF(NOW(), created_at) > 7",
			expected: "(NOW() - created_at)",
		},
		{
			name:     "datediff_in_case",
			input:    "CASE WHEN DATEDIFF(end_date, start_date) > 30 THEN 'overdue' END",
			expected: "(end_date - start_date)",
		},
		{
			name:     "multiple_datediff",
			input:    "SELECT DATEDIFF(a, b), DATEDIFF(c, d)",
			expected: "(a - b)",
		},
		{
			name:     "nested_datediff",
			input:    "SELECT DATEDIFF(DATE(NOW()), created_at)",
			expected: "(DATE(NOW()) - created_at)",
		},
		{
			name:     "datediff_with_expression",
			input:    "SELECT DATEDIFF(end_date, DATE_ADD(start_date, INTERVAL 1 DAY))",
			expected: "(end_date - DATE_ADD(start_date, INTERVAL 1 DAY))",
		},
		{
			name:     "datediff_in_comparison",
			input:    "IF(DATEDIFF(a, b) > 0, 1, 0)",
			expected: "(a - b)",
		},
		{
			name:     "no_datediff",
			input:    "SELECT 1",
			expected: "SELECT 1",
		},
	}
	
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := converter.processDateDiff(tt.input)
			if tt.expected != "" && !strings.Contains(result, tt.expected) {
				t.Errorf("processDateDiff(%q) = %q, want to contain %q", tt.input, result, tt.expected)
			}
			if tt.notExpected != "" && strings.Contains(result, tt.notExpected) {
				t.Errorf("processDateDiff(%q) = %q, not want to contain %q", tt.input, result, tt.notExpected)
			}
		})
	}
}

// ==================== TDD: 提高 fixSyntax 覆盖率 ====================

// TestFunctionConverter_fixSyntax 测试语法修复功能
func TestFunctionConverter_fixSyntax(t *testing.T) {
	converter := &FunctionConverter{}
	
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "if_with_semicolon",
			input:    "IF done = TRUE;",
			expected: "IF done = TRUE THEN",
		},
		{
			name:     "elseif_with_semicolon",
			input:    "ELSEIF done = TRUE;",
			expected: "ELSEIF done = TRUE THEN",
		},
		{
			name:     "else_with_semicolon",
			input:    "ELSE;",
			expected: "ELSE",
		},
		{
			name:     "double_then",
			input:    "IF condition THEN THEN",
			expected: "IF condition THEN",
		},
		{
			name:     "else_then",
			input:    "ELSE THEN",
			expected: "ELSE",
		},
		{
			name:     "empty_lines",
			input:    "SELECT 1;\n\n\nSELECT 2;",
			expected: "SELECT 1;\nSELECT 2;",
		},
		{
			name:     "then_end_if",
			input:    "THEN END IF",
			expected: "THEN\nEND IF;",
		},
		{
			name:     "complete_if_block",
			input:    "IF condition THEN\nSELECT 1;\nELSE\nSELECT 2;\nEND IF;",
			expected: "IF condition THEN\nSELECT 1;\nELSE\nSELECT 2;\nEND IF;",
		},
		{
			name:     "leave_label",
			input:    "LEAVE read_loop;",
			expected: "EXIT",
		},
		{
			name:     "iterate_label",
			input:    "ITERATE read_loop;",
			expected: "CONTINUE",
		},
	}
	
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			converter.body = tt.input
			converter.fixSyntax()
			if !strings.Contains(converter.body, tt.expected) {
				t.Errorf("fixSyntax(%q) = %q, want to contain %q", tt.input, converter.body, tt.expected)
			}
		})
	}
}

// ==================== TDD: 提高 fixLoopSyntax 覆盖率 ====================

// Test_fixLoopSyntax 测试循环语法修复功能
func Test_fixLoopSyntax(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "while_do",
			input:    "WHILE counter < 10 DO",
			expected: "WHILE counter < 10 LOOP",
		},
		{
			name:     "end_while",
			input:    "END WHILE;",
			expected: "END LOOP;",
		},
		{
			name:     "leave_loop",
			input:    "LEAVE my_loop;",
			expected: "EXIT",
		},
		{
			name:     "iterate_loop",
			input:    "ITERATE my_loop;",
			expected: "CONTINUE",
		},
		{
			name:     "loop_loop",
			input:    "LOOP LOOP",
			expected: "LOOP",
		},
		{
			name:     "end_loop_end_loop",
			input:    "END LOOP END LOOP",
			expected: "END LOOP;",
		},
		{
			name:     "loop_fetch",
			input:    "loop fetch; next from",
			expected: "\nFETCH NEXT FROM",
		},
		{
			name:     "while_complete",
			input:    "WHILE done = FALSE DO\nSELECT 1;\nEND WHILE;",
			expected: "WHILE done = FALSE LOOP\nSELECT 1;\nEND LOOP;",
		},
	}
	
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := fixLoopSyntax(tt.input)
			if !strings.Contains(result, tt.expected) {
				t.Errorf("fixLoopSyntax(%q) = %q, want to contain %q", tt.input, result, tt.expected)
			}
		})
	}
}

// ==================== TDD: 提高 convertBuiltinFunctions 覆盖率 ====================

// TestFunctionConverter_convertBuiltinFunctions 测试内置函数转换
func TestFunctionConverter_convertBuiltinFunctions(t *testing.T) {
	converter := &FunctionConverter{}
	
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		// CHAR_LENGTH
		{
			name:     "char_length",
			input:    "SELECT CHAR_LENGTH(name)",
			expected: "LENGTH(name)",
		},
		// REGEXP
		{
			name:     "regexp",
			input:    "WHERE name REGEXP '^[A-Z]'",
			expected: "WHERE name ~ '^[A-Z]'",
		},
		// NOW
		{
			name:     "now",
			input:    "SELECT NOW()",
			expected: "CURRENT_TIMESTAMP",
		},
		// CURRENT_DATE
		{
			name:     "current_date",
			input:    "SELECT CURRENT_DATE()",
			expected: "CURRENT_DATE",
		},
		// SYSDATE
		{
			name:     "sysdate",
			input:    "SELECT SYSDATE()",
			expected: "CURRENT_TIMESTAMP",
		},
		// UNIX_TIMESTAMP
		{
			name:     "unix_timestamp_no_arg",
			input:    "SELECT UNIX_TIMESTAMP()",
			expected: "EXTRACT(EPOCH FROM CURRENT_TIMESTAMP)",
		},
		{
			name:     "unix_timestamp_with_arg",
			input:    "SELECT UNIX_TIMESTAMP(created_at)",
			expected: "EXTRACT(EPOCH FROM created_at)",
		},
		// FROM_UNIXTIME
		{
			name:     "from_unixtime",
			input:    "SELECT FROM_UNIXTIME(ts)",
			expected: "TO_TIMESTAMP(ts)",
		},
		// SUBSTRING
		{
			name:     "substring_one_param",
			input:    "SELECT SUBSTRING(name FROM 1)",
			expected: "SUBSTRING(name FROM 1)",
		},
		{
			name:     "substring_two_params",
			input:    "SELECT SUBSTRING(name FROM 1 FOR 5)",
			expected: "SUBSTRING(name FROM 1 FOR 5)",
		},
		// REPLACE
		{
			name:     "replace",
			input:    "SELECT REPLACE(name, 'old', 'new')",
			expected: "REPLACE(name, 'old', 'new')",
		},
		// CEILING
		{
			name:     "ceiling",
			input:    "SELECT CEILING(value)",
			expected: "CEILING(value)",
		},
		// POWER
		{
			name:     "power",
			input:    "SELECT POWER(base, exp)",
			expected: "POWER(base, exp)",
		},
		// LOG10
		{
			name:     "log10",
			input:    "SELECT LOG10(value)",
			expected: "LOG10(value)",
		},
		// SIN/COS/TAN
		{
			name:     "sin",
			input:    "SELECT SIN(angle)",
			expected: "SIN(angle)",
		},
		{
			name:     "cos",
			input:    "SELECT COS(angle)",
			expected: "COS(angle)",
		},
		{
			name:     "tan",
			input:    "SELECT TAN(angle)",
			expected: "TAN(angle)",
		},
		// YEAR/MONTH/DAY
		{
			name:     "year",
			input:    "SELECT YEAR(created_at)",
			expected: "EXTRACT(YEAR FROM created_at)",
		},
		{
			name:     "month",
			input:    "SELECT MONTH(created_at)",
			expected: "EXTRACT(MONTH FROM created_at)",
		},
		{
			name:     "day",
			input:    "SELECT DAY(created_at)",
			expected: "EXTRACT(DAY FROM created_at)",
		},
		// REPEAT/UNTIL
		{
			name:     "repeat",
			input:    "REPEAT",
			expected: "LOOP",
		},
		{
			name:     "until",
			input:    "UNTIL done = TRUE; END REPEAT;",
			expected: "EXIT WHEN done = TRUE;",
		},
		// SET variable
		{
			name:     "set_var",
			input:    "SET counter = 0;",
			expected: "counter := 0;",
		},
		// RETURN
		{
			name:     "return",
			input:    "RETURN result;",
			expected: "RETURN result;",
		},
		// NULLIF
		{
			name:     "nullif",
			input:    "SELECT NULLIF(value, 0)",
			expected: "NULLIF(value, 0)",
		},
		// nullcase (should be NULL)
		{
			name:     "nullcase",
			input:    "SELECT nullcase",
			expected: "NULL",
		},
	}
	
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			converter.body = tt.input
			converter.convertBuiltinFunctions()
			if !strings.Contains(converter.body, tt.expected) {
				t.Errorf("convertBuiltinFunctions(%q) = %q, want to contain %q", tt.input, converter.body, tt.expected)
			}
		})
	}
}

// ==================== TDD: 提高 applySpecificPatches 覆盖率 ====================

// TestFunctionConverter_applySpecificPatches_EdgeCases 测试特殊补丁应用的边界情况
func TestFunctionConverter_applySpecificPatches_EdgeCases(t *testing.T) {
	converter := &FunctionConverter{}
	
	tests := []struct {
		name     string
		funcName string
		input    string
		check    func(string) bool
	}{
		{
			name:     "not_complex_join_function",
			funcName: "some_other_function",
			input:    "if v_done then exit;",
			check: func(s string) bool {
				// 非 complex_join_function 不应该被修改
				return s == "if v_done then exit;"
			},
		},
		{
			name:     "not_comprehensive_reporting",
			funcName: "some_other_function",
			input:    "SET v_row_index = -1;",
			check: func(s string) bool {
				// 非 comprehensive_reporting 不应该被修改
				return s == "SET v_row_index = -1;"
			},
		},
		{
			name:     "complex_join_with_return",
			funcName: "complex_join_function",
			input:    "close cur; return update_count;",
			check: func(s string) bool {
				return strings.Contains(s, "close cur;") && 
				       strings.Contains(s, "return v_result;")
			},
		},
		{
			name:     "comprehensive_reporting_row_number",
			funcName: "comprehensive_reporting",
			input:    "v_row_index := v_row_index + 1",
			check: func(s string) bool {
				return strings.Contains(s, "ROW_NUMBER() OVER (ORDER BY amount) - 1")
			},
		},

		{
			name:     "comprehensive_reporting_set_index",
			funcName: "comprehensive_reporting",
			input:    "set v_row_index = -1;",
			check: func(s string) bool {
				return !strings.Contains(s, "set v_row_index = -1;")
			},
		},
	}
	
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			converter.mysqlFunc = mysql.FunctionInfo{Name: tt.funcName}
			converter.body = tt.input
			converter.applySpecificPatches()
			if !tt.check(converter.body) {
				t.Errorf("applySpecificPatches(%q) check failed, got: %q", tt.name, converter.body)
			}
		})
	}
}

// ==================== TDD: 提高 parseParameters 覆盖率 ====================

// TestFunctionConverter_parseParameters 测试参数解析
func TestFunctionConverter_parseParameters(t *testing.T) {
	tests := []struct {
		name        string
		ddl         string
		wantParams  string
		wantErr     bool
	}{
		{
			name:       "simple_int_param",
			ddl:        "CREATE FUNCTION f(a INT) RETURNS INT BEGIN RETURN a; END",
			wantParams: "a INT",
			wantErr:    false,
		},
		{
			name:       "multiple_params",
			ddl:        "CREATE FUNCTION f(a INT, b VARCHAR(50)) RETURNS INT BEGIN RETURN a; END",
			wantParams: "a INT, b VARCHAR(50)",
			wantErr:    false,
		},
		{
			name:       "datetime_param",
			ddl:        "CREATE FUNCTION f(dt DATETIME) RETURNS INT BEGIN RETURN 1; END",
			wantParams: "dt TIMESTAMP",
			wantErr:    false,
		},
		{
			name:       "tinyint_param",
			ddl:        "CREATE FUNCTION f(flag TINYINT) RETURNS INT BEGIN RETURN flag; END",
			wantParams: "flag SMALLINT",
			wantErr:    false,
		},
		{
			name:       "unsigned_param",
			ddl:        "CREATE FUNCTION f(id INT UNSIGNED) RETURNS INT BEGIN RETURN id; END",
			wantParams: "id INT",
			wantErr:    false,
		},
		{
			name:       "zerofill_param",
			ddl:        "CREATE FUNCTION f(val INT ZEROFILL) RETURNS INT BEGIN RETURN val; END",
			wantParams: "val INT",
			wantErr:    false,
		},
		{
			name:       "charset_param",
			ddl:        "CREATE FUNCTION f(name VARCHAR(50) CHARACTER SET utf8) RETURNS INT BEGIN RETURN 1; END",
			wantParams: "name VARCHAR(50)",
			wantErr:    false,
		},
		{
			name:       "collate_param",
			ddl:        "CREATE FUNCTION f(name VARCHAR(50) COLLATE utf8_unicode_ci) RETURNS INT BEGIN RETURN 1; END",
			wantParams: "name VARCHAR(50)",
			wantErr:    false,
		},
		{
			name:       "no_params",
			ddl:        "CREATE FUNCTION f() RETURNS INT BEGIN RETURN 1; END",
			wantParams: "",
			wantErr:    false,
		},
		{
			name:       "missing_parenthesis",
			ddl:        "CREATE FUNCTION f RETURNS INT BEGIN RETURN 1; END",
			wantErr:    true,
		},
	}
	
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			converter := NewFunctionConverter(mysql.FunctionInfo{
				Name: "f",
				DDL:  tt.ddl,
			})
			
			err := converter.parseParameters()
			if (err != nil) != tt.wantErr {
				t.Errorf("parseParameters() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && converter.parameters != tt.wantParams {
				t.Errorf("parseParameters() parameters = %q, want %q", converter.parameters, tt.wantParams)
			}
		})
	}
}
