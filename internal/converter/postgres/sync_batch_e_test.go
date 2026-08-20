package postgres

import (
	"strings"
	"testing"

	"github.com/yourusername/mysql2pg/internal/mysql"
)

// TestConvertViewDDL_TailLiteralProtection 视图管道后段词替换不破坏字面量（P1-13）
func TestConvertViewDDL_TailLiteralProtection(t *testing.T) {
	tests := []struct {
		name     string
		viewSQL  string
		contains []string // 字面量内容必须原样保留
		rejects  []string // 不应出现（字面量被误替换的痕迹）
	}{
		{
			name:     "字面量内的 database() 不被替换",
			viewSQL:  "SELECT 'database() info' AS note, database() AS db FROM t1",
			contains: []string{"'database() info'"},
		},
		{
			name:     "字面量内的 year( 不被替换",
			viewSQL:  "SELECT 'year(x) example' AS note, year(d) AS y FROM t1",
			contains: []string{"'year(x) example'", "extract(year from"},
		},
		{
			name:     "字面量内的 user 不被替换",
			viewSQL:  "SELECT 'user: admin' AS note, user() AS u FROM t1",
			contains: []string{"'user: admin'", "current_user"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ddl, err := ConvertViewDDL("v_tail_literal", tt.viewSQL)
			if err != nil {
				t.Fatalf("ConvertViewDDL 返回错误：%v", err)
			}
			lower := strings.ToLower(ddl)
			for _, want := range tt.contains {
				if !strings.Contains(lower, strings.ToLower(want)) {
					t.Errorf("缺少 %q，实际：%s", want, ddl)
				}
			}
			for _, bad := range tt.rejects {
				if strings.Contains(lower, strings.ToLower(bad)) {
					t.Errorf("不应包含 %q，实际：%s", bad, ddl)
				}
			}
		})
	}
}

// TestConvertMySQLDateFormatUnified 统一日期格式转换器的说明符覆盖（P1-12）
func TestConvertMySQLDateFormatUnified(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		{"基础日期", "'%Y-%m-%d'", "'YYYY-MM-DD'"},
		{"完整时间", "'%Y-%m-%d %H:%i:%s'", "'YYYY-MM-DD HH24:MI:SS'"},
		{"12小时制", "'%r'", "'HH12:MI:SS AM'"},
		{"24小时制", "'%T'", "'HH24:MI:SS'"},
		{"微秒", "'%f'", "'US'"},
		{"字面百分号", "'100%%'", "'100%'"},
		{"星期与月份名", "'%W %M'", "'Day Month'"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := convertMySQLDateFormatToPG(tt.in); got != tt.want {
				t.Errorf("convertMySQLDateFormatToPG(%s) = %s, want %s", tt.in, got, tt.want)
			}
		})
	}
}

// TestFunctionHandlerConversion DECLARE HANDLER 转换（P1-14）
func TestFunctionHandlerConversion(t *testing.T) {
	t.Run("NOT FOUND handler 被移除（语义由 FETCH 转换覆盖）", func(t *testing.T) {
		out, err := ConvertFunctionDDL(mysql.FunctionInfo{
			Name: "f_cursor",
			DDL: "CREATE FUNCTION f_cursor() RETURNS INT DETERMINISTIC BEGIN " +
				"DECLARE done INT DEFAULT 0; " +
				"DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = 1; " +
				"RETURN 1; END",
		})
		if err != nil {
			t.Fatalf("ConvertFunctionDDL 返回错误：%v", err)
		}
		if strings.Contains(out, "HANDLER FOR") {
			t.Errorf("NOT FOUND handler 应被移除，实际：%s", out)
		}
	})

	t.Run("SQLEXCEPTION handler 以注释保留", func(t *testing.T) {
		out, err := ConvertFunctionDDL(mysql.FunctionInfo{
			Name: "f_err",
			DDL: "CREATE FUNCTION f_err() RETURNS INT DETERMINISTIC BEGIN " +
				"DECLARE EXIT HANDLER FOR SQLEXCEPTION RETURN -1; " +
				"RETURN 1; END",
		})
		if err != nil {
			t.Fatalf("ConvertFunctionDDL 返回错误：%v", err)
		}
		if !strings.Contains(out, "/* [mysql2pg] MySQL HANDLER 无法自动转换") {
			t.Errorf("SQLEXCEPTION handler 应以注释保留，实际：%s", out)
		}
	})

	// P2-16 回归：多行 BEGIN 块 handler 必须整体被注释，
	// 块内剩余语句不得泄漏进函数体（旧非贪婪匹配只吞到第一个分号）
	t.Run("多行 BEGIN 块 handler 不泄漏", func(t *testing.T) {
		out, err := ConvertFunctionDDL(mysql.FunctionInfo{
			Name: "f_block",
			DDL: "CREATE FUNCTION f_block() RETURNS INT DETERMINISTIC BEGIN " +
				"DECLARE EXIT HANDLER FOR SQLEXCEPTION BEGIN ROLLBACK; RETURN -1; END; " +
				"RETURN 1; END",
		})
		if err != nil {
			t.Fatalf("ConvertFunctionDDL 返回错误：%v", err)
		}
		if !strings.Contains(out, "/* [mysql2pg] MySQL HANDLER 无法自动转换") {
			t.Errorf("多行 handler 应以块注释保留，实际：%s", out)
		}
		// 块注释必须以 */ 闭合，且块尾的孤立 END 不得泄漏
		if !strings.Contains(out, "*/") {
			t.Errorf("块注释未闭合，实际：%s", out)
		}
		if strings.Count(out, "RETURN -1") != 1 || !strings.Contains(out, "/* [mysql2pg]") {
			t.Errorf("handler 块内容位置异常，实际：%s", out)
		}
	})

	// P2-16：SIGNAL SQLSTATE 转 RAISE EXCEPTION
	t.Run("SIGNAL 转 RAISE EXCEPTION", func(t *testing.T) {
		out, err := ConvertFunctionDDL(mysql.FunctionInfo{
			Name: "f_signal",
			DDL: "CREATE FUNCTION f_signal(v INT) RETURNS INT DETERMINISTIC BEGIN " +
				"IF v < 0 THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'v 不能为负'; END IF; " +
				"RETURN v; END",
		})
		if err != nil {
			t.Fatalf("ConvertFunctionDDL 返回错误：%v", err)
		}
		if strings.Contains(out, "SIGNAL") {
			t.Errorf("SIGNAL 应被转换，实际：%s", out)
		}
		if !strings.Contains(out, "RAISE EXCEPTION USING ERRCODE = '45000', MESSAGE = 'v 不能为负';") {
			t.Errorf("缺少 RAISE EXCEPTION 转换结果，实际：%s", out)
		}
	})

	// P2-16：无 MESSAGE_TEXT 的 SIGNAL 与 RESIGNAL
	t.Run("SIGNAL 无 MESSAGE 与 RESIGNAL", func(t *testing.T) {
		out, err := ConvertFunctionDDL(mysql.FunctionInfo{
			Name: "f_signal2",
			DDL: "CREATE FUNCTION f_signal2(v INT) RETURNS INT DETERMINISTIC BEGIN " +
				"IF v < 0 THEN SIGNAL SQLSTATE '45001'; END IF; " +
				"RETURN v; END",
		})
		if err != nil {
			t.Fatalf("ConvertFunctionDDL 返回错误：%v", err)
		}
		if !strings.Contains(out, "RAISE EXCEPTION USING ERRCODE = '45001';") {
			t.Errorf("缺少无 MESSAGE 的 RAISE 转换，实际：%s", out)
		}
	})
}
