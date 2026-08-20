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
		if !strings.Contains(out, "-- [mysql2pg] MySQL HANDLER 无法自动转换") {
			t.Errorf("SQLEXCEPTION handler 应以注释保留，实际：%s", out)
		}
	})
}
