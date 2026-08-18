package postgres

import (
	"strings"
	"testing"

	"github.com/yourusername/mysql2pg/internal/mysql"
)

// TestCompressWhitespaceOutsideLiterals 空白压缩必须保留字面量内的连续空白
func TestCompressWhitespaceOutsideLiterals(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		{
			name: "代码区空白压缩，字面量内保留",
			in:   "SELECT  a   FROM t WHERE b = 'x    y'",
			want: "SELECT a FROM t WHERE b = 'x    y'",
		},
		{
			name: "反引号标识符内空白保留",
			in:   "SELECT  `col  name`   FROM t",
			want: "SELECT `col  name` FROM t",
		},
		{
			name: "双引号字面量内空白保留",
			in:   `SELECT  "a   b"  FROM t`,
			want: `SELECT "a   b" FROM t`,
		},
		{
			name: "段间单个空格分隔保留",
			in:   "SELECT 'x'   ,   'y'",
			want: "SELECT 'x' , 'y'",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := compressWhitespaceOutsideLiterals(tt.in); got != tt.want {
				t.Errorf("compressWhitespaceOutsideLiterals(%q) = %q, want %q", tt.in, got, tt.want)
			}
		})
	}
}

// TestReplaceBackticksOutsideLiterals 反引号替换不得触碰字面量内部
func TestReplaceBackticksOutsideLiterals(t *testing.T) {
	in := "SELECT `col` FROM `t1` WHERE note = 'a`b'"
	got := replaceBackticksOutsideLiterals(in)
	want := `SELECT "col" FROM "t1" WHERE note = 'a` + "`" + `b'`
	if got != want {
		t.Errorf("replaceBackticksOutsideLiterals(%q) = %q, want %q", in, got, want)
	}
}

// TestFindKeywordOutsideLiterals 关键字查找忽略字面量内部
func TestFindKeywordOutsideLiterals(t *testing.T) {
	sql := "SELECT 'call ifnull(a,b)' , ifnull(x, 1) FROM t"
	// 字面量内的 ifnull 不算，应定位到字面量之外的 ifnull
	pos := findKeywordOutsideLiterals(sql, "ifnull", 0)
	if pos == -1 {
		t.Fatal("应找到字面量之外的 ifnull")
	}
	if !strings.HasPrefix(sql[pos:], "ifnull(x") {
		t.Errorf("定位错误：pos=%d, 上下文=%q", pos, sql[pos:])
	}

	// 仅存在于字面量内时返回 -1
	sql2 := "SELECT 'ifnull(a,b)' FROM t"
	if pos := findKeywordOutsideLiterals(sql2, "ifnull", 0); pos != -1 {
		t.Errorf("字面量内的 ifnull 不应被找到, pos=%d", pos)
	}
}

// TestFindMatchingParenQuoteAware 括号匹配必须跳过字符串内的括号
func TestFindMatchingParenQuoteAware(t *testing.T) {
	input := `f(a, ')((', b)`
	idx := strings.Index(input, "(")
	end := findMatchingParen(input, idx)
	if end != len(input)-1 {
		t.Errorf("findMatchingParen(%q) = %d, want %d", input, end, len(input)-1)
	}

	input2 := `f(a, "x)y", b)`
	idx2 := strings.Index(input2, "(")
	end2 := findMatchingParen(input2, idx2)
	if end2 != len(input2)-1 {
		t.Errorf("findMatchingParen(%q) = %d, want %d", input2, end2, len(input2)-1)
	}

	// 反斜杠转义的引号不结束字符串
	input3 := `f('a\'(b')`
	idx3 := strings.Index(input3, "(")
	end3 := findMatchingParen(input3, idx3)
	if end3 != len(input3)-1 {
		t.Errorf("findMatchingParen(%q) = %d, want %d", input3, end3, len(input3)-1)
	}
}

// TestConvertViewDDL_LiteralProtection 视图转换不得破坏字符串字面量内容
func TestConvertViewDDL_LiteralProtection(t *testing.T) {
	tests := []struct {
		name     string
		viewSQL  string
		contains []string // 结果必须包含
		rejects  []string // 结果不得包含
	}{
		{
			name:     "字面量内的 IFNULL 不被替换，外层正常替换",
			viewSQL:  "SELECT IFNULL(name, 'IFNULL(a,b)') AS n FROM t1",
			contains: []string{`coalesce(name, 'IFNULL(a,b)')`},
		},
		{
			name:     "字面量内连续空白保留",
			viewSQL:  "SELECT 'a    b' AS s FROM t1",
			contains: []string{`'a    b'`},
			rejects:  []string{`'a b'`},
		},
		{
			name:     "字面量内的 to_days 不被转换，外层正常转换",
			viewSQL:  "SELECT to_days(dt) AS d, 'to_days(x)' AS s FROM t1",
			contains: []string{`'to_days(x)'`, "extract(epoch"},
		},
		{
			name:     "字面量含括号不破坏 concat 参数切分",
			viewSQL:  "SELECT concat(name, ' (a) ') AS c FROM t1",
			contains: []string{`' (a) '`, "||"},
		},
		{
			name:     "字面量内的反引号保留",
			viewSQL:  "SELECT 'a`b' AS s FROM `t1`",
			contains: []string{"'a`b'", `"t1"`},
		},
		{
			name:     "反斜杠转义引号的字面量保持完整",
			viewSQL:  "SELECT 'it\\'s IFNULL(' AS s FROM t1",
			contains: []string{`'it\'s IFNULL('`},
		},
		{
			name:     "字面量内的 ISNULL 不被替换",
			viewSQL:  "SELECT ISNULL(x) AS a, 'ISNULL(y)' AS b FROM t1",
			contains: []string{`'ISNULL(y)'`, `is null`},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ddl, err := ConvertViewDDL("v_literal_protect", tt.viewSQL)
			if err != nil {
				t.Fatalf("ConvertViewDDL 返回错误：%v", err)
			}
			for _, want := range tt.contains {
				if !strings.Contains(ddl, want) {
					t.Errorf("转换结果缺少 %q，实际：%s", want, ddl)
				}
			}
			for _, bad := range tt.rejects {
				if strings.Contains(ddl, bad) {
					t.Errorf("转换结果不应包含 %q，实际：%s", bad, ddl)
				}
			}
		})
	}
}

// TestConvertFunctionDDL_LiteralProtection 函数体转换不得破坏字符串字面量内容
func TestConvertFunctionDDL_LiteralProtection(t *testing.T) {
	tests := []struct {
		name     string
		ddl      string
		contains []string
	}{
		{
			name:     "函数体字面量内的 IFNULL 不被替换",
			ddl:      "CREATE FUNCTION f_lit_ifnull(a INT) RETURNS VARCHAR(50) DETERMINISTIC BEGIN RETURN IFNULL(a, 'IFNULL(x,y)'); END",
			contains: []string{`'IFNULL(x,y)'`, "COALESCE("},
		},
		{
			name:     "字面量内的 unsigned 字样不被剥离",
			ddl:      "CREATE FUNCTION f_lit_unsigned() RETURNS VARCHAR(20) DETERMINISTIC BEGIN DECLARE v VARCHAR(20) DEFAULT 'unsigned'; RETURN v; END",
			contains: []string{`'unsigned'`},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			out, err := ConvertFunctionDDL(mysql.FunctionInfo{Name: "lit_test", DDL: tt.ddl})
			if err != nil {
				t.Fatalf("ConvertFunctionDDL 返回错误：%v", err)
			}
			for _, want := range tt.contains {
				if !strings.Contains(out, want) {
					t.Errorf("转换结果缺少 %q，实际：%s", want, out)
				}
			}
		})
	}
}

// TestLiteralMaskRoundTrip 遮蔽/恢复往返一致性
func TestLiteralMaskRoundTrip(t *testing.T) {
	sql := "SELECT ifnull(a, 'x(y)') , 'it\\'s' , \"dq\" FROM `t` WHERE b = 'multi  space'"
	mask := newLiteralMask()
	masked := mask.mask(sql)
	// 遮蔽后不应再有引号包裹的字面量（反引号标识符不在遮蔽范围）
	if strings.Contains(masked, "'x(y)'") || strings.Contains(masked, "'multi  space'") {
		t.Fatalf("遮蔽失败：%s", masked)
	}
	// 占位符必须能穿过小写化与空白压缩
	masked = strings.ToLower(masked)
	if got := mask.unmask(masked); !strings.HasPrefix(got, "select ifnull(a, 'x(y)'") {
		t.Errorf("往返恢复失败：%s", got)
	}
}
