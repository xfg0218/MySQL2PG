package postgres

import (
	"strings"
	"testing"
)

// TestCleanTypeDefinition_TinyInt1Boolean P2-03：tinyint(1) → BOOLEAN 映射恢复。
// 旧正则 \btinyint\(1\)\b 因结尾 \b 在 `)` 后永不匹配，
// tinyint(1) 静默落入 tinyint → SMALLINT 分支
func TestCleanTypeDefinition_TinyInt1Boolean(t *testing.T) {
	tests := []struct {
		name        string
		input       string
		wantContain string
		notContain  []string
	}{
		{"tinyint(1) 转 BOOLEAN", "tinyint(1)", "BOOLEAN", []string{"SMALLINT"}},
		{"大写 TINYINT(1) 转 BOOLEAN", "TINYINT(1) NOT NULL DEFAULT 1", "BOOLEAN", []string{"SMALLINT"}},
		{"tinyint(1) unsigned 转 BOOLEAN", "tinyint(1) unsigned", "BOOLEAN", []string{"SMALLINT"}},
		{"tinyint(4) 仍为 SMALLINT", "tinyint(4)", "SMALLINT", []string{"BOOLEAN"}},
		{"tinyint(10) 仍为 SMALLINT", "tinyint(10)", "SMALLINT", []string{"BOOLEAN"}},
		{"tinyint 无显示宽度仍为 SMALLINT", "tinyint", "SMALLINT", []string{"BOOLEAN"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := cleanTypeDefinition(tt.input)
			if !strings.Contains(got, tt.wantContain) {
				t.Errorf("cleanTypeDefinition(%q) = %q，不含 %s", tt.input, got, tt.wantContain)
			}
			for _, nc := range tt.notContain {
				if strings.Contains(got, nc) {
					t.Errorf("cleanTypeDefinition(%q) = %q，不应包含 %s", tt.input, got, nc)
				}
			}
		})
	}
}

// TestCleanTypeDefinition_PrecisionPreserved P2-03：删除 typePatterns/convertDataType
// 死代码后的精度保留回归——实际生效路径是 basicTypeRegexes 关键字替换 + 兜底清理
func TestCleanTypeDefinition_PrecisionPreserved(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"decimal(10,2)", "DECIMAL(10,2)"},
		{"decimal(10)", "DECIMAL(10)"},
		{"numeric(12,4)", "NUMERIC(12,4)"},
		{"datetime(6)", "TIMESTAMP(6)"},
		{"timestamp(3)", "TIMESTAMPTZ(3)"},
		{"varchar(255)", "VARCHAR(255)"},
		{"char(36)", "CHAR(36)"},
		{"time(3)", "TIME(3)"},
		{"float(10,2)", "REAL"},
		{"double(10,2)", "DOUBLE PRECISION"},
		{"json", "JSON"},
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got := cleanTypeDefinition(tt.input)
			if !strings.Contains(got, tt.want) {
				t.Errorf("cleanTypeDefinition(%q) = %q，不含 %s", tt.input, got, tt.want)
			}
		})
	}
}

// TestConvertTableDDL_BacktickInLiteral P2-04：字面量内的反引号不得被替换为双引号，
// 字面量外的反引号标识符正常转换为双引号
func TestConvertTableDDL_BacktickInLiteral(t *testing.T) {
	mysqlDDL := "CREATE TABLE `t1` (\n" +
		"  `id` bigint NOT NULL AUTO_INCREMENT,\n" +
		"  `note` varchar(100) DEFAULT 'a`b' COMMENT 'use `backtick` here',\n" +
		"  PRIMARY KEY (`id`)\n" +
		") ENGINE=InnoDB"

	result, err := ConvertTableDDL(mysqlDDL, true)
	if err != nil {
		t.Fatalf("ConvertTableDDL 返回错误：%v", err)
	}

	// 标识符反引号应转换为双引号
	if !strings.Contains(result.DDL, `"note"`) {
		t.Errorf("DDL 未包含转换后的标识符 \"note\"：%s", result.DDL)
	}

	// 注释字面量内的反引号原样保留
	comment, ok := result.ColumnComments["note"]
	if !ok {
		t.Fatalf("ColumnComments 缺少 note 的注释：%v", result.ColumnComments)
	}
	if !strings.Contains(comment, "`backtick`") {
		t.Errorf("注释字面量内的反引号被破坏：%q", comment)
	}

	// 默认值字面量内的反引号原样保留
	if !strings.Contains(result.DDL, "'a`b'") {
		t.Errorf("默认值字面量内的反引号被破坏：%s", result.DDL)
	}
}
