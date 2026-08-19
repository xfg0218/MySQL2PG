package postgres

import (
	"strings"
	"testing"
)

// TestConvertTableDDL_DecimalUnsignedCheck 验证 DECIMAL 系 UNSIGNED 列
// 转换后补充 CHECK (col >= 0) 非负约束（PostgreSQL 无无符号类型）
func TestConvertTableDDL_DecimalUnsignedCheck(t *testing.T) {
	tests := []struct {
		name      string
		ddl       string
		wantCheck string // 必须包含的 CHECK 子串，空则不要求
		rejectSub string // 不得包含的子串
		wantSub   string // 必须包含的其他子串，空则不要求
	}{
		{
			name: "decimal unsigned 补 CHECK 约束",
			ddl: "CREATE TABLE `orders` (\n" +
				"  `amount` decimal(10,2) unsigned NOT NULL DEFAULT '0.00',\n" +
				"  `name` varchar(20)\n" +
				") ENGINE=InnoDB;",
			wantCheck: `CHECK ("amount" >= 0)`,
		},
		{
			name: "decimal 无 unsigned 不生成 CHECK",
			ddl: "CREATE TABLE `t` (\n" +
				"  `amount` decimal(10,2) NOT NULL\n" +
				");",
			rejectSub: "CHECK",
		},
		{
			name: "float unsigned 补 CHECK 约束",
			ddl: "CREATE TABLE `t` (\n" +
				"  `score` float unsigned DEFAULT NULL\n" +
				");",
			wantCheck: `CHECK ("score" >= 0)`,
		},
		{
			name: "double unsigned 补 CHECK 约束",
			ddl: "CREATE TABLE `t` (\n" +
				"  `val` double unsigned DEFAULT NULL\n" +
				");",
			wantCheck: `CHECK ("val" >= 0)`,
		},
		{
			name: "zerofill 隐含 unsigned 同样补 CHECK",
			ddl: "CREATE TABLE `t` (\n" +
				"  `amount` decimal(8,2) zerofill\n" +
				");",
			wantCheck: `CHECK ("amount" >= 0)`,
		},
		{
			name: "int unsigned 走类型提升不生成 CHECK",
			ddl: "CREATE TABLE `t` (\n" +
				"  `c` int unsigned DEFAULT NULL\n" +
				");",
			rejectSub: "CHECK",
			wantSub:   "BIGINT",
		},
		{
			name: "bigint unsigned 提升 NUMERIC(20,0) 不生成 CHECK",
			ddl: "CREATE TABLE `t` (\n" +
				"  `uid` bigint unsigned NOT NULL\n" +
				");",
			rejectSub: "CHECK",
			wantSub:   "NUMERIC(20,0)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := ConvertTableDDL(tt.ddl, false)
			if err != nil {
				t.Fatalf("ConvertTableDDL() error = %v", err)
			}
			if tt.wantCheck != "" && !strings.Contains(result.DDL, tt.wantCheck) {
				t.Errorf("转换结果缺少 %q，实际 DDL:\n%s", tt.wantCheck, result.DDL)
			}
			if tt.rejectSub != "" && strings.Contains(result.DDL, tt.rejectSub) {
				t.Errorf("转换结果不应包含 %q，实际 DDL:\n%s", tt.rejectSub, result.DDL)
			}
			if tt.wantSub != "" && !strings.Contains(result.DDL, tt.wantSub) {
				t.Errorf("转换结果缺少 %q，实际 DDL:\n%s", tt.wantSub, result.DDL)
			}
		})
	}
}

// TestConvertTableDDL_DecimalUnsignedLowercaseColumns 验证 lowercase_columns
// 开启时 CHECK 约束使用小写列名
func TestConvertTableDDL_DecimalUnsignedLowercaseColumns(t *testing.T) {
	ddl := "CREATE TABLE `t` (\n" +
		"  `Amount` decimal(10,2) unsigned NOT NULL\n" +
		");"

	result, err := ConvertTableDDL(ddl, true)
	if err != nil {
		t.Fatalf("ConvertTableDDL() error = %v", err)
	}
	if !strings.Contains(result.DDL, `CHECK ("amount" >= 0)`) {
		t.Errorf("小写列名 CHECK 缺失，实际 DDL:\n%s", result.DDL)
	}
}

// TestIsUnsignedDecimalLikeColumn 验证 DECIMAL 系 UNSIGNED 列检测的边界
func TestIsUnsignedDecimalLikeColumn(t *testing.T) {
	tests := []struct {
		name string
		line string
		want bool
	}{
		{"decimal unsigned 反引号", "`amount` decimal(10,2) unsigned NOT NULL", true},
		{"decimal zerofill", "`amount` decimal(10,2) zerofill", true},
		{"float unsigned 无反引号", "score float unsigned DEFAULT NULL", true},
		{"double precision unsigned", "`v` double precision unsigned", true},
		{"decimal 无修饰不算", "`amount` decimal(10,2) NOT NULL", false},
		{"int unsigned 不算（整数走类型提升）", "`c` int unsigned NOT NULL", false},
		{"varchar 默认值含 unsigned 字样不算", "`c` varchar(20) DEFAULT 'unsigned'", false},
		{"real unsigned", "`r` real unsigned", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isUnsignedDecimalLikeColumn(tt.line); got != tt.want {
				t.Errorf("isUnsignedDecimalLikeColumn(%q) = %v, want %v", tt.line, got, tt.want)
			}
		})
	}
}
