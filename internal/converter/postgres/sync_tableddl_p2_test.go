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

// TestParseEnumValues P2-02：ENUM 值列表解析（引号/括号感知）
func TestParseEnumValues(t *testing.T) {
	tests := []struct {
		name string
		line string
		want []string
	}{
		{"普通值列表", "`status` enum('active','inactive') NOT NULL", []string{"active", "inactive"}},
		{"值含右括号", "`note` enum('a)b','c')", []string{"a)b", "c"}},
		{"值含双写单引号", "`note` enum('it''s')", []string{"it's"}},
		{"值含反斜杠转义单引号", "`note` enum('a\\'b')", []string{"a'b"}},
		{"非 enum 列", "`c` int NOT NULL", nil},
		{"字面量内的 enum( 不误判", "`c` varchar(20) DEFAULT 'enum('", nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := parseEnumValues(tt.line)
			if len(got) != len(tt.want) {
				t.Fatalf("parseEnumValues(%q) = %v, want %v", tt.line, got, tt.want)
			}
			for i := range got {
				if got[i] != tt.want[i] {
					t.Errorf("parseEnumValues(%q)[%d] = %q, want %q", tt.line, i, got[i], tt.want[i])
				}
			}
		})
	}
}

// TestIsSetTypeColumn P2-02：SET 类型检测，生成列中的 json_set 不得误判
func TestIsSetTypeColumn(t *testing.T) {
	tests := []struct {
		name string
		line string
		want bool
	}{
		{"SET 列", "`tags` set('a','b') NULL", true},
		{"普通列", "`c` int", false},
		{"生成列 json_set 不误判", "`c1` json GENERATED ALWAYS AS (json_set(`doc`, '$.a', 1)) STORED", false},
		{"字面量内的 set( 不误判", "`c` varchar(20) DEFAULT 'set('", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isSetTypeColumn(tt.line); got != tt.want {
				t.Errorf("isSetTypeColumn(%q) = %v, want %v", tt.line, got, tt.want)
			}
		})
	}
}

// TestConvertTableDDL_EnumCheckConstraint P2-02：ENUM 转 VARCHAR(255) 并生成 CHECK IN 约束
func TestConvertTableDDL_EnumCheckConstraint(t *testing.T) {
	mysqlDDL := "CREATE TABLE `t_enum` (\n" +
		"  `id` bigint NOT NULL,\n" +
		"  `status` enum('active','inactive') NOT NULL DEFAULT 'active',\n" +
		"  `note` enum('a)b','it''s') NULL,\n" +
		"  PRIMARY KEY (`id`)\n" +
		") ENGINE=InnoDB"

	result, err := ConvertTableDDL(mysqlDDL, true)
	if err != nil {
		t.Fatalf("ConvertTableDDL 返回错误：%v", err)
	}

	if !strings.Contains(result.DDL, `"status" VARCHAR(255) not null default 'active' CHECK ("status" IN ('active', 'inactive'))`) {
		t.Errorf("status 列缺少 CHECK IN 约束：%s", result.DDL)
	}
	// 值含右括号与单引号：完整保留且按 PG 规则转义
	if !strings.Contains(result.DDL, `CHECK ("note" IN ('a)b', 'it''s'))`) {
		t.Errorf("note 列 CHECK 约束值列表错误：%s", result.DDL)
	}
}

// TestConvertTableDDL_SetColumnWarning P2-02：SET 列转 VARCHAR(255) 且记入降级告警
func TestConvertTableDDL_SetColumnWarning(t *testing.T) {
	mysqlDDL := "CREATE TABLE `t_set` (\n" +
		"  `id` bigint NOT NULL,\n" +
		"  `tags` set('a','b','c') NULL,\n" +
		"  PRIMARY KEY (`id`)\n" +
		") ENGINE=InnoDB"

	result, err := ConvertTableDDL(mysqlDDL, true)
	if err != nil {
		t.Fatalf("ConvertTableDDL 返回错误：%v", err)
	}

	if !strings.Contains(result.DDL, `"tags" VARCHAR(255)`) {
		t.Errorf("tags 列未转为 VARCHAR(255)：%s", result.DDL)
	}
	if strings.Contains(result.DDL, "IN (") {
		t.Errorf("SET 列不应生成 CHECK IN 约束：%s", result.DDL)
	}
	found := false
	for _, w := range result.Warnings {
		if strings.Contains(w, "SET 类型的多值组合语义") {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("缺少 SET 降级告警：%v", result.Warnings)
	}
}

// TestConvertTableDDL_InvisibleAndSrid P2-01：INVISIBLE/SRID 属性剥离并记入降级告警
func TestConvertTableDDL_InvisibleAndSrid(t *testing.T) {
	mysqlDDL := "CREATE TABLE `t_attrs` (\n" +
		"  `id` bigint NOT NULL,\n" +
		"  `c1` int INVISIBLE,\n" +
		"  `geom` point NOT NULL SRID 4326,\n" +
		"  PRIMARY KEY (`id`)\n" +
		") ENGINE=InnoDB"

	result, err := ConvertTableDDL(mysqlDDL, true)
	if err != nil {
		t.Fatalf("ConvertTableDDL 返回错误：%v", err)
	}

	if strings.Contains(result.DDL, "INVISIBLE") || strings.Contains(result.DDL, "invisible") {
		t.Errorf("INVISIBLE 未被剥离：%s", result.DDL)
	}
	if strings.Contains(result.DDL, "SRID") || strings.Contains(result.DDL, "srid") {
		t.Errorf("SRID 未被剥离：%s", result.DDL)
	}

	var hasInvisibleWarning, hasSridWarning bool
	for _, w := range result.Warnings {
		if strings.Contains(w, "INVISIBLE 属性无法迁移") {
			hasInvisibleWarning = true
		}
		if strings.Contains(w, "SRID 属性无法迁移") {
			hasSridWarning = true
		}
	}
	if !hasInvisibleWarning {
		t.Errorf("缺少 INVISIBLE 降级告警：%v", result.Warnings)
	}
	if !hasSridWarning {
		t.Errorf("缺少 SRID 降级告警：%v", result.Warnings)
	}
}
