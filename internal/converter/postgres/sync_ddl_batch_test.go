package postgres

import (
	"strings"
	"testing"
)

// TestParseCheckConstraint CHECK 约束解析（P1-02）
func TestParseCheckConstraint(t *testing.T) {
	tests := []struct {
		name          string
		line          string
		lowercaseCols bool
		wantSubstr    []string
		wantOK        bool
	}{
		{
			name:          "命名 CHECK 约束",
			line:          `CONSTRAINT "chk_age" CHECK ("age" > 18),`,
			lowercaseCols: true,
			wantSubstr:    []string{`ALTER TABLE "t1" ADD CONSTRAINT "chk_age" CHECK ("age" > 18);`},
			wantOK:        true,
		},
		{
			name:          "匿名 CHECK 约束",
			line:          `CHECK ("price" >= 0),`,
			lowercaseCols: false,
			wantSubstr:    []string{`ALTER TABLE "t1" ADD CHECK ("price" >= 0);`},
			wantOK:        true,
		},
		{
			name:          "表达式含 IFNULL 转换",
			line:          `CHECK (IFNULL("qty", 0) >= 0),`,
			lowercaseCols: false,
			wantSubstr:    []string{`COALESCE("qty", 0) >= 0`},
			wantOK:        true,
		},
		{
			name:          "双引号标识符按配置小写化",
			line:          `CHECK ("Age" > 0),`,
			lowercaseCols: true,
			wantSubstr:    []string{`CHECK ("age" > 0)`},
			wantOK:        true,
		},
		{
			name:   "非 CHECK 行",
			line:   `CONSTRAINT "fk_x" FOREIGN KEY ("a") REFERENCES "t2" ("id"),`,
			wantOK: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ddl, ok := parseCheckConstraint(tt.line, "t1", tt.lowercaseCols)
			if ok != tt.wantOK {
				t.Fatalf("ok = %v, want %v（ddl=%s）", ok, tt.wantOK, ddl)
			}
			for _, want := range tt.wantSubstr {
				if !strings.Contains(ddl, want) {
					t.Errorf("缺少 %q，实际：%s", want, ddl)
				}
			}
		})
	}
}

// TestConvertExpressionDefault 表达式默认值转换（P1-04）
func TestConvertExpressionDefault(t *testing.T) {
	tests := []struct {
		name        string
		line        string
		wantSubstr  string
		wantReject  string // 应不包含的内容
		wantWarning bool
	}{
		{
			name:       "UUID() 转 gen_random_uuid()",
			line:       `"id" char(36) DEFAULT (uuid())`,
			wantSubstr: `DEFAULT (gen_random_uuid())`,
		},
		{
			name:       "NOW() 转 CURRENT_TIMESTAMP",
			line:       `"ts" timestamp DEFAULT (now())`,
			wantSubstr: `DEFAULT (CURRENT_TIMESTAMP)`,
		},
		{
			name:        "不可转换表达式剥离并告警",
			line:        `"c" varchar(20) DEFAULT (concat('a', 'b'))`,
			wantReject:  "DEFAULT",
			wantWarning: true,
		},
		{
			name:       "无表达式默认值不受影响",
			line:       `"c" int DEFAULT 5`,
			wantSubstr: `DEFAULT 5`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, warning := convertExpressionDefault(tt.line, true)
			if tt.wantSubstr != "" && !strings.Contains(got, tt.wantSubstr) {
				t.Errorf("缺少 %q，实际：%s", tt.wantSubstr, got)
			}
			if tt.wantReject != "" && strings.Contains(got, tt.wantReject) {
				t.Errorf("不应包含 %q，实际：%s", tt.wantReject, got)
			}
			if tt.wantWarning && warning == "" {
				t.Error("应返回告警说明")
			}
		})
	}
}

// TestConvertTableDDL_CheckAndWarnings 集成：CHECK 约束收集与空间类型告警（P1-02/P1-05）
func TestConvertTableDDL_CheckAndWarnings(t *testing.T) {
	ddl := "CREATE TABLE `orders` (\n" +
		"  `id` int NOT NULL AUTO_INCREMENT,\n" +
		"  `age` int DEFAULT NULL,\n" +
		"  `uid` char(36) DEFAULT (uuid()),\n" +
		"  PRIMARY KEY (`id`),\n" +
		"  CONSTRAINT `chk_age` CHECK (`age` > 18)\n" +
		") ENGINE=InnoDB;"

	result, err := ConvertTableDDL(ddl, true)
	if err != nil {
		t.Fatalf("ConvertTableDDL 返回错误：%v", err)
	}

	if len(result.CheckConstraints) != 1 {
		t.Fatalf("CheckConstraints 数量 = %d, want 1: %v", len(result.CheckConstraints), result.CheckConstraints)
	}
	if !strings.Contains(result.CheckConstraints[0], `ADD CONSTRAINT "chk_age" CHECK ("age" > 18)`) {
		t.Errorf("CHECK 约束转换错误：%s", result.CheckConstraints[0])
	}
	if !strings.Contains(result.DDL, "gen_random_uuid()") {
		t.Errorf("DEFAULT (uuid()) 未转换：%s", result.DDL)
	}
}

// TestConvertTableDDL_SpatialWarning 空间类型告警（P1-05）
func TestConvertTableDDL_SpatialWarning(t *testing.T) {
	ddl := "CREATE TABLE `geo` (\n" +
		"  `id` int NOT NULL,\n" +
		"  `loc` point DEFAULT NULL\n" +
		") ENGINE=InnoDB;"

	result, err := ConvertTableDDL(ddl, true)
	if err != nil {
		t.Fatalf("ConvertTableDDL 返回错误：%v", err)
	}
	found := false
	for _, w := range result.Warnings {
		if strings.Contains(w, "PostGIS") {
			found = true
		}
	}
	if !found {
		t.Errorf("空间类型应产生 PostGIS 告警，实际警告：%v", result.Warnings)
	}
}
