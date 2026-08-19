package postgres

import (
	"strings"
	"testing"
)

// TestConvertTableDDL_TimestampToTimestamptz 验证 MySQL TIMESTAMP 映射为 PG TIMESTAMPTZ
// （带时区语义），DATETIME 保持朴素 TIMESTAMP；二者共存时互不污染
func TestConvertTableDDL_TimestampToTimestamptz(t *testing.T) {
	tests := []struct {
		name   string
		ddl    string
		expect []string // 转换结果必须包含的子串
		reject []string // 转换结果不得包含的子串
	}{
		{
			name: "timestamp 映射为 TIMESTAMPTZ",
			ddl: "CREATE TABLE `t` (\n" +
				"  `id` int NOT NULL AUTO_INCREMENT,\n" +
				"  `ts` timestamp NULL DEFAULT NULL,\n" +
				"  PRIMARY KEY (`id`)\n" +
				") ENGINE=InnoDB;",
			expect: []string{`"ts" TIMESTAMPTZ`},
		},
		{
			name: "timestamp(6) 保留精度映射为 TIMESTAMPTZ(6)",
			ddl: "CREATE TABLE `t` (\n" +
				"  `ts6` timestamp(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)\n" +
				");",
			expect: []string{`"ts6" TIMESTAMPTZ(6)`},
		},
		{
			name: "datetime 保持 TIMESTAMP 且不被误转为 TIMESTAMPTZ",
			ddl: "CREATE TABLE `t` (\n" +
				"  `dt` datetime DEFAULT NULL,\n" +
				"  `dt3` datetime(3) DEFAULT NULL\n" +
				");",
			expect: []string{`"dt" TIMESTAMP`, `"dt3" TIMESTAMP(3)`},
			reject: []string{"TIMESTAMPTZ"},
		},
		{
			name: "timestamp 与 datetime 共存互不污染",
			ddl: "CREATE TABLE `t` (\n" +
				"  `created_at` datetime NOT NULL,\n" +
				"  `updated_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP\n" +
				");",
			expect: []string{`"created_at" TIMESTAMP`, `"updated_at" TIMESTAMPTZ`},
		},
		{
			name: "timestamp 默认值 CURRENT_TIMESTAMP 保留",
			ddl: "CREATE TABLE `t` (\n" +
				"  `ts` timestamp NULL DEFAULT CURRENT_TIMESTAMP\n" +
				");",
			// 类型定义统一小写化，current_timestamp 为合法 PG 表达式
			expect: []string{`"ts" TIMESTAMPTZ`, "current_timestamp"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := ConvertTableDDL(tt.ddl, false)
			if err != nil {
				t.Fatalf("ConvertTableDDL() error = %v", err)
			}
			for _, want := range tt.expect {
				if !strings.Contains(result.DDL, want) {
					t.Errorf("转换结果缺少 %q，实际 DDL:\n%s", want, result.DDL)
				}
			}
			for _, bad := range tt.reject {
				if strings.Contains(result.DDL, bad) {
					t.Errorf("转换结果不应包含 %q，实际 DDL:\n%s", bad, result.DDL)
				}
			}
		})
	}
}
