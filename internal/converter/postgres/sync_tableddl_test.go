package postgres

import (
	"fmt"
	"strings"
	"testing"
)

func TestParsePartitionInfo_RangeInt(t *testing.T) {
	mysqlDDL := `CREATE TABLE test_partition_01 (
  id int NOT NULL,
  issue_id int NOT NULL
) ENGINE=InnoDB
/*!50100 PARTITION BY RANGE (issue_id)
(PARTITION p0 VALUES LESS THAN (1000),
 PARTITION p1 VALUES LESS THAN (2000),
 PARTITION p2 VALUES LESS THAN (3000),
 PARTITION p3 VALUES LESS THAN (10000),
 PARTITION p4 VALUES LESS THAN MAXVALUE) */`

	info := parsePartitionInfo(mysqlDDL)
	if info == nil {
		t.Fatal("parsePartitionInfo returned nil")
	}
	if info.PartitionType != "RANGE" {
		t.Errorf("expected partition type RANGE, got %s", info.PartitionType)
	}
	if info.Expression != "issue_id" {
		t.Errorf("expected expression 'issue_id', got '%s'", info.Expression)
	}
	if len(info.RangeDefs) != 5 {
		t.Errorf("expected 5 partition definitions, got %d", len(info.RangeDefs))
	}
	if info.RangeDefs[0].name != "p0" {
		t.Errorf("expected first partition name 'p0', got '%s'", info.RangeDefs[0].name)
	}
	if info.RangeDefs[0].lessThan != "1000" {
		t.Errorf("expected first partition less than '1000', got '%s'", info.RangeDefs[0].lessThan)
	}
	if info.RangeDefs[4].lessThan != "MAXVALUE" {
		t.Errorf("expected last partition less than 'MAXVALUE', got '%s'", info.RangeDefs[4].lessThan)
	}
}

func TestParsePartitionInfo_RangeToDays(t *testing.T) {
	mysqlDDL := `CREATE TABLE test_partition_02 (
  id int NOT NULL,
  create_time datetime NOT NULL
) ENGINE=InnoDB
/*!50100 PARTITION BY RANGE (TO_DAYS(create_time))
(PARTITION p202501 VALUES LESS THAN (TO_DAYS('2025-02-01')),
 PARTITION p202502 VALUES LESS THAN (TO_DAYS('2025-03-01')),
 PARTITION p_future VALUES LESS THAN MAXVALUE) */`

	info := parsePartitionInfo(mysqlDDL)
	if info == nil {
		t.Fatal("parsePartitionInfo returned nil")
	}
	if info.PartitionType != "RANGE" {
		t.Errorf("expected partition type RANGE, got %s", info.PartitionType)
	}
	if info.Expression != "TO_DAYS(create_time)" {
		t.Errorf("expected expression 'TO_DAYS(create_time)', got '%s'", info.Expression)
	}
	if len(info.RangeDefs) != 3 {
		t.Errorf("expected 3 partition definitions, got %d", len(info.RangeDefs))
	}
	if info.RangeDefs[0].lessThan != "TO_DAYS('2025-02-01')" {
		t.Errorf("expected first partition less than \"TO_DAYS('2025-02-01')\", got '%s'", info.RangeDefs[0].lessThan)
	}
}

func TestParsePartitionInfo_RangeUnixTimestamp(t *testing.T) {
	mysqlDDL := `CREATE TABLE test_partition_03 (
  id int NOT NULL,
  create_time datetime NOT NULL
) ENGINE=InnoDB
/*!50100 PARTITION BY RANGE (UNIX_TIMESTAMP(create_time))
(PARTITION p202501 VALUES LESS THAN (UNIX_TIMESTAMP('2025-02-01')),
 PARTITION p202502 VALUES LESS THAN (UNIX_TIMESTAMP('2025-03-01'))) */`

	info := parsePartitionInfo(mysqlDDL)
	if info == nil {
		t.Fatal("parsePartitionInfo returned nil")
	}
	if info.PartitionType != "RANGE" {
		t.Errorf("expected partition type RANGE, got %s", info.PartitionType)
	}
	if info.Expression != "UNIX_TIMESTAMP(create_time)" {
		t.Errorf("expected expression 'UNIX_TIMESTAMP(create_time)', got '%s'", info.Expression)
	}
	if len(info.RangeDefs) != 2 {
		t.Errorf("expected 2 partition definitions, got %d", len(info.RangeDefs))
	}
}

func TestParsePartitionInfo_List(t *testing.T) {
	mysqlDDL := `CREATE TABLE test_partition_04 (
  id int NOT NULL,
  status int NOT NULL
) ENGINE=InnoDB
/*!50100 PARTITION BY LIST (status)
(PARTITION p0 VALUES IN (0),
 PARTITION p1 VALUES IN (1),
 PARTITION p2 VALUES IN (2,3)) */`

	info := parsePartitionInfo(mysqlDDL)
	if info == nil {
		t.Fatal("parsePartitionInfo returned nil")
	}
	if info.PartitionType != "LIST" {
		t.Errorf("expected partition type LIST, got %s", info.PartitionType)
	}
	if info.Expression != "status" {
		t.Errorf("expected expression 'status', got '%s'", info.Expression)
	}
	if len(info.ListDefs) != 3 {
		t.Errorf("expected 3 partition definitions, got %d", len(info.ListDefs))
	}
	if info.ListDefs[2].valuesIn != "2,3" {
		t.Errorf("expected third partition values '2,3', got '%s'", info.ListDefs[2].valuesIn)
	}
}

func TestParsePartitionInfo_Hash(t *testing.T) {
	mysqlDDL := `CREATE TABLE test_partition_05 (
  id int NOT NULL,
  issue_id int NOT NULL
) ENGINE=InnoDB
/*!50100 PARTITION BY HASH(issue_id)
PARTITIONS 8 */`

	info := parsePartitionInfo(mysqlDDL)
	if info == nil {
		t.Fatal("parsePartitionInfo returned nil")
	}
	if info.PartitionType != "HASH" {
		t.Errorf("expected partition type HASH, got %s", info.PartitionType)
	}
	if info.Expression != "issue_id" {
		t.Errorf("expected expression 'issue_id', got '%s'", info.Expression)
	}
	if info.PartitionCount != 8 {
		t.Errorf("expected partition count 8, got %d", info.PartitionCount)
	}
}

func TestParsePartitionInfo_Key(t *testing.T) {
	mysqlDDL := `CREATE TABLE test_partition_06 (
  id int NOT NULL,
  issue_id int NOT NULL
) ENGINE=InnoDB
/*!50100 PARTITION BY KEY(issue_id)
PARTITIONS 4 */`

	info := parsePartitionInfo(mysqlDDL)
	if info == nil {
		t.Fatal("parsePartitionInfo returned nil")
	}
	if info.PartitionType != "KEY" {
		t.Errorf("expected partition type KEY, got %s", info.PartitionType)
	}
	if info.Expression != "issue_id" {
		t.Errorf("expected expression 'issue_id', got '%s'", info.Expression)
	}
	if info.PartitionCount != 4 {
		t.Errorf("expected partition count 4, got %d", info.PartitionCount)
	}
}

func TestParsePartitionInfo_Subpartition(t *testing.T) {
	mysqlDDL := `CREATE TABLE test_partition_07 (
  id int NOT NULL,
  issue_id int NOT NULL,
  performer varchar(100) NOT NULL
) ENGINE=InnoDB
/*!50100 PARTITION BY RANGE (issue_id)
SUBPARTITION BY HASH(performer)
SUBPARTITIONS 2
(PARTITION p0 VALUES LESS THAN (1000),
 PARTITION p1 VALUES LESS THAN MAXVALUE) */`

	info := parsePartitionInfo(mysqlDDL)
	if info == nil {
		t.Fatal("parsePartitionInfo returned nil")
	}
	if info.PartitionType != "RANGE" {
		t.Errorf("expected partition type RANGE, got %s", info.PartitionType)
	}
	if !info.HasSubPartition {
		t.Error("expected HasSubPartition to be true")
	}
	if info.SubPartitionType != "HASH" {
		t.Errorf("expected sub-partition type HASH, got %s", info.SubPartitionType)
	}
	if info.SubPartitionExpr != "performer" {
		t.Errorf("expected sub-partition expression 'performer', got '%s'", info.SubPartitionExpr)
	}
	if info.SubPartitionCount != 2 {
		t.Errorf("expected sub-partition count 2, got %d", info.SubPartitionCount)
	}
	if len(info.RangeDefs) != 2 {
		t.Errorf("expected 2 partition definitions, got %d", len(info.RangeDefs))
	}
}

func TestParsePartitionInfo_VersionCompatibility(t *testing.T) {
	// 测试不同 MySQL 版本号的注释语法
	testCases := []struct {
		version string
		name    string
	}{
		{"50100", "MySQL 5.1"},
		{"50500", "MySQL 5.5"},
		{"50700", "MySQL 5.7"},
		{"80000", "MySQL 8.0"},
		{"90000", "MySQL 9.0"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mysqlDDL := `CREATE TABLE test (
  id int NOT NULL,
  issue_id int NOT NULL
) ENGINE=InnoDB
/*!` + tc.version + ` PARTITION BY RANGE (issue_id)
(PARTITION p0 VALUES LESS THAN (1000),
 PARTITION p1 VALUES LESS THAN MAXVALUE) */`

			info := parsePartitionInfo(mysqlDDL)
			if info == nil {
				t.Fatalf("parsePartitionInfo returned nil for %s", tc.name)
			}
			if info.PartitionType != "RANGE" {
				t.Errorf("expected partition type RANGE for %s, got %s", tc.name, info.PartitionType)
			}
			if len(info.RangeDefs) != 2 {
				t.Errorf("expected 2 partition definitions for %s, got %d", tc.name, len(info.RangeDefs))
			}
		})
	}
}

func TestConvertTableDDL_RangePartition(t *testing.T) {
	mysqlDDL := `CREATE TABLE test_range (
  id int NOT NULL,
  issue_id int NOT NULL,
  name varchar(100) DEFAULT NULL,
  PRIMARY KEY (id,issue_id)
) ENGINE=InnoDB
/*!50100 PARTITION BY RANGE (issue_id)
(PARTITION p0 VALUES LESS THAN (1000),
 PARTITION p1 VALUES LESS THAN (2000),
 PARTITION p2 VALUES LESS THAN MAXVALUE) */`

	result, err := ConvertTableDDL(mysqlDDL, false)
	if err != nil {
		t.Fatalf("ConvertTableDDL failed: %v", err)
	}

	// 检查主表 DDL
	if !strings.Contains(result.DDL, "PARTITION BY RANGE (issue_id)") {
		t.Errorf("expected DDL to contain 'PARTITION BY RANGE (issue_id)', got: %s", result.DDL)
	}

	// 检查分区 DDL 数量
	if len(result.PartitionDDLs) != 3 {
		t.Errorf("expected 3 partition DDLs, got %d", len(result.PartitionDDLs))
	}

	// 检查第一个分区 DDL
	if !strings.Contains(result.PartitionDDLs[0], "test_range_p0") {
		t.Errorf("expected first partition DDL to contain 'test_range_p0', got: %s", result.PartitionDDLs[0])
	}
	if !strings.Contains(result.PartitionDDLs[0], "FOR VALUES FROM (MINVALUE) TO (1000)") {
		t.Errorf("expected first partition DDL to contain 'FOR VALUES FROM (MINVALUE) TO (1000)', got: %s", result.PartitionDDLs[0])
	}

	// 检查最后一个分区 DDL（MAXVALUE）
	lastIdx := len(result.PartitionDDLs) - 1
	if !strings.Contains(result.PartitionDDLs[lastIdx], "FOR VALUES FROM (2000) TO (MAXVALUE)") {
		t.Errorf("expected last partition DDL to contain 'FOR VALUES FROM (2000) TO (MAXVALUE)', got: %s", result.PartitionDDLs[lastIdx])
	}
}

func TestConvertTableDDL_ListPartition(t *testing.T) {
	mysqlDDL := `CREATE TABLE test_list (
  id int NOT NULL,
  status int NOT NULL,
  name varchar(100) DEFAULT NULL,
  PRIMARY KEY (id,status)
) ENGINE=InnoDB
/*!50100 PARTITION BY LIST (status)
(PARTITION p0 VALUES IN (0),
 PARTITION p1 VALUES IN (1),
 PARTITION p2 VALUES IN (2,3)) */`

	result, err := ConvertTableDDL(mysqlDDL, false)
	if err != nil {
		t.Fatalf("ConvertTableDDL failed: %v", err)
	}

	// 检查主表 DDL
	if !strings.Contains(result.DDL, "PARTITION BY LIST (status)") {
		t.Errorf("expected DDL to contain 'PARTITION BY LIST (status)', got: %s", result.DDL)
	}

	// 检查分区 DDL 数量
	if len(result.PartitionDDLs) != 3 {
		t.Errorf("expected 3 partition DDLs, got %d", len(result.PartitionDDLs))
	}

	// 检查分区 DDL 包含 VALUES IN
	if !strings.Contains(result.PartitionDDLs[2], "FOR VALUES IN (2,3)") {
		t.Errorf("expected third partition DDL to contain 'FOR VALUES IN (2,3)', got: %s", result.PartitionDDLs[2])
	}
}

func TestConvertTableDDL_HashPartition(t *testing.T) {
	mysqlDDL := `CREATE TABLE test_hash (
  id int NOT NULL,
  issue_id int NOT NULL,
  data text,
  PRIMARY KEY (id)
) ENGINE=InnoDB
/*!50100 PARTITION BY HASH(issue_id)
PARTITIONS 8 */`

	result, err := ConvertTableDDL(mysqlDDL, false)
	if err != nil {
		t.Fatalf("ConvertTableDDL failed: %v", err)
	}

	// 检查主表 DDL 包含 HASH 分区语法
	if !strings.Contains(result.DDL, "PARTITION BY HASH (issue_id)") {
		t.Errorf("expected DDL to contain 'PARTITION BY HASH (issue_id)', got: %s", result.DDL)
	}

	// HASH 分区不生成具体的分区 DDL（需要手动创建）
	if len(result.PartitionDDLs) != 0 {
		t.Errorf("expected 0 partition DDLs for HASH, got %d", len(result.PartitionDDLs))
	}
}

func TestConvertTableDDL_KeyPartition(t *testing.T) {
	mysqlDDL := `CREATE TABLE test_key (
  id int NOT NULL,
  issue_id int NOT NULL,
  content varchar(255) DEFAULT NULL,
  PRIMARY KEY (id)
) ENGINE=InnoDB
/*!50100 PARTITION BY KEY(issue_id)
PARTITIONS 4 */`

	result, err := ConvertTableDDL(mysqlDDL, false)
	if err != nil {
		t.Fatalf("ConvertTableDDL failed: %v", err)
	}

	// KEY 分区不生成 PARTITION BY 子句（PostgreSQL 不支持）
	if strings.Contains(result.DDL, "PARTITION BY") {
		t.Errorf("expected DDL to not contain 'PARTITION BY' for KEY partition, got: %s", result.DDL)
	}
}

func TestCleanTypeDefinition_EnumWithCharset(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "enum with CHARACTER SET utf8mb4",
			input:    "enum('active','inactive','pending') CHARACTER SET utf8mb4",
			expected: "VARCHAR(255)",
		},
		{
			name:     "enum with COLLATE utf8mb4_unicode_ci",
			input:    "enum('a','b','c') COLLATE utf8mb4_unicode_ci",
			expected: "VARCHAR(255)",
		},
		{
			name:     "enum with CHARACTER SET and COLLATE",
			input:    "enum('x','y') CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci",
			expected: "VARCHAR(255)",
		},
		{
			name:     "enum with charset=utf8mb4",
			input:    "enum('a','b') charset=utf8mb4",
			expected: "VARCHAR(255)",
		},
		{
			name:     "enum with CHARSET=utf8mb4 uppercase",
			input:    "enum('a','b') CHARSET=utf8mb4",
			expected: "VARCHAR(255)",
		},
		{
			name:     "set with CHARACTER SET utf8mb4",
			input:    "set('a','b','c') CHARACTER SET utf8mb4",
			expected: "VARCHAR(255)",
		},
		{
			name:     "enum without charset",
			input:    "enum('a','b','c')",
			expected: "VARCHAR(255)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := cleanTypeDefinition(tt.input, false)
			if result != tt.expected {
				t.Errorf("cleanTypeDefinition(%q) = %q, want %q", tt.input, result, tt.expected)
			}
		})
	}
}

func TestConvertTableDDL_EnumWithUtf8mb4(t *testing.T) {
	mysqlDDL := `CREATE TABLE test_enum (
  id int NOT NULL AUTO_INCREMENT,
  status enum('active','inactive','pending') CHARACTER SET utf8mb4 NOT NULL,
  role enum('admin','user') CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL,
  tags set('tag1','tag2') charset=utf8mb4 DEFAULT NULL,
  PRIMARY KEY (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`

	result, err := ConvertTableDDL(mysqlDDL, false)
	if err != nil {
		t.Fatalf("ConvertTableDDL failed: %v", err)
	}

	if strings.Contains(result.DDL, "mb4") {
		t.Errorf("DDL should not contain 'mb4' charset remnant, got: %s", result.DDL)
	}
	if strings.Contains(strings.ToLower(result.DDL), "enum(") {
		t.Errorf("DDL should not contain 'enum(' type, got: %s", result.DDL)
	}
	if strings.Contains(strings.ToLower(result.DDL), "set(") {
		t.Errorf("DDL should not contain 'set(' type, got: %s", result.DDL)
	}
	if !strings.Contains(result.DDL, "VARCHAR(255)") {
		t.Errorf("DDL should contain 'VARCHAR(255)', got: %s", result.DDL)
	}
}

func TestConvertTableDDL_TableLevelCharset(t *testing.T) {
	tests := []struct {
		name string
		ddl  string
	}{
		{
			name: "DEFAULT CHARSET=latin1",
			ddl: `CREATE TABLE tbl (
  id int NOT NULL,
  name varchar(100) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=latin1`,
		},
		{
			name: "DEFAULT CHARSET=gbk COLLATE=gbk_chinese_ci",
			ddl: `CREATE TABLE tbl (
  id int NOT NULL,
  name varchar(100) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=gbk COLLATE=gbk_chinese_ci`,
		},
		{
			name: "DEFAULT CHARSET=gb18030",
			ddl: `CREATE TABLE tbl (
  id int NOT NULL,
  name varchar(100) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=gb18030 COLLATE=gb18030_chinese_ci`,
		},
		{
			name: "DEFAULT CHARSET=big5",
			ddl: `CREATE TABLE tbl (
  id int NOT NULL,
  name varchar(100) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=big5 COLLATE=big5_chinese_ci`,
		},
		{
			name: "DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci",
			ddl: `CREATE TABLE tbl (
  id int NOT NULL,
  name varchar(100) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := ConvertTableDDL(tt.ddl, false)
			if err != nil {
				t.Fatalf("ConvertTableDDL failed: %v", err)
			}
			upperDDL := strings.ToUpper(result.DDL)
			if strings.Contains(upperDDL, "CHARSET=") || strings.Contains(upperDDL, "CHARSET ") {
				t.Errorf("DDL should not contain CHARSET clause, got: %s", result.DDL)
			}
			if strings.Contains(upperDDL, "COLLATE=") || strings.Contains(upperDDL, "COLLATE ") {
				t.Errorf("DDL should not contain COLLATE clause, got: %s", result.DDL)
			}
			if strings.Contains(upperDDL, "CHARACTER SET") {
				t.Errorf("DDL should not contain CHARACTER SET clause, got: %s", result.DDL)
			}
		})
	}
}

func TestConvertTableDDL_Subpartition(t *testing.T) {
	mysqlDDL := `CREATE TABLE test_sub (
  id int NOT NULL,
  issue_id int NOT NULL,
  performer varchar(100) NOT NULL,
  name varchar(255) DEFAULT NULL,
  PRIMARY KEY (id,issue_id,performer)
) ENGINE=InnoDB
/*!50100 PARTITION BY RANGE (issue_id)
SUBPARTITION BY HASH(performer)
SUBPARTITIONS 2
(PARTITION p0 VALUES LESS THAN (1000),
 PARTITION p1 VALUES LESS THAN MAXVALUE) */`

	result, err := ConvertTableDDL(mysqlDDL, false)
	if err != nil {
		t.Fatalf("ConvertTableDDL failed: %v", err)
	}

	// 检查主表 DDL 包含 RANGE 分区（忽略子分区）
	if !strings.Contains(result.DDL, "PARTITION BY RANGE (issue_id)") {
		t.Errorf("expected DDL to contain 'PARTITION BY RANGE (issue_id)', got: %s", result.DDL)
	}

	// 检查分区 DDL 数量（只有主分区，没有子分区）
	if len(result.PartitionDDLs) != 2 {
		t.Errorf("expected 2 partition DDLs (ignoring subpartitions), got %d", len(result.PartitionDDLs))
	}
}

// TestPromoteUnsignedTypes 验证无符号整数类型提升规则
func TestPromoteUnsignedTypes(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		{"bigint unsigned", "bigint unsigned", "numeric(20,0)"},
		{"bigint(20) unsigned", "bigint(20) unsigned", "numeric(20,0)"},
		{"大写 BIGINT UNSIGNED", "BIGINT(20) UNSIGNED", "numeric(20,0)"},
		{"混合大小写 Unsigned", "bigint(20) Unsigned", "numeric(20,0)"},
		{"bigint unsigned zerofill", "bigint(20) unsigned zerofill", "numeric(20,0)"},
		{"bigint zerofill 隐含 unsigned", "bigint zerofill", "numeric(20,0)"},
		{"int unsigned", "int unsigned", "bigint"},
		{"int(11) unsigned", "int(11) unsigned", "bigint"},
		{"integer unsigned", "integer unsigned", "bigint"},
		{"int unsigned zerofill", "int(10) unsigned zerofill", "bigint"},
		{"int zerofill 隐含 unsigned", "int zerofill", "bigint"},
		{"smallint unsigned", "smallint unsigned", "int"},
		{"smallint(6) unsigned", "smallint(6) unsigned", "int"},
		{"mediumint unsigned 无需提升", "mediumint unsigned", "mediumint unsigned"},
		{"tinyint unsigned 无需提升", "tinyint unsigned", "tinyint unsigned"},
		{"有符号 bigint 保持不变", "bigint(20)", "bigint(20)"},
		{"有符号 int 保持不变", "int(11)", "int(11)"},
		{"decimal unsigned 不提升", "decimal(10,2) unsigned", "decimal(10,2) unsigned"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := promoteUnsignedTypes(tt.in); got != tt.want {
				t.Errorf("promoteUnsignedTypes(%q) = %q, want %q", tt.in, got, tt.want)
			}
		})
	}
}

// TestConvertTableDDL_UnsignedIntegerTypes 验证无符号整数的端到端 DDL 转换：
// 目标类型必须能容纳完整的无符号范围
func TestConvertTableDDL_UnsignedIntegerTypes(t *testing.T) {
	// 注意：表名不能包含 unsigned 字样，否则残留检查会误报
	mysqlDDL := `CREATE TABLE test_uns_int (
  c1 tinyint unsigned NOT NULL,
  c2 smallint unsigned NOT NULL,
  c3 mediumint unsigned NOT NULL,
  c4 int unsigned NOT NULL,
  c5 bigint unsigned NOT NULL,
  c6 int(11) unsigned DEFAULT NULL,
  c7 bigint(20) unsigned DEFAULT NULL,
  c8 int unsigned zerofill NOT NULL,
  c9 int zerofill NOT NULL,
  c10 decimal(10,2) unsigned DEFAULT NULL,
  c11 INT UNSIGNED NOT NULL,
  c12 int NOT NULL,
  c13 bigint NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`

	result, err := ConvertTableDDL(mysqlDDL, false)
	if err != nil {
		t.Fatalf("ConvertTableDDL failed: %v", err)
	}

	checks := []struct {
		col      string
		wantType string
		reason   string
	}{
		{"c1", "SMALLINT", "tinyint unsigned(0~255) 可由 SMALLINT 容纳"},
		{"c2", "INTEGER", "smallint unsigned(0~65535) 超出 SMALLINT，需 INTEGER"},
		{"c3", "INTEGER", "mediumint unsigned(0~16777215) 可由 INTEGER 容纳"},
		{"c4", "BIGINT", "int unsigned(0~4294967295) 超出 INTEGER，需 BIGINT"},
		{"c5", "NUMERIC(20,0)", "bigint unsigned(0~18446744073709551615) 超出 BIGINT，需 NUMERIC(20,0)"},
		{"c6", "BIGINT", "int(11) unsigned 带显示宽度"},
		{"c7", "NUMERIC(20,0)", "bigint(20) unsigned 带显示宽度"},
		{"c8", "BIGINT", "int unsigned zerofill"},
		{"c9", "BIGINT", "zerofill 隐含 unsigned 语义"},
		{"c10", "DECIMAL(10,2)", "decimal unsigned 仅限制非负，无需提升"},
		{"c11", "BIGINT", "大写 INT UNSIGNED"},
		{"c12", "INTEGER", "有符号 int 回归验证"},
		{"c13", "BIGINT", "有符号 bigint 回归验证"},
	}
	for _, c := range checks {
		want := fmt.Sprintf(`"%s" %s`, c.col, c.wantType)
		if !strings.Contains(result.DDL, want) {
			t.Errorf("%s: DDL 应包含 %q，实际 DDL: %s", c.reason, want, result.DDL)
		}
	}

	upper := strings.ToUpper(result.DDL)
	if strings.Contains(upper, "UNSIGNED") || strings.Contains(upper, "ZEROFILL") {
		t.Errorf("DDL 不应残留 UNSIGNED/ZEROFILL 修饰: %s", result.DDL)
	}
}

// TestConvertTableDDL_UnsignedAutoIncrement 验证无符号自增列转换
func TestConvertTableDDL_UnsignedAutoIncrement(t *testing.T) {
	mysqlDDL := `CREATE TABLE test_unsigned_ai (
  id bigint unsigned NOT NULL AUTO_INCREMENT,
  id2 int unsigned NOT NULL AUTO_INCREMENT,
  PRIMARY KEY (id)
) ENGINE=InnoDB`

	result, err := ConvertTableDDL(mysqlDDL, false)
	if err != nil {
		t.Fatalf("ConvertTableDDL failed: %v", err)
	}

	if !strings.Contains(result.DDL, `"id" BIGSERIAL`) {
		t.Errorf("bigint unsigned AUTO_INCREMENT 应转为 BIGSERIAL，实际 DDL: %s", result.DDL)
	}
	if !strings.Contains(result.DDL, `"id2" BIGSERIAL`) {
		t.Errorf("int unsigned AUTO_INCREMENT 应转为 BIGSERIAL，实际 DDL: %s", result.DDL)
	}
}

// TestConvertTableDDL_BitTypes 验证 BIT 位字段类型转换
// BIT(n) 本质是无符号整数（0 ~ 2^n-1）：n<=63 转 BIGINT，n>=64 转 NUMERIC(20,0)
func TestConvertTableDDL_BitTypes(t *testing.T) {
	mysqlDDL := `CREATE TABLE test_bit (
  b1 bit(1) DEFAULT NULL,
  b2 bit(32) DEFAULT NULL,
  b3 bit(63) DEFAULT NULL,
  b4 bit(64) DEFAULT NULL
) ENGINE=InnoDB`

	result, err := ConvertTableDDL(mysqlDDL, false)
	if err != nil {
		t.Fatalf("ConvertTableDDL failed: %v", err)
	}

	checks := []struct {
		col      string
		wantType string
	}{
		{"b1", "BIGINT"},
		{"b2", "BIGINT"},
		{"b3", "BIGINT"},
		{"b4", "NUMERIC(20,0)"},
	}
	for _, c := range checks {
		want := fmt.Sprintf(`"%s" %s`, c.col, c.wantType)
		if !strings.Contains(result.DDL, want) {
			t.Errorf("DDL 应包含 %q，实际 DDL: %s", want, result.DDL)
		}
	}

	if strings.Contains(strings.ToLower(result.DDL), "bit(") {
		t.Errorf("DDL 不应残留 bit(n) 类型: %s", result.DDL)
	}
}

// TestCleanTypeDefinition_TinyInt1Mapping tinyint(1) 映射策略（P2-03 + 42883 修复）：
// 默认映射为 SMALLINT 保留整数语义（兼容视图/函数中 `col = 1` 等用法），
// 显式开启 tinyInt1AsBoolean 时映射为 BOOLEAN。
// 旧正则 \btinyint\(1\)\b 曾因结尾 \b 永不匹配导致映射静默失效
func TestCleanTypeDefinition_TinyInt1Mapping(t *testing.T) {
	tests := []struct {
		name              string
		input             string
		wantAsBoolean     string // tinyInt1AsBoolean=true 时应包含
		wantDefault       string // 默认（false）时应包含
		notContainDefault []string
	}{
		{"tinyint(1)", "tinyint(1)", "BOOLEAN", "SMALLINT", []string{"BOOLEAN"}},
		{"大写 TINYINT(1) 带修饰", "TINYINT(1) NOT NULL DEFAULT 1", "BOOLEAN", "SMALLINT", []string{"BOOLEAN"}},
		{"tinyint(1) unsigned", "tinyint(1) unsigned", "BOOLEAN", "SMALLINT", []string{"BOOLEAN"}},
	}
	for _, tt := range tests {
		t.Run(tt.name+"（默认 SMALLINT）", func(t *testing.T) {
			got := cleanTypeDefinition(tt.input, false)
			if !strings.Contains(got, tt.wantDefault) {
				t.Errorf("cleanTypeDefinition(%q, false) = %q，不含 %s", tt.input, got, tt.wantDefault)
			}
			for _, nc := range tt.notContainDefault {
				if strings.Contains(got, nc) {
					t.Errorf("cleanTypeDefinition(%q, false) = %q，不应包含 %s", tt.input, got, nc)
				}
			}
		})
		t.Run(tt.name+"（开启 BOOLEAN）", func(t *testing.T) {
			got := cleanTypeDefinition(tt.input, true)
			if !strings.Contains(got, tt.wantAsBoolean) {
				t.Errorf("cleanTypeDefinition(%q, true) = %q，不含 %s", tt.input, got, tt.wantAsBoolean)
			}
		})
	}

	// 其他 tinyint 变宽不受开关影响
	for _, input := range []string{"tinyint(4)", "tinyint(10)", "tinyint"} {
		for _, asBoolean := range []bool{false, true} {
			got := cleanTypeDefinition(input, asBoolean)
			if !strings.Contains(got, "SMALLINT") || strings.Contains(got, "BOOLEAN") {
				t.Errorf("cleanTypeDefinition(%q, %v) = %q，应为 SMALLINT", input, asBoolean, got)
			}
		}
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
			got := cleanTypeDefinition(tt.input, false)
			if !strings.Contains(got, tt.want) {
				t.Errorf("cleanTypeDefinition(%q) = %q，不含 %s", tt.input, got, tt.want)
			}
		})
	}
}

// TestConvertTableDDL_TinyInt1Option tinyint(1) 映射开关的端到端行为：
// 默认 SMALLINT（兼容视图/函数中 `col = 1` 的整数比较），开启后 BOOLEAN
func TestConvertTableDDL_TinyInt1Option(t *testing.T) {
	mysqlDDL := "CREATE TABLE `t_bool` (\n" +
		"  `id` bigint NOT NULL,\n" +
		"  `is_active` tinyint(1) DEFAULT 1,\n" +
		"  PRIMARY KEY (`id`)\n" +
		") ENGINE=InnoDB"

	// 默认：SMALLINT，视图/函数中的 is_active = 1 保持整数比较语义
	result, err := ConvertTableDDL(mysqlDDL, true)
	if err != nil {
		t.Fatalf("ConvertTableDDL 返回错误：%v", err)
	}
	if !strings.Contains(result.DDL, `"is_active" SMALLINT`) {
		t.Errorf("默认应将 tinyint(1) 转为 SMALLINT：%s", result.DDL)
	}
	if strings.Contains(result.DDL, "BOOLEAN") {
		t.Errorf("默认不应出现 BOOLEAN：%s", result.DDL)
	}

	// 显式开启：BOOLEAN
	result, err = ConvertTableDDL(mysqlDDL, true, ConvertTableDDLOptions{TinyInt1AsBoolean: true})
	if err != nil {
		t.Fatalf("ConvertTableDDL 返回错误：%v", err)
	}
	if !strings.Contains(result.DDL, `"is_active" BOOLEAN`) {
		t.Errorf("开启选项后应转为 BOOLEAN：%s", result.DDL)
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
