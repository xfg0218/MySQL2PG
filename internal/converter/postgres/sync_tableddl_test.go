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
