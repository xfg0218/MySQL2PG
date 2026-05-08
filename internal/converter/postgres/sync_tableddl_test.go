package postgres

import (
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
