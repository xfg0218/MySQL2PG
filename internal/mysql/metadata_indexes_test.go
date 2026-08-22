package mysql

import (
	"database/sql"
	"strings"
	"testing"
)

// TestBuildUsersQuery P3-05：系统用户排除按官方命名空间规则，
// 不再依赖含重复项和生造账号的枚举清单
func TestBuildUsersQuery(t *testing.T) {
	query := buildUsersQuery()

	if !strings.Contains(query, `user NOT LIKE 'mysql.%'`) {
		t.Errorf("应按 mysql.%% 前缀排除内置账号：%s", query)
	}
	if !strings.Contains(query, `user != 'root'`) {
		t.Errorf("应排除 root：%s", query)
	}
	// 不应再出现逐个枚举（旧清单有重复与生造账号）
	for _, legacy := range []string{"mysql.pfsadmin", "mysql.sys", "pfs_role_admin", "debian-sys-maint"} {
		if strings.Contains(query, legacy) {
			t.Errorf("查询不应再逐个枚举内置账号（%s）：%s", legacy, query)
		}
	}
}

// TestAppendIndexRow_FunctionalNullColumn P2-01：函数索引部件的 NULL 列名
// 只标记 IsFunctional，不得报错或写入空列名
func TestAppendIndexRow_FunctionalNullColumn(t *testing.T) {
	indexMap := make(map[string]*IndexInfo)

	appendIndexRow(indexMap, "idx_func", "t1", "idx_func", 1,
		sql.NullString{Valid: false}, sql.NullString{String: "A", Valid: true},
		sql.NullString{String: "BTREE", Valid: true}, sql.NullInt64{Valid: false})

	info, ok := indexMap["idx_func"]
	if !ok {
		t.Fatal("索引未被记录")
	}
	if !info.IsFunctional {
		t.Error("NULL 列名应标记 IsFunctional")
	}
	if len(info.Columns) != 0 {
		t.Errorf("NULL 列名不应写入 Columns：%v", info.Columns)
	}
	if info.IndexType != "BTREE" {
		t.Errorf("IndexType = %q, want BTREE", info.IndexType)
	}
}

// TestAppendIndexRow_PrefixAndDesc P2-19：前缀长度与降序方向采集
func TestAppendIndexRow_PrefixAndDesc(t *testing.T) {
	indexMap := make(map[string]*IndexInfo)

	appendIndexRow(indexMap, "idx_mix", "t1", "idx_mix", 1,
		sql.NullString{String: "name", Valid: true}, sql.NullString{String: "A", Valid: true},
		sql.NullString{String: "BTREE", Valid: true}, sql.NullInt64{Int64: 10, Valid: true})
	appendIndexRow(indexMap, "idx_mix", "t1", "idx_mix", 1,
		sql.NullString{String: "created_at", Valid: true}, sql.NullString{String: "D", Valid: true},
		sql.NullString{String: "BTREE", Valid: true}, sql.NullInt64{Valid: false})

	info := indexMap["idx_mix"]
	if info == nil {
		t.Fatal("索引未被记录")
	}
	if len(info.Columns) != 2 || info.Columns[0] != "name" || info.Columns[1] != "created_at" {
		t.Fatalf("Columns = %v", info.Columns)
	}
	if info.ColumnSubParts[0] != 10 || info.ColumnSubParts[1] != 0 {
		t.Errorf("ColumnSubParts = %v", info.ColumnSubParts)
	}
	if info.ColumnDescs[0] || !info.ColumnDescs[1] {
		t.Errorf("ColumnDescs = %v", info.ColumnDescs)
	}
	if info.IsUnique {
		t.Error("non_unique=1 不应为唯一索引")
	}
}

// TestSortIndexes 稳定排序：按表名、索引名
func TestSortIndexes(t *testing.T) {
	indexes := []IndexInfo{
		{Name: "z_idx", Table: "t2"},
		{Name: "a_idx", Table: "t2"},
		{Name: "m_idx", Table: "t1"},
	}
	sortIndexes(indexes)
	if indexes[0].Table != "t1" || indexes[1].Name != "a_idx" || indexes[2].Name != "z_idx" {
		t.Errorf("排序结果错误：%+v", indexes)
	}
}
