package mysql

import (
	"database/sql"
	"testing"
)

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
