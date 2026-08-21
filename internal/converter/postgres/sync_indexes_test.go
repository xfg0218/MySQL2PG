package postgres

import (
	"strings"
	"testing"

	"github.com/yourusername/mysql2pg/internal/mysql"
)

// TestConvertIndexDDL_Basic 普通 BTREE 索引转换
func TestConvertIndexDDL_Basic(t *testing.T) {
	index := mysql.IndexInfo{
		Name:      "idx_ab",
		Table:     "t1",
		Columns:   []string{"a", "b"},
		IndexType: "BTREE",
	}
	ddl, warnings, err := ConvertIndexDDL(index, true, nil)
	if err != nil {
		t.Fatalf("ConvertIndexDDL 返回错误：%v", err)
	}
	want := `CREATE INDEX IF NOT EXISTS "t1_idx_ab" ON "t1" ("a", "b");`
	if ddl != want {
		t.Errorf("DDL = %q, want %q", ddl, want)
	}
	if len(warnings) != 0 {
		t.Errorf("不应有告警：%v", warnings)
	}
}

// TestConvertIndexDDL_UniqueWithColumnMapping 唯一索引 + 列名映射 + 大小写
func TestConvertIndexDDL_UniqueWithColumnMapping(t *testing.T) {
	index := mysql.IndexInfo{
		Name:     "uk_name",
		Table:    "t1",
		Columns:  []string{"UserName"},
		IsUnique: true,
	}
	columnNamesMap := map[string]string{"UserName": `"UserName"`}
	ddl, _, err := ConvertIndexDDL(index, false, columnNamesMap)
	if err != nil {
		t.Fatalf("ConvertIndexDDL 返回错误：%v", err)
	}
	if !strings.Contains(ddl, `CREATE UNIQUE INDEX IF NOT EXISTS`) || !strings.Contains(ddl, `("UserName")`) {
		t.Errorf("唯一索引 DDL 错误：%s", ddl)
	}

	// lowercaseColumns=true 时列名小写
	ddlLower, _, err := ConvertIndexDDL(index, true, nil)
	if err != nil {
		t.Fatalf("ConvertIndexDDL 返回错误：%v", err)
	}
	if !strings.Contains(ddlLower, `("username")`) {
		t.Errorf("列名未小写化：%s", ddlLower)
	}
}

// TestConvertIndexDDL_DescIndex P2-19：降序索引输出列级 DESC
func TestConvertIndexDDL_DescIndex(t *testing.T) {
	index := mysql.IndexInfo{
		Name:        "idx_desc",
		Table:       "t1",
		Columns:     []string{"a", "b"},
		ColumnDescs: []bool{false, true},
		IndexType:   "BTREE",
	}
	ddl, _, err := ConvertIndexDDL(index, true, nil)
	if err != nil {
		t.Fatalf("ConvertIndexDDL 返回错误：%v", err)
	}
	if !strings.Contains(ddl, `("a", "b" DESC)`) {
		t.Errorf("降序列未输出 DESC：%s", ddl)
	}
}

// TestConvertIndexDDL_PrefixIndex P2-19：前缀长度丢弃转整列索引并告警
func TestConvertIndexDDL_PrefixIndex(t *testing.T) {
	index := mysql.IndexInfo{
		Name:           "idx_prefix",
		Table:          "t1",
		Columns:        []string{"name"},
		ColumnSubParts: []int64{10},
		IndexType:      "BTREE",
	}
	ddl, warnings, err := ConvertIndexDDL(index, true, nil)
	if err != nil {
		t.Fatalf("ConvertIndexDDL 返回错误：%v", err)
	}
	if !strings.Contains(ddl, `("name")`) {
		t.Errorf("前缀索引未转为整列索引：%s", ddl)
	}
	found := false
	for _, w := range warnings {
		if strings.Contains(w, "name(10)") && strings.Contains(w, "前缀长度已丢弃") {
			found = true
		}
	}
	if !found {
		t.Errorf("缺少前缀索引降级告警：%v", warnings)
	}
}

// TestConvertIndexDDL_Fulltext P2-18：FULLTEXT 按普通 B-tree 创建并告警
func TestConvertIndexDDL_Fulltext(t *testing.T) {
	index := mysql.IndexInfo{
		Name:      "ft_content",
		Table:     "t1",
		Columns:   []string{"content"},
		IndexType: "FULLTEXT",
	}
	ddl, warnings, err := ConvertIndexDDL(index, true, nil)
	if err != nil {
		t.Fatalf("ConvertIndexDDL 返回错误：%v", err)
	}
	if ddl == "" || !strings.Contains(ddl, `("content")`) {
		t.Errorf("FULLTEXT 索引应创建为普通索引：%s", ddl)
	}
	if strings.Contains(ddl, "FULLTEXT") {
		t.Errorf("DDL 不应包含 FULLTEXT 字样：%s", ddl)
	}
	found := false
	for _, w := range warnings {
		if strings.Contains(w, "FULLTEXT") && strings.Contains(w, "全文检索语义未迁移") {
			found = true
		}
	}
	if !found {
		t.Errorf("缺少 FULLTEXT 降级告警：%v", warnings)
	}
}

// TestConvertIndexDDL_SpatialAndFunctional 跳过的索引类型：SPATIAL 与函数索引
func TestConvertIndexDDL_SpatialAndFunctional(t *testing.T) {
	spatial := mysql.IndexInfo{
		Name:      "sp_geom",
		Table:     "t1",
		Columns:   []string{"geom"},
		IndexType: "SPATIAL",
	}
	ddl, warnings, err := ConvertIndexDDL(spatial, true, nil)
	if err != nil {
		t.Fatalf("SPATIAL 索引转换返回错误：%v", err)
	}
	if ddl != "" {
		t.Errorf("SPATIAL 索引应跳过创建：%s", ddl)
	}
	if len(warnings) == 0 || !strings.Contains(warnings[0], "SPATIAL") {
		t.Errorf("缺少 SPATIAL 跳过告警：%v", warnings)
	}

	functional := mysql.IndexInfo{
		Name:         "idx_func",
		Table:        "t1",
		Columns:      []string{},
		IsFunctional: true,
		IndexType:    "BTREE",
	}
	ddl, warnings, err = ConvertIndexDDL(functional, true, nil)
	if err != nil {
		t.Fatalf("函数索引转换返回错误：%v", err)
	}
	if ddl != "" {
		t.Errorf("函数索引应跳过创建：%s", ddl)
	}
	if len(warnings) == 0 || !strings.Contains(warnings[0], "函数索引") {
		t.Errorf("缺少函数索引跳过告警：%v", warnings)
	}
}

// TestConvertIndexDDL_Hash P2-19：USING HASH 映射
func TestConvertIndexDDL_Hash(t *testing.T) {
	index := mysql.IndexInfo{
		Name:      "idx_hash",
		Table:     "t1",
		Columns:   []string{"code"},
		IndexType: "HASH",
	}
	ddl, _, err := ConvertIndexDDL(index, true, nil)
	if err != nil {
		t.Fatalf("ConvertIndexDDL 返回错误：%v", err)
	}
	if !strings.Contains(ddl, `USING HASH ("code")`) {
		t.Errorf("HASH 索引未输出 USING HASH：%s", ddl)
	}
}

// TestConvertIndexDDL_LongNameHashSuffix P1-17 回归：超长索引名截断+哈希后缀防碰撞
func TestConvertIndexDDL_LongNameHashSuffix(t *testing.T) {
	longSuffixA := strings.Repeat("a", 80)
	longSuffixB := strings.Repeat("b", 80)
	indexA := mysql.IndexInfo{Name: "idx_" + longSuffixA, Table: "t1", Columns: []string{"a"}, IndexType: "BTREE"}
	indexB := mysql.IndexInfo{Name: "idx_" + longSuffixB, Table: "t1", Columns: []string{"a"}, IndexType: "BTREE"}

	ddlA, _, err := ConvertIndexDDL(indexA, true, nil)
	if err != nil {
		t.Fatalf("ConvertIndexDDL 返回错误：%v", err)
	}
	ddlB, _, err := ConvertIndexDDL(indexB, true, nil)
	if err != nil {
		t.Fatalf("ConvertIndexDDL 返回错误：%v", err)
	}
	if ddlA == ddlB {
		t.Errorf("仅尾部不同的长索引名截断后不应撞名：%s", ddlA)
	}
	if len(ddlA) == 0 || !strings.Contains(ddlA, "IF NOT EXISTS") {
		t.Errorf("DDL 生成异常：%s", ddlA)
	}
}

// TestConvertIndexDDL_PriKeyOnlySkip 仅含 pri_key 的索引跳过且不告警
func TestConvertIndexDDL_PriKeyOnlySkip(t *testing.T) {
	index := mysql.IndexInfo{Name: "PRIMARY", Table: "t1", Columns: []string{"pri_key"}}
	ddl, warnings, err := ConvertIndexDDL(index, true, nil)
	if err != nil {
		t.Fatalf("ConvertIndexDDL 返回错误：%v", err)
	}
	if ddl != "" {
		t.Errorf("pri_key 索引应跳过：%s", ddl)
	}
	if len(warnings) != 0 {
		t.Errorf("pri_key 跳过不应产生告警：%v", warnings)
	}
}
