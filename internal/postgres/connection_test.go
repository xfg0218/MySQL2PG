package postgres

import (
	"database/sql"
	"runtime"
	"strings"
	"testing"
	"time"
)

// 基准测试：多级 rowSlicePool vs 单级池
// 测量目标：首次分配 rowSlice 时的内存占用（未复用场景）
// 这模拟了大量不同列表在并发场景下的内存压力
// 使用 runtime.KeepAlive 防止编译器优化掉未使用的分配

// 模拟当前单级池的实现（用于对比）
func benchmarkMakeSingle(numCols int) []interface{} {
	s := make([]interface{}, 128)
	return s[:numCols]
}

// 模拟多级池的实现（零长度、预分配容量）
func benchmarkMakeMulti(numCols int) []interface{} {
	var s *[]interface{}
	if numCols <= 8 {
		s = &[]interface{}{}
		*s = make([]interface{}, 0, 8)
	} else if numCols <= 32 {
		s = &[]interface{}{}
		*s = make([]interface{}, 0, 32)
	} else if numCols <= 128 {
		s = &[]interface{}{}
		*s = make([]interface{}, 0, 128)
	} else {
		s = &[]interface{}{}
		*s = make([]interface{}, 0, 256)
	}
	return (*s)[:numCols]
}

func BenchmarkRowSlicePool_SingleLevel(b *testing.B) {
	columns := []int{3, 5, 8, 20, 50, 100}
	for _, numCols := range columns {
		b.Run(
			string(rune('0'+numCols/10))+string(rune('0'+numCols%10))+"cols",
			func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					s := benchmarkMakeSingle(numCols)
					// 模拟使用切片，防止编译器优化
					for j := 0; j < numCols; j++ {
						s[j] = i
					}
					runtime.KeepAlive(s)
				}
			},
		)
	}
}

func BenchmarkRowSlicePool_MultiLevel(b *testing.B) {
	columns := []int{3, 5, 8, 20, 50, 100}
	for _, numCols := range columns {
		b.Run(
			string(rune('0'+numCols/10))+string(rune('0'+numCols%10))+"cols",
			func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					s := benchmarkMakeMulti(numCols)
					// 模拟使用切片，防止编译器优化
					for j := 0; j < numCols; j++ {
						s[j] = i
					}
					runtime.KeepAlive(s)
				}
			},
		)
	}
}

func TestMakeTypedDestUsesNullableTypes(t *testing.T) {
	cases := []struct {
		name     string
		colType  string
		typeName string
	}{
		{name: "int", colType: "int(11)", typeName: "*sql.NullInt64"},
		{name: "decimal", colType: "decimal(10,2)", typeName: "*sql.NullString"},
		{name: "float", colType: "float", typeName: "*sql.NullFloat64"},
		{name: "bool", colType: "boolean", typeName: "*sql.NullBool"},
		{name: "varchar", colType: "varchar(64)", typeName: "*sql.NullString"},
		// issue #156：date/datetime/timestamp 必须用 NullTime 承接 time.Time，
		// 字符串对 pgx.CopyFrom 二进制协议的 timestamp/timestamptz 无 encode plan
		{name: "date", colType: "date", typeName: "*sql.NullTime"},
		{name: "datetime", colType: "datetime", typeName: "*sql.NullTime"},
		{name: "datetime(6)", colType: "datetime(6)", typeName: "*sql.NullTime"},
		{name: "timestamp", colType: "timestamp", typeName: "*sql.NullTime"},
		{name: "timestamp(6)", colType: "timestamp(6)", typeName: "*sql.NullTime"},
		// TIME 驱动不解析（无日期部分），保持字符串透传
		{name: "time", colType: "time", typeName: "*sql.NullString"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dest := makeTypedDest(tc.colType)
			switch tc.typeName {
			case "*sql.NullInt64":
				if _, ok := dest.value.(*sql.NullInt64); !ok {
					t.Fatalf("期望 *sql.NullInt64，实际 %T", dest.value)
				}
			case "*sql.NullString":
				if _, ok := dest.value.(*sql.NullString); !ok {
					t.Fatalf("期望 *sql.NullString，实际 %T", dest.value)
				}
			case "*sql.NullFloat64":
				if _, ok := dest.value.(*sql.NullFloat64); !ok {
					t.Fatalf("期望 *sql.NullFloat64，实际 %T", dest.value)
				}
			case "*sql.NullBool":
				if _, ok := dest.value.(*sql.NullBool); !ok {
					t.Fatalf("期望 *sql.NullBool，实际 %T", dest.value)
				}
			case "*sql.NullTime":
				if _, ok := dest.value.(*sql.NullTime); !ok {
					t.Fatalf("期望 *sql.NullTime，实际 %T", dest.value)
				}
			}
		})
	}
}

// TestGetTypedValueNullTime issue #156：NullTime 值提取语义，
// 零日期（驱动解析为零值 time.Time 且 Valid=true）统一转 NULL
func TestGetTypedValueNullTime(t *testing.T) {
	null := typedDest{value: &sql.NullTime{}}
	if got := getTypedValue(&null); got != nil {
		t.Fatalf("期望 nil，实际 %v", got)
	}

	zero := typedDest{value: &sql.NullTime{Time: time.Time{}, Valid: true}}
	if got := getTypedValue(&zero); got != nil {
		t.Fatalf("零日期应转 NULL，实际 %v", got)
	}

	ts := time.Date(2026, 7, 30, 2, 59, 34, 0, time.UTC)
	valid := typedDest{value: &sql.NullTime{Time: ts, Valid: true}}
	got, ok := getTypedValue(&valid).(time.Time)
	if !ok || !got.Equal(ts) {
		t.Fatalf("期望 %v，实际 %v", ts, getTypedValue(&valid))
	}
}

func TestGetTypedValueHandlesNullAndValidValues(t *testing.T) {
	intNull := typedDest{value: &sql.NullInt64{}}
	if got := getTypedValue(&intNull); got != nil {
		t.Fatalf("期望 nil，实际 %v", got)
	}

	intValue := typedDest{value: &sql.NullInt64{Int64: 42, Valid: true}}
	if got := getTypedValue(&intValue); got != int64(42) {
		t.Fatalf("期望 42，实际 %v", got)
	}

	strNull := typedDest{value: &sql.NullString{}}
	if got := getTypedValue(&strNull); got != nil {
		t.Fatalf("期望 nil，实际 %v", got)
	}

	strValue := typedDest{value: &sql.NullString{String: "ok", Valid: true}}
	if got := getTypedValue(&strValue); got != "ok" {
		t.Fatalf("期望 ok，实际 %v", got)
	}
}

func TestResetTypedDestinationsResetsNullableState(t *testing.T) {
	dests := []typedDest{
		{value: &sql.NullInt64{Int64: 9, Valid: true}},
		{value: &sql.NullString{String: "x", Valid: true}},
		{value: &sql.NullFloat64{Float64: 1.2, Valid: true}},
		{value: &sql.NullBool{Bool: true, Valid: true}},
		{value: &sql.NullTime{Time: time.Date(2026, 7, 30, 2, 59, 34, 0, time.UTC), Valid: true}},
	}

	resetTypedDestinations(dests)

	if v := dests[0].value.(*sql.NullInt64); v.Valid || v.Int64 != 0 {
		t.Fatalf("NullInt64 未正确重置: %+v", *v)
	}
	if v := dests[1].value.(*sql.NullString); v.Valid || v.String != "" {
		t.Fatalf("NullString 未正确重置: %+v", *v)
	}
	if v := dests[2].value.(*sql.NullFloat64); v.Valid || v.Float64 != 0 {
		t.Fatalf("NullFloat64 未正确重置: %+v", *v)
	}
	if v := dests[3].value.(*sql.NullBool); v.Valid || v.Bool {
		t.Fatalf("NullBool 未正确重置: %+v", *v)
	}
	if v := dests[4].value.(*sql.NullTime); v.Valid || !v.Time.IsZero() {
		t.Fatalf("NullTime 未正确重置: %+v", *v)
	}
}

// TestMakeTypedDestBigintUnsigned BIGINT UNSIGNED 最大值超出 int64 范围，
// 必须使用 NullString 透传（NullInt64 会在 Scan 阶段因 strconv 越界失败）
func TestMakeTypedDestBigintUnsigned(t *testing.T) {
	cases := []struct {
		name    string
		colType string
	}{
		{name: "bigint unsigned", colType: "bigint unsigned"},
		{name: "bigint(20) unsigned", colType: "bigint(20) unsigned"},
		{name: "大写 BIGINT(20) UNSIGNED", colType: "BIGINT(20) UNSIGNED"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dest := makeTypedDest(tc.colType)
			if _, ok := dest.value.(*sql.NullString); !ok {
				t.Fatalf("期望 *sql.NullString，实际 %T", dest.value)
			}
		})
	}

	// 有符号 bigint 保持 NullInt64（回归验证）
	dest := makeTypedDest("bigint(20)")
	if _, ok := dest.value.(*sql.NullInt64); !ok {
		t.Fatalf("有符号 bigint 期望 *sql.NullInt64，实际 %T", dest.value)
	}
}

// TestParseMySQLBitValue 验证 BIT 列大端序二进制值到十进制数的解析
func TestParseMySQLBitValue(t *testing.T) {
	cases := []struct {
		name   string
		data   []byte
		want   string
		wantOk bool
	}{
		{name: "单字节 1", data: []byte{0x01}, want: "1", wantOk: true},
		{name: "单字节 255", data: []byte{0xFF}, want: "255", wantOk: true},
		{name: "4 字节 65535", data: []byte{0x00, 0x00, 0xFF, 0xFF}, want: "65535", wantOk: true},
		{name: "8 字节 uint64 最大值", data: []byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF}, want: "18446744073709551615", wantOk: true},
		{name: "8 字节 int64 最大值+1", data: []byte{0x80, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}, want: "9223372036854775808", wantOk: true},
		{name: "空值", data: []byte{}, want: "", wantOk: false},
		{name: "超过 8 字节", data: make([]byte, 9), want: "", wantOk: false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := parseMySQLBitValue(tc.data)
			if ok != tc.wantOk || got != tc.want {
				t.Fatalf("parseMySQLBitValue(%v) = (%q, %v)，期望 (%q, %v)", tc.data, got, ok, tc.want, tc.wantOk)
			}
		})
	}
}

// TestIsValidPGTime 验证 MySQL TIME 值到 PostgreSQL TIME 范围的判定
func TestIsValidPGTime(t *testing.T) {
	cases := []struct {
		val  string
		want bool
	}{
		{"00:00:00", true},
		{"23:59:59", true},
		{"12:30:45.123456", true},
		{"24:00:00", true},
		{"24:00:00.000000", true},
		{"-00:30:00", false},
		{"-838:59:59", false},
		{"838:59:59", false},
		{"25:00:00", false},
		{"24:00:01", false},
		{"24:01:00", false},
	}
	for _, tc := range cases {
		t.Run(tc.val, func(t *testing.T) {
			if got := isValidPGTime(tc.val); got != tc.want {
				t.Fatalf("isValidPGTime(%q) = %v，期望 %v", tc.val, got, tc.want)
			}
		})
	}
}

// TestConvertBatchColumnValueBitAndTime 验证 BIT 值转换与 TIME 超范围处理
func TestConvertBatchColumnValueBitAndTime(t *testing.T) {
	columnTypes := map[string]string{
		"flags": "bit(8)",
		"big":   "bit(64)",
		"dur":   "time",
		"dt":    "datetime",
	}

	// BIT 值转十进制数（目标列 BIGINT/NUMERIC(20,0)）
	if got := convertBatchColumnValue("flags", []byte{0x05}, columnTypes); got != "5" {
		t.Fatalf("bit(8) 值 0x05 期望转为 \"5\"，实际 %v", got)
	}
	if got := convertBatchColumnValue("big", []byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF}, columnTypes); got != "18446744073709551615" {
		t.Fatalf("bit(64) 最大值期望透传为十进制字符串，实际 %v", got)
	}

	// TIME 超范围值转 NULL，范围内值透传
	if got := convertBatchColumnValue("dur", "838:59:59", columnTypes); got != nil {
		t.Fatalf("超范围 TIME 期望转 NULL，实际 %v", got)
	}
	if got := convertBatchColumnValue("dur", "-01:00:00", columnTypes); got != nil {
		t.Fatalf("负值 TIME 期望转 NULL，实际 %v", got)
	}
	if got := convertBatchColumnValue("dur", "12:30:45", columnTypes); got != "12:30:45" {
		t.Fatalf("范围内 TIME 期望透传，实际 %v", got)
	}

	// datetime 列不走 TIME 范围检查
	if got := convertBatchColumnValue("dt", "2024-01-01 10:00:00", columnTypes); got != "2024-01-01 10:00:00" {
		t.Fatalf("datetime 值期望透传，实际 %v", got)
	}
}

// TestBuildSequenceBackfillQuery 序列回填 SQL 构造（issue：column ""id"" does not exist）：
// pg_get_serial_sequence 的列参数是裸列名（PG 不解析其中双引号），
// MAX()/FROM 位置则保持双引号标识符
func TestBuildSequenceBackfillQuery(t *testing.T) {
	query := buildSequenceBackfillQuery("t1", "id", 100)

	if !strings.Contains(query, `pg_get_serial_sequence('"t1"', 'id')`) {
		t.Errorf("pg_get_serial_sequence 列参数应为裸列名：%s", query)
	}
	if strings.Contains(query, `'''id''`) || strings.Contains(query, `'\"id\"'`) || strings.Contains(query, `'"id"'`) {
		t.Errorf("pg_get_serial_sequence 列参数不得带双引号：%s", query)
	}
	if !strings.Contains(query, `MAX("id") FROM "t1"`) {
		t.Errorf("MAX/FROM 位置应使用双引号标识符：%s", query)
	}
	if !strings.Contains(query, "100") {
		t.Errorf("缺少序列下限值：%s", query)
	}

	// 列名含单引号时按字符串字面量规则双写转义
	query = buildSequenceBackfillQuery("t1", "a'b", 1)
	if !strings.Contains(query, `'a''b'`) {
		t.Errorf("列名中的单引号应双写转义：%s", query)
	}
}
