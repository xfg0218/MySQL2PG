package postgres

import (
	"database/sql"
	"runtime"
	"testing"
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
			}
		})
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
}
