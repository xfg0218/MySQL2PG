package mysql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"strings"
	"testing"
)

// issue #170：验证元数据查询在结果集被截断时必须返回错误，而不是把部分结果当成完整结果。
//
// 用标准库 database/sql/driver 实现最小假 driver 模拟"迭代中途出错"，
// 不引入 go-sqlmock 以保持 go.mod 精简（与现有 connection_test.go 的纯标准库风格一致）。

// truncatedRows 在成功返回 failAfter 行后返回 err。
// err 为 io.EOF 时表示正常读完；为其他错误时表示结果集被截断。
type truncatedRows struct {
	columns   []string
	data      [][]driver.Value
	pos       int
	failAfter int
	err       error
}

func (r *truncatedRows) Columns() []string { return r.columns }
func (r *truncatedRows) Close() error      { return nil }

func (r *truncatedRows) Next(dest []driver.Value) error {
	if r.pos >= r.failAfter {
		return r.err
	}
	copy(dest, r.data[r.pos])
	r.pos++
	return nil
}

// truncatedConn 通过 QueryerContext 直接返回预置的 Rows，绕过 Prepare
type truncatedConn struct {
	rows driver.Rows
}

func (c *truncatedConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("not implemented")
}
func (c *truncatedConn) Close() error              { return nil }
func (c *truncatedConn) Begin() (driver.Tx, error) { return nil, errors.New("not implemented") }

func (c *truncatedConn) QueryContext(context.Context, string, []driver.NamedValue) (driver.Rows, error) {
	return c.rows, nil
}

type truncatedConnector struct {
	conn driver.Conn
}

func (c *truncatedConnector) Connect(context.Context) (driver.Conn, error) { return c.conn, nil }
func (c *truncatedConnector) Driver() driver.Driver                        { return nil }

// newTruncatedConnection 构造一个查询结果在 failAfter 行后被截断的 Connection
func newTruncatedConnection(t *testing.T, columns []string, data [][]driver.Value, failAfter int) *Connection {
	t.Helper()

	rowsErr := io.EOF
	if failAfter < len(data) {
		rowsErr = errors.New("invalid connection: packet sequence mismatch")
	}

	db := sql.OpenDB(&truncatedConnector{conn: &truncatedConn{rows: &truncatedRows{
		columns:   columns,
		data:      data,
		failAfter: failAfter,
		err:       rowsErr,
	}}})
	t.Cleanup(func() { db.Close() })

	return &Connection{db: db, ctx: context.Background()}
}

// showColumnsResult 构造 SHOW COLUMNS 的 6 列结果集
// 字段顺序：Field, Type, Null, Key, Default, Extra
// 其中 Field/Type/Null/Key/Extra 的 Scan 目标是 string（非 nullable），必须给空串而非 nil；
// 只有 Default 的目标是 sql.NullString，可以为 nil
func showColumnsResult(fields ...string) ([][]driver.Value, []string) {
	columns := []string{"Field", "Type", "Null", "Key", "Default", "Extra"}
	data := make([][]driver.Value, 0, len(fields))
	for _, f := range fields {
		data = append(data, []driver.Value{
			[]byte(f), []byte("int(11)"), []byte("YES"), []byte(""), nil, []byte(""),
		})
	}
	return data, columns
}

// showKeysResult 构造 SHOW KEYS 的 15 列结果集（MySQL 8.0 形态），
// Column_name 位于索引 4，与 GetTablePrimaryKeys 的提取逻辑一致
func showKeysResult(keyColumns ...string) ([][]driver.Value, []string) {
	columns := []string{
		"Table", "Non_unique", "Key_name", "Seq_in_index", "Column_name",
		"Collation", "Cardinality", "Sub_part", "Packed", "Null",
		"Index_type", "Comment", "Index_comment", "Visible", "Expression",
	}
	data := make([][]driver.Value, 0, len(keyColumns))
	for i, col := range keyColumns {
		row := make([]driver.Value, len(columns))
		row[0] = []byte("t")
		row[1] = int64(0)
		row[2] = []byte("PRIMARY")
		row[3] = int64(i + 1)
		row[4] = []byte(col)
		row[10] = []byte("BTREE")
		data = append(data, row)
	}
	return data, columns
}

func TestGetTableColumnsWithTypesReturnsErrorOnTruncatedResultSet(t *testing.T) {
	t.Run("完整结果集应返回全部列且无错误", func(t *testing.T) {
		data, columns := showColumnsResult("id", "name", "age")
		conn := newTruncatedConnection(t, columns, data, len(data))

		got, types, err := conn.GetTableColumnsWithTypes("t")
		if err != nil {
			t.Fatalf("完整结果集不应报错，实际 %v", err)
		}
		if len(got) != 3 {
			t.Fatalf("应返回 3 列，实际 %d 列: %v", len(got), got)
		}
		if len(types) != 3 {
			t.Fatalf("应返回 3 个类型，实际 %d", len(types))
		}
	})

	t.Run("结果集截断必须报错而非返回部分列清单", func(t *testing.T) {
		data, columns := showColumnsResult("id", "name", "age")
		// 只成功返回第 1 行后中断：修复前会返回 (["id"], types, nil)，
		// 该残缺列清单会被 sync_data.go 直接用于构造 SELECT 与 CopyFrom 的 copyColumns
		conn := newTruncatedConnection(t, columns, data, 1)

		got, _, err := conn.GetTableColumnsWithTypes("t")
		if err == nil {
			t.Fatalf("结果集被截断时必须返回错误，实际返回 %v 列且 err=nil", got)
		}
		if got != nil {
			t.Errorf("出错时应返回 nil 列清单，实际 %v", got)
		}
	})
}

func TestGetTableColumnsReturnsErrorOnTruncatedResultSet(t *testing.T) {
	data, columns := showColumnsResult("id", "name")
	conn := newTruncatedConnection(t, columns, data, 1)

	got, err := conn.GetTableColumns("t")
	if err == nil {
		t.Fatalf("结果集被截断时必须返回错误，实际返回 %v 且 err=nil", got)
	}
	if got != nil {
		t.Errorf("出错时应返回 nil 列清单，实际 %v", got)
	}
}

func TestGetTablePrimaryKeysReturnsErrorOnTruncatedCompositeKey(t *testing.T) {
	t.Run("完整复合主键应全部返回", func(t *testing.T) {
		data, columns := showKeysResult("a", "b", "c")
		conn := newTruncatedConnection(t, columns, data, len(data))

		got, err := conn.GetTablePrimaryKeys("t")
		if err != nil {
			t.Fatalf("完整结果集不应报错，实际 %v", err)
		}
		if len(got) != 3 {
			t.Fatalf("应返回 3 个主键列，实际 %v", got)
		}
	})

	// 这是本 issue 的核心场景：复合主键 (a,b,c) 被截断成 (a,b) 时，
	// len(primaryKeys)==2 会顺利通过"没有主键"的空检查，
	// 使 keyset 分页的 WHERE (a,b) > (?,?) 游标不再唯一 → 跨批次漏行 + 重复行，
	// 而漏 N 行与重 N 行在 COUNT(*) 校验下可能刚好抵消，报告"数据一致"
	t.Run("复合主键被截断必须报错而非返回部分主键", func(t *testing.T) {
		data, columns := showKeysResult("a", "b", "c")
		conn := newTruncatedConnection(t, columns, data, 2)

		got, err := conn.GetTablePrimaryKeys("t")
		if err == nil {
			t.Fatalf("复合主键被截断时必须返回错误，实际返回 %v 且 err=nil", got)
		}
		if got != nil {
			t.Errorf("出错时应返回 nil 主键清单，实际 %v", got)
		}
	})

	t.Run("完全无主键仍应报原有的没有主键错误", func(t *testing.T) {
		conn := newTruncatedConnection(t, []string{"Field"}, nil, 0)

		_, err := conn.GetTablePrimaryKeys("t")
		if err == nil {
			t.Fatal("无主键表应返回错误")
		}
		if !strings.Contains(err.Error(), "没有主键") {
			t.Errorf("应报'没有主键'，实际 %q", err.Error())
		}
	})
}
