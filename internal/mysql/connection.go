package mysql

import (
	"database/sql"
	"fmt"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/yourusername/mysql2pg/internal/config"
)

// Connection MySQL连接管理器
type Connection struct {
	db     *sql.DB
	config *config.MySQLConfig
}

// NewConnection 创建新的MySQL连接
func NewConnection(config *config.MySQLConfig) (*Connection, error) {
	// 使用无压缩连接
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?parseTime=true&charset=utf8mb4",
		config.Username, config.Password, config.Host, config.Port, config.Database)

	// 添加连接参数
	if config.ConnectionParams != "" {
		// 检查是否需要添加&符号
		if !strings.HasPrefix(config.ConnectionParams, "&") {
			dsn += "&"
		}
		dsn += config.ConnectionParams
	}

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, fmt.Errorf("打开MySQL连接失败: %w", err)
	}

	// 优化连接池配置
	db.SetMaxOpenConns(config.MaxOpenConns)                                    // 最大打开连接数
	db.SetMaxIdleConns(config.MaxIdleConns)                                    // 最大空闲连接数
	db.SetConnMaxLifetime(time.Duration(config.ConnMaxLifetime) * time.Second) // 连接最大生命周期

	// 测试连接
	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("MySQL连接测试失败: %w", err)
	}

	return &Connection{
		db:     db,
		config: config,
	}, nil
}

// Close 关闭连接
func (c *Connection) Close() error {
	return c.db.Close()
}

// GetDB 获取底层数据库连接
func (c *Connection) GetDB() *sql.DB {
	return c.db
}

// GetTableColumns 获取表的列信息
func (c *Connection) GetTableColumns(tableName string) ([]string, error) {
	rows, err := c.db.Query(fmt.Sprintf("SHOW COLUMNS FROM `%s`", tableName))
	if err != nil {
		return nil, fmt.Errorf("获取表列信息失败: %w", err)
	}
	defer rows.Close()

	var columns []string
	for rows.Next() {
		var field, colType, null, key, extra string
		var defaultValue sql.NullString

		if err := rows.Scan(&field, &colType, &null, &key, &defaultValue, &extra); err != nil {
			return nil, fmt.Errorf("扫描列信息失败: %w", err)
		}

		columns = append(columns, field)
	}

	return columns, nil
}

// GetTableColumnsWithTypes 获取表的列名和类型信息
func (c *Connection) GetTableColumnsWithTypes(tableName string) ([]string, map[string]string, error) {
	rows, err := c.db.Query(fmt.Sprintf("SHOW COLUMNS FROM `%s`", tableName))
	if err != nil {
		return nil, nil, fmt.Errorf("获取表列信息失败: %w", err)
	}
	defer rows.Close()

	var columns []string
	columnTypes := make(map[string]string)

	for rows.Next() {
		var field, colType, null, key, extra string
		var defaultValue sql.NullString

		if err := rows.Scan(&field, &colType, &null, &key, &defaultValue, &extra); err != nil {
			return nil, nil, fmt.Errorf("扫描列信息失败: %w", err)
		}

		columns = append(columns, field)
		columnTypes[field] = colType
	}

	return columns, columnTypes, nil
}

// GetTableData 获取表数据
func (c *Connection) GetTableData(tableName string, columns []string, offset, limit int, orderBy string) (*sql.Rows, error) {
	// 使用反引号包围表名和列名，以处理包含特殊字符的名称
	var quotedColumns []string
	for _, col := range columns {
		quotedColumns = append(quotedColumns, fmt.Sprintf("`%s`", col))
	}
	columnsStr := strings.Join(quotedColumns, ", ")

	// 对于大表，使用LIMIT和OFFSET可能会导致性能问题
	// 但在没有主键的情况下，这是唯一的选择
	query := fmt.Sprintf("SELECT %s FROM `%s`", columnsStr, tableName)
	if orderBy != "" {
		query += fmt.Sprintf(" ORDER BY %s", orderBy)
	}
	query += fmt.Sprintf(" LIMIT %d OFFSET %d", limit, offset)

	rows, err := c.db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("获取表数据失败: %w", err)
	}

	return rows, nil
}

// GetTableDataWithPagination 使用基于主键的分页获取表数据
func (c *Connection) GetTableDataWithPagination(tableName string, columns []string, primaryKey string, lastValue interface{}, limit int) (*sql.Rows, error) {
	// 使用反引号包围表名、列名和主键，以处理包含特殊字符的名称
	var quotedColumns []string
	for _, col := range columns {
		quotedColumns = append(quotedColumns, fmt.Sprintf("`%s`", col))
	}
	columnsStr := strings.Join(quotedColumns, ", ")

	var query string
	var args []interface{}

	if lastValue != nil {
		query = fmt.Sprintf("SELECT %s FROM `%s` WHERE `%s` > ? ORDER BY `%s` LIMIT %d",
			columnsStr, tableName, primaryKey, primaryKey, limit)
		args = []interface{}{lastValue}
	} else {
		query = fmt.Sprintf("SELECT %s FROM `%s` ORDER BY `%s` LIMIT %d",
			columnsStr, tableName, primaryKey, limit)
	}

	rows, err := c.db.Query(query, args...)
	if err != nil {
		return nil, fmt.Errorf("获取表数据失败: %w", err)
	}

	return rows, nil
}

// GetTablePrimaryKeys 获取表的主键列名列表
func (c *Connection) GetTablePrimaryKeys(tableName string) ([]string, error) {
	// 使用SHOW KEYS FROM语句获取主键信息，避免查询information_schema导致的权限问题
	// 这样可以同时兼容MySQL 5.7和MySQL 8.0
	query := fmt.Sprintf("SHOW KEYS FROM `%s` WHERE Key_name = 'PRIMARY'", tableName)

	rows, err := c.db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("获取表主键失败: %w", err)
	}
	defer rows.Close()

	var primaryKeys []string
	for rows.Next() {
		// SHOW KEYS FROM返回的字段顺序：
		// Table, Non_unique, Key_name, Seq_in_index, Column_name, Collation, Cardinality, Sub_part, Packed, Null, Index_type, Comment, Index_comment, Visible, Expression
		// 尝试扫描，忽略列数不匹配的错误（为了兼容不同版本的MySQL）
		columns, _ := rows.Columns()
		values := make([]interface{}, len(columns))
		for i := range values {
			values[i] = new(interface{})
		}

		if err := rows.Scan(values...); err != nil {
			return nil, fmt.Errorf("扫描主键信息失败: %w", err)
		}

		var columnName string
		// Column_name通常是第5列 (索引4)
		if len(columns) >= 5 {
			if val, ok := (*values[4].(*interface{})).([]byte); ok {
				columnName = string(val)
			} else if val, ok := (*values[4].(*interface{})).(string); ok {
				columnName = val
			}
		}

		if columnName != "" {
			primaryKeys = append(primaryKeys, columnName)
		}
	}

	if len(primaryKeys) == 0 {
		return nil, fmt.Errorf("表 %s 没有主键", tableName)
	}

	return primaryKeys, nil
}

// GetTablePrimaryKey 获取表的主键列名
func (c *Connection) GetTablePrimaryKey(tableName string) (string, error) {
	primaryKeys, err := c.GetTablePrimaryKeys(tableName)
	if err != nil {
		return "", err
	}

	if len(primaryKeys) > 1 {
		return "", fmt.Errorf("表 %s 有复合主键 %v，不支持基于主键的分页", tableName, primaryKeys)
	}

	return primaryKeys[0], nil
}

// EstimateRowSize 估算单行数据大小
func (c *Connection) EstimateRowSize(tableName string) (int64, error) {
	// 获取表的列信息
	columns, err := c.GetTableColumns(tableName)
	if err != nil {
		return 0, err
	}

	// 直接使用简单估算，避免查询information_schema.TABLES导致的权限问题
	// 这样可以同时兼容MySQL 5.7和MySQL 8.0
	// 简单估算：假设每列平均占用20字节
	avgRowLength := int64(len(columns) * 20)

	return avgRowLength, nil
}

// GetTableRowCount 获取表的行数
func (c *Connection) GetTableRowCount(tableName string) (int64, error) {
	var count int64
	err := c.db.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM `%s`", tableName)).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("获取表行数失败: %w", err)
	}

	return count, nil
}

// GetVersion 获取MySQL版本信息
func (c *Connection) GetVersion() (string, error) {
	var version string
	err := c.db.QueryRow("SELECT VERSION()").Scan(&version)
	if err != nil {
		return "", fmt.Errorf("获取MySQL版本失败: %w", err)
	}
	return version, nil
}

// TestConnection 测试MySQL连接
func TestConnection(config *config.MySQLConfig) error {
	// 测试连接时不使用压缩
	conn, err := NewConnection(config)
	if err != nil {
		return fmt.Errorf("MySQL连接测试失败: %w", err)
	}
	defer conn.Close()

	return nil
}
