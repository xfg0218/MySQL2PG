package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/yourusername/mysql2pg/internal/config"
)

// Connection PostgreSQL连接管理器
type Connection struct {
	pool   *pgxpool.Pool
	config *config.PostgreSQLConfig
}

// NewConnection 创建新的PostgreSQL连接
func NewConnection(config *config.PostgreSQLConfig) (*Connection, error) {
	ctx := context.Background()

	// 使用无压缩连接
	connStr := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
		config.Host, config.Port, config.Username, config.Password, config.Database)

	poolConfig, err := pgxpool.ParseConfig(connStr)
	if err != nil {
		return nil, fmt.Errorf("解析PostgreSQL连接配置失败: %w", err)
	}

	// 设置连接池大小
	poolConfig.MaxConns = int32(config.MaxConns) // 使用配置文件中的最大连接数

	// 创建连接池
	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		return nil, fmt.Errorf("创建PostgreSQL连接池失败: %w", err)
	}

	// 测试连接
	if err := pool.Ping(ctx); err != nil {
		return nil, fmt.Errorf("PostgreSQL连接测试失败: %w", err)
	}

	return &Connection{
		pool:   pool,
		config: config,
	}, nil
}

// Close 关闭连接池
func (c *Connection) Close() error {
	c.pool.Close()
	return nil
}

// GetPool 获取底层连接池
func (c *Connection) GetPool() *pgxpool.Pool {
	return c.pool
}

// BeginTransaction 开始事务
func (c *Connection) BeginTransaction(ctx context.Context) (pgx.Tx, error) {
	return c.pool.Begin(ctx)
}

// ExecuteDDL 执行DDL语句
func (c *Connection) ExecuteDDL(ddl string) error {
	ctx := context.Background()
	_, err := c.pool.Exec(ctx, ddl)
	if err != nil {
		return fmt.Errorf("执行DDL失败: %w, SQL: %s", err, ddl)
	}
	return err
}

// ExecuteDDLWithTransaction 在事务中执行DDL语句
func (c *Connection) ExecuteDDLWithTransaction(tx pgx.Tx, ddl string) error {
	_, err := tx.Exec(context.Background(), ddl)
	return err
}

// InsertData 插入数据
func (c *Connection) InsertData(tableName string, columns []string, rows *sql.Rows) error {
	ctx := context.Background()

	// 构建占位符模板
	placeholders := make([]string, len(columns))
	for i := range placeholders {
		placeholders[i] = fmt.Sprintf("$%d", i+1)
	}
	placeholdersStr := strings.Join(placeholders, ", ")

	// 构建列名字符串（添加双引号以保持大小写）
	var quotedColumns []string
	for _, col := range columns {
		quotedColumns = append(quotedColumns, fmt.Sprintf(`"%s"`, col))
	}
	columnsStr := strings.Join(quotedColumns, ", ")

	// 构建插入语句
	query := fmt.Sprintf("INSERT INTO \"%s\" (%s) VALUES (%s)", tableName, columnsStr, placeholdersStr)

	// 逐行插入数据
	for rows.Next() {
		// 创建值切片
		values := make([]interface{}, len(columns))
		valuePtrs := make([]interface{}, len(columns))

		for i := range values {
			valuePtrs[i] = &values[i]
		}

		// 扫描行数据
		if err := rows.Scan(valuePtrs...); err != nil {
			return fmt.Errorf("扫描行数据失败: %w", err)
		}

		// 执行插入
		_, err := c.pool.Exec(ctx, query, values...)
		if err != nil {
			return fmt.Errorf("执行插入失败: %w", err)
		}
	}

	return rows.Err()
}

// InsertDataWithTransaction 在事务中插入数据
func (c *Connection) InsertDataWithTransaction(tx pgx.Tx, tableName string, columns []string, rows *sql.Rows) error {
	ctx := context.Background()

	// 构建占位符模板
	placeholders := make([]string, len(columns))
	for i := range placeholders {
		placeholders[i] = fmt.Sprintf("$%d", i+1)
	}
	placeholdersStr := strings.Join(placeholders, ", ")

	// 构建列名字符串（添加双引号以保持大小写）
	var quotedColumns []string
	for _, col := range columns {
		quotedColumns = append(quotedColumns, fmt.Sprintf(`"%s"`, col))
	}
	columnsStr := strings.Join(quotedColumns, ", ")

	// 构建插入语句
	query := fmt.Sprintf("INSERT INTO \"%s\" (%s) VALUES (%s)", tableName, columnsStr, placeholdersStr)

	// 逐行插入数据
	for rows.Next() {
		// 创建值切片
		values := make([]interface{}, len(columns))
		valuePtrs := make([]interface{}, len(columns))

		for i := range values {
			valuePtrs[i] = &values[i]
		}

		// 扫描行数据
		if err := rows.Scan(valuePtrs...); err != nil {
			return fmt.Errorf("扫描行数据失败: %w", err)
		}

		// 执行插入
		_, err := tx.Exec(ctx, query, values...)
		if err != nil {
			return fmt.Errorf("执行插入失败: %w", err)
		}
	}

	return rows.Err()
}

// BatchInsertDataWithTransaction 在事务中批量插入数据
func (c *Connection) BatchInsertDataWithTransaction(tx pgx.Tx, tableName string, columns []string, batchSize int, rows *sql.Rows) error {
	ctx := context.Background()

	// 构建列名字符串
	var quotedColumns []string
	for _, col := range columns {
		quotedColumns = append(quotedColumns, fmt.Sprintf(`"%s"`, col))
	}
	columnsStr := strings.Join(quotedColumns, ", ")

	// 准备批量插入
	var batchValues []interface{}
	var rowCount int

	// 严格使用传入的batchSize参数，不使用硬编码默认值
	effectiveBatchSize := batchSize
	if effectiveBatchSize <= 0 {
		effectiveBatchSize = 10000 // 确保至少有一个合理的默认值
	}

	// 预分配切片容量，减少内存分配
	batchValues = make([]interface{}, 0, effectiveBatchSize*len(columns))

	// 重用values和valuePtrs切片，减少内存分配
	values := make([]interface{}, len(columns))
	valuePtrs := make([]interface{}, len(columns))
	for i := range values {
		valuePtrs[i] = &values[i]
	}

	// 处理数据行
	for rows.Next() {
		// 扫描行数据
		if err := rows.Scan(valuePtrs...); err != nil {
			return fmt.Errorf("扫描行数据失败: %w", err)
		}

		// 添加到批量值中
		batchValues = append(batchValues, values...)
		rowCount++

		// 当达到批量大小时执行插入
		if rowCount == effectiveBatchSize {
			if err := c.executeBatchInsert(tx, ctx, tableName, columnsStr, columns, batchValues); err != nil {
				return err
			}
			batchValues = batchValues[:0] // 重置切片，保留容量
			rowCount = 0
		}
	}

	// 执行剩余的数据
	if len(batchValues) > 0 {
		if err := c.executeBatchInsert(tx, ctx, tableName, columnsStr, columns, batchValues); err != nil {
			return err
		}
	}

	if err := rows.Err(); err != nil {
		return err
	}

	return nil
}

// executeBatchInsert 执行批量插入操作
func (c *Connection) executeBatchInsert(tx pgx.Tx, ctx context.Context, tableName, columnsStr string, columns []string, values []interface{}) error {
	// 计算批次大小，确保总参数数量不超过PostgreSQL的限制(65535)
	columnCount := len(columns)
	// 计算每个批次的最大行数，确保总参数数量不超过65535
	maxRowsPerBatch := 65535 / columnCount
	if maxRowsPerBatch == 0 {
		maxRowsPerBatch = 1 // 确保至少有一行
	}

	// 计算总共有多少行数据
	totalRows := len(values) / columnCount

	// 分批执行
	for i := 0; i < totalRows; i += maxRowsPerBatch {
		end := i + maxRowsPerBatch
		if end > totalRows {
			end = totalRows
		}

		// 计算当前批次的起始和结束索引
		startIdx := i * columnCount
		endIdx := end * columnCount

		// 获取当前批次的值
		batchValues := values[startIdx:endIdx]

		// 构建VALUES部分
		var valuesParts strings.Builder
		// 预分配更大的内存
		valuesParts.Grow((end - i) * (columnCount*4 + 5)) // 增加预分配空间

		// 生成参数占位符
		for row := 0; row < end-i; row++ {
			if row > 0 {
				valuesParts.WriteString(", ")
			}
			valuesParts.WriteString("(")
			for col := 0; col < columnCount; col++ {
				if col > 0 {
					valuesParts.WriteString(", ")
				}
				valuesParts.WriteString("$")
				valuesParts.WriteString(strconv.Itoa(row*columnCount + col + 1))
			}
			valuesParts.WriteString(")")
		}

		// 构建完整的SQL语句
		query := fmt.Sprintf("INSERT INTO \"%s\" (%s) VALUES %s", tableName, columnsStr, valuesParts.String())

		// 执行批量插入
		_, err := tx.Exec(ctx, query, batchValues...)
		if err != nil {
			// 打印错误信息和部分数据样本
			sampleSize := 5
			if len(batchValues) < sampleSize {
				sampleSize = len(batchValues)
			}
			var samples []string
			for j := 0; j < sampleSize; j++ {
				samples = append(samples, fmt.Sprintf("%v", batchValues[j]))
			}
			return fmt.Errorf("批量插入失败: %w, 数据样本: %v", err, samples)
		}
	}

	return nil
}

// GetVersion 获取PostgreSQL版本信息
func (c *Connection) GetVersion() (string, error) {
	ctx := context.Background()
	var version string
	err := c.pool.QueryRow(ctx, "SELECT version()").Scan(&version)
	if err != nil {
		return "", fmt.Errorf("获取PostgreSQL版本失败: %w", err)
	}
	return version, nil
}

// TestConnection 测试PostgreSQL连接
func TestConnection(config *config.PostgreSQLConfig) error {
	// 测试连接时不使用压缩
	conn, err := NewConnection(config)
	if err != nil {
		return fmt.Errorf("PostgreSQL连接测试失败: %w", err)
	}
	defer conn.Close()

	return nil
}

// TableExists 检查表是否存在
func (c *Connection) TableExists(tableName string) (bool, error) {
	ctx := context.Background()
	query := `
		SELECT EXISTS (
			SELECT 1 
			FROM information_schema.tables 
			WHERE table_schema = 'public' 
			AND table_name = $1
		)
	`
	var exists bool
	err := c.pool.QueryRow(ctx, query, tableName).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("检查表是否存在失败: %w", err)
	}
	return exists, nil
}

// GrantTablePrivileges 授予表权限
func (c *Connection) GrantTablePrivileges(user, tableName string, privileges []string) error {
	ctx := context.Background()

	// 构建权限字符串
	privilegesStr := strings.Join(privileges, ", ")

	// 构建授权语句
	query := fmt.Sprintf("GRANT %s ON TABLE \"%s\" TO %s", privilegesStr, tableName, user)

	_, err := c.pool.Exec(ctx, query)
	if err != nil {
		return fmt.Errorf("授予表权限失败: %w", err)
	}

	return nil
}

// GetTablePrivileges 获取表的权限信息
func (c *Connection) GetTablePrivileges(tableName string) ([]map[string]string, error) {
	ctx := context.Background()

	query := `
		SELECT 
			grantee::regrole::text AS "user_or_role", 
			privilege_type, 
			is_grantable 
		FROM 
			information_schema.role_table_grants 
		WHERE 
			table_schema = 'public' 
			AND table_name = $1
	`

	rows, err := c.pool.Query(ctx, query, tableName)
	if err != nil {
		return nil, fmt.Errorf("获取表权限失败: %w", err)
	}
	defer rows.Close()

	var privileges []map[string]string
	for rows.Next() {
		var user, privilege, isGrantable string
		if err := rows.Scan(&user, &privilege, &isGrantable); err != nil {
			return nil, fmt.Errorf("扫描表权限信息失败: %w", err)
		}

		privileges = append(privileges, map[string]string{
			"user":         user,
			"privilege":    privilege,
			"is_grantable": isGrantable,
		})
	}

	return privileges, nil
}

// GetTableRowCount 获取表的行数
func (c *Connection) GetTableRowCount(tableName string) (int64, error) {
	ctx := context.Background()
	query := fmt.Sprintf("SELECT COUNT(*) FROM \"%s\"", tableName)

	var count int64
	err := c.pool.QueryRow(ctx, query).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("获取表 %s 行数失败: %w", tableName, err)
	}

	return count, nil
}

// BatchInsertDataWithTransactionAndGetLastValue 在事务中批量插入数据并获取最后一个主键值
func (c *Connection) BatchInsertDataWithTransactionAndGetLastValue(tx pgx.Tx, tableName string, columns []string, batchSize int, primaryKey string, rows *sql.Rows) (int, interface{}, error) {
	ctx := context.Background()

	// 准备批量插入
	var rowCount int
	var totalRows int

	// 严格使用传入的batchSize参数，不使用硬编码默认值
	effectiveBatchSize := batchSize
	if effectiveBatchSize <= 0 {
		effectiveBatchSize = 10000 // 确保至少有一个合理的默认值
	}

	// 跟踪最后一个主键值
	var lastValue interface{}
	var primaryKeyIndex int = -1

	// 找到主键列的索引
	if primaryKey != "" {
		for i, col := range columns {
			if col == primaryKey {
				primaryKeyIndex = i
				break
			}
		}
	}

	// 重用values和valuePtrs切片，减少内存分配
	values := make([]interface{}, len(columns))
	valuePtrs := make([]interface{}, len(columns))
	for i := range values {
		valuePtrs[i] = &values[i]
	}

	// 使用pgx的CopyFrom函数进行高效批量插入
	copyRows := make([][]interface{}, 0, effectiveBatchSize)

	// 处理数据行
	for rows.Next() {
		// 扫描行数据
		if err := rows.Scan(valuePtrs...); err != nil {
			return 0, nil, fmt.Errorf("扫描行数据失败: %w", err)
		}

		// 跟踪最后一个主键值
		if primaryKeyIndex != -1 {
			lastValue = values[primaryKeyIndex]
		}

		// 复制当前行的值到新的切片并进行类型转换
		rowValues := make([]interface{}, len(values))
		for i, v := range values {
			// 进行数据类型转换，处理MySQL和PostgreSQL之间的类型差异
			switch val := v.(type) {
			case []byte:
				// 将[]byte转换为字符串，pgx会自动处理后续的类型转换
				rowValues[i] = string(val)
			default:
				// 其他类型保持不变
				rowValues[i] = val
			}
		}
		copyRows = append(copyRows, rowValues)

		rowCount++
		totalRows++

		// 当达到批量大小时执行CopyFrom
		if rowCount == effectiveBatchSize {
			// 执行CopyFrom
			_, err := tx.CopyFrom(ctx, pgx.Identifier{tableName}, columns, pgx.CopyFromRows(copyRows))
			if err != nil {
				return 0, nil, fmt.Errorf("CopyFrom执行失败: %w", err)
			}

			// 重置切片和计数器
			copyRows = make([][]interface{}, 0, effectiveBatchSize)
			rowCount = 0
		}
	}

	// 执行剩余的数据
	if rowCount > 0 {
		_, err := tx.CopyFrom(ctx, pgx.Identifier{tableName}, columns, pgx.CopyFromRows(copyRows))
		if err != nil {
			return 0, nil, fmt.Errorf("CopyFrom执行失败: %w", err)
		}
	}

	if err := rows.Err(); err != nil {
		return 0, nil, err
	}

	// 只有在没有找到主键值的情况下，才执行MAX查询（作为后备方案）
	if primaryKey != "" && lastValue == nil {
		query := fmt.Sprintf("SELECT MAX(\"%s\") FROM \"%s\"", primaryKey, tableName)
		err := tx.QueryRow(ctx, query).Scan(&lastValue)
		if err != nil && err != pgx.ErrNoRows {
			return 0, nil, fmt.Errorf("获取最后一个主键值失败: %w", err)
		}
	}

	return totalRows, lastValue, nil
}
