package postgres

import (
	"context"
	"database/sql"
	"encoding/binary"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

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

	// 添加连接参数
	if config.PgConnectionParams != "" {
		connStr += " " + config.PgConnectionParams
	}

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
	// 接受原始DDL，不转换为小写
	lowercaseDDL := ddl
	// 将char(0)转换为char(10)，因为PostgreSQL不允许char(0)类型
	lowercaseDDL = strings.ReplaceAll(lowercaseDDL, "char(0)", "char(10)")

	_, err := c.pool.Exec(ctx, lowercaseDDL)
	if err != nil {
		return fmt.Errorf("执行DDL失败: %w, PostgreSQL SQL: %s", err, lowercaseDDL)
	}
	return err
}

// ExecuteDDLWithTransaction 在事务中执行DDL语句
func (c *Connection) ExecuteDDLWithTransaction(tx pgx.Tx, ddl string) error {
	// 将DDL转换为小写
	lowercaseDDL := strings.ToLower(ddl)

	// 将char(0)转换为char(10)，因为PostgreSQL不允许char(0)类型
	lowercaseDDL = strings.ReplaceAll(lowercaseDDL, "char(0)", "char(10)")

	_, err := tx.Exec(context.Background(), lowercaseDDL)
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
func (c *Connection) BatchInsertDataWithTransactionAndGetLastValue(tx pgx.Tx, tableName string, columns []string, columnTypes map[string]string, batchSize int, primaryKey string, rows *sql.Rows) (int, interface{}, error) {
	ctx := context.Background()

	// 准备批量插入
	var rowCount int
	var totalRows int

	// 严格使用传入的batchSize参数，不使用硬编码默认值
	effectiveBatchSize := batchSize
	if effectiveBatchSize <= 0 {
		effectiveBatchSize = 10000 // 确保至少有一个合理的默认值
	}

	// 将所有列名转换为小写，以匹配PostgreSQL的默认行为
	lowercaseColumns := make([]string, len(columns))
	for i, col := range columns {
		lowercaseColumns[i] = strings.ToLower(col)
	}

	// 跟踪最后一个主键值
	var lastValue interface{}
	var primaryKeyIndex int = -1

	// 找到主键列的索引
	if primaryKey != "" {
		// 同样将主键名转换为小写进行比较
		lowercasePrimaryKey := strings.ToLower(primaryKey)
		for i, col := range lowercaseColumns {
			if col == lowercasePrimaryKey {
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
				sVal := string(val)
				// 检查是否为Point类型并尝试转换
				if columnTypes != nil {
					colType, ok := columnTypes[columns[i]]
					if ok {
						colTypeLower := strings.ToLower(colType)
						if strings.Contains(colTypeLower, "point") || strings.Contains(colTypeLower, "geometry") {
							if pointStr, err := parseMySQLPoint(val); err == nil {
								rowValues[i] = pointStr
								continue
							}
						}
					}
				}

				// 处理MySQL零值时间
				if sVal == "0000-00-00 00:00:00" || sVal == "0000-00-00" {
					rowValues[i] = nil
				} else {
					// 将[]byte转换为字符串，pgx会自动处理后续的类型转换
					rowValues[i] = sVal
				}
			case string:
				if val == "0000-00-00 00:00:00" || val == "0000-00-00" {
					rowValues[i] = nil
				} else {
					rowValues[i] = val
				}
			case time.Time:
				if val.IsZero() {
					rowValues[i] = nil
				} else {
					rowValues[i] = val
				}
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
			// 执行CopyFrom，使用转换后的小写列名
			_, err := tx.CopyFrom(ctx, pgx.Identifier{tableName}, lowercaseColumns, pgx.CopyFromRows(copyRows))
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
		// 执行CopyFrom，使用转换后的小写列名
		_, err := tx.CopyFrom(ctx, pgx.Identifier{tableName}, lowercaseColumns, pgx.CopyFromRows(copyRows))
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

// parseMySQLPoint 解析MySQL的WKB格式Point数据
func parseMySQLPoint(data []byte) (string, error) {
	// MySQL Geometry Header (4 bytes SRID) + WKB (1 byte order + 4 bytes type + 16 bytes coords)
	// SRID (4) + Order (1) + Type (4) + X (8) + Y (8) = 25 bytes
	if len(data) != 25 {
		return "", fmt.Errorf("invalid MySQL point data length: %d", len(data))
	}

	// Skip SRID (4 bytes)
	// Byte order (1 byte)
	order := data[4]

	var x, y float64

	if order == 1 { // Little Endian
		// Check type (Point = 1)
		typeCode := binary.LittleEndian.Uint32(data[5:9])
		if typeCode != 1 {
			return "", fmt.Errorf("not a point type: %d", typeCode)
		}
		xBits := binary.LittleEndian.Uint64(data[9:17])
		yBits := binary.LittleEndian.Uint64(data[17:25])
		x = math.Float64frombits(xBits)
		y = math.Float64frombits(yBits)
	} else { // Big Endian
		// Check type (Point = 1)
		typeCode := binary.BigEndian.Uint32(data[5:9])
		if typeCode != 1 {
			return "", fmt.Errorf("not a point type: %d", typeCode)
		}
		xBits := binary.BigEndian.Uint64(data[9:17])
		yBits := binary.BigEndian.Uint64(data[17:25])
		x = math.Float64frombits(xBits)
		y = math.Float64frombits(yBits)
	}

	// 格式化为PostgreSQL Point格式 (x,y)
	return fmt.Sprintf("(%v,%v)", x, y), nil
}
