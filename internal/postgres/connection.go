package postgres

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/binary"
	"fmt"
	"math"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/yourusername/mysql2pg/internal/config"
)

// PostgreSQLVersionInfo PostgreSQL 版本信息
type PostgreSQLVersionInfo struct {
	Major               int    // 主版本号
	Minor               int    // 次版本号
	Full                string // 完整版本字符串
	SupportsJsonbPath   bool   // PG 14+ 支持 JSONB 路径查询
	SupportsAdvancedAgg bool   // PG 13+ 支持高级聚合
	SupportsSqlJson     bool   // PG 16+ 支持 SQL/JSON 标准
}

// IsVersionGreaterOrEqual 检查当前版本是否大于等于指定版本
func (p *PostgreSQLVersionInfo) IsVersionGreaterOrEqual(major, minor int) bool {
	if p.Major > major {
		return true
	}
	if p.Major == major {
		return p.Minor >= minor
	}
	return false
}

// Connection PostgreSQL 连接管理器
type Connection struct {
	pool   *pgxpool.Pool
	config *config.PostgreSQLConfig
	ctx    context.Context
}

// context 返回连接持有的根 context
// 未通过 NewConnection 构造时回退为 context.Background()，保证 nil 安全
func (c *Connection) context() context.Context {
	if c.ctx == nil {
		return context.Background()
	}
	return c.ctx
}

// GetPgConnectionParams 获取 PostgreSQL 连接参数（导出方法）
func (c *Connection) GetPgConnectionParams() string {
	return c.config.PgConnectionParams
}

var charZeroPattern = regexp.MustCompile(`(?i)char\s*\(\s*0\s*\)`)

// 预编译的零日期 []byte 常量，避免在热路径中重复 string() 转换
var (
	zeroDateTimeBytes1 = []byte("0000-00-00 00:00:00")
	zeroDateTimeBytes2 = []byte("0000-00-00")
)

// rowSlicePool 复用 []interface{} 切片，避免每行都 make 新切片
// 多级池设计（4 级 size-class），覆盖 1-256 列场景
// Level 0: 1-8 列    → 容量 8
// Level 1: 9-32 列   → 容量 32
// Level 2: 33-128 列 → 容量 128
// Level 3: 129-256 列→ 容量 256
// Fallback: >256 列  → 直接 make（带告警日志）
var rowSlicePools = [4]*sync.Pool{
	{New: func() any { s := make([]interface{}, 0, 8); return &s }},
	{New: func() any { s := make([]interface{}, 0, 32); return &s }},
	{New: func() any { s := make([]interface{}, 0, 128); return &s }},
	{New: func() any { s := make([]interface{}, 0, 256); return &s }},
}

// getRowSlice 从多级池中获取 rowSlice
func getRowSlice(numCols int) []interface{} {
	var pool *sync.Pool
	if numCols <= 8 {
		pool = rowSlicePools[0]
	} else if numCols <= 32 {
		pool = rowSlicePools[1]
	} else if numCols <= 128 {
		pool = rowSlicePools[2]
	} else if numCols <= 256 {
		pool = rowSlicePools[3]
	} else {
		// Fallback: >256 列，直接分配（不进入池）
		return make([]interface{}, numCols)
	}
	s := pool.Get().(*[]interface{})
	return (*s)[:numCols]
}

// putRowSlice 将 rowSlice 返回多级池复用
func putRowSlice(s []interface{}) {
	cap := cap(s)
	// 重置为零长度，保留容量
	s = s[:0]
	switch {
	case cap <= 8:
		rowSlicePools[0].Put(&s)
	case cap <= 32:
		rowSlicePools[1].Put(&s)
	case cap <= 128:
		rowSlicePools[2].Put(&s)
	case cap <= 256:
		rowSlicePools[3].Put(&s)
	default:
		// >256 列的 fallback 切片不回收（避免污染 pool）
	}
}

// typedDest 类型化 Scan 目标，避免 *interface{} 导致的堆分配
type typedDest struct {
	value  interface{}
	isTime bool // 是否需要特殊处理 time.Time
}

// makeTypedDestinations 根据 MySQL 列类型创建类型化的 Scan 目标
// 避免 *interface{} 导致的每次 Scan 都分配具体对象的问题
func makeTypedDestinations(columns []string, columnTypes map[string]string) []typedDest {
	dests := make([]typedDest, len(columns))
	for i, col := range columns {
		colType := ""
		if columnTypes != nil {
			colType = getColumnTypeByName(col, columnTypes)
		}
		dests[i] = makeTypedDest(colType)
	}
	return dests
}

// makeTypedDest 根据列类型创建单个 Scan 目标
func makeTypedDest(colType string) typedDest {
	if isBinaryLikeType(colType) || isGeometryLikeType(colType) {
		return typedDest{value: new([]byte)}
	}
	if colType != "" {
		lower := strings.ToLower(colType)
		// BIGINT UNSIGNED 最大值 18446744073709551615 超出 int64 范围，
		// 必须用 NullString 透传（NullInt64 在 Scan 阶段会因 strconv 越界而失败），
		// 目标列为 NUMERIC(20,0)，文本协议可正确接收十进制字符串
		if strings.Contains(lower, "bigint") && strings.Contains(lower, "unsigned") {
			return typedDest{value: new(sql.NullString)}
		}
		if strings.Contains(lower, "tinyint") || strings.Contains(lower, "smallint") ||
			strings.Contains(lower, "mediumint") || strings.Contains(lower, "int") ||
			strings.Contains(lower, "bigint") || strings.Contains(lower, "year") ||
			strings.Contains(lower, "serial") {
			return typedDest{value: new(sql.NullInt64)}
		}
		if strings.Contains(lower, "decimal") || strings.Contains(lower, "numeric") {
			return typedDest{value: new(sql.NullString)}
		}
		if strings.Contains(lower, "float") || strings.Contains(lower, "real") {
			return typedDest{value: new(sql.NullFloat64)}
		}
		if strings.Contains(lower, "double") {
			return typedDest{value: new(sql.NullFloat64)}
		}
		if strings.Contains(lower, "bit") {
			return typedDest{value: new([]byte)}
		}
		if strings.Contains(lower, "bool") {
			return typedDest{value: new(sql.NullBool)}
		}
		if strings.Contains(lower, "datetime") || strings.Contains(lower, "timestamp") {
			return typedDest{value: new(sql.NullString), isTime: true}
		}
		if strings.Contains(lower, "date") {
			return typedDest{value: new(sql.NullString)}
		}
		if strings.Contains(lower, "time") {
			return typedDest{value: new(sql.NullString)}
		}
	}
	return typedDest{value: new(sql.NullString)}
}

// resetTypedDestinations 重置类型化目标以便复用
// 对于指针类型，只需重置指向的零值
func resetTypedDestinations(dests []typedDest) {
	for i := range dests {
		switch v := dests[i].value.(type) {
		case *sql.NullInt64:
			*v = sql.NullInt64{}
		case *sql.NullString:
			*v = sql.NullString{}
		case *[]byte:
			*v = nil
		case *sql.NullFloat64:
			*v = sql.NullFloat64{}
		case *sql.NullBool:
			*v = sql.NullBool{}
		case *time.Time:
			*v = time.Time{}
			dests[i].isTime = true
		}
	}
}

// scanDestinations 将类型化目标转换为 Scan 可以接受的 *interface{} 参数
func scanDestinations(dests []typedDest) []interface{} {
	ptrs := make([]interface{}, len(dests))
	for i := range dests {
		ptrs[i] = dests[i].value
	}
	return ptrs
}

// getTypedValue 从类型化目标中提取值（解引用）
func getTypedValue(dest *typedDest) interface{} {
	switch v := dest.value.(type) {
	case *sql.NullInt64:
		if !v.Valid {
			return nil
		}
		return v.Int64
	case *sql.NullString:
		if !v.Valid {
			return nil
		}
		return v.String
	case *[]byte:
		if *v == nil {
			return nil
		}
		return *v
	case *sql.NullFloat64:
		if !v.Valid {
			return nil
		}
		return v.Float64
	case *sql.NullBool:
		if !v.Valid {
			return nil
		}
		return v.Bool
	case *time.Time:
		if v.IsZero() {
			return nil
		}
		return *v
	default:
		return v
	}
}

// NewConnection 创建新的 PostgreSQL 连接
// ctx 为根 context（通常来自 signal.NotifyContext），取消后所有进行中的操作会被中断
func NewConnection(ctx context.Context, config *config.PostgreSQLConfig) (*Connection, error) {

	// 构建基础连接字符串
	connStr := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
		config.Host, config.Port, config.Username, config.Password, config.Database)

	// 根据密码加密方式添加参数
	// PostgreSQL 支持两种密码加密方式：
	// - md5: 传统加密方式，兼容性最好
	// - scram-sha-256: 更安全的加密方式（PostgreSQL 10+ 默认）
	// - auto: 自动选择（默认，由 PostgreSQL 服务器决定）
	if config.PasswordEncryption != "" {
		switch strings.ToLower(config.PasswordEncryption) {
		case "md5":
			connStr += " password_encryption=md5"
		case "scram-sha-256":
			connStr += " password_encryption=scram-sha-256"
			// "auto" 或不设置时使用 PostgreSQL 默认行为
		}
	}

	// 添加连接参数
	if config.PgConnectionParams != "" {
		connStr += " " + config.PgConnectionParams
	}

	poolConfig, err := pgxpool.ParseConfig(connStr)
	if err != nil {
		return nil, fmt.Errorf("解析 PostgreSQL 连接配置失败：%w", err)
	}

	// 设置连接池大小
	poolConfig.MaxConns = int32(config.MaxConns) // 使用配置文件中的最大连接数

	// 固定会话时区为 UTC：MySQL TIMESTAMP 列映射为 TIMESTAMPTZ，读取端已固定 UTC
	// （MySQL 会话时区固定为 UTC）；写入端不带显式时区偏移的值按会话 TimeZone 解释，
	// 因此写入端也必须是 UTC，才能保证 instant 语义正确
	poolConfig.AfterConnect = func(ctx context.Context, conn *pgx.Conn) error {
		_, err := conn.Exec(ctx, "SET TimeZone = 'UTC'")
		return err
	}

	// 创建连接池
	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		return nil, fmt.Errorf("创建 PostgreSQL 连接池失败：%w", err)
	}

	// 测试连接
	if err := pool.Ping(ctx); err != nil {
		return nil, fmt.Errorf("PostgreSQL 连接测试失败：%w", err)
	}

	return &Connection{
		pool:   pool,
		config: config,
		ctx:    ctx,
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

// ExecuteDDL 执行 DDL 语句
func (c *Connection) ExecuteDDL(ddl string, originalMysqlDDL ...string) error {
	ctx := c.context()
	execDDL := sanitizeDDLForExecution(ddl)
	_, err := c.pool.Exec(ctx, execDDL)
	if err != nil {
		if len(originalMysqlDDL) > 0 && originalMysqlDDL[0] != "" {
			return fmt.Errorf("执行 DDL 失败：%w\n  MySQL DDL: %s\n  PostgreSQL DDL: %s", err, originalMysqlDDL[0], execDDL)
		}
		return fmt.Errorf("执行 DDL 失败：%w, PostgreSQL SQL: %s", err, execDDL)
	}
	return err
}

// ExecuteDDLWithTransaction 在事务中执行 DDL 语句
func (c *Connection) ExecuteDDLWithTransaction(tx pgx.Tx, ddl string) error {
	execDDL := sanitizeDDLForExecution(ddl)
	_, err := tx.Exec(c.context(), execDDL)
	return err
}

func sanitizeDDLForExecution(ddl string) string {
	return charZeroPattern.ReplaceAllString(ddl, "char(10)")
}

// InsertData 插入数据
func (c *Connection) InsertData(tableName string, columns []string, rows *sql.Rows) error {
	ctx := c.context()

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
			return fmt.Errorf("扫描行数据失败：%w", err)
		}

		// 执行插入
		_, err := c.pool.Exec(ctx, query, values...)
		if err != nil {
			return fmt.Errorf("执行插入失败：%w", err)
		}
	}

	return rows.Err()
}

// InsertDataWithTransaction 在事务中插入数据
func (c *Connection) InsertDataWithTransaction(tx pgx.Tx, tableName string, columns []string, rows *sql.Rows) error {
	ctx := c.context()

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
			return fmt.Errorf("扫描行数据失败：%w", err)
		}

		// 执行插入
		_, err := tx.Exec(ctx, query, values...)
		if err != nil {
			return fmt.Errorf("执行插入失败：%w", err)
		}
	}

	return rows.Err()
}

// BatchInsertDataWithTransaction 在事务中批量插入数据
func (c *Connection) BatchInsertDataWithTransaction(tx pgx.Tx, tableName string, columns []string, batchSize int, rows *sql.Rows) error {
	ctx := c.context()

	// 构建列名字符串
	var quotedColumns []string
	for _, col := range columns {
		quotedColumns = append(quotedColumns, fmt.Sprintf(`"%s"`, col))
	}
	columnsStr := strings.Join(quotedColumns, ", ")

	// 准备批量插入
	var batchValues []interface{}
	var rowCount int

	// 严格使用传入的 batchSize 参数，不使用硬编码默认值
	effectiveBatchSize := batchSize
	if effectiveBatchSize <= 0 {
		effectiveBatchSize = 10000 // 确保至少有一个合理的默认值
	}

	// 预分配切片容量，减少内存分配
	batchValues = make([]interface{}, 0, effectiveBatchSize*len(columns))

	// 重用 values 和 valuePtrs 切片，减少内存分配
	values := make([]interface{}, len(columns))
	valuePtrs := make([]interface{}, len(columns))
	for i := range values {
		valuePtrs[i] = &values[i]
	}

	// 处理数据行
	for rows.Next() {
		// 扫描行数据
		if err := rows.Scan(valuePtrs...); err != nil {
			return fmt.Errorf("扫描行数据失败：%w", err)
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
	// 计算批次大小，确保总参数数量不超过 PostgreSQL 的限制 (65535)
	columnCount := len(columns)
	// 计算每个批次的最大行数，确保总参数数量不超过 65535
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

		// 构建 VALUES 部分
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

		// 构建完整的 SQL 语句
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
			return fmt.Errorf("批量插入失败：%w, 数据样本：%v", err, samples)
		}
	}

	return nil
}

// GetVersion 获取 PostgreSQL 版本信息
func (c *Connection) GetVersion() (string, error) {
	ctx := c.context()
	var version string
	err := c.pool.QueryRow(ctx, "SELECT version()").Scan(&version)
	if err != nil {
		return "", fmt.Errorf("获取 PostgreSQL 版本失败：%w", err)
	}
	return version, nil
}

// GetVersionInfo 获取 PostgreSQL 详细版本信息
func (c *Connection) GetVersionInfo() (*PostgreSQLVersionInfo, error) {
	version, err := c.GetVersion()
	if err != nil {
		return nil, err
	}
	return ParsePostgreSQLVersion(version), nil
}

// ParsePostgreSQLVersion 解析 PostgreSQL 版本字符串
// 支持格式："PostgreSQL 16.3 on x86_64...", "14.5", "12.0" 等
func ParsePostgreSQLVersion(version string) *PostgreSQLVersionInfo {
	info := &PostgreSQLVersionInfo{
		Full: version,
	}

	// 使用正则表达式提取版本号
	re := regexp.MustCompile(`(?:PostgreSQL\s+)?(\d+)\.(\d+)`)
	matches := re.FindStringSubmatch(version)
	if len(matches) >= 3 {
		major, err := strconv.Atoi(matches[1])
		if err == nil {
			info.Major = major
		}
		minor, err := strconv.Atoi(matches[2])
		if err == nil {
			info.Minor = minor
		}
	}

	// 设置特性标志
	info.SupportsJsonbPath = info.Major >= 14
	info.SupportsAdvancedAgg = info.Major >= 13
	info.SupportsSqlJson = info.Major >= 16

	return info
}

// TestConnection 测试 PostgreSQL 连接
// ctx 用于取消控制（如信号取消）
func TestConnection(ctx context.Context, config *config.PostgreSQLConfig) error {
	// 测试连接时不使用压缩
	conn, err := NewConnection(ctx, config)
	if err != nil {
		return fmt.Errorf("PostgreSQL 连接测试失败：%w", err)
	}
	defer conn.Close()

	return nil
}

// TableExists 检查表是否存在
func (c *Connection) TableExists(tableName string) (bool, error) {
	ctx := c.context()
	query := `
		SELECT EXISTS (
			SELECT 1
			FROM information_schema.tables
			WHERE table_schema = current_schema()
			AND table_name = $1
		)
	`
	var exists bool
	err := c.pool.QueryRow(ctx, query, tableName).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("检查表是否存在失败：%w", err)
	}
	return exists, nil
}

// GrantTablePrivileges 授予表权限
func (c *Connection) GrantTablePrivileges(user, tableName string, privileges []string) error {
	ctx := c.context()

	// 构建权限字符串
	privilegesStr := strings.Join(privileges, ", ")

	// 构建授权语句
	query := fmt.Sprintf("GRANT %s ON TABLE \"%s\" TO %s", privilegesStr, tableName, user)

	_, err := c.pool.Exec(ctx, query)
	if err != nil {
		return fmt.Errorf("授予表权限失败：%w", err)
	}

	return nil
}

// GetTablePrivileges 获取表的权限信息
func (c *Connection) GetTablePrivileges(tableName string) ([]map[string]string, error) {
	ctx := c.context()

	query := `
		SELECT
			grantee::regrole::text AS "user_or_role",
			privilege_type,
			is_grantable
		FROM
			information_schema.role_table_grants
		WHERE
			table_schema = current_schema()
			AND table_name = $1
	`

	rows, err := c.pool.Query(ctx, query, tableName)
	if err != nil {
		return nil, fmt.Errorf("获取表权限失败：%w", err)
	}
	defer rows.Close()

	var privileges []map[string]string
	for rows.Next() {
		var user, privilege, isGrantable string
		if err := rows.Scan(&user, &privilege, &isGrantable); err != nil {
			return nil, fmt.Errorf("扫描表权限信息失败：%w", err)
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
	ctx := c.context()
	query := fmt.Sprintf("SELECT COUNT(*) FROM \"%s\"", tableName)

	var count int64
	err := c.pool.QueryRow(ctx, query).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("获取表 %s 行数失败：%w", tableName, err)
	}

	return count, nil
}

// SyncAutoIncrementSequence 回填自增列（SERIAL/BIGSERIAL）关联的序列
// 使下一次隐式插入获得正确的下一个值。
// nextValueLowerBound 为序列下一值的下限（MySQL 表级 AUTO_INCREMENT 起始值，未知时传 0），
// 实际设置的下一值为 GREATEST(表内当前最大值+1, nextValueLowerBound)。
// 返回 (false, nil) 表示目标列未关联序列（如既有表非 SERIAL 建表），调用方应告警并继续。
func (c *Connection) SyncAutoIncrementSequence(tableName, columnName string, nextValueLowerBound int64) (bool, error) {
	ctx := c.context()
	tbl := pgQuoteIdentifier(tableName)
	col := pgQuoteIdentifier(columnName)
	query := fmt.Sprintf(
		`SELECT setval(pg_get_serial_sequence('%s', '%s'), GREATEST(COALESCE((SELECT MAX(%s) FROM %s), 0) + 1, %d), false)`,
		strings.ReplaceAll(tbl, "'", "''"),
		strings.ReplaceAll(col, "'", "''"),
		col, tbl, nextValueLowerBound)

	var next *int64
	if err := c.pool.QueryRow(ctx, query).Scan(&next); err != nil {
		return false, fmt.Errorf("回填表 %s 列 %s 序列失败：%w", tableName, columnName, err)
	}
	return next != nil, nil
}

// pgQuoteIdentifier 用双引号包裹 PostgreSQL 标识符，并转义其中的双引号
func pgQuoteIdentifier(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}

func getColumnTypeByName(columnName string, columnTypes map[string]string) string {
	if len(columnTypes) == 0 {
		return ""
	}
	if t, ok := columnTypes[columnName]; ok {
		return t
	}
	for k, v := range columnTypes {
		if strings.EqualFold(k, columnName) {
			return v
		}
	}
	return ""
}

func isBinaryLikeType(columnType string) bool {
	t := strings.ToLower(columnType)
	return strings.Contains(t, "blob") || strings.Contains(t, "binary") || strings.Contains(t, "bytea")
}

func isGeometryLikeType(columnType string) bool {
	t := strings.ToLower(columnType)
	return strings.Contains(t, "point") || strings.Contains(t, "geometry")
}

// isBitType 判断是否为 MySQL BIT 位字段类型
func isBitType(columnType string) bool {
	return strings.Contains(strings.ToLower(columnType), "bit")
}

// parseMySQLBitValue 将 MySQL BIT 列的大端序二进制值解析为十进制数字符串
// BIT(n) 本质是无符号整数（0 ~ 2^n-1，n<=64），超过 int64 范围的值
// 以字符串形式透传给 NUMERIC(20,0) 目标列
func parseMySQLBitValue(data []byte) (string, bool) {
	if len(data) == 0 || len(data) > 8 {
		return "", false
	}
	var v uint64
	for _, b := range data {
		v = v<<8 | uint64(b)
	}
	return strconv.FormatUint(v, 10), true
}

// isTimeOnlyType 判断是否为 MySQL TIME 类型（排除 DATETIME/TIMESTAMP）
func isTimeOnlyType(columnType string) bool {
	t := strings.ToLower(columnType)
	return strings.Contains(t, "time") && !strings.Contains(t, "datetime") && !strings.Contains(t, "timestamp")
}

// isValidPGTime 判断 MySQL TIME 值是否在 PostgreSQL TIME 范围（00:00:00 ~ 24:00:00）内
// MySQL TIME 范围为 -838:59:59 ~ 838:59:59，负值和超过 24 小时的值在 PostgreSQL 中无法表示
func isValidPGTime(val string) bool {
	if val == "" || val[0] == '-' {
		return false
	}
	idx := strings.IndexByte(val, ':')
	if idx <= 0 {
		// 非标准格式交由 PostgreSQL 判断
		return true
	}
	hours, err := strconv.Atoi(val[:idx])
	if err != nil {
		return true
	}
	if hours > 24 {
		return false
	}
	// PostgreSQL TIME 最大值为 24:00:00，24 小时仅允许全零分秒
	if hours == 24 && strings.TrimRight(val[idx+1:], "0:.") != "" {
		return false
	}
	return true
}

func convertBatchColumnValue(columnName string, value interface{}, columnTypes map[string]string) interface{} {
	switch val := value.(type) {
	case []byte:
		columnType := getColumnTypeByName(columnName, columnTypes)
		if isGeometryLikeType(columnType) {
			if pointStr, err := parseMySQLPoint(val); err == nil {
				return pointStr
			}
		}
		if isBinaryLikeType(columnType) {
			return val
		}
		// BIT 位字段：大端序二进制值转十进制数（目标列为 BIGINT/NUMERIC(20,0)）
		if isBitType(columnType) {
			if s, ok := parseMySQLBitValue(val); ok {
				return s
			}
			return nil
		}
		// 零日期检测：使用 bytes.Equal 避免 string() 分配
		if bytes.Equal(val, zeroDateTimeBytes1) || bytes.Equal(val, zeroDateTimeBytes2) {
			return nil
		}
		// pgx.CopyFrom 原生支持 []byte → TEXT/VARCHAR，不需要转 string
		// pgx 的 Text format encoder 会将 []byte 直接作为 UTF-8 文本发送
		return val
	case string:
		// 零日期检测
		if val == "0000-00-00 00:00:00" || val == "0000-00-00" {
			return nil
		}
		// TIME 超范围检测：MySQL TIME 可为负值或超过 24 小时，超出 PostgreSQL
		// TIME 范围的值转 NULL（与零日期处理策略一致）
		if isTimeOnlyType(getColumnTypeByName(columnName, columnTypes)) && !isValidPGTime(val) {
			return nil
		}
		return val
	case time.Time:
		if val.IsZero() {
			return nil
		}
		return val
	case int64:
		return val
	case float64:
		return val
	case bool:
		return val
	default:
		return val
	}
}

// BatchInsertDataWithTransactionAndGetLastValue 在事务中批量插入数据并获取最后一个主键值
// 返回值：(总行数, 单主键最后值, 复合主键最后值列表, 错误)
func resolveCopyColumnsAndPrimaryKey(columns []string, primaryKey string, lowercaseColumns bool) ([]string, string) {
	copyColumns := make([]string, len(columns))
	for i, col := range columns {
		if lowercaseColumns {
			copyColumns[i] = strings.ToLower(col)
		} else {
			copyColumns[i] = col
		}
	}
	if primaryKey == "" {
		return copyColumns, ""
	}
	for i, col := range columns {
		if strings.EqualFold(col, primaryKey) {
			return copyColumns, copyColumns[i]
		}
	}
	// fallback: 如果没找到，使用转换后的主键名
	resolvedPrimaryKey := primaryKey
	if lowercaseColumns {
		resolvedPrimaryKey = strings.ToLower(primaryKey)
	}
	return copyColumns, resolvedPrimaryKey
}

// BatchInsertDataWithTransactionAndGetLastValue 批量插入数据并返回最后处理的主键值
// ctx 用于取消控制：数据同步热路径传入脱离根取消信号的批次 context，
// 保证取消时进行中的批次能完整执行完毕
func (c *Connection) BatchInsertDataWithTransactionAndGetLastValue(ctx context.Context, tx pgx.Tx, tableName string, columns []string, columnTypes map[string]string, batchSize int, primaryKey string, lowercaseColumns bool, rows *sql.Rows) (int, interface{}, error) {
	totalRows, lastValue, compositeLastValues, err := c.BatchInsertDataWithCompositeKeys(ctx, tx, tableName, columns, columnTypes, batchSize, []string{primaryKey}, lowercaseColumns, rows)
	if err != nil {
		return 0, nil, err
	}
	// 返回复合主键的第一个值（向后兼容）
	if len(compositeLastValues) > 0 {
		return totalRows, compositeLastValues[0], nil
	}
	return totalRows, lastValue, nil
}

// BatchInsertDataWithCompositeKeys 批量插入数据并支持复合主键
// 返回值：(总行数, 单主键最后值, 复合主键最后值列表, 错误)
// ctx 用于取消控制，语义同 BatchInsertDataWithTransactionAndGetLastValue
func (c *Connection) BatchInsertDataWithCompositeKeys(ctx context.Context, tx pgx.Tx, tableName string, columns []string, columnTypes map[string]string, batchSize int, primaryKeys []string, lowercaseColumns bool, rows *sql.Rows) (int, interface{}, []interface{}, error) {

	// 准备批量插入
	var rowCount int
	var totalRows int

	// 严格使用传入的 batchSize 参数，不使用硬编码默认值
	effectiveBatchSize := batchSize
	if effectiveBatchSize <= 0 {
		effectiveBatchSize = 10000 // 确保至少有一个合理的默认值
	}

	// 处理主键列名（支持单个或多个主键）
	copyColumns := make([]string, len(columns))
	resolvedPrimaryKeys := make([]string, 0, len(primaryKeys))

	for i, col := range columns {
		if lowercaseColumns {
			copyColumns[i] = strings.ToLower(col)
		} else {
			copyColumns[i] = col
		}
	}

	// 解析所有主键列名
	for _, primaryKey := range primaryKeys {
		if primaryKey == "" {
			continue
		}
		for i, col := range columns {
			if strings.EqualFold(col, primaryKey) {
				resolvedPrimaryKeys = append(resolvedPrimaryKeys, copyColumns[i])
				break
			}
		}
		// fallback: 如果没找到，使用转换后的主键名
		if len(resolvedPrimaryKeys) < len(primaryKeys) {
			resolvedPK := primaryKey
			if lowercaseColumns {
				resolvedPK = strings.ToLower(primaryKey)
			}
			resolvedPrimaryKeys = append(resolvedPrimaryKeys, resolvedPK)
		}
	}

	// 找到所有主键列的索引
	primaryKeyIndexes := make([]int, 0, len(resolvedPrimaryKeys))
	for _, resolvedPK := range resolvedPrimaryKeys {
		for i, col := range copyColumns {
			if col == resolvedPK {
				primaryKeyIndexes = append(primaryKeyIndexes, i)
				break
			}
		}
	}

	// 使用类型化 Scan 目标，避免 *interface{} 导致的每次 Scan 都分配具体对象
	typedDests := makeTypedDestinations(columns, columnTypes)
	scanPtrs := scanDestinations(typedDests)
	numCols := len(typedDests)

	// copyRows 存储行数据指针，每个元素从 sync.Pool 复用
	copyRows := make([][]interface{}, 0, effectiveBatchSize)

	// 跟踪最后一个主键值（支持复合主键）
	var lastValue interface{}
	var compositeLastValues []interface{}

	// 处理数据行
	for rows.Next() {
		// 扫描行数据 — 使用类型化指针，避免每次 Scan 分配堆对象
		if err := rows.Scan(scanPtrs...); err != nil {
			return 0, nil, nil, fmt.Errorf("扫描行数据失败：%w", err)
		}

		// 跟踪最后一个主键值（支持复合主键）
		if len(primaryKeyIndexes) > 0 {
			compositeLastValues = make([]interface{}, len(primaryKeyIndexes))
			for i, idx := range primaryKeyIndexes {
				compositeLastValues[i] = getTypedValue(&typedDests[idx])
			}
			// 向后兼容：单主键场景
			if len(primaryKeyIndexes) == 1 {
				lastValue = compositeLastValues[0]
			}
		}

		// 从多级池获取 rowValues 切片，避免每行 make 新切片
		rowValues := getRowSlice(numCols)
		for i := range typedDests {
			rowValues[i] = convertBatchColumnValue(columns[i], getTypedValue(&typedDests[i]), columnTypes)
		}
		copyRows = append(copyRows, rowValues)

		rowCount++
		totalRows++

		// 当达到批量大小时执行 CopyFrom
		if rowCount == effectiveBatchSize {
			// 执行 CopyFrom，使用转换后的小写列名
			_, err := tx.CopyFrom(ctx, pgx.Identifier{tableName}, copyColumns, pgx.CopyFromRows(copyRows))
			if err != nil {
				return 0, nil, nil, fmt.Errorf("CopyFrom 执行失败：%w", err)
			}

			// 将 rowValues 切片返回多级池复用
			for _, rv := range copyRows {
				putRowSlice(rv)
			}

			// 重置切片和计数器
			copyRows = copyRows[:0]
			rowCount = 0
		}
	}

	// 执行剩余的数据
	if len(copyRows) > 0 {
		// 执行 CopyFrom，使用转换后的小写列名
		_, err := tx.CopyFrom(ctx, pgx.Identifier{tableName}, copyColumns, pgx.CopyFromRows(copyRows))
		if err != nil {
			return 0, nil, nil, fmt.Errorf("CopyFrom 执行失败：%w", err)
		}
		// 将 rowValues 切片返回多级池复用
		for _, rv := range copyRows {
			putRowSlice(rv)
		}
	}

	if err := rows.Err(); err != nil {
		return 0, nil, nil, err
	}

	// 只有在没有找到主键值的情况下，才执行 MAX 查询（作为后备方案）
	if len(resolvedPrimaryKeys) > 0 && lastValue == nil {
		// 对于复合主键，只查询第一个主键列
		query := fmt.Sprintf("SELECT MAX(\"%s\") FROM \"%s\"", resolvedPrimaryKeys[0], tableName)
		err := tx.QueryRow(ctx, query).Scan(&lastValue)
		if err != nil && err != pgx.ErrNoRows {
			return 0, nil, nil, fmt.Errorf("获取最后一个主键值失败：%w", err)
		}
	}

	return totalRows, lastValue, compositeLastValues, nil
}

// parseMySQLPoint 解析 MySQL 的 WKB 格式 Point 数据
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

	// 格式化为 PostgreSQL Point 格式 (x,y)
	return fmt.Sprintf("(%v,%v)", x, y), nil
}

// GetCharset 获取 PostgreSQL 的字符集
func (c *Connection) GetCharset() (string, error) {
	query := `
		SELECT pg_encoding_to_char(encoding)
		FROM pg_database
		WHERE datname = $1
	`
	var charset string
	err := c.pool.QueryRow(c.context(), query, c.config.Database).Scan(&charset)
	if err != nil {
		return "", fmt.Errorf("获取 PostgreSQL 字符集失败：%w", err)
	}
	return charset, nil
}
