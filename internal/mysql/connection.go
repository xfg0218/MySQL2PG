package mysql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	gmysql "github.com/go-sql-driver/mysql"
	"github.com/yourusername/mysql2pg/internal/config"
)

// utcConnector 包装 MySQL 驱动的 connector，在每个新建的连接上固定会话时区
// MySQL TIMESTAMP 内部按 UTC 存储、存取时经会话时区与 UTC 互转；将会话时区固定为 UTC
// 后，读取到的 TIMESTAMP 值恒为 UTC 墙钟时间，配合 timestamp -> TIMESTAMPTZ 映射，
// 保证迁移不丢失时区语义（不固定时读取值会随源库会话时区漂移）
type utcConnector struct {
	base driver.Connector
}

func (c *utcConnector) Connect(ctx context.Context) (driver.Conn, error) {
	conn, err := c.base.Connect(ctx)
	if err != nil {
		return nil, err
	}
	if execer, ok := conn.(driver.ExecerContext); ok {
		if _, err := execer.ExecContext(ctx, "SET time_zone = '+00:00'", nil); err != nil {
			conn.Close()
			return nil, fmt.Errorf("设置MySQL会话时区失败: %w", err)
		}
	}
	return conn, nil
}

func (c *utcConnector) Driver() driver.Driver {
	return c.base.Driver()
}

// buildMySQLDriverConfig 解析 DSN 并固化两个关键的驱动行为：
//   - Loc 固定 UTC：与会话时区固定 UTC 保持一致，保证 parseTime 得到的时间值
//     是正确的 UTC 墙钟时间（driver 默认即 UTC，此处显式覆盖，防止被用户连接参数中的 loc 覆盖）
//   - ParseTime 强制开启：go-sql-driver 对重复 DSN 参数取最后一次出现的值，
//     用户 connection_params 中的 parseTime=false 会覆盖内置设置，导致时间列以字符串读回、
//     无法被 pgx.CopyFrom 二进制协议编码（timestamp/timestamptz）；
//     时间值管道（sql.NullTime → time.Time → CopyFrom）依赖该设置
func buildMySQLDriverConfig(dsn string) (*gmysql.Config, error) {
	driverCfg, err := gmysql.ParseDSN(dsn)
	if err != nil {
		return nil, fmt.Errorf("解析MySQL连接串失败: %w", err)
	}
	driverCfg.Loc = time.UTC
	driverCfg.ParseTime = true
	return driverCfg, nil
}

// MySQLVersionInfo MySQL 版本信息
type MySQLVersionInfo struct {
	Major int
	Minor int
	Patch int
	Full  string
	Is57  bool // MySQL 5.7
	Is80  bool // MySQL 8.0
	Is84  bool // MySQL 8.4 LTS
	Is90  bool // MySQL 9.0+
}

// IsVersionGreaterOrEqual 检查当前版本是否大于等于指定版本
func (m *MySQLVersionInfo) IsVersionGreaterOrEqual(major, minor int) bool {
	if m.Major > major {
		return true
	}
	if m.Major == major {
		return m.Minor >= minor
	}
	return false
}

// SupportsRegexpInstrFull 是否支持完整的 REGEXP_INSTR 函数（6 参数版本）
func (m *MySQLVersionInfo) SupportsRegexpInstrFull() bool {
	return m.Is90 || (m.Is80 && m.Minor >= 17)
}

// SupportsJsonArrayInsert 是否支持 JSON_ARRAY_INSERT 函数
func (m *MySQLVersionInfo) SupportsJsonArrayInsert() bool {
	return m.Is90
}

// SupportsJsonTable 是否支持 JSON_TABLE 函数
func (m *MySQLVersionInfo) SupportsJsonTable() bool {
	return m.Major >= 8
}

// Connection MySQL连接管理器
type Connection struct {
	db     *sql.DB
	config *config.MySQLConfig
	ctx    context.Context

	// P1-07：一致性快照事务（consistent_snapshot 配置开启时使用），
	// 开启后数据读取查询均通过该事务执行，保证源库并发写入时读到一致快照
	snapshotMu sync.Mutex
	snapshotTx *sql.Tx
}

// BeginConsistentSnapshot 开启一致性快照事务（P1-07）
// 要求 MySQL 事务隔离级别为 REPEATABLE READ（InnoDB 默认）；
// 开启后数据读取类查询（QueryTableRows/GetTableData*/GetTableRowCount）均通过该事务执行
func (c *Connection) BeginConsistentSnapshot(ctx context.Context) error {
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("开始快照事务失败: %w", err)
	}
	if _, err := tx.ExecContext(ctx, "START TRANSACTION WITH CONSISTENT SNAPSHOT"); err != nil {
		tx.Rollback()
		return fmt.Errorf("开启一致性快照失败: %w", err)
	}
	c.snapshotMu.Lock()
	c.snapshotTx = tx
	c.snapshotMu.Unlock()
	return nil
}

// EndConsistentSnapshot 结束一致性快照事务（只读事务，直接回滚释放连接）
func (c *Connection) EndConsistentSnapshot() {
	c.snapshotMu.Lock()
	tx := c.snapshotTx
	c.snapshotTx = nil
	c.snapshotMu.Unlock()
	if tx != nil {
		tx.Rollback()
	}
}

// querier 返回当前查询执行器：优先快照事务（P1-07），否则连接池
func (c *Connection) querier() interface {
	QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row
} {
	c.snapshotMu.Lock()
	defer c.snapshotMu.Unlock()
	if c.snapshotTx != nil {
		return c.snapshotTx
	}
	return c.db
}

// context 返回连接持有的根 context
// 未通过 NewConnection 构造时回退为 context.Background()，保证 nil 安全
func (c *Connection) context() context.Context {
	if c.ctx == nil {
		return context.Background()
	}
	return c.ctx
}

// NewConnection 创建新的MySQL连接
// ctx 为根 context（通常来自 signal.NotifyContext），取消后所有进行中的查询会被中断
func NewConnection(ctx context.Context, config *config.MySQLConfig) (*Connection, error) {
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

	driverCfg, err := buildMySQLDriverConfig(dsn)
	if err != nil {
		return nil, err
	}

	connector, err := gmysql.NewConnector(driverCfg)
	if err != nil {
		return nil, fmt.Errorf("创建MySQL连接器失败: %w", err)
	}
	db := sql.OpenDB(&utcConnector{base: connector})

	// 优化连接池配置
	db.SetMaxOpenConns(config.MaxOpenConns)                                    // 最大打开连接数
	db.SetMaxIdleConns(config.MaxIdleConns)                                    // 最大空闲连接数
	db.SetConnMaxLifetime(time.Duration(config.ConnMaxLifetime) * time.Second) // 连接最大生命周期

	// 测试连接
	if err := db.PingContext(ctx); err != nil {
		return nil, fmt.Errorf("MySQL连接测试失败: %w", err)
	}

	return &Connection{
		db:     db,
		config: config,
		ctx:    ctx,
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
	rows, err := c.db.QueryContext(c.context(), fmt.Sprintf("SHOW COLUMNS FROM `%s`", tableName))
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
	rows, err := c.db.QueryContext(c.context(), fmt.Sprintf("SHOW COLUMNS FROM `%s`", tableName))
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
// ctx 用于取消控制：数据同步热路径传入脱离根取消信号的批次 context，
// 保证取消时进行中的批次能完整执行完毕
func (c *Connection) GetTableData(ctx context.Context, tableName string, columns []string, offset, limit int, orderBy string) (*sql.Rows, error) {
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

	rows, err := c.querier().QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("获取表数据失败: %w", err)
	}

	return rows, nil
}

// QueryTableRows 流式读取表全部数据（P1-06：无主键表的流式读取入口，
// 单次查询 + 迭代分批读取，替代 O(n²) 的 OFFSET 分页）
// 注意：go-sql-driver 会在客户端缓冲整个结果集，仅适用于中小表（调用方按行数阈值控制）
func (c *Connection) QueryTableRows(ctx context.Context, tableName string, columns []string) (*sql.Rows, error) {
	var quotedColumns []string
	for _, col := range columns {
		quotedColumns = append(quotedColumns, fmt.Sprintf("`%s`", col))
	}
	query := fmt.Sprintf("SELECT %s FROM `%s`", strings.Join(quotedColumns, ", "), tableName)

	rows, err := c.querier().QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("流式读取表数据失败: %w", err)
	}
	return rows, nil
}

// GetTableDataWithPagination 使用基于主键的分页获取表数据
// ctx 用于取消控制，语义同 GetTableData
func (c *Connection) GetTableDataWithPagination(ctx context.Context, tableName string, columns []string, primaryKey string, lastValue interface{}, limit int) (*sql.Rows, error) {
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

	rows, err := c.querier().QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("获取表数据失败: %w", err)
	}

	return rows, nil
}

// GetTableDataWithCompositeKeyPagination 使用复合主键分页获取表数据
// 性能优化：使用 WHERE (k1,k2,k3) > (?,?,?) 替代 OFFSET，避免大偏移量时的性能下降
// MySQL 8.0+ 支持行构造函数比较
// ctx 用于取消控制，语义同 GetTableData
func (c *Connection) GetTableDataWithCompositeKeyPagination(ctx context.Context, tableName string, columns []string, primaryKeys []string, lastValues []interface{}, limit int) (*sql.Rows, error) {
	// 使用反引号包围表名、列名和主键
	var quotedColumns []string
	for _, col := range columns {
		quotedColumns = append(quotedColumns, fmt.Sprintf("`%s`", col))
	}
	columnsStr := strings.Join(quotedColumns, ", ")

	var quotedPrimaryKeys []string
	for _, pk := range primaryKeys {
		quotedPrimaryKeys = append(quotedPrimaryKeys, fmt.Sprintf("`%s`", pk))
	}
	primaryKeyStr := strings.Join(quotedPrimaryKeys, ", ")

	var query string
	var args []interface{}

	if lastValues != nil && len(lastValues) > 0 {
		// 使用行构造函数进行复合主键比较：WHERE (k1,k2) > (?,?)
		placeholderStr := strings.Repeat("?, ", len(primaryKeys)-1) + "?"
		query = fmt.Sprintf("SELECT %s FROM `%s` WHERE (%s) > (%s) ORDER BY %s LIMIT %d",
			columnsStr, tableName, primaryKeyStr, placeholderStr, primaryKeyStr, limit)
		args = lastValues
	} else {
		// 第一批数据，不需要 WHERE 条件
		query = fmt.Sprintf("SELECT %s FROM `%s` ORDER BY %s LIMIT %d",
			columnsStr, tableName, primaryKeyStr, limit)
	}

	rows, err := c.querier().QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("获取表数据失败：%w", err)
	}

	return rows, nil
}

// GetTablePrimaryKeys 获取表的主键列名列表
func (c *Connection) GetTablePrimaryKeys(tableName string) ([]string, error) {
	// 使用SHOW KEYS FROM语句获取主键信息，避免查询information_schema导致的权限问题
	// 这样可以同时兼容MySQL 5.7和MySQL 8.0
	query := fmt.Sprintf("SHOW KEYS FROM `%s` WHERE Key_name = 'PRIMARY'", tableName)

	rows, err := c.db.QueryContext(c.context(), query)
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

// GetTableRowCount 获取表的行数（开启一致性快照时读取快照视图，P1-07）
func (c *Connection) GetTableRowCount(tableName string) (int64, error) {
	var count int64
	err := c.querier().QueryRowContext(c.context(), fmt.Sprintf("SELECT COUNT(*) FROM `%s`", tableName)).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("获取表行数失败: %w", err)
	}

	return count, nil
}

// GetVersion 获取MySQL版本信息
func (c *Connection) GetVersion() (string, error) {
	var version string
	err := c.db.QueryRowContext(c.context(), "SELECT VERSION()").Scan(&version)
	if err != nil {
		return "", fmt.Errorf("获取MySQL版本失败: %w", err)
	}
	return version, nil
}

// GetVersionInfo 获取 MySQL 详细版本信息
func (c *Connection) GetVersionInfo() (*MySQLVersionInfo, error) {
	version, err := c.GetVersion()
	if err != nil {
		return nil, err
	}
	return ParseMySQLVersion(version), nil
}

// ParseMySQLVersion 解析 MySQL 版本字符串
func ParseMySQLVersion(version string) *MySQLVersionInfo {
	info := &MySQLVersionInfo{
		Full: version,
	}

	// 移除可能的后缀，如 "-MySQL", "-log", 等
	cleanVersion := version
	if idx := strings.Index(version, "-"); idx != -1 {
		cleanVersion = version[:idx]
	}

	// 解析版本号 "major.minor.patch"
	parts := strings.Split(cleanVersion, ".")
	if len(parts) >= 2 {
		major, err := strconv.Atoi(parts[0])
		if err == nil {
			info.Major = major
		}
		minor, err := strconv.Atoi(parts[1])
		if err == nil {
			info.Minor = minor
		}
		if len(parts) >= 3 {
			patchStr := parts[2]
			re := regexp.MustCompile(`^\d+`)
			if match := re.FindString(patchStr); match != "" {
				patch, err := strconv.Atoi(match)
				if err == nil {
					info.Patch = patch
				}
			}
		}
	}

	// 设置版本标志
	info.Is57 = info.Major == 5 && info.Minor == 7
	info.Is80 = info.Major == 8 && info.Minor == 0
	info.Is84 = info.Major == 8 && info.Minor == 4
	info.Is90 = info.Major >= 9

	return info
}

// TestConnection 测试MySQL连接
// ctx 用于取消控制（如信号取消）
func TestConnection(ctx context.Context, config *config.MySQLConfig) error {
	// 测试连接时不使用压缩
	conn, err := NewConnection(ctx, config)
	if err != nil {
		return fmt.Errorf("MySQL连接测试失败: %w", err)
	}
	defer conn.Close()

	return nil
}

// GetCharsetAndCollation 获取数据库的字符集和排序规则
func (c *Connection) GetCharsetAndCollation() (string, string, error) {
	var charset, collation string

	// 获取数据库字符集
	query := `
		SELECT default_character_set_name, default_collation_name
		FROM information_schema.SCHEMATA
		WHERE schema_name = ?
	`
	err := c.db.QueryRowContext(c.context(), query, c.config.Database).Scan(&charset, &collation)
	if err != nil {
		// 如果查询失败，尝试使用 SHOW VARIABLES
		charset, collation, err = c.getCharsetFromVariables()
		if err != nil {
			return "", "", fmt.Errorf("获取字符集失败：%w", err)
		}
	}

	return charset, collation, nil
}

// getCharsetFromVariables 从系统变量获取字符集
func (c *Connection) getCharsetFromVariables() (string, string, error) {
	var charset, collation string

	if err := c.db.QueryRowContext(c.context(), "SHOW VARIABLES LIKE 'character_set_database'").Scan(&charset, &charset); err != nil {
		return "", "", err
	}

	if err := c.db.QueryRowContext(c.context(), "SHOW VARIABLES LIKE 'collation_database'").Scan(&collation, &collation); err != nil {
		return "", "", err
	}

	return charset, collation, nil
}

// GetTableDDL 获取表的 DDL（导出方法）
func (c *Connection) GetTableDDL(ctx context.Context, tableName string) (string, error) {
	return c.getTableDDL(ctx, tableName)
}

// GetTableIndexes 获取表的索引信息（导出方法）
func (c *Connection) GetTableIndexes(tableName string) ([]IndexInfo, error) {
	return c.getTableIndexes(tableName)
}
