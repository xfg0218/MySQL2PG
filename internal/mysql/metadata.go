package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"regexp"
	"strings"
	"sync"
	"time"
)

// TableInfo 表信息
type TableInfo struct {
	Name    string
	DDL     string
	Columns []ColumnInfo
	Indexes []IndexInfo
}

// ColumnInfo 列信息
type ColumnInfo struct {
	Name     string
	Type     string
	Nullable string
	Default  *string
	Comment  string
}

// IndexInfo 索引信息
type IndexInfo struct {
	Name     string
	Table    string
	Columns  []string
	IsUnique bool
}

// FunctionInfo 函数信息
type FunctionInfo struct {
	Name       string
	DDL        string
	Parameters string
	ReturnType string
}

// UserInfo 用户信息
// PasswordHash/AuthPlugin 仅用于感知源库用户的密码状态：
// MySQL 与 PostgreSQL 的密码哈希格式不兼容（SHA1 体系 vs SCRAM/MD5），
// 密码本身不可迁移，迁移后需人工重设（issue-10）
type UserInfo struct {
	Name         string
	Grants       []string
	PasswordHash string
	AuthPlugin   string
}

// ViewInfo 视图信息
type ViewInfo struct {
	ViewName       string
	ViewDefinition string
}

// GetTables 获取所有表信息
func (c *Connection) GetTables(skipUseTableList bool, skipTableList []string, useTableList bool, tableList []string) ([]TableInfo, error) {
	// 获取当前连接的用户名，以便更好地诊断权限问题
	var currentUser string
	if err := c.db.QueryRowContext(c.context(), "SELECT USER()").Scan(&currentUser); err != nil {
		return nil, fmt.Errorf("获取当前用户名失败: %w", err)
	}

	// 使用多种方法尝试获取表列表，以兼容不同的MySQL版本和权限配置
	var rows *sql.Rows
	var err error

	// 使用INFORMATION_SCHEMA.TABLES查询，只获取TABLE类型的对象，过滤掉视图
	query := `
		SELECT table_name
		FROM INFORMATION_SCHEMA.TABLES
		WHERE table_schema = ?
		  AND table_type = 'BASE TABLE'
		  AND table_schema NOT IN ('mysql', 'information_schema', 'performance_schema', 'sys')
	`
	rows, err = c.db.QueryContext(c.context(), query, c.config.Database)

	if err != nil {
		// 如果失败，返回包含当前用户名的详细错误信息
		return nil, fmt.Errorf("获取表列表失败: %w。当前用户: %s，数据库: %s", err, currentUser, c.config.Database)
	}
	defer rows.Close()

	var tableNames []string
	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			return nil, fmt.Errorf("扫描表名失败: %w", err)
		}
		tableNames = append(tableNames, tableName)
	}

	// 在应用层面过滤只同步的表
	if useTableList && len(tableList) > 0 {
		// 创建一个map用于快速查找需要同步的表
		useMap := make(map[string]bool)
		for _, table := range tableList {
			useMap[table] = true
		}

		// 过滤表名列表
		filteredTableNames := make([]string, 0, len(tableNames))
		for _, tableName := range tableNames {
			if useMap[tableName] {
				filteredTableNames = append(filteredTableNames, tableName)
			}
		}
		tableNames = filteredTableNames
	}

	// 在应用层面过滤掉需要跳过的表
	if skipUseTableList && len(skipTableList) > 0 {
		// 创建一个map用于快速查找需要跳过的表
		skipMap := make(map[string]bool)
		for _, table := range skipTableList {
			skipMap[table] = true
		}

		// 过滤表名列表
		filteredTableNames := make([]string, 0, len(tableNames))
		for _, tableName := range tableNames {
			if !skipMap[tableName] {
				filteredTableNames = append(filteredTableNames, tableName)
			}
		}
		tableNames = filteredTableNames
	}

	// 使用配置中的 max_open_conns 作为并发数，避免连接池竞争
	maxConcurrent := c.config.MaxOpenConns
	if maxConcurrent <= 0 {
		maxConcurrent = 10 // 默认值
	}

	// ========== 阶段 1: 并发获取所有表的 DDL ==========
	ddlMap := make(map[string]string)
	ddlErrorMap := make(map[string]error)

	type ddlResult struct {
		tableName string
		ddl       string
		err       error
	}

	ddlChan := make(chan ddlResult, len(tableNames))
	var wg sync.WaitGroup
	semaphore := make(chan struct{}, maxConcurrent)

	for _, tableName := range tableNames {
		// 取消检查：根 context 取消后不再派发新的 DDL 获取任务
		if err := c.context().Err(); err != nil {
			break
		}

		wg.Add(1)
		go func(name string) {
			defer wg.Done()

			semaphore <- struct{}{}
			defer func() { <-semaphore }()

			ctx, cancel := context.WithTimeout(c.context(), 120*time.Second)
			defer cancel()

			ddl, err := c.getTableDDL(ctx, name)
			ddlChan <- ddlResult{name, ddl, err}
		}(tableName)
	}

	go func() {
		wg.Wait()
		close(ddlChan)
	}()

	for result := range ddlChan {
		if result.err != nil {
			ddlErrorMap[result.tableName] = result.err
		} else {
			ddlMap[result.tableName] = result.ddl
		}
	}

	if len(ddlErrorMap) > 0 {
		for tableName, err := range ddlErrorMap {
			return nil, fmt.Errorf("获取表 %s 的 DDL 失败：%w", tableName, err)
		}
	}

	// ========== 阶段 2: 并发获取所有表的列信息 ==========
	columnsMap := make(map[string][]ColumnInfo)
	columnsErrorMap := make(map[string]error)

	type columnsResult struct {
		tableName string
		columns   []ColumnInfo
		err       error
	}

	columnsChan := make(chan columnsResult, len(tableNames))
	var wg2 sync.WaitGroup
	semaphore2 := make(chan struct{}, maxConcurrent)

	for _, tableName := range tableNames {
		wg2.Add(1)
		go func(name string) {
			defer wg2.Done()

			semaphore2 <- struct{}{}
			defer func() { <-semaphore2 }()

			columns, err := c.getTableColumns(name)
			columnsChan <- columnsResult{name, columns, err}
		}(tableName)
	}

	go func() {
		wg2.Wait()
		close(columnsChan)
	}()

	for result := range columnsChan {
		if result.err != nil {
			columnsErrorMap[result.tableName] = result.err
		} else {
			columnsMap[result.tableName] = result.columns
		}
	}

	if len(columnsErrorMap) > 0 {
		for tableName, err := range columnsErrorMap {
			return nil, fmt.Errorf("获取表 %s 的列信息失败：%w", tableName, err)
		}
	}

	// ========== 阶段 3: 并发获取所有表的索引信息 ==========
	indexesMap := make(map[string][]IndexInfo)
	indexesErrorMap := make(map[string]error)

	type indexesResult struct {
		tableName string
		indexes   []IndexInfo
		err       error
	}

	indexesChan := make(chan indexesResult, len(tableNames))
	var wg3 sync.WaitGroup
	semaphore3 := make(chan struct{}, maxConcurrent)

	for _, tableName := range tableNames {
		wg3.Add(1)
		go func(name string) {
			defer wg3.Done()

			semaphore3 <- struct{}{}
			defer func() { <-semaphore3 }()

			indexes, err := c.getTableIndexes(name)
			indexesChan <- indexesResult{name, indexes, err}
		}(tableName)
	}

	go func() {
		wg3.Wait()
		close(indexesChan)
	}()

	for result := range indexesChan {
		if result.err != nil {
			indexesErrorMap[result.tableName] = result.err
		} else {
			indexesMap[result.tableName] = result.indexes
		}
	}

	if len(indexesErrorMap) > 0 {
		for tableName, err := range indexesErrorMap {
			return nil, fmt.Errorf("获取表 %s 的索引信息失败：%w", tableName, err)
		}
	}

	// ========== 合并三个阶段的结果 ==========
	var tables []TableInfo
	for _, tableName := range tableNames {
		tables = append(tables, TableInfo{
			Name:    tableName,
			DDL:     ddlMap[tableName],
			Columns: columnsMap[tableName],
			Indexes: indexesMap[tableName],
		})
	}

	return tables, nil
}

// getTableDDL 获取单个表的 DDL
func (c *Connection) getTableDDL(ctx context.Context, tableName string) (string, error) {
	var ddl string
	query := fmt.Sprintf("SHOW CREATE TABLE `%s`", tableName)

	rows, err := c.db.QueryContext(ctx, query)
	if err != nil {
		// 检查错误是否是因为权限不足导致的 SHOW VIEW 命令被拒绝
		if strings.Contains(err.Error(), "SHOW VIEW command denied") || strings.Contains(err.Error(), "1142") {
			return "", fmt.Errorf("权限不足，无法查看表 %s 的 DDL: %w", tableName, err)
		}
		return "", fmt.Errorf("获取表 DDL 失败：%w", err)
	}
	defer rows.Close()

	// 获取列信息
	columns, err := rows.Columns()
	if err != nil {
		return "", fmt.Errorf("获取结果列信息失败：%w", err)
	}

	// 检查是否有行数据
	if !rows.Next() {
		return "", fmt.Errorf("SHOW CREATE TABLE 没有返回结果")
	}

	// 创建足够的字符串指针来存储结果
	vals := make([]interface{}, len(columns))
	valPtrs := make([]*string, len(columns))
	for i := range vals {
		valPtrs[i] = new(string)
		vals[i] = valPtrs[i]
	}

	// 扫描结果
	if err := rows.Scan(vals...); err != nil {
		return "", fmt.Errorf("扫描表 DDL 结果失败：%w", err)
	}

	// 提取 DDL 信息（通常在第 2 个字段，索引 1）
	ddlFound := false
	if len(valPtrs) > 1 && *valPtrs[1] != "" {
		ddl = *valPtrs[1]
		ddlFound = true
	} else if len(valPtrs) > 3 && *valPtrs[3] != "" {
		// 处理某些情况下 DDL 可能在第 4 个字段的情况
		ddl = *valPtrs[3]
		ddlFound = true
	}

	if !ddlFound {
		return "", fmt.Errorf("无法从 SHOW CREATE TABLE 结果中提取 DDL")
	}

	return ddl, nil
}

// getTableColumns 获取表的列信息
func (c *Connection) getTableColumns(tableName string) ([]ColumnInfo, error) {
	// 使用反引号包围表名，以处理包含特殊字符的表名
	query := fmt.Sprintf("SHOW FULL COLUMNS FROM `%s`", tableName)
	rows, err := c.db.QueryContext(c.context(), query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var columns []ColumnInfo
	for rows.Next() {
		var col ColumnInfo
		var field, colType, null, key, extra, comment string
		var defaultValue sql.NullString
		var collation, privileges sql.NullString

		if err := rows.Scan(&field, &colType, &collation, &null, &key, &defaultValue, &extra, &privileges, &comment); err != nil {
			return nil, err
		}

		col.Name = field
		col.Type = colType
		col.Nullable = null
		col.Comment = comment

		if defaultValue.Valid {
			col.Default = &defaultValue.String
		}

		columns = append(columns, col)
	}

	return columns, nil
}

// getTableIndexes 获取表的索引信息
func (c *Connection) getTableIndexes(tableName string) ([]IndexInfo, error) {
	// 使用information_schema.statistics查询索引信息，兼容MySQL 5.7和MySQL 8.0
	// 只查询需要的字段：table_name, index_name, non_unique, column_name, seq_in_index
	query := `
		SELECT table_name, index_name, non_unique, column_name, seq_in_index 
		FROM information_schema.statistics 
		WHERE table_schema = ? AND table_name = ? 
		ORDER BY index_name, seq_in_index
	`
	rows, err := c.db.QueryContext(c.context(), query, c.config.Database, tableName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	// 使用map来按索引名分组
	indexMap := make(map[string]*IndexInfo)

	for rows.Next() {
		var tableName, indexName, columnName string
		var nonUnique int
		var seqInIndex sql.NullString

		if err := rows.Scan(&tableName, &indexName, &nonUnique, &columnName, &seqInIndex); err != nil {
			return nil, err
		}

		if _, exists := indexMap[indexName]; !exists {
			indexMap[indexName] = &IndexInfo{
				Name:     indexName,
				Table:    tableName,
				IsUnique: nonUnique == 0,
			}
		}

		indexMap[indexName].Columns = append(indexMap[indexName].Columns, columnName)
	}

	// 将map转换为slice
	var indexes []IndexInfo
	for _, idx := range indexMap {
		indexes = append(indexes, *idx)
	}

	return indexes, nil
}

// GetAllIndexes 获取所有表的索引信息
func (c *Connection) GetAllIndexes() ([]IndexInfo, error) {
	// 使用 information_schema.statistics 查询所有索引信息
	query := `
		SELECT table_name, index_name, non_unique, column_name, seq_in_index
		FROM information_schema.statistics
		WHERE table_schema = ?
		  AND table_schema NOT IN ('mysql', 'information_schema', 'performance_schema', 'sys')
		ORDER BY table_name, index_name, seq_in_index
	`
	rows, err := c.db.QueryContext(c.context(), query, c.config.Database)
	if err != nil {
		return nil, fmt.Errorf("查询索引信息失败：%w", err)
	}
	defer rows.Close()

	// 使用 map 来按索引名分组
	indexMap := make(map[string]*IndexInfo)

	for rows.Next() {
		var tableName, indexName, columnName string
		var nonUnique int
		var seqInIndex sql.NullString

		if err := rows.Scan(&tableName, &indexName, &nonUnique, &columnName, &seqInIndex); err != nil {
			return nil, fmt.Errorf("扫描索引信息失败：%w", err)
		}

		key := tableName + "." + indexName
		if _, exists := indexMap[key]; !exists {
			indexMap[key] = &IndexInfo{
				Name:     indexName,
				Table:    tableName,
				IsUnique: nonUnique == 0,
			}
		}

		indexMap[key].Columns = append(indexMap[key].Columns, columnName)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("遍历索引结果失败：%w", err)
	}

	// 将 map 转换为 slice
	var indexes []IndexInfo
	for _, idx := range indexMap {
		indexes = append(indexes, *idx)
	}

	return indexes, nil
}

// GetViews 获取所有视图信息
func (c *Connection) GetViews(database string) ([]ViewInfo, error) {
	// 查询视图定义，过滤掉系统数据库的视图
	query := `
		SELECT table_name, view_definition
		FROM INFORMATION_SCHEMA.VIEWS
		WHERE table_schema = ?
		  AND table_schema NOT IN ('mysql', 'information_schema', 'performance_schema', 'sys')
	`
	rows, err := c.db.QueryContext(c.context(), query, database)
	if err != nil {
		return nil, fmt.Errorf("查询视图定义失败: %w", err)
	}
	defer rows.Close()

	var views []ViewInfo
	for rows.Next() {
		var view ViewInfo
		if err := rows.Scan(&view.ViewName, &view.ViewDefinition); err != nil {
			return nil, fmt.Errorf("扫描视图信息失败: %w", err)
		}
		views = append(views, view)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("遍历视图结果失败: %w", err)
	}

	return views, nil
}

// GetViewDDL 获取视图 DDL（使用 SHOW CREATE VIEW 获取格式化的定义）
func (c *Connection) GetViewDDL(viewName string) (string, error) {
	query := `SHOW CREATE VIEW ` + viewName
	var tableName, createView, charset, collation string
	err := c.db.QueryRowContext(c.context(), query).Scan(&tableName, &createView, &charset, &collation)
	if err != nil {
		return "", fmt.Errorf("获取视图 DDL 失败：%w", err)
	}

	// 格式化视图定义，将 CREATE VIEW 和 AS SELECT 分开
	// SHOW CREATE VIEW 返回的是单行字符串，需要添加换行符
	// 例如：CREATE ... VIEW ... AS SELECT ...
	// 使用正则表达式替换 AS select/SELECT，支持大小写
	re := regexp.MustCompile(`(?i)\s+AS\s+SELECT\s+`)
	createView = re.ReplaceAllString(createView, "\nAS\nSELECT ")

	return createView, nil
}

// GetFunctions 获取所有函数信息
func (c *Connection) GetFunctions() ([]FunctionInfo, error) {
	// 使用SHOW FUNCTION STATUS获取函数列表，避免查询information_schema导致的权限问题
	// 这样可以同时兼容MySQL 5.7和MySQL 8.0
	query := fmt.Sprintf("SHOW FUNCTION STATUS WHERE Db = '%s'", c.config.Database)

	rows, err := c.db.QueryContext(c.context(), query)
	if err != nil {
		return nil, fmt.Errorf("获取函数列表失败: %w", err)
	}
	defer rows.Close()

	// 获取列信息，只需要调用一次
	columns, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("获取函数状态结果列信息失败: %w", err)
	}

	var functionNames []string
	for rows.Next() {
		// 创建足够的空指针来存储结果
		values := make([]interface{}, len(columns))
		valuePtrs := make([]interface{}, len(columns))
		for i := range columns {
			valuePtrs[i] = &values[i]
		}

		// 扫描结果
		if err := rows.Scan(valuePtrs...); err != nil {
			return nil, fmt.Errorf("扫描函数状态信息失败: %w", err)
		}

		// 提取函数名（Name字段在第2个位置）
		var name string
		if len(columns) > 1 {
			// 使用通用的类型转换方法
			switch v := values[1].(type) {
			case []byte:
				name = string(v)
			case string:
				name = v
			default:
				name = fmt.Sprintf("%v", v)
			}
		}

		if name != "" {
			functionNames = append(functionNames, name)
		}
	}

	// 检查是否有错误发生
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("迭代函数列表时发生错误: %w", err)
	}

	var functions []FunctionInfo
	for _, funcName := range functionNames {
		// 取消检查：根 context 取消后停止获取后续函数定义
		if err := c.context().Err(); err != nil {
			break
		}

		// 使用SHOW CREATE FUNCTION获取函数定义
		funcQuery := fmt.Sprintf("SHOW CREATE FUNCTION `%s`", funcName)
		funcRows, err := c.db.QueryContext(c.context(), funcQuery)
		if err != nil {
			// 如果获取某个函数的定义失败，跳过该函数，继续处理其他函数
			continue
		}

		// 先检查是否有结果行
		if !funcRows.Next() {
			funcRows.Close()
			continue
		}

		// 使用动态方式处理SHOW CREATE FUNCTION的结果，避免不同MySQL版本返回不同字段数的问题
		columns, err := funcRows.Columns()
		if err != nil {
			funcRows.Close()
			continue
		}

		// 创建足够的空指针来存储结果
		values := make([]interface{}, len(columns))
		valuePtrs := make([]interface{}, len(columns))
		for i := range columns {
			valuePtrs[i] = &values[i]
		}

		// 扫描结果
		if err := funcRows.Scan(valuePtrs...); err != nil {
			funcRows.Close()
			continue
		}

		funcRows.Close()

		// 解析结果，寻找Function和Create Function字段
		var name, definition string
		for i, col := range columns {
			var value string
			if values[i] == nil {
				value = ""
			} else if b, ok := values[i].([]byte); ok {
				value = string(b)
			} else if v, ok := values[i].(string); ok {
				value = v
			} else {
				value = fmt.Sprintf("%v", values[i])
			}

			// 根据列名确定字段值
			switch strings.ToLower(col) {
			case "function":
				name = value
			case "create function":
				definition = value
			default:
				// 忽略其他字段
			}
		}

		if name != "" && definition != "" {
			// 简单处理返回类型
			returnType := ""

			// 从函数体中解析参数
			parameters := ""
			if idx := strings.Index(definition, "("); idx != -1 {
				// 寻找匹配的右括号
				count := 1
				endIdx := idx + 1
				for endIdx < len(definition) {
					if definition[endIdx] == '(' {
						count++
					} else if definition[endIdx] == ')' {
						count--
						if count == 0 {
							break
						}
					}
					endIdx++
				}
				if endIdx < len(definition) {
					parameters = definition[idx+1 : endIdx]
				}
			}

			functions = append(functions, FunctionInfo{
				Name:       name,
				DDL:        definition,
				Parameters: parameters,
				ReturnType: returnType,
			})
		}
	}

	return functions, nil
}

// GetUsers 获取所有用户信息
// 同时读取 authentication_string 与 plugin 以感知密码状态（密码本身不可迁移，issue-10）
func (c *Connection) GetUsers() ([]UserInfo, error) {
	// MySQL中获取用户权限
	rows, err := c.db.QueryContext(c.context(), `
		SELECT user, host, authentication_string, plugin
		FROM mysql.user 
		WHERE user != 'root' AND user != 'mysql.sys' AND
		user != 'mysql.session' AND user != 'mysql.infoschema' AND
		user != 'mysql.pfsadmin' AND user != 'mysql.pfs' AND
		user != 'mysql.pfs_admin' AND user != 'mysql.pfs_admin_role' AND
		user != 'mysql.pfs_role_admin' AND user != 'mysql.pfs_role_admin_role' AND
		user != 'mysql.pfs_role_admin_role_role' AND
		user != 'mysql.pfs_role_admin_role_role_role' AND user != 'mysql.pfsadmin'
	`)
	if err != nil {
		return nil, fmt.Errorf("获取用户列表失败: %w", err)
	}
	defer rows.Close()

	var users []UserInfo
	for rows.Next() {
		var userName, host string
		var passwordHash, authPlugin sql.NullString
		if err := rows.Scan(&userName, &host, &passwordHash, &authPlugin); err != nil {
			return nil, fmt.Errorf("扫描用户信息失败: %w", err)
		}

		// 获取用户权限
		grants, err := c.getUserGrants(userName, host)
		if err != nil {
			return nil, fmt.Errorf("获取用户权限失败: %w", err)
		}

		users = append(users, UserInfo{
			Name:         fmt.Sprintf("%s@%s", userName, host),
			Grants:       grants,
			PasswordHash: passwordHash.String,
			AuthPlugin:   authPlugin.String,
		})
	}

	return users, nil
}

// getUserGrants 获取用户的权限信息
func (c *Connection) getUserGrants(userName, host string) ([]string, error) {
	var grantsStr string
	// 直接使用字符串拼接构建查询语句
	grantQuery := fmt.Sprintf("SHOW GRANTS FOR '%s'@'%s'", userName, host)
	err := c.db.QueryRowContext(c.context(), grantQuery).Scan(&grantsStr)
	if err != nil {
		return nil, err
	}

	// 解析权限字符串
	grants := strings.Split(grantsStr, ";")
	var cleanGrants []string
	for _, grant := range grants {
		grant = strings.TrimSpace(grant)
		if grant != "" {
			cleanGrants = append(cleanGrants, grant)
		}
	}

	return cleanGrants, nil
}

// TablePrivInfo 表权限信息
type TablePrivInfo struct {
	Host      string
	Db        string
	User      string
	TableName string
	TablePriv string
}

// GetTablePrivileges 获取表权限信息
func (c *Connection) GetTablePrivileges() ([]TablePrivInfo, error) {
	query := `
		SELECT Host, Db, User, Table_name, Table_priv 
		FROM mysql.tables_priv 
		WHERE Table_priv != ''
	`

	rows, err := c.db.QueryContext(c.context(), query)
	if err != nil {
		return nil, fmt.Errorf("获取表权限失败: %w", err)
	}
	defer rows.Close()

	var privileges []TablePrivInfo
	for rows.Next() {
		var priv TablePrivInfo
		if err := rows.Scan(&priv.Host, &priv.Db, &priv.User, &priv.TableName, &priv.TablePriv); err != nil {
			return nil, fmt.Errorf("扫描表权限信息失败: %w", err)
		}

		privileges = append(privileges, priv)
	}

	return privileges, nil
}
