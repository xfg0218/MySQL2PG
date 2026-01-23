package mysql

import (
	"context"
	"database/sql"
	"fmt"
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
type UserInfo struct {
	Name   string
	Grants []string
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
	if err := c.db.QueryRow("SELECT USER()").Scan(&currentUser); err != nil {
		return nil, fmt.Errorf("获取当前用户名失败: %w", err)
	}

	// 使用多种方法尝试获取表列表，以兼容不同的MySQL版本和权限配置
	var rows *sql.Rows
	var err error

	// 使用INFORMATION_SCHEMA.TABLES查询，只获取TABLE类型的对象，过滤掉视图
	query := "SELECT table_name FROM INFORMATION_SCHEMA.TABLES WHERE table_schema = ? AND table_type = 'BASE TABLE'"
	rows, err = c.db.Query(query, c.config.Database)

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

	// 使用并发获取表信息
	type tableResult struct {
		table TableInfo
		err   error
	}

	resultChan := make(chan tableResult, len(tableNames))
	var wg sync.WaitGroup

	// 增加并发数量，充分利用数据库连接池
	maxConcurrent := 20
	semaphore := make(chan struct{}, maxConcurrent)

	for _, tableName := range tableNames {
		wg.Add(1)
		go func(name string) {
			defer wg.Done()

			// 获取信号量
			semaphore <- struct{}{}
			defer func() { <-semaphore }()

			// 创建一个带超时的上下文
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()

			// 使用带超时的查询获取表DDL
			var ddl string
			query := fmt.Sprintf("SHOW CREATE TABLE `%s`", name)

			// 使用Rows而不是Row，以便动态获取列数
			rows, err := c.db.QueryContext(ctx, query)
			if err != nil {
				// 检查错误是否是因为权限不足导致的SHOW VIEW命令被拒绝
				if strings.Contains(err.Error(), "SHOW VIEW command denied") || strings.Contains(err.Error(), "1142") {
					// 这是一个视图，当前用户没有权限查看其DDL，跳过该视图
					resultChan <- tableResult{}
					return
				}
				// 其他错误，返回错误信息
				resultChan <- tableResult{err: fmt.Errorf("获取表DDL失败: %w", err)}
				return
			}
			defer rows.Close()

			// 获取列信息
			columns, err := rows.Columns()
			if err != nil {
				resultChan <- tableResult{err: fmt.Errorf("获取结果列信息失败: %w", err)}
				return
			}

			// 检查是否有行数据
			if !rows.Next() {
				resultChan <- tableResult{err: fmt.Errorf("SHOW CREATE TABLE没有返回结果")}
				return
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
				resultChan <- tableResult{err: fmt.Errorf("扫描表DDL结果失败: %w", err)}
				return
			}

			// 提取DDL信息（通常在第2个字段，索引1）
			ddlFound := false
			if len(valPtrs) > 1 && *valPtrs[1] != "" {
				ddl = *valPtrs[1]
				ddlFound = true
			} else if len(valPtrs) > 3 && *valPtrs[3] != "" {
				// 处理某些情况下DDL可能在第4个字段的情况
				ddl = *valPtrs[3]
				ddlFound = true
			}

			if !ddlFound {
				resultChan <- tableResult{err: fmt.Errorf("无法从SHOW CREATE TABLE结果中提取DDL")}
				return
			}

			// 获取表的列信息
			tableColumns, err := c.getTableColumns(name)
			if err != nil {
				resultChan <- tableResult{err: fmt.Errorf("获取表列信息失败: %w", err)}
				return
			}

			// 获取表的索引信息
			indexes, err := c.getTableIndexes(name)
			if err != nil {
				resultChan <- tableResult{err: fmt.Errorf("获取表索引信息失败: %w", err)}
				return
			}

			resultChan <- tableResult{
				table: TableInfo{
					Name:    name,
					DDL:     ddl,
					Columns: tableColumns,
					Indexes: indexes,
				},
			}
		}(tableName)
	}

	// 等待所有goroutine完成
	go func() {
		wg.Wait()
		close(resultChan)
	}()

	// 收集结果
	var tables []TableInfo
	for result := range resultChan {
		if result.err != nil {
			return nil, result.err
		}
		// 只添加成功获取到DDL的表
		if result.table.DDL != "" {
			tables = append(tables, result.table)
		}
	}

	return tables, nil
}

// getTableColumns 获取表的列信息
func (c *Connection) getTableColumns(tableName string) ([]ColumnInfo, error) {
	// 使用反引号包围表名，以处理包含特殊字符的表名
	query := fmt.Sprintf("SHOW FULL COLUMNS FROM `%s`", tableName)
	rows, err := c.db.Query(query)
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
	rows, err := c.db.Query(query, c.config.Database, tableName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	// 使用map来按索引名分组
	indexMap := make(map[string]*IndexInfo)

	for rows.Next() {
		var tableName, indexName string
		var columnName sql.NullString
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

		// 只添加非NULL的列名，跳过基于表达式的索引列
		if columnName.Valid {
			indexMap[indexName].Columns = append(indexMap[indexName].Columns, columnName.String)
		}
	}

	// 将map转换为slice
	var indexes []IndexInfo
	for _, idx := range indexMap {
		indexes = append(indexes, *idx)
	}

	return indexes, nil
}

// GetViews 获取所有视图信息
func (c *Connection) GetViews(database string) ([]ViewInfo, error) {
	// 查询视图定义
	query := `
		SELECT table_name, view_definition 
		FROM INFORMATION_SCHEMA.VIEWS 
		WHERE table_schema = ?
	`
	rows, err := c.db.Query(query, database)
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

// GetFunctions 获取所有函数信息
func (c *Connection) GetFunctions() ([]FunctionInfo, error) {
	// 使用SHOW FUNCTION STATUS获取函数列表，避免查询information_schema导致的权限问题
	// 这样可以同时兼容MySQL 5.7和MySQL 8.0
	query := fmt.Sprintf("SHOW FUNCTION STATUS WHERE Db = '%s'", c.config.Database)

	rows, err := c.db.Query(query)
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
		// 使用SHOW CREATE FUNCTION获取函数定义
		funcQuery := fmt.Sprintf("SHOW CREATE FUNCTION `%s`", funcName)
		funcRows, err := c.db.Query(funcQuery)
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
func (c *Connection) GetUsers() ([]UserInfo, error) {
	// MySQL中获取用户权限
	rows, err := c.db.Query(`
		SELECT user, host 
		FROM mysql.user 
		WHERE user != 'root' AND user != 'mysql.sys' AND user != 'mysql.session' AND user != 'mysql.infoschema'
	`)
	if err != nil {
		return nil, fmt.Errorf("获取用户列表失败: %w", err)
	}
	defer rows.Close()

	var users []UserInfo
	for rows.Next() {
		var userName, host string
		if err := rows.Scan(&userName, &host); err != nil {
			return nil, fmt.Errorf("扫描用户信息失败: %w", err)
		}

		// 获取用户权限
		grants, err := c.getUserGrants(userName, host)
		if err != nil {
			return nil, fmt.Errorf("获取用户权限失败: %w", err)
		}

		users = append(users, UserInfo{
			Name:   fmt.Sprintf("%s@%s", userName, host),
			Grants: grants,
		})
	}

	return users, nil
}

// getUserGrants 获取用户的权限信息
func (c *Connection) getUserGrants(userName, host string) ([]string, error) {
	var grantsStr string
	// 直接使用字符串拼接构建查询语句
	grantQuery := fmt.Sprintf("SHOW GRANTS FOR '%s'@'%s'", userName, host)
	err := c.db.QueryRow(grantQuery).Scan(&grantsStr)
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

	rows, err := c.db.Query(query)
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
