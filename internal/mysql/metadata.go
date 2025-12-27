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
	Name string
	DDL  string
}

// UserInfo 用户信息
type UserInfo struct {
	Name   string
	Grants []string
}

// GetTables 获取所有表信息
func (c *Connection) GetTables() ([]TableInfo, error) {
	// 获取所有表名
	rows, err := c.db.Query("SHOW TABLES")
	if err != nil {
		return nil, fmt.Errorf("获取表列表失败: %w", err)
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
			err := c.db.QueryRowContext(ctx, query).Scan(&name, &ddl)
			if err != nil {
				resultChan <- tableResult{err: fmt.Errorf("获取表DDL失败: %w", err)}
				return
			}

			// 获取表的列信息
			columns, err := c.getTableColumns(name)
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
					Columns: columns,
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
		tables = append(tables, result.table)
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
	// 使用反引号包围表名，以处理包含特殊字符的表名
	rows, err := c.db.Query(fmt.Sprintf("SHOW INDEX FROM `%s`", tableName))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	// 使用map来按索引名分组
	indexMap := make(map[string]*IndexInfo)

	for rows.Next() {
		var table, nonUnique, keyName, columnName string

		// 只获取我们需要的字段，忽略其他字段
		// 这里使用sql.RawBytes来处理可变列数的情况
		values := make([]interface{}, 13) // 标准的13个字段
		for i := range values {
			values[i] = &sql.RawBytes{}
		}

		if err := rows.Scan(values...); err != nil {
			return nil, err
		}

		// 提取我们需要的字段
		if val, ok := values[0].(*sql.RawBytes); ok && val != nil {
			table = string(*val)
		}
		if val, ok := values[1].(*sql.RawBytes); ok && val != nil {
			nonUnique = string(*val)
		}
		if val, ok := values[2].(*sql.RawBytes); ok && val != nil {
			keyName = string(*val)
		}
		if val, ok := values[4].(*sql.RawBytes); ok && val != nil {
			columnName = string(*val)
		}

		if _, exists := indexMap[keyName]; !exists {
			indexMap[keyName] = &IndexInfo{
				Name:     keyName,
				Table:    table,
				IsUnique: nonUnique == "0",
			}
		}

		indexMap[keyName].Columns = append(indexMap[keyName].Columns, columnName)
	}

	// 将map转换为slice
	var indexes []IndexInfo
	for _, idx := range indexMap {
		indexes = append(indexes, *idx)
	}

	return indexes, nil
}

// GetFunctions 获取所有函数信息
func (c *Connection) GetFunctions() ([]FunctionInfo, error) {
	// MySQL中获取函数定义
	rows, err := c.db.Query(`
		SELECT routine_name, routine_definition 
		FROM information_schema.routines 
		WHERE routine_schema = DATABASE() AND routine_type = 'FUNCTION'
	`)
	if err != nil {
		return nil, fmt.Errorf("获取函数列表失败: %w", err)
	}
	defer rows.Close()

	var functions []FunctionInfo
	for rows.Next() {
		var funcName, definition string
		if err := rows.Scan(&funcName, &definition); err != nil {
			return nil, fmt.Errorf("扫描函数信息失败: %w", err)
		}

		functions = append(functions, FunctionInfo{
			Name: funcName,
			DDL:  definition,
		})
	}

	return functions, nil
}

// GetUsers 获取所有用户信息
func (c *Connection) GetUsers() ([]UserInfo, error) {
	// MySQL中获取用户权限
	rows, err := c.db.Query(`
		SELECT user, host 
		FROM mysql.user 
		WHERE user != 'root' AND user != 'mysql.sys' AND user != 'mysql.session'
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
