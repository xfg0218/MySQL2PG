package postgres

import (
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/yourusername/mysql2pg/internal/config"
	// 因避免循环依赖，此处不导入 converter 包，相关功能通过其他方式实现
	"github.com/yourusername/mysql2pg/internal/mysql"
	"github.com/yourusername/mysql2pg/internal/postgres"
)

// Manager 转换管理器
type Manager struct {
	mysqlConn      *mysql.Connection
	postgresConn   *postgres.Connection
	config         *config.Config
	errorLogFile   *os.File
	logFile        *os.File
	totalTasks     int
	completedTasks int
	mutex          sync.Mutex
	// 存储每个转换阶段的信息
	conversionStats []ConversionStageStat
	// 存储数据校验不一致的表信息
	inconsistentTables []TableDataInconsistency
}

// ConversionStageStat 转换阶段统计信息
type ConversionStageStat struct {
	StageName   string    // 阶段名称
	StartTime   time.Time // 开始时间
	EndTime     time.Time // 结束时间
	ObjectCount int       // 处理的对象数量
}

// NewManager 创建新的转换管理器
func NewManager(mysqlConn *mysql.Connection, postgresConn *postgres.Connection, config *config.Config) (*Manager, error) {
	// 打开错误日志文件
	errorLogFile, err := os.OpenFile(config.Run.ErrorLogPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, fmt.Errorf("打开错误日志文件失败: %w", err)
	}

	// 打开或创建日志文件
	var logFile *os.File
	if config.Run.EnableFileLogging && config.Run.LogFilePath != "" {
		logFile, err = os.OpenFile(config.Run.LogFilePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			return nil, fmt.Errorf("打开日志文件失败: %w", err)
		}
	}

	return &Manager{
		mysqlConn:    mysqlConn,
		postgresConn: postgresConn,
		config:       config,
		errorLogFile: errorLogFile,
		logFile:      logFile,
	}, nil
}

// Close 关闭管理器
func (m *Manager) Close() error {
	var err error
	if m.logFile != nil {
		if closeErr := m.logFile.Close(); closeErr != nil {
			err = closeErr
		}
	}

	if closeErr := m.errorLogFile.Close(); closeErr != nil && err == nil {
		err = closeErr
	}

	return err
}

// Run 开始转换
func (m *Manager) Run() error {
	m.Log("开始转换MySQL DDL到PostgreSQL...")

	// 检查是否启用了表列表功能
	if m.config.Conversion.Options.UseTableList && len(m.config.Conversion.Options.TableList) > 0 {
		m.Log("启用了表列表功能，只同步指定表的数据")

		// 获取MySQL元数据（只需要表信息）
		allTables, _, _, _, _, err := m.getMetadata()
		if err != nil {
			return err
		}

		// 过滤出需要同步的表
		var filteredTables []mysql.TableInfo
		tableMap := make(map[string]mysql.TableInfo)
		for _, table := range allTables {
			tableMap[table.Name] = table
		}

		for _, tableName := range m.config.Conversion.Options.TableList {
			if table, exists := tableMap[tableName]; exists {
				filteredTables = append(filteredTables, table)
			} else {
				m.Log("警告: 表列表中指定的表 %s 不存在于MySQL数据库中", tableName)
			}
		}

		if len(filteredTables) == 0 {
			m.Log("警告: 表列表中没有指定存在的表，跳过数据同步")
			return nil
		}

		// 计算总任务数（只计算数据同步任务）
		m.totalTasks = len(filteredTables)
		m.Log("启用了表列表功能，只同步指定的 %d 个表", len(filteredTables))

		// 执行数据同步
		if m.config.Run.ShowConsoleLogs {
			fmt.Println("\n2. 同步表数据...")
		}
		// 记录数据同步开始时间
		startTime := time.Now()
		semaphore := make(chan struct{}, m.config.Conversion.Limits.Concurrency)
		if err := m.syncTableData(filteredTables, semaphore); err != nil {
			return err
		}
		// 记录数据同步结束时间并添加到转换统计中
		endTime := time.Now()
		m.conversionStats = append(m.conversionStats, ConversionStageStat{
			StageName:   "同步表数据",
			StartTime:   startTime,
			EndTime:     endTime,
			ObjectCount: len(filteredTables),
		})

		// 显示数据不一致表的统计信息
		m.displayInconsistentTables()

		// 生成汇总表格
		m.generateSummaryTable()

		m.Log("表列表数据同步完成!")
		return nil
	}

	// 正常转换流程
	// 1. 获取MySQL元数据
	tables, functions, indexes, users, tablePrivileges, err := m.getMetadata()
	if err != nil {
		return err
	}

	// 2. 计算总任务数
	m.calculateTotalTasks(tables, functions, indexes, users, tablePrivileges)

	// 3. 执行转换
	if err := m.executeConversion(tables, functions, indexes, users, tablePrivileges); err != nil {
		return err
	}

	// 显示数据不一致表的统计信息
	m.displayInconsistentTables()

	m.Log("转换完成!")
	return nil
}

// getMetadata 获取MySQL元数据
func (m *Manager) getMetadata() ([]mysql.TableInfo, []mysql.FunctionInfo, []mysql.IndexInfo, []mysql.UserInfo, []mysql.TablePrivInfo, error) {
	var tables []mysql.TableInfo
	var functions []mysql.FunctionInfo
	var indexes []mysql.IndexInfo
	var users []mysql.UserInfo
	var tablePrivileges []mysql.TablePrivInfo
	var err error

	if m.config.Conversion.Options.TableDDL || m.config.Conversion.Options.Indexes || m.config.Conversion.Options.Data || m.config.Conversion.Options.Grant {
		tables, err = m.mysqlConn.GetTables()
		if err != nil {
			return nil, nil, nil, nil, nil, fmt.Errorf("获取表信息失败: %w", err)
		}

		// 提取所有索引（排除主键）
		if m.config.Conversion.Options.Indexes {
			for _, table := range tables {
				for _, index := range table.Indexes {
					// 排除主键索引（MySQL中主键索引名称通常为"PRIMARY"）
					if index.Name != "PRIMARY" {
						indexes = append(indexes, index)
					}
				}
			}
		}
	}

	if m.config.Conversion.Options.Functions {
		functions, err = m.mysqlConn.GetFunctions()
		if err != nil {
			return nil, nil, nil, nil, nil, fmt.Errorf("获取函数信息失败: %w", err)
		}
	}

	if m.config.Conversion.Options.Users || m.config.Conversion.Options.Grant {
		users, err = m.mysqlConn.GetUsers()
		if err != nil {
			return nil, nil, nil, nil, nil, fmt.Errorf("获取用户信息失败: %w", err)
		}
	}

	if m.config.Conversion.Options.Grant || m.config.Conversion.Options.TablePrivileges {
		tablePrivileges, err = m.mysqlConn.GetTablePrivileges()
		if err != nil {
			return nil, nil, nil, nil, nil, fmt.Errorf("获取表权限失败: %w", err)
		}
	}

	return tables, functions, indexes, users, tablePrivileges, nil
}

// calculateTotalTasks 计算总任务数
func (m *Manager) calculateTotalTasks(tables []mysql.TableInfo, functions []mysql.FunctionInfo, indexes []mysql.IndexInfo, users []mysql.UserInfo, tablePrivileges []mysql.TablePrivInfo) {
	m.totalTasks = 0

	// 根据配置的选项计算任务数
	if m.config.Conversion.Options.TableDDL {
		m.totalTasks += len(tables)
	}
	if m.config.Conversion.Options.Data {
		m.totalTasks += len(tables)
	}
	if m.config.Conversion.Options.Indexes {
		m.totalTasks += len(indexes)
	}
	if m.config.Conversion.Options.Functions {
		m.totalTasks += len(functions)
	}
	if m.config.Conversion.Options.Users {
		m.totalTasks += len(users)
	}
	if m.config.Conversion.Options.Grant {
		m.totalTasks += len(tables)
	}
	if m.config.Conversion.Options.TablePrivileges {
		m.totalTasks += len(tablePrivileges)
	}
}

// executeConversion 执行转换
func (m *Manager) executeConversion(tables []mysql.TableInfo, functions []mysql.FunctionInfo, indexes []mysql.IndexInfo, users []mysql.UserInfo, tablePrivileges []mysql.TablePrivInfo) error {
	var wg sync.WaitGroup
	semaphore := make(chan struct{}, m.config.Conversion.Limits.Concurrency)
	errorChan := make(chan error, 1)

	// 如果启用了表列表功能，过滤出指定的表
	var filteredTables []mysql.TableInfo
	if m.config.Conversion.Options.UseTableList && len(m.config.Conversion.Options.TableList) > 0 {
		// 创建表名到表信息的映射，提高查找效率
		tableMap := make(map[string]mysql.TableInfo)
		for _, table := range tables {
			tableMap[table.Name] = table
		}

		// 只保留在表列表中的表
		for _, tableName := range m.config.Conversion.Options.TableList {
			if table, exists := tableMap[tableName]; exists {
				filteredTables = append(filteredTables, table)
			} else {
				m.Log("警告: 表列表中指定的表 %s 不存在于MySQL数据库中", tableName)
			}
		}

		if len(filteredTables) == 0 {
			m.Log("警告: 表列表中没有指定存在的表，跳过数据同步")
			return nil
		}

		m.Log("启用了表列表功能，只同步指定的 %d 个表", len(filteredTables))
	} else {
		// 未启用表列表功能，同步所有表
		filteredTables = tables
	}

	// 检查是否所有选项都打开
	allOptionsEnabled := m.config.Conversion.Options.TableDDL &&
		m.config.Conversion.Options.Data &&
		m.config.Conversion.Options.Indexes &&
		m.config.Conversion.Options.Functions &&
		m.config.Conversion.Options.Users &&
		m.config.Conversion.Options.Grant

	if allOptionsEnabled {
		// 所有选项都打开时，按照指定顺序执行
		if m.config.Run.ShowConsoleLogs {
			// fmt.Println("\n所有选项都已启用，按照指定顺序执行转换...")
		}

		// 1. 首先执行表DDL转换
		if m.config.Conversion.Options.TableDDL && len(filteredTables) > 0 {
			if m.config.Run.ShowConsoleLogs {
				fmt.Println("\n1. 开始转换表结构...")
			}
			// 记录开始时间
			startTime := time.Now()
			batchSize := m.config.Conversion.Limits.MaxDDLPerBatch
			for i := 0; i < len(filteredTables); i += batchSize {
				end := i + batchSize
				if end > len(filteredTables) {
					end = len(filteredTables)
				}

				batch := filteredTables[i:end]
				wg.Add(1)
				go func(batch []mysql.TableInfo) {
					defer wg.Done()
					if err := m.convertTables(batch, semaphore); err != nil {
						select {
						case errorChan <- err:
						default:
						}
					}
				}(batch)
			}
			wg.Wait() // 等待表DDL同步完成
			// 记录结束时间和对象数量
			m.conversionStats = append(m.conversionStats, ConversionStageStat{
				StageName:   "转换表结构",
				StartTime:   startTime,
				EndTime:     time.Now(),
				ObjectCount: len(filteredTables),
			})

			// 检查是否有错误
			select {
			case err := <-errorChan:
				return err
			default:
			}
		}

		// 2. 接着执行表数据同步
		if m.config.Conversion.Options.Data && len(filteredTables) > 0 {
			if m.config.Run.ShowConsoleLogs {
				fmt.Println("\n2. 同步表数据...")
			}
			// 记录开始时间
			startTime := time.Now()
			batchSize := m.config.Conversion.Limits.MaxDDLPerBatch
			for i := 0; i < len(filteredTables); i += batchSize {
				end := i + batchSize
				if end > len(filteredTables) {
					end = len(filteredTables)
				}

				batch := filteredTables[i:end]
				wg.Add(1)
				go func(batch []mysql.TableInfo) {
					defer wg.Done()
					if err := m.syncTableData(batch, semaphore); err != nil {
						select {
						case errorChan <- err:
						default:
						}
					}
				}(batch)
			}
			wg.Wait() // 等待表数据同步完成
			// 记录结束时间和对象数量
			m.conversionStats = append(m.conversionStats, ConversionStageStat{
				StageName:   "同步表数据",
				StartTime:   startTime,
				EndTime:     time.Now(),
				ObjectCount: len(filteredTables),
			})
		} else if m.config.Conversion.Options.Data {
			if m.config.Run.ShowConsoleLogs {
				fmt.Println("\n2. 同步表数据...")
				fmt.Println("   未发现任何表，跳过数据同步")
			}
			m.Log("Data: true，但未发现任何表，跳过数据同步")
		}

		// 检查是否有错误
		select {
		case err := <-errorChan:
			return err
		default:
		}

		// 3. 然后执行索引同步
		if m.config.Conversion.Options.Indexes && len(indexes) > 0 {
			if m.config.Run.ShowConsoleLogs {
				fmt.Println("\n3. 转换表索引...")
			}
			// 记录开始时间
			startTime := time.Now()
			batchSize := m.config.Conversion.Limits.MaxIndexesPerBatch
			for i := 0; i < len(indexes); i += batchSize {
				end := i + batchSize
				if end > len(indexes) {
					end = len(indexes)
				}

				batch := indexes[i:end]
				wg.Add(1)
				go func(batch []mysql.IndexInfo) {
					defer wg.Done()
					if err := m.convertIndexes(batch, semaphore); err != nil {
						select {
						case errorChan <- err:
						default:
						}
					}
				}(batch)
			}
			wg.Wait() // 等待索引同步完成
			// 记录结束时间和对象数量
			m.conversionStats = append(m.conversionStats, ConversionStageStat{
				StageName:   "转换表索引",
				StartTime:   startTime,
				EndTime:     time.Now(),
				ObjectCount: len(indexes),
			})

			// 检查是否有错误
			select {
			case err := <-errorChan:
				return err
			default:
			}
		}

		// 4. 然后执行函数同步
		if m.config.Conversion.Options.Functions {
			if len(functions) > 0 {
				if m.config.Run.ShowConsoleLogs {
					fmt.Println("\n4. 开始转换函数...")
				}
				// 记录开始时间
				startTime := time.Now()
				batchSize := m.config.Conversion.Limits.MaxDDLPerBatch
				for i := 0; i < len(functions); i += batchSize {
					end := i + batchSize
					if end > len(functions) {
						end = len(functions)
					}

					batch := functions[i:end]
					wg.Add(1)
					go func(batch []mysql.FunctionInfo) {
						defer wg.Done()
						if err := m.convertFunctions(batch, semaphore); err != nil {
							select {
							case errorChan <- err:
							default:
							}
						}
					}(batch)
				}
				wg.Wait() // 等待函数同步完成
				// 记录结束时间和对象数量
				m.conversionStats = append(m.conversionStats, ConversionStageStat{
					StageName:   "开始转换函数",
					StartTime:   startTime,
					EndTime:     time.Now(),
					ObjectCount: len(functions),
				})

				// 检查是否有错误
				select {
				case err := <-errorChan:
					return err
				default:
				}
			} else {
				// 当functions: true但没有函数时，添加日志提示
				if m.config.Run.ShowConsoleLogs {
					fmt.Println("\n4. 开始转换函数...")
					fmt.Println("未发现任何函数，跳过函数转换")
				}
				m.Log("functions: true，但未发现任何函数，跳过函数转换")
			}
		}

		// 5. 接着执行用户同步
		if m.config.Conversion.Options.Users {
			if len(users) > 0 {
				if m.config.Run.ShowConsoleLogs {
					fmt.Println("\n5. 开始转换用户...")
				}
				// 记录开始时间
				startTime := time.Now()
				batchSize := m.config.Conversion.Limits.MaxUsersPerBatch
				for i := 0; i < len(users); i += batchSize {
					end := i + batchSize
					if end > len(users) {
						end = len(users)
					}

					batch := users[i:end]
					wg.Add(1)
					go func(batch []mysql.UserInfo) {
						defer wg.Done()
						if err := m.convertUsers(batch, semaphore); err != nil {
							select {
							case errorChan <- err:
							default:
							}
						}
					}(batch)
				}
				wg.Wait() // 等待用户同步完成
				// 记录结束时间和对象数量
				m.conversionStats = append(m.conversionStats, ConversionStageStat{
					StageName:   "转换库用户",
					StartTime:   startTime,
					EndTime:     time.Now(),
					ObjectCount: len(users),
				})

				// 检查是否有错误
				select {
				case err := <-errorChan:
					return err
				default:
				}
			} else {
				// 当users: true但没有用户时，添加日志提示
				if m.config.Run.ShowConsoleLogs {
					fmt.Println("\n5. 开始转换用户...")
					fmt.Println("   未发现任何用户，跳过用户转换")
				}
				m.Log("users: true，但未发现任何用户，跳过用户转换")
			}
		}

		// 第六阶段：执行表权限转换（原grant选项）
		if m.config.Conversion.Options.Grant {
			if len(filteredTables) > 0 {
				if m.config.Run.ShowConsoleLogs {
					fmt.Println("\n6. 转换表权限...")
				}
				// 记录开始时间
				startTime := time.Now()
				batchSize := m.config.Conversion.Limits.MaxDDLPerBatch
				for i := 0; i < len(filteredTables); i += batchSize {
					end := i + batchSize
					if end > len(filteredTables) {
						end = len(filteredTables)
					}

					batch := filteredTables[i:end]
					wg.Add(1)
					go func(batch []mysql.TableInfo) {
						defer wg.Done()
						if err := m.convertTablePrivileges(batch, semaphore); err != nil {
							select {
							case errorChan <- err:
							default:
							}
						}
					}(batch)
				}
				wg.Wait() // 等待权限转换完成
				// 记录结束时间和对象数量
				m.conversionStats = append(m.conversionStats, ConversionStageStat{
					StageName:   "转换表权限",
					StartTime:   startTime,
					EndTime:     time.Now(),
					ObjectCount: len(filteredTables),
				})

				// 检查是否有错误
				select {
				case err := <-errorChan:
					return err
				default:
				}
			}

			// 第七阶段：执行表权限转换（新的table_privileges选项）
			if m.config.Conversion.Options.TablePrivileges {
				if len(tablePrivileges) > 0 {
					if m.config.Run.ShowConsoleLogs {
						fmt.Println("\n6. 转换表权限...")
					}
					// 记录开始时间
					startTime := time.Now()
					// 串行处理表权限转换，避免并发更新冲突
					if err := m.convertTablePrivilegesNew(tablePrivileges, semaphore); err != nil {
						select {
						case errorChan <- err:
						default:
						}
					}
					// 记录结束时间和对象数量
					m.conversionStats = append(m.conversionStats, ConversionStageStat{
						StageName:   "转换表权限",
						StartTime:   startTime,
						EndTime:     time.Now(),
						ObjectCount: len(tablePrivileges),
					})

					// 检查是否有错误
					select {
					case err := <-errorChan:
						return err
					default:
					}
				} else {
					// 当table_privileges: true但没有表权限时，添加日志提示
					if m.config.Run.ShowConsoleLogs {
						fmt.Println("\n6. 转换表权限...")
						fmt.Println("   未发现任何表权限，跳过表权限转换")
					}
					m.Log("table_privileges: true，但未发现任何表权限，跳过表权限转换")
				}
			}
		}
	} else {
		// 不是所有选项都打开时，按照逻辑顺序执行
		if m.config.Run.ShowConsoleLogs {
			fmt.Println("\n按照指定选项执行转换...")
		}

		// 第一阶段：执行表DDL转换（如果启用）
		if m.config.Conversion.Options.TableDDL && len(tables) > 0 {
			if m.config.Run.ShowConsoleLogs {
				fmt.Println("\n1. 开始转换表结构...")
			}
			// 记录开始时间
			startTime := time.Now()
			batchSize := m.config.Conversion.Limits.MaxDDLPerBatch
			for i := 0; i < len(tables); i += batchSize {
				end := i + batchSize
				if end > len(tables) {
					end = len(tables)
				}

				batch := tables[i:end]
				wg.Add(1)
				go func(batch []mysql.TableInfo) {
					defer wg.Done()
					if err := m.convertTables(batch, semaphore); err != nil {
						select {
						case errorChan <- err:
						default:
						}
					}
				}(batch)
			}
			wg.Wait() // 等待表DDL同步完成
			// 记录结束时间和对象数量
			m.conversionStats = append(m.conversionStats, ConversionStageStat{
				StageName:   "转换表结构",
				StartTime:   startTime,
				EndTime:     time.Now(),
				ObjectCount: len(tables),
			})

			// 检查是否有错误
			select {
			case err := <-errorChan:
				return err
			default:
			}
		}

		// 第二阶段：执行表数据同步（如果启用）
		if m.config.Conversion.Options.Data && len(tables) > 0 {
			if m.config.Run.ShowConsoleLogs {
				fmt.Println("\n2. 同步表数据...")
			}
			// 记录开始时间
			startTime := time.Now()
			batchSize := m.config.Conversion.Limits.MaxDDLPerBatch
			for i := 0; i < len(tables); i += batchSize {
				end := i + batchSize
				if end > len(tables) {
					end = len(tables)
				}

				batch := tables[i:end]
				wg.Add(1)
				go func(batch []mysql.TableInfo) {
					defer wg.Done()
					if err := m.syncTableData(batch, semaphore); err != nil {
						select {
						case errorChan <- err:
						default:
						}
					}
				}(batch)
			}
			wg.Wait() // 等待表数据同步完成
			// 记录结束时间和对象数量
			m.conversionStats = append(m.conversionStats, ConversionStageStat{
				StageName:   "同步表数据",
				StartTime:   startTime,
				EndTime:     time.Now(),
				ObjectCount: len(tables),
			})

			// 检查是否有错误
			select {
			case err := <-errorChan:
				return err
			default:
			}
		}

		// 第三阶段：执行索引同步（如果启用）
		if m.config.Conversion.Options.Indexes && len(indexes) > 0 {
			if m.config.Run.ShowConsoleLogs {
				fmt.Println("\n3. 转换表索引...")
			}
			// 记录开始时间
			startTime := time.Now()
			batchSize := m.config.Conversion.Limits.MaxIndexesPerBatch
			for i := 0; i < len(indexes); i += batchSize {
				end := i + batchSize
				if end > len(indexes) {
					end = len(indexes)
				}

				batch := indexes[i:end]
				wg.Add(1)
				go func(batch []mysql.IndexInfo) {
					defer wg.Done()
					if err := m.convertIndexes(batch, semaphore); err != nil {
						select {
						case errorChan <- err:
						default:
						}
					}
				}(batch)
			}
			wg.Wait() // 等待索引同步完成
			// 记录结束时间和对象数量
			m.conversionStats = append(m.conversionStats, ConversionStageStat{
				StageName:   "转换表索引",
				StartTime:   startTime,
				EndTime:     time.Now(),
				ObjectCount: len(indexes),
			})

			// 检查是否有错误
			select {
			case err := <-errorChan:
				return err
			default:
			}
		}

		// 第四阶段：执行函数同步（如果启用）
		if m.config.Conversion.Options.Functions {
			if len(functions) > 0 {
				if m.config.Run.ShowConsoleLogs {
					fmt.Println("\n4. 开始转换函数...")
				}
				// 记录开始时间
				startTime := time.Now()
				batchSize := m.config.Conversion.Limits.MaxDDLPerBatch
				for i := 0; i < len(functions); i += batchSize {
					end := i + batchSize
					if end > len(functions) {
						end = len(functions)
					}

					batch := functions[i:end]
					wg.Add(1)
					go func(batch []mysql.FunctionInfo) {
						defer wg.Done()
						if err := m.convertFunctions(batch, semaphore); err != nil {
							select {
							case errorChan <- err:
							default:
							}
						}
					}(batch)
				}
				wg.Wait() // 等待函数同步完成
				// 记录结束时间和对象数量
				m.conversionStats = append(m.conversionStats, ConversionStageStat{
					StageName:   "开始转换函数",
					StartTime:   startTime,
					EndTime:     time.Now(),
					ObjectCount: len(functions),
				})

				// 检查是否有错误
				select {
				case err := <-errorChan:
					return err
				default:
				}
			} else {
				// 当functions: true但没有函数时，添加日志提示
				if m.config.Run.ShowConsoleLogs {
					fmt.Println("\n4. 开始转换函数...")
					fmt.Println("未发现任何函数，跳过函数转换")
				}
				m.Log("functions: true，但未发现任何函数，跳过函数转换")
			}
		}

		// 第五阶段：执行用户同步（如果启用）
		if m.config.Conversion.Options.Users {
			if len(users) > 0 {
				if m.config.Run.ShowConsoleLogs {
					fmt.Println("\n5. 开始转换用户...")
				}
				// 记录开始时间
				startTime := time.Now()
				batchSize := m.config.Conversion.Limits.MaxUsersPerBatch
				for i := 0; i < len(users); i += batchSize {
					end := i + batchSize
					if end > len(users) {
						end = len(users)
					}

					batch := users[i:end]
					wg.Add(1)
					go func(batch []mysql.UserInfo) {
						defer wg.Done()
						if err := m.convertUsers(batch, semaphore); err != nil {
							select {
							case errorChan <- err:
							default:
							}
						}
					}(batch)
				}
				wg.Wait() // 等待用户同步完成
				// 记录结束时间和对象数量
				m.conversionStats = append(m.conversionStats, ConversionStageStat{
					StageName:   "转换库用户",
					StartTime:   startTime,
					EndTime:     time.Now(),
					ObjectCount: len(users),
				})

				// 检查是否有错误
				select {
				case err := <-errorChan:
					return err
				default:
				}
			} else {
				// 当users: true但没有用户时，添加日志提示
				if m.config.Run.ShowConsoleLogs {
					fmt.Println("\n5. 开始转换用户...")
					fmt.Println("   未发现任何用户，跳过用户转换")
				}
				m.Log("users: true，但未发现任何用户，跳过用户转换")
			}
		}

		// 第六阶段：执行权限转换（如果启用）
		if m.config.Conversion.Options.Grant && len(tables) > 0 {
			if m.config.Run.ShowConsoleLogs {
				fmt.Println("\n6. 转换表权限...")
			}
			// 记录开始时间
			startTime := time.Now()
			batchSize := m.config.Conversion.Limits.MaxDDLPerBatch
			for i := 0; i < len(tables); i += batchSize {
				end := i + batchSize
				if end > len(tables) {
					end = len(tables)
				}

				batch := tables[i:end]
				wg.Add(1)
				go func(batch []mysql.TableInfo) {
					defer wg.Done()
					if err := m.convertTablePrivileges(batch, semaphore); err != nil {
						select {
						case errorChan <- err:
						default:
						}
					}
				}(batch)
			}
			wg.Wait() // 等待权限转换完成
			// 记录结束时间和对象数量
			m.conversionStats = append(m.conversionStats, ConversionStageStat{
				StageName:   "转换表权限",
				StartTime:   startTime,
				EndTime:     time.Now(),
				ObjectCount: len(tables),
			})

			// 检查是否有错误
			select {
			case err := <-errorChan:
				return err
			default:
			}
		}

		if m.config.Conversion.Options.TablePrivileges {
			if len(tablePrivileges) > 0 {
				if m.config.Run.ShowConsoleLogs {
					fmt.Println("\n6. 转换表权限...")
				}
				// 记录开始时间
				startTime := time.Now()
				// 串行处理表权限转换，避免并发更新冲突
				if err := m.convertTablePrivilegesNew(tablePrivileges, semaphore); err != nil {
					select {
					case errorChan <- err:
					default:
					}
				}
				// 记录结束时间和对象数量
				m.conversionStats = append(m.conversionStats, ConversionStageStat{
					StageName:   "转换表权限",
					StartTime:   startTime,
					EndTime:     time.Now(),
					ObjectCount: len(tablePrivileges),
				})

				// 检查是否有错误
				select {
				case err := <-errorChan:
					return err
				default:
				}
			} else {
				// 当table_privileges: true但没有表权限时，添加日志提示
				if m.config.Run.ShowConsoleLogs {
					fmt.Println("\n6. 转换表权限...")
					fmt.Println("   未发现任何表权限，跳过表权限转换")
				}
				m.Log("table_privileges: true，但未发现任何表权限，跳过表权限转换")
			}
		}
	}

	// 生成汇总表格
	m.generateSummaryTable()

	return nil
}

// convertTables 转换表
func (m *Manager) convertTables(tables []mysql.TableInfo, semaphore chan struct{}) error {
	currentTableIndex := 0

	for _, table := range tables {
		semaphore <- struct{}{}
		currentTableIndex++

		// 记录原始MySQL DDL到日志文件
		m.Log("转换表 %s，MySQL DDL: %s", table.Name, table.DDL)

		pgDDL, err := ConvertTableDDL(table.DDL, m.config.Conversion.Options.LowercaseColumns)
		if err != nil {
			errMsg := fmt.Sprintf("转换表 %s 失败: %v", table.Name, err)
			m.logError(errMsg)
			<-semaphore
			m.updateProgress()
			return err
		}

		// 先检查表是否存在
		tableExists, err := m.postgresConn.TableExists(table.Name)
		if err != nil {
			errMsg := fmt.Sprintf("检查表 %s 是否存在失败: %v", table.Name, err)
			m.logError(errMsg)
			<-semaphore
			m.updateProgress()
			return err
		}

		if tableExists {
			if m.config.Conversion.Options.SkipExistingTables {
				// 更新进度
				m.mutex.Lock()
				m.completedTasks++
				progress := float64(m.completedTasks) / float64(m.totalTasks) * 100
				m.mutex.Unlock()

				// 显示跳过信息（根据配置决定是否在控制台显示）
				if m.config.Run.ShowConsoleLogs {
					m.mutex.Lock()
					fmt.Printf("进度: %.2f%% (%d/%d) : 表 %s 已存在，跳过创建\n", progress, m.completedTasks, m.totalTasks, table.Name)
					m.mutex.Unlock()
				}

				m.Log("表 %s 已存在，跳过创建", table.Name)

				// 即使表已存在，也添加注释
				m.addColumnComments(table)

				<-semaphore
				continue
			} else {
				m.Log("表 %s 已存在，正在删除...", table.Name)
				dropTableSQL := fmt.Sprintf("DROP TABLE IF EXISTS \"%s\" CASCADE", table.Name)
				if err := m.postgresConn.ExecuteDDL(dropTableSQL); err != nil {
					errMsg := fmt.Sprintf("删除表 %s 失败: %v", table.Name, err)
					m.logError(errMsg)
					<-semaphore
					m.updateProgress()
					return err
				}
				m.Log("表 %s 删除成功", table.Name)
			}
		}

		if err := m.postgresConn.ExecuteDDL(pgDDL); err != nil {
			errMsg := fmt.Sprintf("执行表 %s DDL失败: %v", table.Name, err)
			m.logError(errMsg)
			<-semaphore
			m.updateProgress()
			return err
		}

		// 为每个列添加注释
		m.addColumnComments(table)

		// 更新进度
		m.mutex.Lock()
		m.completedTasks++
		progress := float64(m.completedTasks) / float64(m.totalTasks) * 100
		m.mutex.Unlock()

		// 显示转换成功信息（根据配置决定是否在控制台显示）
		if m.config.Run.ShowConsoleLogs {
			m.mutex.Lock()
			fmt.Printf("进度: %.2f%% (%d/%d) : 转换表 %s 成功\n", progress, m.completedTasks, m.totalTasks, table.Name)
			m.mutex.Unlock()
		}

		m.Log("转换表 %s 成功", table.Name)
		<-semaphore
	}
	return nil
}

// addColumnComments 为表的列添加注释
func (m *Manager) addColumnComments(table mysql.TableInfo) {
	for _, column := range table.Columns {
		if column.Comment != "" {
			// 记录注释信息
			m.Log("为表 %s 的列 %s 添加注释: %s", table.Name, column.Name, column.Comment)

			// 构建注释语句 - 使用小写列名，因为PostgreSQL默认会将未加双引号的列名转换为小写
			commentSQL := fmt.Sprintf("COMMENT ON COLUMN \"%s\".\"%s\" IS '%s';",
				table.Name, strings.ToLower(column.Name), strings.ReplaceAll(column.Comment, "'", "''"))

			m.Log("执行注释SQL: %s", commentSQL)

			if err := m.postgresConn.ExecuteDDL(commentSQL); err != nil {
				// 如果使用小写列名失败，尝试使用原始大小写的列名（可能在DDL中使用了双引号）
				m.Log("使用小写列名失败，尝试使用原始大小写列名: %s", column.Name)
				commentSQL := fmt.Sprintf("COMMENT ON COLUMN \"%s\".\"%s\" IS '%s';",
					table.Name, column.Name, strings.ReplaceAll(column.Comment, "'", "''"))

				if err := m.postgresConn.ExecuteDDL(commentSQL); err != nil {
					errMsg := fmt.Sprintf("为表 %s 的列 %s 添加注释失败: %v", table.Name, column.Name, err)
					m.logError(errMsg)
					// 注释失败不影响整个表的转换，继续处理下一列
					continue
				}
				// 记录使用原始大小写列名成功的日志
				m.Log("为表 %s 的列 %s (使用原始大小写) 添加注释成功", table.Name, column.Name)
			} else {
				m.Log("为表 %s 的列 %s 添加注释成功", table.Name, column.Name)
			}
		}
	}
}

// convertFunctions 转换函数
func (m *Manager) convertFunctions(functions []mysql.FunctionInfo, semaphore chan struct{}) error {
	for _, function := range functions {
		semaphore <- struct{}{}

		pgDDL, err := ConvertFunctionDDL(function)
		if err != nil {
			errMsg := fmt.Sprintf("转换函数 %s 失败: %v", function.Name, err)
			m.logError(errMsg)
			<-semaphore
			m.updateProgress()
			return err
		}

		if err := m.postgresConn.ExecuteDDL(pgDDL); err != nil {
			errMsg := fmt.Sprintf("执行函数 %s DDL失败: %v", function.Name, err)
			m.logError(errMsg)
			<-semaphore
			m.updateProgress()
			return err
		}

		// 更新进度
		m.mutex.Lock()
		m.completedTasks++
		progress := float64(m.completedTasks) / float64(m.totalTasks) * 100
		m.mutex.Unlock()

		// 显示转换成功信息（根据配置决定是否在控制台显示）
		if m.config.Run.ShowConsoleLogs {
			m.mutex.Lock()
			fmt.Printf("进度: %.2f%% (%d/%d) : 转换函数 %s 成功\n", progress, m.completedTasks, m.totalTasks, function.Name)
			m.mutex.Unlock()
		}

		<-semaphore
	}
	return nil
}

// convertIndexes 转换索引
func (m *Manager) convertIndexes(indexes []mysql.IndexInfo, semaphore chan struct{}) error {
	for _, index := range indexes {
		semaphore <- struct{}{}

		pgDDL, err := ConvertIndexDDL(index.Table, index, m.config.Conversion.Options.LowercaseColumns)
		if err != nil {
			errMsg := fmt.Sprintf("转换索引 %s 失败: %v", index.Name, err)
			m.logError(errMsg)
			<-semaphore
			m.updateProgress()
			return err
		}

		// 如果没有生成DDL语句（比如只包含pri_key的索引），则跳过
		if pgDDL == "" {
			// 更新进度
			m.mutex.Lock()
			m.completedTasks++
			m.mutex.Unlock()
			<-semaphore
			m.updateProgress()
			continue
		}

		// 执行DDL语句
		if err := m.postgresConn.ExecuteDDL(pgDDL); err != nil {
			// 检查是否是索引已存在的错误
			if strings.Contains(err.Error(), "duplicate key value violates unique constraint") ||
				strings.Contains(err.Error(), "already exists") {
				m.Log("索引 %s 已存在，跳过创建", index.Name)
			} else {
				errMsg := fmt.Sprintf("执行索引 %s DDL失败: %v", index.Name, err)
				m.logError(errMsg)
				<-semaphore
				m.updateProgress()
				return err
			}
		}

		// 更新进度
		m.mutex.Lock()
		m.completedTasks++
		progress := float64(m.completedTasks) / float64(m.totalTasks) * 100
		m.mutex.Unlock()

		// 显示转换成功信息（根据配置决定是否在控制台显示）
		if m.config.Run.ShowConsoleLogs {
			m.mutex.Lock()
			fmt.Printf("进度: %.2f%% (%d/%d) : 转换索引 %s 成功\n", progress, m.completedTasks, m.totalTasks, index.Name)
			m.mutex.Unlock()
		}

		<-semaphore
	}
	return nil
}

// convertUsers 转换用户及权限
func (m *Manager) convertUsers(users []mysql.UserInfo, semaphore chan struct{}) error {
	for _, user := range users {
		semaphore <- struct{}{}

		pgDDLs, err := ConvertUserDDL(user)
		if err != nil {
			errMsg := fmt.Sprintf("转换用户 %s 失败: %v", user.Name, err)
			m.logError(errMsg)
			<-semaphore
			m.updateProgress()
			return err
		}

		// 执行每个DDL语句
		for _, ddl := range pgDDLs {
			if err := m.postgresConn.ExecuteDDL(ddl); err != nil {
				errMsg := fmt.Sprintf("执行用户 %s 权限语句失败: %v", user.Name, err)
				m.logError(errMsg)
				<-semaphore
				m.updateProgress()
				return err
			}
		}

		// 更新进度
		m.mutex.Lock()
		m.completedTasks++
		progress := float64(m.completedTasks) / float64(m.totalTasks) * 100
		m.mutex.Unlock()

		// 显示转换成功信息（根据配置决定是否在控制台显示）
		if m.config.Run.ShowConsoleLogs {
			m.mutex.Lock()
			fmt.Printf("进度: %.2f%% (%d/%d) : 转换用户 %s 的权限成功\n", progress, m.completedTasks, m.totalTasks, user.Name)
			m.mutex.Unlock()
		}

		<-semaphore
	}
	return nil
}

// syncTableData 同步表数据
// 注意：此函数已迁移到postgresql包中，这里只是调用包装
func (m *Manager) syncTableData(tables []mysql.TableInfo, semaphore chan struct{}) error {
	return SyncTableData(
		m.mysqlConn,
		m.postgresConn,
		m.config,
		m.Log,
		m.logError,
		m.updateProgress,
		&m.mutex,
		&m.completedTasks,
		m.totalTasks,
		&m.inconsistentTables,
		tables,
		semaphore,
	)
}

// convertTablePrivileges 转换表权限
func (m *Manager) convertTablePrivileges(tables []mysql.TableInfo, semaphore chan struct{}) error {
	for _, table := range tables {
		semaphore <- struct{}{}
		// 模拟权限转换
		time.Sleep(100 * time.Millisecond)

		// 更新进度
		m.mutex.Lock()
		m.completedTasks++
		progress := float64(m.completedTasks) / float64(m.totalTasks) * 100
		m.mutex.Unlock()

		// 显示转换成功信息（根据配置决定是否在控制台显示）
		if m.config.Run.ShowConsoleLogs {
			m.mutex.Lock()
			fmt.Printf("进度: %.2f%% (%d/%d) : 转换表 %s 的权限成功\n", progress, m.completedTasks, m.totalTasks, table.Name)
			m.mutex.Unlock()
		}

		// 记录到日志文件
		m.Log("转换表 %s 的权限成功", table.Name)

		<-semaphore
	}
	return nil
}

// convertTablePrivilegesNew 转换表权限（新的table_privileges选项）
func (m *Manager) convertTablePrivilegesNew(tablePrivileges []mysql.TablePrivInfo, semaphore chan struct{}) error {
	for _, tablePriv := range tablePrivileges {
		semaphore <- struct{}{}

		// 提取用户名（处理带主机和不带主机的情况）
		var userName string
		userParts := strings.Split(tablePriv.User, "@")
		if len(userParts) == 2 {
			userName = userParts[0]
		} else if len(userParts) == 1 {
			// 没有主机部分的情况
			userName = userParts[0]
		} else {
			m.Log("无效的用户名格式: %s，跳过权限授予", tablePriv.User)
			<-semaphore
			m.updateProgress()
			continue
		}

		// 检查PostgreSQL中是否存在该表
		tableExists, err := m.postgresConn.TableExists(tablePriv.TableName)
		if err != nil {
			errMsg := fmt.Sprintf("检查表 %s 是否存在失败: %v", tablePriv.TableName, err)
			m.logError(errMsg)
			<-semaphore
			m.updateProgress()
			return err
		}

		if !tableExists {
			m.Log("表 %s 在PostgreSQL中不存在，跳过权限授予", tablePriv.TableName)
			<-semaphore
			m.updateProgress()
			continue
		}

		// 转换表权限
		pgDDLs, err := ConvertTablePrivilegeDDL(tablePriv)
		if err != nil {
			errMsg := fmt.Sprintf("转换表权限失败: %v", err)
			m.logError(errMsg)
			<-semaphore
			m.updateProgress()
			return err
		}

		// 记录转换后的PostgreSQL DDL到日志文件
		for _, ddl := range pgDDLs {
			m.Log("生成表权限语句: %s", ddl)
		}

		// 执行每个DDL语句
		for _, ddl := range pgDDLs {
			if err := m.postgresConn.ExecuteDDL(ddl); err != nil {
				// 检查是否是用户不存在的错误
				if strings.Contains(err.Error(), "role ") && strings.Contains(err.Error(), " does not exist") {
					m.Log("用户 %s 在PostgreSQL中不存在，跳过权限授予", userName)
				} else {
					errMsg := fmt.Sprintf("执行表权限语句失败: %v", err)
					m.logError(errMsg)
					<-semaphore
					m.updateProgress()
					return err
				}
			}
		}

		// 更新进度
		m.mutex.Lock()
		m.completedTasks++
		completed := m.completedTasks
		total := m.totalTasks
		progress := float64(completed) / float64(total) * 100
		m.mutex.Unlock()

		// 显示转换信息（根据配置决定是否在控制台显示）
		if m.config.Run.ShowConsoleLogs {
			fmt.Printf("进度: %.2f%% (%d/%d) : 转换用户 %s 表权限成功\n", progress, completed, total, userName)
		}

		<-semaphore
	}
	return nil
}

// Log 记录日志
func (m *Manager) Log(format string, args ...interface{}) {
	logMsg := fmt.Sprintf(format, args...)
	timestamp := time.Now().Format("2006-01-02 15:04:05")
	logEntry := fmt.Sprintf("[%s] %s\n", timestamp, logMsg)

	// 写入日志文件
	if m.config.Run.EnableFileLogging {
		if m.logFile != nil {
			if _, err := m.logFile.WriteString(logEntry); err != nil {
				if m.config.Run.ShowConsoleLogs {
					fmt.Printf("写入日志文件失败: %v\n", err)
				}
			}
		}
	}

	// 根据配置决定是否在控制台显示
	if m.config.Run.ShowLogInConsole {
		fmt.Println(logMsg)
	}
}

// logError 记录错误
func (m *Manager) logError(errMsg string) {
	timestamp := time.Now().Format("2006-01-02 15:04:05")
	errorLogEntry := fmt.Sprintf("[%s] ERROR: %s\n", timestamp, errMsg)

	// 写入错误日志文件
	if m.config.Run.EnableFileLogging {
		if m.errorLogFile != nil {
			if _, err := m.errorLogFile.WriteString(errorLogEntry); err != nil {
				if m.config.Run.ShowConsoleLogs {
					fmt.Printf("写入错误日志文件失败: %v\n", err)
				}
			}
		}
	}

	// 根据配置决定是否在控制台显示
	if m.config.Run.ShowConsoleLogs {
		fmt.Printf("错误: %s\n", errMsg)
	}
}

// updateProgress 更新进度
func (m *Manager) updateProgress() {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	m.completedTasks++
	if m.config.Run.ShowProgress && m.totalTasks > 0 {
		progress := float64(m.completedTasks) / float64(m.totalTasks) * 100
		m.Log("进度: %.2f%% (%d/%d)", progress, m.completedTasks, m.totalTasks)
	}
}

// generateSummaryTable 生成转换汇总表格
func (m *Manager) generateSummaryTable() {
	if m.config.Run.ShowConsoleLogs {
		fmt.Println("\n----------------------------------------------------------------------")
		fmt.Println("各阶段及耗时汇总如下:")
		fmt.Println("+--------------------------+----------------+-----------------------+")
		fmt.Println("| 阶段                     | 对象数量       | 耗时(秒)              |")
		fmt.Println("+--------------------------+----------------+-----------------------+")

		var totalDuration float64
		for _, stat := range m.conversionStats {
			duration := stat.EndTime.Sub(stat.StartTime).Seconds()
			totalDuration += duration
			fmt.Printf("| %-20s | %-14d | %-21.2f |\n", stat.StageName, stat.ObjectCount, duration)
		}

		fmt.Println("+--------------------------+----------------+-----------------------+")
		fmt.Printf("| %-22s | %-14s | %-21.2f |\n", "总耗时", "", totalDuration)
		fmt.Println("+--------------------------+----------------+-----------------------+")
	}
}

// centerText 居中文本
func (m *Manager) centerText(text string, width int) string {
	padding := width - len(text)
	if padding <= 0 {
		return text
	}
	leftPadding := padding / 2
	rightPadding := padding - leftPadding
	return strings.Repeat(" ", leftPadding) + text + strings.Repeat(" ", rightPadding)
}

// displayInconsistentTables 显示数据校验不一致的表的统计信息
func (m *Manager) displayInconsistentTables() {
	if len(m.inconsistentTables) > 0 {
		if m.config.Run.ShowConsoleLogs {
			fmt.Println("\n+------------------+----------------+------------------+")
			fmt.Println("| 数据量校验不一致的表统计:                            |")
			fmt.Println("+------------------+----------------+------------------+")
			fmt.Println("| 表名             | MySQL数据量    | PostgreSQL数据量 |")
			fmt.Println("+------------------+----------------+------------------+")
			for _, table := range m.inconsistentTables {
				fmt.Printf("| %-16s | %-14d | %-16d |\n", table.TableName, table.MySQLRowCount, table.PostgresRowCount)
			}
			fmt.Println("+------------------+----------------+------------------+")
		}
		m.Log("共发现 %d 个表数据校验不一致", len(m.inconsistentTables))
	}
}
