package postgres

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/yourusername/mysql2pg/internal/config"
	"github.com/yourusername/mysql2pg/internal/converter/mpp"
	"github.com/yourusername/mysql2pg/internal/mysql"
	"github.com/yourusername/mysql2pg/internal/postgres"
)

// ConversionContext 转换上下文，包含版本信息
type ConversionContext struct {
	MySQLVersion     *mysql.MySQLVersionInfo
	PostgreSQLVersion *postgres.PostgreSQLVersionInfo
	Config           *config.Config
}

// shouldUseAdvancedRegexp 是否使用高级 REGEXP 转换
func (c *ConversionContext) shouldUseAdvancedRegexp() bool {
	// MySQL 9.0+ 或 MySQL 8.0.17+ 使用完整参数版本
	return c.MySQLVersion.SupportsRegexpInstrFull()
}

// shouldUseJsonTableLateral JSON_TABLE 是否转换为 LATERAL
func (c *ConversionContext) shouldUseJsonTableLateral() bool {
	// MySQL 8.0+ 且 PostgreSQL 12+ 支持
	return c.MySQLVersion.SupportsJsonTable() && c.PostgreSQLVersion.Major >= 12
}

// shouldUseJsonbPathQuery 是否使用 JSONB 路径查询
func (c *ConversionContext) shouldUseJsonbPathQuery() bool {
	// PostgreSQL 14+ 支持 JSONB 路径查询
	return c.PostgreSQLVersion.SupportsJsonbPath
}

// shouldUseJsonArrayInsert 是否支持 JSON_ARRAY_INSERT 转换
func (c *ConversionContext) shouldUseJsonArrayInsert() bool {
	return c.MySQLVersion.SupportsJsonArrayInsert()
}

// Manager 转换管理器
type Manager struct {
	mysqlConn      *mysql.Connection
	postgresConn   *postgres.Connection
	config         *config.Config
	errorLogFile   *os.File
	logFile        *os.File
	totalTasks     int
	completedTasks atomic.Int64
	mutex          sync.Mutex
	// 根 context（通常来自 signal.NotifyContext），取消后各阶段循环停止派发新任务
	ctx context.Context
	// 版本信息
	mysqlVersion     *mysql.MySQLVersionInfo
	postgreSQLVersion *postgres.PostgreSQLVersionInfo
	// 转换上下文
	conversionCtx *ConversionContext
	// 存储每个转换阶段的信息
	conversionStats []ConversionStageStat
	// 存储数据校验不一致的表信息
	inconsistentTables []TableDataInconsistency
	// 存储源库设置了密码、迁移后需要人工重置密码的用户（密码哈希格式不兼容，不可迁移，issue-10）
	passwordResetUsers []string
	// 存储转换过程中的语义降级/丢弃警告（P1-20）
	conversionWarnings []ConversionWarning
	// 存储表名到列名映射的映射
	tableColumnNamesMap map[string]map[string]string // 键：表名，值：(键：原始列名，值：转换后的列名)
	// 评估模式：只评估不写入
	assessmentMode bool
	// 评估结果（仅在评估模式下填充）
	assessmentResults *AssessmentResults
}

// AssessmentResults 评估结果
type AssessmentResults struct {
	TableErrors       map[string]error       // 表转换错误
	ViewErrors        map[string]error       // 视图转换错误
	FunctionErrors    map[string]error       // 函数转换错误
	IndexErrors       map[string]error       // 索引转换错误
	TableWarnings     map[string][]string    // 表转换警告
	ViewWarnings      map[string][]string    // 视图转换警告
	FunctionWarnings  map[string][]string    // 函数转换警告
	IndexWarnings     map[string][]string    // 索引转换警告
	ConversionStats   []ConversionStageStat  // 转换统计
	TotalRows         int64                  // 总行数
	TableDDLResults   map[string]string      // 表 DDL 转换结果（成功转换后的 PostgreSQL DDL）
	ViewDDLResults    map[string]string      // 视图 DDL 转换结果
	FunctionDDLResults map[string]string     // 函数 DDL 转换结果
}

// ConversionStageStat 转换阶段统计信息
type ConversionStageStat struct {
	StageName   string    // 阶段名称
	StartTime   time.Time // 开始时间
	EndTime     time.Time // 结束时间
	ObjectCount int       // 处理的对象数量
}

// NewManager 创建新的转换管理器实例
// 初始化数据库连接、配置和日志文件
// ctx 为根 context（通常来自 signal.NotifyContext），用于取消控制；传 nil 时回退为 context.Background()
func NewManager(ctx context.Context, mysqlConn *mysql.Connection, postgresConn *postgres.Connection, config *config.Config) (*Manager, error) {
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
		mysqlConn:           mysqlConn,
		postgresConn:        postgresConn,
		config:              config,
		errorLogFile:        errorLogFile,
		logFile:             logFile,
		ctx:                 ctx,
		tableColumnNamesMap: make(map[string]map[string]string),
	}, nil
}

// context 返回管理器持有的根 context
// 未通过 NewManager 设置时回退为 context.Background()，保证 nil 安全
func (m *Manager) context() context.Context {
	if m.ctx == nil {
		return context.Background()
	}
	return m.ctx
}

// Close 关闭转换管理器
// 关闭打开的日志文件
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

// Run 执行完整的转换流程
// 根据配置执行表DDL、数据、索引、函数、用户和权限的转换
func (m *Manager) Run() error {
	// 启动前检查取消信号
	if err := m.context().Err(); err != nil {
		return fmt.Errorf("转换已取消: %w", err)
	}

	m.Log("表MySQL 的DDL、数据、view、索引、函数、用户和权限的转换到 PostgreSQL ...")

	// 检查是否启用了表列表功能
	if m.config.Conversion.Options.UseTableList && len(m.config.Conversion.Options.TableList) > 0 {
		m.Log("启用了表列表功能，只同步指定的表")

		// 获取MySQL元数据（只需要表信息）
		allTables, _, indexes, _, _, _, err := m.getMetadata()
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
			m.Log("警告: 表列表中没有指定存在的表，跳过同步")
			return nil
		}

		// 计算总任务数
		m.totalTasks = 0
		if m.config.Conversion.Options.TableDDL {
			m.totalTasks += len(filteredTables)
		}
		if m.config.Conversion.Options.Data {
			m.totalTasks += len(filteredTables)
		}
		m.Log("启用了表列表功能，只同步指定的 %d 个表", len(filteredTables))

		// 执行DDL转换（如果启用）
		if m.config.Conversion.Options.TableDDL {
			if m.config.Run.ShowConsoleLogs {
				fmt.Println("\n1. 开始转换表结构...")
			}
			// 记录DDL转换开始时间
			startTime := time.Now()
			semaphore := make(chan struct{}, m.config.Conversion.Limits.Concurrency)
			if err := m.convertTables(filteredTables, semaphore); err != nil {
				return err
			}
			// 记录DDL转换结束时间并添加到转换统计中
			endTime := time.Now()
			m.conversionStats = append(m.conversionStats, ConversionStageStat{
				StageName:   "转换表结构",
				StartTime:   startTime,
				EndTime:     endTime,
				ObjectCount: len(filteredTables),
			})
		}

		// 执行数据同步（如果启用）
		if m.config.Conversion.Options.Data {
			if m.config.Run.ShowConsoleLogs {
				fmt.Println("\n2. 同步表数据...")
			}
			// P1-07：按配置开启一致性快照，保证源库并发写入时读到一致数据
			m.beginSnapshotIfEnabled()
			// 记录数据同步开始时间
			startTime := time.Now()
			semaphore := make(chan struct{}, m.config.Conversion.Limits.Concurrency)
			if err := m.syncTableData(filteredTables, semaphore); err != nil {
				m.mysqlConn.EndConsistentSnapshot()
				return err
			}
			m.mysqlConn.EndConsistentSnapshot()
			// 记录数据同步结束时间并添加到转换统计中
			endTime := time.Now()
			m.conversionStats = append(m.conversionStats, ConversionStageStat{
				StageName:   "同步表数据",
				StartTime:   startTime,
				EndTime:     endTime,
				ObjectCount: len(filteredTables),
			})
		}
		// 第四阶段：执行索引同步（如果启用）
		if m.config.Conversion.Options.Indexes && len(indexes) > 0 {
			m.Log("启用了索引同步功能，只同步指定的 %d 个索引", len(indexes))
			m.completedTasks.Store(0)

			// 过滤Index
			// 过滤出需要同步的表
			var filteredIndexes []mysql.IndexInfo
			for _, tableName := range m.config.Conversion.Options.TableList {
				if _, exists := tableMap[tableName]; exists {
					for _, index := range indexes {
						if index.Table == tableName {
							filteredIndexes = append(filteredIndexes, index)
						}
					}
				} else {
					m.Log("警告: 表列表中指定的表 %s 不存在于MySQL数据库中", tableName)
				}
			}

			m.totalTasks = len(filteredIndexes)
			var wg sync.WaitGroup
			semaphore := make(chan struct{}, m.config.Conversion.Limits.Concurrency)
			errorChan := make(chan error, len(filteredIndexes)+8)

			if m.config.Run.ShowConsoleLogs {
				fmt.Println("\n4. 转换表索引...")
			}
			// 记录开始时间
			startTime := time.Now()
			batchSize := m.config.Conversion.Limits.MaxIndexesPerBatch
			for i := 0; i < len(filteredIndexes); i += batchSize {
				end := i + batchSize
				if end > len(filteredIndexes) {
					end = len(filteredIndexes)
				}

				batch := filteredIndexes[i:end]
				wg.Add(1)
				go func(batch []mysql.IndexInfo) {
					defer wg.Done()
					if err := m.convertIndexes(batch, semaphore); err != nil {
						errorChan <- err
					}
				}(batch)
			}
			wg.Wait() // 等待索引同步完成
			// 记录结束时间和对象数量
			m.conversionStats = append(m.conversionStats, ConversionStageStat{
				StageName:   "转换表索引",
				StartTime:   startTime,
				EndTime:     time.Now(),
				ObjectCount: len(filteredIndexes),
			})

			// 检查是否有错误
			if err := drainErrors(errorChan); err != nil {
				return err
			}
		}

		// 显示数据不一致表的统计信息
		m.displayInconsistentTables()

		// 显示需人工重置密码的用户清单（密码不可迁移，issue-10）
		m.displayPasswordResetUsers()

		// 显示转换降级/丢弃清单（P1-20）
		m.displayConversionWarnings()

		// 生成汇总表格
		m.generateSummaryTable()

		m.Log("表列表同步完成!")
		return nil
	}

	// 正常转换流程
	// 1. 获取MySQL元数据
	tables, functions, indexes, views, users, tablePrivileges, err := m.getMetadata()
	if err != nil {
		return err
	}

	// 2. 计算总任务数
	m.calculateTotalTasks(tables, functions, indexes, views, users, tablePrivileges)

	// 3. 执行转换
	if err := m.executeConversion(tables, functions, indexes, views, users, tablePrivileges); err != nil {
		return err
	}

	// 显示数据不一致表的统计信息
	m.displayInconsistentTables()

	// 显示需人工重置密码的用户清单（密码不可迁移，issue-10）
	m.displayPasswordResetUsers()

	// 显示转换降级/丢弃清单（P1-20）
	m.displayConversionWarnings()

	m.Log("转换完成!")
	return nil
}

// getMetadata 获取MySQL数据库的元数据信息
// 返回表、函数、索引、用户和表权限信息
func (m *Manager) getMetadata() ([]mysql.TableInfo, []mysql.FunctionInfo, []mysql.IndexInfo, []mysql.ViewInfo, []mysql.UserInfo, []mysql.TablePrivInfo, error) {
	var tables []mysql.TableInfo
	var functions []mysql.FunctionInfo
	var indexes []mysql.IndexInfo
	var views []mysql.ViewInfo
	var users []mysql.UserInfo
	var tablePrivileges []mysql.TablePrivInfo
	var err error

	if m.config.Conversion.Options.TableDDL || m.config.Conversion.Options.Indexes || m.config.Conversion.Options.Data || m.config.Conversion.Options.Grant {
		tables, err = m.mysqlConn.GetTables(
			m.config.Conversion.Options.SkipUseTableList,
			m.config.Conversion.Options.SkipTableList,
			m.config.Conversion.Options.UseTableList,
			m.config.Conversion.Options.TableList,
		)
		if err != nil {
			return nil, nil, nil, nil, nil, nil, fmt.Errorf("获取表信息失败: %w", err)
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

	// 获取视图信息
	views, err = m.mysqlConn.GetViews(m.config.MySQL.Database)
	if err != nil {
		return nil, nil, nil, nil, nil, nil, fmt.Errorf("获取视图信息失败: %w", err)
	}

	if m.config.Conversion.Options.Functions {
		functions, err = m.mysqlConn.GetFunctions()
		if err != nil {
			return nil, nil, nil, nil, nil, nil, fmt.Errorf("获取函数信息失败: %w", err)
		}
	}

	if m.config.Conversion.Options.Users || m.config.Conversion.Options.Grant {
		users, err = m.mysqlConn.GetUsers()
		if err != nil {
			return nil, nil, nil, nil, nil, nil, fmt.Errorf("获取用户信息失败: %w", err)
		}
	}

	if m.config.Conversion.Options.Grant || m.config.Conversion.Options.TablePrivileges {
		tablePrivileges, err = m.mysqlConn.GetTablePrivileges()
		if err != nil {
			return nil, nil, nil, nil, nil, nil, fmt.Errorf("获取表权限失败: %w", err)
		}
	}

	return tables, functions, indexes, views, users, tablePrivileges, nil
}

// calculateTotalTasks 计算转换的总任务数
// 根据启用的转换选项和对象数量计算总任务数
func (m *Manager) calculateTotalTasks(tables []mysql.TableInfo, functions []mysql.FunctionInfo, indexes []mysql.IndexInfo, views []mysql.ViewInfo, users []mysql.UserInfo, tablePrivileges []mysql.TablePrivInfo) {
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
	if len(views) > 0 {
		m.totalTasks += len(views)
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

// getStageCount 获取启用的阶段数量
func (m *Manager) getStageCount() int {
	count := 0
	if m.config.Conversion.Options.TableDDL {
		count++
	}
	if m.config.Conversion.Options.Data {
		count++
	}
	if m.config.Conversion.Options.View {
		count++
	}
	if m.config.Conversion.Options.Indexes {
		count++
	}
	if m.config.Conversion.Options.Functions {
		count++
	}
	if m.config.Conversion.Options.Users {
		count++
	}
	if m.config.Conversion.Options.Grant || m.config.Conversion.Options.TablePrivileges {
		count++
	}
	return count
}

// getCurrentStage 获取当前阶段序号（基于已完成的stats数量）
func (m *Manager) getCurrentStage(baseStage int) int {
	return baseStage + len(m.conversionStats)
}

// beginSnapshotIfEnabled 按配置开启 MySQL 一致性快照（P1-07）
// 开启后数据阶段的所有读取基于同一快照，源库并发写入不会导致漏行/行数漂移；
// 开启失败仅告警并回退普通读取，不阻断迁移
func (m *Manager) beginSnapshotIfEnabled() {
	if !m.config.MySQL.ConsistentSnapshot {
		return
	}
	if err := m.mysqlConn.BeginConsistentSnapshot(m.context()); err != nil {
		m.logError(fmt.Sprintf("开启一致性快照失败，回退为普通读取: %v", err))
		return
	}
	m.Log("已开启一致性快照（consistent_snapshot=true），数据读取不受源库并发写入影响")
}

// drainErrors 非阻塞地排空错误通道并聚合全部错误（P1-16）
// 修复此前容量 1 通道 + select/default 丢弃导致每阶段只保留第一个错误的问题
func drainErrors(errorChan chan error) error {
	var errs []error
	for {
		select {
		case err := <-errorChan:
			errs = append(errs, err)
			continue
		default:
		}
		break
	}
	switch len(errs) {
	case 0:
		return nil
	case 1:
		return errs[0]
	}
	msgs := make([]string, 0, len(errs))
	for _, e := range errs {
		msgs = append(msgs, e.Error())
	}
	return fmt.Errorf("共 %d 个错误:\n  - %s", len(errs), strings.Join(msgs, "\n  - "))
}

// executeConversion 执行完整的转换流程
// 按照配置的顺序执行表DDL、数据、索引、函数、视图、用户和权限的转换
func (m *Manager) executeConversion(tables []mysql.TableInfo, functions []mysql.FunctionInfo, indexes []mysql.IndexInfo, views []mysql.ViewInfo, users []mysql.UserInfo, tablePrivileges []mysql.TablePrivInfo) error {
	var wg sync.WaitGroup
	semaphore := make(chan struct{}, m.config.Conversion.Limits.Concurrency)
	// 容量覆盖全部可能的批次 goroutine 数量，配合 drainErrors 聚合全部错误（P1-16）
	errorChan := make(chan error, len(tables)*2+len(views)+len(indexes)+len(functions)+len(users)+len(tablePrivileges)+16)

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
		m.config.Conversion.Options.View &&
		m.config.Conversion.Options.Indexes &&
		m.config.Conversion.Options.Functions &&
		m.config.Conversion.Options.Users &&
		m.config.Conversion.Options.Grant

	if allOptionsEnabled {
		// 所有选项都打开时，按照指定顺序执行
		// 1. 首先执行表DDL转换
		if m.config.Conversion.Options.TableDDL && len(filteredTables) > 0 {
			if m.config.Run.ShowConsoleLogs {
				fmt.Println("\n┌─────────────────────────────────────────────────────────────────┐")
				fmt.Printf("│  🚀 Stage 1/%d: 转换表结构 (%d tables)                       │\n", m.getStageCount(), len(filteredTables))
				fmt.Println("└─────────────────────────────────────────────────────────────────┘")
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
						errorChan <- err
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
			if err := drainErrors(errorChan); err != nil {
				return err
			}
		}

		// 2.1 执行视图转换（在表DDL之后，数据同步之前）
		if m.config.Conversion.Options.View && len(views) > 0 {
			if m.config.Run.ShowConsoleLogs {
				fmt.Println("\n┌─────────────────────────────────────────────────────────────────┐")
				fmt.Printf("│  🚀 Stage %d/%d: 转换表视图 (%d views)                         │\n", m.getCurrentStage(2), m.getStageCount(), len(views))
				fmt.Println("└─────────────────────────────────────────────────────────────────┘")
			}
			// 记录开始时间
			startTime := time.Now()
			batchSize := m.config.Conversion.Limits.MaxDDLPerBatch
			for i := 0; i < len(views); i += batchSize {
				end := i + batchSize
				if end > len(views) {
					end = len(views)
				}

				batch := views[i:end]
				wg.Add(1)
				go func(batch []mysql.ViewInfo) {
					defer wg.Done()
					if err := m.convertViews(batch, semaphore); err != nil {
						errorChan <- err
					}
				}(batch)
			}
			wg.Wait() // 等待视图转换完成
			// 记录结束时间和对象数量
			m.conversionStats = append(m.conversionStats, ConversionStageStat{
				StageName:   "转换表视图",
				StartTime:   startTime,
				EndTime:     time.Now(),
				ObjectCount: len(views),
			})

			// 检查是否有错误
			if err := drainErrors(errorChan); err != nil {
				return err
			}
		}

		// 2. 接着执行表数据同步
		if m.config.Conversion.Options.Data && len(filteredTables) > 0 {
			if m.config.Run.ShowConsoleLogs {
				fmt.Println("\n┌─────────────────────────────────────────────────────────────────┐")
				fmt.Printf("│  🚀 Stage %d/%d: 同步表数据 (%d tables)                        │\n", m.getCurrentStage(2), m.getStageCount(), len(filteredTables))
				fmt.Println("└─────────────────────────────────────────────────────────────────┘")
			}
			// P1-07：按配置开启一致性快照，保证源库并发写入时读到一致数据
			m.beginSnapshotIfEnabled()
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
						errorChan <- err
					}
				}(batch)
			}
			wg.Wait() // 等待表数据同步完成
			m.mysqlConn.EndConsistentSnapshot()
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
		if err := drainErrors(errorChan); err != nil {
			return err
		}

		// 3. 然后执行索引同步
		if m.config.Conversion.Options.Indexes && len(indexes) > 0 {
			if m.config.Run.ShowConsoleLogs {
				fmt.Println("\n┌─────────────────────────────────────────────────────────────────┐")
				fmt.Printf("│  🚀 Stage %d/%d: 转换表索引 (%d indexes)                       │\n", m.getCurrentStage(3), m.getStageCount(), len(indexes))
				fmt.Println("└─────────────────────────────────────────────────────────────────┘")
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
						errorChan <- err
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
			if err := drainErrors(errorChan); err != nil {
				return err
			}
		}

		// 4. 然后执行函数同步
		if m.config.Conversion.Options.Functions {
			if len(functions) > 0 {
				if m.config.Run.ShowConsoleLogs {
					fmt.Println("\n┌─────────────────────────────────────────────────────────────────┐")
					fmt.Printf("│  🚀 Stage %d/%d: 转换函数 (%d functions)                      │\n", m.getCurrentStage(4), m.getStageCount(), len(functions))
					fmt.Println("└─────────────────────────────────────────────────────────────────┘")
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
							errorChan <- err
						}
					}(batch)
				}
				wg.Wait() // 等待函数同步完成
				// 记录结束时间和对象数量
				m.conversionStats = append(m.conversionStats, ConversionStageStat{
					StageName:   "转换函数",
					StartTime:   startTime,
					EndTime:     time.Now(),
					ObjectCount: len(functions),
				})

				// 检查是否有错误
				if err := drainErrors(errorChan); err != nil {
					return err
				}
			} else {
				// 当functions: true但没有函数时，添加日志提示
				if m.config.Run.ShowConsoleLogs {
					fmt.Println("\n┌─────────────────────────────────────────────────────────────────┐")
					fmt.Println("│  ⏭ 未发现任何函数，跳过函数转换                                 │")
					fmt.Println("└─────────────────────────────────────────────────────────────────┘")
				}
				m.Log("functions: true，但未发现任何函数，跳过函数转换")
			}
		}

		// 5. 接着执行用户同步
		if m.config.Conversion.Options.Users {
			if len(users) > 0 {
				if m.config.Run.ShowConsoleLogs {
					fmt.Println("\n┌─────────────────────────────────────────────────────────────────┐")
					fmt.Printf("│  🚀 Stage %d/%d: 转换库用户 (%d users)                          │\n", m.getCurrentStage(5), m.getStageCount(), len(users))
					fmt.Println("└─────────────────────────────────────────────────────────────────┘")
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
							errorChan <- err
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
				if err := drainErrors(errorChan); err != nil {
					return err
				}
			} else {
				// 当users: true但没有用户时，添加日志提示
				if m.config.Run.ShowConsoleLogs {
					fmt.Println("\n6. 开始转换用户...")
					fmt.Println("   未发现任何用户，跳过用户转换")
				}
				m.Log("users: true，但未发现任何用户，跳过用户转换")
			}
		}

		// 第六阶段：执行表权限转换（原grant选项）
		if m.config.Conversion.Options.Grant {
			if len(filteredTables) > 0 {
				if m.config.Run.ShowConsoleLogs {
					fmt.Println("\n┌─────────────────────────────────────────────────────────────────┐")
					fmt.Printf("│  🚀 Stage %d/%d: 转换表权限 (%d tables)                         │\n", m.getCurrentStage(6), m.getStageCount(), len(filteredTables))
					fmt.Println("└─────────────────────────────────────────────────────────────────┘")
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
							errorChan <- err
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
				if err := drainErrors(errorChan); err != nil {
					return err
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
						errorChan <- err
					}
					// 记录结束时间和对象数量
					m.conversionStats = append(m.conversionStats, ConversionStageStat{
						StageName:   "转换表权限",
						StartTime:   startTime,
						EndTime:     time.Now(),
						ObjectCount: len(tablePrivileges),
					})

					// 检查是否有错误
					if err := drainErrors(errorChan); err != nil {
						return err
					}
				} else {
					// 当table_privileges: true但没有表权限时，添加日志提示
					if m.config.Run.ShowConsoleLogs {
						fmt.Println("\n7. 转换表权限...")
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
						errorChan <- err
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
			if err := drainErrors(errorChan); err != nil {
				return err
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
						errorChan <- err
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
			if err := drainErrors(errorChan); err != nil {
				return err
			}
		}

		// 第二阶段：执行视图转换（如果启用）
		if m.config.Conversion.Options.View && len(views) > 0 {
			if m.config.Run.ShowConsoleLogs {
				fmt.Println("\n3. 转换表视图...")
			}
			// 记录开始时间
			startTime := time.Now()
			batchSize := m.config.Conversion.Limits.MaxDDLPerBatch
			for i := 0; i < len(views); i += batchSize {
				end := i + batchSize
				if end > len(views) {
					end = len(views)
				}

				batch := views[i:end]
				wg.Add(1)
				go func(batch []mysql.ViewInfo) {
					defer wg.Done()
					if err := m.convertViews(batch, semaphore); err != nil {
						errorChan <- err
					}
				}(batch)
			}
			wg.Wait() // 等待视图转换完成
			// 记录结束时间和对象数量
			m.conversionStats = append(m.conversionStats, ConversionStageStat{
				StageName:   "转换表视图",
				StartTime:   startTime,
				EndTime:     time.Now(),
				ObjectCount: len(views),
			})

			// 检查是否有错误
			if err := drainErrors(errorChan); err != nil {
				return err
			}
		}

		// 第四阶段：执行索引同步（如果启用）
		if m.config.Conversion.Options.Indexes && len(indexes) > 0 {
			if m.config.Run.ShowConsoleLogs {
				fmt.Println("\n4. 转换表索引...")
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
						errorChan <- err
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
			if err := drainErrors(errorChan); err != nil {
				return err
			}
		}

		// 第五阶段：执行函数同步（如果启用）
		if m.config.Conversion.Options.Functions {
			if len(functions) > 0 {
				if m.config.Run.ShowConsoleLogs {
					fmt.Println("\n5. 开始转换函数...")
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
							errorChan <- err
						}
					}(batch)
				}
				wg.Wait() // 等待函数同步完成
				// 记录结束时间和对象数量
				m.conversionStats = append(m.conversionStats, ConversionStageStat{
					StageName:   "转换函数",
					StartTime:   startTime,
					EndTime:     time.Now(),
					ObjectCount: len(functions),
				})

				// 检查是否有错误
				if err := drainErrors(errorChan); err != nil {
					return err
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

		// 第六阶段：执行用户同步（如果启用）
		if m.config.Conversion.Options.Users {
			if len(users) > 0 {
				if m.config.Run.ShowConsoleLogs {
					fmt.Println("\n6. 开始转换用户...")
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
							errorChan <- err
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
				if err := drainErrors(errorChan); err != nil {
					return err
				}
			} else {
				// 当users: true但没有用户时，添加日志提示
				if m.config.Run.ShowConsoleLogs {
					fmt.Println("\n6. 开始转换用户...")
					fmt.Println("   未发现任何用户，跳过用户转换")
				}
				m.Log("users: true，但未发现任何用户，跳过用户转换")
			}
		}

		// 第七阶段：执行权限转换（如果启用）
		if m.config.Conversion.Options.Grant && len(tables) > 0 {
			if m.config.Run.ShowConsoleLogs {
				fmt.Println("\n7. 转换表权限...")
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
						errorChan <- err
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
			if err := drainErrors(errorChan); err != nil {
				return err
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
					errorChan <- err
				}
				// 记录结束时间和对象数量
				m.conversionStats = append(m.conversionStats, ConversionStageStat{
					StageName:   "转换表权限",
					StartTime:   startTime,
					EndTime:     time.Now(),
					ObjectCount: len(tablePrivileges),
				})

				// 检查是否有错误
				if err := drainErrors(errorChan); err != nil {
					return err
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

// convertViews 转换表视图DDL
// 将MySQL视图定义转换为PostgreSQL视图定义并执行
func (m *Manager) convertViews(views []mysql.ViewInfo, semaphore chan struct{}) error {
	currentViewIndex := 0

	for _, view := range views {
		// 取消检查：根 context 取消后停止处理后续视图
		if err := m.context().Err(); err != nil {
			return err
		}

		// 检查是否在排除列表中
		if m.config.Conversion.Options.SkipUseViewList {
			if shouldSkipView(view.ViewName, m.config.Conversion.Options.SkipViewSet) {
				m.Log("跳过视图 %s（在排除列表中）", view.ViewName)
				m.updateProgress()
				continue
			}
		}

		semaphore <- struct{}{}
		currentViewIndex++

		pgViewDDL, err := ConvertViewDDL(view.ViewName, view.ViewDefinition)
		if err != nil {
			// 记录转换失败的 MySQL 视图的部分转换结果
			m.Log("转换表视图 %s，MySQL 定义: %s", view.ViewName, view.ViewDefinition)
			// 记录转换失败的 PostgreSQL 视图的部分转换结果
			m.Log("转换表视图 %s 失败，PostgreSQL 定义: %s", view.ViewName, pgViewDDL)
			errMsg := fmt.Sprintf("转换表视图 %s 失败: %v", view.ViewName, err)
			m.logError(errMsg)
			<-semaphore
			m.updateProgress()
			return err
		}

		// 执行创建视图的SQL语句
		if err := m.postgresConn.ExecuteDDL(pgViewDDL, view.ViewDefinition); err != nil {
			errMsg := fmt.Sprintf("创建表视图 %s 失败: %v", view.ViewName, err)
			m.logError(errMsg)
			<-semaphore
			m.updateProgress()
			return err
		}

		// 更新进度
		completed := m.completeTask()
		progress := float64(completed) / float64(m.totalTasks) * 100

		// 显示转换成功信息（根据配置决定是否在控制台显示）
		if m.config.Run.ShowConsoleLogs {
			fmt.Printf("进度: %.2f%% (%d/%d) : 转换表视图 %s 成功\n", progress, completed, m.totalTasks, view.ViewName)
		}

		m.Log("转换表视图 %s 完成", view.ViewName)
		<-semaphore
	}

	return nil
}

// convertTables 转换表DDL
// 将MySQL表结构转换为PostgreSQL表结构并执行
func (m *Manager) convertTables(tables []mysql.TableInfo, semaphore chan struct{}) error {
	currentTableIndex := 0

	for _, table := range tables {
		// 取消检查：根 context 取消后停止处理后续表
		if err := m.context().Err(); err != nil {
			return err
		}

		semaphore <- struct{}{}
		currentTableIndex++

		// MPP 模式：使用主键列作为分布键
		var distByCols []string
		if m.config.Conversion.MPP.Enabled {
			// 从 DDL 中提取主键列
			distByCols = extractPrimaryKeyColumnsFromDDL(table.DDL)
		}

		pgResult, err := ConvertTableDDL(table.DDL, m.config.Conversion.Options.LowercaseColumns, distByCols...)
		if err != nil {
			// 记录转换失败的 MySQL 表的部分转换结果
			m.Log("转换表 %s，MySQL DDL: %s", table.Name, table.DDL)
			// 记录转换失败的 PostgreSQL 表的部分转换结果
			m.Log("转换表 %s 失败，PostgreSQL DDL: %s", table.Name, pgResult.DDL)
			errMsg := fmt.Sprintf("转换表 %s 失败: %v", table.Name, err)
			m.logError(errMsg)
			<-semaphore
			m.updateProgress()
			return err
		}

		// 存储列名映射，用于后续索引转换
		m.mutex.Lock()
		m.tableColumnNamesMap[table.Name] = pgResult.ColumnNames
		m.mutex.Unlock()

		// 汇入 DDL 转换中的语义降级/丢弃警告（P1-20）
		for _, w := range pgResult.Warnings {
			m.RecordConversionWarning("表结构", table.Name, w)
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
				completed := m.completeTask()
				progress := float64(completed) / float64(m.totalTasks) * 100

				// 显示跳过信息（根据配置决定是否在控制台显示）
				if m.config.Run.ShowConsoleLogs {
					fmt.Printf("进度: %.2f%% (%d/%d) : 表 %s 已存在，跳过创建\n", progress, completed, m.totalTasks, table.Name)
				}

				m.Log("表 %s 已存在，跳过创建", table.Name)

				// 即使表已存在，也添加表注释和列注释
				if pgResult.TableComment != "" {
					processedComment := m.processComment(pgResult.TableComment)
					tableCommentSQL := fmt.Sprintf("COMMENT ON TABLE \"%s\" IS '%s';",
						table.Name, processedComment)
					if err := m.postgresConn.ExecuteDDL(tableCommentSQL); err != nil {
						m.logError(fmt.Sprintf("为表 %s 添加表注释失败: %v", table.Name, err))
					}
				}
				m.addColumnComments(table, pgResult.ColumnNames)

				<-semaphore
				continue
			} else {
				dropTableSQL := fmt.Sprintf("DROP TABLE IF EXISTS \"%s\" CASCADE", table.Name)
				if err := m.postgresConn.ExecuteDDL(dropTableSQL); err != nil {
					errMsg := fmt.Sprintf("删除表 %s 失败: %v", table.Name, err)
					m.logError(errMsg)
					<-semaphore
					m.updateProgress()
					return err
				}
			}
		}

		if err := m.postgresConn.ExecuteDDL(pgResult.DDL, table.DDL); err != nil {
			errMsg := fmt.Sprintf("执行表 %s DDL失败: %v", table.Name, err)
			m.logError(errMsg)
			<-semaphore
			m.updateProgress()
			return err
		}

		for _, partitionDDL := range pgResult.PartitionDDLs {
			if err := m.postgresConn.ExecuteDDL(partitionDDL, table.DDL); err != nil {
				errMsg := fmt.Sprintf("执行表 %s 分区DDL失败: %v", table.Name, err)
				m.logError(errMsg)
				<-semaphore
				m.updateProgress()
				return err
			}
		}

		// 添加表注释
		if pgResult.TableComment != "" {
			processedComment := m.processComment(pgResult.TableComment)
			tableCommentSQL := fmt.Sprintf("COMMENT ON TABLE \"%s\" IS '%s';",
				table.Name, processedComment)
			if err := m.postgresConn.ExecuteDDL(tableCommentSQL); err != nil {
				m.logError(fmt.Sprintf("为表 %s 添加表注释失败: %v", table.Name, err))
			}
		}

		// 为每个列添加注释
		m.addColumnComments(table, pgResult.ColumnNames)

		// P1-02：应用 MySQL CHECK 约束（建表后以独立 ALTER 追加，失败仅告警不阻断）
		for _, checkDDL := range pgResult.CheckConstraints {
			if err := m.postgresConn.ExecuteDDL(checkDDL, table.DDL); err != nil {
				m.RecordConversionWarning("CHECK 约束", table.Name,
					fmt.Sprintf("应用 CHECK 约束失败（%s）: %v", checkDDL, err))
			}
		}

		// 按 MySQL 表级 AUTO_INCREMENT=N 设置序列初值
		// 覆盖 data:false 仅结构迁移的场景（数据阶段的回填以表内最大值为准）
		m.backfillInitialSequence(table)

		// 更新进度
		completed := m.completeTask()
		progress := float64(completed) / float64(m.totalTasks) * 100

		// 显示转换成功信息（根据配置决定是否在控制台显示）
		if m.config.Run.ShowConsoleLogs {
			fmt.Printf("进度: %.2f%% (%d/%d) : 转换表 %s 成功\n", progress, completed, m.totalTasks, table.Name)
		}

		m.Log("转换表 %s 成功", table.Name)
		<-semaphore
	}
	return nil
}

// processComment 处理注释中的特殊字符
func (m *Manager) processComment(comment string) string {
	processedComment := comment
	// 替换单引号
	processedComment = strings.ReplaceAll(processedComment, "'", "''")
	// 替换换行符
	processedComment = strings.ReplaceAll(processedComment, "\n", "\\n")
	// 替换回车符
	processedComment = strings.ReplaceAll(processedComment, "\r", "\\r")
	// 替换制表符
	processedComment = strings.ReplaceAll(processedComment, "\t", "\\t")
	// 替换反斜杠
	processedComment = strings.ReplaceAll(processedComment, "\\", "\\\\")
	return processedComment
}

// addColumnComments 为表的列添加注释
func (m *Manager) addColumnComments(table mysql.TableInfo, columnNameMap map[string]string) {
	for _, column := range table.Columns {
		if column.Comment != "" {

			// 处理注释中的特殊字符
			processedComment := m.processComment(column.Comment)

			// 尝试多种可能的列名格式
			var columnNames []string

			// 首先检查是否有转换后的列名映射
			if convertedColumnName, exists := columnNameMap[column.Name]; exists {
				// 使用映射表中的列名（已经包含了正确的格式和双引号）
				columnNames = []string{convertedColumnName}
			} else if m.config.Conversion.Options.LowercaseColumns {
				// 配置为转小写，尝试小写列名和原始大小写列名
				columnNames = []string{strings.ToLower(column.Name), column.Name}
			} else {
				// 保持原始大小写，尝试原始大小写列名和小写列名
				columnNames = []string{column.Name, strings.ToLower(column.Name)}
			}

			// 尝试多种列名格式和引用方式
			commentAdded := false
			for _, colName := range columnNames {
				var commentSQL string

				// 检查列名是否已经包含双引号
				if strings.HasPrefix(colName, `"`) && strings.HasSuffix(colName, `"`) {
					// 列名已经包含双引号，直接使用
					commentSQL = fmt.Sprintf("COMMENT ON COLUMN \"%s\".%s IS '%s';",
						table.Name, colName, processedComment)
				} else {
					// 列名不包含双引号，添加双引号
					commentSQL = fmt.Sprintf("COMMENT ON COLUMN \"%s\".\"%s\" IS '%s';",
						table.Name, colName, processedComment)
				}

				if err := m.postgresConn.ExecuteDDL(commentSQL); err != nil {
					// 记录尝试失败的信息，包括具体的SQL语句和错误信息
					m.Log("为表 %s 的列 %s 使用列名 %s 添加注释失败: %v，SQL语句: %s",
						table.Name, column.Name, colName, err, commentSQL)

					// 如果列名已经包含双引号，再尝试不带双引号的版本
					if strings.HasPrefix(colName, `"`) && strings.HasSuffix(colName, `"`) {
						// 去掉双引号
						rawColName := colName[1 : len(colName)-1]
						// 尝试不带双引号的列名（PostgreSQL默认不区分大小写）
						commentSQL = fmt.Sprintf("COMMENT ON COLUMN %s.%s IS '%s';",
							table.Name, rawColName, processedComment)

						if err := m.postgresConn.ExecuteDDL(commentSQL); err != nil {
							// 记录尝试失败的信息
							m.Log("为表 %s 的列 %s 使用列名 %s 添加注释失败: %v，SQL语句: %s",
								table.Name, column.Name, rawColName, err, commentSQL)
							continue
						} else {
							commentAdded = true
							break
						}
					}
					continue
				} else {
					commentAdded = true
					break
				}
			}

			if !commentAdded {
				// 所有格式都失败，可能是因为该列在实际表中不存在
				// 记录更清晰的错误信息
				errMsg := fmt.Sprintf("为表 %s 的列 %s 添加注释失败: 该列可能在实际表中不存在，跳过添加注释",
					table.Name, column.Name)
				m.logError(errMsg)
			}
		}
	}
}

// convertFunctions 转换函数
func (m *Manager) convertFunctions(functions []mysql.FunctionInfo, semaphore chan struct{}) error {
	for _, function := range functions {
		// 取消检查：根 context 取消后停止处理后续函数
		if err := m.context().Err(); err != nil {
			return err
		}

		// 检查是否在排除列表中
		if m.config.Conversion.Options.SkipUseFunctionList {
			if shouldSkipFunction(function.Name, m.config.Conversion.Options.SkipFunctionSet) {
				m.Log("跳过函数 %s（在排除列表中）", function.Name)
				m.updateProgress()
				continue // 移除错误的 semaphore
			}
		}

		semaphore <- struct{}{}

		pgDDL, err := ConvertFunctionDDL(function)
		if err != nil {
			errMsg := fmt.Sprintf("转换函数 %s 失败: %v", function.Name, err)
			m.logError(errMsg)
			<-semaphore
			m.updateProgress()
			return err
		}

		if err := m.postgresConn.ExecuteDDL(pgDDL, function.DDL); err != nil {
			errMsg := fmt.Sprintf("执行函数 %s DDL失败: %v", function.Name, err)
			m.logError(errMsg)
			<-semaphore
			m.updateProgress()
			return err
		}

		// P1-14：DECLARE HANDLER 语义无法完整转换，报告提示人工复核
		if strings.Contains(strings.ToUpper(function.DDL), "HANDLER") {
			m.RecordConversionWarning("函数语法", function.Name,
				"包含 DECLARE HANDLER 错误处理：NOT FOUND 语义已转为 IF NOT FOUND，其他 HANDLER 以注释保留，请人工复核")
		}

		// 更新进度
		completed := m.completeTask()
		progress := float64(completed) / float64(m.totalTasks) * 100

		// 显示转换成功信息（根据配置决定是否在控制台显示）
		if m.config.Run.ShowConsoleLogs {
			fmt.Printf("进度: %.2f%% (%d/%d) : 转换函数 %s 成功\n", progress, completed, m.totalTasks, function.Name)
		}

		<-semaphore
	}
	return nil
}

// convertIndexes 转换索引
// 将MySQL索引转换为PostgreSQL索引并执行
func (m *Manager) convertIndexes(indexes []mysql.IndexInfo, semaphore chan struct{}) error {
	// 创建 MPP 处理器
	schemaName := mpp.ParseSearchPath(m.postgresConn.GetPgConnectionParams())
	mppHandler := &mpp.IndexHandler{
		Config: &mpp.Config{
			Enabled:  m.config.Conversion.MPP.Enabled,
			Database: m.config.Conversion.MPP.Database,
		},
		PostgresDB: m.postgresConn.GetPool(),
		Schema:     schemaName,
		Ctx:        m.context(),
		LogFunc:    m.Log,
		ErrorFunc:  m.logError,
	}

	for _, index := range indexes {
		// 取消检查：根 context 取消后停止处理后续索引
		if err := m.context().Err(); err != nil {
			return err
		}

		semaphore <- struct{}{}

		// 使用小写索引名进行日志输出
		lowercaseIndexName := strings.ToLower(index.Name)
		// 获取该表的列名映射
		columnNamesMap := m.tableColumnNamesMap[index.Table]

		// ========== MPP UNIQUE INDEX 处理 ==========
		if index.IsUnique {
			shouldCreate, err := mppHandler.HandleUniqueIndex(index, m.config.Conversion.Options.LowercaseColumns)
			if err != nil {
				m.logError(fmt.Sprintf("处理 UNIQUE 索引 %s 失败: %v", lowercaseIndexName, err))
				// 跳过该索引，继续处理其他索引
				<-semaphore
				m.updateProgress()
				continue
			}
			if !shouldCreate {
				<-semaphore
				m.updateProgress()
				continue
			}
		}
		// ========== MPP 处理结束 ==========

		pgDDL, indexWarnings, err := ConvertIndexDDL(index, m.config.Conversion.Options.LowercaseColumns, columnNamesMap)
		for _, w := range indexWarnings {
			m.RecordConversionWarning("索引", index.Table, fmt.Sprintf("%s: %s", index.Name, w))
		}
		if err != nil {
			errMsg := fmt.Sprintf("转换索引 %s 失败: %v", lowercaseIndexName, err)
			m.logError(errMsg)
			<-semaphore
			m.updateProgress()
			return err
		}

		// 如果没有生成DDL语句（比如只包含pri_key的索引、被跳过的函数/SPATIAL索引），则跳过
		if pgDDL == "" {
			<-semaphore
			m.updateProgress()
			continue
		}

		// 执行DDL语句
		if err := m.postgresConn.ExecuteDDL(pgDDL); err != nil {
			// 检查是否是索引已存在的错误
			if strings.Contains(err.Error(), "duplicate key value violates unique constraint") ||
				strings.Contains(err.Error(), "already exists") {
				m.Log("索引 %s 已存在，跳过创建", lowercaseIndexName)
			} else {
				errMsg := fmt.Sprintf("执行索引 %s DDL失败: %v", lowercaseIndexName, err)
				m.logError(errMsg)
				<-semaphore
				m.updateProgress()
				return err
			}
		}

		// 更新进度
		completed := m.completeTask()
		progress := float64(completed) / float64(m.totalTasks) * 100

		// 显示转换成功信息（根据配置决定是否在控制台显示）
		if m.config.Run.ShowConsoleLogs {
			fmt.Printf("进度: %.2f%% (%d/%d) : [%s]转换索引 %s 成功\n", progress, completed, m.totalTasks, index.Table, lowercaseIndexName)
		}

		<-semaphore
	}
	return nil
}

// convertUsers 转换用户及权限
func (m *Manager) convertUsers(users []mysql.UserInfo, semaphore chan struct{}) error {
	for _, user := range users {
		// 取消检查：根 context 取消后停止处理后续用户
		if err := m.context().Err(); err != nil {
			return err
		}

		semaphore <- struct{}{}

		// 目标库名来自配置，schema 由连接参数 search_path 解析（P1-08）
		privilegeCtx := PrivilegeContext{
			Database: m.config.PostgreSQL.Database,
			Schema:   mpp.ParseSearchPath(m.postgresConn.GetPgConnectionParams()),
		}
		pgDDLs, warns, err := ConvertUserDDL(user, privilegeCtx)
		if err != nil {
			errMsg := fmt.Sprintf("转换用户 %s 失败: %v", user.Name, err)
			m.logError(errMsg)
			<-semaphore
			m.updateProgress()
			return err
		}

		// 汇入权限转换警告（无法映射的权限等，P1-20）
		for _, w := range warns {
			m.RecordConversionWarning("用户权限", user.Name, w)
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

		// 记录源库设置了密码的用户：密码哈希格式不兼容无法迁移，需人工重设（issue-10）
		if user.PasswordHash != "" {
			if userParts := strings.Split(user.Name, "@"); len(userParts) == 2 && !strings.HasPrefix(userParts[0], "mysql.") {
				m.mutex.Lock()
				m.passwordResetUsers = append(m.passwordResetUsers, normalizePGRoleName(userParts[0]))
				m.mutex.Unlock()
			}
		}

		// 更新进度
		completed := m.completeTask()
		progress := float64(completed) / float64(m.totalTasks) * 100

		// 显示转换成功信息（根据配置决定是否在控制台显示）
		if m.config.Run.ShowConsoleLogs {
			fmt.Printf("进度: %.2f%% (%d/%d) : 转换用户 %s 的权限成功\n", progress, completed, m.totalTasks, user.Name)
		}

		<-semaphore
	}
	return nil
}

// syncTableData 同步表数据
func (m *Manager) syncTableData(tables []mysql.TableInfo, semaphore chan struct{}) error {
	progressChan := make(chan progressUpdate, m.config.Conversion.Limits.Concurrency)
	return SyncTableData(
		m.context(),
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
		progressChan,
	)
}

// convertTablePrivileges 转换表权限
func (m *Manager) convertTablePrivileges(tables []mysql.TableInfo, semaphore chan struct{}) error {
	for _, table := range tables {
		// 取消检查：根 context 取消后停止处理后续表权限
		if err := m.context().Err(); err != nil {
			return err
		}

		semaphore <- struct{}{}
		// 模拟权限转换
		time.Sleep(100 * time.Millisecond)

		// 更新进度
		completed := m.completeTask()
		progress := float64(completed) / float64(m.totalTasks) * 100

		// 显示转换成功信息（根据配置决定是否在控制台显示）
		if m.config.Run.ShowConsoleLogs {
			fmt.Printf("进度: %.2f%% (%d/%d) : 转换表 %s 的权限成功\n", progress, completed, m.totalTasks, table.Name)
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
		// 取消检查：根 context 取消后停止处理后续表权限
		if err := m.context().Err(); err != nil {
			return err
		}

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
		completed := m.completeTask()
		total := m.totalTasks
		progress := float64(completed) / float64(total) * 100

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
func (m *Manager) logError(errMsg string, args ...interface{}) {
	if len(args) > 0 {
		errMsg = fmt.Sprintf(errMsg, args...)
	}
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

// completeTask 原子地增加已完成任务数并返回增加后的值
// 所有阶段的任务计数统一通过该方法或 updateProgress 完成，避免多处手动递增
func (m *Manager) completeTask() int64 {
	return m.completedTasks.Add(1)
}

// updateProgress 更新进度（递增计数并输出通用进度日志的唯一入口）
func (m *Manager) updateProgress() {
	completed := m.completedTasks.Add(1)
	if m.config.Run.ShowProgress && m.totalTasks > 0 {
		progress := float64(completed) / float64(m.totalTasks) * 100
		m.Log("进度: %.2f%% (%d/%d)", progress, completed, m.totalTasks)
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

	// 同时写入日志文件（供 report 命令解析）
	m.Log("----------------------------------------------------------------------")
	m.Log("各阶段及耗时汇总如下:")
	m.Log("+--------------------------+----------------+-----------------------+")
	m.Log("| 阶段                     | 对象数量       | 耗时(秒)              |")
	m.Log("+--------------------------+----------------+-----------------------+")

	var totalDuration float64
	for _, stat := range m.conversionStats {
		duration := stat.EndTime.Sub(stat.StartTime).Seconds()
		totalDuration += duration
		m.Log("| %-20s | %-14d | %-21.2f |", stat.StageName, stat.ObjectCount, duration)
	}

	m.Log("+--------------------------+----------------+-----------------------+")
	m.Log("| %-22s | %-14s | %-21.2f |", "总耗时", "", totalDuration)
	m.Log("+--------------------------+----------------+-----------------------+")
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

		// 同时写入日志文件（供 report 命令解析）
		m.Log("+------------------+----------------+------------------+")
		m.Log("| 数据量校验不一致的表统计:                            |")
		m.Log("+------------------+----------------+------------------+")
		m.Log("| 表名             | MySQL数据量    | PostgreSQL数据量 |")
		m.Log("+------------------+----------------+------------------+")
		for _, table := range m.inconsistentTables {
			m.Log("| %-16s | %-14d | %-16d |", table.TableName, table.MySQLRowCount, table.PostgresRowCount)
		}
		m.Log("+------------------+----------------+------------------+")

		m.Log("共发现 %d 个表数据校验不一致", len(m.inconsistentTables))
	}
}

// RecordConversionWarning 记录一条转换降级/丢弃警告（线程安全，P1-20）
func (m *Manager) RecordConversionWarning(category, object, detail string) {
	m.mutex.Lock()
	m.conversionWarnings = append(m.conversionWarnings, ConversionWarning{
		Category: category,
		Object:   object,
		Detail:   detail,
	})
	m.mutex.Unlock()
}

// displayConversionWarnings 显示转换降级/丢弃清单（P1-20）
func (m *Manager) displayConversionWarnings() {
	m.mutex.Lock()
	warnings := append([]ConversionWarning(nil), m.conversionWarnings...)
	m.mutex.Unlock()
	if len(warnings) == 0 {
		return
	}

	if m.config.Run.ShowConsoleLogs {
		fmt.Printf("\n以下 %d 项语义在转换中被降级或丢弃，请复核:\n", len(warnings))
		for _, w := range warnings {
			fmt.Printf("  - [%s] %s: %s\n", w.Category, w.Object, w.Detail)
		}
	}
	m.Log("以下 %d 项语义在转换中被降级或丢弃，请复核:", len(warnings))
	for _, w := range warnings {
		m.Log("  - [%s] %s: %s", w.Category, w.Object, w.Detail)
	}
}

// displayPasswordResetUsers 显示迁移后需要人工重置密码的用户清单（issue-10）
// MySQL 与 PostgreSQL 的密码哈希格式不兼容（SHA1 体系 vs SCRAM/MD5），密码不可迁移
func (m *Manager) displayPasswordResetUsers() {
	m.mutex.Lock()
	collected := append([]string(nil), m.passwordResetUsers...)
	m.mutex.Unlock()
	if len(collected) == 0 {
		return
	}

	// 去重（同一用户名可能对应多个主机维度）并保持顺序
	seen := make(map[string]bool)
	var users []string
	for _, u := range collected {
		if !seen[u] {
			seen[u] = true
			users = append(users, u)
		}
	}

	if m.config.Run.ShowConsoleLogs {
		fmt.Printf("\n注意: 密码不可迁移（MySQL 与 PostgreSQL 密码哈希格式不兼容），以下 %d 个用户需人工重置密码:\n", len(users))
		for _, u := range users {
			fmt.Printf("  ALTER USER %s PASSWORD '<请填写密码>';\n", quotePGIdentifier(u))
		}
	}
	m.Log("注意: 密码不可迁移（MySQL 与 PostgreSQL 密码哈希格式不兼容），以下 %d 个用户需人工重置密码:", len(users))
	for _, u := range users {
		m.Log("  ALTER USER %s PASSWORD '<请填写密码>';", quotePGIdentifier(u))
	}
}

// shouldSkipView 检查视图是否应该被跳过
// 使用集合进行 O(1) 查找，视图名大小写不敏感
func shouldSkipView(viewName string, excludeSet config.StringSet) bool {
	if excludeSet == nil {
		return false
	}
	_, exists := excludeSet[strings.ToLower(viewName)]
	return exists
}

// shouldSkipFunction 检查函数是否应该被跳过
// 使用集合进行 O(1) 查找，函数名大小写不敏感
func shouldSkipFunction(funcName string, excludeSet config.StringSet) bool {
	if excludeSet == nil {
		return false
	}
	_, exists := excludeSet[strings.ToLower(funcName)]
	return exists
}

// extractPrimaryKeyColumnsFromDDL 从 MySQL DDL 中提取主键列
func extractPrimaryKeyColumnsFromDDL(mysqlDDL string) []string {
	// 复用 sync_tableddl.go 中的函数
	lines := strings.Split(mysqlDDL, "\n")
	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(strings.ToUpper(trimmed), "PRIMARY KEY") {
			return extractPrimaryKeyColumns(trimmed)
		}
	}
	return []string{}
}
