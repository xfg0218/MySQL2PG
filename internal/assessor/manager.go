package assessor

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/yourusername/mysql2pg/internal/config"
	"github.com/yourusername/mysql2pg/internal/mysql"
	pgconn "github.com/yourusername/mysql2pg/internal/postgres"
	"github.com/yourusername/mysql2pg/internal/converter/postgres"
)

// Assessor 评估器
type Assessor struct {
	mysqlConn       *mysql.Connection
	postgresConn    *pgconn.Connection
	config          *config.Config
	report          *AssessmentReport
	mysqlVersion    *mysql.MySQLVersionInfo
	postgresVersion *pgconn.PostgreSQLVersionInfo
	ctx             context.Context
}

// context 返回评估器持有的根 context
// 未通过 NewAssessor 设置时回退为 context.Background()，保证 nil 安全
func (a *Assessor) context() context.Context {
	if a.ctx == nil {
		return context.Background()
	}
	return a.ctx
}

// NewAssessor 创建评估器
// ctx 为根 context（通常来自 signal.NotifyContext），用于取消控制
func NewAssessor(ctx context.Context, mysqlConn *mysql.Connection, postgresConn *pgconn.Connection, cfg *config.Config) (*Assessor, error) {
	// 获取 MySQL 版本信息
	mysqlVersion, _ := mysqlConn.GetVersionInfo()

	// 获取 PostgreSQL 版本信息
	pgVersion, _ := postgresConn.GetVersionInfo()

	return &Assessor{
		mysqlConn:       mysqlConn,
		postgresConn:    postgresConn,
		config:          cfg,
		report:          &AssessmentReport{},
		mysqlVersion:    mysqlVersion,
		postgresVersion: pgVersion,
		ctx:             ctx,
	}, nil
}

// Run 运行完整评估（使用现有迁移逻辑进行评估，不写入目标库）
func (a *Assessor) Run() (*AssessmentReport, error) {
	fmt.Println("开始迁移前评估（使用现有迁移逻辑，不写入目标库）...")
	
	// Step 1: 收集数据库信息
	fmt.Println("  [1/6] 收集数据库信息...")
	if err := a.collectDatabaseInfo(); err != nil {
		return nil, fmt.Errorf("收集数据库信息失败：%w", err)
	}

	// Step 2: 连接性检查
	fmt.Println("  [2/6] 连接性检查...")
	if err := a.checkConnectivity(); err != nil {
		return nil, fmt.Errorf("连接性检查失败：%w", err)
	}

	// Step 3: 获取对象元数据
	fmt.Println("  [3/6] 获取对象元数据...")
	if err := a.collectMetadata(); err != nil {
		return nil, fmt.Errorf("获取对象元数据失败：%w", err)
	}

	// Step 4: 对象检查和风险评估
	fmt.Println("  [4/6] 对象检查和风险评估...")
	a.assessObjects()

	// Step 5: 数据质量检查
	fmt.Println("  [5/6] 数据质量检查...")
	if err := a.checkDataQuality(); err != nil {
		return nil, fmt.Errorf("数据质量检查失败：%w", err)
	}

	// Step 6: 性能预估和配置建议
	fmt.Println("  [6/6] 性能预估和配置建议...")
	a.generateSuggestions()

	// 计算总体评分
	a.report.Summary.Score = CalculateOverallScore(a.report)
	a.report.Summary.RiskLevel = CalculateOverallRiskLevel(a.report)

	fmt.Println("评估完成！")
	return a.report, nil
}

// collectDatabaseInfo 收集数据库信息
func (a *Assessor) collectDatabaseInfo() error {
	// 获取 MySQL 信息
	a.report.SourceDB = SourceDBInfo{
		Type:     "MySQL",
		Host:     a.config.MySQL.Host,
		Port:     a.config.MySQL.Port,
		Database: a.config.MySQL.Database,
	}

	// 获取 MySQL 版本
	mysqlVersion, err := a.mysqlConn.GetVersion()
	if err != nil {
		return fmt.Errorf("获取 MySQL 版本失败：%w", err)
	}
	a.report.SourceDB.Version = mysqlVersion

	// 获取 MySQL 字符集和排序规则
	charset, collation, err := a.mysqlConn.GetCharsetAndCollation()
	if err != nil {
		return fmt.Errorf("获取 MySQL 字符集失败：%w", err)
	}
	a.report.SourceDB.Charset = charset
	a.report.SourceDB.Collation = collation

	// 获取 PostgreSQL 信息
	a.report.TargetDB = TargetDBInfo{
		Type:     "PostgreSQL",
		Host:     a.config.PostgreSQL.Host,
		Port:     a.config.PostgreSQL.Port,
		Database: a.config.PostgreSQL.Database,
	}

	// 获取 PostgreSQL 版本
	pgVersion, err := a.postgresConn.GetVersion()
	if err != nil {
		return fmt.Errorf("获取 PostgreSQL 版本失败：%w", err)
	}
	a.report.TargetDB.Version = pgVersion

	// 获取 PostgreSQL 字符集
	pgCharset, err := a.postgresConn.GetCharset()
	if err != nil {
		return fmt.Errorf("获取 PostgreSQL 字符集失败：%w", err)
	}
	a.report.TargetDB.Charset = pgCharset

	return nil
}

// checkConnectivity 连接性检查
func (a *Assessor) checkConnectivity() error {
	// 这里可以扩展更多的连接性检查
	// 目前基础连接测试已在 main.go 中完成
	return nil
}

// collectMetadata 收集元数据
func (a *Assessor) collectMetadata() error {
	var wg sync.WaitGroup
	errChan := make(chan error, 6)

	// 并发收集所有元数据
	wg.Add(6)

	// 收集表信息
	go func() {
		defer wg.Done()
		if err := a.collectTables(); err != nil {
			errChan <- fmt.Errorf("收集表信息失败：%w", err)
		}
	}()

	// 收集视图信息
	go func() {
		defer wg.Done()
		if err := a.collectViews(); err != nil {
			errChan <- fmt.Errorf("收集视图信息失败：%w", err)
		}
	}()

	// 收集索引信息
	go func() {
		defer wg.Done()
		if err := a.collectIndexes(); err != nil {
			errChan <- fmt.Errorf("收集索引信息失败：%w", err)
		}
	}()

	// 收集函数信息
	go func() {
		defer wg.Done()
		if err := a.collectFunctions(); err != nil {
			errChan <- fmt.Errorf("收集函数信息失败：%w", err)
		}
	}()

	// 收集用户信息
	go func() {
		defer wg.Done()
		if err := a.collectUsers(); err != nil {
			errChan <- fmt.Errorf("收集用户信息失败：%w", err)
		}
	}()

	// 收集权限信息
	go func() {
		defer wg.Done()
		if err := a.collectPrivileges(); err != nil {
			errChan <- fmt.Errorf("收集权限信息失败：%w", err)
		}
	}()

	// 等待所有收集完成
	go func() {
		wg.Wait()
		close(errChan)
	}()

	// 检查错误
	for err := range errChan {
		if err != nil {
			return err
		}
	}

	return nil
}

// collectTables 收集表信息
func (a *Assessor) collectTables() error {
	tables, err := a.mysqlConn.GetTables(
		a.config.Conversion.Options.SkipUseTableList,
		a.config.Conversion.Options.SkipTableList,
		a.config.Conversion.Options.UseTableList,
		a.config.Conversion.Options.TableList,
	)
	if err != nil {
		return err
	}

	a.report.Tables = make([]TableDetail, 0, len(tables))
	
	for i, table := range tables {
		// 获取表行数
		rowCount, err := a.mysqlConn.GetTableRowCount(table.Name)
		if err != nil {
			rowCount = -1 // 如果获取失败，设为 -1
		}

		// 计算 DDL 行数
		ddlRows := len(strings.Split(table.DDL, "\n"))

		a.report.Tables = append(a.report.Tables, TableDetail{
			ID:        i + 1,
			Name:      table.Name,
			RowCount:  rowCount,
			DDLRows:   ddlRows,
			RiskLevel: RiskLevelNone, // 初始化为无风险
			Risks:     []Risk{},
		})

		// 累加总行数
		if rowCount > 0 {
			a.report.Summary.TotalTableRows += rowCount
		}
	}

	a.report.Summary.TotalTables = len(tables)
	return nil
}

// collectViews 收集视图信息
func (a *Assessor) collectViews() error {
	views, err := a.mysqlConn.GetViews(a.config.MySQL.Database)
	if err != nil {
		return err
	}

	a.report.Views = make([]ViewDetail, 0, len(views))
	
	for i, view := range views {
		// 计算视图定义行数
		ddlRows := len(strings.Split(view.ViewDefinition, "\n"))

		a.report.Views = append(a.report.Views, ViewDetail{
			ID:        i + 1,
			Name:      view.ViewName,
			DDLRows:   ddlRows,
			RiskLevel: RiskLevelNone,
			Risks:     []Risk{},
		})

		// 累加总行数
		a.report.Summary.TotalViewRows += int64(ddlRows)
	}

	a.report.Summary.TotalViews = len(views)
	return nil
}

// collectIndexes 收集索引信息
func (a *Assessor) collectIndexes() error {
	// 索引信息需要从表中提取
	indexMap := make(map[string][]IndexDetail)
	
	for _, table := range a.report.Tables {
		tableIndexes, err := a.mysqlConn.GetTableIndexes(table.Name)
		if err != nil {
			continue // 跳过获取失败的表
		}

		for _, idx := range tableIndexes {
			indexMap[table.Name] = append(indexMap[table.Name], IndexDetail{
				Name:      idx.Name,
				TableName: table.Name,
				DDLRows:   1, // 索引通常是一行定义
				RiskLevel: RiskLevelNone,
				Risks:     []Risk{},
			})
		}
	}

	// 扁平化索引列表
	a.report.Indexes = make([]IndexDetail, 0)
	for _, indexes := range indexMap {
		a.report.Indexes = append(a.report.Indexes, indexes...)
	}

	// 设置序号和统计
	for i := range a.report.Indexes {
		a.report.Indexes[i].ID = i + 1
		a.report.Summary.TotalIndexRows += int64(a.report.Indexes[i].DDLRows)
	}

	a.report.Summary.TotalIndexes = len(a.report.Indexes)
	return nil
}

// collectFunctions 收集函数信息
func (a *Assessor) collectFunctions() error {
	functions, err := a.mysqlConn.GetFunctions()
	if err != nil {
		return err
	}

	a.report.Functions = make([]FunctionDetail, 0, len(functions))
	
	for i, fn := range functions {
		// 计算函数定义行数
		ddlRows := len(strings.Split(fn.DDL, "\n"))

		a.report.Functions = append(a.report.Functions, FunctionDetail{
			ID:        i + 1,
			Name:      fn.Name,
			Parameters: fn.Parameters,
			DDLRows:   ddlRows,
			RiskLevel: RiskLevelNone,
			Risks:     []Risk{},
		})

		// 累加总行数
		a.report.Summary.TotalFunctionRows += int64(ddlRows)
	}

	a.report.Summary.TotalFunctions = len(functions)
	return nil
}

// collectUsers 收集用户信息
func (a *Assessor) collectUsers() error {
	users, err := a.mysqlConn.GetUsers()
	if err != nil {
		return err
	}

	a.report.Users = make([]UserDetail, 0, len(users))

	for i, user := range users {
		// 解析用户名和主机
		name, host := parseUserHost(user.Name)

		// 过滤掉 MySQL 系统用户，如 mysql.infoschema、mysql.session 等
		// 这些用户是 MySQL 内部使用的，不需要迁移到 PostgreSQL
		if strings.HasPrefix(name, "mysql.") {
			continue
		}

		a.report.Users = append(a.report.Users, UserDetail{
			ID:        i + 1,
			Name:      name,
			Host:      host,
			RiskLevel: RiskLevelNone,
			Risks:     []Risk{},
		})
	}

	a.report.Summary.TotalUsers = len(a.report.Users)
	return nil
}

// collectPrivileges 收集表权限信息
func (a *Assessor) collectPrivileges() error {
	privileges, err := a.mysqlConn.GetTablePrivileges()
	if err != nil {
		return err
	}

	a.report.Privileges = make([]PrivilegeDetail, 0, len(privileges))
	
	for i, priv := range privileges {
		a.report.Privileges = append(a.report.Privileges, PrivilegeDetail{
			ID:         i + 1,
			UserName:   fmt.Sprintf("%s@%s", priv.User, priv.Host),
			TableName:  priv.TableName,
			Privileges: priv.TablePriv,
			RiskLevel:  RiskLevelNone,
			Risks:      []Risk{},
		})
	}

	a.report.Summary.TotalPrivileges = len(privileges)
	return nil
}

// assessObjects 评估所有对象的风险（使用现有转换函数进行评估）
func (a *Assessor) assessObjects() {
	// 评估表风险 - 使用 ConvertTableDDL 函数进行实际转换评估
	for i := range a.report.Tables {
		table := &a.report.Tables[i]
		// 获取表的 DDL
		ddl := ""
		if tableDDL, err := a.mysqlConn.GetTableDDL(a.context(), table.Name); err == nil {
			ddl = tableDDL
			// 尝试使用现有转换函数进行转换，评估兼容性
			result, err := postgres.ConvertTableDDL(ddl, a.config.Conversion.Options.LowercaseColumns)
			if err != nil {
				// 转换失败，记录为高风险
				table.Risks = append(table.Risks, Risk{
					Level:       RiskLevelHigh,
					Type:        "DDL 转换失败",
					Description: fmt.Sprintf("无法转换为 PostgreSQL DDL: %v", err),
					Suggestion:  "需要手动检查和修改表结构",
				})
			} else if result != nil {
				// 检查转换结果中是否有警告或需要注意的地方
				table.Risks = append(table.Risks, checkTableConversionWarnings(result, ddl)...)
			}
		}
		// 添加基础风险检查
		table.Risks = append(table.Risks, CheckTableRisks(ddl, table.RowCount)...)
		table.RiskLevel = CalculateRiskLevel(table.Risks)
		table.Suggestions = generateTableSuggestions(table.Risks)
	}

	// 评估视图风险 - 使用 ConvertViewDDL 函数进行实际转换评估
	views, _ := a.mysqlConn.GetViews(a.config.MySQL.Database)
	viewDefMap := make(map[string]string)
	for _, v := range views {
		viewDefMap[v.ViewName] = v.ViewDefinition
	}

	for i := range a.report.Views {
		view := &a.report.Views[i]
		viewDef := viewDefMap[view.Name]
		// 尝试使用现有转换函数进行转换
		_, err := postgres.ConvertViewDDL(view.Name, viewDef)
		if err != nil {
			view.Risks = append(view.Risks, Risk{
				Level:       RiskLevelHigh,
				Type:        "视图转换失败",
				Description: fmt.Sprintf("无法转换为 PostgreSQL 视图：%v", err),
				Suggestion:  "需要手动修改视图定义",
			})
		}
		view.Risks = append(view.Risks, CheckViewRisks(viewDef)...)
		view.RiskLevel = CalculateRiskLevel(view.Risks)
		view.Suggestions = generateViewSuggestions(view.Risks)
	}

	// 评估索引风险
	for i := range a.report.Indexes {
		idx := &a.report.Indexes[i]
		// 检查是否是全文索引或空间索引
		isFullText := strings.Contains(strings.ToLower(idx.Name), "ft_") || strings.Contains(strings.ToLower(idx.Name), "fulltext")
		isSpatial := strings.Contains(strings.ToLower(idx.Name), "sp_") || strings.Contains(strings.ToLower(idx.Name), "spatial")
		idx.Risks = CheckIndexRisks("", isFullText, isSpatial)
		idx.RiskLevel = CalculateRiskLevel(idx.Risks)
		idx.Suggestions = generateIndexSuggestions(idx.Risks)
	}

	// 评估函数风险 - 使用 ConvertFunctionDDL 函数进行实际转换评估
	functions, _ := a.mysqlConn.GetFunctions()
	funcDDLMap := make(map[string]mysql.FunctionInfo)
	for _, fn := range functions {
		funcDDLMap[fn.Name] = fn
	}

	for i := range a.report.Functions {
		fn := &a.report.Functions[i]
		if fnInfo, ok := funcDDLMap[fn.Name]; ok {
			// 尝试使用现有转换函数进行转换
			_, err := postgres.ConvertFunctionDDL(fnInfo)
			if err != nil {
				fn.Risks = append(fn.Risks, Risk{
					Level:       RiskLevelHigh,
					Type:        "函数转换失败",
					Description: fmt.Sprintf("无法转换为 PostgreSQL 函数：%v", err),
					Suggestion:  "需要手动修改函数定义",
				})
			}
		}
		fn.Risks = append(fn.Risks, CheckFunctionRisks(getFunctionDDL(funcDDLMap, fn.Name))...)
		fn.RiskLevel = CalculateRiskLevel(fn.Risks)
		fn.Suggestions = generateFunctionSuggestions(fn.Risks)
	}

	// 评估用户风险
	for i := range a.report.Users {
		user := &a.report.Users[i]
		user.Risks = CheckUserRisks(user.Host, false, false)
		user.RiskLevel = CalculateRiskLevel(user.Risks)
		user.Suggestions = generateUserSuggestions(user.Risks)
	}

	// 评估权限风险
	for i := range a.report.Privileges {
		priv := &a.report.Privileges[i]
		priv.Risks = CheckPrivilegeRisks("", priv.Privileges)
		priv.RiskLevel = CalculateRiskLevel(priv.Risks)
		priv.Suggestions = generatePrivilegeSuggestions(priv.Risks)
	}

	// 生成高风险对象汇总
	a.report.HighRisks = generateHighRiskSummary(a.report)
}

// checkDataQuality 数据质量检查
func (a *Assessor) checkDataQuality() error {
	// TODO: 实现数据质量检查
	// - 空表检测
	// - 大表检测
	// - zero-date 检测
	// - 特殊字符检测
	return nil
}

// generateSuggestions 生成配置建议
func (a *Assessor) generateSuggestions() {
	// 基于表大小和行数估算
	totalRows := a.report.Summary.TotalTableRows
	
	// 简单估算：每 10 万行约需 1 分钟
	estimatedMinutes := int(totalRows / 100000)
	if estimatedMinutes < 1 {
		estimatedMinutes = 1
	}

	// 估算数据量：假设平均每行 1KB
	estimatedSizeMB := totalRows / 1024
	if estimatedSizeMB < 1 {
		estimatedSizeMB = 1
	}

	// 生成推荐配置
	recommendedConfig := fmt.Sprintf(`  conversion:
    limits:
      concurrency: 10
      max_rows_per_batch: 50000
      batch_insert_size: 50000
    options:
      truncate_before_sync: true
      validate_data: true`,
	)

	a.report.Suggestions = ConfigSuggestion{
		EstimatedTime:     fmt.Sprintf("%d-%d 分钟", estimatedMinutes, estimatedMinutes*2),
		EstimatedDataSize: fmt.Sprintf("%dMB", estimatedSizeMB),
		RecommendedConfig: recommendedConfig,
	}
}

// 辅助函数

func parseUserHost(userHost string) (name, host string) {
	parts := strings.Split(userHost, "@")
	if len(parts) == 2 {
		return parts[0], parts[1]
	}
	return userHost, "localhost"
}

func generateTableSuggestions(risks []Risk) string {
	if len(risks) == 0 {
		return "可直接执行迁移"
	}
	var suggestions []string
	for _, risk := range risks {
		suggestions = append(suggestions, risk.Suggestion)
	}
	return strings.Join(suggestions, "; ")
}

func generateViewSuggestions(risks []Risk) string {
	if len(risks) == 0 {
		return "可直接执行迁移"
	}
	var suggestions []string
	for _, risk := range risks {
		suggestions = append(suggestions, risk.Suggestion)
	}
	return strings.Join(suggestions, "; ")
}

func generateIndexSuggestions(risks []Risk) string {
	if len(risks) == 0 {
		return "可直接执行迁移"
	}
	var suggestions []string
	for _, risk := range risks {
		suggestions = append(suggestions, risk.Suggestion)
	}
	return strings.Join(suggestions, "; ")
}

func generateFunctionSuggestions(risks []Risk) string {
	if len(risks) == 0 {
		return "可直接执行迁移"
	}
	var suggestions []string
	for _, risk := range risks {
		suggestions = append(suggestions, risk.Suggestion)
	}
	return strings.Join(suggestions, "; ")
}

func generateUserSuggestions(risks []Risk) string {
	if len(risks) == 0 {
		return "可直接执行迁移"
	}
	var suggestions []string
	for _, risk := range risks {
		suggestions = append(suggestions, risk.Suggestion)
	}
	return strings.Join(suggestions, "; ")
}

func generatePrivilegeSuggestions(risks []Risk) string {
	if len(risks) == 0 {
		return "可直接执行迁移"
	}
	var suggestions []string
	for _, risk := range risks {
		suggestions = append(suggestions, risk.Suggestion)
	}
	return strings.Join(suggestions, "; ")
}

func generateHighRiskSummary(report *AssessmentReport) []HighRiskItem {
	var highRisks []HighRiskItem
	id := 1

	// 收集表高风险
	for _, table := range report.Tables {
		for _, risk := range table.Risks {
			if risk.Level == RiskLevelHigh {
				highRisks = append(highRisks, HighRiskItem{
					ID:         id,
					ObjectType: "表",
					ObjectName: table.Name,
					RiskDesc:   fmt.Sprintf("%s: %s", risk.Type, risk.Description),
					Suggestion: risk.Suggestion,
				})
				id++
			}
		}
	}

	// 收集视图高风险
	for _, view := range report.Views {
		for _, risk := range view.Risks {
			if risk.Level == RiskLevelHigh {
				highRisks = append(highRisks, HighRiskItem{
					ID:         id,
					ObjectType: "视图",
					ObjectName: view.Name,
					RiskDesc:   fmt.Sprintf("%s: %s", risk.Type, risk.Description),
					Suggestion: risk.Suggestion,
				})
				id++
			}
		}
	}

	// 收集函数高风险
	for _, fn := range report.Functions {
		for _, risk := range fn.Risks {
			if risk.Level == RiskLevelHigh {
				highRisks = append(highRisks, HighRiskItem{
					ID:         id,
					ObjectType: "函数",
					ObjectName: fn.Name,
					RiskDesc:   fmt.Sprintf("%s: %s", risk.Type, risk.Description),
					Suggestion: risk.Suggestion,
				})
				id++
			}
		}
	}

	return highRisks
}

// checkTableConversionWarnings 检查表转换结果中的警告
func checkTableConversionWarnings(result *postgres.ConvertTableDDLResult, originalDDL string) []Risk {
	var risks []Risk
	
	// 检查是否有未支持的类型或特性
	if result == nil {
		return risks
	}
	
	// 这里可以根据 ConvertTableDDLResult 的具体字段添加更详细的检查
	// 目前 ConvertTableDDLResult 主要包含转换后的 DDL，没有额外的警告信息
	
	return risks
}

// getFunctionDDL 从 map 中获取函数 DDL
func getFunctionDDL(funcMap map[string]mysql.FunctionInfo, functionName string) string {
	if fn, ok := funcMap[functionName]; ok {
		return fn.DDL
	}
	return ""
}
