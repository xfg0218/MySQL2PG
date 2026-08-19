package assessor

import (
	"context"
	"fmt"
	"strings"

	"github.com/yourusername/mysql2pg/internal/config"
	"github.com/yourusername/mysql2pg/internal/converter/postgres"
	"github.com/yourusername/mysql2pg/internal/mysql"
	pgconn "github.com/yourusername/mysql2pg/internal/postgres"
)

// MigrationAssessor 迁移评估器（使用现有迁移逻辑进行评估）
type MigrationAssessor struct {
	mysqlConn       *mysql.Connection
	postgresConn    *pgconn.Connection
	config          *config.Config
	report          *AssessmentReport
	mysqlVersion    *mysql.MySQLVersionInfo
	postgresVersion *pgconn.PostgreSQLVersionInfo
	conversionMgr   *postgres.Manager
	ctx             context.Context
}

// context 返回评估器持有的根 context
// 未通过 NewMigrationAssessor 设置时回退为 context.Background()，保证 nil 安全
func (a *MigrationAssessor) context() context.Context {
	if a.ctx == nil {
		return context.Background()
	}
	return a.ctx
}

// NewMigrationAssessor 创建迁移评估器
// ctx 为根 context（通常来自 signal.NotifyContext），用于取消控制
func NewMigrationAssessor(ctx context.Context, mysqlConn *mysql.Connection, postgresConn *pgconn.Connection, cfg *config.Config) (*MigrationAssessor, error) {
	// 获取 MySQL 版本信息
	mysqlVersion, _ := mysqlConn.GetVersionInfo()

	// 获取 PostgreSQL 版本信息
	pgVersion, _ := postgresConn.GetVersionInfo()

	return &MigrationAssessor{
		mysqlConn:       mysqlConn,
		postgresConn:    postgresConn,
		config:          cfg,
		report:          &AssessmentReport{},
		mysqlVersion:    mysqlVersion,
		postgresVersion: pgVersion,
		ctx:             ctx,
	}, nil
}

// Run 运行评估（使用现有迁移逻辑进行评估，不写入目标库）
func (a *MigrationAssessor) Run() (*AssessmentReport, error) {
	fmt.Println("开始迁移前评估（使用现有迁移逻辑，不写入目标库）...")

	// Step 1: 收集数据库信息
	fmt.Println("  [1/6] 收集数据库信息...")
	if err := a.collectDatabaseInfo(); err != nil {
		return nil, err
	}

	// Step 2: 使用现有转换逻辑评估表 DDL
	fmt.Println("  [2/6] 评估表 DDL 转换（使用 ConvertTableDDL）...")
	a.assessTablesWithMigrationLogic()

	// Step 3: 使用现有转换逻辑评估视图
	fmt.Println("  [3/6] 评估视图转换（使用 ConvertViewDDL）...")
	a.assessViewsWithMigrationLogic()

	// Step 4: 使用现有转换逻辑评估函数
	fmt.Println("  [4/6] 评估函数转换（使用 ConvertFunctionDDL）...")
	a.assessFunctionsWithMigrationLogic()

	// Step 5: 评估索引、用户、权限
	fmt.Println("  [5/6] 评估索引、用户、权限...")
	a.assessOtherObjects()

	// Step 6: 生成风险评估和建议
	fmt.Println("  [6/6] 生成风险评估和建议...")
	a.generateRiskAssessment()

	return a.report, nil
}

// collectDatabaseInfo 收集数据库信息
func (a *MigrationAssessor) collectDatabaseInfo() error {
	// 获取表信息
	tables, err := a.mysqlConn.GetTables(
		a.config.Conversion.Options.SkipUseTableList,
		a.config.Conversion.Options.SkipTableList,
		a.config.Conversion.Options.UseTableList,
		a.config.Conversion.Options.TableList,
	)
	if err != nil {
		return fmt.Errorf("获取表信息失败：%w", err)
	}

	// 获取视图信息
	views, err := a.mysqlConn.GetViews(a.config.MySQL.Database)
	if err != nil {
		return fmt.Errorf("获取视图信息失败：%w", err)
	}

	// 获取函数信息
	functions, err := a.mysqlConn.GetFunctions()
	if err != nil {
		return fmt.Errorf("获取函数信息失败：%w", err)
	}

	// 获取用户信息
	users, err := a.mysqlConn.GetUsers()
	if err != nil {
		return fmt.Errorf("获取用户信息失败：%w", err)
	}

	// 获取表权限信息
	privileges, err := a.mysqlConn.GetTablePrivileges()
	if err != nil {
		return fmt.Errorf("获取表权限信息失败：%w", err)
	}

	// 填充报告数据
	a.report.SourceDB = SourceDBInfo{
		Type:     "MySQL",
		Host:     a.config.MySQL.Host,
		Port:     a.config.MySQL.Port,
		Database: a.config.MySQL.Database,
	}
	a.report.TargetDB = TargetDBInfo{
		Type:     "PostgreSQL",
		Host:     a.config.PostgreSQL.Host,
		Port:     a.config.PostgreSQL.Port,
		Database: a.config.PostgreSQL.Database,
	}

	// 获取数据库版本信息
	mysqlVersion, err := a.mysqlConn.GetVersion()
	if err != nil {
		mysqlVersion = "Unknown"
	}
	a.report.SourceDB.Version = mysqlVersion

	pgVersion, err := a.postgresConn.GetVersion()
	if err != nil {
		pgVersion = "Unknown"
	}
	a.report.TargetDB.Version = pgVersion

	// 填充表信息
	for _, table := range tables {
		rowCount, _ := a.mysqlConn.GetTableRowCount(table.Name)
		// 获取表 DDL 并计算行数
		ddl, _ := a.mysqlConn.GetTableDDL(a.context(), table.Name)
		ddlRows := len(strings.Split(strings.TrimSpace(ddl), "\n"))

		a.report.Tables = append(a.report.Tables, TableDetail{
			Name:        table.Name,
			RowCount:    rowCount,
			DDLRows:     ddlRows,
			RiskLevel:   RiskLevelNone,
			Risks:       []Risk{},
			Suggestions: "",
		})
	}

	// 填充视图信息
	for _, view := range views {
		// 获取视图 DDL 并计算行数（使用 SHOW CREATE VIEW 获取格式化的定义）
		ddl, err := a.mysqlConn.GetViewDDL(view.ViewName)
		ddlRows := 1 // 默认值为 1
		if err == nil && ddl != "" {
			ddlRows = len(strings.Split(strings.TrimSpace(ddl), "\n"))
		}

		a.report.Views = append(a.report.Views, ViewDetail{
			Name:        view.ViewName,
			DDLRows:     ddlRows,
			RiskLevel:   RiskLevelNone,
			Risks:       []Risk{},
			Suggestions: "",
		})
	}

	// 填充函数信息
	for _, fn := range functions {
		// 获取函数 DDL 并计算行数
		ddlRows := len(strings.Split(strings.TrimSpace(fn.DDL), "\n"))

		a.report.Functions = append(a.report.Functions, FunctionDetail{
			Name:        fn.Name,
			DDLRows:     ddlRows,
			RiskLevel:   RiskLevelNone,
			Risks:       []Risk{},
			Suggestions: "",
		})
	}

	// 填充用户信息
	for _, user := range users {
		// 过滤掉 MySQL 系统用户，如 mysql.infoschema、mysql.session 等
		// 这些用户是 MySQL 内部使用的，不需要迁移到 PostgreSQL
		userParts := strings.Split(user.Name, "@")
		if len(userParts) > 0 && strings.HasPrefix(userParts[0], "mysql.") {
			continue
		}

		// 提取用户名（不包含@host 部分）
		userName := user.Name
		if idx := strings.Index(user.Name, "@"); idx != -1 {
			userName = user.Name[:idx]
		}

		a.report.Users = append(a.report.Users, UserDetail{
			Name:        userName,
			Host:        "",
			RiskLevel:   RiskLevelNone,
			Risks:       []Risk{},
			Suggestions: "",
		})
	}

	// 填充权限信息
	for _, priv := range privileges {
		a.report.Privileges = append(a.report.Privileges, PrivilegeDetail{
			UserName:   priv.User,  // 只使用用户名，不包含@host
			TableName:  priv.TableName,
			Privileges: priv.TablePriv,
			RiskLevel:  RiskLevelNone,
			Risks:      []Risk{},
		})
	}

	// 获取索引信息
	indexes, err := a.mysqlConn.GetAllIndexes()
	if err != nil {
		return fmt.Errorf("获取索引信息失败：%w", err)
	}

	// 填充索引信息
	for _, idx := range indexes {
		// 获取索引 DDL 并计算行数
		ddl := fmt.Sprintf("CREATE INDEX %s ON %s (%s)", idx.Name, idx.Table, strings.Join(idx.Columns, ", "))
		if idx.IsUnique {
			ddl = fmt.Sprintf("CREATE UNIQUE INDEX %s ON %s (%s)", idx.Name, idx.Table, strings.Join(idx.Columns, ", "))
		}
		ddlRows := len(strings.Split(strings.TrimSpace(ddl), "\n"))

		a.report.Indexes = append(a.report.Indexes, IndexDetail{
			Name:        idx.Name,
			TableName:   idx.Table,
			DDLRows:     ddlRows,
			RiskLevel:   RiskLevelNone,
			Risks:       []Risk{},
			Suggestions: "",
		})
	}

	a.report.Summary.TotalTables = len(tables)
	a.report.Summary.TotalViews = len(views)
	a.report.Summary.TotalFunctions = len(functions)
	a.report.Summary.TotalUsers = len(users)
	a.report.Summary.TotalPrivileges = len(privileges)
	a.report.Summary.TotalIndexes = len(indexes)

	return nil
}

// assessTablesWithMigrationLogic 使用现有迁移逻辑评估表 DDL
func (a *MigrationAssessor) assessTablesWithMigrationLogic() {
	for i := range a.report.Tables {
		table := &a.report.Tables[i]

		// 获取表的 DDL
		ddl, err := a.mysqlConn.GetTableDDL(a.context(), table.Name)
		if err != nil {
			table.Risks = append(table.Risks, Risk{
				Level:       RiskLevelHigh,
				Type:        "DDL 获取失败",
				Description: fmt.Sprintf("无法获取表 DDL: %v", err),
				Suggestion:  "检查表是否存在",
			})
			table.RiskLevel = CalculateRiskLevel(table.Risks)
			continue
		}

		// 使用现有 ConvertTableDDL 函数进行转换评估
		_, err = postgres.ConvertTableDDL(ddl, a.config.Conversion.Options.LowercaseColumns)
		if err != nil {
			table.Risks = append(table.Risks, Risk{
				Level:       RiskLevelHigh,
				Type:        "DDL 转换失败",
				Description: fmt.Sprintf("无法转换为 PostgreSQL DDL: %v", err),
				Suggestion:  "需要手动检查和修改表结构",
			})
			table.RiskLevel = CalculateRiskLevel(table.Risks)
		}
	}
}

// assessViewsWithMigrationLogic 使用现有迁移逻辑评估视图
func (a *MigrationAssessor) assessViewsWithMigrationLogic() {
	views, _ := a.mysqlConn.GetViews(a.config.MySQL.Database)
	viewDefMap := make(map[string]string)
	for _, v := range views {
		viewDefMap[v.ViewName] = v.ViewDefinition
	}

	for i := range a.report.Views {
		view := &a.report.Views[i]
		viewDef := viewDefMap[view.Name]

		// 使用现有 ConvertViewDDL 函数进行转换评估
		_, err := postgres.ConvertViewDDL(view.Name, viewDef)
		if err != nil {
			view.Risks = append(view.Risks, Risk{
				Level:       RiskLevelHigh,
				Type:        "视图转换失败",
				Description: fmt.Sprintf("无法转换为 PostgreSQL 视图：%v", err),
				Suggestion:  "需要手动修改视图定义",
			})
			view.RiskLevel = CalculateRiskLevel(view.Risks)
		}
	}
}

// assessFunctionsWithMigrationLogic 使用现有迁移逻辑评估函数
func (a *MigrationAssessor) assessFunctionsWithMigrationLogic() {
	functions, _ := a.mysqlConn.GetFunctions()
	funcDDLMap := make(map[string]mysql.FunctionInfo)
	for _, fn := range functions {
		funcDDLMap[fn.Name] = fn
	}

	for i := range a.report.Functions {
		fn := &a.report.Functions[i]
		if fnInfo, ok := funcDDLMap[fn.Name]; ok {
			// 使用现有 ConvertFunctionDDL 函数进行转换评估
			_, err := postgres.ConvertFunctionDDL(fnInfo)
			if err != nil {
				fn.Risks = append(fn.Risks, Risk{
					Level:       RiskLevelHigh,
					Type:        "函数转换失败",
					Description: fmt.Sprintf("无法转换为 PostgreSQL 函数：%v", err),
					Suggestion:  "需要手动修改函数定义",
				})
				fn.RiskLevel = CalculateRiskLevel(fn.Risks)
			}
		}
	}
}

// assessOtherObjects 评估索引、用户、权限
func (a *MigrationAssessor) assessOtherObjects() {
	// 索引、用户、权限的风险评估已在收集信息时完成，此处不再额外检查
	// 评估模式只使用转换函数判断兼容性，不添加额外的风险检查
}

// generateRiskAssessment 生成风险评估
func (a *MigrationAssessor) generateRiskAssessment() {
	// 计算总体评分
	a.calculateScore()

	// 收集高风险对象
	a.collectHighRisks()

	// 计算 DDL 行数统计
	a.calculateDDLRows()

	// 生成建议
	a.generateSuggestions()
}

// calculateDDLRows 计算 DDL 行数统计
func (a *MigrationAssessor) calculateDDLRows() {
	// 计算表 DDL 总行数
	for _, table := range a.report.Tables {
		a.report.Summary.TotalTableDDLRows += table.DDLRows
	}

	// 计算视图 DDL 总行数
	for _, view := range a.report.Views {
		a.report.Summary.TotalViewDDLRows += view.DDLRows
	}

	// 计算函数 DDL 总行数
	for _, fn := range a.report.Functions {
		a.report.Summary.TotalFunctionDDLRows += fn.DDLRows
	}

	// 计算索引 DDL 总行数
	for _, idx := range a.report.Indexes {
		a.report.Summary.TotalIndexDDLRows += idx.DDLRows
	}
}

// calculateScore 计算总体评分
func (a *MigrationAssessor) calculateScore() {
	totalObjects := len(a.report.Tables) + len(a.report.Views) + len(a.report.Functions)
	if totalObjects == 0 {
		a.report.Summary.Score = 100
		return
	}

	highRiskCount := 0
	for _, table := range a.report.Tables {
		if table.RiskLevel == RiskLevelHigh {
			highRiskCount++
		}
	}
	for _, view := range a.report.Views {
		if view.RiskLevel == RiskLevelHigh {
			highRiskCount++
		}
	}
	for _, fn := range a.report.Functions {
		if fn.RiskLevel == RiskLevelHigh {
			highRiskCount++
		}
	}

	score := float64(totalObjects-highRiskCount) / float64(totalObjects) * 100
	a.report.Summary.Score = int(score)

	// 设置风险等级
	if a.report.Summary.Score >= 80 {
		a.report.Summary.RiskLevel = "低"
	} else if a.report.Summary.Score >= 60 {
		a.report.Summary.RiskLevel = "中"
	} else {
		a.report.Summary.RiskLevel = "高"
	}
}

// collectHighRisks 收集高风险对象
func (a *MigrationAssessor) collectHighRisks() {
	id := 1

	for _, table := range a.report.Tables {
		if table.RiskLevel == RiskLevelHigh {
			for _, risk := range table.Risks {
				if risk.Level == RiskLevelHigh {
					a.report.HighRisks = append(a.report.HighRisks, HighRiskItem{
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
	}

	for _, view := range a.report.Views {
		if view.RiskLevel == RiskLevelHigh {
			for _, risk := range view.Risks {
				if risk.Level == RiskLevelHigh {
					a.report.HighRisks = append(a.report.HighRisks, HighRiskItem{
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
	}

	for _, fn := range a.report.Functions {
		if fn.RiskLevel == RiskLevelHigh {
			for _, risk := range fn.Risks {
				if risk.Level == RiskLevelHigh {
					a.report.HighRisks = append(a.report.HighRisks, HighRiskItem{
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
	}
}

// generateSuggestions 生成建议
func (a *MigrationAssessor) generateSuggestions() {
	// 预计迁移时间（简单估算：每 1000 行 1 秒）
	totalRows := int64(0)
	for _, table := range a.report.Tables {
		totalRows += table.RowCount
	}

	if totalRows < 1000 {
		a.report.Suggestions.EstimatedTime = "< 1 秒"
	} else if totalRows < 60000 {
		a.report.Suggestions.EstimatedTime = fmt.Sprintf("%d 秒", totalRows/1000)
	} else if totalRows < 3600000 {
		a.report.Suggestions.EstimatedTime = fmt.Sprintf("%d 分钟", totalRows/60000)
	} else {
		a.report.Suggestions.EstimatedTime = fmt.Sprintf("%d 小时", totalRows/3600000)
	}

	// 预计数据量
	if totalRows < 10000 {
		a.report.Suggestions.EstimatedDataSize = fmt.Sprintf("%d 行", totalRows)
	} else if totalRows < 1000000 {
		a.report.Suggestions.EstimatedDataSize = fmt.Sprintf("%.1f 万行", float64(totalRows)/10000)
	} else {
		a.report.Suggestions.EstimatedDataSize = fmt.Sprintf("%.1f 百万行", float64(totalRows)/1000000)
	}

	// 推荐配置
	concurrency := a.config.Conversion.Limits.Concurrency
	batchSize := a.config.Conversion.Limits.MaxRowsPerBatch
	bandwidth := a.config.Conversion.Limits.BandwidthMbps

	a.report.Suggestions.RecommendedConfig = fmt.Sprintf(
		`conversion:
  limits:
    concurrency: %d        # 当前：%d
    max_rows_per_batch: %d  # 当前：%d
    batch_insert_size: %d   # 当前：%d`,
		concurrency, concurrency,
		batchSize, batchSize,
		batchSize, batchSize,
	)

	// 设置推荐配置字段
	a.report.Suggestions.RecommendedConcurrency = concurrency
	a.report.Suggestions.RecommendedBatchSize = batchSize
	a.report.Suggestions.RecommendedBandwidth = bandwidth
}
