package assessor

// AssessmentReport 评估报告总结构
type AssessmentReport struct {
	SourceDB    SourceDBInfo      // 源数据库信息
	TargetDB    TargetDBInfo      // 目标数据库信息
	Summary     SummaryStats      // 总体统计
	Tables      []TableDetail     // 表详细清单
	Views       []ViewDetail      // 视图详细清单
	Indexes     []IndexDetail     // 索引详细清单
	Functions   []FunctionDetail  // 函数详细清单
	Users       []UserDetail      // 用户详细清单
	Privileges  []PrivilegeDetail // 表权限详细清单
	HighRisks   []HighRiskItem    // 高风险对象汇总
	Suggestions ConfigSuggestion  // 配置建议
}

// SourceDBInfo 源数据库信息
type SourceDBInfo struct {
	Type      string `json:"type"`      // 数据库类型：MySQL
	Version   string `json:"version"`   // 版本号
	Host      string `json:"host"`      // 主机地址
	Port      int    `json:"port"`      // 端口
	Database  string `json:"database"`  // 数据库名
	Charset   string `json:"charset"`   // 字符集
	Collation string `json:"collation"` // 排序规则
}

// TargetDBInfo 目标数据库信息
type TargetDBInfo struct {
	Type     string `json:"type"`     // 数据库类型：PostgreSQL
	Version  string `json:"version"`  // 版本号
	Host     string `json:"host"`     // 主机地址
	Port     int    `json:"port"`     // 端口
	Database string `json:"database"` // 数据库名
	Charset  string `json:"charset"`  // 字符集
}

// SummaryStats 总体统计
type SummaryStats struct {
	Score           int    `json:"score"`            // 总体评分 (0-100)
	RiskLevel       string `json:"risk_level"`       // 风险等级：低/中/高
	TotalTables     int    `json:"total_tables"`     // 表总数
	TotalViews      int    `json:"total_views"`      // 视图总数
	TotalIndexes    int    `json:"total_indexes"`    // 索引总数
	TotalFunctions  int    `json:"total_functions"`  // 函数总数
	TotalUsers      int    `json:"total_users"`      // 用户总数
	TotalPrivileges int    `json:"total_privileges"` // 表权限总数

	// 行数统计
	TotalTableRows    int64 `json:"total_table_rows"`    // 表总行数
	TotalViewRows     int64 `json:"total_view_rows"`     // 视图总行数（定义行数）
	TotalIndexRows    int64 `json:"total_index_rows"`    // 索引总行数（定义行数）
	TotalFunctionRows int64 `json:"total_function_rows"` // 函数总行数

	// DDL 行数统计
	TotalTableDDLRows    int `json:"total_table_ddl_rows"`    // 表 DDL 总行数
	TotalViewDDLRows     int `json:"total_view_ddl_rows"`     // 视图 DDL 总行数
	TotalFunctionDDLRows int `json:"total_function_ddl_rows"` // 函数 DDL 总行数
	TotalIndexDDLRows    int `json:"total_index_ddl_rows"`    // 索引 DDL 总行数
}

// TableDetail 表详细清单
type TableDetail struct {
	ID          int    `json:"id"`          // 序号
	Name        string `json:"name"`        // 表名
	RowCount    int64  `json:"row_count"`   // 行数
	DDLRows     int    `json:"ddl_rows"`    // DDL 行数
	RiskLevel   string `json:"risk_level"`  // 风险等级：无风险/中风险/高风险
	Risks       []Risk `json:"risks"`       // 风险列表
	Suggestions string `json:"suggestions"` // 操作建议
}

// ViewDetail 视图详细清单
type ViewDetail struct {
	ID          int    `json:"id"`          // 序号
	Name        string `json:"name"`        // 视图名
	DDLRows     int    `json:"ddl_rows"`    // 定义行数
	RiskLevel   string `json:"risk_level"`  // 风险等级：无风险/中风险/高风险
	Risks       []Risk `json:"risks"`       // 风险列表
	Suggestions string `json:"suggestions"` // 操作建议
}

// IndexDetail 索引详细清单
type IndexDetail struct {
	ID          int    `json:"id"`          // 序号
	Name        string `json:"name"`        // 索引名
	TableName   string `json:"table_name"`  // 所属表名
	DDLRows     int    `json:"ddl_rows"`    // 定义行数
	RiskLevel   string `json:"risk_level"`  // 风险等级：无风险/中风险/高风险
	Risks       []Risk `json:"risks"`       // 风险列表
	Suggestions string `json:"suggestions"` // 操作建议
}

// FunctionDetail 函数详细清单
type FunctionDetail struct {
	ID          int    `json:"id"`          // 序号
	Name        string `json:"name"`        // 函数名
	Parameters  string `json:"parameters"`  // 参数列表
	DDLRows     int    `json:"ddl_rows"`    // 定义行数
	RiskLevel   string `json:"risk_level"`  // 风险等级：无风险/中风险/高风险
	Risks       []Risk `json:"risks"`       // 风险列表
	Suggestions string `json:"suggestions"` // 操作建议
}

// UserDetail 用户详细清单
type UserDetail struct {
	ID          int    `json:"id"`          // 序号
	Name        string `json:"name"`        // 用户名
	Host        string `json:"host"`        // 主机
	RiskLevel   string `json:"risk_level"`  // 风险等级：无风险/中风险/高风险
	Risks       []Risk `json:"risks"`       // 风险列表
	Suggestions string `json:"suggestions"` // 操作建议
}

// PrivilegeDetail 表权限详细清单
type PrivilegeDetail struct {
	ID          int    `json:"id"`          // 序号
	UserName    string `json:"user_name"`   // 用户名
	TableName   string `json:"table_name"`  // 表名
	Privileges  string `json:"privileges"`  // 权限列表
	RiskLevel   string `json:"risk_level"`  // 风险等级：无风险/中风险/高风险
	Risks       []Risk `json:"risks"`       // 风险列表
	Suggestions string `json:"suggestions"` // 操作建议
}

// Risk 风险定义
type Risk struct {
	Level       string `json:"level"`       // 风险等级：高/中/低
	Type        string `json:"type"`        // 风险类型
	Description string `json:"description"` // 风险描述
	Suggestion  string `json:"suggestion"`  // 处理建议
}

// HighRiskItem 高风险对象汇总
type HighRiskItem struct {
	ID         int    `json:"id"`          // 序号
	ObjectType string `json:"object_type"` // 对象类型：表/视图/索引/函数/用户/权限
	ObjectName string `json:"object_name"` // 对象名称
	RiskDesc   string `json:"risk_desc"`   // 风险描述
	Suggestion string `json:"suggestion"`  // 处理建议
}

// ConfigSuggestion 配置建议
type ConfigSuggestion struct {
	EstimatedTime         string `json:"estimated_time"`          // 预计迁移时间
	EstimatedDataSize     string `json:"estimated_data_size"`     // 预计数据量
	RecommendedConfig     string `json:"recommended_config"`      // 推荐配置（YAML 格式）
	RecommendedConcurrency   int `json:"recommended_concurrency"`   // 推荐并发数
	RecommendedBatchSize     int `json:"recommended_batch_size"`    // 推荐批处理大小
	RecommendedBandwidth     int `json:"recommended_bandwidth"`     // 推荐带宽限制（Mbps）
}

// 风险等级常量
const (
	RiskLevelNone   = "无风险"
	RiskLevelLow    = "低风险"
	RiskLevelMedium = "中风险"
	RiskLevelHigh   = "高风险"
)

// 风险类型常量
const (
	RiskTypeENUM          = "ENUM 类型"
	RiskTypeSET           = "SET 类型"
	RiskTypeMemoryEngine  = "MEMORY 引擎"
	RiskTypeMyISAMEngine  = "MyISAM 引擎"
	RiskTypeZeroDate      = "zero-date 数据"
	RiskTypePartition     = "分区表"
	RiskTypeForeignKey    = "外键循环依赖"
	RiskTypeGroupConcat   = "GROUP_CONCAT 函数"
	RiskTypeNow           = "NOW() 函数"
	RiskTypeIfNull        = "IFNULL() 函数"
	RiskTypeUnixTimestamp = "UNIX_TIMESTAMP() 函数"
	RiskTypeDeclare       = "DECLARE 语法"
	RiskTypeDateFormat    = "DATE_FORMAT() 函数"
	RiskTypeStrToDate     = "STR_TO_DATE() 函数"
	RiskTypeGetLock       = "GET_LOCK() 函数"
	RiskTypeWildcardHost  = "主机通配符"
	RiskTypeSuperUser     = "超级用户权限"
	RiskTypeEmptyPassword = "空密码"
	RiskTypeFullTextIndex = "全文索引"
	RiskTypeSpatialIndex  = "空间索引"
)
