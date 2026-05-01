package assessor

import (
	"regexp"
	"strings"
)

// RiskChecker 风险检查器
type RiskChecker struct {
	// 风险规则映射：风险类型 -> 检查函数
	rules map[string]func(interface{}) []Risk
}

// NewRiskChecker 创建风险检查器
func NewRiskChecker() *RiskChecker {
	return &RiskChecker{
		rules: make(map[string]func(interface{}) []Risk),
	}
}

// CheckTableRisks 检查表风险
func CheckTableRisks(ddl string, rowCount int64) []Risk {
	var risks []Risk

	// 检查 ENUM 类型
	if regexp.MustCompile(`(?i)\bENUM\s*\(`).MatchString(ddl) {
		risks = append(risks, Risk{
			Level:       RiskLevelHigh,
			Type:        RiskTypeENUM,
			Description: "MySQL ENUM 类型在 PostgreSQL 中兼容性较差",
			Suggestion:  "建议改为 VARCHAR(n)，n 为 ENUM 最大长度",
		})
	}

	// 检查 SET 类型
	if regexp.MustCompile(`(?i)\bSET\s*\(`).MatchString(ddl) {
		risks = append(risks, Risk{
			Level:       RiskLevelHigh,
			Type:        RiskTypeSET,
			Description: "MySQL SET 类型在 PostgreSQL 中无直接对应类型",
			Suggestion:  "建议改为 VARCHAR(255) 或使用关联表",
		})
	}

	// 检查 MEMORY 引擎
	if regexp.MustCompile(`(?i)ENGINE\s*=\s*MEMORY`).MatchString(ddl) {
		risks = append(risks, Risk{
			Level:       RiskLevelMedium,
			Type:        RiskTypeMemoryEngine,
			Description: "MEMORY 引擎数据存储在内存中，PostgreSQL 无直接对应",
			Suggestion:  "建议改为 InnoDB 引擎后迁移",
		})
	}

	// 检查 MyISAM 引擎
	if regexp.MustCompile(`(?i)ENGINE\s*=\s*MYISAM`).MatchString(ddl) {
		risks = append(risks, Risk{
			Level:       RiskLevelMedium,
			Type:        RiskTypeMyISAMEngine,
			Description: "MyISAM 引擎不支持事务和外键，PostgreSQL 默认使用 InnoDB 兼容模式",
			Suggestion:  "建议改为 InnoDB 引擎后迁移",
		})
	}

	// 检查分区表
	if regexp.MustCompile(`(?i)\bPARTITION\s+BY\b`).MatchString(ddl) {
		risks = append(risks, Risk{
			Level:       RiskLevelMedium,
			Type:        RiskTypePartition,
			Description: "分区表语法在 MySQL 和 PostgreSQL 之间有差异",
			Suggestion:  "需要手动验证 PostgreSQL 分区兼容性",
		})
	}

	// 检查 zero-date 数据（通过 DDL 中的 DEFAULT 判断）
	if regexp.MustCompile(`(?i)DEFAULT\s*['\"]?0000-00-00`).MatchString(ddl) {
		risks = append(risks, Risk{
			Level:       RiskLevelHigh,
			Type:        RiskTypeZeroDate,
			Description: "zero-date (0000-00-00) 在 PostgreSQL 中不支持",
			Suggestion:  "建议清理数据或使用 NULL/有效日期",
		})
	}

	return risks
}

// CheckViewRisks 检查视图风险
func CheckViewRisks(viewDefinition string) []Risk {
	var risks []Risk
	defUpper := strings.ToUpper(viewDefinition)

	// 检查 GROUP_CONCAT
	if strings.Contains(defUpper, "GROUP_CONCAT") {
		risks = append(risks, Risk{
			Level:       RiskLevelHigh,
			Type:        RiskTypeGroupConcat,
			Description: "GROUP_CONCAT 是 MySQL 特有函数",
			Suggestion:  "建议改为 PostgreSQL 的 STRING_AGG 或 ARRAY_AGG",
		})
	}

	// 检查 NOW()
	if regexp.MustCompile(`\bNOW\s*\(\s*\)`).MatchString(viewDefinition) {
		risks = append(risks, Risk{
			Level:       RiskLevelHigh,
			Type:        RiskTypeNow,
			Description: "NOW() 函数在 PostgreSQL 中应使用 CURRENT_TIMESTAMP",
			Suggestion:  "建议改为 CURRENT_TIMESTAMP",
		})
	}

	// 检查 IFNULL()
	if regexp.MustCompile(`\bIFNULL\s*\(`).MatchString(viewDefinition) {
		risks = append(risks, Risk{
			Level:       RiskLevelMedium,
			Type:        RiskTypeIfNull,
			Description: "IFNULL() 是 MySQL 特有函数",
			Suggestion:  "建议改为 COALESCE()",
		})
	}

	// 检查 UNIX_TIMESTAMP()
	if regexp.MustCompile(`\bUNIX_TIMESTAMP\s*\(`).MatchString(viewDefinition) {
		risks = append(risks, Risk{
			Level:       RiskLevelHigh,
			Type:        RiskTypeUnixTimestamp,
			Description: "UNIX_TIMESTAMP() 是 MySQL 特有函数",
			Suggestion:  "建议改为 EXTRACT(EPOCH FROM timestamp)",
		})
	}

	// 检查 DATE_FORMAT()
	if regexp.MustCompile(`\bDATE_FORMAT\s*\(`).MatchString(viewDefinition) {
		risks = append(risks, Risk{
			Level:       RiskLevelHigh,
			Type:        RiskTypeDateFormat,
			Description: "DATE_FORMAT() 是 MySQL 特有函数",
			Suggestion:  "建议改为 TO_CHAR()",
		})
	}

	// 检查 STR_TO_DATE()
	if regexp.MustCompile(`\bSTR_TO_DATE\s*\(`).MatchString(viewDefinition) {
		risks = append(risks, Risk{
			Level:       RiskLevelHigh,
			Type:        RiskTypeStrToDate,
			Description: "STR_TO_DATE() 是 MySQL 特有函数",
			Suggestion:  "建议改为 TO_DATE() 或 TO_TIMESTAMP()",
		})
	}

	return risks
}

// CheckIndexRisks 检查索引风险
func CheckIndexRisks(indexType string, isFullText bool, isSpatial bool) []Risk {
	var risks []Risk

	// 检查全文索引
	if isFullText {
		risks = append(risks, Risk{
			Level:       RiskLevelMedium,
			Type:        RiskTypeFullTextIndex,
			Description: "全文索引在 PostgreSQL 中使用 GIN 索引和 tsvector",
			Suggestion:  "需要手动验证全文搜索功能",
		})
	}

	// 检查空间索引
	if isSpatial {
		risks = append(risks, Risk{
			Level:       RiskLevelMedium,
			Type:        RiskTypeSpatialIndex,
			Description: "空间索引在 PostgreSQL 中需要 PostGIS 扩展",
			Suggestion:  "确保已安装 PostGIS 扩展并验证兼容性",
		})
	}

	return risks
}

// CheckFunctionRisks 检查函数风险
func CheckFunctionRisks(functionDDL string) []Risk {
	var risks []Risk

	// 检查 DECLARE 语法（存储过程风格）
	if regexp.MustCompile(`(?i)\bDECLARE\s+\w+`).MatchString(functionDDL) {
		risks = append(risks, Risk{
			Level:       RiskLevelHigh,
			Type:        RiskTypeDeclare,
			Description: "DECLARE 语法需要转换为 PL/pgSQL 语法",
			Suggestion:  "建议改写为 PostgreSQL 的 PL/pgSQL 函数",
		})
	}

	// 检查 DATE_FORMAT()
	if regexp.MustCompile(`\bDATE_FORMAT\s*\(`).MatchString(functionDDL) {
		risks = append(risks, Risk{
			Level:       RiskLevelHigh,
			Type:        RiskTypeDateFormat,
			Description: "DATE_FORMAT() 是 MySQL 特有函数",
			Suggestion:  "建议改为 TO_CHAR()",
		})
	}

	// 检查 STR_TO_DATE()
	if regexp.MustCompile(`\bSTR_TO_DATE\s*\(`).MatchString(functionDDL) {
		risks = append(risks, Risk{
			Level:       RiskLevelHigh,
			Type:        RiskTypeStrToDate,
			Description: "STR_TO_DATE() 是 MySQL 特有函数",
			Suggestion:  "建议改为 TO_DATE() 或 TO_TIMESTAMP()",
		})
	}

	// 检查 GET_LOCK()
	if regexp.MustCompile(`\bGET_LOCK\s*\(`).MatchString(functionDDL) {
		risks = append(risks, Risk{
			Level:       RiskLevelHigh,
			Type:        RiskTypeGetLock,
			Description: "GET_LOCK() 是 MySQL 特有函数",
			Suggestion:  "建议改为 pg_advisory_lock()",
		})
	}

	// 检查 IFNULL()
	if regexp.MustCompile(`\bIFNULL\s*\(`).MatchString(functionDDL) {
		risks = append(risks, Risk{
			Level:       RiskLevelMedium,
			Type:        RiskTypeIfNull,
			Description: "IFNULL() 是 MySQL 特有函数",
			Suggestion:  "建议改为 COALESCE()",
		})
	}

	return risks
}

// CheckUserRisks 检查用户风险
func CheckUserRisks(host string, isSuperUser bool, hasEmptyPassword bool) []Risk {
	var risks []Risk

	// 检查主机通配符
	if host == "%" || strings.HasPrefix(host, "%") || strings.HasSuffix(host, ".%") {
		risks = append(risks, Risk{
			Level:       RiskLevelMedium,
			Type:        RiskTypeWildcardHost,
			Description: "使用通配符主机可能带来安全风险",
			Suggestion:  "建议限制为具体 IP 地址或 IP 段",
		})
	}

	// 检查超级用户权限
	if isSuperUser {
		risks = append(risks, Risk{
			Level:       RiskLevelMedium,
			Type:        RiskTypeSuperUser,
			Description: "超级用户权限过大，不符合最小权限原则",
			Suggestion:  "建议根据实际需求分配最小必要权限",
		})
	}

	// 检查空密码
	if hasEmptyPassword {
		risks = append(risks, Risk{
			Level:       RiskLevelHigh,
			Type:        RiskTypeEmptyPassword,
			Description: "空密码存在严重安全隐患",
			Suggestion:  "必须设置强密码",
		})
	}

	return risks
}

// CheckPrivilegeRisks 检查表权限风险
func CheckPrivilegeRisks(host string, privileges string) []Risk {
	var risks []Risk

	// 检查主机通配符
	if host == "%" || strings.Contains(host, "%") {
		risks = append(risks, Risk{
			Level:       RiskLevelMedium,
			Type:        RiskTypeWildcardHost,
			Description: "使用通配符主机可能带来安全风险",
			Suggestion:  "建议限制为具体 IP 地址或 IP 段",
		})
	}

	// 检查过度权限
	privUpper := strings.ToUpper(privileges)
	if strings.Contains(privUpper, "ALL") || strings.Contains(privUpper, "GRANT") {
		risks = append(risks, Risk{
			Level:       RiskLevelMedium,
			Description: "ALL 或 GRANT 权限过大，不符合最小权限原则",
			Type:        "过度权限",
			Suggestion:  "建议根据实际需求分配 SELECT/INSERT/UPDATE/DELETE 等具体权限",
		})
	}

	return risks
}

// CalculateRiskLevel 根据风险列表计算总体风险等级
func CalculateRiskLevel(risks []Risk) string {
	hasHigh := false
	hasMedium := false

	for _, risk := range risks {
		if risk.Level == RiskLevelHigh {
			hasHigh = true
		}
		if risk.Level == RiskLevelMedium {
			hasMedium = true
		}
	}

	if hasHigh {
		return RiskLevelHigh
	}
	if hasMedium {
		return RiskLevelMedium
	}
	return RiskLevelNone
}

// CalculateOverallScore 计算总体评分 (0-100)
func CalculateOverallScore(report *AssessmentReport) int {
	score := 100

	// 高风险每个扣 5 分
	highRiskCount := len(report.HighRisks)
	score -= highRiskCount * 5

	// 中风险每个扣 2 分
	mediumRiskCount := 0
	for _, table := range report.Tables {
		for _, risk := range table.Risks {
			if risk.Level == RiskLevelMedium {
				mediumRiskCount++
			}
		}
	}
	score -= mediumRiskCount * 2

	// 确保分数在 0-100 之间
	if score < 0 {
		score = 0
	}
	if score > 100 {
		score = 100
	}

	return score
}

// CalculateOverallRiskLevel 计算总体风险等级
func CalculateOverallRiskLevel(report *AssessmentReport) string {
	score := CalculateOverallScore(report)

	if score >= 80 {
		return "低"
	} else if score >= 60 {
		return "中"
	}
	return "高"
}
