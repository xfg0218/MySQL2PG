package postgres

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/yourusername/mysql2pg/internal/config"
	"github.com/yourusername/mysql2pg/internal/mysql"
	pgconn "github.com/yourusername/mysql2pg/internal/postgres"
)

var (
	// reAutoIncrementOption 匹配 MySQL DDL 尾部的表级 AUTO_INCREMENT=N 选项
	// 该值为 MySQL 的下一个插入值（当前自增计数器）
	reAutoIncrementOption = regexp.MustCompile(`(?i)\bAUTO_INCREMENT=(\d+)`)

	// reAutoIncrementColumn 匹配 MySQL DDL 中含 AUTO_INCREMENT 的列定义并捕获列名
	// SHOW CREATE TABLE 每列一行，列名带反引号；表级选项行以 ) 开头，不会被误匹配
	reAutoIncrementColumn = regexp.MustCompile("(?im)^\\s*`([^`]+)`\\s+[^,\\n]*\\bAUTO_INCREMENT\\b")

	// reAutoIncrementColumnPlain 匹配列名不带反引号的形式（兜底）
	reAutoIncrementColumnPlain = regexp.MustCompile("(?im)^\\s*([A-Za-z_$][A-Za-z0-9_$]*)\\s+[^,\\n]*\\bAUTO_INCREMENT\\b")
)

// ParseAutoIncrementInfo 从 MySQL DDL 中解析自增列名与表级 AUTO_INCREMENT 起始值
// 返回：列名（保留原始大小写）、起始值（未指定时为 0）、是否存在自增列
// MySQL 每张表最多一个自增列
func ParseAutoIncrementInfo(mysqlDDL string) (columnName string, startValue int64, ok bool) {
	if m := reAutoIncrementOption.FindStringSubmatch(mysqlDDL); m != nil {
		if v, err := strconv.ParseInt(m[1], 10, 64); err == nil {
			startValue = v
		}
	}

	if m := reAutoIncrementColumn.FindStringSubmatch(mysqlDDL); m != nil {
		return m[1], startValue, true
	}
	if m := reAutoIncrementColumnPlain.FindStringSubmatch(mysqlDDL); m != nil {
		return m[1], startValue, true
	}
	return "", startValue, false
}

// backfillAutoIncrementSequence 回填表自增列的 PG 序列（失败仅告警，不中断同步）
// CopyFrom 显式写入 id 值不会推进 SERIAL 序列，若不回填，
// 迁移完成后业务首次隐式插入会因序列仍从 1 开始而触发主键冲突
// 回填值为 GREATEST(表内当前最大值+1, MySQL 的 AUTO_INCREMENT 起始值)
func backfillAutoIncrementSequence(postgresConn *pgconn.Connection, config *config.Config, tableName, mysqlDDL string, log func(format string, args ...interface{}), logError func(errMsg string, args ...interface{})) {
	colName, startValue, ok := ParseAutoIncrementInfo(mysqlDDL)
	if !ok {
		return
	}
	if config.Conversion.Options.LowercaseColumns {
		colName = strings.ToLower(colName)
	}

	found, err := postgresConn.SyncAutoIncrementSequence(tableName, colName, startValue)
	if err != nil {
		logError(fmt.Sprintf("警告: 表 %s 自增列 %s 序列回填失败，数据已同步但序列可能未对齐，请手动执行 setval 修复: %v", tableName, colName, err))
		return
	}
	if !found {
		log("表 %s 跳过序列回填：自增列 %s 未关联序列（目标表可能不是 SERIAL 建表）", tableName, colName)
		return
	}
	log("表 %s 自增列 %s 序列回填完成", tableName, colName)
}

// backfillInitialSequence DDL 阶段建表成功后按 MySQL 表级 AUTO_INCREMENT=N 设置序列初值
// 覆盖 data:false 仅结构迁移的场景（否则空表的起始值意图会丢失）
// 失败仅告警：不影响表的转换成功状态
func (m *Manager) backfillInitialSequence(table mysql.TableInfo) {
	colName, startValue, ok := ParseAutoIncrementInfo(table.DDL)
	if !ok || startValue <= 1 {
		return
	}
	if m.config.Conversion.Options.LowercaseColumns {
		colName = strings.ToLower(colName)
	}

	found, err := m.postgresConn.SyncAutoIncrementSequence(table.Name, colName, startValue)
	if err != nil {
		m.logError(fmt.Sprintf("警告: 表 %s 序列初值回填失败: %v", table.Name, err))
		return
	}
	if !found {
		m.Log("表 %s 跳过序列初值回填：自增列 %s 未关联序列", table.Name, colName)
		return
	}
	m.Log("表 %s 自增列 %s 序列初值设为 %d", table.Name, colName, startValue)
}
