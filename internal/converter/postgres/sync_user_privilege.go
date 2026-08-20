package postgres

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/yourusername/mysql2pg/internal/mysql"
)

// normalizePGRoleName 将 MySQL 用户名规范化为 PostgreSQL 角色名：
// 点号替换为下划线，与 PG 命名习惯对齐。
// 用户创建（ConvertUserDDL）与表权限（ConvertTablePrivilegeDDL）必须使用
// 同一规范化逻辑，否则 GRANT 指向的角色与已创建角色不一致（issue-11）。
func normalizePGRoleName(userName string) string {
	return strings.ReplaceAll(userName, ".", "_")
}

// quotePGIdentifier 用双引号包裹 PostgreSQL 标识符，并转义其中的双引号
func quotePGIdentifier(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}

// escapeSQLString 转义 SQL 字符串字面量中的单引号（双写）
func escapeSQLString(s string) string {
	return strings.ReplaceAll(s, "'", "''")
}

// PrivilegeContext 权限转换的目标端上下文（P1-08）
// 库名来自配置 PostgreSQL.Database，schema 由 pg_connection_params 的 search_path 解析
type PrivilegeContext struct {
	Database string
	Schema   string
}

// parsedGrant SHOW GRANTS 输出语句的解析结果（P1-10）
type parsedGrant struct {
	Privileges  []string // 权限列表：SELECT/INSERT/.../ALL PRIVILEGES
	Level       string   // global（*.*）/ database（db.*）/ table（db.tbl）
	Database    string   // 源库名（global 级为空）
	Table       string   // 表名（非 table 级为空）
	GrantOption bool
}

// reGrantStatement 匹配 SHOW GRANTS 的稳定格式：GRANT <privs> ON <obj> TO <user>...
var reGrantStatement = regexp.MustCompile(`(?i)^GRANT\s+(.+?)\s+ON\s+(.+?)\s+TO\s+`)

// parseGrantStatement 解析一条 SHOW GRANTS 输出语句（P1-10，替代脆弱的子串匹配）
func parseGrantStatement(grant string) (*parsedGrant, error) {
	m := reGrantStatement.FindStringSubmatch(strings.TrimSpace(grant))
	if m == nil {
		return nil, fmt.Errorf("无法解析 GRANT 语句: %s", grant)
	}
	pg := &parsedGrant{
		GrantOption: strings.Contains(strings.ToUpper(grant), "WITH GRANT OPTION"),
	}
	for _, p := range strings.Split(m[1], ",") {
		p = strings.TrimSpace(p)
		if p != "" {
			pg.Privileges = append(pg.Privileges, strings.ToUpper(p))
		}
	}

	object := strings.TrimSpace(m[2])
	parts, err := splitGrantObject(object)
	if err != nil {
		return nil, err
	}
	switch {
	case len(parts) == 2 && parts[0] == "*" && parts[1] == "*":
		pg.Level = "global"
	case len(parts) == 2 && parts[1] == "*":
		pg.Level = "database"
		pg.Database = parts[0]
	case len(parts) == 2:
		pg.Level = "table"
		pg.Database = parts[0]
		pg.Table = parts[1]
	case len(parts) == 1 && parts[0] == "*":
		pg.Level = "global"
	case len(parts) == 1 && parts[0] != "":
		pg.Level = "table"
		pg.Table = parts[0]
	default:
		pg.Level = "global"
	}
	return pg, nil
}

// splitGrantObject 按点号拆分 GRANT 对象部分，引号/反引号感知（P2-09）：
// 标识符内的点号（如 `my.db`.`t` 或转义反引号 “ `a“b` “）不作为分隔符。
// 旧的"剥掉所有引号再 Split"做法在表名/库名含点号或转义反引号时解析错位
func splitGrantObject(object string) ([]string, error) {
	var parts []string
	var cur strings.Builder
	i := 0
	for i < len(object) {
		ch := object[i]
		switch ch {
		case '`', '"':
			quote := ch
			i++
			for i < len(object) {
				if object[i] == quote {
					if i+1 < len(object) && object[i+1] == quote {
						cur.WriteByte(quote) // 双写转义
						i += 2
						continue
					}
					i++
					break
				}
				cur.WriteByte(object[i])
				i++
			}
		case '.':
			parts = append(parts, cur.String())
			cur.Reset()
			i++
		case ' ', '\t':
			i++
		default:
			cur.WriteByte(ch)
			i++
		}
	}
	parts = append(parts, cur.String())
	return parts, nil
}

// databaseLevelGrantDDLs 生成 MySQL 全局级/库级权限对应的 PG DDL（P1-09）
// 返回 DDL 列表与无法映射权限的警告说明
func databaseLevelGrantDDLs(privileges []string, ctx PrivilegeContext, quotedRole, withGrantOption string) ([]string, []string) {
	var ddls, warnings []string
	schemaRef := "SCHEMA " + quotePGIdentifier(ctx.Schema)
	tableDDLs := make(map[string]bool) // 去重

	for _, priv := range privileges {
		switch priv {
		case "ALL PRIVILEGES", "ALL":
			if ctx.Database != "" {
				ddls = append(ddls, fmt.Sprintf("GRANT ALL PRIVILEGES ON DATABASE %s TO %s%s;", quotePGIdentifier(ctx.Database), quotedRole, withGrantOption))
			}
			for _, stmt := range []string{
				fmt.Sprintf("GRANT ALL PRIVILEGES ON ALL TABLES IN %s TO %s%s", schemaRef, quotedRole, withGrantOption),
				fmt.Sprintf("GRANT ALL PRIVILEGES ON ALL SEQUENCES IN %s TO %s%s", schemaRef, quotedRole, withGrantOption),
				fmt.Sprintf("ALTER DEFAULT PRIVILEGES IN %s GRANT ALL ON TABLES TO %s", schemaRef, quotedRole),
			} {
				if !tableDDLs[stmt] {
					tableDDLs[stmt] = true
					ddls = append(ddls, stmt+";")
				}
			}
		case "SELECT", "INSERT", "UPDATE", "DELETE":
			stmt := fmt.Sprintf("GRANT %s ON ALL TABLES IN %s TO %s%s", priv, schemaRef, quotedRole, withGrantOption)
			if !tableDDLs[stmt] {
				tableDDLs[stmt] = true
				ddls = append(ddls, stmt+";")
			}
			stmt = fmt.Sprintf("ALTER DEFAULT PRIVILEGES IN %s GRANT %s ON TABLES TO %s", schemaRef, priv, quotedRole)
			if !tableDDLs[stmt] {
				tableDDLs[stmt] = true
				ddls = append(ddls, stmt+";")
			}
		case "CREATE":
			stmt := fmt.Sprintf("GRANT CREATE ON %s TO %s%s", schemaRef, quotedRole, withGrantOption)
			if !tableDDLs[stmt] {
				tableDDLs[stmt] = true
				ddls = append(ddls, stmt+";")
			}
		case "EXECUTE":
			stmt := fmt.Sprintf("GRANT EXECUTE ON ALL FUNCTIONS IN %s TO %s%s", schemaRef, quotedRole, withGrantOption)
			if !tableDDLs[stmt] {
				tableDDLs[stmt] = true
				ddls = append(ddls, stmt+";")
			}
		case "USAGE":
			stmt := fmt.Sprintf("GRANT USAGE ON %s TO %s%s", schemaRef, quotedRole, withGrantOption)
			if !tableDDLs[stmt] {
				tableDDLs[stmt] = true
				ddls = append(ddls, stmt+";")
			}
		default:
			warnings = append(warnings, fmt.Sprintf("MySQL 权限 %s 在 PostgreSQL 无库级对应权限，已跳过", priv))
		}
	}
	return ddls, warnings
}

// ConvertUserDDL 将MySQL用户权限转换为PostgreSQL用户权限
// ctx 提供目标库名与 schema（P1-08），返回 DDL 列表与转换警告（P1-20）
func ConvertUserDDL(user mysql.UserInfo, ctx PrivilegeContext) ([]string, []string, error) {
	var pgDDLs, warnings []string

	if ctx.Schema == "" {
		ctx.Schema = "public"
	}

	// 提取用户名（去掉主机部分）
	userParts := strings.Split(user.Name, "@")
	if len(userParts) != 2 {
		return nil, nil, fmt.Errorf("无效的用户名格式: %s", user.Name)
	}
	userName := userParts[0]

	// 过滤掉MySQL系统用户，如mysql.infoschema、mysql.session等
	// 这些用户是MySQL内部使用的，不需要转换到PostgreSQL
	if strings.HasPrefix(userName, "mysql.") {
		return nil, nil, nil // 跳过系统用户，返回空的DDL列表
	}

	// 处理用户名中的特殊字符，将点号替换为下划线以符合PostgreSQL命名规则
	pgUserName := normalizePGRoleName(userName)
	quotedRole := quotePGIdentifier(pgUserName)

	// 创建用户 - PostgreSQL 不支持 IF NOT EXISTS，所以先检查用户是否存在
	// 使用引号语法确保特殊字符被正确处理
	pgDDLs = append(pgDDLs, fmt.Sprintf("DO $$ BEGIN IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = '%s') THEN CREATE USER %s; END IF; END $$;", escapeSQLString(pgUserName), quotedRole))

	// 转换权限
	seen := make(map[string]bool)
	for _, grant := range user.Grants {
		parsed, err := parseGrantStatement(grant)
		if err != nil {
			warnings = append(warnings, fmt.Sprintf("授权语句解析失败，已跳过: %s", grant))
			continue
		}

		withGrantOption := ""
		if parsed.GrantOption {
			withGrantOption = " WITH GRANT OPTION"
		}

		var ddls, warns []string
		switch parsed.Level {
		case "global", "database":
			ddls, warns = databaseLevelGrantDDLs(parsed.Privileges, ctx, quotedRole, withGrantOption)
		case "table":
			ddls, warns = tableLevelGrantDDLs(parsed.Privileges, ctx, parsed.Table, quotedRole, withGrantOption)
		}
		warnings = append(warnings, warns...)
		for _, ddl := range ddls {
			if !seen[ddl] {
				seen[ddl] = true
				pgDDLs = append(pgDDLs, ddl)
			}
		}
	}

	return pgDDLs, warnings, nil
}

// tableLevelGrantDDLs 生成 MySQL 表级权限（SHOW GRANTS 中的 db.tbl 形式）对应的 PG DDL
func tableLevelGrantDDLs(privileges []string, ctx PrivilegeContext, tableName, quotedRole, withGrantOption string) ([]string, []string) {
	var ddls, warnings []string
	tableRef := fmt.Sprintf("TABLE %s.%s", quotePGIdentifier(ctx.Schema), quotePGIdentifier(tableName))

	var mapped []string
	for _, priv := range privileges {
		switch priv {
		case "ALL PRIVILEGES", "ALL":
			ddls = append(ddls, fmt.Sprintf("GRANT ALL PRIVILEGES ON %s TO %s%s;", tableRef, quotedRole, withGrantOption))
		case "SELECT", "INSERT", "UPDATE", "DELETE", "REFERENCES", "TRIGGER":
			mapped = append(mapped, priv)
		default:
			warnings = append(warnings, fmt.Sprintf("表 %s 的 MySQL 权限 %s 在 PostgreSQL 无表级对应权限，已跳过", tableName, priv))
		}
	}
	if len(mapped) > 0 {
		ddls = append(ddls, fmt.Sprintf("GRANT %s ON %s TO %s%s;", strings.Join(mapped, ", "), tableRef, quotedRole, withGrantOption))
	}
	return ddls, warnings
}
