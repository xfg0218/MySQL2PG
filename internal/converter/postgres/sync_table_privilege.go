package postgres

// 统一使用小写文件名，避免大小写不敏感文件系统下的冲突
import (
	"fmt"
	"strings"

	"github.com/yourusername/mysql2pg/internal/mysql"
)

// ConvertTablePrivilegeDDL 将MySQL表权限转换为PostgreSQL表权限。
// ctx 提供目标 schema（P2-09）：GRANT 指向 schema 限定的表，
// 与 tableLevelGrantDDLs 的 TABLE "schema"."table" 形式一致，
// 不再依赖执行时的 search_path
func ConvertTablePrivilegeDDL(tablePriv mysql.TablePrivInfo, ctx PrivilegeContext) ([]string, error) {

	var pgDDLs []string

	if ctx.Schema == "" {
		ctx.Schema = "public"
	}

	// 提取用户名（处理带主机和不带主机的情况）
	var rawUserName string
	userParts := strings.Split(tablePriv.User, "@")
	if len(userParts) == 2 {
		rawUserName = userParts[0]
	} else if len(userParts) == 1 {
		// 没有主机部分的情况
		rawUserName = userParts[0]
	} else {
		return nil, fmt.Errorf("无效的用户名格式: %s", tablePriv.User)
	}

	// 与用户创建（ConvertUserDDL）使用相同的规范化逻辑，
	// 保证 GRANT 指向的角色与已创建角色一致（issue-11）
	userName := normalizePGRoleName(rawUserName)
	quotedRole := quotePGIdentifier(userName)
	quotedTable := fmt.Sprintf("%s.%s", quotePGIdentifier(ctx.Schema), quotePGIdentifier(tablePriv.TableName))

	// 转换权限（忽略大小写）
	tablePrivStr := strings.ToUpper(tablePriv.TablePriv)

	if strings.Contains(tablePrivStr, "SELECT") {
		pgDDLs = append(pgDDLs, fmt.Sprintf("GRANT SELECT ON %s TO %s;", quotedTable, quotedRole))
	}
	if strings.Contains(tablePrivStr, "INSERT") {
		pgDDLs = append(pgDDLs, fmt.Sprintf("GRANT INSERT ON %s TO %s;", quotedTable, quotedRole))
	}
	if strings.Contains(tablePrivStr, "UPDATE") {
		pgDDLs = append(pgDDLs, fmt.Sprintf("GRANT UPDATE ON %s TO %s;", quotedTable, quotedRole))
	}
	if strings.Contains(tablePrivStr, "DELETE") {
		pgDDLs = append(pgDDLs, fmt.Sprintf("GRANT DELETE ON %s TO %s;", quotedTable, quotedRole))
	}
	if strings.Contains(tablePrivStr, "ALL PRIVILEGES") {
		pgDDLs = append(pgDDLs, fmt.Sprintf("GRANT ALL PRIVILEGES ON %s TO %s;", quotedTable, quotedRole))
	}

	return pgDDLs, nil
}
