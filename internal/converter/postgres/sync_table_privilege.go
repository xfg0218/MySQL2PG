package postgres

// 统一使用小写文件名，避免大小写不敏感文件系统下的冲突
import (
	"fmt"
	"strings"

	"github.com/yourusername/mysql2pg/internal/mysql"
)

// ConvertTablePrivilegeDDL 将MySQL表权限转换为PostgreSQL表权限
func ConvertTablePrivilegeDDL(tablePriv mysql.TablePrivInfo) ([]string, error) {

	var pgDDLs []string

	// 提取用户名（处理带主机和不带主机的情况）
	var userName string
	userParts := strings.Split(tablePriv.User, "@")
	if len(userParts) == 2 {
		userName = userParts[0]
	} else if len(userParts) == 1 {
		// 没有主机部分的情况
		userName = userParts[0]
	} else {
		return nil, fmt.Errorf("无效的用户名格式: %s", tablePriv.User)
	}

	// 处理表级别的权限
	tableName := tablePriv.TableName

	// 转换权限（忽略大小写）
	tablePrivStr := strings.ToUpper(tablePriv.TablePriv)

	if strings.Contains(tablePrivStr, "SELECT") {
		pgDDLs = append(pgDDLs, fmt.Sprintf("GRANT SELECT ON \"%s\" TO \"%s\";", tableName, userName))
	}
	if strings.Contains(tablePrivStr, "INSERT") {
		pgDDLs = append(pgDDLs, fmt.Sprintf("GRANT INSERT ON \"%s\" TO \"%s\";", tableName, userName))
	}
	if strings.Contains(tablePrivStr, "UPDATE") {
		pgDDLs = append(pgDDLs, fmt.Sprintf("GRANT UPDATE ON \"%s\" TO \"%s\";", tableName, userName))
	}
	if strings.Contains(tablePrivStr, "DELETE") {
		pgDDLs = append(pgDDLs, fmt.Sprintf("GRANT DELETE ON \"%s\" TO \"%s\";", tableName, userName))
	}
	if strings.Contains(tablePrivStr, "ALL PRIVILEGES") {
		pgDDLs = append(pgDDLs, fmt.Sprintf("GRANT ALL PRIVILEGES ON \"%s\" TO \"%s\";", tableName, userName))
	}

	return pgDDLs, nil
}
