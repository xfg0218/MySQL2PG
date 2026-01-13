package postgres

import (
	"fmt"
	"strings"

	"github.com/yourusername/mysql2pg/internal/mysql"
)

// ConvertUserDDL 将MySQL用户权限转换为PostgreSQL用户权限
func ConvertUserDDL(user mysql.UserInfo) ([]string, error) {
	var pgDDLs []string

	// 提取用户名（去掉主机部分）
	userParts := strings.Split(user.Name, "@")
	if len(userParts) != 2 {
		return nil, fmt.Errorf("无效的用户名格式: %s", user.Name)
	}
	userName := userParts[0]

	// 过滤掉MySQL系统用户，如mysql.infoschema、mysql.session等
	// 这些用户是MySQL内部使用的，不需要转换到PostgreSQL
	if strings.HasPrefix(userName, "mysql.") {
		return nil, nil // 跳过系统用户，返回空的DDL列表
	}

	// 处理用户名中的特殊字符，将点号替换为下划线以符合PostgreSQL命名规则
	pgUserName := strings.ReplaceAll(userName, ".", "_")

	// 创建用户 - PostgreSQL 不支持 IF NOT EXISTS，所以先检查用户是否存在
	// 使用引号语法确保特殊字符被正确处理
	pgDDLs = append(pgDDLs, fmt.Sprintf("DO $$ BEGIN IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = '%s') THEN CREATE USER \"%s\"; END IF; END $$;", pgUserName, pgUserName))

	// 转换权限
	for _, grant := range user.Grants {
		// 处理数据库级别的权限
		if strings.Contains(grant, "ALL PRIVILEGES ON") {
			// 提取数据库名
			dbStart := strings.Index(grant, "ON ") + 3
			dbEnd := strings.Index(grant[dbStart:], " TO")
			if dbEnd == -1 {
				continue
			}
			dbSpec := grant[dbStart : dbStart+dbEnd]

			// 处理通配符数据库
			if dbSpec == "*.*" {
				pgDDLs = append(pgDDLs, fmt.Sprintf("GRANT ALL PRIVILEGES ON DATABASE postgres TO \"%s\";", pgUserName))
				pgDDLs = append(pgDDLs, fmt.Sprintf("GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO \"%s\";", pgUserName))
				pgDDLs = append(pgDDLs, fmt.Sprintf("GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO \"%s\";", pgUserName))
			} else {
				// 处理特定数据库
				dbName := strings.Split(dbSpec, ".")[0]
				pgDDLs = append(pgDDLs, fmt.Sprintf("GRANT ALL PRIVILEGES ON DATABASE %s TO \"%s\";", dbName, pgUserName))
				pgDDLs = append(pgDDLs, fmt.Sprintf("GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO \"%s\";", pgUserName))
				pgDDLs = append(pgDDLs, fmt.Sprintf("GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO \"%s\";", pgUserName))
			}
		} else if strings.Contains(grant, "SELECT ON") {
			// 处理SELECT权限
			pgDDLs = append(pgDDLs, fmt.Sprintf("GRANT SELECT ON ALL TABLES IN SCHEMA public TO \"%s\";", pgUserName))
		} else if strings.Contains(grant, "INSERT ON") {
			// 处理INSERT权限
			pgDDLs = append(pgDDLs, fmt.Sprintf("GRANT INSERT ON ALL TABLES IN SCHEMA public TO \"%s\";", pgUserName))
		} else if strings.Contains(grant, "UPDATE ON") {
			// 处理UPDATE权限
			pgDDLs = append(pgDDLs, fmt.Sprintf("GRANT UPDATE ON ALL TABLES IN SCHEMA public TO \"%s\";", pgUserName))
		} else if strings.Contains(grant, "DELETE ON") {
			// 处理DELETE权限
			pgDDLs = append(pgDDLs, fmt.Sprintf("GRANT DELETE ON ALL TABLES IN SCHEMA public TO \"%s\";", pgUserName))
		}
	}

	return pgDDLs, nil
}
