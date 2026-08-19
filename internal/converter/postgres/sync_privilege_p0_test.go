package postgres

import (
	"strings"
	"testing"

	"github.com/yourusername/mysql2pg/internal/config"
	"github.com/yourusername/mysql2pg/internal/mysql"
)

// TestNormalizePGRoleName 用户名规范化：点号替换为下划线（issue-11）
func TestNormalizePGRoleName(t *testing.T) {
	tests := []struct {
		in   string
		want string
	}{
		{"svc.app", "svc_app"},
		{"plain_user", "plain_user"},
		{"a.b.c", "a_b_c"},
	}
	for _, tt := range tests {
		if got := normalizePGRoleName(tt.in); got != tt.want {
			t.Errorf("normalizePGRoleName(%q) = %q, want %q", tt.in, got, tt.want)
		}
	}
}

// TestQuotePGIdentifier 标识符包裹与双引号转义（issue-11）
func TestQuotePGIdentifier(t *testing.T) {
	if got := quotePGIdentifier("svc_app"); got != `"svc_app"` {
		t.Errorf("quotePGIdentifier = %q, want %q", got, `"svc_app"`)
	}
	if got := quotePGIdentifier(`we"ird`); got != `"we""ird"` {
		t.Errorf("quotePGIdentifier 转义错误 = %q, want %q", got, `"we""ird"`)
	}
}

// TestUserPrivilegeRoleNameConsistency 用户创建与表权限 GRANT 的角色名必须一致（issue-11）
// 修复前：ConvertUserDDL 创建 "svc_app" 而 ConvertTablePrivilegeDDL GRANT 到 "svc.app"，
// 导致 role does not exist 错误
func TestUserPrivilegeRoleNameConsistency(t *testing.T) {
	userDDLs, err := ConvertUserDDL(mysql.UserInfo{
		Name:   "svc.app@%",
		Grants: []string{"GRANT SELECT ON test_db.t1 TO 'svc.app'@'%';"},
	})
	if err != nil {
		t.Fatalf("ConvertUserDDL 返回错误：%v", err)
	}

	privDDLs, err := ConvertTablePrivilegeDDL(mysql.TablePrivInfo{
		User:      "svc.app@%",
		TableName: "t1",
		TablePriv: "Select",
	})
	if err != nil {
		t.Fatalf("ConvertTablePrivilegeDDL 返回错误：%v", err)
	}

	joinedUser := strings.Join(userDDLs, "\n")
	joinedPriv := strings.Join(privDDLs, "\n")

	if !strings.Contains(joinedUser, `CREATE USER "svc_app"`) {
		t.Errorf("用户创建应使用规范化角色名 svc_app，实际：%s", joinedUser)
	}
	if !strings.Contains(joinedPriv, `TO "svc_app"`) {
		t.Errorf("GRANT 应指向规范化角色名 svc_app，实际：%s", joinedPriv)
	}
	if strings.Contains(joinedPriv, `"svc.app"`) {
		t.Errorf("GRANT 不应指向原始用户名 svc.app，实际：%s", joinedPriv)
	}
}

// TestConvertFunctionDDL_DateFormatConversion 函数体 DATE_FORMAT 格式串必须转换（issue-12）
// 修复前 %Y-%m-%d 原样进入 PG TO_CHAR，输出字面 %Y-%m-%d 而非日期
func TestConvertFunctionDDL_DateFormatConversion(t *testing.T) {
	out, err := ConvertFunctionDDL(mysql.FunctionInfo{
		Name: "f_date_format",
		DDL:  "CREATE FUNCTION f_date_format(a DATETIME) RETURNS VARCHAR(30) DETERMINISTIC BEGIN RETURN DATE_FORMAT(a, '%Y-%m-%d %H:%i:%s'); END",
	})
	if err != nil {
		t.Fatalf("ConvertFunctionDDL 返回错误：%v", err)
	}
	if !strings.Contains(out, "TO_CHAR(") {
		t.Fatalf("缺少 TO_CHAR 转换，实际：%s", out)
	}
	if !strings.Contains(out, "'YYYY-MM-DD HH24:MI:SS'") {
		t.Errorf("格式说明符未转换为 PG 格式，实际：%s", out)
	}
	if strings.Contains(out, "%Y") {
		t.Errorf("输出不应残留 MySQL 格式说明符 %%Y，实际：%s", out)
	}
}

// TestDisplayPasswordResetUsers 密码重置清单输出冒烟测试（issue-10）
// 密码哈希格式不兼容不可迁移，需输出人工重设清单；验证去重与不崩溃
func TestDisplayPasswordResetUsers(t *testing.T) {
	m := &Manager{
		config: &config.Config{
			Run: config.RunConfig{
				ShowConsoleLogs:   false,
				EnableFileLogging: false,
				ShowLogInConsole:  false,
			},
		},
		passwordResetUsers: []string{"app_user", "app_user", "svc_app"}, // 含重复项
	}
	// 不应 panic；重复用户应被去重（行为在输出中体现，此处做冒烟验证）
	m.displayPasswordResetUsers()
}
