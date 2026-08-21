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
	userDDLs, _, err := ConvertUserDDL(mysql.UserInfo{
		Name:   "svc.app@%",
		Grants: []string{"GRANT SELECT ON test_db.* TO 'svc.app'@'%';"},
	}, PrivilegeContext{Database: "targetdb", Schema: "public"})
	if err != nil {
		t.Fatalf("ConvertUserDDL 返回错误：%v", err)
	}

	privDDLs, err := ConvertTablePrivilegeDDL(mysql.TablePrivInfo{
		User:      "svc.app@%",
		TableName: "t1",
		TablePriv: "Select",
	}, PrivilegeContext{Database: "targetdb", Schema: "public"})
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
	// P2-09：GRANT 必须指向 schema 限定的表
	if !strings.Contains(joinedPriv, `ON "public"."t1"`) {
		t.Errorf("GRANT 应指向 schema 限定的表，实际：%s", joinedPriv)
	}
}

// TestSplitGrantObject P2-09：GRANT 对象解析（引号/反引号感知）
func TestSplitGrantObject(t *testing.T) {
	tests := []struct {
		name   string
		object string
		want   []string
	}{
		{"全局", "*.*", []string{"*", "*"}},
		{"库级", "test_db.*", []string{"test_db", "*"}},
		{"反引号表", "`test_db`.`t1`", []string{"test_db", "t1"}},
		{"库名含点号", "`my.db`.`t1`", []string{"my.db", "t1"}},
		{"转义反引号", "`a``b`.`t1`", []string{"a`b", "t1"}},
		{"双引号标识符", `"my.db"."t1"`, []string{"my.db", "t1"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := splitGrantObject(tt.object)
			if err != nil {
				t.Fatalf("splitGrantObject(%q) 返回错误：%v", tt.object, err)
			}
			if len(got) != len(tt.want) {
				t.Fatalf("splitGrantObject(%q) = %v, want %v", tt.object, got, tt.want)
			}
			for i := range got {
				if got[i] != tt.want[i] {
					t.Errorf("splitGrantObject(%q)[%d] = %q, want %q", tt.object, i, got[i], tt.want[i])
				}
			}
		})
	}
}

// TestParseGrantStatement_DottedName P2-09：库名/表名含点号时解析不错位
func TestParseGrantStatement_DottedName(t *testing.T) {
	parsed, err := parseGrantStatement("GRANT SELECT ON `my.db`.`t1` TO 'u'@'%';")
	if err != nil {
		t.Fatalf("parseGrantStatement 返回错误：%v", err)
	}
	if parsed.Level != "table" || parsed.Database != "my.db" || parsed.Table != "t1" {
		t.Errorf("解析结果错误：level=%s database=%s table=%s", parsed.Level, parsed.Database, parsed.Table)
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

// TestParseGrantStatement GRANT 语句解析（P1-10）
func TestParseGrantStatement(t *testing.T) {
	tests := []struct {
		name       string
		grant      string
		wantLevel  string
		wantDB     string
		wantTable  string
		wantPrivs  []string
		wantOption bool
		wantErr    bool
	}{
		{
			name:       "全局级 ALL",
			grant:      "GRANT ALL PRIVILEGES ON *.* TO 'app'@'%' WITH GRANT OPTION",
			wantLevel:  "global",
			wantPrivs:  []string{"ALL PRIVILEGES"},
			wantOption: true,
		},
		{
			name:      "库级多选",
			grant:     "GRANT SELECT, INSERT ON `mydb`.* TO 'app'@'localhost';",
			wantLevel: "database",
			wantDB:    "mydb",
			wantPrivs: []string{"SELECT", "INSERT"},
		},
		{
			name:      "表级",
			grant:     "GRANT SELECT ON `mydb`.`orders` TO 'app'@'%';",
			wantLevel: "table",
			wantDB:    "mydb",
			wantTable: "orders",
			wantPrivs: []string{"SELECT"},
		},
		{
			name:    "非法语句",
			grant:   "not a grant statement",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parsed, err := parseGrantStatement(tt.grant)
			if tt.wantErr {
				if err == nil {
					t.Fatal("应返回错误")
				}
				return
			}
			if err != nil {
				t.Fatalf("解析错误：%v", err)
			}
			if parsed.Level != tt.wantLevel {
				t.Errorf("Level = %q, want %q", parsed.Level, tt.wantLevel)
			}
			if parsed.Database != tt.wantDB {
				t.Errorf("Database = %q, want %q", parsed.Database, tt.wantDB)
			}
			if parsed.Table != tt.wantTable {
				t.Errorf("Table = %q, want %q", parsed.Table, tt.wantTable)
			}
			if len(parsed.Privileges) != len(tt.wantPrivs) {
				t.Fatalf("Privileges = %v, want %v", parsed.Privileges, tt.wantPrivs)
			}
			for i, p := range tt.wantPrivs {
				if parsed.Privileges[i] != p {
					t.Errorf("Privileges[%d] = %q, want %q", i, parsed.Privileges[i], p)
				}
			}
			if parsed.GrantOption != tt.wantOption {
				t.Errorf("GrantOption = %v, want %v", parsed.GrantOption, tt.wantOption)
			}
		})
	}
}

// TestConvertUserDDL_ParameterizedTarget 目标库/schema 来自上下文而非硬编码（P1-08/P1-19）
func TestConvertUserDDL_ParameterizedTarget(t *testing.T) {
	ddl, warnings, err := ConvertUserDDL(mysql.UserInfo{
		Name:   "app_user@%",
		Grants: []string{"GRANT SELECT, INSERT ON appdb.* TO 'app_user'@'%';"},
	}, PrivilegeContext{Database: "mytargetdb", Schema: "myschema"})
	if err != nil {
		t.Fatalf("ConvertUserDDL 返回错误：%v", err)
	}
	joined := strings.Join(ddl, "\n")

	// 不应再出现硬编码的 postgres 库与 public schema
	if strings.Contains(joined, `DATABASE "postgres"`) {
		t.Errorf("不应硬编码 postgres 库：%s", joined)
	}
	if strings.Contains(joined, `SCHEMA "public"`) {
		t.Errorf("不应硬编码 public schema：%s", joined)
	}
	if !strings.Contains(joined, `GRANT SELECT ON ALL TABLES IN SCHEMA "myschema" TO "app_user"`) {
		t.Errorf("缺少目标 schema 的表级 GRANT：%s", joined)
	}
	if !strings.Contains(joined, `ALTER DEFAULT PRIVILEGES IN SCHEMA "myschema" GRANT SELECT ON TABLES TO "app_user"`) {
		t.Errorf("缺少 ALTER DEFAULT PRIVILEGES：%s", joined)
	}
	if len(warnings) != 0 {
		t.Errorf("SELECT/INSERT 应可完整映射，不应有警告：%v", warnings)
	}
}

// TestConvertUserDDL_GlobalAndUnmappedPrivileges 全局权限映射与不可映射权限告警（P1-09）
func TestConvertUserDDL_GlobalAndUnmappedPrivileges(t *testing.T) {
	ddl, warnings, err := ConvertUserDDL(mysql.UserInfo{
		Name:   "admin_user@%",
		Grants: []string{"GRANT ALL PRIVILEGES, SUPER ON *.* TO 'admin_user'@'%' WITH GRANT OPTION;"},
	}, PrivilegeContext{Database: "targetdb", Schema: "public"})
	if err != nil {
		t.Fatalf("ConvertUserDDL 返回错误：%v", err)
	}
	joined := strings.Join(ddl, "\n")

	if !strings.Contains(joined, `GRANT ALL PRIVILEGES ON DATABASE "targetdb" TO "admin_user" WITH GRANT OPTION`) {
		t.Errorf("缺少目标库 ALL PRIVILEGES（含 WITH GRANT OPTION）：%s", joined)
	}
	if !strings.Contains(joined, `GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA "public" TO "admin_user"`) {
		t.Errorf("缺少序列授权：%s", joined)
	}
	// SUPER 在 PG 无对应库级权限，应产生告警而非静默丢弃
	found := false
	for _, w := range warnings {
		if strings.Contains(w, "SUPER") {
			found = true
		}
	}
	if !found {
		t.Errorf("SUPER 权限应产生告警，实际警告：%v", warnings)
	}
}

// TestConvertUserDDL_TableLevelGrant SHOW GRANTS 中的表级权限映射（P1-09）
func TestConvertUserDDL_TableLevelGrant(t *testing.T) {
	ddl, _, err := ConvertUserDDL(mysql.UserInfo{
		Name:   "reader@%",
		Grants: []string{"GRANT SELECT, REFERENCES ON `appdb`.`orders` TO 'reader'@'%';"},
	}, PrivilegeContext{Database: "targetdb", Schema: "public"})
	if err != nil {
		t.Fatalf("ConvertUserDDL 返回错误：%v", err)
	}
	joined := strings.Join(ddl, "\n")
	if !strings.Contains(joined, `GRANT SELECT, REFERENCES ON TABLE "public"."orders" TO "reader"`) {
		t.Errorf("缺少表级 GRANT：%s", joined)
	}
}
