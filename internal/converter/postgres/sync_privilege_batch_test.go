package postgres

import (
	"strings"
	"testing"

	"github.com/yourusername/mysql2pg/internal/mysql"
)

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
