package mysql

import (
	"testing"
	"time"
)

// TestBuildMySQLDriverConfigForcesParseTimeAndUTC issue #156：
// 用户 connection_params 中的 parseTime=false 会覆盖内置设置（go-sql-driver
// 对重复 DSN 参数取最后一次出现的值），导致时间列以字符串读回、无法进入
// pgx.CopyFrom 二进制协议。无论 DSN 中出现什么，都必须强制 ParseTime/UTC
func TestBuildMySQLDriverConfigForcesParseTimeAndUTC(t *testing.T) {
	cases := []struct {
		name string
		dsn  string
	}{
		{
			name: "用户参数显式关闭 parseTime 并覆盖时区",
			dsn:  "user:pass@tcp(localhost:3306)/db?parseTime=true&charset=utf8mb4&parseTime=false&loc=Local",
		},
		{
			name: "常规连接参数",
			dsn:  "user:pass@tcp(localhost:3306)/db?charset=utf8mb4&interpolateParams=true",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg, err := buildMySQLDriverConfig(tc.dsn)
			if err != nil {
				t.Fatalf("解析 DSN 失败: %v", err)
			}
			if !cfg.ParseTime {
				t.Errorf("ParseTime 应被强制开启")
			}
			if cfg.Loc != time.UTC {
				t.Errorf("Loc 应固定为 UTC，实际 %v", cfg.Loc)
			}
		})
	}
}

func TestBuildMySQLDriverConfigInvalidDSN(t *testing.T) {
	if _, err := buildMySQLDriverConfig(":::not-a-dsn"); err == nil {
		t.Fatal("非法 DSN 应返回错误")
	}
}
