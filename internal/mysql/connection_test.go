package mysql

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"testing"
	"time"

	gmysql "github.com/go-sql-driver/mysql"
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

// TestIsTransientConnError issue #157：瞬时连接错误判定（含包装后的错误链）
func TestIsTransientConnError(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{name: "ErrBadConn", err: driver.ErrBadConn, want: true},
		{name: "ErrInvalidConn", err: gmysql.ErrInvalidConn, want: true},
		{name: "ErrBusyBuffer", err: gmysql.ErrBusyBuffer, want: true},
		{name: "包装后的瞬时错误", err: fmt.Errorf("获取表数据失败: %w", gmysql.ErrInvalidConn), want: true},
		{name: "语法错误不重试", err: errors.New("ERROR 1064 (42000): syntax error"), want: false},
		{name: "nil 不重试", err: nil, want: false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := isTransientConnError(tc.err); got != tc.want {
				t.Errorf("isTransientConnError(%v) = %v, 期望 %v", tc.err, got, tc.want)
			}
		})
	}
}

// TestRetryOnTransientConn 覆盖重试循环的核心语义
func TestRetryOnTransientConn(t *testing.T) {
	baseDelay := time.Millisecond

	t.Run("瞬时错误两次后成功", func(t *testing.T) {
		calls := 0
		err := retryOnTransientConn(context.Background(), transientConnRetries, baseDelay, func() error {
			calls++
			if calls < 3 {
				return gmysql.ErrInvalidConn
			}
			return nil
		})
		if err != nil {
			t.Fatalf("第三次调用应成功，实际 %v", err)
		}
		if calls != 3 {
			t.Fatalf("应调用 3 次（1 次原始 + 2 次重试），实际 %d", calls)
		}
	})

	t.Run("非连接错误立即返回不重试", func(t *testing.T) {
		syntaxErr := errors.New("syntax error")
		calls := 0
		err := retryOnTransientConn(context.Background(), transientConnRetries, baseDelay, func() error {
			calls++
			return syntaxErr
		})
		if !errors.Is(err, syntaxErr) {
			t.Fatalf("应原样返回错误，实际 %v", err)
		}
		if calls != 1 {
			t.Fatalf("非连接错误不应重试，实际调用 %d 次", calls)
		}
	})

	t.Run("重试耗尽后返回最后一次错误", func(t *testing.T) {
		calls := 0
		err := retryOnTransientConn(context.Background(), transientConnRetries, baseDelay, func() error {
			calls++
			return driver.ErrBadConn
		})
		if !errors.Is(err, driver.ErrBadConn) {
			t.Fatalf("应返回最后一次错误，实际 %v", err)
		}
		if calls != transientConnRetries+1 {
			t.Fatalf("应调用 %d 次，实际 %d", transientConnRetries+1, calls)
		}
	})

	t.Run("ctx 已取消时不再重试", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		calls := 0
		err := retryOnTransientConn(ctx, transientConnRetries, baseDelay, func() error {
			calls++
			return gmysql.ErrInvalidConn
		})
		if !errors.Is(err, gmysql.ErrInvalidConn) {
			t.Fatalf("应返回最后一次错误，实际 %v", err)
		}
		if calls != 1 {
			t.Fatalf("ctx 取消后不应重试，实际调用 %d 次", calls)
		}
	})
}
