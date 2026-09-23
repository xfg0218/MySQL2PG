package config

import (
	"strings"
	"testing"
)

func TestConvertExclusionLists_ViewList(t *testing.T) {
	// 测试视图排除列表转换
	config := &ConversionConfig{
		Options: OptionsConfig{
			SkipViewList: []string{"View1", "VIEW2", "view3"},
		},
	}

	config.convertExclusionLists()

	// 验证集合已正确转换
	expected := []string{"view1", "view2", "view3"}
	for _, key := range expected {
		if _, exists := config.Options.SkipViewSet[key]; !exists {
			t.Errorf("SkipViewSet missing key %q, got: %v", key, config.Options.SkipViewSet)
		}
	}

	// 验证大小
	if len(config.Options.SkipViewSet) != 3 {
		t.Errorf("Expected 3 elements, got %d", len(config.Options.SkipViewSet))
	}
}

func TestConvertExclusionLists_FunctionList(t *testing.T) {
	// 测试函数排除列表转换
	config := &ConversionConfig{
		Options: OptionsConfig{
			SkipFunctionList: []string{"Func1", "FUNC2", "func3"},
		},
	}

	config.convertExclusionLists()

	// 验证集合已正确转换
	expected := []string{"func1", "func2", "func3"}
	for _, key := range expected {
		if _, exists := config.Options.SkipFunctionSet[key]; !exists {
			t.Errorf("SkipFunctionSet missing key %q, got: %v", key, config.Options.SkipFunctionSet)
		}
	}

	// 验证大小
	if len(config.Options.SkipFunctionSet) != 3 {
		t.Errorf("Expected 3 elements, got %d", len(config.Options.SkipFunctionSet))
	}
}

func TestConvertExclusionLists_Empty(t *testing.T) {
	// 测试空列表转换
	config := &ConversionConfig{
		Options: OptionsConfig{
			SkipViewList:     []string{},
			SkipFunctionList: nil,
		},
	}

	config.convertExclusionLists()

	// 验证集合仍为 nil
	if config.Options.SkipViewSet != nil {
		t.Errorf("Expected nil SkipViewSet, got %v", config.Options.SkipViewSet)
	}
	if config.Options.SkipFunctionSet != nil {
		t.Errorf("Expected nil SkipFunctionSet, got %v", config.Options.SkipFunctionSet)
	}
}

func TestConvertExclusionLists_Duplicates(t *testing.T) {
	// 测试重复元素（应该自动去重）
	config := &ConversionConfig{
		Options: OptionsConfig{
			SkipViewList: []string{"view1", "VIEW1", "View1"},
		},
	}

	config.convertExclusionLists()

	// 验证只有 1 个元素（去重）
	if len(config.Options.SkipViewSet) != 1 {
		t.Errorf("Expected 1 element (deduplicated), got %d", len(config.Options.SkipViewSet))
	}

	if _, exists := config.Options.SkipViewSet["view1"]; !exists {
		t.Errorf("SkipViewSet missing key 'view1'")
	}
}

// TestValidateConfigTableSyncTimeoutDefault issue #173：
// 单表同步超时必须回落为默认值，否则批次 context 无 deadline，
// go-sql-driver 的 watchCancel 不启动监听，网络半开时永久挂死且 Ctrl-C 无效
func TestValidateConfigTableSyncTimeoutDefault(t *testing.T) {
	newMinimalConfig := func() *Config {
		c := &Config{}
		c.MySQL.Host = "localhost"
		c.MySQL.Username = "u"
		c.MySQL.Database = "d"
		c.PostgreSQL.Host = "localhost"
		c.PostgreSQL.Username = "u"
		c.PostgreSQL.Database = "d"
		return c
	}

	cases := []struct {
		name string
		set  int
		want int
	}{
		{name: "未配置时回落默认 3600", set: 0, want: 3600},
		{name: "负数回落默认 3600", set: -5, want: 3600},
		{name: "显式配置保持不变", set: 7200, want: 7200},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := newMinimalConfig()
			c.Conversion.Limits.TableSyncTimeoutSeconds = tc.set

			if err := c.ValidateConfig(); err != nil {
				t.Fatalf("最小合法配置不应校验失败: %v", err)
			}
			if got := c.Conversion.Limits.TableSyncTimeoutSeconds; got != tc.want {
				t.Errorf("TableSyncTimeoutSeconds = %d, 期望 %d", got, tc.want)
			}
		})
	}
}

// TestValidateConfigConsistentSnapshotConcurrencyMutex issue #175：
// 快照事务是 mysql.Connection 上的单个 *sql.Tx，绑定一条连接，
// querier() 对所有数据读取返回同一个 Tx；concurrency>1 时并发查询会撞
// go-sql-driver 的 packet buffer（ErrBusyBuffer），且因 Tx 不换连接使重试无效。
// 该组合必须在配置校验阶段就被拒绝，而不是运行到数据同步才失败。
func TestValidateConfigConsistentSnapshotConcurrencyMutex(t *testing.T) {
	newCfg := func(snapshot bool, concurrency int) *Config {
		c := &Config{}
		c.MySQL.Host = "localhost"
		c.MySQL.Username = "u"
		c.MySQL.Database = "d"
		c.MySQL.ConsistentSnapshot = snapshot
		c.PostgreSQL.Host = "localhost"
		c.PostgreSQL.Username = "u"
		c.PostgreSQL.Database = "d"
		c.Conversion.Limits.Concurrency = concurrency
		return c
	}

	cases := []struct {
		name        string
		snapshot    bool
		concurrency int
		wantErr     bool
	}{
		{name: "快照关闭 + 高并发（默认场景）", snapshot: false, concurrency: 10, wantErr: false},
		{name: "快照关闭 + 单并发", snapshot: false, concurrency: 1, wantErr: false},
		{name: "快照开启 + 单并发应允许", snapshot: true, concurrency: 1, wantErr: false},
		{name: "快照开启 + 高并发必须拒绝", snapshot: true, concurrency: 10, wantErr: true},
		// 校验必须位于 Concurrency 默认值回落之后：未配置并发时回落为 1，不应误判
		{name: "快照开启 + 未配置并发（回落默认 1）不应误判", snapshot: true, concurrency: 0, wantErr: false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := newCfg(tc.snapshot, tc.concurrency).ValidateConfig()

			if tc.wantErr {
				if err == nil {
					t.Fatal("consistent_snapshot 与 concurrency>1 的组合应被拒绝")
				}
				if !strings.Contains(err.Error(), "consistent_snapshot") {
					t.Errorf("错误信息应指明冲突的配置项，实际 %q", err.Error())
				}
				return
			}
			if err != nil {
				t.Fatalf("该组合不应校验失败: %v", err)
			}
		})
	}
}
