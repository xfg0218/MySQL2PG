package postgres

import (
	"context"
	"errors"
	"testing"

	"github.com/yourusername/mysql2pg/internal/config"
	"github.com/yourusername/mysql2pg/internal/mysql"
)

// TestManagerContextNilSafe 未通过 NewManager 构造的 Manager
// context() 必须回退为 context.Background()，保证测试与直接构造场景的 nil 安全
func TestManagerContextNilSafe(t *testing.T) {
	m := &Manager{}

	ctx := m.context()
	if ctx == nil {
		t.Fatal("context() 不应返回 nil")
	}
	if err := ctx.Err(); err != nil {
		t.Errorf("默认 context 不应已取消: %v", err)
	}
}

// TestConvertViewsRespectsCancelledContext 根 context 已取消时，
// convertViews 必须立即返回取消错误且不处理任何视图
func TestConvertViewsRespectsCancelledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	m := newTestManager(3)
	m.ctx = ctx

	views := []mysql.ViewInfo{
		{ViewName: "view_a"},
		{ViewName: "view_b"},
		{ViewName: "view_c"},
	}

	semaphore := make(chan struct{}, 4)
	err := m.convertViews(views, semaphore)
	if err == nil {
		t.Fatal("convertViews() 在取消后应返回错误")
	}
	if !errors.Is(err, context.Canceled) {
		t.Errorf("convertViews() error = %v, want context.Canceled", err)
	}
	if got := m.completedTasks.Load(); got != 0 {
		t.Errorf("completedTasks = %d, want 0（取消后不应处理任何视图）", got)
	}
}

// TestConvertFunctionsRespectsCancelledContext 根 context 已取消时，
// convertFunctions 必须立即返回取消错误且不处理任何函数
func TestConvertFunctionsRespectsCancelledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	m := newTestManager(2)
	m.ctx = ctx

	functions := []mysql.FunctionInfo{
		{Name: "func_a"},
		{Name: "func_b"},
	}

	semaphore := make(chan struct{}, 4)
	err := m.convertFunctions(functions, semaphore)
	if err == nil {
		t.Fatal("convertFunctions() 在取消后应返回错误")
	}
	if !errors.Is(err, context.Canceled) {
		t.Errorf("convertFunctions() error = %v, want context.Canceled", err)
	}
	if got := m.completedTasks.Load(); got != 0 {
		t.Errorf("completedTasks = %d, want 0（取消后不应处理任何函数）", got)
	}
}

// TestConvertViewsCancelledMidLoop 运行中取消时，循环检查点必须停止后续处理
func TestConvertViewsCancelledMidLoop(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	cfg := &config.Config{
		Run: config.RunConfig{
			ShowProgress:      true,
			EnableFileLogging: false,
			ShowConsoleLogs:   false,
			ShowLogInConsole:  false,
		},
	}
	m := &Manager{
		config:     cfg,
		ctx:        ctx,
		totalTasks: 3,
	}

	views := []mysql.ViewInfo{
		{ViewName: "view_a"},
		{ViewName: "view_b"},
		{ViewName: "view_c"},
	}
	// 全部视图在排除列表中：跳过分支只计数不访问数据库
	m.config.Conversion.Options.SkipUseViewList = true
	m.config.Conversion.Options.SkipViewSet = config.StringSet{
		"view_a": {},
		"view_b": {},
		"view_c": {},
	}

	// 第一个视图处理后取消，后续视图不应再被处理
	semaphore := make(chan struct{}, 4)
	go func() {
		// 等待第一个视图计数完成后取消
		for m.completedTasks.Load() < 1 {
		}
		cancel()
	}()

	err := m.convertViews(views, semaphore)
	if err == nil {
		t.Fatal("convertViews() 在运行中取消后应返回错误")
	}
	if !errors.Is(err, context.Canceled) {
		t.Errorf("convertViews() error = %v, want context.Canceled", err)
	}
	// 取消时机存在调度不确定性，但计数必须落在 [1, 3] 且不发生双计数
	if got := m.completedTasks.Load(); got < 1 || got > int64(len(views)) {
		t.Errorf("completedTasks = %d, want 1~%d 之间（不应为 0，也不应双计数）", got, len(views))
	}
}
