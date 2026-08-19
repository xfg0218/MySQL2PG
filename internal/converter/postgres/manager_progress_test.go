package postgres

import (
	"sync"
	"testing"

	"github.com/yourusername/mysql2pg/internal/config"
	"github.com/yourusername/mysql2pg/internal/mysql"
)

// newTestManager 创建用于进度计数测试的最小化 Manager
// 关闭文件日志与控制台输出，避免测试产生副作用
func newTestManager(totalTasks int) *Manager {
	return &Manager{
		config: &config.Config{
			Run: config.RunConfig{
				ShowProgress:      true,
				EnableFileLogging: false,
				ShowConsoleLogs:   false,
				ShowLogInConsole:  false,
			},
		},
		totalTasks: totalTasks,
	}
}

// TestCompleteTask_ReturnsIncrementedValue 验证 completeTask 原子递增并返回新值
func TestCompleteTask_ReturnsIncrementedValue(t *testing.T) {
	m := newTestManager(3)

	for want := int64(1); want <= 3; want++ {
		if got := m.completeTask(); got != want {
			t.Fatalf("completeTask() = %d, want %d", got, want)
		}
	}
}

// TestUpdateProgress_ConcurrentSafe 并发调用 updateProgress 计数必须精确
// 配合 go test -race 运行，验证计数器无数据竞争
func TestUpdateProgress_ConcurrentSafe(t *testing.T) {
	const goroutines = 50
	const perGoroutine = 20

	m := newTestManager(goroutines * perGoroutine)

	var wg sync.WaitGroup
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < perGoroutine; j++ {
				m.updateProgress()
			}
		}()
	}
	wg.Wait()

	want := int64(goroutines * perGoroutine)
	if got := m.completedTasks.Load(); got != want {
		t.Errorf("completedTasks = %d, want %d", got, want)
	}
}

// TestConvertViews_SkippedViewsCountedOnce 回归测试：
// 排除列表中的视图只应计数一次（修复前手动 ++ 与 updateProgress 双计数）
func TestConvertViews_SkippedViewsCountedOnce(t *testing.T) {
	views := []mysql.ViewInfo{
		{ViewName: "view_a"},
		{ViewName: "view_b"},
		{ViewName: "view_c"},
	}

	m := newTestManager(len(views))
	m.config.Conversion.Options.SkipUseViewList = true
	m.config.Conversion.Options.SkipViewSet = config.StringSet{
		"view_a": {},
		"view_b": {},
		"view_c": {},
	}

	semaphore := make(chan struct{}, 4)
	if err := m.convertViews(views, semaphore); err != nil {
		t.Fatalf("convertViews() error = %v", err)
	}

	want := int64(len(views))
	if got := m.completedTasks.Load(); got != want {
		t.Errorf("completedTasks = %d, want %d（被跳过的视图只应计数一次，不应双计数）", got, want)
	}
}

// TestConvertFunctions_SkippedFunctionsCountedOnce 回归测试：
// 排除列表中的函数只应计数一次（修复前手动 ++ 与 updateProgress 双计数）
func TestConvertFunctions_SkippedFunctionsCountedOnce(t *testing.T) {
	functions := []mysql.FunctionInfo{
		{Name: "func_a"},
		{Name: "func_b"},
	}

	m := newTestManager(len(functions))
	m.config.Conversion.Options.SkipUseFunctionList = true
	m.config.Conversion.Options.SkipFunctionSet = config.StringSet{
		"func_a": {},
		"func_b": {},
	}

	semaphore := make(chan struct{}, 4)
	if err := m.convertFunctions(functions, semaphore); err != nil {
		t.Fatalf("convertFunctions() error = %v", err)
	}

	want := int64(len(functions))
	if got := m.completedTasks.Load(); got != want {
		t.Errorf("completedTasks = %d, want %d（被跳过的函数只应计数一次，不应双计数）", got, want)
	}
}
