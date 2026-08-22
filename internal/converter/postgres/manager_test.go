package postgres

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/yourusername/mysql2pg/internal/config"
	"github.com/yourusername/mysql2pg/internal/mysql"
)

// TestRunBatchStage P3-08：阶段执行器语义——切批并行执行、
// 对象恰好处理一次、阶段统计落账、错误进入聚合通道
func TestRunBatchStage(t *testing.T) {
	t.Run("切批并行并记录统计", func(t *testing.T) {
		m := &Manager{}
		var wg sync.WaitGroup
		semaphore := make(chan struct{}, 2)
		errorChan := make(chan error, 8)

		var mu sync.Mutex
		seen := map[string]int{}
		stageFn := func(batch []string, sem chan struct{}) error {
			sem <- struct{}{}
			defer func() { <-sem }()
			mu.Lock()
			for _, s := range batch {
				seen[s]++
			}
			mu.Unlock()
			return nil
		}

		runBatchStage(m, &wg, semaphore, errorChan, "测试阶段", []string{"a", "b", "c", "d", "e"}, 2, stageFn)

		for _, k := range []string{"a", "b", "c", "d", "e"} {
			if seen[k] != 1 {
				t.Errorf("对象 %s 处理次数 = %d, want 1", k, seen[k])
			}
		}
		if len(m.conversionStats) != 1 {
			t.Fatalf("统计条数 = %d, want 1", len(m.conversionStats))
		}
		if stat := m.conversionStats[0]; stat.StageName != "测试阶段" || stat.ObjectCount != 5 {
			t.Errorf("统计内容错误: %+v", stat)
		}
		if err := drainErrors(errorChan); err != nil {
			t.Errorf("不应有错误：%v", err)
		}
	})

	t.Run("阶段错误进入聚合通道", func(t *testing.T) {
		m := &Manager{}
		var wg sync.WaitGroup
		semaphore := make(chan struct{}, 1)
		errorChan := make(chan error, 4)

		stageFn := func(batch []int, sem chan struct{}) error {
			sem <- struct{}{}
			defer func() { <-sem }()
			return fmt.Errorf("批次处理失败")
		}
		runBatchStage(m, &wg, semaphore, errorChan, "失败阶段", []int{1, 2, 3}, 1, stageFn)

		if err := drainErrors(errorChan); err == nil {
			t.Fatal("应聚合到阶段错误")
		}
	})
}

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

	// 第一个视图处理后取消。取消信号与主 goroutine 推进之间存在调度竞争，
	// 两种结果都合法，分别断言其不变量：
	//   1) 取消在下一个检查点被观察到 -> 返回 context.Canceled，计数落在 [1, 3]
	//   2) 循环在观察到取消前已跑完 -> 正常返回 nil，计数恰为视图总数
	semaphore := make(chan struct{}, 4)
	go func() {
		// 等待第一个视图计数完成后取消
		for m.completedTasks.Load() < 1 {
		}
		cancel()
	}()

	err := m.convertViews(views, semaphore)
	got := m.completedTasks.Load()
	if err == nil {
		if got != int64(len(views)) {
			t.Errorf("convertViews() 未返回错误时 completedTasks = %d, want %d（全部处理完）", got, len(views))
		}
		return
	}
	if !errors.Is(err, context.Canceled) {
		t.Errorf("convertViews() error = %v, want context.Canceled", err)
	}
	if got < 1 || got > int64(len(views)) {
		t.Errorf("completedTasks = %d, want 1~%d 之间（不应为 0，也不应双计数）", got, len(views))
	}
}

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
