package postgres

import (
	"bytes"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"
)

// 基准测试：无锁进度聚合 vs mutex 方案
// 测量目标：工作 goroutine 提交进度更新的延迟
// （不是完整进度更新链路延迟，而是"工作 goroutine 不被阻塞"的能力）

// 模拟当前 mutex 方案的进度显示
var benchmarkMutex = &sync.Mutex{}

func benchmarkDisplayProgressMutex(tableName string, processedRows int64, totalRows int64) {
	progress := float64(processedRows) / float64(totalRows) * 100
	barLength := 40
	filledLength := int(progress / 100 * float64(barLength))
	bar := strings.Repeat("█", filledLength) + strings.Repeat("░", barLength-filledLength)

	benchmarkMutex.Lock()
	fmt.Printf("\r %.1f%% | %s | %s", progress, tableName, bar)
	benchmarkMutex.Unlock()
}

// 模拟无锁 channel 方案的进度更新类型
type benchmarkProgressUpdate struct {
	tableName     string
	processedRows int64
	totalRows     int64
}

func benchmarkDisplayProgressChannel(progressChan chan benchmarkProgressUpdate, tableName string, processedRows int64, totalRows int64) {
	select {
	case progressChan <- benchmarkProgressUpdate{tableName, processedRows, totalRows}:
	default:
		// 丢弃，不阻塞工作 goroutine
	}
}

func BenchmarkDisplayProgress_Mutex(b *testing.B) {
	// 模拟 10 个并发 goroutine 调用 displayProgress
	// 测量 mutex.Lock → fmt.Printf → mutex.Unlock 的耗时
	concurrency := 10
	var wg sync.WaitGroup

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for j := 0; j < b.N; j++ {
				benchmarkDisplayProgressMutex(
					fmt.Sprintf("table_%d", workerID),
					int64(j),
					int64(b.N),
				)
			}
		}(i)
	}
	wg.Wait()
}

func BenchmarkDisplayProgress_Channel(b *testing.B) {
	// 模拟 10 个并发 goroutine 发送进度更新
	// 测量 select { case ch <- update: default: } 的耗时
	// 消费者 goroutine 单独运行，不计入工作 goroutine 延迟
	concurrency := 10
	progressChan := make(chan benchmarkProgressUpdate, concurrency)

	// 启动消费者（不计时）
	done := make(chan struct{})
	go func() {
		for range progressChan {
			// 模拟进度显示（不阻塞发送方）
			time.Sleep(100 * time.Nanosecond)
		}
		close(done)
	}()

	var wg sync.WaitGroup

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for j := 0; j < b.N; j++ {
				benchmarkDisplayProgressChannel(
					progressChan,
					fmt.Sprintf("table_%d", workerID),
					int64(j),
					int64(b.N),
				)
			}
		}(i)
	}
	wg.Wait()

	close(progressChan)
	<-done
}

// TestEstimateRowWidth P2-07：按列类型估算行宽
func TestEstimateRowWidth(t *testing.T) {
	tests := []struct {
		name    string
		types   map[string]string
		wantMin int
		wantMax int
	}{
		{"纯整数列", map[string]string{"a": "bigint(20)", "b": "int"}, 16, 16},
		{"datetime 不误判为 date", map[string]string{"a": "datetime(6)"}, 12, 12},
		{"date 单独计宽", map[string]string{"a": "date"}, 4, 4},
		{"varchar 按声明长度", map[string]string{"a": "varchar(100)"}, 100, 100},
		{"varchar 超长封顶 512", map[string]string{"a": "varchar(4000)"}, 512, 512},
		{"大对象统一 1024", map[string]string{"a": "longtext", "b": "json"}, 2048, 2048},
		{"decimal 计 16", map[string]string{"a": "decimal(10,2)"}, 16, 16},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := estimateRowWidth(tt.types)
			if got < tt.wantMin || got > tt.wantMax {
				t.Errorf("estimateRowWidth(%v) = %d, want [%d, %d]", tt.types, got, tt.wantMin, tt.wantMax)
			}
		})
	}
}

// TestExtractTypeLength 类型长度提取
func TestExtractTypeLength(t *testing.T) {
	tests := []struct {
		input string
		want  int
	}{
		{"varchar(255)", 255},
		{"decimal(10,2)", 10},
		{"text", 0},
		{"varchar(abc)", 0},
	}
	for _, tt := range tests {
		if got := extractTypeLength(tt.input); got != tt.want {
			t.Errorf("extractTypeLength(%q) = %d, want %d", tt.input, got, tt.want)
		}
	}
}

// TestAdaptiveBatchSizes P2-07：内存预算封顶逻辑
func TestAdaptiveBatchSizes(t *testing.T) {
	// 窄表（16 字节/行）：50000 行仅 800KB，远低于预算，保持原值
	readBatch, insertBatch := adaptiveBatchSizes(50000, 50000, 16)
	if readBatch != 50000 || insertBatch != 50000 {
		t.Errorf("窄表不应下调批大小：got (%d, %d)", readBatch, insertBatch)
	}

	// 超宽表（1MB/行）：预算 64MB → 最多 64 行
	readBatch, insertBatch = adaptiveBatchSizes(50000, 50000, 1024*1024)
	wantRows := int64(maxBatchMemoryBytes) / (1024 * 1024)
	if readBatch != wantRows || int64(insertBatch) != wantRows {
		t.Errorf("超宽表应下调至 %d 行：got (%d, %d)", wantRows, readBatch, insertBatch)
	}

	// 极端行宽：至少保留 1 行
	readBatch, insertBatch = adaptiveBatchSizes(50000, 50000, maxBatchMemoryBytes*2)
	if readBatch != 1 || insertBatch != 1 {
		t.Errorf("极端行宽应保底 1 行：got (%d, %d)", readBatch, insertBatch)
	}

	// 行宽为 0（无列类型信息）：原样返回
	readBatch, insertBatch = adaptiveBatchSizes(50000, 50000, 0)
	if readBatch != 50000 || insertBatch != 50000 {
		t.Errorf("零行宽应原样返回：got (%d, %d)", readBatch, insertBatch)
	}
}

// TestEvaluateRowCountValidation 行数校验评估：
// truncate_before_sync=true 且不一致必须返回错误终止迁移（兑现文档承诺），
// truncate=false 时不一致仅记录不阻断
func TestEvaluateRowCountValidation(t *testing.T) {
	tests := []struct {
		name       string
		mysqlCount int64
		pgCount    int64
		truncate   bool
		wantResult string
		wantErr    bool
	}{
		{
			name:       "行数一致",
			mysqlCount: 100,
			pgCount:    100,
			truncate:   true,
			wantResult: "数据一致",
			wantErr:    false,
		},
		{
			name:       "一致时 truncate=false 同样通过",
			mysqlCount: 100,
			pgCount:    100,
			truncate:   false,
			wantResult: "数据一致",
			wantErr:    false,
		},
		{
			name:       "不一致且 truncate=true 必须报错终止",
			mysqlCount: 100,
			pgCount:    80,
			truncate:   true,
			wantResult: "数据不一致",
			wantErr:    true,
		},
		{
			name:       "不一致且 truncate=false 仅记录不阻断",
			mysqlCount: 100,
			pgCount:    150,
			truncate:   false,
			wantResult: "数据不一致",
			wantErr:    false,
		},
		{
			name:       "零行与零行一致",
			mysqlCount: 0,
			pgCount:    0,
			truncate:   true,
			wantResult: "数据一致",
			wantErr:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := evaluateRowCountValidation("t_test", tt.mysqlCount, tt.pgCount, tt.truncate)
			if result != tt.wantResult {
				t.Errorf("result = %q, want %q", result, tt.wantResult)
			}
			if (err != nil) != tt.wantErr {
				t.Errorf("err = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.wantErr {
				if err == nil || !strings.Contains(err.Error(), "t_test") {
					t.Errorf("错误信息应包含表名，实际: %v", err)
				}
				if !strings.Contains(err.Error(), "truncate_before_sync=true") {
					t.Errorf("错误信息应说明终止原因，实际: %v", err)
				}
			}
		})
	}
}

// TestProgressPrinterRenderNonTerminal 非终端模式：原地刷新被禁用，不产生任何输出
func TestProgressPrinterRenderNonTerminal(t *testing.T) {
	var buf bytes.Buffer
	p := &progressPrinter{w: &buf, terminal: false}

	p.render(progressUpdate{tableName: "t1", processedRows: 50, totalRows: 100, elapsed: time.Second})
	if buf.Len() != 0 {
		t.Errorf("非终端模式不应输出进度行，实际: %q", buf.String())
	}

	p.endLine()
	if buf.Len() != 0 {
		t.Errorf("非终端模式 endLine 不应输出，实际: %q", buf.String())
	}

	// println 在非终端模式下仍应正常输出（完成行不依赖 ANSI）
	p.println("done %d", 1)
	if buf.String() != "done 1\n" {
		t.Errorf("非终端模式 println 应正常输出，实际: %q", buf.String())
	}
}

// TestProgressPrinterRenderAndEndLine 终端模式：渲染置脏、进度行不带换行、endLine 收尾
func TestProgressPrinterRenderAndEndLine(t *testing.T) {
	var buf bytes.Buffer
	p := &progressPrinter{w: &buf, terminal: true}

	p.render(progressUpdate{tableName: "t1", processedRows: 50, totalRows: 100, elapsed: time.Second})
	out := buf.String()
	if !strings.HasPrefix(out, ansiClearLine+ansiCarriageReturn) {
		t.Errorf("渲染应以清行+回车开头，实际: %q", out)
	}
	if !strings.Contains(out, "50.0%") || !strings.Contains(out, "t1") {
		t.Errorf("渲染应包含百分比与表名，实际: %q", out)
	}
	if strings.HasSuffix(out, "\n") {
		t.Errorf("进度行不应带换行符，实际: %q", out)
	}

	buf.Reset()
	p.endLine()
	if buf.String() != "\n" {
		t.Errorf("dirty 状态 endLine 应输出换行，实际: %q", buf.String())
	}

	buf.Reset()
	p.endLine()
	if buf.Len() != 0 {
		t.Errorf("干净状态 endLine 不应输出，实际: %q", buf.String())
	}
}

// TestProgressPrinterPrintln 普通行先结束未收尾的进度行再输出
func TestProgressPrinterPrintln(t *testing.T) {
	var buf bytes.Buffer
	p := &progressPrinter{w: &buf, terminal: true}

	// 无进度行时直接输出
	p.println("进度: %.2f%%", 50.0)
	if buf.String() != "进度: 50.00%\n" {
		t.Errorf("println 输出不符，实际: %q", buf.String())
	}

	// 有未收尾进度行时先换行，避免粘连
	buf.Reset()
	p.render(progressUpdate{tableName: "t1", processedRows: 100, totalRows: 100, elapsed: time.Second})
	buf.Reset()
	p.println("done")
	if buf.String() != "\ndone\n" {
		t.Errorf("dirty 状态 println 应先换行再输出，实际: %q", buf.String())
	}
}

// TestConsumeProgressUpdatesFinalRender 通道关闭后渲染最终状态并换行收尾
func TestConsumeProgressUpdatesFinalRender(t *testing.T) {
	var buf bytes.Buffer
	p := &progressPrinter{w: &buf, terminal: true}
	ch := make(chan progressUpdate, 4)

	// 连续快速发送（间隔远小于节流窗口），最终状态必须被渲染
	for i := int64(1); i <= 3; i++ {
		ch <- progressUpdate{tableName: "t1", processedRows: i * 10, totalRows: 30, elapsed: time.Duration(i) * time.Millisecond}
	}
	close(ch)
	consumeProgressUpdates(ch, p)

	out := buf.String()
	if !strings.Contains(out, "100.0%") {
		t.Errorf("关闭后应渲染最终 100.0%% 状态，实际: %q", out)
	}
	if !strings.Contains(out, "30/30 rows") {
		t.Errorf("关闭后应渲染最终行数，实际: %q", out)
	}
	if !strings.HasSuffix(out, "\n") {
		t.Errorf("收尾应换行，实际: %q", out)
	}
}

// TestConsumeProgressUpdatesEmpty 无更新的空通道：不输出任何内容
func TestConsumeProgressUpdatesEmpty(t *testing.T) {
	var buf bytes.Buffer
	p := &progressPrinter{w: &buf, terminal: true}
	ch := make(chan progressUpdate)
	close(ch)

	consumeProgressUpdates(ch, p)
	if buf.Len() != 0 {
		t.Errorf("空通道不应有输出，实际: %q", buf.String())
	}
}

// TestFormatProgressLine 进度行格式：百分比封顶、千位分隔符
func TestFormatProgressLine(t *testing.T) {
	line := formatProgressLine(progressUpdate{tableName: "orders", processedRows: 1234567, totalRows: 1234567, elapsed: 2 * time.Second})
	if !strings.Contains(line, "100.0%") {
		t.Errorf("行数相等应显示 100.0%%，实际: %q", line)
	}
	if !strings.Contains(line, "1,234,567/1,234,567 rows") {
		t.Errorf("千位分隔符格式不符，实际: %q", line)
	}

	// processedRows 超过 totalRows 时百分比封顶 100
	over := formatProgressLine(progressUpdate{tableName: "t", processedRows: 200, totalRows: 100, elapsed: time.Second})
	if !strings.Contains(over, "100.0%") {
		t.Errorf("百分比应封顶 100，实际: %q", over)
	}
}

// TestFormatRows 千位分隔符边界值
func TestFormatRows(t *testing.T) {
	tests := []struct {
		in   int64
		want string
	}{
		{0, "0"},
		{999, "999"},
		{1000, "1,000"},
		{1234567, "1,234,567"},
		{100000000, "100,000,000"},
	}
	for _, tt := range tests {
		if got := formatRows(tt.in); got != tt.want {
			t.Errorf("formatRows(%d) = %q, want %q", tt.in, got, tt.want)
		}
	}
}

// TestIsSortableColumnType JSON 与空间类型不可排序，其余类型可排序
func TestIsSortableColumnType(t *testing.T) {
	sortable := []string{
		"int(11)", "bigint", "varchar(255)", "text", "longtext",
		"datetime", "decimal(10,2)", "enum('a','b')", "TINYINT(1)", "",
	}
	for _, typ := range sortable {
		if !isSortableColumnType(typ) {
			t.Errorf("类型 %q 应可排序", typ)
		}
	}

	unsortable := []string{
		"json", "JSON", "geometry", "point", "linestring", "polygon",
		"multipoint", "multilinestring", "multipolygon",
		"geometrycollection", "GEOMCOLLECTION",
	}
	for _, typ := range unsortable {
		if isSortableColumnType(typ) {
			t.Errorf("类型 %q 应不可排序", typ)
		}
	}
}

// TestBuildOffsetOrderBy 全列排序子句：反引号包裹、内嵌转义、JSON/空间列排除
func TestBuildOffsetOrderBy(t *testing.T) {
	tests := []struct {
		name        string
		columns     []string
		columnTypes map[string]string
		want        string
	}{
		{
			name:        "普通列",
			columns:     []string{"id", "name"},
			columnTypes: map[string]string{"id": "int(11)", "name": "varchar(255)"},
			want:        "`id`, `name`",
		},
		{
			name:        "排除 json 与空间类型",
			columns:     []string{"id", "doc", "geo"},
			columnTypes: map[string]string{"id": "int(11)", "doc": "json", "geo": "point"},
			want:        "`id`",
		},
		{
			name:        "列名内嵌反引号转义",
			columns:     []string{"a`b"},
			columnTypes: map[string]string{"a`b": "int(11)"},
			want:        "`a``b`",
		},
		{
			name:        "全部不可排序返回空",
			columns:     []string{"doc", "geo"},
			columnTypes: map[string]string{"doc": "json", "geo": "geometry"},
			want:        "",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := buildOffsetOrderBy(tt.columns, tt.columnTypes); got != tt.want {
				t.Errorf("buildOffsetOrderBy() = %q, want %q", got, tt.want)
			}
		})
	}
}
