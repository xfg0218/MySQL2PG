package postgres

import (
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
