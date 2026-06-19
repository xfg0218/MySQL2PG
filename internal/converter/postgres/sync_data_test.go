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
	fmt.Printf("\r📊 %.1f%% | %s | %s", progress, tableName, bar)
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
