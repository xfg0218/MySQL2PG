package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/yourusername/mysql2pg/internal/config"
	"github.com/yourusername/mysql2pg/internal/mysql"
	"github.com/yourusername/mysql2pg/internal/postgres"
)

// TableDataInconsistency 表数据不一致信息
type TableDataInconsistency struct {
	TableName        string
	MySQLRowCount    int64
	PostgresRowCount int64
}

// ConversionWarning 记录转换过程中的语义降级/丢弃事件（P1-20）
// 任何无法完整迁移而被降级或丢弃的语义都应在此留痕，供迁移报告展示与用户复核
type ConversionWarning struct {
	Category string // 类别：CHECK 约束 / 表达式默认值 / 空间类型 / 函数语法 等
	Object   string // 对象名（表/视图/函数）
	Detail   string // 具体说明
}

// 常量和配置
const (
	defaultBatchSize       = 10000
	defaultBatchInsertSize = 10000
	progressBarWidth       = 40
	progressUpdateInterval = 500 * time.Millisecond
	ansiClearLine          = "\033[2K"
	ansiCarriageReturn     = "\r"
	// streamRowLimit 无主键表采用流式读取的行数上限（P1-06）：
	// go-sql-driver 在客户端缓冲整个结果集，超过该阈值的表回退 OFFSET 分页以控制内存
	streamRowLimit = 2000000
	// maxBatchMemoryBytes 单批次行数据的估算内存预算（P2-07 自适应批大小）：
	// 行宽×行数超过该预算时按比例下调批大小，防止宽表内存尖峰
	maxBatchMemoryBytes = 64 * 1024 * 1024
)

// estimateRowWidth 按列类型估算单行字节宽度（P2-07），保守取大值：
// 变长字符串按声明长度封顶（上限 512），大对象类型统一按 1024 计
func estimateRowWidth(columnTypes map[string]string) int {
	width := 0
	for _, rawType := range columnTypes {
		t := strings.ToLower(strings.TrimSpace(rawType))
		switch {
		case strings.HasPrefix(t, "tinyint"), strings.HasPrefix(t, "smallint"),
			strings.HasPrefix(t, "mediumint"), strings.HasPrefix(t, "int"),
			strings.HasPrefix(t, "bigint"), strings.HasPrefix(t, "year"),
			strings.HasPrefix(t, "bit"), strings.HasPrefix(t, "bool"):
			width += 8
		case strings.HasPrefix(t, "decimal"), strings.HasPrefix(t, "numeric"),
			strings.HasPrefix(t, "double"), strings.HasPrefix(t, "float"), strings.HasPrefix(t, "real"):
			width += 16
		// datetime/timestamp 必须先于 date 判断（前缀包含关系）
		case strings.HasPrefix(t, "datetime"), strings.HasPrefix(t, "timestamp"), strings.HasPrefix(t, "time"):
			width += 12
		case strings.HasPrefix(t, "date"):
			width += 4
		case strings.HasPrefix(t, "char("), strings.HasPrefix(t, "varchar("),
			strings.HasPrefix(t, "binary("), strings.HasPrefix(t, "varbinary("),
			strings.HasPrefix(t, "enum"), strings.HasPrefix(t, "set"):
			length := extractTypeLength(t)
			if length <= 0 {
				length = 64
			}
			if length > 512 {
				length = 512
			}
			width += length
		case strings.HasPrefix(t, "text"), strings.HasPrefix(t, "longtext"),
			strings.HasPrefix(t, "mediumtext"), strings.HasPrefix(t, "tinytext"),
			strings.HasPrefix(t, "blob"), strings.HasPrefix(t, "longblob"),
			strings.HasPrefix(t, "mediumblob"), strings.HasPrefix(t, "tinyblob"),
			strings.HasPrefix(t, "json"):
			width += 1024
		default:
			width += 64
		}
	}
	return width
}

// extractTypeLength 提取 type(n) 形式中的长度 n，无法解析返回 0
func extractTypeLength(t string) int {
	open := strings.IndexByte(t, '(')
	close := strings.IndexByte(t, ')')
	if open == -1 || close <= open {
		return 0
	}
	numStr := t[open+1 : close]
	if comma := strings.IndexByte(numStr, ','); comma != -1 {
		numStr = numStr[:comma]
	}
	n, err := strconv.Atoi(strings.TrimSpace(numStr))
	if err != nil {
		return 0
	}
	return n
}

// adaptiveBatchSizes 按估算行宽对批大小做内存预算封顶（P2-07）：
// 行宽×行数 ≤ maxBatchMemoryBytes 时原样返回，否则按比例下调。
// 返回调整后的（读取批大小, 插入批大小）
func adaptiveBatchSizes(readBatch int64, insertBatch int, rowWidth int) (int64, int) {
	if rowWidth <= 0 {
		return readBatch, insertBatch
	}
	maxRows := int64(maxBatchMemoryBytes) / int64(rowWidth)
	if maxRows < 1 {
		maxRows = 1
	}
	if readBatch > maxRows {
		readBatch = maxRows
	}
	if insertBatch <= 0 || int64(insertBatch) > maxRows {
		insertBatch = int(maxRows)
	}
	return readBatch, insertBatch
}

// progressUpdate 无锁进度更新类型
type progressUpdate struct {
	tableName     string
	processedRows int64
	totalRows     int64
	elapsed       time.Duration
}

// SyncTableData 同步表数据（主协调函数）
// ctx 为根 context（通常来自 signal.NotifyContext）：取消后停止派发新表，
// 已派发表的进行中批次会完整提交后再退出
func SyncTableData(ctx context.Context, mysqlConn *mysql.Connection, postgresConn *postgres.Connection, config *config.Config, log func(format string, args ...interface{}), logError func(errMsg string, args ...interface{}), updateProgress func(), mutex *sync.Mutex, completedTasks *atomic.Int64, totalTasks int, inconsistentTables *[]TableDataInconsistency, tables []mysql.TableInfo, semaphore chan struct{}, progressChan chan progressUpdate, printer *progressPrinter) error {
	// 启动专用进度消费者 goroutine：高频更新合并渲染，通道关闭时渲染最终状态并收尾换行
	progressDone := make(chan struct{})
	go func() {
		consumeProgressUpdates(progressChan, printer)
		close(progressDone)
	}()

	var wg sync.WaitGroup
	// 创建错误通道来捕获goroutine中的错误
	errorChan := make(chan error, len(tables))

	// 已派发的表数量（取消时用于输出进度）
	dispatched := 0
	for _, table := range tables {
		// 取消检查：取消后不再派发新表，等待进行中的批次提交完成
		select {
		case <-ctx.Done():
			log("收到取消信号，停止派发剩余 %d 个表的同步任务，等待进行中的批次提交后退出", len(tables)-dispatched)
		case semaphore <- struct{}{}:
			wg.Add(1)
			dispatched++

			go func(table mysql.TableInfo) {
				defer func() {
					<-semaphore
					updateProgress()
					wg.Done()
				}()

				// 执行单表同步
				err := syncSingleTable(ctx, mysqlConn, postgresConn, config, table, log, logError, mutex, completedTasks, totalTasks, inconsistentTables, progressChan, printer)
				if err != nil {
					select {
					case errorChan <- err:
					default:
					}
				}
			}(table)
			continue
		}
		break
	}

	// 等待所有goroutine完成
	wg.Wait()

	// 关闭进度通道并等待消费者完成
	close(progressChan)
	<-progressDone

	// 关闭错误通道，收集所有错误
	close(errorChan)

	// 聚合所有错误
	var errs []error
	for err := range errorChan {
		errs = append(errs, err)
	}

	// 如果是取消导致的退出，返回取消错误（优先于批次错误聚合）
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("数据同步已被取消（已派发 %d/%d 个表，进行中的批次已完整提交）: %w", dispatched, len(tables), err)
	}

	// 如果有错误发生，返回聚合错误信息
	if len(errs) > 0 {
		// 构建聚合错误信息
		var errMsgs []string
		for _, err := range errs {
			errMsgs = append(errMsgs, err.Error())
		}
		return fmt.Errorf("同步失败 (%d/%d 个表失败):\n  - %s",
			len(errs), len(tables), strings.Join(errMsgs, "\n  - "))
	}

	// 没有错误发生
	return nil
}

// syncSingleTable 同步单个表的数据
// ctx 为根 context：仅用于取消检查与批次前置操作；
// 进行中批次的 DB 操作使用脱离取消信号的批次 context（见 paginateAndInsert）
func syncSingleTable(ctx context.Context, mysqlConn *mysql.Connection, postgresConn *postgres.Connection, config *config.Config, table mysql.TableInfo, log func(format string, args ...interface{}), logError func(errMsg string, args ...interface{}), mutex *sync.Mutex, completedTasks *atomic.Int64, totalTasks int, inconsistentTables *[]TableDataInconsistency, progressChan chan progressUpdate, printer *progressPrinter) error {
	// 获取表列信息
	columns, columnTypes, err := mysqlConn.GetTableColumnsWithTypes(table.Name)
	if err != nil {
		errMsg := fmt.Sprintf("获取表 %s 列信息失败: %v", table.Name, err)
		logError(errMsg)
		return fmt.Errorf("同步表 %s 失败: %w", table.Name, err)
	}

	// 获取表数据总行数
	totalRows, err := mysqlConn.GetTableRowCount(table.Name)
	if err != nil {
		errMsg := fmt.Sprintf("获取表 %s 行数失败: %v", table.Name, err)
		logError(errMsg)
		return fmt.Errorf("同步表 %s 失败: %w", table.Name, err)
	}

	// 如果表为空，处理空表逻辑
	if totalRows == 0 {
		return handleEmptyTable(postgresConn, config, table.Name, table.DDL, totalRows, log, logError, mutex, completedTasks, totalTasks, inconsistentTables, printer)
	}

	// 取消检查：开始实际写入前若已取消则直接退出
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("同步表 %s 已取消: %w", table.Name, err)
	}

	// 清空表数据（根据配置）
	if config.Conversion.Options.TruncateBeforeSync {
		if err := truncateTable(ctx, postgresConn, table.Name, logError); err != nil {
			return fmt.Errorf("同步表 %s 失败: %w", table.Name, err)
		}
	}

	// 分页插入数据
	processedRows, err := paginateAndInsert(ctx, mysqlConn, postgresConn, config, table, columns, columnTypes, totalRows, log, logError, mutex, progressChan)
	if err != nil {
		return err
	}

	// 数据校验
	validationResult, err := validateData(mysqlConn, postgresConn, config, table.Name, totalRows, log, logError, mutex, inconsistentTables)
	if err != nil {
		return err
	}

	// 回填自增列序列：CopyFrom 显式写入 id 不推进 PG 序列，
	// 必须在数据插入完成后执行（TRUNCATE 会将序列重置回初始值）
	backfillAutoIncrementSequence(postgresConn, config, table.Name, table.DDL, log, logError)

	// 显示同步成功信息
	logSyncComplete(config, table.Name, processedRows, validationResult, log, logError, completedTasks, totalTasks, printer)

	return nil
}

// handleEmptyTable 处理空表逻辑
// mysqlDDL 用于解析表级 AUTO_INCREMENT=N：空表也可能带有业务故意设置的自增起始值
func handleEmptyTable(postgresConn *postgres.Connection, config *config.Config, tableName, mysqlDDL string, totalRows int64, log func(format string, args ...interface{}), logError func(errMsg string, args ...interface{}), mutex *sync.Mutex, completedTasks *atomic.Int64, totalTasks int, inconsistentTables *[]TableDataInconsistency, printer *progressPrinter) error {
	log("表 %s 没有数据，跳过同步", tableName)

	// 回填自增列序列：空表也可能通过 AUTO_INCREMENT=N 指定了起始值
	backfillAutoIncrementSequence(postgresConn, config, tableName, mysqlDDL, log, logError)

	// 执行数据校验（如果启用）
	var validationResult string
	if config.Conversion.Options.ValidateData {
		pgRowCount, err := postgresConn.GetTableRowCount(tableName)
		if err != nil {
			errMsg := fmt.Sprintf("校验表 %s 数据失败: %v", tableName, err)
			logError(errMsg)
			return fmt.Errorf("同步表 %s 失败: %w", tableName, err)
		}

		validationResult, err = evaluateRowCountValidation(tableName, totalRows, pgRowCount, config.Conversion.Options.TruncateBeforeSync)
		if validationResult == "数据不一致" {
			mutex.Lock()
			*inconsistentTables = append(*inconsistentTables, TableDataInconsistency{
				TableName:        tableName,
				MySQLRowCount:    totalRows,
				PostgresRowCount: pgRowCount,
			})
			mutex.Unlock()
		}
		if err != nil {
			logError(err.Error())
			return err
		}
	} else {
		validationResult = "跳过验证"
	}

	// 显示同步成功信息
	if config.Run.ShowConsoleLogs {
		completed := completedTasks.Load()
		overallProgress := float64(completed) / float64(totalTasks) * 100
		currentTask := completed + 1
		printer.println("进度: %.2f%% (%d/%d) : 同步表 %s 数据成功，共有 0 行数据，%s", overallProgress, currentTask, totalTasks, tableName, validationResult)
	}

	log("表 %s 同步完成，0 行数据，%s", tableName, validationResult)
	return nil
}

// truncateTable 清空目标表
// ctx 用于取消控制
func truncateTable(ctx context.Context, postgresConn *postgres.Connection, tableName string, logError func(errMsg string, args ...interface{})) error {
	// 开始事务用于清空表
	tx, err := postgresConn.BeginTransaction(ctx)
	if err != nil {
		errMsg := fmt.Sprintf("开始事务失败: %v", err)
		logError(errMsg)
		return fmt.Errorf("开始事务失败: %w", err)
	}

	truncateQuery := fmt.Sprintf("TRUNCATE TABLE \"%s\"", tableName)
	if _, err := tx.Exec(ctx, truncateQuery); err != nil {
		errMsg := fmt.Sprintf("清空表 %s 数据失败: %v", tableName, err)
		logError(errMsg)
		tx.Rollback(ctx)
		return fmt.Errorf("清空表失败: %w", err)
	}

	// 提交清空表的事务
	if err := tx.Commit(ctx); err != nil {
		errMsg := fmt.Sprintf("提交事务失败: %v", err)
		logError(errMsg)
		return fmt.Errorf("提交事务失败: %w", err)
	}

	return nil
}

// paginateAndInsert 分页读取 MySQL 数据并批量插入 PostgreSQL
// ctx 为根 context：每轮批次开始前检查取消信号，取消后不再开启新批次；
// 已开启批次的 DB 操作使用 context.WithoutCancel 派生的批次 context，
// 脱离取消信号，保证进行中的批次完整提交后再退出
func paginateAndInsert(ctx context.Context, mysqlConn *mysql.Connection, postgresConn *postgres.Connection, config *config.Config, table mysql.TableInfo, columns []string, columnTypes map[string]string, totalRows int64, log func(format string, args ...interface{}), logError func(errMsg string, args ...interface{}), mutex *sync.Mutex, progressChan chan progressUpdate) (int64, error) {
	// 获取批量大小配置
	batchSize := int64(config.Conversion.Limits.MaxRowsPerBatch)
	if batchSize <= 0 {
		batchSize = defaultBatchSize
	}

	batchInsertSize := config.Conversion.Limits.BatchInsertSize
	if batchInsertSize <= 0 {
		batchInsertSize = defaultBatchInsertSize
	}

	// P2-07：按估算行宽做内存预算封顶，宽表自动下调批大小，
	// 普通表行宽估算值远低于预算时保持配置值不变
	rowWidth := estimateRowWidth(columnTypes)
	adjustedBatchSize, adjustedInsertSize := adaptiveBatchSizes(batchSize, batchInsertSize, rowWidth)
	if adjustedBatchSize != batchSize {
		log("表 %s 估算行宽较大（约 %d 字节/行），读取批大小自适应调整 %d -> %d", table.Name, rowWidth, batchSize, adjustedBatchSize)
		batchSize = adjustedBatchSize
	}
	if adjustedInsertSize != batchInsertSize {
		batchInsertSize = adjustedInsertSize
	}

	// 尝试使用基于主键的分页
	var lastValue interface{}
	var primaryKey string
	var useKeyPagination bool
	var orderBy string
	// 复合主键分页支持 - 性能优化
	var useCompositeKeyPagination bool = false
	var compositePrimaryKeys []string
	var compositeLastValues []interface{}

	// 使用 GetTablePrimaryKeys 获取所有主键
	primaryKeys, err := mysqlConn.GetTablePrimaryKeys(table.Name)
	if err != nil {
		log("警告: %v，将使用传统的OFFSET分页", err)
		useKeyPagination = false
	} else if len(primaryKeys) == 1 {
		primaryKey = primaryKeys[0]
		log("表 %s 的主键是 %s，将使用基于主键的分页", table.Name, primaryKey)
		useKeyPagination = true
	} else {
		// 复合主键 - 性能优化：使用行构造函数分页替代 OFFSET
		useKeyPagination = true
		useCompositeKeyPagination = true
		compositePrimaryKeys = primaryKeys
		log("表 %s 有复合主键 %v，将使用基于复合主键的分页（行构造函数）", table.Name, primaryKeys)
	}

	// P1-06：无主键表的读取策略
	// 中小表采用流式读取（单次查询 + 迭代分批插入），消除 OFFSET 分页的 O(n²) 扫描；
	// 大表仍用 OFFSET 分页（go-sql-driver 会缓冲整个结果集，流式读取受内存限制）
	var useStreaming bool
	var streamRows *sql.Rows
	defer func() {
		if streamRows != nil {
			streamRows.Close()
		}
	}()
	if !useKeyPagination {
		if totalRows > 0 && totalRows <= streamRowLimit {
			useStreaming = true
			log("表 %s 无主键（%d 行），采用流式读取", table.Name, totalRows)
		} else {
			log("警告: 表 %s 无主键且行数较大（%d 行），采用 OFFSET 分页（复杂度 O(n²)，建议源表添加主键）", table.Name, totalRows)
		}
	}

	// 同步数据
	var processedRows int64

	// 进度条状态跟踪：起始时间用于计算速度与 ETA
	syncStartTime := time.Now()

	for {
		// 取消检查：开启新批次前若已取消则停止，进行中的批次不受影响（完整提交）
		if err := ctx.Err(); err != nil {
			return processedRows, fmt.Errorf("表 %s 同步已取消（已处理 %d 行）: %w", table.Name, processedRows, err)
		}

		// 批次 context：脱离根 context 的取消信号，
		// 保证本轮已开启的批次（MySQL 分页查询 + PG 事务）能完整执行并提交
		batchCtx := context.WithoutCancel(ctx)

		var rows *sql.Rows
		var currentBatchSize int
		var err error

		// 使用分页查询方法
		if useStreaming {
			// 流式读取：首次迭代发起单次全表查询，后续迭代复用同一 rows 迭代器
			if streamRows == nil {
				streamRows, err = mysqlConn.QueryTableRows(batchCtx, table.Name, columns)
				if err != nil {
					errMsg := fmt.Sprintf("流式获取表 %s 数据失败: %v", table.Name, err)
					logError(errMsg)
					return 0, fmt.Errorf("同步表 %s 失败: %w", table.Name, err)
				}
			}
			rows = streamRows
		} else if useCompositeKeyPagination {
			rows, err = mysqlConn.GetTableDataWithCompositeKeyPagination(batchCtx, table.Name, columns, compositePrimaryKeys, compositeLastValues, int(batchSize))
		} else if useKeyPagination {
			rows, err = mysqlConn.GetTableDataWithPagination(batchCtx, table.Name, columns, primaryKey, lastValue, int(batchSize))
		} else {
			rows, err = mysqlConn.GetTableData(batchCtx, table.Name, columns, int(processedRows), int(batchSize), orderBy)
		}
		if err != nil {
			errMsg := fmt.Sprintf("分页获取表 %s 数据失败: %v", table.Name, err)
			logError(errMsg)
			return 0, fmt.Errorf("分页同步表 %s 失败: %w", table.Name, err)
		}

		// 为每个批次开始新事务
		tx, err := postgresConn.BeginTransaction(batchCtx)
		if err != nil {
			errMsg := fmt.Sprintf("开始事务失败: %v", err)
			logError(errMsg)
			rows.Close()
			return 0, fmt.Errorf("同步表 %s 失败: %w", table.Name, err)
		}

		// 使用批量插入并获取实际处理的行数
		if useCompositeKeyPagination && len(compositePrimaryKeys) > 1 {
			currentBatchSize, lastValue, compositeLastValues, err = postgresConn.BatchInsertDataWithCompositeKeys(batchCtx, tx, table.Name, columns, columnTypes, batchInsertSize, compositePrimaryKeys, config.Conversion.Options.LowercaseColumns, rows)
		} else {
			currentBatchSize, lastValue, err = postgresConn.BatchInsertDataWithTransactionAndGetLastValue(batchCtx, tx, table.Name, columns, columnTypes, batchInsertSize, primaryKey, config.Conversion.Options.LowercaseColumns, rows)
		}
		// 流式读取的 rows 需跨批次复用，由 defer 统一关闭
		if !useStreaming {
			rows.Close()
		}

		if err != nil {
			errMsg := fmt.Sprintf("插入表 %s 数据失败: %v", table.Name, err)
			logError(errMsg)
			tx.Rollback(batchCtx)
			return 0, fmt.Errorf("同步表 %s 失败: %w", table.Name, err)
		}

		// 提交当前批次的事务
		if err := tx.Commit(batchCtx); err != nil {
			errMsg := fmt.Sprintf("提交事务失败: %v", err)
			logError(errMsg)
			return 0, fmt.Errorf("同步表 %s 失败: %w", table.Name, err)
		}

		// 更新处理的行数
		if currentBatchSize > 0 {
			processedRows += int64(currentBatchSize)
		} else {
			log("分页同步表 %s 完成，共处理 %d 行数据", table.Name, processedRows)
			break
		}

		// 显示同步进度（无锁 channel 方式）：由 show_progress 开关控制
		if config.Run.ShowProgress {
			select {
			case progressChan <- progressUpdate{table.Name, processedRows, totalRows, time.Since(syncStartTime)}:
			default:
				// 通道满时丢弃，不阻塞工作 goroutine
			}
		}
	}

	return processedRows, nil
}

// progressPrinter 协调数据同步阶段的控制台输出：
// 进度行由专用消费者 goroutine 原地刷新（无尾部换行），其他输出（完成/错误/日志行）
// 打印前必须先结束未收尾的进度行，否则会粘连或被 \033[2K 清行抹掉
type progressPrinter struct {
	mu       sync.Mutex
	w        io.Writer
	terminal bool // stdout 是否为终端：非终端禁用 ANSI 原地刷新
	dirty    bool // 当前是否存在未换行收尾的进度行
}

// newProgressPrinter 创建写入 stdout 的进度输出器，自动检测终端能力
func newProgressPrinter() *progressPrinter {
	return &progressPrinter{w: os.Stdout, terminal: isTerminalOutput(os.Stdout)}
}

// isTerminalOutput 判断文件是否为终端（字符设备），仅用标准库实现
func isTerminalOutput(f *os.File) bool {
	fi, err := f.Stat()
	if err != nil {
		return false
	}
	return fi.Mode()&os.ModeCharDevice != 0
}

// render 原地刷新一行进度（仅终端模式），由专用消费者 goroutine 调用
func (p *progressPrinter) render(update progressUpdate) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.terminal {
		return
	}
	fmt.Fprintf(p.w, "%s%s %s", ansiClearLine, ansiCarriageReturn, formatProgressLine(update))
	p.dirty = true
}

// println 打印普通行：先结束未收尾的进度行，保证输出独占新行
func (p *progressPrinter) println(format string, args ...interface{}) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.dirty {
		fmt.Fprint(p.w, "\n")
		p.dirty = false
	}
	fmt.Fprintf(p.w, format+"\n", args...)
}

// endLine 结束未收尾的进度行（无进度行时为空操作）
func (p *progressPrinter) endLine() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.dirty {
		fmt.Fprint(p.w, "\n")
		p.dirty = false
	}
}

// consumeProgressUpdates 消费进度更新：高频更新只保留最新值（合并渲染），
// 节流间隔到达时渲染；通道关闭后渲染最终状态并换行收尾，保证 100% 状态可见
func consumeProgressUpdates(ch <-chan progressUpdate, p *progressPrinter) {
	var latest *progressUpdate
	var lastRender time.Time
	for update := range ch {
		u := update
		latest = &u
		if time.Since(lastRender) >= progressUpdateInterval {
			p.render(*latest)
			latest = nil
			lastRender = time.Now()
		}
	}
	if latest != nil {
		p.render(*latest)
	}
	p.endLine()
}

// formatProgressLine 生成单行进度文本：百分比 | 表名 | 进度条 | 行数 | 速度 | ETA
func formatProgressLine(update progressUpdate) string {
	progress := float64(update.processedRows) / float64(update.totalRows) * 100
	if progress > 100 {
		progress = 100
	}

	// 计算速度和 ETA
	elapsed := update.elapsed.Seconds()
	var speed float64
	var etaStr string
	if elapsed > 0 {
		speed = float64(update.processedRows) / elapsed
	}
	remainingRows := update.totalRows - update.processedRows
	if speed > 0 && remainingRows > 0 {
		etaSeconds := float64(remainingRows) / speed
		if etaSeconds < 60 {
			etaStr = fmt.Sprintf("%.0fs", etaSeconds)
		} else if etaSeconds < 3600 {
			etaStr = fmt.Sprintf("%dm%ds", int(etaSeconds)/60, int(etaSeconds)%60)
		} else {
			etaStr = fmt.Sprintf("%dh%dm", int(etaSeconds)/3600, (int(etaSeconds)%3600)/60)
		}
	}

	// 生成进度条
	filledLength := int(progress / 100 * float64(progressBarWidth))
	spaceCount := progressBarWidth - filledLength
	if spaceCount < 0 {
		spaceCount = 0
	}
	bar := strings.Repeat("█", filledLength) + strings.Repeat("░", spaceCount)

	speedStr := ""
	if speed > 0 {
		if speed >= 1000 {
			speedStr = fmt.Sprintf("%.1fK rows/s", speed/1000)
		} else {
			speedStr = fmt.Sprintf("%.0f rows/s", speed)
		}
	}

	return fmt.Sprintf("%.1f%% | %s | %s | %s/%s rows | %s | ETA: %s",
		progress,
		update.tableName,
		bar,
		formatRows(update.processedRows),
		formatRows(update.totalRows),
		speedStr,
		etaStr)
}

// formatRows 数字千位分隔符
func formatRows(n int64) string {
	s := fmt.Sprintf("%d", n)
	for i := len(s) - 3; i > 0; i -= 3 {
		s = s[:i] + "," + s[i:]
	}
	return s
}

// validateData 验证数据一致性
func validateData(mysqlConn *mysql.Connection, postgresConn *postgres.Connection, config *config.Config, tableName string, initialRows int64, log func(format string, args ...interface{}), logError func(errMsg string, args ...interface{}), mutex *sync.Mutex, inconsistentTables *[]TableDataInconsistency) (string, error) {
	var validationResult string
	finalMySQLRowCount := initialRows

	if config.Conversion.Options.ValidateData {
		// 尝试重新获取MySQL表行数以进行更准确的校验
		currentMySQLCount, err := mysqlConn.GetTableRowCount(tableName)
		if err == nil {
			finalMySQLRowCount = currentMySQLCount
		} else {
			log("警告: 无法重新获取表 %s 的行数进行校验: %v，将使用初始行数", tableName, err)
		}

		pgRowCount, err := postgresConn.GetTableRowCount(tableName)
		if err != nil {
			errMsg := fmt.Sprintf("校验表 %s 数据失败: %v", tableName, err)
			logError(errMsg)
			return "", fmt.Errorf("同步表 %s 失败: %w", tableName, err)
		}

		validationResult, err = evaluateRowCountValidation(tableName, finalMySQLRowCount, pgRowCount, config.Conversion.Options.TruncateBeforeSync)
		if validationResult == "数据不一致" {
			mutex.Lock()
			*inconsistentTables = append(*inconsistentTables, TableDataInconsistency{
				TableName:        tableName,
				MySQLRowCount:    finalMySQLRowCount,
				PostgresRowCount: pgRowCount,
			})
			mutex.Unlock()
		}
		if err != nil {
			logError(err.Error())
			return validationResult, err
		}
	} else {
		validationResult = "跳过验证"
	}

	return validationResult, nil
}

// evaluateRowCountValidation 评估行数校验结果
// truncateBeforeSync=true 且行数不一致时返回错误（数据被清空后同步仍不一致，
// 属于真实的数据丢失/损坏，必须终止迁移）；truncate=false 时不一致仅记录不阻断
// （追加模式下目标表原有数据会导致行数天然不相等）
func evaluateRowCountValidation(tableName string, mysqlCount, pgCount int64, truncateBeforeSync bool) (string, error) {
	if mysqlCount == pgCount {
		return "数据一致", nil
	}
	if truncateBeforeSync {
		return "数据不一致", fmt.Errorf("表 %s 数据校验不一致: MySQL %d 行, PostgreSQL %d 行 (truncate_before_sync=true，终止迁移)", tableName, mysqlCount, pgCount)
	}
	return "数据不一致", nil
}

// logSyncComplete 记录同步完成信息
func logSyncComplete(config *config.Config, tableName string, processedRows int64, validationResult string, log func(format string, args ...interface{}), logError func(errMsg string, args ...interface{}), completedTasks *atomic.Int64, totalTasks int, printer *progressPrinter) {
	// 显示同步成功信息（根据配置决定是否在控制台显示）
	if config.Run.ShowConsoleLogs {
		completed := completedTasks.Load()
		overallProgress := float64(completed) / float64(totalTasks) * 100
		currentTask := completed + 1
		printer.println("进度: %.2f%% (%d/%d) : 同步表 %s 完成，%d 行数据，%s", overallProgress, currentTask, totalTasks, tableName, processedRows, validationResult)
	}

	// 记录同步完成信息
	// 不能带前导换行：日志按行解析，时间戳必须与内容同行才能被 report parser 识别
	log("分页同步表 %s 完成，%d 行数据，%s", tableName, processedRows, validationResult)
}
