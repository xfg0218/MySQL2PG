package postgres

import (
	"context"
	"database/sql"
	"fmt"
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

// 常量和配置
const (
	defaultBatchSize       = 10000
	defaultBatchInsertSize = 10000
	progressBarWidth       = 40
	progressUpdateInterval = 500 * time.Millisecond
	ansiClearLine          = "\033[2K"
	ansiCarriageReturn     = "\r"
)

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
func SyncTableData(ctx context.Context, mysqlConn *mysql.Connection, postgresConn *postgres.Connection, config *config.Config, log func(format string, args ...interface{}), logError func(errMsg string, args ...interface{}), updateProgress func(), mutex *sync.Mutex, completedTasks *atomic.Int64, totalTasks int, inconsistentTables *[]TableDataInconsistency, tables []mysql.TableInfo, semaphore chan struct{}, progressChan chan progressUpdate) error {
	// 启动专用进度消费者 goroutine
	progressDone := make(chan struct{})
	go func() {
		var lastUpdate time.Time
		for update := range progressChan {
			if time.Since(lastUpdate) >= progressUpdateInterval {
				displayProgressNoLock(update)
				lastUpdate = time.Now()
			}
		}
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
				err := syncSingleTable(ctx, mysqlConn, postgresConn, config, table, log, logError, mutex, completedTasks, totalTasks, inconsistentTables, progressChan)
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
func syncSingleTable(ctx context.Context, mysqlConn *mysql.Connection, postgresConn *postgres.Connection, config *config.Config, table mysql.TableInfo, log func(format string, args ...interface{}), logError func(errMsg string, args ...interface{}), mutex *sync.Mutex, completedTasks *atomic.Int64, totalTasks int, inconsistentTables *[]TableDataInconsistency, progressChan chan progressUpdate) error {
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
		return handleEmptyTable(postgresConn, config, table.Name, table.DDL, totalRows, log, logError, mutex, completedTasks, totalTasks, inconsistentTables)
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
	logSyncComplete(config, table.Name, processedRows, validationResult, log, logError, completedTasks, totalTasks)

	return nil
}

// handleEmptyTable 处理空表逻辑
// mysqlDDL 用于解析表级 AUTO_INCREMENT=N：空表也可能带有业务故意设置的自增起始值
func handleEmptyTable(postgresConn *postgres.Connection, config *config.Config, tableName, mysqlDDL string, totalRows int64, log func(format string, args ...interface{}), logError func(errMsg string, args ...interface{}), mutex *sync.Mutex, completedTasks *atomic.Int64, totalTasks int, inconsistentTables *[]TableDataInconsistency) error {
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
		fmt.Printf("进度: %.2f%% (%d/%d) : 同步表 %s 数据成功，共有 0 行数据，%s \n", overallProgress, currentTask, totalTasks, tableName, validationResult)
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

// 进度条状态跟踪
type progressState struct {
	lastBarLength int
	lastProgress  float64
	syncStartTime time.Time
	totalRows     int64
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

	// 同步数据
	var processedRows int64

	// 进度条状态跟踪
	state := &progressState{
		syncStartTime: time.Now(),
		totalRows:     totalRows,
	}

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
		if useCompositeKeyPagination {
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
		rows.Close()

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

		// 显示同步进度（无锁 channel 方式）
		if config.Run.ShowConsoleLogs {
			select {
			case progressChan <- progressUpdate{table.Name, processedRows, state.totalRows, time.Since(state.syncStartTime)}:
			default:
				// 通道满时丢弃，不阻塞工作 goroutine
			}
		}
	}

	return processedRows, nil
}

// displayProgressNoLock 无锁进度显示（由专用 goroutine 调用）
func displayProgressNoLock(update progressUpdate) {
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
	barLength := progressBarWidth
	filledLength := int(progress / 100 * float64(barLength))
	spaceCount := barLength - filledLength
	if spaceCount < 0 {
		spaceCount = 0
	}
	bar := strings.Repeat("█", filledLength) + strings.Repeat("░", spaceCount)

	// 格式化数字带千位分隔符
	formatRows := func(n int64) string {
		s := fmt.Sprintf("%d", n)
		for i := len(s) - 3; i > 0; i -= 3 {
			s = s[:i] + "," + s[i:]
		}
		return s
	}

	speedStr := ""
	if speed > 0 {
		if speed >= 1000 {
			speedStr = fmt.Sprintf("%.1fK rows/s", speed/1000)
		} else {
			speedStr = fmt.Sprintf("%.0f rows/s", speed)
		}
	}

	fmt.Printf("%s%s📊 %.1f%% | %s | %s | %s/%s rows | %s | ETA: %s",
		ansiClearLine,
		ansiCarriageReturn,
		progress,
		update.tableName,
		bar,
		formatRows(update.processedRows),
		formatRows(update.totalRows),
		speedStr,
		etaStr)
}

// displayProgress 显示同步进度
func displayProgress(tableName string, processedRows int64, totalRows int64, state *progressState, lastProgressUpdate *time.Time, mutex *sync.Mutex) {
	progress := float64(processedRows) / float64(totalRows) * 100
	if progress > 100 {
		progress = 100
	}

	// 计算速度和ETA
	elapsed := time.Since(state.syncStartTime).Seconds()
	var speed float64
	var etaStr string
	if elapsed > 0 {
		speed = float64(processedRows) / elapsed
	}
	remainingRows := totalRows - processedRows
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
	barLength := progressBarWidth
	filledLength := int(progress / 100 * float64(barLength))
	spaceCount := barLength - filledLength
	if spaceCount < 0 {
		spaceCount = 0
	}
	bar := strings.Repeat("█", filledLength) + strings.Repeat("░", spaceCount)

	// 格式化数字带千位分隔符
	formatRows := func(n int64) string {
		s := fmt.Sprintf("%d", n)
		for i := len(s) - 3; i > 0; i -= 3 {
			s = s[:i] + "," + s[i:]
		}
		return s
	}

	// 时间驱动的进度刷新
	now := time.Now()
	if now.Sub(*lastProgressUpdate) >= progressUpdateInterval {
		mutex.Lock()

		speedStr := ""
		if speed > 0 {
			if speed >= 1000 {
				speedStr = fmt.Sprintf("%.1fK rows/s", speed/1000)
			} else {
				speedStr = fmt.Sprintf("%.0f rows/s", speed)
			}
		}

		fmt.Printf("%s%s📊 %.1f%% | %s | %s | %s/%s rows | %s | ETA: %s",
			ansiClearLine,
			ansiCarriageReturn,
			progress,
			tableName,
			bar,
			formatRows(processedRows),
			formatRows(totalRows),
			speedStr,
			etaStr)

		*lastProgressUpdate = now
		mutex.Unlock()
	}
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
func logSyncComplete(config *config.Config, tableName string, processedRows int64, validationResult string, log func(format string, args ...interface{}), logError func(errMsg string, args ...interface{}), completedTasks *atomic.Int64, totalTasks int) {
	// 显示同步成功信息（根据配置决定是否在控制台显示）
	if config.Run.ShowConsoleLogs {
		completed := completedTasks.Load()
		overallProgress := float64(completed) / float64(totalTasks) * 100
		currentTask := completed + 1
		fmt.Printf("进度: %.2f%% (%d/%d) : 同步表 %s 完成，%d 行数据，%s\n", overallProgress, currentTask, totalTasks, tableName, processedRows, validationResult)
	}

	// 记录同步完成信息
	log("\n分页同步表 %s 完成，%d 行数据，%s", tableName, processedRows, validationResult)
}
