package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync"

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

// SyncTableData 同步表数据
// 注意：此函数是从manager.go中提取的，参数保持与原函数一致
func SyncTableData(mysqlConn *mysql.Connection, postgresConn *postgres.Connection, config *config.Config, log func(format string, args ...interface{}), logError func(errMsg string), updateProgress func(), mutex *sync.Mutex, completedTasks *int, totalTasks int, inconsistentTables *[]TableDataInconsistency, tables []mysql.TableInfo, semaphore chan struct{}) error {
	for _, table := range tables {
		semaphore <- struct{}{}

		go func(table mysql.TableInfo) {
			defer func() {
				<-semaphore
				updateProgress()
			}()

			// 获取表列信息
			columns, err := mysqlConn.GetTableColumns(table.Name)
			if err != nil {
				errMsg := fmt.Sprintf("获取表 %s 列信息失败: %v", table.Name, err)
				logError(errMsg)
				return
			}

			// 获取表数据总行数
			totalRows, err := mysqlConn.GetTableRowCount(table.Name)
			if err != nil {
				errMsg := fmt.Sprintf("获取表 %s 行数失败: %v", table.Name, err)
				logError(errMsg)
				return
			}

			// 如果表为空，直接返回
			if totalRows == 0 {
				log("表 %s 没有数据，跳过同步", table.Name)
				return
			}

			// 先清空表数据（根据配置决定是否执行）
			if config.Conversion.Options.TruncateBeforeSync {
				// 开始事务用于清空表
				tx, err := postgresConn.BeginTransaction(context.Background())
				if err != nil {
					errMsg := fmt.Sprintf("开始事务失败: %v", err)
					logError(errMsg)
					return
				}

				truncateQuery := fmt.Sprintf("TRUNCATE TABLE \"%s\"", table.Name)
				if _, err := tx.Exec(context.Background(), truncateQuery); err != nil {
					errMsg := fmt.Sprintf("清空表 %s 数据失败: %v", table.Name, err)
					logError(errMsg)
					tx.Rollback(context.Background())
					return
				}

				// 提交清空表的事务
				if err := tx.Commit(context.Background()); err != nil {
					errMsg := fmt.Sprintf("提交事务失败: %v", err)
					logError(errMsg)
					return
				}
			}

			// 获取批量大小配置
			batchSize := int64(config.Conversion.Limits.MaxRowsPerBatch)
			if batchSize <= 0 {
				batchSize = 10000 // 默认值，提高到10000以提高性能
			}

			batchInsertSize := config.Conversion.Limits.BatchInsertSize
			if batchInsertSize <= 0 {
				batchInsertSize = 10000 // 默认值，提高到10000以提高性能
			}

			// 尝试使用基于主键的分页
			var lastValue interface{}
			var primaryKey string
			var useKeyPagination bool

			primaryKey, err = mysqlConn.GetTablePrimaryKey(table.Name)
			if err != nil {
				log("警告: %v，将使用传统的OFFSET分页", err)
				useKeyPagination = false
			} else {
				log("表 %s 的主键是 %s，将使用基于主键的分页", table.Name, primaryKey)
				useKeyPagination = true
			}

			// 同步数据
			var processedRows int64

			// 进度条状态跟踪（减少闪烁）
			type progressState struct {
				lastBarLength int
				lastProgress  float64
			}
			state := &progressState{}

			for processedRows < totalRows {
				var rows *sql.Rows
				var currentBatchSize int

				// 使用现有的分页查询方法
				if useKeyPagination {
					// 使用基于主键的分页
					rows, err = mysqlConn.GetTableDataWithPagination(table.Name, columns, primaryKey, lastValue, int(batchSize))
				} else {
					// 使用传统的OFFSET分页
					rows, err = mysqlConn.GetTableData(table.Name, columns, int(processedRows), int(batchSize))
				}

				if err != nil {
					errMsg := fmt.Sprintf("获取表 %s 数据失败: %v", table.Name, err)
					logError(errMsg)
					return
				}

				// 为每个批次开始新事务
				tx, err := postgresConn.BeginTransaction(context.Background())
				if err != nil {
					errMsg := fmt.Sprintf("开始事务失败: %v", err)
					logError(errMsg)
					rows.Close()
					return
				}

				// 使用批量插入并获取实际处理的行数
				currentBatchSize, lastValue, err = postgresConn.BatchInsertDataWithTransactionAndGetLastValue(tx, table.Name, columns, batchInsertSize, primaryKey, rows)
				rows.Close() // 确保关闭rows

				if err != nil {
					errMsg := fmt.Sprintf("插入表 %s 数据失败: %v", table.Name, err)
					logError(errMsg)
					tx.Rollback(context.Background())
					return
				}

				// 提交当前批次的事务
				if err := tx.Commit(context.Background()); err != nil {
					errMsg := fmt.Sprintf("提交事务失败: %v", err)
					logError(errMsg)
					return
				}

				// 更新处理的行数
				if currentBatchSize > 0 {
					processedRows += int64(currentBatchSize)
				} else {
					// 没有更多数据，退出循环
					break
				}

				// 显示同步进度
				if config.Run.ShowConsoleLogs {
					progress := float64(processedRows) / float64(totalRows) * 100
					if progress > 100 {
						progress = 100
					}

					// 生成进度条
					barLength := 20
					filledLength := int(progress / 100 * float64(barLength))
					// 确保空格重复次数不会为负数
					spaceCount := barLength - filledLength - 1
					if spaceCount < 0 {
						spaceCount = 0
					}
					bar := strings.Repeat("-", filledLength) + ">" + strings.Repeat(" ", spaceCount)

					// 使用互斥锁保护日志输出
					mutex.Lock()
					overallProgress := float64(*completedTasks) / float64(totalTasks) * 100
					currentTask := *completedTasks + 1

					// 只有当进度条长度或进度百分比变化时才更新（减少闪烁）
					// 当进度条实际长度变化或进度百分比变化超过0.5%时才更新
					if state.lastBarLength != filledLength || progress-state.lastProgress >= 0.5 {
						// 使用ANSI转义序列清除当前行，然后输出新的进度信息
						// \033[2K 清除整个行，\r 回到行首
						fmt.Printf("\033[2K\r进度: %.2f%% (%d/%d) : 同步表 %s [%s] %.2f%%", overallProgress, currentTask, totalTasks, table.Name, bar, progress)
						state.lastBarLength = filledLength
						state.lastProgress = progress
					}
					mutex.Unlock()
				}
			}

			// 数据校验
			var validationResult string
			if config.Conversion.Options.ValidateData {
				pgRowCount, err := postgresConn.GetTableRowCount(table.Name)
				if err != nil {
					errMsg := fmt.Sprintf("校验表 %s 数据失败: %v", table.Name, err)
					logError(errMsg)
					return
				}

				if pgRowCount == totalRows {
					validationResult = "数据一致"
				} else {
					validationResult = fmt.Sprintf("数据不一致")
					mutex.Lock()
					*inconsistentTables = append(*inconsistentTables, TableDataInconsistency{
						TableName:        table.Name,
						MySQLRowCount:    totalRows,
						PostgresRowCount: pgRowCount,
					})
					mutex.Unlock()
				}
			} else {
				validationResult = "跳过验证"
			}

			// 显示同步成功信息（根据配置决定是否在控制台显示）
			if config.Run.ShowConsoleLogs {
				mutex.Lock()
				overallProgress := float64(*completedTasks) / float64(totalTasks) * 100
				currentTask := *completedTasks + 1
				// 先输出一个换行符，确保完成信息显示在新的一行
				fmt.Printf("\n进度: %.2f%% (%d/%d) : 同步表 %s 完成，%d 行数据，%s\n", overallProgress, currentTask, totalTasks, table.Name, totalRows, validationResult)
				mutex.Unlock()
			}

			// 记录同步完成信息
			log("表 %s 同步完成，%d 行数据，%s", table.Name, totalRows, validationResult)
		}(table)
	}

	// 等待所有goroutine完成
	for i := 0; i < cap(semaphore); i++ {
		semaphore <- struct{}{}
	}
	for i := 0; i < cap(semaphore); i++ {
		<-semaphore
	}

	return nil
}
