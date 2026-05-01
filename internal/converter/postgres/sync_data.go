package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync"
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

// SyncTableData 同步表数据
func SyncTableData(mysqlConn *mysql.Connection, postgresConn *postgres.Connection, config *config.Config, log func(format string, args ...interface{}), logError func(errMsg string, args ...interface{}), updateProgress func(), mutex *sync.Mutex, completedTasks *int, totalTasks int, inconsistentTables *[]TableDataInconsistency, tables []mysql.TableInfo, semaphore chan struct{}) error {
	var wg sync.WaitGroup
	// 创建错误通道来捕获goroutine中的错误
	errorChan := make(chan error, len(tables))

	for _, table := range tables {
		semaphore <- struct{}{}
		wg.Add(1)

		go func(table mysql.TableInfo) {
			defer func() {
				<-semaphore
				updateProgress()
				wg.Done()
			}()

			// 获取表列信息
			columns, columnTypes, err := mysqlConn.GetTableColumnsWithTypes(table.Name)
			if err != nil {
				errMsg := fmt.Sprintf("获取表 %s 列信息失败: %v", table.Name, err)
				logError(errMsg)
				select {
				case errorChan <- fmt.Errorf("同步表 %s 失败: %w", table.Name, err):
				default:
				}
				return
			}

			// 获取表数据总行数
			totalRows, err := mysqlConn.GetTableRowCount(table.Name)
			if err != nil {
				errMsg := fmt.Sprintf("获取表 %s 行数失败: %v", table.Name, err)
				logError(errMsg)
				select {
				case errorChan <- fmt.Errorf("同步表 %s 失败: %w", table.Name, err):
				default:
				}
				return
			}

			// 如果表为空，仍然显示同步信息并更新进度
			if totalRows == 0 {

				log("表 %s 没有数据，跳过同步", table.Name)
				// 执行数据校验（如果启用）
				var validationResult string
				if config.Conversion.Options.ValidateData {
					pgRowCount, err := postgresConn.GetTableRowCount(table.Name)
					if err != nil {
						errMsg := fmt.Sprintf("校验表 %s 数据失败: %v", table.Name, err)
						logError(errMsg)
						select {
						case errorChan <- fmt.Errorf("同步表 %s 失败: %w", table.Name, err):
						default:
						}
						return
					}

					if pgRowCount == totalRows {
						validationResult = "数据一致"
					} else {
						validationResult = "数据不一致"
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

				// 显示同步成功信息
				if config.Run.ShowConsoleLogs {
					mutex.Lock()
					overallProgress := float64(*completedTasks) / float64(totalTasks) * 100
					currentTask := *completedTasks + 1
					fmt.Printf("进度: %.2f%% (%d/%d) : 同步表 %s 数据成功，共有 0 行数据，%s \n", overallProgress, currentTask, totalTasks, table.Name, validationResult)
					mutex.Unlock()
				}
				// 记录同步完成信息
				log("表 %s 同步完成，0 行数据，%s", table.Name, validationResult)
				return
			}

			// 先清空表数据（根据配置决定是否执行）
			if config.Conversion.Options.TruncateBeforeSync {
				// 开始事务用于清空表
				tx, err := postgresConn.BeginTransaction(context.Background())
				if err != nil {
					errMsg := fmt.Sprintf("开始事务失败: %v", err)
					logError(errMsg)
					select {
					case errorChan <- fmt.Errorf("同步表 %s 失败: %w", table.Name, err):
					default:
					}
					return
				}

				truncateQuery := fmt.Sprintf("TRUNCATE TABLE \"%s\"", table.Name)
				if _, err := tx.Exec(context.Background(), truncateQuery); err != nil {
					errMsg := fmt.Sprintf("清空表 %s 数据失败: %v", table.Name, err)
					logError(errMsg)
					tx.Rollback(context.Background())
					select {
					case errorChan <- fmt.Errorf("同步表 %s 失败: %w", table.Name, err):
					default:
					}
					return
				}

				// 提交清空表的事务
				if err := tx.Commit(context.Background()); err != nil {
					errMsg := fmt.Sprintf("提交事务失败: %v", err)
					logError(errMsg)
					select {
					case errorChan <- fmt.Errorf("同步表 %s 失败: %w", table.Name, err):
					default:
					}
					return
				}
			}

			// 获取批量大小配置
			batchSize := int64(config.Conversion.Limits.MaxRowsPerBatch)
			if batchSize <= 0 {
				batchSize = 50000 // 默认值，提高到10000以提高性能
			}

			batchInsertSize := config.Conversion.Limits.BatchInsertSize
			if batchInsertSize <= 0 {
				batchInsertSize = 50000 // 默认值，与 MaxRowsPerBatch 保持一致以减少 CopyFrom 调用次数
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
				// MySQL 8.0+ 支持 WHERE (k1,k2) > (val1,val2) 语法
				useKeyPagination = true
				useCompositeKeyPagination = true
				compositePrimaryKeys = primaryKeys
				log("表 %s 有复合主键 %v，将使用基于复合主键的分页（行构造函数）", table.Name, primaryKeys)
			}

			// 同步数据
			var processedRows int64

			// 进度条状态跟踪（减少闪烁 + 计算速度/ETA）
			type progressState struct {
				lastBarLength int
				lastProgress  float64
				syncStartTime time.Time
				totalRows     int64
			}
			state := &progressState{
				syncStartTime: time.Now(),
				totalRows:     totalRows,
			}

			for {
				var rows *sql.Rows
				var currentBatchSize int

				// 使用分页查询方法 - 性能优化
				if useCompositeKeyPagination {
					// 使用复合主键分页（行构造函数）
					rows, err = mysqlConn.GetTableDataWithCompositeKeyPagination(table.Name, columns, compositePrimaryKeys, compositeLastValues, int(batchSize))
				} else if useKeyPagination {
					// 使用基于单主键的分页
					rows, err = mysqlConn.GetTableDataWithPagination(table.Name, columns, primaryKey, lastValue, int(batchSize))
				} else {
					// 使用传统的 OFFSET 分页（无主键或复合主键降级）
					rows, err = mysqlConn.GetTableData(table.Name, columns, int(processedRows), int(batchSize), orderBy)
				}
				if err != nil {
					errMsg := fmt.Sprintf("分页获取表 %s 数据失败: %v", table.Name, err)
					logError(errMsg)
					select {
					case errorChan <- fmt.Errorf("分页同步表 %s 失败: %w", table.Name, err):
					default:
					}
					return
				}

				// 为每个批次开始新事务
				tx, err := postgresConn.BeginTransaction(context.Background())
				if err != nil {
					errMsg := fmt.Sprintf("开始事务失败: %v", err)
					logError(errMsg)
					rows.Close()
					select {
					case errorChan <- fmt.Errorf("同步表 %s 失败: %w", table.Name, err):
					default:
					}
					return
				}

				// 使用批量插入并获取实际处理的行数
				currentBatchSize, lastValue, err = postgresConn.BatchInsertDataWithTransactionAndGetLastValue(tx, table.Name, columns, columnTypes, batchInsertSize, primaryKey, config.Conversion.Options.LowercaseColumns, rows)
				rows.Close() // 确保关闭rows

				if err != nil {
					errMsg := fmt.Sprintf("插入表 %s 数据失败: %v", table.Name, err)
					logError(errMsg)
					tx.Rollback(context.Background())
					select {
					case errorChan <- fmt.Errorf("同步表 %s 失败: %w", table.Name, err):
					default:
					}
					return
				}

				// 提交当前批次的事务
				if err := tx.Commit(context.Background()); err != nil {
					errMsg := fmt.Sprintf("提交事务失败: %v", err)
					logError(errMsg)
					select {
					case errorChan <- fmt.Errorf("同步表 %s 失败: %w", table.Name, err):
					default:
					}
					return
				}

				// 更新处理的行数
				if currentBatchSize > 0 {
					processedRows += int64(currentBatchSize)
				} else {
					// 没有更多数据，退出循环
					log("分页同步表 %s 完成，共处理 %d 行数据", table.Name, processedRows)
					break
				}

				// 显示同步进度
				if config.Run.ShowConsoleLogs {
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
							etaStr = fmt.Sprintf("%dm%d s", int(etaSeconds)/60, int(etaSeconds)%60)
						} else {
							etaStr = fmt.Sprintf("%dh%dm", int(etaSeconds)/3600, (int(etaSeconds)%3600)/60)
						}
					}

					// 生成更宽的进度条（40字符）
					barLength := 40
					filledLength := int(progress / 100 * float64(barLength))
					spaceCount := barLength - filledLength
					if spaceCount < 0 {
						spaceCount = 0
					}
					// 使用Unicode方块字符更美观
					bar := strings.Repeat("█", filledLength) + strings.Repeat("░", spaceCount)

					// 格式化数字带千位分隔符
					formatRows := func(n int64) string {
						s := fmt.Sprintf("%d", n)
						for i := len(s) - 3; i > 0; i -= 3 {
							s = s[:i] + "," + s[i:]
						}
						return s
					}

					// 使用互斥锁保护日志输出
					mutex.Lock()

					// 只有当进度条长度或进度百分比变化超过0.5%时才更新
					if state.lastBarLength != filledLength || progress-state.lastProgress >= 0.5 {
						// 使用ANSI转义序列清除当前行，然后输出新的进度信息
						// \033[2K 清除整个行，\r 回到行首
						speedStr := ""
						if speed > 0 {
							if speed >= 1000 {
								speedStr = fmt.Sprintf("%.1fK rows/s", speed/1000)
							} else {
								speedStr = fmt.Sprintf("%.0f rows/s", speed)
							}
						}

						fmt.Printf("\033[2K\r📊 %.1f%% | %s | %s | %s/%s rows | %s | ETA: %s",
							progress,
							table.Name,
							bar,
							formatRows(processedRows),
							formatRows(totalRows),
							speedStr,
							etaStr)
						state.lastBarLength = filledLength
						state.lastProgress = progress
					}
					mutex.Unlock()
				}
			}

			// 数据校验
			var validationResult string
			finalMySQLRowCount := totalRows

			if config.Conversion.Options.ValidateData {
				// 尝试重新获取MySQL表行数以进行更准确的校验
				currentMySQLCount, err := mysqlConn.GetTableRowCount(table.Name)
				if err == nil {
					finalMySQLRowCount = currentMySQLCount
				} else {
					log("警告: 无法重新获取表 %s 的行数进行校验: %v，将使用初始行数", table.Name, err)
				}

				pgRowCount, err := postgresConn.GetTableRowCount(table.Name)
				if err != nil {
					errMsg := fmt.Sprintf("校验表 %s 数据失败: %v", table.Name, err)
					logError(errMsg)
					select {
					case errorChan <- fmt.Errorf("同步表 %s 失败: %w", table.Name, err):
					default:
					}
					return
				}

				if pgRowCount == finalMySQLRowCount {
					validationResult = "数据一致"
				} else {
					validationResult = "数据不一致"
					mutex.Lock()
					*inconsistentTables = append(*inconsistentTables, TableDataInconsistency{
						TableName:        table.Name,
						MySQLRowCount:    finalMySQLRowCount,
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
				fmt.Printf("进度: %.2f%% (%d/%d) : 同步表 %s 完成，%d 行数据，%s\n", overallProgress, currentTask, totalTasks, table.Name, processedRows, validationResult)
				mutex.Unlock()
			}

			// 记录同步完成信息
			log("\n分页同步表 %s 完成，%d 行数据，%s", table.Name, processedRows, validationResult)
		}(table)
	}

	// 等待所有goroutine完成
	wg.Wait()

	// 检查是否有错误发生
	select {
	case err := <-errorChan:
		// 返回第一个遇到的错误
		return err
	default:
		// 没有错误发生
		return nil
	}
}
