package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"

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

// 辅助函数：将interface{}转换为int64
func toInt64(val interface{}) (int64, bool) {
	switch v := val.(type) {
	case int:
		return int64(v), true
	case int8:
		return int64(v), true
	case int16:
		return int64(v), true
	case int32:
		return int64(v), true
	case int64:
		return v, true
	case uint:
		return int64(v), true
	case uint8:
		return int64(v), true
	case uint16:
		return int64(v), true
	case uint32:
		return int64(v), true
	case uint64:
		// 注意：如果uint64过大可能会溢出int64
		return int64(v), true
	default:
		return 0, false
	}
}

// 定义数据批次结构用于管道传递
type DataBatch struct {
	Data [][]interface{}
	Err  error
}

// 辅助函数：运行单个Reader
func runSingleReader(ctx context.Context, mysqlConn *mysql.Connection, postgresConn *postgres.Connection, table mysql.TableInfo, columns []string, columnTypes map[string]string, batchSize int64, batchInsertSize int, primaryKey string, useKeyPagination bool, orderBy string, batchChan chan<- DataBatch) {
	var processedRows int64
	var lastValue interface{}

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		var rows *sql.Rows
		var err error

		// 使用现有的分页查询方法
		if useKeyPagination {
			// 使用基于主键的分页
			rows, err = mysqlConn.GetTableDataWithPagination(table.Name, columns, primaryKey, lastValue, int(batchSize))
		} else {
			// 使用传统的OFFSET分页
			rows, err = mysqlConn.GetTableData(table.Name, columns, int(processedRows), int(batchSize), orderBy)
		}

		if err != nil {
			select {
			case batchChan <- DataBatch{Err: fmt.Errorf("分页获取表 %s 数据失败: %w", table.Name, err)}:
			case <-ctx.Done():
			}
			return
		}

		// 读取并转换数据
		batchData, batchLastValue, err := postgresConn.ReadBatchFromRows(rows, columns, columnTypes, int(batchInsertSize), primaryKey)
		rows.Close()

		if err != nil {
			select {
			case batchChan <- DataBatch{Err: fmt.Errorf("读取表 %s 数据失败: %w", table.Name, err)}:
			case <-ctx.Done():
			}
			return
		}

		if len(batchData) == 0 {
			break
		}

		// 发送数据到Writer
		select {
		case batchChan <- DataBatch{Data: batchData}:
			processedRows += int64(len(batchData))
			if batchLastValue != nil {
				lastValue = batchLastValue
			}
		case <-ctx.Done():
			return
		}

		// 如果读取的数据少于batchInsertSize，说明已读完
		if len(batchData) < int(batchInsertSize) {
			break
		}
	}
}

// SyncTableData 同步表数据
func SyncTableData(mysqlConn *mysql.Connection, postgresConn *postgres.Connection, config *config.Config, log func(format string, args ...interface{}), logError func(errMsg string), updateProgress func(), mutex *sync.Mutex, completedTasks *int, totalTasks int, inconsistentTables *[]TableDataInconsistency, tables []mysql.TableInfo, semaphore chan struct{}) error {
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
					// 查询PostgreSQL表行数时，根据配置决定是否使用小写表名
					pgTableName := table.Name
					if config.Conversion.Options.LowercaseColumns {
						pgTableName = strings.ToLower(pgTableName)
					}
					pgRowCount, err := postgresConn.GetTableRowCount(pgTableName)
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
					fmt.Printf("\n进度: %.2f%% (%d/%d) : 同步表 %s 数据成功，共有 0 行数据，%s \n", overallProgress, currentTask, totalTasks, table.Name, validationResult)
					mutex.Unlock()
				}
				// 记录同步完成信息
				log("表 %s 同步完成，0 行数据，%s", table.Name, validationResult)
				return
			}

			// 先清空表数据（根据配置决定是否执行）
			// 根据配置决定是否将表名转换为小写
			tableName := table.Name
			if config.Conversion.Options.LowercaseColumns {
				tableName = strings.ToLower(tableName)
			}

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

				truncateQuery := fmt.Sprintf("TRUNCATE TABLE \"%s\"", tableName)
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
				batchSize = 10000 // 默认值，提高到10000以提高性能
			}

			batchInsertSize := config.Conversion.Limits.BatchInsertSize
			if batchInsertSize <= 0 {
				batchInsertSize = 10000 // 默认值，提高到10000以提高性能
			}

			// 尝试使用基于主键的分页
			var primaryKey string
			var useKeyPagination bool
			var orderBy string
			var minId, maxId interface{}

			// 使用 GetTablePrimaryKeys 获取所有主键
			primaryKeys, err := mysqlConn.GetTablePrimaryKeys(table.Name)
			if err != nil {
				log("警告: %v，将使用传统的OFFSET分页", err)
				useKeyPagination = false
			} else if len(primaryKeys) == 1 {
				primaryKey = primaryKeys[0]
				log("表 %s 的主键是 %s，将使用基于主键的分页", table.Name, primaryKey)
				useKeyPagination = true

				// 尝试获取主键范围，用于并发读取
				minId, maxId, err = mysqlConn.GetMinMaxPrimaryKeys(table.Name, primaryKey)
				if err != nil {
					log("警告: 获取表 %s 主键范围失败: %v，将无法使用并发分片读取", table.Name, err)
				}
			} else {
				// 复合主键
				useKeyPagination = false
				// 构建 ORDER BY 子句
				var quotedKeys []string
				for _, k := range primaryKeys {
					quotedKeys = append(quotedKeys, fmt.Sprintf("`%s`", k))
				}
				orderBy = strings.Join(quotedKeys, ", ")
				log("表 %s 有复合主键 %v，将使用传统的OFFSET分页（带ORDER BY）", table.Name, primaryKeys)
			}

			// 同步数据
			var processedRows int64

			// 进度条状态跟踪（减少闪烁）
			type progressState struct {
				lastBarLength int
				lastProgress  float64
			}
			state := &progressState{}

			// 定义数据批次结构用于管道传递
			// DataBatch 在外部已经定义过了

			// 创建缓冲通道用于传输数据批次
			// 缓冲区大小设置为并发数的2倍，允许Reader超前Writer
			batchChan := make(chan DataBatch, config.Conversion.Limits.Concurrency*2)

			// 用于等待Writer完成
			var wgWriter sync.WaitGroup

			// 上下文用于取消操作
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			// 启动Worker Pool进行并行编码和写入
			// 根据配置的并发度或默认值设置worker数量
			numWorkers := config.Conversion.Limits.Concurrency
			if numWorkers <= 0 {
				numWorkers = 4
			}
			// 限制每个表的最大worker数为4，避免过多连接争用
			if numWorkers > 4 {
				numWorkers = 4
			}

			for i := 0; i < numWorkers; i++ {
				wgWriter.Add(1)
				go func() {
					defer wgWriter.Done()

					for batch := range batchChan {
						if batch.Err != nil {
							// 收到错误，传播并退出
							select {
							case errorChan <- batch.Err:
							default:
							}
							cancel() // 通知Reader停止
							return
						}

						if len(batch.Data) == 0 {
							continue
						}

						// 为每个批次开始新事务
						tx, err := postgresConn.BeginTransaction(context.Background())
						if err != nil {
							errMsg := fmt.Sprintf("开始事务失败: %v", err)
							logError(errMsg)
							select {
							case errorChan <- fmt.Errorf("同步表 %s 失败: %w", table.Name, err):
							default:
							}
							cancel()
							return
						}

						// 执行批量写入
						rowsAffected, err := postgresConn.CopyBatchData(tx, tableName, columns, batch.Data, config.Conversion.Options.LowercaseColumns)
						if err != nil {
							errMsg := fmt.Sprintf("插入表 %s 数据失败: %v", table.Name, err)
							logError(errMsg)
							tx.Rollback(context.Background())
							select {
							case errorChan <- fmt.Errorf("同步表 %s 失败: %w", table.Name, err):
							default:
							}
							cancel()
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
							cancel()
							return
						}

						// 更新处理的行数
						atomic.AddInt64(&processedRows, rowsAffected)

						// 显示同步进度
						if config.Run.ShowConsoleLogs {
							currentProcessed := atomic.LoadInt64(&processedRows)
							progress := float64(currentProcessed) / float64(totalRows) * 100
							if progress > 100 {
								progress = 100
							}

							// 只有当进度每变化0.5%时才尝试更新，减少锁竞争
							mutex.Lock()
							if progress-state.lastProgress >= 0.5 {
								// 生成进度条
								barLength := 20
								filledLength := int(progress / 100 * float64(barLength))
								spaceCount := barLength - filledLength - 1
								if spaceCount < 0 {
									spaceCount = 0
								}
								bar := strings.Repeat("-", filledLength) + ">" + strings.Repeat(" ", spaceCount)

								overallProgress := float64(*completedTasks) / float64(totalTasks) * 100
								currentTask := *completedTasks + 1

								fmt.Printf("\033[2K\r进度: %.2f%% (%d/%d) : 同步表 %s [%s] %.2f%%", overallProgress, currentTask, totalTasks, table.Name, bar, progress)
								state.lastProgress = progress
								state.lastBarLength = filledLength
							}
							mutex.Unlock()
						}
					}
				}()
			}

			// Reader Loop (生产者)
			// 如果可以进行并发读取（有单列主键且成功获取了范围）
			if useKeyPagination && minId != nil && maxId != nil {
				// 将范围切分为多个分片
				numReaders := config.Conversion.Limits.Concurrency
				if numReaders <= 0 {
					numReaders = 4
				}

				// 尝试将minId和maxId转换为int64以计算范围
				minInt, okMin := toInt64(minId)
				maxInt, okMax := toInt64(maxId)

				// 只有当主键是数值类型时才能进行简单的范围切分
				if okMin && okMax {
					var wgReaders sync.WaitGroup
					rangeSize := (maxInt - minInt + 1) / int64(numReaders)
					if rangeSize <= 0 {
						rangeSize = 1
						numReaders = 1
					}

					log("启动 %d 个Reader并发读取表 %s (范围: %d-%d)", numReaders, table.Name, minInt, maxInt)

					for r := 0; r < numReaders; r++ {
						start := minInt + int64(r)*rangeSize
						end := start + rangeSize - 1
						if r == numReaders-1 {
							end = maxInt
						}

						wgReaders.Add(1)
						go func(rangeStart, rangeEnd int64) {
							defer wgReaders.Done()

							var lastRangeValue interface{}
							// 在分片内循环读取
							for {
								select {
								case <-ctx.Done():
									return
								default:
								}

								// 读取分片内的一批数据
								rows, err := mysqlConn.GetTableDataInRange(table.Name, columns, primaryKey, rangeStart, rangeEnd, lastRangeValue, int(batchSize))
								if err != nil {
									select {
									case batchChan <- DataBatch{Err: fmt.Errorf("读取分片数据失败: %w", err)}:
									case <-ctx.Done():
									}
									return
								}

								// 读取并转换数据
								batchData, batchLastValue, err := postgresConn.ReadBatchFromRows(rows, columns, columnTypes, int(batchInsertSize), primaryKey)
								rows.Close()

								if err != nil {
									select {
									case batchChan <- DataBatch{Err: fmt.Errorf("转换分片数据失败: %w", err)}:
									case <-ctx.Done():
									}
									return
								}

								if len(batchData) == 0 {
									break
								}

								// 发送数据到Writer
								select {
								case batchChan <- DataBatch{Data: batchData}:
									if batchLastValue != nil {
										lastRangeValue = batchLastValue
									}
								case <-ctx.Done():
									return
								}

								// 如果读取的数据少于batchInsertSize，说明该分片内暂时没有更多数据或已读完
								// 但由于我们在分片内也是分页读取，需要依赖rows数量判断是否结束
								// ReadBatchFromRows 返回的数据量如果小于请求量，通常意味着读完了
								if len(batchData) < int(batchInsertSize) {
									break
								}
							}
						}(start, end)
					}

					wgReaders.Wait()
				} else {
					// 非数值主键，回退到单Reader
					log("主键非数值类型，回退到单Reader模式")
					runSingleReader(ctx, mysqlConn, postgresConn, table, columns, columnTypes, batchSize, batchInsertSize, primaryKey, useKeyPagination, orderBy, batchChan)
				}
			} else {
				// 无法并发读取，使用单Reader
				runSingleReader(ctx, mysqlConn, postgresConn, table, columns, columnTypes, batchSize, batchInsertSize, primaryKey, useKeyPagination, orderBy, batchChan)
			}

			// 关闭通道，通知Writer结束
			close(batchChan)

			// 等待Writer完成
			wgWriter.Wait()

			if ctx.Err() == nil {
				log("分页同步表 %s 完成，共处理 %d 行数据", table.Name, atomic.LoadInt64(&processedRows))
			}

			// 数据校验

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

				// 查询PostgreSQL表行数时，根据配置决定是否使用小写表名
				pgTableName := table.Name
				if config.Conversion.Options.LowercaseColumns {
					pgTableName = strings.ToLower(pgTableName)
				}
				pgRowCount, err := postgresConn.GetTableRowCount(pgTableName)
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
				fmt.Printf("\n进度: %.2f%% (%d/%d) : 同步表 %s 完成，%d 行数据，%s\n", overallProgress, currentTask, totalTasks, table.Name, processedRows, validationResult)
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
