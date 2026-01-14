package main

import (
	"fmt"
	"net/http"
	_ "net/http/pprof"
	"os"
	"strings"

	"github.com/yourusername/mysql2pg/internal/config"
	converter "github.com/yourusername/mysql2pg/internal/converter/postgres"
	"github.com/yourusername/mysql2pg/internal/mysql"
	pgconn "github.com/yourusername/mysql2pg/internal/postgres"
)

func main() {

	// 监听本地 6060 端口，用于性能分析
	go func() {
		http.ListenAndServe("localhost:6060", nil)
	}()

	// 解析命令行参数
	var configPath string
	for i := 1; i < len(os.Args); i++ {
		if os.Args[i] == "-h" || os.Args[i] == "--help" {
			showHelp()
			return
		} else if os.Args[i] == "-c" && i+1 < len(os.Args) {
			configPath = os.Args[i+1]
			i++
		} else if configPath == "" {
			configPath = os.Args[i]
		}
	}

	// 如果没有指定配置文件，显示帮助信息
	if configPath == "" {
		showHelp()
		return
	}

	cfg, err := config.LoadConfig(configPath)
	if err != nil {
		fmt.Printf("加载配置文件失败: %v\n", err)
		os.Exit(1)
	}

	// 验证配置
	if err := cfg.ValidateConfig(); err != nil {
		fmt.Printf("配置验证失败: %v\n", err)
		os.Exit(1)
	}

	// 测试MySQL连接
	if err := mysql.TestConnection(&cfg.MySQL); err != nil {
		fmt.Printf("MySQL连接测试失败: %v\n", err)
		os.Exit(1)
	}

	// 测试PostgreSQL连接
	if err := pgconn.TestConnection(&cfg.PostgreSQL); err != nil {
		fmt.Printf("PostgreSQL连接测试失败: %v\n", err)
		os.Exit(1)
	}

	// 创建MySQL连接
	mysqlConn, err := mysql.NewConnection(&cfg.MySQL)
	if err != nil {
		fmt.Printf("创建MySQL连接失败: %v\n", err)
		os.Exit(1)
	}
	defer mysqlConn.Close()

	postgresConn, err := pgconn.NewConnection(&cfg.PostgreSQL)
	if err != nil {
		fmt.Printf("创建PostgreSQL连接失败: %v\n", err)
		os.Exit(1)
	}
	defer postgresConn.Close()

	// 显示数据库版本信息
	mysqlVersion, err := mysqlConn.GetVersion()
	if err != nil {
		fmt.Printf("获取MySQL版本失败: %v\n", err)
		os.Exit(1)
	}

	postgresVersion, err := postgresConn.GetVersion()
	if err != nil {
		fmt.Printf("获取PostgreSQL版本失败: %v\n", err)
		os.Exit(1)
	}

	// 显示测试连接成功信息
	if cfg.MySQL.TestOnly || cfg.PostgreSQL.TestOnly {
		fmt.Println("\n+-------------------------------------------------------------+")
		if cfg.MySQL.TestOnly {
			fmt.Println("1. MySQL连接测试完成，版本信息已显示，退出程序。")
		}
		if cfg.PostgreSQL.TestOnly {
			fmt.Println("2. PostgreSQL 连接测试完成，版本信息已显示，退出程序。")
		}
	}

	// 使用表格形式显示版本信息
	fmt.Println("+-------------------------------------------------------------+")
	fmt.Println("| 数据库版本信息:                                             |")
	fmt.Println("+--------------+----------------------------------------------+")
	fmt.Println("| 数据库类型   | 版本信息                                     |")
	fmt.Println("+--------------+----------------------------------------------+")

	// 格式化MySQL版本信息
	mysqlInfo := mysqlVersion
	if len(mysqlInfo) > 40 {
		mysqlInfo = mysqlInfo[:37] + "..."
	}
	fmt.Printf("| MySQL       | %-44s |\n", mysqlInfo)

	// 格式化PostgreSQL版本信息，只显示到"PostgreSQL 16.1 on x86_64"
	postgresInfo := postgresVersion
	// 使用更直接的方法截取版本信息
	parts := strings.Split(postgresInfo, " ")
	if len(parts) >= 5 && parts[3] == "on" && strings.HasPrefix(parts[4], "x86_64") {
		// 只截取到"x86_64"部分
		archPart := strings.Split(parts[4], "-")[0]
		postgresInfo = strings.Join(parts[:4], " ") + " " + archPart
	} else if len(postgresInfo) > 40 {
		postgresInfo = postgresInfo[:37] + "..."
	}
	fmt.Printf("| PostgreSQL  | %-44s |\n", postgresInfo)

	fmt.Println("+--------------+----------------------------------------------+")
	fmt.Println()

	// 如果仅测试MySQL连接，退出
	if cfg.MySQL.TestOnly {
		return
	}

	// 如果仅测试PostgreSQL连接，退出
	if cfg.PostgreSQL.TestOnly {
		return
	}

	// 创建转换管理器并运行转换
	manager, err := converter.NewManager(mysqlConn, postgresConn, cfg)
	if err != nil {
		fmt.Printf("创建转换管理器失败: %v\n", err)
		os.Exit(1)
	}
	defer manager.Close()

	if err := manager.Run(); err != nil {
		fmt.Printf("转换失败: %v\n", err)
		os.Exit(1)
	}
}

// showHelp 显示帮助信息
func showHelp() {
	fmt.Println("MySQL2PG - 高性能MySQL到PostgreSQL转换工具")
	fmt.Println("使用方法:")
	fmt.Println("  mysql2pg [配置文件路径]")
	fmt.Println("  mysql2pg -c [配置文件路径]")
	fmt.Println("  mysql2pg -h|--help 显示帮助信息")
	fmt.Println()
	fmt.Println("配置文件说明:")
	fmt.Println("  配置文件为YAML格式，包含MySQL连接信息、PostgreSQL连接信息、转换选项等")
	fmt.Println("  可参考config.example.yml创建配置文件")
	fmt.Println()
	fmt.Println("MySQL连接配置:")
	fmt.Println("  host: MySQL主机地址 (默认: localhost)")
	fmt.Println("  port: MySQL端口 (默认: 3306)")
	fmt.Println("  username: MySQL用户名")
	fmt.Println("  password: MySQL密码")
	fmt.Println("  database: MySQL数据库名")
	fmt.Println("  test_only: 仅测试连接，不执行转换 (默认: false)")
	fmt.Println("  max_open_conns: 连接池配置的最大连接数 (默认: 100)")
	fmt.Println("  max_idle_conns: 连接池配置的最大空闲连接数 (默认: 50)")
	fmt.Println("  conn_max_lifetime: 连接池配置的最大生命周期（秒） (默认: 3600)")
	fmt.Println("  connection_params: MySQL连接参数 (默认: charset=utf8mb4&parseTime=false&interpolateParams=true)")
	fmt.Println()
	fmt.Println("PostgreSQL连接配置:")
	fmt.Println("  host: PostgreSQL主机地址 (默认: localhost)")
	fmt.Println("  port: PostgreSQL端口 (默认: 5432)")
	fmt.Println("  username: PostgreSQL用户名")
	fmt.Println("  password: PostgreSQL密码")
	fmt.Println("  database: PostgreSQL数据库名")
	fmt.Println("  test_only: 仅测试连接，不执行转换 (默认: false)")
	fmt.Println("  max_conns: 连接池配置的最大连接数 (默认: 50)")
	fmt.Println("  pg_connection_params: PostgreSQL连接参数 (默认: search_path=public connect_timeout=100)")
	fmt.Println()
	fmt.Println("转换配置:")
	fmt.Println("  转换选项:")
	fmt.Println("    tableddl: 是否转换表DDL (默认: true)")
	fmt.Println("    data: 是否转换数据 (默认: true)")
	fmt.Println("    view: 是否转换表视图 (默认: false)")
	fmt.Println("    indexes: 是否转换索引 (默认: true)")
	fmt.Println("    functions: 是否转换函数 (默认: true)")
	fmt.Println("    users: 是否转换用户信息 (默认: true)")
	fmt.Println("    table_privileges: 是否转换表权限 (默认: true)")
	fmt.Println("    skip_existing_tables: 如果表在PostgreSQL中已存在则跳过，否则创建 (默认: false)")
	fmt.Println("    use_table_list: 是否使用指定的表列表进行数据同步 (默认: false)")
	fmt.Println("    table_list: 指定要同步的表列表 (当use_table_list为true时生效，格式为[table1,table2,...])")
	fmt.Println("    exclude_use_table_list: 是否使用跳过表列表 (默认: false)")
	fmt.Println("    exclude_table_list: 要跳过的表列表 (当exclude_use_table_list为true时生效)")
	fmt.Println("    lowercase_columns: 控制表字段是否需要转小写 (默认: false)")
	fmt.Println("    validate_data: 同步数据后验证数据一致性 (默认: true)")
	fmt.Println("    truncate_before_sync: 同步前是否清空表数据 (默认: true)")
	fmt.Println()
	fmt.Println("  限制配置:")
	fmt.Println("    concurrency: 并发数限制 (默认: 10)")
	fmt.Println("    bandwidth_mbps: 网络带宽限制(Mbps) (默认: 100)")
	fmt.Println("    max_ddl_per_batch: 一次性转换DDL的个数限制 (默认: 10)")
	fmt.Println("    max_functions_per_batch: 一次性转换function的个数限制 (默认: 5)")
	fmt.Println("    max_indexes_per_batch: 一次性转换index的个数限制 (默认: 20)")
	fmt.Println("    max_users_per_batch: 一次性转换用户的个数限制 (默认: 10)")
	fmt.Println("    max_rows_per_batch: 一次性同步数据的行数限制 (默认: 10000)")
	fmt.Println("    batch_insert_size: 批量插入的大小 (默认: 10000)")
	fmt.Println()
	fmt.Println("运行配置:")
	fmt.Println("  show_progress: 显示任务进度 (默认: true)")
	fmt.Println("  error_log_path: 转换异常保存路径 (默认: ./errors.log)")
	fmt.Println("  enable_file_logging: 是否启用文件日志 (默认: true)")
	fmt.Println("  log_file_path: 日志文件保存路径 (默认: ./conversion.log)")
	fmt.Println("  show_console_logs: 是否在控制台显示日志信息 (默认: true)")
	fmt.Println("  show_log_in_console: 是否在控制台显示Log日志输出 (默认: false)")
	fmt.Println()
	fmt.Println("重要功能说明:")
	fmt.Println("  1. test_only模式: 仅测试数据库连接，不执行转换，连接测试响应时间<1秒")
	fmt.Println("  2. 数据校验: 同步数据后验证MySQL和PostgreSQL的数据一致性，确保数据迁移的完整性")
	fmt.Println("  3. truncate_before_sync选项: 控制是否在同步数据前清空PostgreSQL中的表数据")
	fmt.Println("  4. 数据不一致表统计: 当数据校验失败时，收集并显示所有数据不一致的表信息")
	fmt.Println("  5. 零数据处理: 支持零数据行表的完整同步流程，包括进度显示和验证")
	fmt.Println("  6. 完整的资源清理: 所有数据库连接和资源都会被正确释放，避免资源泄漏")
	fmt.Println("  7. 完善的性能分析: 监听本地 6060 端口，用于性能分析")
}
