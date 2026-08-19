package main

import (
	"context"
	"fmt"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/yourusername/mysql2pg/internal/config"
	converter "github.com/yourusername/mysql2pg/internal/converter/postgres"
	"github.com/yourusername/mysql2pg/internal/mysql"
	pgconn "github.com/yourusername/mysql2pg/internal/postgres"
)

// Version 是工具版本号，构建时通过 ldflags 注入
var Version = "dev"

// exitCodeCancelled 用户通过 Ctrl-C / SIGTERM 取消迁移时的退出码（SIGINT 惯例值）
const exitCodeCancelled = 130

func main() {

	// 根 context：支持 Ctrl-C / SIGTERM 信号取消，实现优雅停机
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// 监听本地 6060 端口，用于性能分析
	go func() {
		http.ListenAndServe("localhost:6060", nil)
	}()

	// 检测 assess 子命令
	if isAssess, assessArgs := detectAssessCommand(os.Args[1:]); isAssess {
		runAssess(ctx, assessArgs)
		return
	}

	// 检测 report 子命令
	if isReport, reportArgs := detectReportCommand(os.Args[1:]); isReport {
		runReport(reportArgs)
		return
	}

	// 解析命令行参数
	var configPath string
	for i := 1; i < len(os.Args); i++ {
		if os.Args[i] == "-h" || os.Args[i] == "--help" {
			showHelp()
			return
		} else if os.Args[i] == "-v" || os.Args[i] == "--version" {
			showVersion()
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
	if err := mysql.TestConnection(ctx, &cfg.MySQL); err != nil {
		fmt.Printf("MySQL连接测试失败: %v\n", err)
		os.Exit(1)
	}

	// 测试PostgreSQL连接
	if err := pgconn.TestConnection(ctx, &cfg.PostgreSQL); err != nil {
		fmt.Printf("PostgreSQL连接测试失败: %v\n", err)
		os.Exit(1)
	}

	// 创建MySQL连接
	mysqlConn, err := mysql.NewConnection(ctx, &cfg.MySQL)
	if err != nil {
		fmt.Printf("创建MySQL连接失败: %v\n", err)
		os.Exit(1)
	}
	defer mysqlConn.Close()

	postgresConn, err := pgconn.NewConnection(ctx, &cfg.PostgreSQL)
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
	manager, err := converter.NewManager(ctx, mysqlConn, postgresConn, cfg)
	if err != nil {
		fmt.Printf("创建转换管理器失败: %v\n", err)
		os.Exit(1)
	}
	defer manager.Close()

	if err := manager.Run(); err != nil {
		// 区分用户取消与真正的转换失败
		if ctx.Err() != nil {
			fmt.Printf("\n迁移已被用户取消，已安全停止: %v\n", err)
			os.Exit(exitCodeCancelled)
		}
		fmt.Printf("转换失败: %v\n", err)
		os.Exit(1)
	}
}

// showHelp 显示帮助信息
func showHelp() {
	fmt.Println("MySQL2PG - 高性能 MySQL 到 PostgreSQL 转换工具")
	fmt.Println("使用方法:")
	fmt.Println("  mysql2pg [配置文件路径]")
	fmt.Println("  mysql2pg -c [配置文件路径]")
	fmt.Println("  mysql2pg assess <配置文件> [选项]  迁移前评估")
	fmt.Println("  mysql2pg report -l <conversion.log>  从日志生成 HTML 报告")
	fmt.Println("  mysql2pg -v|--version 显示版本信息")
	fmt.Println("  mysql2pg -h|--help 显示帮助信息")
	fmt.Println()
	fmt.Println("子命令:")
	fmt.Println("  assess   迁移前评估，生成详细的兼容性报告和风险提示（v3.4.0 新增）")
	fmt.Println("           评估模式会分析所有表、视图、函数、索引、用户和权限，")
	fmt.Println("           生成 HTML 评估报告，包含总体评分、风险等级和详细清单。")
	fmt.Println("  report   从转换日志生成 HTML 报告")
	fmt.Println("           支持从 conversion.log 和 errors.log 生成可视化报告，")
	fmt.Println("           包含统计卡片、性能柱状图、表明细和错误警告信息。")
	fmt.Println()
	fmt.Println("配置文件说明:")
	fmt.Println("  配置文件为YAML格式，包含MySQL连接信息、PostgreSQL连接信息、转换选项等")
	fmt.Println("  可参考config.example.yml创建配置文件")
	fmt.Println()
}

// showVersion 显示版本信息
func showVersion() {
	fmt.Printf("mysql2pg version %s\n", Version)
}
