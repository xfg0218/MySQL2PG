package main

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/yourusername/mysql2pg/internal/assessor"
	"github.com/yourusername/mysql2pg/internal/config"
	"github.com/yourusername/mysql2pg/internal/mysql"
	pgconn "github.com/yourusername/mysql2pg/internal/postgres"
)

// runAssess 运行评估命令
func runAssess(args []string) {
	// 解析命令行参数
	var configPath string
	var outputPath string

	for i := 0; i < len(args); i++ {
		switch args[i] {
		case "-o", "--output":
			if i+1 < len(args) {
				outputPath = args[i+1]
				i++
			}
		case "-h", "--help":
			showAssessHelp()
			return
		default:
			if configPath == "" {
				configPath = args[i]
			}
		}
	}

	// 检查配置文件路径
	if configPath == "" {
		fmt.Println("错误：请指定配置文件路径")
		fmt.Println("使用方法：mysql2pg assess config.yml [选项]")
		os.Exit(1)
	}

	// 加载配置
	cfg, err := config.LoadConfig(configPath)
	if err != nil {
		fmt.Printf("加载配置文件失败：%v\n", err)
		os.Exit(1)
	}

	// 验证配置
	if err := cfg.ValidateConfig(); err != nil {
		fmt.Printf("配置验证失败：%v\n", err)
		os.Exit(1)
	}

	// 测试 MySQL 连接
	fmt.Println("测试 MySQL 连接...")
	if err := mysql.TestConnection(&cfg.MySQL); err != nil {
		fmt.Printf("MySQL 连接测试失败：%v\n", err)
		os.Exit(1)
	}
	fmt.Println("✓ MySQL 连接成功")

	// 测试 PostgreSQL 连接
	fmt.Println("测试 PostgreSQL 连接...")
	if err := pgconn.TestConnection(&cfg.PostgreSQL); err != nil {
		fmt.Printf("PostgreSQL 连接测试失败：%v\n", err)
		os.Exit(1)
	}
	fmt.Println("✓ PostgreSQL 连接成功")

	// 创建数据库连接
	mysqlConn, err := mysql.NewConnection(&cfg.MySQL)
	if err != nil {
		fmt.Printf("创建 MySQL 连接失败：%v\n", err)
		os.Exit(1)
	}
	defer mysqlConn.Close()

	postgresConn, err := pgconn.NewConnection(&cfg.PostgreSQL)
	if err != nil {
		fmt.Printf("创建 PostgreSQL 连接失败：%v\n", err)
		os.Exit(1)
	}
	defer postgresConn.Close()

	// 创建评估器（使用现有迁移逻辑进行评估）
	assess, err := assessor.NewMigrationAssessor(mysqlConn, postgresConn, cfg)
	if err != nil {
		fmt.Printf("创建评估器失败：%v\n", err)
		os.Exit(1)
	}

	// 运行评估
	fmt.Println()
	report, err := assess.Run()
	if err != nil {
		fmt.Printf("评估失败：%v\n", err)
		os.Exit(1)
	}

	fmt.Println()
	fmt.Println("✅ 评估完成！本评估使用现有转换逻辑进行实际转换测试，")
	fmt.Println("   仅读取 MySQL 元数据并尝试转换，不会写入 PostgreSQL 数据库。")
	fmt.Println()

	// 生成输出路径
	if outputPath == "" {

		timestamp := time.Now().Format("2006-01-02_150405")
		outputPath = fmt.Sprintf("assessment-%s.html", timestamp)
	}

	// 确保输出路径是绝对路径
	if !filepath.IsAbs(outputPath) {
		outputPath, _ = filepath.Abs(outputPath)
	}

	// 生成 HTML 报告
	fmt.Printf("\n生成评估报告：%s\n", outputPath)
	if err := assessor.GenerateAssessmentHTML(report, outputPath); err != nil {
		fmt.Printf("生成报告失败：%v\n", err)
		os.Exit(1)
	}

	fmt.Printf("✓ 评估报告已生成：%s\n", outputPath)

	// 显示评估摘要
	fmt.Println()
	fmt.Println("+-------------------------------------------------------------+")
	fmt.Println("| 评估摘要                                                    |")
	fmt.Println("+-------------------------------------------------------------+")
	fmt.Printf("| 总体评分：%d/100                                              \n", report.Summary.Score)
	fmt.Printf("| 风险等级：%s                                                   \n", report.Summary.RiskLevel)
	fmt.Printf("| 高风险对象：%d 个                                               \n", len(report.HighRisks))
	fmt.Println("+-------------------------------------------------------------+")
	fmt.Printf("| 表：%d 张 | 视图：%d 个 | 索引：%d 个 | 函数：%d 个              \n",
		report.Summary.TotalTables,
		report.Summary.TotalViews,
		report.Summary.TotalIndexes,
		report.Summary.TotalFunctions)
	fmt.Printf("| 用户：%d 个 | 权限：%d 个                                       \n",
		report.Summary.TotalUsers,
		report.Summary.TotalPrivileges)
	fmt.Println("+-------------------------------------------------------------+")
	fmt.Printf("| 预计迁移时间：%s | 预计数据量：%s                            \n",
		report.Suggestions.EstimatedTime,
		report.Suggestions.EstimatedDataSize)
	fmt.Println("+-------------------------------------------------------------+")

	fmt.Println()
	fmt.Println("评估完成！")
}

// showAssessHelp 显示评估命令帮助
func showAssessHelp() {
	fmt.Println("MySQL2PG 迁移前评估")
	fmt.Println()
	fmt.Println("使用方法:")
	fmt.Println("  mysql2pg assess <配置文件> [选项]")
	fmt.Println()
	fmt.Println("选项:")
	fmt.Println("  -o, --output <path>      指定报告输出路径 (默认：{主机名}-assessment-YYYY-MM-DD_HHMMSS.html)")
	fmt.Println("  -h, --help               显示帮助信息")
	fmt.Println()
	fmt.Println("示例:")
	fmt.Println("  mysql2pg assess config.yml")
	fmt.Println("  mysql2pg assess config.yml -o my-assessment.html")
	fmt.Println("  mysql2pg assess config.yml --output ./reports/assessment.html")
	fmt.Println()
	fmt.Println("评估内容:")
	fmt.Println("  - 连接性检查：测试 MySQL 和 PostgreSQL 连接")
	fmt.Println("  - 对象兼容性检查：表、视图、索引、函数、用户、权限")
	fmt.Println("  - 数据质量检查：空表、大表、特殊数据类型")
	fmt.Println("  - 风险评估：识别高风险对象并提供处理建议")
	fmt.Println("  - 性能预估：预计迁移时间和数据量")
	fmt.Println("  - 配置建议：推荐最优迁移配置参数")
}
