package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/yourusername/mysql2pg/internal/report"
)

func runReport(args []string) {
	fs := flag.NewFlagSet("report", flag.ExitOnError)
	logFile := fs.String("l", "", "conversion.log 路径 (必需)")
	errorFile := fs.String("e", "", "errors.log 路径 (可选)")
	output := fs.String("o", "", "输出 HTML 路径 (默认自动生成)")
	fs.Usage = func() {
		fmt.Println("用法: mysql2pg report -l <conversion.log> [选项]")
		fmt.Println()
		fmt.Println("从运行日志生成 HTML 迁移报告")
		fmt.Println()
		fmt.Println("选项:")
		fs.PrintDefaults()
		fmt.Println()
		fmt.Println("示例:")
		fmt.Println("  mysql2pg report -l conversion.log")
		fmt.Println("  mysql2pg report -l conversion.log -e errors.log")
		fmt.Println("  mysql2pg report -l conversion.log -o my-report.html")
	}

	if err := fs.Parse(args); err != nil {
		os.Exit(1)
	}

	if *logFile == "" {
		fmt.Println("错误: 必须指定 conversion.log 路径 (-l)")
		fs.Usage()
		os.Exit(1)
	}

	// 解析日志
	rpt, err := report.ParseLog(*logFile)
	if err != nil {
		fmt.Printf("解析日志失败: %v\n", err)
		os.Exit(1)
	}

	// 解析错误日志
	if *errorFile != "" {
		if err := report.ParseErrors(rpt, *errorFile); err != nil {
			fmt.Printf("解析错误日志警告: %v\n", err)
		}
	} else {
		// 自动查找同目录下的 errors.log
		dir := filepath.Dir(*logFile)
		autoErr := filepath.Join(dir, "errors.log")
		if _, err := os.Stat(autoErr); err == nil {
			if err := report.ParseErrors(rpt, autoErr); err != nil {
				fmt.Printf("解析错误日志警告: %v\n", err)
			}
		}
	}

	// 确定输出路径
	if *output == "" {
		now := time.Now().Format("2006-01-02_150405")
		dir := filepath.Dir(*logFile)
		*output = filepath.Join(dir, fmt.Sprintf("report-%s.html", now))
	}

	// 生成 HTML
	if err := report.GenerateHTML(rpt, *output); err != nil {
		fmt.Printf("生成报告失败: %v\n", err)
		os.Exit(1)
	}

	// 输出摘要
	fmt.Printf("✓ 报告已生成: %s\n", *output)
	fmt.Println()
	fmt.Printf("  同步表: %d\n", rpt.TotalTables)
	fmt.Printf("  同步行: %s\n", formatNum(rpt.TotalRows))
	fmt.Printf("  视图: %d\n", rpt.TotalViews)
	fmt.Printf("  索引: %d\n", rpt.TotalIndexes)
	fmt.Printf("  警告: %d\n", len(rpt.Warnings))
	fmt.Printf("  错误: %d\n", len(rpt.Errors))
	fmt.Printf("  不一致表: %d\n", len(rpt.Inconsistent))

	// 进度信息
	if rpt.ProgressTotal > 0 {
		pct := float64(rpt.ProgressCurrent) / float64(rpt.ProgressTotal) * 100
		if rpt.ProgressComplete {
			fmt.Printf("  进度: %.0f%% (%d/%d) ✓ 完成\n", pct, rpt.ProgressCurrent, rpt.ProgressTotal)
		} else {
			fmt.Printf("  进度: %.0f%% (%d/%d) ⏳ 进行中\n", pct, rpt.ProgressCurrent, rpt.ProgressTotal)
		}
	}
	fmt.Println()

	// 如果有警告，给出提示
	if len(rpt.Warnings) > 0 {
		fmt.Println("⚡ 警告:")
		for _, w := range rpt.Warnings {
			fmt.Printf("    %s\n", w)
		}
		fmt.Println()
	}

	// 如果有不一致或错误，给出提示
	if len(rpt.Inconsistent) > 0 {
		fmt.Println("⚠️  以下表数据不一致:")
		for _, inc := range rpt.Inconsistent {
			fmt.Printf("    %s: MySQL=%d, PG=%d\n", inc.Name, inc.MySQLCnt, inc.PGCnt)
		}
		fmt.Println()
	}
	if len(rpt.Errors) > 0 {
		fmt.Printf("❌ 共 %d 个错误，详见报告\n", len(rpt.Errors))
	}
}

func formatNum(n int64) string {
	s := fmt.Sprintf("%d", n)
	for i := len(s) - 3; i > 0; i -= 3 {
		s = s[:i] + "," + s[i:]
	}
	return s
}

// detectReportCommand 检测是否是 report 子命令
// 返回 (isReport, remainingArgs)
func detectReportCommand(args []string) (bool, []string) {
	for i, arg := range args {
		if strings.TrimLeft(arg, "-") == "report" {
			// 移除 report 参数，返回剩余参数
			remaining := make([]string, 0, len(args)-1)
			remaining = append(remaining, args[:i]...)
			remaining = append(remaining, args[i+1:]...)
			return true, remaining
		}
	}
	return false, nil
}

// detectAssessCommand 检测是否是 assess 子命令
// 返回 (isAssess, remainingArgs)
func detectAssessCommand(args []string) (bool, []string) {
	for i, arg := range args {
		if strings.TrimLeft(arg, "-") == "assess" {
			// 移除 assess 参数，返回剩余参数
			remaining := make([]string, 0, len(args)-1)
			remaining = append(remaining, args[:i]...)
			remaining = append(remaining, args[i+1:]...)
			return true, remaining
		}
	}
	return false, nil
}
