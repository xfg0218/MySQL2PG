package assessor

import (
	"fmt"
	"os"
	"sort"
	"strings"
	"time"
)

// GenerateAssessmentHTML 生成评估报告 HTML 文件
func GenerateAssessmentHTML(report *AssessmentReport, outputPath string) error {
	f, err := os.Create(outputPath)
	if err != nil {
		return fmt.Errorf("创建评估报告文件失败：%w", err)
	}
	defer f.Close()

	now := time.Now().Format("2006-01-02 15:04:05")

	// 计算总体评分颜色
	scoreColor := "#10b981" // green
	if report.Summary.Score < 60 {
		scoreColor = "#ef4444" // red
	} else if report.Summary.Score < 80 {
		scoreColor = "#f59e0b" // amber
	}

	// 风险等级颜色
	riskLevelColor := "#10b981" // green
	if report.Summary.RiskLevel == "中" {
		riskLevelColor = "#f59e0b" // amber
	} else if report.Summary.RiskLevel == "高" {
		riskLevelColor = "#ef4444" // red
	}

	html := fmt.Sprintf(`<!DOCTYPE html>
<html lang="zh-CN">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>MySQL2PG 迁移前评估报告</title>
    <link rel="preconnect" href="https://fonts.googleapis.com">
    <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
    <link href="https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;500;700&family=DM+Sans:wght@400;500;700&display=swap" rel="stylesheet">
    <style>
        :root {
            --bg: #0a0e17;
            --card-bg: #111827;
            --text: #e5e7eb;
            --text-muted: #9ca3af;
            --cyan: #06b6d4;
            --blue: #3b82f6;
            --green: #10b981;
            --red: #ef4444;
            --amber: #f59e0b;
            --purple: #a855f7;
        }
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body {
            font-family: 'DM Sans', system-ui, -apple-system, sans-serif;
            background: var(--bg);
            color: var(--text);
            line-height: 1.6;
            padding: 2rem;
        }
        .container { max-width: 1400px; margin: 0 auto; }
        
        /* Header */
        .header {
            background: linear-gradient(135deg, #1e293b 0%%, #0f172a 100%%);
            border-radius: 12px;
            padding: 2rem;
            margin-bottom: 2rem;
            border: 1px solid #334155;
        }
        .header h1 {
            font-family: 'JetBrains Mono', monospace;
            font-size: 1.75rem;
            color: var(--cyan);
            margin-bottom: 0.5rem;
        }
        .header h1::before { content: "> "; }
        .header-meta {
            display: flex;
            gap: 2rem;
            margin-top: 1rem;
            color: var(--text-muted);
            font-size: 0.9rem;
        }
        .header-meta span {
            display: flex;
            align-items: center;
            gap: 0.5rem;
        }
        
        /* Summary Cards */
        .summary-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 1.5rem;
            margin-bottom: 2rem;
        }
        .summary-card {
            background: var(--card-bg);
            border-radius: 12px;
            padding: 1.5rem;
            border: 1px solid #1f2937;
            position: relative;
            overflow: hidden;
        }
        .summary-card::after {
            content: '';
            position: absolute;
            bottom: 0;
            left: 0;
            right: 0;
            height: 3px;
            background: linear-gradient(90deg, var(--cyan), var(--blue));
        }
        .summary-card .label {
            color: var(--text-muted);
            font-size: 0.85rem;
            margin-bottom: 0.5rem;
        }
        .summary-card .value {
            font-size: 2rem;
            font-weight: 700;
            font-family: 'JetBrains Mono', monospace;
        }
        .summary-card .value.score { color: %s; }
        .summary-card .value.risk { color: %s; }
        
        /* Section */
        .section {
            background: var(--card-bg);
            border-radius: 12px;
            padding: 1.5rem;
            margin-bottom: 1.5rem;
            border: 1px solid #1f2937;
        }
        .section-header {
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 1rem;
            padding-bottom: 0.75rem;
            border-bottom: 1px solid #1f2937;
        }
        .section-title {
            font-family: 'JetBrains Mono', monospace;
            font-size: 1.1rem;
            color: var(--cyan);
        }
        .section-count {
            color: var(--text-muted);
            font-size: 0.85rem;
        }
        
        /* Tables */
        .data-table {
            width: 100%;
            border-collapse: collapse;
            font-size: 0.9rem;
        }
        .data-table thead {
            background: #1f2937;
        }
        .data-table th {
            text-align: left;
            padding: 0.75rem 1rem;
            font-weight: 500;
            color: var(--text-muted);
            font-family: 'JetBrains Mono', monospace;
            font-size: 0.8rem;
            text-transform: uppercase;
        }
        .data-table td {
            padding: 0.75rem 1rem;
            border-bottom: 1px solid #1f2937;
        }
        .data-table tbody tr:hover {
            background: #1f2937;
        }
        .data-table .num {
            font-family: 'JetBrains Mono', monospace;
            text-align: right;
        }
        
        /* Badges */
        .badge {
            display: inline-block;
            padding: 0.25rem 0.75rem;
            border-radius: 9999px;
            font-size: 0.75rem;
            font-weight: 500;
            font-family: 'JetBrains Mono', monospace;
        }
        .badge-none { background: rgba(16, 185, 129, 0.2); color: var(--green); }
        .badge-low { background: rgba(59, 130, 246, 0.2); color: var(--blue); }
        .badge-medium { background: rgba(245, 158, 11, 0.2); color: var(--amber); }
        .badge-high { background: rgba(239, 68, 68, 0.2); color: var(--red); }
        
        /* Risk items */
        .risk-list {
            list-style: none;
            font-size: 0.85rem;
        }
        .risk-list li {
            padding: 0.5rem 0;
            border-bottom: 1px solid #1f2937;
        }
        .risk-list li:last-child { border-bottom: none; }
        .risk-type {
            font-family: 'JetBrains Mono', monospace;
            color: var(--amber);
            margin-right: 0.5rem;
        }
        
        /* Suggestions */
        .suggestion {
            background: rgba(59, 130, 246, 0.1);
            border-left: 3px solid var(--blue);
            padding: 0.75rem 1rem;
            margin-top: 0.5rem;
            font-size: 0.85rem;
            border-radius: 0 4px 4px 0;
        }
        
        /* Config block */
        .config-block {
            background: #0d1117;
            border: 1px solid #30363d;
            border-radius: 8px;
            padding: 1rem;
            font-family: 'JetBrains Mono', monospace;
            font-size: 0.85rem;
            white-space: pre-wrap;
            color: var(--text);
        }
        
        /* Scrollable table container */
        .table-container {
            max-height: 500px;
            overflow-y: auto;
            border: 1px solid #1f2937;
            border-radius: 8px;
        }
        .table-container::-webkit-scrollbar {
            width: 8px;
        }
        .table-container::-webkit-scrollbar-track {
            background: #1f2937;
        }
        .table-container::-webkit-scrollbar-thumb {
            background: #4b5563;
            border-radius: 4px;
        }

        /* Pagination */
        .pagination {
            display: flex;
            justify-content: center;
            align-items: center;
            gap: 0.5rem;
            margin-top: 1rem;
            padding-top: 1rem;
            border-top: 1px solid #1f2937;
        }
        .pagination button {
            background: var(--card-bg);
            border: 1px solid #334155;
            color: var(--text);
            padding: 0.5rem 1rem;
            border-radius: 6px;
            cursor: pointer;
            font-family: 'JetBrains Mono', monospace;
            font-size: 0.85rem;
            transition: all 0.2s;
        }
        .pagination button:hover:not(:disabled) {
            background: var(--cyan);
            border-color: var(--cyan);
            color: var(--bg);
        }
        .pagination button:disabled {
            opacity: 0.5;
            cursor: not-allowed;
        }
        .pagination .page-info {
            color: var(--text-muted);
            font-size: 0.85rem;
            min-width: 100px;
            text-align: center;
        }

        /* List toolbar */
        .list-toolbar {
            display: flex;
            flex-wrap: wrap;
            align-items: center;
            gap: 10px;
            margin-bottom: 12px;
        }
        .list-toolbar input {
            flex: 1;
            min-width: 220px;
            background: var(--bg);
            color: var(--text);
            border: 1px solid #334155;
            border-radius: 6px;
            padding: 8px 10px;
            font-size: 12px;
            font-family: 'JetBrains Mono', monospace;
        }
        .list-toolbar input:focus {
            outline: none;
            border-color: var(--cyan);
        }
        .list-meta {
            font-size: 11px;
            color: var(--text-muted);
            font-family: 'JetBrains Mono', monospace;
        }
        .pager {
            margin-top: 10px;
            display: flex;
            gap: 8px;
            align-items: center;
        }
        .pager button {
            background: var(--card-bg);
            color: var(--text);
            border: 1px solid #334155;
            border-radius: 4px;
            padding: 4px 10px;
            font-size: 12px;
            cursor: pointer;
            font-family: 'JetBrains Mono', monospace;
        }
        .pager button:hover:not(:disabled) {
            background: var(--cyan);
            border-color: var(--cyan);
            color: var(--bg);
        }
        .pager button:disabled {
            opacity: 0.5;
            cursor: not-allowed;
        }
        .pager .pager-info {
            font-size: 11px;
            color: var(--text-muted);
            font-family: 'JetBrains Mono', monospace;
        }
        .section-note {
            font-size: 12px;
            color: var(--text-muted);
            margin-bottom: 10px;
        }
    </style>
</head>
<body>
    <div class="container">`, scoreColor, riskLevelColor)

	// Header
	html += fmt.Sprintf(`
        <div class="header">
            <h1>MySQL2PG 迁移前评估报告</h1>
            <div class="header-meta">
                <span>📅 %s</span>
                <span>🔌 源：%s @ %s:%d</span>
                <span>🎯 目标：%s @ %s:%d</span>
            </div>
            <div class="header-version" style="margin-top: 1rem; padding: 0.75rem 1rem; background: rgba(6, 182, 212, 0.1); border-radius: 8px; border: 1px solid rgba(6, 182, 212, 0.2); font-size: 0.85rem; font-family: 'JetBrains Mono', monospace; color: var(--text);">
                <div style="margin-bottom: 0.5rem;"><span style="color: var(--cyan);">MySQL 版本:</span> <span style="color: var(--text);">%s</span></div>
                <div><span style="color: var(--blue);">PostgreSQL 版本:</span> <span style="color: var(--text);">%s</span></div>
            </div>
        </div>`, now,
		report.SourceDB.Type, report.SourceDB.Host, report.SourceDB.Port,
		report.TargetDB.Type, report.TargetDB.Host, report.TargetDB.Port,
		report.SourceDB.Version, report.TargetDB.Version)

	// Summary cards
	html += fmt.Sprintf(`
        <div class="summary-grid">
            <div class="summary-card">
                <div class="label">总体评分</div>
                <div class="value score">%d/100</div>
            </div>
            <div class="summary-card">
                <div class="label">风险等级</div>
                <div class="value risk">%s</div>
            </div>
            <div class="summary-card">
                <div class="label">表数量</div>
                <div class="value">%d</div>
            </div>
            <div class="summary-card">
                <div class="label">表 DDL 行数</div>
                <div class="value">%d</div>
            </div>
            <div class="summary-card">
                <div class="label">视图数量</div>
                <div class="value">%d</div>
            </div>
            <div class="summary-card">
                <div class="label">视图 DDL 行数</div>
                <div class="value">%d</div>
            </div>
            <div class="summary-card">
                <div class="label">函数数量</div>
                <div class="value">%d</div>
            </div>
            <div class="summary-card">
                <div class="label">函数 DDL 行数</div>
                <div class="value">%d</div>
            </div>
            <div class="summary-card">
                <div class="label">索引数量</div>
                <div class="value">%d</div>
            </div>
            <div class="summary-card">
                <div class="label">索引 DDL 行数</div>
                <div class="value">%d</div>
            </div>
            <div class="summary-card">
                <div class="label">用户数量</div>
                <div class="value">%d</div>
            </div>
            <div class="summary-card">
                <div class="label">权限数量</div>
                <div class="value">%d</div>
            </div>
        </div>`, report.Summary.Score, report.Summary.RiskLevel,
		report.Summary.TotalTables, report.Summary.TotalTableDDLRows,
		report.Summary.TotalViews, report.Summary.TotalViewDDLRows,
		report.Summary.TotalFunctions, report.Summary.TotalFunctionDDLRows,
		report.Summary.TotalIndexes, report.Summary.TotalIndexDDLRows,
		report.Summary.TotalUsers, report.Summary.TotalPrivileges)

	// 高风险对象汇总
	if len(report.HighRisks) > 0 {
		html += fmt.Sprintf(`
        <div class="section">
            <div class="section-header">
                <span class="section-title">⚠️ 高风险对象汇总</span>
                <span class="section-count">%d 个高风险对象</span>
            </div>
            <div class="section-body">
                <div class="section-note">支持搜索和分页，每页 20 条。</div>
                <div class="list-toolbar">
                    <input id="highrisks-search" placeholder="搜索对象名..." />
                    <span class="list-meta" id="highrisks-meta"></span>
                </div>
                <div class="table-wrap">
                    <table class="data-table">
                        <thead>
                            <tr>
                                <th>#</th>
                                <th>类型</th>
                                <th>名称</th>
                                <th>风险描述</th>
                                <th>建议</th>
                            </tr>
                        </thead>
                        <tbody id="highrisks-body"></tbody>
                    </table>
                </div>
                <div class="pager">
                    <button id="highrisks-prev">上一页</button>
                    <button id="highrisks-next">下一页</button>
                    <span class="pager-info" id="highrisks-page"></span>
                </div>
            </div>
        </div>`, len(report.HighRisks))

		// 生成 JSON 数据
		html += `<script id="data-highrisks" type="application/json">[`
		for i, hr := range report.HighRisks {
			if i > 0 {
				html += ","
			}
			html += fmt.Sprintf(`{"objectType":"%s","objectName":"%s","riskDesc":"%s","suggestion":"%s"}`,
				escapeJSON(hr.ObjectType), escapeJSON(hr.ObjectName), escapeJSON(hr.RiskDesc), escapeJSON(hr.Suggestion))
		}
		html += `]</script>`
	}

	// 表详细清单
	html += renderTableSection(report.Tables)

	// 视图详细清单
	html += renderViewSection(report.Views)

	// 索引详细清单
	html += renderIndexSection(report.Indexes)

	// 函数详细清单
	html += renderFunctionSection(report.Functions)

	// 用户详细清单
	html += renderUserSection(report.Users)

	// 表权限详细清单
	html += renderPrivilegeSection(report.Privileges)

	// 性能预估和配置建议
	html += fmt.Sprintf(`
        <div class="section">
            <div class="section-header">
                <span class="section-title">⚡ 性能预估与建议配置</span>
            </div>
            <table class="data-table">
                <thead>
                    <tr>
                        <th>配置项</th>
                        <th>建议值</th>
                        <th>说明</th>
                    </tr>
                </thead>
                <tbody>
                    <tr>
                        <td><strong>预计迁移时间</strong></td>
                        <td>%s</td>
                        <td>基于表结构和数据量的估算</td>
                    </tr>
                    <tr>
                        <td><strong>预计数据量</strong></td>
                        <td>%s</td>
                        <td>需要迁移的总行数</td>
                    </tr>
                    <tr>
                        <td><strong>并发数</strong></td>
                        <td>%d</td>
                        <td>建议的并发迁移协程数</td>
                    </tr>
                    <tr>
                        <td><strong>批处理大小</strong></td>
                        <td>%d</td>
                        <td>每批次读取/写入的行数</td>
                    </tr>
                    <tr>
                        <td><strong>带宽限制</strong></td>
                        <td>%d Mbps</td>
                        <td>网络带宽限制（如需要）</td>
                    </tr>
                </tbody>
            </table>
        </div>`, report.Suggestions.EstimatedTime, report.Suggestions.EstimatedDataSize,
		report.Suggestions.RecommendedConcurrency,
		report.Suggestions.RecommendedBatchSize,
		report.Suggestions.RecommendedBandwidth)

	// Footer
	html += `
    </div>

    <script>
// 表格数据分页和搜索
function initTablePager(tableId, dataId, itemsPerPage) {
    const dataScript = document.getElementById(dataId);
    if (!dataScript) return;
    
    const data = JSON.parse(dataScript.textContent);
    const tbody = document.getElementById(tableId + '-body');
    const searchInput = document.getElementById(tableId + '-search');
    const prevBtn = document.getElementById(tableId + '-prev');
    const nextBtn = document.getElementById(tableId + '-next');
    const pageInfo = document.getElementById(tableId + '-page');
    const metaInfo = document.getElementById(tableId + '-meta');
    
    if (!tbody || !prevBtn || !nextBtn || !pageInfo) return;
    
    let currentPage = 1;
    let filteredData = data.slice();
    const totalPages = Math.ceil(filteredData.length / itemsPerPage);
    
    function render() {
        const start = (currentPage - 1) * itemsPerPage;
        const end = start + itemsPerPage;
        const pageData = filteredData.slice(start, end);
        
        tbody.innerHTML = '';
        pageData.forEach((item, index) => {
            const row = document.createElement('tr');
            const globalIndex = start + index + 1;
            
            // 根据数据类型渲染不同的列
            let cells = '';
            if (item.objectType) {
                // 高风险对象汇总
                const risksHtml = item.riskDesc ? '<div style="font-size:11px;color:var(--text-muted);margin-top:4px;">' + escapeHtml(item.riskDesc) + '</div>' : '';
                const suggestionsHtml = item.suggestion ? '<div style="font-size:11px;color:var(--cyan);margin-top:4px;">💡 ' + escapeHtml(item.suggestion) + '</div>' : '';
                cells = 
                    '<td class="num">' + globalIndex + '</td>' +
                    '<td><span class="badge badge-high">' + escapeHtml(item.objectType) + '</span></td>' +
                    '<td>' + escapeHtml(item.objectName) + '</td>' +
                    '<td>' + risksHtml + '</td>' +
                    '<td>' + suggestionsHtml + '</td>';
            } else if (item.parameters) {
                // 函数
                const risksHtml = item.risks ? '<div style="font-size:11px;color:var(--text-muted);margin-top:4px;">' + escapeHtml(item.risks) + '</div>' : '';
                const suggestionsHtml = item.suggestions ? '<div style="font-size:11px;color:var(--cyan);margin-top:4px;">💡 ' + escapeHtml(item.suggestions) + '</div>' : '';
                cells = 
                    '<td class="num">' + globalIndex + '</td>' +
                    '<td>' + escapeHtml(item.name) + '</td>' +
                    '<td>' + escapeHtml(item.parameters) + '</td>' +
                    '<td class="num">' + item.ddlRows + '</td>' +
                    '<td><span class="badge ' + item.badgeClass + '">' + item.badgeText + '</span></td>' +
                    '<td>' + risksHtml + suggestionsHtml + '</td>';
            } else if (item.host) {
                // 用户
                const risksHtml = item.risks ? '<div style="font-size:11px;color:var(--text-muted);margin-top:4px;">' + escapeHtml(item.risks) + '</div>' : '';
                const suggestionsHtml = item.suggestions ? '<div style="font-size:11px;color:var(--cyan);margin-top:4px;">💡 ' + escapeHtml(item.suggestions) + '</div>' : '';
                cells = 
                    '<td class="num">' + globalIndex + '</td>' +
                    '<td>' + escapeHtml(item.name) + '</td>' +
                    '<td>' + escapeHtml(item.host) + '</td>' +
                    '<td><span class="badge ' + item.badgeClass + '">' + item.badgeText + '</span></td>' +
                    '<td>' + risksHtml + suggestionsHtml + '</td>';
            } else if (item.privileges) {
                // 权限
                const risksHtml = item.risks ? '<div style="font-size:11px;color:var(--text-muted);margin-top:4px;">' + escapeHtml(item.risks) + '</div>' : '';
                const suggestionsHtml = item.suggestions ? '<div style="font-size:11px;color:var(--cyan);margin-top:4px;">💡 ' + escapeHtml(item.suggestions) + '</div>' : '';
                cells = 
                    '<td class="num">' + globalIndex + '</td>' +
                    '<td>' + escapeHtml(item.userName) + '</td>' +
                    '<td>' + escapeHtml(item.tableName) + '</td>' +
                    '<td>' + escapeHtml(item.privileges) + '</td>' +
                    '<td><span class="badge ' + item.badgeClass + '">' + item.badgeText + '</span></td>' +
                    '<td>' + risksHtml + suggestionsHtml + '</td>';
            } else if (item.tableName && item.name) {
                // 索引
                const risksHtml = item.risks ? '<div style="font-size:11px;color:var(--text-muted);margin-top:4px;">' + escapeHtml(item.risks) + '</div>' : '';
                const suggestionsHtml = item.suggestions ? '<div style="font-size:11px;color:var(--cyan);margin-top:4px;">💡 ' + escapeHtml(item.suggestions) + '</div>' : '';
                cells = 
                    '<td class="num">' + globalIndex + '</td>' +
                    '<td>' + escapeHtml(item.name) + '</td>' +
                    '<td>' + escapeHtml(item.tableName) + '</td>' +
                    '<td class="num">' + item.ddlRows + '</td>' +
                    '<td><span class="badge ' + item.badgeClass + '">' + item.badgeText + '</span></td>' +
                    '<td>' + risksHtml + suggestionsHtml + '</td>';
            } else if (item.rows !== undefined) {
                // 表
                const risksHtml = item.risks ? '<div style="font-size:11px;color:var(--text-muted);margin-top:4px;">' + escapeHtml(item.risks) + '</div>' : '';
                const suggestionsHtml = item.suggestions ? '<div style="font-size:11px;color:var(--cyan);margin-top:4px;">💡 ' + escapeHtml(item.suggestions) + '</div>' : '';
                cells = 
                    '<td class="num">' + globalIndex + '</td>' +
                    '<td>' + escapeHtml(item.name) + '</td>' +
                    '<td class="num">' + item.rows + '</td>' +
                    '<td class="num">' + item.ddlRows + '</td>' +
                    '<td><span class="badge ' + item.badgeClass + '">' + item.badgeText + '</span></td>' +
                    '<td>' + risksHtml + suggestionsHtml + '</td>';
            } else {
                // 视图
                const risksHtml = item.risks ? '<div style="font-size:11px;color:var(--text-muted);margin-top:4px;">' + escapeHtml(item.risks) + '</div>' : '';
                const suggestionsHtml = item.suggestions ? '<div style="font-size:11px;color:var(--cyan);margin-top:4px;">💡 ' + escapeHtml(item.suggestions) + '</div>' : '';
                cells = 
                    '<td class="num">' + globalIndex + '</td>' +
                    '<td>' + escapeHtml(item.name) + '</td>' +
                    '<td class="num">' + item.ddlRows + '</td>' +
                    '<td><span class="badge ' + item.badgeClass + '">' + item.badgeText + '</span></td>' +
                    '<td>' + risksHtml + suggestionsHtml + '</td>';
            }
            
            row.innerHTML = cells;
            tbody.appendChild(row);
        });
        
        prevBtn.disabled = currentPage === 1;
        nextBtn.disabled = currentPage === totalPages || totalPages === 0;
        pageInfo.textContent = '第 ' + currentPage + ' / ' + (totalPages || 1) + ' 页';
        if (metaInfo) {
            metaInfo.textContent = filteredData.length + ' / ' + data.length + ' 项';
        }
    }
    
    if (searchInput) {
        searchInput.addEventListener('input', function() {
            const query = this.value.toLowerCase();
            if (query) {
                // 根据数据类型使用不同的搜索字段
                if (data[0] && data[0].objectType) {
                    // 高风险对象：搜索名称
                    filteredData = data.filter(item => item.objectName.toLowerCase().includes(query));
                } else if (data[0] && data[0].privileges) {
                    // 权限：搜索用户名或表名
                    filteredData = data.filter(item => item.userName.toLowerCase().includes(query) || item.tableName.toLowerCase().includes(query));
                } else {
                    // 其他：搜索名称
                    filteredData = data.filter(item => item.name.toLowerCase().includes(query));
                }
            } else {
                filteredData = data.slice();
            }
            currentPage = 1;
            render();
        });
    }
    
    prevBtn.addEventListener('click', function() {
        if (currentPage > 1) {
            currentPage--;
            render();
        }
    });
    
    nextBtn.addEventListener('click', function() {
        if (currentPage < totalPages) {
            currentPage++;
            render();
        }
    });
    
    render();
}

function escapeHtml(text) {
    const div = document.createElement('div');
    div.textContent = text;
    return div.innerHTML;
}

document.addEventListener('DOMContentLoaded', function() {
    initTablePager('highrisks', 'data-highrisks', 20);
    initTablePager('tables', 'data-tables', 20);
    initTablePager('views', 'data-views', 20);
    initTablePager('indexes', 'data-indexes', 20);
    initTablePager('functions', 'data-functions', 20);
    initTablePager('users', 'data-users', 20);
    initTablePager('privileges', 'data-privileges', 20);
});
    </script>

</body>
</html>`

	_, err = f.WriteString(html)
	return err
}

func renderTableSection(tables []TableDetail) string {
	if len(tables) == 0 {
		return ""
	}

	var sb strings.Builder
	sb.WriteString(`
        <div class="section">
            <div class="section-header">
                <span class="section-title">📋 表详细清单</span>
                <span class="section-count">`)
	sb.WriteString(fmt.Sprintf("%d 张表，%d 行", len(tables), countTotalRows(tables)))
	sb.WriteString(`</span>
            </div>
            <div class="section-body">
                <div class="section-note">支持搜索和分页，每页 20 条。</div>
                <div class="list-toolbar">
                    <input id="tables-search" placeholder="搜索表名..." />
                    <span class="list-meta" id="tables-meta"></span>
                </div>
                <div class="table-wrap">
                    <table class="data-table">
                        <thead>
                            <tr>
                                <th>#</th>
                                <th>表名</th>
                                <th class="num">行数</th>
                                <th class="num">DDL 行数</th>
                                <th>风险等级</th>
                                <th>风险与建议</th>
                            </tr>
                        </thead>
                        <tbody id="tables-body"></tbody>
                    </table>
                </div>
                <div class="pager">
                    <button id="tables-prev">上一页</button>
                    <button id="tables-next">下一页</button>
                    <span class="pager-info" id="tables-page"></span>
                </div>
            </div>
        </div>`)

	// 按风险等级排序：高风险优先
	sortedTables := make([]TableDetail, len(tables))
	copy(sortedTables, tables)
	sort.SliceStable(sortedTables, func(i, j int) bool {
		return riskPriority(sortedTables[i].RiskLevel) > riskPriority(sortedTables[j].RiskLevel)
	})

	// 生成 JSON 数据
	sb.WriteString(`<script id="data-tables" type="application/json">[`)
	for i, t := range sortedTables {
		badgeClass := "badge-none"
		badgeText := "无风险"
		if t.RiskLevel == RiskLevelMedium {
			badgeClass = "badge-medium"
			badgeText = "中风险"
		} else if t.RiskLevel == RiskLevelHigh {
			badgeClass = "badge-high"
			badgeText = "高风险"
		}

		risks := ""
		if len(t.Risks) > 0 {
			for _, r := range t.Risks {
				if risks != "" {
					risks += "; "
				}
				risks += fmt.Sprintf("%s: %s", r.Type, r.Description)
			}
		}

		rowCount := "-"
		if t.RowCount >= 0 {
			rowCount = fmt.Sprintf("%d", t.RowCount)
		}

		if i > 0 {
			sb.WriteString(",")
		}
		sb.WriteString(fmt.Sprintf(`{"name":"%s","rows":"%s","ddlRows":"%d","riskLevel":"%s","badgeClass":"%s","badgeText":"%s","risks":"%s","suggestions":"%s"}`,
			escapeJSON(t.Name), rowCount, t.DDLRows, t.RiskLevel, badgeClass, badgeText, escapeJSON(risks), escapeJSON(t.Suggestions)))
	}
	sb.WriteString(`]</script>`)

	return sb.String()
}

func renderViewSection(views []ViewDetail) string {
	if len(views) == 0 {
		return ""
	}

	var sb strings.Builder
	sb.WriteString(`
        <div class="section">
            <div class="section-header">
                <span class="section-title">👁️ 视图详细清单</span>
                <span class="section-count">`)
	sb.WriteString(fmt.Sprintf("%d 个视图，%d 行", len(views), countTotalRows(views)))
	sb.WriteString(`</span>
            </div>
            <div class="section-body">
                <div class="section-note">支持搜索和分页，每页 20 条。</div>
                <div class="list-toolbar">
                    <input id="views-search" placeholder="搜索视图名..." />
                    <span class="list-meta" id="views-meta"></span>
                </div>
                <div class="table-wrap">
                    <table class="data-table">
                        <thead>
                            <tr>
                                <th>#</th>
                                <th>视图名</th>
                                <th class="num">DDL 行数</th>
                                <th>风险等级</th>
                                <th>风险与建议</th>
                            </tr>
                        </thead>
                        <tbody id="views-body"></tbody>
                    </table>
                </div>
                <div class="pager">
                    <button id="views-prev">上一页</button>
                    <button id="views-next">下一页</button>
                    <span class="pager-info" id="views-page"></span>
                </div>
            </div>
        </div>`)

	// 按风险等级排序：高风险优先
	sortedViews := make([]ViewDetail, len(views))
	copy(sortedViews, views)
	sort.SliceStable(sortedViews, func(i, j int) bool {
		return riskPriority(sortedViews[i].RiskLevel) > riskPriority(sortedViews[j].RiskLevel)
	})

	// 生成 JSON 数据
	sb.WriteString(`<script id="data-views" type="application/json">[`)
	for i, v := range sortedViews {
		badgeClass := "badge-none"
		badgeText := "无风险"
		if v.RiskLevel == RiskLevelMedium {
			badgeClass = "badge-medium"
			badgeText = "中风险"
		} else if v.RiskLevel == RiskLevelHigh {
			badgeClass = "badge-high"
			badgeText = "高风险"
		}

		risks := ""
		if len(v.Risks) > 0 {
			for _, r := range v.Risks {
				if risks != "" {
					risks += "; "
				}
				risks += fmt.Sprintf("%s: %s", r.Type, r.Description)
			}
		}

		if i > 0 {
			sb.WriteString(",")
		}
		sb.WriteString(fmt.Sprintf(`{"name":"%s","ddlRows":"%d","riskLevel":"%s","badgeClass":"%s","badgeText":"%s","risks":"%s","suggestions":"%s"}`,
			escapeJSON(v.Name), v.DDLRows, v.RiskLevel, badgeClass, badgeText, escapeJSON(risks), escapeJSON(v.Suggestions)))
	}
	sb.WriteString(`]</script>`)

	return sb.String()
}

func renderIndexSection(indexes []IndexDetail) string {
	if len(indexes) == 0 {
		return ""
	}

	var sb strings.Builder
	sb.WriteString(`
        <div class="section">
            <div class="section-header">
                <span class="section-title">🔖 索引详细清单</span>
                <span class="section-count">`)
	sb.WriteString(fmt.Sprintf("%d 个索引，%d 行", len(indexes), countTotalRows(indexes)))
	sb.WriteString(`</span>
            </div>
            <div class="section-body">
                <div class="section-note">支持搜索和分页，每页 20 条。</div>
                <div class="list-toolbar">
                    <input id="indexes-search" placeholder="搜索索引名..." />
                    <span class="list-meta" id="indexes-meta"></span>
                </div>
                <div class="table-wrap">
                    <table class="data-table">
                        <thead>
                            <tr>
                                <th>#</th>
                                <th>索引名</th>
                                <th>所属表</th>
                                <th class="num">DDL 行数</th>
                                <th>风险等级</th>
                                <th>风险与建议</th>
                            </tr>
                        </thead>
                        <tbody id="indexes-body"></tbody>
                    </table>
                </div>
                <div class="pager">
                    <button id="indexes-prev">上一页</button>
                    <button id="indexes-next">下一页</button>
                    <span class="pager-info" id="indexes-page"></span>
                </div>
            </div>
        </div>`)

	// 生成 JSON 数据
	sb.WriteString(`<script id="data-indexes" type="application/json">[`)
	for i, idx := range indexes {
		badgeClass := "badge-none"
		badgeText := "无风险"
		if idx.RiskLevel == RiskLevelMedium {
			badgeClass = "badge-medium"
			badgeText = "中风险"
		} else if idx.RiskLevel == RiskLevelHigh {
			badgeClass = "badge-high"
			badgeText = "高风险"
		}

		risks := ""
		if len(idx.Risks) > 0 {
			for _, r := range idx.Risks {
				if risks != "" {
					risks += "; "
				}
				risks += fmt.Sprintf("%s: %s", r.Type, r.Description)
			}
		}

		if i > 0 {
			sb.WriteString(",")
		}
		sb.WriteString(fmt.Sprintf(`{"name":"%s","tableName":"%s","ddlRows":"%d","riskLevel":"%s","badgeClass":"%s","badgeText":"%s","risks":"%s","suggestions":"%s"}`,
			escapeJSON(idx.Name), escapeJSON(idx.TableName), idx.DDLRows, idx.RiskLevel, badgeClass, badgeText, escapeJSON(risks), escapeJSON(idx.Suggestions)))
	}
	sb.WriteString(`]</script>`)

	return sb.String()
}

func renderFunctionSection(functions []FunctionDetail) string {
	if len(functions) == 0 {
		return ""
	}

	var sb strings.Builder
	sb.WriteString(`
        <div class="section">
            <div class="section-header">
                <span class="section-title">⚡ 函数详细清单</span>
                <span class="section-count">`)
	sb.WriteString(fmt.Sprintf("%d 个函数，%d 行", len(functions), countTotalRows(functions)))
	sb.WriteString(`</span>
            </div>
            <div class="section-body">
                <div class="section-note">支持搜索和分页，每页 20 条。</div>
                <div class="list-toolbar">
                    <input id="functions-search" placeholder="搜索函数名..." />
                    <span class="list-meta" id="functions-meta"></span>
                </div>
                <div class="table-wrap">
                    <table class="data-table">
                        <thead>
                            <tr>
                                <th>#</th>
                                <th>函数名</th>
                                <th>参数</th>
                                <th class="num">DDL 行数</th>
                                <th>风险等级</th>
                                <th>风险与建议</th>
                            </tr>
                        </thead>
                        <tbody id="functions-body"></tbody>
                    </table>
                </div>
                <div class="pager">
                    <button id="functions-prev">上一页</button>
                    <button id="functions-next">下一页</button>
                    <span class="pager-info" id="functions-page"></span>
                </div>
            </div>
        </div>`)

	// 按风险等级排序：高风险优先
	sortedFuncs := make([]FunctionDetail, len(functions))
	copy(sortedFuncs, functions)
	sort.SliceStable(sortedFuncs, func(i, j int) bool {
		return riskPriority(sortedFuncs[i].RiskLevel) > riskPriority(sortedFuncs[j].RiskLevel)
	})

	// 生成 JSON 数据
	sb.WriteString(`<script id="data-functions" type="application/json">[`)
	for i, fn := range sortedFuncs {
		badgeClass := "badge-none"
		badgeText := "无风险"
		if fn.RiskLevel == RiskLevelMedium {
			badgeClass = "badge-medium"
			badgeText = "中风险"
		} else if fn.RiskLevel == RiskLevelHigh {
			badgeClass = "badge-high"
			badgeText = "高风险"
		}

		risks := ""
		if len(fn.Risks) > 0 {
			for _, r := range fn.Risks {
				if risks != "" {
					risks += "; "
				}
				risks += fmt.Sprintf("%s: %s", r.Type, r.Description)
			}
		}

		if i > 0 {
			sb.WriteString(",")
		}
		sb.WriteString(fmt.Sprintf(`{"name":"%s","parameters":"%s","ddlRows":"%d","riskLevel":"%s","badgeClass":"%s","badgeText":"%s","risks":"%s","suggestions":"%s"}`,
			escapeJSON(fn.Name), escapeJSON(fn.Parameters), fn.DDLRows, fn.RiskLevel, badgeClass, badgeText, escapeJSON(risks), escapeJSON(fn.Suggestions)))
	}
	sb.WriteString(`]</script>`)

	return sb.String()
}

func renderUserSection(users []UserDetail) string {
	if len(users) == 0 {
		return ""
	}

	var sb strings.Builder
	sb.WriteString(`
        <div class="section">
            <div class="section-header">
                <span class="section-title">👤 用户详细清单</span>
                <span class="section-count">`)
	sb.WriteString(fmt.Sprintf("%d 个用户", len(users)))
	sb.WriteString(`</span>
            </div>
            <div class="section-body">
                <div class="section-note">支持搜索和分页，每页 20 条。</div>
                <div class="list-toolbar">
                    <input id="users-search" placeholder="搜索用户名..." />
                    <span class="list-meta" id="users-meta"></span>
                </div>
                <div class="table-wrap">
                    <table class="data-table">
                        <thead>
                            <tr>
                                <th>#</th>
                                <th>用户名</th>
                                <th>主机</th>
                                <th>风险等级</th>
                                <th>风险与建议</th>
                            </tr>
                        </thead>
                        <tbody id="users-body"></tbody>
                    </table>
                </div>
                <div class="pager">
                    <button id="users-prev">上一页</button>
                    <button id="users-next">下一页</button>
                    <span class="pager-info" id="users-page"></span>
                </div>
            </div>
        </div>`)

	// 生成 JSON 数据
	sb.WriteString(`<script id="data-users" type="application/json">[`)
	for i, u := range users {
		badgeClass := "badge-none"
		badgeText := "无风险"
		if u.RiskLevel == RiskLevelMedium {
			badgeClass = "badge-medium"
			badgeText = "中风险"
		} else if u.RiskLevel == RiskLevelHigh {
			badgeClass = "badge-high"
			badgeText = "高风险"
		}

		risks := ""
		if len(u.Risks) > 0 {
			for _, r := range u.Risks {
				if risks != "" {
					risks += "; "
				}
				risks += fmt.Sprintf("%s: %s", r.Type, r.Description)
			}
		}

		if i > 0 {
			sb.WriteString(",")
		}
		sb.WriteString(fmt.Sprintf(`{"name":"%s","host":"%s","riskLevel":"%s","badgeClass":"%s","badgeText":"%s","risks":"%s","suggestions":"%s"}`,
			escapeJSON(u.Name), escapeJSON(u.Host), u.RiskLevel, badgeClass, badgeText, escapeJSON(risks), escapeJSON(u.Suggestions)))
	}
	sb.WriteString(`]</script>`)

	return sb.String()
}

func renderPrivilegeSection(privileges []PrivilegeDetail) string {
	if len(privileges) == 0 {
		return ""
	}

	var sb strings.Builder
	sb.WriteString(`
        <div class="section">
            <div class="section-header">
                <span class="section-title">🔐 表权限详细清单</span>
                <span class="section-count">`)
	sb.WriteString(fmt.Sprintf("%d 个权限", len(privileges)))
	sb.WriteString(`</span>
            </div>
            <div class="section-body">
                <div class="section-note">支持搜索和分页，每页 20 条。</div>
                <div class="list-toolbar">
                    <input id="privileges-search" placeholder="搜索用户名或表名..." />
                    <span class="list-meta" id="privileges-meta"></span>
                </div>
                <div class="table-wrap">
                    <table class="data-table">
                        <thead>
                            <tr>
                                <th>#</th>
                                <th>用户名</th>
                                <th>表名</th>
                                <th>权限</th>
                                <th>风险等级</th>
                                <th>风险与建议</th>
                            </tr>
                        </thead>
                        <tbody id="privileges-body"></tbody>
                    </table>
                </div>
                <div class="pager">
                    <button id="privileges-prev">上一页</button>
                    <button id="privileges-next">下一页</button>
                    <span class="pager-info" id="privileges-page"></span>
                </div>
            </div>
        </div>`)

	// 生成 JSON 数据
	sb.WriteString(`<script id="data-privileges" type="application/json">[`)
	for i, p := range privileges {
		badgeClass := "badge-none"
		badgeText := "无风险"
		if p.RiskLevel == RiskLevelMedium {
			badgeClass = "badge-medium"
			badgeText = "中风险"
		} else if p.RiskLevel == RiskLevelHigh {
			badgeClass = "badge-high"
			badgeText = "高风险"
		}

		risks := ""
		if len(p.Risks) > 0 {
			for _, r := range p.Risks {
				if risks != "" {
					risks += "; "
				}
				risks += fmt.Sprintf("%s: %s", r.Type, r.Description)
			}
		}

		if i > 0 {
			sb.WriteString(",")
		}
		sb.WriteString(fmt.Sprintf(`{"userName":"%s","tableName":"%s","privileges":"%s","riskLevel":"%s","badgeClass":"%s","badgeText":"%s","risks":"%s","suggestions":"%s"}`,
			escapeJSON(p.UserName), escapeJSON(p.TableName), escapeJSON(p.Privileges), p.RiskLevel, badgeClass, badgeText, escapeJSON(risks), escapeJSON(p.Suggestions)))
	}
	sb.WriteString(`]</script>`)

	return sb.String()
}

// 辅助函数

type rowCounter interface {
	GetDDLRows() int
}

func (t TableDetail) GetDDLRows() int   { return t.DDLRows }
func (v ViewDetail) GetDDLRows() int    { return v.DDLRows }
func (i IndexDetail) GetDDLRows() int   { return i.DDLRows }
func (f FunctionDetail) GetDDLRows() int { return f.DDLRows }

func countTotalRows[T rowCounter](items []T) int {
	total := 0
	for _, item := range items {
		total += item.GetDDLRows()
	}
	return total
}

func riskPriority(level string) int {
	switch level {
	case RiskLevelHigh:
		return 3
	case RiskLevelMedium:
		return 2
	case RiskLevelLow:
		return 1
	default:
		return 0
	}
}

func escapeHTML(s string) string {
	return strings.NewReplacer(
		"&", "&amp;",
		"<", "&lt;",
		">", "&gt;",
		`"`, "&quot;",
		"'", "&#39;",
	).Replace(s)
}

func escapeJSON(s string) string {
	return strings.NewReplacer(
		`\`, `\\`,
		`"`, `\"`,
		"\n", `\n`,
		"\r", `\r`,
		"\t", `\t`,
	).Replace(s)
}
