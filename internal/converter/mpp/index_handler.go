package mpp

import (
	"strings"
	"sync"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/yourusername/mysql2pg/internal/mysql"
)

// IndexHandler UNIQUE INDEX 处理器
type IndexHandler struct {
	Config         *Config
	PostgresDB     *pgxpool.Pool
	Schema         string // 当前使用的 schema
	LogFunc        func(string, ...interface{})
	ErrorFunc      func(string, ...interface{})
	detectedDBType string
	detectedOnce   sync.Once
}

// HandleUniqueIndex 处理单个 UNIQUE INDEX
// 返回：shouldCreate bool, err error
func (h *IndexHandler) HandleUniqueIndex(index mysql.IndexInfo, lowercaseColumns bool) (shouldCreate bool, err error) {
	// MPP 模式未启用，跳过分布键调整但继续创建 UNIQUE INDEX
	if !h.Config.Enabled {
		h.LogFunc("跳过 UNIQUE 索引 %s（表 %s）的 MPP 分布键调整（MPP 模式未启用）", index.Name, index.Table)
		return true, nil // 继续创建 UNIQUE INDEX
	}

	// 检测 MPP 数据库类型（带缓存）
	mppDBType := h.detectDBType()

	// 仅对支持的 MPP 数据库调整分布键
	if IsSupportedMPP(mppDBType) {
		// 收集 UNIQUE 列
		uniqueColumns := h.collectUniqueColumns(index, lowercaseColumns)

		// 调整分布键
		adjusted, err := AdjustDistributionKey(h.PostgresDB, index.Table, h.Schema, uniqueColumns, lowercaseColumns, h.LogFunc)
		if err != nil {
			h.ErrorFunc("调整表 %s 分布键失败：%v（将直接创建 UNIQUE 索引）", index.Table, err)
			// 分布键调整失败不影响 UNIQUE INDEX 创建，继续执行
		} else if adjusted {
			h.LogFunc("表 %s 分布键已调整，准备创建 UNIQUE 索引 %s", index.Table, index.Name)
		}
	} else {
		// 非支持的 MPP 数据库（如 YugabyteDB 或标准 PostgreSQL），跳过分布键调整，正常创建 UNIQUE 索引
		h.LogFunc("跳过表 %s 分布键调整（数据库类型：%s），直接创建 UNIQUE 索引 %s", index.Table, mppDBType, index.Name)
	}

	return true, nil
}

// detectDBType 检测 MPP 数据库类型（带缓存，避免重复查询）
func (h *IndexHandler) detectDBType() string {
	h.detectedOnce.Do(func() {
		// 如果用户显式指定了数据库类型，直接使用
		if h.Config.Database != "auto" {
			h.detectedDBType = h.Config.Database
			h.LogFunc("使用配置的 MPP 数据库类型: %s", h.detectedDBType)
			return
		}

		// 否则自动检测（DetectDatabaseType 内部已包含版本号检测 + 扩展检查）
		h.detectedDBType = DetectDatabaseType(h.PostgresDB)
		h.LogFunc("自动检测到 MPP 数据库类型: %s", h.detectedDBType)
	})
	return h.detectedDBType
}

// collectUniqueColumns 收集 UNIQUE 索引的列
func (h *IndexHandler) collectUniqueColumns(index mysql.IndexInfo, lowercase bool) []string {
	cols := []string{}
	for _, col := range index.Columns {
		colName := col
		if lowercase {
			colName = strings.ToLower(col)
		}
		cols = append(cols, colName)
	}
	return cols
}
