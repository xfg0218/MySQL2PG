package config

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/spf13/viper"
)

// StringSet 字符串集合类型（内部使用）
type StringSet map[string]struct{}

// Config 存储所有配置
type Config struct {
	MySQL      MySQLConfig      `mapstructure:"mysql"`
	PostgreSQL PostgreSQLConfig `mapstructure:"postgresql"`
	Conversion ConversionConfig `mapstructure:"conversion"`
	Run        RunConfig        `mapstructure:"run"`
}

// MySQLConfig MySQL连接配置
type MySQLConfig struct {
	Host               string        `mapstructure:"host"`
	Port               int           `mapstructure:"port"`
	Username           string        `mapstructure:"username"`
	Password           string        `mapstructure:"password"`
	Database           string        `mapstructure:"database"`
	TestOnly           bool          `mapstructure:"test_only"`
	MaxOpenConns       int           `mapstructure:"max_open_conns"`      // 最大打开连接数
	MaxIdleConns       int           `mapstructure:"max_idle_conns"`      // 最大空闲连接数
	ConnMaxLifetime    time.Duration `mapstructure:"conn_max_lifetime"`   // 连接最大生命周期（秒）
	ConnectionParams   string        `mapstructure:"connection_params"`   // MySQL连接参数
	ConsistentSnapshot bool          `mapstructure:"consistent_snapshot"` // 数据读取使用一致性快照（P1-07，默认 false）
}

type PostgreSQLConfig struct {
	Host               string `mapstructure:"host"`
	Port               int    `mapstructure:"port"`
	Username           string `mapstructure:"username"`
	Password           string `mapstructure:"password"`
	Database           string `mapstructure:"database"`
	TestOnly           bool   `mapstructure:"test_only"`
	MaxConns           int    `mapstructure:"max_conns"`            // 最大连接数
	PgConnectionParams string `mapstructure:"pg_connection_params"` // PostgreSQL连接参数
	PasswordEncryption string `mapstructure:"password_encryption"`  // 密码加密方式：md5, scram-sha-256, auto
}

// ConversionConfig 转换配置
type ConversionConfig struct {
	Options OptionsConfig `mapstructure:"options"`
	Limits  LimitsConfig  `mapstructure:"limits"`
	MPP     MPPConfig     `mapstructure:"mpp"`
}

// MPPConfig MPP 分布式数据库配置
type MPPConfig struct {
	Enabled  bool   `mapstructure:"enabled"`  // 是否启用 MPP 模式（启用后才会创建 UNIQUE INDEX）
	Database string `mapstructure:"database"` // MPP 数据库类型: greenplum/yugabyte/auto
}

// OptionsConfig 转换选项配置
type OptionsConfig struct {
	Functions          bool     `mapstructure:"functions"`
	Indexes            bool     `mapstructure:"indexes"`
	Users              bool     `mapstructure:"users"`
	TableDDL           bool     `mapstructure:"tableddl"`               // 转换表DDL
	Data               bool     `mapstructure:"data"`                   // 转换数据
	View               bool     `mapstructure:"view"`                   // 转换视图
	Grant              bool     `mapstructure:"grant"`                  // 转换权限
	TablePrivileges    bool     `mapstructure:"table_privileges"`       // 转换表权限
	SkipExistingTables bool     `mapstructure:"skip_existing_tables"`   // 跳过已存在的表
	UseTableList       bool     `mapstructure:"use_table_list"`         // 是否使用指定的表列表进行数据同步
	TableList          []string `mapstructure:"table_list"`             // 指定要同步的表列表
	SkipUseTableList   bool     `mapstructure:"exclude_use_table_list"` // 是否使用跳过表列表
	SkipTableList      []string `mapstructure:"exclude_table_list"`     // 要跳过的表列表
	ValidateData       bool     `mapstructure:"validate_data"`          // 同步后验证数据一致性
	LowercaseColumns   bool     `mapstructure:"lowercase_columns"`      // 表字段是否转小写，true代表转小写，默认，false代表与mysql一致
	TruncateBeforeSync bool     `mapstructure:"truncate_before_sync"`   // 同步前是否清空表数据
	// tinyint(1) 映射策略：MySQL 的 tinyint(1) 虽是布尔惯例但本质是整数，
	// 视图/函数中广泛存在 `col = 1`、`COALESCE(col, 0)` 等用法，映射为 PG BOOLEAN
	// 会因 boolean = integer 无对应运算符而报 42883。
	// 默认 false：tinyint(1) -> SMALLINT，保留整数语义、视图/函数完全兼容；
	// 显式 true：映射为 BOOLEAN，需自行确保无布尔列与整数的比较/算术
	TinyInt1AsBoolean bool `mapstructure:"tinyint1_as_boolean"`

	// 视图排除列表
	SkipUseViewList bool      `mapstructure:"exclude_use_view_list"` // 是否使用视图排除列表
	SkipViewList    []string  `mapstructure:"exclude_view_list"`     // 要跳过的视图列表（原始配置）
	SkipViewSet     StringSet `mapstructure:"-"`                     // 要跳过的视图集合（转换后，内部使用）

	// 函数排除列表
	SkipUseFunctionList bool      `mapstructure:"exclude_use_function_list"` // 是否使用函数排除列表
	SkipFunctionList    []string  `mapstructure:"exclude_function_list"`     // 要跳过的函数列表（原始配置）
	SkipFunctionSet     StringSet `mapstructure:"-"`                         // 要跳过的函数集合（转换后，内部使用）
}

// LimitsConfig 限制配置
type LimitsConfig struct {
	Concurrency          int `mapstructure:"concurrency"`
	BandwidthMbps        int `mapstructure:"bandwidth_mbps"`
	MaxDDLPerBatch       int `mapstructure:"max_ddl_per_batch"`
	MaxFunctionsPerBatch int `mapstructure:"max_functions_per_batch"`
	MaxIndexesPerBatch   int `mapstructure:"max_indexes_per_batch"`
	MaxUsersPerBatch     int `mapstructure:"max_users_per_batch"`
	MaxRowsPerBatch      int `mapstructure:"max_rows_per_batch"` // 一次性同步数据的行数限制
	BatchInsertSize      int `mapstructure:"batch_insert_size"`  // 批量插入的大小
}

// RunConfig 运行配置
type RunConfig struct {
	ShowProgress      bool   `mapstructure:"show_progress"`
	ErrorLogPath      string `mapstructure:"error_log_path"`
	EnableFileLogging bool   `mapstructure:"enable_file_logging"`
	LogFilePath       string `mapstructure:"log_file_path"`
	ShowConsoleLogs   bool   `mapstructure:"show_console_logs"`
	ShowLogInConsole  bool   `mapstructure:"show_log_in_console"`
}

// LoadConfig 加载配置文件
func LoadConfig(configPath string) (*Config, error) {
	// 如果没有指定配置文件路径，尝试在当前目录查找
	if configPath == "" {
		currentDir, err := os.Getwd()
		if err != nil {
			return nil, fmt.Errorf("无法获取当前目录: %w", err)
		}

		// 尝试查找config.yml或config.yaml
		configPaths := []string{
			filepath.Join(currentDir, "config.yml"),
			filepath.Join(currentDir, "config.yaml"),
		}

		found := false
		for _, path := range configPaths {
			if _, err := os.Stat(path); err == nil {
				configPath = path
				found = true
				break
			}
		}

		if !found {
			return nil, fmt.Errorf("未找到配置文件，请指定配置文件路径或在当前目录创建config.yml")
		}
	}

	viper.SetConfigFile(configPath)

	// 读取配置文件
	if err := viper.ReadInConfig(); err != nil {
		return nil, fmt.Errorf("读取配置文件失败: %w", err)
	}

	var config Config
	if err := viper.Unmarshal(&config); err != nil {
		return nil, fmt.Errorf("解析配置文件失败: %w", err)
	}

	// 将排除列表转换为集合（O(1) 查找）
	config.Conversion.convertExclusionLists()

	return &config, nil
}

// convertExclusionLists 将排除列表转换为集合（内部使用）
func (c *ConversionConfig) convertExclusionLists() {
	// 转换视图排除列表
	if len(c.Options.SkipViewList) > 0 {
		c.Options.SkipViewSet = make(StringSet, len(c.Options.SkipViewList))
		for _, v := range c.Options.SkipViewList {
			c.Options.SkipViewSet[strings.ToLower(v)] = struct{}{}
		}
	}

	// 转换函数排除列表
	if len(c.Options.SkipFunctionList) > 0 {
		c.Options.SkipFunctionSet = make(StringSet, len(c.Options.SkipFunctionList))
		for _, f := range c.Options.SkipFunctionList {
			c.Options.SkipFunctionSet[strings.ToLower(f)] = struct{}{}
		}
	}
}

// ValidateConfig 验证配置是否有效
func (c *Config) ValidateConfig() error {
	// 验证MySQL配置
	if c.MySQL.Host == "" {
		return fmt.Errorf("MySQL主机地址不能为空")
	}
	if c.MySQL.Username == "" {
		return fmt.Errorf("MySQL用户名不能为空")
	}
	if c.MySQL.Database == "" {
		return fmt.Errorf("MySQL数据库名不能为空")
	}
	// MySQL连接池默认值
	if c.MySQL.MaxOpenConns <= 0 {
		c.MySQL.MaxOpenConns = 50 // 默认值
	}
	if c.MySQL.MaxIdleConns <= 0 {
		c.MySQL.MaxIdleConns = 20 // 默认值
	}
	if c.MySQL.ConnMaxLifetime <= 0 {
		c.MySQL.ConnMaxLifetime = 3600 // 默认值（秒）
	}

	// 验证PostgreSQL配置
	if c.PostgreSQL.Host == "" {
		return fmt.Errorf("PostgreSQL主机地址不能为空")
	}
	if c.PostgreSQL.Username == "" {
		return fmt.Errorf("PostgreSQL用户名不能为空")
	}
	if c.PostgreSQL.Database == "" {
		return fmt.Errorf("PostgreSQL数据库名不能为空")
	}
	// PostgreSQL连接池默认值
	if c.PostgreSQL.MaxConns <= 0 {
		c.PostgreSQL.MaxConns = 20 // 默认值
	}

	// 验证转换限制
	if c.Conversion.Limits.Concurrency <= 0 {
		c.Conversion.Limits.Concurrency = 1 // 默认值
	}
	if c.Conversion.Limits.MaxDDLPerBatch <= 0 {
		c.Conversion.Limits.MaxDDLPerBatch = 10 // 默认值
	}
	if c.Conversion.Limits.MaxFunctionsPerBatch <= 0 {
		c.Conversion.Limits.MaxFunctionsPerBatch = 5 // 默认值
	}
	if c.Conversion.Limits.MaxIndexesPerBatch <= 0 {
		c.Conversion.Limits.MaxIndexesPerBatch = 20 // 默认值
	}
	if c.Conversion.Limits.MaxUsersPerBatch <= 0 {
		c.Conversion.Limits.MaxUsersPerBatch = 10 // 默认值
	}
	if c.Conversion.Limits.MaxRowsPerBatch <= 0 {
		c.Conversion.Limits.MaxRowsPerBatch = 50000 // 默认值，与 BatchInsertSize 保持一致以减少网络往返
	}
	if c.Conversion.Limits.BatchInsertSize <= 0 {
		c.Conversion.Limits.BatchInsertSize = 50000 // 默认值，与 MaxRowsPerBatch 对齐，避免读 50000 行却按 10000 行分块插入
	}

	// MPP 配置默认值
	if c.Conversion.MPP.Database == "" {
		c.Conversion.MPP.Database = "auto"
	}

	// MPP 数据库类型合法性校验
	if c.Conversion.MPP.Enabled {
		db := c.Conversion.MPP.Database
		if db != "auto" && db != "greenplum" && db != "yugabyte" {
			return fmt.Errorf("MPP数据库类型无效: %s（支持: greenplum, yugabyte, auto）", db)
		}
	}

	return nil
}
