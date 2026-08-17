# MySQL2PG - Project Context

## Code Fixing and Submitting
- When a code bug is discovered, first create an issue, then analyze and fix the problem based on the issue
- After code fixes, it will be automatically submitted to the GitHub repository
- Automatically filter temporary files, test files, PLAN files, etc. during submission
- The code needs to be audited
- Create a pull request (PR) and submit it to GitHub without publishing a new version
- When manually publishing, use the version number linked on GitHub for publishing

## Project Overview

**MySQL2PG** is a professional-grade, high-performance database migration tool written in Go, designed to seamlessly migrate MySQL databases to PostgreSQL. It supports comprehensive conversion capabilities including:

- **Table structures (DDL)** — Full `SHOW CREATE TABLE` parsing and PostgreSQL-compatible DDL generation
- **Data synchronization** — Batch reads from MySQL, batch inserts into PostgreSQL via `pgx.CopyFrom`
- **Views** — MySQL view definitions converted to PostgreSQL compatible syntax
- **Indexes** — Primary keys, unique indexes, normal indexes, full-text indexes
- **Functions** — 50+ common MySQL function mappings to PostgreSQL equivalents
- **Users and roles** — MySQL users → PostgreSQL roles (preserving password hashes)
- **Table-level privileges** — `GRANT` statement conversion
- **HTML migration reports** — Parse conversion logs to generate visual HTML reports

### Key Characteristics

| Attribute | Value |
|-----------|-------|
| **MySQL Support** | 5.7+, 8.0+ |
| **PostgreSQL Support** | 12+, 13+, 14+, 15+, 16+, 17+, 18+ |
| **Go Version** | 1.24+ |
| **License** | Apache-2.0 |
| **Repository** | https://github.com/xfg0218/MySQL2PG |
| **View Conversion** | 42 views 100% convertible |
| **Function Conversion** | 113 functions 100% convertible |

### Core Features

1. **Intelligent Type Mapping**: 40+ MySQL field types → PostgreSQL with 99.9% accuracy
2. **Concurrent Processing**: Configurable concurrency (default 10), 5-10x speedup over single-threaded
3. **Optimized Batch Processing**: Default 50,000 rows/batch, typed Scan destinations, `sync.Pool` for row slices, `[]byte` direct-to-CopyFrom — **5-8x throughput improvement**
4. **Data Validation**: Row count comparison after sync with inconsistency reporting
5. **test_only Mode**: Quick connection testing (<1s) without performing any conversion
6. **Real-time Progress**: Progress display with ANSI/non-ANSI terminal support
7. **HTML Migration Reports**: `./mysql2pg report -l conversion.log` generates visual single-file HTML reports
8. **Performance Profiling**: Built-in pprof on `localhost:6060`
9. **Table Filtering**: Whitelist (`table_list`) and blacklist (`exclude_table_list`) modes
10. **View/Function Exclusion**: Skip specified views (`exclude_view_list`) and functions (`exclude_function_list`) using set-based lookup (O(1) lookup performance)

## Architecture

```
MySQL2PG/
├── cmd/
│   ├── main.go              # Entry point: CLI parsing, config loading, orchestration
│   └── report.go            # report subcommand: log parsing → HTML report generation
├── internal/
│   ├── config/
│   │   └── config.go        # YAML config parsing & validation via Viper
│   ├── converter/postgres/
│   │   ├── manager.go       # Core conversion orchestrator (Run, executeConversion, stats)
│   │   ├── sync_tableddl.go # Table DDL conversion (MySQL → PostgreSQL)
│   │   ├── sync_data.go     # Data synchronization with batching & pagination
│   │   ├── sync_indexes.go  # Index conversion (primary, unique, normal, full-text)
│   │   ├── sync_functions.go# Function mapping (50+ functions)
│   │   ├── sync_viewddl.go  # View definition conversion
│   │   ├── sync_user_privilege.go  # User/role conversion
│   │   ├── sync_table_privilege.go # Table privilege conversion
│   │   └── keywords.go      # PostgreSQL reserved keywords list
│   ├── mysql/
│   │   ├── connection.go    # MySQL connection management, data fetching, pagination
│   │   └── metadata.go      # Metadata retrieval (tables, views, functions, users, privileges)
│   ├── postgres/
│   │   └── connection.go    # PostgreSQL connection, DDL execution, batch inserts via pgx
│   │                        # + typedDest Scan optimization + sync.Pool + []byte passthrough
│   └── report/
│       ├── parser.go        # Log parser: regex-based extraction from conversion.log/errors.log
│       │                    # Parses: table conversion success, empty tables, inconsistencies,
│       │                    # progress lines, stage summary tables, version info, warnings,
│       │                    # table-level errors, paginated sync. All entries deduplicated.
│       └── html.go          # HTML report generator: single-file dark terminal aesthetic
│                            # JetBrains Mono + DM Sans fonts, neon accent colors, scanline texture
│                            # Summary stat cards, bar charts, table details with ERR/WRN indicators
├── scripts/                  # Auxiliary scripts
├── config.example.yml        # Example configuration file
├── Makefile                  # Build/run/clean/test-connection targets
│                             # Note: builds entire ./cmd/ package (main.go + report.go)
├── go.mod / go.sum           # Go module dependencies
└── README.md / README_CN.md  # Bilingual documentation
```

### Key Dependencies

| Package | Version | Purpose |
|---------|---------|---------|
| `github.com/go-sql-driver/mysql` | v1.7.1 | MySQL database driver |
| `github.com/jackc/pgx/v5` | v5.5.0 | PostgreSQL driver with connection pooling (`pgxpool`) |
| `github.com/spf13/viper` | v1.18.2 | YAML configuration parsing |

## Building and Running

### Prerequisites

- Go 1.24+
- MySQL 5.7+ (source)
- PostgreSQL 12+ (target)

### Build

```bash
make build
# Equivalent to:
go build -o mysql2pg ./cmd/
```

> **Note**: Must build the entire `./cmd/` package, not just `./cmd/main.go`, because `report.go` is in the same package.

### Run

```bash
# 1. Create configuration
cp config.example.yml config.yml
# Edit config.yml with your database credentials

# 2. Run with config file
./mysql2pg config.yml
# Or using -c flag
./mysql2pg -c config.yml

# Development run (uses config.yml)
make run
```

### Generate HTML Migration Report

```bash
# Parse conversion.log → generate HTML report
./mysql2pg report -l conversion.log
# → Generates report-YYYY-MM-DD_HHmmss.html

# Specify error log + custom output path
./mysql2pg report -l conversion.log -e errors.log -o my-report.html

# View report help
./mysql2pg report -h
```

### Test Connection Only

```bash
make test-connection
# Tests both MySQL and PostgreSQL connections without performing any conversion
```

### Clean

```bash
make clean
# Removes: mysql2pg binary, errors.log, conversion.log
```

### Help

```bash
./mysql2pg -h
# or
./mysql2pg --help
```

## Configuration

The tool uses a YAML configuration file (`config.yml`). Key sections:

### MySQL Connection

```yaml
mysql:
  host: localhost
  port: 3306
  username: root
  password: password
  database: test_db
  test_only: false              # Only test connection, no conversion
  max_open_conns: 100           # Max open connections (default: 100)
  max_idle_conns: 50            # Max idle connections (default: 50)
  conn_max_lifetime: 3600       # Connection lifetime in seconds (default: 3600)
  connection_params: charset=utf8mb4&parseTime=false&interpolateParams=true
```

### PostgreSQL Connection

```yaml
postgresql:
  host: localhost
  port: 5432
  username: postgres
  password: password
  database: test_db
  test_only: false
  max_conns: 50                 # Max connections (default: 50)
  pg_connection_params: search_path=public connect_timeout=300 statement_timeout=0
```

### Conversion Options

```yaml
conversion:
  options:
    tableddl: true              # Convert table DDL structures
    data: true                  # Synchronize table data
    view: false                 # Convert views
    indexes: true               # Convert indexes
    functions: false            # Convert stored functions
    users: true                 # Convert MySQL users to PostgreSQL roles
    table_privileges: true      # Convert table-level privileges
    lowercase_columns: true     # Convert column names to lowercase
    skip_existing_tables: true  # Skip tables that already exist in PostgreSQL
    use_table_list: false       # Whitelist mode: only sync specified tables
    table_list: [table1]        # Tables to sync (when use_table_list=true)
    exclude_use_table_list: false  # Blacklist mode: skip specified tables
    exclude_table_list: [table1]   # Tables to skip
    validate_data: true         # Validate row counts after sync
    truncate_before_sync: false # Clear target tables before sync

    # View exclusion (skip specified views during migration)
    exclude_use_view_list: false        # Enable view exclusion mode
    exclude_view_list: [view1, view2]                 # Views to skip (case-insensitive, object list format)

    # Function exclusion (skip specified functions during migration)
    exclude_use_function_list: false    # Enable function exclusion mode
    exclude_function_list:  [func1, func2]            # Functions to skip (case-insensitive, object list format)

**View/Function Exclusion Use Cases**:
- Skip complex/temporary views that don't need migration
- Skip deprecated or MySQL-specific functions
- Skip objects that have PostgreSQL-native alternatives
- All names are case-insensitive and logged when skipped
- Configure `exclude_view_list` and `exclude_function_list` as object lists (`- name: xxx`), not string arrays.

  limits:
    concurrency: 10             # Concurrent goroutines
    bandwidth_mbps: 100         # Network bandwidth limit (Mbps) — currently unused
    max_ddl_per_batch: 10       # Max DDL statements per batch
    max_functions_per_batch: 5  # Max functions per batch
    max_indexes_per_batch: 20   # Max indexes per batch
    max_users_per_batch: 10     # Max users per batch
    max_rows_per_batch: 50000   # Max rows per batch read (default: 50000)
    batch_insert_size: 50000    # Batch insert size (default: 50000)
```

### Run Configuration

```yaml
run:
  show_progress: true           # Show task progress
  error_log_path: ./errors.log  # Error log file path
  enable_file_logging: true     # Enable file logging
  log_file_path: ./conversion.log  # Log file path
  show_console_logs: true       # Show logs in console
  show_log_in_console: false    # Show detailed log output in console
```

## Conversion Flow

```
Step 0: test_only mode?
  ├─ Yes → Test MySQL & PostgreSQL connections → Show versions → Exit
  └─ No  → Continue

Step 1: Read MySQL table definitions
  ├─ Filter tables (whitelist/blacklist if configured)
  └─ Fetch metadata: tables, views, indexes, functions, users, privileges

Step 2: Convert table DDL (tableddl: true)
  ├─ Parse MySQL CREATE TABLE
  ├─ Map field types (40+ type mappings)
  └─ Create tables in PostgreSQL (skip if exists)

Step 3: Convert views (view: true)
  ├─ Filter views (exclude_view_list if exclude_use_view_list=true)
  │   └─ Skip views in exclusion list, log and update progress
  ├─ Convert MySQL view definitions to PostgreSQL compatible syntax
  └─ Execute CREATE VIEW statements in PostgreSQL

Step 4: Sync data (data: true)
  ├─ Truncate target tables (if truncate_before_sync=true)
  ├─ Batch read MySQL (max_rows_per_batch, default 50000)
  ├─ Batch insert PostgreSQL via pgx.CopyFrom (batch_insert_size, default 50000)
  │   └─ Uses typed Scan destinations (*int64, *string, *[]byte, *float64) — zero heap allocation per Scan
  │   └─ Uses sync.Pool for row slice reuse — eliminates per-row make()
  │   └─ Passes []byte directly for TEXT/VARCHAR columns — pgx.CopyFrom natively supports it
  └─ Concurrency controlled by limits.concurrency

Step 5: Convert indexes (indexes: true)
  └─ Rebuild: primary keys, unique indexes, normal indexes, full-text indexes

Step 6: Convert functions (functions: true)
  ├─ Filter functions (exclude_function_list if exclude_use_function_list=true)
  │   └─ Skip functions in exclusion list, log and update progress
  ├─ 50+ function mappings (NOW()→CURRENT_TIMESTAMP, IFNULL()→COALESCE(), etc.)
  └─ Execute CREATE FUNCTION statements in PostgreSQL

Step 7: Convert users (users: true)
  └─ MySQL Users → PostgreSQL Roles (preserve password hashes)

Step 8: Convert table privileges (table_privileges: true)
  └─ GRANT statements converted to PostgreSQL equivalents

Final Step: Data validation & Completion (validate_data: true)
  ├─ Compare row counts: MySQL vs PostgreSQL
  ├─ Re-enable foreign key constraints and indexes
  ├─ Report inconsistent tables (if truncate_before_sync=false, continues)
  └─ Output conversion statistics and performance metrics
```

## HTML Report Structure

Generated by `./mysql2pg report -l conversion.log`:

**Design**: Dark terminal aesthetic (`#0a0e17` background), JetBrains Mono monospace font, DM Sans body text, neon accent colors (cyan `#06b6d4`, blue `#3b82f6`, green `#10b981`, red `#ef4444`, amber `#f59e0b`, purple `#a855f7`). CRT scanline texture overlay. Gradient top bar on header. Terminal `>` prefix on title. Bottom neon strip on stat cards.

```
┌──────────────────────────────────────────────┐
│ > MySQL2PG Migration Report    2026-04-07   │
│   Source: conversion.log                     │
│   [MySQL 8.0] → [PostgreSQL 16]             │
│   Progress: ████████████ 100% (796/796) 完成 │
├──────────────────────────────────────────────┤
│  [Tables:177] [Rows:10] [Views:0]           │
│  [Idx:0] [Func:0] [Err:0]  ← neon bottom bars│
├──────────────────────────────────────────────┤
│  ⚡ Performance              [STAGES]        │
│  转换表结构  [6]    ████████       2.79s    │
│  同步表数据  [6]    ████           1.19s    │
│  Total: 5.2s  |  193 rows/s                │
├──────────────────────────────────────────────┤
│  📋 Tables                     [177 items]   │
│  #  | Table              | Rows | Status    │
│  1  | case_47_memory     | -    | [已存在]  │
│  2  | act_hi_comment     | 10   | [不一致]ERR│
├──────────────────────────────────────────────┤
│  ⚠ Data Inconsistencies      [1 tables]      │
│  Table           | MySQL | PG   | Delta     │
│  act_hi_comment  | 10    | 30   | -20       │
├──────────────────────────────────────────────┤
│  ❌ Errors                      [2]          │
│  #1 插入表 sessions 数据失败: ...            │
├──────────────────────────────────────────────┤
│  ⚡ Warnings                    [1]          │
│  #1 表 sessions: 没有主键...                 │
└──────────────────────────────────────────────┘
```

### Supported Log Patterns

The report parser (`internal/report/parser.go`) recognizes these conversion log formats:

| Log Pattern | Example | Parsed Data |
|-------------|---------|-------------|
| Table conversion success | `[...] 转换表 users 成功` | Table name, status = "已转换" |
| Table exists, skip create | `[...] 表 xxx 已存在，跳过创建` | Table name, status = "已存在" |
| Table sync complete | `[...] 表 xxx 同步完成，N 行数据，数据一致` | Table name, row count, status |
| Paginated sync (no status) | `[...] 分页同步表 xxx 完成，共处理 N 行数据` | Table name, row count |
| Paginated sync (with status) | `[...] 分页同步表 xxx 完成，N 行数据，数据一致` | Table name, row count, status |
| Table empty/no data | `[...] 表 xxx 没有数据，跳过同步` | Table name, status = "空表" |
| Data inconsistency | `[...] 表 xxx 数据不一致` | Table name, status = "数据不一致" |
| Warning | `[...] 警告: 表 xxx 没有主键...` | Warning message, linked to table |
| Table-level error | `[...] 插入表 xxx 数据失败: ...` | Error message, linked to table |
| Progress summary | `[...] 进度: 100.00% (192/192)` | Progress current/total/complete |
| Stage summary table | `\| 表结构 \| 192 \| 5.2 \|` | Stage name, count, duration |
| Total duration | `\| 总耗时 \| \| 7.5 \|` | Total migration duration |
| Inconsistent table table | `\| act_hi_comment \| 10 \| 30 \|` | Table name, MySQL count, PG count |
| Version info | `MySQL \| 8.0.35` or `PostgreSQL \| 15.4` | Database versions |
| Error log | `[...] ERROR: message` | Error details (from errors.log) |
| Conversion done | `[...] 转换完成!` | Marks migration complete |

**Deduplication**: All table entries are deduplicated by table name — the first matching pattern wins. This prevents double-counting when the same table appears in multiple log lines (e.g., "已存在" followed by "同步完成"). Errors, warnings, and stage stats are also deduplicated.

**Stage summary & inconsistent table logging**: The `generateSummaryTable()` and `displayInconsistentTables()` functions in `manager.go` now write table-formatted output to both console (`fmt.Println`) AND log file (`m.Log()`), ensuring report parser can extract these statistics.

Report is a single self-contained HTML file with inline CSS and Google Fonts — opens directly in browser.

## Type Mapping Reference

| MySQL Type | PostgreSQL Type | Notes |
|------------|-----------------|-------|
| `bigint`, `bigint(20)` | `BIGINT` | All bigint variants |
| `int`, `int(11)`, `integer` | `INTEGER` | All int variants |
| `mediumint`, `mediumint(9)` | `INTEGER` | |
| `smallint`, `smallint(6)` | `SMALLINT` | All smallint variants |
| `tinyint(1)` | `BOOLEAN` | Special case |
| `tinyint`, `tinyint(4)` | `SMALLINT` | Other tinyint variants |
| `decimal`, `numeric` | `DECIMAL` | Preserves precision |
| `double`, `double precision` | `DOUBLE PRECISION` | |
| `float` | `REAL` | |
| `char`, `char(1)` | `CHAR` | Preserves length |
| `varchar`, `varchar(255)` | `VARCHAR` | Preserves length |
| `text`, `longtext` | `TEXT` | All text variants |
| `blob`, `longblob`, `binary` | `BYTEA` | All binary types |
| `datetime`, `datetime(6)` | `TIMESTAMP` | Naive time, preserves precision |
| `timestamp`, `timestamp(6)` | `TIMESTAMPTZ` | MySQL TIMESTAMP is stored as UTC (timezone-aware); sessions pinned to UTC on both ends during migration |
| `date` | `DATE` | |
| `time` | `TIME` | Preserves precision |
| `year` | `INTEGER` | |
| `json`, `json(1024)` | `JSON` | |
| `jsonb` | `JSONB` | |
| `enum` | `VARCHAR(255)` | |
| `set` | `VARCHAR(255)` | |
| `geometry`, `point`, `linestring`, etc. | Same | Spatial types preserved |
| `bigint AUTO_INCREMENT` | `BIGSERIAL` | |
| `int AUTO_INCREMENT` | `SERIAL` | |
| `tinyint UNSIGNED` | `SMALLINT` | 0~255 可由 SMALLINT 容纳，无需提升 |
| `smallint UNSIGNED` | `INTEGER` | 0~65535 超出 SMALLINT 上限，提升 |
| `mediumint UNSIGNED` | `INTEGER` | 0~16777215 可由 INTEGER 容纳，无需提升 |
| `int UNSIGNED` | `BIGINT` | 0~4294967295 超出 INTEGER 上限，提升 |
| `bigint UNSIGNED` | `NUMERIC(20,0)` | 0~18446744073709551615 超出 BIGINT 上限，提升 |
| `DECIMAL/FLOAT/DOUBLE UNSIGNED` | 对应类型 + `CHECK (col >= 0)` | PG 无无符号数值类型，非负约束以 CHECK 表达；转换报告输出清单 |
| `ZEROFILL` 修饰 | 按 UNSIGNED 处理 | MySQL 中 ZEROFILL 隐含 UNSIGNED 语义 |
| `bit(n)` (n ≤ 63) | `BIGINT` | BIT 本质是无符号整数（0 ~ 2^n-1） |
| `bit(64)` | `NUMERIC(20,0)` | BIT(64) 最大值超出 BIGINT 上限 |

## Performance Optimizations (Applied)

The data sync hot path has been optimized to eliminate the progressive slowdown caused by GC pressure:

| Optimization | Before | After | Impact |
|-------------|--------|-------|--------|
| Batch size default | 1,000 / 10,000 (mismatched) | 50,000 / 50,000 (aligned) | -98% network round-trips |
| Scan destinations | `*interface{}` (200K allocs/batch) | Typed (`*int64`, `*string`, `*[]byte`) — 0 allocs | -99.998% Scan allocations |
| Row slice allocation | `make([]interface{}, N)` per row (50K/batch) | `sync.Pool` reuse — ~0/batch | -99.9% slice allocations |
| `[]byte` → `string` | Every text column per row | Direct `[]byte` to pgx.CopyFrom | -99.6% string allocations |
| Zero-date check | `string(val) == "0000-..."` | `bytes.Equal(val, []byte("0000-..."))` | Zero allocation for zero-date detection |
| `copyRows` reset | `make()` each batch | `[:0]` reuse | Zero reallocation per batch |

**Result**: Per-batch heap allocation reduced from ~60MB to ~0.4MB (**-99.2%**), eliminating GC Assist and delivering **5-8x throughput improvement**.

## Testing

### Integration Tests

The project includes a comprehensive integration test suite in `scripts/integrationtests/run_integration_tests.sh`:

- **84 test cases** covering all configuration options and core features
- Tests are organized by category: connectivity, DDL, data sync, views, indexes, functions, users, privileges, limits, run options, and boundary scenarios
- Each test modifies config.yml, runs the tool, and checks exit code
- Results are displayed in a formatted table with PASS/FAIL status

```bash
# Run all integration tests
bash scripts/integrationtests/run_integration_tests.sh
```

### Test Data

The `scripts/mysql/insert_data.sql` file provides **10 test rows for all 167 original tables** (case_01~case_167) defined in `create_table.sql`, covering:
- Basic types (integers, floats, strings, dates, JSON, binary)
- Complex scenarios (e-commerce, CMS, finance, social, medical, hotel, restaurant)
- Edge cases (partition tables, generated columns, reserved keywords, long identifiers)

Note: `create_table.sql` defines 193 tables in total. The extra 25 (case_169~case_193) are type-length sweep tables (CHAR/VARCHAR/BINARY/VARBINARY/integer widths/DECIMAL/FLOAT/DOUBLE/BIT/temporal fsp, one table per type) used for full-length conversion coverage; they are DDL-only and need no INSERT data.

```bash
# Insert test data (after create_table.sql)
mysql -u root -p test_db < scripts/mysql/insert_data.sql
```

### Test Coverage Summary

| Category | Tables | Test Cases | Notes |
|----------|--------|------------|-------|
| Basic types (case_01~case_40) | 40 | 40 | Integers, floats, strings, dates, JSON, enums, sets |
| Indexes & constraints (case_41~case_60) | 20 | 20 | Foreign keys, fulltext, spatial, composite PK, partitions |
| MySQL 5.7+/8.0 (case_61~case_100) | 40 | 40 | CTEs, window functions, JSON_TABLE, optimizer hints |
| Business scenarios (case_101~case_120) | 20 | 20 | Archive, CSV, Blackhole, UPSERT, multi-table DELETE |
| Daily development (case_121~case_155) | 35 | 35 | E-commerce, CMS, finance, social, logs, sys admin |
| Enhanced scenarios (case_156~case_167) | 12 | 12 | Composite FK, JSON generated columns, temporal mix |
| Type-length sweep (case_169~case_193) | 25 | - | Full-length conversion coverage per type, DDL only |
| **Total** | **193** | **84 integration tests** | Full coverage |

## Development Conventions

### Code Style

- **Language**: Go 1.24+, idiomatic patterns
- **Comments**: Chinese comments in source code (function descriptions, log messages)
- **Formatting**: Standard `gofmt` style
- **Error Handling**: Error wrapping with `fmt.Errorf("context: %w", err)`
- **Package Structure**: Standard Go layout — `cmd/` for entry points, `internal/` for private packages

### Concurrency Patterns

- **Semaphore-based control**: `chan struct{}` for limiting concurrent goroutines
  ```go
  semaphore := make(chan struct{}, config.Conversion.Limits.Concurrency)
  semaphore <- struct{}{}        // Acquire
  defer func() { <-semaphore }() // Release
  ```
- **Worker pools**: `sync.WaitGroup` + goroutines + buffered error channels
- **Batch processing**: Configurable batch sizes, loop with `i += batchSize` slicing

### Database Patterns

- **MySQL data fetching**: `SHOW CREATE TABLE`, `SHOW FULL COLUMNS`, `information_schema` queries
- **PostgreSQL inserts**: `pgx.CopyFrom` for high-performance bulk inserts
- **Typed Scan destinations**: Pre-allocated `*int64`, `*string`, `*[]byte`, `*float64` based on column types — avoids per-row heap allocation
- **sync.Pool for row slices**: `rowSlicePool.Get()` / `rowSlicePool.Put()` reuses `[]interface{}` backing arrays
- **Transaction management**: PostgreSQL transactions for DDL and data integrity
- **Connection pooling**: Configurable pool sizes for both databases

### Key Data Types

```go
// MySQL metadata structures
mysql.TableInfo       // Name, DDL, Columns, Indexes
mysql.ColumnInfo      // Name, Type, Nullable, Default, Comment
mysql.IndexInfo       // Name, Table, Columns, IsUnique
mysql.FunctionInfo    // Name, DDL, Parameters, ReturnType
mysql.UserInfo        // Name, Grants
mysql.ViewInfo        // ViewName, ViewDefinition
mysql.TablePrivInfo   // Host, Db, User, TableName, TablePriv

// Report types
report.ParsedReport   // Parsed log data: tables, stages, errors, inconsistencies
report.StageStat      // Stage name, object count, duration
report.TableDetail    // Per-table: name, row count, validation result
                      // Validation values: "已转换"|"已存在"|"数据一致"|"数据不一致"|"空表"|"跳过验证"
report.InconsistentTable  // Table name, MySQL count, PG count

// Configuration
config.Config         // MySQL, PostgreSQL, Conversion, Run sections
config.OptionsConfig  // All conversion toggle options
config.LimitsConfig   // All batch/concurrency limits
config.StringSet      // String set type for exclusion lists (views, functions) - O(1) lookup

// Exclusion list fields in OptionsConfig:
//   SkipViewList       []string   // Raw view names from YAML config
//   SkipViewSet        StringSet  // Converted set (internal use, case-insensitive)
//   SkipFunctionList   []string   // Raw function names from YAML config
//   SkipFunctionSet    StringSet  // Converted set (internal use, case-insensitive)
```

### Git Ignored Files

- `mysql2pg` — Compiled binary
- `config.yml` — Personal configuration (use `config.example.yml` as template)
- `*.log` — Log files (`errors.log`, `conversion.log`)
- `*.html` — Generated HTML reports
- `go.work`, `go.work.sum` — Go workspace files (for multi-module development)

## Performance Profiling

The tool automatically starts a pprof HTTP server on `localhost:6060`:

```go
// In cmd/main.go
go func() {
    http.ListenAndServe("localhost:6060", nil)
}()
```

### Using pprof

```bash
# CPU profile (30 seconds)
go tool pprof http://localhost:6060/debug/pprof/profile?seconds=30

# Memory/heap profile
go tool pprof http://localhost:6060/debug/pprof/heap

# Goroutine profile
go tool pprof http://localhost:6060/debug/pprof/goroutine

# Interactive web UI
go tool pprof -http=:8080 http://localhost:6060/debug/pprof/heap
```

---

## Latest Updates (2026-04-17)

### MySQL 5.7+ Full Support

#### New DateTime Functions
- `YEARWEEK(dt)` → `(extract(year from dt)::int * 100 + extract(week from dt)::int)`
- `DAYNAME(dt)` → `to_char(dt, 'Day')`
- `MONTHNAME(dt)` → `to_char(dt, 'Month')`
- `QUARTER(dt)` → `extract(quarter from dt)::int`
- `WEEK(dt)` → `extract(week from dt)::int`

#### JSON Functions Validated & Fixed
- `JSON_OBJECT('key', val)` → `json_build_object('key', val)` ✅
- `JSON_ARRAY(a, b)` → `json_build_array(a, b)` ✅
- `JSON_INSERT/REPLACE/SET` → `jsonb_set(..., to_jsonb(val), ...)` ✅
- `JSON_REMOVE` → `doc - 'key'` ✅
- `JSON_MERGE_PATCH` → `(doc1::jsonb || doc2::jsonb)` ✅

#### Key Bug Fixes
1. **jsonb_set Value Type Conversion**
   - Issue: `val::jsonb` causes "invalid input syntax for type json" error
   - Fix: Changed to `to_jsonb(val)` for proper string literal handling
   
2. **JSON_SET Multi-parameter Support**
   - Supports `JSON_SET(doc, path1, val1, path2, val2, ...)` syntax
   - Converts to nested `jsonb_set` calls

3. **CAST(x USING charset) Syntax**
   - MySQL-specific syntax not supported by PostgreSQL
   - Fixed by removing CAST and returning the inner expression

### CI/CD Updates
- Using Go 1.24
- Added unit tests and coverage reports
- Added integration tests (MySQL 8.0 + PostgreSQL 16)
- Auto-executes MySQL2PG migration tool

### Test Status
- ✅ 42 views 100% convertible
- ✅ 113 functions core syntax 100% convertible
- ✅ 41+ test cases all passing
- ✅ Code coverage 88%+

### Recent Commits
- b78b052: ci: Update GitHub Actions workflow configuration
- 0507239: fix: Fix jsonb_set value parameter type conversion
- a4d3955: feat: Add MySQL 5.7+ datetime function conversion support
- 092fec4: fix: Add CAST(x USING charset) syntax conversion support
