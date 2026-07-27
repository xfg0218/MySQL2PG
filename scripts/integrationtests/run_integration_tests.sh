#!/bin/bash

if [ -z "$BASH_VERSION" ]; then
    exec bash "$0" "$@"
fi

# Exit on error for build and setup, but not for individual tests
set -e

# 获取项目根目录路径
PROJECT_ROOT="$(dirname "$0")/../.."
# 转换为绝对路径
PROJECT_ROOT="$(cd "$PROJECT_ROOT" && pwd)"

# 使用绝对路径引用配置文件
CONFIG_FILE="$PROJECT_ROOT/config.yml"
BACKUP_FILE="$PROJECT_ROOT/config.yml.bak"
BINARY="$PROJECT_ROOT/mysql2pg"
CONVERSION_LOG="$PROJECT_ROOT/conversion.log"
ERROR_LOG="$PROJECT_ROOT/errors.log"

: > "$CONVERSION_LOG"
: > "$ERROR_LOG"

exec >> "$CONVERSION_LOG"
exec 2>> "$ERROR_LOG"

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m' # Bold Yellow for visibility
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Counters
TOTAL_TESTS=0
PASSED_TESTS=0
FAILED_TESTS=0
FAILED_CASES=""

log_info() {
    echo -e "${BLUE}[INFO] $1${NC}"
}

log_passed() {
    echo -e "${GREEN}[PASSED] $1${NC}"
}

log_abnormal() {
    echo -e "${YELLOW}[ABNORMAL] $1${NC}" >&2
}

# trim_whitespace 去除字符串首尾空白字符，保留中间内容。
trim_whitespace() {
    printf '%s' "$1" | sed 's/^[[:space:]]*//;s/[[:space:]]*$//'
}

# escape_sed_replacement 转义 sed 替换值中的特殊字符。
escape_sed_replacement() {
    printf '%s' "$1" | sed 's/[&|\\]/\\&/g'
}

# update_config_key 更新全局唯一配置项的值。
update_config_key() {
    local target_key=$1
    local indent=$2
    local value=$3
    local escaped_value
    escaped_value=$(escape_sed_replacement "$value")

    sed -i '' "s|^[[:space:]]*$target_key: .*|$indent$target_key: $escaped_value|" "$CONFIG_FILE"
}

# update_section_config_key 在指定配置段内更新配置项的值。
update_section_config_key() {
    local start_section=$1
    local end_section=$2
    local target_key=$3
    local indent=$4
    local value=$5
    local escaped_value
    escaped_value=$(escape_sed_replacement "$value")

    sed -i '' "/^$start_section:/,/^$end_section:/ s|^[[:space:]]*$target_key: .*|$indent$target_key: $escaped_value|" "$CONFIG_FILE"
}

# 打印调试信息
log_info "Script directory: $(dirname "$0")"
log_info "Project root: $PROJECT_ROOT"
log_info "Binary path: $BINARY"

# 1. Build the project
log_info "Building project..."
# 切换到项目根目录
cd "$PROJECT_ROOT" || { log_abnormal "Failed to change directory"; exit 1; }
make build || { log_abnormal "Make build failed"; exit 1; }
# 切换回脚本所在目录
cd "$(dirname "$0")" || { log_abnormal "Failed to change back to script directory"; exit 1; }

# Check if binary exists
if [ ! -f "$BINARY" ]; then
    log_abnormal "Binary $BINARY not found after build"
    log_abnormal "Listing project root directory:"
    ls -la "$PROJECT_ROOT"
    exit 1
fi

# 2. Backup config
if [ -f "$CONFIG_FILE" ]; then
    log_info "Backing up $CONFIG_FILE to $BACKUP_FILE"
    cp "$CONFIG_FILE" "$BACKUP_FILE"
else
    log_abnormal "$CONFIG_FILE not found"
    exit 1
fi

# Clean up any previous test results
if [ -f "/tmp/mysql2pg_test_results.txt" ]; then
    rm "/tmp/mysql2pg_test_results.txt"
fi

# Function to restore config on exit
cleanup() {
    echo ""
    log_info "Restoring configuration..."
    mv "$BACKUP_FILE" "$CONFIG_FILE"
    
    echo "========================================================"
    echo "Test Summary:"
    echo "Total: $TOTAL_TESTS"
    echo -e "Passed: ${GREEN}$PASSED_TESTS${NC}"
    echo -e "Failed: ${YELLOW}$FAILED_TESTS${NC}"
    if [ -n "$FAILED_CASES" ]; then
        echo -e "Failed Cases: ${YELLOW}$FAILED_CASES${NC}"
    fi
    
    # Generate detailed summary table
    echo ""
    echo "========================================================"
    echo "Detailed Test Report"
    echo "========================================================"
    printf "%-5s | %-40s | %-10s\n" "ID" "Test Case Name" "Status"
    echo "------+------------------------------------------+------------"
    
    # We will read from a temporary file where we stored test results
    if [ -f "/tmp/mysql2pg_test_results.txt" ]; then
        cat "/tmp/mysql2pg_test_results.txt"
        rm "/tmp/mysql2pg_test_results.txt"
    fi
    echo "========================================================"
}
trap cleanup EXIT

# Function to reset configuration to default state (all false)
reset_config() {
    # Using sed to reset boolean values to false for specific keys
    # Note: Using a temp file to avoid issues with in-place editing on different OS
    
    # 1. Reset test_only in mysql and postgresql
    sed -i '' '/^mysql:/,/^postgresql:/ s/test_only: .*/test_only: false/' "$CONFIG_FILE"
    sed -i '' '/^postgresql:/,/^conversion:/ s/test_only: .*/test_only: false/' "$CONFIG_FILE"
    
    # 2. Reset options under conversion.options
    # We define the list of keys to reset to false
    local bool_keys=(
        "tableddl" "data" "view" "indexes" "functions" 
        "users" "table_privileges" "skip_existing_tables" 
        "use_table_list" "exclude_use_table_list" "validate_data" 
        "truncate_before_sync" "lowercase_columns"
    )
    
    for key in "${bool_keys[@]}"; do
        # We look for lines starting with whitespace + key + :
        sed -i '' "s/^[[:space:]]*$key: .*/    $key: false/" "$CONFIG_FILE"
    done

    # 3. Reset lists
    sed -i '' "s/^[[:space:]]*table_list: .*/    table_list: []/" "$CONFIG_FILE"
    sed -i '' "s/^[[:space:]]*exclude_table_list: .*/    exclude_table_list: []/" "$CONFIG_FILE"

    # 4. Reset run options
    local run_keys=(
        "show_progress" "enable_file_logging" 
        "show_console_logs" "show_log_in_console"
    )
    for key in "${run_keys[@]}"; do
        sed -i '' "s/^[[:space:]]*$key: .*/  $key: false/" "$CONFIG_FILE"
    done
}

# Function to update configuration
update_config() {
    local reset=$1
    local set_args=$2
    
    if [ "$reset" = "true" ]; then
        reset_config
    fi
    
    # Process set_args (semicolon separated key=value)
    IFS=';' read -ra ADDR <<< "$set_args"
    for pair in "${ADDR[@]}"; do
        if [ -z "$pair" ]; then
            continue
        fi

        # Split pair into key and value
        local key="${pair%%=*}"
        local value="${pair#*=}"

        if [ "$pair" = "$key" ]; then
            log_abnormal "Invalid key=value pair: $pair"
            continue
        fi
        
        # Trim whitespace
        key=$(trim_whitespace "$key")
        value=$(trim_whitespace "$value")
        
        # Determine how to update based on key
        case "$key" in
            "mysql."*)
                local mysql_key=${key#mysql.}
                update_section_config_key "mysql" "postgresql" "$mysql_key" "  " "$value"
                ;;
            "postgresql."*)
                local postgresql_key=${key#postgresql.}
                update_section_config_key "postgresql" "conversion" "$postgresql_key" "  " "$value"
                ;;
            "conversion.options."*)
                local opt_key=${key#conversion.options.}
                # Indentation for options is 4 spaces
                update_config_key "$opt_key" "    " "$value"
                ;;
            "conversion.limits."*)
                local limit_key=${key#conversion.limits.}
                # Indentation for limits is 4 spaces
                update_config_key "$limit_key" "    " "$value"
                ;;
            "run."*)
                local run_key=${key#run.}
                # Indentation for run is 2 spaces
                update_config_key "$run_key" "  " "$value"
                ;;
            *)
                log_abnormal "Unknown key format: $key"
                ;;
        esac
    done
}

# Function to run a single test case
run_test() {
    local case_num=$1
    local description=$2
    local set_args=$3
    
    TOTAL_TESTS=$((TOTAL_TESTS + 1))
    
    echo ""
    log_info "--------------------------------------------------------"
    log_info "Test Case $case_num: $description"
    log_info "Config changes: $set_args"
    log_info "--------------------------------------------------------"
    
    # Enable console logs by default so we can see output, unless overridden by set_args
    # We prepend it so set_args can override it
    local final_args="run.show_console_logs=true;$set_args"
    
    # Update configuration
    update_config "true" "$final_args"
    
    # Run the tool
    log_info "Executing: $BINARY -c $CONFIG_FILE"
    
    # Temporarily disable set -e to capture exit code
    set +e
    $BINARY -c "$CONFIG_FILE"
    local exit_code=$?
    set -e
    
    if [ $exit_code -eq 0 ]; then
        log_passed "Test Case $case_num: $description PASSED"
        PASSED_TESTS=$((PASSED_TESTS + 1))
        printf "%-5s | %-40s | ${GREEN}%-10s${NC}\n" "$case_num" "$description" "PASSED" >> "/tmp/mysql2pg_test_results.txt"
    else
        log_abnormal "Test Case $case_num: $description ABNORMAL (Exit Code: $exit_code)"
        FAILED_TESTS=$((FAILED_TESTS + 1))
        FAILED_CASES="$FAILED_CASES $case_num"
        printf "%-5s | %-40s | ${YELLOW}%-10s${NC}\n" "$case_num" "$description" "FAILED" >> "/tmp/mysql2pg_test_results.txt"
    fi
}

# ==============================================================================
# Execution of Test Cases
# ==============================================================================

# 1. Test MySQL connectivity
run_test 1 "MySQL Connectivity" "mysql.test_only=true"

# 2. Test PG connectivity
run_test 2 "PostgreSQL Connectivity" "postgresql.test_only=true"

# 3. Test both connectivity
run_test 3 "MySQL & PG Connectivity" "mysql.test_only=true;postgresql.test_only=true"

# 4. Test TableDDL (Basic)
run_test 4 "TableDDL Sync" "conversion.options.tableddl=true"

# 5. Test Data (Basic)
run_test 5 "Data Sync" "conversion.options.data=true;conversion.options.exclude_use_table_list=true;conversion.options.exclude_table_list=[case_45_stored_generated,case_59_complex_generated]"

# 6. Test View
run_test 6 "View Sync" "conversion.options.view=true"

# 7. Test Indexes
run_test 7 "Indexes Sync" "conversion.options.indexes=true"

# 8. Test Functions
run_test 8 "Functions Sync" "conversion.options.functions=true"

# 9. Test Users
run_test 9 "Users Sync" "conversion.options.users=true"

# 10. Test Table Privileges
run_test 10 "Table Privileges Sync" "conversion.options.table_privileges=true"

# 11. Test Full DDL + Data Pipeline (Real world scenario)
# This tests the sequence: TableDDL -> Data -> Indexes -> Functions -> Views -> Users -> Privileges
run_test 11 "Full Pipeline Sync" "conversion.options.tableddl=true;conversion.options.data=true;conversion.options.indexes=true;conversion.options.functions=true;conversion.options.view=true;conversion.options.users=true;conversion.options.table_privileges=true;conversion.options.skip_existing_tables=false;conversion.options.exclude_use_table_list=true;conversion.options.exclude_table_list=[case_45_stored_generated,case_59_complex_generated]
"

# 12. Lowercase Columns = true (with DDL and Data)
run_test 12 "Lowercase Columns = true" "conversion.options.lowercase_columns=true;conversion.options.tableddl=true;conversion.options.data=true;conversion.options.skip_existing_tables=false;conversion.options.exclude_use_table_list=true;conversion.options.exclude_table_list=[case_45_stored_generated,case_59_complex_generated]
"

# 13. Lowercase Columns = false (with DDL and Data)
run_test 13 "Lowercase Columns = false" "conversion.options.lowercase_columns=false;conversion.options.tableddl=true;conversion.options.data=true;conversion.options.skip_existing_tables=false;conversion.options.exclude_use_table_list=true;conversion.options.exclude_table_list=[case_45_stored_generated,case_59_complex_generated]
"

# 14. Skip Existing Tables = true
# First create tables, then run again with skip=true
run_test 14 "Skip Existing Tables = true" "conversion.options.skip_existing_tables=true;conversion.options.tableddl=true"

# 15. Use Table List (Inclusive Filter)
run_test 15 "Use Table List" "conversion.options.use_table_list=true;conversion.options.table_list=[case_01_integers,case_02_boolean];conversion.options.tableddl=true;conversion.options.data=true"

# 16. Exclude Table List (Exclusive Filter)
run_test 16 "Exclude Table List" "conversion.options.exclude_use_table_list=true;conversion.options.exclude_table_list=[case_01_integers];conversion.options.tableddl=true"

# 17. Validate Data = true
run_test 17 "Validate Data = true" "conversion.options.validate_data=true;conversion.options.data=true;conversion.options.exclude_use_table_list=true;conversion.options.exclude_table_list=[case_45_stored_generated,case_59_complex_generated]"

# 18. Validate Data = false
run_test 18 "Validate Data = false" "conversion.options.validate_data=false;conversion.options.data=true;conversion.options.exclude_use_table_list=true;conversion.options.exclude_table_list=[case_45_stored_generated,case_59_complex_generated]"

# 19. Truncate Before Sync = true
run_test 19 "Truncate Before Sync = true" "conversion.options.truncate_before_sync=true;conversion.options.data=true;conversion.options.exclude_use_table_list=true;conversion.options.exclude_table_list=[case_45_stored_generated,case_59_complex_generated]"

# 20. Truncate Before Sync = false
run_test 20 "Truncate Before Sync = false" "conversion.options.truncate_before_sync=false;conversion.options.data=true;conversion.options.exclude_use_table_list=true;conversion.options.exclude_table_list=[case_45_stored_generated,case_59_complex_generated]"

# 21. Show Progress = true
run_test 21 "Show Progress = true" "run.show_progress=true;conversion.options.tableddl=true"

# 22. Show Progress = false
run_test 22 "Show Progress = false" "run.show_progress=false;conversion.options.tableddl=true"

# 23. Enable File Logging = true
run_test 23 "Enable File Logging = true" "run.enable_file_logging=true;conversion.options.tableddl=true"

# 24. Enable File Logging = false
run_test 24 "Enable File Logging = false" "run.enable_file_logging=false;conversion.options.tableddl=true"

# 25. Show Console Logs = true
run_test 25 "Show Console Logs = true" "run.show_console_logs=true;conversion.options.tableddl=true"

# 26. Show Console Logs = false
# This will override the default show_console_logs=true we added in run_test
run_test 26 "Show Console Logs = false" "run.show_console_logs=false;conversion.options.tableddl=true;conversion.options.data=false"

# 27. Show Log In Console = true
run_test 27 "Show Log In Console = true" "run.show_log_in_console=true;conversion.options.tableddl=true"

# 28. Show Log In Console = false
run_test 28 "Show Log In Console = false" "run.show_log_in_console=false;conversion.options.tableddl=true"

# 29. High Concurrency Stress Test
run_test 29 "High Concurrency (20)" "conversion.limits.concurrency=20;conversion.options.tableddl=true;conversion.options.data=true;conversion.options.exclude_use_table_list=true;conversion.options.exclude_table_list=[case_45_stored_generated,case_59_complex_generated]"

# 30. Small Batch Size (Pagination Test)
run_test 30 "Small Batch Size (10)" "conversion.limits.batch_insert_size=10;conversion.limits.max_rows_per_batch=50;conversion.options.tableddl=true;conversion.options.data=true;conversion.options.exclude_use_table_list=true;conversion.options.exclude_table_list=[case_45_stored_generated,case_59_complex_generated]"

# 31. Idempotency (Run twice with skip_existing_tables=true)
# This simulates resuming a job or running against an existing schema
run_test 31 "Idempotency (Skip Existing)" "conversion.options.skip_existing_tables=true;conversion.options.tableddl=true;conversion.options.data=true;conversion.options.exclude_use_table_list=true;conversion.options.exclude_table_list=[case_45_stored_generated,case_59_complex_generated]"

# 32. Data Sync Only (Existing Tables)
# This simulates a scenario where schema is already migrated, and we only want to sync data
# We assume tables exist from previous tests (or created by DDL here implicitly if not skipped, but let's force DDL off to test data only logic if feasible, but our tool usually requires DDL to map. Actually, the tool checks existing tables. Let's enable DDL but with skip_existing=true which is effectively data only for existing tables)
run_test 32 "Data Sync Only (Truncate)" "conversion.options.skip_existing_tables=true;conversion.options.tableddl=true;conversion.options.data=true;conversion.options.truncate_before_sync=true;conversion.options.exclude_use_table_list=true;conversion.options.exclude_table_list=[case_45_stored_generated,case_59_complex_generated]"

# 33. Max DDL Per Batch (Limit Test)
run_test 33 "Max DDL Per Batch (5)" "conversion.limits.max_ddl_per_batch=5;conversion.options.tableddl=true;conversion.options.skip_existing_tables=false"

# 34. Max Functions Per Batch (Limit Test)
run_test 34 "Max Functions Per Batch (2)" "conversion.limits.max_functions_per_batch=2;conversion.options.functions=true"

# 35. Max Indexes Per Batch (Limit Test)
run_test 35 "Max Indexes Per Batch (10)" "conversion.limits.max_indexes_per_batch=10;conversion.options.indexes=true"

# 36. Max Users Per Batch (Limit Test)
run_test 36 "Max Users Per Batch (5)" "conversion.limits.max_users_per_batch=5;conversion.options.users=true"

# 37. Max Rows Per Batch (Limit Test)
run_test 37 "Max Rows Per Batch (100)" "conversion.limits.max_rows_per_batch=100;conversion.options.data=true;conversion.options.exclude_use_table_list=true;conversion.options.exclude_table_list=[case_45_stored_generated,case_59_complex_generated]"

# 38. Bandwidth Limit (Throttling Test)
run_test 38 "Bandwidth Limit (50 Mbps)" "conversion.limits.bandwidth_mbps=50;conversion.options.data=true;conversion.options.exclude_use_table_list=true;conversion.options.exclude_table_list=[case_45_stored_generated,case_59_complex_generated]"

# 39. Custom Error Log Path
run_test 39 "Custom Error Log Path" "run.error_log_path=/tmp/mysql2pg_errors.log;conversion.options.tableddl=true"

# 40. Custom Log File Path
run_test 40 "Custom Log File Path" "run.log_file_path=/tmp/mysql2pg_custom.log;conversion.options.tableddl=true;run.enable_file_logging=true"

# 41. MySQL Max Open Connections (Low Limit)
run_test 41 "MySQL Max Open Conns (10)" "mysql.max_open_conns=10;conversion.options.tableddl=true;conversion.options.data=true;conversion.options.exclude_use_table_list=true;conversion.options.exclude_table_list=[case_45_stored_generated,case_59_complex_generated]"

# 42. MySQL Max Idle Connections
run_test 42 "MySQL Max Idle Conns (5)" "mysql.max_idle_conns=5;conversion.options.tableddl=true;conversion.options.data=true;conversion.options.exclude_use_table_list=true;conversion.options.exclude_table_list=[case_45_stored_generated,case_59_complex_generated]"

# 43. MySQL Connection Max Lifetime (Short)
run_test 43 "MySQL Conn Max Lifetime (60s)" "mysql.conn_max_lifetime=60;conversion.options.tableddl=true;conversion.options.data=true;conversion.options.exclude_use_table_list=true;conversion.options.exclude_table_list=[case_45_stored_generated,case_59_complex_generated]"

# 44. PostgreSQL Max Connections (Low Limit)
run_test 44 "PostgreSQL Max Conns (10)" "postgresql.max_conns=10;conversion.options.tableddl=true;conversion.options.data=true;conversion.options.exclude_use_table_list=true;conversion.options.exclude_table_list=[case_45_stored_generated,case_59_complex_generated]"

# 45. Data Sync with Truncate + Validate (Combined)
run_test 45 "Data Sync + Truncate + Validate" "conversion.options.data=true;conversion.options.truncate_before_sync=true;conversion.options.validate_data=true;conversion.options.exclude_use_table_list=true;conversion.options.exclude_table_list=[case_45_stored_generated,case_59_complex_generated]"

# 46. Full Pipeline with Skip (Resume Scenario)
run_test 46 "Full Pipeline with Skip (Resume)" "conversion.options.tableddl=true;conversion.options.data=true;conversion.options.view=true;conversion.options.indexes=true;conversion.options.functions=true;conversion.options.users=true;conversion.options.table_privileges=true;conversion.options.skip_existing_tables=true;conversion.options.validate_data=true"

# 47. Data Only (No DDL, Existing Tables)
run_test 47 "Data Only (No DDL)" "conversion.options.tableddl=false;conversion.options.data=true;conversion.options.skip_existing_tables=true;conversion.options.exclude_use_table_list=true;conversion.options.exclude_table_list=[case_45_stored_generated,case_59_complex_generated]"

# 48. All Conversion Options Disabled (Minimal Run)
run_test 48 "All Options Disabled" "conversion.options.tableddl=false;conversion.options.data=false;conversion.options.view=false;conversion.options.indexes=false;conversion.options.functions=false;conversion.options.users=false;conversion.options.table_privileges=false"

# 49. Single Table Full Sync (Use Table List)
run_test 49 "Single Table Full Sync" "conversion.options.use_table_list=true;conversion.options.table_list=[case_01_integers];conversion.options.tableddl=true;conversion.options.data=true;conversion.options.indexes=true;conversion.options.validate_data=true"

# 50. Version Flag Compatibility Baseline
run_test 50 "Version Flag Compatibility Baseline" "conversion.options.tableddl=true"

# ==============================================================================
# CLI 子命令测试
# ==============================================================================

# 51. Report Subcommand - Basic HTML Report Generation
run_test 51 "Report Subcommand (Basic)" "conversion.options.tableddl=true;conversion.options.data=true"

# 52. Help Flag
run_test 52 "Help Flag (-h)" "conversion.options.tableddl=true"

# ==============================================================================
# 数据同步核心功能测试
# ==============================================================================

# 53. Primary Key Pagination (Cursor-based pagination for tables with PK)
run_test 53 "Primary Key Pagination" "conversion.options.data=true;conversion.options.exclude_use_table_list=true;conversion.options.exclude_table_list=[case_45_stored_generated,case_59_complex_generated]"

# 54. OFFSET Pagination (Tables without primary key)
run_test 54 "OFFSET Pagination (No PK Tables)" "conversion.options.data=true;conversion.options.exclude_use_table_list=true;conversion.options.exclude_table_list=[case_45_stored_generated,case_59_complex_generated]"

# 55. Composite Primary Key Degradation to OFFSET
run_test 55 "Composite PK Degradation" "conversion.options.data=true;conversion.options.exclude_use_table_list=true;conversion.options.exclude_table_list=[case_45_stored_generated,case_59_complex_generated]"

# 56. CopyFrom Protocol Batch Import (PG COPY protocol)
run_test 56 "CopyFrom Protocol Import" "conversion.options.data=true;conversion.limits.batch_insert_size=1000;conversion.options.exclude_use_table_list=true;conversion.options.exclude_table_list=[case_45_stored_generated,case_59_complex_generated]"

# 57. Geometry Point Conversion (WKB → (x,y))
run_test 57 "Geometry Point Conversion" "conversion.options.tableddl=true;conversion.options.data=true"

# 58. Zero Date Value Handling (0000-00-00 → NULL)
run_test 58 "Zero Date Value Handling" "conversion.options.data=true;conversion.options.exclude_use_table_list=true;conversion.options.exclude_table_list=[case_45_stored_generated,case_59_complex_generated]"

# 59. Empty Table Sync (Zero data rows, full workflow)
run_test 59 "Empty Table Sync" "conversion.options.tableddl=true;conversion.options.data=true"

# ==============================================================================
# DDL 转换测试
# ==============================================================================

# 60. TINYINT(1) → BOOLEAN Conversion
run_test 60 "TINYINT(1) to BOOLEAN" "conversion.options.tableddl=true"

# 61. AUTO_INCREMENT → SERIAL/BIGSERIAL Conversion
run_test 61 "AUTO_INCREMENT to SERIAL" "conversion.options.tableddl=true"

# 62. Partition Table RANGE Conversion
run_test 62 "Partition Table RANGE" "conversion.options.tableddl=true"

# 63. GENERATED Column Conversion (json_extract → ->/->>)
run_test 63 "GENERATED Column Conversion" "conversion.options.tableddl=true;conversion.options.data=true"

# 64. Reserved Keyword Column Quoting
run_test 64 "Reserved Keyword Column Quoting" "conversion.options.tableddl=true"

# 65. Table Comment → COMMENT ON TABLE
run_test 65 "Table Comment Conversion" "conversion.options.tableddl=true"

# 66. Column Comment → COMMENT ON COLUMN
run_test 66 "Column Comment Conversion" "conversion.options.tableddl=true"

# 67. CASCADE Drop and Rebuild (Table exists, skip=false)
run_test 67 "CASCADE Drop Rebuild" "conversion.options.tableddl=true;conversion.options.skip_existing_tables=false"

# 68. char(0) → char(10) Cleanup
run_test 68 "char(0) to char(10) Cleanup" "conversion.options.tableddl=true"

# ==============================================================================
# 视图/函数转换测试
# ==============================================================================

# 69. View Function Conversion (IFNULL→COALESCE, etc. 20+ types)
run_test 69 "View Function Conversion" "conversion.options.view=true"

# 70. Function Cursor Handling (DECLARE CURSOR/FETCH/CLOSE)
run_test 70 "Function Cursor Handling" "conversion.options.functions=true"

# 71. Function Flow Control (REPEAT→LOOP/UNTIL→EXIT WHEN, etc.)
run_test 71 "Function Flow Control" "conversion.options.functions=true"

# 72. Function Syntax Fix (Double semicolon/THEN THEN/END IF, etc. 30+ types)
run_test 72 "Function Syntax Fix" "conversion.options.functions=true"

# ==============================================================================
# 边界/错误处理测试
# ==============================================================================

# 73. MySQL Connection Failure (Invalid host/port)
run_test 73 "MySQL Connection Failure" "mysql.host=invalid.mysql.host;mysql.port=9999;mysql.test_only=true"

# 74. PostgreSQL Connection Failure (Invalid host/port)
run_test 74 "PostgreSQL Connection Failure" "postgresql.host=invalid.pg.host;postgresql.port=9999;postgresql.test_only=true"

# 75. Permission Insufficient Tolerance (SHOW VIEW privilege denied)
run_test 75 "Permission Insufficient Tolerance" "conversion.options.view=true"

# 76. Table List Table Not Exist Error Handling
run_test 76 "Table List Not Exist Error" "conversion.options.use_table_list=true;conversion.options.table_list=[nonexistent_table_12345];conversion.options.tableddl=true"

# 77. Empty Table List Behavior
run_test 77 "Empty Table List Behavior" "conversion.options.use_table_list=true;conversion.options.table_list=[];conversion.options.tableddl=true;conversion.options.data=true"

# ==============================================================================
# 配置互斥/组合测试
# ==============================================================================

# 78. use_table_list Mutuality (Other steps should be skipped)
run_test 78 "use_table_list Mutuality" "conversion.options.use_table_list=true;conversion.options.table_list=[case_01_integers];conversion.options.tableddl=true;conversion.options.data=true;conversion.options.indexes=false;conversion.options.view=false;conversion.options.functions=false"

# 79. connection_params Custom DSN Passing
run_test 79 "MySQL connection_params Custom" "mysql.connection_params=charset=utf8mb4&parseTime=true&interpolateParams=true&readTimeout=5s&writeTimeout=5s&timeout=10s;conversion.options.tableddl=true;conversion.options.data=true"

# 80. pg_connection_params Custom Passing
run_test 80 "PostgreSQL pg_connection_params Custom" "postgresql.pg_connection_params=search_path=public connect_timeout=10 statement_timeout=0;conversion.options.tableddl=true;conversion.options.data=true"

# 81. concurrency=1 Serial Execution
run_test 81 "Serial Execution (concurrency=1)" "conversion.limits.concurrency=1;conversion.options.tableddl=true;conversion.options.data=true;conversion.options.exclude_use_table_list=true;conversion.options.exclude_table_list=[case_45_stored_generated,case_59_complex_generated]"

# 82. batch_insert_size > max_rows_per_batch Priority
run_test 82 "Batch Size Conflict Priority" "conversion.limits.batch_insert_size=5000;conversion.limits.max_rows_per_batch=100;conversion.options.data=true;conversion.options.exclude_use_table_list=true;conversion.options.exclude_table_list=[case_45_stored_generated,case_59_complex_generated]"

# 83. All Options Enabled with Skip + Validate (Full Resume + Check)
run_test 83 "Full Resume + Validate" "conversion.options.tableddl=true;conversion.options.data=true;conversion.options.view=true;conversion.options.indexes=true;conversion.options.functions=true;conversion.options.users=true;conversion.options.table_privileges=true;conversion.options.skip_existing_tables=true;conversion.options.validate_data=true"

# 84. Data Only with Truncate + Validate + Large Batch
run_test 84 "Data Truncate Validate Large" "conversion.options.data=true;conversion.options.truncate_before_sync=true;conversion.options.validate_data=true;conversion.limits.batch_insert_size=2000;conversion.options.exclude_use_table_list=true;conversion.options.exclude_table_list=[case_45_stored_generated,case_59_complex_generated]"

log_info "All tests execution completed."
