#!/bin/bash

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
    echo -e "${YELLOW}[ABNORMAL] $1${NC}"
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
        # Split pair into key and value
        local key=$(echo "$pair" | cut -d'=' -f1)
        local value=$(echo "$pair" | cut -d'=' -f2)
        
        # Trim whitespace
        key=$(echo "$key" | xargs)
        value=$(echo "$value" | xargs)
        
        # Determine how to update based on key
        case "$key" in
            "mysql.max_open_conns")
                sed -i '' '/^mysql:/,/^postgresql:/ s/max_open_conns: .*/max_open_conns: '"$value"'/' "$CONFIG_FILE"
                ;;
            "postgresql.max_conns")
                sed -i '' '/^postgresql:/,/^conversion:/ s/max_conns: .*/max_conns: '"$value"'/' "$CONFIG_FILE"
                ;;
            "mysql.test_only")
                sed -i '' '/^mysql:/,/^postgresql:/ s/test_only: .*/test_only: '"$value"'/' "$CONFIG_FILE"
                ;;
            "postgresql.test_only")
                sed -i '' '/^postgresql:/,/^conversion:/ s/test_only: .*/test_only: '"$value"'/' "$CONFIG_FILE"
                ;;
            "conversion.options."*)
                local opt_key=${key#conversion.options.}
                # Indentation for options is 4 spaces
                sed -i '' "s/^[[:space:]]*$opt_key: .*/    $opt_key: $value/" "$CONFIG_FILE"
                ;;
            "conversion.limits."*)
                local limit_key=${key#conversion.limits.}
                # Indentation for limits is 4 spaces
                sed -i '' "s/^[[:space:]]*$limit_key: .*/    $limit_key: $value/" "$CONFIG_FILE"
                ;;
            "run."*)
                local run_key=${key#run.}
                # Indentation for run is 2 spaces
                sed -i '' "s/^[[:space:]]*$run_key: .*/  $run_key: $value/" "$CONFIG_FILE"
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
run_test 5 "Data Sync" "conversion.options.data=true"

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
run_test 11 "Full Pipeline Sync" "conversion.options.tableddl=true;conversion.options.data=true;conversion.options.indexes=true;conversion.options.functions=true;conversion.options.view=true;conversion.options.users=true;conversion.options.table_privileges=true"

# 12. Lowercase Columns = true (with DDL and Data)
run_test 12 "Lowercase Columns = true" "conversion.options.lowercase_columns=true;conversion.options.tableddl=true;conversion.options.data=true"

# 13. Lowercase Columns = false (with DDL and Data)
run_test 13 "Lowercase Columns = false" "conversion.options.lowercase_columns=false;conversion.options.tableddl=true;conversion.options.data=true"

# 14. Skip Existing Tables = true
# First create tables, then run again with skip=true
run_test 14 "Skip Existing Tables = true" "conversion.options.skip_existing_tables=true;conversion.options.tableddl=true"

# 15. Use Table List (Inclusive Filter)
run_test 15 "Use Table List" "conversion.options.use_table_list=true;conversion.options.table_list=[case_01_integers,case_02_boolean];conversion.options.tableddl=true;conversion.options.data=true"

# 16. Exclude Table List (Exclusive Filter)
run_test 16 "Exclude Table List" "conversion.options.exclude_use_table_list=true;conversion.options.exclude_table_list=[case_01_integers];conversion.options.tableddl=true"

# 17. Validate Data = true
run_test 17 "Validate Data = true" "conversion.options.validate_data=true;conversion.options.data=true"

# 18. Validate Data = false
run_test 18 "Validate Data = false" "conversion.options.validate_data=false;conversion.options.data=true"

# 19. Truncate Before Sync = true
run_test 19 "Truncate Before Sync = true" "conversion.options.truncate_before_sync=true;conversion.options.data=true"

# 20. Truncate Before Sync = false
run_test 20 "Truncate Before Sync = false" "conversion.options.truncate_before_sync=false;conversion.options.data=true"

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
run_test 26 "Show Console Logs = false" "run.show_console_logs=false;conversion.options.tableddl=true"

# 27. Show Log In Console = true
run_test 27 "Show Log In Console = true" "run.show_log_in_console=true;conversion.options.tableddl=true"

# 28. Show Log In Console = false
run_test 28 "Show Log In Console = false" "run.show_log_in_console=false;conversion.options.tableddl=true"

# 29. High Concurrency Stress Test
run_test 29 "High Concurrency (20)" "conversion.limits.concurrency=20;conversion.options.tableddl=true;conversion.options.data=true"

# 30. Small Batch Size (Pagination Test)
run_test 30 "Small Batch Size (10)" "conversion.limits.batch_insert_size=10;conversion.limits.max_rows_per_batch=50;conversion.options.tableddl=true;conversion.options.data=true"

# 31. Idempotency (Run twice with skip_existing_tables=true)
# This simulates resuming a job or running against an existing schema
run_test 31 "Idempotency (Skip Existing)" "conversion.options.skip_existing_tables=true;conversion.options.tableddl=true;conversion.options.data=true"

# 32. Data Sync Only (Existing Tables)
# This simulates a scenario where schema is already migrated, and we only want to sync data
# We assume tables exist from previous tests (or created by DDL here implicitly if not skipped, but let's force DDL off to test data only logic if feasible, but our tool usually requires DDL to map. Actually, the tool checks existing tables. Let's enable DDL but with skip_existing=true which is effectively data only for existing tables)
run_test 32 "Data Sync Only (Truncate)" "conversion.options.skip_existing_tables=true;conversion.options.tableddl=true;conversion.options.data=true;conversion.options.truncate_before_sync=true"

log_info "All tests execution completed."
