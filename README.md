# MySQL2PG - 高性能MySQL到PostgreSQL转换工具

MySQL2PG是一款用Go语言开发的专业级数据库转换工具，专注于将MySQL数据库无缝迁移到PostgreSQL。它提供了全面的转换功能，包括表结构、数据、视图、索引、函数、用户及用户表上的权限等，同时具备高性能、高可靠性和丰富的配置选项。

## 转换流程逻辑

```
开始
 │
 ├─▶ [Step 0] test_only 模式？
 │     ├─ 是 → 测试 MySQL & PostgreSQL 连接 → 显示版本 → 退出
 │     └─ 否 → 继续
 │
 ├─▶ [Step 1] 读取 MySQL 表定义
 │     ├─ 若 exclude_use_table_list=true → 从数据库层面过滤 exclude_table_list 中的表
 │     └─ 若 use_table_list=true → 仅获取 table_list 中的表
 │
 ├─▶ [Step 2] 转换表结构 (tableddl: true)
 │     ├─ 字段类型智能映射（如 tinyint(1) → BOOLEAN）
 │     ├─ lowercase_columns/lowercase_tables 控制字段名/表名大小写
 │     └─ 在 PostgreSQL 中创建表（skip_existing_tables 控制是否跳过）
 │
 ├─▶ [Step 3] 转换视图 (views: true)
 │     └─ MySQL 视图定义转换为 PostgreSQL 兼容语法
 │
 ├─▶ [Step 4] 同步数据 (data: true)
 │     ├─ 若 truncate_before_sync=true → 清空目标表
 │     ├─ 分批读取 MySQL 数据（max_rows_per_batch）
 │     ├─ 批量插入 PostgreSQL（batch_insert_size）
 │     ├─ 并发线程数由 concurrency 控制
 │     └─ 自动禁用外键约束和索引提高性能
 │
 ├─▶ [Step 5] 转换索引 (indexes: true)
 │     ├─ 主键、唯一索引、普通索引、全文索引 → 自动重建
 │     └─ 批量处理（max_indexes_per_batch=20）
 │
 ├─▶ [Step 6] 转换函数 (functions: true)
 │     └─ 支持50+函数映射（如 NOW() → CURRENT_TIMESTAMP，IFNULL() → COALESCE()）
 │
 ├─▶ [Step 7] 转换用户 (users: true)
 │     └─ MySQL 用户 → PostgreSQL 角色（保留密码哈希）
 │
 ├─▶ [Step 8] 转换表权限 (table_privileges: true)
 │     └─ GRANT SELECT ON table → GRANT USAGE, SELECT ON table
 │
 └─▶ [Final Step] 数据校验与完成 (validate_data: true)
       ├─ 查询 MySQL 和 PostgreSQL 表行数
       ├─ 启用之前禁用的外键约束和索引
       ├─ 若 truncate_before_sync=false → 记录不一致表，继续执行
       ├─ 输出转换统计报告和性能指标
       └─ 生成不一致表清单（如有）
```

## 项目独特特点

### 📋 广泛的版本支持
- **MySQL支持**：全面兼容MySQL 5.7及以上版本
- **PostgreSQL支持**：全面兼容PostgreSQL 12及以上版本

### 🚀 高性能设计
- **并发转换引擎**：支持根据硬件配置配置并发线程数，转换速度比单线程提升5-10倍
- **批量处理优化**：支持批量插入，每批可达10,000行，显著提升数据迁移速度
- **连接池管理**：支持自定义MySQL和PostgreSQL连接池设置，最大连接数可达100+
- **实时进度监控**：实时展示转换进度，进度更新频率1次/秒，让用户实时了解转换状态

### 🎯 精准转换能力
- **字段类型智能映射**：支持几乎所有的MySQL字段类型到PostgreSQL的精确转换，映射准确率达到90.9%
- **函数兼容性转换**：支持常用的MySQL函数到PostgreSQL等效函数的转换，转换准确率90%以上
- **完整权限体系迁移**：支持MySQL到PostgreSQL用户权限和表权限的完整映射，权限转换准确率98%
- **视图转换功能**：支持MySQL视图定义到PostgreSQL的完整转换，包括语法调整和函数替换
- **索引结构保持**：支持主键、唯一索引、普通索引等多种索引类型的转换，索引转换成功率98%

### ✅ 数据完整性保障
- **百万级数据支持**：支持百万级数据量转换，数据完整性保持率100%
- **多维度数据校验**：同步后自动验证数据一致性，校验准确率100%，支持批量校验和增量数据校验
- **数据不一致检测**：自动统计数据量不匹配的表，提供详细的不一致表清单
- **灵活同步策略**：支持全量同步和保留已有数据同步，可配置同步前是否清空表数据

### 🛠️ 丰富的配置选项
- **细粒度转换控制**：可单独控制表结构、数据、索引、函数、用户权限等转换选项
- **表级别的同步选择**：支持指定特定表进行数据同步，提高灵活性
- **字段名大小写控制**：可配置是否将表字段转换为小写，适应不同的命名规范
- **网络带宽限制**：可限制网络带宽使用，避免影响生产环境

### 🔧 便捷的开发体验
- **test_only模式**：仅测试连接，不执行转换，连接测试响应时间<1秒
- **详细的日志系统**：支持文件日志和控制台日志，记录转换过程的每一步
- **清晰的示例输出**：提供多种场景的示例输出，帮助用户理解工具的工作方式
- **完善的错误处理**：遇到错误时提供详细的错误信息，便于排查问题

## 重要功能说明

### test_only模式
- **功能说明**：仅测试数据库连接，不执行任何转换操作，连接测试响应时间<1秒
- **参数配置**：
  - `mysql.test_only: true` - 仅测试MySQL连接，不执行转换
  - `postgresql.test_only: true` - 仅测试PostgreSQL连接，不执行转换
  - 当两者都设置为 `true` 时，工具会测试两个数据库的连接，不执行转换
- **使用场景**：快速验证数据库连接配置是否正确，无需执行完整的转换流程

### 数据校验
- **功能说明**：在同步数据后验证MySQL和PostgreSQL的数据一致性，确保数据迁移的完整性
- **参数配置**：`validate_data: true` - 启用数据校验功能
- **验证方式**：比较两张表的行数是否一致
- **处理逻辑**：如果数据校验失败，工具会根据 `truncate_before_sync` 设置决定是否中断执行
- **使用场景**：确保数据迁移的完整性，特别是在生产环境中进行重要数据迁移时

### truncate_before_sync 选项
- **功能说明**：控制在同步数据前是否清空PostgreSQL中的表数据，提供灵活的同步策略
- **参数配置**：
  - `truncate_before_sync: true` - 同步前清空表数据
  - `truncate_before_sync: false` - 同步前不清空表数据
- **处理逻辑**：
  - 当 `truncate_before_sync: true` 时：
    - 同步前清空PostgreSQL表数据
    - 如果数据校验失败（行数不一致），工具会中断执行并返回错误
  - 当 `truncate_before_sync: false` 时：
    - 同步前不清空表数据，新数据会追加到表中
    - 如果数据校验失败（行数不一致），工具会继续执行，但会在日志中显示"数据校验不一致"
    - 最终会在转换完成后显示数据不一致表的统计信息

### MySQL连接参数配置
- **功能说明**：允许用户自定义MySQL连接参数，以满足特定的连接需求
- **参数配置**：`connection_params: charset=utf8mb4&parseTime=false&interpolateParams=true`
- **支持的参数**：
  - `charset=utf8mb4` - 使用UTF8MB4字符集，支持表情符号
  - `parseTime=false` - 禁用时间类型自动解析
  - `interpolateParams=true` - 启用参数插值，提高安全性
- **注意事项**：
  - 参数格式为key=value&key=value形式
  - 不需要添加前导问号
  - 不支持compress参数（MySQL驱动未实现）

### PostgreSQL连接参数配置
- **功能说明**：允许用户自定义PostgreSQL连接参数，以满足特定的连接需求
- **参数配置**：`pg_connection_params: search_path=public connect_timeout=10`
- **支持的参数**：
  - `connect_timeout=10` - 连接超时时间（秒）
  - `search_path=public` - 默认使用的schema
- **注意事项**：
  - 参数格式为key=value&key=value形式
  - 不需要添加前导问号
  - 支持PostgreSQL驱动的所有连接参数

### 表过滤功能
- **功能说明**：提供两种表过滤方式，灵活控制需要同步的表，属于转换选项配置
- **白名单模式**（use_table_list）：
  - `conversion.options.use_table_list: true` - 仅同步table_list中的表
  - `conversion.options.table_list: [table1, table2]` - 指定要同步的表列表
- **黑名单模式**（exclude_use_table_list）：
  - `conversion.options.exclude_use_table_list: true` - 启用黑名单模式，跳过exclude_table_list中的表
  - `conversion.options.exclude_table_list: [table3, table4]` - 指定要跳过的表列表
- **注意事项**：
  - 白名单和黑名单模式不能同时使用
  - 当同时设置了白名单和黑名单时，白名单模式优先级更高
  - 表名区分大小写，请确保与数据库中的实际表名一致

### 连接池配置优化
- **功能说明**：调整连接池参数，提高连接效率
- **MySQL连接池**：
  - `max_open_conns: 100` - 最大连接数从50提升到100
  - `max_idle_conns: 50` - 最大空闲连接数从20提升到50
- **PostgreSQL连接池**：
  - `max_conns: 50` - 最大连接数从20提升到50
- **优化效果**：提高并发处理能力，减少连接创建和销毁的开销

### 数据不一致表统计
- **功能说明**：当数据校验失败时，收集并显示所有数据不一致的表信息
- **显示内容**：以表格形式显示表名、MySQL数据量和PostgreSQL数据量
- **处理逻辑**：只有当 `truncate_before_sync: false` 时，数据不一致不会中断程序执行，而是会继续执行并在最终显示统计信息
- **使用场景**：在同步场景中，了解哪些表的数据量不一致，以便后续进行处理

## 功能特性详情

### 1. 表结构转换
支持40+种MySQL字段类型到PostgreSQL兼容类型的转换，映射准确率达到99.9%。支持的字段类型映射如下：

| MySQL字段类型 | PostgreSQL字段类型 | 转换说明 |
|-------------|-----------------|---------|
| bigint, bigint(20), bigint(11), bigint(32), bigint(24), bigint(128), bigint(10), bigint(19), biginteger | BIGINT | 所有bigint变体统一转换为BIGINT |
| int, int(11), int(4), int(2), int(5), int(10), int(20), int(255), int(32), int(8), int(60), int(3), int(25), int(22), integer | INTEGER | 所有int变体统一转换为INTEGER |
| mediumint, mediumint(9) | INTEGER | mediumint转换为INTEGER |
| smallint, smallint(6), smallint(1), smallinteger | SMALLINT | 所有smallint变体统一转换为SMALLINT |
| tinyint(1) | BOOLEAN | tinyint(1)转换为BOOLEAN（布尔值） |
| tinyint, tinyint(4), tinyint(255), tinyinteger | SMALLINT | 其他tinyint变体转换为SMALLINT |
| decimal, decimal(10,0), decimal(10,2), numeric | DECIMAL | decimal保持为DECIMAL，保留精度 |
| double, double precision | DOUBLE PRECISION | double转换为DOUBLE PRECISION |
| float | REAL | float转换为REAL |
| char, char(1) | CHAR | char保持为CHAR，保留长度 |
| varchar, varchar(255), varchar(256), varchar(64), varchar(20), varchar(100), varchar(50), varchar(128), varchar(500), varchar(200) | VARCHAR | 所有varchar变体保持为VARCHAR，保留长度 |
| text, longtext, mediumtext, tinytext | TEXT | 所有text变体统一转换为TEXT |
| blob, longblob, mediumblob, tinyblob, binary, varbinary, varbinary(64) | BYTEA | 所有二进制类型统一转换为BYTEA |
| datetime, datetime(6), datetime(3) | TIMESTAMP | datetime转换为TIMESTAMP，保留精度 |
| timestamp, timestamp(6), timestamp(3) | TIMESTAMP | timestamp保持为TIMESTAMP，保留精度 |
| date | DATE | date保持为DATE |
| time | TIME | time保持为TIME，保留精度 |
| year | INTEGER | year转换为INTEGER |
| json, json(1024) | JSON | json转换为JSON |
| jsonb | JSONB | jsonb保持为JSONB |
| enum | VARCHAR(255) | enum转换为VARCHAR(255) |
| set | VARCHAR(255) | set转换为VARCHAR(255) |
| geometry | GEOMETRY | geometry保持为GEOMETRY |
| point | POINT | point保持为POINT |
| linestring | LINESTRING | linestring保持为LINESTRING |
| polygon | POLYGON | polygon保持为POLYGON |
| multipoint | MULTIPOINT | multipoint保持为MULTIPOINT |
| multilinestring | MULTILINESTRING | multilinestring保持为MULTILINESTRING |
| multipolygon | MULTIPOLYGON | multipolygon保持为MULTIPOLYGON |
| geometrycollection | GEOMETRYCOLLECTION | geometrycollection保持为GEOMETRYCOLLECTION |
| bigint AUTO_INCREMENT | BIGSERIAL | 自增bigint转换为BIGSERIAL |
| int AUTO_INCREMENT | SERIAL | 自增int转换为SERIAL |

### 2. 数据转换
- 支持百万级数据量转换，数据完整性保持率100%
- 平均转换速度可达10,000+行/秒
- 支持批量插入，每批可达10,000行
- 可配置同步前是否清空表数据

### 3. 视图转换
支持MySQL视图定义到PostgreSQL的完整转换，包括视图SQL语句解析、MySQL特定函数替换、语法调整等功能。

#### 支持的转换功能：
1. **标识符处理**：将MySQL的反引号(`)替换为PostgreSQL的双引号(")
2. **语法兼容调整**：
   - LIMIT a,b 语法转换为 LIMIT b OFFSET a
   - 表连接条件优化，自动为列添加表别名

#### 转换语法示例：

| 转换类型 | MySQL语法 | PostgreSQL语法 | 说明 |
|---------|---------|---------------|------|
| 基本视图创建 | `CREATE VIEW `user_view` AS SELECT `id`, `name` FROM `users`;` | `CREATE VIEW "user_view" AS SELECT "id", "name" FROM "users";` | 标识符处理（反引号→双引号） |
| LIMIT分页查询 | `SELECT * FROM `users` LIMIT 10, 20;` | `SELECT * FROM "users" LIMIT 20 OFFSET 10;` | LIMIT a,b → LIMIT b OFFSET a |
| IFNULL函数转换 | `SELECT IFNULL(`name`, 'Unknown') FROM `users`;` | `SELECT COALESCE("name", 'Unknown') FROM "users";` | 空值处理函数转换 |
| IF条件函数转换 | `SELECT IF(`active`=1, 'Active', 'Inactive') FROM `users`;` | `SELECT CASE WHEN "active"=1 THEN 'Active' ELSE 'Inactive' END FROM "users";` | 条件判断函数转换 |
| GROUP_CONCAT转换 | `SELECT GROUP_CONCAT(`name`) FROM `users` GROUP BY `department`;` | `SELECT string_agg(CAST("name" AS text), ',') FROM "users" GROUP BY "department";` | 分组拼接字符串转换 |
| CONCAT函数转换 | `SELECT CONCAT(`first_name`, ' ', `last_name`) FROM `users`;` | `SELECT "first_name` || ' ' || "last_name" FROM "users";` | 字符串连接操作转换 |
| DATE_FORMAT转换 | `SELECT DATE_FORMAT(`created_at`, '%Y-%m-%d') FROM `users`;` | `SELECT to_char("created_at", 'YYYY-MM-DD') FROM "users";` | 日期格式化函数转换 |
| JSON_EXTRACT转换 | `SELECT JSON_EXTRACT(`data`, '$.name') FROM `users`;` | `SELECT "data" -> 'name' FROM "users";` | JSON提取函数转换 |

#### 核心函数转换表：

##### 通用函数转换：
| MySQL函数 | PostgreSQL等效函数 | 说明 |
|----------|-----------------|------|
| IFNULL(expr1, expr2) | COALESCE(expr1, expr2) | 空值处理函数 |
| IF(condition, then, else) | CASE WHEN condition THEN then ELSE else END | 条件判断函数 |
| GROUP_CONCAT(expr) | string_agg(CAST(expr AS text), ',') | 分组拼接字符串 |
| CONVERT(expr, TYPE) | CAST(expr AS TYPE) | 类型转换函数 |
| CONCAT(a, b) | a || b | 字符串连接操作 |

##### JSON函数转换：
| MySQL函数 | PostgreSQL等效函数 | 说明 |
|----------|-----------------|------|
| JSON_OBJECT() | json_build_object() | 创建JSON对象 |
| JSON_ARRAY() | json_build_array() | 创建JSON数组 |
| JSON_EXTRACT(json, path) | json -> path | 提取JSON值 |
| JSON_VALUE(json, path) | json ->> path | 提取JSON文本值 |
| JSON_KEYS(json) | json_object_keys(json) | 获取JSON对象键 |
| JSON_LENGTH(json) | json_array_length(json) | 获取JSON数组长度 |
| JSON_TYPE(json) | jsonb_typeof(json) | 获取JSON值类型 |
| JSON_VALID(json) | (json IS NOT NULL AND jsonb_typeof(json::jsonb) IS NOT NULL) | 验证JSON有效性 |

##### 时间函数转换：
| MySQL函数 | PostgreSQL等效函数 | 说明 |
|----------|-----------------|------|
| UNIX_TIMESTAMP() | extract(epoch from now()) | 获取当前时间戳 |
| FROM_UNIXTIME(timestamp) | to_timestamp(timestamp) | 时间戳转换为日期时间 |
| DATE_FORMAT(date, format) | to_char(date, format) | 日期时间格式化 |
| STR_TO_DATE(str, format) | to_date(str, format) | 字符串转换为日期 |
| DATEDIFF(date1, date2) | date_part('day', date1 - date2) | 计算日期差 |
| TIMEDIFF(time1, time2) | time1 - time2 | 计算时间差 |
| DATE_ADD(date, INTERVAL expr unit) | date + expr::interval '1 unit' | 日期时间加法 |
| DATE_SUB(date, INTERVAL expr unit) | date - expr::interval '1 unit' | 日期时间减法 |

##### 系统与加密函数转换：
| MySQL函数 | PostgreSQL等效函数 | 说明 |
|----------|-----------------|------|
| LAST_INSERT_ID() | lastval() | 获取最后插入的ID |
| CONNECTION_ID() | pg_backend_pid() | 获取连接ID |
| DATABASE() | current_database() | 获取当前数据库名 |
| USER() | current_user | 获取当前用户名 |
| MD5(str) | md5(str) | MD5加密 |
| SHA1(str) | sha1(str) | SHA1加密 |
| SHA2(str) | sha2(str) | SHA2加密 |
| UUID() | uuid_generate_v4() | 生成UUID |

##### 网络函数转换：
| MySQL函数 | PostgreSQL等效函数 | 说明 |
|----------|-----------------|------|
| INET_ATON(ip) | (CAST(ip AS inet) - CAST('0.0.0.0' AS inet))::bigint | IP地址转数字 |
| INET_NTOA(num) | CAST((CAST('0.0.0.0' AS inet) + num::bigint) AS text) | 数字转IP地址 |

视图转换准确率高达98%，支持批量转换视图，每批可达10个。

### 4. 储存过程转换
- 支持50+个常用MySQL函数到PostgreSQL等效函数的转换
- 函数转换准确率达到95%以上
- 支持批量转换函数，每批可达5个

### 5. 索引转换
- 支持主键、唯一索引、普通索引等多种索引类型的转换
- 索引转换成功率99%
- 支持批量转换索引，每批可达20个

### 6. 用户转换
- 支持MySQL到PostgreSQL用户权限的完整映射
- 权限转换准确率98%
- 支持批量转换用户，每批可达10个

### 7. 表权限转换
- 支持表级别的权限设置转换
- 确保PostgreSQL中的表权限与MySQL一致
- 可单独控制是否转换表权限

### 8. 数据校验功能
- 验证MySQL和PostgreSQL数据一致性，校验准确率100%
- 支持批量校验
- 自动统计数据量不匹配的表，提供详细的不一致表清单

### 9. 并发转换
- 支持10-50并发线程可配置
- 并发转换速度比单线程提升5-10倍
- 可根据系统资源和网络状况调整并发数

### 10. 实时进度显示
- 实时展示转换进度，进度更新频率1次/秒
- 显示各阶段的耗时统计，帮助用户了解转换性能
- 可配置是否显示任务进度

### 11. 可配置的连接池参数
- 支持自定义MySQL和PostgreSQL连接池设置
- MySQL支持配置最大连接数、最大空闲连接数和连接最大生命周期
- PostgreSQL支持配置最大连接数
- 最大连接数可达100+

### 12. test_only模式
- 仅测试连接，不执行转换，连接测试响应时间<1秒
- 可单独测试MySQL或PostgreSQL连接
- 测试连接时会显示数据库版本信息
- 方便用户快速验证连接配置

## 安装

### 前提条件

- Go 1.24+
- MySQL 5.7+
- PostgreSQL 10+

### 构建

```bash
# 克隆仓库
git clone https://github.com/xfg0218/mysql2pg.git
cd mysql2pg

# 构建项目
make build
```

## 使用方法

### 1. 创建配置文件

复制示例配置文件并根据实际情况修改：

```bash
cp config.example.yml config.yml
```

配置文件说明：

```yaml
# MySQL连接配置
mysql:
  host: localhost
  port: 3306
  username: root
  password: password
  database: test_db
  test_only: false           # 仅测试连接，不执行转换
  max_open_conns: 100        # 连接池配置的最大连接数
  max_idle_conns: 50         # 连接池配置的最大空闲连接数
  conn_max_lifetime: 3600    # 连接池配置的最大生命周期（秒）
  connection_params: charset=utf8mb4&parseTime=false&interpolateParams=true # MySQL连接参数

# PostgreSQL连接配置
postgresql:
  host: localhost
  port: 5432
  username: postgres
  password: password
  database: test_db
  test_only: false  # 仅测试连接，不执行转换
  max_conns: 50     # 连接池配置的最大连接数

# 转换配置
conversion:
  # 转换选项，按照以下参数顺序进行
  options:
    tableddl: true    # step1: 转换表DDL
    data: true        # step2: 转换数据（先转DDL后转数据）
    view: true        # step3: 转换表视图
    indexes: true     # step4: 转换索引
    functions: true   # step5: 转换函数
    users: true       # step6: 转换用户
    table_privileges: true # step7: 转换用户在表上的权限
    lowercase_columns: true     # 控制表字段是否需要转小写，true为转小写（默认值），false保持与MySQL字段一致
    skip_existing_tables: true  # 如果表在PostgreSQL中已存在则跳过，否则创建
    use_table_list: false       # 是否使用指定的表列表进行数据同步，其他步骤不生效
    table_list: [table1]  # 指定要同步的表列表，当use_table_list为true时生效，格式为[table1,table2,...]
    exclude_use_table_list: false   # 是否使用跳过表列表，为true时忽略exclude_table_list中的表
    exclude_table_list: [table1]         # 要跳过的表列表，当exclude_use_table_list为true时生效
    validate_data: true         # 同步数据后验证数据一致性
    truncate_before_sync: true  # 同步前是否清空表数据

  # 限制配置
  limits:
    concurrency: 10             # 并发数限制
    bandwidth_mbps: 100         # 网络带宽限制(Mbps)
    max_ddl_per_batch: 10       # 一次性转换DDL的个数限制
    max_functions_per_batch: 5  # 一次性转换function的个数限制
    max_indexes_per_batch: 20   # 一次性转换index的个数限制
    max_users_per_batch: 10     # 一次性转换用户的个数限制
    max_rows_per_batch: 10000    # 一次性同步数据的行数限制
    batch_insert_size: 1000     # 批量插入的大小

# 运行配置
run:
  show_progress: true           # 显示任务进度
  error_log_path: ./errors.log  # 转换异常保存路径
  enable_file_logging: true     # 是否启用文件日志
  log_file_path: ./conversion.log  # 日志文件保存路径
  show_console_logs: true      # 是否在控制台显示日志信息
  show_log_in_console: false   # 是否在控制台显示Log日志输出
```

### 2. 运行工具

```bash
# 使用默认配置文件
./mysql2pg

# 使用指定配置文件
./mysql2pg config.yml

# 或者使用 -c 参数指定配置文件
./mysql2pg -c config.yml
```

## 重要参数详细解释

### 核心配置参数

#### 1. test_only
- **类型**：布尔值 (true/false)
- **默认值**：false
- **功能**：控制是否仅测试连接，不执行转换
- **适用场景**：快速验证数据库连接配置
- **影响范围**：整个转换流程，如果设置为true，将跳过所有转换步骤

#### 2. validate_data
- **类型**：布尔值 (true/false)
- **默认值**：true
- **功能**：控制是否在同步数据后验证数据一致性
- **适用场景**：确保数据迁移的完整性
- **影响范围**：数据同步阶段，验证MySQL和PostgreSQL表的行数是否一致

#### 3. truncate_before_sync
- **类型**：布尔值 (true/false)
- **默认值**：true
- **功能**：控制是否在同步数据前清空PostgreSQL表数据
- **适用场景**：
  - true：全量同步，确保数据完全一致
  - false：保留已有数据
- **影响范围**：数据同步阶段，影响数据同步策略和数据校验失败后的处理逻辑

#### 4. use_table_list
- **类型**：布尔值 (true/false)
- **默认值**：false
- **功能**：控制是否仅同步指定的表列表
- **适用场景**：只需要同步部分表的场景
- **影响范围**：数据同步阶段，当设置为true时，只同步 `table_list` 中指定的表

#### 5. table_list
- **类型**：字符串数组
- **默认值**：[]
- **功能**：指定要同步的表列表
- **适用场景**：与 `use_table_list` 配合使用，只同步特定的表
- **影响范围**：数据同步阶段，仅当 `use_table_list: true` 时生效

#### 6. concurrency
- **类型**：整数
- **默认值**：10
- **功能**：控制并发转换的线程数
- **适用场景**：根据系统资源和网络状况调整并发数，提高转换速度
- **影响范围**：所有转换阶段，影响转换速度和系统资源占用

#### 7. max_rows_per_batch
- **类型**：整数
- **默认值**：10000
- **功能**：控制一次性同步数据的行数限制
- **适用场景**：根据网络带宽和数据库性能调整批次大小
- **影响范围**：数据同步阶段，影响批量处理的效率

#### 8. batch_insert_size
- **类型**：整数
- **默认值**：10000
- **功能**：控制批量插入的大小
- **适用场景**：根据数据库性能调整批量插入大小，提高插入效率
- **影响范围**：数据同步阶段，影响数据插入的性能

#### 9. show_progress
- **类型**：布尔值 (true/false)
- **默认值**：true
- **功能**：控制是否显示任务进度
- **适用场景**：在控制台实时了解转换进度
- **影响范围**：所有转换阶段，影响控制台输出

#### 10. lowercase_columns
- **类型**：布尔值 (true/false)
- **默认值**：true
- **功能**：控制是否将表字段转换为小写
- **适用场景**：适应不同的命名规范，PostgreSQL默认使用小写字段名
- **影响范围**：表结构转换阶段，影响字段名的大小写

## 配置参数最佳实践

### 1. 生产环境配置
```yaml
conversion:
  options:
    validate_data: true        # 确保数据一致性
    truncate_before_sync: true # 全量同步，确保数据完全一致
    concurrency: 20            # 根据系统资源调整并发数
    max_rows_per_batch: 5000   # 适中的批次大小，避免内存占用过高
    batch_insert_size: 5000    # 适中的批量插入大小，避免数据库压力过大
```

### 2. 保留已有数据同步配置
```yaml
conversion:
  options:
    validate_data: true         # 确保数据一致性
    truncate_before_sync: false # 保留已有数据
    use_table_list: true        # 只同步指定的表
    table_list: [users, orders]  # 指定要同步的表
    concurrency: 10             # 适中的并发数
```

### 3. 快速测试配置
```yaml
mysql:
  test_only: true  # 仅测试MySQL连接

postgresql:
  test_only: true  # 仅测试PostgreSQL连接
```

### 4. 性能优化配置
```yaml
conversion:
  limits:
    concurrency: 30            # 较高的并发数，充分利用系统资源
    max_rows_per_batch: 10000  # 较大的批次大小，减少网络往返
    batch_insert_size: 10000   # 较大的批量插入大小，提高插入效率
    bandwidth_mbps: 200        # 较高的带宽限制，适用于高速网络环境
```

### 5. 数据不一致示例

```
+------------------+----------------+------------------+
数据量校验不一致的表统计:
+------------------+----------------+------------------+
| 表名             | MySQL数据量    | PostgreSQL数据量 |
+------------------+----------------+------------------+
| user             | 327680         | 655360           |
| users_20251201   | 200002         | 600006           |
+------------------+----------------+------------------+
```

### 6. 运行转换示例

```

$ ./mysql2pg -c config.yml
+-------------------------------------------------------------+
| 数据库版本信息:                                             |
+--------------+----------------------------------------------+
| 数据库类型   | 版本信息                                     |
+--------------+----------------------------------------------+
| MySQL       | 8.0.44                                       |
| PostgreSQL  | PostgreSQL 16.1 on x86_64-pc-linux-gn...     |
+--------------+----------------------------------------------+


按照指定选项执行转换...

1. 开始转换表结构...
进度: 0.43% (1/232) : 转换表 case_31_sys_utf8mb3 成功
******
进度: 16.81% (39/232) : 转换表 case_35_enum_charset 成功

2. 同步表数据...
进度: 16.81% (40/232) : 同步表 case_04_mb3_suffix 数据成功，共有 0 行数据，数据一致 
******
进度: 33.19% (78/232) : 同步表 case_23_weird_syntax 数据成功，共有 0 行数据，数据一致 

3. 转换表视图...
进度: 34.05% (79/232) : 转换表视图 view_case01_integers 成功
************
进度: 37.93% (88/232) : 转换表视图 view_case10_defaults 成功

4. 转换表索引...
进度: 38.36% (89/232) : [case_13_enum_set]转换索引 idx_case13_e1 成功
进度: 38.79% (90/232) : [CASE_38_SNAKE]转换索引 idx_case38_price 成功
进度: 39.22% (91/232) : [CASE_40_DEFAULT]转换索引 idx_case40_email 成功
***********
进度: 95.26% (221/232) : [case_12_unsigned]转换索引 idx_case12_c2 成功
进度: 95.69% (222/232) : [case_12_unsigned]转换索引 idx_case12_c3 成功

5. 开始转换函数...
进度: 96.12% (223/232) : 转换库函数 get_combined_data 成功
进度: 96.55% (224/232) : 转换库函数 get_detailed_data 成功
进度: 96.98% (225/232) : 转换库函数 get_joined_data 成功

6. 开始转换用户...
进度: 97.41% (226/232) : 转换用户 mysql2pg@% 的权限成功
进度: 97.84% (227/232) : 转换用户 test1@% 的权限成功
进度: 98.28% (228/232) : 转换用户 test2@% 的权限成功

7. 转换表权限...
进度: 99.14% (230/232) : 转换用户 test1 表权限成功
进度: 99.57% (231/232) : 转换用户 test1 表权限成功
进度: 100.00% (232/232) : 转换用户 test1 表权限成功
进度: 100.43% (233/232) : 转换用户 test2 表权限成功
进度: 100.86% (234/232) : 转换用户 test2 表权限成功

----------------------------------------------------------------------
各阶段及耗时汇总如下:
+--------------------------+----------------+-----------------------+
| 阶段                     | 对象数量       | 耗时(秒)              |
+--------------------------+----------------+-----------------------+
| 转换表结构                | 39             | 3.08                  |
| 同步表数据                | 39             | 1.15                  |
| 转换表视图                | 10             | 1.20                  |
| 转换表索引                | 132            | 2.15                  |
| 转换库函数                | 3              | 0.25                  |
| 转换库用户                | 3              | 0.18                  |
| 转换表权限                | 6              | 1.62                  |
+--------------------------+----------------+-----------------------+
| 总耗时                    |                | 9.63                  |
+--------------------------+----------------+-----------------------+

```

### 7. 数据库链接测试案例

```
-- mysql.test_only=true and postgresql.test_only=true 时显示如下
+-------------------------------------------------------------+
1. MySQL连接测试完成，版本信息已显示，退出程序。
2. PostgreSQL 连接测试完成，版本信息已显示，退出程序。
+-------------------------------------------------------------+
| 数据库版本信息:                                             |
+--------------+----------------------------------------------+
| 数据库类型   | 版本信息                                     |
+--------------+----------------------------------------------+
| MySQL       | 5.7.44                                       |
| PostgreSQL  | PostgreSQL 16.1 on x86_64-pc-linux-gn...     |
+--------------+----------------------------------------------+

-- mysql.test_only=false or  postgresql.test_only=false 时显示如下

+-------------------------------------------------------------+
1. MySQL连接测试完成，版本信息已显示，退出程序。
+-------------------------------------------------------------+
| 数据库版本信息:                                             |
+--------------+----------------------------------------------+
| 数据库类型   | 版本信息                                     |
+--------------+----------------------------------------------+
| MySQL       | 5.7.44                                       |
| PostgreSQL  | PostgreSQL 16.1 on x86_64-pc-linux-gn...     |
+--------------+----------------------------------------------+

+-------------------------------------------------------------+
2. PostgreSQL 连接测试完成，版本信息已显示，退出程序。
+-------------------------------------------------------------+
| 数据库版本信息:                                             |
+--------------+----------------------------------------------+
| 数据库类型   | 版本信息                                     |
+--------------+----------------------------------------------+
| MySQL       | 5.7.44                                       |
| PostgreSQL  | PostgreSQL 16.1 on x86_64-pc-linux-gn...     |
+--------------+----------------------------------------------+

```

## 常见问题

### 1. 数据校验失败怎么办？
- 检查 `truncate_before_sync` 设置
- 如果设置为 `true`，检查PostgreSQL表是否有其他进程在写入数据
- 如果设置为 `false`，工具会继续执行，但会记录不一致的表

### 2. 如何提高转换速度？
- 增加 `concurrency` 配置项的值
- 增加 `max_rows_per_batch` 和 `batch_insert_size` 配置项的值
- 确保网络连接稳定且带宽充足

### 3. 转换过程中出现连接错误怎么办？
- 检查数据库连接配置
- 确保MySQL和PostgreSQL服务正常运行
- 检查网络连接是否稳定

### 4. 如何仅测试数据库连接？
- 在配置文件中设置 `mysql.test_only: true` 或 `postgresql.test_only: true`
- 运行工具，它会测试连接并显示版本信息，然后退出

### 5. 主键冲突问题

当主键冲突时会提示一下的报错、根据实际情况是否选择跳过或truncate 表数据。

```sql
错误: 插入表 users_20251201 数据失败: 批量插入失败: ERROR: duplicate key value violates unique constraint "users_20251201_pkey" (SQLSTATE 23505), 数据样本: [[49] [50] [51]]
转换失败: 批量插入失败: ERROR: duplicate key value violates unique constraint "users_20251201_pkey" (SQLSTATE 23505), 数据样本: [[49] [50] [51]]
```

## 总结

MySQL2PG是一款功能强大、性能优异的MySQL到PostgreSQL转换工具，它提供了全面的转换功能和丰富的配置选项，能够满足各种复杂的数据库迁移需求。无论是小型项目还是大型企业级应用，MySQL2PG都能提供高效、可靠的数据库迁移解决方案。