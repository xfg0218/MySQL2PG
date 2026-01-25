-- MySQL2PG 测试表定义文件
-- 包含各种MySQL语法场景，用于测试MySQL到PostgreSQL的转换功能
-- 
-- 表列表及其特点：
-- 1. case_01_integers         - 整数类型测试（tinyint, smallint, mediumint, int, bigint）
-- 2. case_02_boolean          - 布尔类型测试（tinyint(1) 转换为 BOOLEAN）
-- 3. case_03_floats           - 浮点数类型测试（float, double, decimal, numeric）
-- 4. case_04_mb3_suffix       - 字符类型测试（mb3后缀）
-- 5. case_05_charsets         - 字符集类型测试（utf8, utf8mb4, latin1, utf16）
-- 6. case_06_collates         - 排序规则类型测试（utf8mb4_general_ci, utf8_bin等）
-- 7. case_07_complex_charsets - 复杂字符集类型测试
-- 8. case_08_json             - JSON类型测试
-- 9. case_09_datetime         - 日期时间类型测试（date, time, datetime, timestamp, year）
-- 10. case_10_defaults        - 默认值类型测试
-- 11. case_11_autoincrement   - 自增类型测试（AUTO_INCREMENT）
-- 12. case_12_unsigned        - 无符号类型测试（unsigned, zerofill）
-- 13. case_13_enum_set        - 枚举和集合类型测试（enum, set）
-- 14. case_14_binary          - 二进制类型测试（binary, varbinary, blob系列）
-- 15. case_15_options         - 表选项类型测试（ENGINE, CHARSET, COLLATE, ROW_FORMAT）
-- 16. case_16_partition        - 分区类型测试（RANGE分区）
-- 17. case_17_temp            - 临时表类型测试
-- 18. case_18_quotes          - 引号类型测试（反引号包围的表名和列名）
-- 19. case_19_comments         - 注释类型测试（列注释和表注释）
-- 20. case_20_constraints      - 约束类型测试（PRIMARY KEY, KEY, UNIQUE KEY, INDEX）
-- 21. case_21_virtual          - 虚拟列类型测试（VIRTUAL生成列）
-- 22. case_22_spatial          - 空间类型测试（geometry, point, linestring等）
-- 23. case_23_weird_syntax     - 怪异语法类型测试（不规则空格、大小写混合等）
-- 24. case_24_edge_cases       - 边缘情况类型测试
-- 25. case_25_mysql8_reserved  - MySQL 8.0保留字测试（rank, system, groups等）
-- 26. case_26_mysql8_invisible - MySQL 8.0不可见列测试（INVISIBLE列）
-- 27. case_27_mysql8_check     - MySQL 8.0检查约束测试（CHECK约束）
-- 28. case_28_mysql8_func_index - MySQL 8.0函数索引测试
-- 29. case_29_mysql8_defaults  - MySQL 8.0默认值测试（函数默认值）
-- 30. case_30_mysql8_collations - MySQL 8.0字符集和排序规则测试
-- 31. case_31_sys_utf8mb3      - MySQL 8.0系统表测试（utf8mb3字符集）
-- 32. case_32_complex_generated - MySQL 8.0复杂生成列测试（包含CASE表达式）
-- 33. case_33_desc_index       - MySQL 8.0降序索引测试
-- 34. case_34_table_options    - MySQL 8.0表选项测试
-- 35. case_35_enum_charset     - MySQL 8.0枚举和集合字符集测试
-- 36. case_36_uppercase        - MySQL 8.0大写表名测试
-- 37. case_37_hump             - MySQL 8.0驼峰表名测试
-- 38. case_38_snake            - MySQL 8.0蛇形表名测试
-- 39. case_39_underscore       - MySQL 8.0下划线表名测试
-- 40. case_40_default          - MySQL 8.0默认值测试
-- 41. case_41_foreign_key      - 外键约束测试
-- 42. case_42_fulltext         - 全文索引测试
-- 43. case_43_spatial_index    - 空间索引测试
-- 44. case_44_composite_pk     - 复合主键测试
-- 45. case_45_stored_generated - 存储生成列测试（STORED和VIRTUAL）
-- 46. case_46_myisam           - MyISAM存储引擎测试
-- 47. case_47_memory           - MEMORY存储引擎测试
-- 48. case_48_index_types      - 不同索引类型测试（BTREE, HASH）
-- 49. case_49_list_partition   - LIST分区测试
-- 50. case_50_hash_partition   - HASH分区测试
-- 51. case_51_copy_like        - 表复制测试（CREATE TABLE LIKE）
-- 52. case_52_copy_as          - 表数据复制测试（CREATE TABLE AS SELECT）
-- 53. case_53_deferred_constraint - 延迟约束测试
-- 54. case_54_tablespace       - 表空间测试
-- 55. case_55_compressed       - 压缩表测试
-- 56. case_56_encrypted        - 加密表测试
-- 57. case_57_column_privileges - 列级权限测试
-- 58. case_58_subpartition     - 子分区测试
-- 59. case_59_complex_generated - 复杂生成列测试（包含多函数表达式）
-- 60. case_60_statistics       - 表统计信息测试（STATS_PERSISTENT等）
-- 61. case_61_many_columns     - 大量列测试（20+列）
-- 62. case_62_various_defaults - 多样默认值测试（函数默认值、JSON默认值等）
-- 63. case_63_charset_collation - 多语言字符集测试（utf8mb4_zh_0900_as_cs等）

-- 创建整数类型表
DROP TABLE IF EXISTS case_01_integers;
CREATE TABLE case_01_integers (
  col_tiny tinyint,               -- -> SMALLINT
  col_small smallint,             -- -> SMALLINT
  col_medium mediumint,           -- -> INTEGER
  col_int int,                    -- -> INTEGER
  col_integer integer,            -- -> INTEGER
  col_big bigint,                 -- -> BIGINT
  col_int_prec int(11),           -- -> INTEGER (precision ignored)
  col_big_prec bigint(20)         -- -> BIGINT (precision ignored)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 创建布尔类型表
DROP TABLE IF EXISTS case_02_boolean;
CREATE TABLE case_02_boolean (
  is_active tinyint(1),           -- -> BOOLEAN
  status tinyint(4),              -- -> SMALLINT (not 1, so not boolean)
  is_deleted TINYINT(1)           -- -> BOOLEAN (case insensitive)
) ENGINE=InnoDB;

-- 创建浮点数类型表
DROP TABLE IF EXISTS case_03_floats;
CREATE TABLE case_03_floats (
  col_float float,                -- -> REAL
  col_float_p float(10),          -- -> REAL(10)
  col_float_ps float(10,2),       -- -> REAL(10,2)
  col_double double,              -- -> DOUBLE PRECISION
  col_double_ps double(10,2),     -- -> DOUBLE PRECISION(10,2)
  col_decimal decimal(10,2),      -- -> DECIMAL(10,2)
  col_numeric numeric(10,2),      -- -> NUMERIC(10,2)
  col_real real                   -- -> REAL
) ENGINE=InnoDB;

-- 创建字符类型表
DROP TABLE IF EXISTS case_04_mb3_suffix;
CREATE TABLE case_04_mb3_suffix (
  col_var_mb3 varchar(255) CHARACTER SET utf8mb3,    -- -> VARCHAR(255)
  col_char_mb3 char(10) CHARACTER SET utf8mb3,       -- -> CHAR(10)
  col_text_mb3 text CHARACTER SET utf8mb3,           -- -> TEXT
  col_mixed_mb3 varchar(100) CHARACTER SET utf8mb3  -- -> VARCHAR(100)
) ENGINE=InnoDB;

-- 创建字符集类型表
DROP TABLE IF EXISTS case_05_charsets;
CREATE TABLE case_05_charsets (
  c1 varchar(20) character set utf8,
  c2 varchar(20) CHARACTER SET utf8mb4,
  c3 varchar(20) character set latin1,
  c4 varchar(20) character set utf16,
  c5 varchar(20) charset utf8mb4,
  c6 varchar(20) charset latin1
) ENGINE=InnoDB;

-- 创建排序规则类型表
DROP TABLE IF EXISTS case_06_collates;
CREATE TABLE case_06_collates (
  c1 varchar(20) collate utf8mb4_general_ci,
  c2 varchar(20) COLLATE utf8mb4_unicode_ci,
  c3 varchar(20) collate utf8_bin,
  c4 varchar(20) collate latin1_swedish_ci,
  c5 varchar(20) COLLATE ascii_general_ci
) ENGINE=InnoDB;

-- 创建复杂字符集类型表
DROP TABLE IF EXISTS case_07_complex_charsets;
CREATE TABLE case_07_complex_charsets (
  c1 char(10) CHARACTER SET ascii,     -- -> CHAR(10) CHARACTER SET ascii
  c2 varchar(10) CHARACTER SET ascii,   -- -> VARCHAR(10) CHARACTER SET ascii
  c3 char(10) CHARACTER SET utf8        -- -> CHAR(10) CHARACTER SET utf8
) ENGINE=InnoDB;

-- 创建JSON类型表
DROP TABLE IF EXISTS case_08_json;
CREATE TABLE case_08_json (
  data json,
  data_len json,
  data_upper json
) ENGINE=InnoDB;

-- 创建日期时间类型表
DROP TABLE IF EXISTS case_09_datetime;
CREATE TABLE case_09_datetime (
  d1 date,                        -- -> DATE
  t1 time,                        -- -> TIME
  t2 time(6),                     -- -> TIME(6)
  dt1 datetime,                   -- -> TIMESTAMP
  dt2 datetime(3),                -- -> TIMESTAMP(3)
  ts1 timestamp,                  -- -> TIMESTAMP
  ts2 timestamp(6),               -- -> TIMESTAMP(6)
  y1 year                         -- -> INTEGER
) ENGINE=InnoDB;

-- 创建默认值类型表
DROP TABLE IF EXISTS case_10_defaults;
CREATE TABLE case_10_defaults (
  c1 int default 0,
  c2 int default  1,
  c3 varchar(10) default 'abc',
  c4 timestamp(3) NULL DEFAULT CURRENT_TIMESTAMP(3),
  c5 timestamp(6) NULL DEFAULT CURRENT_TIMESTAMP(6),
  c6 timestamp(3) NULL DEFAULT CURRENT_TIMESTAMP(3) -- Hits reCurrentTimestamp -> current_timestamp(3)
) ENGINE=InnoDB;

-- 创建自增类型表
DROP TABLE IF EXISTS case_11_autoincrement;
CREATE TABLE case_11_autoincrement (
  id int AUTO_INCREMENT PRIMARY KEY,           -- 保留一个自增主键
  big_id bigint UNIQUE,                        -- 去掉 AUTO_INCREMENT，仅保留唯一约束
  mixed_case INT                               -- 去掉 AUTO_INCREMENT，普通整数
) ENGINE=InnoDB;

-- 创建无符号类型表
DROP TABLE IF EXISTS case_12_unsigned;
CREATE TABLE case_12_unsigned (
  c1 int unsigned,                -- -> INTEGER
  c2 bigint unsigned,             -- -> BIGINT
  c3 int zerofill,                -- -> INTEGER
  c4 int unsigned zerofill        -- -> INTEGER
) ENGINE=InnoDB;

-- 创建枚举和集合类型表
DROP TABLE IF EXISTS case_13_enum_set;
CREATE TABLE case_13_enum_set (
  e1 enum('a', 'b', 'c'),         -- -> VARCHAR(255)
  s1 set('x', 'y', 'z')           -- -> VARCHAR(255)
) ENGINE=InnoDB;

-- 创建二进制类型表
DROP TABLE IF EXISTS case_14_binary;
CREATE TABLE case_14_binary (
  b1 binary(10),                  -- -> BYTEA
  b2 varbinary(20),               -- -> BYTEA
  b3 blob,                        -- -> BYTEA
  b4 longblob,                    -- -> BYTEA
  b5 mediumblob,                  -- -> BYTEA
  b6 tinyblob                     -- -> BYTEA
) ENGINE=InnoDB;

-- 创建表选项类型表
DROP TABLE IF EXISTS case_15_options;
CREATE TABLE case_15_options (
  id int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci ROW_FORMAT=DYNAMIC;

-- 创建分区类型表
DROP TABLE IF EXISTS case_16_partition;
CREATE TABLE case_16_partition (
  id int,
  created_at datetime
) PARTITION BY RANGE (YEAR(created_at)) (
    PARTITION p0 VALUES LESS THAN (2020),
    PARTITION p1 VALUES LESS THAN (2021)
);

-- 创建临时表类型表
DROP TEMPORARY TABLE IF EXISTS case_17_temp;
CREATE TEMPORARY TABLE case_17_temp (
  id int
);

-- 创建引号类型表
DROP TABLE IF EXISTS `case_18_quotes`;
CREATE TABLE `case_18_quotes` (
  `id` int,
  `name` varchar(20),
  `desc` text
);

-- 创建注释类型表
DROP TABLE IF EXISTS case_19_comments;
CREATE TABLE case_19_comments (
  c1 int COMMENT 'Simple comment',
  c2 int COMMENT "Double quote comment",
  c3 int COMMENT 'Comment with '' quote',
  c4 int COMMENT "Comment with "" quote"
) COMMENT='Table comment';

-- 创建约束类型表
DROP TABLE IF EXISTS case_20_constraints;
CREATE TABLE case_20_constraints (
  id int,
  name varchar(20),
  PRIMARY KEY (id),
  KEY idx_name (name),
  UNIQUE KEY uk_name (name),
  INDEX idx_all (id, name)
  );

-- 创建虚拟列类型表
DROP TABLE IF EXISTS case_21_virtual;
CREATE TABLE case_21_virtual (
  id int,
  c1 int,
  c2 int GENERATED ALWAYS AS (c1 + 1) VIRTUAL
);

-- 创建空间类型表
DROP TABLE IF EXISTS case_22_spatial;
CREATE TABLE case_22_spatial (
  g geometry,                     -- -> GEOMETRY
  p point,                        -- -> POINT
  ls linestring,                  -- -> LINESTRING
  poly polygon,                   -- -> POLYGON
  mp multipoint,                  -- -> MULTIPOINT
  mls multilinestring,            -- -> MULTILINESTRING
  mpoly multipolygon,             -- -> MULTIPOLYGON
  gc geometrycollection           -- -> GEOMETRYCOLLECTION
);

-- 创建怪异语法类型表
DROP TABLE IF EXISTS case_23_weird_syntax;
CREATE TABLE case_23_weird_syntax (
  c1 INTEGER(10),
  c2 DOUBLE PRECISION(10,2),
  c3 Varchar( 20 ),
  c4 int( 10 ) unsigned,
  c5 tinyint( 1 )
);

-- 创建边缘情况类型表
DROP TABLE IF EXISTS case_24_edge_cases;
create table case_24_edge_cases (
  c1 text character set utf8mb4,
  c2 varchar(255),
  c3 int,
  c4 bigint unsigned not null auto_increment primary key,
  c5 double precision,
  c6 longblob
);

-- 创建MySQL 8.0保留字类型表
DROP TABLE IF EXISTS case_25_mysql8_reserved;
CREATE TABLE case_25_mysql8_reserved (
  id int PRIMARY KEY,
  `rank` int,                      -- RANK is reserved
  `system` varchar(10),            -- SYSTEM is reserved
  `groups` text,                   -- GROUPS is reserved
  `window` varchar(20),            -- WINDOW is reserved
  `function` int,                  -- FUNCTION is reserved
  `role` varchar(10),              -- ROLE is reserved
  `admin` boolean                  -- ADMIN is reserved
);

-- 创建MySQL 8.0不可见列类型表
DROP TABLE IF EXISTS case_26_mysql8_invisible;
CREATE TABLE case_26_mysql8_invisible (
  id int,
  c1 int INVISIBLE,               -- 8.0.23+ Invisible Column
  c2 int VISIBLE,
  KEY idx_c1 (c1) INVISIBLE,      -- Invisible Index
  KEY idx_c2 (c2) VISIBLE
);

-- 创建MySQL 8.0检查约束类型表
DROP TABLE IF EXISTS case_27_mysql8_check;
CREATE TABLE case_27_mysql8_check (
  id int,
  age int,
  CONSTRAINT chk_age CHECK (age > 18) ENFORCED,
  CHECK (age < 150) NOT ENFORCED
);

-- 创建MySQL 8.0函数索引类型表
DROP TABLE IF EXISTS case_28_mysql8_func_index;
CREATE TABLE case_28_mysql8_func_index (
  data json,
  name varchar(50),
  KEY idx_name_upper ((UPPER(name))),
  KEY idx_data_val ((CAST(data->>'$.id' AS UNSIGNED ARRAY)))
);

-- 创建MySQL 8.0默认值类型表
DROP TABLE IF EXISTS case_29_mysql8_defaults;
CREATE TABLE case_29_mysql8_defaults (
  id char(36) DEFAULT (UUID()),
  val int DEFAULT (1 + 1),
  j json DEFAULT (JSON_OBJECT('key', 'val'))
);

-- 创建MySQL 8.0字符集和排序规则类型表
DROP TABLE IF EXISTS case_30_mysql8_collations;
CREATE TABLE case_30_mysql8_collations (
  c1 varchar(10) COLLATE utf8mb4_0900_ai_ci,
  c2 varchar(10) COLLATE utf8mb4_zh_0900_as_cs,
  c3 varchar(10) COLLATE utf8mb4_bin
) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- 创建MySQL 8.0系统表类型表
DROP TABLE IF EXISTS case_31_sys_utf8mb3;
CREATE TABLE case_31_sys_utf8mb3 (
  Host char(255) CHARACTER SET ascii COLLATE ascii_general_ci NOT NULL DEFAULT '',
  Db char(64) COLLATE utf8mb3_bin NOT NULL DEFAULT '',
  User char(32) COLLATE utf8mb3_bin NOT NULL DEFAULT ''
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_bin STATS_PERSISTENT=0 COMMENT='System table imitation';

-- 创建MySQL 8.0复杂生成列类型表
DROP TABLE IF EXISTS case_32_complex_generated;
CREATE TABLE case_32_complex_generated (
  cost_name varchar(64) NOT NULL,
  default_value float GENERATED ALWAYS AS ((case cost_name when _utf8mb3'io_block_read_cost' then 1.0 else NULL end)) VIRTUAL
);

-- 创建MySQL 8.0降序索引类型表
DROP TABLE IF EXISTS case_33_desc_index;
CREATE TABLE case_33_desc_index (
  Host char(255),
  User char(32),
  Password_timestamp timestamp(6),
  PRIMARY KEY (Host, User, Password_timestamp DESC),
  KEY idx_ts (Password_timestamp DESC)
);

-- 创建MySQL 8.0表选项类型表
DROP TABLE IF EXISTS case_34_table_options;
CREATE TABLE case_34_table_options (
  id int
)  ENGINE=InnoDB;

-- 创建MySQL 8.0枚举和集合类型表
DROP TABLE IF EXISTS case_35_enum_charset;
CREATE TABLE case_35_enum_charset (
  col_enum enum('N','Y') CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci NOT NULL DEFAULT 'N',
  col_set set('A','B') CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci NOT NULL DEFAULT ''
);

-- 创建MySQL 8.0大写表名类型表
DROP TABLE IF EXISTS `CASE_36_UPPERCASE`;
CREATE TABLE `CASE_36_UPPERCASE` (
  `ID` INT,
  `NAME` VARCHAR(50),
  `AGE` INT,
  `EMAIL` VARCHAR(100),
  `CREATE_DATE` DATETIME
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 创建MySQL 8.0驼峰表名类型表
DROP TABLE IF EXISTS `CASE_37_HUMP`;
CREATE TABLE `CASE_37_HUMP` (
  `ProductId` int,
  `ProductName` varchar(100),
  `Price` Decimal(10,2),
  `Stock` Int,
  `Category` varchar(50),
  `LastUpdate` datetime
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 创建MySQL 8.0蛇形表名类型表
DROP TABLE IF EXISTS `CASE_38_SNAKE`;
CREATE TABLE `CASE_38_SNAKE` (
  `product_id` int,
  `product_name` varchar(100),
  `price` Decimal(10,2),
  `stock` int,
  `category` varchar(50),
  `last_update` datetime
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 创建MySQL 8.0下划线表名类型表
DROP TABLE IF EXISTS `CASE_39_UNDERSCORE`;
CREATE TABLE `CASE_39_UNDERSCORE` (
  `product_id` int,
  `product_name` varchar(100),
  `price` Decimal(10,2),
  `stock` int,
  `category` varchar(50),
  `last_update` datetime
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 创建MySQL 8.0默认值类型表
DROP TABLE IF EXISTS `CASE_40_DEFAULT`;
CREATE TABLE `CASE_40_DEFAULT` (
  `id` int,
  `name` varchar(50) DEFAULT 'unknown',
  `age` int DEFAULT 0,
  `email` varchar(100) DEFAULT 'unknown@example.com'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 创建外键约束表
DROP TABLE IF EXISTS case_41_foreign_key;
DROP TABLE IF EXISTS case_41_parent;
CREATE TABLE case_41_parent (
  id int PRIMARY KEY,
  name varchar(50)
) ENGINE=InnoDB;

CREATE TABLE case_41_foreign_key (
  id int PRIMARY KEY,
  parent_id int,
  name varchar(50),
  FOREIGN KEY (parent_id) REFERENCES case_41_parent(id)
    ON DELETE CASCADE
    ON UPDATE SET NULL
) ENGINE=InnoDB;

-- 创建全文索引表
DROP TABLE IF EXISTS case_42_fulltext;
CREATE TABLE case_42_fulltext (
  id int PRIMARY KEY,
  title varchar(100),
  content text,
  FULLTEXT KEY ft_title_content (title, content)
) ENGINE=InnoDB;

-- 创建空间索引表
DROP TABLE IF EXISTS case_43_spatial_index;
CREATE TABLE case_43_spatial_index (
  id int PRIMARY KEY,
  location point
) ENGINE=InnoDB;

-- 创建复合主键表
DROP TABLE IF EXISTS case_44_composite_pk;
CREATE TABLE case_44_composite_pk (
  id1 int,
  id2 int,
  name varchar(50),
  PRIMARY KEY (id1, id2)
) ENGINE=InnoDB;

-- 创建存储生成列表
DROP TABLE IF EXISTS case_45_stored_generated;
CREATE TABLE case_45_stored_generated (
  id int,
  c1 int,
  c2 int GENERATED ALWAYS AS (c1 * 2) STORED,
  c3 int GENERATED ALWAYS AS (c1 + c2) VIRTUAL
) ENGINE=InnoDB;

-- 创建MyISAM存储引擎表
DROP TABLE IF EXISTS case_46_myisam;
CREATE TABLE case_46_myisam (
  id int PRIMARY KEY,
  name varchar(50)
) ENGINE=MyISAM;

-- 创建MEMORY存储引擎表
DROP TABLE IF EXISTS case_47_memory;
CREATE TABLE case_47_memory (
  id int PRIMARY KEY,
  name varchar(50)
) ENGINE=MEMORY;

-- 创建不同索引类型表
DROP TABLE IF EXISTS case_48_index_types;
CREATE TABLE case_48_index_types (
  id int PRIMARY KEY,
  name varchar(50),
  value int,
  KEY idx_name_btree (name) USING BTREE,
  KEY idx_value_hash (value) USING HASH
) ENGINE=InnoDB;

-- 创建LIST分区表
DROP TABLE IF EXISTS case_49_list_partition;
CREATE TABLE case_49_list_partition (
  id int,
  category int
) PARTITION BY LIST (category) (
  PARTITION p0 VALUES IN (1, 2, 3),
  PARTITION p1 VALUES IN (4, 5, 6)
);

-- 创建HASH分区表
DROP TABLE IF EXISTS case_50_hash_partition;
CREATE TABLE case_50_hash_partition (
  id int,
  name varchar(50)
) PARTITION BY HASH (id) PARTITIONS 4;

-- 创建表复制测试
DROP TABLE IF EXISTS case_51_copy_like;
CREATE TABLE case_51_copy_like LIKE case_01_integers;

-- 创建表数据复制测试
DROP TABLE IF EXISTS case_52_copy_as;
CREATE TABLE case_52_copy_as AS
SELECT * FROM case_01_integers WHERE 1=0;

-- 创建延迟约束表
DROP TABLE IF EXISTS case_53_deferred_constraint;
CREATE TABLE case_53_deferred_constraint (
  id int PRIMARY KEY,
  name varchar(50) UNIQUE
) ENGINE=InnoDB;

-- 创建表空间表
DROP TABLE IF EXISTS case_54_tablespace;
CREATE TABLE case_54_tablespace (
  id int PRIMARY KEY,
  name varchar(50)
) ENGINE=InnoDB
  TABLESPACE=`innodb_file_per_table`;

-- 创建压缩表
DROP TABLE IF EXISTS case_55_compressed;
CREATE TABLE case_55_compressed (
  id int PRIMARY KEY,
  data text
) ENGINE=InnoDB
  ROW_FORMAT=COMPRESSED
  KEY_BLOCK_SIZE=8;

-- 创建加密表
DROP TABLE IF EXISTS case_56_encrypted;
CREATE TABLE case_56_encrypted (
  id int PRIMARY KEY,
  sensitive_data varchar(100)
) ENGINE=InnoDB;

-- 创建列级权限表
DROP TABLE IF EXISTS case_57_column_privileges;
CREATE TABLE case_57_column_privileges (
  id int PRIMARY KEY,
  public_data varchar(50),
  sensitive_data varchar(50)
) ENGINE=InnoDB;

-- 创建子分区表
DROP TABLE IF EXISTS case_58_subpartition;
CREATE TABLE case_58_subpartition (
  id int,
  year int,
  month int
) PARTITION BY RANGE (year)
  SUBPARTITION BY HASH (month)
  SUBPARTITIONS 12 (
    PARTITION p2020 VALUES LESS THAN (2021),
    PARTITION p2021 VALUES LESS THAN (2022)
  );

-- 创建复杂生成列表（包含多函数表达式）
DROP TABLE IF EXISTS case_59_complex_generated;
CREATE TABLE case_59_complex_generated (
  id int,
  price decimal(10,2),
  quantity int,
  discount decimal(5,2),
  subtotal decimal(12,2) GENERATED ALWAYS AS ((price * quantity)) STORED,
  total decimal(12,2) GENERATED ALWAYS AS ((price * quantity) * (1 - discount / 100)) STORED,
  formatted_total varchar(50)
);

-- 创建带多列统计信息的表
DROP TABLE IF EXISTS case_60_statistics;
CREATE TABLE case_60_statistics (
  id int PRIMARY KEY,
  category varchar(50),
  subcategory varchar(50),
  value decimal(10,2)
) ENGINE=InnoDB
  STATS_PERSISTENT=1
  STATS_AUTO_RECALC=1
  STATS_SAMPLE_PAGES=10;

-- 创建带大量列的表（包含 MySQL 所有支持类型及其最小和最大长度）
DROP TABLE IF EXISTS case_61_many_columns;
CREATE TABLE case_61_many_columns (
  id int PRIMARY KEY,
  -- 整数类型
  tinyint_min tinyint,
  tinyint_max tinyint,
  smallint_min smallint,
  smallint_max smallint,
  mediumint_min mediumint,
  mediumint_max mediumint,
  int_min int,
  int_max int,
  bigint_min bigint,
  bigint_max bigint,
  
  -- 浮点数类型 (注意: float/double的(M,D)语法也受限制, 通常直接写float)
  float_min float,
  float_max float,
  double_min double,
  double_max double,
  decimal_min decimal(1,0),
  decimal_max decimal(65,30),
  
  -- 字符串类型
  char_min char(1),
  char_max char(255),
  varchar_min varchar(1),
  varchar_max varchar(255),
  text_min text,
  text_max text,
  tinytext_min tinytext,
  tinytext_max tinytext,
  mediumtext_min mediumtext,
  mediumtext_max mediumtext,
  longtext_min longtext,
  longtext_max longtext,
  
  -- 二进制类型
  binary_min binary(1),
  binary_max binary(255),
  varbinary_min varbinary(1),
  varbinary_max varbinary(255),
  blob_min blob,
  blob_max blob,
  tinyblob_min tinyblob,
  tinyblob_max tinyblob,
  mediumblob_min mediumblob,
  mediumblob_max mediumblob,
  longblob_min longblob,
  longblob_max longblob,
  
  -- 日期时间类型
  date_col date,
  time_col time,
  datetime_col datetime,
  timestamp_col timestamp,
  year_col year,
  
  -- 其他类型
  boolean_col boolean,
  enum_min enum('a'),
  enum_max enum('a', 'b', 'c', 'd', 'e'),
  set_min set('x'),
  set_max set('x', 'y', 'z'),
  json_col json
  
) ENGINE=InnoDB 
  DEFAULT CHARSET=utf8mb4 -- 支持存储 emoji 和中文
  COLLATE=utf8mb4_unicode_ci;

-- 创建带不同默认值类型的表
DROP TABLE IF EXISTS case_62_various_defaults;
CREATE TABLE case_62_various_defaults (
  id int PRIMARY KEY AUTO_INCREMENT,
  name varchar(50) DEFAULT 'Unknown',
  age int DEFAULT 18,
  active boolean DEFAULT true,
  created_at timestamp DEFAULT CURRENT_TIMESTAMP,
  updated_at timestamp DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  price decimal(10,2) DEFAULT 0.00,
  quantity int DEFAULT 1,
  status varchar(20) DEFAULT 'pending',
  data json DEFAULT (JSON_OBJECT('key', 'value')),
  uuid char(36) DEFAULT (UUID())
) ENGINE=InnoDB;

-- 创建带字符集和排序规则的复杂表
DROP TABLE IF EXISTS case_63_charset_collation;
CREATE TABLE case_63_charset_collation (
  id int PRIMARY KEY,
  name_en varchar(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci,
  name_zh varchar(50) CHARACTER SET utf8mb4,
  name_de varchar(50) CHARACTER SET utf8mb4,
  code varchar(10) CHARACTER SET ascii COLLATE ascii_bin
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

