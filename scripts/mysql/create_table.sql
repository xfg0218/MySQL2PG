-- MySQL2PG 测试表定义文件
-- 包含各种MySQL语法场景，用于测试MySQL到PostgreSQL的转换功能
-- 
-- 表覆盖范围说明（case_01 ~ case_193）：
-- 1) case_01 ~ case_40：基础类型与DDL语法（数值、字符集/排序规则、JSON、时间、默认值、自增、约束、生成列、保留字、命名风格等）
-- 2) case_41 ~ case_80：索引/约束与表特性（外键、全文、空间、复合主键、存储引擎、分区、复制建表、压缩、统计信息等）
-- 3) case_81 ~ case_120：边界语法与 MySQL 5.7/8.0 常见特性（SRID、长标识符、高精度数值、多值索引、窗口函数、JSON_TABLE、锁相关语法等）
-- 4) case_121 ~ case_155：业务化建模样例
-- 5) case_156 ~ case_167：新增综合增强场景（复合外键、JSON生成列、时间类型组合、文本二进制混合、数值边界、建表方式专项）
-- 6) case_168：PRIMARY KEY ... USING BTREE 主键保留回归案例
-- 7) case_169 ~ case_193：类型全长度转换能力测试（CHAR/VARCHAR/BINARY/VARBINARY/整数宽度/DECIMAL/FLOAT/DOUBLE/BIT/时间fsp，每种类型一张表）
-- 总计：193 个测试表案例

-- 创建整数类型表
DROP TABLE IF EXISTS case_01_integers;
CREATE TABLE case_01_integers (
  col_tiny tinyint primary key,               -- -> SMALLINT
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
  id int auto_increment primary key,
  is_active tinyint(1),             -- -> BOOLEAN
  status tinyint(10),                -- -> SMALLINT (not 1, so not boolean)
  is_deleted TINYINT(1)             -- -> BOOLEAN (case insensitive)
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
CREATE TABLE case_04_mb3_suffix  (
  col_var_mb3 varchar(255) CHARACTER SET utf8mb4,    -- -> VARCHAR(255)
  col_char_mb3 char(20) CHARACTER SET utf8mb4,       -- -> CHAR(10)
  col_text_mb3 text CHARACTER SET utf8mb4,           -- -> TEXT
  col_mixed_mb3 varchar(100) CHARACTER SET utf8mb4  -- -> VARCHAR(100)
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
  ts1 timestamp DEFAULT CURRENT_TIMESTAMP,                  -- -> TIMESTAMP
  ts2 timestamp(6) DEFAULT CURRENT_TIMESTAMP(6),               -- -> TIMESTAMP(6)
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
-- 无符号整数提升为能容纳完整无符号范围的 PG 类型：
-- int unsigned(0~4294967295) -> BIGINT，bigint unsigned(0~18446744073709551615) -> NUMERIC(20,0)
-- ZEROFILL 隐含 UNSIGNED 语义，同样按无符号处理
DROP TABLE IF EXISTS case_12_unsigned;
CREATE TABLE case_12_unsigned (
  c1 int unsigned,                -- -> BIGINT
  c2 bigint unsigned,             -- -> NUMERIC(20,0)
  c3 int zerofill,                -- -> BIGINT (zerofill 隐含 unsigned)
  c4 int unsigned zerofill        -- -> BIGINT
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
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci ROW_FORMAT=DYNAMIC;

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
/***
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
***/

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
  c1 int,
  c2 int,
  KEY idx_c1 (c1),      -- Invisible Index
  KEY idx_c2 (c2)
);

-- 创建MySQL 8.0检查约束类型表
DROP TABLE IF EXISTS case_27_mysql8_check;
CREATE TABLE case_27_mysql8_check (
  id int,
  age int,
  CONSTRAINT chk_age CHECK (age > 18),
  CHECK (age < 150)
);

-- 创建MySQL 8.0函数索引类型表
DROP TABLE IF EXISTS case_28_mysql8_func_index;
CREATE TABLE case_28_mysql8_func_index (
  data json,
  name varchar(50),
  KEY idx_name (name)
);

-- 创建MySQL 8.0默认值类型表
DROP TABLE IF EXISTS case_29_mysql8_defaults;
CREATE TABLE case_29_mysql8_defaults (
  id char(36) DEFAULT NULL,
  val int DEFAULT 2,
  j json DEFAULT NULL
);

-- 创建MySQL 8.0字符集和排序规则类型表
DROP TABLE IF EXISTS case_30_mysql8_collations;
CREATE TABLE case_30_mysql8_collations (
  c1 varchar(10) COLLATE utf8mb4_general_ci,
  c2 varchar(10) COLLATE utf8mb4_general_ci,
  c3 varchar(10) COLLATE utf8mb4_bin
) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

-- 创建MySQL 8.0系统表类型表
DROP TABLE IF EXISTS case_31_sys_utf8;
CREATE TABLE case_31_sys_utf8 (
  Host char(255) CHARACTER SET ascii COLLATE ascii_general_ci NOT NULL DEFAULT '',
  Db char(64) COLLATE utf8_bin NOT NULL DEFAULT '',
  User char(32) COLLATE utf8_bin NOT NULL DEFAULT ''
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin STATS_PERSISTENT=0 COMMENT='System table imitation';

-- 创建MySQL 8.0复杂生成列类型表
DROP TABLE IF EXISTS case_32_complex_generated;
CREATE TABLE case_32_complex_generated (
  cost_name varchar(64) NOT NULL,
  default_value float GENERATED ALWAYS AS ((case cost_name when _utf8'io_block_read_cost' then 1.0 else NULL end)) VIRTUAL
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
  col_enum enum('N','Y') CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT 'N',
  col_set set('A','B') CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT ''
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
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;;

CREATE TABLE case_41_foreign_key (
  id int PRIMARY KEY,
  parent_id int,
  name varchar(50),
  FOREIGN KEY (parent_id) REFERENCES case_41_parent(id)
    ON DELETE CASCADE
    ON UPDATE SET NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

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
  data json DEFAULT NULL,
  uuid char(36) DEFAULT NULL
) ENGINE=InnoDB;

-- 创建带字符集和排序规则的复杂表
DROP TABLE IF EXISTS case_63_charset_collation;
CREATE TABLE case_63_charset_collation (
  id int PRIMARY KEY,
  name_en varchar(50) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci,
  name_zh varchar(50) CHARACTER SET utf8mb4,
  name_de varchar(50) CHARACTER SET utf8mb4,
  code varchar(10) CHARACTER SET ascii COLLATE ascii_bin
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 创建BIT类型表
DROP TABLE IF EXISTS case_64_bit_types;
CREATE TABLE case_64_bit_types (
  id int PRIMARY KEY,
  b1 bit(1) COMMENT '1 bit',
  b8 bit(8) COMMENT '1 byte',
  b16 bit(16) COMMENT '2 bytes',
  b32 bit(32) COMMENT '4 bytes',
  b64 bit(64) COMMENT '8 bytes'
) ENGINE=InnoDB COMMENT='BIT data types test';

-- 创建YEAR类型变体表
DROP TABLE IF EXISTS case_65_year_types;
CREATE TABLE case_65_year_types (
  id int PRIMARY KEY,
  y4 year(4) COMMENT 'Standard year',
  y_default year DEFAULT '2000' COMMENT 'Year with default'
) ENGINE=InnoDB COMMENT='Year type variations';

-- 创建更多空间类型表
/**
DROP TABLE IF EXISTS case_66_geometry_subtypes;
CREATE TABLE case_66_geometry_subtypes (
  id int PRIMARY KEY,
  geo geometry NOT NULL COMMENT 'Geometry not null',
  pt point DEFAULT NULL COMMENT 'Point nullable'
) ENGINE=InnoDB COMMENT='Geometry subtypes';
**/

-- 创建触发器模拟表
DROP TABLE IF EXISTS case_67_trigger_simulation;
CREATE TABLE case_67_trigger_simulation (
  id int AUTO_INCREMENT PRIMARY KEY,
  created_at datetime DEFAULT CURRENT_TIMESTAMP,
  updated_at datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Auto update time'
) ENGINE=InnoDB COMMENT='Trigger behavior simulation';

-- 创建视图模拟表
DROP TABLE IF EXISTS case_68_view_simulation;
CREATE TABLE case_68_view_simulation (
  view_id int,
  calc_result decimal(10,4) COMMENT 'Calculated field',
  summary text COMMENT 'Aggregated text'
) ENGINE=InnoDB COMMENT='Structure mimicking a view';

-- 创建深层嵌套JSON表
DROP TABLE IF EXISTS case_69_deeply_nested_json;
CREATE TABLE case_69_deeply_nested_json (
  id int PRIMARY KEY,
  config json COMMENT 'Configuration object',
  tags json COMMENT 'Array of tags',
  metadata json COMMENT 'Deeply nested metadata'
) ENGINE=InnoDB COMMENT='Deep JSON structures';

-- 创建MySQL 8.0特定排序规则表
DROP TABLE IF EXISTS case_70_utf8mb4_900;
CREATE TABLE case_70_utf8mb4_900 (
  id int PRIMARY KEY,
  str1 varchar(100) COLLATE utf8mb4_general_ci COMMENT 'Accent insensitive',
  str2 varchar(100) COLLATE utf8mb4_bin COMMENT 'Accent sensitive Case sensitive'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='MySQL 8.0 specific collations';

-- 创建复杂函数索引表
DROP TABLE IF EXISTS case_71_functional_index_complex;
CREATE TABLE case_71_functional_index_complex (
  id int PRIMARY KEY,
  first_name varchar(50),
  last_name varchar(50)
--   KEY idx_full_name ((concat(first_name, ' ', last_name))) COMMENT 'Functional index on concatenation'
) ENGINE=InnoDB COMMENT='Complex functional indexes';

-- 创建正则检查约束表
DROP TABLE IF EXISTS case_72_check_constraint_regex;
CREATE TABLE case_72_check_constraint_regex (
  id int PRIMARY KEY,
  email varchar(100),
  CONSTRAINT chk_email_format CHECK (email LIKE '%@%')
) ENGINE=InnoDB COMMENT='Check constraints with patterns';

-- 创建混合生成列表
DROP TABLE IF EXISTS case_73_generated_stored_mixed;
CREATE TABLE case_73_generated_stored_mixed (
  side_a double,
  side_b double,
  area double GENERATED ALWAYS AS (side_a * side_b) STORED COMMENT 'Stored area',
  perimeter double GENERATED ALWAYS AS (2 * (side_a + side_b)) VIRTUAL COMMENT 'Virtual perimeter'
) ENGINE=InnoDB COMMENT='Mixed stored and virtual columns';

-- 创建混合可见性列的表
DROP TABLE IF EXISTS case_74_invisible_cols_mixed;
CREATE TABLE case_74_invisible_cols_mixed (
  id int PRIMARY KEY,
  secret_code varchar(20) COMMENT 'Hidden column',
  public_code varchar(20) COMMENT 'Visible column'
) ENGINE=InnoDB COMMENT='Mixed visibility columns';

-- 创建降序主键表
DROP TABLE IF EXISTS case_75_desc_primary_key;
CREATE TABLE case_75_desc_primary_key (
  category_id int,
  rank_score int,
  PRIMARY KEY (category_id ASC, rank_score DESC) COMMENT 'Mixed direction PK'
) ENGINE=InnoDB COMMENT='Descending primary key parts';

-- 创建BLOB前缀索引表
DROP TABLE IF EXISTS case_76_blob_keys;
CREATE TABLE case_76_blob_keys (
  id int PRIMARY KEY,
  data blob,
  KEY idx_blob_prefix (data(10)) COMMENT 'Index on first 10 bytes'
) ENGINE=InnoDB COMMENT='Indexes on BLOB prefix';

-- 创建TEXT前缀索引表
DROP TABLE IF EXISTS case_77_text_keys;
CREATE TABLE case_77_text_keys (
  id int PRIMARY KEY,
  content text,
  KEY idx_text_prefix (content(20)) COMMENT 'Index on first 20 chars'
) ENGINE=InnoDB COMMENT='Indexes on TEXT prefix';

-- 创建允许NULL的多列唯一索引表
DROP TABLE IF EXISTS case_78_multi_col_unique_null;
CREATE TABLE case_78_multi_col_unique_null (
  id int PRIMARY KEY,
  code varchar(10),
  category varchar(10),
  UNIQUE KEY uk_code_cat (code, category) COMMENT 'Unique allowing NULLs'
) ENGINE=InnoDB COMMENT='Unique constraints with NULLs';

-- 创建SERIAL默认值别名表
DROP TABLE IF EXISTS case_79_serial_default;
CREATE TABLE case_79_serial_default (
  id SERIAL COMMENT 'Alias for BIGINT UNSIGNED NOT NULL AUTO_INCREMENT UNIQUE',
  name varchar(50)
) ENGINE=InnoDB COMMENT='SERIAL alias usage';

-- 创建ON UPDATE时间戳表
DROP TABLE IF EXISTS case_80_on_update_current_timestamp;
CREATE TABLE case_80_on_update_current_timestamp (
  id int PRIMARY KEY,
  modified_at datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Datetime auto update'
) ENGINE=InnoDB COMMENT='Explicit ON UPDATE clauses';

-- 创建带SRID的空间类型表
/***
DROP TABLE IF EXISTS case_81_geometry_srid;
CREATE TABLE case_81_geometry_srid (
  id int PRIMARY KEY,
  geo geometry COMMENT 'Geometry with'
) ENGINE=InnoDB COMMENT='Geometry with SRID';
***/

-- 创建宽表（多列）
DROP TABLE IF EXISTS case_82_wide_table;
CREATE TABLE case_82_wide_table (
  id int PRIMARY KEY,
  c01 int, c02 int, c03 int, c04 int, c05 int,
  c06 int, c07 int, c08 int, c09 int, c10 int
) ENGINE=InnoDB COMMENT='Table with multiple similar columns';

-- 创建长标识符表
DROP TABLE IF EXISTS case_83_long_identifiers;
CREATE TABLE case_83_long_identifiers (
  this_is_a_very_long_column_name_that_reaches_limit_of_64_chars int COMMENT 'Max length column name',
  id int PRIMARY KEY
) ENGINE=InnoDB COMMENT='Very long identifiers';

-- 创建引用保留字表
DROP TABLE IF EXISTS case_84_reserved_words_quoted;
CREATE TABLE case_84_reserved_words_quoted (
  `select` int COMMENT 'Reserved SELECT',
  `update` int COMMENT 'Reserved UPDATE',
  `delete` int COMMENT 'Reserved DELETE',
  `insert` int COMMENT 'Reserved INSERT'
) ENGINE=InnoDB COMMENT='Quoted reserved words';

-- 创建高精度数值表
DROP TABLE IF EXISTS case_85_numeric_precision_scale;
CREATE TABLE case_85_numeric_precision_scale (
  id int PRIMARY KEY,
  high_prec decimal(65, 30) COMMENT 'Max precision decimal',
  low_scale decimal(10, 0) COMMENT 'No scale decimal'
) ENGINE=InnoDB COMMENT='High precision and scale';

-- 创建Zerofill变体表
DROP TABLE IF EXISTS case_86_zerofill_variants;
CREATE TABLE case_86_zerofill_variants (
  id int PRIMARY KEY,
  z_tiny tinyint(3) zerofill COMMENT 'Tinyint zerofill',
  z_big bigint(20) zerofill COMMENT 'Bigint zerofill'
) ENGINE=InnoDB COMMENT='Zerofill integer variants';

-- 创建无符号浮点数表
DROP TABLE IF EXISTS case_87_float_double_unsigned;
CREATE TABLE case_87_float_double_unsigned (
  id int PRIMARY KEY,
  f_uns float unsigned COMMENT 'Unsigned float',
  d_uns double unsigned COMMENT 'Unsigned double'
) ENGINE=InnoDB COMMENT='Unsigned floating points';

-- 创建年份转换表
DROP TABLE IF EXISTS case_88_year_conversion;
CREATE TABLE case_88_year_conversion (
  id int PRIMARY KEY,
  birth_year year COMMENT 'Birth year'
) ENGINE=InnoDB COMMENT='Year type usage';

-- 创建国家字符集表
DROP TABLE IF EXISTS case_89_national_char;
CREATE TABLE case_89_national_char (
  id int PRIMARY KEY,
  nat_char NATIONAL CHAR(10) COMMENT 'National Char',
  nat_varchar NATIONAL VARCHAR(50) COMMENT 'National Varchar'
) ENGINE=InnoDB COMMENT='National character types';

-- 创建空间参考系统表
DROP TABLE IF EXISTS case_90_spatial_reference;
CREATE TABLE case_90_spatial_reference (
  id int PRIMARY KEY,
  loc point COMMENT 'Point location'
) ENGINE=InnoDB COMMENT='Implicit spatial reference';

-- 创建JSON多值索引表
DROP TABLE IF EXISTS case_91_json_array_index;
CREATE TABLE case_91_json_array_index (
  id int PRIMARY KEY,
  tags json
--   KEY idx_tags ((CAST(tags AS CHAR(20) ARRAY))) COMMENT 'Multi-valued index on JSON array'
) ENGINE=InnoDB COMMENT='Multi-valued indexes on JSON';

-- 创建Fulltext Ngram解析器表
DROP TABLE IF EXISTS case_92_fulltext_ngram;
CREATE TABLE case_92_fulltext_ngram (
  id int PRIMARY KEY,
  content text,
  FULLTEXT KEY ft_ngram (content) WITH PARSER ngram COMMENT 'Ngram parser'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='Fulltext with ngram parser';

-- 创建Fulltext通用解析器表
DROP TABLE IF EXISTS case_93_fulltext_parser;
CREATE TABLE case_93_fulltext_parser (
  id int PRIMARY KEY,
  description text,
  FULLTEXT KEY ft_desc (description) COMMENT 'Standard fulltext'
) ENGINE=InnoDB COMMENT='Standard fulltext parser';

-- 创建不同行格式表
DROP TABLE IF EXISTS case_94_innodb_row_formats;
CREATE TABLE case_94_innodb_row_formats (
  id int PRIMARY KEY,
  data varchar(100)
) ENGINE=InnoDB ROW_FORMAT=COMPACT COMMENT='Compact row format';

-- 创建联合查询模拟表
DROP TABLE IF EXISTS case_95_union_view_table;
CREATE TABLE case_95_union_view_table (
  id int,
  source_type varchar(10) COMMENT 'Source indicator',
  common_field varchar(50) COMMENT 'Shared field'
) ENGINE=InnoDB COMMENT='Union result structure';

-- 创建List Columns分区表
DROP TABLE IF EXISTS case_96_partition_list_columns;
CREATE TABLE case_96_partition_list_columns (
  id int,
  region_code varchar(10),
  store_id int
) PARTITION BY LIST COLUMNS(region_code) (
  PARTITION p_east VALUES IN ('East', 'NorthEast'),
  PARTITION p_west VALUES IN ('West', 'SouthWest')
);

-- 创建Range Columns分区表
DROP TABLE IF EXISTS case_97_partition_range_columns;
CREATE TABLE case_97_partition_range_columns (
  id int,
  event_date date
) PARTITION BY RANGE COLUMNS(event_date) (
  PARTITION p_past VALUES LESS THAN ('2020-01-01'),
  PARTITION p_future VALUES LESS THAN (MAXVALUE)
);

-- 创建Key分区表
DROP TABLE IF EXISTS case_98_partition_key;
CREATE TABLE case_98_partition_key (
  uuid varchar(36) PRIMARY KEY,
  data json
) PARTITION BY KEY(uuid) PARTITIONS 4;

-- 创建Linear Hash分区表
DROP TABLE IF EXISTS case_99_partition_linear_hash;
CREATE TABLE case_99_partition_linear_hash (
  id int PRIMARY KEY,
  val int
) PARTITION BY LINEAR HASH(id) PARTITIONS 4;

-- 创建综合复杂性表
DROP TABLE IF EXISTS case_100_max_complexity;
CREATE TABLE case_100_max_complexity (
  id bigint UNSIGNED AUTO_INCREMENT PRIMARY KEY COMMENT 'Primary Key',
  user_code char(10) CHARACTER SET ascii COLLATE ascii_bin COMMENT 'ASCII Code',
  display_name varchar(100)  COMMENT 'Generated Name',
  meta_info json COMMENT 'Metadata JSON',
  created_at timestamp(6) DEFAULT CURRENT_TIMESTAMP(6) COMMENT 'Microsecond timestamp',
  is_deleted tinyint(1) DEFAULT 0 COMMENT 'Boolean flag',
  KEY idx_composite (user_code, created_at) COMMENT 'Composite Index'
--   FULLTEXT KEY ft_name (display_name) COMMENT 'Fulltext Index'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='Max complexity test table';

-- ============================================================
-- 以下为补充的 MySQL 5.7+ 语法案例 (case_101 ~ case_120)
-- ============================================================

-- 创建 ARCHIVE 存储引擎表 (用于归档数据)
DROP TABLE IF EXISTS case_101_archive_engine;
CREATE TABLE case_101_archive_engine (
  id INT AUTO_INCREMENT PRIMARY KEY,
  log_data TEXT,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
) DEFAULT CHARSET=utf8mb4 COMMENT='Archive storage engine for log data';

-- 创建 CSV 存储引擎表
DROP TABLE IF EXISTS case_102_csv_engine;
CREATE TABLE case_102_csv_engine (
  id INT,
  name VARCHAR(50),
  value DECIMAL(10,2)
) COMMENT='CSV storage engine';

-- 创建 BLACKHOLE 存储引擎表 (接收数据但不存储)
DROP TABLE IF EXISTS case_103_blackhole_engine;
CREATE TABLE case_103_blackhole_engine (
  id INT,
  data VARCHAR(100)
) ENGINE=BLACKHOLE COMMENT='Blackhole storage engine';

-- 创建 MyISAM 延迟键写入表
DROP TABLE IF EXISTS case_104_delay_key_write;
CREATE TABLE case_104_delay_key_write (
  id INT PRIMARY KEY,
  name VARCHAR(50),
  INDEX idx_name (name)
) ENGINE=MyISAM DELAY_KEY_WRITE=1 COMMENT='MyISAM with delayed key write';

-- 创建 UPSERT 测试表 (INSERT ... ON DUPLICATE KEY UPDATE)
DROP TABLE IF EXISTS case_105_upsert_test;
CREATE TABLE case_105_upsert_test (
  id INT PRIMARY KEY,
  name VARCHAR(50),
  counter INT DEFAULT 0,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB COMMENT='Table for UPSERT testing';

-- 示例：INSERT ... ON DUPLICATE KEY UPDATE
-- INSERT INTO case_105_upsert_test (id, name, counter) VALUES (1, 'test', 1)
-- ON DUPLICATE KEY UPDATE counter = counter + 1;

-- 创建 REPLACE INTO 测试表
DROP TABLE IF EXISTS case_106_replace_test;
CREATE TABLE case_106_replace_test (
  id INT PRIMARY KEY,
  name VARCHAR(50),
  value INT
) ENGINE=InnoDB COMMENT='Table for REPLACE INTO testing';

-- 示例：REPLACE INTO
-- REPLACE INTO case_106_replace_test (id, name, value) VALUES (1, 'updated', 100);

-- 创建多表 DELETE 测试表
DROP TABLE IF EXISTS case_107_multi_delete_child;
DROP TABLE IF EXISTS case_107_multi_delete_parent;

CREATE TABLE case_107_multi_delete_parent (
  id INT PRIMARY KEY,
  name VARCHAR(50)
) ENGINE=InnoDB CHARSET=utf8mb4  COMMENT='Parent table for multi-table delete';

CREATE TABLE case_107_multi_delete_child (
  id INT PRIMARY KEY,
  parent_id INT,
  value INT,
  FOREIGN KEY (parent_id) REFERENCES case_107_multi_delete_parent(id) ON DELETE CASCADE
) ENGINE=InnoDB CHARSET=utf8mb4 COMMENT='Child table for multi-table delete';

-- 示例：多表 DELETE
-- DELETE p, c FROM case_107_multi_delete_parent p
-- INNER JOIN case_107_multi_delete_child c ON p.id = c.parent_id
-- WHERE p.id = 1;

-- 创建 LOAD DATA 测试表
DROP TABLE IF EXISTS case_108_load_data_test;
CREATE TABLE case_108_load_data_test (
  id INT PRIMARY KEY,
  name VARCHAR(50),
  email VARCHAR(100),
  amount DECIMAL(10,2)
) ENGINE=InnoDB COMMENT='Table for LOAD DATA testing';

-- 示例：LOAD DATA LOCAL INFILE
-- LOAD DATA LOCAL INFILE '/path/to/data.csv'
-- INTO TABLE case_108_load_data_test
-- FIELDS TERMINATED BY ',' ENCLOSED BY '"'
-- LINES TERMINATED BY '\n'
-- IGNORE 1 LINES;

-- 创建 CTE 测试表 (MySQL 8.0+)
DROP TABLE IF EXISTS case_109_cte_test;
CREATE TABLE case_109_cte_test (
  id INT PRIMARY KEY,
  parent_id INT,
  name VARCHAR(50),
  level INT DEFAULT 0
) ENGINE=InnoDB COMMENT='Table for CTE testing';

-- 示例：CTE 查询
-- WITH RECURSIVE cte AS (
--   SELECT id, parent_id, name, level FROM case_109_cte_test WHERE parent_id IS NULL
--   UNION ALL
--   SELECT t.id, t.parent_id, t.name, t.level + 1
--   FROM case_109_cte_test t
--   INNER JOIN cte ON t.parent_id = cte.id
-- )
-- SELECT * FROM cte;

-- 创建窗口函数测试表 (MySQL 8.0+)
DROP TABLE IF EXISTS case_110_window_function_test;
CREATE TABLE case_110_window_function_test (
  id INT PRIMARY KEY,
  department VARCHAR(50),
  employee_name VARCHAR(50),
  salary DECIMAL(10,2),
  hire_date DATE
) ENGINE=InnoDB COMMENT='Table for window function testing';

-- 示例：窗口函数
-- SELECT 
--   id,
--   department,
--   employee_name,
--   salary,
--   ROW_NUMBER() OVER (PARTITION BY department ORDER BY salary DESC) AS rank,
--   AVG(salary) OVER (PARTITION BY department) AS dept_avg
-- FROM case_110_window_function_test;

-- 创建 JSON 表函数测试表 (MySQL 8.0+)
DROP TABLE IF EXISTS case_111_json_table_test;
CREATE TABLE case_111_json_table_test (
  id INT PRIMARY KEY,
  json_data JSON
) ENGINE=InnoDB COMMENT='Table for JSON_TABLE testing';

-- 示例：JSON_TABLE
-- SELECT jt.* FROM case_111_json_table_test,
-- JSON_TABLE(json_data, '$[*]' COLUMNS(
--   name VARCHAR(50) PATH '$.name',
--   value INT PATH '$.value'
-- )) AS jt;

-- 创建正则表达式函数测试表 (MySQL 8.0+)
DROP TABLE IF EXISTS case_112_regex_function_test;
CREATE TABLE case_112_regex_function_test (
  id INT PRIMARY KEY,
  text_content TEXT,
  email VARCHAR(100),
  phone VARCHAR(20)
) ENGINE=InnoDB COMMENT='Table for regex function testing';

-- 示例：正则表达式函数
-- SELECT 
--   id,
--   REGEXP_REPLACE(text_content, '[0-9]+', '#') AS masked,
--   REGEXP_SUBSTR(email, '[A-Za-z0-9._%+-]+') AS username,
--   REGEXP_INSTR(phone, '[0-9]{3,4}-[0-9]{7,8}') AS pos
-- FROM case_112_regex_function_test;

-- 创建优化器提示测试表
DROP TABLE IF EXISTS case_113_optimizer_hint_test;
CREATE TABLE case_113_optimizer_hint_test (
  id INT PRIMARY KEY,
  name VARCHAR(50),
  status VARCHAR(20),
  INDEX idx_name (name),
  INDEX idx_status (status)
) ENGINE=InnoDB COMMENT='Table for optimizer hint testing';

-- 示例：优化器提示
-- SELECT /*+ INDEX(idx_name) */ * FROM case_113_optimizer_hint_test WHERE name = 'test';
-- SELECT /*+ HASH_JOIN(t1, t2) */ * FROM case_113_optimizer_hint_test t1
-- JOIN case_113_optimizer_hint_test t2 ON t1.id = t2.id;

-- 创建角色管理测试表 (MySQL 8.0+)
DROP TABLE IF EXISTS case_114_role_test;
CREATE TABLE case_114_role_test (
  id INT PRIMARY KEY,
  role_name VARCHAR(50),
  permissions JSON
) ENGINE=InnoDB COMMENT='Table for role management testing';

-- 示例：角色管理
-- CREATE ROLE 'read_only', 'read_write', 'admin';
-- GRANT SELECT ON *.* TO 'read_only';
-- GRANT 'read_only' TO 'user1'@'localhost';

-- 创建资源组测试表 (MySQL 8.0+)
DROP TABLE IF EXISTS case_115_resource_group_test;
CREATE TABLE case_115_resource_group_test (
  id INT PRIMARY KEY,
  query_name VARCHAR(50),
  priority VARCHAR(20)
) ENGINE=InnoDB COMMENT='Table for resource group testing';

-- 示例：资源组
-- CREATE RESOURCE GROUP rg_high VCPU=2-4 PRIORITY=HIGH;
-- SET RESOURCE_GROUP rg_high;

-- 创建多值索引测试表 (MySQL 8.0.17+)
DROP TABLE IF EXISTS case_116_multi_valued_index_test;
CREATE TABLE case_116_multi_valued_index_test (
  id INT PRIMARY KEY,
  tags JSON,
  attributes JSON
) ENGINE=InnoDB COMMENT='Table for multi-valued index testing';

-- 示例：多值索引
-- CREATE INDEX idx_tags ON case_116_multi_valued_index_test((CAST(tags->'$[*]' AS UNSIGNED ARRAY)));
-- SELECT * FROM case_116_multi_valued_index_test WHERE 1 MEMBER OF(tags);

-- 创建 NOWAIT/SKIP LOCKED 测试表
DROP TABLE IF EXISTS case_117_nowait_skip_locked_test;
CREATE TABLE case_117_nowait_skip_locked_test (
  id INT PRIMARY KEY,
  task_name VARCHAR(50),
  status VARCHAR(20),
  locked_at TIMESTAMP NULL
) ENGINE=InnoDB COMMENT='Table for NOWAIT/SKIP LOCKED testing';

-- 示例：NOWAIT/SKIP LOCKED
-- SELECT * FROM case_117_nowait_skip_locked_test WHERE status = 'pending' FOR UPDATE NOWAIT;
-- SELECT * FROM case_117_nowait_skip_locked_test WHERE status = 'pending' FOR UPDATE SKIP LOCKED;

-- 创建持久化全局变量测试表
DROP TABLE IF EXISTS case_118_persist_variable_test;
CREATE TABLE case_118_persist_variable_test (
  id INT PRIMARY KEY,
  variable_name VARCHAR(50),
  variable_value VARCHAR(100)
) ENGINE=InnoDB COMMENT='Table for persistent variable testing';

-- 示例：持久化全局变量
-- SET PERSIST max_connections = 1000;
-- SET PERSIST_ONLY max_connections = 1000;

-- 创建 FORCE INDEX 测试表
DROP TABLE IF EXISTS case_119_force_index_test;
CREATE TABLE case_119_force_index_test (
  id INT PRIMARY KEY,
  name VARCHAR(50),
  category VARCHAR(20),
  INDEX idx_name (name),
  INDEX idx_category (category)
) ENGINE=InnoDB COMMENT='Table for FORCE INDEX testing';

-- 示例：FORCE INDEX / USE INDEX / IGNORE INDEX
-- SELECT * FROM case_119_force_index_test FORCE INDEX (idx_name) WHERE name = 'test';
-- SELECT * FROM case_119_force_index_test USE INDEX (idx_category) WHERE category = 'A';
-- SELECT * FROM case_119_force_index_test IGNORE INDEX (idx_name) WHERE id = 1;

-- 创建表锁测试表
DROP TABLE IF EXISTS case_120_table_lock_test;
CREATE TABLE case_120_table_lock_test (
  id INT PRIMARY KEY,
  data VARCHAR(100),
  version INT DEFAULT 1
) ENGINE=InnoDB COMMENT='Table for table lock testing';

-- 示例：表锁
-- LOCK TABLES case_120_table_lock_test READ, case_120_table_lock_test WRITE;
-- UNLOCK TABLES;

-- ============================================================
-- 结束：MySQL 5.7+ 语法案例补充完成
-- 范围：case_101 ~ case_120（20 个测试表案例）
-- ============================================================

-- ============================================================
-- 以下为日常开发常用语法案例 (case_121 ~ case_160)
-- ============================================================

-- ------------------------------
-- 日常开发场景 - 电商系统
-- ------------------------------

-- 创建用户表（电商场景）
DROP TABLE IF EXISTS case_121_ecom_users;
CREATE TABLE case_121_ecom_users (
  user_id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY COMMENT '用户 ID',
  username VARCHAR(32) NOT NULL COMMENT '用户名',
  password_hash VARCHAR(255) NOT NULL COMMENT '密码哈希',
  nickname VARCHAR(50) COMMENT '昵称',
  avatar_url VARCHAR(255) COMMENT '头像 URL',
  phone VARCHAR(20) COMMENT '手机号',
  email VARCHAR(100) COMMENT '邮箱',
  gender TINYINT DEFAULT 0 COMMENT '性别：0-未知，1-男，2-女',
  birthday DATE COMMENT '生日',
  status TINYINT DEFAULT 1 COMMENT '状态：0-禁用，1-正常，2-锁定',
  last_login_at DATETIME COMMENT '最后登录时间',
  last_login_ip VARCHAR(50) COMMENT '最后登录 IP',
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  deleted_at TIMESTAMP NULL COMMENT '删除时间（软删除）',
  UNIQUE KEY uk_username (username),
  UNIQUE KEY uk_phone (phone),
  UNIQUE KEY uk_email (email),
  INDEX idx_status (status),
  INDEX idx_created_at (created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='用户表';

-- 创建商品表
DROP TABLE IF EXISTS case_122_ecom_products;
CREATE TABLE case_122_ecom_products (
  product_id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY COMMENT '商品 ID',
  category_id INT NOT NULL COMMENT '分类 ID',
  brand_id INT COMMENT '品牌 ID',
  product_name VARCHAR(200) NOT NULL COMMENT '商品名称',
  product_code VARCHAR(50) COMMENT '商品编码',
  short_desc VARCHAR(500) COMMENT '简短描述',
  detail_desc TEXT COMMENT '详细描述',
  main_image VARCHAR(255) COMMENT '主图',
  images JSON COMMENT '图片列表',
  unit_price DECIMAL(10,2) NOT NULL COMMENT '单价',
  cost_price DECIMAL(10,2) COMMENT '成本价',
  stock_quantity INT DEFAULT 0 COMMENT '库存数量',
  warn_stock INT DEFAULT 10 COMMENT '预警库存',
  status TINYINT DEFAULT 1 COMMENT '状态：0-下架，1-上架，2-售罄',
  is_hot TINYINT DEFAULT 0 COMMENT '是否热销',
  is_new TINYINT DEFAULT 0 COMMENT '是否新品',
  sort_order INT DEFAULT 0 COMMENT '排序',
  view_count INT DEFAULT 0 COMMENT '浏览次数',
  sale_count INT DEFAULT 0 COMMENT '销售数量',
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  UNIQUE KEY uk_product_code (product_code),
  INDEX idx_category (category_id),
  INDEX idx_brand (brand_id),
  INDEX idx_status (status),
  INDEX idx_price (unit_price),
  INDEX idx_created_at (created_at),
  FULLTEXT KEY ft_product_name (product_name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='商品表';

-- 创建订单表
DROP TABLE IF EXISTS case_123_ecom_orders;
CREATE TABLE case_123_ecom_orders (
  order_id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY COMMENT '订单 ID',
  order_no VARCHAR(50) NOT NULL COMMENT '订单编号',
  user_id BIGINT NOT NULL COMMENT '用户 ID',
  order_status TINYINT DEFAULT 0 COMMENT '订单状态：0-待支付，1-已支付，2-已发货，3-已完成，4-已取消',
  total_amount DECIMAL(12,2) NOT NULL COMMENT '订单总额',
  discount_amount DECIMAL(10,2) DEFAULT 0 COMMENT '优惠金额',
  freight_amount DECIMAL(10,2) DEFAULT 0 COMMENT '运费',
  pay_amount DECIMAL(10,2) NOT NULL COMMENT '实付金额',
  pay_type TINYINT COMMENT '支付方式：1-微信，2-支付宝，3-银行卡',
  pay_time DATETIME COMMENT '支付时间',
  delivery_sn VARCHAR(100) COMMENT '物流单号',
  delivery_company VARCHAR(50) COMMENT '物流公司',
  receiver_name VARCHAR(50) COMMENT '收货人姓名',
  receiver_phone VARCHAR(20) COMMENT '收货人电话',
  receiver_province VARCHAR(50) COMMENT '省',
  receiver_city VARCHAR(50) COMMENT '市',
  receiver_district VARCHAR(50) COMMENT '区',
  receiver_address VARCHAR(200) COMMENT '详细地址',
  remark VARCHAR(500) COMMENT '订单备注',
  confirm_time DATETIME COMMENT '确认收货时间',
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  UNIQUE KEY uk_order_no (order_no),
  INDEX idx_user_id (user_id),
  INDEX idx_order_status (order_status),
  INDEX idx_created_at (created_at),
  INDEX idx_pay_time (pay_time)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='订单表';

-- 创建订单明细表
DROP TABLE IF EXISTS case_124_ecom_order_items;
CREATE TABLE case_124_ecom_order_items (
  item_id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY COMMENT '明细 ID',
  order_id BIGINT NOT NULL COMMENT '订单 ID',
  product_id BIGINT NOT NULL COMMENT '商品 ID',
  product_name VARCHAR(200) COMMENT '商品名称（快照）',
  product_image VARCHAR(255) COMMENT '商品图片（快照）',
  unit_price DECIMAL(10,2) NOT NULL COMMENT '单价',
  quantity INT NOT NULL COMMENT '数量',
  subtotal DECIMAL(12,2) NOT NULL COMMENT '小计',
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  INDEX idx_order_id (order_id),
  INDEX idx_product_id (product_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='订单明细表';

-- 创建购物车表
DROP TABLE IF EXISTS case_125_ecom_cart;
CREATE TABLE case_125_ecom_cart (
  cart_id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY COMMENT '购物车 ID',
  user_id BIGINT NOT NULL COMMENT '用户 ID',
  product_id BIGINT NOT NULL COMMENT '商品 ID',
  quantity INT NOT NULL DEFAULT 1 COMMENT '数量',
  is_selected TINYINT DEFAULT 1 COMMENT '是否选中',
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  UNIQUE KEY uk_user_product (user_id, product_id),
  INDEX idx_user_id (user_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='购物车表';

-- ------------------------------
-- 日常开发场景 - 内容管理系统
-- ------------------------------

-- 创建文章表
DROP TABLE IF EXISTS case_126_cms_articles;
CREATE TABLE case_126_cms_articles (
  article_id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY COMMENT '文章 ID',
  category_id INT NOT NULL COMMENT '分类 ID',
  user_id BIGINT COMMENT '作者 ID',
  title VARCHAR(200) NOT NULL COMMENT '标题',
  summary VARCHAR(500) COMMENT '摘要',
  content LONGTEXT COMMENT '内容',
  cover_image VARCHAR(255) COMMENT '封面图',
  tags JSON COMMENT '标签',
  view_count INT DEFAULT 0 COMMENT '浏览量',
  like_count INT DEFAULT 0 COMMENT '点赞数',
  comment_count INT DEFAULT 0 COMMENT '评论数',
  is_top TINYINT DEFAULT 0 COMMENT '是否置顶',
  is_recommend TINYINT DEFAULT 0 COMMENT '是否推荐',
  status TINYINT DEFAULT 0 COMMENT '状态：0-草稿，1-已发布，2-已下架',
  published_at DATETIME COMMENT '发布时间',
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  INDEX idx_category (category_id),
  INDEX idx_user_id (user_id),
  INDEX idx_status (status),
  INDEX idx_published_at (published_at),
  INDEX idx_view_count (view_count),
  FULLTEXT KEY ft_title_content (title, content)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='文章表';

-- 创建评论表
DROP TABLE IF EXISTS case_127_cms_comments;
CREATE TABLE case_127_cms_comments (
  comment_id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY COMMENT '评论 ID',
  article_id BIGINT NOT NULL COMMENT '文章 ID',
  user_id BIGINT NOT NULL COMMENT '用户 ID',
  parent_id BIGINT DEFAULT 0 COMMENT '父评论 ID',
  content TEXT NOT NULL COMMENT '评论内容',
  like_count INT DEFAULT 0 COMMENT '点赞数',
  status TINYINT DEFAULT 1 COMMENT '状态：0-待审核，1-已通过，2-已拒绝',
  ip_address VARCHAR(50) COMMENT 'IP 地址',
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  INDEX idx_article_id (article_id),
  INDEX idx_user_id (user_id),
  INDEX idx_parent_id (parent_id),
  INDEX idx_status (status)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='评论表';

-- ------------------------------
-- 日常开发场景 - 金融系统
-- ------------------------------

-- 创建账户表
DROP TABLE IF EXISTS case_128_finance_accounts;
CREATE TABLE case_128_finance_accounts (
  account_id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY COMMENT '账户 ID',
  user_id BIGINT NOT NULL COMMENT '用户 ID',
  account_no VARCHAR(50) NOT NULL COMMENT '账号',
  account_type TINYINT DEFAULT 1 COMMENT '账户类型：1-储蓄，2-信用，3-投资',
  currency VARCHAR(3) DEFAULT 'CNY' COMMENT '币种',
  balance DECIMAL(20,4) DEFAULT 0 COMMENT '余额',
  available_balance DECIMAL(20,4) DEFAULT 0 COMMENT '可用余额',
  frozen_balance DECIMAL(20,4) DEFAULT 0 COMMENT '冻结余额',
  status TINYINT DEFAULT 1 COMMENT '状态：0-冻结，1-正常，2-注销',
  opened_at DATETIME COMMENT '开户时间',
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  UNIQUE KEY uk_account_no (account_no),
  INDEX idx_user_id (user_id),
  INDEX idx_account_type (account_type),
  INDEX idx_status (status)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='账户表';

-- 创建交易流水表
DROP TABLE IF EXISTS case_129_finance_transactions;
CREATE TABLE case_129_finance_transactions (
  trans_id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY COMMENT '交易 ID',
  trans_no VARCHAR(50) NOT NULL COMMENT '交易流水号',
  account_id BIGINT NOT NULL COMMENT '账户 ID',
  trans_type TINYINT NOT NULL COMMENT '交易类型：1-存入，2-取出，3-转账，4-消费',
  amount DECIMAL(20,4) NOT NULL COMMENT '金额',
  balance_before DECIMAL(20,4) COMMENT '交易前余额',
  balance_after DECIMAL(20,4) COMMENT '交易后余额',
  counterparty_account VARCHAR(50) COMMENT '对方账户',
  remark VARCHAR(200) COMMENT '备注',
  status TINYINT DEFAULT 1 COMMENT '状态：0-失败，1-成功，2-处理中',
  trans_time DATETIME NOT NULL COMMENT '交易时间',
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  UNIQUE KEY uk_trans_no (trans_no),
  INDEX idx_account_id (account_id),
  INDEX idx_trans_time (trans_time),
  INDEX idx_trans_type (trans_type)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='交易流水表';

-- ------------------------------
-- 日常开发场景 - 社交系统
-- ------------------------------

-- 创建关注关系表
DROP TABLE IF EXISTS case_130_social_follows;
CREATE TABLE case_130_social_follows (
  id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY COMMENT 'ID',
  follower_id BIGINT NOT NULL COMMENT '关注者 ID',
  followee_id BIGINT NOT NULL COMMENT '被关注者 ID',
  status TINYINT DEFAULT 1 COMMENT '状态：0-已取消，1-已关注',
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  UNIQUE KEY uk_follower_followee (follower_id, followee_id),
  INDEX idx_follower (follower_id),
  INDEX idx_followee (followee_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='关注关系表';

-- 创建点赞表
DROP TABLE IF EXISTS case_131_social_likes;
CREATE TABLE case_131_social_likes (
  id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY COMMENT 'ID',
  user_id BIGINT NOT NULL COMMENT '用户 ID',
  target_type TINYINT NOT NULL COMMENT '目标类型：1-文章，2-评论，3-动态',
  target_id BIGINT NOT NULL COMMENT '目标 ID',
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  UNIQUE KEY uk_user_target (user_id, target_type, target_id),
  INDEX idx_target (target_type, target_id),
  INDEX idx_user_id (user_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='点赞表';

-- 创建消息通知表
DROP TABLE IF EXISTS case_132_social_notifications;
CREATE TABLE case_132_social_notifications (
  notify_id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY COMMENT '通知 ID',
  user_id BIGINT NOT NULL COMMENT '用户 ID',
  notify_type TINYINT NOT NULL COMMENT '通知类型：1-系统，2-点赞，3-评论，4-关注',
  title VARCHAR(100) COMMENT '标题',
  content VARCHAR(500) COMMENT '内容',
  target_url VARCHAR(255) COMMENT '目标链接',
  is_read TINYINT DEFAULT 0 COMMENT '是否已读',
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  INDEX idx_user_id (user_id),
  INDEX idx_is_read (is_read),
  INDEX idx_created_at (created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='消息通知表';

-- ------------------------------
-- 日常开发场景 - 日志系统
-- ------------------------------

-- 创建操作日志表
DROP TABLE IF EXISTS case_133_log_operations;
CREATE TABLE case_133_log_operations (
  log_id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY COMMENT '日志 ID',
  trace_id VARCHAR(50) COMMENT '追踪 ID',
  user_id BIGINT COMMENT '用户 ID',
  username VARCHAR(50) COMMENT '用户名',
  module VARCHAR(50) COMMENT '模块',
  action VARCHAR(50) COMMENT '操作',
  method VARCHAR(10) COMMENT '请求方法',
  request_url VARCHAR(500) COMMENT '请求 URL',
  ip_address VARCHAR(50) COMMENT 'IP 地址',
  user_agent VARCHAR(500) COMMENT 'User-Agent',
  request_params JSON COMMENT '请求参数',
  response_code INT COMMENT '响应码',
  response_time INT COMMENT '响应时间 (ms)',
  error_message TEXT COMMENT '错误信息',
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  INDEX idx_user_id (user_id),
  INDEX idx_module (module),
  INDEX idx_action (action),
  INDEX idx_created_at (created_at),
  INDEX idx_response_code (response_code)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='操作日志表';

-- 创建登录日志表
DROP TABLE IF EXISTS case_134_log_logins;
CREATE TABLE case_134_log_logins (
  login_id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY COMMENT '登录 ID',
  user_id BIGINT NOT NULL COMMENT '用户 ID',
  username VARCHAR(50) COMMENT '用户名',
  login_type TINYINT DEFAULT 1 COMMENT '登录方式：1-密码，2-短信，3-第三方',
  login_result TINYINT DEFAULT 0 COMMENT '登录结果：0-失败，1-成功',
  ip_address VARCHAR(50) COMMENT 'IP 地址',
  ip_location VARCHAR(100) COMMENT 'IP 归属地',
  user_agent VARCHAR(500) COMMENT 'User-Agent',
  device_type VARCHAR(20) COMMENT '设备类型',
  error_message VARCHAR(200) COMMENT '错误信息',
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  INDEX idx_user_id (user_id),
  INDEX idx_login_result (login_result),
  INDEX idx_created_at (created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='登录日志表';

-- ------------------------------
-- 日常开发场景 - 系统管理
-- ------------------------------

-- 创建部门表
DROP TABLE IF EXISTS case_135_sys_departments;
CREATE TABLE case_135_sys_departments (
  dept_id INT UNSIGNED AUTO_INCREMENT PRIMARY KEY COMMENT '部门 ID',
  parent_id INT DEFAULT 0 COMMENT '父部门 ID',
  dept_name VARCHAR(50) NOT NULL COMMENT '部门名称',
  dept_code VARCHAR(50) COMMENT '部门编码',
  sort_order INT DEFAULT 0 COMMENT '排序',
  leader_id BIGINT COMMENT '负责人 ID',
  phone VARCHAR(20) COMMENT '联系电话',
  email VARCHAR(100) COMMENT '邮箱',
  status TINYINT DEFAULT 1 COMMENT '状态：0-禁用，1-正常',
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  INDEX idx_parent_id (parent_id),
  INDEX idx_status (status)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='部门表';

-- 创建角色表
DROP TABLE IF EXISTS case_136_sys_roles;
CREATE TABLE case_136_sys_roles (
  role_id INT UNSIGNED AUTO_INCREMENT PRIMARY KEY COMMENT '角色 ID',
  role_name VARCHAR(50) NOT NULL COMMENT '角色名称',
  role_code VARCHAR(50) NOT NULL COMMENT '角色编码',
  description VARCHAR(200) COMMENT '描述',
  data_scope TINYINT DEFAULT 1 COMMENT '数据范围：1-全部，2-本部门，3-仅本人',
  status TINYINT DEFAULT 1 COMMENT '状态：0-禁用，1-正常',
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  UNIQUE KEY uk_role_code (role_code),
  INDEX idx_status (status)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='角色表';

-- 创建菜单权限表
DROP TABLE IF EXISTS case_137_sys_menus;
CREATE TABLE case_137_sys_menus (
  menu_id INT UNSIGNED AUTO_INCREMENT PRIMARY KEY COMMENT '菜单 ID',
  parent_id INT DEFAULT 0 COMMENT '父菜单 ID',
  menu_name VARCHAR(50) NOT NULL COMMENT '菜单名称',
  menu_type TINYINT DEFAULT 1 COMMENT '类型：1-目录，2-菜单，3-按钮',
  menu_icon VARCHAR(50) COMMENT '图标',
  menu_url VARCHAR(200) COMMENT '路由地址',
  perms VARCHAR(100) COMMENT '权限标识',
  sort_order INT DEFAULT 0 COMMENT '排序',
  is_visible TINYINT DEFAULT 1 COMMENT '是否可见',
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  INDEX idx_parent_id (parent_id),
  INDEX idx_menu_type (menu_type)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='菜单权限表';

-- 创建用户角色关联表
DROP TABLE IF EXISTS case_138_sys_user_roles;
CREATE TABLE case_138_sys_user_roles (
  id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY COMMENT 'ID',
  user_id BIGINT NOT NULL COMMENT '用户 ID',
  role_id INT NOT NULL COMMENT '角色 ID',
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  UNIQUE KEY uk_user_role (user_id, role_id),
  INDEX idx_user_id (user_id),
  INDEX idx_role_id (role_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='用户角色关联表';

-- 创建角色菜单关联表
DROP TABLE IF EXISTS case_139_sys_role_menus;
CREATE TABLE case_139_sys_role_menus (
  id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY COMMENT 'ID',
  role_id INT NOT NULL COMMENT '角色 ID',
  menu_id INT NOT NULL COMMENT '菜单 ID',
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  UNIQUE KEY uk_role_menu (role_id, menu_id),
  INDEX idx_role_id (role_id),
  INDEX idx_menu_id (menu_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='角色菜单关联表';

-- ------------------------------
-- 日常开发场景 - 配置管理
-- ------------------------------

-- 创建系统配置表
DROP TABLE IF EXISTS case_140_sys_config;
CREATE TABLE case_140_sys_config (
  config_id INT UNSIGNED AUTO_INCREMENT PRIMARY KEY COMMENT '配置 ID',
  config_key VARCHAR(100) NOT NULL COMMENT '配置键',
  config_value TEXT COMMENT '配置值',
  config_type TINYINT DEFAULT 1 COMMENT '配置类型：1-字符串，2-数字，3-布尔，4-JSON',
  description VARCHAR(200) COMMENT '描述',
  is_editable TINYINT DEFAULT 1 COMMENT '是否可修改',
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  UNIQUE KEY uk_config_key (config_key),
  INDEX idx_config_type (config_type)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='系统配置表';

-- 创建字典表
DROP TABLE IF EXISTS case_141_sys_dict;
CREATE TABLE case_141_sys_dict (
  dict_id INT UNSIGNED AUTO_INCREMENT PRIMARY KEY COMMENT '字典 ID',
  dict_type VARCHAR(50) NOT NULL COMMENT '字典类型',
  dict_label VARCHAR(100) NOT NULL COMMENT '字典标签',
  dict_value VARCHAR(100) NOT NULL COMMENT '字典值',
  sort_order INT DEFAULT 0 COMMENT '排序',
  is_default TINYINT DEFAULT 0 COMMENT '是否默认',
  status TINYINT DEFAULT 1 COMMENT '状态：0-禁用，1-正常',
  remark VARCHAR(200) COMMENT '备注',
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  INDEX idx_dict_type (dict_type),
  INDEX idx_status (status)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='字典表';

-- ------------------------------
-- 日常开发场景 - 文件管理
-- ------------------------------

-- 创建文件上传表
DROP TABLE IF EXISTS case_142_files_uploads;
CREATE TABLE case_142_files_uploads (
  file_id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY COMMENT '文件 ID',
  file_name VARCHAR(255) NOT NULL COMMENT '文件名',
  original_name VARCHAR(255) COMMENT '原始文件名',
  file_path VARCHAR(500) NOT NULL COMMENT '文件路径',
  file_url VARCHAR(500) COMMENT '文件 URL',
  file_size BIGINT COMMENT '文件大小 (字节)',
  file_type VARCHAR(50) COMMENT '文件类型',
  file_ext VARCHAR(20) COMMENT '文件扩展名',
  user_id BIGINT COMMENT '上传用户 ID',
  module VARCHAR(50) COMMENT '所属模块',
  status TINYINT DEFAULT 1 COMMENT '状态：0-禁用，1-正常',
  download_count INT DEFAULT 0 COMMENT '下载次数',
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  INDEX idx_user_id (user_id),
  INDEX idx_file_type (file_type),
  INDEX idx_created_at (created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='文件上传表';

-- ------------------------------
-- 日常开发场景 - 定时任务
-- ------------------------------

-- 创建定时任务表
DROP TABLE IF EXISTS case_143_job_tasks;
CREATE TABLE case_143_job_tasks (
  job_id INT UNSIGNED AUTO_INCREMENT PRIMARY KEY COMMENT '任务 ID',
  job_name VARCHAR(100) NOT NULL COMMENT '任务名称',
  job_group VARCHAR(50) DEFAULT 'DEFAULT' COMMENT '任务组',
  job_type TINYINT DEFAULT 1 COMMENT '任务类型：1-简单，2-分片',
  bean_name VARCHAR(100) COMMENT 'Bean 名称',
  method_name VARCHAR(100) COMMENT '方法名称',
  method_params VARCHAR(500) COMMENT '参数',
  cron_expression VARCHAR(50) COMMENT 'Cron 表达式',
  misfire_policy TINYINT DEFAULT 1 COMMENT '错失执行策略：1-立即执行，2-执行一次，3-放弃',
  concurrent TINYINT DEFAULT 0 COMMENT '是否并发：0-否，1-是',
  status TINYINT DEFAULT 1 COMMENT '状态：0-暂停，1-正常',
  last_execute_time DATETIME COMMENT '上次执行时间',
  next_execute_time DATETIME COMMENT '下次执行时间',
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  INDEX idx_job_group (job_group),
  INDEX idx_status (status)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='定时任务表';

-- 创建任务执行日志表
DROP TABLE IF EXISTS case_143_job_logs;
CREATE TABLE case_143_job_logs (
  log_id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY COMMENT '日志 ID',
  job_id INT NOT NULL COMMENT '任务 ID',
  job_name VARCHAR(100) COMMENT '任务名称',
  job_group VARCHAR(50) COMMENT '任务组',
  execute_time DATETIME COMMENT '执行时间',
  execute_status TINYINT DEFAULT 0 COMMENT '执行状态：0-失败，1-成功',
  execute_msg TEXT COMMENT '执行信息',
  execute_duration INT COMMENT '执行时长 (ms)',
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  INDEX idx_job_id (job_id),
  INDEX idx_execute_time (execute_time),
  INDEX idx_execute_status (execute_status)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='任务执行日志表';

-- ------------------------------
-- 日常开发场景 - API 管理
-- ------------------------------

-- 创建 API 接口表
DROP TABLE IF EXISTS case_144_api_interfaces;
CREATE TABLE case_144_api_interfaces (
  api_id INT UNSIGNED AUTO_INCREMENT PRIMARY KEY COMMENT '接口 ID',
  api_name VARCHAR(100) NOT NULL COMMENT '接口名称',
  api_path VARCHAR(200) NOT NULL COMMENT '接口路径',
  api_method VARCHAR(10) NOT NULL COMMENT '请求方法',
  api_category VARCHAR(50) COMMENT '接口分类',
  content_type VARCHAR(50) DEFAULT 'application/json' COMMENT 'Content-Type',
  request_schema JSON COMMENT '请求参数 Schema',
  response_schema JSON COMMENT '响应参数 Schema',
  is_auth TINYINT DEFAULT 1 COMMENT '是否需要认证',
  rate_limit INT DEFAULT 0 COMMENT '限流次数/分钟',
  status TINYINT DEFAULT 1 COMMENT '状态：0-禁用，1-启用，2-废弃',
  version VARCHAR(20) DEFAULT 'v1' COMMENT '版本号',
  description VARCHAR(500) COMMENT '描述',
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  UNIQUE KEY uk_api_path_method (api_path, api_method),
  INDEX idx_api_category (api_category),
  INDEX idx_status (status)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='API 接口表';

-- 创建 API 访问日志表
DROP TABLE IF EXISTS case_144_api_logs;
CREATE TABLE case_144_api_logs (
  log_id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY COMMENT '日志 ID',
  trace_id VARCHAR(50) COMMENT '追踪 ID',
  api_id INT COMMENT '接口 ID',
  api_path VARCHAR(200) COMMENT '接口路径',
  api_method VARCHAR(10) COMMENT '请求方法',
  client_ip VARCHAR(50) COMMENT '客户端 IP',
  user_id BIGINT COMMENT '用户 ID',
  request_headers JSON COMMENT '请求头',
  request_body TEXT COMMENT '请求体',
  response_code INT COMMENT '响应码',
  response_body TEXT COMMENT '响应体',
  response_time INT COMMENT '响应时间 (ms)',
  error_message TEXT COMMENT '错误信息',
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  INDEX idx_api_path (api_path),
  INDEX idx_user_id (user_id),
  INDEX idx_response_code (response_code),
  INDEX idx_created_at (created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='API 访问日志表';

-- ------------------------------
-- 日常开发场景 - 多租户
-- ------------------------------

-- 创建租户表
DROP TABLE IF EXISTS case_145_tenant_info;
CREATE TABLE case_145_tenant_info (
  tenant_id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY COMMENT '租户 ID',
  tenant_name VARCHAR(100) NOT NULL COMMENT '租户名称',
  tenant_code VARCHAR(50) NOT NULL COMMENT '租户编码',
  contact_name VARCHAR(50) COMMENT '联系人',
  contact_phone VARCHAR(20) COMMENT '联系电话',
  contact_email VARCHAR(100) COMMENT '联系邮箱',
  max_users INT DEFAULT 0 COMMENT '最大用户数',
  max_storage BIGINT DEFAULT 0 COMMENT '最大存储 (GB)',
  expire_date DATE COMMENT '到期日期',
  status TINYINT DEFAULT 1 COMMENT '状态：0-禁用，1-正常，2-过期',
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  UNIQUE KEY uk_tenant_code (tenant_code),
  INDEX idx_status (status),
  INDEX idx_expire_date (expire_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='租户信息表';

-- 创建租户配置表
DROP TABLE IF EXISTS case_145_tenant_config;
CREATE TABLE case_145_tenant_config (
  config_id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY COMMENT '配置 ID',
  tenant_id BIGINT NOT NULL COMMENT '租户 ID',
  config_key VARCHAR(100) NOT NULL COMMENT '配置键',
  config_value TEXT COMMENT '配置值',
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  UNIQUE KEY uk_tenant_config (tenant_id, config_key),
  INDEX idx_tenant_id (tenant_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='租户配置表';

-- ------------------------------
-- 日常开发场景 - 数据统计
-- ------------------------------

-- 创建统计表（日统计）
DROP TABLE IF EXISTS case_146_stats_daily;
CREATE TABLE case_146_stats_daily (
  id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY COMMENT 'ID',
  stat_date DATE NOT NULL COMMENT '统计日期',
  stat_type VARCHAR(50) NOT NULL COMMENT '统计类型',
  stat_key VARCHAR(100) COMMENT '统计维度',
  stat_value DECIMAL(20,4) DEFAULT 0 COMMENT '统计值',
  stat_count INT DEFAULT 0 COMMENT '统计数量',
  extra_data JSON COMMENT '额外数据',
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  UNIQUE KEY uk_date_type_key (stat_date, stat_type, stat_key),
  INDEX idx_stat_date (stat_date),
  INDEX idx_stat_type (stat_type)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='日统计表';

-- 创建统计表（月统计）
DROP TABLE IF EXISTS case_146_stats_monthly;
CREATE TABLE case_146_stats_monthly (
  id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY COMMENT 'ID',
  stat_month CHAR(7) NOT NULL COMMENT '统计月份 (YYYY-MM)',
  stat_type VARCHAR(50) NOT NULL COMMENT '统计类型',
  stat_key VARCHAR(100) COMMENT '统计维度',
  stat_value DECIMAL(20,4) DEFAULT 0 COMMENT '统计值',
  stat_count INT DEFAULT 0 COMMENT '统计数量',
  extra_data JSON COMMENT '额外数据',
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  UNIQUE KEY uk_month_type_key (stat_month, stat_type, stat_key),
  INDEX idx_stat_month (stat_month),
  INDEX idx_stat_type (stat_type)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='月统计表';

-- ------------------------------
-- 日常开发场景 - 地理位置
-- ------------------------------

-- 创建地区表
DROP TABLE IF EXISTS case_147_geo_regions;
CREATE TABLE case_147_geo_regions (
  region_code VARCHAR(20) PRIMARY KEY COMMENT '地区编码',
  parent_code VARCHAR(20) COMMENT '父地区编码',
  region_name VARCHAR(100) NOT NULL COMMENT '地区名称',
  region_level TINYINT DEFAULT 1 COMMENT '地区级别：1-省，2-市，3-区',
  sort_order INT DEFAULT 0 COMMENT '排序',
  is_hot TINYINT DEFAULT 0 COMMENT '是否热门',
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  INDEX idx_parent_code (parent_code),
  INDEX idx_region_level (region_level)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='地区表';

-- 创建用户地址表
DROP TABLE IF EXISTS case_147_user_addresses;
CREATE TABLE case_147_user_addresses (
  address_id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY COMMENT '地址 ID',
  user_id BIGINT NOT NULL COMMENT '用户 ID',
  contact_name VARCHAR(50) NOT NULL COMMENT '联系人',
  contact_phone VARCHAR(20) NOT NULL COMMENT '联系电话',
  province_code VARCHAR(20) COMMENT '省编码',
  city_code VARCHAR(20) COMMENT '市编码',
  district_code VARCHAR(20) COMMENT '区编码',
  detail_address VARCHAR(200) COMMENT '详细地址',
  is_default TINYINT DEFAULT 0 COMMENT '是否默认',
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  INDEX idx_user_id (user_id),
  INDEX idx_is_default (is_default)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='用户地址表';

-- ------------------------------
-- 日常开发场景 - 优惠券系统
-- ------------------------------

-- 创建优惠券模板表
DROP TABLE IF EXISTS case_148_coupon_templates;
CREATE TABLE case_148_coupon_templates (
  template_id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY COMMENT '模板 ID',
  template_name VARCHAR(100) NOT NULL COMMENT '模板名称',
  coupon_type TINYINT DEFAULT 1 COMMENT '券类型：1-满减，2-折扣，3-免邮',
  discount_value DECIMAL(10,2) COMMENT '优惠额度',
  min_purchase DECIMAL(10,2) COMMENT '最低消费',
  max_discount DECIMAL(10,2) COMMENT '最高折扣',
  total_count INT COMMENT '发放总量',
  per_limit INT DEFAULT 1 COMMENT '每人限领',
  valid_type TINYINT DEFAULT 1 COMMENT '有效期类型：1-固定，2-领取后',
  valid_start DATETIME COMMENT '有效期开始',
  valid_end DATETIME COMMENT '有效期结束',
  valid_days INT COMMENT '有效天数',
  status TINYINT DEFAULT 1 COMMENT '状态：0-禁用，1-启用',
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  INDEX idx_coupon_type (coupon_type),
  INDEX idx_status (status)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='优惠券模板表';

-- 创建用户优惠券表
DROP TABLE IF EXISTS case_148_user_coupons;
CREATE TABLE case_148_user_coupons (
  coupon_id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY COMMENT '优惠券 ID',
  user_id BIGINT NOT NULL COMMENT '用户 ID',
  template_id BIGINT NOT NULL COMMENT '模板 ID',
  coupon_code VARCHAR(50) NOT NULL COMMENT '优惠券码',
  status TINYINT DEFAULT 0 COMMENT '状态：0-未使用，1-已使用，2-已过期',
  order_id BIGINT COMMENT '使用订单 ID',
  used_at DATETIME COMMENT '使用时间',
  valid_start DATETIME NOT NULL COMMENT '有效期开始',
  valid_end DATETIME NOT NULL COMMENT '有效期结束',
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  UNIQUE KEY uk_coupon_code (coupon_code),
  INDEX idx_user_id (user_id),
  INDEX idx_template_id (template_id),
  INDEX idx_status (status)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='用户优惠券表';

-- ------------------------------
-- 日常开发场景 - 积分系统
-- ------------------------------

-- 创建用户积分表
DROP TABLE IF EXISTS case_149_points_accounts;
CREATE TABLE case_149_points_accounts (
  account_id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY COMMENT '账户 ID',
  user_id BIGINT NOT NULL COMMENT '用户 ID',
  total_points BIGINT DEFAULT 0 COMMENT '累计积分',
  available_points BIGINT DEFAULT 0 COMMENT '可用积分',
  frozen_points BIGINT DEFAULT 0 COMMENT '冻结积分',
  used_points BIGINT DEFAULT 0 COMMENT '已用积分',
  expired_points BIGINT DEFAULT 0 COMMENT '过期积分',
  level TINYINT DEFAULT 1 COMMENT '会员等级',
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  UNIQUE KEY uk_user_id (user_id),
  INDEX idx_level (level)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='用户积分表';

-- 创建积分流水表
DROP TABLE IF EXISTS case_149_points_logs;
CREATE TABLE case_149_points_logs (
  log_id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY COMMENT '流水 ID',
  user_id BIGINT NOT NULL COMMENT '用户 ID',
  points_type TINYINT DEFAULT 1 COMMENT '积分类型：1-收入，2-支出',
  points_value INT NOT NULL COMMENT '积分值',
  balance_before BIGINT COMMENT '变动前余额',
  balance_after BIGINT COMMENT '变动后余额',
  source_type VARCHAR(50) COMMENT '来源类型：签到，消费，活动等',
  source_id BIGINT COMMENT '来源 ID',
  description VARCHAR(200) COMMENT '描述',
  expire_date DATE COMMENT '过期日期',
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  INDEX idx_user_id (user_id),
  INDEX idx_points_type (points_type),
  INDEX idx_created_at (created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='积分流水表';

-- ------------------------------
-- 日常开发场景 - 版本控制
-- ------------------------------

-- 创建数据版本表
DROP TABLE IF EXISTS case_150_data_versions;
CREATE TABLE case_150_data_versions (
  version_id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY COMMENT '版本 ID',
  entity_type VARCHAR(50) NOT NULL COMMENT '实体类型',
  entity_id BIGINT NOT NULL COMMENT '实体 ID',
  version_no INT NOT NULL COMMENT '版本号',
  old_data JSON COMMENT '旧数据',
  new_data JSON COMMENT '新数据',
  change_type TINYINT DEFAULT 1 COMMENT '变更类型：1-创建，2-更新，3-删除',
  change_user BIGINT COMMENT '变更用户',
  change_reason VARCHAR(200) COMMENT '变更原因',
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  INDEX idx_entity (entity_type, entity_id),
  INDEX idx_version_no (version_no),
  INDEX idx_created_at (created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='数据版本表';

-- ------------------------------
-- 日常开发场景 - 项目管理
-- ------------------------------

-- 创建项目表
DROP TABLE IF EXISTS case_151_pm_projects;
CREATE TABLE case_151_pm_projects (
  project_id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY COMMENT '项目 ID',
  project_name VARCHAR(100) NOT NULL COMMENT '项目名称',
  project_code VARCHAR(50) NOT NULL COMMENT '项目编码',
  project_type TINYINT DEFAULT 1 COMMENT '项目类型：1-研发，2-市场，3-运营',
  priority TINYINT DEFAULT 2 COMMENT '优先级：1-低，2-中，3-高，4-紧急',
  status TINYINT DEFAULT 1 COMMENT '状态：0-取消，1-规划，2-进行中，3-暂停，4-完成',
  manager_id BIGINT COMMENT '负责人 ID',
  start_date DATE COMMENT '开始日期',
  end_date DATE COMMENT '结束日期',
  actual_end_date DATE COMMENT '实际结束日期',
  budget DECIMAL(12,2) COMMENT '预算',
  actual_cost DECIMAL(12,2) COMMENT '实际成本',
  progress INT DEFAULT 0 COMMENT '进度百分比',
  description TEXT COMMENT '项目描述',
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  UNIQUE KEY uk_project_code (project_code),
  INDEX idx_status (status),
  INDEX idx_manager_id (manager_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='项目表';

-- 创建任务表
DROP TABLE IF EXISTS case_151_pm_tasks;
CREATE TABLE case_151_pm_tasks (
  task_id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY COMMENT '任务 ID',
  project_id BIGINT NOT NULL COMMENT '项目 ID',
  parent_task_id BIGINT DEFAULT 0 COMMENT '父任务 ID',
  task_name VARCHAR(200) NOT NULL COMMENT '任务名称',
  task_type TINYINT DEFAULT 1 COMMENT '任务类型：1-需求，2-设计，3-开发，4-测试，5-部署',
  priority TINYINT DEFAULT 2 COMMENT '优先级：1-低，2-中，3-高，4-紧急',
  status TINYINT DEFAULT 1 COMMENT '状态：0-取消，1-待办，2-进行中，3-已完成',
  assignee_id BIGINT COMMENT '执行人 ID',
  reporter_id BIGINT COMMENT '报告人 ID',
  estimated_hours DECIMAL(8,2) COMMENT '预估工时',
  actual_hours DECIMAL(8,2) COMMENT '实际工时',
  start_date DATE COMMENT '开始日期',
  due_date DATE COMMENT '截止日期',
  completed_at DATETIME COMMENT '完成时间',
  description TEXT COMMENT '任务描述',
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  INDEX idx_project_id (project_id),
  INDEX idx_parent_task_id (parent_task_id),
  INDEX idx_status (status),
  INDEX idx_assignee_id (assignee_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='任务表';

-- ------------------------------
-- 日常开发场景 - 在线教育系统
-- ------------------------------

-- 创建课程表
DROP TABLE IF EXISTS case_152_edu_courses;
CREATE TABLE case_152_edu_courses (
  course_id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY COMMENT '课程 ID',
  course_name VARCHAR(200) NOT NULL COMMENT '课程名称',
  course_code VARCHAR(50) NOT NULL COMMENT '课程编码',
  instructor_id BIGINT COMMENT '讲师 ID',
  category_id INT COMMENT '分类 ID',
  level TINYINT DEFAULT 1 COMMENT '难度：1-入门，2-初级，3-中级，4-高级',
  price DECIMAL(10,2) DEFAULT 0 COMMENT '价格',
  discount_price DECIMAL(10,2) COMMENT '折扣价',
  max_students INT DEFAULT 0 COMMENT '最大学员数',
  enrolled_count INT DEFAULT 0 COMMENT '已报名数',
  duration_hours INT DEFAULT 0 COMMENT '课程时长 (小时)',
  status TINYINT DEFAULT 1 COMMENT '状态：0-下架，1-上架，2-预告',
  description TEXT COMMENT '课程描述',
  cover_image VARCHAR(255) COMMENT '封面图',
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  UNIQUE KEY uk_course_code (course_code),
  INDEX idx_instructor_id (instructor_id),
  INDEX idx_category_id (category_id),
  INDEX idx_status (status)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='课程表';

-- 创建学员课程关联表
DROP TABLE IF EXISTS case_152_edu_enrollments;
CREATE TABLE case_152_edu_enrollments (
  enrollment_id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY COMMENT '报名 ID',
  student_id BIGINT NOT NULL COMMENT '学员 ID',
  course_id BIGINT NOT NULL COMMENT '课程 ID',
  order_id BIGINT COMMENT '订单 ID',
  enroll_status TINYINT DEFAULT 1 COMMENT '状态：1-学习中，2-已完成，3-已退款',
  progress INT DEFAULT 0 COMMENT '学习进度%',
  enrolled_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP COMMENT '报名时间',
  completed_at DATETIME COMMENT '完成时间',
  last_learned_at DATETIME COMMENT '最后学习时间',
  UNIQUE KEY uk_student_course (student_id, course_id),
  INDEX idx_student_id (student_id),
  INDEX idx_course_id (course_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='学员课程关联表';

-- 创建课程章节表
DROP TABLE IF EXISTS case_152_edu_chapters;
CREATE TABLE case_152_edu_chapters (
  chapter_id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY COMMENT '章节 ID',
  course_id BIGINT NOT NULL COMMENT '课程 ID',
  chapter_title VARCHAR(200) NOT NULL COMMENT '章节标题',
  chapter_order INT NOT NULL COMMENT '章节顺序',
  duration_minutes INT DEFAULT 0 COMMENT '时长 (分钟)',
  is_free TINYINT DEFAULT 0 COMMENT '是否免费试听',
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  INDEX idx_course_id (course_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='课程章节表';

-- ------------------------------
-- 日常开发场景 - 医疗系统
-- ------------------------------

-- 创建患者表
DROP TABLE IF EXISTS case_153_med_patients;
CREATE TABLE case_153_med_patients (
  patient_id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY COMMENT '患者 ID',
  patient_no VARCHAR(50) NOT NULL COMMENT '患者编号',
  name VARCHAR(50) NOT NULL COMMENT '姓名',
  id_card VARCHAR(20) COMMENT '身份证号',
  gender TINYINT DEFAULT 0 COMMENT '性别：0-未知，1-男，2-女',
  birthday DATE COMMENT '出生日期',
  phone VARCHAR(20) COMMENT '联系电话',
  address VARCHAR(200) COMMENT '联系地址',
  emergency_contact VARCHAR(50) COMMENT '紧急联系人',
  emergency_phone VARCHAR(20) COMMENT '紧急联系电话',
  blood_type VARCHAR(5) COMMENT '血型',
  allergy_info TEXT COMMENT '过敏信息',
  medical_history TEXT COMMENT '既往病史',
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  UNIQUE KEY uk_patient_no (patient_no),
  UNIQUE KEY uk_id_card (id_card),
  INDEX idx_name (name),
  INDEX idx_phone (phone)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='患者表';

-- 创建医生表
DROP TABLE IF EXISTS case_153_med_doctors;
CREATE TABLE case_153_med_doctors (
  doctor_id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY COMMENT '医生 ID',
  doctor_no VARCHAR(50) NOT NULL COMMENT '医生编号',
  name VARCHAR(50) NOT NULL COMMENT '姓名',
  title VARCHAR(50) COMMENT '职称',
  department_id INT COMMENT '科室 ID',
  specialty VARCHAR(200) COMMENT '擅长',
  phone VARCHAR(20) COMMENT '联系电话',
  email VARCHAR(100) COMMENT '邮箱',
  status TINYINT DEFAULT 1 COMMENT '状态：0-离职，1-在职，2-停诊',
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  UNIQUE KEY uk_doctor_no (doctor_no),
  INDEX idx_department_id (department_id),
  INDEX idx_status (status)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='医生表';

-- 创建挂号表
DROP TABLE IF EXISTS case_153_med_registrations;
CREATE TABLE case_153_med_registrations (
  reg_id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY COMMENT '挂号 ID',
  reg_no VARCHAR(50) NOT NULL COMMENT '挂号编号',
  patient_id BIGINT NOT NULL COMMENT '患者 ID',
  doctor_id BIGINT NOT NULL COMMENT '医生 ID',
  reg_date DATE NOT NULL COMMENT '挂号日期',
  reg_time_slot VARCHAR(20) NOT NULL COMMENT '时间段',
  reg_type TINYINT DEFAULT 1 COMMENT '类型：1-普通，2-专家，3-特需',
  reg_status TINYINT DEFAULT 1 COMMENT '状态：1-已挂号，2-已就诊，3-已取消，4-已爽约',
  reg_fee DECIMAL(10,2) DEFAULT 0 COMMENT '挂号费',
  visit_room VARCHAR(20) COMMENT '诊室',
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  UNIQUE KEY uk_reg_no (reg_no),
  INDEX idx_patient_id (patient_id),
  INDEX idx_doctor_id (doctor_id),
  INDEX idx_reg_date (reg_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='挂号表';

-- 创建病历表
DROP TABLE IF EXISTS case_153_med_medical_records;
CREATE TABLE case_153_med_medical_records (
  record_id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY COMMENT '病历 ID',
  reg_id BIGINT NOT NULL COMMENT '挂号 ID',
  patient_id BIGINT NOT NULL COMMENT '患者 ID',
  doctor_id BIGINT NOT NULL COMMENT '医生 ID',
  chief_complaint TEXT COMMENT '主诉',
  present_illness TEXT COMMENT '现病史',
  diagnosis TEXT COMMENT '诊断',
  treatment_plan TEXT COMMENT '治疗方案',
  prescription TEXT COMMENT '处方',
  advice TEXT COMMENT '医嘱',
  visit_time DATETIME COMMENT '就诊时间',
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  INDEX idx_reg_id (reg_id),
  INDEX idx_patient_id (patient_id),
  INDEX idx_doctor_id (doctor_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='病历表';

-- ------------------------------
-- 日常开发场景 - 酒店管理系统
-- ------------------------------

-- 创建房型表
DROP TABLE IF EXISTS case_154_hotel_room_types;
CREATE TABLE case_154_hotel_room_types (
  type_id INT UNSIGNED AUTO_INCREMENT PRIMARY KEY COMMENT '房型 ID',
  type_name VARCHAR(50) NOT NULL COMMENT '房型名称',
  type_code VARCHAR(20) NOT NULL COMMENT '房型编码',
  bed_type TINYINT DEFAULT 1 COMMENT '床型：1-大床，2-双床，3-套房',
  max_occupancy INT DEFAULT 2 COMMENT '最大入住人数',
  area_sqm INT COMMENT '面积 (平米)',
  floor_min INT COMMENT '最低楼层',
  floor_max INT COMMENT '最高楼层',
  base_price DECIMAL(10,2) NOT NULL COMMENT '基础价格',
  weekend_price DECIMAL(10,2) COMMENT '周末价格',
  holiday_price DECIMAL(10,2) COMMENT '节假日价格',
  amenities JSON COMMENT '设施',
  description TEXT COMMENT '描述',
  status TINYINT DEFAULT 1 COMMENT '状态：0-停售，1-在售',
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  UNIQUE KEY uk_type_code (type_code),
  INDEX idx_status (status)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='房型表';

-- 创建房间表
DROP TABLE IF EXISTS case_154_hotel_rooms;
CREATE TABLE case_154_hotel_rooms (
  room_id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY COMMENT '房间 ID',
  room_no VARCHAR(10) NOT NULL COMMENT '房间号',
  type_id INT NOT NULL COMMENT '房型 ID',
  floor INT NOT NULL COMMENT '楼层',
  status TINYINT DEFAULT 1 COMMENT '状态：0-维修，1-空闲，2-入住，3-清洁中',
  price_override DECIMAL(10,2) COMMENT '覆盖价格',
  remark VARCHAR(200) COMMENT '备注',
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  UNIQUE KEY uk_room_no (room_no),
  INDEX idx_type_id (type_id),
  INDEX idx_status (status)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='房间表';

-- 创建订单表
DROP TABLE IF EXISTS case_154_hotel_orders;
CREATE TABLE case_154_hotel_orders (
  order_id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY COMMENT '订单 ID',
  order_no VARCHAR(50) NOT NULL COMMENT '订单编号',
  user_id BIGINT NOT NULL COMMENT '用户 ID',
  room_id BIGINT NOT NULL COMMENT '房间 ID',
  check_in_date DATE NOT NULL COMMENT '入住日期',
  check_out_date DATE NOT NULL COMMENT '退房日期',
  nights INT NOT NULL COMMENT '住宿晚数',
  guests INT DEFAULT 1 COMMENT '入住人数',
  room_price DECIMAL(10,2) NOT NULL COMMENT '房间单价',
  total_amount DECIMAL(10,2) NOT NULL COMMENT '订单总额',
  paid_amount DECIMAL(10,2) DEFAULT 0 COMMENT '已付金额',
  order_status TINYINT DEFAULT 1 COMMENT '状态：0-已取消，1-待支付，2-已支付，3-已入住，4-已完成',
  payment_type TINYINT COMMENT '支付方式：1-微信，2-支付宝，3-银行卡',
  guest_name VARCHAR(50) COMMENT '入住人姓名',
  guest_phone VARCHAR(20) COMMENT '入住人电话',
  remark VARCHAR(500) COMMENT '备注',
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  UNIQUE KEY uk_order_no (order_no),
  INDEX idx_user_id (user_id),
  INDEX idx_room_id (room_id),
  INDEX idx_check_in_date (check_in_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='酒店订单表';

-- ------------------------------
-- 日常开发场景 - 餐饮系统
-- ------------------------------

-- 创建菜品分类表
DROP TABLE IF EXISTS case_155_rest_categories;
CREATE TABLE case_155_rest_categories (
  category_id INT UNSIGNED AUTO_INCREMENT PRIMARY KEY COMMENT '分类 ID',
  parent_id INT DEFAULT 0 COMMENT '父分类 ID',
  category_name VARCHAR(50) NOT NULL COMMENT '分类名称',
  category_icon VARCHAR(100) COMMENT '分类图标',
  sort_order INT DEFAULT 0 COMMENT '排序',
  status TINYINT DEFAULT 1 COMMENT '状态：0-禁用，1-启用',
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  INDEX idx_parent_id (parent_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='菜品分类表';

-- 创建订单表
DROP TABLE IF EXISTS case_155_rest_orders;
CREATE TABLE case_155_rest_orders (
  order_id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY COMMENT '订单 ID',
  order_no VARCHAR(50) NOT NULL COMMENT '订单编号',
  table_no VARCHAR(20) COMMENT '桌号',
  user_id BIGINT COMMENT '用户 ID',
  order_type TINYINT DEFAULT 1 COMMENT '类型：1-堂食，2-外卖，3-自取',
  order_status TINYINT DEFAULT 1 COMMENT '状态：0-已取消，1-待接单，2-制作中，3-已完成',
  subtotal DECIMAL(10,2) NOT NULL COMMENT '菜品总额',
  discount_amount DECIMAL(10,2) DEFAULT 0 COMMENT '优惠金额',
  delivery_fee DECIMAL(10,2) DEFAULT 0 COMMENT '配送费',
  total_amount DECIMAL(10,2) NOT NULL COMMENT '订单总额',
  payment_status TINYINT DEFAULT 0 COMMENT '支付状态：0-未支付，1-已支付',
  payment_time DATETIME COMMENT '支付时间',
  remark VARCHAR(200) COMMENT '备注',
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  UNIQUE KEY uk_order_no (order_no),
  INDEX idx_table_no (table_no),
  INDEX idx_order_status (order_status)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='餐饮订单表';

-- 创建订单明细表
DROP TABLE IF EXISTS case_155_rest_order_items;
CREATE TABLE case_155_rest_order_items (
  item_id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY COMMENT '明细 ID',
  order_id BIGINT NOT NULL COMMENT '订单 ID',
  dish_id BIGINT NOT NULL COMMENT '菜品 ID',
  dish_name VARCHAR(100) NOT NULL COMMENT '菜品名称（快照）',
  unit_price DECIMAL(10,2) NOT NULL COMMENT '单价',
  quantity INT NOT NULL COMMENT '数量',
  subtotal DECIMAL(12,2) NOT NULL COMMENT '小计',
  remark VARCHAR(100) COMMENT '备注',
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  INDEX idx_order_id (order_id),
  INDEX idx_dish_id (dish_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='订单明细表';

DROP TABLE IF EXISTS case_156_orders_parent;
CREATE TABLE case_156_orders_parent (
  tenant_id BIGINT UNSIGNED NOT NULL COMMENT '租户 ID',
  order_no VARCHAR(64) NOT NULL COMMENT '业务订单号',
  status TINYINT NOT NULL DEFAULT 0 COMMENT '状态',
  created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) COMMENT '创建时间',
  PRIMARY KEY (tenant_id, order_no),
  INDEX idx_status_created (status, created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='复合主键父表';

DROP TABLE IF EXISTS case_156_orders_child;
CREATE TABLE case_156_orders_child (
  item_id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY COMMENT '明细 ID',
  tenant_id BIGINT UNSIGNED NOT NULL COMMENT '租户 ID',
  order_no VARCHAR(64) NOT NULL COMMENT '业务订单号',
  sku_code VARCHAR(64) NOT NULL COMMENT 'SKU 编码',
  qty INT UNSIGNED NOT NULL DEFAULT 1 COMMENT '数量',
  unit_price DECIMAL(18,4) NOT NULL COMMENT '单价',
  amount DECIMAL(18,4) GENERATED ALWAYS AS (qty * unit_price) STORED COMMENT '金额',
  created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) COMMENT '创建时间',
  UNIQUE KEY uk_order_sku (tenant_id, order_no, sku_code),
  INDEX idx_tenant_order (tenant_id, order_no),
  CONSTRAINT fk_case_156_order FOREIGN KEY (tenant_id, order_no) REFERENCES case_156_orders_parent (tenant_id, order_no) ON DELETE CASCADE ON UPDATE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='复合外键子表';

DROP TABLE IF EXISTS case_157_json_generated_index;
CREATE TABLE case_157_json_generated_index (
  id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY COMMENT '主键',
  payload JSON NOT NULL COMMENT 'JSON 数据',
  tags JSON COMMENT '标签',
  created_at TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
  updated_at TIMESTAMP(3) NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '更新时间',
  INDEX idx_created_at (created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='JSON 与生成列混合';

DROP TABLE IF EXISTS case_158_temporal_mix;
CREATE TABLE case_158_temporal_mix (
  id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY COMMENT '主键',
  d DATE NOT NULL COMMENT '日期',
  t TIME(6) NOT NULL COMMENT '时间',
  dt DATETIME(6) NOT NULL COMMENT '日期时间',
  ts TIMESTAMP(6) NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6) COMMENT '时间戳',
  y YEAR NOT NULL DEFAULT 2026 COMMENT '年份',
  period_label CHAR(7) NOT NULL DEFAULT '2026-01' COMMENT '期间',
  INDEX idx_dt (dt),
  INDEX idx_y_d (y, d)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='时间类型组合';

DROP TABLE IF EXISTS case_159_text_blob_mix;
CREATE TABLE case_159_text_blob_mix (
  id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY COMMENT '主键',
  title VARCHAR(200) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '标题',
  summary TEXT CHARACTER SET utf8mb4 COMMENT '摘要',
  content LONGTEXT CHARACTER SET utf8mb4 COMMENT '正文',
  attachment_name VARCHAR(255) CHARACTER SET latin1 COLLATE latin1_swedish_ci COMMENT '附件名',
  attachment BLOB COMMENT '附件',
  hash_code BINARY(16) COMMENT '哈希',
  is_deleted TINYINT(1) NOT NULL DEFAULT 0 COMMENT '是否删除',
  UNIQUE KEY uk_title_prefix (title(100)),
  INDEX idx_attachment_name (attachment_name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='文本与二进制混合';

DROP TABLE IF EXISTS case_160_numeric_boundary;
CREATE TABLE case_160_numeric_boundary (
  id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY COMMENT '主键',
  tiny_signed TINYINT NOT NULL COMMENT 'tinyint',
  tiny_unsigned TINYINT UNSIGNED NOT NULL COMMENT 'tinyint unsigned',
  int_signed INT NOT NULL COMMENT 'int',
  int_unsigned INT UNSIGNED NOT NULL COMMENT 'int unsigned',
  big_signed BIGINT NOT NULL COMMENT 'bigint',
  big_unsigned BIGINT UNSIGNED NOT NULL COMMENT 'bigint unsigned',
  dec_low DECIMAL(10,0) NOT NULL DEFAULT 0 COMMENT '整型 decimal',
  dec_high DECIMAL(65,30) NOT NULL COMMENT '高精度 decimal',
  fl FLOAT COMMENT 'float',
  db DOUBLE COMMENT 'double',
  ratio NUMERIC(20,10) NOT NULL DEFAULT 0.0000000000 COMMENT 'numeric',
  serial_no BIGINT UNSIGNED NOT NULL COMMENT '业务序号',
  UNIQUE KEY uk_serial_no (serial_no),
  INDEX idx_dec_low (dec_low)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='数值边界类型组合';

-- 创建临时表语法测试（会话级对象）
drop table if exists  case_161_temp_orders_stage;
CREATE TEMPORARY TABLE case_161_temp_orders_stage (
  id BIGINT NOT NULL,
  biz_no VARCHAR(64) NOT NULL,
  payload JSON,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (id)
) COMMENT='临时表测试';

-- 创建带表级 AUTO_INCREMENT 起始值的表
DROP TABLE IF EXISTS case_162_auto_inc_option;
CREATE TABLE case_162_auto_inc_option (
  id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
  name VARCHAR(100) NOT NULL,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB AUTO_INCREMENT=100000 DEFAULT CHARSET=utf8mb4 COMMENT='表级自增起始值测试';

-- 创建外键动作补齐测试表（RESTRICT / NO ACTION）
DROP TABLE IF EXISTS case_163_fk_action_child;
DROP TABLE IF EXISTS case_163_fk_action_parent;
CREATE TABLE case_163_fk_action_parent (
  id BIGINT NOT NULL PRIMARY KEY,
  code VARCHAR(32) NOT NULL UNIQUE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='外键动作父表';

CREATE TABLE case_163_fk_action_child (
  id BIGINT NOT NULL PRIMARY KEY,
  parent_id BIGINT,
  parent_code VARCHAR(32),
  CONSTRAINT fk_case_163_parent_id
    FOREIGN KEY (parent_id) REFERENCES case_163_fk_action_parent(id)
    ON DELETE RESTRICT
    ON UPDATE RESTRICT,
  CONSTRAINT fk_case_163_parent_code
    FOREIGN KEY (parent_code) REFERENCES case_163_fk_action_parent(code)
    ON DELETE NO ACTION
    ON UPDATE RESTRICT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='外键动作子表';

-- 创建自关联外键表（组织树）
DROP TABLE IF EXISTS case_164_org_tree;
CREATE TABLE case_164_org_tree (
  id BIGINT NOT NULL PRIMARY KEY,
  parent_id BIGINT NULL,
  org_name VARCHAR(100) NOT NULL,
  org_level TINYINT NOT NULL DEFAULT 1,
  sort_no INT NOT NULL DEFAULT 0,
  CONSTRAINT fk_case_164_parent
    FOREIGN KEY (parent_id) REFERENCES case_164_org_tree(id)
    ON DELETE RESTRICT
    ON UPDATE RESTRICT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='自关联组织树';

-- 创建 CHECK 约束测试表（兼容 MySQL 5.7+）
-- 注：MySQL 5.7 解析但忽略 CHECK 约束，8.0.16+ 才真正执行
-- ENFORCED / NOT ENFORCED 是 8.0.16+ 特性，这里使用兼容语法
DROP TABLE IF EXISTS case_165_check_enforced;
CREATE TABLE case_165_check_enforced (
  id BIGINT NOT NULL PRIMARY KEY,
  amount DECIMAL(12,2) NOT NULL,
  status TINYINT NOT NULL,
  CONSTRAINT chk_case_165_amount_nonneg CHECK (amount >= 0),
  CONSTRAINT chk_case_165_status CHECK (status IN (0, 1, 2))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='CHECK 约束测试';

-- 创建 ENGINE=MEMORY + ROW_FORMAT + COMMENT 组合表
DROP TABLE IF EXISTS case_166_memory_rowfmt;
CREATE TABLE case_166_memory_rowfmt (
  id INT NOT NULL PRIMARY KEY,
  session_key VARCHAR(64) NOT NULL,
  session_value VARCHAR(255),
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  KEY idx_session_key (session_key)
) ENGINE=MEMORY ROW_FORMAT=FIXED COMMENT='MEMORY 引擎组合选项测试';

-- 创建 UNION / INSERT_METHOD 测试表（MRG_MYISAM）
DROP TABLE IF EXISTS case_167_merge;
CREATE TABLE case_167_merge (
  id INT NOT NULL,
  tenant_id INT NOT NULL,
  biz_no VARCHAR(64) NOT NULL,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (id),
  KEY idx_tenant_biz (tenant_id, biz_no)
) ENGINE=MyISAM COMMENT='MERGE 源表 1';

-- 创建 PRIMARY KEY ... USING BTREE 测试表（修复主键丢失问题）
-- 典型案例：MySQL 8.0 默认在主键后添加 USING BTREE，迁移时需确保主键不丢失
-- 使用 MySQL 5.7+ 兼容的排序规则：utf8mb4_general_ci（5.7 和 8.0 都支持）
DROP TABLE IF EXISTS case_168_merge;
CREATE TABLE `case_168_merge` (
  `normalize_id` int NOT NULL AUTO_INCREMENT,
  `front_name` varchar(255) DEFAULT NULL,
  `queen_name` varchar(255) DEFAULT NULL,
  `usestatus` int DEFAULT '0',
  `type` int DEFAULT NULL,
  `retain` int DEFAULT NULL,
  `create_by` int DEFAULT NULL,
  `create_time` datetime DEFAULT NULL,
  `update_by` int DEFAULT NULL,
  `update_time` datetime DEFAULT NULL,
  PRIMARY KEY (`normalize_id`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=12506 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci ROW_FORMAT=DYNAMIC;


-- ============================================================
-- case_169 ~ case_193：类型全长度转换能力测试（每种类型一张表）
-- 覆盖常用类型从最小长度到最大长度的全部取值：
--   CHAR(1..255)、VARCHAR(1..355 行宽内全覆盖 + 长尾边界采样至 65532)、
--   BINARY(1..255)、VARBINARY(1..355 + 长尾边界采样至 65532)、
--   TINYINT/SMALLINT/MEDIUMINT/INT/BIGINT 显示宽度(1..255)、
--   DECIMAL 精度 M(1..65) 与标度 D(0..30)、FLOAT/DOUBLE (M,D) 语法(1..65)、
--   BIT(1..64)、TIME/DATETIME/TIMESTAMP 小数秒精度 fsp(0..6)
-- 说明：
--   1) CHAR/VARCHAR 全长度表使用 latin1（1字节/字符），否则超出 MySQL 65,535 字节行宽限制
--   2) VARCHAR/VARBINARY 超过行宽限制后无法全量穷举，按 2 的幂与常见边界值分表采样
--   3) 长度从 1 开始：PostgreSQL 的 CHAR(n)/VARCHAR(n) 要求 n >= 1，MySQL 的 0 长度无法对应
--   4) 四张全覆盖宽表（169/170/175/176）使用 MyISAM 引擎：InnoDB 16K 页另有 ~8126 字节
--      行宽限制（定长列无法溢出外部页、大量变长列溢出指针开销仍超限），MyISAM 只受 65,535 限制
-- ============================================================
-- 创建 CHAR 全长度测试表：CHAR(1) ~ CHAR(255)，每个长度一个字段，测试全长度转换
-- 使用 latin1（1字节/字符）以满足 MySQL 65,535 字节行宽限制；CHAR(n) -> CHAR(n)
-- 引擎使用 MyISAM：InnoDB 16K 页有 ~8126 字节行宽限制，定长 CHAR 无法溢出到外部页
DROP TABLE IF EXISTS case_169_char_full;
CREATE TABLE case_169_char_full (
  col_char_001 char(1),
  col_char_002 char(2),
  col_char_003 char(3),
  col_char_004 char(4),
  col_char_005 char(5),
  col_char_006 char(6),
  col_char_007 char(7),
  col_char_008 char(8),
  col_char_009 char(9),
  col_char_010 char(10),
  col_char_011 char(11),
  col_char_012 char(12),
  col_char_013 char(13),
  col_char_014 char(14),
  col_char_015 char(15),
  col_char_016 char(16),
  col_char_017 char(17),
  col_char_018 char(18),
  col_char_019 char(19),
  col_char_020 char(20),
  col_char_021 char(21),
  col_char_022 char(22),
  col_char_023 char(23),
  col_char_024 char(24),
  col_char_025 char(25),
  col_char_026 char(26),
  col_char_027 char(27),
  col_char_028 char(28),
  col_char_029 char(29),
  col_char_030 char(30),
  col_char_031 char(31),
  col_char_032 char(32),
  col_char_033 char(33),
  col_char_034 char(34),
  col_char_035 char(35),
  col_char_036 char(36),
  col_char_037 char(37),
  col_char_038 char(38),
  col_char_039 char(39),
  col_char_040 char(40),
  col_char_041 char(41),
  col_char_042 char(42),
  col_char_043 char(43),
  col_char_044 char(44),
  col_char_045 char(45),
  col_char_046 char(46),
  col_char_047 char(47),
  col_char_048 char(48),
  col_char_049 char(49),
  col_char_050 char(50),
  col_char_051 char(51),
  col_char_052 char(52),
  col_char_053 char(53),
  col_char_054 char(54),
  col_char_055 char(55),
  col_char_056 char(56),
  col_char_057 char(57),
  col_char_058 char(58),
  col_char_059 char(59),
  col_char_060 char(60),
  col_char_061 char(61),
  col_char_062 char(62),
  col_char_063 char(63),
  col_char_064 char(64),
  col_char_065 char(65),
  col_char_066 char(66),
  col_char_067 char(67),
  col_char_068 char(68),
  col_char_069 char(69),
  col_char_070 char(70),
  col_char_071 char(71),
  col_char_072 char(72),
  col_char_073 char(73),
  col_char_074 char(74),
  col_char_075 char(75),
  col_char_076 char(76),
  col_char_077 char(77),
  col_char_078 char(78),
  col_char_079 char(79),
  col_char_080 char(80),
  col_char_081 char(81),
  col_char_082 char(82),
  col_char_083 char(83),
  col_char_084 char(84),
  col_char_085 char(85),
  col_char_086 char(86),
  col_char_087 char(87),
  col_char_088 char(88),
  col_char_089 char(89),
  col_char_090 char(90),
  col_char_091 char(91),
  col_char_092 char(92),
  col_char_093 char(93),
  col_char_094 char(94),
  col_char_095 char(95),
  col_char_096 char(96),
  col_char_097 char(97),
  col_char_098 char(98),
  col_char_099 char(99),
  col_char_100 char(100),
  col_char_101 char(101),
  col_char_102 char(102),
  col_char_103 char(103),
  col_char_104 char(104),
  col_char_105 char(105),
  col_char_106 char(106),
  col_char_107 char(107),
  col_char_108 char(108),
  col_char_109 char(109),
  col_char_110 char(110),
  col_char_111 char(111),
  col_char_112 char(112),
  col_char_113 char(113),
  col_char_114 char(114),
  col_char_115 char(115),
  col_char_116 char(116),
  col_char_117 char(117),
  col_char_118 char(118),
  col_char_119 char(119),
  col_char_120 char(120),
  col_char_121 char(121),
  col_char_122 char(122),
  col_char_123 char(123),
  col_char_124 char(124),
  col_char_125 char(125),
  col_char_126 char(126),
  col_char_127 char(127),
  col_char_128 char(128),
  col_char_129 char(129),
  col_char_130 char(130),
  col_char_131 char(131),
  col_char_132 char(132),
  col_char_133 char(133),
  col_char_134 char(134),
  col_char_135 char(135),
  col_char_136 char(136),
  col_char_137 char(137),
  col_char_138 char(138),
  col_char_139 char(139),
  col_char_140 char(140),
  col_char_141 char(141),
  col_char_142 char(142),
  col_char_143 char(143),
  col_char_144 char(144),
  col_char_145 char(145),
  col_char_146 char(146),
  col_char_147 char(147),
  col_char_148 char(148),
  col_char_149 char(149),
  col_char_150 char(150),
  col_char_151 char(151),
  col_char_152 char(152),
  col_char_153 char(153),
  col_char_154 char(154),
  col_char_155 char(155),
  col_char_156 char(156),
  col_char_157 char(157),
  col_char_158 char(158),
  col_char_159 char(159),
  col_char_160 char(160),
  col_char_161 char(161),
  col_char_162 char(162),
  col_char_163 char(163),
  col_char_164 char(164),
  col_char_165 char(165),
  col_char_166 char(166),
  col_char_167 char(167),
  col_char_168 char(168),
  col_char_169 char(169),
  col_char_170 char(170),
  col_char_171 char(171),
  col_char_172 char(172),
  col_char_173 char(173),
  col_char_174 char(174),
  col_char_175 char(175),
  col_char_176 char(176),
  col_char_177 char(177),
  col_char_178 char(178),
  col_char_179 char(179),
  col_char_180 char(180),
  col_char_181 char(181),
  col_char_182 char(182),
  col_char_183 char(183),
  col_char_184 char(184),
  col_char_185 char(185),
  col_char_186 char(186),
  col_char_187 char(187),
  col_char_188 char(188),
  col_char_189 char(189),
  col_char_190 char(190),
  col_char_191 char(191),
  col_char_192 char(192),
  col_char_193 char(193),
  col_char_194 char(194),
  col_char_195 char(195),
  col_char_196 char(196),
  col_char_197 char(197),
  col_char_198 char(198),
  col_char_199 char(199),
  col_char_200 char(200),
  col_char_201 char(201),
  col_char_202 char(202),
  col_char_203 char(203),
  col_char_204 char(204),
  col_char_205 char(205),
  col_char_206 char(206),
  col_char_207 char(207),
  col_char_208 char(208),
  col_char_209 char(209),
  col_char_210 char(210),
  col_char_211 char(211),
  col_char_212 char(212),
  col_char_213 char(213),
  col_char_214 char(214),
  col_char_215 char(215),
  col_char_216 char(216),
  col_char_217 char(217),
  col_char_218 char(218),
  col_char_219 char(219),
  col_char_220 char(220),
  col_char_221 char(221),
  col_char_222 char(222),
  col_char_223 char(223),
  col_char_224 char(224),
  col_char_225 char(225),
  col_char_226 char(226),
  col_char_227 char(227),
  col_char_228 char(228),
  col_char_229 char(229),
  col_char_230 char(230),
  col_char_231 char(231),
  col_char_232 char(232),
  col_char_233 char(233),
  col_char_234 char(234),
  col_char_235 char(235),
  col_char_236 char(236),
  col_char_237 char(237),
  col_char_238 char(238),
  col_char_239 char(239),
  col_char_240 char(240),
  col_char_241 char(241),
  col_char_242 char(242),
  col_char_243 char(243),
  col_char_244 char(244),
  col_char_245 char(245),
  col_char_246 char(246),
  col_char_247 char(247),
  col_char_248 char(248),
  col_char_249 char(249),
  col_char_250 char(250),
  col_char_251 char(251),
  col_char_252 char(252),
  col_char_253 char(253),
  col_char_254 char(254),
  col_char_255 char(255)
) ENGINE=MyISAM DEFAULT CHARSET=latin1;

-- 创建 VARCHAR 行宽内全长度测试表：VARCHAR(1) ~ VARCHAR(355)，每个长度一个字段
-- 355 为 MySQL 65,535 字节行宽限制内可容纳的最大连续覆盖范围（latin1）；VARCHAR(n) -> VARCHAR(n)
-- 引擎使用 MyISAM：InnoDB 16K 页有 ~8126 字节行宽限制，大量 VARCHAR 列的溢出指针开销仍会超限
DROP TABLE IF EXISTS case_170_varchar_full;
CREATE TABLE case_170_varchar_full (
  col_varchar_001 varchar(1),
  col_varchar_002 varchar(2),
  col_varchar_003 varchar(3),
  col_varchar_004 varchar(4),
  col_varchar_005 varchar(5),
  col_varchar_006 varchar(6),
  col_varchar_007 varchar(7),
  col_varchar_008 varchar(8),
  col_varchar_009 varchar(9),
  col_varchar_010 varchar(10),
  col_varchar_011 varchar(11),
  col_varchar_012 varchar(12),
  col_varchar_013 varchar(13),
  col_varchar_014 varchar(14),
  col_varchar_015 varchar(15),
  col_varchar_016 varchar(16),
  col_varchar_017 varchar(17),
  col_varchar_018 varchar(18),
  col_varchar_019 varchar(19),
  col_varchar_020 varchar(20),
  col_varchar_021 varchar(21),
  col_varchar_022 varchar(22),
  col_varchar_023 varchar(23),
  col_varchar_024 varchar(24),
  col_varchar_025 varchar(25),
  col_varchar_026 varchar(26),
  col_varchar_027 varchar(27),
  col_varchar_028 varchar(28),
  col_varchar_029 varchar(29),
  col_varchar_030 varchar(30),
  col_varchar_031 varchar(31),
  col_varchar_032 varchar(32),
  col_varchar_033 varchar(33),
  col_varchar_034 varchar(34),
  col_varchar_035 varchar(35),
  col_varchar_036 varchar(36),
  col_varchar_037 varchar(37),
  col_varchar_038 varchar(38),
  col_varchar_039 varchar(39),
  col_varchar_040 varchar(40),
  col_varchar_041 varchar(41),
  col_varchar_042 varchar(42),
  col_varchar_043 varchar(43),
  col_varchar_044 varchar(44),
  col_varchar_045 varchar(45),
  col_varchar_046 varchar(46),
  col_varchar_047 varchar(47),
  col_varchar_048 varchar(48),
  col_varchar_049 varchar(49),
  col_varchar_050 varchar(50),
  col_varchar_051 varchar(51),
  col_varchar_052 varchar(52),
  col_varchar_053 varchar(53),
  col_varchar_054 varchar(54),
  col_varchar_055 varchar(55),
  col_varchar_056 varchar(56),
  col_varchar_057 varchar(57),
  col_varchar_058 varchar(58),
  col_varchar_059 varchar(59),
  col_varchar_060 varchar(60),
  col_varchar_061 varchar(61),
  col_varchar_062 varchar(62),
  col_varchar_063 varchar(63),
  col_varchar_064 varchar(64),
  col_varchar_065 varchar(65),
  col_varchar_066 varchar(66),
  col_varchar_067 varchar(67),
  col_varchar_068 varchar(68),
  col_varchar_069 varchar(69),
  col_varchar_070 varchar(70),
  col_varchar_071 varchar(71),
  col_varchar_072 varchar(72),
  col_varchar_073 varchar(73),
  col_varchar_074 varchar(74),
  col_varchar_075 varchar(75),
  col_varchar_076 varchar(76),
  col_varchar_077 varchar(77),
  col_varchar_078 varchar(78),
  col_varchar_079 varchar(79),
  col_varchar_080 varchar(80),
  col_varchar_081 varchar(81),
  col_varchar_082 varchar(82),
  col_varchar_083 varchar(83),
  col_varchar_084 varchar(84),
  col_varchar_085 varchar(85),
  col_varchar_086 varchar(86),
  col_varchar_087 varchar(87),
  col_varchar_088 varchar(88),
  col_varchar_089 varchar(89),
  col_varchar_090 varchar(90),
  col_varchar_091 varchar(91),
  col_varchar_092 varchar(92),
  col_varchar_093 varchar(93),
  col_varchar_094 varchar(94),
  col_varchar_095 varchar(95),
  col_varchar_096 varchar(96),
  col_varchar_097 varchar(97),
  col_varchar_098 varchar(98),
  col_varchar_099 varchar(99),
  col_varchar_100 varchar(100),
  col_varchar_101 varchar(101),
  col_varchar_102 varchar(102),
  col_varchar_103 varchar(103),
  col_varchar_104 varchar(104),
  col_varchar_105 varchar(105),
  col_varchar_106 varchar(106),
  col_varchar_107 varchar(107),
  col_varchar_108 varchar(108),
  col_varchar_109 varchar(109),
  col_varchar_110 varchar(110),
  col_varchar_111 varchar(111),
  col_varchar_112 varchar(112),
  col_varchar_113 varchar(113),
  col_varchar_114 varchar(114),
  col_varchar_115 varchar(115),
  col_varchar_116 varchar(116),
  col_varchar_117 varchar(117),
  col_varchar_118 varchar(118),
  col_varchar_119 varchar(119),
  col_varchar_120 varchar(120),
  col_varchar_121 varchar(121),
  col_varchar_122 varchar(122),
  col_varchar_123 varchar(123),
  col_varchar_124 varchar(124),
  col_varchar_125 varchar(125),
  col_varchar_126 varchar(126),
  col_varchar_127 varchar(127),
  col_varchar_128 varchar(128),
  col_varchar_129 varchar(129),
  col_varchar_130 varchar(130),
  col_varchar_131 varchar(131),
  col_varchar_132 varchar(132),
  col_varchar_133 varchar(133),
  col_varchar_134 varchar(134),
  col_varchar_135 varchar(135),
  col_varchar_136 varchar(136),
  col_varchar_137 varchar(137),
  col_varchar_138 varchar(138),
  col_varchar_139 varchar(139),
  col_varchar_140 varchar(140),
  col_varchar_141 varchar(141),
  col_varchar_142 varchar(142),
  col_varchar_143 varchar(143),
  col_varchar_144 varchar(144),
  col_varchar_145 varchar(145),
  col_varchar_146 varchar(146),
  col_varchar_147 varchar(147),
  col_varchar_148 varchar(148),
  col_varchar_149 varchar(149),
  col_varchar_150 varchar(150),
  col_varchar_151 varchar(151),
  col_varchar_152 varchar(152),
  col_varchar_153 varchar(153),
  col_varchar_154 varchar(154),
  col_varchar_155 varchar(155),
  col_varchar_156 varchar(156),
  col_varchar_157 varchar(157),
  col_varchar_158 varchar(158),
  col_varchar_159 varchar(159),
  col_varchar_160 varchar(160),
  col_varchar_161 varchar(161),
  col_varchar_162 varchar(162),
  col_varchar_163 varchar(163),
  col_varchar_164 varchar(164),
  col_varchar_165 varchar(165),
  col_varchar_166 varchar(166),
  col_varchar_167 varchar(167),
  col_varchar_168 varchar(168),
  col_varchar_169 varchar(169),
  col_varchar_170 varchar(170),
  col_varchar_171 varchar(171),
  col_varchar_172 varchar(172),
  col_varchar_173 varchar(173),
  col_varchar_174 varchar(174),
  col_varchar_175 varchar(175),
  col_varchar_176 varchar(176),
  col_varchar_177 varchar(177),
  col_varchar_178 varchar(178),
  col_varchar_179 varchar(179),
  col_varchar_180 varchar(180),
  col_varchar_181 varchar(181),
  col_varchar_182 varchar(182),
  col_varchar_183 varchar(183),
  col_varchar_184 varchar(184),
  col_varchar_185 varchar(185),
  col_varchar_186 varchar(186),
  col_varchar_187 varchar(187),
  col_varchar_188 varchar(188),
  col_varchar_189 varchar(189),
  col_varchar_190 varchar(190),
  col_varchar_191 varchar(191),
  col_varchar_192 varchar(192),
  col_varchar_193 varchar(193),
  col_varchar_194 varchar(194),
  col_varchar_195 varchar(195),
  col_varchar_196 varchar(196),
  col_varchar_197 varchar(197),
  col_varchar_198 varchar(198),
  col_varchar_199 varchar(199),
  col_varchar_200 varchar(200),
  col_varchar_201 varchar(201),
  col_varchar_202 varchar(202),
  col_varchar_203 varchar(203),
  col_varchar_204 varchar(204),
  col_varchar_205 varchar(205),
  col_varchar_206 varchar(206),
  col_varchar_207 varchar(207),
  col_varchar_208 varchar(208),
  col_varchar_209 varchar(209),
  col_varchar_210 varchar(210),
  col_varchar_211 varchar(211),
  col_varchar_212 varchar(212),
  col_varchar_213 varchar(213),
  col_varchar_214 varchar(214),
  col_varchar_215 varchar(215),
  col_varchar_216 varchar(216),
  col_varchar_217 varchar(217),
  col_varchar_218 varchar(218),
  col_varchar_219 varchar(219),
  col_varchar_220 varchar(220),
  col_varchar_221 varchar(221),
  col_varchar_222 varchar(222),
  col_varchar_223 varchar(223),
  col_varchar_224 varchar(224),
  col_varchar_225 varchar(225),
  col_varchar_226 varchar(226),
  col_varchar_227 varchar(227),
  col_varchar_228 varchar(228),
  col_varchar_229 varchar(229),
  col_varchar_230 varchar(230),
  col_varchar_231 varchar(231),
  col_varchar_232 varchar(232),
  col_varchar_233 varchar(233),
  col_varchar_234 varchar(234),
  col_varchar_235 varchar(235),
  col_varchar_236 varchar(236),
  col_varchar_237 varchar(237),
  col_varchar_238 varchar(238),
  col_varchar_239 varchar(239),
  col_varchar_240 varchar(240),
  col_varchar_241 varchar(241),
  col_varchar_242 varchar(242),
  col_varchar_243 varchar(243),
  col_varchar_244 varchar(244),
  col_varchar_245 varchar(245),
  col_varchar_246 varchar(246),
  col_varchar_247 varchar(247),
  col_varchar_248 varchar(248),
  col_varchar_249 varchar(249),
  col_varchar_250 varchar(250),
  col_varchar_251 varchar(251),
  col_varchar_252 varchar(252),
  col_varchar_253 varchar(253),
  col_varchar_254 varchar(254),
  col_varchar_255 varchar(255),
  col_varchar_256 varchar(256),
  col_varchar_257 varchar(257),
  col_varchar_258 varchar(258),
  col_varchar_259 varchar(259),
  col_varchar_260 varchar(260),
  col_varchar_261 varchar(261),
  col_varchar_262 varchar(262),
  col_varchar_263 varchar(263),
  col_varchar_264 varchar(264),
  col_varchar_265 varchar(265),
  col_varchar_266 varchar(266),
  col_varchar_267 varchar(267),
  col_varchar_268 varchar(268),
  col_varchar_269 varchar(269),
  col_varchar_270 varchar(270),
  col_varchar_271 varchar(271),
  col_varchar_272 varchar(272),
  col_varchar_273 varchar(273),
  col_varchar_274 varchar(274),
  col_varchar_275 varchar(275),
  col_varchar_276 varchar(276),
  col_varchar_277 varchar(277),
  col_varchar_278 varchar(278),
  col_varchar_279 varchar(279),
  col_varchar_280 varchar(280),
  col_varchar_281 varchar(281),
  col_varchar_282 varchar(282),
  col_varchar_283 varchar(283),
  col_varchar_284 varchar(284),
  col_varchar_285 varchar(285),
  col_varchar_286 varchar(286),
  col_varchar_287 varchar(287),
  col_varchar_288 varchar(288),
  col_varchar_289 varchar(289),
  col_varchar_290 varchar(290),
  col_varchar_291 varchar(291),
  col_varchar_292 varchar(292),
  col_varchar_293 varchar(293),
  col_varchar_294 varchar(294),
  col_varchar_295 varchar(295),
  col_varchar_296 varchar(296),
  col_varchar_297 varchar(297),
  col_varchar_298 varchar(298),
  col_varchar_299 varchar(299),
  col_varchar_300 varchar(300),
  col_varchar_301 varchar(301),
  col_varchar_302 varchar(302),
  col_varchar_303 varchar(303),
  col_varchar_304 varchar(304),
  col_varchar_305 varchar(305),
  col_varchar_306 varchar(306),
  col_varchar_307 varchar(307),
  col_varchar_308 varchar(308),
  col_varchar_309 varchar(309),
  col_varchar_310 varchar(310),
  col_varchar_311 varchar(311),
  col_varchar_312 varchar(312),
  col_varchar_313 varchar(313),
  col_varchar_314 varchar(314),
  col_varchar_315 varchar(315),
  col_varchar_316 varchar(316),
  col_varchar_317 varchar(317),
  col_varchar_318 varchar(318),
  col_varchar_319 varchar(319),
  col_varchar_320 varchar(320),
  col_varchar_321 varchar(321),
  col_varchar_322 varchar(322),
  col_varchar_323 varchar(323),
  col_varchar_324 varchar(324),
  col_varchar_325 varchar(325),
  col_varchar_326 varchar(326),
  col_varchar_327 varchar(327),
  col_varchar_328 varchar(328),
  col_varchar_329 varchar(329),
  col_varchar_330 varchar(330),
  col_varchar_331 varchar(331),
  col_varchar_332 varchar(332),
  col_varchar_333 varchar(333),
  col_varchar_334 varchar(334),
  col_varchar_335 varchar(335),
  col_varchar_336 varchar(336),
  col_varchar_337 varchar(337),
  col_varchar_338 varchar(338),
  col_varchar_339 varchar(339),
  col_varchar_340 varchar(340),
  col_varchar_341 varchar(341),
  col_varchar_342 varchar(342),
  col_varchar_343 varchar(343),
  col_varchar_344 varchar(344),
  col_varchar_345 varchar(345),
  col_varchar_346 varchar(346),
  col_varchar_347 varchar(347),
  col_varchar_348 varchar(348),
  col_varchar_349 varchar(349),
  col_varchar_350 varchar(350),
  col_varchar_351 varchar(351),
  col_varchar_352 varchar(352),
  col_varchar_353 varchar(353),
  col_varchar_354 varchar(354),
  col_varchar_355 varchar(355)
) ENGINE=MyISAM DEFAULT CHARSET=latin1;

-- 创建 VARCHAR 长长度边界测试表 A（超出单表行宽限制，按边界值分表采样）
DROP TABLE IF EXISTS case_171_varchar_bounds_a;
CREATE TABLE case_171_varchar_bounds_a (
  col_varchar_512 varchar(512),
  col_varchar_1000 varchar(1000),
  col_varchar_2000 varchar(2000),
  col_varchar_4000 varchar(4000),
  col_varchar_8000 varchar(8000),
  col_varchar_16000 varchar(16000)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

-- 创建 VARCHAR 长长度边界测试表 B
DROP TABLE IF EXISTS case_172_varchar_bounds_b;
CREATE TABLE case_172_varchar_bounds_b (
  col_varchar_20000 varchar(20000),
  col_varchar_32000 varchar(32000)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

-- 创建 VARCHAR 长长度边界测试表 C（32767 为常见 2 字节长度边界）
DROP TABLE IF EXISTS case_173_varchar_bounds_c;
CREATE TABLE case_173_varchar_bounds_c (
  col_varchar_30000 varchar(30000),
  col_varchar_32767 varchar(32767)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

-- 创建 VARCHAR 最大长度测试表：65532 为 latin1 下行宽限制内的最大可用长度
-- NOT NULL 避免 NULL 标志位占用行宽
DROP TABLE IF EXISTS case_174_varchar_max;
CREATE TABLE case_174_varchar_max (
  col_varchar_65532 varchar(65532) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

-- 创建 BINARY 全长度测试表：BINARY(1) ~ BINARY(255)，每个长度一个字段；BINARY(n) -> BYTEA
-- 引擎使用 MyISAM：InnoDB 16K 页有 ~8126 字节行宽限制，定长 BINARY 无法溢出到外部页
DROP TABLE IF EXISTS case_175_binary_full;
CREATE TABLE case_175_binary_full (
  col_binary_001 binary(1),
  col_binary_002 binary(2),
  col_binary_003 binary(3),
  col_binary_004 binary(4),
  col_binary_005 binary(5),
  col_binary_006 binary(6),
  col_binary_007 binary(7),
  col_binary_008 binary(8),
  col_binary_009 binary(9),
  col_binary_010 binary(10),
  col_binary_011 binary(11),
  col_binary_012 binary(12),
  col_binary_013 binary(13),
  col_binary_014 binary(14),
  col_binary_015 binary(15),
  col_binary_016 binary(16),
  col_binary_017 binary(17),
  col_binary_018 binary(18),
  col_binary_019 binary(19),
  col_binary_020 binary(20),
  col_binary_021 binary(21),
  col_binary_022 binary(22),
  col_binary_023 binary(23),
  col_binary_024 binary(24),
  col_binary_025 binary(25),
  col_binary_026 binary(26),
  col_binary_027 binary(27),
  col_binary_028 binary(28),
  col_binary_029 binary(29),
  col_binary_030 binary(30),
  col_binary_031 binary(31),
  col_binary_032 binary(32),
  col_binary_033 binary(33),
  col_binary_034 binary(34),
  col_binary_035 binary(35),
  col_binary_036 binary(36),
  col_binary_037 binary(37),
  col_binary_038 binary(38),
  col_binary_039 binary(39),
  col_binary_040 binary(40),
  col_binary_041 binary(41),
  col_binary_042 binary(42),
  col_binary_043 binary(43),
  col_binary_044 binary(44),
  col_binary_045 binary(45),
  col_binary_046 binary(46),
  col_binary_047 binary(47),
  col_binary_048 binary(48),
  col_binary_049 binary(49),
  col_binary_050 binary(50),
  col_binary_051 binary(51),
  col_binary_052 binary(52),
  col_binary_053 binary(53),
  col_binary_054 binary(54),
  col_binary_055 binary(55),
  col_binary_056 binary(56),
  col_binary_057 binary(57),
  col_binary_058 binary(58),
  col_binary_059 binary(59),
  col_binary_060 binary(60),
  col_binary_061 binary(61),
  col_binary_062 binary(62),
  col_binary_063 binary(63),
  col_binary_064 binary(64),
  col_binary_065 binary(65),
  col_binary_066 binary(66),
  col_binary_067 binary(67),
  col_binary_068 binary(68),
  col_binary_069 binary(69),
  col_binary_070 binary(70),
  col_binary_071 binary(71),
  col_binary_072 binary(72),
  col_binary_073 binary(73),
  col_binary_074 binary(74),
  col_binary_075 binary(75),
  col_binary_076 binary(76),
  col_binary_077 binary(77),
  col_binary_078 binary(78),
  col_binary_079 binary(79),
  col_binary_080 binary(80),
  col_binary_081 binary(81),
  col_binary_082 binary(82),
  col_binary_083 binary(83),
  col_binary_084 binary(84),
  col_binary_085 binary(85),
  col_binary_086 binary(86),
  col_binary_087 binary(87),
  col_binary_088 binary(88),
  col_binary_089 binary(89),
  col_binary_090 binary(90),
  col_binary_091 binary(91),
  col_binary_092 binary(92),
  col_binary_093 binary(93),
  col_binary_094 binary(94),
  col_binary_095 binary(95),
  col_binary_096 binary(96),
  col_binary_097 binary(97),
  col_binary_098 binary(98),
  col_binary_099 binary(99),
  col_binary_100 binary(100),
  col_binary_101 binary(101),
  col_binary_102 binary(102),
  col_binary_103 binary(103),
  col_binary_104 binary(104),
  col_binary_105 binary(105),
  col_binary_106 binary(106),
  col_binary_107 binary(107),
  col_binary_108 binary(108),
  col_binary_109 binary(109),
  col_binary_110 binary(110),
  col_binary_111 binary(111),
  col_binary_112 binary(112),
  col_binary_113 binary(113),
  col_binary_114 binary(114),
  col_binary_115 binary(115),
  col_binary_116 binary(116),
  col_binary_117 binary(117),
  col_binary_118 binary(118),
  col_binary_119 binary(119),
  col_binary_120 binary(120),
  col_binary_121 binary(121),
  col_binary_122 binary(122),
  col_binary_123 binary(123),
  col_binary_124 binary(124),
  col_binary_125 binary(125),
  col_binary_126 binary(126),
  col_binary_127 binary(127),
  col_binary_128 binary(128),
  col_binary_129 binary(129),
  col_binary_130 binary(130),
  col_binary_131 binary(131),
  col_binary_132 binary(132),
  col_binary_133 binary(133),
  col_binary_134 binary(134),
  col_binary_135 binary(135),
  col_binary_136 binary(136),
  col_binary_137 binary(137),
  col_binary_138 binary(138),
  col_binary_139 binary(139),
  col_binary_140 binary(140),
  col_binary_141 binary(141),
  col_binary_142 binary(142),
  col_binary_143 binary(143),
  col_binary_144 binary(144),
  col_binary_145 binary(145),
  col_binary_146 binary(146),
  col_binary_147 binary(147),
  col_binary_148 binary(148),
  col_binary_149 binary(149),
  col_binary_150 binary(150),
  col_binary_151 binary(151),
  col_binary_152 binary(152),
  col_binary_153 binary(153),
  col_binary_154 binary(154),
  col_binary_155 binary(155),
  col_binary_156 binary(156),
  col_binary_157 binary(157),
  col_binary_158 binary(158),
  col_binary_159 binary(159),
  col_binary_160 binary(160),
  col_binary_161 binary(161),
  col_binary_162 binary(162),
  col_binary_163 binary(163),
  col_binary_164 binary(164),
  col_binary_165 binary(165),
  col_binary_166 binary(166),
  col_binary_167 binary(167),
  col_binary_168 binary(168),
  col_binary_169 binary(169),
  col_binary_170 binary(170),
  col_binary_171 binary(171),
  col_binary_172 binary(172),
  col_binary_173 binary(173),
  col_binary_174 binary(174),
  col_binary_175 binary(175),
  col_binary_176 binary(176),
  col_binary_177 binary(177),
  col_binary_178 binary(178),
  col_binary_179 binary(179),
  col_binary_180 binary(180),
  col_binary_181 binary(181),
  col_binary_182 binary(182),
  col_binary_183 binary(183),
  col_binary_184 binary(184),
  col_binary_185 binary(185),
  col_binary_186 binary(186),
  col_binary_187 binary(187),
  col_binary_188 binary(188),
  col_binary_189 binary(189),
  col_binary_190 binary(190),
  col_binary_191 binary(191),
  col_binary_192 binary(192),
  col_binary_193 binary(193),
  col_binary_194 binary(194),
  col_binary_195 binary(195),
  col_binary_196 binary(196),
  col_binary_197 binary(197),
  col_binary_198 binary(198),
  col_binary_199 binary(199),
  col_binary_200 binary(200),
  col_binary_201 binary(201),
  col_binary_202 binary(202),
  col_binary_203 binary(203),
  col_binary_204 binary(204),
  col_binary_205 binary(205),
  col_binary_206 binary(206),
  col_binary_207 binary(207),
  col_binary_208 binary(208),
  col_binary_209 binary(209),
  col_binary_210 binary(210),
  col_binary_211 binary(211),
  col_binary_212 binary(212),
  col_binary_213 binary(213),
  col_binary_214 binary(214),
  col_binary_215 binary(215),
  col_binary_216 binary(216),
  col_binary_217 binary(217),
  col_binary_218 binary(218),
  col_binary_219 binary(219),
  col_binary_220 binary(220),
  col_binary_221 binary(221),
  col_binary_222 binary(222),
  col_binary_223 binary(223),
  col_binary_224 binary(224),
  col_binary_225 binary(225),
  col_binary_226 binary(226),
  col_binary_227 binary(227),
  col_binary_228 binary(228),
  col_binary_229 binary(229),
  col_binary_230 binary(230),
  col_binary_231 binary(231),
  col_binary_232 binary(232),
  col_binary_233 binary(233),
  col_binary_234 binary(234),
  col_binary_235 binary(235),
  col_binary_236 binary(236),
  col_binary_237 binary(237),
  col_binary_238 binary(238),
  col_binary_239 binary(239),
  col_binary_240 binary(240),
  col_binary_241 binary(241),
  col_binary_242 binary(242),
  col_binary_243 binary(243),
  col_binary_244 binary(244),
  col_binary_245 binary(245),
  col_binary_246 binary(246),
  col_binary_247 binary(247),
  col_binary_248 binary(248),
  col_binary_249 binary(249),
  col_binary_250 binary(250),
  col_binary_251 binary(251),
  col_binary_252 binary(252),
  col_binary_253 binary(253),
  col_binary_254 binary(254),
  col_binary_255 binary(255)
) ENGINE=MyISAM;

-- 创建 VARBINARY 行宽内全长度测试表：VARBINARY(1) ~ VARBINARY(355)，每个长度一个字段；VARBINARY(n) -> BYTEA
-- 引擎使用 MyISAM：InnoDB 16K 页有 ~8126 字节行宽限制，大量 VARBINARY 列的溢出指针开销仍会超限
DROP TABLE IF EXISTS case_176_varbinary_full;
CREATE TABLE case_176_varbinary_full (
  col_varbinary_001 varbinary(1),
  col_varbinary_002 varbinary(2),
  col_varbinary_003 varbinary(3),
  col_varbinary_004 varbinary(4),
  col_varbinary_005 varbinary(5),
  col_varbinary_006 varbinary(6),
  col_varbinary_007 varbinary(7),
  col_varbinary_008 varbinary(8),
  col_varbinary_009 varbinary(9),
  col_varbinary_010 varbinary(10),
  col_varbinary_011 varbinary(11),
  col_varbinary_012 varbinary(12),
  col_varbinary_013 varbinary(13),
  col_varbinary_014 varbinary(14),
  col_varbinary_015 varbinary(15),
  col_varbinary_016 varbinary(16),
  col_varbinary_017 varbinary(17),
  col_varbinary_018 varbinary(18),
  col_varbinary_019 varbinary(19),
  col_varbinary_020 varbinary(20),
  col_varbinary_021 varbinary(21),
  col_varbinary_022 varbinary(22),
  col_varbinary_023 varbinary(23),
  col_varbinary_024 varbinary(24),
  col_varbinary_025 varbinary(25),
  col_varbinary_026 varbinary(26),
  col_varbinary_027 varbinary(27),
  col_varbinary_028 varbinary(28),
  col_varbinary_029 varbinary(29),
  col_varbinary_030 varbinary(30),
  col_varbinary_031 varbinary(31),
  col_varbinary_032 varbinary(32),
  col_varbinary_033 varbinary(33),
  col_varbinary_034 varbinary(34),
  col_varbinary_035 varbinary(35),
  col_varbinary_036 varbinary(36),
  col_varbinary_037 varbinary(37),
  col_varbinary_038 varbinary(38),
  col_varbinary_039 varbinary(39),
  col_varbinary_040 varbinary(40),
  col_varbinary_041 varbinary(41),
  col_varbinary_042 varbinary(42),
  col_varbinary_043 varbinary(43),
  col_varbinary_044 varbinary(44),
  col_varbinary_045 varbinary(45),
  col_varbinary_046 varbinary(46),
  col_varbinary_047 varbinary(47),
  col_varbinary_048 varbinary(48),
  col_varbinary_049 varbinary(49),
  col_varbinary_050 varbinary(50),
  col_varbinary_051 varbinary(51),
  col_varbinary_052 varbinary(52),
  col_varbinary_053 varbinary(53),
  col_varbinary_054 varbinary(54),
  col_varbinary_055 varbinary(55),
  col_varbinary_056 varbinary(56),
  col_varbinary_057 varbinary(57),
  col_varbinary_058 varbinary(58),
  col_varbinary_059 varbinary(59),
  col_varbinary_060 varbinary(60),
  col_varbinary_061 varbinary(61),
  col_varbinary_062 varbinary(62),
  col_varbinary_063 varbinary(63),
  col_varbinary_064 varbinary(64),
  col_varbinary_065 varbinary(65),
  col_varbinary_066 varbinary(66),
  col_varbinary_067 varbinary(67),
  col_varbinary_068 varbinary(68),
  col_varbinary_069 varbinary(69),
  col_varbinary_070 varbinary(70),
  col_varbinary_071 varbinary(71),
  col_varbinary_072 varbinary(72),
  col_varbinary_073 varbinary(73),
  col_varbinary_074 varbinary(74),
  col_varbinary_075 varbinary(75),
  col_varbinary_076 varbinary(76),
  col_varbinary_077 varbinary(77),
  col_varbinary_078 varbinary(78),
  col_varbinary_079 varbinary(79),
  col_varbinary_080 varbinary(80),
  col_varbinary_081 varbinary(81),
  col_varbinary_082 varbinary(82),
  col_varbinary_083 varbinary(83),
  col_varbinary_084 varbinary(84),
  col_varbinary_085 varbinary(85),
  col_varbinary_086 varbinary(86),
  col_varbinary_087 varbinary(87),
  col_varbinary_088 varbinary(88),
  col_varbinary_089 varbinary(89),
  col_varbinary_090 varbinary(90),
  col_varbinary_091 varbinary(91),
  col_varbinary_092 varbinary(92),
  col_varbinary_093 varbinary(93),
  col_varbinary_094 varbinary(94),
  col_varbinary_095 varbinary(95),
  col_varbinary_096 varbinary(96),
  col_varbinary_097 varbinary(97),
  col_varbinary_098 varbinary(98),
  col_varbinary_099 varbinary(99),
  col_varbinary_100 varbinary(100),
  col_varbinary_101 varbinary(101),
  col_varbinary_102 varbinary(102),
  col_varbinary_103 varbinary(103),
  col_varbinary_104 varbinary(104),
  col_varbinary_105 varbinary(105),
  col_varbinary_106 varbinary(106),
  col_varbinary_107 varbinary(107),
  col_varbinary_108 varbinary(108),
  col_varbinary_109 varbinary(109),
  col_varbinary_110 varbinary(110),
  col_varbinary_111 varbinary(111),
  col_varbinary_112 varbinary(112),
  col_varbinary_113 varbinary(113),
  col_varbinary_114 varbinary(114),
  col_varbinary_115 varbinary(115),
  col_varbinary_116 varbinary(116),
  col_varbinary_117 varbinary(117),
  col_varbinary_118 varbinary(118),
  col_varbinary_119 varbinary(119),
  col_varbinary_120 varbinary(120),
  col_varbinary_121 varbinary(121),
  col_varbinary_122 varbinary(122),
  col_varbinary_123 varbinary(123),
  col_varbinary_124 varbinary(124),
  col_varbinary_125 varbinary(125),
  col_varbinary_126 varbinary(126),
  col_varbinary_127 varbinary(127),
  col_varbinary_128 varbinary(128),
  col_varbinary_129 varbinary(129),
  col_varbinary_130 varbinary(130),
  col_varbinary_131 varbinary(131),
  col_varbinary_132 varbinary(132),
  col_varbinary_133 varbinary(133),
  col_varbinary_134 varbinary(134),
  col_varbinary_135 varbinary(135),
  col_varbinary_136 varbinary(136),
  col_varbinary_137 varbinary(137),
  col_varbinary_138 varbinary(138),
  col_varbinary_139 varbinary(139),
  col_varbinary_140 varbinary(140),
  col_varbinary_141 varbinary(141),
  col_varbinary_142 varbinary(142),
  col_varbinary_143 varbinary(143),
  col_varbinary_144 varbinary(144),
  col_varbinary_145 varbinary(145),
  col_varbinary_146 varbinary(146),
  col_varbinary_147 varbinary(147),
  col_varbinary_148 varbinary(148),
  col_varbinary_149 varbinary(149),
  col_varbinary_150 varbinary(150),
  col_varbinary_151 varbinary(151),
  col_varbinary_152 varbinary(152),
  col_varbinary_153 varbinary(153),
  col_varbinary_154 varbinary(154),
  col_varbinary_155 varbinary(155),
  col_varbinary_156 varbinary(156),
  col_varbinary_157 varbinary(157),
  col_varbinary_158 varbinary(158),
  col_varbinary_159 varbinary(159),
  col_varbinary_160 varbinary(160),
  col_varbinary_161 varbinary(161),
  col_varbinary_162 varbinary(162),
  col_varbinary_163 varbinary(163),
  col_varbinary_164 varbinary(164),
  col_varbinary_165 varbinary(165),
  col_varbinary_166 varbinary(166),
  col_varbinary_167 varbinary(167),
  col_varbinary_168 varbinary(168),
  col_varbinary_169 varbinary(169),
  col_varbinary_170 varbinary(170),
  col_varbinary_171 varbinary(171),
  col_varbinary_172 varbinary(172),
  col_varbinary_173 varbinary(173),
  col_varbinary_174 varbinary(174),
  col_varbinary_175 varbinary(175),
  col_varbinary_176 varbinary(176),
  col_varbinary_177 varbinary(177),
  col_varbinary_178 varbinary(178),
  col_varbinary_179 varbinary(179),
  col_varbinary_180 varbinary(180),
  col_varbinary_181 varbinary(181),
  col_varbinary_182 varbinary(182),
  col_varbinary_183 varbinary(183),
  col_varbinary_184 varbinary(184),
  col_varbinary_185 varbinary(185),
  col_varbinary_186 varbinary(186),
  col_varbinary_187 varbinary(187),
  col_varbinary_188 varbinary(188),
  col_varbinary_189 varbinary(189),
  col_varbinary_190 varbinary(190),
  col_varbinary_191 varbinary(191),
  col_varbinary_192 varbinary(192),
  col_varbinary_193 varbinary(193),
  col_varbinary_194 varbinary(194),
  col_varbinary_195 varbinary(195),
  col_varbinary_196 varbinary(196),
  col_varbinary_197 varbinary(197),
  col_varbinary_198 varbinary(198),
  col_varbinary_199 varbinary(199),
  col_varbinary_200 varbinary(200),
  col_varbinary_201 varbinary(201),
  col_varbinary_202 varbinary(202),
  col_varbinary_203 varbinary(203),
  col_varbinary_204 varbinary(204),
  col_varbinary_205 varbinary(205),
  col_varbinary_206 varbinary(206),
  col_varbinary_207 varbinary(207),
  col_varbinary_208 varbinary(208),
  col_varbinary_209 varbinary(209),
  col_varbinary_210 varbinary(210),
  col_varbinary_211 varbinary(211),
  col_varbinary_212 varbinary(212),
  col_varbinary_213 varbinary(213),
  col_varbinary_214 varbinary(214),
  col_varbinary_215 varbinary(215),
  col_varbinary_216 varbinary(216),
  col_varbinary_217 varbinary(217),
  col_varbinary_218 varbinary(218),
  col_varbinary_219 varbinary(219),
  col_varbinary_220 varbinary(220),
  col_varbinary_221 varbinary(221),
  col_varbinary_222 varbinary(222),
  col_varbinary_223 varbinary(223),
  col_varbinary_224 varbinary(224),
  col_varbinary_225 varbinary(225),
  col_varbinary_226 varbinary(226),
  col_varbinary_227 varbinary(227),
  col_varbinary_228 varbinary(228),
  col_varbinary_229 varbinary(229),
  col_varbinary_230 varbinary(230),
  col_varbinary_231 varbinary(231),
  col_varbinary_232 varbinary(232),
  col_varbinary_233 varbinary(233),
  col_varbinary_234 varbinary(234),
  col_varbinary_235 varbinary(235),
  col_varbinary_236 varbinary(236),
  col_varbinary_237 varbinary(237),
  col_varbinary_238 varbinary(238),
  col_varbinary_239 varbinary(239),
  col_varbinary_240 varbinary(240),
  col_varbinary_241 varbinary(241),
  col_varbinary_242 varbinary(242),
  col_varbinary_243 varbinary(243),
  col_varbinary_244 varbinary(244),
  col_varbinary_245 varbinary(245),
  col_varbinary_246 varbinary(246),
  col_varbinary_247 varbinary(247),
  col_varbinary_248 varbinary(248),
  col_varbinary_249 varbinary(249),
  col_varbinary_250 varbinary(250),
  col_varbinary_251 varbinary(251),
  col_varbinary_252 varbinary(252),
  col_varbinary_253 varbinary(253),
  col_varbinary_254 varbinary(254),
  col_varbinary_255 varbinary(255),
  col_varbinary_256 varbinary(256),
  col_varbinary_257 varbinary(257),
  col_varbinary_258 varbinary(258),
  col_varbinary_259 varbinary(259),
  col_varbinary_260 varbinary(260),
  col_varbinary_261 varbinary(261),
  col_varbinary_262 varbinary(262),
  col_varbinary_263 varbinary(263),
  col_varbinary_264 varbinary(264),
  col_varbinary_265 varbinary(265),
  col_varbinary_266 varbinary(266),
  col_varbinary_267 varbinary(267),
  col_varbinary_268 varbinary(268),
  col_varbinary_269 varbinary(269),
  col_varbinary_270 varbinary(270),
  col_varbinary_271 varbinary(271),
  col_varbinary_272 varbinary(272),
  col_varbinary_273 varbinary(273),
  col_varbinary_274 varbinary(274),
  col_varbinary_275 varbinary(275),
  col_varbinary_276 varbinary(276),
  col_varbinary_277 varbinary(277),
  col_varbinary_278 varbinary(278),
  col_varbinary_279 varbinary(279),
  col_varbinary_280 varbinary(280),
  col_varbinary_281 varbinary(281),
  col_varbinary_282 varbinary(282),
  col_varbinary_283 varbinary(283),
  col_varbinary_284 varbinary(284),
  col_varbinary_285 varbinary(285),
  col_varbinary_286 varbinary(286),
  col_varbinary_287 varbinary(287),
  col_varbinary_288 varbinary(288),
  col_varbinary_289 varbinary(289),
  col_varbinary_290 varbinary(290),
  col_varbinary_291 varbinary(291),
  col_varbinary_292 varbinary(292),
  col_varbinary_293 varbinary(293),
  col_varbinary_294 varbinary(294),
  col_varbinary_295 varbinary(295),
  col_varbinary_296 varbinary(296),
  col_varbinary_297 varbinary(297),
  col_varbinary_298 varbinary(298),
  col_varbinary_299 varbinary(299),
  col_varbinary_300 varbinary(300),
  col_varbinary_301 varbinary(301),
  col_varbinary_302 varbinary(302),
  col_varbinary_303 varbinary(303),
  col_varbinary_304 varbinary(304),
  col_varbinary_305 varbinary(305),
  col_varbinary_306 varbinary(306),
  col_varbinary_307 varbinary(307),
  col_varbinary_308 varbinary(308),
  col_varbinary_309 varbinary(309),
  col_varbinary_310 varbinary(310),
  col_varbinary_311 varbinary(311),
  col_varbinary_312 varbinary(312),
  col_varbinary_313 varbinary(313),
  col_varbinary_314 varbinary(314),
  col_varbinary_315 varbinary(315),
  col_varbinary_316 varbinary(316),
  col_varbinary_317 varbinary(317),
  col_varbinary_318 varbinary(318),
  col_varbinary_319 varbinary(319),
  col_varbinary_320 varbinary(320),
  col_varbinary_321 varbinary(321),
  col_varbinary_322 varbinary(322),
  col_varbinary_323 varbinary(323),
  col_varbinary_324 varbinary(324),
  col_varbinary_325 varbinary(325),
  col_varbinary_326 varbinary(326),
  col_varbinary_327 varbinary(327),
  col_varbinary_328 varbinary(328),
  col_varbinary_329 varbinary(329),
  col_varbinary_330 varbinary(330),
  col_varbinary_331 varbinary(331),
  col_varbinary_332 varbinary(332),
  col_varbinary_333 varbinary(333),
  col_varbinary_334 varbinary(334),
  col_varbinary_335 varbinary(335),
  col_varbinary_336 varbinary(336),
  col_varbinary_337 varbinary(337),
  col_varbinary_338 varbinary(338),
  col_varbinary_339 varbinary(339),
  col_varbinary_340 varbinary(340),
  col_varbinary_341 varbinary(341),
  col_varbinary_342 varbinary(342),
  col_varbinary_343 varbinary(343),
  col_varbinary_344 varbinary(344),
  col_varbinary_345 varbinary(345),
  col_varbinary_346 varbinary(346),
  col_varbinary_347 varbinary(347),
  col_varbinary_348 varbinary(348),
  col_varbinary_349 varbinary(349),
  col_varbinary_350 varbinary(350),
  col_varbinary_351 varbinary(351),
  col_varbinary_352 varbinary(352),
  col_varbinary_353 varbinary(353),
  col_varbinary_354 varbinary(354),
  col_varbinary_355 varbinary(355)
) ENGINE=MyISAM;

-- 创建 VARBINARY 长长度边界测试表 A
DROP TABLE IF EXISTS case_177_varbinary_bounds_a;
CREATE TABLE case_177_varbinary_bounds_a (
  col_varbinary_512 varbinary(512),
  col_varbinary_1000 varbinary(1000),
  col_varbinary_2000 varbinary(2000),
  col_varbinary_4000 varbinary(4000),
  col_varbinary_8000 varbinary(8000),
  col_varbinary_16000 varbinary(16000)
) ENGINE=InnoDB;

-- 创建 VARBINARY 长长度边界测试表 B
DROP TABLE IF EXISTS case_178_varbinary_bounds_b;
CREATE TABLE case_178_varbinary_bounds_b (
  col_varbinary_20000 varbinary(20000),
  col_varbinary_32000 varbinary(32000)
) ENGINE=InnoDB;

-- 创建 VARBINARY 长长度边界测试表 C
DROP TABLE IF EXISTS case_179_varbinary_bounds_c;
CREATE TABLE case_179_varbinary_bounds_c (
  col_varbinary_30000 varbinary(30000),
  col_varbinary_32767 varbinary(32767)
) ENGINE=InnoDB;

-- 创建 VARBINARY 最大长度测试表：65532 为行宽限制内的最大可用长度
DROP TABLE IF EXISTS case_180_varbinary_max;
CREATE TABLE case_180_varbinary_max (
  col_varbinary_65532 varbinary(65532) NOT NULL
) ENGINE=InnoDB;

-- 创建 TINYINT 显示宽度全扫测试表：tinyint(1) ~ tinyint(255)，每个宽度一个字段
-- MySQL 8.0.17+ 已弃用整数显示宽度（仅警告，仍可建表），转换时按基础类型映射为 注意 tinyint(1) -> BOOLEAN 为特例，tinyint(2..255) -> SMALLINT
DROP TABLE IF EXISTS case_181_tinyint_widths;
CREATE TABLE case_181_tinyint_widths (
  col_tinyint_w001 tinyint(1),
  col_tinyint_w002 tinyint(2),
  col_tinyint_w003 tinyint(3),
  col_tinyint_w004 tinyint(4),
  col_tinyint_w005 tinyint(5),
  col_tinyint_w006 tinyint(6),
  col_tinyint_w007 tinyint(7),
  col_tinyint_w008 tinyint(8),
  col_tinyint_w009 tinyint(9),
  col_tinyint_w010 tinyint(10),
  col_tinyint_w011 tinyint(11),
  col_tinyint_w012 tinyint(12),
  col_tinyint_w013 tinyint(13),
  col_tinyint_w014 tinyint(14),
  col_tinyint_w015 tinyint(15),
  col_tinyint_w016 tinyint(16),
  col_tinyint_w017 tinyint(17),
  col_tinyint_w018 tinyint(18),
  col_tinyint_w019 tinyint(19),
  col_tinyint_w020 tinyint(20),
  col_tinyint_w021 tinyint(21),
  col_tinyint_w022 tinyint(22),
  col_tinyint_w023 tinyint(23),
  col_tinyint_w024 tinyint(24),
  col_tinyint_w025 tinyint(25),
  col_tinyint_w026 tinyint(26),
  col_tinyint_w027 tinyint(27),
  col_tinyint_w028 tinyint(28),
  col_tinyint_w029 tinyint(29),
  col_tinyint_w030 tinyint(30),
  col_tinyint_w031 tinyint(31),
  col_tinyint_w032 tinyint(32),
  col_tinyint_w033 tinyint(33),
  col_tinyint_w034 tinyint(34),
  col_tinyint_w035 tinyint(35),
  col_tinyint_w036 tinyint(36),
  col_tinyint_w037 tinyint(37),
  col_tinyint_w038 tinyint(38),
  col_tinyint_w039 tinyint(39),
  col_tinyint_w040 tinyint(40),
  col_tinyint_w041 tinyint(41),
  col_tinyint_w042 tinyint(42),
  col_tinyint_w043 tinyint(43),
  col_tinyint_w044 tinyint(44),
  col_tinyint_w045 tinyint(45),
  col_tinyint_w046 tinyint(46),
  col_tinyint_w047 tinyint(47),
  col_tinyint_w048 tinyint(48),
  col_tinyint_w049 tinyint(49),
  col_tinyint_w050 tinyint(50),
  col_tinyint_w051 tinyint(51),
  col_tinyint_w052 tinyint(52),
  col_tinyint_w053 tinyint(53),
  col_tinyint_w054 tinyint(54),
  col_tinyint_w055 tinyint(55),
  col_tinyint_w056 tinyint(56),
  col_tinyint_w057 tinyint(57),
  col_tinyint_w058 tinyint(58),
  col_tinyint_w059 tinyint(59),
  col_tinyint_w060 tinyint(60),
  col_tinyint_w061 tinyint(61),
  col_tinyint_w062 tinyint(62),
  col_tinyint_w063 tinyint(63),
  col_tinyint_w064 tinyint(64),
  col_tinyint_w065 tinyint(65),
  col_tinyint_w066 tinyint(66),
  col_tinyint_w067 tinyint(67),
  col_tinyint_w068 tinyint(68),
  col_tinyint_w069 tinyint(69),
  col_tinyint_w070 tinyint(70),
  col_tinyint_w071 tinyint(71),
  col_tinyint_w072 tinyint(72),
  col_tinyint_w073 tinyint(73),
  col_tinyint_w074 tinyint(74),
  col_tinyint_w075 tinyint(75),
  col_tinyint_w076 tinyint(76),
  col_tinyint_w077 tinyint(77),
  col_tinyint_w078 tinyint(78),
  col_tinyint_w079 tinyint(79),
  col_tinyint_w080 tinyint(80),
  col_tinyint_w081 tinyint(81),
  col_tinyint_w082 tinyint(82),
  col_tinyint_w083 tinyint(83),
  col_tinyint_w084 tinyint(84),
  col_tinyint_w085 tinyint(85),
  col_tinyint_w086 tinyint(86),
  col_tinyint_w087 tinyint(87),
  col_tinyint_w088 tinyint(88),
  col_tinyint_w089 tinyint(89),
  col_tinyint_w090 tinyint(90),
  col_tinyint_w091 tinyint(91),
  col_tinyint_w092 tinyint(92),
  col_tinyint_w093 tinyint(93),
  col_tinyint_w094 tinyint(94),
  col_tinyint_w095 tinyint(95),
  col_tinyint_w096 tinyint(96),
  col_tinyint_w097 tinyint(97),
  col_tinyint_w098 tinyint(98),
  col_tinyint_w099 tinyint(99),
  col_tinyint_w100 tinyint(100),
  col_tinyint_w101 tinyint(101),
  col_tinyint_w102 tinyint(102),
  col_tinyint_w103 tinyint(103),
  col_tinyint_w104 tinyint(104),
  col_tinyint_w105 tinyint(105),
  col_tinyint_w106 tinyint(106),
  col_tinyint_w107 tinyint(107),
  col_tinyint_w108 tinyint(108),
  col_tinyint_w109 tinyint(109),
  col_tinyint_w110 tinyint(110),
  col_tinyint_w111 tinyint(111),
  col_tinyint_w112 tinyint(112),
  col_tinyint_w113 tinyint(113),
  col_tinyint_w114 tinyint(114),
  col_tinyint_w115 tinyint(115),
  col_tinyint_w116 tinyint(116),
  col_tinyint_w117 tinyint(117),
  col_tinyint_w118 tinyint(118),
  col_tinyint_w119 tinyint(119),
  col_tinyint_w120 tinyint(120),
  col_tinyint_w121 tinyint(121),
  col_tinyint_w122 tinyint(122),
  col_tinyint_w123 tinyint(123),
  col_tinyint_w124 tinyint(124),
  col_tinyint_w125 tinyint(125),
  col_tinyint_w126 tinyint(126),
  col_tinyint_w127 tinyint(127),
  col_tinyint_w128 tinyint(128),
  col_tinyint_w129 tinyint(129),
  col_tinyint_w130 tinyint(130),
  col_tinyint_w131 tinyint(131),
  col_tinyint_w132 tinyint(132),
  col_tinyint_w133 tinyint(133),
  col_tinyint_w134 tinyint(134),
  col_tinyint_w135 tinyint(135),
  col_tinyint_w136 tinyint(136),
  col_tinyint_w137 tinyint(137),
  col_tinyint_w138 tinyint(138),
  col_tinyint_w139 tinyint(139),
  col_tinyint_w140 tinyint(140),
  col_tinyint_w141 tinyint(141),
  col_tinyint_w142 tinyint(142),
  col_tinyint_w143 tinyint(143),
  col_tinyint_w144 tinyint(144),
  col_tinyint_w145 tinyint(145),
  col_tinyint_w146 tinyint(146),
  col_tinyint_w147 tinyint(147),
  col_tinyint_w148 tinyint(148),
  col_tinyint_w149 tinyint(149),
  col_tinyint_w150 tinyint(150),
  col_tinyint_w151 tinyint(151),
  col_tinyint_w152 tinyint(152),
  col_tinyint_w153 tinyint(153),
  col_tinyint_w154 tinyint(154),
  col_tinyint_w155 tinyint(155),
  col_tinyint_w156 tinyint(156),
  col_tinyint_w157 tinyint(157),
  col_tinyint_w158 tinyint(158),
  col_tinyint_w159 tinyint(159),
  col_tinyint_w160 tinyint(160),
  col_tinyint_w161 tinyint(161),
  col_tinyint_w162 tinyint(162),
  col_tinyint_w163 tinyint(163),
  col_tinyint_w164 tinyint(164),
  col_tinyint_w165 tinyint(165),
  col_tinyint_w166 tinyint(166),
  col_tinyint_w167 tinyint(167),
  col_tinyint_w168 tinyint(168),
  col_tinyint_w169 tinyint(169),
  col_tinyint_w170 tinyint(170),
  col_tinyint_w171 tinyint(171),
  col_tinyint_w172 tinyint(172),
  col_tinyint_w173 tinyint(173),
  col_tinyint_w174 tinyint(174),
  col_tinyint_w175 tinyint(175),
  col_tinyint_w176 tinyint(176),
  col_tinyint_w177 tinyint(177),
  col_tinyint_w178 tinyint(178),
  col_tinyint_w179 tinyint(179),
  col_tinyint_w180 tinyint(180),
  col_tinyint_w181 tinyint(181),
  col_tinyint_w182 tinyint(182),
  col_tinyint_w183 tinyint(183),
  col_tinyint_w184 tinyint(184),
  col_tinyint_w185 tinyint(185),
  col_tinyint_w186 tinyint(186),
  col_tinyint_w187 tinyint(187),
  col_tinyint_w188 tinyint(188),
  col_tinyint_w189 tinyint(189),
  col_tinyint_w190 tinyint(190),
  col_tinyint_w191 tinyint(191),
  col_tinyint_w192 tinyint(192),
  col_tinyint_w193 tinyint(193),
  col_tinyint_w194 tinyint(194),
  col_tinyint_w195 tinyint(195),
  col_tinyint_w196 tinyint(196),
  col_tinyint_w197 tinyint(197),
  col_tinyint_w198 tinyint(198),
  col_tinyint_w199 tinyint(199),
  col_tinyint_w200 tinyint(200),
  col_tinyint_w201 tinyint(201),
  col_tinyint_w202 tinyint(202),
  col_tinyint_w203 tinyint(203),
  col_tinyint_w204 tinyint(204),
  col_tinyint_w205 tinyint(205),
  col_tinyint_w206 tinyint(206),
  col_tinyint_w207 tinyint(207),
  col_tinyint_w208 tinyint(208),
  col_tinyint_w209 tinyint(209),
  col_tinyint_w210 tinyint(210),
  col_tinyint_w211 tinyint(211),
  col_tinyint_w212 tinyint(212),
  col_tinyint_w213 tinyint(213),
  col_tinyint_w214 tinyint(214),
  col_tinyint_w215 tinyint(215),
  col_tinyint_w216 tinyint(216),
  col_tinyint_w217 tinyint(217),
  col_tinyint_w218 tinyint(218),
  col_tinyint_w219 tinyint(219),
  col_tinyint_w220 tinyint(220),
  col_tinyint_w221 tinyint(221),
  col_tinyint_w222 tinyint(222),
  col_tinyint_w223 tinyint(223),
  col_tinyint_w224 tinyint(224),
  col_tinyint_w225 tinyint(225),
  col_tinyint_w226 tinyint(226),
  col_tinyint_w227 tinyint(227),
  col_tinyint_w228 tinyint(228),
  col_tinyint_w229 tinyint(229),
  col_tinyint_w230 tinyint(230),
  col_tinyint_w231 tinyint(231),
  col_tinyint_w232 tinyint(232),
  col_tinyint_w233 tinyint(233),
  col_tinyint_w234 tinyint(234),
  col_tinyint_w235 tinyint(235),
  col_tinyint_w236 tinyint(236),
  col_tinyint_w237 tinyint(237),
  col_tinyint_w238 tinyint(238),
  col_tinyint_w239 tinyint(239),
  col_tinyint_w240 tinyint(240),
  col_tinyint_w241 tinyint(241),
  col_tinyint_w242 tinyint(242),
  col_tinyint_w243 tinyint(243),
  col_tinyint_w244 tinyint(244),
  col_tinyint_w245 tinyint(245),
  col_tinyint_w246 tinyint(246),
  col_tinyint_w247 tinyint(247),
  col_tinyint_w248 tinyint(248),
  col_tinyint_w249 tinyint(249),
  col_tinyint_w250 tinyint(250),
  col_tinyint_w251 tinyint(251),
  col_tinyint_w252 tinyint(252),
  col_tinyint_w253 tinyint(253),
  col_tinyint_w254 tinyint(254),
  col_tinyint_w255 tinyint(255)
) ENGINE=InnoDB;

-- 创建 SMALLINT 显示宽度全扫测试表：smallint(1) ~ smallint(255)，每个宽度一个字段
-- MySQL 8.0.17+ 已弃用整数显示宽度（仅警告，仍可建表），转换时按基础类型映射为 SMALLINT
DROP TABLE IF EXISTS case_182_smallint_widths;
CREATE TABLE case_182_smallint_widths (
  col_smallint_w001 smallint(1),
  col_smallint_w002 smallint(2),
  col_smallint_w003 smallint(3),
  col_smallint_w004 smallint(4),
  col_smallint_w005 smallint(5),
  col_smallint_w006 smallint(6),
  col_smallint_w007 smallint(7),
  col_smallint_w008 smallint(8),
  col_smallint_w009 smallint(9),
  col_smallint_w010 smallint(10),
  col_smallint_w011 smallint(11),
  col_smallint_w012 smallint(12),
  col_smallint_w013 smallint(13),
  col_smallint_w014 smallint(14),
  col_smallint_w015 smallint(15),
  col_smallint_w016 smallint(16),
  col_smallint_w017 smallint(17),
  col_smallint_w018 smallint(18),
  col_smallint_w019 smallint(19),
  col_smallint_w020 smallint(20),
  col_smallint_w021 smallint(21),
  col_smallint_w022 smallint(22),
  col_smallint_w023 smallint(23),
  col_smallint_w024 smallint(24),
  col_smallint_w025 smallint(25),
  col_smallint_w026 smallint(26),
  col_smallint_w027 smallint(27),
  col_smallint_w028 smallint(28),
  col_smallint_w029 smallint(29),
  col_smallint_w030 smallint(30),
  col_smallint_w031 smallint(31),
  col_smallint_w032 smallint(32),
  col_smallint_w033 smallint(33),
  col_smallint_w034 smallint(34),
  col_smallint_w035 smallint(35),
  col_smallint_w036 smallint(36),
  col_smallint_w037 smallint(37),
  col_smallint_w038 smallint(38),
  col_smallint_w039 smallint(39),
  col_smallint_w040 smallint(40),
  col_smallint_w041 smallint(41),
  col_smallint_w042 smallint(42),
  col_smallint_w043 smallint(43),
  col_smallint_w044 smallint(44),
  col_smallint_w045 smallint(45),
  col_smallint_w046 smallint(46),
  col_smallint_w047 smallint(47),
  col_smallint_w048 smallint(48),
  col_smallint_w049 smallint(49),
  col_smallint_w050 smallint(50),
  col_smallint_w051 smallint(51),
  col_smallint_w052 smallint(52),
  col_smallint_w053 smallint(53),
  col_smallint_w054 smallint(54),
  col_smallint_w055 smallint(55),
  col_smallint_w056 smallint(56),
  col_smallint_w057 smallint(57),
  col_smallint_w058 smallint(58),
  col_smallint_w059 smallint(59),
  col_smallint_w060 smallint(60),
  col_smallint_w061 smallint(61),
  col_smallint_w062 smallint(62),
  col_smallint_w063 smallint(63),
  col_smallint_w064 smallint(64),
  col_smallint_w065 smallint(65),
  col_smallint_w066 smallint(66),
  col_smallint_w067 smallint(67),
  col_smallint_w068 smallint(68),
  col_smallint_w069 smallint(69),
  col_smallint_w070 smallint(70),
  col_smallint_w071 smallint(71),
  col_smallint_w072 smallint(72),
  col_smallint_w073 smallint(73),
  col_smallint_w074 smallint(74),
  col_smallint_w075 smallint(75),
  col_smallint_w076 smallint(76),
  col_smallint_w077 smallint(77),
  col_smallint_w078 smallint(78),
  col_smallint_w079 smallint(79),
  col_smallint_w080 smallint(80),
  col_smallint_w081 smallint(81),
  col_smallint_w082 smallint(82),
  col_smallint_w083 smallint(83),
  col_smallint_w084 smallint(84),
  col_smallint_w085 smallint(85),
  col_smallint_w086 smallint(86),
  col_smallint_w087 smallint(87),
  col_smallint_w088 smallint(88),
  col_smallint_w089 smallint(89),
  col_smallint_w090 smallint(90),
  col_smallint_w091 smallint(91),
  col_smallint_w092 smallint(92),
  col_smallint_w093 smallint(93),
  col_smallint_w094 smallint(94),
  col_smallint_w095 smallint(95),
  col_smallint_w096 smallint(96),
  col_smallint_w097 smallint(97),
  col_smallint_w098 smallint(98),
  col_smallint_w099 smallint(99),
  col_smallint_w100 smallint(100),
  col_smallint_w101 smallint(101),
  col_smallint_w102 smallint(102),
  col_smallint_w103 smallint(103),
  col_smallint_w104 smallint(104),
  col_smallint_w105 smallint(105),
  col_smallint_w106 smallint(106),
  col_smallint_w107 smallint(107),
  col_smallint_w108 smallint(108),
  col_smallint_w109 smallint(109),
  col_smallint_w110 smallint(110),
  col_smallint_w111 smallint(111),
  col_smallint_w112 smallint(112),
  col_smallint_w113 smallint(113),
  col_smallint_w114 smallint(114),
  col_smallint_w115 smallint(115),
  col_smallint_w116 smallint(116),
  col_smallint_w117 smallint(117),
  col_smallint_w118 smallint(118),
  col_smallint_w119 smallint(119),
  col_smallint_w120 smallint(120),
  col_smallint_w121 smallint(121),
  col_smallint_w122 smallint(122),
  col_smallint_w123 smallint(123),
  col_smallint_w124 smallint(124),
  col_smallint_w125 smallint(125),
  col_smallint_w126 smallint(126),
  col_smallint_w127 smallint(127),
  col_smallint_w128 smallint(128),
  col_smallint_w129 smallint(129),
  col_smallint_w130 smallint(130),
  col_smallint_w131 smallint(131),
  col_smallint_w132 smallint(132),
  col_smallint_w133 smallint(133),
  col_smallint_w134 smallint(134),
  col_smallint_w135 smallint(135),
  col_smallint_w136 smallint(136),
  col_smallint_w137 smallint(137),
  col_smallint_w138 smallint(138),
  col_smallint_w139 smallint(139),
  col_smallint_w140 smallint(140),
  col_smallint_w141 smallint(141),
  col_smallint_w142 smallint(142),
  col_smallint_w143 smallint(143),
  col_smallint_w144 smallint(144),
  col_smallint_w145 smallint(145),
  col_smallint_w146 smallint(146),
  col_smallint_w147 smallint(147),
  col_smallint_w148 smallint(148),
  col_smallint_w149 smallint(149),
  col_smallint_w150 smallint(150),
  col_smallint_w151 smallint(151),
  col_smallint_w152 smallint(152),
  col_smallint_w153 smallint(153),
  col_smallint_w154 smallint(154),
  col_smallint_w155 smallint(155),
  col_smallint_w156 smallint(156),
  col_smallint_w157 smallint(157),
  col_smallint_w158 smallint(158),
  col_smallint_w159 smallint(159),
  col_smallint_w160 smallint(160),
  col_smallint_w161 smallint(161),
  col_smallint_w162 smallint(162),
  col_smallint_w163 smallint(163),
  col_smallint_w164 smallint(164),
  col_smallint_w165 smallint(165),
  col_smallint_w166 smallint(166),
  col_smallint_w167 smallint(167),
  col_smallint_w168 smallint(168),
  col_smallint_w169 smallint(169),
  col_smallint_w170 smallint(170),
  col_smallint_w171 smallint(171),
  col_smallint_w172 smallint(172),
  col_smallint_w173 smallint(173),
  col_smallint_w174 smallint(174),
  col_smallint_w175 smallint(175),
  col_smallint_w176 smallint(176),
  col_smallint_w177 smallint(177),
  col_smallint_w178 smallint(178),
  col_smallint_w179 smallint(179),
  col_smallint_w180 smallint(180),
  col_smallint_w181 smallint(181),
  col_smallint_w182 smallint(182),
  col_smallint_w183 smallint(183),
  col_smallint_w184 smallint(184),
  col_smallint_w185 smallint(185),
  col_smallint_w186 smallint(186),
  col_smallint_w187 smallint(187),
  col_smallint_w188 smallint(188),
  col_smallint_w189 smallint(189),
  col_smallint_w190 smallint(190),
  col_smallint_w191 smallint(191),
  col_smallint_w192 smallint(192),
  col_smallint_w193 smallint(193),
  col_smallint_w194 smallint(194),
  col_smallint_w195 smallint(195),
  col_smallint_w196 smallint(196),
  col_smallint_w197 smallint(197),
  col_smallint_w198 smallint(198),
  col_smallint_w199 smallint(199),
  col_smallint_w200 smallint(200),
  col_smallint_w201 smallint(201),
  col_smallint_w202 smallint(202),
  col_smallint_w203 smallint(203),
  col_smallint_w204 smallint(204),
  col_smallint_w205 smallint(205),
  col_smallint_w206 smallint(206),
  col_smallint_w207 smallint(207),
  col_smallint_w208 smallint(208),
  col_smallint_w209 smallint(209),
  col_smallint_w210 smallint(210),
  col_smallint_w211 smallint(211),
  col_smallint_w212 smallint(212),
  col_smallint_w213 smallint(213),
  col_smallint_w214 smallint(214),
  col_smallint_w215 smallint(215),
  col_smallint_w216 smallint(216),
  col_smallint_w217 smallint(217),
  col_smallint_w218 smallint(218),
  col_smallint_w219 smallint(219),
  col_smallint_w220 smallint(220),
  col_smallint_w221 smallint(221),
  col_smallint_w222 smallint(222),
  col_smallint_w223 smallint(223),
  col_smallint_w224 smallint(224),
  col_smallint_w225 smallint(225),
  col_smallint_w226 smallint(226),
  col_smallint_w227 smallint(227),
  col_smallint_w228 smallint(228),
  col_smallint_w229 smallint(229),
  col_smallint_w230 smallint(230),
  col_smallint_w231 smallint(231),
  col_smallint_w232 smallint(232),
  col_smallint_w233 smallint(233),
  col_smallint_w234 smallint(234),
  col_smallint_w235 smallint(235),
  col_smallint_w236 smallint(236),
  col_smallint_w237 smallint(237),
  col_smallint_w238 smallint(238),
  col_smallint_w239 smallint(239),
  col_smallint_w240 smallint(240),
  col_smallint_w241 smallint(241),
  col_smallint_w242 smallint(242),
  col_smallint_w243 smallint(243),
  col_smallint_w244 smallint(244),
  col_smallint_w245 smallint(245),
  col_smallint_w246 smallint(246),
  col_smallint_w247 smallint(247),
  col_smallint_w248 smallint(248),
  col_smallint_w249 smallint(249),
  col_smallint_w250 smallint(250),
  col_smallint_w251 smallint(251),
  col_smallint_w252 smallint(252),
  col_smallint_w253 smallint(253),
  col_smallint_w254 smallint(254),
  col_smallint_w255 smallint(255)
) ENGINE=InnoDB;

-- 创建 MEDIUMINT 显示宽度全扫测试表：mediumint(1) ~ mediumint(255)，每个宽度一个字段
-- MySQL 8.0.17+ 已弃用整数显示宽度（仅警告，仍可建表），转换时按基础类型映射为 INTEGER
DROP TABLE IF EXISTS case_183_mediumint_widths;
CREATE TABLE case_183_mediumint_widths (
  col_mediumint_w001 mediumint(1),
  col_mediumint_w002 mediumint(2),
  col_mediumint_w003 mediumint(3),
  col_mediumint_w004 mediumint(4),
  col_mediumint_w005 mediumint(5),
  col_mediumint_w006 mediumint(6),
  col_mediumint_w007 mediumint(7),
  col_mediumint_w008 mediumint(8),
  col_mediumint_w009 mediumint(9),
  col_mediumint_w010 mediumint(10),
  col_mediumint_w011 mediumint(11),
  col_mediumint_w012 mediumint(12),
  col_mediumint_w013 mediumint(13),
  col_mediumint_w014 mediumint(14),
  col_mediumint_w015 mediumint(15),
  col_mediumint_w016 mediumint(16),
  col_mediumint_w017 mediumint(17),
  col_mediumint_w018 mediumint(18),
  col_mediumint_w019 mediumint(19),
  col_mediumint_w020 mediumint(20),
  col_mediumint_w021 mediumint(21),
  col_mediumint_w022 mediumint(22),
  col_mediumint_w023 mediumint(23),
  col_mediumint_w024 mediumint(24),
  col_mediumint_w025 mediumint(25),
  col_mediumint_w026 mediumint(26),
  col_mediumint_w027 mediumint(27),
  col_mediumint_w028 mediumint(28),
  col_mediumint_w029 mediumint(29),
  col_mediumint_w030 mediumint(30),
  col_mediumint_w031 mediumint(31),
  col_mediumint_w032 mediumint(32),
  col_mediumint_w033 mediumint(33),
  col_mediumint_w034 mediumint(34),
  col_mediumint_w035 mediumint(35),
  col_mediumint_w036 mediumint(36),
  col_mediumint_w037 mediumint(37),
  col_mediumint_w038 mediumint(38),
  col_mediumint_w039 mediumint(39),
  col_mediumint_w040 mediumint(40),
  col_mediumint_w041 mediumint(41),
  col_mediumint_w042 mediumint(42),
  col_mediumint_w043 mediumint(43),
  col_mediumint_w044 mediumint(44),
  col_mediumint_w045 mediumint(45),
  col_mediumint_w046 mediumint(46),
  col_mediumint_w047 mediumint(47),
  col_mediumint_w048 mediumint(48),
  col_mediumint_w049 mediumint(49),
  col_mediumint_w050 mediumint(50),
  col_mediumint_w051 mediumint(51),
  col_mediumint_w052 mediumint(52),
  col_mediumint_w053 mediumint(53),
  col_mediumint_w054 mediumint(54),
  col_mediumint_w055 mediumint(55),
  col_mediumint_w056 mediumint(56),
  col_mediumint_w057 mediumint(57),
  col_mediumint_w058 mediumint(58),
  col_mediumint_w059 mediumint(59),
  col_mediumint_w060 mediumint(60),
  col_mediumint_w061 mediumint(61),
  col_mediumint_w062 mediumint(62),
  col_mediumint_w063 mediumint(63),
  col_mediumint_w064 mediumint(64),
  col_mediumint_w065 mediumint(65),
  col_mediumint_w066 mediumint(66),
  col_mediumint_w067 mediumint(67),
  col_mediumint_w068 mediumint(68),
  col_mediumint_w069 mediumint(69),
  col_mediumint_w070 mediumint(70),
  col_mediumint_w071 mediumint(71),
  col_mediumint_w072 mediumint(72),
  col_mediumint_w073 mediumint(73),
  col_mediumint_w074 mediumint(74),
  col_mediumint_w075 mediumint(75),
  col_mediumint_w076 mediumint(76),
  col_mediumint_w077 mediumint(77),
  col_mediumint_w078 mediumint(78),
  col_mediumint_w079 mediumint(79),
  col_mediumint_w080 mediumint(80),
  col_mediumint_w081 mediumint(81),
  col_mediumint_w082 mediumint(82),
  col_mediumint_w083 mediumint(83),
  col_mediumint_w084 mediumint(84),
  col_mediumint_w085 mediumint(85),
  col_mediumint_w086 mediumint(86),
  col_mediumint_w087 mediumint(87),
  col_mediumint_w088 mediumint(88),
  col_mediumint_w089 mediumint(89),
  col_mediumint_w090 mediumint(90),
  col_mediumint_w091 mediumint(91),
  col_mediumint_w092 mediumint(92),
  col_mediumint_w093 mediumint(93),
  col_mediumint_w094 mediumint(94),
  col_mediumint_w095 mediumint(95),
  col_mediumint_w096 mediumint(96),
  col_mediumint_w097 mediumint(97),
  col_mediumint_w098 mediumint(98),
  col_mediumint_w099 mediumint(99),
  col_mediumint_w100 mediumint(100),
  col_mediumint_w101 mediumint(101),
  col_mediumint_w102 mediumint(102),
  col_mediumint_w103 mediumint(103),
  col_mediumint_w104 mediumint(104),
  col_mediumint_w105 mediumint(105),
  col_mediumint_w106 mediumint(106),
  col_mediumint_w107 mediumint(107),
  col_mediumint_w108 mediumint(108),
  col_mediumint_w109 mediumint(109),
  col_mediumint_w110 mediumint(110),
  col_mediumint_w111 mediumint(111),
  col_mediumint_w112 mediumint(112),
  col_mediumint_w113 mediumint(113),
  col_mediumint_w114 mediumint(114),
  col_mediumint_w115 mediumint(115),
  col_mediumint_w116 mediumint(116),
  col_mediumint_w117 mediumint(117),
  col_mediumint_w118 mediumint(118),
  col_mediumint_w119 mediumint(119),
  col_mediumint_w120 mediumint(120),
  col_mediumint_w121 mediumint(121),
  col_mediumint_w122 mediumint(122),
  col_mediumint_w123 mediumint(123),
  col_mediumint_w124 mediumint(124),
  col_mediumint_w125 mediumint(125),
  col_mediumint_w126 mediumint(126),
  col_mediumint_w127 mediumint(127),
  col_mediumint_w128 mediumint(128),
  col_mediumint_w129 mediumint(129),
  col_mediumint_w130 mediumint(130),
  col_mediumint_w131 mediumint(131),
  col_mediumint_w132 mediumint(132),
  col_mediumint_w133 mediumint(133),
  col_mediumint_w134 mediumint(134),
  col_mediumint_w135 mediumint(135),
  col_mediumint_w136 mediumint(136),
  col_mediumint_w137 mediumint(137),
  col_mediumint_w138 mediumint(138),
  col_mediumint_w139 mediumint(139),
  col_mediumint_w140 mediumint(140),
  col_mediumint_w141 mediumint(141),
  col_mediumint_w142 mediumint(142),
  col_mediumint_w143 mediumint(143),
  col_mediumint_w144 mediumint(144),
  col_mediumint_w145 mediumint(145),
  col_mediumint_w146 mediumint(146),
  col_mediumint_w147 mediumint(147),
  col_mediumint_w148 mediumint(148),
  col_mediumint_w149 mediumint(149),
  col_mediumint_w150 mediumint(150),
  col_mediumint_w151 mediumint(151),
  col_mediumint_w152 mediumint(152),
  col_mediumint_w153 mediumint(153),
  col_mediumint_w154 mediumint(154),
  col_mediumint_w155 mediumint(155),
  col_mediumint_w156 mediumint(156),
  col_mediumint_w157 mediumint(157),
  col_mediumint_w158 mediumint(158),
  col_mediumint_w159 mediumint(159),
  col_mediumint_w160 mediumint(160),
  col_mediumint_w161 mediumint(161),
  col_mediumint_w162 mediumint(162),
  col_mediumint_w163 mediumint(163),
  col_mediumint_w164 mediumint(164),
  col_mediumint_w165 mediumint(165),
  col_mediumint_w166 mediumint(166),
  col_mediumint_w167 mediumint(167),
  col_mediumint_w168 mediumint(168),
  col_mediumint_w169 mediumint(169),
  col_mediumint_w170 mediumint(170),
  col_mediumint_w171 mediumint(171),
  col_mediumint_w172 mediumint(172),
  col_mediumint_w173 mediumint(173),
  col_mediumint_w174 mediumint(174),
  col_mediumint_w175 mediumint(175),
  col_mediumint_w176 mediumint(176),
  col_mediumint_w177 mediumint(177),
  col_mediumint_w178 mediumint(178),
  col_mediumint_w179 mediumint(179),
  col_mediumint_w180 mediumint(180),
  col_mediumint_w181 mediumint(181),
  col_mediumint_w182 mediumint(182),
  col_mediumint_w183 mediumint(183),
  col_mediumint_w184 mediumint(184),
  col_mediumint_w185 mediumint(185),
  col_mediumint_w186 mediumint(186),
  col_mediumint_w187 mediumint(187),
  col_mediumint_w188 mediumint(188),
  col_mediumint_w189 mediumint(189),
  col_mediumint_w190 mediumint(190),
  col_mediumint_w191 mediumint(191),
  col_mediumint_w192 mediumint(192),
  col_mediumint_w193 mediumint(193),
  col_mediumint_w194 mediumint(194),
  col_mediumint_w195 mediumint(195),
  col_mediumint_w196 mediumint(196),
  col_mediumint_w197 mediumint(197),
  col_mediumint_w198 mediumint(198),
  col_mediumint_w199 mediumint(199),
  col_mediumint_w200 mediumint(200),
  col_mediumint_w201 mediumint(201),
  col_mediumint_w202 mediumint(202),
  col_mediumint_w203 mediumint(203),
  col_mediumint_w204 mediumint(204),
  col_mediumint_w205 mediumint(205),
  col_mediumint_w206 mediumint(206),
  col_mediumint_w207 mediumint(207),
  col_mediumint_w208 mediumint(208),
  col_mediumint_w209 mediumint(209),
  col_mediumint_w210 mediumint(210),
  col_mediumint_w211 mediumint(211),
  col_mediumint_w212 mediumint(212),
  col_mediumint_w213 mediumint(213),
  col_mediumint_w214 mediumint(214),
  col_mediumint_w215 mediumint(215),
  col_mediumint_w216 mediumint(216),
  col_mediumint_w217 mediumint(217),
  col_mediumint_w218 mediumint(218),
  col_mediumint_w219 mediumint(219),
  col_mediumint_w220 mediumint(220),
  col_mediumint_w221 mediumint(221),
  col_mediumint_w222 mediumint(222),
  col_mediumint_w223 mediumint(223),
  col_mediumint_w224 mediumint(224),
  col_mediumint_w225 mediumint(225),
  col_mediumint_w226 mediumint(226),
  col_mediumint_w227 mediumint(227),
  col_mediumint_w228 mediumint(228),
  col_mediumint_w229 mediumint(229),
  col_mediumint_w230 mediumint(230),
  col_mediumint_w231 mediumint(231),
  col_mediumint_w232 mediumint(232),
  col_mediumint_w233 mediumint(233),
  col_mediumint_w234 mediumint(234),
  col_mediumint_w235 mediumint(235),
  col_mediumint_w236 mediumint(236),
  col_mediumint_w237 mediumint(237),
  col_mediumint_w238 mediumint(238),
  col_mediumint_w239 mediumint(239),
  col_mediumint_w240 mediumint(240),
  col_mediumint_w241 mediumint(241),
  col_mediumint_w242 mediumint(242),
  col_mediumint_w243 mediumint(243),
  col_mediumint_w244 mediumint(244),
  col_mediumint_w245 mediumint(245),
  col_mediumint_w246 mediumint(246),
  col_mediumint_w247 mediumint(247),
  col_mediumint_w248 mediumint(248),
  col_mediumint_w249 mediumint(249),
  col_mediumint_w250 mediumint(250),
  col_mediumint_w251 mediumint(251),
  col_mediumint_w252 mediumint(252),
  col_mediumint_w253 mediumint(253),
  col_mediumint_w254 mediumint(254),
  col_mediumint_w255 mediumint(255)
) ENGINE=InnoDB;

-- 创建 INT 显示宽度全扫测试表：int(1) ~ int(255)，每个宽度一个字段
-- MySQL 8.0.17+ 已弃用整数显示宽度（仅警告，仍可建表），转换时按基础类型映射为 INTEGER
DROP TABLE IF EXISTS case_184_int_widths;
CREATE TABLE case_184_int_widths (
  col_int_w001 int(1),
  col_int_w002 int(2),
  col_int_w003 int(3),
  col_int_w004 int(4),
  col_int_w005 int(5),
  col_int_w006 int(6),
  col_int_w007 int(7),
  col_int_w008 int(8),
  col_int_w009 int(9),
  col_int_w010 int(10),
  col_int_w011 int(11),
  col_int_w012 int(12),
  col_int_w013 int(13),
  col_int_w014 int(14),
  col_int_w015 int(15),
  col_int_w016 int(16),
  col_int_w017 int(17),
  col_int_w018 int(18),
  col_int_w019 int(19),
  col_int_w020 int(20),
  col_int_w021 int(21),
  col_int_w022 int(22),
  col_int_w023 int(23),
  col_int_w024 int(24),
  col_int_w025 int(25),
  col_int_w026 int(26),
  col_int_w027 int(27),
  col_int_w028 int(28),
  col_int_w029 int(29),
  col_int_w030 int(30),
  col_int_w031 int(31),
  col_int_w032 int(32),
  col_int_w033 int(33),
  col_int_w034 int(34),
  col_int_w035 int(35),
  col_int_w036 int(36),
  col_int_w037 int(37),
  col_int_w038 int(38),
  col_int_w039 int(39),
  col_int_w040 int(40),
  col_int_w041 int(41),
  col_int_w042 int(42),
  col_int_w043 int(43),
  col_int_w044 int(44),
  col_int_w045 int(45),
  col_int_w046 int(46),
  col_int_w047 int(47),
  col_int_w048 int(48),
  col_int_w049 int(49),
  col_int_w050 int(50),
  col_int_w051 int(51),
  col_int_w052 int(52),
  col_int_w053 int(53),
  col_int_w054 int(54),
  col_int_w055 int(55),
  col_int_w056 int(56),
  col_int_w057 int(57),
  col_int_w058 int(58),
  col_int_w059 int(59),
  col_int_w060 int(60),
  col_int_w061 int(61),
  col_int_w062 int(62),
  col_int_w063 int(63),
  col_int_w064 int(64),
  col_int_w065 int(65),
  col_int_w066 int(66),
  col_int_w067 int(67),
  col_int_w068 int(68),
  col_int_w069 int(69),
  col_int_w070 int(70),
  col_int_w071 int(71),
  col_int_w072 int(72),
  col_int_w073 int(73),
  col_int_w074 int(74),
  col_int_w075 int(75),
  col_int_w076 int(76),
  col_int_w077 int(77),
  col_int_w078 int(78),
  col_int_w079 int(79),
  col_int_w080 int(80),
  col_int_w081 int(81),
  col_int_w082 int(82),
  col_int_w083 int(83),
  col_int_w084 int(84),
  col_int_w085 int(85),
  col_int_w086 int(86),
  col_int_w087 int(87),
  col_int_w088 int(88),
  col_int_w089 int(89),
  col_int_w090 int(90),
  col_int_w091 int(91),
  col_int_w092 int(92),
  col_int_w093 int(93),
  col_int_w094 int(94),
  col_int_w095 int(95),
  col_int_w096 int(96),
  col_int_w097 int(97),
  col_int_w098 int(98),
  col_int_w099 int(99),
  col_int_w100 int(100),
  col_int_w101 int(101),
  col_int_w102 int(102),
  col_int_w103 int(103),
  col_int_w104 int(104),
  col_int_w105 int(105),
  col_int_w106 int(106),
  col_int_w107 int(107),
  col_int_w108 int(108),
  col_int_w109 int(109),
  col_int_w110 int(110),
  col_int_w111 int(111),
  col_int_w112 int(112),
  col_int_w113 int(113),
  col_int_w114 int(114),
  col_int_w115 int(115),
  col_int_w116 int(116),
  col_int_w117 int(117),
  col_int_w118 int(118),
  col_int_w119 int(119),
  col_int_w120 int(120),
  col_int_w121 int(121),
  col_int_w122 int(122),
  col_int_w123 int(123),
  col_int_w124 int(124),
  col_int_w125 int(125),
  col_int_w126 int(126),
  col_int_w127 int(127),
  col_int_w128 int(128),
  col_int_w129 int(129),
  col_int_w130 int(130),
  col_int_w131 int(131),
  col_int_w132 int(132),
  col_int_w133 int(133),
  col_int_w134 int(134),
  col_int_w135 int(135),
  col_int_w136 int(136),
  col_int_w137 int(137),
  col_int_w138 int(138),
  col_int_w139 int(139),
  col_int_w140 int(140),
  col_int_w141 int(141),
  col_int_w142 int(142),
  col_int_w143 int(143),
  col_int_w144 int(144),
  col_int_w145 int(145),
  col_int_w146 int(146),
  col_int_w147 int(147),
  col_int_w148 int(148),
  col_int_w149 int(149),
  col_int_w150 int(150),
  col_int_w151 int(151),
  col_int_w152 int(152),
  col_int_w153 int(153),
  col_int_w154 int(154),
  col_int_w155 int(155),
  col_int_w156 int(156),
  col_int_w157 int(157),
  col_int_w158 int(158),
  col_int_w159 int(159),
  col_int_w160 int(160),
  col_int_w161 int(161),
  col_int_w162 int(162),
  col_int_w163 int(163),
  col_int_w164 int(164),
  col_int_w165 int(165),
  col_int_w166 int(166),
  col_int_w167 int(167),
  col_int_w168 int(168),
  col_int_w169 int(169),
  col_int_w170 int(170),
  col_int_w171 int(171),
  col_int_w172 int(172),
  col_int_w173 int(173),
  col_int_w174 int(174),
  col_int_w175 int(175),
  col_int_w176 int(176),
  col_int_w177 int(177),
  col_int_w178 int(178),
  col_int_w179 int(179),
  col_int_w180 int(180),
  col_int_w181 int(181),
  col_int_w182 int(182),
  col_int_w183 int(183),
  col_int_w184 int(184),
  col_int_w185 int(185),
  col_int_w186 int(186),
  col_int_w187 int(187),
  col_int_w188 int(188),
  col_int_w189 int(189),
  col_int_w190 int(190),
  col_int_w191 int(191),
  col_int_w192 int(192),
  col_int_w193 int(193),
  col_int_w194 int(194),
  col_int_w195 int(195),
  col_int_w196 int(196),
  col_int_w197 int(197),
  col_int_w198 int(198),
  col_int_w199 int(199),
  col_int_w200 int(200),
  col_int_w201 int(201),
  col_int_w202 int(202),
  col_int_w203 int(203),
  col_int_w204 int(204),
  col_int_w205 int(205),
  col_int_w206 int(206),
  col_int_w207 int(207),
  col_int_w208 int(208),
  col_int_w209 int(209),
  col_int_w210 int(210),
  col_int_w211 int(211),
  col_int_w212 int(212),
  col_int_w213 int(213),
  col_int_w214 int(214),
  col_int_w215 int(215),
  col_int_w216 int(216),
  col_int_w217 int(217),
  col_int_w218 int(218),
  col_int_w219 int(219),
  col_int_w220 int(220),
  col_int_w221 int(221),
  col_int_w222 int(222),
  col_int_w223 int(223),
  col_int_w224 int(224),
  col_int_w225 int(225),
  col_int_w226 int(226),
  col_int_w227 int(227),
  col_int_w228 int(228),
  col_int_w229 int(229),
  col_int_w230 int(230),
  col_int_w231 int(231),
  col_int_w232 int(232),
  col_int_w233 int(233),
  col_int_w234 int(234),
  col_int_w235 int(235),
  col_int_w236 int(236),
  col_int_w237 int(237),
  col_int_w238 int(238),
  col_int_w239 int(239),
  col_int_w240 int(240),
  col_int_w241 int(241),
  col_int_w242 int(242),
  col_int_w243 int(243),
  col_int_w244 int(244),
  col_int_w245 int(245),
  col_int_w246 int(246),
  col_int_w247 int(247),
  col_int_w248 int(248),
  col_int_w249 int(249),
  col_int_w250 int(250),
  col_int_w251 int(251),
  col_int_w252 int(252),
  col_int_w253 int(253),
  col_int_w254 int(254),
  col_int_w255 int(255)
) ENGINE=InnoDB;

-- 创建 BIGINT 显示宽度全扫测试表：bigint(1) ~ bigint(255)，每个宽度一个字段
-- MySQL 8.0.17+ 已弃用整数显示宽度（仅警告，仍可建表），转换时按基础类型映射为 BIGINT
DROP TABLE IF EXISTS case_185_bigint_widths;
CREATE TABLE case_185_bigint_widths (
  col_bigint_w001 bigint(1),
  col_bigint_w002 bigint(2),
  col_bigint_w003 bigint(3),
  col_bigint_w004 bigint(4),
  col_bigint_w005 bigint(5),
  col_bigint_w006 bigint(6),
  col_bigint_w007 bigint(7),
  col_bigint_w008 bigint(8),
  col_bigint_w009 bigint(9),
  col_bigint_w010 bigint(10),
  col_bigint_w011 bigint(11),
  col_bigint_w012 bigint(12),
  col_bigint_w013 bigint(13),
  col_bigint_w014 bigint(14),
  col_bigint_w015 bigint(15),
  col_bigint_w016 bigint(16),
  col_bigint_w017 bigint(17),
  col_bigint_w018 bigint(18),
  col_bigint_w019 bigint(19),
  col_bigint_w020 bigint(20),
  col_bigint_w021 bigint(21),
  col_bigint_w022 bigint(22),
  col_bigint_w023 bigint(23),
  col_bigint_w024 bigint(24),
  col_bigint_w025 bigint(25),
  col_bigint_w026 bigint(26),
  col_bigint_w027 bigint(27),
  col_bigint_w028 bigint(28),
  col_bigint_w029 bigint(29),
  col_bigint_w030 bigint(30),
  col_bigint_w031 bigint(31),
  col_bigint_w032 bigint(32),
  col_bigint_w033 bigint(33),
  col_bigint_w034 bigint(34),
  col_bigint_w035 bigint(35),
  col_bigint_w036 bigint(36),
  col_bigint_w037 bigint(37),
  col_bigint_w038 bigint(38),
  col_bigint_w039 bigint(39),
  col_bigint_w040 bigint(40),
  col_bigint_w041 bigint(41),
  col_bigint_w042 bigint(42),
  col_bigint_w043 bigint(43),
  col_bigint_w044 bigint(44),
  col_bigint_w045 bigint(45),
  col_bigint_w046 bigint(46),
  col_bigint_w047 bigint(47),
  col_bigint_w048 bigint(48),
  col_bigint_w049 bigint(49),
  col_bigint_w050 bigint(50),
  col_bigint_w051 bigint(51),
  col_bigint_w052 bigint(52),
  col_bigint_w053 bigint(53),
  col_bigint_w054 bigint(54),
  col_bigint_w055 bigint(55),
  col_bigint_w056 bigint(56),
  col_bigint_w057 bigint(57),
  col_bigint_w058 bigint(58),
  col_bigint_w059 bigint(59),
  col_bigint_w060 bigint(60),
  col_bigint_w061 bigint(61),
  col_bigint_w062 bigint(62),
  col_bigint_w063 bigint(63),
  col_bigint_w064 bigint(64),
  col_bigint_w065 bigint(65),
  col_bigint_w066 bigint(66),
  col_bigint_w067 bigint(67),
  col_bigint_w068 bigint(68),
  col_bigint_w069 bigint(69),
  col_bigint_w070 bigint(70),
  col_bigint_w071 bigint(71),
  col_bigint_w072 bigint(72),
  col_bigint_w073 bigint(73),
  col_bigint_w074 bigint(74),
  col_bigint_w075 bigint(75),
  col_bigint_w076 bigint(76),
  col_bigint_w077 bigint(77),
  col_bigint_w078 bigint(78),
  col_bigint_w079 bigint(79),
  col_bigint_w080 bigint(80),
  col_bigint_w081 bigint(81),
  col_bigint_w082 bigint(82),
  col_bigint_w083 bigint(83),
  col_bigint_w084 bigint(84),
  col_bigint_w085 bigint(85),
  col_bigint_w086 bigint(86),
  col_bigint_w087 bigint(87),
  col_bigint_w088 bigint(88),
  col_bigint_w089 bigint(89),
  col_bigint_w090 bigint(90),
  col_bigint_w091 bigint(91),
  col_bigint_w092 bigint(92),
  col_bigint_w093 bigint(93),
  col_bigint_w094 bigint(94),
  col_bigint_w095 bigint(95),
  col_bigint_w096 bigint(96),
  col_bigint_w097 bigint(97),
  col_bigint_w098 bigint(98),
  col_bigint_w099 bigint(99),
  col_bigint_w100 bigint(100),
  col_bigint_w101 bigint(101),
  col_bigint_w102 bigint(102),
  col_bigint_w103 bigint(103),
  col_bigint_w104 bigint(104),
  col_bigint_w105 bigint(105),
  col_bigint_w106 bigint(106),
  col_bigint_w107 bigint(107),
  col_bigint_w108 bigint(108),
  col_bigint_w109 bigint(109),
  col_bigint_w110 bigint(110),
  col_bigint_w111 bigint(111),
  col_bigint_w112 bigint(112),
  col_bigint_w113 bigint(113),
  col_bigint_w114 bigint(114),
  col_bigint_w115 bigint(115),
  col_bigint_w116 bigint(116),
  col_bigint_w117 bigint(117),
  col_bigint_w118 bigint(118),
  col_bigint_w119 bigint(119),
  col_bigint_w120 bigint(120),
  col_bigint_w121 bigint(121),
  col_bigint_w122 bigint(122),
  col_bigint_w123 bigint(123),
  col_bigint_w124 bigint(124),
  col_bigint_w125 bigint(125),
  col_bigint_w126 bigint(126),
  col_bigint_w127 bigint(127),
  col_bigint_w128 bigint(128),
  col_bigint_w129 bigint(129),
  col_bigint_w130 bigint(130),
  col_bigint_w131 bigint(131),
  col_bigint_w132 bigint(132),
  col_bigint_w133 bigint(133),
  col_bigint_w134 bigint(134),
  col_bigint_w135 bigint(135),
  col_bigint_w136 bigint(136),
  col_bigint_w137 bigint(137),
  col_bigint_w138 bigint(138),
  col_bigint_w139 bigint(139),
  col_bigint_w140 bigint(140),
  col_bigint_w141 bigint(141),
  col_bigint_w142 bigint(142),
  col_bigint_w143 bigint(143),
  col_bigint_w144 bigint(144),
  col_bigint_w145 bigint(145),
  col_bigint_w146 bigint(146),
  col_bigint_w147 bigint(147),
  col_bigint_w148 bigint(148),
  col_bigint_w149 bigint(149),
  col_bigint_w150 bigint(150),
  col_bigint_w151 bigint(151),
  col_bigint_w152 bigint(152),
  col_bigint_w153 bigint(153),
  col_bigint_w154 bigint(154),
  col_bigint_w155 bigint(155),
  col_bigint_w156 bigint(156),
  col_bigint_w157 bigint(157),
  col_bigint_w158 bigint(158),
  col_bigint_w159 bigint(159),
  col_bigint_w160 bigint(160),
  col_bigint_w161 bigint(161),
  col_bigint_w162 bigint(162),
  col_bigint_w163 bigint(163),
  col_bigint_w164 bigint(164),
  col_bigint_w165 bigint(165),
  col_bigint_w166 bigint(166),
  col_bigint_w167 bigint(167),
  col_bigint_w168 bigint(168),
  col_bigint_w169 bigint(169),
  col_bigint_w170 bigint(170),
  col_bigint_w171 bigint(171),
  col_bigint_w172 bigint(172),
  col_bigint_w173 bigint(173),
  col_bigint_w174 bigint(174),
  col_bigint_w175 bigint(175),
  col_bigint_w176 bigint(176),
  col_bigint_w177 bigint(177),
  col_bigint_w178 bigint(178),
  col_bigint_w179 bigint(179),
  col_bigint_w180 bigint(180),
  col_bigint_w181 bigint(181),
  col_bigint_w182 bigint(182),
  col_bigint_w183 bigint(183),
  col_bigint_w184 bigint(184),
  col_bigint_w185 bigint(185),
  col_bigint_w186 bigint(186),
  col_bigint_w187 bigint(187),
  col_bigint_w188 bigint(188),
  col_bigint_w189 bigint(189),
  col_bigint_w190 bigint(190),
  col_bigint_w191 bigint(191),
  col_bigint_w192 bigint(192),
  col_bigint_w193 bigint(193),
  col_bigint_w194 bigint(194),
  col_bigint_w195 bigint(195),
  col_bigint_w196 bigint(196),
  col_bigint_w197 bigint(197),
  col_bigint_w198 bigint(198),
  col_bigint_w199 bigint(199),
  col_bigint_w200 bigint(200),
  col_bigint_w201 bigint(201),
  col_bigint_w202 bigint(202),
  col_bigint_w203 bigint(203),
  col_bigint_w204 bigint(204),
  col_bigint_w205 bigint(205),
  col_bigint_w206 bigint(206),
  col_bigint_w207 bigint(207),
  col_bigint_w208 bigint(208),
  col_bigint_w209 bigint(209),
  col_bigint_w210 bigint(210),
  col_bigint_w211 bigint(211),
  col_bigint_w212 bigint(212),
  col_bigint_w213 bigint(213),
  col_bigint_w214 bigint(214),
  col_bigint_w215 bigint(215),
  col_bigint_w216 bigint(216),
  col_bigint_w217 bigint(217),
  col_bigint_w218 bigint(218),
  col_bigint_w219 bigint(219),
  col_bigint_w220 bigint(220),
  col_bigint_w221 bigint(221),
  col_bigint_w222 bigint(222),
  col_bigint_w223 bigint(223),
  col_bigint_w224 bigint(224),
  col_bigint_w225 bigint(225),
  col_bigint_w226 bigint(226),
  col_bigint_w227 bigint(227),
  col_bigint_w228 bigint(228),
  col_bigint_w229 bigint(229),
  col_bigint_w230 bigint(230),
  col_bigint_w231 bigint(231),
  col_bigint_w232 bigint(232),
  col_bigint_w233 bigint(233),
  col_bigint_w234 bigint(234),
  col_bigint_w235 bigint(235),
  col_bigint_w236 bigint(236),
  col_bigint_w237 bigint(237),
  col_bigint_w238 bigint(238),
  col_bigint_w239 bigint(239),
  col_bigint_w240 bigint(240),
  col_bigint_w241 bigint(241),
  col_bigint_w242 bigint(242),
  col_bigint_w243 bigint(243),
  col_bigint_w244 bigint(244),
  col_bigint_w245 bigint(245),
  col_bigint_w246 bigint(246),
  col_bigint_w247 bigint(247),
  col_bigint_w248 bigint(248),
  col_bigint_w249 bigint(249),
  col_bigint_w250 bigint(250),
  col_bigint_w251 bigint(251),
  col_bigint_w252 bigint(252),
  col_bigint_w253 bigint(253),
  col_bigint_w254 bigint(254),
  col_bigint_w255 bigint(255)
) ENGINE=InnoDB;

-- 创建 DECIMAL 精度全扫测试表：DECIMAL(1,0) ~ DECIMAL(65,0)，M 从最小 1 到最大 65；DECIMAL(M,D) -> DECIMAL(M,D)
DROP TABLE IF EXISTS case_186_decimal_precision;
CREATE TABLE case_186_decimal_precision (
  col_decimal_p01 decimal(1,0),
  col_decimal_p02 decimal(2,0),
  col_decimal_p03 decimal(3,0),
  col_decimal_p04 decimal(4,0),
  col_decimal_p05 decimal(5,0),
  col_decimal_p06 decimal(6,0),
  col_decimal_p07 decimal(7,0),
  col_decimal_p08 decimal(8,0),
  col_decimal_p09 decimal(9,0),
  col_decimal_p10 decimal(10,0),
  col_decimal_p11 decimal(11,0),
  col_decimal_p12 decimal(12,0),
  col_decimal_p13 decimal(13,0),
  col_decimal_p14 decimal(14,0),
  col_decimal_p15 decimal(15,0),
  col_decimal_p16 decimal(16,0),
  col_decimal_p17 decimal(17,0),
  col_decimal_p18 decimal(18,0),
  col_decimal_p19 decimal(19,0),
  col_decimal_p20 decimal(20,0),
  col_decimal_p21 decimal(21,0),
  col_decimal_p22 decimal(22,0),
  col_decimal_p23 decimal(23,0),
  col_decimal_p24 decimal(24,0),
  col_decimal_p25 decimal(25,0),
  col_decimal_p26 decimal(26,0),
  col_decimal_p27 decimal(27,0),
  col_decimal_p28 decimal(28,0),
  col_decimal_p29 decimal(29,0),
  col_decimal_p30 decimal(30,0),
  col_decimal_p31 decimal(31,0),
  col_decimal_p32 decimal(32,0),
  col_decimal_p33 decimal(33,0),
  col_decimal_p34 decimal(34,0),
  col_decimal_p35 decimal(35,0),
  col_decimal_p36 decimal(36,0),
  col_decimal_p37 decimal(37,0),
  col_decimal_p38 decimal(38,0),
  col_decimal_p39 decimal(39,0),
  col_decimal_p40 decimal(40,0),
  col_decimal_p41 decimal(41,0),
  col_decimal_p42 decimal(42,0),
  col_decimal_p43 decimal(43,0),
  col_decimal_p44 decimal(44,0),
  col_decimal_p45 decimal(45,0),
  col_decimal_p46 decimal(46,0),
  col_decimal_p47 decimal(47,0),
  col_decimal_p48 decimal(48,0),
  col_decimal_p49 decimal(49,0),
  col_decimal_p50 decimal(50,0),
  col_decimal_p51 decimal(51,0),
  col_decimal_p52 decimal(52,0),
  col_decimal_p53 decimal(53,0),
  col_decimal_p54 decimal(54,0),
  col_decimal_p55 decimal(55,0),
  col_decimal_p56 decimal(56,0),
  col_decimal_p57 decimal(57,0),
  col_decimal_p58 decimal(58,0),
  col_decimal_p59 decimal(59,0),
  col_decimal_p60 decimal(60,0),
  col_decimal_p61 decimal(61,0),
  col_decimal_p62 decimal(62,0),
  col_decimal_p63 decimal(63,0),
  col_decimal_p64 decimal(64,0),
  col_decimal_p65 decimal(65,0)
) ENGINE=InnoDB;

-- 创建 DECIMAL 标度全扫测试表：DECIMAL(65,0) ~ DECIMAL(65,30)，D 从最小 0 到最大 30
DROP TABLE IF EXISTS case_187_decimal_scale;
CREATE TABLE case_187_decimal_scale (
  col_decimal_d00 decimal(65,0),
  col_decimal_d01 decimal(65,1),
  col_decimal_d02 decimal(65,2),
  col_decimal_d03 decimal(65,3),
  col_decimal_d04 decimal(65,4),
  col_decimal_d05 decimal(65,5),
  col_decimal_d06 decimal(65,6),
  col_decimal_d07 decimal(65,7),
  col_decimal_d08 decimal(65,8),
  col_decimal_d09 decimal(65,9),
  col_decimal_d10 decimal(65,10),
  col_decimal_d11 decimal(65,11),
  col_decimal_d12 decimal(65,12),
  col_decimal_d13 decimal(65,13),
  col_decimal_d14 decimal(65,14),
  col_decimal_d15 decimal(65,15),
  col_decimal_d16 decimal(65,16),
  col_decimal_d17 decimal(65,17),
  col_decimal_d18 decimal(65,18),
  col_decimal_d19 decimal(65,19),
  col_decimal_d20 decimal(65,20),
  col_decimal_d21 decimal(65,21),
  col_decimal_d22 decimal(65,22),
  col_decimal_d23 decimal(65,23),
  col_decimal_d24 decimal(65,24),
  col_decimal_d25 decimal(65,25),
  col_decimal_d26 decimal(65,26),
  col_decimal_d27 decimal(65,27),
  col_decimal_d28 decimal(65,28),
  col_decimal_d29 decimal(65,29),
  col_decimal_d30 decimal(65,30)
) ENGINE=InnoDB;

-- 创建 FLOAT(M,D) 宽度扫描测试表：FLOAT(1,1) ~ FLOAT(65,1)
-- (M,D) 语法在 MySQL 8.0.17+ 已弃用（仅警告）；MySQL 要求 M >= D，故 D 固定为 1；FLOAT -> REAL
DROP TABLE IF EXISTS case_188_float_widths;
CREATE TABLE case_188_float_widths (
  col_float_w01 float(1,1),
  col_float_w02 float(2,1),
  col_float_w03 float(3,1),
  col_float_w04 float(4,1),
  col_float_w05 float(5,1),
  col_float_w06 float(6,1),
  col_float_w07 float(7,1),
  col_float_w08 float(8,1),
  col_float_w09 float(9,1),
  col_float_w10 float(10,1),
  col_float_w11 float(11,1),
  col_float_w12 float(12,1),
  col_float_w13 float(13,1),
  col_float_w14 float(14,1),
  col_float_w15 float(15,1),
  col_float_w16 float(16,1),
  col_float_w17 float(17,1),
  col_float_w18 float(18,1),
  col_float_w19 float(19,1),
  col_float_w20 float(20,1),
  col_float_w21 float(21,1),
  col_float_w22 float(22,1),
  col_float_w23 float(23,1),
  col_float_w24 float(24,1),
  col_float_w25 float(25,1),
  col_float_w26 float(26,1),
  col_float_w27 float(27,1),
  col_float_w28 float(28,1),
  col_float_w29 float(29,1),
  col_float_w30 float(30,1),
  col_float_w31 float(31,1),
  col_float_w32 float(32,1),
  col_float_w33 float(33,1),
  col_float_w34 float(34,1),
  col_float_w35 float(35,1),
  col_float_w36 float(36,1),
  col_float_w37 float(37,1),
  col_float_w38 float(38,1),
  col_float_w39 float(39,1),
  col_float_w40 float(40,1),
  col_float_w41 float(41,1),
  col_float_w42 float(42,1),
  col_float_w43 float(43,1),
  col_float_w44 float(44,1),
  col_float_w45 float(45,1),
  col_float_w46 float(46,1),
  col_float_w47 float(47,1),
  col_float_w48 float(48,1),
  col_float_w49 float(49,1),
  col_float_w50 float(50,1),
  col_float_w51 float(51,1),
  col_float_w52 float(52,1),
  col_float_w53 float(53,1),
  col_float_w54 float(54,1),
  col_float_w55 float(55,1),
  col_float_w56 float(56,1),
  col_float_w57 float(57,1),
  col_float_w58 float(58,1),
  col_float_w59 float(59,1),
  col_float_w60 float(60,1),
  col_float_w61 float(61,1),
  col_float_w62 float(62,1),
  col_float_w63 float(63,1),
  col_float_w64 float(64,1),
  col_float_w65 float(65,1)
) ENGINE=InnoDB;

-- 创建 DOUBLE(M,D) 宽度扫描测试表：DOUBLE(1,1) ~ DOUBLE(65,1)
-- (M,D) 语法在 MySQL 8.0.17+ 已弃用（仅警告）；MySQL 要求 M >= D，故 D 固定为 1；DOUBLE -> DOUBLE PRECISION
DROP TABLE IF EXISTS case_189_double_widths;
CREATE TABLE case_189_double_widths (
  col_double_w01 double(1,1),
  col_double_w02 double(2,1),
  col_double_w03 double(3,1),
  col_double_w04 double(4,1),
  col_double_w05 double(5,1),
  col_double_w06 double(6,1),
  col_double_w07 double(7,1),
  col_double_w08 double(8,1),
  col_double_w09 double(9,1),
  col_double_w10 double(10,1),
  col_double_w11 double(11,1),
  col_double_w12 double(12,1),
  col_double_w13 double(13,1),
  col_double_w14 double(14,1),
  col_double_w15 double(15,1),
  col_double_w16 double(16,1),
  col_double_w17 double(17,1),
  col_double_w18 double(18,1),
  col_double_w19 double(19,1),
  col_double_w20 double(20,1),
  col_double_w21 double(21,1),
  col_double_w22 double(22,1),
  col_double_w23 double(23,1),
  col_double_w24 double(24,1),
  col_double_w25 double(25,1),
  col_double_w26 double(26,1),
  col_double_w27 double(27,1),
  col_double_w28 double(28,1),
  col_double_w29 double(29,1),
  col_double_w30 double(30,1),
  col_double_w31 double(31,1),
  col_double_w32 double(32,1),
  col_double_w33 double(33,1),
  col_double_w34 double(34,1),
  col_double_w35 double(35,1),
  col_double_w36 double(36,1),
  col_double_w37 double(37,1),
  col_double_w38 double(38,1),
  col_double_w39 double(39,1),
  col_double_w40 double(40,1),
  col_double_w41 double(41,1),
  col_double_w42 double(42,1),
  col_double_w43 double(43,1),
  col_double_w44 double(44,1),
  col_double_w45 double(45,1),
  col_double_w46 double(46,1),
  col_double_w47 double(47,1),
  col_double_w48 double(48,1),
  col_double_w49 double(49,1),
  col_double_w50 double(50,1),
  col_double_w51 double(51,1),
  col_double_w52 double(52,1),
  col_double_w53 double(53,1),
  col_double_w54 double(54,1),
  col_double_w55 double(55,1),
  col_double_w56 double(56,1),
  col_double_w57 double(57,1),
  col_double_w58 double(58,1),
  col_double_w59 double(59,1),
  col_double_w60 double(60,1),
  col_double_w61 double(61,1),
  col_double_w62 double(62,1),
  col_double_w63 double(63,1),
  col_double_w64 double(64,1),
  col_double_w65 double(65,1)
) ENGINE=InnoDB;

-- 创建 BIT 全长度测试表：BIT(1) ~ BIT(64)，每个长度一个字段
-- BIT(n<=63) -> BIGINT，BIT(64) -> NUMERIC(20,0)
DROP TABLE IF EXISTS case_190_bit_full;
CREATE TABLE case_190_bit_full (
  col_bit_01 bit(1),
  col_bit_02 bit(2),
  col_bit_03 bit(3),
  col_bit_04 bit(4),
  col_bit_05 bit(5),
  col_bit_06 bit(6),
  col_bit_07 bit(7),
  col_bit_08 bit(8),
  col_bit_09 bit(9),
  col_bit_10 bit(10),
  col_bit_11 bit(11),
  col_bit_12 bit(12),
  col_bit_13 bit(13),
  col_bit_14 bit(14),
  col_bit_15 bit(15),
  col_bit_16 bit(16),
  col_bit_17 bit(17),
  col_bit_18 bit(18),
  col_bit_19 bit(19),
  col_bit_20 bit(20),
  col_bit_21 bit(21),
  col_bit_22 bit(22),
  col_bit_23 bit(23),
  col_bit_24 bit(24),
  col_bit_25 bit(25),
  col_bit_26 bit(26),
  col_bit_27 bit(27),
  col_bit_28 bit(28),
  col_bit_29 bit(29),
  col_bit_30 bit(30),
  col_bit_31 bit(31),
  col_bit_32 bit(32),
  col_bit_33 bit(33),
  col_bit_34 bit(34),
  col_bit_35 bit(35),
  col_bit_36 bit(36),
  col_bit_37 bit(37),
  col_bit_38 bit(38),
  col_bit_39 bit(39),
  col_bit_40 bit(40),
  col_bit_41 bit(41),
  col_bit_42 bit(42),
  col_bit_43 bit(43),
  col_bit_44 bit(44),
  col_bit_45 bit(45),
  col_bit_46 bit(46),
  col_bit_47 bit(47),
  col_bit_48 bit(48),
  col_bit_49 bit(49),
  col_bit_50 bit(50),
  col_bit_51 bit(51),
  col_bit_52 bit(52),
  col_bit_53 bit(53),
  col_bit_54 bit(54),
  col_bit_55 bit(55),
  col_bit_56 bit(56),
  col_bit_57 bit(57),
  col_bit_58 bit(58),
  col_bit_59 bit(59),
  col_bit_60 bit(60),
  col_bit_61 bit(61),
  col_bit_62 bit(62),
  col_bit_63 bit(63),
  col_bit_64 bit(64)
) ENGINE=InnoDB;

-- 创建 TIME 小数秒精度全扫测试表：TIME(0) ~ TIME(6)；TIME(fsp) -> TIME(fsp)
DROP TABLE IF EXISTS case_191_time_fsp;
CREATE TABLE case_191_time_fsp (
  col_time_fsp0 time(0),
  col_time_fsp1 time(1),
  col_time_fsp2 time(2),
  col_time_fsp3 time(3),
  col_time_fsp4 time(4),
  col_time_fsp5 time(5),
  col_time_fsp6 time(6)
) ENGINE=InnoDB;

-- 创建 DATETIME 小数秒精度全扫测试表：DATETIME(0) ~ DATETIME(6)；DATETIME(fsp) -> TIMESTAMP(fsp)
DROP TABLE IF EXISTS case_192_datetime_fsp;
CREATE TABLE case_192_datetime_fsp (
  col_datetime_fsp0 datetime(0),
  col_datetime_fsp1 datetime(1),
  col_datetime_fsp2 datetime(2),
  col_datetime_fsp3 datetime(3),
  col_datetime_fsp4 datetime(4),
  col_datetime_fsp5 datetime(5),
  col_datetime_fsp6 datetime(6)
) ENGINE=InnoDB;

-- 创建 TIMESTAMP 小数秒精度全扫测试表：TIMESTAMP(0) ~ TIMESTAMP(6)；TIMESTAMP(fsp) -> TIMESTAMP(fsp)
-- 显式 NULL DEFAULT NULL 避免首个 TIMESTAMP 列的隐式默认值行为
DROP TABLE IF EXISTS case_193_timestamp_fsp;
CREATE TABLE case_193_timestamp_fsp (
  col_timestamp_fsp0 timestamp(0) NULL DEFAULT NULL,
  col_timestamp_fsp1 timestamp(1) NULL DEFAULT NULL,
  col_timestamp_fsp2 timestamp(2) NULL DEFAULT NULL,
  col_timestamp_fsp3 timestamp(3) NULL DEFAULT NULL,
  col_timestamp_fsp4 timestamp(4) NULL DEFAULT NULL,
  col_timestamp_fsp5 timestamp(5) NULL DEFAULT NULL,
  col_timestamp_fsp6 timestamp(6) NULL DEFAULT NULL
) ENGINE=InnoDB;

