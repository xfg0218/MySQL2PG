-- MySQL2PG 测试表定义文件
-- 包含各种MySQL语法场景，用于测试MySQL到PostgreSQL的转换功能
-- 
-- 表覆盖范围说明（case_01 ~ case_167）：
-- 1) case_01 ~ case_40：基础类型与DDL语法（数值、字符集/排序规则、JSON、时间、默认值、自增、约束、生成列、保留字、命名风格等）
-- 2) case_41 ~ case_80：索引/约束与表特性（外键、全文、空间、复合主键、存储引擎、分区、复制建表、压缩、统计信息等）
-- 3) case_81 ~ case_120：边界语法与 MySQL 5.7/8.0 常见特性（SRID、长标识符、高精度数值、多值索引、窗口函数、JSON_TABLE、锁相关语法等）
-- 4) case_121 ~ case_155：业务化建模样例
-- 5) case_156 ~ case_167：新增综合增强场景（复合外键、JSON生成列、时间类型组合、文本二进制混合、数值边界、建表方式专项）
-- 总计：167 个测试表案例

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
CREATE TABLE case_04_mb3_suffix (
  col_var_mb3 varchar(255) CHARACTER SET utf8mb4,    -- -> VARCHAR(255)
  col_char_mb3 char(10) CHARACTER SET utf8mb4,       -- -> CHAR(10)
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
) ENGINE=InnoDB COMMENT='Fulltext with ngram parser';

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
) ENGINE=ARCHIVE COMMENT='Archive storage engine for log data';

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
DROP TABLE IF EXISTS case_107_multi_delete_parent;
DROP TABLE IF EXISTS case_107_multi_delete_child;

CREATE TABLE case_107_multi_delete_parent (
  id INT PRIMARY KEY,
  name VARCHAR(50)
) ENGINE=InnoDB COMMENT='Parent table for multi-table delete';

CREATE TABLE case_107_multi_delete_child (
  id INT PRIMARY KEY,
  parent_id INT,
  value INT,
  FOREIGN KEY (parent_id) REFERENCES case_107_multi_delete_parent(id) ON DELETE CASCADE
) ENGINE=InnoDB COMMENT='Child table for multi-table delete';

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

-- 创建菜品表
DROP TABLE IF EXISTS case_155_rest_dishes;
CREATE TABLE case_155_rest_dishes (
  dish_id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY COMMENT '菜品 ID',
  dish_name VARCHAR(100) NOT NULL COMMENT '菜品名称',
  dish_code VARCHAR(50) COMMENT '菜品编码',
  category_id INT NOT NULL COMMENT '分类 ID',
  price DECIMAL(10,2) NOT NULL COMMENT '价格',
  cost_price DECIMAL(10,2) COMMENT '成本价',
  discount_price DECIMAL(10,2) COMMENT '折扣价',
  images JSON COMMENT '图片',
  ingredients TEXT COMMENT '配料',
  spice_level TINYINT DEFAULT 0 COMMENT '口味：0-不辣，1-微辣，2-中辣，3-特辣',
  is_recommend TINYINT DEFAULT 0 COMMENT '是否推荐',
  is_available TINYINT DEFAULT 1 COMMENT '是否可售',
  monthly_sales INT DEFAULT 0 COMMENT '月销量',
  total_sales INT DEFAULT 0 COMMENT '总销量',
  description TEXT COMMENT '描述',
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  UNIQUE KEY uk_dish_code (dish_code),
  INDEX idx_category_id (category_id),
  INDEX idx_is_available (is_available)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='菜品表';

-- 菜品表测试数据
INSERT INTO case_155_rest_dishes (dish_id, dish_name, dish_code, category_id, price, cost_price, spice_level, is_recommend, is_available, monthly_sales, description) VALUES
(1, '宫保鸡丁', 'DISH-001', 3, 38.00, 15.00, 2, 1, 1, 856, '经典川菜，鸡肉鲜嫩，花生香脆'),
(2, '鱼香肉丝', 'DISH-002', 3, 32.00, 12.00, 1, 1, 1, 720, '酸甜微辣，开胃下饭'),
(3, '水煮鱼', 'DISH-003', 4, 68.00, 28.00, 3, 1, 1, 650, '麻辣鲜香，鱼肉嫩滑'),
(4, '清蒸鲈鱼', 'DISH-004', 4, 88.00, 35.00, 0, 1, 1, 420, '清淡鲜美，保留原汁原味'),
(5, '麻婆豆腐', 'DISH-005', 5, 18.00, 6.00, 3, 1, 1, 980, '经典川菜，麻辣鲜香'),
(6, '炒饭', 'DISH-006', 6, 15.00, 5.00, 0, 0, 1, 1200, '粒粒分明，香气扑鼻'),
(7, '可乐', 'DISH-007', 7, 5.00, 2.50, 0, 0, 1, 2000, '冰镇可乐'),
(8, '红豆双皮奶', 'DISH-008', 8, 12.00, 4.00, 0, 1, 1, 380, '经典粤式甜品');

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

drop table if exists case_169_merge;
create table `case_169_merge` (
  `id` int not null auto_increment,
  `issue_id` int not null,
  `isrcnumber` varchar(255) default null,
  `fsconfirm` tinyint default null,
  primary key (`id`,`issue_id`) using btree
) engine=innodb default charset=utf8mb4 row_format=dynamic;