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
