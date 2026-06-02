-- ==============================================
-- 表名: case_169_merge
-- 分区类型: RANGE 分区（单分区）
-- 分区键: issue_id (整数类型)
-- 特点说明: 
--   1. 基础 RANGE 分区示例，仅包含一个分区
--   2. 主键必须包含分区键 issue_id
--   3. 使用 ENGINE = InnoDB 指定存储引擎
--   4. 适用于按整数范围进行简单数据划分的场景
--   5. row_format=dynamic 支持动态行格式
-- ==============================================

drop table if exists case_169_merge;
create table `case_169_merge` (
  `id` int not null auto_increment,
  `issue_id` int not null,
  `isrcnumber` varchar(255) default null,
  `fsconfirm` tinyint default null,
  primary key (`id`,`issue_id`) using btree
) engine=innodb default charset=utf8mb4 row_format=dynamic
/*!50100 PARTITION BY RANGE (issue_id)
(PARTITION p0 VALUES LESS THAN (100) ENGINE = InnoDB) */
;

-- ==============================================
-- 表名: test_partition_170_range_int
-- 分区类型: RANGE 分区（多分区）
-- 分区键: issue_id (整数类型)
-- 特点说明: 
--   1. 经典 RANGE 分区模式，包含 5 个分区
--   2. 分区范围递增：1000, 2000, 3000, 10000, MAXVALUE
--   3. 使用 MAXVALUE 作为最后一个分区的边界
--   4. 主键包含分区键，满足 MySQL 分区约束
--   5. 适用于按连续整数范围均匀分布数据的场景
--   6. 常用于时间序列数据、ID 范围分片等场景
-- ==============================================
drop table if exists test_partition_170_range_int;
CREATE TABLE `test_partition_170_range_int` (
  `id` int NOT NULL,
  `issue_id` int NOT NULL,
  `name` varchar(100) DEFAULT NULL,
  PRIMARY KEY (`id`,`issue_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
/*!50100 PARTITION BY RANGE (issue_id)
(PARTITION p0 VALUES LESS THAN (1000),
 PARTITION p1 VALUES LESS THAN (2000),
 PARTITION p2 VALUES LESS THAN (3000),
 PARTITION p3 VALUES LESS THAN (10000),
 PARTITION p4 VALUES LESS THAN MAXVALUE) */;


-- ==============================================
-- 表名: test_partition_172_list_int
-- 分区类型: LIST 分区
-- 分区键: status (整数类型)
-- 特点说明: 
--   1. LIST 分区模式，按离散值列表进行分区
--   2. 分区键为状态字段，支持业务状态分类
--   3. 分区值为离散集合：p0(0), p1(1), p2(2,3)
--   4. 一个分区可包含多个离散值（如 p2 包含 2 和 3）
--   5. 主键必须包含分区键 status
--   6. 适用于按枚举值、状态码等离散值进行数据划分
--   7. 注意：LIST 分区不支持 DEFAULT 分区，插入不在列表中的值会报错
-- ==============================================
drop table if exists test_partition_172_list_int;
CREATE TABLE `test_partition_172_list_int` (
  `id` int NOT NULL,
  `status` int NOT NULL,
  `name` varchar(100) DEFAULT NULL,
  PRIMARY KEY (`id`,`status`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
/*!50100 PARTITION BY LIST (status)
(PARTITION p0 VALUES IN (0),
 PARTITION p1 VALUES IN (1),
 PARTITION p2 VALUES IN (2,3)) */;

-- ==============================================
-- 表名: test_partition_173_range_multi
-- 分区类型: RANGE 分区（非均匀分布）
-- 分区键: issue_id (整数类型)
-- 特点说明: 
--   1. RANGE 分区模式，包含 5 个分区
--   2. 分区范围呈指数增长：1000, 5000, 10000, 50000, MAXVALUE
--   3. 非均匀分区策略，适合数据分布不均匀的场景
--   4. 早期分区范围小，后期分区范围大
--   5. 包含 TEXT 类型字段，测试大字段在分区表中的兼容性
--   6. 主键包含分区键 issue_id
--   7. 适用于数据量随时间增长的业务场景
-- ==============================================
drop table if exists test_partition_173_range_multi;
CREATE TABLE `test_partition_173_range_multi` (
  `id` int NOT NULL,
  `issue_id` int NOT NULL,
  `title` varchar(255) DEFAULT NULL,
  `content` text,
  PRIMARY KEY (`id`,`issue_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
/*!50100 PARTITION BY RANGE (issue_id)
(PARTITION p0 VALUES LESS THAN (1000),
 PARTITION p1 VALUES LESS THAN (5000),
 PARTITION p2 VALUES LESS THAN (10000),
 PARTITION p3 VALUES LESS THAN (50000),
 PARTITION p4 VALUES LESS THAN MAXVALUE) */;
