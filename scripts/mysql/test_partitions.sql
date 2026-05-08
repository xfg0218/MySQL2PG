-- MySQL 分区表测试用例 - 支持 5.1 到 9.0+ 所有版本
-- 测试 8 种不同的分区语法

-- 1. RANGE 分区 - 整数范围
CREATE TABLE `test_partition_01_range_int` (
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

-- 2. RANGE 分区 - TO_DAYS 日期函数
CREATE TABLE `test_partition_02_range_todays` (
  `id` int NOT NULL,
  `create_time` datetime NOT NULL,
  `content` text,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
/*!50100 PARTITION BY RANGE (TO_DAYS(create_time))
(PARTITION p202501 VALUES LESS THAN (TO_DAYS('2025-02-01')),
 PARTITION p202502 VALUES LESS THAN (TO_DAYS('2025-03-01')),
 PARTITION p_future VALUES LESS THAN MAXVALUE) */;

-- 3. RANGE 分区 - UNIX_TIMESTAMP 时间戳函数
CREATE TABLE `test_partition_03_range_unix_timestamp` (
  `id` int NOT NULL,
  `create_time` datetime NOT NULL,
  `data` json DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
/*!50100 PARTITION BY RANGE (UNIX_TIMESTAMP(create_time))
(PARTITION p202501 VALUES LESS THAN (UNIX_TIMESTAMP('2025-02-01')),
 PARTITION p202502 VALUES LESS THAN (UNIX_TIMESTAMP('2025-03-01'))) */;

-- 4. LIST 分区 - 整数列表
CREATE TABLE `test_partition_04_list_int` (
  `id` int NOT NULL,
  `status` int NOT NULL,
  `name` varchar(100) DEFAULT NULL,
  PRIMARY KEY (`id`,`status`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
/*!50100 PARTITION BY LIST (status)
(PARTITION p0 VALUES IN (0),
 PARTITION p1 VALUES IN (1),
 PARTITION p2 VALUES IN (2,3)) */;

-- 5. HASH 分区
CREATE TABLE `test_partition_05_hash` (
  `id` int NOT NULL,
  `issue_id` int NOT NULL,
  `data` text,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
/*!50100 PARTITION BY HASH(issue_id)
PARTITIONS 8 */;

-- 6. KEY 分区
CREATE TABLE `test_partition_06_key` (
  `id` int NOT NULL,
  `issue_id` int NOT NULL,
  `content` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
/*!50100 PARTITION BY KEY(issue_id)
PARTITIONS 4 */;

-- 7. RANGE + SUBPARTITION 子分区
CREATE TABLE `test_partition_07_subpartition` (
  `id` int NOT NULL,
  `issue_id` int NOT NULL,
  `performer` varchar(100) NOT NULL,
  `name` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`id`,`issue_id`,`performer`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
/*!50100 PARTITION BY RANGE (issue_id)
SUBPARTITION BY HASH(performer)
SUBPARTITIONS 2
(PARTITION p0 VALUES LESS THAN (1000),
 PARTITION p1 VALUES LESS THAN MAXVALUE) */;

-- 8. RANGE 分区 - 多分区（5 个以上分区）
CREATE TABLE `test_partition_08_range_multi` (
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
