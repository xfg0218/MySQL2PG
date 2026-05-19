--  RANGE 分区 - 整数范围
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

--  RANGE 分区 - 整数范围
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


-- LIST 分区 - 整数列表
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

-- RANGE 分区 - 多分区（5 个以上分区）
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
