-- ==============================================
-- MySQL2PG 唯一键测试表 - 唯一索引特点说明
-- ==============================================
-- 本文件包含多种唯一键约束的测试表，用于验证MySQL到PostgreSQL迁移时
-- 唯一索引的兼容性和分布式表的处理策略
-- 
-- 唯一键类型分类及特点：
-- 
-- 1. 普通唯一索引
--    - 单列唯一索引: 约束单个字段的唯一性
--    - 复合唯一索引: 约束多个字段组合的唯一性
--    - 唯一约束: 通过 UNIQUE KEY 语法定义
-- 
-- 2. 唯一索引与分区表
--    - 分区表的唯一键必须包含分区键
--    - 全局唯一索引 vs 分区本地索引
--    - Greenplum 分布式表的唯一键约束
-- 
-- 3. 唯一索引特性
--    - NULL 值处理: MySQL允许多个NULL，PostgreSQL也允许
--    - 索引覆盖: 唯一索引可作为覆盖索引使用
--    - 隐式唯一索引: 主键自动创建唯一索引
-- 
-- 4. 分布式数据库注意事项
--    - Greenplum: 唯一键通常需要分布键包含在唯一键中
--    - 分布式唯一约束: 需要通过 distributed by 保证数据分布
--    - 全局唯一: 需要额外的序列或UUID保证
-- 
-- PostgreSQL/Greenplum迁移注意事项：
--    - UNIQUE INDEX → CREATE UNIQUE INDEX
--    - 分区表唯一键必须包含分区键
--    - 分布式表需注意数据分布策略
-- ==============================================

-- 1. 普通表：不带唯一索引
drop table if exists mpp_case_normal;
create table mpp_case_normal (
    id bigint not null,
    name varchar(100),
    created_at datetime,
    primary key (id)
) engine=innodb default charset=utf8mb4;

insert into mpp_case_normal (id, name, created_at) values
(1, 'alice', now()),
(2, 'bob', now());

-- 2. 单列唯一索引：应触发 distributed by (order_no)
drop table if exists mpp_case_unique_single;
create table mpp_case_unique_single (
    id bigint not null auto_increment,
    order_no varchar(64) not null,
    customer_id bigint,
    amount decimal(18,2),
    created_at datetime,
    primary key (id)
) engine=innodb default charset=utf8mb4;

create unique index uk_order_no on mpp_case_unique_single (order_no);
create index idx_unique_single_customer_id on mpp_case_unique_single (customer_id);

insert into mpp_case_unique_single (order_no, customer_id, amount, created_at) values
('ord001', 101, 88.50, now()),
('ord002', 102, 99.90, now());


-- 3. 多列唯一索引：应触发 distributed by (tenant_id, biz_no)
drop table if exists mpp_case_unique_multi;
create table mpp_case_unique_multi (
    id bigint not null auto_increment,
    tenant_id bigint not null,
    biz_no varchar(64) not null,
    status tinyint,
    created_at datetime,
    primary key (id)
) engine=innodb default charset=utf8mb4;

create unique index uk_tenant_biz on mpp_case_unique_multi (tenant_id, biz_no);
create index idx_unique_multi_status_created_at on mpp_case_unique_multi (status, created_at);

insert into mpp_case_unique_multi (tenant_id, biz_no, status, created_at) values
(1, 'biz001', 1, now()),
(1, 'biz002', 1, now()),
(2, 'biz001', 0, now());

-- 4. 普通索引：不应触发 distributed by
drop table if exists mpp_case_non_unique_idx;
create table mpp_case_non_unique_idx (
    id bigint not null auto_increment,
    user_id bigint not null,
    phone varchar(32),
    created_at datetime,
    primary key (id)
) engine=innodb default charset=utf8mb4;


create index idx_user_id on mpp_case_non_unique_idx (user_id);
create index idx_non_unique_phone on mpp_case_non_unique_idx (phone);

insert into mpp_case_non_unique_idx (user_id, phone, created_at) values
(1001, '13800000001', now()),
(1001, '13800000002', now());


-- 5. 主键 + 唯一索引混合：重点看唯一索引列是否进入分布键
drop table if exists mpp_case_pk_plus_unique;
create table mpp_case_pk_plus_unique (
    id bigint not null,
    tenant_code varchar(32) not null,
    mobile varchar(32) not null,
    nickname varchar(64),
    created_at datetime,
    primary key (id)
) engine=innodb default charset=utf8mb4;

create unique index uk_tenant_mobile on mpp_case_pk_plus_unique (tenant_code, mobile);
create index idx_pk_plus_unique_created_at on mpp_case_pk_plus_unique (created_at);

insert into mpp_case_pk_plus_unique (id, tenant_code, mobile, nickname, created_at) values
(1, 't001', '13900000001', 'u1', now()),
(2, 't001', '13900000002', 'u2', now());


-- 6. 多个唯一索引：验证分布键在多唯一约束场景下的调整顺序
drop table if exists mpp_case_multi_unique_paths;
create table mpp_case_multi_unique_paths (
    id bigint not null auto_increment,
    tenant_id bigint not null,
    order_no varchar(64) not null,
    biz_code varchar(64) not null,
    mobile varchar(32),
    created_at datetime,
    primary key (id)
) engine=innodb default charset=utf8mb4;

create unique index uk_multi_paths_order_no on mpp_case_multi_unique_paths (order_no);
create unique index uk_multi_paths_tenant_biz on mpp_case_multi_unique_paths (tenant_id, biz_code);
create index idx_multi_paths_mobile on mpp_case_multi_unique_paths (mobile);

insert into mpp_case_multi_unique_paths (tenant_id, order_no, biz_code, mobile, created_at) values
(1, 'ord1001', 'biz1001', '13700000001', now()),
(1, 'ord1002', 'biz1002', '13700000002', now()),
(2, 'ord2001', 'biz2001', '13700000003', now());


-- 7. 宽表普通复合索引：验证普通复合索引不会触发 distributed by
drop table if exists mpp_case_non_unique_composite;
create table mpp_case_non_unique_composite (
    id bigint not null auto_increment,
    tenant_id bigint not null,
    user_id bigint not null,
    status tinyint not null,
    region_code varchar(16),
    created_at datetime,
    primary key (id)
) engine=innodb default charset=utf8mb4;

create index idx_non_unique_composite_tenant_user on mpp_case_non_unique_composite (tenant_id, user_id);
create index idx_non_unique_composite_status_region on mpp_case_non_unique_composite (status, region_code);

insert into mpp_case_non_unique_composite (tenant_id, user_id, status, region_code, created_at) values
(1, 9001, 1, 'cn-bj', now()),
(1, 9002, 1, 'cn-sh', now()),
(2, 9001, 0, 'cn-gd', now());

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

