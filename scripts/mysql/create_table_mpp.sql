-- 1. 普通表：不带唯一索引
drop table if exists mpp_case_normal;
create table mpp_case_normal (
    id bigint not null,
    name varchar(100),
    created_at datetime,
    primary key (id)
) engine=innodb default charset=utf8mb4;

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

-- 4. 普通索引：不应触发 distributed by
drop table if exists mpp_case_non_unique_idx;
create table mpp_case_non_unique_idx (
    id bigint not null auto_increment,
    user_id bigint not null,
    phone varchar(32),
    created_at datetime,
    primary key (id)
) engine=innodb default charset=utf8mb4;

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

-- 索引统一单独创建，便于观察表创建与索引迁移/mpp 分布键处理的先后顺序
create unique index uk_order_no on mpp_case_unique_single (order_no);
create index idx_unique_single_customer_id on mpp_case_unique_single (customer_id);

create unique index uk_tenant_biz on mpp_case_unique_multi (tenant_id, biz_no);
create index idx_unique_multi_status_created_at on mpp_case_unique_multi (status, created_at);

create index idx_user_id on mpp_case_non_unique_idx (user_id);
create index idx_non_unique_phone on mpp_case_non_unique_idx (phone);

create unique index uk_tenant_mobile on mpp_case_pk_plus_unique (tenant_code, mobile);
create index idx_pk_plus_unique_created_at on mpp_case_pk_plus_unique (created_at);

create unique index uk_multi_paths_order_no on mpp_case_multi_unique_paths (order_no);
create unique index uk_multi_paths_tenant_biz on mpp_case_multi_unique_paths (tenant_id, biz_code);
create index idx_multi_paths_mobile on mpp_case_multi_unique_paths (mobile);

create index idx_non_unique_composite_tenant_user on mpp_case_non_unique_composite (tenant_id, user_id);
create index idx_non_unique_composite_status_region on mpp_case_non_unique_composite (status, region_code);

insert into mpp_case_normal (id, name, created_at) values
(1, 'alice', now()),
(2, 'bob', now());

insert into mpp_case_unique_single (order_no, customer_id, amount, created_at) values
('ord001', 101, 88.50, now()),
('ord002', 102, 99.90, now());

insert into mpp_case_unique_multi (tenant_id, biz_no, status, created_at) values
(1, 'biz001', 1, now()),
(1, 'biz002', 1, now()),
(2, 'biz001', 0, now());

insert into mpp_case_non_unique_idx (user_id, phone, created_at) values
(1001, '13800000001', now()),
(1001, '13800000002', now());

insert into mpp_case_pk_plus_unique (id, tenant_code, mobile, nickname, created_at) values
(1, 't001', '13900000001', 'u1', now()),
(2, 't001', '13900000002', 'u2', now());

insert into mpp_case_multi_unique_paths (tenant_id, order_no, biz_code, mobile, created_at) values
(1, 'ord1001', 'biz1001', '13700000001', now()),
(1, 'ord1002', 'biz1002', '13700000002', now()),
(2, 'ord2001', 'biz2001', '13700000003', now());

insert into mpp_case_non_unique_composite (tenant_id, user_id, status, region_code, created_at) values
(1, 9001, 1, 'cn-bj', now()),
(1, 9002, 1, 'cn-sh', now()),
(2, 9001, 0, 'cn-gd', now());
