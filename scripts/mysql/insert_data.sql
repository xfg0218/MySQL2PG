-- 插入数据到 case_01_integers 表
INSERT INTO case_01_integers VALUES (-1,-32768,838,2147483647,-1000,922, 123456789,-92233);

-- 插入数据到 case_02_boolean 表
INSERT INTO case_02_boolean (is_active, status, is_deleted) VALUES  (1, 1, 0);
INSERT INTO case_02_boolean (is_active, status, is_deleted) VALUES  (0, 123, 1);

-- 插入数据到 case_03_floats 表
INSERT INTO case_03_floats VALUES
(3.14, 10.5, 10.50, 2.71, 10.50, 10.50, 10.50, 3.14),
(1.5, 20.8, 20.80, 3.14, 20.80, 20.80, 20.80, 1.5),
(0.0, 0.0, 0.00, 0.0, 0.00, 0.00, 0.00, 0.0),
(123.46, 123.5, 123.45, 123.46, 123.45, 123.45, 123.45, 123.46),
(-1.1, -1.1, -1.10, -1.1, -1.10, -1.10, -1.10, -1.1),
(100.0, 100, 100.00, 100.0, 100.00, 100.00, 100.00, 100.0),
(0.5, 0.5, 0.50, 0.5, 0.50, 0.50, 0.50, 0.5),
(500.5, 500, 500.50, 500.5, 500.50, 500.50, 500.50, 500.5),
(999.9, 999, 999.90, 999.9, 999.90, 999.90, 999.90, 999.9),
(2.7, 2.7, 2.70, 2.7, 2.70, 2.70, 2.70, 2.7);

-- 插入数据到 case_10_defaults 表（测试默认值）
insert into case_10_defaults(c1) values(1);

--  插入数据到 case_11_autoincrement 表（测试自增）
insert into case_11_autoincrement(big_id,mixed_case) values(1,1);
insert into case_11_autoincrement(big_id,mixed_case) values(2,2);

-- 插入数据到 case_27_mysql8_check 表（测试检查约束，正常第二条SQL会报错）
INSERT INTO case_27_mysql8_check (id, age) VALUES (1, 25);
INSERT INTO case_27_mysql8_check (id, age) VALUES (2, 16);

-- 插入数据到 case_28_mysql8_func_index 表（测试函数索引）
INSERT INTO case_28_mysql8_func_index (name, data) 
VALUES ('alice', '{"id": 123, "city": "Beijing"}');

-- 插入数据到 case_29_mysql8_defaults 表（测试默认值）
INSERT INTO case_29_mysql8_defaults (id) VALUES ('custom-id-123');

-- 插入数据到 case_45_stored_generated 表（测试存储生成列）
INSERT INTO case_45_stored_generated (id, c1) VALUES (1, 10);

-- 插入数据到 case_59_complex_generated 表（测试复杂生成列）
INSERT INTO case_59_complex_generated (id, price, quantity, discount)  VALUES (1, 100.00, 5, 10.00);

