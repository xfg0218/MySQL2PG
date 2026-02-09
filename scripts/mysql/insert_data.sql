-- 插入数据到 case_01_integers 表，共300,0000条数据
truncate table case_01_integers;
SET SESSION cte_max_recursion_depth = 3000000;
INSERT INTO case_01_integers (
    col_tiny, col_small, col_medium, col_int,
    col_integer, col_big, col_int_prec, col_big_prec
)
WITH RECURSIVE nums AS (
    SELECT 1 AS n
    UNION ALL
    SELECT n + 1 FROM nums WHERE n < 3000000
)
SELECT
    FLOOR(RAND(n * 1) * 255 - 128),
    FLOOR(RAND(n * 2) * 65535 - 32768),
    FLOOR(RAND(n * 3) * 16777215 - 8388608),
    FLOOR(RAND(n * 4) * 4294967295 - 2147483648),
    FLOOR(RAND(n * 5) * 4294967295 - 2147483648),
    FLOOR(RAND(n * 6) * 18446744073709551615 - 9223372036854775808),
    FLOOR(RAND(n * 7) * 4294967295 - 2147483648),
    FLOOR(RAND(n * 8) * 18446744073709551615 - 9223372036854775808)
FROM nums;


-- 插入数据到 case_02_boolean 表
truncate table case_02_boolean;
INSERT INTO case_02_boolean (is_active, status, is_deleted) VALUES  (1, 1, 0);
INSERT INTO case_02_boolean (is_active, status, is_deleted) VALUES  (0, 123, 1);

-- 插入数据到 case_03_floats 表
truncate table case_03_floats;
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
truncate table case_10_defaults;
insert into case_10_defaults(c1) values(1);

--  插入数据到 case_11_autoincrement 表（测试自增）
truncate table case_11_autoincrement; 
insert into case_11_autoincrement(big_id,mixed_case) values(1,1);
insert into case_11_autoincrement(big_id,mixed_case) values(2,2);

-- 插入数据到 case_27_mysql8_check 表（测试检查约束，正常第二条SQL会报错）
truncate table case_27_mysql8_check;
INSERT INTO case_27_mysql8_check (id, age) VALUES (1, 25);
-- INSERT INTO case_27_mysql8_check (id, age) VALUES (2, 100);

-- 插入数据到 case_28_mysql8_func_index 表（测试函数索引）
truncate table case_28_mysql8_func_index;
INSERT INTO case_28_mysql8_func_index (name, data) 
VALUES ('alice', '{"id": 123, "city": "Beijing"}');

-- 插入数据到 case_29_mysql8_defaults 表（测试默认值）
truncate table case_29_mysql8_defaults;
INSERT INTO case_29_mysql8_defaults (id) VALUES ('custom-id-123');

-- 插入数据到 case_45_stored_generated 表（测试存储生成列）
truncate table case_45_stored_generated;
INSERT INTO case_45_stored_generated (id, c1) VALUES (1, 10);

-- 插入数据到 case_59_complex_generated 表（测试复杂生成列）
truncate table case_59_complex_generated;
INSERT INTO case_59_complex_generated (id, price, quantity, discount)  VALUES (1, 100.00, 5, 10.00);

