-- MySQL2PG 测试数据插入文件
-- 为每张表插入 10 条测试数据
-- 与 create_table.sql 配合使用

-- ============================================================================
-- case_01_integers (8列: 整数类型)
-- ============================================================================
TRUNCATE TABLE case_01_integers;
INSERT INTO case_01_integers (col_tiny, col_small, col_medium, col_int, col_integer, col_big, col_int_prec, col_big_prec) VALUES
(-128, -32768, -8388608, -2147483648, -2147483648, -9223372036854775808, -100, -1000),
(-100, -20000, -5000000, -1000000000, -1000000000, -5000000000000000000, -50, -500),
(-1, -1, -1, -1, -1, -1, -1, -1),
(0, 0, 0, 0, 0, 0, 0, 0),
(1, 1, 1, 1, 1, 1, 1, 1),
(50, 500, 50000, 500000, 500000, 5000000000, 100, 1000),
(100, 10000, 1000000, 1000000000, 1000000000, 9000000000000000000, 500, 5000),
(127, 32767, 8388607, 2147483647, 2147483647, 9223372036854775807, 999, 9999),
(64, 16384, 4194304, 1073741824, 1073741824, 4611686018427387904, 256, 2560),
(-64, -16384, -4194304, -1073741824, -1073741824, -4611686018427387904, -256, -2560);

-- ============================================================================
-- case_02_boolean (3列: 布尔类型)
-- ============================================================================
TRUNCATE TABLE case_02_boolean;
INSERT INTO case_02_boolean (is_active, status, is_deleted) VALUES
(1, 1, 0), (0, 0, 1), (1, 100, 0), (0, 50, 1),
(1, 127, 0), (0, 0, 0), (1, 1, 1), (0, 50, 0),
(1, -1, 0), (0, 20, 1);

-- ============================================================================
-- case_03_floats (8列: 浮点数类型)
-- ============================================================================
TRUNCATE TABLE case_03_floats;
INSERT INTO case_03_floats (col_float, col_float_p, col_float_ps, col_double, col_double_ps, col_decimal, col_numeric, col_real) VALUES
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

-- ============================================================================
-- case_04_mb3_suffix (4列: utf8字符集)
-- ============================================================================
TRUNCATE TABLE case_04_mb3_suffix;
INSERT INTO case_04_mb3_suffix (col_var_mb3, col_char_mb3, col_text_mb3, col_mixed_mb3) VALUES
('Hello', 'World', 'This is a test text', 'Mixed1'),
('测试中文', '中文测试', '中文长文本内容', 'Mixed2'),
('ABC', 'DEF', 'Simple ASCII text', 'Mixed5'),
('数据', '测试', 'VARCHAR 文本内容扩展', 'Mixed7'),
('Multi', 'Byte', '多字节字符支持测试文本', 'Mixed8'),
('Test', 'Data', 'Additional text entry for testing', 'Mixed9'),
('Final', 'Entry', 'Last text data insertion', 'Mixed10');

-- ============================================================================
-- case_05_charsets (6列: 字符集)
-- ============================================================================
TRUNCATE TABLE case_05_charsets;
INSERT INTO case_05_charsets (c1, c2, c3, c4, c5, c6) VALUES
('utf8_val', 'utf8mb4_val', 'latin1_val', 'utf16_val', 'mb4_short', 'latin1_short'),
('Hello', 'World', 'Test', 'Data', 'Entry', 'Value'),
('123', '456', '789', '012', '345', '678'),
('A', 'B', 'C', 'D', 'E', 'F'),
('foo', 'bar', 'baz', 'qux', 'quux', 'corge'),
('test1', 'test2', 'test3', 'test4', 'test5', 'test6'),
('data1', 'data2', 'data3', 'data4', 'data5', 'data6');

-- ============================================================================
-- case_06_collates (5列: 排序规则)
-- ============================================================================
TRUNCATE TABLE case_06_collates;
INSERT INTO case_06_collates (c1, c2, c3, c4, c5) VALUES
('general_ci', 'unicode_ci', 'bin_collate', 'swedish_ci', 'ascii_ci'),
('Apple', 'apple', 'APPLE', 'Àpple', 'apple'),
('Banana', 'banana', 'BANANA', 'Bänana', 'banana'),
('Cherry', 'cherry', 'CHERRY', 'Cherry', 'cherry'),
('abc123', 'ABC123', 'abc123', 'abc123', 'abc123'),
('ZZZ', 'zzz', 'ZZZ', 'ZZZ', 'zzz'),
('aaa', 'AAA', 'aaa', 'aaa', 'AAA'),
('test', 'TEST', 'test', 'test', 'TEST'),
('data', 'DATA', 'data', 'data', 'DATA'),
('final', 'FINAL', 'final', 'final', 'FINAL');

-- ============================================================================
-- case_07_complex_charsets (3列: 复杂字符集)
-- ============================================================================
TRUNCATE TABLE case_07_complex_charsets;
INSERT INTO case_07_complex_charsets (c1, c2, c3) VALUES
('A', 'B', 'C'), ('D', 'E', 'F'), ('G', 'H', 'I'),
('J', 'K', 'L'), ('M', 'N', 'O'), ('P', 'Q', 'R'),
('S', 'T', 'U'), ('V', 'W', 'X'), ('Y', 'Z', 'A'),
('B', 'C', 'D');

-- ============================================================================
-- case_08_json (3列: JSON类型)
-- ============================================================================
TRUNCATE TABLE case_08_json;
INSERT INTO case_08_json (data, data_len, data_upper) VALUES
('{"id": 1, "name": "Alice"}', '{"len": 10}', '{"NAME": "ALICE"}'),
('{"id": 2, "name": "Bob"}', '{"len": 20}', '{"NAME": "BOB"}'),
('{"id": 3, "name": "Charlie"}', '{"len": 30}', '{"NAME": "CHARLIE"}'),
('{"key": "value", "count": 5}', '{"len": 40}', '{"KEY": "VALUE"}'),
('{"array": [1, 2, 3]}', '{"len": 50}', '{"ARRAY": [1, 2, 3]}'),
('{"nested": {"a": 1, "b": 2}}', '{"len": 60}', '{"NESTED": {"A": 1}}'),
('{"bool": true, "null": null}', '{"len": 70}', '{"BOOL": true}'),
('{"str": "hello", "num": 42}', '{"len": 80}', '{"STR": "HELLO"}'),
('{"empty": {}}', '{"len": 90}', '{"EMPTY": {}}'),
('{"full": [1, "two", true, null]}', '{"len": 100}', '{"FULL": [1, "two", true, null]}');

-- ============================================================================
-- case_09_datetime (8列: 日期时间类型)
-- ============================================================================
TRUNCATE TABLE case_09_datetime;
INSERT INTO case_09_datetime (d1, t1, t2, dt1, dt2, ts1, ts2, y1) VALUES
('2024-01-01', '08:00:00', '08:00:00.123456', '2024-01-01 08:00:00', '2024-01-01 08:00:00.123', '2024-01-01 08:00:00', '2024-01-01 08:00:00.123456', 2024),
('2024-02-15', '12:30:00', '12:30:00.654321', '2024-02-15 12:30:00', '2024-02-15 12:30:00.456', '2024-02-15 12:30:00', '2024-02-15 12:30:00.654321', 2024),
('2024-06-30', '23:59:59', '23:59:59.999999', '2024-06-30 23:59:59', '2024-06-30 23:59:59.789', '2024-06-30 23:59:59', '2024-06-30 23:59:59.999999', 2024),
('2023-12-25', '00:00:00', '00:00:00.000001', '2023-12-25 00:00:00', '2023-12-25 00:00:00.001', '2023-12-25 00:00:00', '2023-12-25 00:00:00.000001', 2023),
('2025-03-08', '06:15:30', '06:15:30.111111', '2025-03-08 06:15:30', '2025-03-08 06:15:30.222', '2025-03-08 06:15:30', '2025-03-08 06:15:30.111111', 2025),
('2024-07-04', '14:20:10', '14:20:10.222222', '2024-07-04 14:20:10', '2024-07-04 14:20:10.333', '2024-07-04 14:20:10', '2024-07-04 14:20:10.222222', 2024),
('2024-10-31', '18:45:00', '18:45:00.333333', '2024-10-31 18:45:00', '2024-10-31 18:45:00.444', '2024-10-31 18:45:00', '2024-10-31 18:45:00.333333', 2024),
('2024-11-11', '11:11:11', '11:11:11.111111', '2024-11-11 11:11:11', '2024-11-11 11:11:11.555', '2024-11-11 11:11:11', '2024-11-11 11:11:11.111111', 2024),
('2024-04-01', '09:30:45', '09:30:45.444444', '2024-04-01 09:30:45', '2024-04-01 09:30:45.666', '2024-04-01 09:30:45', '2024-04-01 09:30:45.444444', 2024),
('2024-09-09', '21:21:21', '21:21:21.555555', '2024-09-09 21:21:21', '2024-09-09 21:21:21.777', '2024-09-09 21:21:21', '2024-09-09 21:21:21.555555', 2024);

-- ============================================================================
-- case_10_defaults (6列: 默认值)
-- ============================================================================
TRUNCATE TABLE case_10_defaults;
INSERT INTO case_10_defaults (c1) VALUES (1);
INSERT INTO case_10_defaults (c1) VALUES (2);
INSERT INTO case_10_defaults (c1) VALUES (3);
INSERT INTO case_10_defaults (c1) VALUES (4);
INSERT INTO case_10_defaults (c1) VALUES (5);
INSERT INTO case_10_defaults (c1) VALUES (6);
INSERT INTO case_10_defaults (c1) VALUES (7);
INSERT INTO case_10_defaults (c1) VALUES (8);
INSERT INTO case_10_defaults (c1) VALUES (9);
INSERT INTO case_10_defaults (c1) VALUES (10);

-- ============================================================================
-- case_11_autoincrement (3列: 自增)
-- ============================================================================
TRUNCATE TABLE case_11_autoincrement;
INSERT INTO case_11_autoincrement (big_id, mixed_case) VALUES
(100, 1), (200, 2), (300, 3), (400, 4), (500, 5),
(600, 6), (700, 7), (800, 8), (900, 9), (1000, 10);

-- ============================================================================
-- case_12_unsigned (4列: 无符号类型)
-- ============================================================================
TRUNCATE TABLE case_12_unsigned;
INSERT INTO case_12_unsigned (c1, c2, c3, c4) VALUES
(0, 0, 0, 0), (100, 100, 100, 100), (1000, 1000, 1000, 1000),
(10000, 10000, 10000, 10000), (100000, 100000, 100000, 100000),
(1000000, 1000000, 1000000, 1000000), (10000000, 10000000, 10000000, 10000000),
(100000000, 100000000, 100000000, 100000000), (1000000000, 1000000000, 1000000000, 1000000000),
(2147483647, 2147483647, 2147483647, 2147483647);

-- ============================================================================
-- case_13_enum_set (2列: 枚举和集合)
-- ============================================================================
TRUNCATE TABLE case_13_enum_set;
INSERT INTO case_13_enum_set (e1, s1) VALUES
('a', 'x'), ('b', 'y'), ('c', 'z'), ('a', 'x,y'), ('b', 'y,z'),
('c', 'x,z'), ('a', 'x,y,z'), ('b', 'x'), ('c', 'y'), ('a', 'z');

-- ============================================================================
-- case_14_binary (6列: 二进制类型)
-- ============================================================================
TRUNCATE TABLE case_14_binary;
INSERT INTO case_14_binary (b1, b2, b3, b4, b5, b6) VALUES
(UNHEX('48656C6C6F'), UNHEX('576F726C64'), 'blob1', 'longblob1', 'mediumblob1', 'tinyblob1'),
(UNHEX('54657374'), UNHEX('44617461'), 'blob2', 'longblob2', 'mediumblob2', 'tinyblob2'),
(UNHEX('4279746573'), UNHEX('4279746573'), 'blob3', 'longblob3', 'mediumblob3', 'tinyblob3'),
(UNHEX('42696E617279'), UNHEX('44617461'), 'blob4', 'longblob4', 'mediumblob4', 'tinyblob4'),
(UNHEX('303130323033'), UNHEX('313233343536'), 'blob5', 'longblob5', 'mediumblob5', 'tinyblob5'),
(UNHEX('414243444546'), UNHEX('464544434241'), 'blob6', 'longblob6', 'mediumblob6', 'tinyblob6'),
(UNHEX('616263646566'), UNHEX('666564636261'), 'blob7', 'longblob7', 'mediumblob7', 'tinyblob7'),
(UNHEX('31323334353637383930'), UNHEX('30393837363534333231'), 'blob8', 'longblob8', 'mediumblob8', 'tinyblob8'),
(X'48656C6C6F', X'576F726C64', 'blob9', 'longblob9', 'mediumblob9', 'tinyblob9'),
(X'54657374', X'44617461', 'blob10', 'longblob10', 'mediumblob10', 'tinyblob10');

-- ============================================================================
-- case_15_options (1列: 表选项)
-- ============================================================================
TRUNCATE TABLE case_15_options;
INSERT INTO case_15_options (id) VALUES (1), (2), (3), (4), (5), (6), (7), (8), (9), (10);

-- ============================================================================
-- case_16_partition (2列: 分区)
-- ============================================================================
TRUNCATE TABLE case_16_partition;
INSERT INTO case_16_partition (id, created_at) VALUES
(1, '2019-01-01'), (2, '2019-06-15'), (3, '2019-12-31'),
(4, '2020-01-01'), (5, '2020-06-15'), (6, '2020-12-31'),
(7, '2018-05-20'), (8, '2019-08-08'), (9, '2020-03-15'), (10, '2019-11-11');

-- ============================================================================
-- case_19_comments (4列: 注释)
-- ============================================================================
TRUNCATE TABLE case_19_comments;
INSERT INTO case_19_comments (c1, c2, c3, c4) VALUES
(1, 2, 3, 4), (5, 6, 7, 8), (9, 10, 11, 12),
(13, 14, 15, 16), (17, 18, 19, 20), (21, 22, 23, 24),
(25, 26, 27, 28), (29, 30, 31, 32), (33, 34, 35, 36), (37, 38, 39, 40);

-- ============================================================================
-- case_20_constraints (3列: 约束)
-- ============================================================================
TRUNCATE TABLE case_20_constraints;
INSERT INTO case_20_constraints (id, name) VALUES
(1, 'Alice'), (2, 'Bob'), (3, 'Charlie'), (4, 'David'), (5, 'Eve'),
(6, 'Frank'), (7, 'Grace'), (8, 'Henry'), (9, 'Ivy'), (10, 'Jack');

-- ============================================================================
-- case_21_virtual (3列: 虚拟列)
-- ============================================================================
TRUNCATE TABLE case_21_virtual;
INSERT INTO case_21_virtual (id, c1) VALUES
(1, 10), (2, 20), (3, 30), (4, 40), (5, 50),
(6, 60), (7, 70), (8, 80), (9, 90), (10, 100);

-- ============================================================================
-- case_23_weird_syntax (5列: 怪异语法)
-- ============================================================================
TRUNCATE TABLE case_23_weird_syntax;
INSERT INTO case_23_weird_syntax (c1, c2, c3, c4, c5) VALUES
(1, 1.5, 'abc', 100, 1), (2, 2.5, 'def', 200, 0), (3, 3.5, 'ghi', 300, 1),
(4, 4.5, 'jkl', 400, 0), (5, 5.5, 'mno', 500, 1), (6, 6.5, 'pqr', 600, 0),
(7, 7.5, 'stu', 700, 1), (8, 8.5, 'vwx', 800, 0), (9, 9.5, 'yza', 900, 1), (10, 10.5, 'bcd', 1000, 0);

-- ============================================================================
-- case_24_edge_cases (6列: 边缘情况)
-- ============================================================================
TRUNCATE TABLE case_24_edge_cases;
INSERT INTO case_24_edge_cases (c1, c2, c3, c5, c6) VALUES
('text1', 'varchar1', 1, 1.5, 'blob1'), ('text2', 'varchar2', 2, 2.5, 'blob2'),
('text3', 'varchar3', 3, 3.5, 'blob3'), ('text4', 'varchar4', 4, 4.5, 'blob4'),
('text5', 'varchar5', 5, 5.5, 'blob5'), ('text6', 'varchar6', 6, 6.5, 'blob6'),
('text7', 'varchar7', 7, 7.5, 'blob7'), ('text8', 'varchar8', 8, 8.5, 'blob8'),
('text9', 'varchar9', 9, 9.5, 'blob9'), ('text10', 'varchar10', 10, 10.5, 'blob10');

-- ============================================================================
-- case_25_mysql8_reserved (8列: 保留字)
-- ============================================================================
TRUNCATE TABLE case_25_mysql8_reserved;
INSERT INTO case_25_mysql8_reserved (id, `rank`, `system`, `groups`, `window`, `function`, `role`, `admin`) VALUES
(1, 1, 'sys1', 'grp1', 'win1', 1, 'role1', true),
(2, 2, 'sys2', 'grp2', 'win2', 2, 'role2', false),
(3, 3, 'sys3', 'grp3', 'win3', 3, 'role3', true),
(4, 4, 'sys4', 'grp4', 'win4', 4, 'role4', false),
(5, 5, 'sys5', 'grp5', 'win5', 5, 'role5', true),
(6, 6, 'sys6', 'grp6', 'win6', 6, 'role6', false),
(7, 7, 'sys7', 'grp7', 'win7', 7, 'role7', true),
(8, 8, 'sys8', 'grp8', 'win8', 8, 'role8', false),
(9, 9, 'sys9', 'grp9', 'win9', 9, 'role9', true),
(10, 10, 'sys10', 'grp10', 'win10', 10, 'role10', false);

-- ============================================================================
-- case_26_mysql8_invisible (3列: 不可见列)
-- ============================================================================
TRUNCATE TABLE case_26_mysql8_invisible;
INSERT INTO case_26_mysql8_invisible (id, c1, c2) VALUES
(1, 10, 20), (2, 30, 40), (3, 50, 60), (4, 70, 80), (5, 90, 100),
(6, 110, 120), (7, 130, 140), (8, 150, 160), (9, 170, 180), (10, 190, 200);

-- ============================================================================
-- case_27_mysql8_check (2列: 检查约束)
-- ============================================================================
TRUNCATE TABLE case_27_mysql8_check;
INSERT INTO case_27_mysql8_check (id, age) VALUES
(1, 20), (2, 25), (3, 30), (4, 35), (5, 40),
(6, 45), (7, 50), (8, 55), (9, 60), (10, 65);

-- ============================================================================
-- case_28_mysql8_func_index (2列: 函数索引)
-- ============================================================================
TRUNCATE TABLE case_28_mysql8_func_index;
INSERT INTO case_28_mysql8_func_index (name, data) VALUES
('alice', '{"id": 1, "city": "Beijing"}'), ('bob', '{"id": 2, "city": "Shanghai"}'),
('charlie', '{"id": 3, "city": "Guangzhou"}'), ('david', '{"id": 4, "city": "Shenzhen"}'),
('eve', '{"id": 5, "city": "Hangzhou"}'), ('frank', '{"id": 6, "city": "Chengdu"}'),
('grace', '{"id": 7, "city": "Wuhan"}'), ('henry', '{"id": 8, "city": "Nanjing"}'),
('ivy', '{"id": 9, "city": "Tianjin"}'), ('jack', '{"id": 10, "city": "Suzhou"}');

-- ============================================================================
-- case_29_mysql8_defaults (3列: MySQL 8默认值)
-- ============================================================================
TRUNCATE TABLE case_29_mysql8_defaults;
INSERT INTO case_29_mysql8_defaults (id, val, j) VALUES
('id1', 1, '{"key": "val1"}'), ('id2', 2, '{"key": "val2"}'),
('id3', 3, '{"key": "val3"}'), ('id4', 4, '{"key": "val4"}'),
('id5', 5, '{"key": "val5"}'), ('id6', 6, '{"key": "val6"}'),
('id7', 7, '{"key": "val7"}'), ('id8', 8, '{"key": "val8"}'),
('id9', 9, '{"key": "val9"}'), ('id10', 10, '{"key": "val10"}');

-- ============================================================================
-- case_30_mysql8_collations (3列: 排序规则)
-- ============================================================================
TRUNCATE TABLE case_30_mysql8_collations;
INSERT INTO case_30_mysql8_collations (c1, c2, c3) VALUES
('Alice', 'alice', 'Alice'), ('Bob', 'bob', 'Bob'),
('Charlie', 'charlie', 'Charlie'), ('David', 'david', 'David'),
('Eve', 'eve', 'Eve'), ('Frank', 'frank', 'Frank'),
('Grace', 'grace', 'Grace'), ('Henry', 'henry', 'Henry'),
('Ivy', 'ivy', 'Ivy'), ('Jack', 'jack', 'Jack');

-- ============================================================================
-- case_31_sys_utf8 (3列: 系统表)
-- ============================================================================
TRUNCATE TABLE case_31_sys_utf8;
INSERT INTO case_31_sys_utf8 (Host, Db, User) VALUES
('localhost', 'test_db', 'root'), ('127.0.0.1', 'test_db', 'root'),
('localhost', 'test_db', 'admin'), ('%', 'test_db', 'user1'),
('localhost', 'mysql', 'root'), ('127.0.0.1', 'mysql', 'admin'),
('localhost', 'app_db', 'app_user'), ('%', 'app_db', 'app_user'),
('10.0.0.1', 'test_db', 'remote_user'), ('localhost', 'test_db', 'local_user');

-- ============================================================================
-- case_32_complex_generated (2列: 复杂生成列)
-- ============================================================================
TRUNCATE TABLE case_32_complex_generated;
INSERT INTO case_32_complex_generated (cost_name) VALUES
('io_block_read_cost'), ('cpu_per_row_cost'),
('memory_per_join_cost'), ('disk_access_cost'),
('network_cost'), ('io_block_read_cost'),
('cpu_per_row_cost'), ('memory_per_join_cost'),
('disk_access_cost'), ('network_cost');

-- ============================================================================
-- case_33_desc_index (3列: 降序索引)
-- ============================================================================
TRUNCATE TABLE case_33_desc_index;
INSERT INTO case_33_desc_index (Host, User, Password_timestamp) VALUES
('localhost', 'root', '2024-01-01 00:00:00.000000'), ('127.0.0.1', 'root', '2024-02-15 12:30:00.000000'),
('localhost', 'admin', '2024-03-20 08:00:00.000000'), ('%', 'user1', '2024-04-10 15:45:00.000000'),
('localhost', 'user2', '2024-05-05 09:15:00.000000'), ('10.0.0.1', 'remote', '2024-06-01 18:30:00.000000'),
('localhost', 'local', '2024-07-07 07:07:07.000000'), ('%', 'guest', '2024-08-08 08:08:08.000000'),
('localhost', 'test', '2024-09-09 09:09:09.000000'), ('127.0.0.1', 'dev', '2024-10-10 10:10:10.000000');

-- ============================================================================
-- case_34_table_options (1列: 表选项)
-- ============================================================================
TRUNCATE TABLE case_34_table_options;
INSERT INTO case_34_table_options (id) VALUES (1), (2), (3), (4), (5), (6), (7), (8), (9), (10);

-- ============================================================================
-- case_35_enum_charset (2列: 枚举字符集)
-- ============================================================================
TRUNCATE TABLE case_35_enum_charset;
INSERT INTO case_35_enum_charset (col_enum, col_set) VALUES
('N', ''), ('Y', 'A'), ('N', 'B'), ('Y', 'A,B'), ('N', ''),
('Y', 'A'), ('N', 'B'), ('Y', 'A,B'), ('N', 'A'), ('Y', 'B');

-- ============================================================================
-- case_36_UPPERCASE (5列: 大写表名)
-- ============================================================================
TRUNCATE TABLE `CASE_36_UPPERCASE`;
INSERT INTO `CASE_36_UPPERCASE` (`ID`, `NAME`, `AGE`, `EMAIL`, `CREATE_DATE`) VALUES
(1, 'Alice', 25, 'alice@example.com', '2024-01-01'), (2, 'Bob', 30, 'bob@example.com', '2024-01-02'),
(3, 'Charlie', 35, 'charlie@example.com', '2024-01-03'), (4, 'David', 28, 'david@example.com', '2024-01-04'),
(5, 'Eve', 22, 'eve@example.com', '2024-01-05'), (6, 'Frank', 40, 'frank@example.com', '2024-01-06'),
(7, 'Grace', 27, 'grace@example.com', '2024-01-07'), (8, 'Henry', 33, 'henry@example.com', '2024-01-08'),
(9, 'Ivy', 29, 'ivy@example.com', '2024-01-09'), (10, 'Jack', 31, 'jack@example.com', '2024-01-10');

-- ============================================================================
-- case_37_HUMP (6列: 驼峰表名)
-- ============================================================================
TRUNCATE TABLE `CASE_37_HUMP`;
INSERT INTO `CASE_37_HUMP` (`ProductId`, `ProductName`, `Price`, `Stock`, `Category`, `LastUpdate`) VALUES
(1, 'iPhone', 5999.00, 100, 'Electronics', '2024-01-01'), (2, 'MacBook', 9999.00, 50, 'Electronics', '2024-01-02'),
(3, 'iPad', 3999.00, 80, 'Electronics', '2024-01-03'), (4, 'AirPods', 1299.00, 200, 'Accessories', '2024-01-04'),
(5, 'Watch', 2999.00, 60, 'Wearables', '2024-01-05'), (6, 'TV', 4999.00, 30, 'Home', '2024-01-06'),
(7, 'Mouse', 199.00, 500, 'Accessories', '2024-01-07'), (8, 'Keyboard', 399.00, 300, 'Accessories', '2024-01-08'),
(9, 'Monitor', 1999.00, 40, 'Electronics', '2024-01-09'), (10, 'Desk', 899.00, 20, 'Furniture', '2024-01-10');

-- ============================================================================
-- case_38_SNAKE (6列: 蛇形表名)
-- ============================================================================
TRUNCATE TABLE `CASE_38_SNAKE`;
INSERT INTO `CASE_38_SNAKE` (`product_id`, `product_name`, `price`, `stock`, `category`, `last_update`) VALUES
(1, 'Product A', 100.00, 50, 'Category A', '2024-01-01'), (2, 'Product B', 200.00, 30, 'Category B', '2024-01-02'),
(3, 'Product C', 150.00, 40, 'Category A', '2024-01-03'), (4, 'Product D', 300.00, 20, 'Category C', '2024-01-04'),
(5, 'Product E', 250.00, 60, 'Category B', '2024-01-05'), (6, 'Product F', 180.00, 70, 'Category A', '2024-01-06'),
(7, 'Product G', 220.00, 35, 'Category C', '2024-01-07'), (8, 'Product H', 160.00, 45, 'Category B', '2024-01-08'),
(9, 'Product I', 280.00, 25, 'Category A', '2024-01-09'), (10, 'Product J', 320.00, 15, 'Category C', '2024-01-10');

-- ============================================================================
-- case_39_UNDERSCORE (6列: 下划线表名)
-- ============================================================================
TRUNCATE TABLE `CASE_39_UNDERSCORE`;
INSERT INTO `CASE_39_UNDERSCORE` (`product_id`, `product_name`, `price`, `stock`, `category`, `last_update`) VALUES
(1, 'Item 1', 10.00, 100, 'Cat1', '2024-01-01'), (2, 'Item 2', 20.00, 200, 'Cat2', '2024-01-02'),
(3, 'Item 3', 30.00, 300, 'Cat3', '2024-01-03'), (4, 'Item 4', 40.00, 400, 'Cat1', '2024-01-04'),
(5, 'Item 5', 50.00, 500, 'Cat2', '2024-01-05'), (6, 'Item 6', 60.00, 600, 'Cat3', '2024-01-06'),
(7, 'Item 7', 70.00, 700, 'Cat1', '2024-01-07'), (8, 'Item 8', 80.00, 800, 'Cat2', '2024-01-08'),
(9, 'Item 9', 90.00, 900, 'Cat3', '2024-01-09'), (10, 'Item 10', 100.00, 1000, 'Cat1', '2024-01-10');

-- ============================================================================
-- case_40_DEFAULT (4列: 默认值)
-- ============================================================================
TRUNCATE TABLE `CASE_40_DEFAULT`;
INSERT INTO `CASE_40_DEFAULT` (`id`, `name`, `age`, `email`) VALUES
(1, 'unknown', 0, 'unknown@example.com'), (2, 'Alice', 25, 'alice@example.com'),
(3, 'Bob', 30, 'bob@example.com'), (4, 'unknown', 0, 'unknown@example.com'),
(5, 'Charlie', 35, 'charlie@example.com'), (6, 'unknown', 0, 'unknown@example.com'),
(7, 'David', 28, 'david@example.com'), (8, 'Eve', 22, 'eve@example.com'),
(9, 'unknown', 0, 'unknown@example.com'), (10, 'Frank', 40, 'frank@example.com');

-- ============================================================================
-- case_41_parent (2列: 父表)
-- ============================================================================
/**
TRUNCATE TABLE case_41_foreign_key;
TRUNCATE TABLE case_41_parent;
INSERT INTO case_41_parent (id, name) VALUES
(1, 'Parent 1'), (2, 'Parent 2'), (3, 'Parent 3'), (4, 'Parent 4'), (5, 'Parent 5'),
(6, 'Parent 6'), (7, 'Parent 7'), (8, 'Parent 8'), (9, 'Parent 9'), (10, 'Parent 10');
**/

-- ============================================================================
-- case_41_foreign_key (3列: 外键)
-- ============================================================================
/**
SET FOREIGN_KEY_CHECKS = 0;
TRUNCATE TABLE `case_41_parent`;
TRUNCATE TABLE `case_41_foreign_key`;
SET FOREIGN_KEY_CHECKS = 1;
insert into case_41_parent (id, name) values (1, '父级数据a');
insert into case_41_parent (id, name) values (2, '父级数据b');
insert into case_41_foreign_key (id, parent_id, name) values (101, 1, '子级数据a-1');
insert into case_41_foreign_key (id, parent_id, name) values (102, 2, '子级数据b-1');
**/

-- ============================================================================
-- case_42_fulltext (3列: 全文索引)
-- ============================================================================
TRUNCATE TABLE case_42_fulltext;
INSERT INTO case_42_fulltext (id, title, content) VALUES
(1, 'First Post', 'This is the first post content'), (2, 'Second Post', 'Another interesting post'),
(3, 'Third Post', 'Database migration testing'), (4, 'MySQL Guide', 'Complete guide to MySQL'),
(5, 'PG Tips', 'PostgreSQL best practices'), (6, 'Data Sync', 'How to sync data between databases'),
(7, 'Migration Tools', 'Comparison of migration tools'), (8, 'Performance', 'Database performance optimization'),
(9, 'Testing', 'Integration testing strategies'), (10, 'Deployment', 'Database deployment guide');

-- ============================================================================
-- case_43_spatial_index (2列: 空间索引)
-- ============================================================================
TRUNCATE TABLE case_43_spatial_index;
INSERT INTO case_43_spatial_index (id, location) VALUES
(1, POINT(116.4074, 39.9042)), (2, POINT(121.4737, 31.2304)),
(3, POINT(113.2644, 23.1291)), (4, POINT(114.0579, 22.5431)),
(5, POINT(120.1551, 30.2741)), (6, POINT(104.0665, 30.5723)),
(7, POINT(117.2008, 39.0842)), (8, POINT(108.9402, 34.3416)),
(9, POINT(106.5504, 29.5630)), (10, POINT(120.3818, 36.0671));

-- ============================================================================
-- case_44_composite_pk (3列: 复合主键)
-- ============================================================================
TRUNCATE TABLE case_44_composite_pk;
INSERT INTO case_44_composite_pk (id1, id2, name) VALUES
(1, 1, 'Row 1-1'), (1, 2, 'Row 1-2'), (2, 1, 'Row 2-1'), (2, 2, 'Row 2-2'),
(3, 1, 'Row 3-1'), (3, 2, 'Row 3-2'), (4, 1, 'Row 4-1'), (4, 2, 'Row 4-2'),
(5, 1, 'Row 5-1'), (5, 2, 'Row 5-2');

-- ============================================================================
-- case_45_stored_generated (4列: 存储生成列)
-- ============================================================================
TRUNCATE TABLE case_45_stored_generated;
INSERT INTO case_45_stored_generated (id, c1) VALUES
(1, 10), (2, 20), (3, 30), (4, 40), (5, 50),
(6, 60), (7, 70), (8, 80), (9, 90), (10, 100);

-- ============================================================================
-- case_46_myisam (2列: MyISAM)
-- ============================================================================
TRUNCATE TABLE case_46_myisam;
INSERT INTO case_46_myisam (id, name) VALUES
(1, 'MyISAM 1'), (2, 'MyISAM 2'), (3, 'MyISAM 3'), (4, 'MyISAM 4'), (5, 'MyISAM 5'),
(6, 'MyISAM 6'), (7, 'MyISAM 7'), (8, 'MyISAM 8'), (9, 'MyISAM 9'), (10, 'MyISAM 10');

-- ============================================================================
-- case_47_memory (2列: MEMORY)
-- ============================================================================
TRUNCATE TABLE case_47_memory;
INSERT INTO case_47_memory (id, name) VALUES
(1, 'Mem 1'), (2, 'Mem 2'), (3, 'Mem 3'), (4, 'Mem 4'), (5, 'Mem 5'),
(6, 'Mem 6'), (7, 'Mem 7'), (8, 'Mem 8'), (9, 'Mem 9'), (10, 'Mem 10');

-- ============================================================================
-- case_48_index_types (4列: 索引类型)
-- ============================================================================
TRUNCATE TABLE case_48_index_types;
INSERT INTO case_48_index_types (id, name, value) VALUES
(1, 'Name 1', 100), (2, 'Name 2', 200), (3, 'Name 3', 300), (4, 'Name 4', 400),
(5, 'Name 5', 500), (6, 'Name 6', 600), (7, 'Name 7', 700), (8, 'Name 8', 800),
(9, 'Name 9', 900), (10, 'Name 10', 1000);

-- ============================================================================
-- case_49_list_partition (2列: LIST分区)
-- ============================================================================
TRUNCATE TABLE case_49_list_partition;
INSERT INTO case_49_list_partition (id, category) VALUES
(1, 1), (2, 2), (3, 3), (4, 4), (5, 5),
(6, 6), (7, 1), (8, 2), (9, 3), (10, 4);

-- ============================================================================
-- case_50_hash_partition (2列: HASH分区)
-- ============================================================================
TRUNCATE TABLE case_50_hash_partition;
INSERT INTO case_50_hash_partition (id, name) VALUES
(1, 'Hash 1'), (2, 'Hash 2'), (3, 'Hash 3'), (4, 'Hash 4'), (5, 'Hash 5'),
(6, 'Hash 6'), (7, 'Hash 7'), (8, 'Hash 8'), (9, 'Hash 9'), (10, 'Hash 10');

-- ============================================================================
-- case_51_copy_like (同 case_01_integers)
-- ============================================================================
TRUNCATE TABLE case_51_copy_like;
INSERT INTO case_51_copy_like SELECT * FROM case_01_integers LIMIT 10;

-- ============================================================================
-- case_52_copy_as (同 case_01_integers 结构)
-- ============================================================================
TRUNCATE TABLE case_52_copy_as;
INSERT INTO case_52_copy_as SELECT * FROM case_01_integers LIMIT 10;

-- ============================================================================
-- case_53_deferred_constraint (2列: 延迟约束)
-- ============================================================================
TRUNCATE TABLE case_53_deferred_constraint;
INSERT INTO case_53_deferred_constraint (id, name) VALUES
(1, 'Deferred 1'), (2, 'Deferred 2'), (3, 'Deferred 3'), (4, 'Deferred 4'), (5, 'Deferred 5'),
(6, 'Deferred 6'), (7, 'Deferred 7'), (8, 'Deferred 8'), (9, 'Deferred 9'), (10, 'Deferred 10');

-- ============================================================================
-- case_54_tablespace (2列: 表空间)
-- ============================================================================
TRUNCATE TABLE case_54_tablespace;
INSERT INTO case_54_tablespace (id, name) VALUES
(1, 'TS 1'), (2, 'TS 2'), (3, 'TS 3'), (4, 'TS 4'), (5, 'TS 5'),
(6, 'TS 6'), (7, 'TS 7'), (8, 'TS 8'), (9, 'TS 9'), (10, 'TS 10');

-- ============================================================================
-- case_55_compressed (2列: 压缩表)
-- ============================================================================
TRUNCATE TABLE case_55_compressed;
INSERT INTO case_55_compressed (id, data) VALUES
(1, 'Compressed data 1'), (2, 'Compressed data 2'), (3, 'Compressed data 3'),
(4, 'Compressed data 4'), (5, 'Compressed data 5'), (6, 'Compressed data 6'),
(7, 'Compressed data 7'), (8, 'Compressed data 8'), (9, 'Compressed data 9'), (10, 'Compressed data 10');

-- ============================================================================
-- case_56_encrypted (2列: 加密表)
-- ============================================================================
TRUNCATE TABLE case_56_encrypted;
INSERT INTO case_56_encrypted (id, sensitive_data) VALUES
(1, 'Sensitive 1'), (2, 'Sensitive 2'), (3, 'Sensitive 3'), (4, 'Sensitive 4'), (5, 'Sensitive 5'),
(6, 'Sensitive 6'), (7, 'Sensitive 7'), (8, 'Sensitive 8'), (9, 'Sensitive 9'), (10, 'Sensitive 10');

-- ============================================================================
-- case_57_column_privileges (3列: 列权限)
-- ============================================================================
TRUNCATE TABLE case_57_column_privileges;
INSERT INTO case_57_column_privileges (id, public_data, sensitive_data) VALUES
(1, 'Public 1', 'Secret 1'), (2, 'Public 2', 'Secret 2'), (3, 'Public 3', 'Secret 3'),
(4, 'Public 4', 'Secret 4'), (5, 'Public 5', 'Secret 5'), (6, 'Public 6', 'Secret 6'),
(7, 'Public 7', 'Secret 7'), (8, 'Public 8', 'Secret 8'), (9, 'Public 9', 'Secret 9'), (10, 'Public 10', 'Secret 10');

-- ============================================================================
-- case_58_subpartition (3列: 子分区)
-- ============================================================================
TRUNCATE TABLE case_58_subpartition;
INSERT INTO case_58_subpartition (id, year, month) VALUES
(1, 2020, 1), (2, 2020, 6), (3, 2020, 12), (4, 2021, 3), (5, 2021, 7),
(6, 2021, 11), (7, 2020, 2), (8, 2021, 5), (9, 2020, 9), (10, 2021, 8);

-- ============================================================================
-- case_59_complex_generated (6列: 复杂生成列)
-- ============================================================================
TRUNCATE TABLE case_59_complex_generated;
INSERT INTO case_59_complex_generated (id, price, quantity, discount) VALUES
(1, 100.00, 5, 10.00), (2, 200.00, 10, 15.00), (3, 150.00, 8, 5.00),
(4, 300.00, 3, 20.00), (5, 250.00, 7, 10.00), (6, 180.00, 12, 8.00),
(7, 220.00, 6, 12.00), (8, 160.00, 9, 7.00), (9, 280.00, 4, 18.00), (10, 320.00, 11, 25.00);

-- ============================================================================
-- case_60_statistics (4列: 统计信息)
-- ============================================================================
TRUNCATE TABLE case_60_statistics;
INSERT INTO case_60_statistics (id, category, subcategory, value) VALUES
(1, 'Electronics', 'Phones', 1999.00), (2, 'Electronics', 'Laptops', 5999.00),
(3, 'Clothing', 'Shirts', 199.00), (4, 'Clothing', 'Pants', 299.00),
(5, 'Food', 'Fruits', 29.00), (6, 'Food', 'Vegetables', 19.00),
(7, 'Books', 'Tech', 89.00), (8, 'Books', 'Fiction', 49.00),
(9, 'Sports', 'Equipment', 599.00), (10, 'Sports', 'Apparel', 399.00);

-- ============================================================================
-- case_61_many_columns (多列类型测试)
-- ============================================================================
TRUNCATE TABLE case_61_many_columns;
INSERT INTO case_61_many_columns (
  id,
  tinyint_min, tinyint_max, smallint_min, smallint_max, mediumint_min, mediumint_max, int_min, int_max, bigint_min, bigint_max,
  float_min, float_max, double_min, double_max, decimal_min, decimal_max,
  char_min, char_max, varchar_min, varchar_max, text_min, text_max, tinytext_min, tinytext_max, mediumtext_min, mediumtext_max, longtext_min, longtext_max,
  binary_min, binary_max, varbinary_min, varbinary_max, blob_min, blob_max, tinyblob_min, tinyblob_max, mediumblob_min, mediumblob_max, longblob_min, longblob_max,
  date_col, time_col, datetime_col, timestamp_col, year_col,
  boolean_col, enum_min, enum_max, set_min, set_max, json_col
) VALUES
-- 第1条数据：常规基础数据
(1, 
 1, 127, 1, 32767, 1, 8388607, 1, 2147483647, 1, 9223372036854775807,
 0.0001, 999999.99, 0.0000001, 999999999.999999, 1, 12345.678901234567890123456789,
 'A', 'Hello', 'B', 'World', 'Text A', 'Text B', 'Tiny A', 'Tiny B', 'Medium A', 'Medium B', 'Long A', 'Long B',
 'A', 'Binary', 'B', 'VarBinary', 'Blob', 'Blob', 'Tiny', 'Tiny', 'Medium', 'Medium', 'Long', 'Long',
 '2026-06-02', '16:30:00', '2026-06-02 16:30:00', '2026-06-02 16:30:00', 2026,
 TRUE, 'a', 'a', 'x', 'x,y', '{"name": "测试1", "status": "active"}'
),
-- 第2条数据：包含极值、负数、NULL 以及 Emoji 表情
(2, 
 -128, -1, -32768, -1, -8388608, -1, -2147483648, -1, -9223372036854775808, -1,
 -999.5, -0.0001, -888.888, -0.000001, 0, 0.0000000000000000000000001,
 'Z', '你好', 'C', '支持中文', 'Text C', 'Text D', 'Tiny C', 'Tiny D', 'Medium C', 'Medium D', 'Long C', 'Long D',
 'Z', 'Bin', 'C', 'VarBin', 'BlobC', 'BlobD', 'TinyC', 'TinyD', 'MediumC', 'MediumD', 'LongC', 'LongD',
 '1990-01-01', '08:00:00', '1990-01-01 08:00:00', '1990-01-01 08:00:00', 1990,
 FALSE, 'a', 'e', 'x', 'z', '{"name": "测试2", "emoji": "🚀"}'
),
-- 第3条数据：包含 NULL 值（测试字段的可空性）
(3, 
 NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
 NULL, NULL, NULL, NULL, NULL, NULL,
 NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
 NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
 NULL, NULL, NULL, NULL, NULL,
 NULL, NULL, NULL, NULL, NULL, NULL
);
-- ============================================================================
-- case_62_various_defaults (11列: 各种默认值)
-- ============================================================================
TRUNCATE TABLE case_62_various_defaults;
INSERT INTO case_62_various_defaults (name, age, active, price, quantity, status, data, uuid) VALUES
('Alice', 25, true, 99.99, 5, 'active', '{"key": "val1"}', 'uuid-001'),
('Bob', 30, false, 199.99, 10, 'pending', '{"key": "val2"}', 'uuid-002'),
('Charlie', 35, true, 299.99, 15, 'active', '{"key": "val3"}', 'uuid-003'),
('David', 28, false, 49.99, 2, 'inactive', '{"key": "val4"}', 'uuid-004'),
('Eve', 22, true, 149.99, 8, 'active', '{"key": "val5"}', 'uuid-005'),
('Frank', 40, false, 399.99, 20, 'pending', '{"key": "val6"}', 'uuid-006'),
('Grace', 27, true, 89.99, 3, 'active', '{"key": "val7"}', 'uuid-007'),
('Henry', 33, false, 249.99, 12, 'inactive', '{"key": "val8"}', 'uuid-008'),
('Ivy', 29, true, 179.99, 7, 'active', '{"key": "val9"}', 'uuid-009'),
('Jack', 31, false, 329.99, 18, 'pending', '{"key": "val10"}', 'uuid-010');

-- ============================================================================
-- case_63_charset_collation (5列: 字符集排序规则)
-- ============================================================================
TRUNCATE TABLE case_63_charset_collation;
INSERT INTO case_63_charset_collation (id, name_en, name_zh, name_de, code) VALUES
(1, 'Alice', '爱丽丝', 'Алиса', 'A001'), (2, 'Bob', '鲍勃', 'Боб', 'B002'),
(3, 'Charlie', '查理', 'Чарли', 'C003'), (4, 'David', '大卫', 'Давид', 'D004'),
(5, 'Eve', '夏娃', 'Ева', 'E005'), (6, 'Frank', '弗兰克', 'Франк', 'F006'),
(7, 'Grace', '格蕾丝', 'Грейс', 'G007'), (8, 'Henry', '亨利', 'Генри', 'H008'),
(9, 'Ivy', '艾薇', 'Айви', 'I009'), (10, 'Jack', '杰克', 'Джек', 'J010');

-- ============================================================================
-- case_64_bit_types (6列: BIT类型)
-- ============================================================================
TRUNCATE TABLE case_64_bit_types;
INSERT INTO case_64_bit_types (id, b1, b8, b16, b32, b64) VALUES
(1, b'1', b'10101010', b'1010101010101010', b'10101010101010101010101010101010', b'1010101010101010101010101010101010101010101010101010101010101010'),
(2, b'0', b'01010101', b'0101010101010101', b'01010101010101010101010101010101', b'0101010101010101010101010101010101010101010101010101010101010101'),
(3, b'1', b'11110000', b'1111000011110000', b'11110000111100001111000011110000', b'1111000011110000111100001111000011110000111100001111000011110000'),
(4, b'0', b'00001111', b'0000111100001111', b'00001111000011110000111100001111', b'0000111100001111000011110000111100001111000011110000111100001111'),
(5, b'1', b'10101010', b'1010101010101010', b'10101010101010101010101010101010', b'1010101010101010101010101010101010101010101010101010101010101010'),
(6, b'0', b'11001100', b'1100110011001100', b'11001100110011001100110011001100', b'1100110011001100110011001100110011001100110011001100110011001100'),
(7, b'1', b'00110011', b'0011001100110011', b'00110011001100110011001100110011', b'0011001100110011001100110011001100110011001100110011001100110011'),
(8, b'0', b'11111111', b'1111111111111111', b'11111111111111111111111111111111', b'1111111111111111111111111111111111111111111111111111111111111111'),
(9, b'1', b'00000000', b'0000000000000000', b'00000000000000000000000000000000', b'0000000000000000000000000000000000000000000000000000000000000000'),
(10, b'1', b'11110000', b'1111000011110000', b'11110000111100001111000011110000', b'1111000011110000111100001111000011110000111100001111000011110000');

-- ============================================================================
-- case_65_year_types (3列: YEAR类型)
-- ============================================================================
TRUNCATE TABLE case_65_year_types;
INSERT INTO case_65_year_types (id, y4, y_default) VALUES
(1, 2020, 2020), (2, 2021, 2021), (3, 2022, 2022), (4, 2023, 2023),
(5, 2024, 2024), (6, 2025, 2025), (7, 2026, 2026), (8, 2000, 2000),
(9, 1999, 1999), (10, 2100, 2100);

-- ============================================================================
-- case_67_trigger_simulation (3列: 触发器模拟)
-- ============================================================================
TRUNCATE TABLE case_67_trigger_simulation;
INSERT INTO case_67_trigger_simulation (id, created_at, updated_at) VALUES
(1, '2024-01-01', '2024-01-01'), (2, '2024-01-02', '2024-01-03'),
(3, '2024-01-03', '2024-01-05'), (4, '2024-01-04', '2024-01-04'),
(5, '2024-01-05', '2024-01-06'), (6, '2024-01-06', '2024-01-07'),
(7, '2024-01-07', '2024-01-08'), (8, '2024-01-08', '2024-01-09'),
(9, '2024-01-09', '2024-01-10'), (10, '2024-01-10', '2024-01-11');

-- ============================================================================
-- case_68_view_simulation (3列: 视图模拟)
-- ============================================================================
TRUNCATE TABLE case_68_view_simulation;
INSERT INTO case_68_view_simulation (view_id, calc_result, summary) VALUES
(1, 100.0001, 'Summary 1'), (2, 200.0002, 'Summary 2'), (3, 300.0003, 'Summary 3'),
(4, 400.0004, 'Summary 4'), (5, 500.0005, 'Summary 5'), (6, 600.0006, 'Summary 6'),
(7, 700.0007, 'Summary 7'), (8, 800.0008, 'Summary 8'), (9, 900.0009, 'Summary 9'), (10, 1000.0010, 'Summary 10');

-- ============================================================================
-- case_69_deeply_nested_json (4列: 深层嵌套JSON)
-- ============================================================================
TRUNCATE TABLE case_69_deeply_nested_json;
INSERT INTO case_69_deeply_nested_json (id, config, tags, metadata) VALUES
(1, '{"theme": "dark", "lang": "zh"}', '["tag1", "tag2"]', '{"level": {"a": 1, "b": 2}}'),
(2, '{"theme": "light", "lang": "en"}', '["tag3", "tag4"]', '{"level": {"a": 3, "b": 4}}'),
(3, '{"theme": "auto", "lang": "ja"}', '["tag5"]', '{"level": {"a": 5, "b": 6}}'),
(4, '{"theme": "dark", "lang": "ko"}', '["tag6", "tag7", "tag8"]', '{"level": {"a": 7, "b": 8}}'),
(5, '{"theme": "light", "lang": "zh"}', '["tag9"]', '{"level": {"a": 9, "b": 10}}'),
(6, '{"theme": "dark", "lang": "en"}', '["tag10", "tag11"]', '{"level": {"a": 11, "b": 12}}'),
(7, '{"theme": "auto", "lang": "zh"}', '["tag12"]', '{"level": {"a": 13, "b": 14}}'),
(8, '{"theme": "light", "lang": "ja"}', '["tag13", "tag14"]', '{"level": {"a": 15, "b": 16}}'),
(9, '{"theme": "dark", "lang": "ko"}', '["tag15"]', '{"level": {"a": 17, "b": 18}}'),
(10, '{"theme": "auto", "lang": "en"}', '["tag16", "tag17", "tag18"]', '{"level": {"a": 19, "b": 20}}');

-- ============================================================================
-- case_70_utf8mb4_900 (3列: 排序规则)
-- ============================================================================
TRUNCATE TABLE case_70_utf8mb4_900;
INSERT INTO case_70_utf8mb4_900 (id, str1, str2) VALUES
(1, 'Apple', 'apple'), (2, 'BANANA', 'banana'), (3, 'Cherry', 'cherry'),
(4, 'Date', 'date'), (5, 'Elderberry', 'elderberry'), (6, 'Fig', 'fig'),
(7, 'Grape', 'grape'), (8, 'Honeydew', 'honeydew'), (9, 'Kiwi', 'kiwi'), (10, 'Lemon', 'lemon');

-- ============================================================================
-- case_71_functional_index_complex (3列: 函数索引)
-- ============================================================================
TRUNCATE TABLE case_71_functional_index_complex;
INSERT INTO case_71_functional_index_complex (id, first_name, last_name) VALUES
(1, 'John', 'Doe'), (2, 'Jane', 'Smith'), (3, 'Bob', 'Johnson'),
(4, 'Alice', 'Williams'), (5, 'Charlie', 'Brown'), (6, 'Diana', 'Jones'),
(7, 'Edward', 'Miller'), (8, 'Fiona', 'Davis'), (9, 'George', 'Garcia'), (10, 'Helen', 'Rodriguez');

-- ============================================================================
-- case_72_check_constraint_regex (2列: 检查约束)
-- ============================================================================
TRUNCATE TABLE case_72_check_constraint_regex;
INSERT INTO case_72_check_constraint_regex (id, email) VALUES
(1, 'user1@example.com'), (2, 'user2@example.com'), (3, 'user3@example.com'),
(4, 'user4@example.com'), (5, 'user5@example.com'), (6, 'user6@example.com'),
(7, 'user7@example.com'), (8, 'user8@example.com'), (9, 'user9@example.com'), (10, 'user10@example.com');

-- ============================================================================
-- case_73_generated_stored_mixed (4列: 混合生成列)
-- ============================================================================
TRUNCATE TABLE case_73_generated_stored_mixed;
INSERT INTO case_73_generated_stored_mixed (side_a, side_b) VALUES
(10, 20), (15, 25), (30, 40), (5, 10), (8, 12),
(20, 30), (7, 14), (25, 35), (12, 18), (40, 50);

-- ============================================================================
-- case_74_invisible_cols_mixed (3列: 不可见列)
-- ============================================================================
TRUNCATE TABLE case_74_invisible_cols_mixed;
INSERT INTO case_74_invisible_cols_mixed (id, secret_code, public_code) VALUES
(1, 'SEC001', 'PUB001'), (2, 'SEC002', 'PUB002'), (3, 'SEC003', 'PUB003'),
(4, 'SEC004', 'PUB004'), (5, 'SEC005', 'PUB005'), (6, 'SEC006', 'PUB006'),
(7, 'SEC007', 'PUB007'), (8, 'SEC008', 'PUB008'), (9, 'SEC009', 'PUB009'), (10, 'SEC010', 'PUB010');

-- ============================================================================
-- case_75_desc_primary_key (2列: 降序主键)
-- ============================================================================
TRUNCATE TABLE case_75_desc_primary_key;
INSERT INTO case_75_desc_primary_key (category_id, rank_score) VALUES
(1, 100), (1, 90), (2, 85), (2, 80), (3, 75),
(3, 70), (4, 65), (4, 60), (5, 55), (5, 50);

-- ============================================================================
-- case_76_blob_keys (2列: BLOB索引)
-- ============================================================================
TRUNCATE TABLE case_76_blob_keys;
INSERT INTO case_76_blob_keys (id, data) VALUES
(1, UNHEX('48656C6C6F')), (2, UNHEX('576F726C64')), (3, UNHEX('54657374')),
(4, UNHEX('44617461')), (5, UNHEX('4279746573')), (6, UNHEX('42696E617279')),
(7, UNHEX('303130323033')), (8, UNHEX('414243444546')), (9, UNHEX('616263646566')), (10, UNHEX('313233343536'));

-- ============================================================================
-- case_77_text_keys (2列: TEXT索引)
-- ============================================================================
TRUNCATE TABLE case_77_text_keys;
INSERT INTO case_77_text_keys (id, content) VALUES
(1, 'This is a long text content for testing'), (2, 'Another text entry for index testing'),
(3, 'MySQL to PostgreSQL migration test'), (4, 'Database sync testing scenarios'),
(5, 'Integration test data for text'), (6, 'Performance testing with text'),
(7, 'Full text search preparation'), (8, 'Text indexing and searching'),
(9, 'Large text data insertion'), (10, 'Final text test entry');

-- ============================================================================
-- case_78_multi_col_unique_null (3列: 多列唯一)
-- ============================================================================
TRUNCATE TABLE case_78_multi_col_unique_null;
INSERT INTO case_78_multi_col_unique_null (id, code, category) VALUES
(1, 'C001', 'Cat1'), (2, 'C002', 'Cat1'), (3, 'C003', 'Cat2'),
(4, 'C004', 'Cat2'), (5, 'C005', 'Cat3'), (6, 'C006', 'Cat3'),
(7, 'C007', 'Cat4'), (8, 'C008', 'Cat4'), (9, 'C009', 'Cat5'), (10, 'C010', 'Cat5');

-- ============================================================================
-- case_79_serial_default (2列: SERIAL)
-- ============================================================================
TRUNCATE TABLE case_79_serial_default;
INSERT INTO case_79_serial_default (name) VALUES
('Serial 1'), ('Serial 2'), ('Serial 3'), ('Serial 4'), ('Serial 5'),
('Serial 6'), ('Serial 7'), ('Serial 8'), ('Serial 9'), ('Serial 10');

-- ============================================================================
-- case_80_on_update_current_timestamp (2列: ON UPDATE)
-- ============================================================================
TRUNCATE TABLE case_80_on_update_current_timestamp;
INSERT INTO case_80_on_update_current_timestamp (id, modified_at) VALUES
(1, '2024-01-01'), (2, '2024-02-15'), (3, '2024-03-20'), (4, '2024-04-10'),
(5, '2024-05-05'), (6, '2024-06-01'), (7, '2024-07-07'), (8, '2024-08-08'),
(9, '2024-09-09'), (10, '2024-10-10');

-- ============================================================================
-- case_82_wide_table (11列: 宽表)
-- ============================================================================
TRUNCATE TABLE case_82_wide_table;
INSERT INTO case_82_wide_table (id, c01, c02, c03, c04, c05, c06, c07, c08, c09, c10) VALUES
(1, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10),
(2, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1),
(3, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20),
(4, 20, 19, 18, 17, 16, 15, 14, 13, 12, 11),
(5, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30),
(6, 30, 29, 28, 27, 26, 25, 24, 23, 22, 21),
(7, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40),
(8, 40, 39, 38, 37, 36, 35, 34, 33, 32, 31),
(9, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50),
(10, 50, 49, 48, 47, 46, 45, 44, 43, 42, 41);

-- ============================================================================
-- case_83_long_identifiers (2列: 长标识符)
-- ============================================================================
TRUNCATE TABLE case_83_long_identifiers;
INSERT INTO case_83_long_identifiers (this_is_a_very_long_column_name_that_reaches_limit_of_64_chars, id) VALUES
(1, 1), (2, 2), (3, 3), (4, 4), (5, 5),
(6, 6), (7, 7), (8, 8), (9, 9), (10, 10);

-- ============================================================================
-- case_84_reserved_words_quoted (4列: 保留字)
-- ============================================================================
TRUNCATE TABLE case_84_reserved_words_quoted;
INSERT INTO case_84_reserved_words_quoted (`select`, `update`, `delete`, `insert`) VALUES
(1, 2, 3, 4), (5, 6, 7, 8), (9, 10, 11, 12),
(13, 14, 15, 16), (17, 18, 19, 20), (21, 22, 23, 24),
(25, 26, 27, 28), (29, 30, 31, 32), (33, 34, 35, 36), (37, 38, 39, 40);

-- ============================================================================
-- case_85_numeric_precision_scale (3列: 高精度数值)
-- ============================================================================
TRUNCATE TABLE case_85_numeric_precision_scale;
INSERT INTO case_85_numeric_precision_scale (id, high_prec, low_scale) VALUES
(1, 123456789012345678901234567890.123456789012345678901234567890, 100),
(2, 987654321098765432109876543210.987654321098765432109876543210, 200),
(3, 111111111111111111111111111111.111111111111111111111111111111, 300),
(4, 222222222222222222222222222222.222222222222222222222222222222, 400),
(5, 333333333333333333333333333333.333333333333333333333333333333, 500),
(6, 444444444444444444444444444444.444444444444444444444444444444, 600),
(7, 555555555555555555555555555555.555555555555555555555555555555, 700),
(8, 666666666666666666666666666666.666666666666666666666666666666, 800),
(9, 777777777777777777777777777777.777777777777777777777777777777, 900),
(10, 888888888888888888888888888888.888888888888888888888888888888, 1000);

-- ============================================================================
-- case_86_zerofill_variants (3列: Zerofill)
-- ============================================================================
TRUNCATE TABLE case_86_zerofill_variants;
INSERT INTO case_86_zerofill_variants (id, z_tiny, z_big) VALUES
(1, 1, 1), (2, 12, 12), (3, 123, 123), (4, 5, 456), (5, 56, 567),
(6, 7, 678), (7, 89, 789), (8, 90, 890), (9, 100, 901), (10, 111, 1234567890123456789);

-- ============================================================================
-- case_87_float_double_unsigned (3列: 无符号浮点)
-- ============================================================================
TRUNCATE TABLE case_87_float_double_unsigned;
INSERT INTO case_87_float_double_unsigned (id, f_uns, d_uns) VALUES
(1, 1.1, 1.1), (2, 2.2, 2.2), (3, 3.3, 3.3), (4, 4.4, 4.4),
(5, 5.5, 5.5), (6, 6.6, 6.6), (7, 7.7, 7.7), (8, 8.8, 8.8),
(9, 9.9, 9.9), (10, 10.10, 10.10);

-- ============================================================================
-- case_88_year_conversion (2列: YEAR转换)
-- ============================================================================
TRUNCATE TABLE case_88_year_conversion;
INSERT INTO case_88_year_conversion (id, birth_year) VALUES
(1, 1990), (2, 1991), (3, 1992), (4, 1993), (5, 1994),
(6, 1995), (7, 1996), (8, 1997), (9, 1998), (10, 1999);

-- ============================================================================
-- case_89_national_char (3列: 国家字符)
-- ============================================================================
TRUNCATE TABLE case_89_national_char;
INSERT INTO case_89_national_char (id, nat_char, nat_varchar) VALUES
(1, 'A', 'National 1'), (2, 'B', 'National 2'), (3, 'C', 'National 3'),
(4, 'D', 'National 4'), (5, 'E', 'National 5'), (6, 'F', 'National 6'),
(7, 'G', 'National 7'), (8, 'H', 'National 8'), (9, 'I', 'National 9'), (10, 'J', 'National 10');

-- ============================================================================
-- case_90_spatial_reference (2列: 空间参考)
-- ============================================================================
TRUNCATE TABLE case_90_spatial_reference;
INSERT INTO case_90_spatial_reference (id, loc) VALUES
(1, POINT(10, 20)), (2, POINT(30, 40)), (3, POINT(50, 60)), (4, POINT(70, 80)),
(5, POINT(90, 100)), (6, POINT(110, 120)), (7, POINT(130, 140)), (8, POINT(150, 160)),
(9, POINT(170, 180)), (10, POINT(190, 200));

-- ============================================================================
-- case_91_json_array_index (2列: JSON数组索引)
-- ============================================================================
TRUNCATE TABLE case_91_json_array_index;
INSERT INTO case_91_json_array_index (id, tags) VALUES
(1, '["a", "b", "c"]'), (2, '["d", "e", "f"]'), (3, '["g", "h"]'),
(4, '["i", "j", "k"]'), (5, '["l", "m"]'), (6, '["n", "o", "p"]'),
(7, '["q", "r"]'), (8, '["s", "t", "u"]'), (9, '["v", "w"]'), (10, '["x", "y", "z"]');

-- ============================================================================
-- case_92_fulltext_ngram (2列: Ngram全文)
-- ============================================================================
TRUNCATE TABLE case_92_fulltext_ngram;
INSERT INTO case_92_fulltext_ngram (id, content) VALUES
(1, 'MySQL数据库测试文本内容'), (2, 'PostgreSQL数据库同步测试'),
(3, '数据迁移工具比较分析'), (4, '集成测试覆盖率报告'),
(5, '性能优化最佳实践'), (6, '数据库索引设计与优化'),
(7, 'SQL查询优化技巧'), (8, '分布式数据库架构设计'),
(9, '数据备份与恢复策略'), (10, '数据库安全加固指南');

-- ============================================================================
-- case_93_fulltext_parser (2列: 标准全文)
-- ============================================================================
TRUNCATE TABLE case_93_fulltext_parser;
INSERT INTO case_93_fulltext_parser (id, description) VALUES
(1, 'Standard fulltext search testing'), (2, 'Database migration and synchronization'),
(3, 'Performance optimization strategies'), (4, 'Integration test coverage'),
(5, 'SQL query optimization tips'), (6, 'Database index design principles'),
(7, 'Distributed architecture patterns'), (8, 'Backup and recovery procedures'),
(9, 'Security hardening guidelines'), (10, 'Monitoring and alerting setup');

-- ============================================================================
-- case_94_innodb_row_formats (2列: 行格式)
-- ============================================================================
TRUNCATE TABLE case_94_innodb_row_formats;
INSERT INTO case_94_innodb_row_formats (id, data) VALUES
(1, 'Data 1'), (2, 'Data 2'), (3, 'Data 3'), (4, 'Data 4'), (5, 'Data 5'),
(6, 'Data 6'), (7, 'Data 7'), (8, 'Data 8'), (9, 'Data 9'), (10, 'Data 10');

-- ============================================================================
-- case_95_union_view_table (3列: UNION视图)
-- ============================================================================
TRUNCATE TABLE case_95_union_view_table;
INSERT INTO case_95_union_view_table (id, source_type, common_field) VALUES
(1, 'Source A', 'Field A1'), (2, 'Source A', 'Field A2'), (3, 'Source B', 'Field B1'),
(4, 'Source B', 'Field B2'), (5, 'Source C', 'Field C1'), (6, 'Source C', 'Field C2'),
(7, 'Source A', 'Field A3'), (8, 'Source B', 'Field B3'), (9, 'Source C', 'Field C3'), (10, 'Source A', 'Field A4');

-- ============================================================================
-- case_96_partition_list_columns (3列: LIST COLUMNS分区)
-- ============================================================================
TRUNCATE TABLE case_96_partition_list_columns;
INSERT INTO case_96_partition_list_columns (id, region_code, store_id) VALUES
(1, 'East', 101), (2, 'NorthEast', 102), (3, 'West', 103),
(4, 'SouthWest', 104), (5, 'East', 105), (6, 'NorthEast', 106),
(7, 'West', 107), (8, 'SouthWest', 108), (9, 'East', 109), (10, 'West', 110);

-- ============================================================================
-- case_97_partition_range_columns (2列: RANGE COLUMNS分区)
-- ============================================================================
TRUNCATE TABLE case_97_partition_range_columns;
INSERT INTO case_97_partition_range_columns (id, event_date) VALUES
(1, '2019-01-01'), (2, '2019-06-15'), (3, '2019-12-31'),
(4, '2020-01-01'), (5, '2020-06-15'), (6, '2021-01-01'),
(7, '2022-01-01'), (8, '2023-01-01'), (9, '2024-01-01'), (10, '2025-01-01');

-- ============================================================================
-- case_98_partition_key (2列: KEY分区)
-- ============================================================================
TRUNCATE TABLE case_98_partition_key;
INSERT INTO case_98_partition_key (uuid, data) VALUES
('uuid-0001-0001-0001', '{"key": 1}'), ('uuid-0002-0002-0002', '{"key": 2}'),
('uuid-0003-0003-0003', '{"key": 3}'), ('uuid-0004-0004-0004', '{"key": 4}'),
('uuid-0005-0005-0005', '{"key": 5}'), ('uuid-0006-0006-0006', '{"key": 6}'),
('uuid-0007-0007-0007', '{"key": 7}'), ('uuid-0008-0008-0008', '{"key": 8}'),
('uuid-0009-0009-0009', '{"key": 9}'), ('uuid-0010-0010-0010', '{"key": 10}');

-- ============================================================================
-- case_99_partition_linear_hash (2列: LINEAR HASH分区)
-- ============================================================================
TRUNCATE TABLE case_99_partition_linear_hash;
INSERT INTO case_99_partition_linear_hash (id, val) VALUES
(1, 100), (2, 200), (3, 300), (4, 400), (5, 500),
(6, 600), (7, 700), (8, 800), (9, 900), (10, 1000);

-- ============================================================================
-- case_100_max_complexity (6列: 最大复杂性)
-- ============================================================================
TRUNCATE TABLE case_100_max_complexity;
INSERT INTO case_100_max_complexity (user_code, display_name, meta_info, created_at, is_deleted) VALUES
('A001', 'User 001', '{"level": 1}', '2024-01-01 00:00:00.000000', 0),
('A002', 'User 002', '{"level": 2}', '2024-01-02 00:00:00.000000', 0),
('A003', 'User 003', '{"level": 3}', '2024-01-03 00:00:00.000000', 1),
('A004', 'User 004', '{"level": 4}', '2024-01-04 00:00:00.000000', 0),
('A005', 'User 005', '{"level": 5}', '2024-01-05 00:00:00.000000', 0),
('A006', 'User 006', '{"level": 6}', '2024-01-06 00:00:00.000000', 1),
('A007', 'User 007', '{"level": 7}', '2024-01-07 00:00:00.000000', 0),
('A008', 'User 008', '{"level": 8}', '2024-01-08 00:00:00.000000', 0),
('A009', 'User 009', '{"level": 9}', '2024-01-09 00:00:00.000000', 1),
('A010', 'User 010', '{"level": 10}', '2024-01-10 00:00:00.000000', 0);

-- ============================================================================
-- case_101 ~ case_120 (MySQL 5.7+/8.0 语法案例)
-- ============================================================================

-- case_101_archive_engine
TRUNCATE TABLE case_101_archive_engine;
INSERT INTO case_101_archive_engine (log_data) VALUES ('这是一条普通的日志数据');
INSERT INTO case_101_archive_engine (log_data, created_at) VALUES
('用户 A 登录系统', '2026-06-02 19:00:00'),
('用户 B 修改了个人资料', '2026-06-02 19:05:00'),
('系统执行了定时任务', '2026-06-02 19:10:00'),
('用户 C 退出了系统', NOW());

-- case_102_csv_engine
TRUNCATE TABLE case_102_csv_engine;
INSERT INTO case_102_csv_engine (id, name, value) VALUES
(1, 'CSV 1', 10.00), (2, 'CSV 2', 20.00), (3, 'CSV 3', 30.00), (4, 'CSV 4', 40.00),
(5, 'CSV 5', 50.00), (6, 'CSV 6', 60.00), (7, 'CSV 7', 70.00), (8, 'CSV 8', 80.00),
(9, 'CSV 9', 90.00), (10, 'CSV 10', 100.00);

-- case_103_blackhole_engine
TRUNCATE TABLE case_103_blackhole_engine;
INSERT INTO case_103_blackhole_engine (id, data) VALUES
(1, 'Data 1'), (2, 'Data 2'), (3, 'Data 3'), (4, 'Data 4'), (5, 'Data 5'),
(6, 'Data 6'), (7, 'Data 7'), (8, 'Data 8'), (9, 'Data 9'), (10, 'Data 10');

-- case_104_delay_key_write
TRUNCATE TABLE case_104_delay_key_write;
INSERT INTO case_104_delay_key_write (id, name) VALUES
(1, 'Name 1'), (2, 'Name 2'), (3, 'Name 3'), (4, 'Name 4'), (5, 'Name 5'),
(6, 'Name 6'), (7, 'Name 7'), (8, 'Name 8'), (9, 'Name 9'), (10, 'Name 10');

-- case_105_upsert_test
TRUNCATE TABLE case_105_upsert_test;
INSERT INTO case_105_upsert_test (id, name, counter) VALUES
(1, 'Upsert 1', 1), (2, 'Upsert 2', 2), (3, 'Upsert 3', 3), (4, 'Upsert 4', 4),
(5, 'Upsert 5', 5), (6, 'Upsert 6', 6), (7, 'Upsert 7', 7), (8, 'Upsert 8', 8),
(9, 'Upsert 9', 9), (10, 'Upsert 10', 10);

-- case_106_replace_test
TRUNCATE TABLE case_106_replace_test;
INSERT INTO case_106_replace_test (id, name, value) VALUES
(1, 'Replace 1', 100), (2, 'Replace 2', 200), (3, 'Replace 3', 300), (4, 'Replace 4', 400),
(5, 'Replace 5', 500), (6, 'Replace 6', 600), (7, 'Replace 7', 700), (8, 'Replace 8', 800),
(9, 'Replace 9', 900), (10, 'Replace 10', 1000);

-- case_107_multi_delete_parent
/**
SET FOREIGN_KEY_CHECKS = 0;
TRUNCATE TABLE case_107_multi_delete_child;
TRUNCATE TABLE case_107_multi_delete_parent;
SET FOREIGN_KEY_CHECKS = 1;
INSERT INTO case_107_multi_delete_parent (id, name) VALUES
(1, 'Parent 1'), (2, 'Parent 2'), (3, 'Parent 3'), (4, 'Parent 4'), (5, 'Parent 5'),
(6, 'Parent 6'), (7, 'Parent 7'), (8, 'Parent 8'), (9, 'Parent 9'), (10, 'Parent 10');
**/

-- case_107_multi_delete_child
TRUNCATE TABLE case_107_multi_delete_child;
INSERT INTO case_107_multi_delete_parent (id, name) VALUES
(1, '父级数据A'),
(2, '父级数据B'),
(3, '父级数据C'),
(4, '父级数据D'),
(5, '父级数据E');

INSERT INTO case_107_multi_delete_child (id, parent_id, value) VALUES
(101, 1, 1000),
(102, 2, 2000),
(103, 3, 3000),
(104, 4, 4000),
(105, 5, 5000);

-- case_108_load_data_test
TRUNCATE TABLE case_108_load_data_test;
INSERT INTO case_108_load_data_test (id, name, email, amount) VALUES
(1, 'Alice', 'alice@example.com', 100.00), (2, 'Bob', 'bob@example.com', 200.00),
(3, 'Charlie', 'charlie@example.com', 300.00), (4, 'David', 'david@example.com', 400.00),
(5, 'Eve', 'eve@example.com', 500.00), (6, 'Frank', 'frank@example.com', 600.00),
(7, 'Grace', 'grace@example.com', 700.00), (8, 'Henry', 'henry@example.com', 800.00),
(9, 'Ivy', 'ivy@example.com', 900.00), (10, 'Jack', 'jack@example.com', 1000.00);

-- case_109_cte_test
TRUNCATE TABLE case_109_cte_test;
INSERT INTO case_109_cte_test (id, parent_id, name, level) VALUES
(1, NULL, 'Root', 0), (2, 1, 'Child 1', 1), (3, 1, 'Child 2', 1),
(4, 2, 'Grandchild 1', 2), (5, 2, 'Grandchild 2', 2), (6, 3, 'Grandchild 3', 2),
(7, 4, 'Great 1', 3), (8, 5, 'Great 2', 3), (9, 6, 'Great 3', 3), (10, 7, 'Great 4', 4);

-- case_110_window_function_test
TRUNCATE TABLE case_110_window_function_test;
INSERT INTO case_110_window_function_test (id, department, employee_name, salary, hire_date) VALUES
(1, 'Engineering', 'Alice', 120000, '2020-01-01'), (2, 'Engineering', 'Bob', 110000, '2019-06-01'),
(3, 'Sales', 'Charlie', 90000, '2021-03-01'), (4, 'Sales', 'David', 95000, '2020-08-01'),
(5, 'HR', 'Eve', 80000, '2022-01-01'), (6, 'HR', 'Frank', 85000, '2021-05-01'),
(7, 'Engineering', 'Grace', 130000, '2018-01-01'), (8, 'Sales', 'Henry', 100000, '2019-01-01'),
(9, 'HR', 'Ivy', 75000, '2023-01-01'), (10, 'Engineering', 'Jack', 115000, '2020-03-01');

-- case_111_json_table_test
TRUNCATE TABLE case_111_json_table_test;
INSERT INTO case_111_json_table_test (id, json_data) VALUES
(1, '[{"name": "A", "value": 1}]'), (2, '[{"name": "B", "value": 2}]'),
(3, '[{"name": "C", "value": 3}]'), (4, '[{"name": "D", "value": 4}]'),
(5, '[{"name": "E", "value": 5}]'), (6, '[{"name": "F", "value": 6}]'),
(7, '[{"name": "G", "value": 7}]'), (8, '[{"name": "H", "value": 8}]'),
(9, '[{"name": "I", "value": 9}]'), (10, '[{"name": "J", "value": 10}]');

-- case_112_regex_function_test
TRUNCATE TABLE case_112_regex_function_test;
INSERT INTO case_112_regex_function_test (id, text_content, email, phone) VALUES
(1, 'Hello 123 World', 'user1@test.com', '123-4567890'), (2, 'Test 456 Data', 'user2@test.com', '234-5678901'),
(3, 'Sample 789 Text', 'user3@test.com', '345-6789012'), (4, 'Regex 012 Test', 'user4@test.com', '456-7890123'),
(5, 'Match 345 Pattern', 'user5@test.com', '567-8901234'), (6, 'Search 678 Replace', 'user6@test.com', '678-9012345'),
(7, 'Find 901 Value', 'user7@test.com', '789-0123456'), (8, 'Extract 234 Info', 'user8@test.com', '890-1234567'),
(9, 'Parse 567 Data', 'user9@test.com', '901-2345678'), (10, 'Capture 890 Group', 'user10@test.com', '012-3456789');

-- case_113_optimizer_hint_test
TRUNCATE TABLE case_113_optimizer_hint_test;
INSERT INTO case_113_optimizer_hint_test (id, name, status) VALUES
(1, 'Hint 1', 'active'), (2, 'Hint 2', 'inactive'), (3, 'Hint 3', 'active'),
(4, 'Hint 4', 'pending'), (5, 'Hint 5', 'active'), (6, 'Hint 6', 'inactive'),
(7, 'Hint 7', 'active'), (8, 'Hint 8', 'pending'), (9, 'Hint 9', 'active'), (10, 'Hint 10', 'inactive');

-- case_114_role_test
TRUNCATE TABLE case_114_role_test;
INSERT INTO case_114_role_test (id, role_name, permissions) VALUES
(1, 'admin', '{"read": true, "write": true}'), (2, 'editor', '{"read": true, "write": true}'),
(3, 'viewer', '{"read": true, "write": false}'), (4, 'manager', '{"read": true, "write": true}'),
(5, 'analyst', '{"read": true, "write": false}'), (6, 'dev', '{"read": true, "write": true}'),
(7, 'ops', '{"read": true, "write": true}'), (8, 'support', '{"read": true, "write": false}'),
(9, 'guest', '{"read": false, "write": false}'), (10, 'superadmin', '{"read": true, "write": true}');

-- case_115_resource_group_test
TRUNCATE TABLE case_115_resource_group_test;
INSERT INTO case_115_resource_group_test (id, query_name, priority) VALUES
(1, 'Query 1', 'HIGH'), (2, 'Query 2', 'MEDIUM'), (3, 'Query 3', 'LOW'),
(4, 'Query 4', 'HIGH'), (5, 'Query 5', 'MEDIUM'), (6, 'Query 6', 'LOW'),
(7, 'Query 7', 'HIGH'), (8, 'Query 8', 'MEDIUM'), (9, 'Query 9', 'LOW'), (10, 'Query 10', 'HIGH');

-- case_116_multi_valued_index_test
TRUNCATE TABLE case_116_multi_valued_index_test;
INSERT INTO case_116_multi_valued_index_test (id, tags, attributes) VALUES
(1, '[1, 2, 3]', '{"color": "red"}'), (2, '[4, 5, 6]', '{"size": "M"}'),
(3, '[7, 8, 9]', '{"type": "A"}'), (4, '[10, 11, 12]', '{"color": "blue"}'),
(5, '[13, 14, 15]', '{"size": "L"}'), (6, '[16, 17, 18]', '{"type": "B"}'),
(7, '[19, 20, 21]', '{"color": "green"}'), (8, '[22, 23, 24]', '{"size": "S"}'),
(9, '[25, 26, 27]', '{"type": "C"}'), (10, '[28, 29, 30]', '{"color": "yellow"}');

-- case_117_nowait_skip_locked_test
TRUNCATE TABLE case_117_nowait_skip_locked_test;
INSERT INTO case_117_nowait_skip_locked_test (id, task_name, status) VALUES
(1, 'Task 1', 'pending'), (2, 'Task 2', 'pending'), (3, 'Task 3', 'processing'),
(4, 'Task 4', 'pending'), (5, 'Task 5', 'done'), (6, 'Task 6', 'pending'),
(7, 'Task 7', 'processing'), (8, 'Task 8', 'pending'), (9, 'Task 9', 'done'), (10, 'Task 10', 'pending');

-- case_118_persist_variable_test
TRUNCATE TABLE case_118_persist_variable_test;
INSERT INTO case_118_persist_variable_test (id, variable_name, variable_value) VALUES
(1, 'max_connections', '1000'), (2, 'innodb_buffer_pool_size', '1G'),
(3, 'query_cache_size', '256M'), (4, 'tmp_table_size', '64M'),
(5, 'max_heap_table_size', '64M'), (6, 'thread_cache_size', '8'),
(7, 'table_open_cache', '4000'), (8, 'key_buffer_size', '256M'),
(9, 'sort_buffer_size', '2M'), (10, 'read_buffer_size', '1M');

-- case_119_force_index_test
TRUNCATE TABLE case_119_force_index_test;
INSERT INTO case_119_force_index_test (id, name, category) VALUES
(1, 'Index 1', 'A'), (2, 'Index 2', 'B'), (3, 'Index 3', 'A'),
(4, 'Index 4', 'C'), (5, 'Index 5', 'B'), (6, 'Index 6', 'A'),
(7, 'Index 7', 'C'), (8, 'Index 8', 'B'), (9, 'Index 9', 'A'), (10, 'Index 10', 'C');

-- case_120_table_lock_test
TRUNCATE TABLE case_120_table_lock_test;
INSERT INTO case_120_table_lock_test (id, data, version) VALUES
(1, 'Lock 1', 1), (2, 'Lock 2', 1), (3, 'Lock 3', 1), (4, 'Lock 4', 1),
(5, 'Lock 5', 1), (6, 'Lock 6', 1), (7, 'Lock 7', 1), (8, 'Lock 8', 1),
(9, 'Lock 9', 1), (10, 'Lock 10', 1);

-- ============================================================================
-- case_121 ~ case_167 (日常开发场景与综合增强场景)
-- 由于这些表大部分都有 AUTO_INCREMENT 主键，我们插入时省略主键让数据库自动生成
-- ============================================================================

-- case_121_ecom_users (16列)
TRUNCATE TABLE case_121_ecom_users;
INSERT INTO case_121_ecom_users (username, password_hash, nickname, phone, email, gender, status) VALUES
('user01', 'hash01', '用户1', '13800000001', 'user01@test.com', 1, 1),
('user02', 'hash02', '用户2', '13800000002', 'user02@test.com', 2, 1),
('user03', 'hash03', '用户3', '13800000003', 'user03@test.com', 1, 1),
('user04', 'hash04', '用户4', '13800000004', 'user04@test.com', 2, 1),
('user05', 'hash05', '用户5', '13800000005', 'user05@test.com', 1, 1),
('user06', 'hash06', '用户6', '13800000006', 'user06@test.com', 2, 1),
('user07', 'hash07', '用户7', '13800000007', 'user07@test.com', 1, 1),
('user08', 'hash08', '用户8', '13800000008', 'user08@test.com', 2, 1),
('user09', 'hash09', '用户9', '13800000009', 'user09@test.com', 1, 1),
('user10', 'hash10', '用户10', '13800000010', 'user10@test.com', 2, 1);

-- case_122_ecom_products (18列)
TRUNCATE TABLE case_122_ecom_products;
INSERT INTO case_122_ecom_products (category_id, product_name, unit_price, cost_price, stock_quantity, status) VALUES
(1, 'iPhone 15 Pro', 7999.00, 5500.00, 100, 1),
(2, 'MacBook Air M3', 8999.00, 6200.00, 50, 1),
(3, 'iPad Pro 12.9', 8499.00, 5800.00, 80, 1),
(1, 'AirPods Pro 2', 1999.00, 1200.00, 200, 1),
(2, 'Apple Watch Ultra', 5999.00, 4200.00, 60, 1),
(4, 'Nike Air Max', 1299.00, 600.00, 300, 1),
(5, 'Sony WH-1000XM5', 2499.00, 1800.00, 150, 1),
(3, 'Samsung Galaxy S24', 5999.00, 4000.00, 70, 1),
(6, 'Kindle Paperwhite', 998.00, 600.00, 200, 1),
(7, 'Dyson V15', 4690.00, 3200.00, 40, 1);

-- case_123_ecom_orders (20列)
TRUNCATE TABLE case_123_ecom_orders;
INSERT INTO case_123_ecom_orders (order_no, user_id, order_status, total_amount, pay_amount, pay_type, receiver_name, receiver_phone, receiver_address) VALUES
('ORD-20240101-001', 1, 3, 7999.00, 7999.00, 1, '张三', '13800000001', '北京市朝阳区XX路XX号'),
('ORD-20240102-002', 2, 2, 8999.00, 8999.00, 2, '李四', '13800000002', '上海市浦东新区XX路XX号'),
('ORD-20240103-003', 3, 3, 1999.00, 1999.00, 1, '王五', '13800000003', '广州市天河区XX路XX号'),
('ORD-20240104-004', 4, 1, 5999.00, 0, NULL, '赵六', '13800000004', '深圳市南山区XX路XX号'),
('ORD-20240105-005', 5, 3, 2499.00, 2499.00, 2, '钱七', '13800000005', '杭州市西湖区XX路XX号'),
('ORD-20240106-006', 6, 4, 1299.00, 1299.00, 1, '孙八', '13800000006', '成都市武侯区XX路XX号'),
('ORD-20240107-007', 7, 3, 998.00, 998.00, 2, '周九', '13800000007', '武汉市江汉区XX路XX号'),
('ORD-20240108-008', 8, 0, 4690.00, 0, NULL, '吴十', '13800000008', '南京市鼓楼区XX路XX号'),
('ORD-20240109-009', 9, 3, 8499.00, 8499.00, 1, '郑十一', '13800000009', '西安市雁塔区XX路XX号'),
('ORD-20240110-010', 10, 2, 5999.00, 5999.00, 2, '王十二', '13800000010', '重庆市渝北区XX路XX号');

-- case_124_ecom_order_items (7列)
TRUNCATE TABLE case_124_ecom_order_items;
INSERT INTO case_124_ecom_order_items (order_id, product_id, product_name, unit_price, quantity, subtotal) VALUES
(1, 1, 'iPhone 15 Pro', 7999.00, 1, 7999.00),
(2, 2, 'MacBook Air M3', 8999.00, 1, 8999.00),
(3, 4, 'AirPods Pro 2', 1999.00, 1, 1999.00),
(5, 7, 'Sony WH-1000XM5', 2499.00, 1, 2499.00),
(6, 6, 'Nike Air Max', 1299.00, 1, 1299.00),
(7, 9, 'Kindle Paperwhite', 998.00, 1, 998.00),
(9, 3, 'iPad Pro 12.9', 8499.00, 1, 8499.00),
(10, 8, 'Samsung Galaxy S24', 5999.00, 1, 5999.00),
(4, 5, 'Apple Watch Ultra', 5999.00, 1, 5999.00),
(8, 10, 'Dyson V15', 4690.00, 1, 4690.00);

-- case_125_ecom_cart (5列)
TRUNCATE TABLE case_125_ecom_cart;
INSERT INTO case_125_ecom_cart (user_id, product_id, quantity) VALUES
(1, 1, 1), (1, 2, 2), (2, 3, 1), (2, 4, 3), (3, 5, 1),
(3, 6, 2), (4, 7, 1), (4, 8, 1), (5, 9, 2), (5, 10, 1);

-- case_126_cms_articles (14列)
TRUNCATE TABLE case_126_cms_articles;
INSERT INTO case_126_cms_articles (category_id, title, summary, status, view_count, tags) VALUES
(1, 'MySQL 8.0 新特性解析', '介绍MySQL 8.0的最新功能和特性', 1, 1520, '["MySQL", "数据库"]'),
(2, 'PostgreSQL 性能优化指南', '深入分析PG性能调优策略', 1, 2340, '["PostgreSQL", "性能"]'),
(1, '数据库迁移最佳实践', '从MySQL迁移到PG的经验总结', 1, 1890, '["迁移", "最佳实践"]'),
(3, 'Go 语言并发编程', '探索Go语言的goroutine和channel', 1, 3200, '["Go", "并发"]'),
(2, 'Docker 容器化部署', '使用Docker进行应用容器化', 1, 2100, '["Docker", "部署"]'),
(4, 'Kubernetes 入门教程', 'K8s基础概念和操作指南', 1, 4500, '["K8s", "云原生"]'),
(1, 'Redis 缓存策略', 'Redis缓存设计与应用场景', 1, 1780, '["Redis", "缓存"]'),
(3, '微服务架构设计', '微服务拆分与治理策略', 1, 2900, '["微服务", "架构"]'),
(2, 'CI/CD 流水线搭建', '自动化构建和部署流程', 1, 1650, '["CI/CD", "DevOps"]'),
(4, '系统监控与告警', 'Prometheus + Grafana 监控方案', 1, 2200, '["监控", "告警"]');

-- case_127_cms_comments (8列)
TRUNCATE TABLE case_127_cms_comments;
INSERT INTO case_127_cms_comments (article_id, user_id, parent_id, content, status) VALUES
(1, 1, 0, '写得很好，学习了！', 1), (1, 2, 0, '非常详细的教程', 1),
(2, 3, 0, 'PG确实很强大', 1), (2, 4, 1, '同意楼上的看法', 1),
(3, 5, 0, '迁移过程中遇到了问题', 1), (3, 6, 5, '什么问题？可以详细描述吗', 1),
(4, 7, 0, 'Go的并发模型真的很棒', 1), (4, 8, 0, 'channel设计很巧妙', 1),
(5, 9, 0, 'Docker改变了部署方式', 1), (5, 10, 0, '容器化是趋势', 1);

-- case_128_finance_accounts (9列)
TRUNCATE TABLE case_128_finance_accounts;
INSERT INTO case_128_finance_accounts (user_id, account_no, account_type, currency, balance, available_balance, frozen_balance, status) VALUES
(1, 'ACC-001', 1, 'CNY', 100000.0000, 80000.0000, 20000.0000, 1),
(2, 'ACC-002', 1, 'CNY', 50000.0000, 50000.0000, 0, 1),
(3, 'ACC-003', 2, 'CNY', 200000.0000, 150000.0000, 50000.0000, 1),
(4, 'ACC-004', 3, 'USD', 50000.0000, 40000.0000, 10000.0000, 1),
(5, 'ACC-005', 1, 'CNY', 80000.0000, 70000.0000, 10000.0000, 1),
(6, 'ACC-006', 2, 'CNY', 300000.0000, 250000.0000, 50000.0000, 1),
(7, 'ACC-007', 1, 'CNY', 150000.0000, 120000.0000, 30000.0000, 1),
(8, 'ACC-008', 3, 'USD', 80000.0000, 60000.0000, 20000.0000, 1),
(9, 'ACC-009', 1, 'CNY', 60000.0000, 55000.0000, 5000.0000, 1),
(10, 'ACC-010', 2, 'CNY', 120000.0000, 100000.0000, 20000.0000, 1);

-- case_129_finance_transactions (11列)
TRUNCATE TABLE case_129_finance_transactions;
INSERT INTO case_129_finance_transactions (trans_no, account_id, trans_type, amount, balance_before, balance_after, status, trans_time) VALUES
('TXN-001', 1, 1, 10000.0000, 90000.0000, 100000.0000, 1, '2024-01-01 10:00:00'),
('TXN-002', 1, 2, 5000.0000, 100000.0000, 95000.0000, 1, '2024-01-02 11:00:00'),
('TXN-003', 2, 1, 20000.0000, 30000.0000, 50000.0000, 1, '2024-01-03 12:00:00'),
('TXN-004', 3, 3, 15000.0000, 215000.0000, 200000.0000, 1, '2024-01-04 13:00:00'),
('TXN-005', 4, 1, 30000.0000, 20000.0000, 50000.0000, 1, '2024-01-05 14:00:00'),
('TXN-006', 5, 4, 8000.0000, 88000.0000, 80000.0000, 1, '2024-01-06 15:00:00'),
('TXN-007', 6, 1, 50000.0000, 250000.0000, 300000.0000, 1, '2024-01-07 16:00:00'),
('TXN-008', 7, 2, 10000.0000, 160000.0000, 150000.0000, 1, '2024-01-08 17:00:00'),
('TXN-009', 8, 3, 25000.0000, 105000.0000, 80000.0000, 1, '2024-01-09 18:00:00'),
('TXN-010', 9, 1, 15000.0000, 45000.0000, 60000.0000, 1, '2024-01-10 19:00:00');

-- case_130_social_follows (4列)
TRUNCATE TABLE case_130_social_follows;
INSERT INTO case_130_social_follows (follower_id, followee_id, status) VALUES
(1, 2, 1), (1, 3, 1), (2, 1, 1), (2, 4, 1),
(3, 1, 1), (3, 5, 1), (4, 2, 1), (4, 6, 1),
(5, 3, 1), (5, 7, 1);

-- case_131_social_likes (4列)
TRUNCATE TABLE case_131_social_likes;
INSERT INTO case_131_social_likes (user_id, target_type, target_id) VALUES
(1, 1, 1), (1, 2, 1), (2, 1, 2), (2, 2, 2),
(3, 1, 3), (3, 2, 3), (4, 1, 4), (4, 2, 4),
(5, 1, 5), (5, 2, 5);

-- case_132_social_notifications (6列)
TRUNCATE TABLE case_132_social_notifications;
INSERT INTO case_132_social_notifications (user_id, notify_type, title, content, is_read) VALUES
(1, 2, '点赞通知', '你的文章被点赞了', 0), (1, 4, '关注通知', '有人关注了你', 0),
(2, 3, '评论通知', '你的文章收到评论', 1), (2, 2, '点赞通知', '你的文章被点赞了', 0),
(3, 1, '系统通知', '系统维护通知', 1), (3, 4, '关注通知', '有人关注了你', 0),
(4, 2, '点赞通知', '你的文章被点赞了', 1), (4, 3, '评论通知', '你的文章收到评论', 0),
(5, 1, '系统通知', '活动通知', 1), (5, 4, '关注通知', '有人关注了你', 0);

-- case_133_log_operations (13列)
TRUNCATE TABLE case_133_log_operations;
INSERT INTO case_133_log_operations (user_id, username, module, action, method, request_url, ip_address, response_code, response_time) VALUES
(1, 'admin', 'User', 'Create', 'POST', '/api/users', '192.168.1.1', 200, 120),
(2, 'admin', 'User', 'Update', 'PUT', '/api/users/1', '192.168.1.1', 200, 85),
(3, 'user1', 'Order', 'Create', 'POST', '/api/orders', '192.168.1.2', 201, 230),
(4, 'user2', 'Product', 'List', 'GET', '/api/products', '192.168.1.3', 200, 45),
(5, 'user3', 'Order', 'Update', 'PUT', '/api/orders/2', '192.168.1.4', 200, 150),
(6, 'admin', 'User', 'Delete', 'DELETE', '/api/users/3', '192.168.1.1', 200, 65),
(7, 'user4', 'Product', 'Create', 'POST', '/api/products', '192.168.1.5', 201, 180),
(8, 'user5', 'Order', 'List', 'GET', '/api/orders', '192.168.1.6', 200, 90),
(9, 'user6', 'User', 'Get', 'GET', '/api/users/6', '192.168.1.7', 200, 35),
(10, 'admin', 'Product', 'Update', 'PUT', '/api/products/7', '192.168.1.1', 200, 110);

-- case_134_log_logins (9列)
TRUNCATE TABLE case_134_log_logins;
INSERT INTO case_134_log_logins (user_id, username, login_type, login_result, ip_address, user_agent, device_type) VALUES
(1, 'admin', 1, 1, '192.168.1.1', 'Mozilla/5.0 Chrome', 'Desktop'),
(2, 'user1', 2, 1, '192.168.1.2', 'Mozilla/5.0 Safari', 'Mobile'),
(3, 'user2', 1, 0, '192.168.1.3', 'Mozilla/5.0 Firefox', 'Desktop'),
(4, 'user3', 3, 1, '192.168.1.4', 'Mozilla/5.0 Edge', 'Tablet'),
(5, 'user4', 1, 1, '192.168.1.5', 'Mozilla/5.0 Chrome', 'Desktop'),
(6, 'user5', 2, 1, '192.168.1.6', 'WeChat/7.0', 'Mobile'),
(7, 'user6', 1, 0, '192.168.1.7', 'Mozilla/5.0 Safari', 'Desktop'),
(8, 'user7', 3, 1, '192.168.1.8', 'Alipay/10.0', 'Mobile'),
(9, 'user8', 1, 1, '192.168.1.9', 'Mozilla/5.0 Chrome', 'Desktop'),
(10, 'user9', 2, 1, '192.168.1.10', 'Mozilla/5.0 Firefox', 'Mobile');

-- case_135_sys_departments (8列)
TRUNCATE TABLE case_135_sys_departments;
INSERT INTO case_135_sys_departments (parent_id, dept_name, dept_code, sort_order, status) VALUES
(0, '总公司', 'HQ', 1, 1),
(1, '技术部', 'TECH', 10, 1),
(1, '市场部', 'MKT', 20, 1),
(1, '财务部', 'FIN', 30, 1),
(2, '前端组', 'FE', 100, 1),
(2, '后端组', 'BE', 101, 1),
(3, '品牌组', 'BRAND', 200, 1),
(3, '推广组', 'PROMO', 201, 1),
(4, '会计组', 'ACCT', 300, 1),
(4, '审计组', 'AUDIT', 301, 1);

-- case_136_sys_roles (5列)
TRUNCATE TABLE case_136_sys_roles;
INSERT INTO case_136_sys_roles (role_name, role_code, description, data_scope, status) VALUES
('超级管理员', 'SUPER_ADMIN', '系统最高权限', 1, 1),
('管理员', 'ADMIN', '日常管理权限', 1, 1),
('部门经理', 'DEPT_MGR', '部门管理权限', 2, 1),
('普通员工', 'EMPLOYEE', '基本操作权限', 3, 1),
('访客', 'GUEST', '只读权限', 1, 1),
('财务', 'FINANCE', '财务相关权限', 2, 1),
('HR', 'HR', '人事管理权限', 2, 1),
('技术负责人', 'TECH_LEAD', '技术管理权限', 2, 1),
('项目经理', 'PM', '项目管理权限', 2, 1),
('审计员', 'AUDITOR', '审计查看权限', 1, 1);

-- case_137_sys_menus (8列)
TRUNCATE TABLE case_137_sys_menus;
INSERT INTO case_137_sys_menus (parent_id, menu_name, menu_type, menu_url, perms, sort_order, is_visible) VALUES
(0, '系统管理', 1, '/system', NULL, 100, 1),
(0, '用户管理', 2, '/system/users', 'system:user:list', 101, 1),
(0, '角色管理', 2, '/system/roles', 'system:role:list', 102, 1),
(0, '菜单管理', 2, '/system/menus', 'system:menu:list', 103, 1),
(1, '用户新增', 3, NULL, 'system:user:add', 110, 1),
(1, '用户编辑', 3, NULL, 'system:user:edit', 111, 1),
(1, '用户删除', 3, NULL, 'system:user:delete', 112, 1),
(0, '数据统计', 2, '/stats', 'stats:view', 200, 1),
(0, '日志管理', 2, '/logs', 'logs:view', 300, 1),
(0, '个人设置', 2, '/profile', 'profile:edit', 400, 1);

-- case_138_sys_user_roles (3列)
TRUNCATE TABLE case_138_sys_user_roles;
INSERT INTO case_138_sys_user_roles (user_id, role_id) VALUES
(1, 1), (2, 2), (3, 3), (4, 4), (5, 5),
(6, 6), (7, 7), (8, 8), (9, 9), (10, 10);

-- case_139_sys_role_menus (3列)
TRUNCATE TABLE case_139_sys_role_menus;
INSERT INTO case_139_sys_role_menus (role_id, menu_id) VALUES
(1, 1), (1, 2), (1, 3), (1, 4), (1, 5),
(2, 1), (2, 2), (2, 3), (3, 2), (4, 2);

-- case_140_sys_config (6列)
TRUNCATE TABLE case_140_sys_config;
INSERT INTO case_140_sys_config (config_key, config_value, config_type, description, is_editable) VALUES
('site.name', 'MySQL2PG', 1, '站点名称', 1),
('site.version', '1.0.0', 1, '版本号', 0),
('system.max_conns', '100', 2, '最大连接数', 1),
('system.enable_log', 'true', 3, '启用日志', 1),
('system.allowed_ips', '["127.0.0.1", "192.168.1.0/24"]', 4, '允许的IP列表', 1),
('email.host', 'smtp.example.com', 1, '邮件服务器', 1),
('email.port', '465', 2, '邮件端口', 1),
('email.ssl', 'true', 3, '邮件SSL', 1),
('upload.max_size', '10485760', 2, '上传最大大小(字节)', 1),
('upload.allowed_types', '["jpg", "png", "pdf"]', 4, '允许上传的文件类型', 1);

-- case_141_sys_dict (8列)
TRUNCATE TABLE case_141_sys_dict;
INSERT INTO case_141_sys_dict (dict_type, dict_label, dict_value, sort_order, is_default, status) VALUES
('gender', '男', '1', 1, 0, 1),
('gender', '女', '2', 2, 0, 1),
('status', '启用', '1', 1, 1, 1),
('status', '禁用', '0', 2, 0, 1),
('order_status', '待支付', '0', 1, 0, 1),
('order_status', '已支付', '1', 2, 0, 1),
('order_status', '已发货', '2', 3, 0, 1),
('order_status', '已完成', '3', 4, 0, 1),
('order_status', '已取消', '4', 5, 0, 1),
('yes_no', '是', 'Y', 1, 0, 1);

-- case_142_files_uploads (11列)
TRUNCATE TABLE case_142_files_uploads;
INSERT INTO case_142_files_uploads (file_name, original_name, file_path, file_size, file_type, file_ext, user_id, download_count) VALUES
('avatar_001.jpg', '头像.jpg', '/uploads/avatars/avatar_001.jpg', 102400, 'image/jpeg', 'jpg', 1, 10),
('doc_001.pdf', '文档.pdf', '/uploads/docs/doc_001.pdf', 2048000, 'application/pdf', 'pdf', 2, 5),
('img_001.png', '图片.png', '/uploads/images/img_001.png', 512000, 'image/png', 'png', 3, 20),
('report_001.xlsx', '报表.xlsx', '/uploads/reports/report_001.xlsx', 1024000, 'application/xlsx', 'xlsx', 4, 15),
('video_001.mp4', '视频.mp4', '/uploads/videos/video_001.mp4', 10240000, 'video/mp4', 'mp4', 5, 30),
('audio_001.mp3', '音频.mp3', '/uploads/audios/audio_001.mp3', 5120000, 'audio/mp3', 'mp3', 6, 25),
('code_001.zip', '代码.zip', '/uploads/archives/code_001.zip', 3072000, 'application/zip', 'zip', 7, 8),
('txt_001.txt', '文本.txt', '/uploads/texts/txt_001.txt', 10240, 'text/plain', 'txt', 8, 12),
('csv_001.csv', '数据.csv', '/uploads/data/csv_001.csv', 204800, 'text/csv', 'csv', 9, 18),
('json_001.json', '配置.json', '/uploads/configs/json_001.json', 5120, 'application/json', 'json', 10, 6);

-- case_143_job_tasks (13列)
TRUNCATE TABLE case_143_job_tasks;
INSERT INTO case_143_job_tasks (job_name, job_group, bean_name, method_name, cron_expression, status) VALUES
('数据同步任务', 'DEFAULT', 'dataSyncJob', 'execute', '0 0 2 * * ?', 1),
('日志清理任务', 'DEFAULT', 'logCleanJob', 'execute', '0 0 3 * * ?', 1),
('报表生成任务', 'REPORT', 'reportGenJob', 'execute', '0 0 8 * * ?', 1),
('缓存刷新任务', 'CACHE', 'cacheRefreshJob', 'execute', '0 */30 * * * ?', 1),
('邮件发送任务', 'NOTIFY', 'emailSendJob', 'execute', '0 0 9 * * ?', 1),
('数据备份任务', 'BACKUP', 'dataBackupJob', 'execute', '0 0 1 * * ?', 1),
('健康检查任务', 'MONITOR', 'healthCheckJob', 'execute', '0 */5 * * * ?', 1),
('订单超时取消任务', 'ORDER', 'orderTimeoutJob', 'execute', '0 */10 * * * ?', 1),
('库存预警任务', 'STOCK', 'stockAlertJob', 'execute', '0 0 */6 * * ?', 1),
('用户积分过期任务', 'POINTS', 'pointsExpireJob', 'execute', '0 0 0 1 * ?', 1);

-- case_143_job_logs (8列)
TRUNCATE TABLE case_143_job_logs;
INSERT INTO case_143_job_logs (job_id, job_name, execute_time, execute_status, execute_msg, execute_duration) VALUES
(1, '数据同步任务', '2024-01-01 02:00:00', 1, '执行成功', 5230),
(1, '数据同步任务', '2024-01-02 02:00:00', 1, '执行成功', 4890),
(2, '日志清理任务', '2024-01-01 03:00:00', 1, '执行成功', 1200),
(2, '日志清理任务', '2024-01-02 03:00:00', 1, '执行成功', 1150),
(3, '报表生成任务', '2024-01-01 08:00:00', 1, '执行成功', 15600),
(4, '缓存刷新任务', '2024-01-01 02:00:00', 0, '连接超时', 30000),
(5, '邮件发送任务', '2024-01-01 09:00:00', 1, '执行成功', 2300),
(6, '数据备份任务', '2024-01-01 01:00:00', 1, '执行成功', 45000),
(7, '健康检查任务', '2024-01-01 02:05:00', 1, '执行成功', 500),
(8, '订单超时取消任务', '2024-01-01 02:10:00', 1, '执行成功', 800);

-- case_144_api_interfaces (12列)
TRUNCATE TABLE case_144_api_interfaces;
INSERT INTO case_144_api_interfaces (api_name, api_path, api_method, api_category, is_auth, rate_limit, status, version) VALUES
('用户列表', '/api/v1/users', 'GET', '用户管理', 1, 100, 1, 'v1'),
('创建用户', '/api/v1/users', 'POST', '用户管理', 1, 50, 1, 'v1'),
('更新用户', '/api/v1/users/:id', 'PUT', '用户管理', 1, 50, 1, 'v1'),
('删除用户', '/api/v1/users/:id', 'DELETE', '用户管理', 1, 20, 1, 'v1'),
('商品列表', '/api/v1/products', 'GET', '商品管理', 1, 200, 1, 'v1'),
('创建商品', '/api/v1/products', 'POST', '商品管理', 1, 50, 1, 'v1'),
('订单列表', '/api/v1/orders', 'GET', '订单管理', 1, 100, 1, 'v1'),
('创建订单', '/api/v1/orders', 'POST', '订单管理', 1, 30, 1, 'v1'),
('健康检查', '/api/health', 'GET', '系统接口', 0, 1000, 1, 'v1'),
('登录', '/api/v1/auth/login', 'POST', '认证接口', 0, 20, 1, 'v1');

-- case_144_api_logs (13列)
TRUNCATE TABLE case_144_api_logs;
INSERT INTO case_144_api_logs (api_path, api_method, client_ip, user_id, response_code, response_time) VALUES
('/api/v1/users', 'GET', '192.168.1.1', 1, 200, 45),
('/api/v1/users', 'POST', '192.168.1.2', 2, 201, 120),
('/api/v1/products', 'GET', '192.168.1.3', 3, 200, 85),
('/api/v1/orders', 'POST', '192.168.1.4', 4, 201, 230),
('/api/v1/users/1', 'PUT', '192.168.1.5', 5, 200, 150),
('/api/v1/users/2', 'DELETE', '192.168.1.6', 6, 200, 65),
('/api/health', 'GET', '10.0.0.1', NULL, 200, 5),
('/api/v1/auth/login', 'POST', '192.168.1.7', 7, 200, 200),
('/api/v1/products/1', 'GET', '192.168.1.8', 8, 404, 30),
('/api/v1/orders/1', 'GET', '192.168.1.9', 9, 200, 55);

-- case_145_tenant_info (11列)
TRUNCATE TABLE case_145_tenant_info;
INSERT INTO case_145_tenant_info (tenant_name, tenant_code, contact_name, contact_phone, max_users, max_storage, expire_date, status) VALUES
('租户A', 'TENANT_A', '张三', '13800000001', 100, 10, '2025-12-31', 1),
('租户B', 'TENANT_B', '李四', '13800000002', 200, 20, '2025-12-31', 1),
('租户C', 'TENANT_C', '王五', '13800000003', 50, 5, '2024-06-30', 1),
('租户D', 'TENANT_D', '赵六', '13800000004', 500, 50, '2025-12-31', 1),
('租户E', 'TENANT_E', '钱七', '13800000005', 30, 3, '2024-03-31', 2),
('租户F', 'TENANT_F', '孙八', '13800000006', 150, 15, '2025-12-31', 1),
('租户G', 'TENANT_G', '周九', '13800000007', 80, 8, '2025-12-31', 1),
('租户H', 'TENANT_H', '吴十', '13800000008', 120, 12, '2024-01-31', 0),
('租户I', 'TENANT_I', '郑十一', '13800000009', 60, 6, '2025-12-31', 1),
('租户J', 'TENANT_J', '王十二', '13800000010', 90, 9, '2025-12-31', 1);

-- case_145_tenant_config (4列)
TRUNCATE TABLE case_145_tenant_config;
INSERT INTO case_145_tenant_config (tenant_id, config_key, config_value) VALUES
(1, 'theme', 'dark'), (1, 'language', 'zh-CN'),
(2, 'theme', 'light'), (2, 'language', 'en-US'),
(3, 'theme', 'dark'), (3, 'language', 'zh-CN'),
(4, 'theme', 'light'), (4, 'language', 'ja-JP'),
(5, 'theme', 'dark'), (5, 'language', 'ko-KR');

-- case_146_stats_daily (7列)
TRUNCATE TABLE case_146_stats_daily;
INSERT INTO case_146_stats_daily (stat_date, stat_type, stat_key, stat_value, stat_count) VALUES
('2024-01-01', 'page_view', 'home', 15230.0000, 1523),
('2024-01-02', 'page_view', 'home', 16890.0000, 1689),
('2024-01-03', 'page_view', 'home', 14560.0000, 1456),
('2024-01-01', 'order_count', 'total', 567890.0000, 567),
('2024-01-02', 'order_count', 'total', 623450.0000, 623),
('2024-01-03', 'order_count', 'total', 589012.0000, 589),
('2024-01-01', 'user_register', 'total', 1234.0000, 123),
('2024-01-02', 'user_register', 'total', 1567.0000, 156),
('2024-01-03', 'user_register', 'total', 1890.0000, 189),
('2024-01-01', 'revenue', 'total', 98765.4321, 987);

-- case_146_stats_monthly (7列)
TRUNCATE TABLE case_146_stats_monthly;
INSERT INTO case_146_stats_monthly (stat_month, stat_type, stat_key, stat_value, stat_count) VALUES
('2024-01', 'page_view', 'home', 456780.0000, 45678),
('2024-02', 'page_view', 'home', 423450.0000, 42345),
('2024-03', 'page_view', 'home', 489012.0000, 48901),
('2024-01', 'order_count', 'total', 1678901.0000, 1678),
('2024-02', 'order_count', 'total', 1523450.0000, 1523),
('2024-03', 'order_count', 'total', 1789012.0000, 1789),
('2024-01', 'user_register', 'total', 3456.0000, 345),
('2024-02', 'user_register', 'total', 3123.0000, 312),
('2024-03', 'user_register', 'total', 3789.0000, 378),
('2024-01', 'revenue', 'total', 298765.4321, 2987);

-- case_147_geo_regions (6列)
TRUNCATE TABLE case_147_geo_regions;
INSERT INTO case_147_geo_regions (region_code, parent_code, region_name, region_level, is_hot) VALUES
('110000', NULL, '北京市', 1, 1),
('310000', NULL, '上海市', 1, 1),
('440000', NULL, '广东省', 1, 1),
('110100', '110000', '北京市', 2, 1),
('310100', '310000', '上海市', 2, 1),
('440100', '440000', '广州市', 2, 1),
('440300', '440000', '深圳市', 2, 1),
('110101', '110100', '东城区', 3, 0),
('310101', '310100', '黄浦区', 3, 0),
('440106', '440100', '天河区', 3, 0);

-- case_147_user_addresses (9列)
TRUNCATE TABLE case_147_user_addresses;
INSERT INTO case_147_user_addresses (user_id, contact_name, contact_phone, province_code, city_code, district_code, detail_address, is_default) VALUES
(1, '张三', '13800000001', '110000', '110100', '110101', 'XX街道XX号', 1),
(2, '李四', '13800000002', '310000', '310100', '310101', 'XX路XX号', 1),
(3, '王五', '13800000003', '440000', '440100', '440106', 'XX大道XX号', 1),
(4, '赵六', '13800000004', '440000', '440300', '440305', 'XX科技路XX号', 1),
(5, '钱七', '13800000005', '330000', '330100', '330106', 'XX西湖区XX号', 1),
(6, '孙八', '13800000006', '510000', '510100', '510107', 'XX武侯区XX号', 1),
(7, '周九', '13800000007', '420000', '420100', '420111', 'XX洪山区XX号', 1),
(8, '吴十', '13800000008', '320000', '320100', '320106', 'XX鼓楼区XX号', 1),
(9, '郑十一', '13800000009', '610000', '610100', '610113', 'XX雁塔区XX号', 1),
(10, '王十二', '13800000010', '500000', '500100', '500112', 'XX渝北区XX号', 1);

-- case_148_coupon_templates (13列)
TRUNCATE TABLE case_148_coupon_templates;
INSERT INTO case_148_coupon_templates (template_name, coupon_type, discount_value, min_purchase, total_count, per_limit, valid_type, valid_start, valid_end, status) VALUES
('新年满减券', 1, 50.00, 200.00, 10000, 1, 1, '2024-01-01', '2024-01-31', 1),
('春节折扣券', 2, 0.85, 100.00, 5000, 1, 1, '2024-02-01', '2024-02-29', 1),
('开学季免邮券', 3, NULL, 50.00, 20000, 2, 2, '2024-09-01', NULL, 1),
('双十一满减券', 1, 100.00, 500.00, 50000, 1, 1, '2024-11-01', '2024-11-30', 1),
('会员专属券', 1, 30.00, 100.00, 1000, 1, 2, NULL, NULL, 1),
('新人注册券', 2, 0.90, 0, 100000, 1, 2, NULL, NULL, 1),
('生日特惠券', 1, 80.00, 300.00, 5000, 1, 2, NULL, NULL, 1),
('周末狂欢券', 2, 0.75, 200.00, 3000, 1, 1, '2024-06-01', '2024-06-02', 1),
('清仓大减价', 1, 200.00, 1000.00, 1000, 1, 1, '2024-07-01', '2024-07-31', 1),
('限时秒杀券', 2, 0.50, 50.00, 100, 1, 1, '2024-12-12', '2024-12-12', 1);

-- case_148_user_coupons (9列)
TRUNCATE TABLE case_148_user_coupons;
INSERT INTO case_148_user_coupons (user_id, template_id, coupon_code, status, valid_start, valid_end) VALUES
(1, 1, 'CPN-001', 0, '2024-01-01', '2024-01-31'),
(1, 6, 'CPN-002', 1, '2024-01-01', '2024-12-31'),
(2, 2, 'CPN-003', 0, '2024-02-01', '2024-02-29'),
(2, 4, 'CPN-004', 2, '2024-11-01', '2024-11-30'),
(3, 3, 'CPN-005', 0, '2024-09-01', '2024-09-30'),
(4, 5, 'CPN-006', 0, '2024-01-01', '2024-12-31'),
(5, 7, 'CPN-007', 1, '2024-01-01', '2024-12-31'),
(6, 8, 'CPN-008', 0, '2024-06-01', '2024-06-02'),
(7, 9, 'CPN-009', 2, '2024-07-01', '2024-07-31'),
(8, 10, 'CPN-010', 0, '2024-12-12', '2024-12-12');

-- case_149_points_accounts (8列)
TRUNCATE TABLE case_149_points_accounts;
INSERT INTO case_149_points_accounts (user_id, total_points, available_points, frozen_points, used_points, expired_points, level) VALUES
(1, 10000, 8000, 1000, 1000, 0, 3),
(2, 5000, 4500, 0, 500, 0, 2),
(3, 20000, 15000, 2000, 3000, 0, 4),
(4, 3000, 2800, 0, 200, 0, 1),
(5, 50000, 40000, 5000, 5000, 0, 5),
(6, 8000, 7000, 0, 1000, 0, 3),
(7, 15000, 12000, 1000, 2000, 0, 3),
(8, 1000, 900, 0, 100, 0, 1),
(9, 30000, 25000, 3000, 2000, 0, 4),
(10, 7000, 6000, 0, 1000, 0, 2);

-- case_149_points_logs (9列)
TRUNCATE TABLE case_149_points_logs;
INSERT INTO case_149_points_logs (user_id, points_type, points_value, balance_before, balance_after, source_type, description, expire_date) VALUES
(1, 1, 1000, 9000, 10000, '消费返积分', '购物返积分', '2025-12-31'),
(1, 2, -500, 10000, 9500, '兑换商品', '兑换优惠券', NULL),
(2, 1, 500, 4500, 5000, '签到', '连续签到7天', '2025-12-31'),
(2, 2, -200, 5000, 4800, '兑换商品', '兑换礼品', NULL),
(3, 1, 2000, 18000, 20000, '活动奖励', '双十一活动', '2025-12-31'),
(3, 1, 1000, 13000, 14000, '消费返积分', '购物返积分', '2025-12-31'),
(4, 1, 300, 2700, 3000, '签到', '每日签到', '2025-12-31'),
(5, 1, 5000, 45000, 50000, '活动奖励', '会员日奖励', '2025-12-31'),
(6, 2, -1000, 8000, 7000, '兑换商品', '兑换积分礼品', NULL),
(7, 1, 1500, 13500, 15000, '消费返积分', '大额消费奖励', '2025-12-31');

-- case_150_data_versions (8列)
TRUNCATE TABLE case_150_data_versions;
INSERT INTO case_150_data_versions (entity_type, entity_id, version_no, change_type, old_data, new_data, change_reason) VALUES
('User', 1, 1, 1, NULL, '{"name": "Alice"}', '创建用户'),
('User', 1, 2, 2, '{"name": "Alice"}', '{"name": "Alice Updated"}', '更新用户信息'),
('Product', 1, 1, 1, NULL, '{"name": "iPhone", "price": 7999}', '创建商品'),
('Product', 1, 2, 2, '{"name": "iPhone", "price": 7999}', '{"name": "iPhone", "price": 6999}', '降价促销'),
('Order', 1, 1, 1, NULL, '{"status": "pending"}', '创建订单'),
('Order', 1, 2, 2, '{"status": "pending"}', '{"status": "paid"}', '订单支付'),
('Order', 1, 3, 2, '{"status": "paid"}', '{"status": "shipped"}', '订单发货'),
('User', 2, 1, 1, NULL, '{"name": "Bob"}', '创建用户'),
('Product', 2, 1, 1, NULL, '{"name": "MacBook", "price": 8999}', '创建商品'),
('Order', 2, 1, 1, NULL, '{"status": "pending"}', '创建订单');

-- case_151_pm_projects (14列)
TRUNCATE TABLE case_151_pm_projects;
INSERT INTO case_151_pm_projects (project_name, project_code, project_type, priority, status, start_date, end_date, budget, progress) VALUES
('MySQL2PG 迁移工具', 'PROJ-001', 1, 3, 2, '2024-01-01', '2024-06-30', 500000, 65),
('电商平台重构', 'PROJ-002', 1, 4, 2, '2024-02-01', '2024-12-31', 1000000, 40),
('移动端APP开发', 'PROJ-003', 1, 3, 1, '2024-03-01', '2024-09-30', 800000, 20),
('数据中台建设', 'PROJ-004', 1, 2, 2, '2024-01-15', '2024-08-31', 1200000, 50),
('监控系统升级', 'PROJ-005', 3, 2, 3, '2024-04-01', '2024-05-31', 200000, 100),
('CI/CD 流水线', 'PROJ-006', 3, 2, 4, '2023-10-01', '2024-01-31', 150000, 100),
('用户增长项目', 'PROJ-007', 2, 3, 2, '2024-05-01', '2024-12-31', 600000, 10),
('AI 推荐系统', 'PROJ-008', 1, 4, 1, '2024-06-01', '2025-06-30', 2000000, 5),
('安全加固项目', 'PROJ-009', 3, 1, 4, '2023-01-01', '2023-12-31', 300000, 100),
('品牌升级', 'PROJ-010', 2, 2, 2, '2024-07-01', '2024-12-31', 400000, 15);

-- case_151_pm_tasks (15列)
TRUNCATE TABLE case_151_pm_tasks;
INSERT INTO case_151_pm_tasks (project_id, parent_task_id, task_name, task_type, priority, status, estimated_hours, actual_hours, start_date, due_date) VALUES
(1, 0, '需求分析', 1, 3, 3, 40, 38, '2024-01-01', '2024-01-15'),
(1, 0, '数据库设计', 1, 3, 3, 80, 75, '2024-01-16', '2024-02-15'),
(1, 2, '类型映射实现', 3, 3, 3, 120, 110, '2024-02-16', '2024-03-31'),
(1, 2, '数据同步实现', 3, 3, 2, 160, 140, '2024-03-01', '2024-04-30'),
(1, 0, '测试与优化', 4, 2, 2, 80, 50, '2024-04-01', '2024-05-31'),
(2, 0, '前端重构', 1, 3, 2, 200, 150, '2024-02-01', '2024-06-30'),
(2, 0, '后端重构', 1, 3, 2, 240, 180, '2024-02-01', '2024-08-31'),
(3, 0, 'iOS 开发', 3, 3, 2, 300, 100, '2024-03-01', '2024-08-31'),
(4, 0, '数据采集模块', 3, 2, 3, 120, 100, '2024-01-15', '2024-03-31'),
(5, 0, 'Prometheus 部署', 4, 2, 3, 40, 40, '2024-04-01', '2024-04-15');

-- case_152_edu_courses (13列)
TRUNCATE TABLE case_152_edu_courses;
INSERT INTO case_152_edu_courses (course_name, course_code, level, price, max_students, enrolled_count, duration_hours, status, description) VALUES
('MySQL 入门到精通', 'COURSE-001', 1, 199.00, 500, 456, 40, 1, '全面的MySQL入门教程'),
('PostgreSQL 高级进阶', 'COURSE-002', 3, 299.00, 300, 234, 60, 1, '深入理解PG内部原理'),
('数据库性能优化', 'COURSE-003', 4, 399.00, 200, 189, 80, 1, '性能调优实战'),
('Go 语言基础', 'COURSE-004', 1, 149.00, 800, 723, 30, 1, 'Go语言入门'),
('Docker 容器实战', 'COURSE-005', 2, 249.00, 400, 356, 50, 1, 'Docker实战教程'),
('Kubernetes 运维', 'COURSE-006', 3, 349.00, 250, 198, 70, 1, 'K8s运维实战'),
('Redis 缓存实战', 'COURSE-007', 2, 199.00, 600, 534, 35, 1, 'Redis应用场景'),
('微服务架构', 'COURSE-008', 4, 499.00, 150, 123, 100, 1, '微服务设计与实现'),
('CI/CD 自动化', 'COURSE-009', 2, 179.00, 500, 445, 25, 1, '自动化部署实践'),
('系统架构设计', 'COURSE-010', 4, 599.00, 100, 87, 120, 1, '架构师必修课');

-- case_152_edu_enrollments (7列)
TRUNCATE TABLE case_152_edu_enrollments;
INSERT INTO case_152_edu_enrollments (student_id, course_id, enroll_status, progress) VALUES
(1, 1, 2, 100), (1, 4, 1, 65),
(2, 2, 1, 45), (2, 5, 1, 30),
(3, 3, 2, 100), (3, 6, 1, 20),
(4, 7, 1, 78), (4, 8, 1, 15),
(5, 9, 2, 100), (5, 10, 1, 10);

-- case_152_edu_chapters (5列)
TRUNCATE TABLE case_152_edu_chapters;
INSERT INTO case_152_edu_chapters (course_id, chapter_title, chapter_order, duration_minutes, is_free) VALUES
(1, 'MySQL 简介', 1, 30, 1), (1, '安装与配置', 2, 45, 1),
(1, '数据类型', 3, 60, 0), (1, 'SQL 基础', 4, 90, 0),
(1, '索引优化', 5, 75, 0), (2, 'PG 架构', 1, 60, 1),
(2, 'MVCC 原理', 2, 90, 0), (2, '查询优化', 3, 75, 0),
(2, '并发控制', 4, 80, 0), (2, '性能调优', 5, 95, 0);

-- case_153_med_patients (13列)
TRUNCATE TABLE case_153_med_patients;
INSERT INTO case_153_med_patients (patient_no, name, gender, birthday, phone, address, blood_type) VALUES
('PAT-001', '张三', 1, '1990-01-01', '13800000001', '北京市朝阳区', 'A'),
('PAT-002', '李四', 2, '1985-05-15', '13800000002', '上海市浦东新区', 'B'),
('PAT-003', '王五', 1, '1978-08-20', '13800000003', '广州市天河区', 'O'),
('PAT-004', '赵六', 2, '1992-12-10', '13800000004', '深圳市南山区', 'AB'),
('PAT-005', '钱七', 1, '1988-03-25', '13800000005', '杭州市西湖区', 'A'),
('PAT-006', '孙八', 2, '1995-07-08', '13800000006', '成都市武侯区', 'B'),
('PAT-007', '周九', 1, '1982-11-30', '13800000007', '武汉市江汉区', 'O'),
('PAT-008', '吴十', 2, '1970-04-18', '13800000008', '南京市鼓楼区', 'AB'),
('PAT-009', '郑十一', 1, '1998-09-22', '13800000009', '西安市雁塔区', 'A'),
('PAT-010', '王十二', 2, '1987-06-14', '13800000010', '重庆市渝北区', 'B');

-- case_153_med_doctors (9列)
TRUNCATE TABLE case_153_med_doctors;
INSERT INTO case_153_med_doctors (doctor_no, name, title, department_id, specialty, status) VALUES
('DOC-001', '李医生', '主任医师', 1, '心血管内科', 1),
('DOC-002', '王医生', '副主任医师', 2, '神经内科', 1),
('DOC-003', '张医生', '主治医师', 1, '消化内科', 1),
('DOC-004', '刘医生', '主任医师', 3, '骨科', 1),
('DOC-005', '陈医生', '副主任医师', 4, '呼吸内科', 1),
('DOC-006', '杨医生', '主治医师', 2, '内分泌科', 1),
('DOC-007', '黄医生', '主任医师', 5, '眼科', 1),
('DOC-008', '林医生', '副主任医师', 6, '耳鼻喉科', 1),
('DOC-009', '徐医生', '主治医师', 3, '皮肤科', 1),
('DOC-010', '孙医生', '主任医师', 7, '中医科', 1);

-- case_153_med_registrations (10列)
TRUNCATE TABLE case_153_med_registrations;
INSERT INTO case_153_med_registrations (reg_no, patient_id, doctor_id, reg_date, reg_time_slot, reg_type, reg_status, reg_fee, visit_room) VALUES
('REG-001', 1, 1, '2024-01-15', '09:00-09:30', 2, 2, 50.00, '诊室1'),
('REG-002', 2, 2, '2024-01-15', '10:00-10:30', 1, 2, 25.00, '诊室2'),
('REG-003', 3, 3, '2024-01-16', '09:00-09:30', 1, 2, 25.00, '诊室3'),
('REG-004', 4, 4, '2024-01-16', '14:00-14:30', 2, 2, 50.00, '诊室4'),
('REG-005', 5, 5, '2024-01-17', '09:00-09:30', 1, 2, 25.00, '诊室5'),
('REG-006', 6, 6, '2024-01-17', '10:00-10:30', 1, 2, 25.00, '诊室6'),
('REG-007', 7, 7, '2024-01-18', '09:00-09:30', 3, 2, 100.00, '诊室7'),
('REG-008', 8, 8, '2024-01-18', '14:00-14:30', 1, 2, 25.00, '诊室8'),
('REG-009', 9, 9, '2024-01-19', '09:00-09:30', 1, 2, 25.00, '诊室9'),
('REG-010', 10, 10, '2024-01-19', '10:00-10:30', 2, 2, 50.00, '诊室10');

-- case_153_med_medical_records (9列)
TRUNCATE TABLE case_153_med_medical_records;
INSERT INTO case_153_med_medical_records (reg_id, patient_id, doctor_id, chief_complaint, diagnosis, treatment_plan, visit_time) VALUES
(1, 1, 1, '胸闷气短', '冠心病', '药物治疗', '2024-01-15 09:00:00'),
(2, 2, 2, '头晕头痛', '脑梗塞', '住院观察', '2024-01-15 10:00:00'),
(3, 3, 3, '胃痛', '慢性胃炎', '药物治疗', '2024-01-16 09:00:00'),
(4, 4, 4, '骨折', '右臂骨折', '手术治疗', '2024-01-16 14:00:00'),
(5, 5, 5, '咳嗽', '支气管炎', '药物治疗', '2024-01-17 09:00:00'),
(6, 6, 6, '多饮多尿', '糖尿病', '药物+饮食控制', '2024-01-17 10:00:00'),
(7, 7, 7, '视力下降', '白内障', '手术建议', '2024-01-18 09:00:00'),
(8, 8, 8, '鼻塞', '鼻窦炎', '药物治疗', '2024-01-18 14:00:00'),
(9, 9, 9, '皮疹', '湿疹', '外用药治疗', '2024-01-19 09:00:00'),
(10, 10, 10, '失眠', '神经衰弱', '中药调理', '2024-01-19 10:00:00');

-- case_154_hotel_room_types (12列)
TRUNCATE TABLE case_154_hotel_room_types;
INSERT INTO case_154_hotel_room_types (type_name, type_code, bed_type, max_occupancy, area_sqm, base_price, weekend_price, status) VALUES
('豪华大床房', 'DELUXE_KING', 1, 2, 45, 888.00, 1088.00, 1),
('标准双床房', 'STANDARD_TWIN', 2, 2, 35, 588.00, 688.00, 1),
('行政套房', 'EXEC_SUITE', 1, 4, 80, 1588.00, 1888.00, 1),
('总统套房', 'PRESIDENTIAL', 3, 6, 200, 5888.00, 6888.00, 1),
('经济单人间', 'ECONOMY_SINGLE', 1, 1, 20, 288.00, 338.00, 1),
('海景大床房', 'SEA_VIEW_KING', 1, 2, 50, 1288.00, 1588.00, 1),
('花园双床房', 'GARDEN_TWIN', 2, 2, 40, 688.00, 788.00, 1),
('商务套房', 'BUSINESS_SUITE', 1, 3, 65, 1088.00, 1288.00, 1),
('家庭房', 'FAMILY', 2, 4, 55, 988.00, 1188.00, 1),
('青年旅舍床位', 'HOSTEL_BED', 1, 1, 10, 98.00, 128.00, 1);

-- case_154_hotel_rooms (7列)
TRUNCATE TABLE case_154_hotel_rooms;
INSERT INTO case_154_hotel_rooms (room_no, type_id, floor, status) VALUES
('8001', 1, 8, 2), ('8002', 1, 8, 1), ('6001', 2, 6, 2),
('6002', 2, 6, 3), ('10001', 3, 10, 1), ('12001', 4, 12, 2),
('2001', 5, 2, 1), ('2002', 5, 2, 3), ('9001', 6, 9, 2),
('3001', 7, 3, 1);

-- case_154_hotel_orders (16列)
TRUNCATE TABLE case_154_hotel_orders;
INSERT INTO case_154_hotel_orders (order_no, user_id, room_id, check_in_date, check_out_date, nights, guests, room_price, total_amount, paid_amount, order_status, guest_name, guest_phone) VALUES
('HTL-001', 1, 1, '2024-02-01', '2024-02-03', 2, 2, 888.00, 1776.00, 1776.00, 4, '张三', '13800000001'),
('HTL-002', 2, 3, '2024-02-10', '2024-02-12', 2, 2, 588.00, 1176.00, 1176.00, 4, '李四', '13800000002'),
('HTL-003', 3, 5, '2024-03-01', '2024-03-04', 3, 2, 1588.00, 4764.00, 4764.00, 3, '王五', '13800000003'),
('HTL-004', 4, 7, '2024-03-15', '2024-03-16', 1, 1, 288.00, 288.00, 0, 1, '赵六', '13800000004'),
('HTL-005', 5, 9, '2024-04-01', '2024-04-03', 2, 2, 1288.00, 2576.00, 2576.00, 2, '钱七', '13800000005'),
('HTL-006', 6, 2, '2024-04-15', '2024-04-17', 2, 2, 888.00, 1776.00, 1776.00, 4, '孙八', '13800000006'),
('HTL-007', 7, 4, '2024-05-01', '2024-05-02', 1, 2, 688.00, 688.00, 688.00, 4, '周九', '13800000007'),
('HTL-008', 8, 6, '2024-05-15', '2024-05-16', 1, 2, 1088.00, 1088.00, 1088.00, 4, '吴十', '13800000008'),
('HTL-009', 9, 8, '2024-06-01', '2024-06-03', 2, 2, 988.00, 1976.00, 0, 0, '郑十一', '13800000009'),
('HTL-010', 10, 10, '2024-06-15', '2024-06-16', 1, 1, 98.00, 98.00, 98.00, 4, '王十二', '13800000010');

-- case_155_rest_categories (6列)
TRUNCATE TABLE case_155_rest_categories;
INSERT INTO case_155_rest_categories (parent_id, category_name, sort_order, status) VALUES
(0, '全部菜品', 1, 1),
(1, '热菜', 10, 1),
(1, '凉菜', 20, 1),
(1, '汤品', 30, 1),
(1, '主食', 40, 1),
(1, '甜点', 50, 1),
(1, '饮品', 60, 1),
(2, '川菜', 100, 1),
(2, '粤菜', 101, 1),
(2, '湘菜', 102, 1);

-- case_155_rest_orders (12列)
TRUNCATE TABLE case_155_rest_orders;
INSERT INTO case_155_rest_orders (order_no, table_no, order_type, order_status, subtotal, discount_amount, total_amount, payment_status) VALUES
('REST-001', 'A01', 1, 3, 156.00, 20.00, 136.00, 1),
('REST-002', 'A02', 1, 3, 234.00, 30.00, 204.00, 1),
('REST-003', 'B01', 2, 2, 89.00, 0, 89.00, 1),
('REST-004', 'B02', 1, 3, 345.00, 50.00, 295.00, 1),
('REST-005', 'C01', 2, 1, 67.00, 0, 67.00, 0),
('REST-006', 'C02', 1, 3, 178.00, 10.00, 168.00, 1),
('REST-007', 'A03', 1, 3, 267.00, 30.00, 237.00, 1),
('REST-008', 'B03', 3, 2, 45.00, 0, 45.00, 1),
('REST-009', 'A04', 1, 3, 456.00, 60.00, 396.00, 1),
('REST-010', 'C03', 2, 1, 123.00, 0, 123.00, 0);

-- case_155_rest_order_items (7列)
TRUNCATE TABLE case_155_rest_order_items;
INSERT INTO case_155_rest_order_items (order_id, dish_id, dish_name, unit_price, quantity, subtotal) VALUES
(1, 1, '宫保鸡丁', 38.00, 2, 76.00),
(1, 5, '麻婆豆腐', 18.00, 1, 18.00),
(2, 3, '水煮鱼', 68.00, 1, 68.00),
(2, 9, '拍黄瓜', 10.00, 2, 20.00),
(3, 6, '炒饭', 15.00, 1, 15.00),
(3, 10, '紫菜蛋花汤', 8.00, 2, 16.00),
(4, 4, '清蒸鲈鱼', 88.00, 1, 88.00),
(4, 2, '鱼香肉丝', 32.00, 3, 96.00),
(5, 7, '可乐', 5.00, 3, 15.00),
(5, 8, '红豆双皮奶', 12.00, 2, 24.00);

-- case_156_orders_parent (4列)
TRUNCATE TABLE case_156_orders_parent;
INSERT INTO case_156_orders_parent (tenant_id, order_no, status) VALUES
(1, 'ORD-001', 1), (1, 'ORD-002', 2),
(2, 'ORD-003', 1), (2, 'ORD-004', 3),
(3, 'ORD-005', 0), (3, 'ORD-006', 1),
(4, 'ORD-007', 2), (4, 'ORD-008', 1),
(5, 'ORD-009', 3), (5, 'ORD-010', 1);

-- case_156_orders_child (7列)
/**
SET FOREIGN_KEY_CHECKS = 0;
TRUNCATE TABLE case_156_orders_child;
TRUNCATE TABLE  case_156_orders_parent;
SET FOREIGN_KEY_CHECKS = 1;
INSERT INTO case_156_orders_parent (tenant_id, order_no) VALUES
(1, 'ORD-001'), (1, 'ORD-002'), (2, 'ORD-003'), (2, 'ORD-004'),
(3, 'ORD-005'), (3, 'ORD-006'), (4, 'ORD-007'), (4, 'ORD-008'), (5, 'ORD-009');
**/

-- case_157_json_generated_index (6列)
TRUNCATE TABLE case_157_json_generated_index;
INSERT INTO case_157_json_generated_index (payload, tags) VALUES
('{"bizId": "BIZ-001", "eventTime": "2024-01-01 10:00:00.000"}', '["tag1", "tag2"]'),
('{"bizId": "BIZ-002", "eventTime": "2024-01-02 11:00:00.000"}', '["tag3"]'),
('{"bizId": "BIZ-003", "eventTime": "2024-01-03 12:00:00.000"}', '["tag4", "tag5"]'),
('{"bizId": "BIZ-004", "eventTime": "2024-01-04 13:00:00.000"}', '["tag6"]'),
('{"bizId": "BIZ-005", "eventTime": "2024-01-05 14:00:00.000"}', '["tag7", "tag8"]'),
('{"bizId": "BIZ-006", "eventTime": "2024-01-06 15:00:00.000"}', '["tag9"]'),
('{"bizId": "BIZ-007", "eventTime": "2024-01-07 16:00:00.000"}', '["tag10"]'),
('{"bizId": "BIZ-008", "eventTime": "2024-01-08 17:00:00.000"}', '["tag11", "tag12"]'),
('{"bizId": "BIZ-009", "eventTime": "2024-01-09 18:00:00.000"}', '["tag13"]'),
('{"bizId": "BIZ-010", "eventTime": "2024-01-10 19:00:00.000"}', '["tag14", "tag15"]');

-- case_158_temporal_mix (6列)
TRUNCATE TABLE case_158_temporal_mix;
INSERT INTO case_158_temporal_mix (d, t, dt, ts, y, period_label) VALUES
('2024-01-01', '08:00:00.000000', '2024-01-01 08:00:00.000000', '2024-01-01 08:00:00.000000', 2024, '2024-01'),
('2024-02-15', '12:30:00.000000', '2024-02-15 12:30:00.000000', '2024-02-15 12:30:00.000000', 2024, '2024-02'),
('2024-03-20', '18:45:30.123456', '2024-03-20 18:45:30.123456', '2024-03-20 18:45:30.123456', 2024, '2024-03'),
('2024-04-10', '06:15:00.000000', '2024-04-10 06:15:00.000000', '2024-04-10 06:15:00.000000', 2024, '2024-04'),
('2024-05-05', '21:21:21.000000', '2024-05-05 21:21:21.000000', '2024-05-05 21:21:21.000000', 2024, '2024-05'),
('2024-06-01', '00:00:00.000000', '2024-06-01 00:00:00.000000', '2024-06-01 00:00:00.000000', 2024, '2024-06'),
('2024-07-04', '11:11:11.111111', '2024-07-04 11:11:11.111111', '2024-07-04 11:11:11.111111', 2024, '2024-07'),
('2024-08-08', '08:08:08.000000', '2024-08-08 08:08:08.000000', '2024-08-08 08:08:08.000000', 2024, '2024-08'),
('2024-09-09', '09:09:09.000000', '2024-09-09 09:09:09.000000', '2024-09-09 09:09:09.000000', 2024, '2024-09'),
('2024-10-10', '10:10:10.000000', '2024-10-10 10:10:10.000000', '2024-10-10 10:10:10.000000', 2024, '2024-10');

-- case_159_text_blob_mix (8列)
TRUNCATE TABLE case_159_text_blob_mix;
INSERT INTO case_159_text_blob_mix (title, summary, content, attachment_name, attachment, hash_code) VALUES
('标题1', '摘要1', '正文内容1...', 'file1.txt', 'blob data 1', UNHEX(MD5('hash1'))),
('标题2', '摘要2', '正文内容2...', 'file2.pdf', 'blob data 2', UNHEX(MD5('hash2'))),
('标题3', '摘要3', '正文内容3...', 'file3.jpg', 'blob data 3', UNHEX(MD5('hash3'))),
('标题4', '摘要4', '正文内容4...', 'file4.doc', 'blob data 4', UNHEX(MD5('hash4'))),
('标题5', '摘要5', '正文内容5...', 'file5.xlsx', 'blob data 5', UNHEX(MD5('hash5'))),
('标题6', '摘要6', '正文内容6...', 'file6.csv', 'blob data 6', UNHEX(MD5('hash6'))),
('标题7', '摘要7', '正文内容7...', 'file7.zip', 'blob data 7', UNHEX(MD5('hash7'))),
('标题8', '摘要8', '正文内容8...', 'file8.mp3', 'blob data 8', UNHEX(MD5('hash8'))),
('标题9', '摘要9', '正文内容9...', 'file9.mp4', 'blob data 9', UNHEX(MD5('hash9'))),
('标题10', '摘要10', '正文内容10...', 'file10.json', 'blob data 10', UNHEX(MD5('hash10')));

-- case_160_numeric_boundary (12列)
TRUNCATE TABLE case_160_numeric_boundary;
INSERT INTO case_160_numeric_boundary (tiny_signed, tiny_unsigned, int_signed, int_unsigned, big_signed, big_unsigned, dec_low, dec_high, fl, db, ratio, serial_no) VALUES
(-128, 0, -2147483648, 0, -9223372036854775808, 0, 0, 123456789012345678901234567890.123456789012345678901234567890, 1.1, 2.2, 0.1234567890, 1),
(-100, 100, -1000000000, 1000000000, -5000000000000000000, 5000000000000000000, 100, 987654321098765432109876543210.987654321098765432109876543210, 3.3, 4.4, 0.2345678901, 2),
(-50, 200, -500000000, 500000000, -2000000000000000000, 2000000000000000000, 500, 111111111111111111111111111111.111111111111111111111111111111, 5.5, 6.6, 0.3456789012, 3),
(0, 255, 0, 4294967295, 0, 18446744073709551615, 1000, 222222222222222222222222222222.222222222222222222222222222222, 7.7, 8.8, 0.4567890123, 4),
(50, 128, 500000000, 500000000, 2000000000000000000, 2000000000000000000, 1500, 333333333333333333333333333333.333333333333333333333333333333, 9.9, 10.10, 0.5678901234, 5),
(100, 64, 1000000000, 1000000000, 5000000000000000000, 5000000000000000000, 2000, 444444444444444444444444444444.444444444444444444444444444444, 11.1, 12.12, 0.6789012345, 6),
(-75, 150, -750000000, 750000000, -3000000000000000000, 3000000000000000000, 2500, 555555555555555555555555555555.555555555555555555555555555555, 13.13, 14.14, 0.7890123456, 7),
(127, 1, 2147483647, 1, 9223372036854775807, 1, 9999, 666666666666666666666666666666.666666666666666666666666666666, 15.15, 16.16, 0.8901234567, 8),
(-1, 254, -1, 4294967294, -1, 18446744073709551614, 0, 777777777777777777777777777777.777777777777777777777777777777, 17.17, 18.18, 0.9012345678, 9),
(1, 2, 1, 2, 1, 2, 1, 888888888888888888888888888888.888888888888888888888888888888, 19.19, 20.20, 0.0123456789, 10);

-- case_162_auto_inc_option (3列)
TRUNCATE TABLE case_162_auto_inc_option;
INSERT INTO case_162_auto_inc_option (name) VALUES
('Name 1'), ('Name 2'), ('Name 3'), ('Name 4'), ('Name 5'),
('Name 6'), ('Name 7'), ('Name 8'), ('Name 9'), ('Name 10');

-- case_163_fk_action_parent (2列)
/**
SET FOREIGN_KEY_CHECKS = 0;
TRUNCATE TABLE case_163_fk_action_child;
TRUNCATE TABLE case_163_fk_action_parent;
SET FOREIGN_KEY_CHECKS = 1;
INSERT INTO case_163_fk_action_parent (id, code) VALUES
(1, 'CODE-001'), (2, 'CODE-002'), (3, 'CODE-003'), (4, 'CODE-004'), (5, 'CODE-005'),
(6, 'CODE-006'), (7, 'CODE-007'), (8, 'CODE-008'), (9, 'CODE-009'), (10, 'CODE-010');

INSERT INTO case_163_fk_action_child (id, parent_id, parent_code) VALUES
(1, 1, 'CODE-001'), (2, 1, 'CODE-001'), (3, 2, 'CODE-002'), (4, 2, 'CODE-002'),
(5, 3, 'CODE-003'), (6, 3, 'CODE-003'), (7, 4, 'CODE-004'), (8, 4, 'CODE-004'),
(9, 5, 'CODE-005'), (10, 5, 'CODE-005');
**/

-- case_164_org_tree (4列)
TRUNCATE TABLE case_164_org_tree;
INSERT INTO case_164_org_tree (id, parent_id, org_name, org_level) VALUES
(1, NULL, '总公司', 1),
(2, 1, '技术部', 2), (3, 1, '市场部', 2),
(4, 2, '前端组', 3), (5, 2, '后端组', 3),
(6, 3, '品牌组', 3), (7, 3, '推广组', 3),
(8, 4, 'React 小组', 4), (9, 5, 'Go 小组', 4),
(10, 5, 'Java 小组', 4);

-- case_165_check_enforced (3列)
TRUNCATE TABLE case_165_check_enforced;
INSERT INTO case_165_check_enforced (id, amount, status) VALUES
(1, 100.00, 0), (2, 200.00, 1), (3, 300.00, 2),
(4, 400.00, 0), (5, 500.00, 1), (6, 600.00, 2),
(7, 700.00, 0), (8, 800.00, 1), (9, 900.00, 2), (10, 1000.00, 0);

-- case_166_memory_rowfmt (4列)
TRUNCATE TABLE case_166_memory_rowfmt;
INSERT INTO case_166_memory_rowfmt (id, session_key, session_value) VALUES
(1, 'session_001', 'value_001'), (2, 'session_002', 'value_002'),
(3, 'session_003', 'value_003'), (4, 'session_004', 'value_004'),
(5, 'session_005', 'value_005'), (6, 'session_006', 'value_006'),
(7, 'session_007', 'value_007'), (8, 'session_008', 'value_008'),
(9, 'session_009', 'value_009'), (10, 'session_010', 'value_010');

-- case_167_merge (4列)
TRUNCATE TABLE case_167_merge;
INSERT INTO case_167_merge (id, tenant_id, biz_no) VALUES
(1, 1, 'BIZ-001'), (2, 1, 'BIZ-002'), (3, 2, 'BIZ-003'),
(4, 2, 'BIZ-004'), (5, 3, 'BIZ-005'), (6, 3, 'BIZ-006'),
(7, 4, 'BIZ-007'), (8, 4, 'BIZ-008'), (9, 5, 'BIZ-009'), (10, 5, 'BIZ-010');

-- ============================================================================
-- para_normalize (10列: PRIMARY KEY ... USING BTREE 典型案例)
-- 用于测试 MySQL 8.0 默认主键带 USING BTREE 子句的迁移场景
-- ============================================================================
TRUNCATE TABLE case_168_merge;
INSERT INTO case_168_merge (front_name, queen_name, usestatus, type, retain, create_by, create_time, update_by, update_time) VALUES
('周杰伦', 'Jay Chou', 0, 1, 2, 1, '2024-01-01 10:00:00', 1, '2024-01-01 10:00:00'),
('林俊杰', 'JJ Lin', 0, 1, 2, 1, '2024-01-02 10:00:00', 1, '2024-01-02 10:00:00'),
('Eason Chan', '陈奕迅', 0, 1, 2, 1, '2024-01-03 10:00:00', 1, '2024-01-03 10:00:00'),
('Faye Wong', '王菲', 0, 1, 2, 1, '2024-01-04 10:00:00', 1, '2024-01-04 10:00:00'),
('David Tao', '陶喆', 1, 1, 2, 1, '2024-01-05 10:00:00', 1, '2024-01-05 10:00:00'),
(' ', ',', 0, 0, 0, 1, '2024-01-06 10:00:00', 1, '2024-01-06 10:00:00'),
('、', ',', 0, 0, 0, 1, '2024-01-07 10:00:00', 1, '2024-01-07 10:00:00'),
('（', '(', 0, 0, 1, 1, '2024-01-08 10:00:00', 1, '2024-01-08 10:00:00'),
('）', ')', 0, 0, 1, 1, '2024-01-09 10:00:00', 1, '2024-01-09 10:00:00'),
('张学友', 'Jacky Cheung', 0, 1, 2, 1, '2024-01-10 10:00:00', 1, '2024-01-10 10:00:00');
