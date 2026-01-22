/*****
视图特点：
1. 使用MySQL保留字作为列别名（rank、system、groups、window、function、role、admin、user等）
2. 包含多表LEFT JOIN操作，实现表间关联查询
3. 每个视图包含丰富的函数操作，行数约50行
4. 支持多种MySQL函数：
   - 字符串函数：CONCAT、LENGTH、SUBSTRING、UPPER、LOWER、TRIM、REVERSE等
   - 数值函数：ROUND、CEIL、FLOOR、ABS、MOD、POWER、SQRT等
   - 条件函数：CASE、IFNULL、COALESCE等
   - 日期函数：YEAR、MONTH、DAY、DATE_FORMAT、DATEDIFF等
   - JSON函数：JSON_EXTRACT、JSON_UNQUOTE、JSON_KEYS、JSON_LENGTH等
5. 使用CREATE OR REPLACE VIEW语法，支持视图更新
6. 包含类型转换操作（CAST函数）

***/
-- 为 case_01_integers 表创建视图
CREATE OR REPLACE VIEW view_case01_integers AS
SELECT 
    i.col_tiny AS `rank`,
    i.col_small AS `system`,
    i.col_medium AS `groups`,
    i.col_int AS `window`,
    i.col_integer AS `function`,
    i.col_big AS `role`,
    i.col_int_prec AS `admin`,
    i.col_big_prec AS `user`,
    b.is_active AS `status`,
    b.is_deleted AS `type`,
    f.col_float AS `float_value`,
    f.col_double AS `double_value`,
    f.col_decimal AS `decimal_value`,
    CASE 
        WHEN i.col_tiny > 0 THEN 'positive'
        WHEN i.col_tiny < 0 THEN 'negative'
        ELSE 'zero'
    END AS `case_result`,
    IFNULL(i.col_small, 0) AS `ifnull_result`,
    COALESCE(i.col_medium, i.col_int, 0) AS `coalesce_result`,
    CONCAT('Value: ', i.col_integer) AS `concat_result`,
    LENGTH(i.col_big) AS `length_result`,
    SUBSTRING(i.col_int_prec, 1, 5) AS `substring_result`,
    ROUND(i.col_big_prec, 2) AS `round_result`,
    CEIL(i.col_tiny) AS `ceil_result`,
    FLOOR(i.col_small) AS `floor_result`,
    ABS(i.col_medium) AS `abs_result`,
    MOD(i.col_int, 10) AS `mod_result`,
    POWER(i.col_integer, 2) AS `power_result`,
    SQRT(i.col_big) AS `sqrt_result`,
    SIN(i.col_int_prec) AS `sin_result`,
    COS(i.col_big_prec) AS `cos_result`,
    TAN(i.col_tiny) AS `tan_result`,
    LOG(i.col_small) AS `log_result`,
    EXP(i.col_medium) AS `exp_result`,
    GREATEST(i.col_int, i.col_integer, i.col_big) AS `greatest_result`,
    LEAST(i.col_int, i.col_integer, i.col_big) AS `least_result`,
    INSTR(i.col_big_prec, '5') AS `instr_result`,
    REPLACE(i.col_int_prec, '1', '0') AS `replace_result`,
    TRIM(i.col_small) AS `trim_result`,
    UPPER(i.col_medium) AS `upper_result`,
    LOWER(i.col_int) AS `lower_result`,
    REVERSE(i.col_integer) AS `reverse_result`,
    LEFT(i.col_big, 5) AS `left_result`,
    RIGHT(i.col_int_prec, 5) AS `right_result`,
    MID(i.col_big_prec, 2, 3) AS `mid_result`
FROM 
    case_01_integers i
LEFT JOIN 
    case_02_boolean b ON i.col_tiny = b.status
LEFT JOIN 
    case_03_floats f ON i.col_small = CAST(f.col_float AS SIGNED);

-- 为 case_02_boolean 表创建视图
CREATE OR REPLACE VIEW view_case02_boolean AS
SELECT 
    b.is_active AS `rank`,
    b.status AS `system`,
    b.is_deleted AS `groups`,
    i.col_tiny AS `window`,
    i.col_small AS `function`,
    i.col_medium AS `role`,
    i.col_int AS `admin`,
    i.col_integer AS `user`,
    f.col_float AS `float_value`,
    f.col_double AS `double_value`,
    f.col_decimal AS `decimal_value`,
    CASE 
        WHEN b.is_active = 1 THEN 'active'
        ELSE 'inactive'
    END AS `case_result`,
    IFNULL(b.status, 0) AS `ifnull_result`,
    COALESCE(b.is_deleted, b.is_active, 0) AS `coalesce_result`,
    CONCAT('Status: ', b.status) AS `concat_result`,
    LENGTH(b.status) AS `length_result`,
    SUBSTRING(b.status, 1, 2) AS `substring_result`,
    ROUND(b.is_active, 2) AS `round_result`,
    CEIL(b.status) AS `ceil_result`,
    FLOOR(b.is_active) AS `floor_result`,
    ABS(b.status) AS `abs_result`,
    MOD(b.status, 2) AS `mod_result`,
    POWER(b.is_active, 2) AS `power_result`,
    SQRT(b.status) AS `sqrt_result`,
    SIN(b.is_deleted) AS `sin_result`,
    COS(b.status) AS `cos_result`,
    TAN(b.is_active) AS `tan_result`,
    LOG(b.status) AS `log_result`,
    EXP(b.is_active) AS `exp_result`,
    GREATEST(b.is_active, b.status, b.is_deleted) AS `greatest_result`,
    LEAST(b.is_active, b.status, b.is_deleted) AS `least_result`,
    INSTR(b.status, '1') AS `instr_result`,
    REPLACE(b.status, '0', '1') AS `replace_result`,
    TRIM(b.status) AS `trim_result`,
    UPPER(b.status) AS `upper_result`,
    LOWER(b.status) AS `lower_result`,
    REVERSE(b.status) AS `reverse_result`,
    LEFT(b.status, 1) AS `left_result`,
    RIGHT(b.status, 1) AS `right_result`,
    MID(b.status, 1, 1) AS `mid_result`
FROM 
    case_02_boolean b
LEFT JOIN 
    case_01_integers i ON b.status = i.col_tiny
LEFT JOIN 
    case_03_floats f ON b.is_active = CAST(f.col_float AS SIGNED);

-- 为 case_03_floats 表创建视图
CREATE OR REPLACE VIEW view_case03_floats AS
SELECT 
    f.col_float AS `rank`,
    f.col_float_p AS `system`,
    f.col_float_ps AS `groups`,
    f.col_double AS `window`,
    f.col_double_ps AS `function`,
    f.col_decimal AS `role`,
    f.col_numeric AS `admin`,
    f.col_real AS `user`,
    i.col_tiny AS `int_value`,
    i.col_small AS `small_value`,
    i.col_medium AS `medium_value`,
    b.is_active AS `active_status`,
    b.is_deleted AS `deleted_status`,
    CASE 
        WHEN f.col_float > 0 THEN 'positive'
        WHEN f.col_float < 0 THEN 'negative'
        ELSE 'zero'
    END AS `case_result`,
    IFNULL(f.col_float, 0) AS `ifnull_result`,
    COALESCE(f.col_double, f.col_real, 0) AS `coalesce_result`,
    CONCAT('Float: ', f.col_float) AS `concat_result`,
    LENGTH(f.col_float) AS `length_result`,
    SUBSTRING(f.col_float, 1, 5) AS `substring_result`,
    ROUND(f.col_float, 2) AS `round_result`,
    CEIL(f.col_float) AS `ceil_result`,
    FLOOR(f.col_float) AS `floor_result`,
    ABS(f.col_float) AS `abs_result`,
    MOD(f.col_float, 10) AS `mod_result`,
    POWER(f.col_float, 2) AS `power_result`,
    SQRT(f.col_float) AS `sqrt_result`,
    SIN(f.col_float) AS `sin_result`,
    COS(f.col_float) AS `cos_result`,
    TAN(f.col_float) AS `tan_result`,
    LOG(f.col_float) AS `log_result`,
    EXP(f.col_float) AS `exp_result`,
    GREATEST(f.col_float, f.col_double, f.col_real) AS `greatest_result`,
    LEAST(f.col_float, f.col_double, f.col_real) AS `least_result`,
    INSTR(f.col_float, '.') AS `instr_result`,
    REPLACE(f.col_float, '.', ',') AS `replace_result`,
    TRIM(f.col_float) AS `trim_result`,
    UPPER(f.col_float) AS `upper_result`,
    LOWER(f.col_float) AS `lower_result`,
    REVERSE(f.col_float) AS `reverse_result`,
    LEFT(f.col_float, 5) AS `left_result`,
    RIGHT(f.col_float, 5) AS `right_result`,
    MID(f.col_float, 2, 3) AS `mid_result`
FROM 
    case_03_floats f
LEFT JOIN 
    case_01_integers i ON CAST(f.col_float AS SIGNED) = i.col_tiny
LEFT JOIN 
    case_02_boolean b ON CAST(f.col_float AS SIGNED) = b.status;

-- 为 case_04_mb3_suffix 表创建视图
CREATE OR REPLACE VIEW view_case04_mb3_suffix AS
SELECT 
    m.col_var_mb3 AS `rank`,
    m.col_char_mb3 AS `system`,
    m.col_text_mb3 AS `groups`,
    m.col_mixed_mb3 AS `window`,
    i.col_tiny AS `function`,
    i.col_small AS `role`,
    i.col_medium AS `admin`,
    i.col_int AS `user`,
    b.is_active AS `status`,
    b.is_deleted AS `type`,
    f.col_float AS `float_value`,
    f.col_double AS `double_value`,
    CASE 
        WHEN LENGTH(m.col_var_mb3) > 10 THEN 'long'
        ELSE 'short'
    END AS `case_result`,
    IFNULL(m.col_var_mb3, 'default') AS `ifnull_result`,
    COALESCE(m.col_char_mb3, m.col_mixed_mb3, 'default') AS `coalesce_result`,
    CONCAT('Var: ', m.col_var_mb3) AS `concat_result`,
    LENGTH(m.col_var_mb3) AS `length_result`,
    SUBSTRING(m.col_var_mb3, 1, 10) AS `substring_result`,
    UPPER(m.col_var_mb3) AS `upper_result`,
    LOWER(m.col_var_mb3) AS `lower_result`,
    TRIM(m.col_var_mb3) AS `trim_result`,
    REVERSE(m.col_var_mb3) AS `reverse_result`,
    LEFT(m.col_var_mb3, 5) AS `left_result`,
    RIGHT(m.col_var_mb3, 5) AS `right_result`,
    MID(m.col_var_mb3, 2, 5) AS `mid_result`,
    INSTR(m.col_var_mb3, 'a') AS `instr_result`,
    REPLACE(m.col_var_mb3, 'a', 'b') AS `replace_result`,
    CONCAT_WS('-', m.col_var_mb3, m.col_char_mb3) AS `concat_ws_result`,
    SUBSTRING_INDEX(m.col_var_mb3, ' ', 1) AS `substring_index_result`,
    CHAR_LENGTH(m.col_var_mb3) AS `char_length_result`,
    LPAD(m.col_var_mb3, 20, '0') AS `lpad_result`,
    RPAD(m.col_var_mb3, 20, '0') AS `rpad_result`,
    REPEAT(m.col_var_mb3, 2) AS `repeat_result`,
    SPACE(5) AS `space_result`,
    STRCMP(m.col_var_mb3, m.col_char_mb3) AS `strcmp_result`,
    LCASE(m.col_var_mb3) AS `lcase_result`,
    UCASE(m.col_var_mb3) AS `ucase_result`,
    ASCII(m.col_var_mb3) AS `ascii_result`,
    BIN(ASCII(m.col_var_mb3)) AS `bin_result`,
    HEX(ASCII(m.col_var_mb3)) AS `hex_result`
FROM 
    case_04_mb3_suffix m
LEFT JOIN 
    case_01_integers i ON LENGTH(m.col_var_mb3) = i.col_tiny
LEFT JOIN 
    case_02_boolean b ON LENGTH(m.col_var_mb3) = b.status
LEFT JOIN 
    case_03_floats f ON LENGTH(m.col_var_mb3) = CAST(f.col_float AS SIGNED);

-- 为 case_05_charsets 表创建视图
CREATE OR REPLACE VIEW view_case05_charsets AS
SELECT 
    c.c1 AS `rank`,
    c.c2 AS `system`,
    c.c3 AS `groups`,
    c.c4 AS `window`,
    c.c5 AS `function`,
    c.c6 AS `role`,
    i.col_tiny AS `admin`,
    i.col_small AS `user`,
    b.is_active AS `status`,
    b.is_deleted AS `type`,
    f.col_float AS `float_value`,
    m.col_var_mb3 AS `text_value`,
    CASE 
        WHEN LENGTH(c.c1) > 5 THEN 'long'
        ELSE 'short'
    END AS `case_result`,
    IFNULL(c.c1, 'default') AS `ifnull_result`,
    COALESCE(c.c2, c.c3, c.c4, 'default') AS `coalesce_result`,
    CONCAT('C1: ', c.c1) AS `concat_result`,
    LENGTH(c.c1) AS `length_result`,
    SUBSTRING(c.c1, 1, 5) AS `substring_result`,
    UPPER(c.c1) AS `upper_result`,
    LOWER(c.c1) AS `lower_result`,
    TRIM(c.c1) AS `trim_result`,
    REVERSE(c.c1) AS `reverse_result`,
    LEFT(c.c1, 3) AS `left_result`,
    RIGHT(c.c1, 3) AS `right_result`,
    MID(c.c1, 2, 3) AS `mid_result`,
    INSTR(c.c1, 'a') AS `instr_result`,
    REPLACE(c.c1, 'a', 'b') AS `replace_result`,
    CONCAT_WS('-', c.c1, c.c2, c.c3) AS `concat_ws_result`,
    SUBSTRING_INDEX(c.c1, ' ', 1) AS `substring_index_result`,
    CHAR_LENGTH(c.c1) AS `char_length_result`,
    LPAD(c.c1, 10, '0') AS `lpad_result`,
    RPAD(c.c1, 10, '0') AS `rpad_result`,
    REPEAT(c.c1, 2) AS `repeat_result`,
    SPACE(3) AS `space_result`,
    STRCMP(CONVERT(c.c1 USING utf8mb4) COLLATE utf8mb4_unicode_ci, c.c2) AS `strcmp_result`,
    LCASE(c.c1) AS `lcase_result`,
    UCASE(c.c1) AS `ucase_result`,
    ASCII(c.c1) AS `ascii_result`,
    BIN(ASCII(c.c1)) AS `bin_result`,
    HEX(ASCII(c.c1)) AS `hex_result`
FROM 
    case_05_charsets c
LEFT JOIN 
    case_01_integers i ON LENGTH(c.c1) = i.col_tiny
LEFT JOIN 
    case_02_boolean b ON LENGTH(c.c1) = b.status
LEFT JOIN 
    case_03_floats f ON LENGTH(c.c1) = CAST(f.col_float AS SIGNED)
LEFT JOIN 
    case_04_mb3_suffix m ON c.c1 = m.col_var_mb3;

-- 为 case_06_collates 表创建视图
CREATE OR REPLACE VIEW view_case06_collates AS
SELECT 
    c.c1 AS `rank`,
    c.c2 AS `system`,
    c.c3 AS `groups`,
    c.c4 AS `window`,
    c.c5 AS `function`,
    i.col_tiny AS `role`,
    i.col_small AS `admin`,
    i.col_medium AS `user`,
    b.is_active AS `status`,
    b.is_deleted AS `type`,
    f.col_float AS `float_value`,
    m.col_var_mb3 AS `text_value`,
    ch.c1 AS `charset_value`,
    CASE 
        WHEN LENGTH(c.c1) > 8 THEN 'long'
        ELSE 'short'
    END AS `case_result`,
    IFNULL(c.c1, 'default') AS `ifnull_result`,
    COALESCE(c.c2, c.c3, c.c4, c.c5, 'default') AS `coalesce_result`,
    CONCAT('Collate: ', c.c1) AS `concat_result`,
    LENGTH(c.c1) AS `length_result`,
    SUBSTRING(c.c1, 1, 8) AS `substring_result`,
    UPPER(c.c1) AS `upper_result`,
    LOWER(c.c1) AS `lower_result`,
    TRIM(c.c1) AS `trim_result`,
    REVERSE(c.c1) AS `reverse_result`,
    LEFT(c.c1, 4) AS `left_result`,
    RIGHT(c.c1, 4) AS `right_result`,
    MID(c.c1, 2, 4) AS `mid_result`,
    INSTR(c.c1, '_') AS `instr_result`,
    REPLACE(c.c1, '_', '-') AS `replace_result`,
    CONCAT_WS('|', c.c1, c.c2, c.c3) AS `concat_ws_result`,
    SUBSTRING_INDEX(c.c1, '_', 1) AS `substring_index_result`,
    CHAR_LENGTH(c.c1) AS `char_length_result`,
    LPAD(c.c1, 15, ' ') AS `lpad_result`,
    RPAD(c.c1, 15, ' ') AS `rpad_result`,
    REPEAT(c.c1, 2) AS `repeat_result`,
    SPACE(4) AS `space_result`,
    STRCMP(c.c1 COLLATE utf8mb4_unicode_ci, c.c2) AS `strcmp_result`,
    LCASE(c.c1) AS `lcase_result`,
    UCASE(c.c1) AS `ucase_result`,
    ASCII(c.c1) AS `ascii_result`,
    BIN(ASCII(c.c1)) AS `bin_result`,
    HEX(ASCII(c.c1)) AS `hex_result`
FROM 
    case_06_collates c
LEFT JOIN 
    case_01_integers i ON LENGTH(c.c1) = i.col_tiny
LEFT JOIN 
    case_02_boolean b ON LENGTH(c.c1) = b.status
LEFT JOIN 
    case_03_floats f ON LENGTH(c.c1) = CAST(f.col_float AS SIGNED)
LEFT JOIN 
    case_04_mb3_suffix m ON c.c1 = m.col_var_mb3
LEFT JOIN 
    case_05_charsets ch ON c.c1 = ch.c1;

-- 为 case_07_complex_charsets 表创建视图
CREATE OR REPLACE VIEW view_case07_complex_charsets AS
SELECT 
    c.c1 AS `rank`,
    c.c2 AS `system`,
    c.c3 AS `groups`,
    i.col_tiny AS `window`,
    i.col_small AS `function`,
    i.col_medium AS `role`,
    i.col_int AS `admin`,
    i.col_integer AS `user`,
    b.is_active AS `status`,
    b.is_deleted AS `type`,
    f.col_float AS `float_value`,
    m.col_var_mb3 AS `text_value`,
    ch.c1 AS `charset_value`,
    co.c1 AS `collate_value`,
    CASE 
        WHEN LENGTH(c.c1) > 5 THEN 'long'
        ELSE 'short'
    END AS `case_result`,
    IFNULL(c.c1, 'default') AS `ifnull_result`,
    COALESCE(c.c2, c.c3, 'default') AS `coalesce_result`,
    CONCAT('Complex: ', c.c1) AS `concat_result`,
    LENGTH(c.c1) AS `length_result`,
    SUBSTRING(c.c1, 1, 5) AS `substring_result`,
    UPPER(c.c1) AS `upper_result`,
    LOWER(c.c1) AS `lower_result`,
    TRIM(c.c1) AS `trim_result`,
    REVERSE(c.c1) AS `reverse_result`,
    LEFT(c.c1, 3) AS `left_result`,
    RIGHT(c.c1, 3) AS `right_result`,
    MID(c.c1, 2, 3) AS `mid_result`,
    INSTR(c.c1, 'a') AS `instr_result`,
    REPLACE(c.c1, 'a', 'z') AS `replace_result`,
    CONCAT_WS('-', c.c1, c.c2, c.c3) AS `concat_ws_result`,
    SUBSTRING_INDEX(c.c1, ' ', 1) AS `substring_index_result`,
    CHAR_LENGTH(c.c1) AS `char_length_result`,
    LPAD(c.c1, 10, 'x') AS `lpad_result`,
    RPAD(c.c1, 10, 'x') AS `rpad_result`,
    REPEAT(c.c1, 2) AS `repeat_result`,
    SPACE(3) AS `space_result`,
    STRCMP(c.c1, c.c2) AS `strcmp_result`,
    LCASE(c.c1) AS `lcase_result`,
    UCASE(c.c1) AS `ucase_result`,
    ASCII(c.c1) AS `ascii_result`,
    BIN(ASCII(c.c1)) AS `bin_result`,
    HEX(ASCII(c.c1)) AS `hex_result`
FROM 
    case_07_complex_charsets c
LEFT JOIN 
    case_01_integers i ON LENGTH(c.c1) = i.col_tiny
LEFT JOIN 
    case_02_boolean b ON LENGTH(c.c1) = b.status
LEFT JOIN 
    case_03_floats f ON LENGTH(c.c1) = CAST(f.col_float AS SIGNED)
LEFT JOIN 
    case_04_mb3_suffix m ON c.c1 = m.col_var_mb3
LEFT JOIN 
    case_05_charsets ch ON c.c1 = ch.c1
LEFT JOIN 
    case_06_collates co ON c.c1 = co.c1;

-- 为 case_08_json 表创建视图
CREATE OR REPLACE VIEW view_case08_json AS
SELECT 
    j.data AS `rank`,
    j.data_len AS `system`,
    j.data_upper AS `groups`,
    i.col_tiny AS `window`,
    i.col_small AS `function`,
    i.col_medium AS `role`,
    i.col_int AS `admin`,
    i.col_integer AS `user`,
    b.is_active AS `status`,
    b.is_deleted AS `type`,
    f.col_float AS `float_value`,
    m.col_var_mb3 AS `text_value`,
    ch.c1 AS `charset_value`,
    co.c1 AS `collate_value`,
    cm.c1 AS `complex_value`,
    CASE 
        WHEN JSON_LENGTH(j.data) > 0 THEN 'has_data'
        ELSE 'empty'
    END AS `case_result`,
    IFNULL(j.data, '{}') AS `ifnull_result`,
    COALESCE(j.data_len, j.data_upper, j.data, '{}') AS `coalesce_result`,
    CONCAT('JSON: ', JSON_TYPE(j.data)) AS `concat_result`,
    LENGTH(j.data) AS `length_result`,
    SUBSTRING(j.data, 1, 20) AS `substring_result`,
    JSON_EXTRACT(j.data, '$.id') AS `json_extract_result`,
    JSON_UNQUOTE(JSON_EXTRACT(j.data, '$.name')) AS `json_unquote_result`,
    JSON_KEYS(j.data) AS `json_keys_result`,
    JSON_LENGTH(j.data) AS `json_length_result`,
    JSON_TYPE(j.data) AS `json_type_result`,
    JSON_VALID(j.data) AS `json_valid_result`,
    JSON_CONTAINS_PATH(j.data, 'one', '$.id') AS `json_contains_path_result`,
    JSON_DEPTH(j.data) AS `json_depth_result`,
    JSON_OVERLAPS(j.data, j.data_len) AS `json_overlaps_result`,
    JSON_PRETTY(j.data) AS `json_pretty_result`,
    TRIM(j.data) AS `trim_result`,
    UPPER(j.data) AS `upper_result`,
    LOWER(j.data) AS `lower_result`,
    REVERSE(j.data) AS `reverse_result`,
    LEFT(j.data, 10) AS `left_result`,
    RIGHT(j.data, 10) AS `right_result`,
    MID(j.data, 2, 10) AS `mid_result`,
    INSTR(j.data, 'id') AS `instr_result`,
    REPLACE(j.data, 'id', 'identifier') AS `replace_result`
FROM 
    case_08_json j
LEFT JOIN 
    case_01_integers i ON JSON_LENGTH(j.data) = i.col_tiny
LEFT JOIN 
    case_02_boolean b ON JSON_LENGTH(j.data) = b.status
LEFT JOIN 
    case_03_floats f ON JSON_LENGTH(j.data) = CAST(f.col_float AS SIGNED)
LEFT JOIN 
    case_04_mb3_suffix m ON JSON_UNQUOTE(JSON_EXTRACT(j.data, '$.name')) = m.col_var_mb3
LEFT JOIN 
    case_05_charsets ch ON JSON_UNQUOTE(JSON_EXTRACT(j.data, '$.name')) = ch.c1
LEFT JOIN 
    case_06_collates co ON JSON_UNQUOTE(JSON_EXTRACT(j.data, '$.name')) = co.c1
LEFT JOIN 
    case_07_complex_charsets cm ON JSON_UNQUOTE(JSON_EXTRACT(j.data, '$.name')) = cm.c1;

-- 为 case_09_datetime 表创建视图
CREATE OR REPLACE VIEW view_case09_datetime AS
SELECT 
    d.d1 AS `rank`,
    d.t1 AS `system`,
    d.t2 AS `groups`,
    d.dt1 AS `window`,
    d.dt2 AS `function`,
    d.ts1 AS `role`,
    d.ts2 AS `admin`,
    d.y1 AS `user`,
    i.col_tiny AS `int_value`,
    i.col_small AS `small_value`,
    b.is_active AS `status`,
    b.is_deleted AS `type`,
    f.col_float AS `float_value`,
    m.col_var_mb3 AS `text_value`,
    ch.c1 AS `charset_value`,
    co.c1 AS `collate_value`,
    cm.c1 AS `complex_value`,
    j.data AS `json_value`,
    CASE 
        WHEN d.d1 > CURDATE() THEN 'future'
        WHEN d.d1 < CURDATE() THEN 'past'
        ELSE 'today'
    END AS `case_result`,
    IFNULL(d.d1, CURDATE()) AS `ifnull_result`,
    COALESCE(d.dt1, d.ts1, NOW()) AS `coalesce_result`,
    CONCAT('Date: ', d.d1) AS `concat_result`,
    YEAR(d.d1) AS `year_result`,
    MONTH(d.d1) AS `month_result`,
    DAY(d.d1) AS `day_result`,
    HOUR(d.t1) AS `hour_result`,
    MINUTE(d.t1) AS `minute_result`,
    SECOND(d.t1) AS `second_result`,
    DATE_FORMAT(d.dt1, '%Y-%m-%d') AS `date_format_result`,
    DATE_ADD(d.d1, INTERVAL 1 DAY) AS `date_add_result`,
    DATE_SUB(d.d1, INTERVAL 1 DAY) AS `date_sub_result`,
    DATEDIFF(d.d1, CURDATE()) AS `datediff_result`,
    TIMEDIFF(d.t1, d.t2) AS `timediff_result`,
    NOW() AS `now_result`,
    CURDATE() AS `curdate_result`,
    CURTIME() AS `curtime_result`,
    UNIX_TIMESTAMP(d.dt1) AS `unix_timestamp_result`,
    FROM_UNIXTIME(UNIX_TIMESTAMP(d.dt1)) AS `from_unixtime_result`,
    DATE(d.dt1) AS `date_result`,
    TIME(d.dt1) AS `time_result`,
    LAST_DAY(d.d1) AS `last_day_result`,
    DAYOFWEEK(d.d1) AS `dayofweek_result`,
    DAYOFMONTH(d.d1) AS `dayofmonth_result`,
    DAYOFYEAR(d.d1) AS `dayofyear_result`,
    WEEK(d.d1) AS `week_result`,
    QUARTER(d.d1) AS `quarter_result`
FROM 
    case_09_datetime d
LEFT JOIN 
    case_01_integers i ON YEAR(d.d1) = i.col_tiny
LEFT JOIN 
    case_02_boolean b ON MONTH(d.d1) = b.status
LEFT JOIN 
    case_03_floats f ON DAY(d.d1) = CAST(f.col_float AS SIGNED)
LEFT JOIN 
    case_04_mb3_suffix m ON DATE_FORMAT(d.d1, '%Y-%m-%d') = m.col_var_mb3
LEFT JOIN 
    case_05_charsets ch ON DATE_FORMAT(d.d1, '%Y-%m-%d') = ch.c1
LEFT JOIN 
    case_06_collates co ON DATE_FORMAT(d.d1, '%Y-%m-%d') = co.c1
LEFT JOIN 
    case_07_complex_charsets cm ON DATE_FORMAT(d.d1, '%Y-%m-%d') = cm.c1
LEFT JOIN 
    case_08_json j ON DATE_FORMAT(d.d1, '%Y-%m-%d') = JSON_UNQUOTE(JSON_EXTRACT(j.data, '$.date'));

-- 为 case_10_defaults 表创建视图
CREATE OR REPLACE VIEW view_case10_defaults AS
SELECT 
    d.c1 AS `rank`,
    d.c2 AS `system`,
    d.c3 AS `groups`,
    d.c4 AS `window`,
    d.c5 AS `function`,
    d.c6 AS `role`,
    i.col_tiny AS `admin`,
    i.col_small AS `user`,
    b.is_active AS `status`,
    b.is_deleted AS `type`,
    f.col_float AS `float_value`,
    m.col_var_mb3 AS `text_value`,
    ch.c1 AS `charset_value`,
    co.c1 AS `collate_value`,
    cm.c1 AS `complex_value`,
    j.data AS `json_value`,
    dt.d1 AS `date_value`,
    CASE 
        WHEN d.c1 > 0 THEN 'positive'
        WHEN d.c1 < 0 THEN 'negative'
        ELSE 'zero'
    END AS `case_result`,
    IFNULL(d.c1, 0) AS `ifnull_result`,
    COALESCE(d.c2, d.c3, d.c4, 0) AS `coalesce_result`,
    CONCAT('Default: ', d.c3) AS `concat_result`,
    LENGTH(d.c3) AS `length_result`,
    SUBSTRING(d.c3, 1, 5) AS `substring_result`,
    UPPER(d.c3) AS `upper_result`,
    LOWER(d.c3) AS `lower_result`,
    TRIM(d.c3) AS `trim_result`,
    REVERSE(d.c3) AS `reverse_result`,
    LEFT(d.c3, 3) AS `left_result`,
    RIGHT(d.c3, 3) AS `right_result`,
    MID(d.c3, 2, 3) AS `mid_result`,
    INSTR(d.c3, 'a') AS `instr_result`,
    REPLACE(d.c3, 'a', 'b') AS `replace_result`,
    DATE_FORMAT(d.c4, '%Y-%m-%d %H:%i:%s') AS `date_format_result`,
    TIMESTAMPDIFF(SECOND, d.c4, NOW()) AS `timestampdiff_result`,
    FROM_UNIXTIME(UNIX_TIMESTAMP(d.c4)) AS `from_unixtime_result`,
    UNIX_TIMESTAMP(d.c4) AS `unix_timestamp_result`,
    NOW() AS `now_result`,
    CURDATE() AS `curdate_result`,
    CURTIME() AS `curtime_result`,
    DATEDIFF(NOW(), d.c4) AS `datediff_result`,
    TIME_TO_SEC(d.c4) AS `time_to_sec_result`,
    SEC_TO_TIME(TIME_TO_SEC(d.c4)) AS `sec_to_time_result`
FROM 
    case_10_defaults d
LEFT JOIN 
    case_01_integers i ON d.c1 = i.col_tiny
LEFT JOIN 
    case_02_boolean b ON d.c1 = b.status
LEFT JOIN 
    case_03_floats f ON d.c1 = CAST(f.col_float AS SIGNED)
LEFT JOIN 
    case_04_mb3_suffix m ON CONVERT(d.c3 USING utf8mb3) = m.col_var_mb3
LEFT JOIN 
    case_05_charsets ch ON CONVERT(d.c3 USING utf8mb3) = ch.c1
LEFT JOIN 
    case_06_collates co ON d.c3 COLLATE utf8mb4_general_ci = co.c1
LEFT JOIN 
    case_07_complex_charsets cm ON CONVERT(d.c3 USING ascii) = cm.c1
LEFT JOIN 
    case_08_json j ON d.c3 = JSON_UNQUOTE(JSON_EXTRACT(j.data, '$.name'))
LEFT JOIN 
    case_09_datetime dt ON d.c4 = dt.dt1;
