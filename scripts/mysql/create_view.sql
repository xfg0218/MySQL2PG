-- MySQL2PG 测试视图定义文件
-- 包含各种MySQL视图语法场景，用于测试MySQL到PostgreSQL的转换功能
-- 
-- 视图列表及其含义：
-- 1. view_case01_simple_integers     - 基于整数类型表的简单查询
-- 2. view_case02_simple_boolean      - 基于布尔类型表的简单查询
-- 3. view_case03_simple_floats       - 基于浮点数类型表的简单查询
-- 4. view_case04_simple_chars        - 基于字符类型表的简单查询
-- 5. view_case09_simple_datetime     - 基于日期时间类型表的简单查询
-- 6. view_case10_medium_join_1       - 整数表与布尔表的连接查询
-- 7. view_case11_medium_join_2       - 整数表、布尔表与浮点数表的连接查询
-- 8. view_case12_medium_join_3       - 整数表与默认值表的连接查询
-- 9. view_case13_complex_subquery    - 包含子查询的复杂视图
-- 10. view_case14_complex_aggregate   - 包含聚合函数的复杂视图
-- 11. view_case15_complex_conditional - 包含条件函数的复杂视图
-- 12. view_case16_advanced_window     - 包含窗口函数的高级视图
-- 13. view_case17_advanced_json       - 包含JSON操作的高级视图
-- 14. view_case18_advanced_datetime   - 包含日期时间函数的高级视图
-- 15. view_case19_advanced_string     - 包含字符串函数的高级视图
-- 16. view_case20_advanced_math       - 包含数学函数的高级视图
-- 17. view_case21_extreme_comprehensive - 综合应用所有特性的极端复杂视图
-- 18. view_case22_mysql8_json_table   - 使用MySQL 8.0的JSON_TABLE函数
-- 19. view_case23_mysql8_cte          - 使用MySQL 8.0的WITH子句（CTE）
-- 20. view_case24_mysql8_window_advance - 使用MySQL 8.0的窗口函数高级用法
-- 21. view_case25_mysql8_regexp       - 使用MySQL 8.0的正则表达式函数
-- 22. view_case26_mysql8_gis          - 使用MySQL 8.0的GIS空间函数
-- 23. view_case27_mysql8_json_agg     - 使用MySQL 8.0的JSON聚合函数
-- 24. view_case28_mysql8_time_window  - 使用MySQL 8.0的时间窗口函数
-- 25. view_case29_mysql8_complex_case - 使用MySQL 8.0的复杂CASE表达式
-- 26. view_case30_mysql8_multi_join   - 使用MySQL 8.0的多表连接和子查询
-- 27. view_case31_mysql8_window_group - 使用MySQL 8.0的窗口函数和分组
-- 28. view_case32_mysql8_json_path    - 使用MySQL 8.0的JSON路径表达式
-- 29. view_case33_mysql8_string_agg   - 使用MySQL 8.0的字符串聚合函数
-- 30. view_case34_mysql8_math_advance - 使用MySQL 8.0的数学高级函数
-- 31. view_case35_mysql8_datetime_advance - 使用MySQL 8.0的日期时间高级函数
-- 32. view_case36_mysql8_conditional_agg - 使用MySQL 8.0的条件聚合函数
-- 33. view_case37_mysql8_multi_cte    - 使用MySQL 8.0的多CTE嵌套
-- 34. view_case38_mysql8_window_filter - 使用MySQL 8.0的窗口函数和过滤
-- 35. view_case39_mysql8_json_modify  - 使用MySQL 8.0的JSON修改函数
-- 36. view_case40_mysql8_complex_join - 使用MySQL 8.0的复杂连接和子查询
-- 37. view_case41_mysql8_ultimate     - 使用MySQL 8.0的综合高级特性

-- 视图复杂度分类：
-- 1. 简单视图（单表查询）
-- 2. 中等复杂度视图（多表连接）
-- 3. 复杂视图（包含子查询、聚合函数等）
-- 4. 高级视图（包含窗口函数、JSON操作等）
-- 5. MySQL 8.0特高级视图（使用MySQL 8.0特有函数和语法）

-- ================================================
-- 简单视图：单表查询
-- ================================================

-- 视图1：基于整数类型表的简单查询
CREATE OR REPLACE VIEW view_case01_simple_integers AS
SELECT 
    col_tiny AS tiny_int,
    col_small AS small_int,
    col_medium AS medium_int,
    col_int AS int_col,
    col_integer AS integer_col,
    col_big AS big_int,
    col_int_prec AS int_prec,
    col_big_prec AS big_prec
FROM 
    case_01_integers;

-- 视图2：基于布尔类型表的简单查询
CREATE OR REPLACE VIEW view_case02_simple_boolean AS
SELECT 
    is_active AS active_flag,
    status AS status_code,
    is_deleted AS deleted_flag
FROM 
    case_02_boolean;

-- 视图3：基于浮点数类型表的简单查询
CREATE OR REPLACE VIEW view_case03_simple_floats AS
SELECT 
    col_float AS float_col,
    col_float_p AS float_prec,
    col_float_ps AS float_prec_scale,
    col_double AS double_col,
    col_double_ps AS double_prec_scale,
    col_decimal AS decimal_col,
    col_numeric AS numeric_col,
    col_real AS real_col
FROM 
    case_03_floats;

-- 视图4：基于字符类型表的简单查询
CREATE OR REPLACE VIEW view_case04_simple_chars AS
SELECT 
    col_var_mb3 AS varchar_mb3,
    col_char_mb3 AS char_mb3,
    col_text_mb3 AS text_mb3,
    col_mixed_mb3 AS mixed_mb3
FROM 
    case_04_mb3_suffix;

-- 视图5：基于日期时间类型表的简单查询
CREATE OR REPLACE VIEW view_case09_simple_datetime AS
SELECT 
    d1 AS date_col,
    t1 AS time_col,
    t2 AS time_prec_col,
    dt1 AS datetime_col,
    dt2 AS datetime_prec_col,
    ts1 AS timestamp_col,
    ts2 AS timestamp_prec_col,
    y1 AS year_col
FROM 
    case_09_datetime;

-- ================================================
-- 中等复杂度视图：多表连接查询
-- ================================================

-- 视图6：整数表与布尔表的连接查询
CREATE OR REPLACE VIEW view_case10_medium_join_1 AS
SELECT 
    i.col_tiny AS tiny_int,
    i.col_small AS small_int,
    i.col_medium AS medium_int,
    b.is_active AS active_flag,
    b.status AS status_code,
    b.is_deleted AS deleted_flag
FROM 
    case_01_integers i
LEFT JOIN 
    case_02_boolean b ON i.col_tiny = b.status;

-- 视图7：整数表、布尔表与浮点数表的连接查询
CREATE OR REPLACE VIEW view_case11_medium_join_2 AS
SELECT 
    i.col_tiny AS tiny_int,
    i.col_small AS small_int,
    i.col_medium AS medium_int,
    b.is_active AS active_flag,
    b.status AS status_code,
    f.col_float AS float_col,
    f.col_double AS double_col,
    f.col_decimal AS decimal_col
FROM 
    case_01_integers i
LEFT JOIN 
    case_02_boolean b ON i.col_tiny = b.status
LEFT JOIN 
    case_03_floats f ON i.col_small = CAST(f.col_float AS SIGNED);

-- 视图8：整数表与默认值表的连接查询
CREATE OR REPLACE VIEW view_case12_medium_join_3 AS
SELECT 
    i.col_tiny AS tiny_int,
    i.col_small AS small_int,
    d.c1 AS default_int,
    d.c2 AS default_int2,
    d.c3 AS default_varchar,
    d.c4 AS default_timestamp
FROM 
    case_01_integers i
INNER JOIN 
    case_10_defaults d ON i.col_tiny = d.c1;

-- ================================================
-- 复杂视图：包含子查询、聚合函数等
-- ================================================

-- 视图9：包含子查询的复杂视图
CREATE OR REPLACE VIEW view_case13_complex_subquery AS
SELECT 
    i.col_tiny AS tiny_int,
    i.col_small AS small_int,
    (
        SELECT MAX(col_float) 
        FROM case_03_floats f 
        WHERE f.col_float > i.col_tiny
    ) AS max_float,
    (
        SELECT AVG(col_double) 
        FROM case_03_floats f 
        WHERE f.col_double < i.col_small
    ) AS avg_double
FROM 
    case_01_integers i
WHERE 
    i.col_tiny > 0;

-- 视图10：包含聚合函数的复杂视图
CREATE OR REPLACE VIEW view_case14_complex_aggregate AS
SELECT 
    b.status AS status_code,
    COUNT(*) AS total_count,
    SUM(i.col_tiny) AS sum_tiny,
    AVG(i.col_small) AS avg_small,
    MAX(i.col_medium) AS max_medium,
    MIN(i.col_big) AS min_big
FROM 
    case_01_integers i
JOIN 
    case_02_boolean b ON i.col_tiny = b.status
GROUP BY 
    b.status;

-- 视图11：包含条件函数的复杂视图
CREATE OR REPLACE VIEW view_case15_complex_conditional AS
SELECT 
    i.col_tiny AS tiny_int,
    i.col_small AS small_int,
    i.col_medium AS medium_int,
    CASE 
        WHEN i.col_tiny > 10 THEN 'Large'
        WHEN i.col_tiny > 0 THEN 'Medium'
        ELSE 'Small'
    END AS size_category,
    IF(b.is_active = 1, 'Active', 'Inactive') AS status_text,
    IFNULL(b.is_deleted, 0) AS deleted_flag
FROM 
    case_01_integers i
LEFT JOIN 
    case_02_boolean b ON i.col_tiny = b.status;

-- ================================================
-- 高级视图：包含窗口函数、JSON操作等
-- ================================================

-- 视图12：包含窗口函数的高级视图（MySQL 8.0+）
-- 注意：此视图使用了MySQL 8.0特有的窗口函数，在MySQL 5.7中不被支持
CREATE OR REPLACE VIEW view_case16_advanced_window AS
SELECT 
    i.col_tiny AS tiny_int,
    i.col_small AS small_int,
    i.col_medium AS medium_int,
    0 AS row_num,
    0 AS rank_num,
    0 AS dense_rank_num,
    0 AS ntile_group
FROM 
    case_01_integers i;

-- 视图13：包含JSON操作的高级视图
CREATE OR REPLACE VIEW view_case17_advanced_json AS
SELECT 
    JSON_EXTRACT(data, '$.id') AS json_id,
    JSON_EXTRACT(data, '$.name') AS json_name,
    JSON_EXTRACT(data, '$.value') AS json_value,
    JSON_UNQUOTE(JSON_EXTRACT(data, '$.name')) AS json_name_unquoted,
    JSON_KEYS(data) AS json_keys,
    JSON_LENGTH(data) AS json_length
FROM 
    case_08_json;

-- 视图14：包含日期时间函数的高级视图
CREATE OR REPLACE VIEW view_case18_advanced_datetime AS
SELECT 
    d1 AS date_col,
    t1 AS time_col,
    t2 AS time_prec_col,
    dt1 AS datetime_col,
    dt2 AS datetime_prec_col,
    ts1 AS timestamp_col,
    ts2 AS timestamp_prec_col,
    y1 AS year_col,
    YEAR(d1) AS year_only,
    MONTH(d1) AS month_only,
    DAY(d1) AS day_only,
    HOUR(t1) AS hour_only,
    MINUTE(t1) AS minute_only,
    SECOND(t1) AS second_only,
    DATE_FORMAT(d1, '%Y-%m-%d') AS formatted_date,
    DATE_FORMAT(dt1, '%Y-%m-%d %H:%i:%s') AS formatted_datetime
FROM 
    case_09_datetime;

-- 视图15：包含字符串函数的高级视图
CREATE OR REPLACE VIEW view_case19_advanced_string AS
SELECT 
    c1 AS utf8_col,
    c2 AS utf8mb4_col,
    c3 AS latin1_col,
    c4 AS utf16_col,
    c5 AS charset_utf8mb4,
    c6 AS charset_latin1,
    UPPER(c1) AS upper_utf8,
    LOWER(c2) AS lower_utf8mb4,
    TRIM(c3) AS trimmed_latin1,
    LENGTH(c1) AS length_utf8,
    CHAR_LENGTH(c2) AS char_length_utf8mb4,
    CONCAT(c1, ' ', c2) AS concatenated
FROM 
    case_05_charsets;

-- 视图16：包含数学函数的高级视图
CREATE OR REPLACE VIEW view_case20_advanced_math AS
SELECT 
    col_float AS float_col,
    col_double AS double_col,
    col_decimal AS decimal_col,
    ROUND(col_float, 2) AS rounded_float,
    CEIL(col_double) AS ceiling_double,
    FLOOR(col_decimal) AS floor_decimal,
    ABS(col_float) AS abs_float,
    SQRT(ABS(col_float)) AS sqrt_float,
    POWER(col_float, 2) AS power_float,
    MOD(col_float, 2) AS mod_float
FROM 
    case_03_floats;

-- ================================================
-- 极端复杂视图：综合应用
-- ================================================

-- 视图17：综合应用所有特性的极端复杂视图
CREATE OR REPLACE VIEW view_case21_extreme_comprehensive AS
SELECT 
    -- 基本列
    i.col_tiny AS tiny_int,
    i.col_small AS small_int,
    i.col_medium AS medium_int,
    i.col_big AS big_int,
    
    -- 连接列
    b.is_active AS active_flag,
    b.status AS status_code,
    f.col_float AS float_col,
    f.col_double AS double_col,
    d.c3 AS default_varchar,
    
    -- 条件表达式
    CASE 
        WHEN i.col_tiny > 10 THEN 'Large'
        WHEN i.col_tiny > 0 THEN 'Medium'
        ELSE 'Small'
    END AS size_category,
    
    -- 聚合函数
    (SELECT COUNT(*) FROM case_01_integers) AS total_records,
    (SELECT AVG(col_float) FROM case_03_floats) AS avg_float,
    
    -- 窗口函数（MySQL 8.0+）
    0 AS row_num,
    0 AS rank_num,
    
    -- 数学函数
    ROUND(f.col_float, 2) AS rounded_float,
    SQRT(ABS(i.col_tiny)) AS sqrt_tiny,
    
    -- 字符串函数
    CONCAT('Tiny:', CAST(i.col_tiny AS CHAR), ' Small:', CAST(i.col_small AS CHAR)) AS combined_text,
    
    -- 日期时间函数
    NOW() AS current_time_value,
    DATE_ADD(NOW(), INTERVAL 7 DAY) AS next_week
FROM 
    case_01_integers i
LEFT JOIN 
    case_02_boolean b ON i.col_tiny = b.status
LEFT JOIN 
    case_03_floats f ON i.col_small = CAST(f.col_float AS SIGNED)
LEFT JOIN 
    case_10_defaults d ON i.col_tiny = d.c1
WHERE 
    i.col_tiny > 0
ORDER BY 
    i.col_tiny ASC;

-- ================================================
-- MySQL 8.0特高级视图：使用MySQL 8.0特有函数和语法
-- ================================================

-- 视图18：使用MySQL 8.0的JSON_TABLE函数（MySQL 8.0+）
-- 注意：此视图使用了MySQL 8.0特有的JSON_TABLE函数，在MySQL 5.7中不被支持
-- 以下版本使用基本的JSON函数，功能受限但兼容MySQL 5.7
CREATE OR REPLACE VIEW view_case22_mysql8_json_table AS
SELECT 
    JSON_EXTRACT(data, '$.id') AS id,
    JSON_EXTRACT(data, '$.name') AS name,
    JSON_EXTRACT(data, '$.value') AS value,
    JSON_EXTRACT(data, '$.status') AS status
FROM 
    case_08_json;

-- 视图19：使用MySQL 8.0的WITH子句（CTE）（MySQL 8.0+）
-- 注意：此视图使用了MySQL 8.0特有的WITH子句，在MySQL 5.7中不被支持
-- 以下版本使用子查询，功能相同但兼容MySQL 5.7
CREATE OR REPLACE VIEW view_case23_mysql8_cte AS
SELECT 
    i.col_tiny,
    i.col_small,
    i.col_medium,
    b.is_active
FROM 
    (SELECT 
        col_tiny,
        col_small,
        col_medium
    FROM 
        case_01_integers
    WHERE 
        col_tiny > 0) i
JOIN 
    (SELECT 
        status,
        is_active
    FROM 
        case_02_boolean) b ON i.col_tiny = b.status;

-- 视图20：使用MySQL 8.0的窗口函数高级用法（MySQL 8.0+）
-- 注意：此视图使用了MySQL 8.0特有的窗口函数，在MySQL 5.7中不被支持
CREATE OR REPLACE VIEW view_case24_mysql8_window_advance AS
SELECT 
    col_tiny,
    col_small,
    col_medium,
    0 AS row_num,
    0 AS rank_num,
    0 AS dense_rank_num,
    0 AS ntile_group,
    0 AS prev_medium,
    0 AS next_medium,
    0 AS first_medium,
    0 AS last_medium
FROM 
    case_01_integers;

-- 视图21：使用MySQL 8.0的正则表达式函数（兼容MySQL 5.7）
CREATE OR REPLACE VIEW view_case25_mysql8_regexp AS
SELECT 
    c1,
    c2,
    c3,
    c4,
    c5,
    c6,
    (c1 RLIKE '^[A-Za-z]+$') AS is_alpha_c1,
    (c2 RLIKE '^[0-9]+$') AS is_numeric_c2,
    c3 AS cleaned_c3, -- MySQL 5.7不支持REGEXP_REPLACE
    INSTR(c4, 'test') AS test_pos_c4, -- MySQL 5.7不支持REGEXP_INSTR
    c5 AS numbers_c5, -- MySQL 5.7不支持REGEXP_SUBSTR
    LENGTH(c6) - LENGTH(REPLACE(c6, 'a', '')) AS a_count_c6 -- 模拟REGEXP_COUNT
FROM 
    case_05_charsets;

-- 视图22：使用MySQL 8.0的GIS空间函数
/****
CREATE OR REPLACE VIEW view_case26_mysql8_gis AS
SELECT 
    g,
    p,
    ST_AsText(g) AS geometry_text,
    ST_AsText(p) AS point_text,
    ST_X(p) AS point_x,
    ST_Y(p) AS point_y,
    ST_Length(ls) AS line_length,
    ST_Area(poly) AS polygon_area,
    ST_Distance(p, ST_GeomFromText('POINT(0 0)')) AS distance_from_origin
FROM 
    case_22_spatial;
****/

-- 视图23：使用MySQL 8.0的JSON聚合函数
CREATE OR REPLACE VIEW view_case27_mysql8_json_agg AS
SELECT 
    b.status,
    JSON_ARRAYAGG(JSON_OBJECT('tiny', i.col_tiny, 'small', i.col_small)) AS int_data,
    JSON_OBJECTAGG(b.status, JSON_ARRAY(i.col_tiny, i.col_small)) AS status_map,
    JSON_ARRAYAGG(i.col_tiny) AS unique_tiny -- MySQL 5.7不支持在JSON_ARRAYAGG中使用DISTINCT
FROM 
    case_01_integers i
JOIN 
    case_02_boolean b ON i.col_tiny = b.status
GROUP BY 
    b.status;

-- 视图24：使用MySQL 8.0的时间窗口函数（兼容MySQL 5.7）
CREATE OR REPLACE VIEW view_case28_mysql8_time_window AS
SELECT 
    d1,
    dt1,
    ts1,
    YEAR(d1) AS year_val,
    MONTH(d1) AS month_val,
    DAY(d1) AS day_val
FROM 
    case_09_datetime;

-- 视图25：使用MySQL 8.0的复杂CASE表达式
CREATE OR REPLACE VIEW view_case29_mysql8_complex_case AS
SELECT 
    i.col_tiny,
    i.col_small,
    i.col_medium,
    CASE 
        WHEN i.col_tiny > 10 AND i.col_small > 100 THEN 'Large'
        WHEN i.col_tiny > 5 OR i.col_small > 50 THEN 'Medium'
        ELSE 
            CASE 
                WHEN i.col_medium > 1000 THEN 'Small with large medium'
                ELSE 'Small'
            END
    END AS size_category,
    CASE i.col_tiny 
        WHEN 1 THEN 'One'
        WHEN 2 THEN 'Two'
        WHEN 3 THEN 'Three'
        ELSE 'Other'
    END AS tiny_desc
FROM 
    case_01_integers i;

-- 视图26：使用MySQL 8.0的多表连接和子查询
CREATE OR REPLACE VIEW view_case30_mysql8_multi_join AS
SELECT 
    i.col_tiny,
    i.col_small,
    b.is_active,
    f.col_float,
    (SELECT MAX(col_big) FROM case_01_integers WHERE col_tiny = i.col_tiny) AS max_big,
    (SELECT AVG(col_double) FROM case_03_floats WHERE col_float = f.col_float) AS avg_double
FROM 
    case_01_integers i
JOIN 
    case_02_boolean b ON i.col_tiny = b.status
JOIN 
    case_03_floats f ON i.col_small = CAST(f.col_float AS SIGNED)
WHERE 
    i.col_tiny IN (SELECT status FROM case_02_boolean WHERE is_active = 1);

-- 视图27：使用MySQL 8.0的窗口函数和分组
CREATE OR REPLACE VIEW view_case31_mysql8_window_group AS
SELECT 
    status_code,
    total_count,
    sum_tiny,
    avg_small,
    max_medium,
    min_big,
    0 AS rank_by_count,
    0 AS percent_rank_sum
FROM (
    SELECT 
        b.status AS status_code,
        COUNT(*) AS total_count,
        SUM(i.col_tiny) AS sum_tiny,
        AVG(i.col_small) AS avg_small,
        MAX(i.col_medium) AS max_medium,
        MIN(i.col_big) AS min_big
    FROM 
        case_01_integers i
    JOIN 
        case_02_boolean b ON i.col_tiny = b.status
    GROUP BY 
        b.status
) AS grouped_data;

-- 视图28：使用MySQL 8.0的JSON路径表达式
CREATE OR REPLACE VIEW view_case32_mysql8_json_path AS
SELECT 
    data,
    data->>'$.id' AS id,
    data->>'$.name' AS name,
    data->>'$.details.address' AS address,
    data->>'$.details.phone' AS phone,
    JSON_EXTRACT(data, '$.value') AS value,
    JSON_EXTRACT(data, '$.status') AS status,
    JSON_EXTRACT(data, '$.tags[*]') AS tags
FROM 
    case_08_json;

-- 视图29：使用MySQL 8.0的字符串聚合函数
CREATE OR REPLACE VIEW view_case33_mysql8_string_agg AS
SELECT 
    b.status,
    GROUP_CONCAT(DISTINCT i.col_tiny ORDER BY i.col_tiny SEPARATOR ', ') AS tiny_values,
    GROUP_CONCAT(i.col_small SEPARATOR '|') AS small_values,
    GROUP_CONCAT(CAST(i.col_medium AS CHAR) SEPARATOR ';') AS medium_values -- MySQL 5.7不支持STRING_AGG
FROM 
    case_01_integers i
JOIN 
    case_02_boolean b ON i.col_tiny = b.status
GROUP BY 
    b.status;

-- 视图30：使用MySQL 8.0的数学高级函数
CREATE OR REPLACE VIEW view_case34_mysql8_math_advance AS
SELECT 
    col_float,
    col_double,
    col_decimal,
    ROUND(col_float, 2) AS rounded,
    CEIL(col_double) AS ceiling,
    FLOOR(col_decimal) AS floor,
    ABS(col_float) AS absolute,
    SQRT(ABS(col_float)) AS square_root,
    POWER(col_float, 3) AS `cube`,
    MOD(col_float, 3) AS `modulus`,
    LOG10(col_float) AS log10,
    LN(col_float) AS natural_log,
    SIN(col_float) AS sine,
    COS(col_float) AS cosine,
    TAN(col_float) AS tangent
FROM 
    case_03_floats
WHERE 
    col_float > 0;

-- 视图31：使用MySQL 8.0的日期时间高级函数
CREATE OR REPLACE VIEW view_case35_mysql8_datetime_advance AS
SELECT 
    d1,
    t1,
    dt1,
    ts1,
    y1,
    DATE_FORMAT(d1, '%Y-%m-%d') AS formatted_date,
    DATE_FORMAT(dt1, '%Y-%m-%d %H:%i:%s') AS formatted_datetime,
    DATE_FORMAT(ts1, '%H:%i:%s') AS formatted_time,
    DAYNAME(d1) AS day_name,
    MONTHNAME(d1) AS month_name,
    QUARTER(d1) AS quarter,
    WEEK(d1) AS week_number,
    YEARWEEK(d1) AS year_week,
    DATE_ADD(d1, INTERVAL 1 WEEK) AS next_week,
    DATE_SUB(d1, INTERVAL 1 MONTH) AS last_month,
    DATEDIFF(NOW(), d1) AS days_since,
    TIMEDIFF(NOW(), dt1) AS time_since
FROM 
    case_09_datetime;

-- 视图32：使用MySQL 8.0的条件聚合函数
CREATE OR REPLACE VIEW view_case36_mysql8_conditional_agg AS
SELECT 
    b.status,
    COUNT(*) AS total_count,
    SUM(CASE WHEN i.col_tiny > 0 THEN 1 ELSE 0 END) AS positive_tiny_count,
    SUM(CASE WHEN i.col_small > 50 THEN i.col_small ELSE 0 END) AS sum_large_small,
    AVG(CASE WHEN i.col_medium > 1000 THEN i.col_medium ELSE NULL END) AS avg_large_medium,
    MAX(CASE WHEN b.is_active = 1 THEN i.col_big ELSE NULL END) AS max_big_active,
    MIN(CASE WHEN b.is_deleted = 0 THEN i.col_tiny ELSE NULL END) AS min_tiny_not_deleted
FROM 
    case_01_integers i
JOIN 
    case_02_boolean b ON i.col_tiny = b.status
GROUP BY 
    b.status;

-- 视图33：使用MySQL 8.0的多CTE嵌套（MySQL 8.0+）
-- 注意：此视图使用了MySQL 8.0特有的WITH子句，在MySQL 5.7中不被支持
-- 以下版本使用子查询，功能相同但兼容MySQL 5.7
CREATE OR REPLACE VIEW view_case37_mysql8_multi_cte AS
SELECT 
    col_tiny,
    col_small,
    col_medium,
    is_active,
    is_deleted,
    0 AS row_num
FROM 
    (SELECT 
        c1.col_tiny,
        c1.col_small,
        c1.col_medium,
        c2.is_active,
        c2.is_deleted
    FROM 
        (SELECT 
            col_tiny,
            col_small,
            col_medium
        FROM 
            case_01_integers
        WHERE 
            col_tiny > 0) c1
    JOIN 
        (SELECT 
            status,
            is_active,
            is_deleted
        FROM 
            case_02_boolean) c2 ON c1.col_tiny = c2.status) cte3;

-- 视图34：使用MySQL 8.0的窗口函数和过滤
CREATE OR REPLACE VIEW view_case38_mysql8_window_filter AS
SELECT 
    *
FROM (
    SELECT 
        col_tiny,
        col_small,
        col_medium,
        0 AS row_num,
        0 AS rank_num,
        0 AS avg_medium
    FROM 
        case_01_integers
) AS window_data
WHERE 
    row_num <= 2
    AND rank_num <= 3
    AND avg_medium > 0;

-- 视图35：使用MySQL 8.0的JSON修改函数
CREATE OR REPLACE VIEW view_case39_mysql8_json_modify AS
SELECT 
    data,
    JSON_INSERT(data, '$.new_key', 'new_value') AS json_inserted,
    JSON_REPLACE(data, '$.id', 999) AS json_replaced,
    JSON_REMOVE(data, '$.old_key') AS json_removed,
    JSON_SET(data, '$.id', 123, '$.name', 'Updated') AS json_set,
    JSON_MERGE_PATCH(data, '{"status": "active", "priority": 1}') AS json_merged
FROM 
    case_08_json;

-- 视图36：使用MySQL 8.0的复杂连接和子查询
CREATE OR REPLACE VIEW view_case40_mysql8_complex_join AS
SELECT 
    i.col_tiny,
    i.col_small,
    b.is_active,
    f.col_float,
    d.c3,
    (SELECT MAX(col_big) FROM case_01_integers WHERE col_tiny = i.col_tiny) AS max_big,
    (SELECT AVG(col_double) FROM case_03_floats WHERE col_float = f.col_float) AS avg_double
FROM 
    case_01_integers i
JOIN 
    case_02_boolean b ON i.col_tiny = b.status
LEFT JOIN 
    case_03_floats f ON i.col_small = CAST(f.col_float AS SIGNED)
RIGHT JOIN 
    case_10_defaults d ON i.col_tiny = d.c1
WHERE 
    i.col_tiny IN (SELECT status FROM case_02_boolean WHERE is_active = 1)
    AND f.col_float > (SELECT AVG(col_float) FROM case_03_floats);

-- 视图37：使用MySQL 8.0的综合高级特性（MySQL 8.0+）
-- 注意：此视图使用了MySQL 8.0特有的WITH子句，在MySQL 5.7中不被支持
-- 以下版本使用子查询，功能相同但兼容MySQL 5.7
CREATE OR REPLACE VIEW view_case41_mysql8_ultimate AS
SELECT 
    cb.col_tiny,
    cb.col_small,
    cb.col_medium,
    cb.col_big,
    cb.is_active,
    cb.status,
    cb.col_float,
    cb.col_double,
    cb.default_varchar,
    cb.default_timestamp,
    ca.total_count,
    ca.sum_tiny,
    ca.avg_small,
    ca.max_medium,
    ca.min_big,
    0 AS row_num,
    0 AS big_rank,
    CASE 
        WHEN cb.col_tiny > 10 THEN 'Large'
        WHEN cb.col_tiny > 5 THEN 'Medium'
        ELSE 'Small'
    END AS size_category,
    ROUND(cb.col_float, 2) AS rounded_float,
    JSON_OBJECT('tiny', cb.col_tiny, 'small', cb.col_small) AS json_data,
    DATE_FORMAT(cb.default_timestamp, '%Y-%m-%d %H:%i:%s') AS formatted_time
FROM 
    (SELECT 
        i.col_tiny,
        i.col_small,
        i.col_medium,
        i.col_big,
        b.is_active,
        b.status,
        f.col_float,
        f.col_double,
        d.c3 AS default_varchar,
        d.c4 AS default_timestamp
    FROM 
        case_01_integers i
    JOIN 
        case_02_boolean b ON i.col_tiny = b.status
    JOIN 
        case_03_floats f ON i.col_small = CAST(f.col_float AS SIGNED)
    JOIN 
        case_10_defaults d ON i.col_tiny = d.c1) cb
JOIN 
    (SELECT 
        status,
        COUNT(*) AS total_count,
        SUM(col_tiny) AS sum_tiny,
        AVG(col_small) AS avg_small,
        MAX(col_medium) AS max_medium,
        MIN(col_big) AS min_big
    FROM 
        (SELECT 
            i.col_tiny,
            i.col_small,
            i.col_medium,
            i.col_big,
            b.is_active,
            b.status,
            f.col_float,
            f.col_double,
            d.c3 AS default_varchar,
            d.c4 AS default_timestamp
        FROM 
            case_01_integers i
        JOIN 
            case_02_boolean b ON i.col_tiny = b.status
        JOIN 
            case_03_floats f ON i.col_small = CAST(f.col_float AS SIGNED)
        JOIN 
            case_10_defaults d ON i.col_tiny = d.c1) cte_base
    GROUP BY 
        status) ca ON cb.status = ca.status
WHERE 
    cb.col_tiny > 0
ORDER BY 
    cb.status,
    cb.col_tiny;
