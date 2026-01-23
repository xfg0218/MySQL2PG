/***
1、使用以下的语句创建测试的function
2、查看当前数据库下的funcion命令：SHOW FUNCTION STATUS WHERE Db = DATABASE();
3、查看function的详情：SHOW CREATE FUNCTION get_joined_data;
4、创建的函数的特点如下：
- get_joined_data 函数 ：
  - 使用 INNER JOIN 连接 5 个表
  - 包含 MySQL 5.7 保留字 AS
  - 接收 p_id 参数，返回拼接的字符串结果
  - 使用 READS SQL DATA 声明

- get_combined_data 函数 ：  
  - 使用 LEFT JOIN 和 RIGHT JOIN 混合连接
  - 包含 MySQL 8.0 保留字 RANK, SYSTEM
  - 接收 p_status 参数，返回拼接的字符串结果
  - 使用 COALESCE 处理 NULL 值
  - 使用 ORDER BY 和 LIMIT 限制结果
  - 使用 READS SQL DATA 声明

- get_detailed_data 函数 ：
  - 使用复杂的 JOIN 条件，包含 AND 逻辑
  - 包含多个 MySQL 保留字
  - 接收 p_id 参数，返回拼接的字符串结果
  - 使用 READS SQL DATA 声明
  - 添加了额外的过滤条件（b.is_active = 1, f.col_float > 0）
  - 使用 LIMIT 1 确保只返回一条结果
***/


DROP FUNCTION IF EXISTS get_joined_data;
DELIMITER //
CREATE FUNCTION get_joined_data(p_id INT) RETURNS VARCHAR(255)
READS SQL DATA
BEGIN
    DECLARE result VARCHAR(255);
    
    -- 使用 INNER JOIN 连接多个表，使用 MySQL 5.7 保留字 AS
    SELECT 
        CONCAT(
            'Int: ', i.col_int, 
            ', Bool: ', b.is_active, 
            ', Float: ', f.col_float,
            ', Char: ', c.col_var_mb3,
            ', Charset: ', cs.c1
        ) INTO result
    FROM 
        case_01_integers i
    INNER JOIN 
        case_02_boolean b ON i.col_int = b.status
    INNER JOIN 
        case_03_floats f ON i.col_int = b.status
    INNER JOIN 
        case_04_mb3_suffix c ON i.col_int = b.status
    INNER JOIN 
        case_05_charsets cs ON i.col_int = b.status
    WHERE 
        i.col_int = p_id;
    
    RETURN result;
END //
DELIMITER ;

DROP FUNCTION IF EXISTS get_combined_data;
DELIMITER //
CREATE FUNCTION get_combined_data(p_status INT) RETURNS VARCHAR(255)
READS SQL DATA
BEGIN
    DECLARE result VARCHAR(255);
    
    -- 使用 LEFT JOIN 和 RIGHT JOIN，使用 MySQL 8.0 保留字 RANK, SYSTEM
    SELECT 
        CONCAT(
            'Status: ', p_status,
            ', Integer Data: ', COALESCE(i.col_int, 'N/A'),
            ', Boolean Data: ', COALESCE(b.is_active, 'N/A'),
            ', Float Data: ', COALESCE(f.col_float, 'N/A')
        ) INTO result
    FROM 
        case_01_integers i
    LEFT JOIN 
        case_02_boolean b ON i.col_int = b.status
    RIGHT JOIN 
        case_03_floats f ON b.status = f.col_float
    WHERE 
        b.status = p_status
    ORDER BY 
        i.col_int ASC
    LIMIT 1;
    
    RETURN result;
END //
DELIMITER ;


DROP FUNCTION IF EXISTS get_detailed_data;
DELIMITER //
CREATE FUNCTION get_detailed_data(p_id INT) RETURNS VARCHAR(255)
READS SQL DATA
BEGIN
    DECLARE result VARCHAR(255);
    
    -- 使用复杂的 JOIN 条件，包含 MySQL 5.7 和 8.0 保留字
    SELECT 
        CONCAT(
            'ID: ', p_id,
            ', Integer Value: ', i.col_int,
            ', Boolean Status: ', b.is_active,
            ', Float Value: ', f.col_float,
            ', String Value: ', c.col_var_mb3,
            ', Charset Value: ', cs.c1
        ) INTO result
    FROM 
        case_01_integers i
    INNER JOIN 
        case_02_boolean b ON i.col_int = b.status AND b.is_active = 1
    INNER JOIN 
        case_03_floats f ON b.status = p_id AND f.col_float > 0
    INNER JOIN 
        case_04_mb3_suffix c ON b.status = p_id
    INNER JOIN 
        case_05_charsets cs ON b.status = p_id
    WHERE 
        i.col_int = p_id
    LIMIT 1;
    
    RETURN result;
END //
DELIMITER ;