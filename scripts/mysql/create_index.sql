--  每个表都有对饮的索引，语法兼容MySQL5.7 和MySQL8.0

DROP PROCEDURE IF EXISTS `drop_index_if_exists`;
DELIMITER $$
CREATE PROCEDURE `drop_index_if_exists`(IN idx_name VARCHAR(64), IN tbl_name VARCHAR(64))
BEGIN
    DECLARE idx_count INT;
    SELECT COUNT(*) INTO idx_count FROM information_schema.statistics 
    WHERE table_schema = DATABASE() AND table_name = tbl_name AND index_name = idx_name;
    
    IF idx_count > 0 THEN
        SET @s = CONCAT('DROP INDEX `', idx_name, '` ON `', tbl_name, '`');
        PREPARE stmt FROM @s;
        EXECUTE stmt;
        DEALLOCATE PREPARE stmt;
    END IF;
END$$
DELIMITER ;

-- MySQL2PG 索引创建文件
-- 包含各种MySQL索引类型，用于测试MySQL到PostgreSQL的转换功能

-- 为case_01_integers表添加索引
CALL drop_index_if_exists('idx_case_01_col_tiny', 'case_01_integers');
CALL drop_index_if_exists('idx_case_01_col_small', 'case_01_integers');
CALL drop_index_if_exists('idx_case_01_col_medium', 'case_01_integers');
CALL drop_index_if_exists('idx_case_01_col_int', 'case_01_integers');
CALL drop_index_if_exists('idx_case_01_col_big', 'case_01_integers');
CALL drop_index_if_exists('idx_case_01_composite', 'case_01_integers');
CREATE INDEX idx_case_01_col_tiny ON case_01_integers(col_tiny);
CREATE INDEX idx_case_01_col_small ON case_01_integers(col_small);
CREATE INDEX idx_case_01_col_medium ON case_01_integers(col_medium);
CREATE INDEX idx_case_01_col_int ON case_01_integers(col_int);
CREATE INDEX idx_case_01_col_big ON case_01_integers(col_big);
CREATE INDEX idx_case_01_composite ON case_01_integers(col_int, col_big);

-- 为case_02_boolean表添加索引
CALL drop_index_if_exists('idx_case_02_is_active', 'case_02_boolean');
CALL drop_index_if_exists('idx_case_02_status', 'case_02_boolean');
CALL drop_index_if_exists('idx_case_02_is_deleted', 'case_02_boolean');
CREATE INDEX idx_case_02_is_active ON case_02_boolean(is_active);
CREATE INDEX idx_case_02_status ON case_02_boolean(status);
CREATE INDEX idx_case_02_is_deleted ON case_02_boolean(is_deleted);

-- 为case_03_floats表添加索引
CALL drop_index_if_exists('idx_case_03_col_float', 'case_03_floats');
CALL drop_index_if_exists('idx_case_03_col_double', 'case_03_floats');
CALL drop_index_if_exists('idx_case_03_col_decimal', 'case_03_floats');
CALL drop_index_if_exists('idx_case_03_composite', 'case_03_floats');
CREATE INDEX idx_case_03_col_float ON case_03_floats(col_float);
CREATE INDEX idx_case_03_col_double ON case_03_floats(col_double);
CREATE INDEX idx_case_03_col_decimal ON case_03_floats(col_decimal);
CREATE INDEX idx_case_03_composite ON case_03_floats(col_float, col_double);

-- 为case_04_mb3_suffix表添加索引
CALL drop_index_if_exists('idx_case_04_col_var_mb3', 'case_04_mb3_suffix');
CALL drop_index_if_exists('idx_case_04_col_char_mb3', 'case_04_mb3_suffix');
CREATE INDEX idx_case_04_col_var_mb3 ON case_04_mb3_suffix(col_var_mb3);
CREATE INDEX idx_case_04_col_char_mb3 ON case_04_mb3_suffix(col_char_mb3);

-- 为case_05_charsets表添加索引
CALL drop_index_if_exists('idx_case_05_c1', 'case_05_charsets');
CALL drop_index_if_exists('idx_case_05_c2', 'case_05_charsets');
CALL drop_index_if_exists('idx_case_05_c3', 'case_05_charsets');
CALL drop_index_if_exists('idx_case_05_c4', 'case_05_charsets');
CALL drop_index_if_exists('idx_case_05_c5', 'case_05_charsets');
CALL drop_index_if_exists('idx_case_05_c6', 'case_05_charsets');
CREATE INDEX idx_case_05_c1 ON case_05_charsets(c1);
CREATE INDEX idx_case_05_c2 ON case_05_charsets(c2);
CREATE INDEX idx_case_05_c3 ON case_05_charsets(c3);
CREATE INDEX idx_case_05_c4 ON case_05_charsets(c4);
CREATE INDEX idx_case_05_c5 ON case_05_charsets(c5);
CREATE INDEX idx_case_05_c6 ON case_05_charsets(c6);

-- 为case_06_collates表添加索引
CALL drop_index_if_exists('idx_case_06_c1', 'case_06_collates');
CALL drop_index_if_exists('idx_case_06_c2', 'case_06_collates');
CALL drop_index_if_exists('idx_case_06_c3', 'case_06_collates');
CALL drop_index_if_exists('idx_case_06_c4', 'case_06_collates');
CALL drop_index_if_exists('idx_case_06_c5', 'case_06_collates');
CREATE INDEX idx_case_06_c1 ON case_06_collates(c1);
CREATE INDEX idx_case_06_c2 ON case_06_collates(c2);
CREATE INDEX idx_case_06_c3 ON case_06_collates(c3);
CREATE INDEX idx_case_06_c4 ON case_06_collates(c4);
CREATE INDEX idx_case_06_c5 ON case_06_collates(c5);

-- 为case_07_complex_charsets表添加索引
CALL drop_index_if_exists('idx_case_07_c1', 'case_07_complex_charsets');
CALL drop_index_if_exists('idx_case_07_c2', 'case_07_complex_charsets');
CALL drop_index_if_exists('idx_case_07_c3', 'case_07_complex_charsets');
CREATE INDEX idx_case_07_c1 ON case_07_complex_charsets(c1);
CREATE INDEX idx_case_07_c2 ON case_07_complex_charsets(c2);
CREATE INDEX idx_case_07_c3 ON case_07_complex_charsets(c3);

-- 为case_08_json表添加索引
-- 注意：JSON列不能直接创建索引，需要通过虚拟列或函数索引

-- 为case_09_datetime表添加索引
CALL drop_index_if_exists('idx_case_09_d1', 'case_09_datetime');
CALL drop_index_if_exists('idx_case_09_t1', 'case_09_datetime');
CALL drop_index_if_exists('idx_case_09_dt1', 'case_09_datetime');
CALL drop_index_if_exists('idx_case_09_ts1', 'case_09_datetime');
CALL drop_index_if_exists('idx_case_09_y1', 'case_09_datetime');
CALL drop_index_if_exists('idx_case_09_composite', 'case_09_datetime');
CREATE INDEX idx_case_09_d1 ON case_09_datetime(d1);
CREATE INDEX idx_case_09_t1 ON case_09_datetime(t1);
CREATE INDEX idx_case_09_dt1 ON case_09_datetime(dt1);
CREATE INDEX idx_case_09_ts1 ON case_09_datetime(ts1);
CREATE INDEX idx_case_09_y1 ON case_09_datetime(y1);
CREATE INDEX idx_case_09_composite ON case_09_datetime(d1, dt1);

-- 为case_10_defaults表添加索引
CALL drop_index_if_exists('idx_case_10_c1', 'case_10_defaults');
CALL drop_index_if_exists('idx_case_10_c2', 'case_10_defaults');
CALL drop_index_if_exists('idx_case_10_c3', 'case_10_defaults');
CALL drop_index_if_exists('idx_case_10_c4', 'case_10_defaults');
CREATE INDEX idx_case_10_c1 ON case_10_defaults(c1);
CREATE INDEX idx_case_10_c2 ON case_10_defaults(c2);
CREATE INDEX idx_case_10_c3 ON case_10_defaults(c3);
CREATE INDEX idx_case_10_c4 ON case_10_defaults(c4);

-- 为case_12_unsigned表添加索引
CALL drop_index_if_exists('idx_case_12_c1', 'case_12_unsigned');
CALL drop_index_if_exists('idx_case_12_c2', 'case_12_unsigned');
CALL drop_index_if_exists('idx_case_12_c3', 'case_12_unsigned');
CALL drop_index_if_exists('idx_case_12_c4', 'case_12_unsigned');
CREATE INDEX idx_case_12_c1 ON case_12_unsigned(c1);
CREATE INDEX idx_case_12_c2 ON case_12_unsigned(c2);
CREATE INDEX idx_case_12_c3 ON case_12_unsigned(c3);
CREATE INDEX idx_case_12_c4 ON case_12_unsigned(c4);

-- 为case_13_enum_set表添加索引
CALL drop_index_if_exists('idx_case_13_e1', 'case_13_enum_set');
CALL drop_index_if_exists('idx_case_13_s1', 'case_13_enum_set');
CREATE INDEX idx_case_13_e1 ON case_13_enum_set(e1);
CREATE INDEX idx_case_13_s1 ON case_13_enum_set(s1);

-- 为case_14_binary表添加索引
-- 为二进制列添加前缀索引
CALL drop_index_if_exists('idx_case_14_b1', 'case_14_binary');
CALL drop_index_if_exists('idx_case_14_b2', 'case_14_binary');
CALL drop_index_if_exists('idx_case_14_b3', 'case_14_binary');
CALL drop_index_if_exists('idx_case_14_b4', 'case_14_binary');
CALL drop_index_if_exists('idx_case_14_b5', 'case_14_binary');
CALL drop_index_if_exists('idx_case_14_b6', 'case_14_binary');
CREATE INDEX idx_case_14_b1 ON case_14_binary(b1);
CREATE INDEX idx_case_14_b2 ON case_14_binary(b2);
-- BLOB列添加前缀索引
CREATE INDEX idx_case_14_b3 ON case_14_binary(b3(10));
CREATE INDEX idx_case_14_b4 ON case_14_binary(b4(10));
CREATE INDEX idx_case_14_b5 ON case_14_binary(b5(10));
CREATE INDEX idx_case_14_b6 ON case_14_binary(b6(10));

-- 为case_15_options表添加索引
CALL drop_index_if_exists('idx_case_15_id', 'case_15_options');
CREATE INDEX idx_case_15_id ON case_15_options(id);

-- 为case_16_partition表添加索引
CALL drop_index_if_exists('idx_case_16_id', 'case_16_partition');
CALL drop_index_if_exists('idx_case_16_created_at', 'case_16_partition');
CALL drop_index_if_exists('idx_case_16_composite', 'case_16_partition');
CREATE INDEX idx_case_16_id ON case_16_partition(id);
CREATE INDEX idx_case_16_created_at ON case_16_partition(created_at);
CREATE INDEX idx_case_16_composite ON case_16_partition(id, created_at);

-- 为case_17_temp表添加索引

-- 为case_18_quotes表添加索引
CALL drop_index_if_exists('idx_case_18_id', 'case_18_quotes');
CALL drop_index_if_exists('idx_case_18_name', 'case_18_quotes');
CREATE INDEX idx_case_18_id ON `case_18_quotes`(`id`);
CREATE INDEX idx_case_18_name ON `case_18_quotes`(`name`);

-- 为case_19_comments表添加索引
CALL drop_index_if_exists('idx_case_19_c1', 'case_19_comments');
CALL drop_index_if_exists('idx_case_19_c2', 'case_19_comments');
CALL drop_index_if_exists('idx_case_19_c3', 'case_19_comments');
CALL drop_index_if_exists('idx_case_19_c4', 'case_19_comments');
CREATE INDEX idx_case_19_c1 ON case_19_comments(c1);
CREATE INDEX idx_case_19_c2 ON case_19_comments(c2);
CREATE INDEX idx_case_19_c3 ON case_19_comments(c3);
CREATE INDEX idx_case_19_c4 ON case_19_comments(c4);

-- 为case_21_virtual表添加索引
CALL drop_index_if_exists('idx_case_21_id', 'case_21_virtual');
CALL drop_index_if_exists('idx_case_21_c1', 'case_21_virtual');
CREATE INDEX idx_case_21_id ON case_21_virtual(id);
CREATE INDEX idx_case_21_c1 ON case_21_virtual(c1);

-- 为case_22_spatial表添加索引
-- 注意：空间索引要求字段不能为NULL，而表中字段默认允许NULL
-- 暂时注释掉空间索引的创建，可在需要时再创建
-- CREATE SPATIAL INDEX idx_case_22_g ON case_22_spatial(g);
-- CREATE SPATIAL INDEX idx_case_22_p ON case_22_spatial(p);
-- CREATE SPATIAL INDEX idx_case_22_ls ON case_22_spatial(ls);
-- CREATE SPATIAL INDEX idx_case_22_poly ON case_22_spatial(poly);

-- 为case_23_weird_syntax表添加索引
CALL drop_index_if_exists('idx_case_23_c1', 'case_23_weird_syntax');
CALL drop_index_if_exists('idx_case_23_c2', 'case_23_weird_syntax');
CALL drop_index_if_exists('idx_case_23_c3', 'case_23_weird_syntax');
CALL drop_index_if_exists('idx_case_23_c4', 'case_23_weird_syntax');
CALL drop_index_if_exists('idx_case_23_c5', 'case_23_weird_syntax');
CREATE INDEX idx_case_23_c1 ON case_23_weird_syntax(c1);
CREATE INDEX idx_case_23_c2 ON case_23_weird_syntax(c2);
CREATE INDEX idx_case_23_c3 ON case_23_weird_syntax(c3);
CREATE INDEX idx_case_23_c4 ON case_23_weird_syntax(c4);
CREATE INDEX idx_case_23_c5 ON case_23_weird_syntax(c5);

-- 为case_24_edge_cases表添加索引
CALL drop_index_if_exists('idx_case_24_c2', 'case_24_edge_cases');
CALL drop_index_if_exists('idx_case_24_c3', 'case_24_edge_cases');
CALL drop_index_if_exists('idx_case_24_c5', 'case_24_edge_cases');
CREATE INDEX idx_case_24_c2 ON case_24_edge_cases(c2);
CREATE INDEX idx_case_24_c3 ON case_24_edge_cases(c3);
CREATE INDEX idx_case_24_c5 ON case_24_edge_cases(c5);

-- 为case_25_mysql8_reserved表添加索引
CALL drop_index_if_exists('idx_case_25_rank', 'case_25_mysql8_reserved');
CALL drop_index_if_exists('idx_case_25_system', 'case_25_mysql8_reserved');
CALL drop_index_if_exists('idx_case_25_groups', 'case_25_mysql8_reserved');
CALL drop_index_if_exists('idx_case_25_window', 'case_25_mysql8_reserved');
CALL drop_index_if_exists('idx_case_25_function', 'case_25_mysql8_reserved');
CALL drop_index_if_exists('idx_case_25_role', 'case_25_mysql8_reserved');
CALL drop_index_if_exists('idx_case_25_admin', 'case_25_mysql8_reserved');
CREATE INDEX idx_case_25_rank ON case_25_mysql8_reserved(`rank`);
CREATE INDEX idx_case_25_system ON case_25_mysql8_reserved(`system`);
CREATE INDEX idx_case_25_groups ON case_25_mysql8_reserved(`groups`(20));
CREATE INDEX idx_case_25_window ON case_25_mysql8_reserved(`window`);
CREATE INDEX idx_case_25_function ON case_25_mysql8_reserved(`function`);
CREATE INDEX idx_case_25_role ON case_25_mysql8_reserved(`role`);
CREATE INDEX idx_case_25_admin ON case_25_mysql8_reserved(`admin`);

-- 为case_26_mysql8_invisible表添加索引
CALL drop_index_if_exists('idx_case_26_id', 'case_26_mysql8_invisible');
CREATE INDEX idx_case_26_id ON case_26_mysql8_invisible(id);

-- 为case_27_mysql8_check表添加索引
CALL drop_index_if_exists('idx_case_27_id', 'case_27_mysql8_check');
CALL drop_index_if_exists('idx_case_27_age', 'case_27_mysql8_check');
CREATE INDEX idx_case_27_id ON case_27_mysql8_check(id);
CREATE INDEX idx_case_27_age ON case_27_mysql8_check(age);

-- 为case_28_mysql8_func_index表添加索引
CALL drop_index_if_exists('idx_case_28_name', 'case_28_mysql8_func_index');
CREATE INDEX idx_case_28_name ON case_28_mysql8_func_index(name);

-- 为case_29_mysql8_defaults表添加索引
CALL drop_index_if_exists('idx_case_29_id', 'case_29_mysql8_defaults');
CALL drop_index_if_exists('idx_case_29_val', 'case_29_mysql8_defaults');
CREATE INDEX idx_case_29_id ON case_29_mysql8_defaults(id);
CREATE INDEX idx_case_29_val ON case_29_mysql8_defaults(val);

-- 为case_30_mysql8_collations表添加索引
CALL drop_index_if_exists('idx_case_30_c1', 'case_30_mysql8_collations');
CALL drop_index_if_exists('idx_case_30_c2', 'case_30_mysql8_collations');
CALL drop_index_if_exists('idx_case_30_c3', 'case_30_mysql8_collations');
CREATE INDEX idx_case_30_c1 ON case_30_mysql8_collations(c1);
CREATE INDEX idx_case_30_c2 ON case_30_mysql8_collations(c2);
CREATE INDEX idx_case_30_c3 ON case_30_mysql8_collations(c3);

-- 为case_31_sys_utf8表添加索引
CALL drop_index_if_exists('idx_case_31_host', 'case_31_sys_utf8');
CALL drop_index_if_exists('idx_case_31_db', 'case_31_sys_utf8');
CALL drop_index_if_exists('idx_case_31_user', 'case_31_sys_utf8');
CALL drop_index_if_exists('idx_case_31_composite', 'case_31_sys_utf8');
CREATE INDEX idx_case_31_host ON case_31_sys_utf8(Host);
CREATE INDEX idx_case_31_db ON case_31_sys_utf8(Db);
CREATE INDEX idx_case_31_user ON case_31_sys_utf8(User);
CREATE INDEX idx_case_31_composite ON case_31_sys_utf8(Host, Db, User);

-- 为case_32_complex_generated表添加索引
CALL drop_index_if_exists('idx_case_32_cost_name', 'case_32_complex_generated');
CREATE INDEX idx_case_32_cost_name ON case_32_complex_generated(cost_name);

-- 为case_33_desc_index表添加索引
CALL drop_index_if_exists('idx_case_33_host', 'case_33_desc_index');
CALL drop_index_if_exists('idx_case_33_user', 'case_33_desc_index');
CREATE INDEX idx_case_33_host ON case_33_desc_index(Host);
CREATE INDEX idx_case_33_user ON case_33_desc_index(User);

-- 为case_34_table_options表添加索引
CALL drop_index_if_exists('idx_case_34_id', 'case_34_table_options');
CREATE INDEX idx_case_34_id ON case_34_table_options(id);

-- 为case_35_enum_charset表添加索引
CALL drop_index_if_exists('idx_case_35_col_enum', 'case_35_enum_charset');
CALL drop_index_if_exists('idx_case_35_col_set', 'case_35_enum_charset');
CREATE INDEX idx_case_35_col_enum ON case_35_enum_charset(col_enum);
CREATE INDEX idx_case_35_col_set ON case_35_enum_charset(col_set);

-- 为CASE_36_UPPERCASE表添加索引
CALL drop_index_if_exists('idx_case_36_id', 'CASE_36_UPPERCASE');
CALL drop_index_if_exists('idx_case_36_name', 'CASE_36_UPPERCASE');
CALL drop_index_if_exists('idx_case_36_age', 'CASE_36_UPPERCASE');
CALL drop_index_if_exists('idx_case_36_email', 'CASE_36_UPPERCASE');
CALL drop_index_if_exists('idx_case_36_create_date', 'CASE_36_UPPERCASE');
CREATE INDEX idx_case_36_id ON `CASE_36_UPPERCASE`(`ID`);
CREATE INDEX idx_case_36_name ON `CASE_36_UPPERCASE`(`NAME`);
CREATE INDEX idx_case_36_age ON `CASE_36_UPPERCASE`(`AGE`);
CREATE INDEX idx_case_36_email ON `CASE_36_UPPERCASE`(`EMAIL`);
CREATE INDEX idx_case_36_create_date ON `CASE_36_UPPERCASE`(`CREATE_DATE`);

-- 为CASE_37_HUMP表添加索引
CALL drop_index_if_exists('idx_case_37_product_id', 'CASE_37_HUMP');
CALL drop_index_if_exists('idx_case_37_product_name', 'CASE_37_HUMP');
CALL drop_index_if_exists('idx_case_37_category', 'CASE_37_HUMP');
CALL drop_index_if_exists('idx_case_37_last_update', 'CASE_37_HUMP');
CREATE INDEX idx_case_37_product_id ON `CASE_37_HUMP`(`ProductId`);
CREATE INDEX idx_case_37_product_name ON `CASE_37_HUMP`(`ProductName`);
CREATE INDEX idx_case_37_category ON `CASE_37_HUMP`(`Category`);
CREATE INDEX idx_case_37_last_update ON `CASE_37_HUMP`(`LastUpdate`);

-- 为CASE_38_SNAKE表添加索引
CALL drop_index_if_exists('idx_case_38_product_id', 'CASE_38_SNAKE');
CALL drop_index_if_exists('idx_case_38_product_name', 'CASE_38_SNAKE');
CALL drop_index_if_exists('idx_case_38_category', 'CASE_38_SNAKE');
CALL drop_index_if_exists('idx_case_38_last_update', 'CASE_38_SNAKE');
CREATE INDEX idx_case_38_product_id ON `CASE_38_SNAKE`(`product_id`);
CREATE INDEX idx_case_38_product_name ON `CASE_38_SNAKE`(`product_name`);
CREATE INDEX idx_case_38_category ON `CASE_38_SNAKE`(`category`);
CREATE INDEX idx_case_38_last_update ON `CASE_38_SNAKE`(`last_update`);

-- 为CASE_39_UNDERSCORE表添加索引
CALL drop_index_if_exists('idx_case_39_product_id', 'CASE_39_UNDERSCORE');
CALL drop_index_if_exists('idx_case_39_product_name', 'CASE_39_UNDERSCORE');
CALL drop_index_if_exists('idx_case_39_category', 'CASE_39_UNDERSCORE');
CALL drop_index_if_exists('idx_case_39_last_update', 'CASE_39_UNDERSCORE');
CREATE INDEX idx_case_39_product_id ON `CASE_39_UNDERSCORE`(`product_id`);
CREATE INDEX idx_case_39_product_name ON `CASE_39_UNDERSCORE`(`product_name`);
CREATE INDEX idx_case_39_category ON `CASE_39_UNDERSCORE`(`category`);
CREATE INDEX idx_case_39_last_update ON `CASE_39_UNDERSCORE`(`last_update`);

-- 为CASE_40_DEFAULT表添加索引
CALL drop_index_if_exists('idx_case_40_id', 'CASE_40_DEFAULT');
CALL drop_index_if_exists('idx_case_40_name', 'CASE_40_DEFAULT');
CALL drop_index_if_exists('idx_case_40_age', 'CASE_40_DEFAULT');
CALL drop_index_if_exists('idx_case_40_email', 'CASE_40_DEFAULT');
CREATE INDEX idx_case_40_id ON `CASE_40_DEFAULT`(`id`);
CREATE INDEX idx_case_40_name ON `CASE_40_DEFAULT`(`name`);
CREATE INDEX idx_case_40_age ON `CASE_40_DEFAULT`(`age`);
CREATE INDEX idx_case_40_email ON `CASE_40_DEFAULT`(`email`);

-- 为case_41_parent表添加索引
CALL drop_index_if_exists('idx_case_41_parent_name', 'case_41_parent');
CREATE INDEX idx_case_41_parent_name ON case_41_parent(name);

-- 为case_41_foreign_key表添加索引
-- CALL drop_index_if_exists('idx_case_41_foreign_parent_id', 'case_41_foreign_key');
CALL drop_index_if_exists('idx_case_41_foreign_name', 'case_41_foreign_key');
-- CREATE INDEX idx_case_41_foreign_parent_id ON case_41_foreign_key(parent_id);
CREATE INDEX idx_case_41_foreign_name ON case_41_foreign_key(name);

-- 为case_42_fulltext表添加全文索引
CALL drop_index_if_exists('ft_case_42_title_content', 'case_42_fulltext');
CREATE FULLTEXT INDEX ft_case_42_title_content ON case_42_fulltext(title, content);

-- 为case_43_spatial_index表添加空间索引
CALL drop_index_if_exists('idx_case_43_location', 'case_43_spatial_index');
-- CREATE SPATIAL INDEX idx_case_43_location ON case_43_spatial_index(location);

-- 为case_45_stored_generated表添加索引
CALL drop_index_if_exists('idx_case_45_id', 'case_45_stored_generated');
CALL drop_index_if_exists('idx_case_45_c1', 'case_45_stored_generated');
CREATE INDEX idx_case_45_id ON case_45_stored_generated(id);
CREATE INDEX idx_case_45_c1 ON case_45_stored_generated(c1);

-- 为case_46_myisam表添加索引
CALL drop_index_if_exists('idx_case_46_name', 'case_46_myisam');
CREATE INDEX idx_case_46_name ON case_46_myisam(name);

-- 为case_47_memory表添加索引
CALL drop_index_if_exists('idx_case_47_name', 'case_47_memory');
CREATE INDEX idx_case_47_name ON case_47_memory(name);

-- 为case_49_list_partition表添加索引
CALL drop_index_if_exists('idx_case_49_id', 'case_49_list_partition');
CALL drop_index_if_exists('idx_case_49_category', 'case_49_list_partition');
CREATE INDEX idx_case_49_id ON case_49_list_partition(id);
CREATE INDEX idx_case_49_category ON case_49_list_partition(category);

-- 为case_50_hash_partition表添加索引
CALL drop_index_if_exists('idx_case_50_name', 'case_50_hash_partition');
CREATE INDEX idx_case_50_name ON case_50_hash_partition(name);

-- 为case_51_copy_like表添加索引（复制自case_01_integers）
CALL drop_index_if_exists('idx_case_51_col_tiny', 'case_51_copy_like');
CALL drop_index_if_exists('idx_case_51_col_big', 'case_51_copy_like');
CREATE INDEX idx_case_51_col_tiny ON case_51_copy_like(col_tiny);
CREATE INDEX idx_case_51_col_big ON case_51_copy_like(col_big);

-- 为case_52_copy_as表添加索引（复制自case_01_integers）
CALL drop_index_if_exists('idx_case_52_col_tiny', 'case_52_copy_as');
CALL drop_index_if_exists('idx_case_52_col_big', 'case_52_copy_as');
CREATE INDEX idx_case_52_col_tiny ON case_52_copy_as(col_tiny);
CREATE INDEX idx_case_52_col_big ON case_52_copy_as(col_big);

-- 为case_53_deferred_constraint表添加索引
CALL drop_index_if_exists('idx_case_53_name', 'case_53_deferred_constraint');
CREATE INDEX idx_case_53_name ON case_53_deferred_constraint(name);

-- 为case_54_tablespace表添加索引
CALL drop_index_if_exists('idx_case_54_name', 'case_54_tablespace');
CREATE INDEX idx_case_54_name ON case_54_tablespace(name);

-- 为case_55_compressed表添加索引
CALL drop_index_if_exists('idx_case_55_data', 'case_55_compressed');
CREATE INDEX idx_case_55_data ON case_55_compressed(data(10));

-- 为case_56_encrypted表添加索引
CALL drop_index_if_exists('idx_case_56_sensitive_data', 'case_56_encrypted');
CREATE INDEX idx_case_56_sensitive_data ON case_56_encrypted(sensitive_data);

-- 为case_57_column_privileges表添加索引
CALL drop_index_if_exists('idx_case_57_public_data', 'case_57_column_privileges');
CALL drop_index_if_exists('idx_case_57_sensitive_data', 'case_57_column_privileges');
CREATE INDEX idx_case_57_public_data ON case_57_column_privileges(public_data);
CREATE INDEX idx_case_57_sensitive_data ON case_57_column_privileges(sensitive_data);

-- 为case_58_subpartition表添加索引
CALL drop_index_if_exists('idx_case_58_id', 'case_58_subpartition');
CALL drop_index_if_exists('idx_case_58_year', 'case_58_subpartition');
CALL drop_index_if_exists('idx_case_58_month', 'case_58_subpartition');
CALL drop_index_if_exists('idx_case_58_composite', 'case_58_subpartition');
CREATE INDEX idx_case_58_id ON case_58_subpartition(id);
CREATE INDEX idx_case_58_year ON case_58_subpartition(year);
CREATE INDEX idx_case_58_month ON case_58_subpartition(month);
CREATE INDEX idx_case_58_composite ON case_58_subpartition(year, month);

-- 为case_59_complex_generated表添加索引
CALL drop_index_if_exists('idx_case_59_id', 'case_59_complex_generated');
CALL drop_index_if_exists('idx_case_59_price', 'case_59_complex_generated');
CALL drop_index_if_exists('idx_case_59_quantity', 'case_59_complex_generated');
CALL drop_index_if_exists('idx_case_59_discount', 'case_59_complex_generated');
CALL drop_index_if_exists('idx_case_59_composite', 'case_59_complex_generated');
CREATE INDEX idx_case_59_id ON case_59_complex_generated(id);
CREATE INDEX idx_case_59_price ON case_59_complex_generated(price);
CREATE INDEX idx_case_59_quantity ON case_59_complex_generated(quantity);
CREATE INDEX idx_case_59_discount ON case_59_complex_generated(discount);
CREATE INDEX idx_case_59_composite ON case_59_complex_generated(price, quantity);

-- 为case_60_statistics表添加索引
CALL drop_index_if_exists('idx_case_60_category', 'case_60_statistics');
CALL drop_index_if_exists('idx_case_60_subcategory', 'case_60_statistics');
CALL drop_index_if_exists('idx_case_60_composite', 'case_60_statistics');
CREATE INDEX idx_case_60_category ON case_60_statistics(category);
CREATE INDEX idx_case_60_subcategory ON case_60_statistics(subcategory);
CREATE INDEX idx_case_60_composite ON case_60_statistics(category, subcategory);

-- 为case_61_many_columns表添加索引
CALL drop_index_if_exists('idx_case_61_tinyint_min', 'case_61_many_columns');
CALL drop_index_if_exists('idx_case_61_int_min', 'case_61_many_columns');
CALL drop_index_if_exists('idx_case_61_bigint_max', 'case_61_many_columns');
CALL drop_index_if_exists('idx_case_61_date_col', 'case_61_many_columns');
CALL drop_index_if_exists('idx_case_61_datetime_col', 'case_61_many_columns');
CALL drop_index_if_exists('idx_case_61_timestamp_col', 'case_61_many_columns');
CALL drop_index_if_exists('idx_case_61_enum_min', 'case_61_many_columns');
CALL drop_index_if_exists('idx_case_61_enum_max', 'case_61_many_columns');
CALL drop_index_if_exists('idx_case_61_set_min', 'case_61_many_columns');
CALL drop_index_if_exists('idx_case_61_set_max', 'case_61_many_columns');
CREATE INDEX idx_case_61_tinyint_min ON case_61_many_columns(tinyint_min);
CREATE INDEX idx_case_61_int_min ON case_61_many_columns(int_min);
CREATE INDEX idx_case_61_bigint_max ON case_61_many_columns(bigint_max);
CREATE INDEX idx_case_61_date_col ON case_61_many_columns(date_col);
CREATE INDEX idx_case_61_datetime_col ON case_61_many_columns(datetime_col);
CREATE INDEX idx_case_61_timestamp_col ON case_61_many_columns(timestamp_col);
CREATE INDEX idx_case_61_enum_min ON case_61_many_columns(enum_min);
CREATE INDEX idx_case_61_enum_max ON case_61_many_columns(enum_max);
CREATE INDEX idx_case_61_set_min ON case_61_many_columns(set_min);
CREATE INDEX idx_case_61_set_max ON case_61_many_columns(set_max);

-- 为case_62_various_defaults表添加索引
CALL drop_index_if_exists('idx_case_62_name', 'case_62_various_defaults');
CALL drop_index_if_exists('idx_case_62_age', 'case_62_various_defaults');
CALL drop_index_if_exists('idx_case_62_active', 'case_62_various_defaults');
CALL drop_index_if_exists('idx_case_62_created_at', 'case_62_various_defaults');
CALL drop_index_if_exists('idx_case_62_updated_at', 'case_62_various_defaults');
CALL drop_index_if_exists('idx_case_62_price', 'case_62_various_defaults');
CALL drop_index_if_exists('idx_case_62_quantity', 'case_62_various_defaults');
CALL drop_index_if_exists('idx_case_62_status', 'case_62_various_defaults');
CREATE INDEX idx_case_62_name ON case_62_various_defaults(name);
CREATE INDEX idx_case_62_age ON case_62_various_defaults(age);
CREATE INDEX idx_case_62_active ON case_62_various_defaults(active);
CREATE INDEX idx_case_62_created_at ON case_62_various_defaults(created_at);
CREATE INDEX idx_case_62_updated_at ON case_62_various_defaults(updated_at);
CREATE INDEX idx_case_62_price ON case_62_various_defaults(price);
CREATE INDEX idx_case_62_quantity ON case_62_various_defaults(quantity);
CREATE INDEX idx_case_62_status ON case_62_various_defaults(status);

-- 为case_63_charset_collation表添加索引
CALL drop_index_if_exists('idx_case_63_name_en', 'case_63_charset_collation');
CALL drop_index_if_exists('idx_case_63_name_zh', 'case_63_charset_collation');
CALL drop_index_if_exists('idx_case_63_name_de', 'case_63_charset_collation');
CALL drop_index_if_exists('idx_case_63_code', 'case_63_charset_collation');
CREATE INDEX idx_case_63_name_en ON case_63_charset_collation(name_en);
CREATE INDEX idx_case_63_name_zh ON case_63_charset_collation(name_zh);
CREATE INDEX idx_case_63_name_de ON case_63_charset_collation(name_de);
CREATE INDEX idx_case_63_code ON case_63_charset_collation(code);

-- 为case_64_bit_types表添加索引
CALL drop_index_if_exists('idx_case_64_b1', 'case_64_bit_types');
CALL drop_index_if_exists('idx_case_64_b8', 'case_64_bit_types');
CALL drop_index_if_exists('idx_case_64_b16', 'case_64_bit_types');
CALL drop_index_if_exists('idx_case_64_b32', 'case_64_bit_types');
CALL drop_index_if_exists('idx_case_64_b64', 'case_64_bit_types');
CREATE INDEX idx_case_64_b1 ON case_64_bit_types(b1);
CREATE INDEX idx_case_64_b8 ON case_64_bit_types(b8);
CREATE INDEX idx_case_64_b16 ON case_64_bit_types(b16);
CREATE INDEX idx_case_64_b32 ON case_64_bit_types(b32);
CREATE INDEX idx_case_64_b64 ON case_64_bit_types(b64);

-- 为case_65_year_types表添加索引
CALL drop_index_if_exists('idx_case_65_y4', 'case_65_year_types');
CALL drop_index_if_exists('idx_case_65_y_default', 'case_65_year_types');
CREATE INDEX idx_case_65_y4 ON case_65_year_types(y4);
CREATE INDEX idx_case_65_y_default ON case_65_year_types(y_default);

-- 为case_66_geometry_subtypes表添加空间索引
/***
CALL drop_index_if_exists('idx_case_66_geo', 'case_66_geometry_subtypes');
CALL drop_index_if_exists('idx_case_66_pt', 'case_66_geometry_subtypes');
CREATE SPATIAL INDEX idx_case_66_geo ON case_66_geometry_subtypes(geo);
-- CREATE SPATIAL INDEX idx_case_66_pt ON case_66_geometry_subtypes(pt);
**/

-- 为case_67_trigger_simulation表添加索引
CALL drop_index_if_exists('idx_case_67_created_at', 'case_67_trigger_simulation');
CALL drop_index_if_exists('idx_case_67_updated_at', 'case_67_trigger_simulation');
CREATE INDEX idx_case_67_created_at ON case_67_trigger_simulation(created_at);
CREATE INDEX idx_case_67_updated_at ON case_67_trigger_simulation(updated_at);

-- 为case_68_view_simulation表添加索引
CALL drop_index_if_exists('idx_case_68_view_id', 'case_68_view_simulation');
CALL drop_index_if_exists('idx_case_68_calc_result', 'case_68_view_simulation');
CREATE INDEX idx_case_68_view_id ON case_68_view_simulation(view_id);
CREATE INDEX idx_case_68_calc_result ON case_68_view_simulation(calc_result);

-- 为case_69_deeply_nested_json表添加索引
-- JSON列不能直接创建索引

-- 为case_70_utf8mb4_900表添加索引
CALL drop_index_if_exists('idx_case_70_str1', 'case_70_utf8mb4_900');
CALL drop_index_if_exists('idx_case_70_str2', 'case_70_utf8mb4_900');
CREATE INDEX idx_case_70_str1 ON case_70_utf8mb4_900(str1);
CREATE INDEX idx_case_70_str2 ON case_70_utf8mb4_900(str2);

-- 为case_71_functional_index_complex表添加索引
CALL drop_index_if_exists('idx_case_71_first_name', 'case_71_functional_index_complex');
CALL drop_index_if_exists('idx_case_71_last_name', 'case_71_functional_index_complex');
CALL drop_index_if_exists('idx_case_71_composite', 'case_71_functional_index_complex');
CREATE INDEX idx_case_71_first_name ON case_71_functional_index_complex(first_name);
CREATE INDEX idx_case_71_last_name ON case_71_functional_index_complex(last_name);
CREATE INDEX idx_case_71_composite ON case_71_functional_index_complex(first_name, last_name);

-- 为case_72_check_constraint_regex表添加索引
CALL drop_index_if_exists('idx_case_72_email', 'case_72_check_constraint_regex');
CREATE INDEX idx_case_72_email ON case_72_check_constraint_regex(email);

-- 为case_73_generated_stored_mixed表添加索引
CALL drop_index_if_exists('idx_case_73_side_a', 'case_73_generated_stored_mixed');
CALL drop_index_if_exists('idx_case_73_side_b', 'case_73_generated_stored_mixed');
CALL drop_index_if_exists('idx_case_73_area', 'case_73_generated_stored_mixed');
CREATE INDEX idx_case_73_side_a ON case_73_generated_stored_mixed(side_a);
CREATE INDEX idx_case_73_side_b ON case_73_generated_stored_mixed(side_b);
CREATE INDEX idx_case_73_area ON case_73_generated_stored_mixed(area);

-- 为case_74_invisible_cols_mixed表添加索引
CALL drop_index_if_exists('idx_case_74_secret_code', 'case_74_invisible_cols_mixed');
CALL drop_index_if_exists('idx_case_74_public_code', 'case_74_invisible_cols_mixed');
CREATE INDEX idx_case_74_secret_code ON case_74_invisible_cols_mixed(secret_code);
CREATE INDEX idx_case_74_public_code ON case_74_invisible_cols_mixed(public_code);

-- 为case_75_desc_primary_key表添加索引
-- 主键已存在，添加额外索引

-- 为case_76_blob_keys表添加前缀索引
CALL drop_index_if_exists('idx_case_76_data', 'case_76_blob_keys');
CREATE INDEX idx_case_76_data ON case_76_blob_keys(data(10));

-- 为case_77_text_keys表添加前缀索引
CALL drop_index_if_exists('idx_case_77_content', 'case_77_text_keys');
CREATE INDEX idx_case_77_content ON case_77_text_keys(content(20));

-- 为case_78_multi_col_unique_null表添加索引
CALL drop_index_if_exists('idx_case_78_code', 'case_78_multi_col_unique_null');
CALL drop_index_if_exists('idx_case_78_category', 'case_78_multi_col_unique_null');
CREATE INDEX idx_case_78_code ON case_78_multi_col_unique_null(code);
CREATE INDEX idx_case_78_category ON case_78_multi_col_unique_null(category);

-- 为case_79_serial_default表添加索引
CALL drop_index_if_exists('idx_case_79_name', 'case_79_serial_default');
CREATE INDEX idx_case_79_name ON case_79_serial_default(name);

-- 为case_80_on_update_current_timestamp表添加索引
CALL drop_index_if_exists('idx_case_80_modified_at', 'case_80_on_update_current_timestamp');
CREATE INDEX idx_case_80_modified_at ON case_80_on_update_current_timestamp(modified_at);

-- 为case_81_geometry_srid表添加空间索引
CALL drop_index_if_exists('idx_case_81_geo', 'case_81_geometry_srid');
-- CREATE SPATIAL INDEX idx_case_81_geo ON case_81_geometry_srid(geo);

-- 为case_82_wide_table表添加索引
CALL drop_index_if_exists('idx_case_82_c01', 'case_82_wide_table');
CALL drop_index_if_exists('idx_case_82_c05', 'case_82_wide_table');
CALL drop_index_if_exists('idx_case_82_c10', 'case_82_wide_table');
CALL drop_index_if_exists('idx_case_82_composite', 'case_82_wide_table');
CREATE INDEX idx_case_82_c01 ON case_82_wide_table(c01);
CREATE INDEX idx_case_82_c05 ON case_82_wide_table(c05);
CREATE INDEX idx_case_82_c10 ON case_82_wide_table(c10);
CREATE INDEX idx_case_82_composite ON case_82_wide_table(c01, c05, c10);

-- 为case_83_long_identifiers表添加索引
CALL drop_index_if_exists('idx_case_83_long_col', 'case_83_long_identifiers');
CREATE INDEX idx_case_83_long_col ON case_83_long_identifiers(this_is_a_very_long_column_name_that_reaches_limit_of_64_chars);

-- 为case_84_reserved_words_quoted表添加索引
CALL drop_index_if_exists('idx_case_84_select', 'case_84_reserved_words_quoted');
CALL drop_index_if_exists('idx_case_84_update', 'case_84_reserved_words_quoted');
CALL drop_index_if_exists('idx_case_84_delete', 'case_84_reserved_words_quoted');
CALL drop_index_if_exists('idx_case_84_insert', 'case_84_reserved_words_quoted');
CREATE INDEX idx_case_84_select ON case_84_reserved_words_quoted(`select`);
CREATE INDEX idx_case_84_update ON case_84_reserved_words_quoted(`update`);
CREATE INDEX idx_case_84_delete ON case_84_reserved_words_quoted(`delete`);
CREATE INDEX idx_case_84_insert ON case_84_reserved_words_quoted(`insert`);

-- 为case_85_numeric_precision_scale表添加索引
CALL drop_index_if_exists('idx_case_85_high_prec', 'case_85_numeric_precision_scale');
CALL drop_index_if_exists('idx_case_85_low_scale', 'case_85_numeric_precision_scale');
CREATE INDEX idx_case_85_high_prec ON case_85_numeric_precision_scale(high_prec);
CREATE INDEX idx_case_85_low_scale ON case_85_numeric_precision_scale(low_scale);

-- 为case_86_zerofill_variants表添加索引
CALL drop_index_if_exists('idx_case_86_z_tiny', 'case_86_zerofill_variants');
CALL drop_index_if_exists('idx_case_86_z_big', 'case_86_zerofill_variants');
CREATE INDEX idx_case_86_z_tiny ON case_86_zerofill_variants(z_tiny);
CREATE INDEX idx_case_86_z_big ON case_86_zerofill_variants(z_big);

-- 为case_87_float_double_unsigned表添加索引
CALL drop_index_if_exists('idx_case_87_f_uns', 'case_87_float_double_unsigned');
CALL drop_index_if_exists('idx_case_87_d_uns', 'case_87_float_double_unsigned');
CREATE INDEX idx_case_87_f_uns ON case_87_float_double_unsigned(f_uns);
CREATE INDEX idx_case_87_d_uns ON case_87_float_double_unsigned(d_uns);

-- 为case_88_year_conversion表添加索引
CALL drop_index_if_exists('idx_case_88_birth_year', 'case_88_year_conversion');
CREATE INDEX idx_case_88_birth_year ON case_88_year_conversion(birth_year);

-- 为case_89_national_char表添加索引
CALL drop_index_if_exists('idx_case_89_nat_char', 'case_89_national_char');
CALL drop_index_if_exists('idx_case_89_nat_varchar', 'case_89_national_char');
CREATE INDEX idx_case_89_nat_char ON case_89_national_char(nat_char);
CREATE INDEX idx_case_89_nat_varchar ON case_89_national_char(nat_varchar);

-- 为case_90_spatial_reference表添加空间索引
CALL drop_index_if_exists('idx_case_90_loc', 'case_90_spatial_reference');
-- CREATE SPATIAL INDEX idx_case_90_loc ON case_90_spatial_reference(loc);

-- 为case_91_json_array_index表添加索引
-- JSON列不能直接创建索引

-- 为case_92_fulltext_ngram表添加全文索引
CALL drop_index_if_exists('ft_case_92_content', 'case_92_fulltext_ngram');
CREATE FULLTEXT INDEX ft_case_92_content ON case_92_fulltext_ngram(content) WITH PARSER ngram;

-- 为case_93_fulltext_parser表添加全文索引
CALL drop_index_if_exists('ft_case_93_description', 'case_93_fulltext_parser');
CREATE FULLTEXT INDEX ft_case_93_description ON case_93_fulltext_parser(description);

-- 为case_94_innodb_row_formats表添加索引
CALL drop_index_if_exists('idx_case_94_data', 'case_94_innodb_row_formats');
CREATE INDEX idx_case_94_data ON case_94_innodb_row_formats(data);

-- 为case_95_union_view_table表添加索引
CALL drop_index_if_exists('idx_case_95_id', 'case_95_union_view_table');
CALL drop_index_if_exists('idx_case_95_source_type', 'case_95_union_view_table');
CALL drop_index_if_exists('idx_case_95_common_field', 'case_95_union_view_table');
CREATE INDEX idx_case_95_id ON case_95_union_view_table(id);
CREATE INDEX idx_case_95_source_type ON case_95_union_view_table(source_type);
CREATE INDEX idx_case_95_common_field ON case_95_union_view_table(common_field);

-- 为case_96_partition_list_columns表添加索引
CALL drop_index_if_exists('idx_case_96_id', 'case_96_partition_list_columns');
CALL drop_index_if_exists('idx_case_96_region_code', 'case_96_partition_list_columns');
CALL drop_index_if_exists('idx_case_96_store_id', 'case_96_partition_list_columns');
CREATE INDEX idx_case_96_id ON case_96_partition_list_columns(id);
CREATE INDEX idx_case_96_region_code ON case_96_partition_list_columns(region_code);
CREATE INDEX idx_case_96_store_id ON case_96_partition_list_columns(store_id);

-- 为case_97_partition_range_columns表添加索引
CALL drop_index_if_exists('idx_case_97_id', 'case_97_partition_range_columns');
CALL drop_index_if_exists('idx_case_97_event_date', 'case_97_partition_range_columns');
CREATE INDEX idx_case_97_id ON case_97_partition_range_columns(id);
CREATE INDEX idx_case_97_event_date ON case_97_partition_range_columns(event_date);

-- 为case_98_partition_key表添加索引
-- 主键已存在，添加额外索引

-- 为case_99_partition_linear_hash表添加索引
-- 主键已存在，添加额外索引
CALL drop_index_if_exists('idx_case_99_val', 'case_99_partition_linear_hash');
CREATE INDEX idx_case_99_val ON case_99_partition_linear_hash(val);

-- 为case_100_max_complexity表添加索引
CALL drop_index_if_exists('idx_case_100_user_code', 'case_100_max_complexity');
CALL drop_index_if_exists('idx_case_100_display_name', 'case_100_max_complexity');
CALL drop_index_if_exists('idx_case_100_created_at', 'case_100_max_complexity');
CALL drop_index_if_exists('idx_case_100_is_deleted', 'case_100_max_complexity');
CALL drop_index_if_exists('ft_case_100_display_name', 'case_100_max_complexity');
CREATE INDEX idx_case_100_user_code ON case_100_max_complexity(user_code);
CREATE INDEX idx_case_100_display_name ON case_100_max_complexity(display_name);
CREATE INDEX idx_case_100_created_at ON case_100_max_complexity(created_at);
CREATE INDEX idx_case_100_is_deleted ON case_100_max_complexity(is_deleted);
-- 创建全文索引
CREATE FULLTEXT INDEX ft_case_100_display_name ON case_100_max_complexity(display_name);
