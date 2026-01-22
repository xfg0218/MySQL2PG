-- 为 case_01_integers 表创建索引
CREATE INDEX idx_case01_col_int ON case_01_integers(col_int);
CREATE UNIQUE INDEX uidx_case01_col_big ON case_01_integers(col_big);
CREATE INDEX idx_case01_composite ON case_01_integers(col_small, col_medium);
CREATE INDEX idx_case01_col_int_prec ON case_01_integers(col_int_prec);

-- 为 case_02_boolean 表创建索引
CREATE INDEX idx_case02_is_active ON case_02_boolean(is_active);
CREATE INDEX idx_case02_status ON case_02_boolean(status);
CREATE INDEX idx_case02_is_deleted ON case_02_boolean(is_deleted);
CREATE INDEX idx_case02_composite ON case_02_boolean(is_active, status);

-- 为 case_03_floats 表创建索引
CREATE INDEX idx_case03_col_decimal ON case_03_floats(col_decimal);
CREATE INDEX idx_case03_col_float ON case_03_floats(col_float);
CREATE INDEX idx_case03_composite ON case_03_floats(col_double, col_real);

-- 为 case_04_mb3_suffix 表创建索引
CREATE INDEX idx_case04_col_var_mb3 ON case_04_mb3_suffix(col_var_mb3);
CREATE INDEX idx_case04_col_char_mb3 ON case_04_mb3_suffix(col_char_mb3);
CREATE INDEX idx_case04_composite ON case_04_mb3_suffix(col_var_mb3, col_char_mb3);
CREATE INDEX idx_case04_col_var_mb3_prefix ON case_04_mb3_suffix(col_var_mb3(10));

-- 为 case_05_charsets 表创建索引
CREATE INDEX idx_case05_c1 ON case_05_charsets(c1);
CREATE INDEX idx_case05_c2 ON case_05_charsets(c2);
CREATE INDEX idx_case05_c3 ON case_05_charsets(c3);
CREATE INDEX idx_case05_composite ON case_05_charsets(c4, c5);

-- 为 case_06_collates 表创建索引
CREATE INDEX idx_case06_c1 ON case_06_collates(c1);
CREATE INDEX idx_case06_c2 ON case_06_collates(c2);
CREATE INDEX idx_case06_c3 ON case_06_collates(c3);
CREATE INDEX idx_case06_composite ON case_06_collates(c4, c5);

-- 为 case_07_complex_charsets 表创建索引
CREATE INDEX idx_case07_c1 ON case_07_complex_charsets(c1);
CREATE INDEX idx_case07_c2 ON case_07_complex_charsets(c2);
CREATE INDEX idx_case07_c3 ON case_07_complex_charsets(c3);
CREATE INDEX idx_case07_composite ON case_07_complex_charsets(c1, c2);

-- 为 case_09_datetime 表创建索引
CREATE INDEX idx_case09_d1 ON case_09_datetime(d1);
CREATE INDEX idx_case09_dt1 ON case_09_datetime(dt1);
CREATE INDEX idx_case09_ts1 ON case_09_datetime(ts1);
CREATE INDEX idx_case09_composite ON case_09_datetime(d1, t1);
-- 移除了 DESC，5.7 语法支持但无物理降序效果，统一为升序
CREATE INDEX idx_case09_dt1_desc ON case_09_datetime(dt1);

-- 为 case_10_defaults 表创建索引
CREATE INDEX idx_case10_c1 ON case_10_defaults(c1);
CREATE INDEX idx_case10_c3 ON case_10_defaults(c3);
CREATE INDEX idx_case10_c4 ON case_10_defaults(c4);
CREATE INDEX idx_case10_composite ON case_10_defaults(c1, c2);

-- 为 case_11_autoincrement 表创建索引
CREATE INDEX idx_case11_big_id ON case_11_autoincrement(big_id);
CREATE INDEX idx_case11_mixed_case ON case_11_autoincrement(mixed_case);

-- 为 case_12_unsigned 表创建索引
CREATE INDEX idx_case12_c1 ON case_12_unsigned(c1);
CREATE INDEX idx_case12_c2 ON case_12_unsigned(c2);
CREATE INDEX idx_case12_c3 ON case_12_unsigned(c3);
CREATE INDEX idx_case12_composite ON case_12_unsigned(c1, c2);

-- 为 case_13_enum_set 表创建索引
CREATE INDEX idx_case13_e1 ON case_13_enum_set(e1);
CREATE INDEX idx_case13_s1 ON case_13_enum_set(s1);
CREATE INDEX idx_case13_composite ON case_13_enum_set(e1, s1);

-- 为 case_14_binary 表创建索引
CREATE INDEX idx_case14_b1 ON case_14_binary(b1);
CREATE INDEX idx_case14_b2 ON case_14_binary(b2);
CREATE INDEX idx_case14_composite ON case_14_binary(b1, b2);

-- 为 case_15_options 表创建索引
CREATE INDEX idx_case15_id ON case_15_options(id);

-- 为 case_16_partition 表创建索引
CREATE INDEX idx_case16_id ON case_16_partition(id);
CREATE INDEX idx_case16_created_at ON case_16_partition(created_at);
CREATE INDEX idx_case16_composite ON case_16_partition(id, created_at);

-- 为 case_18_quotes 表创建索引
CREATE INDEX idx_case18_id ON case_18_quotes(`id`);
CREATE INDEX idx_case18_name ON case_18_quotes(`name`);
CREATE INDEX idx_case18_desc ON case_18_quotes(`desc`(100));
CREATE INDEX idx_case18_composite ON case_18_quotes(`id`, `name`);

-- 为 case_19_comments 表创建索引
CREATE INDEX idx_case19_c1 ON case_19_comments(c1);
CREATE INDEX idx_case19_c2 ON case_19_comments(c2);
CREATE INDEX idx_case19_c3 ON case_19_comments(c3);
CREATE INDEX idx_case19_composite ON case_19_comments(c1, c2);

-- 为 case_20_constraints 表创建索引
CREATE INDEX idx_case20_name ON case_20_constraints(name);

-- 为 case_21_virtual 表创建索引
CREATE INDEX idx_case21_id ON case_21_virtual(id);
CREATE INDEX idx_case21_c1 ON case_21_virtual(c1);
CREATE INDEX idx_case21_c2 ON case_21_virtual(c2);
CREATE INDEX idx_case21_composite ON case_21_virtual(id, c1);

-- 为 case_23_weird_syntax 表创建索引
CREATE INDEX idx_case23_c1 ON case_23_weird_syntax(c1);
CREATE INDEX idx_case23_c3 ON case_23_weird_syntax(c3);
CREATE INDEX idx_case23_composite ON case_23_weird_syntax(c1, c2);

-- 为 case_24_edge_cases 表创建索引
CREATE INDEX idx_case24_c1 ON case_24_edge_cases(c1(100));
CREATE INDEX idx_case24_c3 ON case_24_edge_cases(c3);
CREATE INDEX idx_case24_composite ON case_24_edge_cases(c3, c4);

-- 为 case_25_mysql8_reserved 表创建索引
CREATE INDEX idx_case25_rank ON case_25_mysql8_reserved(`rank`);
CREATE INDEX idx_case25_system ON case_25_mysql8_reserved(`system`);
CREATE INDEX idx_case25_groups ON case_25_mysql8_reserved(`groups`(100));
CREATE INDEX idx_case25_composite ON case_25_mysql8_reserved(`rank`, `system`);

-- 为 case_26_mysql8_invisible 表创建索引
CREATE INDEX idx_case26_id ON case_26_mysql8_invisible(id);
-- 移除了 INVISIBLE，5.7 不支持，创建为普通可见索引
CREATE INDEX idx_case26_c2_invisible ON case_26_mysql8_invisible(c2);

-- 为 case_27_mysql8_check 表创建索引
CREATE INDEX idx_case27_id ON case_27_mysql8_check(id);
CREATE INDEX idx_case27_age ON case_27_mysql8_check(age);
CREATE INDEX idx_case27_composite ON case_27_mysql8_check(id, age);

-- 为 case_28_mysql8_func_index 表创建索引
-- 函数索引改为普通列索引
CREATE INDEX idx_case28_name ON case_28_mysql8_func_index(name);

-- 为 case_29_mysql8_defaults 表创建索引
CREATE INDEX idx_case29_id ON case_29_mysql8_defaults(id);
CREATE INDEX idx_case29_val ON case_29_mysql8_defaults(val);
CREATE INDEX idx_case29_composite ON case_29_mysql8_defaults(id, val);

-- 为 case_30_mysql8_collations 表创建索引
CREATE INDEX idx_case30_c1 ON case_30_mysql8_collations(c1);
CREATE INDEX idx_case30_c2 ON case_30_mysql8_collations(c2);
CREATE INDEX idx_case30_c3 ON case_30_mysql8_collations(c3);
CREATE INDEX idx_case30_composite ON case_30_mysql8_collations(c1, c2);

-- 为 case_31_sys_utf8mb3 表创建索引
CREATE INDEX idx_case31_host ON case_31_sys_utf8mb3(Host);
CREATE INDEX idx_case31_db ON case_31_sys_utf8mb3(Db);
CREATE INDEX idx_case31_user ON case_31_sys_utf8mb3(User);
CREATE INDEX idx_case31_composite ON case_31_sys_utf8mb3(Host, Db, User);

-- 为 case_32_complex_generated 表创建索引
CREATE INDEX idx_case32_cost_name ON case_32_complex_generated(cost_name);
CREATE INDEX idx_case32_default_value ON case_32_complex_generated(default_value);

-- 为 case_33_desc_index 表创建索引
CREATE INDEX idx_case33_host ON case_33_desc_index(Host);
CREATE INDEX idx_case33_user ON case_33_desc_index(User);

-- 为 case_34_table_options 表创建索引
CREATE INDEX idx_case34_id ON case_34_table_options(id);

-- 为 case_35_enum_charset 表创建索引
CREATE INDEX idx_case35_col_enum ON case_35_enum_charset(col_enum);
CREATE INDEX idx_case35_col_set ON case_35_enum_charset(col_set);
CREATE INDEX idx_case35_composite ON case_35_enum_charset(col_enum, col_set);

-- 为 CASE_36_UPPERCASE 表创建索引
CREATE INDEX idx_test1_id ON `CASE_36_UPPERCASE`(ID);
CREATE INDEX idx_test1_name ON `CASE_36_UPPERCASE`(NAME);
CREATE INDEX idx_test1_email ON `CASE_36_UPPERCASE`(EMAIL);
CREATE INDEX idx_test1_create_date ON `CASE_36_UPPERCASE`(CREATE_DATE);
CREATE INDEX idx_test1_composite ON `CASE_36_UPPERCASE`(ID, NAME);
-- 函数索引改为普通索引
CREATE INDEX idx_test1_name_upper ON `CASE_36_UPPERCASE`(NAME);

-- 为 CASE_37_HUMP 表创建索引
CREATE INDEX idx_case37_productid ON `CASE_37_HUMP`(ProductId);
CREATE INDEX idx_case37_productname ON `CASE_37_HUMP`(`ProductName`);
CREATE INDEX idx_case37_price ON `CASE_37_HUMP`(`Price`);
CREATE INDEX idx_case37_stock ON `CASE_37_HUMP`(`Stock`);
CREATE INDEX idx_case37_category ON `CASE_37_HUMP`(ProductId, CATEGORY);


-- 为 CASE_38_SNAKE 表创建索引
CREATE INDEX   idx_case38_productid on `CASE_38_SNAKE`(product_id);
CREATE INDEX   idx_case38_product_name on `CASE_38_SNAKE`(product_name);
CREATE INDEX   idx_case38_price on `CASE_38_SNAKE`(price);
CREATE INDEX   idx_case38_category on `CASE_38_SNAKE`(product_id, category);

-- 为 CASE_39_UNDERSCORE 表创建索引
CREATE INDEX  idx_case39_productid ON `CASE_39_UNDERSCORE`(product_id);
CREATE INDEX  idx_case39_product_name ON `CASE_39_UNDERSCORE`(product_name);
CREATE INDEX  idx_case39_price ON `CASE_39_UNDERSCORE`(price);
CREATE INDEX  idx_case39_category ON `CASE_39_UNDERSCORE`(product_id, category);

-- 为 CASE_40_DEFAULT 表创建索引
CREATE INDEX  idx_case40_id ON `CASE_40_DEFAULT`(id);
CREATE INDEX  idx_case40_name ON `CASE_40_DEFAULT`(name);
CREATE INDEX  idx_case40_email ON `CASE_40_DEFAULT`(email);
CREATE INDEX  idx_case40_id_name ON `CASE_40_DEFAULT`(id, name);
