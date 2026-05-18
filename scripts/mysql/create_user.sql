-- 创建test1用户，密码为Abcd123，允许所有IP连接
DROP USER IF EXISTS 'test1'@'%';
CREATE USER 'test1'@'%' IDENTIFIED BY 'Abcd123!@#$';

-- 创建test2用户，密码为Abcd123，允许所有IP连接
DROP USER IF EXISTS 'test2'@'%';
CREATE USER 'test2'@'%' IDENTIFIED BY 'Abcd123!@#$';

-- 赋予test1和test2用户所有数据库所有表的权限
GRANT ALL PRIVILEGES ON *.* TO 'test1'@'%';
GRANT ALL PRIVILEGES ON *.* TO 'test2'@'%';

-- 允许查看用户、权限等系统表的权限的信息
GRANT SELECT ON mysql.* TO 'test1'@'%';

-- 刷新权限
FLUSH PRIVILEGES;

-- 赋予test1用户指定表的权限
GRANT ALL PRIVILEGES ON case_01_integers TO 'test1'@'%';
GRANT ALL PRIVILEGES ON case_02_boolean TO 'test1'@'%';
GRANT ALL PRIVILEGES ON case_03_floats TO 'test1'@'%';
GRANT ALL PRIVILEGES ON case_04_mb3_suffix TO 'test1'@'%';

-- 赋予test2用户指定表的权限
GRANT ALL PRIVILEGES ON case_01_integers TO 'test2'@'%';
GRANT ALL PRIVILEGES ON case_02_boolean TO 'test2'@'%';

GRANT SELECT, INSERT, UPDATE, DELETE, CREATE, ALTER, INDEX, DROP, CREATE VIEW, SHOW VIEW ON *.* TO 'test1'@'%';
GRANT SELECT, INSERT, UPDATE, DELETE, CREATE, ALTER, INDEX, DROP, CREATE VIEW, SHOW VIEW ON *.* TO 'test2'@'%';

GRANT ALL PRIVILEGES ON *.* TO 'test1'@'%' WITH GRANT OPTION;
GRANT ALL PRIVILEGES ON *.* TO 'test2'@'%' WITH GRANT OPTION;

GRANT RELOAD, PROCESS, LOCK TABLES, REPLICATION CLIENT, REPLICATION SLAVE ON *.* TO 'test1'@'%'; 
GRANT RELOAD, PROCESS, LOCK TABLES, REPLICATION CLIENT, REPLICATION SLAVE ON *.* TO 'test2'@'%';

-- 刷新权限
FLUSH PRIVILEGES;

-- 查看test1用户的权限
SELECT 
    User, 
    Host, 
    Db, 
    Table_name, 
    Table_priv 
FROM mysql.tables_priv 
WHERE User = 'test1' and Db = DATABASE();

-- 查看test2用户的权限
SELECT 
    User, 
    Host, 
    Db, 
    Table_name, 
    Table_priv 
FROM mysql.tables_priv 
WHERE User = 'test2' and Db = DATABASE();


GRANT SELECT ON case_01_integers TO 'test1'@'%';
GRANT SELECT ON case_02_boolean TO 'test1'@'%';
GRANT SELECT ON case_03_floats TO 'test1'@'%';

GRANT SELECT ON case_01_integers TO 'test2'@'%';
GRANT SELECT ON case_02_boolean TO 'test2'@'%';
GRANT SELECT ON case_03_floats TO 'test2'@'%';
FLUSH PRIVILEGES;


