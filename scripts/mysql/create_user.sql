-- 创建test1用户，密码为Abcd123，允许所有IP连接
DROP USER IF EXISTS 'test1'@'%';
CREATE USER 'test1'@'%' IDENTIFIED BY 'Abcd123';

-- 创建test2用户，密码为Abcd123，允许所有IP连接
DROP USER IF EXISTS 'test2'@'%';
CREATE USER 'test2'@'%' IDENTIFIED BY 'Abcd123';

-- 赋予test1用户所有数据库所有表的权限
GRANT ALL PRIVILEGES ON *.* TO 'test1'@'%';

-- 赋予test2用户所有数据库所有表的权限
GRANT ALL PRIVILEGES ON *.* TO 'test2'@'%';

-- 刷新权限
FLUSH PRIVILEGES;