-- 1. 创建数据库用户 test1
create user test1 with password 'abcd123!@#$';

-- 2. 创建数据库 test_db
create database test_db owner test1;

-- 3. 给用户授权（确保能正常连接、使用）
grant all privileges on database test_db to test1;

-- 4. 给用户赋予管理员的权限
alter user test1 with superuser ;

-- 5. 进入 test_db 后，授权 schema 权限（必须，否则无法建表）
\c test_db
grant all on schema public to test1;