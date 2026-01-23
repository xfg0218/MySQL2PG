-- 查看test1用户对case_01_integers表的权限
SELECT
    grantee,
    table_schema,
    table_name,
    privilege_type
FROM
    information_schema.table_privileges
WHERE grantee = 'test1';

-- 查看test2用户对case_01_integers表的权限
SELECT
    grantee,
    table_schema,
    table_name,
    privilege_type
FROM
    information_schema.table_privileges
WHERE grantee = 'test2';
