-- 查看指定表的列信息
SELECT
    c.relname,
    a.attname,
    d.description
FROM
    pg_class c
INNER JOIN
    pg_attribute a ON a.attrelid = c.oid
LEFT JOIN
    pg_description d ON d.objoid = c.oid AND d.objsubid = a.attnum
WHERE
    c.relkind = 'r'
    AND c.relname IN ('case_19_comments')
    AND a.attnum > 0
ORDER BY
    c.relname, a.attnum;


