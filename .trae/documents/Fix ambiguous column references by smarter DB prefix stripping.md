I will fix the ambiguous column reference error in `view_case06_collates` (and potentially others) by refining how database prefixes are stripped in `sync_viewddl.go`.

The current implementation blindly strips the first part of any `"part1"."part2"` pattern, incorrectly treating table aliases (e.g., `"c"."c1"`) as database names and stripping them (resulting in `"c1"`), causing ambiguity.

I will implement a smarter `cleanDatabasePrefixes` function that:
1.  Counts the occurrences of all identifiers in the DDL.
2.  Identifies prefix candidates (`"prefix"."suffix"`).
3.  Only strips a `prefix` if it **only** appears as a prefix in the DDL (implying it's likely a database name).
4.  Preserves the `prefix` if it appears elsewhere (implying it's a table alias or table name used in the query).

This heuristic correctly distinguishes between `db.table` (where `db` is usually not referenced elsewhere) and `alias.col` (where `alias` is defined in the `FROM` clause).

Files to modify:
- `internal/converter/postgres/sync_viewddl.go`