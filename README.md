# Unity Catalog Extension
Warning: this extension is an experimental, proof-of-concept for an extension, feel free to try it out, but no guarantees are given whatsoever.
This extension could be renamed, moved or removed at any point.

This is a proof-of-concept extension demonstrating DuckDB connecting to the Unity Catalog to scan Delta Table using 
the [delta extension](https://github.com/duckdb/duckdb_delta).

You can try it out using DuckDB (>= v1.0.0) on the platforms: `linux_amd64`, `linux_amd64_gcc4`, `osx_arm64` and `osx_arm64` by running:

```SQL
INSTALL uc_catalog from core_nightly;
INSTALL delta from core;
LOAD delta;
LOAD uc_catalog;
CREATE SECRET (
	TYPE UC,
	TOKEN '${UC_TOKEN}',
	ENDPOINT '${UC_ENDPOINT}',
	AWS_REGION '${UC_AWS_REGION}'
)
ATTACH 'test_catalog' AS test_catalog (TYPE UC_CATALOG)
SHOW ALL TABLES;
SELECT * FROM test_catalog.test_schema.test_table;
```