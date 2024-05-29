# This file is included by DuckDB's build system. It specifies which extension to load

# Extension from this repo
duckdb_extension_load(uc_catalog
    SOURCE_DIR ${CMAKE_CURRENT_LIST_DIR}
    LOAD_TESTS
)

# Any extra extensions that should be built
duckdb_extension_load(httpfs)
duckdb_extension_load(json)
duckdb_extension_load(delta
    SOURCE_DIR /Users/sam/Development/delta-kernel-testing
)