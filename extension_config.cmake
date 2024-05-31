# This file is included by DuckDB's build system. It specifies which extension to load

# Extension from this repo
duckdb_extension_load(uc_catalog
    SOURCE_DIR ${CMAKE_CURRENT_LIST_DIR}
    LOAD_TESTS
)

# TODO enable this to test with delta
#duckdb_extension_load(httpfs)
#duckdb_extension_load(json)
#duckdb_extension_load(delta
#        GIT_URL https://github.com/duckdb/duckdb_delta
#        GIT_TAG 90f9557fe6d37e99160438ec1ba7b54c5d21dd4e
#)