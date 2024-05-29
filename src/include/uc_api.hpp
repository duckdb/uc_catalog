//===----------------------------------------------------------------------===//
//                         DuckDB
//
// src/include/uc_api.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types.hpp"

namespace duckdb {
struct UCCredentials;

struct UCAPIColumnDefinition {
    string name;
    string type_text;
    idx_t precision;
    idx_t scale;
    idx_t position;
};

struct UCAPITable {
    string table_name;
    string schema_name;
    string catalog_name;

    string table_type;

    string delta_path;
    string delta_last_commit_timestamp;
    string delta_last_update_version;

    vector<UCAPIColumnDefinition> columns;
};

struct UCAPISchema {
    string schema_name;
    string catalog_name;
};

class UCAPI {
public:
    static vector<string> GetCatalogs(const string &catalog, UCCredentials credentials);
    static vector<UCAPITable> GetTables(const string &catalog, const string &schema, UCCredentials credentials);
    static vector<UCAPISchema> GetSchemas(const string &catalog, UCCredentials credentials);
    static vector<UCAPITable> GetTablesInSchema(const string &catalog, const string &schema, UCCredentials credentials);
};
} // namespace duckdb
