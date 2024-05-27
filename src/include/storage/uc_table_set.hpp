//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/uc_table_set.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "storage/uc_catalog_set.hpp"
#include "storage/uc_table_entry.hpp"

namespace duckdb {
struct CreateTableInfo;
class UCResult;
class UCSchemaEntry;

class UCTableSet : public UCInSchemaSet {
public:
	explicit UCTableSet(UCSchemaEntry &schema);

public:
	optional_ptr<CatalogEntry> CreateTable(ClientContext &context, BoundCreateTableInfo &info);

	static unique_ptr<UCTableInfo> GetTableInfo(ClientContext &context, UCSchemaEntry &schema,
	                                               const string &table_name);
	optional_ptr<CatalogEntry> RefreshTable(ClientContext &context, const string &table_name);

	void AlterTable(ClientContext &context, AlterTableInfo &info);

protected:
	void LoadEntries(ClientContext &context) override;

	void AlterTable(ClientContext &context, RenameTableInfo &info);
	void AlterTable(ClientContext &context, RenameColumnInfo &info);
	void AlterTable(ClientContext &context, AddColumnInfo &info);
	void AlterTable(ClientContext &context, RemoveColumnInfo &info);

	static void AddColumn(ClientContext &context, UCResult &result, UCTableInfo &table_info,
	                      idx_t column_offset = 0);
};

} // namespace duckdb
