#include "uc_api.hpp"
#include "uc_utils.hpp"

#include "storage/uc_catalog.hpp"
#include "storage/uc_table_set.hpp"
#include "storage/uc_transaction.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/parser/constraints/not_null_constraint.hpp"
#include "duckdb/parser/constraints/unique_constraint.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"
#include "duckdb/catalog/dependency_list.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/parser/constraints/list.hpp"
#include "storage/uc_schema_entry.hpp"
#include "duckdb/parser/parser.hpp"

namespace duckdb {

UCTableSet::UCTableSet(UCSchemaEntry &schema) : UCInSchemaSet(schema) {
}

static ColumnDefinition CreateColumnDefinition(ClientContext &context, UCAPIColumnDefinition &coldef) {
    return {coldef.name, UCUtils::TypeToLogicalType(context, coldef.type_text)};
}

void UCTableSet::LoadEntries(ClientContext &context) {
	auto &transaction = UCTransaction::Get(context, catalog);

    auto &uc_catalog = catalog.Cast<UCCatalog>();

    // TODO: handle out-of-order columns using position property
    auto tables = UCAPI::GetTables(catalog.GetDBPath(), schema.name, uc_catalog.credentials);

    for (auto &table : tables) {
        D_ASSERT(schema.name == table.schema_name);
        CreateTableInfo info;
        for (auto &col : table.columns) {
            info.columns.AddColumn(CreateColumnDefinition(context, col));
        }

        info.table = table.name;
        auto table_entry = make_uniq<UCTableEntry>(catalog, schema, info);
        table_entry->table_data = make_uniq<UCAPITable>(table);

        CreateEntry(std::move(table_entry));
    }
}

optional_ptr<CatalogEntry> UCTableSet::RefreshTable(ClientContext &context, const string &table_name) {
	auto table_info = GetTableInfo(context, schema, table_name);
	auto table_entry = make_uniq<UCTableEntry>(catalog, schema, *table_info);
	auto table_ptr = table_entry.get();
	CreateEntry(std::move(table_entry));
	return table_ptr;
}

unique_ptr<UCTableInfo> UCTableSet::GetTableInfo(ClientContext &context, UCSchemaEntry &schema,
                                            const string &table_name) {
    throw NotImplementedException("UCTableSet::CreateTable");
}

optional_ptr<CatalogEntry> UCTableSet::CreateTable(ClientContext &context, BoundCreateTableInfo &info) {
    throw NotImplementedException("UCTableSet::CreateTable");
}

void UCTableSet::AlterTable(ClientContext &context, RenameTableInfo &info) {
    throw NotImplementedException("UCTableSet::AlterTable");
}

void UCTableSet::AlterTable(ClientContext &context, RenameColumnInfo &info) {
    throw NotImplementedException("UCTableSet::AlterTable");
}

void UCTableSet::AlterTable(ClientContext &context, AddColumnInfo &info) {
    throw NotImplementedException("UCTableSet::AlterTable");
}

void UCTableSet::AlterTable(ClientContext &context, RemoveColumnInfo &info) {
    throw NotImplementedException("UCTableSet::AlterTable");
}

void UCTableSet::AlterTable(ClientContext &context, AlterTableInfo &alter) {
    throw NotImplementedException("UCTableSet::AlterTable");
}

} // namespace duckdb
