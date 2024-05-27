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

void UCTableSet::AddColumn(ClientContext &context, UCResult &result, UCTableInfo &table_info,
                              idx_t column_offset) {
    throw NotImplementedException("UCTableSet::AddColumn");

//	UCTypeData type_info;
//	idx_t column_index = column_offset;
//	auto column_name = result.GetString(column_index);
//	type_info.type_name = result.GetString(column_index + 1);
//	type_info.column_type = result.GetString(column_index + 2);
//	string default_value;
//	auto is_nullable = result.GetString(column_index + 4);
//	type_info.precision = result.IsNull(column_index + 5) ? -1 : result.GetInt64(column_index + 5);
//	type_info.scale = result.IsNull(column_index + 6) ? -1 : result.GetInt64(column_index + 6);
//
//	auto column_type = UCUtils::TypeToLogicalType(context, type_info);
//	ColumnDefinition column(std::move(column_name), std::move(column_type));
//	if (!default_value.empty()) {
//		auto expressions = Parser::ParseExpressionList(default_value);
//		if (expressions.empty()) {
//			throw InternalException("Expression list is empty");
//		}
//		column.SetDefaultValue(std::move(expressions[0]));
//	}
//	auto &create_info = *table_info.create_info;
//	if (is_nullable != "YES") {
//		auto column_idx = create_info.columns.LogicalColumnCount();
//		create_info.constraints.push_back(make_uniq<NotNullConstraint>(LogicalIndex(column_idx)));
//	}
//	create_info.columns.AddColumn(std::move(column));
}

void UCTableSet::LoadEntries(ClientContext &context) {
	auto &transaction = UCTransaction::Get(context, catalog);

    vector<string> dummy_tables {"table1", "table2"};

    for (auto &dummy_table : dummy_tables) {
        CreateTableInfo info;
        info.table = dummy_table;
        auto table_entry = make_uniq<UCTableEntry>(catalog, schema, info);
        CreateEntry(std::move(table_entry));
    }
}

string GetTableInfoQuery(const string &schema_name, const string &table_name) {
    throw NotImplementedException("UCTableSet::GetTableInfoQuery");

//	return StringUtil::Replace(StringUtil::Replace(R"(
//SELECT column_name, data_type, column_type, column_default, is_nullable, numeric_precision, numeric_scale
//FROM information_schema.columns
//WHERE table_schema=${SCHEMA_NAME} AND table_name=${TABLE_NAME}
//ORDER BY table_name, ordinal_position;
//)",
//	                                               "${SCHEMA_NAME}", UCUtils::WriteLiteral(schema_name)),
//	                           "${TABLE_NAME}", UCUtils::WriteLiteral(table_name));
//}
//
//unique_ptr<UCTableInfo> UCTableSet::GetTableInfo(ClientContext &context, UCSchemaEntry &schema,
//                                                       const string &table_name) {
//	auto &transaction = UCTransaction::Get(context, schema.ParentCatalog());
//	auto query = GetTableInfoQuery(schema.name, table_name);
//	auto result = transaction.Query(query);
//	auto table_info = make_uniq<UCTableInfo>(schema, table_name);
//	while (result->Next()) {
//		AddColumn(context, *result, *table_info, 0);
//	}
//	return table_info;
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
