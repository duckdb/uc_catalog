#include "storage/uc_catalog.hpp"
#include "storage/uc_schema_entry.hpp"
#include "storage/uc_table_entry.hpp"
#include "storage/uc_transaction.hpp"
#include "duckdb/storage/statistics/base_statistics.hpp"
#include "duckdb/storage/table_storage_info.hpp"

namespace duckdb {

UCTableEntry::UCTableEntry(Catalog &catalog, SchemaCatalogEntry &schema, CreateTableInfo &info)
    : TableCatalogEntry(catalog, schema, info) {
	this->internal = false;
}

UCTableEntry::UCTableEntry(Catalog &catalog, SchemaCatalogEntry &schema, UCTableInfo &info)
    : TableCatalogEntry(catalog, schema, *info.create_info) {
	this->internal = false;
}

unique_ptr<BaseStatistics> UCTableEntry::GetStatistics(ClientContext &context, column_t column_id) {
	return nullptr;
}

void UCTableEntry::BindUpdateConstraints(Binder &binder, LogicalGet &, LogicalProjection &, LogicalUpdate &, ClientContext &) {
    throw NotImplementedException("BindUpdateConstraints");
}

TableFunction UCTableEntry::GetScanFunction(ClientContext &context, unique_ptr<FunctionData> &bind_data) {
    // TODO look up the delta scan function
    throw NotImplementedException("UCTableEntry::GetScanFunction");

//	auto result = make_uniq<UCBindData>(*this);
//	for (auto &col : columns.Logical()) {
//		result->types.push_back(col.GetType());
//		result->names.push_back(col.GetName());
//	}
//
//	bind_data = std::move(result);
//
//	auto function = UCScanFunction();
//	Value filter_pushdown;
//	if (context.TryGetCurrentSetting("uc_experimental_filter_pushdown", filter_pushdown)) {
//		function.filter_pushdown = BooleanValue::Get(filter_pushdown);
//	}
//	return function;
}

TableStorageInfo UCTableEntry::GetStorageInfo(ClientContext &context) {
	auto &transaction = Transaction::Get(context, catalog).Cast<UCTransaction>();
	TableStorageInfo result;
	result.cardinality = 0;
	return result;
}

} // namespace duckdb
