#include "storage/uc_catalog.hpp"
#include "storage/uc_schema_entry.hpp"
#include "storage/uc_table_entry.hpp"
#include "storage/uc_transaction.hpp"
#include "duckdb/storage/statistics/base_statistics.hpp"
#include "duckdb/storage/table_storage_info.hpp"
#include "duckdb/main/extension_util.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/catalog/catalog_entry/table_function_catalog_entry.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"
#include "uc_api.hpp"

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
    auto &db = DatabaseInstance::GetDatabase(context);
    auto &delta_function_set = ExtensionUtil::GetTableFunction(db, "delta_scan");
    auto delta_scan_function = delta_function_set.functions.GetFunctionByArguments(context, {LogicalType::VARCHAR});

    D_ASSERT(table_data);

    // TODO: remove override
    vector<Value> inputs = {"s3://test-bucket-ceiveran/delta_testing/simple_sf1_with_dv/delta_lake"};

    named_parameter_map_t param_map;
    vector<LogicalType> return_types;
    vector<string> names;
    TableFunctionRef empty_ref;

    // TODO: mind the types and names here!
    TableFunctionBindInput bind_input(inputs, param_map, return_types, names, nullptr, nullptr, delta_scan_function, empty_ref);

    auto result = delta_scan_function.bind(context, bind_input, return_types, names);
    bind_data = std::move(result);

    return delta_scan_function;
}

TableStorageInfo UCTableEntry::GetStorageInfo(ClientContext &context) {
	auto &transaction = Transaction::Get(context, catalog).Cast<UCTransaction>();
	TableStorageInfo result;
	result.cardinality = 0;
	return result;
}

} // namespace duckdb
