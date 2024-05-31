#include "storage/uc_catalog.hpp"
#include "storage/uc_schema_entry.hpp"
#include "storage/uc_table_entry.hpp"
#include "storage/uc_transaction.hpp"
#include "duckdb/storage/statistics/base_statistics.hpp"
#include "duckdb/storage/table_storage_info.hpp"
#include "duckdb/main/extension_util.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/secret/secret_manager.hpp"
#include "duckdb/catalog/catalog_entry/table_function_catalog_entry.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"
#include "uc_api.hpp"
#include "../../duckdb/third_party/catch/catch.hpp"

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
	auto &uc_catalog = catalog.Cast<UCCatalog>();
	auto &secret_manager = SecretManager::Get(context);

    D_ASSERT(table_data);

	if (table_data->data_source_format != "DELTA") {
		throw NotImplementedException("Table '%s' is of unsupported format '%s', ", table_data->name, table_data->data_source_format);
	}

	// Set the S3 path as input to table function
    vector<Value> inputs = {table_data->storage_location};

	// Get Credentials from UCAPI
	auto table_credentials = UCAPI::GetTableCredentials(table_data->table_id, uc_catalog.credentials);

	// Inject secret into secret manager scoped to this path
	CreateSecretInfo info(OnCreateConflict::REPLACE_ON_CONFLICT, SecretPersistType::TEMPORARY);
	info.name = "__internal_uc_" + table_data->table_id;
	info.type = "s3";
	info.provider = "config";
	info.options = {
		{"key_id", table_credentials.key_id},
		{"secret", table_credentials.secret},
		{"session_token", table_credentials.session_token},
		{"region", uc_catalog.credentials.aws_region},
	};
	info.scope = {table_data->storage_location};
	secret_manager.CreateSecret(context, info);

    named_parameter_map_t param_map;
    vector<LogicalType> return_types;
    vector<string> names;
    TableFunctionRef empty_ref;

    TableFunctionBindInput bind_input(inputs, param_map, return_types, names, nullptr, nullptr, delta_scan_function, empty_ref);

    auto result = delta_scan_function.bind(context, bind_input, return_types, names);
    bind_data = std::move(result);

    return delta_scan_function;
}

TableStorageInfo UCTableEntry::GetStorageInfo(ClientContext &context) {
	TableStorageInfo result;
	// TODO fill info
	return result;
}

} // namespace duckdb
