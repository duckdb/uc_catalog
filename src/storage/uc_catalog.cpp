#include "storage/uc_catalog.hpp"
#include "storage/uc_schema_entry.hpp"
#include "storage/uc_transaction.hpp"
#include "duckdb/storage/database_size.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "duckdb/main/attached_database.hpp"

namespace duckdb {

UCCatalog::UCCatalog(AttachedDatabase &db_p, const string &internal_name, AccessMode access_mode,
                     UCCredentials credentials)
    : Catalog(db_p), internal_name(internal_name), access_mode(access_mode), credentials(std::move(credentials)),
      schemas(*this) {
}

UCCatalog::~UCCatalog() = default;

void UCCatalog::Initialize(bool load_builtin) {
}

optional_ptr<CatalogEntry> UCCatalog::CreateSchema(CatalogTransaction transaction, CreateSchemaInfo &info) {
	if (info.on_conflict == OnCreateConflict::REPLACE_ON_CONFLICT) {
		DropInfo try_drop;
		try_drop.type = CatalogType::SCHEMA_ENTRY;
		try_drop.name = info.schema;
		try_drop.if_not_found = OnEntryNotFound::RETURN_NULL;
		try_drop.cascade = false;
		schemas.DropEntry(transaction.GetContext(), try_drop);
	}
	return schemas.CreateSchema(transaction.GetContext(), info);
}

void UCCatalog::DropSchema(ClientContext &context, DropInfo &info) {
	return schemas.DropEntry(context, info);
}

void UCCatalog::ScanSchemas(ClientContext &context, std::function<void(SchemaCatalogEntry &)> callback) {
	schemas.Scan(context, [&](CatalogEntry &schema) { callback(schema.Cast<UCSchemaEntry>()); });
}

optional_ptr<SchemaCatalogEntry> UCCatalog::GetSchema(CatalogTransaction transaction, const string &schema_name,
                                                      OnEntryNotFound if_not_found, QueryErrorContext error_context) {
	if (schema_name == DEFAULT_SCHEMA) {
		if (default_schema.empty()) {
			throw InvalidInputException("Attempting to fetch the default schema - but no database was "
			                            "provided in the connection string");
		}
		return GetSchema(transaction, default_schema, if_not_found, error_context);
	}
	auto entry = schemas.GetEntry(transaction.GetContext(), schema_name);
	if (!entry && if_not_found != OnEntryNotFound::RETURN_NULL) {
		throw BinderException("Schema with name \"%s\" not found", schema_name);
	}
	return reinterpret_cast<SchemaCatalogEntry *>(entry.get());
}

bool UCCatalog::InMemory() {
	return false;
}

string UCCatalog::GetDBPath() {
	return internal_name;
}

DatabaseSize UCCatalog::GetDatabaseSize(ClientContext &context) {
	if (default_schema.empty()) {
		throw InvalidInputException("Attempting to fetch the database size - but no database was provided "
		                            "in the connection string");
	}
	DatabaseSize size;
	return size;
}

void UCCatalog::ClearCache() {
	schemas.ClearEntries();
}

unique_ptr<PhysicalOperator> UCCatalog::PlanInsert(ClientContext &context, LogicalInsert &op,
                                                   unique_ptr<PhysicalOperator> plan) {
	throw NotImplementedException("UCCatalog PlanInsert");
}
unique_ptr<PhysicalOperator> UCCatalog::PlanCreateTableAs(ClientContext &context, LogicalCreateTable &op,
                                                          unique_ptr<PhysicalOperator> plan) {
	throw NotImplementedException("UCCatalog PlanCreateTableAs");
}
unique_ptr<PhysicalOperator> UCCatalog::PlanDelete(ClientContext &context, LogicalDelete &op,
                                                   unique_ptr<PhysicalOperator> plan) {
	throw NotImplementedException("UCCatalog PlanDelete");
}
unique_ptr<PhysicalOperator> UCCatalog::PlanUpdate(ClientContext &context, LogicalUpdate &op,
                                                   unique_ptr<PhysicalOperator> plan) {
	throw NotImplementedException("UCCatalog PlanUpdate");
}
unique_ptr<LogicalOperator> UCCatalog::BindCreateIndex(Binder &binder, CreateStatement &stmt, TableCatalogEntry &table,
                                                       unique_ptr<LogicalOperator> plan) {
	throw NotImplementedException("UCCatalog BindCreateIndex");
}

} // namespace duckdb
