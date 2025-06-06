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

optional_ptr<SchemaCatalogEntry> UCCatalog::LookupSchema(CatalogTransaction transaction,
									 const EntryLookupInfo &schema_lookup,
									 OnEntryNotFound if_not_found) {
	if (schema_lookup.GetEntryName() == DEFAULT_SCHEMA) {
		if (default_schema.empty()) {
			throw InvalidInputException("Attempting to fetch the default schema - but no database was "
						    "provided in the connection string");
		}
		return GetSchema(transaction, default_schema, if_not_found);
	}
	auto entry = schemas.GetEntry(transaction.GetContext(), schema_lookup.GetEntryName());
	if (!entry && if_not_found != OnEntryNotFound::RETURN_NULL) {
		throw BinderException("Schema with name \"%s\" not found", schema_lookup.GetEntryName());
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

PhysicalOperator &UCCatalog::PlanCreateTableAs(ClientContext &context, PhysicalPlanGenerator &planner,
			    LogicalCreateTable &op, PhysicalOperator &plan) {
	throw NotImplementedException("UCCatalog PlanInsert");
}

PhysicalOperator &UCCatalog::PlanInsert(ClientContext &context, PhysicalPlanGenerator &planner, LogicalInsert &op,
	     optional_ptr<PhysicalOperator> plan) {
		throw NotImplementedException("UCCatalog PlanCreateTableAs");
}

PhysicalOperator &UCCatalog::PlanDelete(ClientContext &context, PhysicalPlanGenerator &planner, LogicalDelete &op,
	     PhysicalOperator &plan) {
	throw NotImplementedException("UCCatalog PlanDelete");
}

PhysicalOperator &UCCatalog::PlanDelete(ClientContext &context, PhysicalPlanGenerator &planner, LogicalDelete &op) {
	throw NotImplementedException("UCCatalog PlanDelete");
}

PhysicalOperator &UCCatalog::PlanUpdate(ClientContext &context, PhysicalPlanGenerator &planner, LogicalUpdate &op,
				     PhysicalOperator &plan) {
	throw NotImplementedException("UCCatalog PlanUpdate");
}

unique_ptr<LogicalOperator> UCCatalog::BindCreateIndex(Binder &binder, CreateStatement &stmt, TableCatalogEntry &table,
                                                       unique_ptr<LogicalOperator> plan) {
	throw NotImplementedException("UCCatalog BindCreateIndex");
}

} // namespace duckdb
