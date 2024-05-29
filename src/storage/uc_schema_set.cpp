#include "storage/uc_schema_set.hpp"
#include "storage/uc_catalog.hpp"
#include "uc_api.hpp"
#include "storage/uc_transaction.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "duckdb/catalog/catalog.hpp"

namespace duckdb {

UCSchemaSet::UCSchemaSet(Catalog &catalog) : UCCatalogSet(catalog) {
}

void UCSchemaSet::LoadEntries(ClientContext &context) {

    auto &uc_catalog = catalog.Cast<UCCatalog>();
    auto tables = UCAPI::GetSchemas(catalog.GetName(), uc_catalog.credentials);

//    auto &transaction = UCTransaction::Get(context, catalog);

    for (const auto& schema: tables) {
//        D_ASSERT(schema.catalog_name == catalog.GetName());
        CreateSchemaInfo info;
        info.schema = schema.schema_name;
        info.internal = false; // TODO detect internal schemas?
        auto schema_entry = make_uniq<UCSchemaEntry>(catalog, info);
        schema_entry->schema_data = make_uniq<UCAPISchema>(schema);
        CreateEntry(std::move(schema_entry));
    }
}

optional_ptr<CatalogEntry> UCSchemaSet::CreateSchema(ClientContext &context, CreateSchemaInfo &info) {
	throw NotImplementedException("Schema creation");
}

} // namespace duckdb
