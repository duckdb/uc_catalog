#include "storage/uc_schema_set.hpp"
#include "storage/uc_transaction.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"

namespace duckdb {

UCSchemaSet::UCSchemaSet(Catalog &catalog) : UCCatalogSet(catalog) {
}

void UCSchemaSet::LoadEntries(ClientContext &context) {
    // TODO: replace with HTTPS call to UC api
    vector<string> dummy_schemas {"schema1", "schema2"};

    auto &transaction = UCTransaction::Get(context, catalog);

    for (const auto& schema_name: dummy_schemas) {
        CreateSchemaInfo info;
        info.schema = schema_name;
        info.internal = false;
        auto schema = make_uniq<UCSchemaEntry>(catalog, info);
        CreateEntry(std::move(schema));
    }
}

optional_ptr<CatalogEntry> UCSchemaSet::CreateSchema(ClientContext &context, CreateSchemaInfo &info) {
	throw NotImplementedException("Schema creation");
}

} // namespace duckdb
