//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/uc_schema_set.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "storage/uc_catalog_set.hpp"
#include "storage/uc_schema_entry.hpp"

namespace duckdb {
struct CreateSchemaInfo;

class UCSchemaSet : public UCCatalogSet {
public:
	explicit UCSchemaSet(Catalog &catalog);

public:
	optional_ptr<CatalogEntry> CreateSchema(ClientContext &context, CreateSchemaInfo &info);

protected:
	void LoadEntries(ClientContext &context) override;
};

} // namespace duckdb
