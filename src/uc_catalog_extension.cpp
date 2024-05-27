#define DUCKDB_EXTENSION_MAIN

#include "uc_catalog_extension.hpp"
#include "storage/uc_catalog.hpp"
#include "storage/uc_transaction_manager.hpp"


#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/extension_util.hpp"
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>
#include "duckdb/parser/parsed_data/attach_info.hpp"
#include "duckdb/storage/storage_extension.hpp"

namespace duckdb {

static unique_ptr<Catalog> UCCatalogAttach(StorageExtensionInfo *storage_info,
                                           ClientContext &context,
                                           AttachedDatabase &db,
                                           const string &name, AttachInfo &info,
                                           AccessMode access_mode) {
    return make_uniq<UCCatalog>(db, info.path, access_mode);
}

static unique_ptr<TransactionManager> CreateTransactionManager(StorageExtensionInfo *storage_info,
                                                                    AttachedDatabase &db, Catalog &catalog) {
    auto &uc_catalog = catalog.Cast<UCCatalog>();
    return make_uniq<UCTransactionManager>(db, uc_catalog);
}

class UCCatalogStorageExtension : public StorageExtension {
public:
      UCCatalogStorageExtension() {
        attach = UCCatalogAttach;
        create_transaction_manager = CreateTransactionManager;
      }
};

static void LoadInternal(DatabaseInstance &instance) {
    auto &config = DBConfig::GetConfig(instance);
    config.storage_extensions["uc_catalog"] = make_uniq<UCCatalogStorageExtension>();
}

void UcCatalogExtension::Load(DuckDB &db) { LoadInternal(*db.instance); }
std::string UcCatalogExtension::Name() { return "uc_catalog"; }

std::string UcCatalogExtension::Version() const {
#ifdef EXT_VERSION_UC_CATALOG
  return EXT_VERSION_UC_CATALOG;
#else
  return "";
#endif
}

} // namespace duckdb

extern "C" {

DUCKDB_EXTENSION_API void uc_catalog_init(duckdb::DatabaseInstance &db) {
  duckdb::DuckDB db_wrapper(db);
  db_wrapper.LoadExtension<duckdb::UcCatalogExtension>();
}

DUCKDB_EXTENSION_API const char *uc_catalog_version() {
  return duckdb::DuckDB::LibraryVersion();
}
}

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif
