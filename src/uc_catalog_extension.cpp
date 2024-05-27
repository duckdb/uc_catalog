#define DUCKDB_EXTENSION_MAIN

#include "uc_catalog_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/extension_util.hpp"
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>

// OpenSSL linked through vcpkg
#include <openssl/opensslv.h>

namespace duckdb {

inline void UcCatalogScalarFun(DataChunk &args, ExpressionState &state, Vector &result) {
    auto &name_vector = args.data[0];
    UnaryExecutor::Execute<string_t, string_t>(
	    name_vector, result, args.size(),
	    [&](string_t name) {
			return StringVector::AddString(result, "UcCatalog "+name.GetString()+" 🐥");;
        });
}

inline void UcCatalogOpenSSLVersionScalarFun(DataChunk &args, ExpressionState &state, Vector &result) {
    auto &name_vector = args.data[0];
    UnaryExecutor::Execute<string_t, string_t>(
	    name_vector, result, args.size(),
	    [&](string_t name) {
			return StringVector::AddString(result, "UcCatalog " + name.GetString() +
                                                     ", my linked OpenSSL version is " +
                                                     OPENSSL_VERSION_TEXT );;
        });
}

static void LoadInternal(DatabaseInstance &instance) {
    // Register a scalar function
    auto uc_catalog_scalar_function = ScalarFunction("uc_catalog", {LogicalType::VARCHAR}, LogicalType::VARCHAR, UcCatalogScalarFun);
    ExtensionUtil::RegisterFunction(instance, uc_catalog_scalar_function);

    // Register another scalar function
    auto uc_catalog_openssl_version_scalar_function = ScalarFunction("uc_catalog_openssl_version", {LogicalType::VARCHAR},
                                                LogicalType::VARCHAR, UcCatalogOpenSSLVersionScalarFun);
    ExtensionUtil::RegisterFunction(instance, uc_catalog_openssl_version_scalar_function);
}

void UcCatalogExtension::Load(DuckDB &db) {
	LoadInternal(*db.instance);
}
std::string UcCatalogExtension::Name() {
	return "uc_catalog";
}

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
