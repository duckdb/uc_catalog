#define DUCKDB_EXTENSION_MAIN

#include "uc_catalog_extension.hpp"
#include "storage/uc_catalog.hpp"
#include "storage/uc_transaction_manager.hpp"


#include "duckdb.hpp"
#include "duckdb/main/secret/secret_manager.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/extension_util.hpp"
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>
#include "duckdb/parser/parsed_data/attach_info.hpp"
#include "duckdb/storage/storage_extension.hpp"

namespace duckdb {

static unique_ptr<BaseSecret> CreateUCSecretFunction(ClientContext &, CreateSecretInput &input) {
    // apply any overridden settings
    vector<string> prefix_paths;
    auto result = make_uniq<KeyValueSecret>(prefix_paths, "uc", "config", input.name);
    for (const auto &named_param : input.options) {
        auto lower_name = StringUtil::Lower(named_param.first);

        if (lower_name == "token") {
            result->secret_map["token"] = named_param.second.ToString();
        } else if (lower_name == "endpoint") {
            result->secret_map["endpoint"] = named_param.second.ToString();
        } else if (lower_name == "aws_region") {
            result->secret_map["aws_region"] = named_param.second.ToString();
        } else {
            throw InternalException("Unknown named parameter passed to CreateUCSecretFunction: " + lower_name);
        }
    }

    //! Set redact keys
    result->redact_keys = {"token"};

    return std::move(result);
}

static void SetUCSecretParameters(CreateSecretFunction &function) {
    function.named_parameters["token"] = LogicalType::VARCHAR;
    function.named_parameters["endpoint"] = LogicalType::VARCHAR;
    function.named_parameters["aws_region"] = LogicalType::VARCHAR;
}

unique_ptr<SecretEntry> GetSecret(ClientContext &context, const string &secret_name) {
    auto &secret_manager = SecretManager::Get(context);
    auto transaction = CatalogTransaction::GetSystemCatalogTransaction(context);
    // FIXME: this should be adjusted once the `GetSecretByName` API supports this
    // use case
    auto secret_entry = secret_manager.GetSecretByName(transaction, secret_name, "memory");
    if (secret_entry) {
        return secret_entry;
    }
    secret_entry = secret_manager.GetSecretByName(transaction, secret_name, "local_file");
    if (secret_entry) {
        return secret_entry;
    }
    return nullptr;
}

static unique_ptr<Catalog> UCCatalogAttach(StorageExtensionInfo *storage_info,
                                           ClientContext &context,
                                           AttachedDatabase &db,
                                           const string &name, AttachInfo &info,
                                           AccessMode access_mode) {
    UCCredentials credentials;

    // check if we have a secret provided
    string secret_name;
    for (auto &entry : info.options) {
        auto lower_name = StringUtil::Lower(entry.first);
        if (lower_name == "type" || lower_name == "read_only") {
            // already handled
        } else if (lower_name == "secret") {
            secret_name = entry.second.ToString();
        } else {
            throw BinderException("Unrecognized option for UC attach: %s", entry.first);
        }
    }

    // if no secret is specified we default to the unnamed mysql secret, if it
    // exists
    bool explicit_secret = !secret_name.empty();
    if (!explicit_secret) {
        // look up settings from the default unnamed mysql secret if none is
        // provided
        secret_name = "__default_uc";
    }

    string connection_string = info.path;
    auto secret_entry = GetSecret(context, secret_name);
    if (secret_entry) {
        // secret found - read data
        const auto &kv_secret = dynamic_cast<const KeyValueSecret &>(*secret_entry->secret);
        string new_connection_info;

        Value input_val = kv_secret.TryGetValue("token");
        credentials.token = input_val.IsNull() ? "" : input_val.ToString();

        Value endpoint_val = kv_secret.TryGetValue("endpoint");
        credentials.endpoint = endpoint_val.IsNull() ? "" : endpoint_val.ToString();
        StringUtil::RTrim(credentials.endpoint, "/");

        Value aws_region_val = kv_secret.TryGetValue("aws_region");
        credentials.aws_region = endpoint_val.IsNull() ? "" : aws_region_val.ToString();

    } else if (explicit_secret) {
        // secret not found and one was explicitly provided - throw an error
        throw BinderException("Secret with name \"%s\" not found", secret_name);
    }

    return make_uniq<UCCatalog>(db, info.path, access_mode, credentials);
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
    SecretType secret_type;
    secret_type.name = "uc";
    secret_type.deserializer = KeyValueSecret::Deserialize<KeyValueSecret>;
    secret_type.default_provider = "config";

    ExtensionUtil::RegisterSecretType(instance, secret_type);

    CreateSecretFunction mysql_secret_function = {"uc", "config", CreateUCSecretFunction};
    SetUCSecretParameters(mysql_secret_function);
    ExtensionUtil::RegisterFunction(instance, mysql_secret_function);

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
