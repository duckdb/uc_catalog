#include "uc_api.hpp"
#include "storage/uc_catalog.hpp"
#include "yyjson.hpp"
#include <curl/curl.h>


namespace duckdb {

static size_t GetRequestWriteCallback(void *contents, size_t size, size_t nmemb, void *userp)
{
    ((std::string*)userp)->append((char*)contents, size * nmemb);
    return size * nmemb;
}

static string GetRequest(const string& url, const string& token = ""){
    CURL *curl;
    CURLcode res;
    string readBuffer;

    curl = curl_easy_init();
    if(curl) {
        curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
        curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, GetRequestWriteCallback);
        curl_easy_setopt(curl, CURLOPT_WRITEDATA, &readBuffer);
        if (!token.empty()) {
            curl_easy_setopt(curl, CURLOPT_XOAUTH2_BEARER, token.c_str());
            curl_easy_setopt(curl, CURLOPT_HTTPAUTH, CURLAUTH_BEARER);
        }
        res = curl_easy_perform(curl);
        curl_easy_cleanup(curl);

        if (res != CURLcode::CURLE_OK) {
            string error = curl_easy_strerror(res);
            throw IOException("Curl Request to '%s' failed with error: '%s'", url, error);
        }
        return readBuffer;
    }
    throw InternalException("Failed to initialize curl");
}

//# list catalogs
//    echo "List of catalogs"
//    curl --request GET "https://${DATABRICKS_HOST}/api/2.1/unity-catalog/catalogs" \
// 	--header "Authorization: Bearer ${TOKEN}" | jq .
//
//# list short version of all tables
//    echo "Table Summaries"
//    curl --request GET "https://${DATABRICKS_HOST}/api/2.1/unity-catalog/table-summaries?catalog_name=workspace" \
// 	--header "Authorization: Bearer ${TOKEN}" | jq .
//
//# list tables in `default` schema
//    echo "Tables in default schema"
//    curl --request GET "https://${DATABRICKS_HOST}/api/2.1/unity-catalog/tables?catalog_name=workspace&schema_name=default" \
// 	--header "Authorization: Bearer ${TOKEN}" | jq .


vector<string> UCAPI::GetCatalogs(const string &catalog, UCCredentials credentials) {
    throw NotImplementedException("UCAPI::GetCatalogs");
}

static UCAPIColumnDefinition ParseColumnDefinition(duckdb_yyjson::yyjson_val *column_def) {
    UCAPIColumnDefinition result;

    auto *name_json = yyjson_obj_get(column_def, "name");
    if (name_json) {
        result.name = yyjson_get_str(name_json);
    } else {
        throw IOException("Failed to parse name from column");
    }

    auto *type_text_json = yyjson_obj_get(column_def, "type_text");
    if (type_text_json) {
        result.type_text = yyjson_get_str(type_text_json);
    } else {
        throw IOException("Failed to parse type_text from column");
    }

    auto *type_precision = yyjson_obj_get(column_def, "type_precision");
    if (type_precision) {
        result.precision = yyjson_get_int(type_text_json);
    } else {
        throw IOException("Failed to parse type_precision from column");
    }

    auto *type_scale = yyjson_obj_get(column_def, "type_scale");
    if (type_scale) {
        result.scale = yyjson_get_int(type_scale);
    } else {
        throw IOException("Failed to parse type_scale from column");
    }

    auto *position = yyjson_obj_get(column_def, "position");
    if (position) {
        result.position = yyjson_get_int(type_scale);
    } else {
        throw IOException("Failed to parse position from column");
    }

    return result;
}

vector<UCAPITable> UCAPI::GetTables(const string &catalog, const string &schema, UCCredentials credentials) {
    vector<UCAPITable> result;
    auto api_result = GetRequest(credentials.endpoint + "/api/2.1/unity-catalog/tables?catalog_name=" + catalog + "&schema_name=" + schema, credentials.token);

    // Read JSON and get root
    duckdb_yyjson::yyjson_doc *doc = duckdb_yyjson::yyjson_read(api_result.c_str(), api_result.size(), 0);
    duckdb_yyjson::yyjson_val *root = yyjson_doc_get_root(doc);

    // Get root["hits"], iterate over the array
    auto *hits = yyjson_obj_get(root, "tables");
    size_t idx, max;
    duckdb_yyjson::yyjson_val *hit;
    yyjson_arr_foreach(hits, idx, max, hit) {
        UCAPITable table_result;
        table_result.catalog_name = catalog;
        table_result.schema_name = schema;

        auto *name = yyjson_obj_get(hit, "name");
        if (name) {
            table_result.table_name = yyjson_get_str(name);
        }

        auto *storage_location = yyjson_obj_get(hit, "storage_location");
        if (storage_location) {
            table_result.delta_path = yyjson_get_str(storage_location);
        }

        auto *columns = yyjson_obj_get(hit, "columns");
        duckdb_yyjson::yyjson_val *col;
        size_t col_idx, col_max;
        yyjson_arr_foreach(columns, col_idx, col_max, col) {
            auto column_definition = ParseColumnDefinition(col);
            table_result.columns.push_back(column_definition);
        }

        // TODO: remove override
        table_result.columns = {
            {
                "id",
                "bigint"
            },
            {
                "value",
                "string"
            }
        };

        result.push_back(table_result);
    }

    return result;
}

vector<UCAPISchema> UCAPI::GetSchemas(const string &catalog, UCCredentials credentials) {
    // TODO query API
    return {
        {
            "default",
            "workspace"
        },
        {
            "information_schema",
            "workspace"
        }
    };
}

} // namespace duckdb
