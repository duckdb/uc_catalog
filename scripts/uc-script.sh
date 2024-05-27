#!/bin/bash

TOKEN=""
DATABRICKS_HOST="dbc-1bcdcee8-1250.staging.cloud.databricks.com"

# list catalogs
echo "List of catalogs"
curl --request GET "https://${DATABRICKS_HOST}/api/2.1/unity-catalog/catalogs" \
 	--header "Authorization: Bearer ${TOKEN}" | jq .

# list short version of all tables
echo "Table Summaries"
curl --request GET "https://${DATABRICKS_HOST}/api/2.1/unity-catalog/table-summaries?catalog_name=workspace" \
 	--header "Authorization: Bearer ${TOKEN}" | jq .

# list tables in `default` schema
echo "Tables in default schema"
curl --request GET "https://${DATABRICKS_HOST}/api/2.1/unity-catalog/tables?catalog_name=workspace&schema_name=default" \
 	--header "Authorization: Bearer ${TOKEN}" | jq .
