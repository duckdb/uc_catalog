#include "uc_utils.hpp"
#include "storage/uc_schema_entry.hpp"
#include "storage/uc_transaction.hpp"

namespace duckdb {

string UCUtils::TypeToString(const LogicalType &input) {
	switch (input.id()) {
	case LogicalType::VARCHAR:
		return "TEXT";
	case LogicalType::UTINYINT:
		return "TINYINT UNSIGNED";
	case LogicalType::USMALLINT:
		return "SMALLINT UNSIGNED";
	case LogicalType::UINTEGER:
		return "INTEGER UNSIGNED";
	case LogicalType::UBIGINT:
		return "BIGINT UNSIGNED";
	case LogicalType::TIMESTAMP:
		return "DATETIME";
	case LogicalType::TIMESTAMP_TZ:
		return "TIMESTAMP";
	default:
		return input.ToString();
	}
}

LogicalType UCUtils::TypeToLogicalType(ClientContext &context, const UCAPIColumnDefinition &column_definition) {
	if (column_definition.type_text == "tinyint") {
			return LogicalType::TINYINT;
	} else if (column_definition.type_text == "smallint") {
        return LogicalType::SMALLINT;
	} else if (column_definition.type_text == "bigint") {
        return LogicalType::BIGINT;
	} else if (column_definition.type_text == "int") {
        return LogicalType::INTEGER;
	} else if (column_definition.type_text == "long") {
        return LogicalType::BIGINT;
	} else if (column_definition.type_text == "string") {
        return LogicalType::VARCHAR;
	}  else if (column_definition.type_text == "timestamp") {
        return LogicalType::TIMESTAMP;
    }

    throw NotImplementedException("Tried to fallback to unknown type for '%s'", column_definition.type_text);
	// fallback for unknown types
	return LogicalType::VARCHAR;
}

LogicalType UCUtils::ToUCType(const LogicalType &input) {
    //todo do we need this mapping?
    throw NotImplementedException("ToUCType not yet implemented");
	switch (input.id()) {
	case LogicalTypeId::BOOLEAN:
	case LogicalTypeId::SMALLINT:
	case LogicalTypeId::INTEGER:
	case LogicalTypeId::BIGINT:
	case LogicalTypeId::TINYINT:
	case LogicalTypeId::UTINYINT:
	case LogicalTypeId::USMALLINT:
	case LogicalTypeId::UINTEGER:
	case LogicalTypeId::UBIGINT:
	case LogicalTypeId::FLOAT:
	case LogicalTypeId::DOUBLE:
	case LogicalTypeId::BLOB:
	case LogicalTypeId::DATE:
	case LogicalTypeId::DECIMAL:
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIMESTAMP_TZ:
	case LogicalTypeId::VARCHAR:
		return input;
	case LogicalTypeId::LIST:
		throw NotImplementedException("UC does not support arrays - unsupported type \"%s\"", input.ToString());
	case LogicalTypeId::STRUCT:
	case LogicalTypeId::MAP:
	case LogicalTypeId::UNION:
		throw NotImplementedException("UC does not support composite types - unsupported type \"%s\"",
		                              input.ToString());
	case LogicalTypeId::TIMESTAMP_SEC:
	case LogicalTypeId::TIMESTAMP_MS:
	case LogicalTypeId::TIMESTAMP_NS:
		return LogicalType::TIMESTAMP;
	case LogicalTypeId::HUGEINT:
		return LogicalType::DOUBLE;
	default:
		return LogicalType::VARCHAR;
	}
}

} // namespace duckdb
