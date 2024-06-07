#include "storage/uc_catalog_set.hpp"
#include "storage/uc_transaction.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "storage/uc_schema_entry.hpp"

namespace duckdb {

UCCatalogSet::UCCatalogSet(Catalog &catalog) : catalog(catalog), is_loaded(false) {
}

optional_ptr<CatalogEntry> UCCatalogSet::GetEntry(ClientContext &context, const string &name) {
	if (!is_loaded) {
		is_loaded = true;
		LoadEntries(context);
	}
	lock_guard<mutex> l(entry_lock);
	auto entry = entries.find(name);
	if (entry == entries.end()) {
		return nullptr;
	}
	return entry->second.get();
}

void UCCatalogSet::DropEntry(ClientContext &context, DropInfo &info) {
	throw NotImplementedException("UCCatalogSet::DropEntry");
}

void UCCatalogSet::EraseEntryInternal(const string &name) {
	lock_guard<mutex> l(entry_lock);
	entries.erase(name);
}

void UCCatalogSet::Scan(ClientContext &context, const std::function<void(CatalogEntry &)> &callback) {
	if (!is_loaded) {
		is_loaded = true;
		LoadEntries(context);
	}
	lock_guard<mutex> l(entry_lock);
	for (auto &entry : entries) {
		callback(*entry.second);
	}
}

optional_ptr<CatalogEntry> UCCatalogSet::CreateEntry(unique_ptr<CatalogEntry> entry) {
	lock_guard<mutex> l(entry_lock);
	auto result = entry.get();
	if (result->name.empty()) {
		throw InternalException("UCCatalogSet::CreateEntry called with empty name");
	}
	entries.insert(make_pair(result->name, std::move(entry)));
	return result;
}

void UCCatalogSet::ClearEntries() {
	entries.clear();
	is_loaded = false;
}

UCInSchemaSet::UCInSchemaSet(UCSchemaEntry &schema) : UCCatalogSet(schema.ParentCatalog()), schema(schema) {
}

optional_ptr<CatalogEntry> UCInSchemaSet::CreateEntry(unique_ptr<CatalogEntry> entry) {
	if (!entry->internal) {
		entry->internal = schema.internal;
	}
	return UCCatalogSet::CreateEntry(std::move(entry));
}

} // namespace duckdb
