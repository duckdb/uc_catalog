//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/uc_transaction.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/transaction/transaction.hpp"

namespace duckdb {
class UCCatalog;
class UCSchemaEntry;
class UCTableEntry;

enum class UCTransactionState { TRANSACTION_NOT_YET_STARTED, TRANSACTION_STARTED, TRANSACTION_FINISHED };

class UCTransaction : public Transaction {
public:
	UCTransaction(UCCatalog &uc_catalog, TransactionManager &manager, ClientContext &context);
	~UCTransaction() override;

	void Start();
	void Commit();
	void Rollback();

//	UCConnection &GetConnection();
//	unique_ptr<UCResult> Query(const string &query);
	static UCTransaction &Get(ClientContext &context, Catalog &catalog);
	AccessMode GetAccessMode() const {
		return access_mode;
	}

private:
//	UCConnection connection;
	UCTransactionState transaction_state;
	AccessMode access_mode;
};

} // namespace duckdb
