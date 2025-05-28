#include "storage/uc_transaction_manager.hpp"
#include "duckdb/main/attached_database.hpp"

namespace duckdb {

UCTransactionManager::UCTransactionManager(AttachedDatabase &db_p, UCCatalog &uc_catalog)
    : TransactionManager(db_p), uc_catalog(uc_catalog) {
}

Transaction &UCTransactionManager::StartTransaction(ClientContext &context) {
	auto transaction = make_uniq<UCTransaction>(uc_catalog, *this, context);
	transaction->Start();
	auto &result = *transaction;
	lock_guard<mutex> l(transaction_lock);
	transactions[result] = std::move(transaction);
	return result;
}

ErrorData UCTransactionManager::CommitTransaction(ClientContext &context, Transaction &transaction) {
	auto &uc_transaction = transaction.Cast<UCTransaction>();
	uc_transaction.Commit();
	lock_guard<mutex> l(transaction_lock);
	transactions.erase(transaction);
	return ErrorData();
}

void UCTransactionManager::RollbackTransaction(Transaction &transaction) {
	auto &uc_transaction = transaction.Cast<UCTransaction>();
	uc_transaction.Rollback();
	lock_guard<mutex> l(transaction_lock);
	transactions.erase(transaction);
}

void UCTransactionManager::Checkpoint(ClientContext &context, bool force) {
	// auto &transaction = UCTransaction::Get(context, db.GetCatalog());
	//	auto &db = transaction.GetConnection();
	//	db.Execute("CHECKPOINT");
}

} // namespace duckdb
