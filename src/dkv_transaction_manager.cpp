#include "dkv_transaction_manager.hpp"
#include "dkv_logger.hpp"
#include <algorithm>
#include <map>
using namespace std;

namespace dkv {

TransactionManager::TransactionManager(StorageEngine* storage_engine, TransactionIsolationLevel isolation_level)
    : storage_engine_(storage_engine), isolation_level_(isolation_level) {
}

TransactionManager::~TransactionManager() {
}

TransactionID TransactionManager::begin() {
    TransactionID txid = nextTransactionId();
    lock_guard<mutex> lock(active_transactions_mutex_);
    Transaction new_tx(txid);
    active_transactions_.insert({txid, new_tx});
    return txid;
}

bool TransactionManager::commit(TransactionID txid) {
    lock_guard<mutex> lock(active_transactions_mutex_);
    if (active_transactions_.find(txid) == active_transactions_.end()) {
        DKV_LOG_ERROR("Transaction {} not found", txid);
        return false;
    }
    Transaction& tx = active_transactions_.at(txid);
    tx.deactivate();
    active_transactions_.erase(txid);
    return true;
}

void TransactionManager::rollback(TransactionID txid) {
    lock_guard<mutex> lock(active_transactions_mutex_);
    if (active_transactions_.find(txid) == active_transactions_.end()) {
        DKV_LOG_ERROR("Transaction {} not found", txid);
        return;
    }
    Transaction& tx = active_transactions_.at(txid);
    // todo: rollback operations
    tx.deactivate();
    active_transactions_.erase(txid);
}

bool TransactionManager::isActive(TransactionID txid) const {
    lock_guard<mutex> lock(active_transactions_mutex_);
    return active_transactions_.find(txid) != active_transactions_.end();
}

vector<TransactionID> TransactionManager::getActiveTransactions() const {
    lock_guard<mutex> lock(active_transactions_mutex_);
    vector<TransactionID> active_txids;
    active_txids.reserve(active_transactions_.size());
    for (const auto& pair : active_transactions_) {
        active_txids.push_back(pair.first);
    }
    return active_txids;
}


} // namespace dkv