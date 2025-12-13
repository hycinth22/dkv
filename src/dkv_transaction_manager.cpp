#include "dkv_transaction_manager.hpp"
#include "dkv_logger.hpp"
#include "dkv_datatype_base.hpp"
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
    // generate a new transaction id
    TransactionID txid = nextTransactionId();
    // activate the transaction
    lock_guard<mutex> lock(active_transactions_mutex_);
    Transaction new_tx(txid, createReadView(txid));
    active_transactions_.insert({txid, new_tx});
    return txid;
}

bool TransactionManager::commit(TransactionID txid) {
    lock_guard<mutex> lock(active_transactions_mutex_);
    // deactivate the transaction
    if (active_transactions_.find(txid) == active_transactions_.end()) {
        DKV_LOG_ERROR("Transaction {} not found", txid);
        return false;
    }
    active_transactions_.erase(txid);
    return true;
}

bool TransactionManager::rollback(TransactionID txid) {
    lock_guard<mutex> lock(active_transactions_mutex_);
    if (active_transactions_.find(txid) == active_transactions_.end()) {
        DKV_LOG_ERROR("Transaction {} not found", txid);
        return false;
    }
    // do rollback operations
    Transaction& tx = active_transactions_.at(txid);
    for (const auto& version : tx.get_versions()) {
        version.item->setDiscard(); // will be purged later
    }
    // deactivate the transaction and push to rollback transactions
    rollback_transactions_.insert({txid, move(tx)});
    active_transactions_.erase(txid);
    return true;
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

bool TransactionManager::isRolledback(TransactionID txid) const {
    lock_guard<mutex> lock(rollback_transactions_mutex_);
    return rollback_transactions_.find(txid) != rollback_transactions_.end();
}

vector<TransactionID> TransactionManager::getRolledbackTransactions() const {
    lock_guard<mutex> lock(rollback_transactions_mutex_);
    vector<TransactionID> rollback_txids;
    rollback_txids.reserve(rollback_transactions_.size());
    for (const auto& pair : rollback_transactions_) {
        rollback_txids.push_back(pair.first);
    }
    return rollback_txids;
}

const Transaction& TransactionManager::getTransaction(TransactionID transaction_id) const {
    return getTransactionImpl(*this, transaction_id);
}

Transaction& TransactionManager::getTransactionMut(TransactionID transaction_id) {
    return getTransactionImpl(*this, transaction_id);
}

ReadView TransactionManager::getReadView(TransactionID transaction_id) const {
    if (isolation_level_ == TransactionIsolationLevel::READ_COMMITTED) {
        return createReadView(transaction_id);
    } else if (isolation_level_ == TransactionIsolationLevel::READ_UNCOMMITTED) {
        return getTransaction(transaction_id).get_read_view();
    }
    exit(1);
}

ReadView TransactionManager::createReadView(TransactionID transaction_id) const {
    ReadView read_view;
    read_view.creator = transaction_id;
    const auto& actives = getActiveTransactions();
    const auto& rolledback = getRolledbackTransactions();
    read_view.actives.reserve(actives.size() + rolledback.size());
    read_view.actives.insert(actives.end(), actives.begin(), actives.end());
    read_view.actives.insert(actives.end(), rolledback.begin(), rolledback.end());
    auto pmin = min_element(actives.begin(), actives.end());
    read_view.low = (pmin != read_view.actives.end() ? *pmin : 0);
    read_view.high = peekNextTransactionID();
    return read_view;
}


} // namespace dkv