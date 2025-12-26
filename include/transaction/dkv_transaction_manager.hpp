#ifndef DKV_TRANSACTION_MANAGER_HPP
#define DKV_TRANSACTION_MANAGER_HPP

#include "../dkv_core.hpp"
#include "dkv_transaction.hpp"
#include "dkv_mvcc.hpp"
#include <vector>
#include <unordered_map>
#include <mutex>
#include <atomic>
#include <algorithm>
namespace dkv {

class StorageEngine;
// 事务类，负责事务的创建、执行和回滚
class TransactionManager {
public:
    TransactionManager(StorageEngine* storage_engine, TransactionIsolationLevel isolation_level);
    ~TransactionManager();

    // 获取事务隔离等级
    TransactionIsolationLevel getIsolationLevel() const {
        return isolation_level_;
    }

    // 开始事务
    TransactionID begin();

    // 提交/回滚事务
    bool commit(TransactionID transaction_id);
    bool rollback(TransactionID transaction_id);
    
    const Transaction& getTransaction(TransactionID transaction_id) const;
    Transaction& getTransactionMut(TransactionID transaction_id);

    // 查询事务是否活跃
    bool isActive(TransactionID transaction_id) const;
    std::vector<TransactionID> getActiveTransactions() const;
    // 查询事务是否已回滚
    bool isRolledback(TransactionID transaction_id) const;
    std::vector<TransactionID> getRolledbackTransactions() const;

    TransactionID peekNextTransactionID() const {
        return transaction_id_generator_.load();
    }

    // 获取事务的读取视图
    ReadView getReadView(TransactionID transaction_id) const;

    ReadView createReadView(TransactionID transaction_id) const;
private:
    StorageEngine* storage_engine_;              // 存储引擎指针

    const TransactionIsolationLevel isolation_level_;
    std::atomic<TransactionID> transaction_id_generator_{1};
    
    mutable std::mutex active_transactions_mutex_, rollback_transactions_mutex_;
    std::unordered_map<TransactionID, Transaction> active_transactions_, rollback_transactions_;

    TransactionID nextTransactionId() {
        return transaction_id_generator_.fetch_add(1);
    }

    template<typename Self>
    static auto& getTransactionImpl(Self& self, TransactionID transaction_id) {
        std::lock_guard<std::mutex> lock(self.active_transactions_mutex_);
        return self.active_transactions_.at(transaction_id);
    }
};

} // namespace dkv

#endif // DKV_TRANSACTION_MANAGER_HPP