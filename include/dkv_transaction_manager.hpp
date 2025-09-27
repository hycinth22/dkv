#ifndef DKV_TRANSACTION_MANAGER_HPP
#define DKV_TRANSACTION_MANAGER_HPP

#include "dkv_core.hpp"
#include "dkv_transaction.hpp"
#include <vector>
#include <unordered_map>
#include <mutex>
#include <atomic>

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

    // 提交事务
    bool commit(TransactionID transaction_id);

    // 回滚事务
    void rollback(TransactionID transaction_id);
    
    // 检查事务是否活跃
    bool isActive(TransactionID transaction_id) const;

    // 获取活跃事务列表
    std::vector<TransactionID> getActiveTransactions() const;

    TransactionID peekNextTransactionID() const {
        return transaction_id_generator_.load();
    }
private:
    StorageEngine* storage_engine_;              // 存储引擎指针

    const TransactionIsolationLevel isolation_level_;
    std::atomic<TransactionID> transaction_id_generator_{1};
    
    mutable std::mutex active_transactions_mutex_;
    std::unordered_map<TransactionID, Transaction> active_transactions_;

    TransactionID nextTransactionId() {
        return transaction_id_generator_.fetch_add(1);
    }

    std::shared_ptr<DataItem> saveOldValue(const std::string& key);
};

} // namespace dkv

#endif // DKV_TRANSACTION_MANAGER_HPP