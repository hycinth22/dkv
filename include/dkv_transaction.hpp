#ifndef DKV_TRANSACTION_HPP
#define DKV_TRANSACTION_HPP

#include "dkv_core.hpp"
#include <string>
#include <memory>
#include <unordered_map>

namespace dkv {

// // 事务操作类，代表单个事务中的操作
// struct TransactionOperation {
//     Command command;            // 命令
//     bool is_readonly;           // 是否为只读命令
//     std::shared_ptr<DataType> old_value; // 操作前的值，用于回滚
//     std::string key;            // 操作的键
// };

using TransactionID = uint64_t;

class Transaction {
public:
    Transaction(TransactionID transaction_id);
    ~Transaction();
    
    // 检查事务是否活跃
    bool isActive(TransactionID transaction_id) const;
    void deactivate();
    // std::shared_ptr<DataItem> snapshotValue(const std::string& key);

private:
    bool active_;                                  // 事务是否活跃
    TransactionID transaction_id_;               // 事务ID，用于MVCC
    Timestamp start_timestamp_;                   // 事务开始时间戳
    std::unordered_map<std::string, std::shared_ptr<DataItem>> snapshot_versions_;
};

} // namespace dkv

#endif // DKV_TRANSACTION_HPP