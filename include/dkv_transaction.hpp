#ifndef DKV_TRANSACTION_HPP
#define DKV_TRANSACTION_HPP

#include "dkv_core.hpp"
#include <string>
#include <memory>
#include <unordered_map>

namespace dkv {

using TransactionID = uint64_t;

struct TransactionRecordVersion {
    std::string key;
    DataItem* item; // Safety: Dont access version if storage is destroyed or transaction is rolled back and purged.
};

class Transaction {
public:
    Transaction(TransactionID transaction_id);
    ~Transaction();
    void push_version(const std::string& key, DataItem* item);
    const std::vector<TransactionRecordVersion>& get_versions() const;

private:
    TransactionID transaction_id_;               // 事务ID，用于MVCC
    Timestamp start_timestamp_;                   // 事务开始时间戳
    std::vector<TransactionRecordVersion> versions_; // 本事务更新的所有数据项版本。
};

} // namespace dkv

#endif // DKV_TRANSACTION_HPP