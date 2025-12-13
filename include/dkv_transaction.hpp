#ifndef DKV_TRANSACTION_HPP
#define DKV_TRANSACTION_HPP
#include "dkv_core.hpp"
#include <string>
#include <memory>
#include <unordered_map>

namespace dkv {

struct ReadView {
    TransactionID creator;
    TransactionID low, high;
    std::vector<TransactionID> actives;
    bool isVisible(TransactionID tx_id) const;
};

struct TransactionRecordVersion {
    std::string key;
    DataItem* item; // Safety: Dont access version if storage is destroyed or transaction is rolled back and purged.
};

class Transaction {
public:
    Transaction(TransactionID transaction_id, ReadView read_view);
    ~Transaction();
    void push_version(const std::string& key, DataItem* item);
    const std::vector<TransactionRecordVersion>& get_versions() const;
    void push_command(const Command& command);
    const std::vector<Command>& get_commands() const;
    const ReadView& get_read_view() const;

private:
    TransactionID transaction_id_;               // 事务ID，用于MVCC
    Timestamp start_timestamp_;                   // 事务开始时间戳
    ReadView read_view_; // 事务开始时的读取视图
    std::vector<TransactionRecordVersion> versions_; // 本事务更新的所有数据项版本。
    std::vector<Command> commands_; // 本事务执行的所有命令。
};

} // namespace dkv

#endif // DKV_TRANSACTION_HPP