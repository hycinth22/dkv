#pragma once

#include "dkv_core.hpp"
#include "dkv_datatype_base.hpp"
#include "dkv_transaction.hpp"
#include "dkv_transaction_manager.hpp"
#include <vector>
#include <unordered_map>
#include <memory>
#include <atomic>

namespace dkv {
class StorageEngine;
class IInnerStorage;

struct ReadView {
    TransactionID creator;
    TransactionID low, high;
    std::vector<TransactionID> actives;
    bool isVisible(TransactionID tx_id);
};

// MVCC类，提供多版本并发控制
class MVCC {
private:
    IInnerStorage& inner_storage_; // 内部存储引用
    std::unordered_map<Key, UndoLog> undo_log_;
public:
    // 构造函数，接收IInnerStorage作为参数
    explicit MVCC(IInnerStorage& inner_storage)
        : inner_storage_(inner_storage) {}

    // 析构函数
    ~MVCC() = default;

    // 禁止拷贝和移动
    MVCC(const MVCC&) = delete;
    MVCC& operator=(const MVCC&) = delete;
    MVCC(MVCC&&) = delete;
    MVCC& operator=(MVCC&&) = delete;

    // 获取指定事务可见的版本
    DataItem* get(ReadView& read_view, const Key& key) const;

    // 设置键值，并记录到UNDOLOG
    bool set(TransactionID tx_id, const Key& key, std::unique_ptr<DataItem> item);

    // 删除键，并记录到UNDOLOG
    bool del(TransactionID tx_id, const Key& key);

    // 创建ReadView，用于实现可重复读隔离级别
    ReadView createReadView(TransactionID tx_id, TransactionManager& txm);
};

} // namespace dkv

#include <iostream>
#include <sstream>
inline std::ostream& operator<<(std::ostream& os, const dkv::ReadView& read_view) {
    os << "ReadView{creator=" << read_view.creator
       << ", low=" << read_view.low
       << ", high=" << read_view.high
       << ", actives=[";
    for (auto tx_id : read_view.actives) {
        os << tx_id << " ";
    }
    os << "]";
    return os;
}

inline std::stringstream& operator<<(std::stringstream& os, const dkv::ReadView& read_view) {
    os << "ReadView{creator=" << read_view.creator
       << ", low=" << read_view.low
       << ", high=" << read_view.high
       << ", actives=[";
    for (auto tx_id : read_view.actives) {
        os << tx_id << " ";
    }
    os << "]";
    return os;
}
