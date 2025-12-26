#pragma once

#include "../dkv_core.hpp"
#include "../dkv_datatypes.hpp"
#include "../transaction/dkv_mvcc.hpp"
#include "../transaction/dkv_transaction_manager.hpp"
#include <memory>
#include <shared_mutex>
#include <vector>
#include <unordered_map>

namespace dkv {

// InnerStorage类
class InnerStorage {
private:
    // 数据存储，用于存储实际数据项
    std::unordered_map<Key, std::unique_ptr<DataItem>> data_;
    mutable std::shared_mutex mutex_;

    // MVCC管理器，用于处理多版本并发控制
    MVCC mvcc_;
public:
    InnerStorage() : mvcc_(*this) {}
    ~InnerStorage() = default;
    
    // 禁止拷贝和移动
    InnerStorage(const InnerStorage&) = delete;
    InnerStorage& operator=(const InnerStorage&) = delete;
    InnerStorage(InnerStorage&&) = delete;
    InnerStorage& operator=(InnerStorage&&) = delete;

    // 获取数据项
    DataItem* get(const Key& key) const;
    DataItem* get(const Key& key, const ReadView& read_view) const;
    bool set(TransactionID tx_id, const Key& key, std::unique_ptr<DataItem> item);
    bool del(TransactionID tx_id, const Key& key);
    bool exists(const Key& key) const;
    bool exists(const Key& key, const ReadView& read_view) const;
    // 获取数据项引用，不支持事务
    std::unique_ptr<DataItem>& getRefOrInsert(const Key& key);

    // 容器操作，不支持事务
    void clear();
    size_t size() const;
    std::vector<Key> getAllKeys() const;

    // 迭代器相关操作，不支持事务
    std::unordered_map<Key, std::unique_ptr<DataItem>>::const_iterator find(const Key& key) const;
    std::unordered_map<Key, std::unique_ptr<DataItem>>::const_iterator begin() const;
    std::unordered_map<Key, std::unique_ptr<DataItem>>::const_iterator end() const;
    std::unique_ptr<DataItem>& operator[](const Key& key);
    std::unordered_map<Key, std::unique_ptr<DataItem>>::iterator erase(std::unordered_map<Key, std::unique_ptr<DataItem>>::iterator it);
    std::unordered_map<Key, std::unique_ptr<DataItem>>::iterator erase(std::unordered_map<Key, std::unique_ptr<DataItem>>::const_iterator it);
    std::pair<std::unordered_map<Key, std::unique_ptr<DataItem>>::iterator, bool> insert_or_assign(const Key& key, std::unique_ptr<DataItem> item);

    // 锁操作方法
    std::unique_lock<std::shared_mutex> wlock() const;
    std::shared_lock<std::shared_mutex> rlock() const;
    std::unique_lock<std::shared_mutex> wlock_deferred() const;
    std::shared_lock<std::shared_mutex> rlock_deferred() const;
    std::shared_mutex& getMutex() const;
};

} // namespace dkv