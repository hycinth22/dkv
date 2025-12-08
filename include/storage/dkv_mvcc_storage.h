#pragma once

#include "dkv_inner_storage.h"
#include "dkv_simple_storage.h"
#include "dkv_mvcc.hpp"
#include <unordered_map>
#include <shared_mutex>

namespace dkv {

// MVCC存储实现，支持多版本并发控制
class MVCCInnerStorage : public IInnerStorage {
private:
    // 基础数据存储，用于存储实际数据项
    SimpleInnerStorage data_;

    // MVCC管理器，用于处理多版本并发控制
    MVCC mvcc_;
public:
    MVCCInnerStorage();
    ~MVCCInnerStorage() override = default;

    // 获取数据项
    DataItem* get(const Key& key) const override;
    bool set(const Key& key, std::unique_ptr<DataItem> item) override;
    bool del(const Key& key) override;
    bool exists(const Key& key) override;
    std::unique_ptr<DataItem>& getRefOrInsert(const Key& key) override;

    // 容器操作
    void clear() override;
    size_t size() const override;
    std::vector<Key> getAllKeys() const override;

    // 迭代器相关操作
    std::unordered_map<Key, std::unique_ptr<DataItem>>::const_iterator find(const Key& key) const override;
    std::unordered_map<Key, std::unique_ptr<DataItem>>::const_iterator begin() const override;
    std::unordered_map<Key, std::unique_ptr<DataItem>>::const_iterator end() const override;
    std::unique_ptr<DataItem>& operator[](const Key& key) override;
    std::unordered_map<Key, std::unique_ptr<DataItem>>::iterator erase(std::unordered_map<Key, std::unique_ptr<DataItem>>::iterator it) override;
    std::unordered_map<Key, std::unique_ptr<DataItem>>::iterator erase(std::unordered_map<Key, std::unique_ptr<DataItem>>::const_iterator it) override;
    std::pair<std::unordered_map<Key, std::unique_ptr<DataItem>>::iterator, bool> insert_or_assign(const Key& key, std::unique_ptr<DataItem> item) override;

    // 锁操作方法
    std::unique_lock<std::shared_mutex> wlock() const override;
    std::shared_lock<std::shared_mutex> rlock() const override;
    std::unique_lock<std::shared_mutex> wlock_deferred() const override;
    std::shared_lock<std::shared_mutex> rlock_deferred() const override;
    std::shared_mutex& getMutex() const override;
};

} // namespace dkv
