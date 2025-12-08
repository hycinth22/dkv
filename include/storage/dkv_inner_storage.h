#pragma once

#include "dkv_core.hpp"
#include "dkv_datatypes.hpp"
#include <memory>
#include <shared_mutex>
#include <vector>

namespace dkv {

// 存储接口基类，用于支持不同的存储实现（如SimpleStorage、MVCCStorage等）
class IInnerStorage {
public:
    IInnerStorage() = default;
    virtual ~IInnerStorage() = default;

    // 禁止拷贝和移动
    IInnerStorage(const IInnerStorage&) = delete;
    IInnerStorage& operator=(const IInnerStorage&) = delete;
    IInnerStorage(IInnerStorage&&) = delete;
    IInnerStorage& operator=(IInnerStorage&&) = delete;

    // 获取数据项
    virtual DataItem* get(const Key& key) const = 0;
    virtual bool set(const Key& key, std::unique_ptr<DataItem> item) = 0;
    virtual bool del(const Key& key) = 0;
    virtual bool exists(const Key& key) = 0;
    virtual std::unique_ptr<DataItem>& getRefOrInsert(const Key& key) = 0;
    // 容器操作
    virtual void clear() = 0;
    virtual size_t size() const = 0;
    virtual std::vector<Key> getAllKeys() const = 0;

    // 迭代器相关操作
    virtual std::unordered_map<Key, std::unique_ptr<DataItem>>::const_iterator find(const Key& key) const = 0;
    virtual std::unordered_map<Key, std::unique_ptr<DataItem>>::const_iterator begin() const = 0;
    virtual std::unordered_map<Key, std::unique_ptr<DataItem>>::const_iterator end() const = 0;
    virtual std::unique_ptr<DataItem>& operator[](const Key& key) = 0;
    virtual std::unordered_map<Key, std::unique_ptr<DataItem>>::iterator erase(std::unordered_map<Key, std::unique_ptr<DataItem>>::iterator it) = 0;
    virtual std::unordered_map<Key, std::unique_ptr<DataItem>>::iterator erase(std::unordered_map<Key, std::unique_ptr<DataItem>>::const_iterator it) = 0;
    virtual std::pair<std::unordered_map<Key, std::unique_ptr<DataItem>>::iterator, bool> insert_or_assign(const Key& key, std::unique_ptr<DataItem> item) = 0;

    // 锁操作方法
    virtual std::unique_lock<std::shared_mutex> wlock() const = 0;
    virtual std::shared_lock<std::shared_mutex> rlock() const = 0;
    virtual std::unique_lock<std::shared_mutex> wlock_deferred() const = 0;
    virtual std::shared_lock<std::shared_mutex> rlock_deferred() const = 0;
    virtual std::shared_mutex& getMutex() const = 0;
};

} // namespace dkv
