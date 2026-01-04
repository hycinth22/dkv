#pragma once

#include "dkv_inner_storage.hpp"

namespace dkv {

// StorageBucket类
// 封装InnerStorage，提供bucket功能
class StorageBucket {
private:
    InnerStorage inner_storage_;
public:
    StorageBucket() : inner_storage_() {}
    ~StorageBucket() = default;
    
    // 禁止拷贝和移动
    StorageBucket(const StorageBucket&) = delete;
    StorageBucket& operator=(const StorageBucket&) = delete;
    StorageBucket(StorageBucket&&) = delete;
    StorageBucket& operator=(StorageBucket&&) = delete;
    
    // 数据操作方法，委托给inner_storage_
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
    
    // 获取内部存储的引用
    InnerStorage& getInnerStorage();
    const InnerStorage& getInnerStorage() const;
};

} // namespace dkv
