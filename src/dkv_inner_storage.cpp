#include "dkv_inner_storage.h"
#include "dkv_datatypes.hpp"
#include <algorithm>
#include <mutex>
#include <cassert>

namespace dkv {

// 获取数据项
DataItem* InnerStorage::get(const Key& key) const {
    auto it = data_.find(key);
    return it != data_.end() ? it->second.get() : nullptr;
}

DataItem* InnerStorage::get(const Key& key, const ReadView& read_view) const {
    // 使用MVCC获取可见版本
    return mvcc_.get(read_view, key);
}

bool InnerStorage::set(TransactionID tx_id, const Key& key, std::unique_ptr<DataItem> item) {
    if (tx_id == NO_TX) {
        // 非事务操作，直接存储
        data_[key] = std::move(item);
        return true;
    }
    // 事务操作，使用MVCC
    return mvcc_.set(tx_id, key, std::move(item));
}

bool InnerStorage::del(TransactionID tx_id, const Key& key) {
    if (tx_id == NO_TX) {
        // 非事务操作，直接删除
        return data_.erase(key) > 0;
    }
    // 事务操作，使用MVCC
    return mvcc_.del(tx_id, key);
}

bool InnerStorage::exists(const Key& key) const {
    auto it = data_.find(key);
    return it != data_.end() && !it->second->isDeleted();
}

bool InnerStorage::exists(const Key& key, const ReadView& read_view) const {
    // 事务操作，使用MVCC
    auto item = mvcc_.get(read_view, key);
    return item != nullptr && !item->isDeleted();
}

std::unique_ptr<DataItem>& InnerStorage::getRefOrInsert(const Key& key) {
    return data_[key];
}

// 容器操作
void InnerStorage::clear() {
    data_.clear();
}

size_t InnerStorage::size() const {
    return data_.size();
}

std::vector<Key> InnerStorage::getAllKeys() const {
    std::vector<Key> keys;
    keys.reserve(data_.size());
    for (const auto& pair : data_) {
        keys.push_back(pair.first);
    }
    return keys;
}

// 迭代器相关操作
std::unordered_map<Key, std::unique_ptr<DataItem>>::const_iterator InnerStorage::find(const Key& key) const {
    return data_.find(key);
}

std::unordered_map<Key, std::unique_ptr<DataItem>>::const_iterator InnerStorage::begin() const {
    return data_.begin();
}

std::unordered_map<Key, std::unique_ptr<DataItem>>::const_iterator InnerStorage::end() const {
    return data_.end();
}

std::unique_ptr<DataItem>& InnerStorage::operator[](const Key& key) {
    return data_[key];
}

std::unordered_map<Key, std::unique_ptr<DataItem>>::iterator InnerStorage::erase(std::unordered_map<Key, std::unique_ptr<DataItem>>::iterator it) {
    return data_.erase(it);
}

std::unordered_map<Key, std::unique_ptr<DataItem>>::iterator InnerStorage::erase(std::unordered_map<Key, std::unique_ptr<DataItem>>::const_iterator it) {
    return data_.erase(it);
}

std::pair<std::unordered_map<Key, std::unique_ptr<DataItem>>::iterator, bool> InnerStorage::insert_or_assign(const Key& key, std::unique_ptr<DataItem> item) {
    return data_.insert_or_assign(key, std::move(item));
}

// 锁操作方法
std::unique_lock<std::shared_mutex> InnerStorage::wlock() const {
    return std::unique_lock<std::shared_mutex>(mutex_);
}

std::shared_lock<std::shared_mutex> InnerStorage::rlock() const {
    return std::shared_lock<std::shared_mutex>(mutex_);
}

std::unique_lock<std::shared_mutex> InnerStorage::wlock_deferred() const {
    return std::unique_lock<std::shared_mutex>(mutex_, std::defer_lock);
}

std::shared_lock<std::shared_mutex> InnerStorage::rlock_deferred() const {
    return std::shared_lock<std::shared_mutex>(mutex_, std::defer_lock);
}

std::shared_mutex& InnerStorage::getMutex() const {
    return mutex_;
}

} // namespace dkv