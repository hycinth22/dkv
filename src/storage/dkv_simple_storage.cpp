#include "storage/dkv_simple_storage.h"
#include "dkv_datatypes.hpp"
#include <algorithm>
#include <mutex>
#include <cassert>

namespace dkv {

// 获取数据项
DataItem* SimpleInnerStorage::get(const Key& key) const {
    auto it = data_.find(key);
    if (it != data_.end()) {
        return it->second.get();
    }
    return nullptr;
}

bool SimpleInnerStorage::set(const Key& key, std::unique_ptr<DataItem> item) {
    data_[key] = std::move(item);
    return true;
}

bool SimpleInnerStorage::del(const Key& key) {
    return data_.erase(key) > 0;
}

bool SimpleInnerStorage::exists(const Key& key) {
    return data_.find(key) != data_.end();
}

std::unique_ptr<DataItem>& SimpleInnerStorage::getRefOrInsert(const Key& key) {
    return data_[key];
}

// 容器操作
void SimpleInnerStorage::clear() {
    data_.clear();
}

size_t SimpleInnerStorage::size() const {
    return data_.size();
}

std::vector<Key> SimpleInnerStorage::getAllKeys() const {
    std::vector<Key> keys;
    keys.reserve(data_.size());
    for (const auto& pair : data_) {
        keys.push_back(pair.first);
    }
    return keys;
}

// 迭代器相关操作
std::unordered_map<Key, std::unique_ptr<DataItem>>::const_iterator SimpleInnerStorage::find(const Key& key) const {
    return data_.find(key);
}

std::unordered_map<Key, std::unique_ptr<DataItem>>::const_iterator SimpleInnerStorage::begin() const {
    return data_.begin();
}

std::unordered_map<Key, std::unique_ptr<DataItem>>::const_iterator SimpleInnerStorage::end() const {
    return data_.end();
}

std::unique_ptr<DataItem>& SimpleInnerStorage::operator[](const Key& key) {
    return data_[key];
}

std::unordered_map<Key, std::unique_ptr<DataItem>>::iterator SimpleInnerStorage::erase(std::unordered_map<Key, std::unique_ptr<DataItem>>::iterator it) {
    return data_.erase(it);
}

std::unordered_map<Key, std::unique_ptr<DataItem>>::iterator SimpleInnerStorage::erase(std::unordered_map<Key, std::unique_ptr<DataItem>>::const_iterator it) {
    return data_.erase(it);
}

std::pair<std::unordered_map<Key, std::unique_ptr<DataItem>>::iterator, bool> SimpleInnerStorage::insert_or_assign(const Key& key, std::unique_ptr<DataItem> item) {
    return data_.insert_or_assign(key, std::move(item));
}

// 锁操作方法
std::unique_lock<std::shared_mutex> SimpleInnerStorage::wlock() const {
    return std::unique_lock<std::shared_mutex>(mutex_);
}

std::shared_lock<std::shared_mutex> SimpleInnerStorage::rlock() const {
    return std::shared_lock<std::shared_mutex>(mutex_);
}

std::unique_lock<std::shared_mutex> SimpleInnerStorage::wlock_deferred() const {
    return std::unique_lock<std::shared_mutex>(mutex_, std::defer_lock);
}

std::shared_lock<std::shared_mutex> SimpleInnerStorage::rlock_deferred() const {
    return std::shared_lock<std::shared_mutex>(mutex_, std::defer_lock);
}

std::shared_mutex& SimpleInnerStorage::getMutex() const {
    return mutex_;
}

} // namespace dkv
