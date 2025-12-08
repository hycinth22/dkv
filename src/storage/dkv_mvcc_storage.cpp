#include "storage/dkv_mvcc_storage.h"
#include "dkv_datatypes.hpp"
#include <algorithm>
#include <mutex>
#include <cassert>

namespace dkv {

MVCCInnerStorage::MVCCInnerStorage() 
    : mvcc_(data_) {}

// 获取数据项
DataItem* MVCCInnerStorage::get(const Key& key) const {
    ReadView read_view; // todo
    return mvcc_.get(read_view, key);
}

bool MVCCInnerStorage::set(const Key& key, std::unique_ptr<DataItem> item) {
    TransactionID tx_id = 0; // todo
    return mvcc_.set(tx_id, key, move(item));
}

bool MVCCInnerStorage::del(const Key& key) {
    TransactionID tx_id = 0; // todo
    return mvcc_.del(tx_id, key);
}

bool MVCCInnerStorage::exists(const Key& key) {

}

std::unique_ptr<DataItem>& MVCCInnerStorage::getRefOrInsert(const Key& key) {

}

// 容器操作
void MVCCInnerStorage::clear() {

}

size_t MVCCInnerStorage::size() const {

}

std::vector<Key> MVCCInnerStorage::getAllKeys() const {
    std::vector<Key> keys;
    keys.reserve(data_.size());
    for (const auto& pair : data_) {
        keys.push_back(pair.first);
    }
    return keys;
}

// 迭代器相关操作
std::unordered_map<Key, std::unique_ptr<DataItem>>::const_iterator MVCCInnerStorage::find(const Key& key) const {

}

std::unordered_map<Key, std::unique_ptr<DataItem>>::const_iterator MVCCInnerStorage::begin() const {

}

std::unordered_map<Key, std::unique_ptr<DataItem>>::const_iterator MVCCInnerStorage::end() const {

}

std::unique_ptr<DataItem>& MVCCInnerStorage::operator[](const Key& key) {

}

std::unordered_map<Key, std::unique_ptr<DataItem>>::iterator MVCCInnerStorage::erase(std::unordered_map<Key, std::unique_ptr<DataItem>>::iterator it) {

}

std::unordered_map<Key, std::unique_ptr<DataItem>>::iterator MVCCInnerStorage::erase(std::unordered_map<Key, std::unique_ptr<DataItem>>::const_iterator it) {

}

std::pair<std::unordered_map<Key, std::unique_ptr<DataItem>>::iterator, bool> MVCCInnerStorage::insert_or_assign(const Key& key, std::unique_ptr<DataItem> item) {

}

// 锁操作方法
std::unique_lock<std::shared_mutex> MVCCInnerStorage::wlock() const {
    return data_.wlock();
}

std::shared_lock<std::shared_mutex> MVCCInnerStorage::rlock() const {
    return data_.rlock();
}

std::unique_lock<std::shared_mutex> MVCCInnerStorage::wlock_deferred() const {
    return data_.wlock_deferred();
}

std::shared_lock<std::shared_mutex> MVCCInnerStorage::rlock_deferred() const {
    return data_.rlock_deferred();
}

std::shared_mutex& MVCCInnerStorage::getMutex() const {
    return data_.getMutex();
}



} // namespace dkv
