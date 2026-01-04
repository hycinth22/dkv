#include "../../include/storage/dkv_storage_bucket.hpp"

namespace dkv {

// 数据操作方法实现
DataItem* StorageBucket::get(const Key& key) const {
    return inner_storage_.get(key);
}

DataItem* StorageBucket::get(const Key& key, const ReadView& read_view) const {
    return inner_storage_.get(key, read_view);
}

bool StorageBucket::set(TransactionID tx_id, const Key& key, std::unique_ptr<DataItem> item) {
    return inner_storage_.set(tx_id, key, std::move(item));
}

bool StorageBucket::del(TransactionID tx_id, const Key& key) {
    return inner_storage_.del(tx_id, key);
}

bool StorageBucket::exists(const Key& key) const {
    return inner_storage_.exists(key);
}

bool StorageBucket::exists(const Key& key, const ReadView& read_view) const {
    return inner_storage_.exists(key, read_view);
}

// 获取数据项引用，不支持事务
std::unique_ptr<DataItem>& StorageBucket::getRefOrInsert(const Key& key) {
    return inner_storage_.getRefOrInsert(key);
}

// 容器操作，不支持事务
void StorageBucket::clear() {
    inner_storage_.clear();
}

size_t StorageBucket::size() const {
    return inner_storage_.size();
}

std::vector<Key> StorageBucket::getAllKeys() const {
    return inner_storage_.getAllKeys();
}

// 迭代器相关操作，不支持事务
std::unordered_map<Key, std::unique_ptr<DataItem>>::const_iterator StorageBucket::find(const Key& key) const {
    return inner_storage_.find(key);
}

std::unordered_map<Key, std::unique_ptr<DataItem>>::const_iterator StorageBucket::begin() const {
    return inner_storage_.begin();
}

std::unordered_map<Key, std::unique_ptr<DataItem>>::const_iterator StorageBucket::end() const {
    return inner_storage_.end();
}

std::unique_ptr<DataItem>& StorageBucket::operator[](const Key& key) {
    return inner_storage_[key];
}

std::unordered_map<Key, std::unique_ptr<DataItem>>::iterator StorageBucket::erase(std::unordered_map<Key, std::unique_ptr<DataItem>>::iterator it) {
    return inner_storage_.erase(it);
}

std::unordered_map<Key, std::unique_ptr<DataItem>>::iterator StorageBucket::erase(std::unordered_map<Key, std::unique_ptr<DataItem>>::const_iterator it) {
    return inner_storage_.erase(it);
}

std::pair<std::unordered_map<Key, std::unique_ptr<DataItem>>::iterator, bool> StorageBucket::insert_or_assign(const Key& key, std::unique_ptr<DataItem> item) {
    return inner_storage_.insert_or_assign(key, std::move(item));
}

// 锁操作方法
std::unique_lock<std::shared_mutex> StorageBucket::wlock() const {
    return inner_storage_.wlock();
}

std::shared_lock<std::shared_mutex> StorageBucket::rlock() const {
    return inner_storage_.rlock();
}

std::unique_lock<std::shared_mutex> StorageBucket::wlock_deferred() const {
    return inner_storage_.wlock_deferred();
}

std::shared_lock<std::shared_mutex> StorageBucket::rlock_deferred() const {
    return inner_storage_.rlock_deferred();
}

std::shared_mutex& StorageBucket::getMutex() const {
    return inner_storage_.getMutex();
}

// 获取内部存储的引用
InnerStorage& StorageBucket::getInnerStorage() {
    return inner_storage_;
}

const InnerStorage& StorageBucket::getInnerStorage() const {
    return inner_storage_;
}

} // namespace dkv
