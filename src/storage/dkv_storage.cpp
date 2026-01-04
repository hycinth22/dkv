#include "storage/dkv_storage.hpp"
#include "dkv_datatypes.hpp"
#include "dkv_memory_allocator.hpp"
#include "dkv_logger.hpp"
#include "storage/dkv_inner_storage.hpp"
#include <algorithm>
#include <mutex>
#include <cassert>
using namespace std;

namespace dkv {

StorageEngine::StorageEngine(TransactionIsolationLevel tx_isolation_level, size_t num_buckets)
: num_buckets_(num_buckets), buckets_(num_buckets), memory_usage_(0) {
    DKV_LOG_DEBUG("创建事务管理器，隔离级别: ", static_cast<int>(tx_isolation_level));
    transaction_manager_ = make_unique<TransactionManager>(this, tx_isolation_level);
    
    // 初始化所有buckets
    for (size_t i = 0; i < num_buckets_; ++i) {
        buckets_[i] = std::make_unique<StorageBucket>();
    }
}

StorageEngine::~StorageEngine() {}

ReadView StorageEngine::getReadView(TransactionID tx_id) const {
    return transaction_manager_->getReadView(tx_id);
}

// StorageEngine 实现
bool StorageEngine::set(TransactionID tx_id, const Key& key, const Value& value) {
    size_t bucket_idx = getBucketIndex(key);
    auto& bucket = buckets_[bucket_idx];
    auto lock = bucket->wlock();
    auto item = createStringItem(value);
    return bucket->set(tx_id, key, std::move(item));
}

bool StorageEngine::set(TransactionID tx_id, const Key& key, const Value& value, int64_t expire_seconds) {
    size_t bucket_idx = getBucketIndex(key);
    auto& bucket = buckets_[bucket_idx];
    auto lock = bucket->wlock();
    auto expire_time = Utils::getCurrentTime() + std::chrono::seconds(expire_seconds);
    auto item = createStringItem(value, expire_time);
    return bucket->set(tx_id, key, std::move(item));
}

std::string StorageEngine::get(TransactionID tx_id, const Key& key) {
    size_t bucket_idx = getBucketIndex(key);
    auto& bucket = buckets_[bucket_idx];
    auto lock = bucket->rlock();
    auto item = bucket->get(key, getReadView(tx_id));
    if (!item || item->isExpired()) {
        return "";
    }
    
    auto* string_item = dynamic_cast<StringItem*>(item);
    if (string_item) {
        // 更新访问时间和频率
        string_item->touch();
        string_item->incrementFrequency();
        return string_item->getValue();
    }
    return "";
}

bool StorageEngine::del(TransactionID tx_id, const Key& key) {
    size_t bucket_idx = getBucketIndex(key);
    auto& bucket = buckets_[bucket_idx];
    auto lock = bucket->wlock();
    return bucket->del(tx_id, key);
}

bool StorageEngine::exists(TransactionID tx_id, const Key& key) {
    size_t bucket_idx = getBucketIndex(key);
    auto& bucket = buckets_[bucket_idx];
    auto lock = bucket->rlock();
    auto item = bucket->get(key, getReadView(tx_id));
    if (!item || item->isExpired()) {
        return false;
    }
    
    // 更新访问时间和频率
    item->touch();
    item->incrementFrequency();
    return true;
}

bool StorageEngine::expire(TransactionID tx_id, const Key& key, int64_t seconds) {
    size_t bucket_idx = getBucketIndex(key);
    auto& bucket = buckets_[bucket_idx];
    auto lock = bucket->wlock();
    auto item = bucket->get(key, getReadView(tx_id));
    if (!item || item->isExpired()) {
        return false;
    }
    
    auto expire_time = Utils::getCurrentTime() + std::chrono::seconds(seconds);
    item->setExpiration(expire_time);
    return true;
}

int64_t StorageEngine::ttl(TransactionID tx_id, const Key& key) {
    size_t bucket_idx = getBucketIndex(key);
    auto& bucket = buckets_[bucket_idx];
    auto lock = bucket->rlock();
    auto item = bucket->get(key, getReadView(tx_id));
    if (!item || item->isExpired()) {
        return -2; // 键不存在
    }
    
    if (item->hasExpiration()) {
        auto now = Utils::getCurrentTime();
        auto expire_time = item->getExpiration();
        auto duration = std::chrono::duration_cast<std::chrono::seconds>(expire_time - now).count();
        return duration > 0 ? duration : -2; // 已过期但未清理
    }
    
    return -1; // 键存在但没有过期时间
}

int64_t StorageEngine::incr(TransactionID tx_id, const Key& key) {
    size_t bucket_idx = getBucketIndex(key);
    auto& bucket = buckets_[bucket_idx];
    auto lock = bucket->wlock();
    auto item = bucket->get(key, getReadView(tx_id));
    if (!item || item->isExpired()) {
        // 键不存在，创建新的数值项
        auto new_item = createStringItem("1");
        bucket->set(tx_id, key, std::move(new_item));
        return 1;
    }
    
    auto* string_item = dynamic_cast<StringItem*>(item);
    if (!string_item) {
        return -1; // 不是字符串类型
    }
    
    // 更新数值
    std::string current_value = string_item->getValue();
    if (Utils::isNumeric(current_value)) {
        int64_t new_value = Utils::stringToInt(current_value) + 1;
        string_item->setValue(Utils::intToString(new_value));
        return new_value;
    }
    
    return -1; // 不是数值类型
}

int64_t StorageEngine::decr(TransactionID tx_id, const Key& key) {
    size_t bucket_idx = getBucketIndex(key);
    auto& bucket = buckets_[bucket_idx];
    auto lock = bucket->wlock();
    auto item = bucket->get(key, getReadView(tx_id));
    if (!item || item->isExpired()) {
        // 键不存在，创建新的数值项
        auto new_item = createStringItem("-1");
        bucket->set(tx_id, key, std::move(new_item));
        return -1;
    }
    
    auto* string_item = dynamic_cast<StringItem*>(item);
    if (!string_item) {
        return -1; // 不是字符串类型
    }
    
    // 更新数值
    std::string current_value = string_item->getValue();
    if (Utils::isNumeric(current_value)) {
        int64_t new_value = Utils::stringToInt(current_value) - 1;
        string_item->setValue(Utils::intToString(new_value));
        return new_value;
    }
    
    return -1; // 不是数值类型
}

void StorageEngine::flush() {
    // 遍历所有buckets并清空它们
    for (size_t i = 0; i < num_buckets_; ++i) {
        auto& bucket = buckets_[i];
        auto writelock = bucket->wlock();
        bucket->clear();
    }
    total_keys_ = 0;
    expired_keys_ = 0;
}

size_t StorageEngine::size() const {
    size_t total_size = 0;
    // 遍历所有buckets并累加大小
    for (size_t i = 0; i < num_buckets_; ++i) {
        auto& bucket = buckets_[i];
        auto readlock = bucket->rlock();
        total_size += bucket->size();
    }
    return total_size;
}

std::vector<Key> StorageEngine::keys() const {
    std::vector<Key> result;
    // 首先估算总大小
    size_t total_size = 0;
    for (size_t i = 0; i < num_buckets_; ++i) {
        auto& bucket = buckets_[i];
        auto readlock = bucket->rlock();
        total_size += bucket->size();
    }
    result.reserve(total_size);
    
    // 遍历所有buckets并收集键
    for (size_t i = 0; i < num_buckets_; ++i) {
        auto& bucket = buckets_[i];
        auto readlock = bucket->rlock();
        for (const auto& pair : bucket->getInnerStorage()) {
            if (!isKeyExpired(pair.first)) {
                result.push_back(pair.first);
            }
        }
    }
    
    return result;
}

uint64_t StorageEngine::getTotalKeys() const {
    return total_keys_.load();
}

uint64_t StorageEngine::getExpiredKeys() const {
    return expired_keys_.load();
}

size_t StorageEngine::getCurrentMemoryUsage() const {
    return MemoryAllocator::getInstance().getCurrentUsage();
}

void StorageEngine::resetMemoryStats() {
    MemoryAllocator::getInstance().resetStats();
}

std::string StorageEngine::getMemoryStats() const {
    return MemoryAllocator::getInstance().getStats();
}

void StorageEngine::cleanupExpiredKeys() {
    // 遍历所有buckets并清理过期键
    for (size_t i = 0; i < num_buckets_; ++i) {
        auto& bucket = buckets_[i];
        auto writelock = bucket->wlock();
        
        auto it = bucket->getInnerStorage().begin();
        while (it != bucket->getInnerStorage().end()) {
            if (it->second->isExpired()) {
                it = bucket->getInnerStorage().erase(it);
                expired_keys_++;
            } else {
                ++it;
            }
        }
    }
}

void StorageEngine::cleanupEmptyKey() {
    // 遍历所有buckets并清理空键
    for (size_t i = 0; i < num_buckets_; ++i) {
        auto& bucket = buckets_[i];
        auto writelock = bucket->wlock();
        
        auto it = bucket->getInnerStorage().begin();
        while (it != bucket->getInnerStorage().end()) {
            HashItem* hash_item = dynamic_cast<HashItem*>(it->second.get());
            if (hash_item) {
                if (hash_item->size() == 0) {
                    it = bucket->getInnerStorage().erase(it);
                    total_keys_--;
                } else {
                    ++it;
                }
                continue;
            }
            ListItem* list_item = dynamic_cast<ListItem*>(it->second.get());
            if (list_item) {
                if (list_item->empty()) {
                    it = bucket->getInnerStorage().erase(it);
                    total_keys_--;
                } else {
                    ++it;
                }
                continue;
            }
            SetItem* set_item = dynamic_cast<SetItem*>(it->second.get());
            if (set_item) {
                if (set_item->empty()) {
                    it = bucket->getInnerStorage().erase(it);
                    total_keys_--;
                } else {
                    ++it;
                }
                continue;
            }
            ZSetItem* zset_item = dynamic_cast<ZSetItem*>(it->second.get());
            if (zset_item) {
                if (zset_item->empty()) {
                    it = bucket->getInnerStorage().erase(it);
                    total_keys_--;
                } else {
                    ++it;
                }
                continue;
            }
        }
    }
}

// 保存数据到RDB文件
bool StorageEngine::saveRDB(const std::string& filename) {
    RDBPersistence rdb;
    return rdb.saveToFile(this, filename);
}

// 从RDB文件加载数据
bool StorageEngine::loadRDB(const std::string& filename) {
    RDBPersistence rdb;
    return rdb.loadFromFile(this, filename);
}

bool StorageEngine::isKeyExpired(const Key& key) const {
    size_t bucket_idx = getBucketIndex(key);
    auto& bucket = buckets_[bucket_idx];
    auto lock = bucket->rlock();
    
    auto it = bucket->find(key);
    if (it == bucket->end()) {
        return false;
    }
    return it->second->isExpired();
}

void StorageEngine::removeExpiredKey(const Key& key) {
    size_t bucket_idx = getBucketIndex(key);
    auto& bucket = buckets_[bucket_idx];
    auto lock = bucket->wlock();
    
    auto it = bucket->find(key);
    if (it != bucket->end()) {
        bucket->erase(it);
        expired_keys_++;
    }
}

std::unique_ptr<DataItem> StorageEngine::createStringItem(const Value& value) {
    return std::make_unique<StringItem>(value);
}

std::unique_ptr<DataItem> StorageEngine::createStringItem(const Value& value, Timestamp expire_time) {
    return std::make_unique<StringItem>(value, expire_time);
}

std::unique_ptr<DataItem> StorageEngine::createHashItem() {
    return std::make_unique<HashItem>();
}

std::unique_ptr<DataItem> StorageEngine::createHashItem(Timestamp expire_time) {
    return std::make_unique<HashItem>(expire_time);
}

std::unique_ptr<DataItem> StorageEngine::createListItem() {
    return std::make_unique<ListItem>();
}

std::unique_ptr<DataItem> StorageEngine::createListItem(Timestamp expire_time) {
    return std::make_unique<ListItem>(expire_time);
}

std::unique_ptr<DataItem> StorageEngine::createSetItem() {
    return std::make_unique<SetItem>();
}

std::unique_ptr<DataItem> StorageEngine::createSetItem(Timestamp expire_time) {
    return std::make_unique<SetItem>(expire_time);
}

std::unique_ptr<DataItem> StorageEngine::createZSetItem() {
    return std::make_unique<ZSetItem>();
}

std::unique_ptr<DataItem> StorageEngine::createZSetItem(Timestamp expire_time) {
    return std::make_unique<ZSetItem>(expire_time);
}

std::unique_ptr<DataItem> StorageEngine::createBitmapItem() {
    return std::make_unique<BitmapItem>();
}

std::unique_ptr<DataItem> StorageEngine::createBitmapItem(Timestamp expire_time) {
    return std::make_unique<BitmapItem>(expire_time);
}

bool StorageEngine::hset(TransactionID tx_id, const Key& key, const Value& field, const Value& value) {
    size_t bucket_idx = getBucketIndex(key);
    auto& bucket = buckets_[bucket_idx];
    auto lock = bucket->wlock();
    DataItem* item = getDataItem(tx_id, key);
    if (!item || item->isExpired()) {
        // 键不存在，创建新的哈希项
        auto new_hash_item = createHashItem();
        auto* hash_item_ptr = dynamic_cast<HashItem*>(new_hash_item.get());
        if (hash_item_ptr && hash_item_ptr->setField(field, value)) {
            return bucket->set(tx_id, key, std::move(new_hash_item));
        }
        return false;
    }
    
    auto* hash_item = dynamic_cast<HashItem*>(item);
    if (!hash_item) {
        return false; // 键存在但不是哈希类型
    }
    
    // 更新哈希项
    return hash_item->setField(field, value);
}

std::string StorageEngine::hget(TransactionID tx_id, const Key& key, const Value& field) {
    size_t bucket_idx = getBucketIndex(key);
    auto& bucket = buckets_[bucket_idx];
    auto lock = bucket->rlock();
    // 使用getDataItem方法获取数据项
    DataItem* item = getDataItem(tx_id, key);
    if (!item || item->isExpired()) {
        return "";
    }
    
    auto* hash_item = dynamic_cast<HashItem*>(item);
    if (!hash_item) {
        return "";
    }
    
    Value value;
    if (hash_item->getField(field, value)) {
        // 更新访问时间和频率
        hash_item->touch();
        hash_item->incrementFrequency();
        return value;
    }
    return "";
}

std::vector<std::pair<Value, Value>> StorageEngine::hgetall(TransactionID tx_id, const Key& key) {
    size_t bucket_idx = getBucketIndex(key);
    auto& bucket = buckets_[bucket_idx];
    auto lock = bucket->rlock();
    // 使用getDataItem方法获取数据项
    DataItem* item = getDataItem(tx_id, key);
    if (!item || item->isExpired()) {
        return {};
    }
    
    auto* hash_item = dynamic_cast<HashItem*>(item);
    if (!hash_item) {
        return {};
    }
    
    // 更新访问时间和频率
    hash_item->touch();
    hash_item->incrementFrequency();
    
    return hash_item->getAll();
}

bool StorageEngine::hdel(TransactionID tx_id, const Key& key, const Value& field) {
    size_t bucket_idx = getBucketIndex(key);
    auto& bucket = buckets_[bucket_idx];
    auto lock = bucket->wlock();
    // 使用getDataItem方法获取数据项
    DataItem* item = getDataItem(tx_id, key);
    if (!item || item->isExpired()) {
        return false;
    }
    
    auto* hash_item = dynamic_cast<HashItem*>(item);
    if (!hash_item) {
        return false;
    }
    
    bool result = hash_item->delField(field);
    return result;
}

bool StorageEngine::hexists(TransactionID tx_id, const Key& key, const Value& field) {
    size_t bucket_idx = getBucketIndex(key);
    auto& bucket = buckets_[bucket_idx];
    auto lock = bucket->rlock();
    DataItem* item = getDataItem(tx_id, key);
    if (!item || item->isExpired()) {
        return false;
    }
    
    auto* hash_item = dynamic_cast<HashItem*>(item);
    if (!hash_item) {
        return false;
    }
    
    return hash_item->existsField(field);
}

std::vector<Value> StorageEngine::hkeys(TransactionID tx_id, const Key& key) {
    size_t bucket_idx = getBucketIndex(key);
    auto& bucket = buckets_[bucket_idx];
    auto lock = bucket->rlock();
    DataItem* item = getDataItem(tx_id, key);
    if (!item || item->isExpired()) {
        return {};
    }
    
    auto* hash_item = dynamic_cast<HashItem*>(item);
    if (!hash_item) {
        return {};
    }
    
    // 更新访问时间和频率
    hash_item->touch();
    hash_item->incrementFrequency();
    return hash_item->getKeys();
}

std::vector<Value> StorageEngine::hvals(TransactionID tx_id, const Key& key) {
    size_t bucket_idx = getBucketIndex(key);
    auto& bucket = buckets_[bucket_idx];
    auto lock = bucket->rlock();
    DataItem* item = getDataItem(tx_id, key);
    if (!item || item->isExpired()) {
        return {};
    }
    
    auto* hash_item = dynamic_cast<HashItem*>(item);
    if (!hash_item) {
        return {};
    }
    
    // 更新访问时间和频率
    hash_item->touch();
    hash_item->incrementFrequency();
    return hash_item->getValues();
}

size_t StorageEngine::hlen(TransactionID tx_id, const Key& key) {
    size_t bucket_idx = getBucketIndex(key);
    auto& bucket = buckets_[bucket_idx];
    auto lock = bucket->rlock();
    DataItem* item = getDataItem(tx_id, key);
    if (!item || item->isExpired()) {
        return 0;
    }
    
    auto* hash_item = dynamic_cast<HashItem*>(item);
    if (!hash_item) {
        return 0;
    }
    
    return hash_item->size();
}

size_t StorageEngine::lpush(TransactionID tx_id, const Key& key, const Value& value) {
    size_t bucket_idx = getBucketIndex(key);
    auto& bucket = buckets_[bucket_idx];
    auto lock = bucket->wlock();
    DataItem* item = getDataItem(tx_id, key);
    if (!item || item->isExpired()) {
        // 键不存在，创建新的列表项
        auto new_list_item = createListItem();
        auto* list_item_ptr = dynamic_cast<ListItem*>(new_list_item.get());
        if (list_item_ptr) {
            list_item_ptr->lpush(value);
            if (bucket->set(tx_id, key, std::move(new_list_item))) {
                return list_item_ptr->size();
            }
        }
        return 0;
    }
    
    auto* list_item = dynamic_cast<ListItem*>(item);
    if (!list_item) {
        return 0; // 键存在但不是列表类型
    }
    
    // 更新列表项
    return list_item->lpush(value);
}

size_t StorageEngine::rpush(TransactionID tx_id, const Key& key, const Value& value) {
    size_t bucket_idx = getBucketIndex(key);
    auto& bucket = buckets_[bucket_idx];
    auto lock = bucket->wlock();
    DataItem* item = getDataItem(tx_id, key);
    if (!item || item->isExpired()) {
        // 键不存在，创建新的列表项
        auto new_list_item = createListItem();
        auto* list_item_ptr = dynamic_cast<ListItem*>(new_list_item.get());
        if (list_item_ptr) {
            list_item_ptr->rpush(value);
            if (bucket->set(tx_id, key, std::move(new_list_item))) {
                return list_item_ptr->size();
            }
        }
        return 0;
    }
    
    auto* list_item = dynamic_cast<ListItem*>(item);
    if (!list_item) {
        return 0; // 键存在但不是列表类型
    }
    
    // 更新列表项
    return list_item->rpush(value);
}

std::string StorageEngine::lpop(TransactionID tx_id, const Key& key) {
    size_t bucket_idx = getBucketIndex(key);
    auto& bucket = buckets_[bucket_idx];
    auto lock = bucket->wlock();
    DataItem* item = getDataItem(tx_id, key);
    if (!item || item->isExpired()) {
        return "";
    }
    
    auto* list_item = dynamic_cast<ListItem*>(item);
    if (!list_item) {
        return "";
    }
    
    Value value;
    if (list_item->lpop(value)) {
        // 更新访问时间和频率
        list_item->touch();
        list_item->incrementFrequency();
        return value;
    }
    
    return "";
}

std::string StorageEngine::rpop(TransactionID tx_id, const Key& key) {
    size_t bucket_idx = getBucketIndex(key);
    auto& bucket = buckets_[bucket_idx];
    auto lock = bucket->wlock();
    DataItem* item = getDataItem(tx_id, key);
    if (!item || item->isExpired()) {
        return "";
    }
    
    auto* list_item = dynamic_cast<ListItem*>(item);
    if (!list_item) {
        return "";
    }
    
    Value value;
    if (list_item->rpop(value)) {
        // 更新访问时间和频率
        list_item->touch();
        list_item->incrementFrequency();
        return value;
    }
    
    return "";
}

size_t StorageEngine::llen(TransactionID tx_id, const Key& key) {
    size_t bucket_idx = getBucketIndex(key);
    auto& bucket = buckets_[bucket_idx];
    auto lock = bucket->rlock();
    DataItem* item = getDataItem(tx_id, key);
    if (!item || item->isExpired()) {
        return 0;
    }
    
    auto* list_item = dynamic_cast<ListItem*>(item);
    if (!list_item) {
        return 0;
    }
    
    return list_item->size();
}

std::vector<Value> StorageEngine::lrange(TransactionID tx_id, const Key& key, size_t start, size_t stop) {
    size_t bucket_idx = getBucketIndex(key);
    auto& bucket = buckets_[bucket_idx];
    auto lock = bucket->rlock();
    DataItem* item = getDataItem(tx_id, key);
    if (!item || item->isExpired()) {
        return {};
    }
    
    auto* list_item = dynamic_cast<ListItem*>(item);
    if (!list_item) {
        return {};
    }
    
    return list_item->lrange(start, stop);
}

// DataItemFactory 实现
std::unique_ptr<DataItem> DataItemFactory::create(DataType type, const std::string& data) {
    switch (type) {
        case DataType::STRING:
            return std::make_unique<StringItem>(data);
        case DataType::HASH: {
            auto hash_item = std::make_unique<HashItem>();
            hash_item->deserialize(data);
            return hash_item;
        }
        case DataType::LIST: {
            auto list_item = std::make_unique<ListItem>();
            list_item->deserialize(data);
            return list_item;
        }
        case DataType::SET: {
            auto set_item = std::make_unique<SetItem>();
            set_item->deserialize(data);
            return set_item;
        }
        default:
            return nullptr;
    }
}

std::unique_ptr<DataItem> DataItemFactory::create(DataType type, const std::string& data, Timestamp expire_time) {
    switch (type) {
        case DataType::STRING:
            return std::make_unique<StringItem>(data, expire_time);
        case DataType::HASH: {
            auto hash_item = std::make_unique<HashItem>(expire_time);
            hash_item->deserialize(data);
            return hash_item;
        }
        case DataType::LIST: {
            auto list_item = std::make_unique<ListItem>(expire_time);
            list_item->deserialize(data);
            return list_item;
        }
        case DataType::SET: {
            auto set_item = std::make_unique<SetItem>(expire_time);
            set_item->deserialize(data);
            return set_item;
        }
        case DataType::ZSET: {
            auto zset_item = std::make_unique<ZSetItem>(expire_time);
            zset_item->deserialize(data);
            return zset_item;
        }
        default:
            return nullptr;
    }
}

size_t StorageEngine::sadd(TransactionID tx_id, const Key& key, const std::vector<Value>& members) {
    size_t bucket_idx = getBucketIndex(key);
    auto& bucket = buckets_[bucket_idx];
    auto lock = bucket->wlock();
    DataItem* item = getDataItem(tx_id, key);
    if (!item || item->isExpired()) {
        // 键不存在，创建新的集合项
        auto new_set_item = createSetItem();
        auto* set_item_ptr = dynamic_cast<SetItem*>(new_set_item.get());
        if (set_item_ptr) {
            size_t result = set_item_ptr->sadd(members);
            if (bucket->set(tx_id, key, std::move(new_set_item))) {
                return result;
            }
        }
        return 0;
    }
    
    auto* set_item = dynamic_cast<SetItem*>(item);
    if (!set_item) {
        return 0; // 键存在但不是集合类型
    }
    
    // 添加多个元素并返回成功添加的个数
    return set_item->sadd(members);
}

size_t StorageEngine::srem(TransactionID tx_id, const Key& key, const std::vector<Value>& members) {
    size_t bucket_idx = getBucketIndex(key);
    auto& bucket = buckets_[bucket_idx];
    auto lock = bucket->wlock();
    DataItem* item = getDataItem(tx_id, key);
    if (!item || item->isExpired()) {
        return 0; // 键不存在
    }
    
    // 检查是否是集合类型
    auto* set_item = dynamic_cast<SetItem*>(item);
    if (!set_item) {
        return 0; // 键存在但不是集合类型
    }
    
    // 删除多个元素并返回成功删除的个数
    return set_item->srem(members);
}

std::vector<Value> StorageEngine::smembers(TransactionID tx_id, const Key& key) {
    size_t bucket_idx = getBucketIndex(key);
    auto& bucket = buckets_[bucket_idx];
    auto lock = bucket->rlock();
    DataItem* item = getDataItem(tx_id, key);
    if (!item || item->isExpired()) {
        return {};
    }
    
    auto* set_item = dynamic_cast<SetItem*>(item);
    if (!set_item) {
        return {};
    }
    
    return set_item->smembers();
}

bool StorageEngine::sismember(TransactionID tx_id, const Key& key, const Value& member) {
    size_t bucket_idx = getBucketIndex(key);
    auto& bucket = buckets_[bucket_idx];
    auto lock = bucket->rlock();
    DataItem* item = getDataItem(tx_id, key);
    if (!item || item->isExpired()) {
        return false;
    }
    
    auto* set_item = dynamic_cast<SetItem*>(item);
    if (!set_item) {
        return false;
    }
    
    return set_item->sismember(member);
}

size_t StorageEngine::scard(TransactionID tx_id, const Key& key) {
    size_t bucket_idx = getBucketIndex(key);
    auto& bucket = buckets_[bucket_idx];
    auto lock = bucket->rlock();
    DataItem* item = getDataItem(tx_id, key);
    if (!item || item->isExpired()) {
        return 0;
    }
    
    auto* set_item = dynamic_cast<SetItem*>(item);
    if (!set_item) {
        return 0;
    }
    
    return set_item->scard();
}

DataItem* StorageEngine::getDataItem(TransactionID tx_id, const Key& key) {
    size_t bucket_idx = getBucketIndex(key);
    auto& bucket = buckets_[bucket_idx];
    DataItem* item = bucket->get(key, getReadView(tx_id));
    if (!item || item->isExpired()) {
        return nullptr;
    }
    return item;
}

void StorageEngine::setDataItem(const Key& key, std::unique_ptr<DataItem> item) {
    assert(item.get());
    size_t bucket_idx = getBucketIndex(key);
    auto& bucket = buckets_[bucket_idx];
    auto writelock = bucket->wlock();

    auto it = bucket->find(key);
    if (it == bucket->end()) {
        // 新键
        total_keys_++;
    }
    // 更新现有键
    it = bucket->insert_or_assign(key, std::move(item)).first;
}

size_t StorageEngine::zadd(TransactionID tx_id, const Key& key, const std::vector<std::pair<Value, double>>& members_with_scores) {
    size_t bucket_idx = getBucketIndex(key);
    auto& bucket = buckets_[bucket_idx];
    auto lock = bucket->wlock();
    DataItem* item = getDataItem(tx_id, key);
    if (!item || item->isExpired()) {
        // 键不存在，创建新的有序集合项
        auto new_zset_item = createZSetItem();
        auto* zset_item_ptr = dynamic_cast<ZSetItem*>(new_zset_item.get());
        if (zset_item_ptr) {
            size_t result = zset_item_ptr->zadd(members_with_scores);
            if (bucket->set(tx_id, key, std::move(new_zset_item))) {
                return result;
            }
        }
        return 0;
    }
    
    auto* zset_item = dynamic_cast<ZSetItem*>(item);
    if (!zset_item) {
        return 0; // 键存在但不是有序集合类型
    }
    
    // 添加多个元素并返回成功添加的个数
    return zset_item->zadd(members_with_scores);
}

size_t StorageEngine::zrem(TransactionID tx_id, const Key& key, const std::vector<Value>& members) {
    size_t bucket_idx = getBucketIndex(key);
    auto& bucket = buckets_[bucket_idx];
    auto lock = bucket->wlock();
    DataItem* item = getDataItem(tx_id, key);
    if (!item || item->isExpired()) {
        return 0; // 键不存在
    }
    
    // 检查是否是有序集合类型
    auto* zset_item = dynamic_cast<ZSetItem*>(item);
    if (!zset_item) {
        return 0; // 键存在但不是有序集合类型
    }
    
    // 删除多个元素并返回成功删除的个数
    return zset_item->zrem(members);
}

bool StorageEngine::zscore(TransactionID tx_id, const Key& key, const Value& member, double& score) {
    size_t bucket_idx = getBucketIndex(key);
    auto& bucket = buckets_[bucket_idx];
    auto lock = bucket->rlock();
    DataItem* item = getDataItem(tx_id, key);
    if (!item || item->isExpired()) {
        return false;
    }
    
    auto* zset_item = dynamic_cast<ZSetItem*>(item);
    if (!zset_item) {
        return false;
    }
    
    return zset_item->zscore(member, score);
}

bool StorageEngine::zismember(TransactionID tx_id, const Key& key, const Value& member) {
    size_t bucket_idx = getBucketIndex(key);
    auto& bucket = buckets_[bucket_idx];
    auto lock = bucket->rlock();
    DataItem* item = getDataItem(tx_id, key);
    if (!item || item->isExpired()) {
        return false;
    }
    
    auto* zset_item = dynamic_cast<ZSetItem*>(item);
    if (!zset_item) {
        return false;
    }
    
    return zset_item->zismember(member);
}

bool StorageEngine::zrank(TransactionID tx_id, const Key& key, const Value& member, size_t& rank) {
    size_t bucket_idx = getBucketIndex(key);
    auto& bucket = buckets_[bucket_idx];
    auto lock = bucket->rlock();
    DataItem* item = getDataItem(tx_id, key);
    if (!item || item->isExpired()) {
        return false;
    }
    
    auto* zset_item = dynamic_cast<ZSetItem*>(item);
    if (!zset_item) {
        return false;
    }
    
    return zset_item->zrank(member, rank);
}

bool StorageEngine::zrevrank(TransactionID tx_id, const Key& key, const Value& member, size_t& rank) {
    size_t bucket_idx = getBucketIndex(key);
    auto& bucket = buckets_[bucket_idx];
    auto lock = bucket->rlock();
    DataItem* item = getDataItem(tx_id, key);
    if (!item || item->isExpired()) {
        return false;
    }
    
    auto* zset_item = dynamic_cast<ZSetItem*>(item);
    if (!zset_item) {
        return false;
    }
    
    return zset_item->zrevrank(member, rank);
}

std::vector<std::pair<Value, double>> StorageEngine::zrange(TransactionID tx_id, const Key& key, size_t start, size_t stop) {
    size_t bucket_idx = getBucketIndex(key);
    auto& bucket = buckets_[bucket_idx];
    auto lock = bucket->rlock();
    DataItem* item = getDataItem(tx_id, key);
    if (!item || item->isExpired()) {
        return {};
    }
    
    auto* zset_item = dynamic_cast<ZSetItem*>(item);
    if (!zset_item) {
        return {};
    }
    
    return zset_item->zrange(start, stop);
}

std::vector<std::pair<Value, double>> StorageEngine::zrevrange(TransactionID tx_id, const Key& key, size_t start, size_t stop) {
    size_t bucket_idx = getBucketIndex(key);
    auto& bucket = buckets_[bucket_idx];
    auto lock = bucket->rlock();
    DataItem* item = getDataItem(tx_id, key);
    if (!item || item->isExpired()) {
        return {};
    }
    
    auto* zset_item = dynamic_cast<ZSetItem*>(item);
    if (!zset_item) {
        return {};
    }
    
    return zset_item->zrevrange(start, stop);
}

std::vector<std::pair<Value, double>> StorageEngine::zrangebyscore(TransactionID tx_id, const Key& key, double min, double max) {
    size_t bucket_idx = getBucketIndex(key);
    auto& bucket = buckets_[bucket_idx];
    auto lock = bucket->rlock();
    DataItem* item = getDataItem(tx_id, key);
    if (!item || item->isExpired()) {
        return {};
    }
    
    auto* zset_item = dynamic_cast<ZSetItem*>(item);
    if (!zset_item) {
        return {};
    }
    
    return zset_item->zrangebyscore(min, max);
}

std::vector<std::pair<Value, double>> StorageEngine::zrevrangebyscore(TransactionID tx_id, const Key& key, double max, double min) {
    size_t bucket_idx = getBucketIndex(key);
    auto& bucket = buckets_[bucket_idx];
    auto lock = bucket->rlock();
    DataItem* item = getDataItem(tx_id, key);
    if (!item || item->isExpired()) {
        return {};
    }
    
    auto* zset_item = dynamic_cast<ZSetItem*>(item);
    if (!zset_item) {
        return {};
    }
    
    return zset_item->zrevrangebyscore(max, min);
}

size_t StorageEngine::zcount(TransactionID tx_id, const Key& key, double min, double max) {
    size_t bucket_idx = getBucketIndex(key);
    auto& bucket = buckets_[bucket_idx];
    auto lock = bucket->rlock();
    DataItem* item = getDataItem(tx_id, key);
    if (!item || item->isExpired()) {
        return 0;
    }
    
    auto* zset_item = dynamic_cast<ZSetItem*>(item);
    if (!zset_item) {
        return 0;
    }
    return zset_item->zcount(min, max);
}

size_t StorageEngine::zcard(TransactionID tx_id, const Key& key) {
    size_t bucket_idx = getBucketIndex(key);
    auto& bucket = buckets_[bucket_idx];
    auto lock = bucket->rlock();
    DataItem* item = getDataItem(tx_id, key);
    if (!item || item->isExpired()) {
        return 0;
    }
    
    auto* zset_item = dynamic_cast<ZSetItem*>(item);
    if (!zset_item) {
        return 0;
    }
    return zset_item->zcard();
}

// 位图操作实现
bool StorageEngine::setBit(TransactionID tx_id, const Key& key, size_t offset, bool value) {
    size_t bucket_idx = getBucketIndex(key);
    auto& bucket = buckets_[bucket_idx];
    auto lock = bucket->wlock();
    DataItem* item = getDataItem(tx_id, key);
    if (!item || item->isExpired()) {
        // 键不存在，创建新的位图项
        auto new_bitmap_item = createBitmapItem();
        auto* bitmap_item_ptr = dynamic_cast<BitmapItem*>(new_bitmap_item.get());
        if (bitmap_item_ptr) {
            bitmap_item_ptr->setBit(offset, value);
            if (bucket->set(tx_id, key, std::move(new_bitmap_item))) {
                return true;
            }
        }
        return false;
    }
    
    auto* bitmap_item = dynamic_cast<BitmapItem*>(item);
    if (!bitmap_item) {
        return false; // 键存在但不是位图类型
    }
    
    return bitmap_item->setBit(offset, value);
}

bool StorageEngine::getBit(TransactionID tx_id, const Key& key, size_t offset) {
    size_t bucket_idx = getBucketIndex(key);
    auto& bucket = buckets_[bucket_idx];
    auto lock = bucket->rlock();
    DataItem* item = getDataItem(tx_id, key);
    if (!item || item->isExpired()) {
        return false;
    }
    
    auto* bitmap_item = dynamic_cast<BitmapItem*>(item);
    if (!bitmap_item) {
        return false;
    }
    
    return bitmap_item->getBit(offset);
}

size_t StorageEngine::bitCount(TransactionID tx_id, const Key& key) {
    size_t bucket_idx = getBucketIndex(key);
    auto& bucket = buckets_[bucket_idx];
    auto lock = bucket->rlock();
    DataItem* item = getDataItem(tx_id, key);
    if (!item || item->isExpired()) {
        return 0;
    }
    
    auto* bitmap_item = dynamic_cast<BitmapItem*>(item);
    if (!bitmap_item) {
        return 0;
    }
    
    return bitmap_item->bitCount();
}

size_t StorageEngine::bitCount(TransactionID tx_id, const Key& key, size_t start, size_t end) {
    size_t bucket_idx = getBucketIndex(key);
    auto& bucket = buckets_[bucket_idx];
    auto lock = bucket->rlock();
    DataItem* item = getDataItem(tx_id, key);
    if (!item || item->isExpired()) {
        return 0;
    }
    
    auto* bitmap_item = dynamic_cast<BitmapItem*>(item);
    if (!bitmap_item) {
        return 0;
    }
    
    return bitmap_item->bitCount(start, end);
}

bool StorageEngine::bitOp(TransactionID tx_id, const std::string& operation, const Key& destkey, const std::vector<Key>& keys) {
    // 检查源键是否都存在且未过期且都是位图类型
    std::vector<BitmapItem*> bitmap_items;
    for (const auto& key : keys) {
        DataItem* item = getDataItem(tx_id, key);
        if (!item || item->isExpired()) {
            return false;
        }
        
        auto* bitmap_item = dynamic_cast<BitmapItem*>(item);
        if (!bitmap_item) {
            return false;
        }
        
        bitmap_items.push_back(bitmap_item);
    }
    
    // 为目标键获取对应的bucket
    size_t dest_bucket_idx = getBucketIndex(destkey);
    auto& dest_bucket = buckets_[dest_bucket_idx];
    auto dest_lock = dest_bucket->wlock();
    
    // 创建或更新目标键
    auto new_bitmap_item = createBitmapItem();
    BitmapItem* dest_bitmap = dynamic_cast<BitmapItem*>(new_bitmap_item.get());
    
    bool result = false;
    if (operation == "AND") {
        result = dest_bitmap->bitOpAnd(bitmap_items);
    } else if (operation == "OR") {
        result = dest_bitmap->bitOpOr(bitmap_items);
    } else if (operation == "XOR") {
        result = dest_bitmap->bitOpXor(bitmap_items);
    } else if (operation == "NOT" && keys.size() == 1) {
        result = dest_bitmap->bitOpNot(bitmap_items[0]);
    }
    
    if (result) {
        return dest_bucket->set(tx_id, destkey, std::move(new_bitmap_item));
    }
    
    return false;
}

// HyperLogLog操作实现
bool StorageEngine::pfadd(TransactionID tx_id, const Key& key, const std::vector<Value>& elements) {
    size_t bucket_idx = getBucketIndex(key);
    auto& bucket = buckets_[bucket_idx];
    auto lock = bucket->wlock();
    DataItem* item = getDataItem(tx_id, key);
    if (!item || item->isExpired()) {
        // 键不存在，创建新的HyperLogLog项
        auto new_hll_item = createHyperLogLogItem();
        auto* hll_item_ptr = dynamic_cast<HyperLogLogItem*>(new_hll_item.get());
        if (hll_item_ptr) {
            bool modified = false;
            for (const auto& element : elements) {
                if (hll_item_ptr->add(element)) {
                    modified = true;
                }
            }
            if (bucket->set(tx_id, key, std::move(new_hll_item))) {
                return modified;
            }
        }
        return false;
    }
    
    auto* hll_item = dynamic_cast<HyperLogLogItem*>(item);
    if (!hll_item) {
        return false; // 键存在但不是HyperLogLog类型
    }
    
    bool modified = false;
    for (const auto& element : elements) {
        if (hll_item->add(element)) {
            modified = true;
        }
    }
    
    return modified;
}

uint64_t StorageEngine::pfcount(TransactionID tx_id, const Key& key) {
    size_t bucket_idx = getBucketIndex(key);
    auto& bucket = buckets_[bucket_idx];
    auto lock = bucket->rlock();
    // 使用getDataItem方法获取数据项，它会处理MVCC
    DataItem* item = getDataItem(tx_id, key);
    if (!item || item->isExpired()) {
        return 0;
    }
    
    auto* hll_item = dynamic_cast<HyperLogLogItem*>(item);
    if (!hll_item) {
        return 0;
    }
    
    return hll_item->count();
}

bool StorageEngine::pfmerge(TransactionID tx_id, const Key& destkey, const std::vector<Key>& sourcekeys) {
    // 检查源键是否都存在且未过期且都是HyperLogLog类型
    std::vector<HyperLogLogItem*> hll_items;
    for (const auto& key : sourcekeys) {
        DataItem* item = getDataItem(tx_id, key);
        if (!item || item->isExpired()) {
            continue; // 忽略不存在或过期的键
        }
        
        auto* hll_item = dynamic_cast<HyperLogLogItem*>(item);
        if (!hll_item) {
            return false; // 源键不是HyperLogLog类型
        }
        
        hll_items.push_back(hll_item);
    }
    
    // 为目标键获取对应的bucket
    size_t dest_bucket_idx = getBucketIndex(destkey);
    auto& dest_bucket = buckets_[dest_bucket_idx];
    auto dest_lock = dest_bucket->wlock();
    
    auto new_hll_item = createHyperLogLogItem();
    HyperLogLogItem* dest_hll = dynamic_cast<HyperLogLogItem*>(new_hll_item.get());
    
    if (hll_items.empty()) {
        // 如果没有有效的源键，创建一个空的HyperLogLog
        return dest_bucket->set(tx_id, destkey, std::move(new_hll_item));
    }
    
    bool result = dest_hll->merge(hll_items);
    if (result) {
        return dest_bucket->set(tx_id, destkey, std::move(new_hll_item));
    }
    
    return false;
}

// 创建HyperLogLogItem的工厂方法
std::unique_ptr<DataItem> StorageEngine::createHyperLogLogItem() {
    return std::unique_ptr<DataItem>(dkv::createHyperLogLogItem());
}

std::unique_ptr<DataItem> StorageEngine::createHyperLogLogItem(Timestamp expire_time) {
    return std::unique_ptr<DataItem>(dkv::createHyperLogLogItem(expire_time));
}

// 淘汰策略相关方法实现
std::vector<Key> StorageEngine::getAllKeys() const {
    std::vector<Key> result;
    // 首先估算总大小
    size_t total_size = 0;
    for (size_t i = 0; i < num_buckets_; ++i) {
        auto& bucket = buckets_[i];
        auto readlock = bucket->rlock();
        total_size += bucket->size();
    }
    result.reserve(total_size);
    
    // 遍历所有buckets并收集键
    for (size_t i = 0; i < num_buckets_; ++i) {
        auto& bucket = buckets_[i];
        auto readlock = bucket->rlock();
        for (const auto& pair : bucket->getInnerStorage()) {
            result.push_back(pair.first);
        }
    }
    
    return result;
}

bool StorageEngine::hasExpiration(const Key& key) const {
    size_t bucket_idx = getBucketIndex(key);
    auto& bucket = buckets_[bucket_idx];
    auto readlock = bucket->rlock();
    
    auto it = bucket->find(key);
    if (it == bucket->end()) {
        return false;
    }
    // hasExpiration is atomic, so we can read it without a lock
    return it->second->hasExpiration();
}

Timestamp StorageEngine::getLastAccessed(const Key& key) const {
    size_t bucket_idx = getBucketIndex(key);
    auto& bucket = buckets_[bucket_idx];
    auto readlock = bucket->rlock();
    
    auto it = bucket->find(key);
    if (it == bucket->end()) {
        return Timestamp::min();
    }
    // getLastAccessed is atomic, so we can read it without a lock
    return it->second->getLastAccessed();
}

int StorageEngine::getAccessFrequency(const Key& key) const {
    size_t bucket_idx = getBucketIndex(key);
    auto& bucket = buckets_[bucket_idx];
    auto readlock = bucket->rlock();
    
    auto it = bucket->find(key);
    if (it == bucket->end()) {
        return 0;
    }
    // getAccessFrequency is atomic, so we can read it without a lock
    return it->second->getAccessFrequency();
}

Timestamp StorageEngine::getExpiration(const Key& key) const {
    size_t bucket_idx = getBucketIndex(key);
    auto& bucket = buckets_[bucket_idx];
    auto readlock = bucket->rlock();
    
    auto it = bucket->find(key);
    if (it == bucket->end() || !it->second->hasExpiration()) {
        return Timestamp::max();
    }
    // getExpiration is atomic, so we can read it without a lock
    return it->second->getExpiration();
}

size_t StorageEngine::getKeySize(const Key& key) const {
    size_t bucket_idx = getBucketIndex(key);
    auto& bucket = buckets_[bucket_idx];
    auto readlock = bucket->rlock();
    
    auto it = bucket->find(key);
    if (it == bucket->end()) {
        return 0;
    }
    
    // 估算键的大小，包括键名和值
    auto keylock = it->second->rlock();
    size_t size = key.size() + it->second->serialize().size();
    return size;
}

} // namespace dkv
