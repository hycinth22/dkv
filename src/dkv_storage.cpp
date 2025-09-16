#include "dkv_storage.hpp"
#include "dkv_datatypes.hpp"
#include "dkv_memory_allocator.hpp"
#include <algorithm>
#include <mutex>

namespace dkv {

// StorageEngine 实现
bool StorageEngine::set(const Key& key, const Value& value) {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    
    // 检查键是否已存在且未过期
    auto it = data_.find(key);
    if (it != data_.end() && !isKeyExpired(key)) {
        // 更新现有值
        auto* string_item = dynamic_cast<StringItem*>(it->second.get());
        if (string_item) {
            string_item->setValue(value);
            return true;
        }
    }
    
    // 创建新的字符串项
    data_[key] = createStringItem(value);
    total_keys_++;
    return true;
}

bool StorageEngine::set(const Key& key, const Value& value, int64_t expire_seconds) {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    
    // 计算过期时间
    auto expire_time = Utils::getCurrentTime() + std::chrono::seconds(expire_seconds);
    
    // 检查键是否已存在且未过期
    auto it = data_.find(key);
    if (it != data_.end() && !isKeyExpired(key)) {
        // 更新现有值
        auto* string_item = dynamic_cast<StringItem*>(it->second.get());
        if (string_item) {
            string_item->setValue(value);
            string_item->setExpiration(expire_time);
            return true;
        }
    }
    
    // 创建新的字符串项
    data_[key] = createStringItem(value, expire_time);
    total_keys_++;
    return true;
}

std::string StorageEngine::get(const Key& key) {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    
    auto it = data_.find(key);
    if (it == data_.end() || isKeyExpired(key)) {
        return "";
    }
    
    auto* string_item = dynamic_cast<StringItem*>(it->second.get());
    if (string_item) {
        return string_item->getValue();
    }
    
    return "";
}

bool StorageEngine::del(const Key& key) {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    
    auto it = data_.find(key);
    if (it != data_.end()) {
        data_.erase(it);
        total_keys_--;
        return true;
    }
    
    return false;
}

bool StorageEngine::exists(const Key& key) {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    
    auto it = data_.find(key);
    return it != data_.end() && !isKeyExpired(key);
}

bool StorageEngine::expire(const Key& key, int64_t seconds) {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    
    auto it = data_.find(key);
    if (it == data_.end() || isKeyExpired(key)) {
        return false;
    }
    
    DataItem* item = it->second.get();
    if (item) {
        auto expire_time = Utils::getCurrentTime() + std::chrono::seconds(seconds);
        item->setExpiration(expire_time);
        return true;
    }
    
    return false;
}

int64_t StorageEngine::ttl(const Key& key) {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    
    auto it = data_.find(key);
    if (it == data_.end() || isKeyExpired(key)) {
        return -2; // 键不存在
    }
    
    DataItem* item = it->second.get();
    if (item && item->hasExpiration()) {
        auto now = Utils::getCurrentTime();
        auto expire_time = item->getExpiration();
        auto duration = std::chrono::duration_cast<std::chrono::seconds>(expire_time - now).count();
        return duration > 0 ? duration : -2; // 已过期但未清理
    }
    
    return -1; // 键存在但没有过期时间
}

int64_t StorageEngine::incr(const Key& key) {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    
    auto it = data_.find(key);
    if (it != data_.end() && !isKeyExpired(key)) {
        auto* string_item = dynamic_cast<StringItem*>(it->second.get());
        if (string_item) {
            std::string current_value = string_item->getValue();
            if (Utils::isNumeric(current_value)) {
                int64_t new_value = Utils::stringToInt(current_value) + 1;
                string_item->setValue(Utils::intToString(new_value));
                return new_value;
            }
        }
    }
    
    // 键不存在或不是数字，设置为1
    data_[key] = createStringItem("1");
    total_keys_++;
    return 1;
}

int64_t StorageEngine::decr(const Key& key) {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    
    auto it = data_.find(key);
    if (it != data_.end() && !isKeyExpired(key)) {
        auto* string_item = dynamic_cast<StringItem*>(it->second.get());
        if (string_item) {
            std::string current_value = string_item->getValue();
            if (Utils::isNumeric(current_value)) {
                int64_t new_value = Utils::stringToInt(current_value) - 1;
                string_item->setValue(Utils::intToString(new_value));
                return new_value;
            }
        }
    }
    
    // 键不存在或不是数字，设置为-1
    data_[key] = createStringItem("-1");
    total_keys_++;
    return -1;
}

void StorageEngine::flush() {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    data_.clear();
    total_keys_ = 0;
    expired_keys_ = 0;
}

size_t StorageEngine::size() const {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    return data_.size();
}

std::vector<Key> StorageEngine::keys() const {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    std::vector<Key> result;
    result.reserve(data_.size());
    
    for (const auto& pair : data_) {
        if (!isKeyExpired(pair.first)) {
            result.push_back(pair.first);
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
    std::unique_lock<std::shared_mutex> lock(mutex_);
    
    auto it = data_.begin();
    while (it != data_.end()) {
        if (isKeyExpired(it->first)) {
            it = data_.erase(it);
            expired_keys_++;
        } else {
            ++it;
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
    auto it = data_.find(key);
    if (it == data_.end()) {
        return false;
    }
    
    return it->second->isExpired();
}

void StorageEngine::removeExpiredKey(const Key& key) {
    auto it = data_.find(key);
    if (it != data_.end()) {
        data_.erase(it);
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

bool StorageEngine::hset(const Key& key, const Value& field, const Value& value) {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    
    auto it = data_.find(key);
    HashItem* hash_item = nullptr;
    
    if (it == data_.end() || isKeyExpired(key)) {
        // 创建新的哈希项
        data_[key] = createHashItem();
        hash_item = dynamic_cast<HashItem*>(data_[key].get());
        total_keys_++;
    } else {
        // 检查是否是哈希类型
        hash_item = dynamic_cast<HashItem*>(it->second.get());
        if (!hash_item) {
            return false; // 键存在但不是哈希类型
        }
    }
    
    return hash_item->setField(field, value);
}

std::string StorageEngine::hget(const Key& key, const Value& field) {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    
    auto it = data_.find(key);
    if (it == data_.end() || isKeyExpired(key)) {
        return "";
    }
    
    auto* hash_item = dynamic_cast<HashItem*>(it->second.get());
    if (!hash_item) {
        return "";
    }
    
    Value value;
    if (hash_item->getField(field, value)) {
        return value;
    }
    return "";
}

std::vector<std::pair<Value, Value>> StorageEngine::hgetall(const Key& key) {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    
    auto it = data_.find(key);
    if (it == data_.end() || isKeyExpired(key)) {
        return {};
    }
    
    auto* hash_item = dynamic_cast<HashItem*>(it->second.get());
    if (!hash_item) {
        return {};
    }
    
    return hash_item->getAll();
}

bool StorageEngine::hdel(const Key& key, const Value& field) {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    
    auto it = data_.find(key);
    if (it == data_.end() || isKeyExpired(key)) {
        return false;
    }
    
    auto* hash_item = dynamic_cast<HashItem*>(it->second.get());
    if (!hash_item) {
        return false;
    }
    
    bool result = hash_item->delField(field);
    
    // 如果哈希为空，删除整个键
    if (hash_item->size() == 0) {
        data_.erase(it);
        total_keys_--;
    }
    
    return result;
}

bool StorageEngine::hexists(const Key& key, const Value& field) {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    
    auto it = data_.find(key);
    if (it == data_.end() || isKeyExpired(key)) {
        return false;
    }
    
    auto* hash_item = dynamic_cast<HashItem*>(it->second.get());
    if (!hash_item) {
        return false;
    }
    
    return hash_item->existsField(field);
}

std::vector<Value> StorageEngine::hkeys(const Key& key) {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    
    auto it = data_.find(key);
    if (it == data_.end() || isKeyExpired(key)) {
        return {};
    }
    
    auto* hash_item = dynamic_cast<HashItem*>(it->second.get());
    if (!hash_item) {
        return {};
    }
    
    return hash_item->getKeys();
}

std::vector<Value> StorageEngine::hvals(const Key& key) {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    
    auto it = data_.find(key);
    if (it == data_.end() || isKeyExpired(key)) {
        return {};
    }
    
    auto* hash_item = dynamic_cast<HashItem*>(it->second.get());
    if (!hash_item) {
        return {};
    }
    
    return hash_item->getValues();
}

size_t StorageEngine::hlen(const Key& key) {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    
    auto it = data_.find(key);
    if (it == data_.end() || isKeyExpired(key)) {
        return 0;
    }
    
    auto* hash_item = dynamic_cast<HashItem*>(it->second.get());
    if (!hash_item) {
        return 0;
    }
    
    return hash_item->size();
}

size_t StorageEngine::lpush(const Key& key, const Value& value) {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    
    auto it = data_.find(key);
    ListItem* list_item = nullptr;
    
    if (it == data_.end() || isKeyExpired(key)) {
        // 创建新的列表项
        data_[key] = createListItem();
        list_item = dynamic_cast<ListItem*>(data_[key].get());
        total_keys_++;
    } else {
        // 检查是否是列表类型
        list_item = dynamic_cast<ListItem*>(it->second.get());
        if (!list_item) {
            return 0; // 键存在但不是列表类型
        }
    }
    
    return list_item->lpush(value);
}

size_t StorageEngine::rpush(const Key& key, const Value& value) {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    
    auto it = data_.find(key);
    ListItem* list_item = nullptr;
    
    if (it == data_.end() || isKeyExpired(key)) {
        // 创建新的列表项
        data_[key] = createListItem();
        list_item = dynamic_cast<ListItem*>(data_[key].get());
        total_keys_++;
    } else {
        // 检查是否是列表类型
        list_item = dynamic_cast<ListItem*>(it->second.get());
        if (!list_item) {
            return 0; // 键存在但不是列表类型
        }
    }
    
    return list_item->rpush(value);
}

std::string StorageEngine::lpop(const Key& key) {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    
    auto it = data_.find(key);
    if (it == data_.end() || isKeyExpired(key)) {
        return "";
    }
    
    auto* list_item = dynamic_cast<ListItem*>(it->second.get());
    if (!list_item) {
        return "";
    }
    
    Value value;
    if (list_item->lpop(value)) {
        // 如果列表为空，删除整个键
        if (list_item->empty()) {
            data_.erase(it);
            total_keys_--;
        }
        return value;
    }
    
    return "";
}

std::string StorageEngine::rpop(const Key& key) {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    
    auto it = data_.find(key);
    if (it == data_.end() || isKeyExpired(key)) {
        return "";
    }
    
    auto* list_item = dynamic_cast<ListItem*>(it->second.get());
    if (!list_item) {
        return "";
    }
    
    Value value;
    if (list_item->rpop(value)) {
        // 如果列表为空，删除整个键
        if (list_item->empty()) {
            data_.erase(it);
            total_keys_--;
        }
        return value;
    }
    
    return "";
}

size_t StorageEngine::llen(const Key& key) {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    
    auto it = data_.find(key);
    if (it == data_.end() || isKeyExpired(key)) {
        return 0;
    }
    
    auto* list_item = dynamic_cast<ListItem*>(it->second.get());
    if (!list_item) {
        return 0;
    }
    
    return list_item->size();
}

std::vector<Value> StorageEngine::lrange(const Key& key, size_t start, size_t stop) {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    
    auto it = data_.find(key);
    if (it == data_.end() || isKeyExpired(key)) {
        return {};
    }
    
    auto* list_item = dynamic_cast<ListItem*>(it->second.get());
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

size_t StorageEngine::sadd(const Key& key, const std::vector<Value>& members) {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    
    auto it = data_.find(key);
    SetItem* set_item = nullptr;
    
    if (it == data_.end() || isKeyExpired(key)) {
        // 创建新的集合项
        data_[key] = createSetItem();
        set_item = dynamic_cast<SetItem*>(data_[key].get());
        total_keys_++;
    } else {
        // 检查是否是集合类型
        set_item = dynamic_cast<SetItem*>(it->second.get());
        if (!set_item) {
            return 0; // 键存在但不是集合类型
        }
    }
    
    // 添加多个元素并返回成功添加的个数
    return set_item->sadd(members);
}

size_t StorageEngine::srem(const Key& key, const std::vector<Value>& members) {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    
    auto it = data_.find(key);
    if (it == data_.end() || isKeyExpired(key)) {
        return 0; // 键不存在
    }
    
    // 检查是否是集合类型
    SetItem* set_item = dynamic_cast<SetItem*>(it->second.get());
    if (!set_item) {
        return 0; // 键存在但不是集合类型
    }
    
    // 删除多个元素并返回成功删除的个数
    size_t removed_count = set_item->srem(members);
    
    // 如果集合为空，删除整个键
    if (set_item->empty()) {
        data_.erase(it);
        total_keys_--;
    }
    
    return removed_count;
}

std::vector<Value> StorageEngine::smembers(const Key& key) {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    
    auto it = data_.find(key);
    if (it == data_.end() || isKeyExpired(key)) {
        return {};
    }
    
    auto* set_item = dynamic_cast<SetItem*>(it->second.get());
    if (!set_item) {
        return {};
    }
    
    return set_item->smembers();
}

bool StorageEngine::sismember(const Key& key, const Value& member) const {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    
    auto it = data_.find(key);
    if (it == data_.end() || isKeyExpired(key)) {
        return false;
    }
    
    auto* set_item = dynamic_cast<SetItem*>(it->second.get());
    if (!set_item) {
        return false;
    }
    
    return set_item->sismember(member);
}

size_t StorageEngine::scard(const Key& key) const {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    
    auto it = data_.find(key);
    if (it == data_.end() || isKeyExpired(key)) {
        return 0;
    }
    
    auto* set_item = dynamic_cast<SetItem*>(it->second.get());
    if (!set_item) {
        return 0;
    }
    
    return set_item->scard();
}

DataItem* StorageEngine::getDataItem(const Key& key) {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    
    auto it = data_.find(key);
    if (it == data_.end() || isKeyExpired(key)) {
        return nullptr;
    }
    
    return it->second.get();
}

size_t StorageEngine::zadd(const Key& key, const std::vector<std::pair<Value, double>>& members_with_scores) {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    
    auto it = data_.find(key);
    ZSetItem* zset_item = nullptr;
    
    if (it == data_.end() || isKeyExpired(key)) {
        // 创建新的有序集合项
        data_[key] = createZSetItem();
        zset_item = dynamic_cast<ZSetItem*>(data_[key].get());
        total_keys_++;
    } else {
        // 检查是否是有序集合类型
        zset_item = dynamic_cast<ZSetItem*>(it->second.get());
        if (!zset_item) {
            return 0; // 键存在但不是有序集合类型
        }
    }
    
    // 添加多个元素并返回成功添加的个数
    return zset_item->zadd(members_with_scores);
}

size_t StorageEngine::zrem(const Key& key, const std::vector<Value>& members) {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    
    auto it = data_.find(key);
    if (it == data_.end() || isKeyExpired(key)) {
        return 0; // 键不存在
    }
    
    // 检查是否是有序集合类型
    ZSetItem* zset_item = dynamic_cast<ZSetItem*>(it->second.get());
    if (!zset_item) {
        return 0; // 键存在但不是有序集合类型
    }
    
    // 删除多个元素并返回成功删除的个数
    size_t removed_count = zset_item->zrem(members);
    
    // 如果集合为空，删除整个键
    if (zset_item->empty()) {
        data_.erase(it);
        total_keys_--;
    }
    
    return removed_count;
}

bool StorageEngine::zscore(const Key& key, const Value& member, double& score) {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    
    auto it = data_.find(key);
    if (it == data_.end() || isKeyExpired(key)) {
        return false;
    }
    
    ZSetItem* zset_item = dynamic_cast<ZSetItem*>(it->second.get());
    if (!zset_item) {
        return false;
    }
    
    return zset_item->zscore(member, score);
}

bool StorageEngine::zismember(const Key& key, const Value& member) const {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    
    auto it = data_.find(key);
    if (it == data_.end() || isKeyExpired(key)) {
        return false;
    }
    
    ZSetItem* zset_item = dynamic_cast<ZSetItem*>(it->second.get());
    if (!zset_item) {
        return false;
    }
    
    return zset_item->zismember(member);
}

bool StorageEngine::zrank(const Key& key, const Value& member, size_t& rank) {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    
    auto it = data_.find(key);
    if (it == data_.end() || isKeyExpired(key)) {
        return false;
    }
    
    ZSetItem* zset_item = dynamic_cast<ZSetItem*>(it->second.get());
    if (!zset_item) {
        return false;
    }
    
    return zset_item->zrank(member, rank);
}

bool StorageEngine::zrevrank(const Key& key, const Value& member, size_t& rank) {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    
    auto it = data_.find(key);
    if (it == data_.end() || isKeyExpired(key)) {
        return false;
    }
    
    ZSetItem* zset_item = dynamic_cast<ZSetItem*>(it->second.get());
    if (!zset_item) {
        return false;
    }
    
    return zset_item->zrevrank(member, rank);
}

std::vector<std::pair<Value, double>> StorageEngine::zrange(const Key& key, size_t start, size_t stop) {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    
    auto it = data_.find(key);
    if (it == data_.end() || isKeyExpired(key)) {
        return {};
    }
    
    ZSetItem* zset_item = dynamic_cast<ZSetItem*>(it->second.get());
    if (!zset_item) {
        return {};
    }
    
    return zset_item->zrange(start, stop);
}

std::vector<std::pair<Value, double>> StorageEngine::zrevrange(const Key& key, size_t start, size_t stop) {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    
    auto it = data_.find(key);
    if (it == data_.end() || isKeyExpired(key)) {
        return {};
    }
    
    ZSetItem* zset_item = dynamic_cast<ZSetItem*>(it->second.get());
    if (!zset_item) {
        return {};
    }
    
    return zset_item->zrevrange(start, stop);
}

std::vector<std::pair<Value, double>> StorageEngine::zrangebyscore(const Key& key, double min, double max) {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    
    auto it = data_.find(key);
    if (it == data_.end() || isKeyExpired(key)) {
        return {};
    }
    
    ZSetItem* zset_item = dynamic_cast<ZSetItem*>(it->second.get());
    if (!zset_item) {
        return {};
    }
    
    return zset_item->zrangebyscore(min, max);
}

std::vector<std::pair<Value, double>> StorageEngine::zrevrangebyscore(const Key& key, double max, double min) {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    
    auto it = data_.find(key);
    if (it == data_.end() || isKeyExpired(key)) {
        return {};
    }
    
    ZSetItem* zset_item = dynamic_cast<ZSetItem*>(it->second.get());
    if (!zset_item) {
        return {};
    }
    
    return zset_item->zrevrangebyscore(max, min);
}

size_t StorageEngine::zcount(const Key& key, double min, double max) {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    
    auto it = data_.find(key);
    if (it == data_.end() || isKeyExpired(key)) {
        return 0;
    }
    
    ZSetItem* zset_item = dynamic_cast<ZSetItem*>(it->second.get());
    if (!zset_item) {
        return 0;
    }
    
    return zset_item->zcount(min, max);
}

size_t StorageEngine::zcard(const Key& key) const {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    
    auto it = data_.find(key);
    if (it == data_.end() || isKeyExpired(key)) {
        return 0;
    }
    
    ZSetItem* zset_item = dynamic_cast<ZSetItem*>(it->second.get());
    if (!zset_item) {
        return 0;
    }
    
    return zset_item->zcard();
}

} // namespace dkv
