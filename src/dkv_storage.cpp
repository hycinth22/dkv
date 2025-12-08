#include "dkv_storage.hpp"
#include "dkv_datatypes.hpp"
#include "dkv_memory_allocator.hpp"
#include "dkv_logger.hpp"
#include "storage/dkv_inner_storage.h"
#include "storage/dkv_simple_storage.h"
#include <algorithm>
#include <mutex>
#include <cassert>
namespace dkv {

StorageEngine::StorageEngine(TransactionIsolationLevel tx_isolation_level) 
: memory_usage_(0) { 
    if (tx_isolation_level == TransactionIsolationLevel::READ_UNCOMMITTED || tx_isolation_level == TransactionIsolationLevel::SERIALIZABLE) {
        inner_storage_ = std::make_unique<SimpleInnerStorage>();
    } else if (tx_isolation_level == TransactionIsolationLevel::READ_COMMITTED || tx_isolation_level == TransactionIsolationLevel::REPEATABLE_READ) {
        inner_storage_ = std::make_unique<MVCCInnerStorage>();
    } else {
        DKV_LOG_ERROR("Error: Unsupported transaction isolation level");
        exit(1);
    }
    transaction_manager_ = new TransactionManager(this, tx_isolation_level);
}

// StorageEngine 实现
bool StorageEngine::set(const Key& key, const Value& value) {
    auto readlock = inner_storage_->rlock();
    // 检查键是否已存在且未过期
    auto it = inner_storage_->find(key);
    if (it != inner_storage_->end() && !it->second->isExpired()) {
        auto keylock = it->second->lock();
        // 更新现有值
        auto* string_item = dynamic_cast<StringItem*>(it->second.get());
        if (string_item) {
            string_item->setValue(value);
            return true;
        }
    }
    readlock.unlock();
    auto writelock = inner_storage_->wlock();
    // 创建新的字符串项
    (*inner_storage_)[key] = createStringItem(value);
    total_keys_++;
    return true;
}

bool StorageEngine::set(const Key& key, const Value& value, int64_t expire_seconds) {
    auto readlock = inner_storage_->rlock();
    
    // 计算过期时间
    auto expire_time = Utils::getCurrentTime() + std::chrono::seconds(expire_seconds);
    
    // 检查键是否已存在且未过期
    auto it = inner_storage_->find(key);
    if (it != inner_storage_->end() && !it->second->isExpired()) {
        auto keylock = it->second->lock();
        // 更新现有值
        auto* string_item = dynamic_cast<StringItem*>(it->second.get());
        if (string_item) {
            string_item->setValue(value);
            string_item->setExpiration(expire_time);
            return true;
        }
    }
    
    // 创建新的字符串项
    readlock.unlock();
    auto writelock = inner_storage_->wlock();
    (*inner_storage_)[key] = createStringItem(value, expire_time);
    total_keys_++;
    return true;
}

std::string StorageEngine::get(const Key& key) {
    auto readlock = inner_storage_->rlock();
    
    auto it = inner_storage_->find(key);
    if (it == inner_storage_->end() || it->second->isExpired()) {
        return "";
    }
    auto keylock = it->second->rlock();
    
    auto* string_item = dynamic_cast<StringItem*>(it->second.get());
    if (string_item) {
        // 更新访问时间和频率
        string_item->touch();
        string_item->incrementFrequency();
        return string_item->getValue();
    }
    
    return "";
}

bool StorageEngine::del(const Key& key) {
    auto writelock = inner_storage_->wlock();
    
    auto it = inner_storage_->find(key);
    if (it != inner_storage_->end()) {
        inner_storage_->erase(it);
        total_keys_--;
        return true;
    }
    
    return false;
}

bool StorageEngine::exists(const Key& key) {
    auto readlock = inner_storage_->rlock();
    
    auto it = inner_storage_->find(key);
    if (it != inner_storage_->end() && !it->second->isExpired()) {
        // 更新访问时间和频率
        DataItem* item = it->second.get();
        item->touch();
        item->incrementFrequency();
        return true;
    }
    return false;
}

bool StorageEngine::expire(const Key& key, int64_t seconds) {
    auto readlock = inner_storage_->rlock();
    
    auto it = inner_storage_->find(key);
    if (it == inner_storage_->end() || it->second->isExpired()) {
        return false;
    }
    auto keylock = it->second->lock();
    
    DataItem* item = it->second.get();
    if (item) {
        auto expire_time = Utils::getCurrentTime() + std::chrono::seconds(seconds);
        item->setExpiration(expire_time);
        return true;
    }
    
    return false;
}

int64_t StorageEngine::ttl(const Key& key) {
    auto readlock = inner_storage_->rlock();
    
    auto it = inner_storage_->find(key);
    if (it == inner_storage_->end() || it->second->isExpired()) {
        return -2; // 键不存在
    }
    auto keylock = it->second->rlock();

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
    auto readlock = inner_storage_->rlock();
    
    auto it = inner_storage_->find(key);
    if (it != inner_storage_->end() && !it->second->isExpired()) {
        auto keylock = it->second->lock();
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
    readlock.unlock();
    auto writelock = inner_storage_->wlock();
    (*inner_storage_)[key] = createStringItem("1");
    total_keys_++;
    return 1;
}

int64_t StorageEngine::decr(const Key& key) {
    auto readlock = inner_storage_->rlock();
    
    auto it = inner_storage_->find(key);
    if (it != inner_storage_->end() && !it->second->isExpired()) {
        auto keylock = it->second->lock();
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
    readlock.unlock();
    auto writelock = inner_storage_->wlock();
    (*inner_storage_)[key] = createStringItem("-1");
    total_keys_++;
    return -1;
}

void StorageEngine::flush() {
    auto writelock = inner_storage_->wlock();
    inner_storage_->clear();
    total_keys_ = 0;
    expired_keys_ = 0;
}

size_t StorageEngine::size() const {
    auto readlock = inner_storage_->rlock();
    return inner_storage_->size();
}

std::vector<Key> StorageEngine::keys() const {
    auto readlock = inner_storage_->rlock();
    std::vector<Key> result;
    result.reserve(inner_storage_->size());
    
    for (const auto& pair : *inner_storage_) {
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
    auto writelock = inner_storage_->wlock();
    
    auto it = inner_storage_->begin();
    while (it != inner_storage_->end()) {
        if (it->second->isExpired()) {
            it = inner_storage_->erase(it);
            expired_keys_++;
        } else {
            ++it;
        }
    }
}

void StorageEngine::cleanupEmptyKey() {
    auto writelock = inner_storage_->wlock();
    
    auto it = inner_storage_->begin();
    while (it != inner_storage_->end()) {
        HashItem* hash_item = dynamic_cast<HashItem*>(it->second.get());
        if (hash_item) {
            if (hash_item->size() == 0) {
                it = inner_storage_->erase(it);
                total_keys_--;
            } else {
                ++it;
            }
            continue;
        }
        ListItem* list_item = dynamic_cast<ListItem*>(it->second.get());
        if (list_item) {
            if (list_item->empty()) {
                it = inner_storage_->erase(it);
                total_keys_--;
            } else {
                ++it;
            }
            continue;
        }
        SetItem* set_item = dynamic_cast<SetItem*>(it->second.get());
        if (set_item) {
            if (set_item->empty()) {
                it = inner_storage_->erase(it);
                total_keys_--;
            } else {
                ++it;
            }
            continue;
        }
        ZSetItem* zset_item = dynamic_cast<ZSetItem*>(it->second.get());
        if (zset_item) {
            if (zset_item->empty()) {
                it = inner_storage_->erase(it);
                total_keys_--;
            } else {
                ++it;
            }
            continue;
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
    auto it = inner_storage_->find(key);
    if (it == inner_storage_->end()) {
        return false;
    }
    return it->second->isExpired();
}

void StorageEngine::removeExpiredKey(const Key& key) {
    auto it = inner_storage_->find(key);
    if (it != inner_storage_->end()) {
        inner_storage_->erase(it);
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

bool StorageEngine::hset(const Key& key, const Value& field, const Value& value) {
    auto readlock = inner_storage_->rlock();
    auto writelock = inner_storage_->wlock_deferred();
    HashItem* hash_item = nullptr;
    auto it = inner_storage_->find(key);
    if (it == inner_storage_->end() || it->second->isExpired()) {
        readlock.unlock();
        writelock.lock();
        it = inner_storage_->find(key);
        if (it == inner_storage_->end() || it->second->isExpired()) {
            auto is_newkey = (it == inner_storage_->end());
            // 创建新的哈希项
            auto new_item = createHashItem();
            hash_item = dynamic_cast<HashItem*>(new_item.get());
            assert(hash_item != nullptr);
            it = inner_storage_->insert_or_assign(key, std::move(new_item)).first;
            it = inner_storage_->find(key);
            if (is_newkey) {
                total_keys_++;
            }
        }
    }
    auto keylock = it->second->lock();
    // 检查是否是哈希类型
    hash_item = dynamic_cast<HashItem*>(it->second.get());
    if (!hash_item) {
        return false; // 键存在但不是哈希类型
    }
    return hash_item->setField(field, value);
}

std::string StorageEngine::hget(const Key& key, const Value& field) {
    auto readlock = inner_storage_->rlock();
    
    auto it = inner_storage_->find(key);
    if (it == inner_storage_->end() || it->second->isExpired()) {
        return "";
    }
    auto keylock = it->second->rlock();
    
    auto* hash_item = dynamic_cast<HashItem*>(it->second.get());
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

std::vector<std::pair<Value, Value>> StorageEngine::hgetall(const Key& key) {
    auto readlock = inner_storage_->rlock();
    
    auto it = inner_storage_->find(key);
    if (it == inner_storage_->end() || it->second->isExpired()) {
        return {};
    }
    auto keylock = it->second->rlock();
    
    auto* hash_item = dynamic_cast<HashItem*>(it->second.get());
    if (!hash_item) {
        return {};
    }
    
    // 更新访问时间和频率
    hash_item->touch();
    hash_item->incrementFrequency();
    
    return hash_item->getAll();
}

bool StorageEngine::hdel(const Key& key, const Value& field) {
    auto readlock = inner_storage_->rlock();
    
    auto it = inner_storage_->find(key);
    if (it == inner_storage_->end() || it->second->isExpired()) {
        return false;
    }
    auto keylock = it->second->lock();

    auto* hash_item = dynamic_cast<HashItem*>(it->second.get());
    if (!hash_item) {
        return false;
    }
    
    bool result = hash_item->delField(field);
    return result;
}

bool StorageEngine::hexists(const Key& key, const Value& field) {
    auto readlock = inner_storage_->rlock();
    
    auto it = inner_storage_->find(key);
    if (it == inner_storage_->end() || it->second->isExpired()) {
        return false;
    }
    auto keylock = it->second->rlock();

    auto* hash_item = dynamic_cast<HashItem*>(it->second.get());
    if (!hash_item) {
        return false;
    }
    
    return hash_item->existsField(field);
}

std::vector<Value> StorageEngine::hkeys(const Key& key) {
    auto readlock = inner_storage_->rlock();
    
    auto it = inner_storage_->find(key);
    if (it == inner_storage_->end() || it->second->isExpired()) {
        return {};
    }
    auto keylock = it->second->rlock();

    auto* hash_item = dynamic_cast<HashItem*>(it->second.get());
    if (!hash_item) {
        return {};
    }
    
    // 更新访问时间和频率
    hash_item->touch();
    hash_item->incrementFrequency();
    
    return hash_item->getKeys();
}

std::vector<Value> StorageEngine::hvals(const Key& key) {
    auto readlock = inner_storage_->rlock();
    
    auto it = inner_storage_->find(key);
    if (it == inner_storage_->end() || it->second->isExpired()) {
        return {};
    }
    auto keylock = it->second->rlock();
    
    auto* hash_item = dynamic_cast<HashItem*>(it->second.get());
    if (!hash_item) {
        return {};
    }
    
    // 更新访问时间和频率
    hash_item->touch();
    hash_item->incrementFrequency();
    
    return hash_item->getValues();
}

size_t StorageEngine::hlen(const Key& key) {
    auto readlock = inner_storage_->rlock();
    
    auto it = inner_storage_->find(key);
    if (it == inner_storage_->end() || it->second->isExpired()) {
        return 0;
    }
    auto keylock = it->second->rlock();
    
    auto* hash_item = dynamic_cast<HashItem*>(it->second.get());
    if (!hash_item) {
        return 0;
    }
    
    return hash_item->size();
}

size_t StorageEngine::lpush(const Key& key, const Value& value) {
    auto readlock = inner_storage_->rlock();
    auto writelock = inner_storage_->wlock_deferred();
    
    auto it = inner_storage_->find(key);
    ListItem* list_item = nullptr;
    
    if (it == inner_storage_->end() || it->second->isExpired()) {
        bool is_newkey = (it == inner_storage_->end());
        // 创建新的列表项
        readlock.unlock();
        writelock.lock();
        it = inner_storage_->insert_or_assign(key, createListItem()).first;
        
        list_item = dynamic_cast<ListItem*>(it->second.get());
        if (is_newkey) {
            total_keys_++;
        }
    } else {
        // 检查是否是列表类型
        list_item = dynamic_cast<ListItem*>(it->second.get());
        if (!list_item) {
            return 0; // 键存在但不是列表类型
        }
    }
    auto keylock = it->second->lock();
    return list_item->lpush(value);
}

size_t StorageEngine::rpush(const Key& key, const Value& value) {
    auto readlock = inner_storage_->rlock();
    auto writelock = inner_storage_->wlock_deferred();
    
    auto it = inner_storage_->find(key);
    ListItem* list_item = nullptr;
    
    if (it == inner_storage_->end() || it->second->isExpired()) {
        bool is_newkey = (it == inner_storage_->end());
        // 创建新的列表项
        readlock.unlock();
        writelock.lock();
        it = inner_storage_->insert_or_assign(key, createListItem()).first;
        list_item = dynamic_cast<ListItem*>(it->second.get());
        if (is_newkey) {
            total_keys_++;
        }
    } else {
        // 检查是否是列表类型
        list_item = dynamic_cast<ListItem*>(it->second.get());
        if (!list_item) {
            return 0; // 键存在但不是列表类型
        }
    }
    auto keylock = it->second->lock();
    return list_item->rpush(value);
}

std::string StorageEngine::lpop(const Key& key) {
    auto readlock = inner_storage_->rlock();
    
    auto it = inner_storage_->find(key);
    if (it == inner_storage_->end() || it->second->isExpired()) {
        return "";
    }
    
    auto* list_item = dynamic_cast<ListItem*>(it->second.get());
    if (!list_item) {
        return "";
    }
    
    Value value;
    auto keylock = it->second->lock();
    if (list_item->lpop(value)) {
        // 更新访问时间和频率
        list_item->touch();
        list_item->incrementFrequency();

        return value;
    }
    
    return "";
}

std::string StorageEngine::rpop(const Key& key) {
    auto readlock = inner_storage_->rlock();
    
    auto it = inner_storage_->find(key);
    if (it == inner_storage_->end() || it->second->isExpired()) {
        return "";
    }
    
    auto* list_item = dynamic_cast<ListItem*>(it->second.get());
    if (!list_item) {
        return "";
    }
    
    Value value;
    auto keylock = it->second->lock();
    if (list_item->rpop(value)) {
        // 更新访问时间和频率
        list_item->touch();
        list_item->incrementFrequency();
        return value;
    }
    
    return "";
}

size_t StorageEngine::llen(const Key& key) {
    auto readlock = inner_storage_->rlock();
    
    auto it = inner_storage_->find(key);
    if (it == inner_storage_->end() || it->second->isExpired()) {
        return 0;
    }
    
    auto* list_item = dynamic_cast<ListItem*>(it->second.get());
    if (!list_item) {
        return 0;
    }
    
    // 更新访问时间和频率
    list_item->touch();
    list_item->incrementFrequency();
    auto keylock = it->second->rlock();
    return list_item->size();
}

std::vector<Value> StorageEngine::lrange(const Key& key, size_t start, size_t stop) {
    auto readlock = inner_storage_->rlock();
    
    auto it = inner_storage_->find(key);
    if (it == inner_storage_->end() || it->second->isExpired()) {
        return {};
    }
    
    auto* list_item = dynamic_cast<ListItem*>(it->second.get());
    if (!list_item) {
        return {};
    }
    
    // 更新访问时间和频率
    list_item->touch();
    list_item->incrementFrequency();
    auto keylock = it->second->rlock();
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
    auto readlock = inner_storage_->rlock();
    auto writelock = inner_storage_->wlock_deferred();
    auto it = inner_storage_->find(key);
    SetItem* set_item = nullptr;
    
    if (it == inner_storage_->end() || it->second->isExpired()) {
        readlock.unlock();
        writelock.lock();
        it = inner_storage_->find(key);
        if (it == inner_storage_->end() || it->second->isExpired()) {
            // 创建新的集合项
            it = inner_storage_->insert_or_assign(key, createSetItem()).first;
            set_item = dynamic_cast<SetItem*>((*inner_storage_)[key].get());
            total_keys_++;
        }
    }
    // 检查是否是集合类型
    set_item = dynamic_cast<SetItem*>(it->second.get());
    if (!set_item) {
        return 0; // 键存在但不是集合类型
    }
    // 添加多个元素并返回成功添加的个数
    auto keylock = it->second->lock();
    return set_item->sadd(members);
}

size_t StorageEngine::srem(const Key& key, const std::vector<Value>& members) {
    auto readlock = inner_storage_->rlock();
    auto writelock = inner_storage_->wlock_deferred();

    auto it = inner_storage_->find(key);
    if (it == inner_storage_->end() || it->second->isExpired()) {
        return 0; // 键不存在
    }
    
    // 检查是否是集合类型
    SetItem* set_item = dynamic_cast<SetItem*>(it->second.get());
    if (!set_item) {
        return 0; // 键存在但不是集合类型
    }
    
    // 删除多个元素并返回成功删除的个数
    auto keylock = it->second->lock();
    size_t removed_count = set_item->srem(members);

    return removed_count;
}

std::vector<Value> StorageEngine::smembers(const Key& key) {
    auto readlock = inner_storage_->rlock();
    
    auto it = inner_storage_->find(key);
    if (it == inner_storage_->end() || it->second->isExpired()) {
        return {};
    }
    
    auto* set_item = dynamic_cast<SetItem*>(it->second.get());
    if (!set_item) {
        return {};
    }
    
    // 更新访问时间和频率
    set_item->touch();
    set_item->incrementFrequency();
    auto keylock = it->second->rlock();
    return set_item->smembers();
}

bool StorageEngine::sismember(const Key& key, const Value& member) {
    auto readlock = inner_storage_->rlock();
    
    auto it = inner_storage_->find(key);
    if (it == inner_storage_->end() || it->second->isExpired()) {
        return false;
    }
    
    auto* set_item = dynamic_cast<SetItem*>(it->second.get());
    if (!set_item) {
        return false;
    }
    
    // 更新访问时间和频率
    set_item->touch();
    set_item->incrementFrequency();
    auto keylock = it->second->rlock();
    return set_item->sismember(member);
}

size_t StorageEngine::scard(const Key& key) {
    auto readlock = inner_storage_->rlock();
    
    auto it = inner_storage_->find(key);
    if (it == inner_storage_->end() || it->second->isExpired()) {
        return 0;
    }
    
    auto* set_item = dynamic_cast<SetItem*>(it->second.get());
    if (!set_item) {
        return 0;
    }
    
    // 更新访问时间和频率
    set_item->touch();
    set_item->incrementFrequency();
    auto keylock = it->second->rlock();
    return set_item->scard();
}

DataItem* StorageEngine::getDataItem(const Key& key) {
    auto readlock = inner_storage_->rlock();
    
    auto it = inner_storage_->find(key);
    if (it == inner_storage_->end() || it->second->isExpired()) {
        return nullptr;
    }
    auto keylock = it->second->rlock();
    return it->second.get();
}

void StorageEngine::setDataItem(const Key& key, std::unique_ptr<DataItem> item) {
    assert(item.get());
    auto writelock = inner_storage_->wlock();

    auto it = inner_storage_->find(key);
    if (it == inner_storage_->end()) {
        // 新键
        total_keys_++;
    }
    // 更新现有键
    it = inner_storage_->insert_or_assign(key, std::move(item)).first;
}

size_t StorageEngine::zadd(const Key& key, const std::vector<std::pair<Value, double>>& members_with_scores) {
    auto readlock = inner_storage_->rlock();
    auto writelock = inner_storage_->wlock_deferred();
    
    auto it = inner_storage_->find(key);
    ZSetItem* zset_item = nullptr;
    
    if (it == inner_storage_->end() || it->second->isExpired()) {
        readlock.unlock();
        writelock.lock();
        it = inner_storage_->find(key);
        if (it == inner_storage_->end()) {
            // 创建新的有序集合项
            it = inner_storage_->insert_or_assign(key, createZSetItem()).first;
            zset_item = dynamic_cast<ZSetItem*>((*inner_storage_)[key].get());
            total_keys_++;
        }
    }
    // 检查是否是有序集合类型
    zset_item = dynamic_cast<ZSetItem*>(it->second.get());
    if (!zset_item) {
        return 0; // 键存在但不是有序集合类型
    }
    
    // 添加多个元素并返回成功添加的个数
    auto keylock = it->second->lock();
    return zset_item->zadd(members_with_scores);
}

size_t StorageEngine::zrem(const Key& key, const std::vector<Value>& members) {
    auto readlock = inner_storage_->rlock();
    auto writelock = inner_storage_->wlock_deferred();
    
    auto it = inner_storage_->find(key);
    if (it == inner_storage_->end() || it->second->isExpired()) {
        return 0; // 键不存在
    }
    
    // 检查是否是有序集合类型
    ZSetItem* zset_item = dynamic_cast<ZSetItem*>(it->second.get());
    if (!zset_item) {
        return 0; // 键存在但不是有序集合类型
    }
    
    // 删除多个元素并返回成功删除的个数
    auto keylock = it->second->lock();
    size_t removed_count = zset_item->zrem(members);
    return removed_count;
}

bool StorageEngine::zscore(const Key& key, const Value& member, double& score) {
    auto readlock = inner_storage_->rlock();
    
    auto it = inner_storage_->find(key);
    if (it == inner_storage_->end() || it->second->isExpired()) {
        return false;
    }
    
    ZSetItem* zset_item = dynamic_cast<ZSetItem*>(it->second.get());
    if (!zset_item) {
        return false;
    }
    
    // 更新访问时间和频率
    zset_item->touch();
    zset_item->incrementFrequency();
    auto keylock = zset_item->rlock();
    return zset_item->zscore(member, score);
}

bool StorageEngine::zismember(const Key& key, const Value& member) {
    auto readlock = inner_storage_->rlock();
    
    auto it = inner_storage_->find(key);
    if (it == inner_storage_->end() || it->second->isExpired()) {
        return false;
    }
    
    ZSetItem* zset_item = dynamic_cast<ZSetItem*>(it->second.get());
    if (!zset_item) {
        return false;
    }
    
    // 更新访问时间和频率
    zset_item->touch();
    zset_item->incrementFrequency();
    auto keylock = zset_item->rlock();
    return zset_item->zismember(member);
}

bool StorageEngine::zrank(const Key& key, const Value& member, size_t& rank) {
    auto readlock = inner_storage_->rlock();
    
    auto it = inner_storage_->find(key);
    if (it == inner_storage_->end() || it->second->isExpired()) {
        return false;
    }
    
    ZSetItem* zset_item = dynamic_cast<ZSetItem*>(it->second.get());
    if (!zset_item) {
        return false;
    }
    
    // 更新访问时间和频率
    zset_item->touch();
    zset_item->incrementFrequency();
    auto keylock = zset_item->rlock();
    return zset_item->zrank(member, rank);
}

bool StorageEngine::zrevrank(const Key& key, const Value& member, size_t& rank) {
    auto readlock = inner_storage_->rlock();
    
    auto it = inner_storage_->find(key);
    if (it == inner_storage_->end() || it->second->isExpired()) {
        return false;
    }
    
    ZSetItem* zset_item = dynamic_cast<ZSetItem*>(it->second.get());
    if (!zset_item) {
        return false;
    }
    
    // 更新访问时间和频率
    zset_item->touch();
    zset_item->incrementFrequency();
    auto keylock = zset_item->rlock();
    return zset_item->zrevrank(member, rank);
}

std::vector<std::pair<Value, double>> StorageEngine::zrange(const Key& key, size_t start, size_t stop) {
    auto readlock = inner_storage_->rlock();
    
    auto it = inner_storage_->find(key);
    if (it == inner_storage_->end() || it->second->isExpired()) {
        return {};
    }
    
    auto* zset_item = dynamic_cast<ZSetItem*>(it->second.get());
    if (!zset_item) {
        return {};
    }
    
    // 更新访问时间和频率
    zset_item->touch();
    zset_item->incrementFrequency();
    auto keylock = zset_item->rlock();
    return zset_item->zrange(start, stop);
}

std::vector<std::pair<Value, double>> StorageEngine::zrevrange(const Key& key, size_t start, size_t stop) {
    auto readlock = inner_storage_->rlock();
    
    auto it = inner_storage_->find(key);
    if (it == inner_storage_->end() || it->second->isExpired()) {
        return {};
    }
    
    auto* zset_item = dynamic_cast<ZSetItem*>(it->second.get());
    if (!zset_item) {
        return {};
    }
    
    // 更新访问时间和频率
    zset_item->touch();
    zset_item->incrementFrequency();
    auto keylock = zset_item->rlock();
    return zset_item->zrevrange(start, stop);
}

std::vector<std::pair<Value, double>> StorageEngine::zrangebyscore(const Key& key, double min, double max) {
    auto readlock = inner_storage_->rlock();
    
    auto it = inner_storage_->find(key);
    if (it == inner_storage_->end() || it->second->isExpired()) {
        return {};
    }
    
    auto* zset_item = dynamic_cast<ZSetItem*>(it->second.get());
    if (!zset_item) {
        return {};
    }
    
    // 更新访问时间和频率
    zset_item->touch();
    zset_item->incrementFrequency();
    auto keylock = zset_item->rlock();
    return zset_item->zrangebyscore(min, max);
}

std::vector<std::pair<Value, double>> StorageEngine::zrevrangebyscore(const Key& key, double max, double min) {
    auto readlock = inner_storage_->rlock();
    
    auto it = inner_storage_->find(key);
    if (it == inner_storage_->end() || it->second->isExpired()) {
        return {};
    }
    
    auto* zset_item = dynamic_cast<ZSetItem*>(it->second.get());
    if (!zset_item) {
        return {};
    }
    
    // 更新访问时间和频率
    zset_item->touch();
    zset_item->incrementFrequency();
    auto keylock = zset_item->rlock();
    return zset_item->zrevrangebyscore(max, min);
}

size_t StorageEngine::zcount(const Key& key, double min, double max) {
    auto readlock = inner_storage_->rlock();
    
    auto it = inner_storage_->find(key);
    if (it == inner_storage_->end() || it->second->isExpired()) {
        return 0;
    }
    
    ZSetItem* zset_item = dynamic_cast<ZSetItem*>(it->second.get());
    if (!zset_item) {
        return 0;
    }
    auto keylock = zset_item->rlock();
    return zset_item->zcount(min, max);
}

size_t StorageEngine::zcard(const Key& key) {
    auto readlock = inner_storage_->rlock();
    
    auto it = inner_storage_->find(key);
    if (it == inner_storage_->end() || it->second->isExpired()) {
        return 0;
    }
    
    ZSetItem* zset_item = dynamic_cast<ZSetItem*>(it->second.get());
    if (!zset_item) {
        return 0;
    }
    auto keylock = zset_item->rlock();
    return zset_item->zcard();
}

// 位图操作实现
bool StorageEngine::setBit(const Key& key, size_t offset, bool value) {
    auto readlock = inner_storage_->rlock();
    auto writelock = inner_storage_->wlock_deferred();
    auto it = inner_storage_->find(key);
    BitmapItem* bitmap_item = nullptr;
    if (it == inner_storage_->end() || it->second->isExpired()) {
        // 创建新的位图项
        readlock.unlock();
        writelock.lock();
        // 检查键是否已存在
        if (it == inner_storage_->end() || it->second->isExpired()) {
            auto new_item = createBitmapItem();
            bitmap_item = dynamic_cast<BitmapItem*>(new_item.get());
            it = inner_storage_->insert_or_assign(key, std::move(new_item)).first;
            total_keys_++;
        }
    }
    // 检查是否是位图类型
    bitmap_item = dynamic_cast<BitmapItem*>(it->second.get());
    if (!bitmap_item) {
        return false; // 键存在但不是位图类型
    }
    // 更新访问时间和频率
    bitmap_item->touch();
    bitmap_item->incrementFrequency();
    auto keylock = bitmap_item->lock();
    return bitmap_item->setBit(offset, value);
}

bool StorageEngine::getBit(const Key& key, size_t offset) {
    auto readlock = inner_storage_->rlock();
    
    auto it = inner_storage_->find(key);
    if (it == inner_storage_->end() || it->second->isExpired()) {
        return false;
    }
    
    auto* bitmap_item = dynamic_cast<BitmapItem*>(it->second.get());
    if (!bitmap_item) {
        return false;
    }
    
    // 更新访问时间和频率
    bitmap_item->touch();
    bitmap_item->incrementFrequency();
    auto keylock = bitmap_item->rlock();
    return bitmap_item->getBit(offset);
}

size_t StorageEngine::bitCount(const Key& key) {
    auto readlock = inner_storage_->rlock();
    
    auto it = inner_storage_->find(key);
    if (it == inner_storage_->end() || it->second->isExpired()) {
        return 0;
    }
    
    auto* bitmap_item = dynamic_cast<BitmapItem*>(it->second.get());
    if (!bitmap_item) {
        return 0;
    }
    
    // 更新访问时间和频率
    bitmap_item->touch();
    bitmap_item->incrementFrequency();
    auto keylock = bitmap_item->rlock();
    return bitmap_item->bitCount();
}

size_t StorageEngine::bitCount(const Key& key, size_t start, size_t end) {
    auto readlock = inner_storage_->rlock();
    
    auto it = inner_storage_->find(key);
    if (it == inner_storage_->end() || it->second->isExpired()) {
        return 0;
    }
    
    auto* bitmap_item = dynamic_cast<BitmapItem*>(it->second.get());
    if (!bitmap_item) {
        return 0;
    }
    
    // 更新访问时间和频率
    bitmap_item->touch();
    bitmap_item->incrementFrequency();
    auto keylock = bitmap_item->rlock();
    return bitmap_item->bitCount(start, end);
}

bool StorageEngine::bitOp(const std::string& operation, const Key& destkey, const std::vector<Key>& keys) {
    auto writelock = inner_storage_->wlock(); // todo: opt here

    // 检查源键是否都存在且未过期且都是位图类型
    std::vector<BitmapItem*> bitmap_items;
    for (const auto& key : keys) {
        auto it = inner_storage_->find(key);
        if (it == inner_storage_->end() || it->second->isExpired()) {
            return false;
        }
        
        auto* bitmap_item = dynamic_cast<BitmapItem*>(it->second.get());
        if (!bitmap_item) {
            return false;
        }
        
        bitmap_items.push_back(bitmap_item);
    }
    
    // 创建或更新目标键
    (*inner_storage_)[destkey] = createBitmapItem();
    BitmapItem* dest_bitmap = dynamic_cast<BitmapItem*>((*inner_storage_)[destkey].get());
    
    if (operation == "AND") {
        return dest_bitmap->bitOpAnd(bitmap_items);
    } else if (operation == "OR") {
        return dest_bitmap->bitOpOr(bitmap_items);
    } else if (operation == "XOR") {
        return dest_bitmap->bitOpXor(bitmap_items);
    } else if (operation == "NOT" && keys.size() == 1) {
        return dest_bitmap->bitOpNot(bitmap_items[0]);
    }
    
    return false;
}

// HyperLogLog操作实现
bool StorageEngine::pfadd(const Key& key, const std::vector<Value>& elements) {
    auto writelock = inner_storage_->wlock(); // todo: opt here
    
    auto it = inner_storage_->find(key);
    HyperLogLogItem* hll_item = nullptr;
    
    if (it == inner_storage_->end() || it->second->isExpired()) {
        // 创建新的HyperLogLog项
        (*inner_storage_)[key] = createHyperLogLogItem();
        hll_item = dynamic_cast<HyperLogLogItem*>((*inner_storage_)[key].get());
        total_keys_++;
    } else {
        // 检查是否是HyperLogLog类型
        hll_item = dynamic_cast<HyperLogLogItem*>(it->second.get());
        if (!hll_item) {
            return false; // 键存在但不是HyperLogLog类型
        }
    }
    
    bool modified = false;
    for (const auto& element : elements) {
        if (hll_item->add(element)) {
            modified = true;
        }
    }
    
    return modified;
}

uint64_t StorageEngine::pfcount(const Key& key) {
    auto readlock = inner_storage_->rlock();
    
    auto it = inner_storage_->find(key);
    if (it == inner_storage_->end()) {
        return 0;
    }
    if (it->second->isExpired()) {
        return 0;
    }
    
    auto* hll_item = dynamic_cast<HyperLogLogItem*>(it->second.get());
    if (!hll_item) {
        return 0;
    }
    
    // 更新访问时间和频率
    hll_item->touch();
    hll_item->incrementFrequency();
    auto keylock = hll_item->rlock();
    return hll_item->count();
}

bool StorageEngine::pfmerge(const Key& destkey, const std::vector<Key>& sourcekeys) {
    auto writelock = inner_storage_->wlock(); // todo: opt here
    
    // 检查源键是否都存在且未过期且都是HyperLogLog类型
    std::vector<HyperLogLogItem*> hll_items;
    for (const auto& key : sourcekeys) {
        auto it = inner_storage_->find(key);
        if (it == inner_storage_->end() || it->second->isExpired()) {
            continue; // 忽略不存在或过期的键
        }
        
        auto* hll_item = dynamic_cast<HyperLogLogItem*>(it->second.get());
        if (!hll_item) {
            return false; // 源键不是HyperLogLog类型
        }
        
        hll_items.push_back(hll_item);
    }
    
    if (hll_items.empty()) {
        // 如果没有有效的源键，创建一个空的HyperLogLog
        (*inner_storage_)[destkey] = createHyperLogLogItem();
        return true;
    }
    
    // 创建或更新目标键
    (*inner_storage_)[destkey] = createHyperLogLogItem();
    HyperLogLogItem* dest_hll = dynamic_cast<HyperLogLogItem*>((*inner_storage_)[destkey].get());
    
    return dest_hll->merge(hll_items);
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
    auto readlock = inner_storage_->rlock();
    std::vector<Key> result;
    result.reserve(inner_storage_->size());
    
    for (const auto& pair : *inner_storage_) {
        result.push_back(pair.first);
    }
    
    return result;
}

bool StorageEngine::hasExpiration(const Key& key) const {
    auto readlock = inner_storage_->rlock();
    
    auto it = inner_storage_->find(key);
    if (it == inner_storage_->end()) {
        return false;
    }
    // hasExpiration is atomic, so we can read it without a lock
    return it->second->hasExpiration();
}

Timestamp StorageEngine::getLastAccessed(const Key& key) const {
    auto readlock = inner_storage_->rlock();
    
    auto it = inner_storage_->find(key);
    if (it == inner_storage_->end()) {
        return Timestamp::min();
    }
    // getLastAccessed is atomic, so we can read it without a lock
    return it->second->getLastAccessed();
}

int StorageEngine::getAccessFrequency(const Key& key) const {
    auto readlock = inner_storage_->rlock();
    
    auto it = inner_storage_->find(key);
    if (it == inner_storage_->end()) {
        return 0;
    }
    // getAccessFrequency is atomic, so we can read it without a lock
    return it->second->getAccessFrequency();
}

Timestamp StorageEngine::getExpiration(const Key& key) const {
    auto readlock = inner_storage_->rlock();
    
    auto it = inner_storage_->find(key);
    if (it == inner_storage_->end() || !it->second->hasExpiration()) {
        return Timestamp::max();
    }
    // getExpiration is atomic, so we can read it without a lock
    return it->second->getExpiration();
}

size_t StorageEngine::getKeySize(const Key& key) const {
    auto readlock = inner_storage_->rlock();
    
    auto it = inner_storage_->find(key);
    if (it == inner_storage_->end()) {
        return 0;
    }
    
    // 估算键的大小，包括键名和值
    auto keylock = it->second->rlock();
    size_t size = key.size() + it->second->serialize().size();
    return size;
}

} // namespace dkv
