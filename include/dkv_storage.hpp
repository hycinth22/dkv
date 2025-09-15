#pragma once

#include "dkv_core.hpp"
#include <unordered_map>
#include <shared_mutex>
#include <memory>

namespace dkv {

// 存储引擎
class StorageEngine {
private:
    // 使用读写锁保护数据访问
    mutable std::shared_mutex mutex_;
    std::unordered_map<Key, std::unique_ptr<DataItem>> data_;
    
    // 统计信息
    std::atomic<uint64_t> total_keys_{0};
    std::atomic<uint64_t> expired_keys_{0};

public:
    StorageEngine() = default;
    ~StorageEngine() = default;
    
    // 禁止拷贝和移动
    StorageEngine(const StorageEngine&) = delete;
    StorageEngine& operator=(const StorageEngine&) = delete;
    StorageEngine(StorageEngine&&) = delete;
    StorageEngine& operator=(StorageEngine&&) = delete;
    
    // 基本操作
    bool set(const Key& key, const Value& value);
    bool set(const Key& key, const Value& value, int64_t expire_seconds);
    std::string get(const Key& key);
    bool del(const Key& key);
    bool exists(const Key& key);
    bool expire(const Key& key, int64_t seconds);
    int64_t ttl(const Key& key);
    
    // 数值操作
    int64_t incr(const Key& key);
    int64_t decr(const Key& key);
    
    // 数据库管理
    void flush();
    size_t size() const;
    std::vector<Key> keys() const;
    
    // 统计信息
    uint64_t getTotalKeys() const;
    uint64_t getExpiredKeys() const;
    
    // 清理过期键
    void cleanupExpiredKeys();
    
    // 哈希操作
    bool hset(const Key& key, const Value& field, const Value& value);
    std::string hget(const Key& key, const Value& field);
    std::vector<std::pair<Value, Value>> hgetall(const Key& key);
    bool hdel(const Key& key, const Value& field);
    bool hexists(const Key& key, const Value& field);
    std::vector<Value> hkeys(const Key& key);
    std::vector<Value> hvals(const Key& key);
    size_t hlen(const Key& key);
    
    // 列表操作
    size_t lpush(const Key& key, const Value& value);
    size_t rpush(const Key& key, const Value& value);
    std::string lpop(const Key& key);
    std::string rpop(const Key& key);
    size_t llen(const Key& key);
    std::vector<Value> lrange(const Key& key, size_t start, size_t stop);
    
private:
    // 内部辅助方法
    bool isKeyExpired(const Key& key) const;
    void removeExpiredKey(const Key& key);
    std::unique_ptr<DataItem> createStringItem(const Value& value);
    std::unique_ptr<DataItem> createStringItem(const Value& value, Timestamp expire_time);
    std::unique_ptr<DataItem> createHashItem();
    std::unique_ptr<DataItem> createHashItem(Timestamp expire_time);
    std::unique_ptr<DataItem> createListItem();
    std::unique_ptr<DataItem> createListItem(Timestamp expire_time);
};

// 数据项工厂
class DataItemFactory {
public:
    static std::unique_ptr<DataItem> create(DataType type, const std::string& data = "");
    static std::unique_ptr<DataItem> create(DataType type, const std::string& data, Timestamp expire_time);
};

} // namespace dkv
