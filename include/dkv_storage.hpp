#pragma once

#include "dkv_core.hpp"
#include "dkv_memory_allocator.hpp"
#include "dkv_rdb.hpp"
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
    
    // 内存使用统计
    std::atomic<size_t> memory_usage_;
    
    // 获取内存使用量
    size_t getCurrentMemoryUsage() const;
    
    // 重置内存统计
    void resetMemoryStats();
    
    // 打印内存使用详情
    std::string getMemoryStats() const;

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
    
    // RDB持久化
    bool saveRDB(const std::string& filename);
    bool loadRDB(const std::string& filename);
    
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
    
    // 集合操作
    size_t sadd(const Key& key, const std::vector<Value>& members);
    size_t srem(const Key& key, const std::vector<Value>& members);
    std::vector<Value> smembers(const Key& key);
    bool sismember(const Key& key, const Value& member);
    size_t scard(const Key& key);
    
    // 有序集合操作
    size_t zadd(const Key& key, const std::vector<std::pair<Value, double>>& members_with_scores);
    size_t zrem(const Key& key, const std::vector<Value>& members);
    bool zscore(const Key& key, const Value& member, double& score);
    bool zismember(const Key& key, const Value& member);
    bool zrank(const Key& key, const Value& member, size_t& rank);
    bool zrevrank(const Key& key, const Value& member, size_t& rank);
    std::vector<std::pair<Value, double>> zrange(const Key& key, size_t start, size_t stop);
    std::vector<std::pair<Value, double>> zrevrange(const Key& key, size_t start, size_t stop);
    std::vector<std::pair<Value, double>> zrangebyscore(const Key& key, double min, double max);
    std::vector<std::pair<Value, double>> zrevrangebyscore(const Key& key, double max, double min);
    size_t zcount(const Key& key, double min, double max);
    size_t zcard(const Key& key);
    
    // 位图操作
    bool setBit(const Key& key, size_t offset, bool value);
    bool getBit(const Key& key, size_t offset);
    size_t bitCount(const Key& key);
    size_t bitCount(const Key& key, size_t start, size_t end);
    bool bitOp(const std::string& operation, const Key& destkey, const std::vector<Key>& keys);
    
    // HyperLogLog操作
    bool pfadd(const Key& key, const std::vector<Value>& elements);
    uint64_t pfcount(const Key& key);
    bool pfmerge(const Key& destkey, const std::vector<Key>& sourcekeys);
    
    // 获取数据项
    DataItem* getDataItem(const Key& key);
    
    // 设置数据项（AOF重写恢复专用）
    void setDataItem(const Key& key, std::unique_ptr<DataItem> item);
    
    // 淘汰策略相关方法
    std::vector<Key> getAllKeys() const; // 获取所有键
    bool hasExpiration(const Key& key) const; // 检查键是否有过期时间
    Timestamp getLastAccessed(const Key& key) const; // 获取键的最后访问时间
    int getAccessFrequency(const Key& key) const; // 获取键的访问频率
    Timestamp getExpiration(const Key& key) const; // 获取键的过期时间
    size_t getKeySize(const Key& key) const; // 获取键的大小
    
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
    std::unique_ptr<DataItem> createSetItem();
    std::unique_ptr<DataItem> createSetItem(Timestamp expire_time);
    std::unique_ptr<DataItem> createZSetItem();
    std::unique_ptr<DataItem> createZSetItem(Timestamp expire_time);
    std::unique_ptr<DataItem> createBitmapItem();
    std::unique_ptr<DataItem> createBitmapItem(Timestamp expire_time);
    std::unique_ptr<DataItem> createHyperLogLogItem();
    std::unique_ptr<DataItem> createHyperLogLogItem(Timestamp expire_time);

    // 锁操作方法
    std::unique_lock<std::shared_mutex> wlock() const;
    std::shared_lock<std::shared_mutex> rlock() const;
    std::unique_lock<std::shared_mutex> wlock_deferred() const;
    std::shared_lock<std::shared_mutex> rlock_deferred() const;
    std::shared_mutex& getMutex() const;
};

// 数据项工厂
class DataItemFactory {
public:
    static std::unique_ptr<DataItem> create(DataType type, const std::string& data = "");
    static std::unique_ptr<DataItem> create(DataType type, const std::string& data, Timestamp expire_time);
};

} // namespace dkv
