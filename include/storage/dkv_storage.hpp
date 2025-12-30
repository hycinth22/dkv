#pragma once

#include "../dkv_core.hpp"
#include "../dkv_datatypes.hpp"
#include "../dkv_memory_allocator.hpp"
#include "../persist/dkv_rdb.hpp"
#include "../transaction/dkv_transaction_manager.hpp"
#include "dkv_inner_storage.hpp"
#include <unordered_map>
#include <shared_mutex>
#include <memory>

namespace dkv {

// 存储引擎
class StorageEngine {
private:
    // 内部存储
    InnerStorage inner_storage_;
    
    // 统计信息
    std::atomic<uint64_t> total_keys_{0};
    std::atomic<uint64_t> expired_keys_{0};
    
    // 内存使用统计
    std::atomic<size_t> memory_usage_;

    std::unique_ptr<TransactionManager> transaction_manager_; // 事务管理器
    
    // 获取内存使用量
    size_t getCurrentMemoryUsage() const;
    
    // 重置内存统计
    void resetMemoryStats();
    
    // 打印内存使用详情
    std::string getMemoryStats() const;

public:
    // 构造函数
    StorageEngine(TransactionIsolationLevel tx_isolation_level = TransactionIsolationLevel::READ_COMMITTED);
    ~StorageEngine();

    // 事务管理器
    std::unique_ptr<TransactionManager>& getTransactionManager() { return transaction_manager_; }
    
    // 禁止拷贝和移动
    StorageEngine(const StorageEngine&) = delete;
    StorageEngine& operator=(const StorageEngine&) = delete;
    StorageEngine(StorageEngine&&) = delete;
    StorageEngine& operator=(StorageEngine&&) = delete;

    // 基本操作
    bool set(TransactionID tx_id, const Key& key, const Value& value);
    bool set(TransactionID tx_id, const Key& key, const Value& value, int64_t expire_seconds);
    std::string get(TransactionID tx_id, const Key& key);
    bool del(TransactionID tx_id, const Key& key);
    bool exists(TransactionID tx_id, const Key& key);
    bool expire(TransactionID tx_id, const Key& key, int64_t seconds);
    int64_t ttl(TransactionID tx_id, const Key& key);
    
    // 数值操作
    int64_t incr(TransactionID tx_id, const Key& key);
    int64_t decr(TransactionID tx_id, const Key& key);
    
    // 数据库管理
    void flush();
    size_t size() const;
    std::vector<Key> keys() const;
    
    // 统计信息
    uint64_t getTotalKeys() const;
    uint64_t getExpiredKeys() const;
    
    // 清理过期键和空键
    void cleanupExpiredKeys();
    void cleanupEmptyKey();
    
    // RDB持久化
    bool saveRDB(const std::string& filename);
    bool loadRDB(const std::string& filename);
    
    // 哈希操作
    bool hset(TransactionID tx_id, const Key& key, const Value& field, const Value& value);
    std::string hget(TransactionID tx_id, const Key& key, const Value& field);
    std::vector<std::pair<Value, Value>> hgetall(TransactionID tx_id, const Key& key);
    bool hdel(TransactionID tx_id, const Key& key, const Value& field);
    bool hexists(TransactionID tx_id, const Key& key, const Value& field);
    std::vector<Value> hkeys(TransactionID tx_id, const Key& key);
    std::vector<Value> hvals(TransactionID tx_id, const Key& key);
    size_t hlen(TransactionID tx_id, const Key& key);
    
    // 列表操作
    size_t lpush(TransactionID tx_id, const Key& key, const Value& value);
    size_t rpush(TransactionID tx_id, const Key& key, const Value& value);
    std::string lpop(TransactionID tx_id, const Key& key);
    std::string rpop(TransactionID tx_id, const Key& key);
    size_t llen(TransactionID tx_id, const Key& key);
    std::vector<Value> lrange(TransactionID tx_id, const Key& key, size_t start, size_t stop);
    
    // 集合操作
    size_t sadd(TransactionID tx_id, const Key& key, const std::vector<Value>& members);
    size_t srem(TransactionID tx_id, const Key& key, const std::vector<Value>& members);
    std::vector<Value> smembers(TransactionID tx_id, const Key& key);
    bool sismember(TransactionID tx_id, const Key& key, const Value& member);
    size_t scard(TransactionID tx_id, const Key& key);
    
    // 有序集合操作
    size_t zadd(TransactionID tx_id, const Key& key, const std::vector<std::pair<Value, double>>& members_with_scores);
    size_t zrem(TransactionID tx_id, const Key& key, const std::vector<Value>& members);
    bool zscore(TransactionID tx_id, const Key& key, const Value& member, double& score);
    bool zismember(TransactionID tx_id, const Key& key, const Value& member);
    bool zrank(TransactionID tx_id, const Key& key, const Value& member, size_t& rank);
    bool zrevrank(TransactionID tx_id, const Key& key, const Value& member, size_t& rank);
    std::vector<std::pair<Value, double>> zrange(TransactionID tx_id, const Key& key, size_t start, size_t stop);
    std::vector<std::pair<Value, double>> zrevrange(TransactionID tx_id, const Key& key, size_t start, size_t stop);
    std::vector<std::pair<Value, double>> zrangebyscore(TransactionID tx_id, const Key& key, double min, double max);
    std::vector<std::pair<Value, double>> zrevrangebyscore(TransactionID tx_id, const Key& key, double max, double min);
    size_t zcount(TransactionID tx_id, const Key& key, double min, double max);
    size_t zcard(TransactionID tx_id, const Key& key);
    
    // 位图操作
    bool setBit(TransactionID tx_id, const Key& key, size_t offset, bool value);
    bool getBit(TransactionID tx_id, const Key& key, size_t offset);
    size_t bitCount(TransactionID tx_id, const Key& key);
    size_t bitCount(TransactionID tx_id, const Key& key, size_t start, size_t end);
    bool bitOp(TransactionID tx_id, const std::string& operation, const Key& destkey, const std::vector<Key>& keys);
    
    // HyperLogLog操作
    bool pfadd(TransactionID tx_id, const Key& key, const std::vector<Value>& elements);
    uint64_t pfcount(TransactionID tx_id, const Key& key);
    bool pfmerge(TransactionID tx_id, const Key& destkey, const std::vector<Key>& sourcekeys);
    
    // 获取数据项
    DataItem* getDataItem(TransactionID tx_id, const Key& key);
    
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
    ReadView getReadView(TransactionID tx_id) const;
};

// 数据项工厂
class DataItemFactory {
public:
    static std::unique_ptr<DataItem> create(DataType type, const std::string& data = "");
    static std::unique_ptr<DataItem> create(DataType type, const std::string& data, Timestamp expire_time);
};

} // namespace dkv
