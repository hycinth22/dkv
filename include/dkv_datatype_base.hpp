#pragma once

#include <cstdint>
#include <string>
#include <memory>
#include <chrono>
#include <atomic>
#include <mutex>
#include <shared_mutex>

namespace dkv {

// 基础类型定义
using Key = std::string;
using Value = std::string;
using Timestamp = std::chrono::system_clock::time_point;

// 数据类型枚举
enum class DataType {
    STRING = 0,
    HASH = 1,
    LIST = 2,
    SET = 3,
    ZSET = 4,
    BITMAP = 5,
    HYPERLOGLOG = 6
};

// 基础数据项接口
class DataItem {
public:
    virtual ~DataItem() = default;
    virtual DataType getType() const = 0;
    virtual std::string serialize() const = 0;
    virtual void deserialize(const std::string& data) = 0;

    // 构造函数
    DataItem();
    DataItem(Timestamp expire_time);

    // TTL方法
    bool isExpired() const;
    void setExpiration(Timestamp expire_time);
    Timestamp getExpiration() const;
    bool hasExpiration() const;
    
    // 用于淘汰策略的方法
    void touch();
    Timestamp getLastAccessed() const;
    void incrementFrequency();
    uint64_t getAccessFrequency() const;
    
    // 锁操作方法
    std::unique_lock<std::shared_mutex> lock();
    std::shared_lock<std::shared_mutex> rlock();
    std::shared_mutex& getMutex();
    
protected:
    // TTL
    std::atomic<Timestamp> expire_time_;

    // 淘汰策略
    std::atomic<Timestamp> last_accessed_; // 最后访问时间
    std::atomic<uint64_t> access_frequency_ = {0}; // 访问频率
    
    // 读写锁，用于保护数据项的并发访问
    mutable std::shared_mutex item_mutex_;
};

// 前向声明
class StringItem;
class HashItem;
class ListItem;
class SetItem;
class ZSetItem;
class BitmapItem;
class HyperLogLogItem;
} // namespace dkv