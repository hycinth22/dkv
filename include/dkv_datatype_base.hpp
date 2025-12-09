#pragma once
#include "dkv_core.hpp"
#include <cstdint>
#include <string>
#include <memory>
#include <chrono>
#include <atomic>
#include <mutex>
#include <shared_mutex>

namespace dkv {

class UndoLog;

// 基础数据项接口
class DataItem {
public:
    virtual ~DataItem() = default;
    virtual DataType getType() const = 0;
    virtual std::string serialize() const = 0;
    virtual void deserialize(const std::string& data) = 0;
    
    // 用于MVCC的克隆方法
    virtual std::unique_ptr<DataItem> clone() const = 0;

    // 构造函数
    DataItem();
    DataItem(Timestamp expire_time);
    DataItem(const DataItem& other);

    DataItem& operator=(const DataItem& other) = delete;

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
    
    // MVCC相关方法
    uint64_t getTransactionId() const {
        return transaction_id_;
    }
    void setTransactionId(uint64_t id) {
        transaction_id_ = id;
    }
    const std::unique_ptr<UndoLog>& getUndoLog() const {
        return undo_log_;
    }
    std::unique_ptr<UndoLog>& getUndoLog() {
        return undo_log_;
    }
    void setUndoLog(std::unique_ptr<UndoLog> undo_log) {
        undo_log_ = std::move(undo_log);
    }
    // Deleted: 用于用户删除键值对，在相应MVCC版本中记录已删除（是有效记录）
    bool isDeleted() const {
        return deleted_;
    }
    void setDeleted(bool deleted) {
        deleted_ = deleted;
    }
    // Discard: 用于事务回滚，相应MVCC版本失效，记录失效，稍后purge
    bool isDiscard() const {
        return discard_;
    }
    void setDiscard() {
        discard_ = true;
    }
    
protected:
    // TTL
    std::atomic<Timestamp> expire_time_;

    // 淘汰策略
    std::atomic<Timestamp> last_accessed_; // 最后访问时间
    std::atomic<uint64_t> access_frequency_ = {0}; // 访问频率
    
    // 读写锁，用于保护数据项的并发访问
    mutable std::shared_mutex item_mutex_;

    // MVCC
    std::atomic<uint64_t> transaction_id_ = {0};
    std::unique_ptr<UndoLog> undo_log_;
    bool deleted_ = {false};
    bool discard_ = {false};
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