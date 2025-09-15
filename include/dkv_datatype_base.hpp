#pragma once

#include <cstdint>
#include <string>
#include <memory>
#include <chrono>
#include <atomic>

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
    ZSET = 4
};

// 基础数据项接口
class DataItem {
public:
    virtual ~DataItem() = default;
    virtual DataType getType() const = 0;
    virtual std::string serialize() const = 0;
    virtual void deserialize(const std::string& data) = 0;
    virtual bool isExpired() const = 0;
    virtual void setExpiration(Timestamp expire_time) = 0;
    virtual Timestamp getExpiration() const = 0;
    virtual bool hasExpiration() const = 0;
};

// 前向声明
class StringItem;
class HashItem;
class ListItem;
class SetItem;
class ZSetItem;

} // namespace dkv