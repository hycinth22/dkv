#pragma once

#include <cstdint>
#include <string>
#include <vector>
#include <memory>
#include <chrono>

namespace dkv {

// 基础类型定义
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

// 命令类型枚举
enum class CommandType {
    UNKNOWN = -1,
    // String
    SET = 0,
    GET = 1,
    DEL = 2,
    EXISTS = 3,
    EXPIRE = 4,
    TTL = 5,
    INCR = 6,
    DECR = 7,
    // 哈希命令
    HSET = 8,
    HGET = 9,
    HGETALL = 10,
    HDEL = 11,
    HEXISTS = 12,
    HKEYS = 13,
    HVALS = 14,
    HLEN = 15,
    // 列表命令
    LPUSH = 16,
    RPUSH = 17,
    LPOP = 18,
    RPOP = 19,
    LLEN = 20,
    LRANGE = 21,
    // 集合命令
    SADD = 22,
    SREM = 23,
    SMEMBERS = 24,
    SISMEMBER = 25,
    SCARD = 26,
    // 服务器管理命令
    FLUSHDB = 27,
    DBSIZE = 28,
    INFO = 29,
    SHUTDOWN = 30,
    SAVE = 31,
    BGSAVE = 32,
    // 有序集合命令
    ZADD = 33,
    ZREM = 34,
    ZSCORE = 35,
    ZISMEMBER = 36,
    ZRANK = 37,
    ZREVRANK = 38,
    ZRANGE = 39,
    ZREVRANGE = 40,
    ZRANGEBYSCORE = 41,
    ZREVRANGEBYSCORE = 42,
    ZCOUNT = 43,
    ZCARD = 44,
    // 位图命令
    SETBIT = 45,
    GETBIT = 46,
    BITCOUNT = 47,
    BITOP = 48,
    // HyperLogLog命令
    PFADD = 49,
    PFCOUNT = 50,
    PFMERGE = 51,
    // AOF重写专用命令
    RESTORE_HLL = 52,
    // 事务命令
    MULTI = 53,
    EXEC = 54,
    DISCARD = 55
};

inline bool isReadOnlyCommand(CommandType type) {
    switch (type) {
        case CommandType::GET:
        case CommandType::EXISTS:
        case CommandType::HGET:
        case CommandType::HGETALL:
        case CommandType::HEXISTS:
        case CommandType::HKEYS:
        case CommandType::HVALS:
        case CommandType::HLEN:
        case CommandType::LLEN:
        case CommandType::LRANGE:
        case CommandType::SMEMBERS:
        case CommandType::SISMEMBER:
        case CommandType::SCARD:
        case CommandType::ZSCORE:
        case CommandType::ZISMEMBER:
        case CommandType::ZRANK:
        case CommandType::ZREVRANK:
        case CommandType::ZRANGE:
        case CommandType::ZREVRANGE:
        case CommandType::ZRANGEBYSCORE:
        case CommandType::ZREVRANGEBYSCORE:
        case CommandType::ZCOUNT:
        case CommandType::ZCARD:
        case CommandType::GETBIT:
        case CommandType::BITCOUNT:
        case CommandType::PFCOUNT:
        case CommandType::DBSIZE:
        case CommandType::INFO:
        case CommandType::SHUTDOWN:
            return true;
        default:
            return false;
    }
}

inline bool commandNotAllowedInTx(CommandType type) {
    switch (type) {
        case CommandType::FLUSHDB:
        case CommandType::SHUTDOWN:
        case CommandType::SAVE:
        case CommandType::BGSAVE:
        case CommandType::RESTORE_HLL:
        case CommandType::MULTI:
            return true;
        default:
            return false;
    }
}

// 响应状态枚举
enum class ResponseStatus {
    OK = 0,
    ERROR = 1,
    NOT_FOUND = 2,
    INVALID_COMMAND = 3
};

// 内存淘汰策略枚举
enum class EvictionPolicy {
    NOEVICTION = 0,           // 不移除任何键
    VOLATILE_LRU = 1,         // 只对设置了过期时间的键使用LRU算法
    ALLKEYS_LRU = 2,          // 对所有键使用LRU算法
    VOLATILE_LFU = 3,         // 只对设置了过期时间的键使用LFU算法
    ALLKEYS_LFU = 4,          // 对所有键使用LFU算法
    VOLATILE_RANDOM = 5,      // 随机移除设置了过期时间的键
    ALLKEYS_RANDOM = 6,       // 随机移除任何键
    VOLATILE_TTL = 7          // 移除那些TTL值最小的键
};

// 事务隔离等级枚举
enum class TransactionIsolationLevel {
    READ_UNCOMMITTED = 0,     // 读未提交：允许事务读取其他事务尚未提交的数据
    READ_COMMITTED = 1,       // 读已提交：只允许事务读取其他事务已提交的数据
    REPEATABLE_READ = 2,      // 可重复读：确保事务多次读取同一数据时得到相同结果
    SERIALIZABLE = 3          // 串行化：最高隔离级别，强制事务串行执行
};

// 命令结构
struct Command {
    CommandType type;
    std::vector<std::string> args;
    
    Command() : type(CommandType::UNKNOWN) {}
    Command(CommandType t, const std::vector<std::string>& a) : type(t), args(a) {}
};

// 响应结构
struct Response {
    ResponseStatus status;
    std::string message;
    std::string data;
    
    Response() : status(ResponseStatus::OK) {}
    Response(ResponseStatus s, const std::string& m = "", const std::string& d = "") 
        : status(s), message(m), data(d) {}
};


class DataItem;
enum class UndoLogType {
    SET,
    DELETE
};

using TransactionID = uint64_t;
const TransactionID NO_TX = 0;

struct UndoLog {
    UndoLogType ty;
    std::unique_ptr<DataItem> old_value;
};

} // namespace dkv

#include "dkv_utils.hpp"
