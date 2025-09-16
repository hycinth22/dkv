#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "dkv_datatypes.hpp"

namespace dkv {

// 基础类型定义
using Timestamp = std::chrono::system_clock::time_point;

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
};

// 响应状态枚举
enum class ResponseStatus {
    OK = 0,
    ERROR = 1,
    NOT_FOUND = 2,
    INVALID_COMMAND = 3
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

} // namespace dkv


#include "dkv_utils.hpp"
