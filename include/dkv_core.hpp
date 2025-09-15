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
    UNKNOWN = -1
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
