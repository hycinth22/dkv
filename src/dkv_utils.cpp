#include "dkv_core.hpp"
#include <sstream>
#include <algorithm>
#include <cctype>
#include <unordered_map>

namespace dkv {

// Utils 实现
CommandType Utils::stringToCommandType(const std::string& cmd) {
    static const std::unordered_map<std::string, CommandType> command_map = {
        {"SET", CommandType::SET},
        {"GET", CommandType::GET},
        {"DEL", CommandType::DEL},
        {"EXISTS", CommandType::EXISTS},
        {"EXPIRE", CommandType::EXPIRE},
        {"TTL", CommandType::TTL},
        {"INCR", CommandType::INCR},
        {"DECR", CommandType::DECR},
        // 哈希命令
        {"HSET", CommandType::HSET},
        {"HGET", CommandType::HGET},
        {"HGETALL", CommandType::HGETALL},
        {"HDEL", CommandType::HDEL},
        {"HEXISTS", CommandType::HEXISTS},
        {"HKEYS", CommandType::HKEYS},
        {"HVALS", CommandType::HVALS},
        {"HLEN", CommandType::HLEN},
        // 集合命令
        {"SADD", CommandType::SADD},
        {"SREM", CommandType::SREM},
        {"SMEMBERS", CommandType::SMEMBERS},
        {"SISMEMBER", CommandType::SISMEMBER},
        {"SCARD", CommandType::SCARD},
    };
    
    auto it = command_map.find(cmd);
    return (it != command_map.end()) ? it->second : CommandType::UNKNOWN;
}

std::string Utils::commandTypeToString(CommandType type) {
    static const std::unordered_map<CommandType, std::string> type_map = {
        {CommandType::UNKNOWN, "UNKNOWN"},
        {CommandType::SET, "SET"},
        {CommandType::GET, "GET"},
        {CommandType::DEL, "DEL"},
        {CommandType::EXISTS, "EXISTS"},
        {CommandType::EXPIRE, "EXPIRE"},
        {CommandType::TTL, "TTL"},
        {CommandType::INCR, "INCR"},
        {CommandType::DECR, "DECR"},
        // 哈希命令
        {CommandType::HSET, "HSET"},
        {CommandType::HGET, "HGET"},
        {CommandType::HGETALL, "HGETALL"},
        {CommandType::HDEL, "HDEL"},
        {CommandType::HEXISTS, "HEXISTS"},
        {CommandType::HKEYS, "HKEYS"},
        {CommandType::HVALS, "HVALS"},
        {CommandType::HLEN, "HLEN"},
        // 集合命令
        {CommandType::SADD, "SADD"},
        {CommandType::SREM, "SREM"},
        {CommandType::SMEMBERS, "SMEMBERS"},
        {CommandType::SISMEMBER, "SISMEMBER"},
        {CommandType::SCARD, "SCARD"},
    };
    
    auto it = type_map.find(type);
    return (it != type_map.end()) ? it->second : "UNKNOWN";
}

Timestamp Utils::getCurrentTime() {
    return std::chrono::system_clock::now();
}

bool Utils::isNumeric(const std::string& str) {
    if (str.empty()) {
        return false;
    }
    
    // 检查第一个字符是否为数字或负号
    size_t start = 0;
    if (str[0] == '-') {
        if (str.length() == 1) {
            return false;
        }
        start = 1;
    }
    
    // 检查其余字符是否都是数字
    return std::all_of(str.begin() + start, str.end(), ::isdigit);
}

int64_t Utils::stringToInt(const std::string& str) {
    return std::stoll(str);
}

std::string Utils::intToString(int64_t value) {
    return std::to_string(value);
}

} // namespace dkv