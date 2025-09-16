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
        // 列表命令
        {"LPUSH", CommandType::LPUSH},
        {"RPUSH", CommandType::RPUSH},
        {"LPOP", CommandType::LPOP},
        {"RPOP", CommandType::RPOP},
        {"LLEN", CommandType::LLEN},
        {"LRANGE", CommandType::LRANGE},
        // 集合命令
        {"SADD", CommandType::SADD},
        {"SREM", CommandType::SREM},
        {"SMEMBERS", CommandType::SMEMBERS},
        {"SISMEMBER", CommandType::SISMEMBER},
        {"SCARD", CommandType::SCARD},
        // 服务器管理命令
        {"FLUSHDB", CommandType::FLUSHDB},
        {"DBSIZE", CommandType::DBSIZE},
        {"INFO", CommandType::INFO},
        {"SHUTDOWN", CommandType::SHUTDOWN},
        {"SAVE", CommandType::SAVE},
        {"BGSAVE", CommandType::BGSAVE},
        // 有序集合命令
        {"ZADD", CommandType::ZADD},
        {"ZREM", CommandType::ZREM},
        {"ZSCORE", CommandType::ZSCORE},
        {"ZISMEMBER", CommandType::ZISMEMBER},
        {"ZRANK", CommandType::ZRANK},
        {"ZREVRANK", CommandType::ZREVRANK},
        {"ZRANGE", CommandType::ZRANGE},
        {"ZREVRANGE", CommandType::ZREVRANGE},
        {"ZRANGEBYSCORE", CommandType::ZRANGEBYSCORE},
        {"ZREVRANGEBYSCORE", CommandType::ZREVRANGEBYSCORE},
        {"ZCOUNT", CommandType::ZCOUNT},
        {"ZCARD", CommandType::ZCARD},
        // 位图命令
        {"SETBIT", CommandType::SETBIT},
        {"GETBIT", CommandType::GETBIT},
        {"BITCOUNT", CommandType::BITCOUNT},
        {"BITOP", CommandType::BITOP},
        // HyperLogLog命令
        {"PFADD", CommandType::PFADD},
        {"PFCOUNT", CommandType::PFCOUNT},
        {"PFMERGE", CommandType::PFMERGE},
        // AOF重写专用命令
        {"RESTORE_HLL", CommandType::RESTORE_HLL},
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
        // 列表命令
        {CommandType::LPUSH, "LPUSH"},
        {CommandType::RPUSH, "RPUSH"},
        {CommandType::LPOP, "LPOP"},
        {CommandType::RPOP, "RPOP"},
        {CommandType::LLEN, "LLEN"},
        {CommandType::LRANGE, "LRANGE"},
        // 集合命令
        {CommandType::SADD, "SADD"},
        {CommandType::SREM, "SREM"},
        {CommandType::SMEMBERS, "SMEMBERS"},
        {CommandType::SISMEMBER, "SISMEMBER"},
        {CommandType::SCARD, "SCARD"},
        // 服务器管理命令
        {CommandType::FLUSHDB, "FLUSHDB"},
        {CommandType::DBSIZE, "DBSIZE"},
        {CommandType::INFO, "INFO"},
        {CommandType::SHUTDOWN, "SHUTDOWN"},
        {CommandType::SAVE, "SAVE"},
        {CommandType::BGSAVE, "BGSAVE"},
        // 有序集合命令
        {CommandType::ZADD, "ZADD"},
        {CommandType::ZREM, "ZREM"},
        {CommandType::ZSCORE, "ZSCORE"},
        {CommandType::ZISMEMBER, "ZISMEMBER"},
        {CommandType::ZRANK, "ZRANK"},
        {CommandType::ZREVRANK, "ZREVRANK"},
        {CommandType::ZRANGE, "ZRANGE"},
        {CommandType::ZREVRANGE, "ZREVRANGE"},
        {CommandType::ZRANGEBYSCORE, "ZRANGEBYSCORE"},
        {CommandType::ZREVRANGEBYSCORE, "ZREVRANGEBYSCORE"},
        {CommandType::ZCOUNT, "ZCOUNT"},
        {CommandType::ZCARD, "ZCARD"},
        // 位图命令
        {CommandType::SETBIT, "SETBIT"},
        {CommandType::GETBIT, "GETBIT"},
        {CommandType::BITCOUNT, "BITCOUNT"},
        {CommandType::BITOP, "BITOP"},
        // HyperLogLog命令
        {CommandType::PFADD, "PFADD"},
        {CommandType::PFCOUNT, "PFCOUNT"},
        {CommandType::PFMERGE, "PFMERGE"},
        // AOF重写专用命令
        {CommandType::RESTORE_HLL, "RESTORE_HLL"},
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
    
    // 检查第一个字符是否为数字、负号或小数点
    size_t start = 0;
    if (str[0] == '-' || str[0] == '.') {
        if (str.length() == 1) {
            return false;
        }
        start = 1;
    }
    
    // 检查是否包含小数点
    bool has_decimal = (str[start] == '.');
    if (has_decimal && str.length() == 2) {
        return false;
    }
    
    // 检查其余字符是否都是数字或小数点
    return std::all_of(str.begin() + start, str.end(), 
                       [](char c) { return std::isdigit(c) || c == '.'; });
}

int64_t Utils::stringToInt(const std::string& str) {
    return std::stoll(str);
}

std::string Utils::intToString(int64_t value) {
    return std::to_string(value);
}

} // namespace dkv