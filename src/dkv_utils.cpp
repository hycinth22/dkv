#include "dkv_core.hpp"
#include <sstream>
#include <algorithm>
#include <cctype>
#include <unordered_map>
#include <execinfo.h>
#include <cxxabi.h>
#include <iostream>
#include <cstdlib>
#include <csignal>

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
        // 事务命令
        {"MULTI", CommandType::MULTI},
        {"EXEC", CommandType::EXEC},
        {"DISCARD", CommandType::DISCARD},
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
        // 事务命令
        {CommandType::MULTI, "MULTI"},
        {CommandType::EXEC, "EXEC"},
        {CommandType::DISCARD, "DISCARD"},
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

void printBacktrace() {
    constexpr int MAX_FRAMES = 64;
    void* addrlist[MAX_FRAMES + 1];

    int addrlen = backtrace(addrlist, MAX_FRAMES);
    if (addrlen == 0) {
        std::cerr << "  <empty stack>\n";
        return;
    }

    char** symbollist = backtrace_symbols(addrlist, addrlen);

    for (int i = 0; i < addrlen; i++) {
        char *mangled = nullptr, *offset_begin = nullptr, *offset_end = nullptr;

        // Find parentheses and +offset
        for (char *p = symbollist[i]; *p; ++p) {
            if (*p == '(') mangled = p;
            else if (*p == '+') offset_begin = p;
            else if (*p == ')' && offset_begin) {
                offset_end = p;
                break;
            }
        }

        if (mangled && offset_begin && offset_end) {
            *mangled++ = '\0';
            *offset_begin++ = '\0';
            *offset_end = '\0';

            int status;
            char* demangled = abi::__cxa_demangle(mangled, nullptr, nullptr, &status);

            std::cerr << "  [" << i << "] " << symbollist[i]
                      << " : " << (status == 0 ? demangled : mangled)
                      << " + " << offset_begin << '\n';

            std::free(demangled);
        } else {
            std::cerr << "  [" << i << "] " << symbollist[i] << '\n';
        }
    }

    std::free(symbollist);
}

void signal_handler(int signo)
{
    printf("<<<<<<<<<<<<<<<<<catch signal %d>>>>>>>>>>>>>>>>>>>>>>>>>\n", signo);
    printf("Dump stack start...\n");
    printBacktrace();
    printf("Dump stack end...\n");

    signal(signo, SIG_DFL); /* 恢复信号默认处理 */
    raise(signo);           /* 重新发送信号 */
}

void setSignalHandler() {
    signal(SIGSEGV, signal_handler);
    signal(SIGINT, signal_handler);
    signal(SIGABRT, signal_handler);
}

} // namespace dkv