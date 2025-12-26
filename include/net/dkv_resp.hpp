#pragma once

#include "../dkv_core.hpp"
#include <string>
#include <vector>

namespace dkv {

// RESP协议类型
enum class RESPType {
    SIMPLE_STRING = '+',  // +
    ERROR = '-',          // -
    INTEGER = ':',        // :
    BULK_STRING = '$',    // $
    ARRAY = '*'           // *
};

// RESP协议解析器
class RESPProtocol {
public:
    // 解析命令
    static Command parseCommand(const std::string& data, size_t& pos);
    static Command parseCommand(const std::string& data, size_t&& pos) {
        return parseCommand(data, pos);
    }
    // 序列化响应
    static std::string serializeResponse(const Response& response);
    
    // 序列化简单字符串
    static std::string serializeSimpleString(const std::string& str);
    
    // 序列化错误
    static std::string serializeError(const std::string& error);
    
    // 序列化整数
    static std::string serializeInteger(int64_t value);
    
    // 序列化批量字符串
    static std::string serializeBulkString(const std::string& str);
    
    // 序列化数组
    static std::string serializeArray(const std::vector<std::string>& array);
    
    // 序列化空值
    static std::string serializeNull();

private:
    // 解析数组
    static std::vector<std::string> parseArray(const std::string& data, size_t& pos);
    
    // 解析批量字符串
    static std::string parseBulkString(const std::string& data, size_t& pos);
    
    // 跳过CRLF
    static void skipCRLF(const std::string& data, size_t& pos);
    
    // 读取直到CRLF
    static std::string readUntilCRLF(const std::string& data, size_t& pos);
};

} // namespace dkv