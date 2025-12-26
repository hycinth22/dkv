#include "net/dkv_resp.hpp"
#include "dkv_utils.hpp"
#include <iostream>
#include <sstream>
#include <cstring>
#include <algorithm>

namespace dkv {

// RESPProtocol 实现

Command RESPProtocol::parseCommand(const std::string& data, size_t& pos) {
    if (data.empty()) {
        return Command();
    }
    
    // 处理简单字符串命令
    if (pos < data.length() && data[pos] == '+') {
        std::string cmd = readUntilCRLF(data, pos);
        CommandType type = Utils::stringToCommandType(cmd);
        return Command(type, {});
    }
    // 处理数组类型
    else if (pos < data.length() && data[pos] == '*') {
        auto args = parseArray(data, pos);
        if (!args.empty()) {
            CommandType type = Utils::stringToCommandType(args[0]);
            std::vector<std::string> command_args(args.begin() + 1, args.end());
            return Command(type, command_args);
        }
    }
    // 处理其他类型
    else if (pos < data.length()) {
        // 尝试解析为简单命令
        std::string cmd = readUntilCRLF(data, pos);
        // 简单的命令解析
        size_t space_pos = cmd.find(' ');
        if (space_pos != std::string::npos) {
            std::string cmd_name = cmd.substr(0, space_pos);
            CommandType type = Utils::stringToCommandType(cmd_name);
            
            // 解析参数
            std::vector<std::string> args;
            size_t start = space_pos + 1;
            while (start < cmd.length()) {
                size_t end = cmd.find(' ', start);
                if (end == std::string::npos) {
                    args.push_back(cmd.substr(start));
                    break;
                }
                args.push_back(cmd.substr(start, end - start));
                start = end + 1;
            }
            
            return Command(type, args);
        } else {
            CommandType type = Utils::stringToCommandType(cmd);
            return Command(type, {});
        }
    }
    
    return Command();
}

std::string RESPProtocol::serializeResponse(const Response& response) {
    switch (response.status) {
        case ResponseStatus::OK:
            return serializeSimpleString(response.message.empty() ? "OK" : response.message);
        case ResponseStatus::ERROR:
            return serializeError(response.message);
        case ResponseStatus::NOT_FOUND:
            return serializeNull();
        case ResponseStatus::INVALID_COMMAND:
            return serializeError("Invalid command");
        default:
            return serializeError("Unknown error");
    }
}

std::string RESPProtocol::serializeSimpleString(const std::string& str) {
    return "+" + str + "\r\n";
}

std::string RESPProtocol::serializeError(const std::string& error) {
    return "-" + error + "\r\n";
}

std::string RESPProtocol::serializeInteger(int64_t value) {
    return ":" + std::to_string(value) + "\r\n";
}

std::string RESPProtocol::serializeBulkString(const std::string& str) {
    if (str.empty()) {
        return "$-1\r\n"; // NULL bulk string
    }
    return "$" + std::to_string(str.length()) + "\r\n" + str + "\r\n";
}

std::string RESPProtocol::serializeArray(const std::vector<std::string>& array) {
    std::string result = "*" + std::to_string(array.size()) + "\r\n";
    for (const auto& item : array) {
        result += serializeBulkString(item);
    }
    return result;
}

std::string RESPProtocol::serializeNull() {
    return "$-1\r\n";
}

std::vector<std::string> RESPProtocol::parseArray(const std::string& data, size_t& pos) {
    std::vector<std::string> result;
    
    if (pos >= data.length() || data[pos] != '*') {
        return result;
    }
    
    pos++; // 跳过 '*'
    std::string count_str = readUntilCRLF(data, pos);
    
    // 处理特殊情况：*-1 表示空数组
    if (count_str == "-1") {
        return result; // 返回空数组
    }
    
    int count = std::stoi(count_str);
    
    if (count < 0) {
        return result; // 负数表示错误
    }
    
    result.reserve(count);
    
    for (int i = 0; i < count; i++) {
        if (pos >= data.length()) {
            break;
        }
        
        if (data[pos] == '+') {
            pos++; // 跳过 '+'
            std::string simple_str = readUntilCRLF(data, pos);
            result.push_back(simple_str);
        } else if (data[pos] == '-') {
            pos++; // 跳过 '-'
            std::string error_str = readUntilCRLF(data, pos);
            result.push_back(error_str);
        } else if (data[pos] == ':') {
            pos++; // 跳过 ':'
            std::string integer_str = readUntilCRLF(data, pos);
            result.push_back(integer_str);
        } else if (data[pos] == '*') {
            // 嵌套数组
            auto nested_array = parseArray(data, pos);
            std::string nested_array_str = "[";
            for (size_t j = 0; j < nested_array.size(); j++) {
                nested_array_str += nested_array[j];
                if (j < nested_array.size() - 1) {
                    nested_array_str += ", ";
                }
            }
            nested_array_str += "]";
            result.push_back(nested_array_str);
        } else if (data[pos] == '$') {
            std::string bulk_str = parseBulkString(data, pos);
            result.push_back(bulk_str);
        } else {
            // 简单字符串
            std::string simple_str = readUntilCRLF(data, pos);
            result.push_back(simple_str);
        }
    }
    
    return result;
}

std::string RESPProtocol::parseBulkString(const std::string& data, size_t& pos) {
    if (pos >= data.length() || data[pos] != '$') {
        return "";
    }
    
    pos++; // 跳过 '$'
    std::string length_str = readUntilCRLF(data, pos);
    int length = std::stoi(length_str);
    
    if (length == -1) {
        return ""; // NULL bulk string
    }
    
    if (pos + length >= data.length()) {
        return "";
    }
    
    std::string result = data.substr(pos, length);
    pos += length;
    skipCRLF(data, pos);
    
    return result;
}

void RESPProtocol::skipCRLF(const std::string& data, size_t& pos) {
    if (pos + 1 < data.length() && data[pos] == '\r' && data[pos + 1] == '\n') {
        pos += 2;
    }
}

std::string RESPProtocol::readUntilCRLF(const std::string& data, size_t& pos) {
    size_t start = pos;
    while (pos < data.length() && !(pos + 1 < data.length() && data[pos] == '\r' && data[pos + 1] == '\n')) {
        pos++;
    }
    
    std::string result = data.substr(start, pos - start);
    skipCRLF(data, pos);
    
    return result;
}

} // namespace dkv