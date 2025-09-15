#pragma once

#include <string>
#include <chrono>
#include "dkv_core.hpp"

namespace dkv {

// 工具函数
class Utils {
public:
    // 将字符串转换为命令类型
    static CommandType stringToCommandType(const std::string& cmd);
    
    // 将命令类型转换为字符串
    static std::string commandTypeToString(CommandType type);
    
    // 获取当前时间戳
    static Timestamp getCurrentTime();
    
    // 检查字符串是否为数字
    static bool isNumeric(const std::string& str);
    
    // 字符串转整数
    static int64_t stringToInt(const std::string& str);
    
    // 整数转字符串
    static std::string intToString(int64_t value);
};

} // namespace dkv