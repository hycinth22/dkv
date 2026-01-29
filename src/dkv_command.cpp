#include <iostream>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <cstring>
#include "dkv_core.hpp"
#include "dkv_utils.hpp"

namespace dkv {

std::string Command::desc() const {
    return Utils::commandTypeToString(type) + " " + args[0];
}

void Command::serialize(std::vector<char>& buffer) const {
    // 1. 序列化CommandType类型
    uint32_t commandType = htonl(static_cast<uint32_t>(type));
    buffer.insert(buffer.end(), (char*)&commandType, (char*)&commandType + sizeof(commandType));
    
    // 2. 序列化args向量大小
    uint32_t argsSize = htonl(args.size());
    buffer.insert(buffer.end(), (char*)&argsSize, (char*)&argsSize + sizeof(argsSize));
    
    // 3. 序列化args向量中的每个字符串
    for (const auto& arg : args) {
        // 序列化字符串大小
        uint32_t argSize = htonl(arg.size());
        buffer.insert(buffer.end(), (char*)&argSize, (char*)&argSize + sizeof(argSize));
        
        // 序列化字符串内容
        buffer.insert(buffer.end(), arg.begin(), arg.end());
    }
    
    // 4. 序列化timestamp
    auto timestamp_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(timestamp.time_since_epoch()).count();
    uint64_t timestamp_hton = htonll(timestamp_ns);
    buffer.insert(buffer.end(), (char*)&timestamp_hton, (char*)&timestamp_hton + sizeof(timestamp_hton));
}

bool Command::deserialize(const std::vector<char>& buffer) {
    // 1. 反序列化CommandType类型
    uint32_t commandType = ntohl(*(uint32_t*)buffer.data());
    type = static_cast<CommandType>(commandType);

    // 2. 反序列化args向量大小
    uint32_t argsSize = ntohl(*(uint32_t*)(buffer.data() + sizeof(commandType)));
    args.resize(argsSize);
    
    // 3. 反序列化args向量中的每个字符串
    size_t offset = sizeof(commandType) + sizeof(argsSize);
    for (auto& arg : args) {
        // 反序列化字符串大小
        uint32_t argSize = ntohl(*(uint32_t*)(buffer.data() + offset));
        offset += sizeof(argSize);
        
        // 反序列化字符串内容
        arg.resize(argSize);
        memcpy(arg.data(), buffer.data() + offset, argSize);
        offset += argSize;
    }
    
    // 4. 反序列化timestamp
    if (offset + sizeof(uint64_t) <= buffer.size()) {
        uint64_t timestamp_ntoh = ntohll(*(uint64_t*)(buffer.data() + offset));
        timestamp = std::chrono::system_clock::time_point(std::chrono::nanoseconds(timestamp_ntoh));
    }
    return true;
}

std::ostream& Command::write(std::ostream& os) const {
    // 先输出命令类型和参数数量
    os << static_cast<int>(type) << " " << args.size() << " ";
    
    // 保存命令参数
    for (const auto& arg : args) {
        os << arg.size() << " " << arg << " ";
    }
    
    // 输出timestamp
    auto timestamp_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(timestamp.time_since_epoch()).count();
    os << timestamp_ns << " ";
    return os;
}

std::istream& Command::read(std::istream& is) {
    // 先读取命令类型和参数数量
    int t;
    size_t args_size;
    is >> t >> args_size;
    type = static_cast<CommandType>(t);
    
    // 读取参数
    args.resize(args_size);
    for (auto& arg : args) {
        size_t size;
        is >> size;
        arg.resize(size);
        is.read(arg.data(), size);
    }
    
    // 读取timestamp
    uint64_t timestamp_ns;
    is >> timestamp_ns;
    timestamp = std::chrono::system_clock::time_point(std::chrono::nanoseconds(timestamp_ns));
    return is;
}

size_t Command::PersistBytes() const {
    size_t size = 0;
    // 命令类型
    size += sizeof(type);
    // 参数数量
    size += sizeof(args.size());
    // 每个参数的大小
    for (const auto& arg : args) {
        size += sizeof(arg.size());
        size += arg.size();
    }
    // timestamp大小
    size += sizeof(uint64_t);
    return size;
}

}