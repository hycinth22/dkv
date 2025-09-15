#pragma once

#include "dkv_core.hpp"
#include "dkv_storage.hpp"
#include <fstream>
#include <string>

namespace dkv {
class StorageEngine;

// RDB文件格式的魔数和版本号
constexpr const char* RDB_MAGIC_STRING = "REDIS0009";
constexpr uint32_t RDB_VERSION = 9;

// RDB文件操作相关函数
class RDBPersistence {
public:
    // 将存储引擎的数据保存到RDB文件
    static bool saveToFile(StorageEngine* storage_engine, const std::string& filename);
    
    // 从RDB文件加载数据到存储引擎
    static bool loadFromFile(StorageEngine* storage_engine, const std::string& filename);
    
private:
    // 写入RDB文件头部
    static bool writeHeader(std::ofstream& file);
    
    // 写入单个键值对
    static bool writeKeyValue(std::ofstream& file, const Key& key, DataItem* item);
    
    // 读取RDB文件头部
    static bool readHeader(std::ifstream& file);
    
    // 读取单个键值对
    static bool readKeyValue(std::ifstream& file, StorageEngine* storage_engine);
    
    // 写入字符串（长度前缀）
    static void writeString(std::ofstream& file, const std::string& str);
    
    // 读取字符串（长度前缀）
    static std::string readString(std::ifstream& file);
    
    // 写入整数
    static void writeInt(std::ofstream& file, int64_t value);
    
    // 读取整数
    static int64_t readInt(std::ifstream& file);
};

} // namespace dkv