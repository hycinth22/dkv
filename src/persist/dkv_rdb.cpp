#include "persist/dkv_rdb.hpp"
#include "dkv_datatypes.hpp"
#include "dkv_utils.hpp"
#include "dkv_logger.hpp"
#include <iostream>
#include <chrono>
#include <sstream>
#include <cstring>

namespace dkv {

// 保存数据到RDB文件
// TODO: RDB保存已提交的事务数据项，如果最新版本未提交，则查询UNDOLOG
bool RDBPersistence::saveToFile(StorageEngine* storage_engine, const std::string& filename) {
    if (!storage_engine) {
        DKV_LOG_ERROR("Error: Storage engine is null");
        return false;
    }
    
    std::ofstream file(filename, std::ios::binary);
    if (!file.is_open()) {
        DKV_LOG_ERROR("Error: Failed to open file ", filename.c_str(), " for writing");
        return false;
    }
    
    // 写入RDB文件头部
    if (!writeHeader(file)) {
        file.close();
        return false;
    }
    
    // 获取所有未过期的键
    std::vector<Key> keys = storage_engine->keys();
    
    // 写入键值对数量
    writeInt(file, static_cast<int64_t>(keys.size()));
    
    // 遍历所有键值对并写入文件
    for (const auto& key : keys) {
        DataItem* item = storage_engine->getDataItem(NO_TX, key);
        if (item) {
            if (!writeKeyValue(file, key, item)) {
                file.close();
                return false;
            }
        }
    }
    
    file.close();
    DKV_LOG_INFO("Successfully saved data to RDB file: ", filename.c_str());
    return true;
}

// 从RDB文件加载数据
bool RDBPersistence::loadFromFile(StorageEngine* storage_engine, const std::string& filename) {
    if (!storage_engine) {
        DKV_LOG_ERROR("Error: Storage engine is null");
        return false;
    }
    
    std::ifstream file(filename, std::ios::binary);
    if (!file.is_open()) {
        DKV_LOG_ERROR("Error: Failed to open file ", filename.c_str(), " for reading");
        return false;
    }
    
    // 读取RDB文件头部
    if (!readHeader(file)) {
        file.close();
        return false;
    }
    
    // 读取键值对数量
    int64_t key_count = readInt(file);
    
    // 读取所有键值对
    for (int64_t i = 0; i < key_count; ++i) {
        if (!readKeyValue(file, storage_engine)) {
            file.close();
            return false;
        }
    }
    
    file.close();
    DKV_LOG_INFO("Successfully loaded data from RDB file: ", filename.c_str());
    return true;
}

// 写入RDB文件头部
bool RDBPersistence::writeHeader(std::ofstream& file) {
    // 写入魔数
    file.write(RDB_MAGIC_STRING, strlen(RDB_MAGIC_STRING));
    
    // 写入版本号
    writeInt(file, RDB_VERSION);
    
    return file.good();
}

// 读取RDB文件头部
bool RDBPersistence::readHeader(std::ifstream& file) {
    // 读取魔数
    char magic[10];
    file.read(magic, 9);
    magic[9] = '\0';
    
    if (strcmp(magic, RDB_MAGIC_STRING) != 0) {
        DKV_LOG_ERROR("Error: Invalid RDB file format");
        return false;
    }
    
    // 读取版本号
    uint32_t version = static_cast<uint32_t>(readInt(file));
    if (version != RDB_VERSION) {
        DKV_LOG_ERROR("Error: Unsupported RDB version: ", version, ", expected:", RDB_VERSION);
        return false;
    }
    
    return true;
}

// 写入单个键值对
bool RDBPersistence::writeKeyValue(std::ofstream& file, const Key& key, DataItem* item) {
    if (!item) {
        return false;
    }
    
    // 写入数据类型
    writeInt(file, static_cast<int64_t>(item->getType()));
    
    // 写入键
    writeString(file, key);
    
    // 写入过期时间信息
    writeInt(file, item->hasExpiration() ? 1 : 0);
    if (item->hasExpiration()) {
        auto duration = item->getExpiration().time_since_epoch();
        auto seconds = std::chrono::duration_cast<std::chrono::seconds>(duration).count();
        writeInt(file, seconds);
    }
    
    // 写入序列化的数据
    std::string serialized_data = item->serialize();
    writeString(file, serialized_data);
    
    return file.good();
}

// 读取单个键值对
bool RDBPersistence::readKeyValue(std::ifstream& file, StorageEngine* storage_engine) {
    // 读取数据类型
    int64_t type_int = readInt(file);
    DataType type = static_cast<DataType>(type_int);
    
    // 读取键
    std::string key = readString(file);
    
    // 读取过期时间信息
    int64_t has_expiration = readInt(file);
    Timestamp expire_time;
    bool has_expire = (has_expiration == 1);
    if (has_expire) {
        int64_t seconds = readInt(file);
        expire_time = Timestamp(std::chrono::seconds(seconds));
    }
    
    // 读取序列化的数据
    std::string serialized_data = readString(file);
    
    // 根据数据类型创建相应的DataItem并调用deserialize
    std::unique_ptr<DataItem> item;
    switch (type) {
        case DataType::STRING:
            item = std::make_unique<StringItem>();
            break;
        case DataType::HASH:
            item = std::make_unique<HashItem>();
            break;
        case DataType::LIST:
            item = std::make_unique<ListItem>();
            break;
        case DataType::SET:
            item = std::make_unique<SetItem>();
            break;
        default:
            return false;
    }
    
    // 调用deserialize解析数据
    item->deserialize(serialized_data);
    
    // 设置过期时间
    if (has_expire) {
        item->setExpiration(expire_time);
    }
    
    if (!item) {
        DKV_LOG_ERROR("Error: Failed to create DataItem of type ", static_cast<int>(type));
        return false;
    }
    
    // 将数据项添加到存储引擎
    // 注意：这里需要根据不同的数据类型调用不同的方法
    switch (type) {
        case DataType::STRING: {
            auto* string_item = dynamic_cast<StringItem*>(item.get());
            if (string_item) {
                if (has_expire) {
                    auto now = Utils::getCurrentTime();
                    auto duration = std::chrono::duration_cast<std::chrono::seconds>(expire_time - now).count();
                    if (duration > 0) {
                        storage_engine->set(NO_TX, key, string_item->getValue(), duration);
                    }
                } else {
                    storage_engine->set(NO_TX, key, string_item->getValue());
                }
            }
            break;
        }
        case DataType::HASH: {
            auto* hash_item = dynamic_cast<HashItem*>(item.get());
            if (hash_item) {
                auto all_fields = hash_item->getAll();
                for (const auto& [field, value] : all_fields) {
                    storage_engine->hset(NO_TX, key, field, value);
                }
                if (has_expire) {
                    auto now = Utils::getCurrentTime();
                    auto duration = std::chrono::duration_cast<std::chrono::seconds>(expire_time - now).count();
                    if (duration > 0) {
                        storage_engine->expire(NO_TX, key, duration);
                    }
                }
            }
            break;
        }
        case DataType::LIST: {
            auto* list_item = dynamic_cast<ListItem*>(item.get());
            if (list_item) {
                auto elements = list_item->lrange(0, -1);
                for (const auto& element : elements) {
                    storage_engine->rpush(NO_TX, key, element);
                }
                if (has_expire) {
                    auto now = Utils::getCurrentTime();
                    auto duration = std::chrono::duration_cast<std::chrono::seconds>(expire_time - now).count();
                    if (duration > 0) {
                        storage_engine->expire(NO_TX, key, duration);
                    }
                }
            }
            break;
        }
        case DataType::SET: {
            auto* set_item = dynamic_cast<SetItem*>(item.get());
            if (set_item) {
                auto members = set_item->smembers();
                storage_engine->sadd(NO_TX, key, members);
                if (has_expire) {
                    auto now = Utils::getCurrentTime();
                    auto duration = std::chrono::duration_cast<std::chrono::seconds>(expire_time - now).count();
                    if (duration > 0) {
                        storage_engine->expire(NO_TX, key, duration);
                    }
                }
            }
            break;
        }
        default:
            DKV_LOG_WARNING("Warning: Unknown data type: ", static_cast<int>(type));
            break;
    }
    
    return true;
}

// 写入字符串（长度前缀）
void RDBPersistence::writeString(std::ofstream& file, const std::string& str) {
    // 写入字符串长度
    writeInt(file, static_cast<int64_t>(str.length()));
    
    // 写入字符串内容
    file.write(str.c_str(), str.length());
}

// 读取字符串（长度前缀）
std::string RDBPersistence::readString(std::ifstream& file) {
    // 读取字符串长度
    int64_t length = readInt(file);
    
    // 读取字符串内容
    std::string str(length, '\0');
    file.read(&str[0], length);
    
    return str;
}

// 写入整数
void RDBPersistence::writeInt(std::ofstream& file, int64_t value) {
    file.write(reinterpret_cast<const char*>(&value), sizeof(value));
}

// 读取整数
int64_t RDBPersistence::readInt(std::ifstream& file) {
    int64_t value;
    file.read(reinterpret_cast<char*>(&value), sizeof(value));
    return value;
}

} // namespace dkv