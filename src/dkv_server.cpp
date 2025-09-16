#include "dkv_server.hpp"
#include "dkv_memory_allocator.hpp"
#include "dkv_logger.hpp"
#include <iostream>
#include <fstream>
#include <sstream>
#include <chrono>

namespace dkv {

DKVServer::DKVServer(int port, size_t num_sub_reactors, size_t num_workers) 
    : running_(false), cleanup_running_(false), 
      port_(port), max_memory_(0), num_sub_reactors_(num_sub_reactors), num_workers_(num_workers),
      enable_rdb_(true), rdb_filename_("dump.rdb"), rdb_save_interval_(3600), rdb_save_changes_(1000),
      rdb_changes_(0), last_save_time_(std::chrono::system_clock::now()), rdb_save_running_(false),
      enable_aof_(false), aof_filename_("appendonly.aof"), aof_fsync_policy_("everysec") {
}

DKVServer::~DKVServer() {
    stop();
}

uint16_t DKVServer::getPort() const {
    return port_;
}

bool DKVServer::start() {
    if (running_) {
        return true;
    }
    
    if (!initialize()) {
        return false;
    }
    
    // 初始化AOF组件
    if (enable_aof_) {
        DKV_LOG_INFO("初始化AOF持久化");
        aof_persistence_ = std::make_unique<AOFPersistence>();
        
        // 设置fsync策略
        AOFPersistence::FsyncPolicy policy = AOFPersistence::FsyncPolicy::EVERYSEC;
        if (aof_fsync_policy_ == "always") {
            policy = AOFPersistence::FsyncPolicy::ALWAYS;
        } else if (aof_fsync_policy_ == "never") {
            policy = AOFPersistence::FsyncPolicy::NEVER;
        }
        
        // 初始化AOF文件
        if (aof_persistence_->initialize(aof_filename_, policy)) {
            // 从AOF文件加载数据
            if (aof_persistence_->loadFromFile(this)) {
                DKV_LOG_INFO("成功从AOF文件加载数据");
                // AOF加载成功后，不需要再加载RDB
            } else {
                DKV_LOG_INFO("AOF文件加载失败，尝试从RDB文件加载数据");
                loadRDBFromConfig();
            }
        }
    } else {
        DKV_LOG_INFO("AOF持久化已禁用");
        // 尝试从RDB文件加载数据
        loadRDBFromConfig();
    }
    
    running_ = true;
    cleanup_running_ = true;
    
    // 启动清理线程
    cleanup_thread_ = std::thread(&DKVServer::cleanupExpiredKeys, this);
    
    // 启动RDB自动保存线程
    if (enable_rdb_) {
        rdb_save_running_ = false;
        rdb_save_thread_ = std::thread(&DKVServer::rdbAutoSaveThread, this);
    }
    
    // 启动网络服务器
    if (!network_server_->start()) {
        DKV_LOG_ERROR("启动网络服务失败");
        stop();
        return false;
    }
    
    DKV_LOG_INFO("DKV服务启动成功");
    return true;
}

void DKVServer::loadRDBFromConfig() {
    // 检查是否启用了RDB持久化
    if (!enable_rdb_) {
        DKV_LOG_INFO("RDB持久化已禁用");
        return;
    }
    
    // 使用配置的RDB文件名
    std::string rdb_file = rdb_filename_;
    
    if (storage_engine_ && !rdb_file.empty()) {
        if (!storage_engine_->loadRDB(rdb_file)) {
            DKV_LOG_WARNING("无法加载RDB文件 ", rdb_file.c_str(), "，可能是文件不存在或格式不正确");
        } else {
            DKV_LOG_INFO("成功从RDB文件 ", rdb_file.c_str(), " 加载数据");
            
            // 更新上次保存时间和重置变更计数
            last_save_time_ = std::chrono::system_clock::now();
            rdb_changes_ = 0;
        }
    }
}

void DKVServer::stop() {
    if (!running_.load()) {
        return;
    }
    
    DKV_LOG_INFO("开始停止DKV服务器");
    running_ = false;
    cleanup_running_ = false;
    
    // 保存数据到RDB文件
    saveRDBFromConfig();
    
    // 关闭AOF文件
    if (aof_persistence_) {
        aof_persistence_->close();
        aof_persistence_.reset();
    }
    
    // 等待工作线程结束
    DKV_LOG_INFO("等待工作线程结束");
    if (worker_pool_) {
        worker_pool_->stop();
    }

    // 停止网络服务器
    DKV_LOG_INFO("停止网络服务");
    if (network_server_) {
        network_server_->stop();
    }
    
    // 等待清理线程结束
    DKV_LOG_INFO("等待清理线程结束");
    if (cleanup_thread_.joinable()) {
        cleanup_thread_.join();
    }
    
    // 停止RDB自动保存线程
    DKV_LOG_INFO("等待RDB自动保存线程结束");
    rdb_save_running_ = false;
    if (rdb_save_thread_.joinable()) {
        rdb_save_thread_.join();
    }
    
    DKV_LOG_INFO("DKV服务已停止");
}

void DKVServer::saveRDBFromConfig() {
    // 检查是否启用了RDB持久化
    if (!enable_rdb_) {
        DKV_LOG_INFO("RDB持久化已禁用");
        return;
    }
    
    // 使用配置的RDB文件名
    std::string rdb_file = rdb_filename_;
    
    if (storage_engine_ && !rdb_file.empty()) {
        if (!storage_engine_->saveRDB(rdb_file)) {
            DKV_LOG_WARNING("无法保存RDB文件 ", rdb_file.c_str());
        } else {
            DKV_LOG_INFO("成功将数据保存到RDB文件 ", rdb_file.c_str());
            
            // 更新上次保存时间和重置变更计数
                last_save_time_ = std::chrono::system_clock::now();
                rdb_changes_ = 0;
        }
    }
}

bool DKVServer::loadConfig(const std::string& config_file) {
    config_file_ = config_file;
    return parseConfigFile(config_file);
}

void DKVServer::setPort(int port) {
    port_ = port;
}

void DKVServer::setMaxMemory(size_t max_memory) {
    max_memory_ = max_memory;
}

size_t DKVServer::getKeyCount() const {
    return storage_engine_ ? storage_engine_->size() : 0;
}

uint64_t DKVServer::getTotalKeys() const {
    return storage_engine_ ? storage_engine_->getTotalKeys() : 0;
}

uint64_t DKVServer::getExpiredKeys() const {
    return storage_engine_ ? storage_engine_->getExpiredKeys() : 0;
}

bool DKVServer::isRunning() const {
    return running_.load();
}

size_t DKVServer::getMemoryUsage() const {
    return dkv::MemoryAllocator::getInstance().getCurrentUsage();
}

size_t DKVServer::getMaxMemory() const {
    return max_memory_;
}

// RDB持久化配置方法实现
void DKVServer::setRDBEnabled(bool enabled) {
    enable_rdb_ = enabled;
}

void DKVServer::setRDBFilename(const std::string& filename) {
    rdb_filename_ = filename;
}

void DKVServer::setRDBSaveInterval(uint64_t interval) {
    rdb_save_interval_ = interval;
}

void DKVServer::setRDBSaveChanges(uint64_t changes) {
    rdb_save_changes_ = changes;
}

// AOF持久化配置方法实现
void DKVServer::setAOFEnabled(bool enabled) {
    enable_aof_ = enabled;
}

void DKVServer::setAOFFilename(const std::string& filename) {
    aof_filename_ = filename;
}

void DKVServer::setAOFFsyncPolicy(const std::string& policy) {
    aof_fsync_policy_ = policy;
}

bool DKVServer::initialize() {
    DKV_LOG_INFO("开始初始化DKV服务器");
    
    // 创建存储引擎实例
    DKV_LOG_DEBUG("创建存储引擎实例");
    storage_engine_ = std::make_unique<StorageEngine>();
    
    // 创建工作线程池
    DKV_LOG_DEBUG("创建工作线程池，线程数: ", num_workers_);
    worker_pool_ = std::make_unique<WorkerThreadPool>(this, num_workers_);

    // 创建网络服务实例（使用多线程Reactor模式）
    DKV_LOG_DEBUG("创建网络服务实例，端口: ", port_, ", SubReactor数量: ", num_sub_reactors_);
    network_server_ = std::make_unique<NetworkServer>(worker_pool_.get(), port_, num_sub_reactors_);
    
    DKV_LOG_INFO("DKV服务器初始化完成");
    return true;
}

Response DKVServer::executeCommand(const Command& command) {
    if (!storage_engine_) {
        return Response(ResponseStatus::ERROR, "Storage engine not initialized");
    }
    
    // 检查是否是可能分配内存的命令
    bool mayAllocateMemory = true;
    switch (command.type) {
        // readonly list
        case CommandType::GET:
        case CommandType::EXISTS:
        case CommandType::SCARD:
        case CommandType::DBSIZE:
        case CommandType::INFO:
        case CommandType::SHUTDOWN:
            mayAllocateMemory = false;
            break;
        default:
            mayAllocateMemory = true;
            break;
    }
    
    // 如果是可能分配内存的命令，且设置了最大内存限制，则检查内存使用情况
    if (mayAllocateMemory && max_memory_ > 0) {
        size_t currentUsage = getMemoryUsage();
        if (currentUsage >= max_memory_) {
            DKV_LOG_WARNING("内存使用已达到上限，拒绝执行命令");
            return Response(ResponseStatus::ERROR, "OOM command not allowed when used memory > 'maxmemory'");
        }
    }
    
    switch (command.type) {
        case CommandType::SET: {
            if (command.args.size() < 2) {
                return Response(ResponseStatus::ERROR, "SET命令需要至少2个参数");
            }
            
            std::string key = command.args[0];
            std::string value = command.args[1];
            bool success;
            
            if (command.args.size() >= 4 && command.args[2] == "PX") {
                // 支持 PX 参数设置毫秒过期时间
                try {
                    int64_t ms = std::stoll(command.args[3]);
                    success = storage_engine_->set(key, value, ms / 1000);
                    DKV_LOG_DEBUG("设置键 ", key.c_str(), " 带有过期时间 ", ms, " 毫秒");
                } catch (const std::invalid_argument&) {
                    return Response(ResponseStatus::ERROR, "无效的过期时间");
                }
            } else if (command.args.size() >= 4 && command.args[2] == "EX") {
                // 支持 EX 参数设置秒过期时间
                try {
                    int64_t seconds = std::stoll(command.args[3]);
                    success = storage_engine_->set(key, value, seconds);
                    DKV_LOG_DEBUG("设置键 ", key.c_str(), " 带有过期时间 ", seconds, " 秒");
                } catch (const std::invalid_argument&) {
                    return Response(ResponseStatus::ERROR, "无效的过期时间");
                }
            } else {
                success = storage_engine_->set(key, value);
                DKV_LOG_DEBUG("设置键 ", key.c_str());
            }
            
            if (success) {
                incDirty();
                // 如果启用了AOF持久化，将命令写入AOF文件
                if (enable_aof_ && aof_persistence_) {
                    aof_persistence_->appendCommand(command);
                }
                return Response(ResponseStatus::OK, "OK");
            } else {
                DKV_LOG_ERROR("设置键值失败: ", key.c_str());
                return Response(ResponseStatus::ERROR, "设置键值失败");
            }
        }
        
        case CommandType::GET: {
            if (command.args.empty()) {
                return Response(ResponseStatus::ERROR, "GET命令需要1个参数");
            }
            std::string key = command.args[0];
            DKV_LOG_DEBUG("获取键 ", key.c_str(), " 的值");
            std::string value = storage_engine_->get(key);
            if (value.empty()) {
                DKV_LOG_DEBUG("键 ", key.c_str(), " 不存在");
                return Response(ResponseStatus::NOT_FOUND);
            }
            DKV_LOG_DEBUG("获取键 ", key.c_str(), " 的值成功");
            return Response(ResponseStatus::OK, "", value);
        }
        
        case CommandType::DEL: {
            if (command.args.empty()) {
                return Response(ResponseStatus::ERROR, "DEL命令需要至少1个参数");
            }
            
            int deleted_count = 0;
            for (const auto& key : command.args) {
                bool success = storage_engine_->del(key);
                if (success) {
                    deleted_count++;
                    incDirty();
                    DKV_LOG_DEBUG("删除键 ", key.c_str(), " 成功");
                } else {
                    DKV_LOG_DEBUG("键 ", key.c_str(), " 不存在，删除失败");
                }
            }
            
            if (deleted_count > 0) {
                DKV_LOG_DEBUG("成功删除 ", deleted_count, " 个键");
                // 如果启用了AOF持久化，将命令写入AOF文件
                if (enable_aof_ && aof_persistence_) {
                    aof_persistence_->appendCommand(command);
                }
            }
            
            return Response(ResponseStatus::OK, "", std::to_string(deleted_count));
        }
        
        case CommandType::EXISTS: {
            if (command.args.empty()) {
                return Response(ResponseStatus::ERROR, "EXISTS命令需要至少1个参数");
            }
            
            int exists_count = 0;
            for (const auto& key : command.args) {
                if (storage_engine_->exists(key)) {
                    exists_count++;
                }
            }
            
            return Response(ResponseStatus::OK, "", std::to_string(exists_count));
        }
        
        case CommandType::INCR: {
            if (command.args.empty()) {
                return Response(ResponseStatus::ERROR, "INCR命令需要1个参数");
            }
            int64_t value = storage_engine_->incr(command.args[0]);
            incDirty();
            // 如果启用了AOF持久化，将命令写入AOF文件
            if (enable_aof_ && aof_persistence_) {
                aof_persistence_->appendCommand(command);
            }
            return Response(ResponseStatus::OK, "", std::to_string(value));
        }
        
        case CommandType::DECR: {
            if (command.args.empty()) {
                return Response(ResponseStatus::ERROR, "DECR命令需要1个参数");
            }
            int64_t value = storage_engine_->decr(command.args[0]);
            incDirty();
            // 如果启用了AOF持久化，将命令写入AOF文件
            if (enable_aof_ && aof_persistence_) {
                aof_persistence_->appendCommand(command);
            }
            return Response(ResponseStatus::OK, "", std::to_string(value));
        }
        
        case CommandType::EXPIRE: {
            if (command.args.size() < 2) {
                return Response(ResponseStatus::ERROR, "EXPIRE命令需要2个参数");
            }
            try {
                int64_t seconds = std::stoll(command.args[1]);
                bool success = storage_engine_->expire(command.args[0], seconds);
                
                if (success) {
                    incDirty();
                    // 如果启用了AOF持久化，将命令写入AOF文件
                    if (enable_aof_ && aof_persistence_) {
                        aof_persistence_->appendCommand(command);
                    }
                    return Response(ResponseStatus::OK, "", "1"); // 1表示成功设置过期时间
                } else {
                    return Response(ResponseStatus::OK, "", "0"); // 0表示未设置过期时间（键不存在或操作被跳过）
                }
            } catch (const std::invalid_argument&) {
                return Response(ResponseStatus::ERROR, "无效的过期时间");
            }
        }
        
        case CommandType::TTL: {
            if (command.args.empty()) {
                return Response(ResponseStatus::ERROR, "TTL命令需要1个参数");
            }
            int64_t ttl = storage_engine_->ttl(command.args[0]);
            return Response(ResponseStatus::OK, "", std::to_string(ttl));
        }
        
        // 哈希命令
        case CommandType::HSET: {
            if (command.args.size() < 3 || command.args.size() % 2 == 0) {
                return Response(ResponseStatus::ERROR, "HSET命令需要奇数个参数(至少3个)");
            }
            
            int added_count = 0;
            const std::string& key = command.args[0];
            
            // 遍历所有字段值对
            for (size_t i = 1; i < command.args.size(); i += 2) {
                const std::string& field = command.args[i];
                const std::string& value = command.args[i + 1];
                
                bool success = storage_engine_->hset(key, field, value);
                if (success) {
                    added_count++;
                    incDirty();
                }
            }
            
            // 如果启用了AOF持久化且有更新，将命令写入AOF文件
            if (enable_aof_ && aof_persistence_ && added_count > 0) {
                aof_persistence_->appendCommand(command);
            }
            
            return Response(ResponseStatus::OK, "", std::to_string(added_count));
        }
        
        case CommandType::HGET: {
            if (command.args.size() < 2) {
                return Response(ResponseStatus::ERROR, "HGET命令需要至少2个参数");
            }
            std::string value = storage_engine_->hget(command.args[0], command.args[1]);
            if (value.empty()) {
                return Response(ResponseStatus::NOT_FOUND);
            }
            return Response(ResponseStatus::OK, "", value);
        }
        
        case CommandType::HGETALL: {
            if (command.args.empty()) {
                return Response(ResponseStatus::ERROR, "HGETALL命令需要1个参数");
            }
            std::vector<std::pair<Value, Value>> all = storage_engine_->hgetall(command.args[0]);
            
            // 将结果转换为RESP协议数组格式
            std::vector<std::string> result;
            for (const auto& pair : all) {
                result.push_back(pair.first);
                result.push_back(pair.second);
            }
            
            Response response;
            response.status = ResponseStatus::OK;
            response.data = RESPProtocol::serializeArray(result);
            return response;
        }
        
        case CommandType::HDEL: {
            if (command.args.size() < 2) {
                return Response(ResponseStatus::ERROR, "HDEL命令需要至少2个参数");
            }
            int deleted_count = 0;
            const Key& key = command.args[0];
            
            for (size_t i = 1; i < command.args.size(); ++i) {
                const Value& field = command.args[i];
                bool success = storage_engine_->hdel(key, field);
                if (success) {
                    deleted_count++;
                }
            }
            
            if (deleted_count > 0) {
                incDirty();
                // 如果启用了AOF持久化，将命令写入AOF文件
                if (enable_aof_ && aof_persistence_) {
                    aof_persistence_->appendCommand(command);
                }
            }
            
            return Response(ResponseStatus::OK, "", std::to_string(deleted_count));
        }
        
        case CommandType::HEXISTS: {
            if (command.args.size() < 2) {
                return Response(ResponseStatus::ERROR, "HEXISTS命令需要至少2个参数");
            }
            bool exists = storage_engine_->hexists(command.args[0], command.args[1]);
            return Response(ResponseStatus::OK, "", exists ? "1" : "0");
        }
        
        case CommandType::HKEYS: {
            if (command.args.empty()) {
                return Response(ResponseStatus::ERROR, "HKEYS命令需要1个参数");
            }
            std::vector<Value> keys = storage_engine_->hkeys(command.args[0]);
            
            Response response;
            response.status = ResponseStatus::OK;
            response.data = RESPProtocol::serializeArray(keys);
            return response;
        }
        
        case CommandType::HVALS: {
            if (command.args.empty()) {
                return Response(ResponseStatus::ERROR, "HVALS命令需要1个参数");
            }
            std::vector<Value> values = storage_engine_->hvals(command.args[0]);
            
            Response response;
            response.status = ResponseStatus::OK;
            response.data = RESPProtocol::serializeArray(values);
            return response;
        }
        
        case CommandType::HLEN: {
            if (command.args.empty()) {
                return Response(ResponseStatus::ERROR, "HLEN命令需要1个参数");
            }
            size_t len = storage_engine_->hlen(command.args[0]);
            return Response(ResponseStatus::OK, "", std::to_string(len));
        }
        
        // 列表命令
        case CommandType::LPUSH: {
            if (command.args.size() < 2) {
                return Response(ResponseStatus::ERROR, "LPUSH命令需要至少2个参数");
            }
            size_t len = 0;
            const Key& key = command.args[0];
            
            // 处理多个值参数
            for (size_t i = 1; i < command.args.size(); ++i) {
                len = storage_engine_->lpush(key, command.args[i]);
            }
            
            incDirty();
            // 如果启用了AOF持久化，将命令写入AOF文件
            if (enable_aof_ && aof_persistence_) {
                aof_persistence_->appendCommand(command);
            }
            return Response(ResponseStatus::OK, "", std::to_string(len));
        }
        
        case CommandType::RPUSH: {
            if (command.args.size() < 2) {
                return Response(ResponseStatus::ERROR, "RPUSH命令需要至少2个参数");
            }
            size_t len = 0;
            const Key& key = command.args[0];
            
            // 处理多个值参数
            for (size_t i = 1; i < command.args.size(); ++i) {
                len = storage_engine_->rpush(key, command.args[i]);
            }
            
            incDirty();
            // 如果启用了AOF持久化，将命令写入AOF文件
            if (enable_aof_ && aof_persistence_) {
                aof_persistence_->appendCommand(command);
            }
            return Response(ResponseStatus::OK, "", std::to_string(len));
        }
        
        case CommandType::LPOP: {
            if (command.args.empty()) {
                return Response(ResponseStatus::ERROR, "LPOP命令需要至少1个参数");
            }
            
            const Key& key = command.args[0];
            
            // 只有键参数的情况 - 返回单个元素
            if (command.args.size() == 1) {
                std::string value = storage_engine_->lpop(key);
                if (value.empty()) {
                    return Response(ResponseStatus::NOT_FOUND);
                }
                
                incDirty();
                // 如果启用了AOF持久化，将命令写入AOF文件
                if (enable_aof_ && aof_persistence_) {
                    aof_persistence_->appendCommand(command);
                }
                return Response(ResponseStatus::OK, "", value);
            }
            
            // 有count参数的情况 - 返回元素列表
            try {
                size_t count = std::stoull(command.args[1]);
                std::vector<std::string> values;
                
                for (size_t i = 0; i < count; ++i) {
                    std::string value = storage_engine_->lpop(key);
                    if (value.empty()) {
                        break;  // 列表为空，停止弹出
                    }
                    values.push_back(value);
                }
                
                if (values.empty()) {
                    return Response(ResponseStatus::NOT_FOUND);
                }
                
                incDirty();
                // 如果启用了AOF持久化，将命令写入AOF文件
                if (enable_aof_ && aof_persistence_) {
                    aof_persistence_->appendCommand(command);
                }
                
                // 返回列表格式的响应
                Response response(ResponseStatus::OK);
                response.data = RESPProtocol::serializeArray(values);
                return response;
            } catch (const std::invalid_argument&) {
                return Response(ResponseStatus::ERROR, "count参数必须是正整数");
            } catch (const std::out_of_range&) {
                return Response(ResponseStatus::ERROR, "count参数太大");
            }
        }
        
        case CommandType::RPOP: {
            if (command.args.empty()) {
                return Response(ResponseStatus::ERROR, "RPOP命令需要至少1个参数");
            }
            
            const Key& key = command.args[0];
            
            // 只有键参数的情况 - 返回单个元素
            if (command.args.size() == 1) {
                std::string value = storage_engine_->rpop(key);
                if (value.empty()) {
                    return Response(ResponseStatus::NOT_FOUND);
                }
                
                incDirty();
                // 如果启用了AOF持久化，将命令写入AOF文件
                if (enable_aof_ && aof_persistence_) {
                    aof_persistence_->appendCommand(command);
                }
                return Response(ResponseStatus::OK, "", value);
            }
            
            // 有count参数的情况 - 返回元素列表
            try {
                size_t count = std::stoull(command.args[1]);
                std::vector<std::string> values;
                
                for (size_t i = 0; i < count; ++i) {
                    std::string value = storage_engine_->rpop(key);
                    if (value.empty()) {
                        break;  // 列表为空，停止弹出
                    }
                    values.push_back(value);
                }
                
                if (values.empty()) {
                    return Response(ResponseStatus::NOT_FOUND);
                }
                
                incDirty();
                // 如果启用了AOF持久化，将命令写入AOF文件
                if (enable_aof_ && aof_persistence_) {
                    aof_persistence_->appendCommand(command);
                }
                
                // 返回列表格式的响应
                Response response(ResponseStatus::OK);
                response.data = RESPProtocol::serializeArray(values);
                return response;
            } catch (const std::invalid_argument&) {
                return Response(ResponseStatus::ERROR, "count参数必须是正整数");
            } catch (const std::out_of_range&) {
                return Response(ResponseStatus::ERROR, "count参数太大");
            }
        }
        
        case CommandType::LLEN: {
            if (command.args.empty()) {
                return Response(ResponseStatus::ERROR, "LLEN命令需要1个参数");
            }
            size_t len = storage_engine_->llen(command.args[0]);
            return Response(ResponseStatus::OK, "", std::to_string(len));
        }
        
        case CommandType::LRANGE: {
            if (command.args.size() < 3) {
                return Response(ResponseStatus::ERROR, "LRANGE命令需要至少3个参数");
            }
            try {
                size_t start = std::stoull(command.args[1]);
                size_t stop = std::stoull(command.args[2]);
                std::vector<Value> values = storage_engine_->lrange(command.args[0], start, stop);
                
                Response response;
                response.status = ResponseStatus::OK;
                response.data = RESPProtocol::serializeArray(values);
                return response;
            } catch (const std::invalid_argument&) {
                return Response(ResponseStatus::ERROR, "无效的范围参数");
            }
        }
        
        // 集合命令
        case CommandType::SADD: {
            if (command.args.size() < 2) {
                return Response(ResponseStatus::ERROR, "SADD命令需要至少2个参数");
            }
            std::vector<Value> members(command.args.begin() + 1, command.args.end());
            size_t addedCount = storage_engine_->sadd(command.args[0], members);
            
            if (addedCount > 0) {
                incDirty();
                // 如果启用了AOF持久化，将命令写入AOF文件
                if (enable_aof_ && aof_persistence_) {
                    aof_persistence_->appendCommand(command);
                }
            }
            
            return Response(ResponseStatus::OK, "", std::to_string(addedCount));
        }
        
        case CommandType::SREM: {
            if (command.args.size() < 2) {
                return Response(ResponseStatus::ERROR, "SREM命令需要至少2个参数");
            }
            std::vector<Value> members(command.args.begin() + 1, command.args.end());
            size_t removedCount = storage_engine_->srem(command.args[0], members);
            
            if (removedCount > 0) {
                incDirty();
                // 如果启用了AOF持久化，将命令写入AOF文件
                if (enable_aof_ && aof_persistence_) {
                    aof_persistence_->appendCommand(command);
                }
            }
            
            return Response(ResponseStatus::OK, "", std::to_string(removedCount));
        }
        
        case CommandType::SMEMBERS: {
            if (command.args.empty()) {
                return Response(ResponseStatus::ERROR, "SMEMBERS命令需要1个参数");
            }
            std::vector<Value> members = storage_engine_->smembers(command.args[0]);
            
            Response response;
            response.status = ResponseStatus::OK;
            response.data = RESPProtocol::serializeArray(members);
            return response;
        }
        
        case CommandType::SISMEMBER: {
            if (command.args.size() < 2) {
                return Response(ResponseStatus::ERROR, "SISMEMBER命令需要至少2个参数");
            }
            bool is_member = storage_engine_->sismember(command.args[0], command.args[1]);
            return Response(ResponseStatus::OK, "", is_member ? "1" : "0");
        }
        
        case CommandType::SCARD: {
            if (command.args.empty()) {
                return Response(ResponseStatus::ERROR, "SCARD命令需要1个参数");
            }
            size_t count = storage_engine_->scard(command.args[0]);
            return Response(ResponseStatus::OK, "", std::to_string(count));
        }
        
        case CommandType::FLUSHDB: {
            storage_engine_->flush();
            incDirty();
            // 如果启用了AOF持久化，将命令写入AOF文件
            if (enable_aof_ && aof_persistence_) {
                aof_persistence_->appendCommand(command);
            }
            return Response(ResponseStatus::OK, "OK");
        }
        
        case CommandType::DBSIZE: {
            size_t size = storage_engine_->size();
            return Response(ResponseStatus::OK, "", std::to_string(size));
        }
        
        case CommandType::INFO: {
            std::string info = "# DKV Server Info\r\n";
            info += "total_keys:" + std::to_string(getTotalKeys()) + "\r\n";
            info += "expired_keys:" + std::to_string(getExpiredKeys()) + "\r\n";
            info += "current_keys:" + std::to_string(getKeyCount()) + "\r\n";
            info += "version:1.0.0\r\n";
            info += "used_memory:" + std::to_string(getMemoryUsage()) + "\r\n";
            info += "max_memory:" + std::to_string(getMaxMemory()) + "\r\n";
            
            // 详细内存统计信息，按行分割并添加到响应中
            std::string memory_stats = dkv::MemoryAllocator::getInstance().getStats();
            std::istringstream stats_stream(memory_stats);
            std::string line;
            while (std::getline(stats_stream, line)) {
                info += line + "\r\n";
            }
            
            return Response(ResponseStatus::OK, "", info);
        }
        
        case CommandType::SHUTDOWN: {
            // 异步停止服务器
            std::thread shutdown_thread([this]() {
                // 延迟一点时间，确保响应能够发送回去
                std::this_thread::sleep_for(std::chrono::milliseconds(100));
                stop();
            });
            shutdown_thread.detach();
            return Response(ResponseStatus::OK, "Shutting down...");
        }
        
        // RDB持久化命令
        case CommandType::SAVE: {
            // 检查是否启用了RDB持久化
            if (!enable_rdb_) {
                return Response(ResponseStatus::ERROR, "RDB持久化已禁用");
            }
            
            // 同步保存数据到RDB文件
            bool success = storage_engine_->saveRDB(rdb_filename_);
            
            if (success) {
                // 更新上次保存时间和重置变更计数
                last_save_time_ = std::chrono::system_clock::now();
                rdb_changes_ = 0;
            }
            
            return Response(success ? ResponseStatus::OK : ResponseStatus::ERROR);
        }
        
        case CommandType::BGSAVE: {
            // 检查是否启用了RDB持久化
            if (!enable_rdb_) {
                return Response(ResponseStatus::ERROR, "RDB持久化已禁用");
            }
            
            // 异步保存数据到RDB文件
            std::thread save_thread([this]() {
                bool success = storage_engine_->saveRDB(rdb_filename_);
                
                if (success) {
                    // 更新上次保存时间和重置变更计数
                    last_save_time_ = std::chrono::system_clock::now();
                    rdb_changes_ = 0;
                    DKV_LOG_INFO("异步RDB保存成功");
                } else {
                    DKV_LOG_ERROR("异步RDB保存失败");
                }
            });
            save_thread.detach();
            return Response(ResponseStatus::OK, "Background saving started");
        }
        
        // 有序集合命令
        case CommandType::ZADD: {
            if (command.args.size() < 3 || command.args.size() % 2 != 1) {
                return Response(ResponseStatus::ERROR, "ZADD命令需要奇数个参数（1个键名 + 多个分数-成员对）");
            }
            
            try {
                std::string key = command.args[0];
                std::vector<std::pair<Value, double>> members_with_scores;
                
                // 解析所有的分数-成员对
                for (size_t i = 1; i < command.args.size(); i += 2) {
                    double score = std::stod(command.args[i]);
                    std::string member = command.args[i + 1];
                    members_with_scores.push_back({member, score});
                }
                
                size_t addedCount = storage_engine_->zadd(key, members_with_scores);
                
                if (addedCount > 0) {
                    incDirty();
                    // 如果启用了AOF持久化，将命令写入AOF文件
                    if (enable_aof_ && aof_persistence_) {
                        aof_persistence_->appendCommand(command);
                    }
                }
                
                return Response(ResponseStatus::OK, "", std::to_string(addedCount));
            } catch (const std::invalid_argument&) {
                return Response(ResponseStatus::ERROR, "无效的分数参数");
            }
        }
        
        case CommandType::ZREM: {
            if (command.args.size() < 2) {
                return Response(ResponseStatus::ERROR, "ZREM命令需要至少2个参数");
            }
            
            std::string key = command.args[0];
            std::vector<Value> members(command.args.begin() + 1, command.args.end());
            size_t removedCount = storage_engine_->zrem(key, members);
            
            if (removedCount > 0) {
                incDirty();
                // 如果启用了AOF持久化，将命令写入AOF文件
                if (enable_aof_ && aof_persistence_) {
                    aof_persistence_->appendCommand(command);
                }
            }
            
            return Response(ResponseStatus::OK, "", std::to_string(removedCount));
        }
        
        case CommandType::ZSCORE: {
            if (command.args.size() < 2) {
                return Response(ResponseStatus::ERROR, "ZSCORE命令需要至少2个参数");
            }
            
            std::string key = command.args[0];
            std::string member = command.args[1];
            double score = -1.0;
            bool found = storage_engine_->zscore(key, member, score);
            
            if (!found) {
                return Response(ResponseStatus::NOT_FOUND);
            }
            
            return Response(ResponseStatus::OK, "", std::to_string(score));
        }
        
        case CommandType::ZISMEMBER: {
            if (command.args.size() < 2) {
                return Response(ResponseStatus::ERROR, "ZISMEMBER命令需要至少2个参数");
            }
            
            bool is_member = storage_engine_->zismember(command.args[0], command.args[1]);
            return Response(ResponseStatus::OK, "", is_member ? "1" : "0");
        }
        
        case CommandType::ZRANK: {
            if (command.args.size() < 2) {
                return Response(ResponseStatus::ERROR, "ZRANK命令需要至少2个参数");
            }
            
            size_t rank = 0;
            bool found = storage_engine_->zrank(command.args[0], command.args[1], rank);
            if (!found) {
                return Response(ResponseStatus::NOT_FOUND);
            }
            
            return Response(ResponseStatus::OK, "", std::to_string(rank));
        }
        
        case CommandType::ZREVRANK: {
            if (command.args.size() < 2) {
                return Response(ResponseStatus::ERROR, "ZREVRANK命令需要至少2个参数");
            }
            
            size_t rank = 0;
            bool found = storage_engine_->zrevrank(command.args[0], command.args[1], rank);
            if (!found) {
                return Response(ResponseStatus::NOT_FOUND);
            }
            
            return Response(ResponseStatus::OK, "", std::to_string(rank));
        }
        
        case CommandType::ZRANGE: {
            if (command.args.size() < 3) {
                return Response(ResponseStatus::ERROR, "ZRANGE命令需要至少3个参数");
            }
            
            try {
                size_t start = std::stoull(command.args[1]);
                size_t stop = std::stoull(command.args[2]);
                bool withScores = (command.args.size() >= 4 && command.args[3] == "WITHSCORES");
                
                std::vector<std::pair<Value, double>> members = 
                    storage_engine_->zrange(command.args[0], start, stop);
                
                Response response;
                response.status = ResponseStatus::OK;
                
                if (withScores) {
                    std::vector<std::string> result;
                    for (const auto& pair : members) {
                        result.push_back(pair.first);
                        result.push_back(std::to_string(pair.second));
                    }
                    response.data = RESPProtocol::serializeArray(result);
                } else {
                    std::vector<std::string> result;
                    for (const auto& pair : members) {
                        result.push_back(pair.first);
                    }
                    response.data = RESPProtocol::serializeArray(result);
                }
                
                return response;
            } catch (const std::invalid_argument&) {
                return Response(ResponseStatus::ERROR, "无效的范围参数");
            }
        }
        
        case CommandType::ZREVRANGE: {
            if (command.args.size() < 3) {
                return Response(ResponseStatus::ERROR, "ZREVRANGE命令需要至少3个参数");
            }
            
            try {
                size_t start = std::stoull(command.args[1]);
                size_t stop = std::stoull(command.args[2]);
                bool withScores = (command.args.size() >= 4 && command.args[3] == "WITHSCORES");
                
                std::vector<std::pair<Value, double>> members = 
                    storage_engine_->zrevrange(command.args[0], start, stop);
                
                Response response;
                response.status = ResponseStatus::OK;
                
                if (withScores) {
                    std::vector<std::string> result;
                    for (const auto& pair : members) {
                        result.push_back(pair.first);
                        result.push_back(std::to_string(pair.second));
                    }
                    response.data = RESPProtocol::serializeArray(result);
                } else {
                    std::vector<std::string> result;
                    for (const auto& pair : members) {
                        result.push_back(pair.first);
                    }
                    response.data = RESPProtocol::serializeArray(result);
                }
                
                return response;
            } catch (const std::invalid_argument&) {
                return Response(ResponseStatus::ERROR, "无效的范围参数");
            }
        }
        
        case CommandType::ZRANGEBYSCORE: {
            if (command.args.size() < 3) {
                return Response(ResponseStatus::ERROR, "ZRANGEBYSCORE命令需要至少3个参数");
            }
            
            try {
                double min = std::stod(command.args[1]);
                double max = std::stod(command.args[2]);
                bool withScores = (command.args.size() >= 4 && command.args[3] == "WITHSCORES");
                
                std::vector<std::pair<Value, double>> members = 
                    storage_engine_->zrangebyscore(command.args[0], min, max);
                
                Response response;
                response.status = ResponseStatus::OK;
                
                if (withScores) {
                    std::vector<std::string> result;
                    for (const auto& pair : members) {
                        result.push_back(pair.first);
                        result.push_back(std::to_string(pair.second));
                    }
                    response.data = RESPProtocol::serializeArray(result);
                } else {
                    std::vector<std::string> result;
                    for (const auto& pair : members) {
                        result.push_back(pair.first);
                    }
                    response.data = RESPProtocol::serializeArray(result);
                }
                
                return response;
            } catch (const std::invalid_argument&) {
                return Response(ResponseStatus::ERROR, "无效的分数参数");
            }
        }
        
        case CommandType::ZREVRANGEBYSCORE: {
            if (command.args.size() < 3) {
                return Response(ResponseStatus::ERROR, "ZREVRANGEBYSCORE命令需要至少3个参数");
            }
            
            try {
                double max = std::stod(command.args[1]);
                double min = std::stod(command.args[2]);
                bool withScores = (command.args.size() >= 4 && command.args[3] == "WITHSCORES");
                
                std::vector<std::pair<Value, double>> members = 
                    storage_engine_->zrevrangebyscore(command.args[0], max, min);
                
                Response response;
                response.status = ResponseStatus::OK;
                
                if (withScores) {
                    std::vector<std::string> result;
                    for (const auto& pair : members) {
                        result.push_back(pair.first);
                        result.push_back(std::to_string(pair.second));
                    }
                    response.data = RESPProtocol::serializeArray(result);
                } else {
                    std::vector<std::string> result;
                    for (const auto& pair : members) {
                        result.push_back(pair.first);
                    }
                    response.data = RESPProtocol::serializeArray(result);
                }
                
                return response;
            } catch (const std::invalid_argument&) {
                return Response(ResponseStatus::ERROR, "无效的分数参数");
            }
        }
        
        case CommandType::ZCOUNT: {
            if (command.args.size() < 3) {
                return Response(ResponseStatus::ERROR, "ZCOUNT命令需要至少3个参数");
            }
            
            try {
                double min = std::stod(command.args[1]);
                double max = std::stod(command.args[2]);
                size_t count = storage_engine_->zcount(command.args[0], min, max);
                return Response(ResponseStatus::OK, "", std::to_string(count));
            } catch (const std::invalid_argument&) {
                return Response(ResponseStatus::ERROR, "无效的分数参数");
            }
        }
        
        case CommandType::ZCARD: {
            if (command.args.empty()) {
                return Response(ResponseStatus::ERROR, "ZCARD命令需要1个参数");
            }
            
            size_t count = storage_engine_->zcard(command.args[0]);
            return Response(ResponseStatus::OK, "", std::to_string(count));
        }
        
        // 位图命令
        case CommandType::SETBIT: {
            if (command.args.size() < 3) {
                return Response(ResponseStatus::ERROR, "SETBIT命令需要至少3个参数");
            }
            try {
                Key key(command.args[0]);
                uint64_t offset = std::stoull(command.args[1]);
                int bit = std::stoi(command.args[2]);
                
                bool oldBit = storage_engine_->getBit(key, offset);
                storage_engine_->setBit(key, offset, bit != 0);
                incDirty();
                // 如果启用了AOF持久化，将命令写入AOF文件
                if (enable_aof_ && aof_persistence_) {
                    aof_persistence_->appendCommand(command);
                }
                return Response(ResponseStatus::OK, "", std::to_string(oldBit ? 1 : 0));
            } catch (const std::invalid_argument&) {
                return Response(ResponseStatus::ERROR, "无效的参数类型");
            }
        }
        
        case CommandType::GETBIT: {
            if (command.args.size() < 2) {
                return Response(ResponseStatus::ERROR, "GETBIT命令需要至少2个参数");
            }
            try {
                Key key(command.args[0]);
                uint64_t offset = std::stoull(command.args[1]);
                
                bool bit = storage_engine_->getBit(key, offset);
                return Response(ResponseStatus::OK, "", std::to_string(bit ? 1 : 0));
            } catch (const std::invalid_argument&) {
                return Response(ResponseStatus::ERROR, "无效的参数类型");
            }
        }
        
        case CommandType::BITCOUNT: {
            if (command.args.empty()) {
                return Response(ResponseStatus::ERROR, "BITCOUNT命令需要至少1个参数");
            }
            
            Key key(command.args[0]);
            uint64_t count = 0;
            
            if (command.args.size() == 1) {
                // 没有额外参数，统计整个位图
                count = storage_engine_->bitCount(key);
            } else if (command.args.size() == 3) {
                // 有start和end参数，这些参数指的是字节索引
                try {
                    uint64_t start = std::stoull(command.args[1]);
                    uint64_t end = std::stoull(command.args[2]);
                    count = storage_engine_->bitCount(key, start, end);
                } catch (const std::invalid_argument&) {
                    return Response(ResponseStatus::ERROR, "无效的参数类型");
                }
            } else {
                return Response(ResponseStatus::ERROR, "BITCOUNT命令参数数量不正确");
            }
            
            return Response(ResponseStatus::OK, "", std::to_string(count));
        }
        
        case CommandType::BITOP: {
            if (command.args.size() < 4) {
                return Response(ResponseStatus::ERROR, "BITOP命令需要至少4个参数");
            }
            
            const std::string& operation = command.args[0];
            Key destKey(command.args[1]);
            std::vector<Key> srcKeys;
            for (size_t i = 2; i < command.args.size(); ++i) {
                srcKeys.push_back(Key(command.args[i]));
            }
            
            bool success = storage_engine_->bitOp(operation, destKey, srcKeys);
            
            if (success) {
                incDirty();
                // 如果启用了AOF持久化，将命令写入AOF文件
                if (enable_aof_ && aof_persistence_) {
                    aof_persistence_->appendCommand(command);
                }
                return Response(ResponseStatus::OK, "", "1"); // 返回成功状态
            } else {
                return Response(ResponseStatus::ERROR, "BITOP操作失败");
            }
        }
        
        // HyperLogLog命令
        case CommandType::PFADD: {
            if (command.args.size() < 2) {
                return Response(ResponseStatus::ERROR, "PFADD命令需要至少2个参数");
            }
            
            Key key(command.args[0]);
            std::vector<Value> elements;
            for (size_t i = 1; i < command.args.size(); ++i) {
                elements.push_back(Value(command.args[i]));
            }
            
            bool success = storage_engine_->pfadd(key, elements);
            
            if (success) {
                incDirty();
                // 如果启用了AOF持久化，将命令写入AOF文件
                if (enable_aof_ && aof_persistence_) {
                    aof_persistence_->appendCommand(command);
                }
            }
            
            return Response(ResponseStatus::OK, "", success ? "1" : "0");
        }
        
        case CommandType::PFCOUNT: {
            if (command.args.empty()) {
                return Response(ResponseStatus::ERROR, "PFCOUNT命令需要至少1个参数");
            }
            
            uint64_t count = 0;
            if (command.args.size() == 1) {
                // 单个键
                count = storage_engine_->pfcount(command.args[0]);
            } else {
                // 多个键，需要创建临时合并后的HyperLogLog
                // 注意：此实现简化了Redis的PFCOUNT多个键的处理，实际应合并后再计数
                // 这里仅返回第一个键的计数作为示例
                count = storage_engine_->pfcount(command.args[0]);
            }
            
            return Response(ResponseStatus::OK, "", std::to_string(count));
        }
        
        case CommandType::PFMERGE: {
            if (command.args.size() < 2) {
                return Response(ResponseStatus::ERROR, "PFMERGE命令需要至少2个参数");
            }
            
            Key destKey(command.args[0]);
            std::vector<Key> sourceKeys;
            for (size_t i = 1; i < command.args.size(); ++i) {
                sourceKeys.push_back(Key(command.args[i]));
            }
            
            bool success = storage_engine_->pfmerge(destKey, sourceKeys);
            
            if (success) {
                incDirty();
                // 如果启用了AOF持久化，将命令写入AOF文件
                if (enable_aof_ && aof_persistence_) {
                    aof_persistence_->appendCommand(command);
                }
                return Response(ResponseStatus::OK, "", "OK");
            } else {
                return Response(ResponseStatus::ERROR, "PFMERGE操作失败");
            }
        }
        
        default:
            return Response(ResponseStatus::INVALID_COMMAND);
    }
}

void DKVServer::cleanupExpiredKeys() {
    while (cleanup_running_) {
        // 使用更短的睡眠时间，以便快速响应停止信号
        for (int i = 0; i < 60 && cleanup_running_; ++i) {
            std::this_thread::sleep_for(std::chrono::seconds(1));
        }
        
        if (cleanup_running_ && storage_engine_) {
            storage_engine_->cleanupExpiredKeys();
        }
    }
}

bool DKVServer::parseConfigFile(const std::string& config_file) {
    std::ifstream file(config_file);
    if (!file.is_open()) {
        DKV_LOG_ERROR("无法打开配置文件: ", config_file);
        return false;
    }
    
    std::string line;
    while (std::getline(file, line)) {
        // 跳过注释和空行
        if (line.empty() || line[0] == '#') {
            continue;
        }
        
        std::istringstream iss(line);
        std::string key, value;
        
        if (iss >> key >> value) {
            if (key == "port") {
                port_ = std::stoi(value);
            } else if (key == "maxmemory") {
                max_memory_ = std::stoull(value);
            } else if (key == "enable_rdb") {
                enable_rdb_ = (value == "yes" || value == "true" || value == "1");
            } else if (key == "rdb_filename") {
                rdb_filename_ = value;
            } else if (key == "rdb_save_interval") {
                rdb_save_interval_ = std::stoull(value);
            } else if (key == "rdb_save_changes") {
                rdb_save_changes_ = std::stoull(value);
            } else if (key == "enable_aof") {
                enable_aof_ = (value == "yes" || value == "true" || value == "1");
            } else if (key == "aof_filename") {
                aof_filename_ = value;
            } else if (key == "aof_fsync_policy") {
                aof_fsync_policy_ = value;
            }
        }
    }
    
    return true;
}

void DKVServer::incDirty() {
    rdb_changes_++;
}

void DKVServer::incDirty(int delta) {
    rdb_changes_ += delta;
}

void DKVServer::rdbAutoSaveThread() {
    // 实现自动保存线程逻辑
    while (running_) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        
        if (!enable_rdb_ || rdb_save_running_ || rdb_save_interval_ <= 0) {
            continue;
        }
        
        auto now = std::chrono::system_clock::now();
        auto now_seconds = std::chrono::duration_cast<std::chrono::seconds>(
            now.time_since_epoch()).count();
        auto last_save_seconds = std::chrono::duration_cast<std::chrono::seconds>(
            last_save_time_.load().time_since_epoch()).count();
        
        // 检查是否达到自动保存条件
        if ((rdb_changes_ >= rdb_save_changes_) && 
            (static_cast<uint64_t>(now_seconds - last_save_seconds) >= rdb_save_interval_)) {
            
            rdb_save_running_ = true;
            
            if (storage_engine_->saveRDB(rdb_filename_)) {
                last_save_time_ = now;
                rdb_changes_ = 0;
                DKV_LOG_INFO("自动保存RDB文件成功");
            } else {
                DKV_LOG_ERROR("自动保存RDB文件失败");
            }
            
            rdb_save_running_ = false;
        }
    }
}

} // namespace dkv