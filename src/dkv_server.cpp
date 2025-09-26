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
      enable_aof_(false), aof_filename_("appendonly.aof"), aof_fsync_policy_("everysec"),
      auto_aof_rewrite_percentage_(100), auto_aof_rewrite_min_size_(64 * 1024 * 1024) {
}

DKVServer::~DKVServer() {
    stop();
    command_handler_.reset();
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
        // 设置服务器引用，用于AOF重写
        aof_persistence_->setServer(this);
            
        // 设置fsync策略
        AOFPersistence::FsyncPolicy policy = AOFPersistence::FsyncPolicy::EVERYSEC;
        if (aof_fsync_policy_ == "always") {
            policy = AOFPersistence::FsyncPolicy::ALWAYS;
        } else if (aof_fsync_policy_ == "never") {
            policy = AOFPersistence::FsyncPolicy::NEVER;
        }
            
        // 初始化AOF文件
        if (aof_persistence_->initialize(aof_filename_, policy)) {
            // 更新命令处理器的AOF持久化指针
            command_handler_->setAofPersistence(aof_persistence_.get());
            
            // 设置AOF自动重写参数
            aof_persistence_->setAutoRewriteParams(auto_aof_rewrite_percentage_, auto_aof_rewrite_min_size_ / (1024 * 1024));
            DKV_LOG_INFO("AOF自动重写配置: 百分比=", auto_aof_rewrite_percentage_, "%, 最小大小=", auto_aof_rewrite_min_size_ / (1024 * 1024), "MB");
            
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

void DKVServer::setEvictionPolicy(EvictionPolicy policy) {
    eviction_policy_ = policy;
}

EvictionPolicy DKVServer::getEvictionPolicy() const {
    return eviction_policy_;
}

void DKVServer::evictKeys() {
    if (!storage_engine_) {
        DKV_LOG_ERROR("存储引擎未初始化，无法执行淘汰策略");
        return;
    }
    
    // 获取存储引擎的所有键
    std::vector<Key> all_keys = storage_engine_->getAllKeys();
    
    // 根据不同的淘汰策略选择要淘汰的键
    std::vector<Key> keys_to_evict;
    
    // 目标内存使用量（低于最大内存限制的一定比例）
    size_t target_usage = max_memory_ * 0.8; // 目标内存使用量为最大内存的80%
    size_t current_usage = getMemoryUsage();
    
    // 收集符合条件的键
    std::vector<Key> eligible_keys;
    for (const auto& key : all_keys) {
        // 检查键是否满足淘汰条件
        bool eligible = false;
        
        // 根据策略类型过滤键
        switch (eviction_policy_) {
            case EvictionPolicy::VOLATILE_LRU:
            case EvictionPolicy::VOLATILE_LFU:
            case EvictionPolicy::VOLATILE_RANDOM:
            case EvictionPolicy::VOLATILE_TTL:
                // 只考虑有过期时间的键
                eligible = storage_engine_->hasExpiration(key);
                break;
            case EvictionPolicy::ALLKEYS_LRU:
            case EvictionPolicy::ALLKEYS_LFU:
            case EvictionPolicy::ALLKEYS_RANDOM:
                // 考虑所有键
                eligible = true;
                break;
            default:
                // NOEVICTION等其他策略不执行淘汰
                return;
        }
        
        if (eligible) {
            eligible_keys.push_back(key);
        }
    }
    
    if (eligible_keys.empty()) {
        DKV_LOG_WARNING("没有符合条件的键可以淘汰");
        return;
    }
    
    // 根据策略选择要淘汰的键
    switch (eviction_policy_) {
        case EvictionPolicy::VOLATILE_LRU:
        case EvictionPolicy::ALLKEYS_LRU:
            // LRU策略：淘汰最后访问时间最早的键
            {
                std::sort(eligible_keys.begin(), eligible_keys.end(), [this](const Key& a, const Key& b) {
                    return storage_engine_->getLastAccessed(a) < storage_engine_->getLastAccessed(b);
                });
                break;
            }
        case EvictionPolicy::VOLATILE_LFU:
        case EvictionPolicy::ALLKEYS_LFU:
            // LFU策略：淘汰访问频率最低的键
            {
                std::sort(eligible_keys.begin(), eligible_keys.end(), [this](const Key& a, const Key& b) {
                    return storage_engine_->getAccessFrequency(a) < storage_engine_->getAccessFrequency(b);
                });
                break;
            }
        case EvictionPolicy::VOLATILE_TTL:
            // TTL策略：淘汰TTL最小的键
            {
                std::sort(eligible_keys.begin(), eligible_keys.end(), [this](const Key& a, const Key& b) {
                    return storage_engine_->getExpiration(a) < storage_engine_->getExpiration(b);
                });
                break;
            }
        case EvictionPolicy::VOLATILE_RANDOM:
        case EvictionPolicy::ALLKEYS_RANDOM:
            // 随机策略：打乱键的顺序
            {
                std::random_shuffle(eligible_keys.begin(), eligible_keys.end());
                break;
            }
        default:
            break;
    }
    
    // 开始淘汰键，直到达到目标内存使用量或没有更多键可淘汰
    size_t evicted_count = 0;
    for (const auto& key : eligible_keys) {
        // 检查是否已经达到目标内存使用量
        if (current_usage <= target_usage) {
            break;
        }
        
        // 删除键
        size_t key_size = storage_engine_->getKeySize(key);
        if (storage_engine_->del(key)) {
            DKV_LOG_INFO("淘汰键: ", key.c_str(), " 大小: ", key_size);
            current_usage -= key_size;
            evicted_count++;
            
            // 如果启用了AOF持久化，记录DEL命令
            if (enable_aof_ && aof_persistence_) {
                Command del_cmd(CommandType::DEL, {std::string(key)});
                aof_persistence_->appendCommand(del_cmd);
            }
        }
    }
    
    DKV_LOG_INFO("执行淘汰策略完成，共淘汰了 ", evicted_count, " 个键");
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

bool DKVServer::rewriteAOF() {
    if (!enable_aof_ || !aof_persistence_) {
        DKV_LOG_WARNING("AOF持久化未启用或AOF组件未初始化");
        return false;
    }
    
    DKV_LOG_INFO("开始执行AOF重写");
    
    // 生成临时文件名
    std::string temp_filename = aof_filename_ + ".tmp";
    
    // 执行AOF重写
    if (aof_persistence_->rewrite(storage_engine_.get(), temp_filename)) {
        DKV_LOG_INFO("AOF重写成功");
        return true;
    } else {
        DKV_LOG_ERROR("AOF重写失败");
        return false;
    }
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

    // 创建命令处理器
    DKV_LOG_DEBUG("创建命令处理器");
    command_handler_ = std::make_unique<CommandHandler>(
        storage_engine_.get(), 
        nullptr, // AOF持久化稍后初始化
        enable_aof_);
    
    DKV_LOG_INFO("DKV服务器初始化完成");
    return true;
}

Response DKVServer::executeCommand(const Command& command) {
    if (!storage_engine_ || !command_handler_) {
        return Response(ResponseStatus::ERROR, "Storage engine or command handler not initialized");
    }
    
    // 检查是否是只读命令，用于内存管理
    bool isReadOnly = command_handler_->isReadOnlyCommand(command.type);
    
    // 如果不是只读命令，且设置了最大内存限制，则检查内存使用情况
    if (!isReadOnly && max_memory_ > 0) {
        size_t currentUsage = getMemoryUsage();
        
        // 如果内存使用达到上限，尝试执行淘汰策略
        if (currentUsage >= max_memory_) {
            if (eviction_policy_ != EvictionPolicy::NOEVICTION) {
                // 尝试淘汰一些键
                DKV_LOG_INFO("内存使用已达到上限，尝试执行淘汰策略");
                evictKeys();
                
                // 重新检查内存使用情况
                currentUsage = getMemoryUsage();
                
                // 如果淘汰后内存使用仍然达到上限，拒绝执行命令
                if (currentUsage >= max_memory_) {
                    DKV_LOG_WARNING("执行淘汰策略后内存使用仍达到上限，拒绝执行命令");
                    return Response(ResponseStatus::ERROR, "OOM command not allowed when used memory > 'maxmemory'");
                }
            } else {
                DKV_LOG_WARNING("内存使用已达到上限，拒绝执行命令");
                return Response(ResponseStatus::ERROR, "OOM command not allowed when used memory > 'maxmemory'");
            }
        }
    }
    
    bool need_inc_dirty = false;
    Response response;
    
    switch (command.type) {
        case CommandType::SET:
            response = command_handler_->handleSetCommand(command, need_inc_dirty);
            break;
        case CommandType::GET:
            response = command_handler_->handleGetCommand(command);
            break;
        case CommandType::DEL:
            response = command_handler_->handleDelCommand(command, need_inc_dirty);
            break;
        case CommandType::EXISTS:
            response = command_handler_->handleExistsCommand(command);
            break;
        case CommandType::INCR:
            response = command_handler_->handleIncrCommand(command, need_inc_dirty);
            break;
        case CommandType::DECR:
            response = command_handler_->handleDecrCommand(command, need_inc_dirty);
            break;
        case CommandType::EXPIRE:
            response = command_handler_->handleExpireCommand(command, need_inc_dirty);
            break;
        case CommandType::TTL:
            response = command_handler_->handleTtlCommand(command);
            break;
        
        // 哈希命令
        case CommandType::HSET:
            response = command_handler_->handleHSetCommand(command, need_inc_dirty);
            break;
        case CommandType::HGET:
            response = command_handler_->handleHGetCommand(command);
            break;
        case CommandType::HGETALL:
            response = command_handler_->handleHGetAllCommand(command);
            break;
        case CommandType::HDEL:
            response = command_handler_->handleHDeldCommand(command, need_inc_dirty);
            break;
        case CommandType::HEXISTS:
            response = command_handler_->handleHExistsCommand(command);
            break;
        case CommandType::HKEYS:
            response = command_handler_->handleHKeysCommand(command);
            break;
        case CommandType::HVALS:
            response = command_handler_->handleHValsCommand(command);
            break;
        case CommandType::HLEN:
            response = command_handler_->handleHLenCommand(command);
            break;
        
        // 列表命令
        case CommandType::LPUSH:
            response = command_handler_->handleLPushCommand(command, need_inc_dirty);
            break;
        case CommandType::RPUSH:
            response = command_handler_->handleRPushCommand(command, need_inc_dirty);
            break;
        case CommandType::LPOP:
            response = command_handler_->handleLPopCommand(command, need_inc_dirty);
            break;
        case CommandType::RPOP:
            response = command_handler_->handleRPopCommand(command, need_inc_dirty);
            break;
        case CommandType::LLEN:
            response = command_handler_->handleLLenCommand(command);
            break;
        case CommandType::LRANGE:
            response = command_handler_->handleLRangeCommand(command);
            break;
        
        // 集合命令
        case CommandType::SADD:
            response = command_handler_->handleSAddCommand(command, need_inc_dirty);
            break;
        case CommandType::SREM:
            response = command_handler_->handleSRemCommand(command, need_inc_dirty);
            break;
        case CommandType::SMEMBERS:
            response = command_handler_->handleSMembersCommand(command);
            break;
        case CommandType::SISMEMBER:
            response = command_handler_->handleSIsMemberCommand(command);
            break;
        case CommandType::SCARD:
            response = command_handler_->handleSCardCommand(command);
            break;
        
        // 服务器管理命令
        case CommandType::FLUSHDB:
            response = command_handler_->handleFlushDBCommand(need_inc_dirty);
            break;
        case CommandType::DBSIZE:
            response = command_handler_->handleDBSizeCommand();
            break;
        case CommandType::INFO:
            response = command_handler_->handleInfoCommand(
                getKeyCount(), 
                getExpiredKeys(), 
                getTotalKeys(), 
                getMemoryUsage(), 
                getMaxMemory());
            break;
        case CommandType::SHUTDOWN:
            response = command_handler_->handleShutdownCommand(this);
            break;
        
        // RDB持久化命令
        case CommandType::SAVE:
            response = command_handler_->handleSaveCommand(
                rdb_filename_);
            if (response.status == ResponseStatus::OK) {
                last_save_time_ = std::chrono::system_clock::now();
                rdb_changes_ = 0;
            }
            break;
        case CommandType::BGSAVE:
            response = command_handler_->handleBgSaveCommand(
                rdb_filename_);
            if (response.status == ResponseStatus::OK) {
                last_save_time_ = std::chrono::system_clock::now();
                rdb_changes_ = 0;
            }
            break;
        
        // 有序集合命令
        case CommandType::ZADD:
            response = command_handler_->handleZAddCommand(command, need_inc_dirty);
            break;
        case CommandType::ZREM:
            response = command_handler_->handleZRemCommand(command, need_inc_dirty);
            break;
        case CommandType::ZSCORE:
            response = command_handler_->handleZScoreCommand(command);
            break;
        case CommandType::ZISMEMBER:
            response = command_handler_->handleZIsMemberCommand(command);
            break;
        case CommandType::ZRANK:
            response = command_handler_->handleZRankCommand(command);
            break;
        case CommandType::ZREVRANK:
            response = command_handler_->handleZRevRankCommand(command);
            break;
        case CommandType::ZRANGE:
            response = command_handler_->handleZRangeCommand(command);
            break;
        case CommandType::ZREVRANGE:
            response = command_handler_->handleZRevRangeCommand(command);
            break;
        case CommandType::ZRANGEBYSCORE:
            response = command_handler_->handleZRangeByScoreCommand(command);
            break;
        case CommandType::ZREVRANGEBYSCORE:
            response = command_handler_->handleZRevRangeByScoreCommand(command);
            break;
        case CommandType::ZCOUNT:
            response = command_handler_->handleZCountCommand(command);
            break;
        case CommandType::ZCARD:
            response = command_handler_->handleZCardCommand(command);
            break;
        
        // 位图命令
        case CommandType::SETBIT:
            response = command_handler_->handleSetBitCommand(command, need_inc_dirty);
            break;
        case CommandType::GETBIT:
            response = command_handler_->handleGetBitCommand(command);
            break;
        case CommandType::BITCOUNT:
            response = command_handler_->handleBitCountCommand(command);
            break;
        case CommandType::BITOP:
            response = command_handler_->handleBitOpCommand(command, need_inc_dirty);
            break;
        
        // HyperLogLog命令
        case CommandType::RESTORE_HLL:
            response = command_handler_->handleRestoreHLLCommand(command, need_inc_dirty);
            break;
        case CommandType::PFADD:
            response = command_handler_->handlePFAddCommand(command, need_inc_dirty);
            break;
        case CommandType::PFCOUNT:
            response = command_handler_->handlePFCountCommand(command);
            break;
        case CommandType::PFMERGE:
            response = command_handler_->handlePFMergeCommand(command, need_inc_dirty);
            break;
        
        default:
            return Response(ResponseStatus::INVALID_COMMAND);
    }
    
    // 如果需要增加脏标志，调用incDirty()
    if (need_inc_dirty) {
        incDirty();
        // 如果启用了AOF持久化，记录命令
        command_handler_->appendAOFCommand(command);
    }
    
    return response;
}

void DKVServer::cleanupExpiredKeys() {
    while (cleanup_running_) {
        // 使用更短的睡眠时间，以便快速响应停止信号
        for (int i = 0; i < 60 && cleanup_running_; ++i) {
            std::this_thread::sleep_for(std::chrono::seconds(1));
        }
        
        if (cleanup_running_ && storage_engine_) {
            storage_engine_->cleanupExpiredKeys();
            storage_engine_->cleanupEmptyKey();
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
            } else if (key == "auto_aof_rewrite_percentage") {
                auto_aof_rewrite_percentage_ = std::stoi(value);
            } else if (key == "auto_aof_rewrite_min_size") {
                // 处理大小单位，支持mb、gb等
                std::string size_str = value;
                std::string unit = "";
                int multiplier = 1;
                
                if (size_str.length() > 2) {
                    unit = size_str.substr(size_str.length() - 2);
                    std::transform(unit.begin(), unit.end(), unit.begin(), ::tolower);
                    
                    if (unit == "mb") {
                        multiplier = 1024 * 1024;
                        size_str = size_str.substr(0, size_str.length() - 2);
                    } else if (unit == "gb") {
                        multiplier = 1024 * 1024 * 1024;
                        size_str = size_str.substr(0, size_str.length() - 2);
                    }
                }
                
                auto_aof_rewrite_min_size_ = std::stoi(size_str) * multiplier;
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