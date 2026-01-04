#include "dkv_server.hpp"
#include "dkv_memory_allocator.hpp"
#include "dkv_logger.hpp"
#include "multinode/raft/dkv_raft.hpp"
#include "multinode/raft/dkv_raft_network.hpp"
#include "multinode/raft/dkv_raft_statemachine.hpp"
#include "multinode/raft/dkv_raft_persist.hpp"
#include "multinode/shard/dkv_shard.hpp"
#include <iostream>
#include <fstream>
#include <sstream>
#include <chrono>
#include <mutex>
using namespace std;

namespace dkv {

DKVServer::DKVServer(int port, size_t num_sub_reactors, size_t num_workers) 
    : running_(false), cleanup_running_(false), 
      port_(port), max_memory_(0), num_sub_reactors_(num_sub_reactors), num_workers_(num_workers),
      enable_rdb_(true), rdb_filename_("dump.rdb"), rdb_save_interval_(3600), rdb_save_changes_(1000),
      rdb_changes_(0), last_save_time_(chrono::system_clock::now()), rdb_save_running_(false),
      enable_aof_(false), aof_filename_("appendonly.aof"), aof_fsync_policy_("everysec"),
      auto_aof_rewrite_percentage_(100), auto_aof_rewrite_min_size_(64 * 1024 * 1024),
      enable_raft_(false), raft_node_id_(0), total_raft_nodes_(1), max_raft_state_(100 * 1024 * 1024),
      shard_data_dir_("./shard_data"), shard_raft_data_dir_("./shard_raft_data") {
    
    // 初始化默认分片配置
    InitializeDefaultShardConfig();
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
        aof_persistence_ = make_unique<AOFPersistence>();
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
    cleanup_thread_ = thread(&DKVServer::cleanupExpiredKeys, this);
    
    // 启动RDB自动保存线程
    if (enable_rdb_) {
        rdb_save_running_ = false;
        rdb_save_thread_ = thread(&DKVServer::rdbAutoSaveThread, this);
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
    string rdb_file = rdb_filename_;
    
    if (storage_engine_ && !rdb_file.empty()) {
        if (!storage_engine_->loadRDB(rdb_file)) {
            DKV_LOG_WARNING("无法加载RDB文件 ", rdb_file.c_str(), "，可能是文件不存在或格式不正确");
        } else {
            DKV_LOG_INFO("成功从RDB文件 ", rdb_file.c_str(), " 加载数据");
            
            // 更新上次保存时间和重置变更计数
            last_save_time_ = chrono::system_clock::now();
            rdb_changes_ = 0;
        }
    }
}

void DKVServer::stop() {
    if (!running_.load()) {
        DKV_LOG_INFO("服务器已停止，不需要再执行停止");
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
    
    // 停止RAFT组件（如果启用）
    if (enable_raft_) {
        DKV_LOG_INFO("停止RAFT组件");

        // 停止RAFT实例
        if (raft_) {
            raft_->Stop();
            raft_.reset();
        }
    }
    
    // 停止分片管理器（如果存在）
    DKV_LOG_INFO("停止分片管理器");
    if (shard_manager_) {
        shard_manager_->Stop();
        shard_manager_.reset();
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
    string rdb_file = rdb_filename_;
    
    if (storage_engine_ && !rdb_file.empty()) {
        if (!storage_engine_->saveRDB(rdb_file)) {
            DKV_LOG_WARNING("无法保存RDB文件 ", rdb_file.c_str());
        } else {
            DKV_LOG_INFO("成功将数据保存到RDB文件 ", rdb_file.c_str());
            
            // 更新上次保存时间和重置变更计数
            last_save_time_ = chrono::system_clock::now();
            rdb_changes_ = 0;
        }
    }
}

bool DKVServer::loadConfig(const string& config_file) {
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

void DKVServer::setTransactionIsolationLevel(TransactionIsolationLevel level) {
    if (running_) {
        DKV_LOG_WARNING("不能在运行时设置事务隔离等级");
        return;
    }
    transaction_isolation_level_ = level;
}

TransactionIsolationLevel DKVServer::getTransactionIsolationLevel() const {
    return transaction_isolation_level_;
}

void recordCommandForAOF(TransactionID tx_id, const Command& command, unique_ptr<dkv::CommandHandler>& command_handler, unique_ptr<dkv::TransactionManager>& transaction_manager) {
    if (!isReadOnlyCommand(command.type)) {
        if (tx_id == NO_TX) {
            command_handler->appendAOFCommand(command);
        } else {
            transaction_manager->getTransactionMut(tx_id).push_command(command);
        }
    }
}

void DKVServer::evictKeys(TransactionID tx_id) {
    if (!storage_engine_) {
        DKV_LOG_ERROR("存储引擎未初始化，无法执行淘汰策略");
        return;
    }
    
    // 获取存储引擎的所有键
    vector<Key> all_keys = storage_engine_->getAllKeys();
    
    // 根据不同的淘汰策略选择要淘汰的键
    vector<Key> keys_to_evict;
    
    // 目标内存使用量（低于最大内存限制的一定比例）
    size_t target_usage = max_memory_ * 0.8; // 目标内存使用量为最大内存的80%
    size_t current_usage = getMemoryUsage();
    
    // 收集符合条件的键
    vector<Key> eligible_keys;
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
                sort(eligible_keys.begin(), eligible_keys.end(), [this](const Key& a, const Key& b) {
                    return storage_engine_->getLastAccessed(a) < storage_engine_->getLastAccessed(b);
                });
                break;
            }
        case EvictionPolicy::VOLATILE_LFU:
        case EvictionPolicy::ALLKEYS_LFU:
            // LFU策略：淘汰访问频率最低的键
            {
                sort(eligible_keys.begin(), eligible_keys.end(), [this](const Key& a, const Key& b) {
                    return storage_engine_->getAccessFrequency(a) < storage_engine_->getAccessFrequency(b);
                });
                break;
            }
        case EvictionPolicy::VOLATILE_TTL:
            // TTL策略：淘汰TTL最小的键
            {
                sort(eligible_keys.begin(), eligible_keys.end(), [this](const Key& a, const Key& b) {
                    return storage_engine_->getExpiration(a) < storage_engine_->getExpiration(b);
                });
                break;
            }
        case EvictionPolicy::VOLATILE_RANDOM:
        case EvictionPolicy::ALLKEYS_RANDOM:
            // 随机策略：打乱键的顺序
            {
                random_shuffle(eligible_keys.begin(), eligible_keys.end());
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
        Command del_cmd(CommandType::DEL, {string(key)});  // todo: generate a batch delete command to avoid waiting for raft commit for too much time
        Response response = executeCommand(del_cmd, tx_id);
        if (response.status == ResponseStatus::OK) {
            DKV_LOG_INFO("淘汰键: ", key.c_str(), " 大小: ", key_size);
            current_usage -= key_size;
            evicted_count++;
        }
    }
    
    DKV_LOG_INFO("执行淘汰策略完成，共淘汰了 ", evicted_count, " 个键");
}

// RDB持久化配置方法实现
void DKVServer::setRDBEnabled(bool enabled) {
    enable_rdb_ = enabled;
}

void DKVServer::setRDBFilename(const string& filename) {
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

void DKVServer::setAOFFilename(const string& filename) {
    aof_filename_ = filename;
}

void DKVServer::setAOFFsyncPolicy(const string& policy) {
    aof_fsync_policy_ = policy;
}

bool DKVServer::rewriteAOF() {
    if (!enable_aof_ || !aof_persistence_) {
        DKV_LOG_WARNING("AOF持久化未启用或AOF组件未初始化");
        return false;
    }
    
    DKV_LOG_INFO("开始执行AOF重写");
    
    // 生成临时文件名
    string temp_filename = aof_filename_ + ".tmp";
    
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
    storage_engine_ = make_unique<StorageEngine>();
    
    // 创建工作线程池
    DKV_LOG_DEBUG("创建工作线程池，线程数: ", num_workers_);
    worker_pool_ = make_unique<WorkerThreadPool>(this, num_workers_);

    // 创建网络服务实例（使用多线程Reactor模式）
    DKV_LOG_DEBUG("创建网络服务实例，端口: ", port_, ", SubReactor数量: ", num_sub_reactors_);
    network_server_ = make_unique<NetworkServer>(worker_pool_.get(), port_, num_sub_reactors_);

    // 创建命令处理器
    DKV_LOG_DEBUG("创建命令处理器");
    command_handler_ = make_unique<CommandHandler>(
        storage_engine_.get(), 
        nullptr, // AOF持久化稍后初始化
        enable_aof_
    );
    
    // 设置脚本命令执行回调
    command_handler_->setScriptCommandCallback([this](const std::string& cmd_str, TransactionID tx_id) -> std::string {
        size_t pos = 0;
        Command cmd = RESPProtocol::parseCommand(cmd_str, pos);
        if (cmd.type == CommandType::UNKNOWN) {
            return "(error) ERR unknown command";
        }
        Response resp = doCommandNative(cmd, tx_id);
        return resp.message.empty() ? resp.data : resp.message;
    });
    
    // 初始化RAFT组件（如果启用）
    if (enable_raft_) {
        DKV_LOG_INFO("初始化RAFT组件");
        
        // 创建RAFT状态机管理器
        raft_state_machine_ = std::make_shared<RaftStateMachineManager>();
        raft_state_machine_->SetCommandHandler(command_handler_.get());
        raft_state_machine_->SetStorageEngine(storage_engine_.get());
        raft_state_machine_->SetDKVServer(this);
        
        // 创建RAFT持久化
        raft_persister_ = std::make_shared<RaftFilePersister>(raft_data_dir_);
        
        // 创建RAFT网络
        raft_network_ = std::make_shared<RaftTcpNetwork>(raft_node_id_, raft_peers_);
        
        // 创建RAFT实例
        raft_ = std::make_shared<Raft>(raft_node_id_, raft_peers_, raft_persister_, raft_network_, raft_state_machine_);

        // 设置RAFT实例到网络组件
        auto tcp_network = std::dynamic_pointer_cast<RaftTcpNetwork>(raft_network_);
        if (tcp_network) {
            tcp_network->SetRaft(raft_);
        }

        // 启动RAFT
        raft_->Start();
        
        DKV_LOG_INFO("RAFT组件初始化完成，节点ID: ", raft_node_id_, ", 总节点数: ", total_raft_nodes_);
    }
    
    // 初始化分片管理器
    if (shard_config_ && shard_config_->enable_sharding) {
        DKV_LOG_INFO("初始化分片管理器，启用分片功能");
        
        // 创建分片管理器实例
        shard_manager_ = make_unique<ShardManager>(this);
        
        // 设置分片配置
        if (shard_manager_) {
            // 初始化分片管理器
            if (!shard_manager_->Initialize(*shard_config_)) {
                DKV_LOG_ERROR("分片管理器初始化失败");
                return false;
            }
            
            // 启动分片管理器
            if (!shard_manager_->Start()) {
                DKV_LOG_ERROR("分片管理器启动失败");
                return false;
            }
            
            DKV_LOG_INFO("分片管理器初始化完成，分片数量: ", shard_config_->num_shards);
        }
    } else {
        DKV_LOG_INFO("分片功能未启用，使用单机模式");
    }
    
    DKV_LOG_INFO("DKV服务器初始化完成");
    return true;
}


Response DKVServer::OnClientCommand(int client_fd, const Command& command) {
    unique_lock<mutex> serializable_lock(serializable_mutex_, defer_lock);
    if (transaction_isolation_level_ == TransactionIsolationLevel::SERIALIZABLE) {
        serializable_lock.lock();
    }
    int tx_id = NO_TX;
    if (transaction_isolation_level_ != TransactionIsolationLevel::READ_UNCOMMITTED) {
        shared_lock<shared_mutex> readlock_client_transaction_ids_(transaction_mutex_);
        auto it = client_transaction_ids_.find(client_fd);
        if (it != client_transaction_ids_.end()) {
            tx_id = it->second;
        }
    }
    Response response = executeCommand(command, tx_id);
    if (response.status == ResponseStatus::OK) {
        if (command.type == CommandType::MULTI) {
            tx_id = stoi(response.message);
            lock_guard writelock_client_transaction_ids_(transaction_mutex_);
            client_transaction_ids_[client_fd] = tx_id;
        } else if (command.type == CommandType::EXEC || command.type == CommandType::DISCARD) {
            lock_guard writelock_client_transaction_ids_(transaction_mutex_);
            client_transaction_ids_.erase(client_fd);
        }
    }
    return response;
}

Response DKVServer::executeCommand(const Command& command, TransactionID tx_id) {
    if (!storage_engine_ || !command_handler_) {
        return Response(ResponseStatus::ERROR, "Storage engine or command handler not initialized");
    }

    if (commandNotAllowedInTx(command.type) && tx_id != NO_TX) {
        // 不允许在事务中执行的命令，先自动提交当前事务
        Command commit_command(CommandType::EXEC, {});
        Response commit_response = executeCommand(commit_command, tx_id);
        if (commit_response.status != ResponseStatus::OK) {
            DKV_LOG_ERROR("提交事务失败: ", commit_response.message);
            return commit_response;
        }
        tx_id = NO_TX;
    }
    
    // 检查是否启用了分片功能
    if (shard_config_ && shard_config_->enable_sharding) {
        // 使用分片管理器处理命令
        return shard_manager_->HandleCommand(command, tx_id);
    }
    
    // 检查是否是只读命令，用于内存管理和Raft集成
    bool isReadOnly = isReadOnlyCommand(command.type);
    
    // 如果不是只读命令，且设置了最大内存限制，则检查内存使用情况
    if (!isReadOnly && command.type != CommandType::DEL && max_memory_ > 0) {
        size_t currentUsage = getMemoryUsage();
        
        // 如果内存使用达到上限，尝试执行淘汰策略
        if (currentUsage >= max_memory_) {
            if (eviction_policy_ != EvictionPolicy::NOEVICTION) {
                // 尝试淘汰一些键
                DKV_LOG_INFO("内存使用已达到上限，尝试执行淘汰策略");
                evictKeys(tx_id);
                
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

    // 如果启用了Raft，处理写命令的Raft集成
    if (enable_raft_ && !isReadOnly) {
        // 检查当前节点是否是领导者
        if (!raft_->IsLeader()) {
            // 获取当前节点认为的领导者ID
            int leaderId = raft_->GetCurrentLeaderId();
            if (leaderId == -1) {
                // 没有已知的领导者，返回错误
                return Response(ResponseStatus::ERROR, "No known leader, please try again later");
            } else {
                // 返回当前领导者信息，格式为"MOVED <leaderId>"
                string leaderInfo = "MOVED " + to_string(leaderId);
                return Response(ResponseStatus::ERROR, leaderInfo);
            }
        }
        
        // 当前节点是领导者，将命令提交到Raft
        int index, term;
        auto raft_cmd = make_shared<RaftCommand>(tx_id, command);
        bool ok = raft_->StartCommand(raft_cmd, index, term);
        if (!ok) {
            // 提交失败，可能是因为在提交过程中失去了领导者地位
            return Response(ResponseStatus::ERROR, "Failed to commit command to Raft");
        }
        
        // 等待命令被提交和应用到状态机后再返回结果
        Response response = raft_->waitForCommandResult(index, term, 5000);
        return response;
    }
    // 直接操作本机数据
    return doCommandNative(command, tx_id);
}

// 在本机执行指定Command
Response DKVServer::doCommandNative(const Command& command, TransactionID tx_id) {
    unique_ptr<TransactionManager> &transaction_manager = storage_engine_->getTransactionManager();
    recordCommandForAOF(tx_id, command, command_handler_, transaction_manager);
    bool need_inc_dirty = false;
    Response response;
    switch (command.type) {
        case CommandType::MULTI:
        {
            if (tx_id != NO_TX) {
                return Response(ResponseStatus::ERROR, "Transaction already started");
            }
            if (command.args.size() == 0) {
                // 开启事务
                tx_id = transaction_manager->begin();
            } else if (command.args.size() == 1) {
                TransactionID spec_tx_id = stoi(command.args[0]);
                if (!transaction_manager->isActive(spec_tx_id)) {
                    return Response(ResponseStatus::ERROR, "Invalid transaction ID");
                }
                tx_id = spec_tx_id;
            } else {
                return Response(ResponseStatus::ERROR, "Invalid transaction ID");
            }
            return Response(ResponseStatus::OK, to_string(tx_id));
        }
        case CommandType::EXEC:
        {
            if (tx_id == NO_TX) {
                return Response(ResponseStatus::ERROR, "Transaction not started");
            }
            auto commands = transaction_manager->getTransaction(tx_id).get_commands();
            // 提交事务
            transaction_manager->commit(tx_id);
            return Response(ResponseStatus::OK, "OK");
        }
        case CommandType::DISCARD:
        {
            if (tx_id == NO_TX) {
                return Response(ResponseStatus::ERROR, "Transaction not started");
            }
            // 回滚事务
            transaction_manager->rollback(tx_id);
            return Response(ResponseStatus::OK, "OK");
        }
        case CommandType::SET:
            response = command_handler_->handleSetCommand(tx_id, command, need_inc_dirty);
            break;
        case CommandType::GET:
            response = command_handler_->handleGetCommand(tx_id, command);
            break;
        case CommandType::DEL:
            response = command_handler_->handleDelCommand(tx_id, command, need_inc_dirty);
            break;
        case CommandType::EXISTS:
            response = command_handler_->handleExistsCommand(tx_id, command);
            break;
        case CommandType::INCR:
            response = command_handler_->handleIncrCommand(tx_id, command, need_inc_dirty);
            break;
        case CommandType::DECR:
            response = command_handler_->handleDecrCommand(tx_id, command, need_inc_dirty);
            break;
        case CommandType::EXPIRE:
            response = command_handler_->handleExpireCommand(tx_id, command, need_inc_dirty);
            break;
        case CommandType::TTL:
            response = command_handler_->handleTtlCommand(tx_id, command);
            break;
        
        // 哈希命令
        case CommandType::HSET:
            response = command_handler_->handleHSetCommand(tx_id, command, need_inc_dirty);
            break;
        case CommandType::HGET:
            response = command_handler_->handleHGetCommand(tx_id, command);
            break;
        case CommandType::HGETALL:
            response = command_handler_->handleHGetAllCommand(tx_id, command);
            break;
        case CommandType::HDEL:
            response = command_handler_->handleHDeldCommand(tx_id, command, need_inc_dirty);
            break;
        case CommandType::HEXISTS:
            response = command_handler_->handleHExistsCommand(tx_id, command);
            break;
        case CommandType::HKEYS:
            response = command_handler_->handleHKeysCommand(tx_id, command);
            break;
        case CommandType::HVALS:
            response = command_handler_->handleHValsCommand(tx_id, command);
            break;
        case CommandType::HLEN:
            response = command_handler_->handleHLenCommand(tx_id, command);
            break;
        
        // 列表命令
        case CommandType::LPUSH:
            response = command_handler_->handleLPushCommand(tx_id, command, need_inc_dirty);
            break;
        case CommandType::RPUSH:
            response = command_handler_->handleRPushCommand(tx_id, command, need_inc_dirty);
            break;
        case CommandType::LPOP:
            response = command_handler_->handleLPopCommand(tx_id, command, need_inc_dirty);
            break;
        case CommandType::RPOP:
            response = command_handler_->handleRPopCommand(tx_id, command, need_inc_dirty);
            break;
        case CommandType::LLEN:
            response = command_handler_->handleLLenCommand(tx_id, command);
            break;
        case CommandType::LRANGE:
            response = command_handler_->handleLRangeCommand(tx_id, command);
            break;
        
        // 集合命令
        case CommandType::SADD:
            response = command_handler_->handleSAddCommand(tx_id, command, need_inc_dirty);
            break;
        case CommandType::SREM:
            response = command_handler_->handleSRemCommand(tx_id, command, need_inc_dirty);
            break;
        case CommandType::SMEMBERS:
            response = command_handler_->handleSMembersCommand(tx_id, command);
            break;
        case CommandType::SISMEMBER:
            response = command_handler_->handleSIsMemberCommand(tx_id, command);
            break;
        case CommandType::SCARD:
            response = command_handler_->handleSCardCommand(tx_id, command);
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
                last_save_time_ = chrono::system_clock::now();
                rdb_changes_ = 0;
            }
            break;
        case CommandType::BGSAVE:
            response = command_handler_->handleBgSaveCommand(
                rdb_filename_);
            if (response.status == ResponseStatus::OK) {
                last_save_time_ = chrono::system_clock::now();
                rdb_changes_ = 0;
            }
            break;

        // 有序集合命令
        case CommandType::ZADD:
            response = command_handler_->handleZAddCommand(tx_id, command, need_inc_dirty);
            break;
        case CommandType::ZREM:
            response = command_handler_->handleZRemCommand(tx_id, command, need_inc_dirty);
            break;
        case CommandType::ZSCORE:
            response = command_handler_->handleZScoreCommand(tx_id, command);
            break;
        case CommandType::ZISMEMBER:
            response = command_handler_->handleZIsMemberCommand(tx_id, command);
            break;
        case CommandType::ZRANK:
            response = command_handler_->handleZRankCommand(tx_id, command);
            break;
        case CommandType::ZREVRANK:
            response = command_handler_->handleZRevRankCommand(tx_id, command);
            break;
        case CommandType::ZRANGE:
            response = command_handler_->handleZRangeCommand(tx_id, command);
            break;
        case CommandType::ZREVRANGE:
            response = command_handler_->handleZRevRangeCommand(tx_id, command);
            break;
        case CommandType::ZRANGEBYSCORE:
            response = command_handler_->handleZRangeByScoreCommand(tx_id, command);
            break;
        case CommandType::ZREVRANGEBYSCORE:
            response = command_handler_->handleZRevRangeByScoreCommand(tx_id, command);
            break;
        case CommandType::ZCOUNT:
            response = command_handler_->handleZCountCommand(tx_id, command);
            break;
        case CommandType::ZCARD:
            response = command_handler_->handleZCardCommand(tx_id, command);
            break;
        
        // 位图命令
        case CommandType::SETBIT:
            response = command_handler_->handleSetBitCommand(tx_id, command, need_inc_dirty);
            break;
        case CommandType::GETBIT:
            response = command_handler_->handleGetBitCommand(tx_id, command);
            break;
        case CommandType::BITCOUNT:
            response = command_handler_->handleBitCountCommand(tx_id, command);
            break;
        case CommandType::BITOP:
            response = command_handler_->handleBitOpCommand(tx_id, command, need_inc_dirty);
            break;
        
        // HyperLogLog命令
        case CommandType::RESTORE_HLL:
            response = command_handler_->handleRestoreHLLCommand(command, need_inc_dirty);
            break;
        case CommandType::PFADD:
            response = command_handler_->handlePFAddCommand(tx_id, command, need_inc_dirty);
            break;
        case CommandType::PFCOUNT:
            response = command_handler_->handlePFCountCommand(tx_id, command);
            break;
        case CommandType::PFMERGE:
            response = command_handler_->handlePFMergeCommand(tx_id, command, need_inc_dirty);
            break;
        case CommandType::EVALX:
            response = command_handler_->handleEvalXCommand(tx_id, command);
            break;
        default:
            return Response(ResponseStatus::INVALID_COMMAND);
    }
    
    // 如果需要增加脏标志，调用incDirty()
    if (need_inc_dirty) {
        incDirty();
    }
    return response;
}

void DKVServer::cleanupExpiredKeys() {
    while (cleanup_running_) {
        // 使用更短的睡眠时间，以便快速响应停止信号
        for (int i = 0; i < 60 && cleanup_running_; ++i) {
            this_thread::sleep_for(chrono::seconds(1));
        }
        
        if (cleanup_running_ && storage_engine_) {
            storage_engine_->cleanupExpiredKeys();
            storage_engine_->cleanupEmptyKey();
        }
    }
}

void DKVServer::InitializeDefaultShardConfig() {
    if (!shard_config_) {
        shard_config_ = std::make_unique<ShardConfig>();
    }
    
    // 设置默认分片配置
    shard_config_->enable_sharding = false; // 默认禁用分片
    shard_config_->num_shards = 1;
    shard_config_->hash_type = HashFunctionType::MD5;
    shard_config_->num_virtual_nodes = 100;
    shard_config_->heartbeat_interval_ms = 1000;
    shard_config_->migration_batch_size = 1000;
    shard_config_->max_concurrent_migrations = 2;
    shard_config_->failover_timeout_ms = 5000;
    shard_config_->enable_auto_migration = true;
    shard_config_->health_check_interval_ms = 30000; // 30秒
    shard_config_->monitoring_interval_ms = 10000; // 10秒
    
    // 设置默认目录
    shard_data_dir_ = "./shard_data";
    shard_raft_data_dir_ = "./shard_raft_data";
}

bool DKVServer::parseConfigFile(const string& config_file) {
    ifstream file(config_file);
    if (!file.is_open()) {
        DKV_LOG_ERROR("无法打开配置文件: ", config_file);
        return false;
    }
    
    string line;
    while (getline(file, line)) {
        // 跳过注释和空行
        if (line.empty() || line[0] == '#') {
            continue;
        }
        
        istringstream iss(line);
        string key, value;
        
        if (iss >> key >> value) {
            if (key == "port") {
                port_ = stoi(value);
            } else if (key == "maxmemory") {
                max_memory_ = stoull(value);
            } else if (key == "enable_rdb") {
                enable_rdb_ = (value == "yes" || value == "true" || value == "1");
            } else if (key == "rdb_filename") {
                rdb_filename_ = value;
            } else if (key == "rdb_save_interval") {
                rdb_save_interval_ = stoull(value);
            } else if (key == "rdb_save_changes") {
                rdb_save_changes_ = stoull(value);
            } else if (key == "enable_aof") {
                enable_aof_ = (value == "yes" || value == "true" || value == "1");
            } else if (key == "aof_filename") {
                aof_filename_ = value;
            } else if (key == "aof_fsync_policy") {
                aof_fsync_policy_ = value;
            } else if (key == "auto_aof_rewrite_percentage") {
                auto_aof_rewrite_percentage_ = stoi(value);
            } else if (key == "auto_aof_rewrite_min_size") {
                // 处理大小单位，支持mb、gb等
                string size_str = value;
                string unit = "";
                int multiplier = 1;
                
                if (size_str.length() > 2) {
                    unit = size_str.substr(size_str.length() - 2);
                    transform(unit.begin(), unit.end(), unit.begin(), ::tolower);
                    
                    if (unit == "mb") {
                        multiplier = 1024 * 1024;
                        size_str = size_str.substr(0, size_str.length() - 2);
                    } else if (unit == "gb") {
                        multiplier = 1024 * 1024 * 1024;
                        size_str = size_str.substr(0, size_str.length() - 2);
                    }
                }
                auto_aof_rewrite_min_size_ = stoi(size_str) * multiplier;
            } else if (key == "transaction_isolation_level") {
                // 事务隔离等级配置
                transform(value.begin(), value.end(), value.begin(), ::tolower);
                if (value == "read_uncommitted") {
                    transaction_isolation_level_ = TransactionIsolationLevel::READ_UNCOMMITTED;
                } else if (value == "read_committed") {
                    transaction_isolation_level_ = TransactionIsolationLevel::READ_COMMITTED;
                } else if (value == "repeatable_read") {
                    transaction_isolation_level_ = TransactionIsolationLevel::REPEATABLE_READ;
                } else if (value == "serializable") {
                    transaction_isolation_level_ = TransactionIsolationLevel::SERIALIZABLE;
                }
            } else if (key == "enable_raft") {
                // 是否启用RAFT
                enable_raft_ = (value == "yes" || value == "true" || value == "1");
            } else if (key == "raft_node_id") {
                // RAFT节点ID
                raft_node_id_ = stoi(value);
            } else if (key == "total_raft_nodes") {
                // 总节点数
                total_raft_nodes_ = stoi(value);
            } else if (key == "raft_data_dir") {
                // RAFT数据目录
                raft_data_dir_ = value;
            } else if (key == "max_raft_state") {
                // RAFT日志最大大小
                max_raft_state_ = stoi(value);
            } else if (key.find("raft_peer_") == 0) {
                // RAFT集群节点
                int peer_id = stoi(key.substr(10)); // 从"raft_peer_"后面提取ID
                if ((size_t)peer_id >= raft_peers_.size()) {
                    raft_peers_.resize(peer_id + 1);
                }
                raft_peers_[peer_id] = value;
            } else if (key == "enable_sharding") {
                // 是否启用分片功能
                if (!shard_config_) {
                    shard_config_ = std::make_unique<ShardConfig>();
                    InitializeDefaultShardConfig();
                }
                shard_config_->enable_sharding = (value == "yes" || value == "true" || value == "1");
            } else if (key == "shard_count") {
                // 分片数量
                if (!shard_config_) {
                    shard_config_ = std::make_unique<ShardConfig>();
                    InitializeDefaultShardConfig();
                }
                shard_config_->num_shards = stoi(value);
            } else if (key == "shard_replicas") {
                // 虚拟节点数量
                if (!shard_config_) {
                    shard_config_ = std::make_unique<ShardConfig>();
                    InitializeDefaultShardConfig();
                }
                shard_config_->num_virtual_nodes = stoi(value);
            } else if (key == "hash_function_type") {
                // 哈希函数类型
                if (!shard_config_) {
                    shard_config_ = std::make_unique<ShardConfig>();
                    InitializeDefaultShardConfig();
                }
                std::string hash_type = value;
                transform(hash_type.begin(), hash_type.end(), hash_type.begin(), ::tolower);
                if (hash_type == "md5") {
                    shard_config_->hash_type = HashFunctionType::MD5;
                } else if (hash_type == "sha1") {
                    shard_config_->hash_type = HashFunctionType::SHA1;
                } else if (hash_type == "murmur3") {
                    shard_config_->hash_type = HashFunctionType::MURMUR3;
                }
            } else if (key == "auto_migration") {
                // 是否自动迁移数据
                if (!shard_config_) {
                    shard_config_ = std::make_unique<ShardConfig>();
                    InitializeDefaultShardConfig();
                }
                shard_config_->enable_auto_migration = (value == "yes" || value == "true" || value == "1");
            } else if (key == "health_check_interval") {
                // 健康检查间隔
                if (!shard_config_) {
                    shard_config_ = std::make_unique<ShardConfig>();
                    InitializeDefaultShardConfig();
                }
                shard_config_->health_check_interval_ms = stoi(value) * 1000; // 转换为毫秒
            } else if (key == "monitoring_interval") {
                // 监控间隔
                if (!shard_config_) {
                    shard_config_ = std::make_unique<ShardConfig>();
                    InitializeDefaultShardConfig();
                }
                shard_config_->monitoring_interval_ms = stoi(value) * 1000; // 转换为毫秒
            } else if (key == "shard_data_dir") {
                // 分片数据目录
                shard_data_dir_ = value;
            } else if (key == "shard_raft_data_dir") {
                // 分片RAFT数据目录
                shard_raft_data_dir_ = value;
            } else if (key.find("shard_peer_") == 0) {
                // 分片RAFT集群节点配置
                // 格式: shard_peer_<shard_id>_<peer_id>
                size_t first_underscore = key.find('_', 11); // 查找第二个下划线
                if (first_underscore != string::npos) {
                    int shard_id = stoi(key.substr(11, first_underscore - 11)); // 分片ID
                    int peer_id = stoi(key.substr(first_underscore + 1)); // 节点ID
                    
                    // 确保分片节点数组足够大
                    if ((size_t)shard_id >= shard_peers_.size()) {
                        shard_peers_.resize(shard_id + 1);
                    }
                    if ((size_t)peer_id >= shard_peers_[shard_id].size()) {
                        shard_peers_[shard_id].resize(peer_id + 1);
                    }
                    shard_peers_[shard_id][peer_id] = value;
                }
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
        this_thread::sleep_for(chrono::milliseconds(100));
        
        if (!enable_rdb_ || rdb_save_running_ || rdb_save_interval_ <= 0) {
            continue;
        }
        
        auto now = chrono::system_clock::now();
        auto now_seconds = chrono::duration_cast<chrono::seconds>(
            now.time_since_epoch()).count();
        auto last_save_seconds = chrono::duration_cast<chrono::seconds>(
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