#include "multinode/shard/dkv_shard.hpp"
#include "multinode/raft/dkv_raft.hpp"
#include "multinode/raft/dkv_raft_persist.hpp"
#include "multinode/raft/dkv_raft_network.hpp"
#include "multinode/raft/dkv_raft_statemachine.hpp"
#include "dkv_server.hpp"
#include "dkv_utils.hpp"
#include <algorithm>
#include <sstream>
#include <chrono>

namespace dkv {

// 获取当前时间戳（毫秒）
uint64_t GetCurrentTimestamp() {
    auto now = std::chrono::system_clock::now();
    auto duration = now.time_since_epoch();
    return std::chrono::duration_cast<std::chrono::milliseconds>(duration).count();
}

// Shard类实现

// 构造函数
Shard::Shard(int shard_id, const std::vector<std::string>& raft_peers, 
             const std::string& raft_data_dir, int max_raft_state)
    : shard_id_(shard_id),
      state_(ShardState::INACTIVE),
      raft_peers_(raft_peers),
      raft_data_dir_(raft_data_dir),
      max_raft_state_(max_raft_state),
      key_count_(0),
      memory_usage_(0),
      operations_per_second_(0),
      migration_progress_(0),
      last_heartbeat_(0),
      is_migrating_(false),
      migration_target_shard_(-1) {
    
    // 为每个分片创建独立的Raft数据目录
    std::string shard_raft_dir = raft_data_dir_ + "/shard_" + std::to_string(shard_id_);
    
    // 初始化Raft组件
    raft_persister_ = std::make_shared<RaftFilePersister>(shard_raft_dir);
    raft_network_ = std::make_shared<RaftTcpNetwork>(shard_id_, raft_peers_);
    raft_state_machine_ = std::make_shared<RaftStateMachineManager>();
    
    // 创建Raft实例
    raft_ = std::make_shared<Raft>(shard_id_, raft_peers_, raft_persister_, raft_network_, raft_state_machine_);
    
    // 设置Raft实例到网络组件
    auto tcp_network = std::dynamic_pointer_cast<RaftTcpNetwork>(raft_network_);
    if (tcp_network) {
        tcp_network->SetRaft(raft_);
    }
}

// 析构函数
Shard::~Shard() {
    Stop();
}

// 启动分片
bool Shard::Start() {
    std::lock_guard<std::mutex> lock(state_mutex_);
    
    if (state_ == ShardState::ACTIVE) {
        return true; // 已经在运行
    }
    
    // 启动Raft
    raft_->Start();
    
    // 更新状态
    state_ = ShardState::ACTIVE;
    
    return true;
}

// 停止分片
void Shard::Stop() {
    std::lock_guard<std::mutex> lock(state_mutex_);
    
    if (state_ == ShardState::INACTIVE) {
        return; // 已经停止
    }
    
    // 停止Raft
    raft_->Stop();
    
    // 更新状态
    state_ = ShardState::INACTIVE;
}

// 执行命令
Response Shard::ExecuteCommand(const Command& command, TransactionID tx_id) {
    // 检查分片状态
    if (state_ != ShardState::ACTIVE) {
        return Response(ResponseStatus::ERROR, "Shard is not active");
    }
    
    // 创建Raft命令
    RaftCommand raft_cmd(tx_id, command);
    
    // 提交命令到Raft
    int index, term;
    bool ok = raft_->StartCommand(raft_cmd, index, term);
    
    if (!ok) {
        return Response(ResponseStatus::ERROR, "Failed to start command");
    }
    
    // 等待命令结果
    Response response = raft_->waitForCommandResult(index, term, 10000); // 10秒超时
    
    // 更新统计信息
    {   std::lock_guard<std::mutex> lock(stats_mutex_);
        operations_per_second_++;
    }
    
    return response;
}

// 设置分片状态
void Shard::SetState(ShardState state) {
    std::lock_guard<std::mutex> lock(state_mutex_);
    state_ = state;
}

// 获取分片统计信息
ShardStats Shard::GetStats() const {
    std::lock_guard<std::mutex> lock(stats_mutex_);
    
    ShardStats stats;
    stats.shard_id = shard_id_;
    stats.state = state_;
    stats.key_count = key_count_;
    stats.memory_usage = memory_usage_;
    stats.raft_group_size = static_cast<int>(raft_peers_.size());
    stats.raft_leader_id = raft_->GetCurrentLeaderId();
    stats.operations_per_second = operations_per_second_;
    stats.migration_progress = migration_progress_;
    stats.last_heartbeat = last_heartbeat_;
    
    return stats;
}

// 执行心跳检查
bool Shard::Heartbeat() {
    std::lock_guard<std::mutex> lock(stats_mutex_);
    
    // 更新最后心跳时间
    last_heartbeat_ = GetCurrentTimestamp();
    
    // 重置每秒操作数计数
    operations_per_second_ = 0;
    
    return true;
}

// 开始迁移数据到目标分片
bool Shard::StartMigration(int target_shard_id, const std::string& start_key, const std::string& end_key) {
    if (is_migrating_.load()) {
        return false; // 已经在迁移
    }
    
    std::lock_guard<std::mutex> lock(stats_mutex_);
    
    is_migrating_ = true;
    migration_target_shard_ = target_shard_id;
    migration_start_key_ = start_key;
    migration_end_key_ = end_key;
    migration_progress_ = 0;
    
    // 更新分片状态
    SetState(ShardState::MIGRATING);
    
    return true;
}

// 停止迁移
void Shard::StopMigration() {
    is_migrating_ = false;
    
    std::lock_guard<std::mutex> lock(stats_mutex_);
    migration_target_shard_ = -1;
    migration_start_key_.clear();
    migration_end_key_.clear();
    migration_progress_ = 0;
    
    // 更新分片状态
    SetState(ShardState::ACTIVE);
}

// 获取迁移进度
uint64_t Shard::GetMigrationProgress() const {
    std::lock_guard<std::mutex> lock(stats_mutex_);
    return migration_progress_;
}

// ShardManager类实现

// 构造函数
ShardManager::ShardManager(DKVServer* server)
    : server_(server),
      is_running_(false) {
    // 默认配置
    config_.enable_sharding = false;
    config_.num_shards = 1;
    config_.hash_type = HashFunctionType::MD5;
    config_.num_virtual_nodes = 100;
    config_.heartbeat_interval_ms = 1000;
    config_.migration_batch_size = 1000;
    config_.max_concurrent_migrations = 2;
    config_.failover_timeout_ms = 5000;
    
    // 初始化一致性哈希
    consistent_hash_ = std::make_unique<ConsistentHash<int>>(config_.num_virtual_nodes, config_.hash_type);
}

// 析构函数
ShardManager::~ShardManager() {
    Stop();
}

// 初始化分片管理器
bool ShardManager::Initialize(const ShardConfig& config) {
    std::lock_guard<std::mutex> lock(config_mutex_);
    config_ = config;
    
    // 更新一致性哈希配置
    consistent_hash_->SetNumReplicas(config_.num_virtual_nodes);
    consistent_hash_->SetHashFunctionType(config_.hash_type);
    
    return true;
}

// 启动分片管理器
bool ShardManager::Start() {
    if (is_running_.load()) {
        return true; // 已经在运行
    }
    
    // 初始化分片
    if (!InitializeShards()) {
        return false;
    }
    
    // 启动健康检查线程
    health_check_thread_ = std::thread(&ShardManager::HealthCheckThread, this);
    
    // 启动迁移线程
    migration_thread_ = std::thread(&ShardManager::MigrationThread, this);
    
    is_running_ = true;
    
    return true;
}

// 停止分片管理器
void ShardManager::Stop() {
    if (!is_running_.load()) {
        return; // 已经停止
    }
    
    is_running_ = false;
    
    // 停止所有分片
    {   std::lock_guard<std::mutex> lock(shards_mutex_);
        for (auto& pair : shards_) {
            pair.second->Stop();
        }
    }
    
    // 等待健康检查线程结束
    if (health_check_thread_.joinable()) {
        health_check_thread_.join();
    }
    
    // 等待迁移线程结束
    if (migration_thread_.joinable()) {
        migration_thread_.join();
    }
}

// 初始化分片
bool ShardManager::InitializeShards() {
    std::lock_guard<std::mutex> lock(shards_mutex_);
    
    // 如果没有启用分片，创建一个默认分片
    if (!config_.enable_sharding) {
        config_.num_shards = 1;
    }
    
    // 清空现有分片
    shards_.clear();
    
    // 创建指定数量的分片
    for (int i = 0; i < config_.num_shards; i++) {
        // 为每个分片创建Raft组，这里简化处理，使用相同的peers
        // 在实际生产环境中，应该为每个分片配置独立的Raft peers
        std::vector<std::string> raft_peers = {"127.0.0.1:8000", "127.0.0.1:8001", "127.0.0.1:8002"};
        
        // 创建分片
        auto shard = std::make_shared<Shard>(i, raft_peers, "/tmp/dkv_raft", 100 * 1024 * 1024);
        
        // 启动分片
        if (!shard->Start()) {
            return false;
        }
        
        // 添加到分片映射
        shards_[i] = shard;
    }
    
    // 更新一致性哈希环
    UpdateConsistentHash();
    
    return true;
}

// 更新一致性哈希环
void ShardManager::UpdateConsistentHash() {
    std::lock_guard<std::mutex> lock(hash_mutex_);
    
    // 清空现有节点
    consistent_hash_->SetNumReplicas(config_.num_virtual_nodes);
    consistent_hash_->SetHashFunctionType(config_.hash_type);
    
    // 添加所有分片ID作为节点
    std::lock_guard<std::mutex> shard_lock(shards_mutex_);
    for (const auto& pair : shards_) {
        consistent_hash_->AddNode(pair.first);
    }
}

// 处理命令，根据key路由到对应的分片
Response ShardManager::HandleCommand(const Command& command, TransactionID tx_id) {
    // 如果没有启用分片，或者只有一个分片，直接执行
    if (!config_.enable_sharding || config_.num_shards == 1) {
        std::lock_guard<std::mutex> lock(shards_mutex_);
        auto it = shards_.begin();
        if (it != shards_.end()) {
            return it->second->ExecuteCommand(command, tx_id);
        }
        return Response(ResponseStatus::ERROR, "No shards available");
    }
    
    // 获取命令的key
    std::string key;
    if (command.type == CommandType::SET || 
        command.type == CommandType::GET || 
        command.type == CommandType::DEL ||
        command.type == CommandType::EXPIRE ||
        command.type == CommandType::TTL) {
        if (command.args.size() > 0) {
            key = command.args[0];
        }
    }
    
    if (key.empty()) {
        return Response(ResponseStatus::ERROR, "Command requires a key");
    }
    
    // 获取key对应的分片ID
    int shard_id = GetShardId(key);
    
    // 获取分片实例
    std::shared_ptr<Shard> shard;
    {   std::lock_guard<std::mutex> lock(shards_mutex_);
        auto it = shards_.find(shard_id);
        if (it == shards_.end()) {
            return Response(ResponseStatus::ERROR, "Shard not found");
        }
        shard = it->second;
    }
    
    // 执行命令
    return shard->ExecuteCommand(command, tx_id);
}

// 获取key对应的分片ID
int ShardManager::GetShardId(const std::string& key) const {
    if (!config_.enable_sharding || config_.num_shards == 1) {
        return 0; // 默认返回第一个分片
    }
    
    std::lock_guard<std::mutex> lock(hash_mutex_);
    return consistent_hash_->GetNode(key);
}

// 获取分片实例
std::shared_ptr<Shard> ShardManager::GetShard(int shard_id) const {
    std::lock_guard<std::mutex> lock(shards_mutex_);
    auto it = shards_.find(shard_id);
    if (it != shards_.end()) {
        return it->second;
    }
    return nullptr;
}

// 添加分片
bool ShardManager::AddShard(int shard_id, const std::vector<std::string>& raft_peers) {
    std::lock_guard<std::mutex> lock(shards_mutex_);
    
    // 检查分片是否已存在
    if (shards_.find(shard_id) != shards_.end()) {
        return false;
    }
    
    // 创建分片
    auto shard = std::make_shared<Shard>(shard_id, raft_peers, "/tmp/dkv_raft", 100 * 1024 * 1024);
    
    // 启动分片
    if (!shard->Start()) {
        return false;
    }
    
    // 添加到分片映射
    shards_[shard_id] = shard;
    
    // 更新一致性哈希环
    UpdateConsistentHash();
    
    // 更新分片数量配置
    {   std::lock_guard<std::mutex> config_lock(config_mutex_);
        config_.num_shards = static_cast<int>(shards_.size());
    }
    
    return true;
}

// 删除分片
bool ShardManager::RemoveShard(int shard_id) {
    std::lock_guard<std::mutex> lock(shards_mutex_);
    
    // 检查分片是否存在
    auto it = shards_.find(shard_id);
    if (it == shards_.end()) {
        return false;
    }
    
    // 停止分片
    it->second->Stop();
    
    // 从分片映射中删除
    shards_.erase(it);
    
    // 更新一致性哈希环
    UpdateConsistentHash();
    
    // 更新分片数量配置
    {   std::lock_guard<std::mutex> config_lock(config_mutex_);
        config_.num_shards = static_cast<int>(shards_.size());
    }
    
    return true;
}

// 获取所有分片统计信息
std::vector<ShardStats> ShardManager::GetAllShardStats() const {
    std::vector<ShardStats> stats_list;
    
    std::lock_guard<std::mutex> lock(shards_mutex_);
    for (const auto& pair : shards_) {
        stats_list.push_back(pair.second->GetStats());
    }
    
    return stats_list;
}

// 获取分片配置
ShardConfig ShardManager::GetConfig() const {
    std::lock_guard<std::mutex> lock(config_mutex_);
    return config_;
}

// 更新分片配置
bool ShardManager::UpdateConfig(const ShardConfig& config) {
    std::lock_guard<std::mutex> lock(config_mutex_);
    
    // 如果分片数量发生变化，需要重新初始化分片
    if (config_.num_shards != config.num_shards) {
        config_ = config;
        
        // 停止并重新初始化分片
        Stop();
        return Start();
    }
    
    // 更新其他配置
    config_ = config;
    
    // 更新一致性哈希配置
    consistent_hash_->SetNumReplicas(config_.num_virtual_nodes);
    consistent_hash_->SetHashFunctionType(config_.hash_type);
    
    return true;
}

// 触发分片迁移
bool ShardManager::TriggerMigration(int source_shard_id, int target_shard_id, 
                                   const std::string& start_key, const std::string& end_key) {
    // 检查源分片和目标分片是否存在
    std::shared_ptr<Shard> source_shard, target_shard;
    {
        std::lock_guard<std::mutex> lock(shards_mutex_);
        
        auto it_source = shards_.find(source_shard_id);
        auto it_target = shards_.find(target_shard_id);
        
        if (it_source == shards_.end() || it_target == shards_.end()) {
            return false;
        }
        
        source_shard = it_source->second;
        target_shard = it_target->second;
    }
    
    // 开始迁移
    return source_shard->StartMigration(target_shard_id, start_key, end_key);
}

// 获取迁移任务列表
std::vector<ShardMigrationTask> ShardManager::GetMigrationTasks() const {
    std::vector<ShardMigrationTask> tasks;
    
    // 目前简化实现，实际需要从分片的迁移状态中收集
    return tasks;
}

// 执行健康检查
void ShardManager::RunHealthCheck() {
    std::lock_guard<std::mutex> lock(shards_mutex_);
    
    for (auto& pair : shards_) {
        pair.second->Heartbeat();
    }
    
    // 检查故障转移
    CheckFailover();
}

// 健康检查线程函数
void ShardManager::HealthCheckThread() {
    while (is_running_.load()) {
        // 执行健康检查
        RunHealthCheck();
        
        // 等待心跳间隔
        std::this_thread::sleep_for(std::chrono::milliseconds(config_.heartbeat_interval_ms));
    }
}

// 迁移线程函数
void ShardManager::MigrationThread() {
    while (is_running_.load()) {
        // 检查是否有迁移任务需要执行
        // 目前简化实现，实际需要从迁移任务队列中获取任务并执行
        
        // 等待一段时间
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }
}

// 检查分片是否需要故障转移
void ShardManager::CheckFailover() {
    std::lock_guard<std::mutex> lock(shards_mutex_);
    
    uint64_t current_time = GetCurrentTimestamp();
    
    for (auto& pair : shards_) {
        ShardStats stats = pair.second->GetStats();
        
        // 检查最后心跳时间，如果超过故障转移超时，标记为故障
        if (current_time - stats.last_heartbeat > config_.failover_timeout_ms) {
            pair.second->SetState(ShardState::FAILED);
            
            // 添加到故障分片集合
            {   std::lock_guard<std::mutex> fail_lock(failed_shards_mutex_);
                failed_shards_.insert(pair.first);
            }
            
            // 执行故障转移
            FailoverShard(pair.first);
        }
    }
}

// 执行故障转移
bool ShardManager::FailoverShard(int shard_id) {
    // 简化实现：目前只打印日志，实际需要启动新的Raft节点或重新分配分片
    std::cout << "Failover for shard " << shard_id << " started" << std::endl;
    
    // 从故障分片集合中移除
    {   std::lock_guard<std::mutex> fail_lock(failed_shards_mutex_);
        failed_shards_.erase(shard_id);
    }
    
    return true;
}

// 重新平衡分片
bool ShardManager::RebalanceShards() {
    // 简化实现：目前只更新一致性哈希环
    UpdateConsistentHash();
    return true;
}

} // namespace dkv
