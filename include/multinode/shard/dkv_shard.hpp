#pragma once

#include "dkv_consistent_hash.hpp"
#include "../raft/dkv_raft.hpp"
#include "../../dkv_server.hpp"
#include <atomic>
#include <memory>
#include <vector>
#include <unordered_map>
#include <mutex>
#include <condition_variable>
#include <thread>
#include <chrono>

namespace dkv {

// 分片状态枚举
enum class ShardState {
    ACTIVE,     // 活跃状态
    INACTIVE,   // 非活跃状态
    MIGRATING,  // 迁移中
    FAILED      // 故障状态
};

// 分片统计信息结构体
struct ShardStats {
    int shard_id;               // 分片ID
    ShardState state;           // 分片状态
    uint64_t key_count;         // 键数量
    size_t memory_usage;        // 内存使用量（字节）
    int raft_group_size;        // Raft组大小
    int raft_leader_id;         // Raft领导者ID
    uint64_t operations_per_second; // 每秒操作数
    uint64_t migration_progress;    // 迁移进度（0-100）
    uint64_t last_heartbeat;        // 最后心跳时间戳
};

// 分片配置结构体
struct ShardConfig {
    bool enable_sharding;       // 是否启用分片功能
    int num_shards;             // 分片数量
    HashFunctionType hash_type; // 哈希函数类型
    int num_virtual_nodes;      // 虚拟节点数量
    int heartbeat_interval_ms;  // 心跳间隔（毫秒）
    int migration_batch_size;   // 迁移批次大小
    int max_concurrent_migrations; // 最大并发迁移数
    int failover_timeout_ms;    // 故障转移超时时间（毫秒）
    bool enable_auto_migration; // 是否启用自动迁移
    int health_check_interval_ms; // 健康检查间隔（毫秒）
    int monitoring_interval_ms; // 监控间隔（毫秒）
};

// 分片迁移任务结构体
struct ShardMigrationTask {
    int source_shard_id;        // 源分片ID
    int target_shard_id;        // 目标分片ID
    std::string start_key;      // 起始键
    std::string end_key;        // 结束键
    uint64_t total_keys;        // 总键数
    uint64_t migrated_keys;     // 已迁移键数
    bool is_completed;          // 是否已完成
    bool is_failed;             // 是否失败
    std::string error_message;  // 错误信息
};

// 分片类，每个分片对应一个独立的Raft group
class Shard {
public:
    // 构造函数
    Shard(int shard_id, const std::vector<std::string>& raft_peers, 
          const std::string& raft_data_dir, int max_raft_state);
    
    // 析构函数
    ~Shard();
    
    // 启动分片
    bool Start();
    
    // 停止分片
    void Stop();
    
    // 执行命令
    Response ExecuteCommand(const Command& command, TransactionID tx_id);
    
    // 获取分片ID
    int GetShardId() const { return shard_id_; }
    
    // 获取分片状态
    ShardState GetState() const { return state_; }
    
    // 设置分片状态
    void SetState(ShardState state);
    
    // 获取Raft实例
    std::shared_ptr<Raft> GetRaft() const { return raft_; }
    
    // 获取分片统计信息
    ShardStats GetStats() const;
    
    // 执行心跳检查
    bool Heartbeat();
    
    // 开始迁移数据到目标分片
    bool StartMigration(int target_shard_id, const std::string& start_key, const std::string& end_key);
    
    // 停止迁移
    void StopMigration();
    
    // 获取迁移进度
    uint64_t GetMigrationProgress() const;
    
private:
    int shard_id_;              // 分片ID
    ShardState state_;          // 分片状态
    mutable std::mutex state_mutex_; // 状态锁
    
    // Raft相关组件
    std::shared_ptr<RaftPersister> raft_persister_;
    std::shared_ptr<RaftNetwork> raft_network_;
    std::shared_ptr<RaftStateMachine> raft_state_machine_;
    std::shared_ptr<Raft> raft_;
    
    // Raft配置
    std::vector<std::string> raft_peers_;
    std::string raft_data_dir_;
    int max_raft_state_;
    
    // 统计信息
    mutable std::mutex stats_mutex_;
    uint64_t key_count_;        // 键数量
    size_t memory_usage_;       // 内存使用量
    uint64_t operations_per_second_; // 每秒操作数
    uint64_t migration_progress_;    // 迁移进度
    uint64_t last_heartbeat_;        // 最后心跳时间
    
    // 迁移相关
    std::atomic<bool> is_migrating_; // 是否正在迁移
    int migration_target_shard_;     // 迁移目标分片ID
    std::string migration_start_key_; // 迁移起始键
    std::string migration_end_key_;   // 迁移结束键
};

// 分片管理器类，用于管理多个分片
class ShardManager {
public:
    // 构造函数
    ShardManager(DKVServer* server);
    
    // 析构函数
    ~ShardManager();
    
    // 初始化分片管理器
    bool Initialize(const ShardConfig& config);
    
    // 启动分片管理器
    bool Start();
    
    // 停止分片管理器
    void Stop();
    
    // 处理命令，根据key路由到对应的分片
    Response HandleCommand(const Command& command, TransactionID tx_id);
    
    // 获取key对应的分片ID
    int GetShardId(const std::string& key) const;
    
    // 获取分片实例
    std::shared_ptr<Shard> GetShard(int shard_id) const;
    
    // 添加分片
    bool AddShard(int shard_id, const std::vector<std::string>& raft_peers);
    
    // 删除分片
    bool RemoveShard(int shard_id);
    
    // 获取所有分片统计信息
    std::vector<ShardStats> GetAllShardStats() const;
    
    // 获取分片配置
    ShardConfig GetConfig() const;
    
    // 更新分片配置
    bool UpdateConfig(const ShardConfig& config);
    
    // 触发分片迁移
    bool TriggerMigration(int source_shard_id, int target_shard_id, 
                          const std::string& start_key, const std::string& end_key);
    
    // 获取迁移任务列表
    std::vector<ShardMigrationTask> GetMigrationTasks() const;
    
    // 执行健康检查
    void RunHealthCheck();
    
    // 执行故障转移
    bool FailoverShard(int shard_id);
    
private:
    // 初始化分片
    bool InitializeShards();
    
    // 重新平衡分片
    bool RebalanceShards();
    
    // 健康检查线程函数
    void HealthCheckThread();
    
    // 迁移线程函数
    void MigrationThread();
    
    // 检查分片是否需要故障转移
    void CheckFailover();
    
    // 更新一致性哈希环
    void UpdateConsistentHash();
    
    DKVServer* server_;         // 指向服务器实例
    
    // 分片配置
    ShardConfig config_;
    mutable std::mutex config_mutex_;
    
    // 分片映射，分片ID到分片实例
    std::unordered_map<int, std::shared_ptr<Shard>> shards_;
    mutable std::mutex shards_mutex_;
    
    // 一致性哈希实例
    std::unique_ptr<ConsistentHash<int>> consistent_hash_;
    mutable std::mutex hash_mutex_;
    
    // 运行状态
    std::atomic<bool> is_running_;
    
    // 健康检查线程
    std::thread health_check_thread_;
    
    // 迁移线程
    std::thread migration_thread_;
    
    // 迁移任务队列
    std::vector<ShardMigrationTask> migration_tasks_;
    mutable std::mutex migration_mutex_;
    std::condition_variable migration_cv_;
    
    // 故障分片ID集合
    std::unordered_set<int> failed_shards_;
    mutable std::mutex failed_shards_mutex_;
};

} // namespace dkv
