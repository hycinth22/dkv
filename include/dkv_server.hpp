#pragma once

#include "dkv_core.hpp"
#include "dkv_storage.hpp"
#include "dkv_network.hpp"
#include <memory>
#include <thread>
#include <atomic>

namespace dkv {

// 数据库服务器主类
class DKVServer {
private:
    std::unique_ptr<StorageEngine> storage_engine_;
    std::unique_ptr<NetworkServer> network_server_;
    std::unique_ptr<WorkerThreadPool> worker_pool_;
    std::atomic<bool> running_;
    
    // 清理线程
    std::thread cleanup_thread_;
    std::atomic<bool> cleanup_running_;
    
    // 配置参数
    std::string config_file_;
    int port_;
    size_t max_memory_; // 最大内存限制（字节）
    size_t num_sub_reactors_; // 子Reactor数量
    size_t num_workers_;      // 工作线程数量
    
    // RDB持久化相关配置
    bool enable_rdb_;         // 是否启用RDB持久化
    std::string rdb_filename_; // RDB文件名
    uint64_t rdb_save_interval_;   // RDB保存间隔（秒）
    uint64_t rdb_save_changes_;    // RDB保存变更数量阈值
    
    // RDB自动保存相关
    std::atomic<uint64_t> rdb_changes_;  // 数据变更计数
    std::atomic<Timestamp> last_save_time_; // 上次RDB保存时间
    std::thread rdb_save_thread_;        // RDB自动保存线程
    std::atomic<bool> rdb_save_running_; // RDB自动保存线程运行标志

public:
    DKVServer(int port = 6379, size_t num_sub_reactors = 4, size_t num_workers = 8);
    ~DKVServer();
    
    // 启动和停止服务器
    bool start();
    void stop();
    
    // 配置管理
    bool loadConfig(const std::string& config_file);
    void setPort(int port);
    void setMaxMemory(size_t max_memory);
    
    // 统计信息
    size_t getKeyCount() const;
    uint64_t getTotalKeys() const;
    uint64_t getExpiredKeys() const;
    
    // 运行状态
    bool isRunning() const;
    
    // 执行命令
    Response executeCommand(const Command& command);
    
    // 获取内存使用量
    size_t getMemoryUsage() const;
    
    // 获取最大内存限制
    size_t getMaxMemory() const;
    
    // RDB持久化配置方法
    void setRDBEnabled(bool enabled);
    void setRDBFilename(const std::string& filename);
    void setRDBSaveInterval(uint64_t interval);
    void setRDBSaveChanges(uint64_t changes);
    
private:
    // 初始化服务器
    bool initialize();
    
    // 清理过期键的线程函数
    void cleanupExpiredKeys();
    
    // 解析配置文件
    bool parseConfigFile(const std::string& config_file);
    
    // RDB持久化辅助方法
    void loadRDBFromConfig();
    void saveRDBFromConfig();
    
    // RDB自动保存线程函数
    void rdbAutoSaveThread();
    
    // 增加数据变更计数
    void incDirty();
    void incDirty(int delta);
};

} // namespace dkv