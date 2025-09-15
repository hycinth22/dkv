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
    std::atomic<bool> running_;
    
    // 清理线程
    std::thread cleanup_thread_;
    std::atomic<bool> cleanup_running_;
    
    // 配置参数
    int port_;
    std::string config_file_;
    size_t max_memory_; // 最大内存限制（字节）

public:
    DKVServer(int port = 6379);
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
    
private:
    // 初始化服务器
    bool initialize();
    
    // 清理过期键的线程函数
    void cleanupExpiredKeys();
    
    // 解析配置文件
    bool parseConfigFile(const std::string& config_file);
};

} // namespace dkv