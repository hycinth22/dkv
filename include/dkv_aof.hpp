#ifndef DKV_AOF_HPP
#define DKV_AOF_HPP

#include "dkv_core.hpp"
#include <fstream>
#include <string>
#include <mutex>
#include <atomic>
#include <thread>
#include <condition_variable>

namespace dkv {
class DKVServer;
class StorageEngine;

// AOF文件操作相关类
class AOFPersistence {
public:
    // AOF持久化策略
    enum class FsyncPolicy {
        NEVER = 0,   // 从不调用fsync
        EVERYSEC = 1, // 每秒调用一次fsync
        ALWAYS = 2   // 每次写入都调用fsync
    };

    AOFPersistence();
    ~AOFPersistence();
    
    // 设置服务器引用
    void setServer(DKVServer* server);

    // 初始化AOF文件
    bool initialize(const std::string& filename, FsyncPolicy fsync_policy = FsyncPolicy::EVERYSEC);

    // 关闭AOF文件
    void close();

    // 写入命令到AOF文件
    bool appendCommand(const Command& command);

    // 从AOF文件恢复数据
    bool loadFromFile(DKVServer* server);

    // 执行AOF重写
    bool rewrite(StorageEngine* storage_engine, const std::string& temp_filename);
    
    // 异步执行AOF重写
    void asyncRewrite(DKVServer* server);

    // 获取AOF当前状态
    bool isEnabled() const { return enabled_; }

    // 设置自动重写参数
    void setAutoRewriteParams(double percentage, size_t min_size_mb);

    // 检查是否需要自动重写
    bool shouldRewrite();

    // 获取当前AOF文件大小
    size_t getFileSize();

private:
    // 服务器引用
    DKVServer* server_;
    // AOF文件相关
    std::ofstream aof_file_;
    std::string filename_;
    bool enabled_, recovering; 
    std::mutex file_mutex_; // 保护文件操作

    // Fsync策略
    FsyncPolicy fsync_policy_;
    std::atomic<Timestamp> last_fsync_time_;
    
    // 后台fsync线程相关
    std::thread bg_fsync_thread_;
    std::atomic<bool> running_;
    
    // 后台重写检查线程相关
    std::thread bg_rewrite_check_thread_;
    std::atomic<bool> rewrite_check_running_;
    std::condition_variable rewrite_check_cv_;
    std::mutex rewrite_check_mutex_;
    
    // 自动重写相关参数
    double auto_rewrite_percentage_;  // 自动重写百分比阈值
    size_t auto_rewrite_min_size_mb_; // 自动重写最小文件大小(MB)
    size_t last_rewrite_size_;        // 上次重写后的文件大小(字节)

    // 同步文件到磁盘
    void fsyncIfNeeded();

    // 将命令序列化为RESP格式并写入文件
    bool writeCommandToFile(const Command& command);

    // 解析AOF文件中的命令
    Command parseCommandFromFile(std::ifstream& file);
    
    // 后台fsync线程函数
    void bgFsyncThreadFunc();
    
    // 后台重写检查线程函数
    void bgRewriteCheckThreadFunc();
};

} // namespace dkv

#endif // DKV_AOF_HPP