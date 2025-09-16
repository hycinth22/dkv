#ifndef DKV_AOF_HPP
#define DKV_AOF_HPP

#include "dkv_core.hpp"
#include <fstream>
#include <string>
#include <mutex>
#include <atomic>
#include <thread>

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

    // 获取AOF当前状态
    bool isEnabled() const { return enabled_; }

private:
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

    // 同步文件到磁盘
    void fsyncIfNeeded();

    // 将命令序列化为RESP格式并写入文件
    bool writeCommandToFile(const Command& command);

    // 解析AOF文件中的命令
    Command parseCommandFromFile(std::ifstream& file);
    
    // 后台fsync线程函数
    void bgFsyncThreadFunc();
};

} // namespace dkv

#endif // DKV_AOF_HPP