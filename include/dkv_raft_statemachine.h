#pragma once

#include "dkv_raft.h"
#include "dkv_storage.hpp"
#include <string>
#include <mutex>

namespace dkv {

class DKVServer;

// RAFT状态机管理器
class RaftStateMachineManager : public RaftStateMachine {
public:
    // 构造函数
    RaftStateMachineManager();
    
    // 执行命令
    Response DoOp(const RaftCommand& command) override;
    
    // 创建快照
    std::vector<char> Snapshot() override;
    
    // 从快照恢复
    void Restore(const std::vector<char>& snapshot) override;
    
    // 设置命令处理器
    void SetCommandHandler(void* commandHandler);
    
    // 设置存储引擎
    void SetStorageEngine(StorageEngine* storageEngine);
    
    // 设置DKV服务器
    void SetDKVServer(DKVServer* server);
    
private:
    // 命令处理器指针
    void* commandHandler_;
    
    // 存储引擎指针
    StorageEngine* storageEngine_;
    
    // DKV服务器指针
    DKVServer* dkvServer_;
    
    // 互斥锁
    mutable std::mutex mutex_;
};

} // namespace dkv
