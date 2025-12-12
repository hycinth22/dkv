#pragma once

#include "dkv_raft.h"
#include <string>
#include <mutex>

namespace dkv {

// RAFT持久化实现
class RaftFilePersister : public RaftPersister {
public:
    // 构造函数
    explicit RaftFilePersister(const std::string& dir);
    
    // 保存状态
    void SaveState(int term, int votedFor) override;
    
    // 保存日志
    void SaveLog(const std::vector<RaftLogEntry>& log) override;
    
    // 保存快照
    void SaveSnapshot(const std::vector<char>& snapshot) override;
    
    // 读取任期
    int ReadTerm() override;
    
    // 读取投票给谁
    int ReadVotedFor() override;
    
    // 读取日志
    std::vector<RaftLogEntry> ReadLog() override;
    
    // 读取快照
    std::vector<char> ReadSnapshot() override;
    
private:
    // 持久化目录
    std::string dir_;
    
    // 互斥锁
    mutable std::mutex mutex_;
    
    // 状态文件路径
    std::string stateFilePath_;
    
    // 日志文件路径
    std::string logFilePath_;
    
    // 快照文件路径
    std::string snapshotFilePath_;
};

} // namespace dkv
