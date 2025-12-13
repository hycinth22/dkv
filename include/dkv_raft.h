#pragma once

#include <vector>
#include <string>
#include <memory>
#include <functional>
#include <mutex>
#include <condition_variable>
#include <atomic>
#include <queue>
#include <thread>
#include <unordered_map>
#include <iostream>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <cstring>
#include "dkv_core.hpp"
#include "dkv_utils.hpp"

namespace dkv {

// RAFT协议相关的常量定义
static constexpr int RAFT_INVALID_TERM = -1;
static constexpr int RAFT_INVALID_INDEX = -1;
static constexpr int RAFT_DEFAULT_ELECTION_TIMEOUT = 500; // 默认选举超时时间（毫秒）
static constexpr int RAFT_DEFAULT_HEARTBEAT_INTERVAL = 100; // 默认心跳间隔（毫秒）

// RAFT节点状态枚举
enum class RaftState {
    FOLLOWER,
    CANDIDATE,
    LEADER
};

// RAFT日志条目结构
struct RaftLogEntry {
    int term;                 // 日志的任期
    std::shared_ptr<Command> command;  // 日志命令
    int index;                // 日志索引
};

// RAFT RPC请求和响应结构
struct AppendEntriesRequest {
    int term;                 // 领导者的任期
    int leaderId;             // 领导者ID
    int prevLogIndex;         // 前一个日志的索引
    int prevLogTerm;          // 前一个日志的任期
    std::vector<RaftLogEntry> entries; // 要追加的日志条目
    int leaderCommit;         // 领导者的已提交索引
};

struct AppendEntriesResponse {
    int term;                 // 当前节点的任期
    bool success;             // 请求是否成功
    int matchIndex;           // 匹配的索引
};

struct RequestVoteRequest {
    int term;                 // 候选人的任期
    int candidateId;          // 候选人ID
    int lastLogIndex;         // 候选人最后一个日志的索引
    int lastLogTerm;          // 候选人最后一个日志的任期
};

struct RequestVoteResponse {
    int term;                 // 当前节点的任期
    bool voteGranted;         // 是否授予选票
};

struct InstallSnapshotRequest {
    int term;                 // 领导者的任期
    int leaderId;             // 领导者ID
    int lastIncludedIndex;    // 快照包含的最后一个日志索引
    int lastIncludedTerm;     // 快照包含的最后一个日志任期
    std::vector<char> snapshot; // 快照数据
    int leaderCommit;         // 领导者的已提交索引
};

struct InstallSnapshotResponse {
    int term;                 // 当前节点的任期
    bool success;             // 请求是否成功
};

// RAFT状态机接口
class RaftStateMachine {
public:
    virtual ~RaftStateMachine() = default;
    
    // 执行命令并返回结果
    virtual Response DoOp(const Command& command) = 0;
    
    // 创建快照
    virtual std::vector<char> Snapshot() = 0;
    
    // 从快照恢复
    virtual void Restore(const std::vector<char>& snapshot) = 0;
};

// RAFT持久化接口
class RaftPersister {
public:
    virtual ~RaftPersister() = default;
    
    // 保存RAFT状态
    virtual void SaveState(int term, int votedFor) = 0;
    
    // 保存日志
    virtual void SaveLog(const std::vector<RaftLogEntry>& log) = 0;
    
    // 保存快照
    virtual void SaveSnapshot(const std::vector<char>& snapshot) = 0;
    
    // 读取任期
    virtual int ReadTerm() = 0;
    
    // 读取投票给谁
    virtual int ReadVotedFor() = 0;
    
    // 读取日志
    virtual std::vector<RaftLogEntry> ReadLog() = 0;
    
    // 读取快照
    virtual std::vector<char> ReadSnapshot() = 0;
};

// RAFT网络接口
class RaftNetwork {
public:
    virtual ~RaftNetwork() = default;
    
    // 发送AppendEntries请求
    virtual AppendEntriesResponse SendAppendEntries(int serverId, const AppendEntriesRequest& request) = 0;
    
    // 发送RequestVote请求
    virtual RequestVoteResponse SendRequestVote(int serverId, const RequestVoteRequest& request) = 0;
    
    // 发送InstallSnapshot请求
    virtual InstallSnapshotResponse SendInstallSnapshot(int serverId, const InstallSnapshotRequest& request) = 0;
};

// RAFT核心类
class Raft {
public:
    // 构造函数
    Raft(int me, const std::vector<std::string>& peers, 
         std::shared_ptr<RaftPersister> persister, 
         std::shared_ptr<RaftNetwork> network, 
         std::shared_ptr<RaftStateMachine> stateMachine);
    
    // 析构函数
    ~Raft();
    
    // 启动RAFT
    void Start();
    
    // 停止RAFT
    void Stop();
    
    // 提交命令到RAFT日志
    bool StartCommand(const Command& command, int& index, int& term) {
        return StartCommand(std::make_shared<Command>(command), index, term);
    }
    bool StartCommand(const std::shared_ptr<Command>& command, int& index, int& term);

    // 获取当前节点ID
    int GetMe() const { return me_; }
    
    // 获取当前节点状态
    RaftState GetState() const;
    
    // 获取当前任期
    int GetCurrentTerm() const;
    
    // 判断是否是领导者
    bool IsLeader() const;
    
    // 获取提交索引
    int GetCommitIndex() const;
    
    // 获取当前节点认为的领导者ID
    int GetCurrentLeaderId() const;

    // 处理AppendEntries请求
    AppendEntriesResponse OnAppendEntries(const AppendEntriesRequest& request);
    
    // 处理RequestVote请求
    RequestVoteResponse OnRequestVote(const RequestVoteRequest& request);
    
    // 处理InstallSnapshot请求
    InstallSnapshotResponse OnInstallSnapshot(const InstallSnapshotRequest& request);
    
    // 创建快照
    void Snapshot(int index, const std::vector<char>& snapshot);
    
    // 获取持久化字节数（用于快照决策）
    size_t PersistBytes() const;
    
private:
    // 重置选举计时器
    void ResetElectionTimer();
    
    // 开始选举
    void StartElection();
    
    // 发送心跳
    void SendHeartbeats();
    
    // 处理选举超时
    void HandleElectionTimeout();
    
    // 应用日志到状态机
    void ApplyLogs();
    
    // 更新提交索引
    void UpdateCommitIndex();
    
    // 检查日志一致性
    bool IsLogConsistent(int prevLogIndex, int prevLogTerm);
    
    // 验证并添加日志条目
    bool ValidateAndAppendEntries(const std::vector<RaftLogEntry>& entries, int prevLogIndex);
    
    // 复制日志条目到follower
    void ReplicateLogs();
    
    // 持久化状态
    void PersistState();
    
    // 持久化日志
    void PersistLog();
    
    // 从持久化恢复
    void RestoreFromPersist();
    
    // RAFT节点ID
    int me_;
    
    // 集群节点列表
    std::vector<std::string> peers_;
    
    // RAFT状态
    mutable std::mutex mutex_;
    RaftState state_;
    int currentTerm_;
    int votedFor_;
    std::vector<RaftLogEntry> log_;
    
    // 提交和应用相关
    int commitIndex_;
    int lastApplied_;
    
    // 领导者相关
    std::vector<int> nextIndex_;
    std::vector<int> matchIndex_;
    
    // 计时器
    std::atomic<int> electionTimeout_;
    std::atomic<int64_t> lastElectionTime_;
    
    // 持久化接口
    std::shared_ptr<RaftPersister> persister_;
    
    // 网络接口
    std::shared_ptr<RaftNetwork> network_;
    
    // 状态机接口
    std::shared_ptr<RaftStateMachine> stateMachine_;
    
    // 运行标志
    std::atomic<bool> running_;
    
    // 内部线程
    std::thread raftThread_;
    
    // 日志起始索引
    int logStartIndex_;
    
    // 快照配置
    int max_raft_state_; // 日志最大大小，超过则创建快照
    
    // 当前节点认为的领导者ID
    int currentLeaderId_;
    
};

// RAFT状态机管理器的声明
// 详细实现见 dkv_raft_statemachine.h
class RaftStateMachineManager;

// RAFT持久化实现的声明
// 详细实现见 dkv_raft_persist.h
class RaftFilePersister;


} // namespace dkv
