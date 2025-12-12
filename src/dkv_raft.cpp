#include "dkv_raft.h"
#include "dkv_logger.hpp"
#include "dkv_command_handler.hpp"
#include "dkv_resp.hpp"
#include <fstream>
#include <sstream>
#include <chrono>
#include <random>
#include <algorithm>

namespace dkv {

// RAFT构造函数
Raft::Raft(int me, const std::vector<std::string>& peers, std::shared_ptr<RaftPersister> persister, std::shared_ptr<RaftNetwork> network, std::shared_ptr<RaftStateMachine> stateMachine)
    : me_(me), peers_(peers), persister_(persister), network_(network), stateMachine_(stateMachine),
      state_(RaftState::FOLLOWER), currentTerm_(0), votedFor_(-1),
      commitIndex_(0), lastApplied_(0), running_(false), max_raft_state_(100 * 1024 * 1024),
      logStartIndex_(1) {
    
    // 初始化领导者相关数组
    nextIndex_.resize(peers_.size(), 0);
    matchIndex_.resize(peers_.size(), 0);
    
    // 从持久化恢复状态
    RestoreFromPersist();
    
    // 初始化选举计时器
    ResetElectionTimer();
}

// RAFT析构函数
Raft::~Raft() {
    Stop();
}

// 启动RAFT
void Raft::Start() {
    if (running_) {
        return;
    }
    
    running_ = true;
    
    // 启动RAFT核心线程
    raftThread_ = std::thread([this]() {
        while (running_) {
            switch (state_) {
                case RaftState::FOLLOWER:
                    HandleElectionTimeout();
                    break;
                case RaftState::CANDIDATE:
                    StartElection();
                    break;
                case RaftState::LEADER:
                    SendHeartbeats();
                    ReplicateLogs();
                    break;
            }
        }
    });
}

// 停止RAFT
void Raft::Stop() {
    if (!running_) {
        return;
    }
    
    running_ = false;
    
    // 等待线程结束
    if (raftThread_.joinable()) {
        raftThread_.join();
    }
}

// 提交命令到RAFT日志
bool Raft::StartCommand(const std::vector<char>& command, int& index, int& term) {
    std::unique_lock<std::mutex> lock(mutex_);
    
    // 如果不是领导者，返回false
    if (state_ != RaftState::LEADER) {
        DKV_LOG_INFOF("[Node {}] 不是领导者，无法提交命令", me_);
        return false;
    }
    
    // 记录当前任期
    term = currentTerm_;
    
    // 创建日志条目
    RaftLogEntry entry;
    entry.term = currentTerm_;
    entry.command = command;
    entry.index = log_.size() + logStartIndex_;
    
    // 添加到日志
    log_.push_back(entry);
    
    // 持久化日志
    PersistLog();
    
    // 更新领导者自己的matchIndex
    if (me_ >= 0 && me_ < static_cast<int>(matchIndex_.size())) {
        matchIndex_[me_] = entry.index;
        DKV_LOG_INFOF("[Node {}] 更新自己的matchIndex为 {}", me_, matchIndex_[me_]);
    }
    
    // 返回索引
    index = entry.index;
    
    DKV_LOG_INFOF("[Node {}] 成功提交命令，索引: {}, 任期: {}", me_, index, term);
    

    // 立即调用ReplicateLogs，确保日志条目能够及时被复制到跟随者
    lock.unlock();
    ReplicateLogs();
    
    return true;
}

// 获取当前节点状态
RaftState Raft::GetState() const {
    std::unique_lock<std::mutex> lock(mutex_);
    return state_;
}

// 获取当前任期
int Raft::GetCurrentTerm() const {
    std::unique_lock<std::mutex> lock(mutex_);
    return currentTerm_;
}

// 判断是否是领导者
bool Raft::IsLeader() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return running_ && state_ == RaftState::LEADER;
}

// 获取提交索引
int Raft::GetCommitIndex() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return commitIndex_;
}

// 处理AppendEntries请求
AppendEntriesResponse Raft::OnAppendEntries(const AppendEntriesRequest& request) {
    std::unique_lock<std::mutex> lock(mutex_);
    
    AppendEntriesResponse response;
    response.term = currentTerm_;
    response.success = false;
    
    // 1. 如果请求的任期小于当前任期，返回false
    if (request.term < currentTerm_) {
        return response;
    }
    
    // 2. 如果请求的任期大于当前任期，更新当前任期和状态
    if (request.term > currentTerm_) {
        currentTerm_ = request.term;
        state_ = RaftState::FOLLOWER;
        votedFor_ = -1;
        PersistState();
    }
    
    // 3. 重置选举计时器
    ResetElectionTimer();
    
    // 4. 检查日志一致性
    if (IsLogConsistent(request.prevLogIndex, request.prevLogTerm)) {
        // 5. 删除冲突的日志条目
        size_t index = 0;
        for (const auto& entry : log_) {
            if (entry.index > request.prevLogIndex) {
                break;
            }
            index++;
        }
        
        if (index < log_.size()) {
            DKV_LOG_DEBUGF("[Node {}] 删除冲突的日志条目，从索引 {} 开始", me_, index);
            log_.erase(log_.begin() + index, log_.end());
        }
        
        // 6. 检查并添加新的日志条目
        if (ValidateAndAppendEntries(request.entries, request.prevLogIndex)) {
            DKV_LOG_DEBUGF("[Node {}] 添加了 {} 个新的日志条目，当前日志数量: {}", me_, request.entries.size(), log_.size());
            // 7. 持久化日志
            PersistLog();
            
            // 8. 更新提交索引
            if (request.leaderCommit > commitIndex_) {
                int oldCommitIndex = commitIndex_;
                commitIndex_ = std::min(request.leaderCommit, log_.empty() ? logStartIndex_ - 1 : log_.back().index);
                DKV_LOG_INFOF("[Node {}] 更新提交索引从 {} 到 {}", me_, oldCommitIndex, commitIndex_);
                lock.unlock();
                ApplyLogs();
                lock.lock();
            }
            
            // 9. 返回成功
            response.success = true;
            response.matchIndex = log_.empty() ? logStartIndex_ - 1 : log_.back().index;
            DKV_LOG_DEBUGF("[Node {}] AppendEntries 请求成功，matchIndex: {}", me_, response.matchIndex);
        } else {
            response.success = false;
            response.matchIndex = log_.empty() ? logStartIndex_ - 1 : log_.back().index;
            DKV_LOG_DEBUGF("[Node {}] AppendEntries 请求失败：日志条目验证失败", me_);
            return response;
        }
    } else {
        DKV_LOG_DEBUGF("[Node {}] AppendEntries 请求失败：日志不一致，prevLogIndex: {}, prevLogTerm: {}", me_, request.prevLogIndex, request.prevLogTerm);
    }
    
    return response;
}

// 处理RequestVote请求
RequestVoteResponse Raft::OnRequestVote(const RequestVoteRequest& request) {
    std::unique_lock<std::mutex> lock(mutex_);
    
    DKV_LOG_DEBUGF("[Node {}] 收到来自节点 {} 的RequestVote请求，任期 {}，lastLogIndex {}，lastLogTerm {}", 
                me_, request.candidateId, request.term, request.lastLogIndex, request.lastLogTerm);
    
    RequestVoteResponse response;
    response.term = currentTerm_;
    response.voteGranted = false;
    
    // 1. 如果请求的任期小于当前任期，返回false
    if (request.term < currentTerm_) {
        DKV_LOG_DEBUGF("[Node {}] RequestVote请求任期 {} < 当前任期 {}，拒绝投票", me_, request.term, currentTerm_);
        return response;
    }
    
    // 2. 如果请求的任期大于当前任期，更新当前任期和状态
    if (request.term > currentTerm_) {
        DKV_LOG_INFOF("[Node {}] RequestVote请求任期 {} > 当前任期 {}，更新任期和状态为FOLLOWER", me_, request.term, currentTerm_);
        currentTerm_ = request.term;
        state_ = RaftState::FOLLOWER;
        votedFor_ = -1;
        PersistState();
    }
    
    // 3. 检查是否已经投票给其他候选人
    bool votedForValid = (votedFor_ == -1 || votedFor_ == request.candidateId);
    if (!votedForValid) {
        DKV_LOG_DEBUGF("[Node {}] 已经投票给节点 {}，拒绝投票给节点 {}", me_, votedFor_, request.candidateId);
    }
    
    // 4. 检查候选人的日志是否至少和自己一样新
    bool logIsUpToDate = false;
    int myLastLogIndex = log_.empty() ? (logStartIndex_ - 1) : log_.back().index;
    int myLastLogTerm = log_.empty() ? 0 : log_.back().term;
    
    DKV_LOG_DEBUGF("[Node {}] 候选人日志: lastLogIndex {}, lastLogTerm {} | 自己的日志: lastLogIndex {}, lastLogTerm {}", 
                me_, request.lastLogIndex, request.lastLogTerm, myLastLogIndex, myLastLogTerm);
    
    // 如果自己的日志为空，认为候选人的日志是最新的
    if (log_.empty()) {
        logIsUpToDate = true;
        DKV_LOG_DEBUGF("[Node {}] 自己的日志为空，认为候选人日志是最新的", me_);
    } else {
        // 获取自己的最后一个日志条目
        const auto& lastEntry = log_.back();
        
        // 如果候选人的最后一个日志条目任期大于自己的，认为是最新的
        if (request.lastLogTerm > lastEntry.term) {
            logIsUpToDate = true;
            DKV_LOG_DEBUGF("[Node {}] 候选人日志任期 {} > 自己的日志任期 {}，认为是最新的", me_, request.lastLogTerm, lastEntry.term);
        } else if (request.lastLogTerm == lastEntry.term) {
            // 如果任期相同，检查索引是否大于等于自己的
            if (request.lastLogIndex >= lastEntry.index) {
                logIsUpToDate = true;
                DKV_LOG_DEBUGF("[Node {}] 候选人日志任期相同 {}，索引 {} >= 自己的索引 {}，认为是最新的", 
                            me_, request.lastLogTerm, request.lastLogIndex, lastEntry.index);
            } else {
                DKV_LOG_DEBUGF("[Node {}] 候选人日志任期相同 {}，索引 {} < 自己的索引 {}，认为不是最新的", 
                            me_, request.lastLogTerm, request.lastLogIndex, lastEntry.index);
            }
        } else {
            DKV_LOG_DEBUGF("[Node {}] 候选人日志任期 {} < 自己的日志任期 {}，认为不是最新的", me_, request.lastLogTerm, lastEntry.term);
        }
    }
    
    // 5. 如果满足条件，投票给候选人
    if (votedForValid && logIsUpToDate) {
        DKV_LOG_DEBUGF("[Node {}] 满足投票条件，投票给节点 {}，任期 {}", me_, request.candidateId, currentTerm_);
        votedFor_ = request.candidateId;
        PersistState();
        ResetElectionTimer();
        response.voteGranted = true;
    } else {
        DKV_LOG_DEBUGF("[Node {}] 不满足投票条件，拒绝投票给节点 {}", me_, request.candidateId);
    }
    
    return response;
}

// 处理InstallSnapshot请求
InstallSnapshotResponse Raft::OnInstallSnapshot(const InstallSnapshotRequest& request) {
    std::unique_lock<std::mutex> lock(mutex_);
    
    DKV_LOG_DEBUGF("[Node {}] 收到来自节点 {} 的InstallSnapshot请求，任期 {}，lastIncludedIndex={}，lastIncludedTerm={}", 
                me_, request.leaderId, request.term, request.lastIncludedIndex, request.lastIncludedTerm);
    
    InstallSnapshotResponse response;
    response.term = currentTerm_;
    response.success = false;
    
    // 1. 如果请求的任期小于当前任期，返回false
    if (request.term < currentTerm_) {
        DKV_LOG_DEBUGF("[Node {}] InstallSnapshot请求任期 {} < 当前任期 {}，拒绝请求", me_, request.term, currentTerm_);
        return response;
    }
    
    // 2. 如果请求的任期大于当前任期，更新当前任期和状态
    if (request.term > currentTerm_) {
        DKV_LOG_INFOF("[Node {}] InstallSnapshot请求任期 {} > 当前任期 {}，更新任期和状态为FOLLOWER", me_, request.term, currentTerm_);
        currentTerm_ = request.term;
        state_ = RaftState::FOLLOWER;
        votedFor_ = -1;
        PersistState();
    }
    
    // 重置选举计时器
    ResetElectionTimer();
    
    // 3. 处理快照
    int lastIncludedIndex = request.lastIncludedIndex;
    int lastIncludedTerm = request.lastIncludedTerm;
    
    int lastLogIndex = log_.empty() ? (logStartIndex_ - 1) : log_.back().index;
    
    DKV_LOG_DEBUGF("[Node {}] InstallSnapshot处理：lastIncludedIndex={}, lastLogIndex={}, logStartIndex={}", 
                me_, lastIncludedIndex, lastLogIndex, logStartIndex_);
    
    // 如果快照包含的日志比当前日志新，应用快照
    if (lastIncludedIndex > lastLogIndex) {
        DKV_LOG_DEBUGF("[Node {}] 快照包含的日志比当前日志新，应用快照", me_);
        // 清除所有旧日志
        log_.clear();
        logStartIndex_ = lastIncludedIndex + 1;
        DKV_LOG_DEBUGF("[Node {}] 清除旧日志，更新logStartIndex={}", me_, logStartIndex_);
        
        // 应用快照到状态机
        stateMachine_->Restore(request.snapshot);
        DKV_LOG_DEBUGF("[Node {}] 成功应用快照到状态机", me_);
        
        // 更新lastApplied_和commitIndex_
        lastApplied_ = lastIncludedIndex;
        commitIndex_ = std::max(commitIndex_, lastIncludedIndex);
        DKV_LOG_INFOF("[Node {}] 更新lastApplied={}，commitIndex={}", me_, lastApplied_, commitIndex_);
        
        // 持久化快照
        persister_->SaveSnapshot(request.snapshot);
        DKV_LOG_DEBUGF("[Node {}] 持久化快照成功", me_);
        
        response.success = true;
    } else {
        DKV_LOG_DEBUGF("[Node {}] 快照包含的日志不比当前日志新，跳过应用快照", me_);
        response.success = true;
    }
    
    // 4. 更新commitIndex_从leaderCommit
    if (request.leaderCommit > commitIndex_) {
        int oldCommitIndex = commitIndex_;
        commitIndex_ = std::min(request.leaderCommit, log_.empty() ? logStartIndex_ - 1 : log_.back().index);
        DKV_LOG_INFOF("[Node {}] 从leaderCommit更新commitIndex从 {} 到 {}", me_, oldCommitIndex, commitIndex_);
        ApplyLogs();
    }
    
    DKV_LOG_DEBUGF("[Node {}] InstallSnapshot请求处理完成，返回success={}", me_, response.success);
    return response;
}

// 创建快照
void Raft::Snapshot(int index, const std::vector<char>& snapshot) {
    std::unique_lock<std::mutex> lock(mutex_);
    
    DKV_LOG_DEBUGF("[Node {}] 收到创建快照请求，索引 {}，当前logStartIndex={}", me_, index, logStartIndex_);
    
    // 忽略比当前快照更旧的请求
    if (log_.empty() || index <= (logStartIndex_ - 1)) {
        DKV_LOG_DEBUGF("[Node {}] 快照请求索引 {} <= logStartIndex-1={}，忽略请求", me_, index, logStartIndex_ - 1);
        return;
    }
    
    // 1. 保留快照索引及之后的日志
    std::vector<RaftLogEntry> newLog;
    for (const auto& entry : log_) {
        if (entry.index > index) {
            newLog.push_back(std::move(entry));
        }
    }
    
    size_t oldLogSize = log_.size();
    log_.swap(newLog);
    
    // 更新日志起始索引
    logStartIndex_ = index + 1;
    
    DKV_LOG_DEBUGF("[Node {}] 快照创建：保留索引 {} 之后的日志，旧日志数量 {}，新日志数量 {}，更新logStartIndex={}", 
                me_, index, oldLogSize, log_.size(), logStartIndex_);
    
    // 2. 持久化快照和状态
    persister_->SaveSnapshot(snapshot);
    PersistState();
    PersistLog();
    
    DKV_LOG_INFOF("[Node {}] 快照创建完成，当前日志数量: {}, 日志起始索引: {}", me_, log_.size(), logStartIndex_);
}

// 获取持久化字节数，需要加锁再调用
size_t Raft::PersistBytes() const {
    // 估计持久化数据的大小
    size_t size = 0;
    
    // 状态大小：term + votedFor
    size += sizeof(currentTerm_);
    size += sizeof(votedFor_);
    
    // 日志大小：每个日志条目包括term + index + command大小 + command内容
    for (const auto& entry : log_) {
        size += sizeof(entry.term);
        size += sizeof(entry.index);
        size += sizeof(entry.command.size());
        size += entry.command.size();
    }
    
    return size;
}

// 重置选举计时器
void Raft::ResetElectionTimer() {
    // 生成随机的选举超时时间（150-300ms）
    static std::random_device rd;
    static std::mt19937 gen(rd());
    static std::uniform_int_distribution<> dis(150, 300);
    
    electionTimeout_ = dis(gen);
    lastElectionTime_ = std::chrono::system_clock::now().time_since_epoch().count() / 1000000;
}

// 开始选举
void Raft::StartElection() {
    std::unique_lock<std::mutex> lock(mutex_);
    
    // 增加当前任期
    currentTerm_++;
    votedFor_ = me_;
    
    // 持久化状态
    PersistState();
    
    // 重置选举计时器
    ResetElectionTimer();
    
    // 创建RequestVote请求
    RequestVoteRequest request;
    request.term = currentTerm_;
    request.candidateId = me_;
    request.lastLogTerm = log_.empty() ? 0 : log_.back().term;
    request.lastLogIndex = log_.empty() ? (logStartIndex_ - 1) : log_.back().index;
    
    lock.unlock();
    
    // 发送请求并统计投票
    int votes = 1; // 自己的投票
    DKV_LOG_DEBUGF("[Node {}] 开始选举，任期 {}，请求投票给 {} 个节点", me_, currentTerm_, peers_.size() - 1);
    
    for (int i = 0; i < peers_.size(); i++) {
        if (i == me_) {
            continue;
        }
        
        // 发送请求
        DKV_LOG_DEBUGF("[Node {}] 向节点 {} 发送RequestVote请求，任期 {}", me_, i, request.term);
        RequestVoteResponse response = network_->SendRequestVote(i, request);
        
        lock.lock();
        
        // 检查响应
        if (response.term > currentTerm_) {
            // 更新当前任期和状态
            DKV_LOG_INFOF("[Node {}] 收到更高任期 {}，转换为FOLLOWER", me_, response.term);
            currentTerm_ = response.term;
            state_ = RaftState::FOLLOWER;
            votedFor_ = -1;
            PersistState();
            lock.unlock();
            return;
        }
        
        if (response.voteGranted) {
            votes++;
            DKV_LOG_DEBUGF("[Node {}] 获得节点 {} 的投票，当前票数: {}", me_, i, votes);
            
            // 检查是否获得多数投票
            if (votes > peers_.size() / 2) {
                // 成为领导者
                DKV_LOG_INFOF("[Node {}] 获得多数投票 ({}/{})，成为RAFT领导者，任期: {}", me_, votes, peers_.size(), currentTerm_);
                state_ = RaftState::LEADER;
                
                // 初始化领导者相关数组
                for (size_t j = 0; j < nextIndex_.size(); j++) {
                    nextIndex_[j] = log_.empty() ? logStartIndex_ : log_.back().index + 1;
                    matchIndex_[j] = 0;
                    DKV_LOG_INFOF("[Node {}] 初始化节点 {}: nextIndex={}, matchIndex={}", me_, j, nextIndex_[j], matchIndex_[j]);
                }
                
                lock.unlock();
                return;
            }
        } else {
            DKV_LOG_DEBUGF("[Node {}] 未获得节点 {} 的投票，当前票数: {}", me_, i, votes);
        }
        
        lock.unlock();
    }
    
    DKV_LOG_INFOF("[Node {}] 选举失败，未获得足够的投票，当前票数: {}", me_, votes);
    
    // 没有获得多数投票，继续作为候选人
}

// 发送心跳
void Raft::SendHeartbeats() {
    std::unique_lock<std::mutex> lock(mutex_);
    
    // 创建AppendEntries请求作为心跳
    AppendEntriesRequest request;
    request.term = currentTerm_;
    request.leaderId = me_;
    request.prevLogIndex = log_.empty() ? (logStartIndex_ - 1) : log_.back().index;
    request.prevLogTerm = log_.empty() ? 0 : log_.back().term;
    request.leaderCommit = commitIndex_;
    
    lock.unlock();
    
    DKV_LOG_DEBUGF("[Node {}] 发送心跳，任期 {}，commitIndex: {}", me_, request.term, request.leaderCommit);
    
    // 发送心跳给所有节点
    for (int i = 0; i < peers_.size(); i++) {
        if (i == me_) {
            continue;
        }
        
        // 发送请求
        DKV_LOG_DEBUGF("[Node {}] 向节点 {} 发送心跳，任期 {}", me_, i, request.term);
        AppendEntriesResponse response = network_->SendAppendEntries(i, request);
        
        lock.lock();
        
        // 检查响应
        if (response.term > currentTerm_) {
            // 更新当前任期和状态
            DKV_LOG_INFOF("[Node {}] 收到更高任期 {}，转换为FOLLOWER", me_, response.term);
            currentTerm_ = response.term;
            state_ = RaftState::FOLLOWER;
            votedFor_ = -1;
            PersistState();
            lock.unlock();
            return;
        }
        
        lock.unlock();
    }
    
    // 短暂休眠，避免CPU占用过高
    std::this_thread::sleep_for(std::chrono::milliseconds(RAFT_DEFAULT_HEARTBEAT_INTERVAL));
}

// 处理选举超时
void Raft::HandleElectionTimeout() {
    std::unique_lock<std::mutex> lock(mutex_);
    
    // 检查是否超时
    int64_t now = std::chrono::system_clock::now().time_since_epoch().count() / 1000000;
    if (now - lastElectionTime_ > electionTimeout_) {
        // 超时，成为候选人
        DKV_LOG_INFOF("[Node {}] 选举超时，当前时间: {}, 上次选举时间: {}, 超时时间: {}, 成为候选人，当前任期: {}", 
                me_, now, lastElectionTime_, electionTimeout_, currentTerm_);
        state_ = RaftState::CANDIDATE;
        lock.unlock();
        StartElection();
    } else {
        DKV_LOG_DEBUGF("[Node {}] 未超时，当前时间: {}, 上次选举时间: {}, 剩余时间: {} ms", 
                me_, now, lastElectionTime_, (electionTimeout_ - (now - lastElectionTime_)));
        lock.unlock();
        // 短暂休眠，避免CPU占用过高
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
}

// 应用日志到状态机
void Raft::ApplyLogs() {
    std::unique_lock<std::mutex> lock(mutex_);
    
    DKV_LOG_DEBUGF("[Node {}] 开始应用日志，lastApplied={}，commitIndex={}", me_, lastApplied_, commitIndex_);
    
    // 检查是否有新的日志需要应用
    while (lastApplied_ < commitIndex_) {
        // 寻找需要应用的日志条目
        int nextIndex = lastApplied_ + 1;
        
        DKV_LOG_DEBUGF("[Node {}] 准备应用日志索引 {}，logStartIndex={}", me_, nextIndex, logStartIndex_);
        
        // 检查日志是否在当前日志列表中
        const RaftLogEntry* entry = nullptr;
        
        if (nextIndex >= logStartIndex_) {
            for (const auto& logEntry : log_) {
                if (logEntry.index == nextIndex) {
                    entry = &logEntry;
                    break;
                }
            }
        }
        
        lock.unlock();
        
        if (entry != nullptr) {
            DKV_LOG_DEBUGF("[Node {}] 找到日志条目，索引 {}，任期 {}，准备应用到状态机", me_, entry->index, entry->term);
            // 应用命令到状态机
            std::vector<char> result = stateMachine_->DoOp(entry->command);
            
            lock.lock();
            
            // 更新应用索引
            lastApplied_ = nextIndex;
            DKV_LOG_DEBUGF("[Node {}] 成功应用日志索引 {}，更新lastApplied={}", me_, nextIndex, lastApplied_);

            // 检查是否需要创建快照
            if (max_raft_state_ > 0 && PersistBytes() > max_raft_state_) {
                DKV_LOG_INFOF("[Node {}] 持久化数据大小超过阈值，创建快照，lastApplied={}", me_, lastApplied_);
                // 创建快照
                std::vector<char> snapshot = stateMachine_->Snapshot();
                
                // 保存快照
                lock.unlock();
                Snapshot(lastApplied_, snapshot);
                lock.lock();
            }
        } else {
            // 日志不在当前列表中，可能需要从快照恢复
            // 这里可以添加从快照恢复的逻辑
            DKV_LOG_WARNINGF("[Node {}] 无法找到日志条目，索引: {}", me_, nextIndex);
            lock.unlock();
            break;
        }
    }
    
    DKV_LOG_DEBUGF("[Node {}] 日志应用完成，lastApplied={}，commitIndex={}", me_, lastApplied_, commitIndex_);
}

// 更新提交索引
void Raft::UpdateCommitIndex() {
    std::unique_lock<std::mutex> lock(mutex_);
    
    DKV_LOG_DEBUGF("[Node {}] 开始更新提交索引，当前commitIndex={}，lastLogIndex={}", me_, commitIndex_, (log_.empty() ? (logStartIndex_ - 1) : log_.back().index));
    
    int newCommitIndex = commitIndex_;
    
    // 从当前提交索引+1开始检查
    for (int i = commitIndex_ + 1; i <= (log_.empty() ? (logStartIndex_ - 1) : log_.back().index); i++) {
        // 统计当前任期的日志条目数量
        int count = 1; // 自己的投票
        
        for (size_t j = 0; j < peers_.size(); j++) {
            if (j == me_) {
                continue;
            }
            
            if (matchIndex_[j] >= i) {
                count++;
            }
        }
        
        DKV_LOG_DEBUGF("[Node {}] 检查索引 {}: 获得 {} 个匹配，需要 {} 个多数票", me_, i, count, (peers_.size() / 2) + 1);
        
        // 如果获得多数投票，更新提交索引
        if (count > peers_.size() / 2) {
            newCommitIndex = i;
            DKV_LOG_DEBUGF("[Node {}] 索引 {} 获得多数票，更新newCommitIndex={}", me_, i, newCommitIndex);
        } else {
            DKV_LOG_DEBUGF("[Node {}] 索引 {} 未获得多数票，停止检查", me_, i);
            break;
        }
    }
    
    if (newCommitIndex > commitIndex_) {
        int oldCommitIndex = commitIndex_;
        commitIndex_ = newCommitIndex;
        DKV_LOG_INFOF("[Node {}] 更新提交索引从 {} 到 {}", me_, oldCommitIndex, commitIndex_);
        lock.unlock();
        ApplyLogs();
        lock.lock();
    } else {
        DKV_LOG_DEBUGF("[Node {}] 无需更新提交索引，保持为 {}", me_, commitIndex_);
    }
}

// 检查日志一致性
bool Raft::IsLogConsistent(int prevLogIndex, int prevLogTerm) {
    // 如果prevLogIndex小于日志起始索引，认为是一致的
    if (prevLogIndex < logStartIndex_) {
        return true;
    }
    
    // 寻找prevLogIndex对应的日志条目
    for (const auto& entry : log_) {
        if (entry.index == prevLogIndex) {
            return entry.term == prevLogTerm;
        }
    }
    
    // 没有找到对应的日志条目
    return false;
}

// 验证并添加日志条目
bool Raft::ValidateAndAppendEntries(const std::vector<RaftLogEntry>& entries, int prevLogIndex) {
    // 检查日志条目是否合法
    bool entriesValid = true;
    
    if (!entries.empty()) {
        // 检查第一条日志索引是否等于prevLogIndex + 1
        if (entries[0].index != prevLogIndex + 1) {
            entriesValid = false;
        } else {
            // 检查日志条目是否连续且严格递增
            int expectedIndex = prevLogIndex + 1;
            for (const auto& entry : entries) {
                // 检查索引是否连续
                if (entry.index != expectedIndex) {
                    entriesValid = false;
                    break;
                }
                
                // 检查任期是否合法（非负）
                if (entry.term < 0) {
                    entriesValid = false;
                    break;
                }
                
                expectedIndex++;
            }
        }
    }
    
    // 如果日志条目合法，添加到日志中
    if (entriesValid) {
        for (const auto& entry : entries) {
            log_.push_back(entry);
        }
    }
    
    return entriesValid;
}

// 复制日志条目到follower
void Raft::ReplicateLogs() {
    std::unique_lock<std::mutex> lock(mutex_);
    
    DKV_LOG_DEBUGF("[Node {}] 开始复制日志，任期 {}，当前日志数量: {}", me_, currentTerm_, log_.size());
    
    for (int i = 0; i < peers_.size(); i++) {
        if (i == me_) {
            continue;
        }
        
        DKV_LOG_DEBUGF("[Node {}] 处理节点 {}: nextIndex={}, matchIndex={}, logStartIndex={}", me_, i, nextIndex_[i], matchIndex_[i], logStartIndex_);
        
        // 检查follower的nextIndex是否小于日志起始索引
        if (nextIndex_[i] < logStartIndex_) {
            // 需要发送InstallSnapshot请求
            DKV_LOG_DEBUGF("[Node {}] 节点 {} nextIndex={} <= logStartIndex={}，发送InstallSnapshot请求", me_, i, nextIndex_[i], logStartIndex_);
            
            // 创建InstallSnapshot请求
            InstallSnapshotRequest snapshotRequest;
            snapshotRequest.term = currentTerm_;
            snapshotRequest.leaderId = me_;
            snapshotRequest.lastIncludedIndex = logStartIndex_ - 1;
            snapshotRequest.lastIncludedTerm = 0;
            snapshotRequest.snapshot = persister_->ReadSnapshot();
            snapshotRequest.leaderCommit = commitIndex_;

            // 发送InstallSnapshot请求
            DKV_LOG_DEBUGF("[Node {}] 向节点 {} 发送InstallSnapshot请求，lastIncludedIndex={}", me_, i, snapshotRequest.lastIncludedIndex);
            lock.unlock();
            InstallSnapshotResponse snapshotResponse = network_->SendInstallSnapshot(i, snapshotRequest);
            lock.lock();
            
            // 检查响应
            if (snapshotResponse.term > currentTerm_) {
                // 更新当前任期和状态
                DKV_LOG_INFOF("[Node {}] 节点 {} 返回更高任期 {}，转换为FOLLOWER", me_, i, snapshotResponse.term);
                currentTerm_ = snapshotResponse.term;
                state_ = RaftState::FOLLOWER;
                votedFor_ = -1;
                PersistState();
                break;
            }
            
            if (state_ != RaftState::LEADER) {
                DKV_LOG_INFOF("[Node {}] 不再是领导者，停止复制日志", me_);
                break;
            }
            
            if (snapshotResponse.success) {
                // 更新nextIndex和matchIndex
                DKV_LOG_DEBUGF("[Node {}] 节点 {} InstallSnapshot成功，更新nextIndex={}，matchIndex={}", me_, i, logStartIndex_, logStartIndex_ - 1);
                nextIndex_[i] = logStartIndex_;
                matchIndex_[i] = logStartIndex_ - 1;
            } else {
                DKV_LOG_DEBUGF("[Node {}] 节点 {} InstallSnapshot失败", me_, i);
            }
        } else {
            // 准备发送的日志条目
            std::vector<RaftLogEntry> entries;
            int prevLogIndex = 0;
            int prevLogTerm = 0;
            
            // 找到prevLogIndex对应的日志条目
            for (const auto& entry : log_) {
                if (entry.index == (nextIndex_[i] - 1)) {
                    prevLogIndex = entry.index;
                    prevLogTerm = entry.term;
                    break;
                }
            }
            
            // 收集需要发送的日志条目
            for (const auto& entry : log_) {
                if (entry.index >= nextIndex_[i]) {
                    entries.push_back(entry);
                }
            }
            
            DKV_LOG_DEBUGF("[Node {}] 向节点 {} 发送 {} 条日志，prevLogIndex={}, prevLogTerm={}", me_, i, entries.size(), prevLogIndex, prevLogTerm);
            
            // 创建AppendEntries请求
            AppendEntriesRequest request;
            request.term = currentTerm_;
            request.leaderId = me_;
            request.prevLogIndex = prevLogIndex;
            request.prevLogTerm = prevLogTerm;
            request.entries = entries;
            request.leaderCommit = commitIndex_;
            
            auto currentNextIndex = nextIndex_[i];
            auto currentTerm = currentTerm_;

            // 发送请求
            lock.unlock();
            AppendEntriesResponse response = network_->SendAppendEntries(i, request);
            lock.lock();
            
            // 检查响应
            if (response.term > currentTerm) {
                // 更新当前任期和状态
                DKV_LOG_INFOF("[Node {}] 节点 {} 返回更高任期 {}，转换为FOLLOWER", me_, i, response.term);
                currentTerm_ = response.term;
                state_ = RaftState::FOLLOWER;
                votedFor_ = -1;
                PersistState();
                break;
            }
            
            if (state_ != RaftState::LEADER) {
                DKV_LOG_INFOF("[Node {}] 不再是领导者，停止复制日志", me_);
                break;
            }
            
            if (response.success) {
                // 更新nextIndex和matchIndex
                nextIndex_[i] = currentNextIndex + entries.size();
                matchIndex_[i] = nextIndex_[i] - 1;
                DKV_LOG_DEBUGF("[Node {}] 节点 {} AppendEntries成功，更新nextIndex={}，matchIndex={}", me_, i, nextIndex_[i], matchIndex_[i]);
                
                // 更新提交索引
                lock.unlock();
                UpdateCommitIndex();
                lock.lock();
            } else {
                // 减少nextIndex并重试
                if (nextIndex_[i] > logStartIndex_) {
                    DKV_LOG_DEBUGF("[Node {}] 节点 {} AppendEntries失败，减少nextIndex从 {} 到 {}", me_, i, nextIndex_[i], nextIndex_[i] - 1);
                    nextIndex_[i]--;
                }
            }
        }
    }
    
    DKV_LOG_DEBUGF("[Node {}] 日志复制完成，当前commitIndex={}", me_, commitIndex_);
}

// 持久化状态
void Raft::PersistState() {
    if (persister_) {
        persister_->SaveState(currentTerm_, votedFor_);
    }
}

// 持久化日志
void Raft::PersistLog() {
    if (persister_) {
        persister_->SaveLog(log_);
    }
}

// 从持久化恢复
void Raft::RestoreFromPersist() {
    if (!persister_) {
        DKV_LOG_INFOF("[Node {}] 没有持久化对象，跳过恢复", me_);
        return;
    }
    
    // 读取状态
    currentTerm_ = persister_->ReadTerm();
    votedFor_ = persister_->ReadVotedFor();
    
    // 读取日志
    log_ = persister_->ReadLog();
    
    DKV_LOG_INFOF("[Node {}] 从持久化恢复RAFT状态，任期: {}, 投票给: {}, 日志数量: {}", me_, currentTerm_, votedFor_, log_.size());
}





} // namespace dkv
