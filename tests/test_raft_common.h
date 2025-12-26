#pragma once

#include "dkv_logger.hpp"
#include "multinode/raft/dkv_raft.hpp"
#include "multinode/raft/dkv_raft_network.hpp"
#include "multinode/raft/dkv_raft_statemachine.hpp"
#include "multinode/raft/dkv_raft_persist.hpp"
#include "test_runner.hpp"
#include <iostream>
#include <vector>
#include <chrono>
#include <thread>
#include <memory>
#include <cassert>
#include <random>
#include <mutex>
#include <atomic>
#include <condition_variable>

using namespace std;

namespace dkv {

// 前向声明
class MockRaftStateMachine;
class MockRaftNetwork;

// 测试框架类 - 先定义，因为MockRaftNetwork依赖它
class RaftTest {
public:
    RaftTest(int servers) : servers_(servers), max_index_(0) {
        // 初始化Raft实例
        for (int i = 0; i < servers_; i++) {
            vector<string> peers;
            for (int j = 0; j < servers_; j++) {
                peers.push_back("127.0.0.1:" + to_string(12345 + j));
            }
            
            auto network = make_shared<MockRaftNetwork>(this, i);
            auto state_machine = make_shared<MockRaftStateMachine>();
            auto persister = make_shared<RaftFilePersister>("./test_raft_data" + to_string(i));
            
            auto raft = make_shared<Raft>(i, peers, persister, network, state_machine);
            
            raft_instances_.push_back(raft);
            networks_.push_back(network);
            state_machines_.push_back(state_machine);
        }
    }
    
    ~RaftTest() {
        StopAll();
    }
    
    // 启动所有Raft实例
    void StartAll() {
        for (auto& raft : raft_instances_) {
            raft->Start();
        }
    }
    
    // 停止所有Raft实例
    void StopAll() {
        for (auto& raft : raft_instances_) {
            raft->Stop();
        }
    }
    
    // 检查是否有且只有一个领导者
    int CheckOneLeader() {
        for (int iters = 0; iters < 10; iters++) {
            vector<int> leaders;
            for (int i = 0; i < servers_; i++) {
                if (raft_instances_[i]->IsLeader()) {
                    leaders.push_back(i);
                }
            }
            
            if (leaders.size() == 1) {
                return leaders[0];
            }
            
            this_thread::sleep_for(chrono::milliseconds(100));
        }
        
        // 如果没有找到领导者，返回-1
        return -1;
    }
    
    // 检查所有节点的任期是否一致
    int CheckTerms() {
        int term = -1;
        for (int i = 0; i < servers_; i++) {
            int current_term = raft_instances_[i]->GetCurrentTerm();
            if (term == -1) {
                term = current_term;
            } else if (term != current_term) {
                return -1;
            }
        }
        return term;
    }
    
    // 检查没有领导者
    bool CheckNoLeader() {
        for (int i = 0; i < servers_; i++) {
            if (raft_instances_[i]->IsLeader()) {
                return false;
            }
        }
        return true;
    }
    
    // 提交一个命令并等待至少n个服务器提交
    int One(const Command& cmd, int expectedServers, bool retry = true) {
        auto start_time = chrono::steady_clock::now();
        int starts = 0;
        
        while (chrono::steady_clock::now() - start_time < chrono::seconds(10)) {
            // 尝试所有服务器，找到领导者
            int index = -1;
            for (int i = 0; i < servers_; i++) {
                starts = (starts + 1) % servers_;
                auto& raft = raft_instances_[starts];
                
                if (raft->IsLeader()) {
                    int term;
                    bool ok = raft->StartCommand(RaftCommand(0, cmd), index, term);
                    if (ok) {
                        break;
                    }
                }
            }
            
            if (index != -1) {
                // 等待命令被提交
                auto t1 = chrono::steady_clock::now();
                while (chrono::steady_clock::now() - t1 < chrono::seconds(5)) {
                    int committed = 0;
                    for (auto& raft : raft_instances_) {
                        // 检查该索引是否被提交
                        if (raft->GetCommitIndex() >= index) {
                            committed++;
                        }
                    }
                    
                    if (committed >= expectedServers) {
                        return index;
                    }
                    
                    this_thread::sleep_for(chrono::milliseconds(100));
                }
                
                if (!retry) {
                    return -1;
                }
            } else {
                this_thread::sleep_for(chrono::milliseconds(50));
            }
        }
        
        return -1;
    }
    
    // 等待至少n个服务器提交指定索引的日志
    bool Wait(int index, int n) {
        auto timeout = chrono::milliseconds(10);
        
        for (int iters = 0; iters < 30; iters++) {
            int committed = 0;
            for (auto& raft : raft_instances_) {
                // 检查该索引是否被提交
                if (raft->GetCommitIndex() >= index) {
                    committed++;
                }
            }
            
            if (committed >= n) {
                return true;
            }
            
            this_thread::sleep_for(timeout);
            if (timeout < chrono::seconds(1)) {
                timeout *= 2;
            }
        }
        
        return false;
    }
    
    // 获取Raft实例
    shared_ptr<Raft> GetRaft(int i) {
        if (i >= 0 && i < servers_) {
            return raft_instances_[i];
        }
        return nullptr;
    }
    
    // 获取网络实例
    shared_ptr<MockRaftNetwork> GetNetwork(int i) {
        if (i >= 0 && i < servers_) {
            return networks_[i];
        }
        return nullptr;
    }
    
    // 获取状态机实例
    shared_ptr<MockRaftStateMachine> GetStateMachine(int i) {
        if (i >= 0 && i < servers_) {
            return state_machines_[i];
        }
        return nullptr;
    }
    
private:
    int servers_;
    vector<shared_ptr<Raft>> raft_instances_;
    vector<shared_ptr<MockRaftNetwork>> networks_;
    vector<shared_ptr<MockRaftStateMachine>> state_machines_;
    int max_index_;
};

// 模拟的RAFT状态机实现，用于测试
class MockRaftStateMachine : public RaftStateMachine {
public:
    MockRaftStateMachine() : snapshot_calls_(0), restore_calls_(0), counter_(0) {
    }

    // 执行命令并返回结果   
    Response DoOp(const RaftCommand& command) override {
        //DKV_LOG_INFOF("MockRaftStateMachine::DoOp: {}", command.db_command.desc());
        //DKV_LOG_INFOF("MockRaftStateMachine::DoOp: counter_ = {}", counter_);
        // 处理计数器命令
        if (!command.db_command.args.empty()) {
            switch (command.db_command.args[0][0]) {
                case 'i': // increment
                    counter_++;
                    break;
                case 'd': // decrement
                    counter_--;
                    break;
                case 'r': // reset
                    counter_ = 0;
                    break;
            }
        }
        //DKV_LOG_INFOF("MockRaftStateMachine::DoOp: counter_ = {}", counter_);
        return Response(ResponseStatus::OK, "", "OK");
    }

    // 创建快照
    vector<char> Snapshot() override {
        snapshot_calls_++;
        std::string snapshot_str = "counter=" + std::to_string(counter_);
        return std::vector<char>(snapshot_str.begin(), snapshot_str.end());
    }

    // 从快照恢复
    void Restore(const vector<char>& snapshot) override {
        last_snapshot_ = snapshot;
        restore_calls_++;
        
        // 从快照恢复计数器值
        std::string snapshot_str(snapshot.begin(), snapshot.end());
        if (snapshot_str.find("counter=") == 0) {
            std::string counter_str = snapshot_str.substr(8);
            counter_ = std::stoi(counter_str);
        }
    }

    // 获取快照调用次数
    int GetSnapshotCalls() const {
        return snapshot_calls_;
    }

    // 获取恢复调用次数
    int GetRestoreCalls() const {
        return restore_calls_;
    }

    // 获取最后恢复的快照
    vector<char> GetLastSnapshot() const {
        return last_snapshot_;
    }
    
    // 获取当前计数器值
    int GetCounter() const {
        return counter_;
    }

private:
    vector<char> last_snapshot_;
    int snapshot_calls_;
    int restore_calls_;
    int counter_;
};

// 模拟的RAFT网络实现，用于测试
class MockRaftNetwork : public RaftNetwork {
public:
    // 默认构造函数，用于单个Raft实例测试
    MockRaftNetwork() : test_(nullptr), selfId_(0) {
        // 初始化默认响应
        mock_response_.term = 0;
        mock_response_.success = true;
        mock_vote_response_.term = 0;
        mock_vote_response_.voteGranted = true;
        mock_snapshot_response_.term = 0;
        mock_snapshot_response_.success = true;
    }
    
    // 带参数的构造函数，用于测试框架
    MockRaftNetwork(RaftTest* test, int selfId) : test_(test), selfId_(selfId) {
        // 初始化默认响应
        mock_response_.term = 0;
        mock_response_.success = true;
        mock_vote_response_.term = 0;
        mock_vote_response_.voteGranted = true;
        mock_snapshot_response_.term = 0;
        mock_snapshot_response_.success = true;
    }

    // 发送AppendEntries请求
    AppendEntriesResponse SendAppendEntries(int serverId, const AppendEntriesRequest& request) override {
        // 记录请求
        last_append_request_ = request;
        // 在测试框架中直接调用目标服务器的OnAppendEntries方法
        if (test_) {
            auto raft = test_->GetRaft(serverId);
            if (raft) {
                return raft->OnAppendEntries(request);
            }
        }
        // 返回模拟响应
        return mock_response_;
    }

    // 发送RequestVote请求
    RequestVoteResponse SendRequestVote(int serverId, const RequestVoteRequest& request) override {
        // 记录请求
        last_vote_request_ = request;
        // 在测试框架中直接调用目标服务器的OnRequestVote方法
        if (test_) {
            auto raft = test_->GetRaft(serverId);
            if (raft) {
                return raft->OnRequestVote(request);
            }
        }
        // 返回模拟响应
        mock_vote_response_.term = request.term;
        return mock_vote_response_;
    }

    // 发送InstallSnapshot请求
    InstallSnapshotResponse SendInstallSnapshot(int serverId, const InstallSnapshotRequest& request) override {
        // 记录请求
        last_snapshot_request_ = request;
        // 在测试框架中直接调用目标服务器的OnInstallSnapshot方法
        if (test_) {
            auto raft = test_->GetRaft(serverId);
            if (raft) {
                return raft->OnInstallSnapshot(request);
            }
        }
        // 返回模拟响应
        return mock_snapshot_response_;
    }

    // 设置模拟响应
    void SetMockAppendResponse(const AppendEntriesResponse& response) {
        mock_response_ = response;
    }

    // 设置模拟投票响应
    void SetMockVoteResponse(const RequestVoteResponse& response) {
        mock_vote_response_ = response;
    }

    // 设置模拟快照响应
    void SetMockSnapshotResponse(const InstallSnapshotResponse& response) {
        mock_snapshot_response_ = response;
    }

    // 获取最后一个AppendEntries请求
    AppendEntriesRequest GetLastAppendRequest() const {
        return last_append_request_;
    }

    // 获取最后一个RequestVote请求
    RequestVoteRequest GetLastVoteRequest() const {
        return last_vote_request_;
    }

    // 获取最后一个InstallSnapshot请求
    InstallSnapshotRequest GetLastSnapshotRequest() const {
        return last_snapshot_request_;
    }

private:
    RaftTest* test_;
    int selfId_;
    AppendEntriesResponse mock_response_;
    RequestVoteResponse mock_vote_response_;
    InstallSnapshotResponse mock_snapshot_response_;
    AppendEntriesRequest last_append_request_;
    RequestVoteRequest last_vote_request_;
    InstallSnapshotRequest last_snapshot_request_;
};

} // namespace dkv
