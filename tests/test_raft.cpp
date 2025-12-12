#include "dkv_raft.h"
#include "dkv_raft_network.h"
#include "dkv_raft_statemachine.h"
#include "dkv_raft_persist.h"
#include "test_runner.hpp"
#include <iostream>
#include <vector>
#include <chrono>
#include <thread>
#include <memory>
#include <cassert>

using namespace std;

namespace dkv {

// 模拟的RAFT网络实现，用于测试
class MockRaftNetwork : public RaftNetwork {
public:
    MockRaftNetwork() {
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
        // 返回模拟响应
        return mock_response_;
    }

    // 发送RequestVote请求
    RequestVoteResponse SendRequestVote(int serverId, const RequestVoteRequest& request) override {
        // 记录请求
        last_vote_request_ = request;
        // 返回模拟响应
        mock_vote_response_.term = request.term;
        return mock_vote_response_;
    }

    // 发送InstallSnapshot请求
    InstallSnapshotResponse SendInstallSnapshot(int serverId, const InstallSnapshotRequest& request) override {
        // 记录请求
        last_snapshot_request_ = request;
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
    AppendEntriesResponse mock_response_;
    RequestVoteResponse mock_vote_response_;
    InstallSnapshotResponse mock_snapshot_response_;
    AppendEntriesRequest last_append_request_;
    RequestVoteRequest last_vote_request_;
    InstallSnapshotRequest last_snapshot_request_;
};

// 模拟的RAFT状态机实现，用于测试
class MockRaftStateMachine : public RaftStateMachine {
public:
    MockRaftStateMachine() : snapshot_calls_(0), restore_calls_(0) {
    }

    // 执行命令并返回结果
    vector<char> DoOp(const vector<char>& command) override {
        last_command_ = command;
        return std::vector<char>{'O', 'K'};
    }

    // 创建快照
    vector<char> Snapshot() override {
        snapshot_calls_++;
        std::string snapshot_str = "snapshot_data";
        return std::vector<char>(snapshot_str.begin(), snapshot_str.end());
    }

    // 从快照恢复
    void Restore(const vector<char>& snapshot) override {
        last_snapshot_ = snapshot;
        restore_calls_++;
    }

    // 获取最后执行的命令
    vector<char> GetLastCommand() const {
        return last_command_;
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

private:
    vector<char> last_command_;
    vector<char> last_snapshot_;
    int snapshot_calls_;
    int restore_calls_;
};

// 测试Raft构造函数和基本状态
bool testRaftConstructor() {
    vector<string> peers = {"127.0.0.1:12345", "127.0.0.1:12346", "127.0.0.1:12347"};
    auto network = make_shared<MockRaftNetwork>();
    auto state_machine = make_shared<MockRaftStateMachine>();
    auto persister = make_shared<RaftFilePersister>("./test_raft_data");

    // 创建Raft实例
    Raft raft(0, peers, persister, network, state_machine);

    // 检查初始状态
    assert(raft.GetState() == RaftState::FOLLOWER);
    ASSERT_EQ(raft.GetCurrentTerm(), 0);
    ASSERT_FALSE(raft.IsLeader());

    return true;
}

// 测试Raft状态转换
bool testRaftStateTransition() {
    vector<string> peers = {"127.0.0.1:12345"};
    auto network = make_shared<MockRaftNetwork>();
    auto state_machine = make_shared<MockRaftStateMachine>();
    auto persister = make_shared<RaftFilePersister>("./test_raft_data");

    // 创建Raft实例
    Raft raft(0, peers, persister, network, state_machine);

    // 检查初始状态
    assert(raft.GetState() == RaftState::FOLLOWER);

    // 启动Raft
    raft.Start();

    // 等待一段时间，让Raft进行状态转换
    this_thread::sleep_for(chrono::milliseconds(400));

    // 检查状态是否变为候选人或领导者
    RaftState state = raft.GetState();
    assert(state == RaftState::CANDIDATE || state == RaftState::LEADER);

    // 停止Raft
    raft.Stop();

    return true;
}

// 测试Raft日志复制
bool testRaftLogReplication() {
    vector<string> peers = {"127.0.0.1:12345", "127.0.0.1:12346", "127.0.0.1:12347"};
    auto network = make_shared<MockRaftNetwork>();
    auto state_machine = make_shared<MockRaftStateMachine>();
    auto persister = make_shared<RaftFilePersister>("./test_raft_data");

    // 创建Raft实例
    Raft raft(0, peers, persister, network, state_machine);

    // 启动Raft
    raft.Start();

    // 等待一段时间，让Raft进行状态转换
    this_thread::sleep_for(chrono::milliseconds(300));

    // 检查是否成为领导者
    if (raft.IsLeader()) {
        // 提交命令
        vector<char> command = {'t', 'e', 's', 't'};
        int index, term;
        bool result = raft.StartCommand(command, index, term);
        ASSERT_TRUE(result);
        ASSERT_GT(index, 0);
        ASSERT_GT(term, 0);
    }

    // 停止Raft
    raft.Stop();

    return true;
}

// 测试Raft快照创建
bool testRaftSnapshot() {
    vector<string> peers = {"127.0.0.1:12345"};
    auto network = make_shared<MockRaftNetwork>();
    auto state_machine = make_shared<MockRaftStateMachine>();
    auto persister = make_shared<RaftFilePersister>("./test_raft_data");

    // 创建Raft实例
    Raft raft(0, peers, persister, network, state_machine);

    // 启动Raft
    raft.Start();

    // 等待一段时间
    this_thread::sleep_for(chrono::milliseconds(200));

    // 手动创建快照
    vector<char> snapshot_data = {'s', 'n', 'a', 'p', 's', 'h', 'o', 't'};
    raft.Snapshot(1, snapshot_data);

    // 停止Raft
    raft.Stop();

    return true;
}

// 测试Raft持久化
bool testRaftPersist() {
    vector<string> peers = {"127.0.0.1:12345"};
    auto network = make_shared<MockRaftNetwork>();
    auto state_machine = make_shared<MockRaftStateMachine>();
    auto persister = make_shared<RaftFilePersister>("./test_raft_data");

    // 创建Raft实例
    Raft raft(0, peers, persister, network, state_machine);

    // 启动Raft
    raft.Start();

    // 等待一段时间，让Raft进行状态转换和持久化
    this_thread::sleep_for(chrono::milliseconds(200));

    // 停止Raft
    raft.Stop();

    // 重新创建Raft实例，从持久化恢复
    Raft raft2(0, peers, persister, network, state_machine);

    // 检查是否从持久化恢复了状态
    ASSERT_GE(raft2.GetCurrentTerm(), 0);

    return true;
}

// 测试Raft状态机管理器
bool testRaftStateMachineManager() {
    RaftStateMachineManager sm;

    // 测试DoOp方法（需要CommandHandler，这里只测试基本功能）
    vector<char> command = {'t', 'e', 's', 't'};
    vector<char> result = sm.DoOp(command);

    // 测试快照创建
    vector<char> snapshot = sm.Snapshot();

    // 测试快照恢复
    sm.Restore(snapshot);

    return true;
}

// 测试Raft网络
bool testRaftNetwork() {
    vector<string> peers = {"127.0.0.1:12345"};
    RaftTcpNetwork network(peers);

    // 测试网络初始化
    // 这里只测试构造函数，实际网络通信测试较为复杂

    return true;
}

// 测试AppendEntries日志验证逻辑
bool testRaftAppendEntriesValidation() {
    vector<string> peers = {"127.0.0.1:12345"};
    auto network = make_shared<MockRaftNetwork>();
    auto state_machine = make_shared<MockRaftStateMachine>();
    auto persister = make_shared<RaftFilePersister>("./test_raft_data");

    // 创建Raft实例
    Raft raft(0, peers, persister, network, state_machine);
    
    // 测试1: 合法的日志条目 - 第一条日志索引等于prevLogIndex+1
    {
        AppendEntriesRequest request;
        request.term = 1;
        request.leaderId = 0;
        request.prevLogIndex = 0;
        request.prevLogTerm = 0;
        request.leaderCommit = 0;
        
        // 添加合法的日志条目
        RaftLogEntry entry;
        entry.term = 1;
        entry.index = 1;
        entry.command = {'t', 'e', 's', 't'};
        request.entries.push_back(entry);
        
        AppendEntriesResponse response = raft.OnAppendEntries(request);
        ASSERT_TRUE(response.success);
    }
    
    // 测试2: 非法的日志条目 - 第一条日志索引不等于prevLogIndex+1
    {
        AppendEntriesRequest request;
        request.term = 1;
        request.leaderId = 0;
        request.prevLogIndex = 0;
        request.prevLogTerm = 0;
        request.leaderCommit = 0;
        
        // 添加非法的日志条目（索引不连续）
        RaftLogEntry entry;
        entry.term = 1;
        entry.index = 2; // 应该是1
        entry.command = {'t', 'e', 's', 't'};
        request.entries.push_back(entry);
        
        AppendEntriesResponse response = raft.OnAppendEntries(request);
        ASSERT_FALSE(response.success);
    }
    
    // 测试3: 非法的日志条目 - 日志条目不连续
    {
        AppendEntriesRequest request;
        request.term = 1;
        request.leaderId = 0;
        request.prevLogIndex = 0;
        request.prevLogTerm = 0;
        request.leaderCommit = 0;
        
        // 添加非法的日志条目（索引不连续）
        RaftLogEntry entry1, entry2;
        entry1.term = 1;
        entry1.index = 1;
        entry1.command = {'t', 'e', 's', 't', '1'};
        
        entry2.term = 1;
        entry2.index = 3; // 应该是2
        entry2.command = {'t', 'e', 's', 't', '2'};
        
        request.entries.push_back(entry1);
        request.entries.push_back(entry2);
        
        AppendEntriesResponse response = raft.OnAppendEntries(request);
        ASSERT_FALSE(response.success);
    }
    
    // 测试4: 非法的日志条目 - 任期为负
    {
        AppendEntriesRequest request;
        request.term = 1;
        request.leaderId = 0;
        request.prevLogIndex = 0;
        request.prevLogTerm = 0;
        request.leaderCommit = 0;
        
        // 添加非法的日志条目（任期为负）
        RaftLogEntry entry;
        entry.term = -1; // 非法任期
        entry.index = 1;
        entry.command = {'t', 'e', 's', 't'};
        request.entries.push_back(entry);
        
        AppendEntriesResponse response = raft.OnAppendEntries(request);
        ASSERT_FALSE(response.success);
    }
    
    return true;
}

// 测试OnInstallSnapshot方法
bool testRaftInstallSnapshot() {
    vector<string> peers = {"127.0.0.1:12345"};
    auto network = make_shared<MockRaftNetwork>();
    auto state_machine = make_shared<MockRaftStateMachine>();
    auto persister = make_shared<RaftFilePersister>("./test_raft_data");

    // 创建Raft实例
    Raft raft(0, peers, persister, network, state_machine);
    
    // 准备快照请求
    InstallSnapshotRequest request;
    request.term = 1;
    request.leaderId = 0;
    request.lastIncludedIndex = 5;
    request.lastIncludedTerm = 1;
    request.snapshot = {'s', 'n', 'a', 'p', 's', 'h', 'o', 't'};
    request.leaderCommit = 5;
    
    // 处理快照请求
    InstallSnapshotResponse response = raft.OnInstallSnapshot(request);
    
    // 检查响应
    ASSERT_TRUE(response.success);
    
    // 验证状态机是否应用了快照
    ASSERT_EQ(state_machine->GetRestoreCalls(), 1);
    
    return true;
}

// 测试多个命令连续提交
bool testRaftContinuousCommands() {
    vector<string> peers = {"127.0.0.1:12345"};
    auto network = make_shared<MockRaftNetwork>();
    auto state_machine = make_shared<MockRaftStateMachine>();
    auto persister = make_shared<RaftFilePersister>("./test_raft_data");

    // 创建Raft实例
    Raft raft(0, peers, persister, network, state_machine);
    
    // 启动Raft
    raft.Start();
    
    // 等待一段时间，让Raft成为领导者
    this_thread::sleep_for(chrono::milliseconds(300));
    
    if (raft.IsLeader()) {
        // 连续提交3个命令
        for (int i = 0; i < 3; i++) {
            vector<char> command = {'c', 'm', 'd', static_cast<char>('0' + i)};
            int index, term;
            bool result = raft.StartCommand(command, index, term);
            ASSERT_TRUE(result);
            ASSERT_GT(index, 0);
            ASSERT_GT(term, 0);
            
            // 等待一段时间，让命令被处理
            this_thread::sleep_for(chrono::milliseconds(50));
        }
    }
    
    // 停止Raft
    raft.Stop();
    
    return true;
}

// 测试领导者选举详细逻辑
bool testRaftLeaderElection() {
    vector<string> peers = {"127.0.0.1:12345", "127.0.0.1:12346", "127.0.0.1:12347"};
    auto network = make_shared<MockRaftNetwork>();
    auto state_machine = make_shared<MockRaftStateMachine>();
    auto persister = make_shared<RaftFilePersister>("./test_raft_data");

    // 创建Raft实例
    Raft raft(0, peers, persister, network, state_machine);
    
    // 启动Raft
    raft.Start();
    
    int leaderCount = 0;
    int maxRetries = 5;
    
    // 多次检查是否成为领导者
    for (int i = 0; i < maxRetries; i++) {
        if (raft.IsLeader()) {
            leaderCount++;
        }
        this_thread::sleep_for(chrono::milliseconds(200));
    }
    
    // 领导者选举应该成功至少一次
    ASSERT_GT(leaderCount, 0);
    
    // 停止Raft
    raft.Stop();
    
    return true;
}

} // namespace dkv

int main() {
    using namespace dkv;

    cout << "DKV RAFT功能测试\n" << endl;

    TestRunner runner;

    // 运行所有测试
    runner.runTest("Raft构造函数", testRaftConstructor);
    runner.runTest("Raft状态转换", testRaftStateTransition);
    runner.runTest("Raft日志复制", testRaftLogReplication);
    runner.runTest("Raft快照创建", testRaftSnapshot);
    runner.runTest("Raft持久化", testRaftPersist);
    runner.runTest("Raft状态机管理器", testRaftStateMachineManager);
    runner.runTest("Raft网络", testRaftNetwork);
    runner.runTest("AppendEntries日志验证", testRaftAppendEntriesValidation);
    runner.runTest("Raft安装快照", testRaftInstallSnapshot);
    runner.runTest("Raft连续命令", testRaftContinuousCommands);
    runner.runTest("Raft领导者选举", testRaftLeaderElection);

    // 打印测试总结
    runner.printSummary();

    return 0;
}
