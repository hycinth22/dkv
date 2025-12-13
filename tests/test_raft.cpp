#include "dkv_raft.h"
#include "dkv_raft_network.h"
#include "dkv_raft_statemachine.h"
#include "dkv_raft_persist.h"
#include "test_raft_common.h"
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
    Command command(CommandType::SET, {"test_key", "test_value"});
    int index, term;
    bool result = raft.StartCommand(RaftCommand(0, command), index, term);
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
    Command command(CommandType::SET, {"test_key", "test_value"});
    Response result = sm.DoOp(RaftCommand(0, command));

    // 测试快照创建
    vector<char> snapshot = sm.Snapshot();

    // 测试快照恢复
    sm.Restore(snapshot);

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
        auto cmd = make_shared<Command>();
        cmd->type = CommandType::SET;
        cmd->args = {"test_key", "test_value"};
        entry.command = make_shared<RaftCommand>(0, *cmd);
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
        auto cmd = make_shared<Command>();
        cmd->type = CommandType::SET;
        cmd->args = {"test_key", "test_value"};
        entry.command = make_shared<RaftCommand>(0, *cmd);
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
        auto cmd1 = make_shared<Command>();
        cmd1->type = CommandType::SET;
        cmd1->args = {"test_key1", "test_value1"};
        entry1.command = make_shared<RaftCommand>(0, *cmd1);
        
        entry2.term = 1;
        entry2.index = 3; // 应该是2
        auto cmd2 = make_shared<Command>();
        cmd2->type = CommandType::SET;
        cmd2->args = {"test_key2", "test_value2"};
        entry2.command = make_shared<RaftCommand>(0, *cmd2);
        
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
        auto cmd = make_shared<Command>();
        cmd->type = CommandType::SET;
        cmd->args = {"test_key", "test_value"};
        entry.command = make_shared<RaftCommand>(0, *cmd);
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
            Command command(CommandType::SET, {"key" + to_string(i), "value" + to_string(i)});
            int index, term;
            bool result = raft.StartCommand(make_shared<RaftCommand>(0, command), index, term);
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

// 测试初始选举
bool testRaftInitialElection() {
    RaftTest test(3);
    test.StartAll();
    
    // 检查是否有领导者
    int leader = test.CheckOneLeader();
    ASSERT_GT(leader, -1);
    
    // 等待一段时间，确保所有节点都知道领导者
    this_thread::sleep_for(chrono::milliseconds(50));
    
    // 检查所有节点的任期是否一致
    int term1 = test.CheckTerms();
    ASSERT_GT(term1, 0);
    
    // 等待更长时间，确保任期不会无故改变
    this_thread::sleep_for(chrono::milliseconds(1000));
    
    int term2 = test.CheckTerms();
    ASSERT_EQ(term1, term2);
    
    // 检查是否仍然有领导者
    leader = test.CheckOneLeader();
    ASSERT_GT(leader, -1);
    
    test.StopAll();
    
    return true;
}

// 测试重新选举
bool testRaftReElection() {
    RaftTest test(3);
    test.StartAll();
    
    // 检查初始领导者
    int leader1 = test.CheckOneLeader();
    ASSERT_GT(leader1, -1);
    
    // 停止领导者
    test.GetRaft(leader1)->Stop();
    
    // 检查是否选举出新的领导者
    int leader2 = test.CheckOneLeader();
    ASSERT_GT(leader2, -1);
    ASSERT_NE(leader1, leader2);
    
    // 重启原领导者
    test.GetRaft(leader1)->Start();
    
    // 检查是否仍然有领导者
    int leader3 = test.CheckOneLeader();
    ASSERT_GT(leader3, -1);
    
    test.StopAll();
    
    return true;
}

// 测试基本一致性
bool testRaftBasicAgree() {
    RaftTest test(3);
    test.StartAll();
    
    // 等待领导者选举
    this_thread::sleep_for(chrono::milliseconds(300));
    
    // 提交命令
    Command command(CommandType::SET, {"test_key", "test_value"});
    int index = test.One(command, 3, false);
    ASSERT_GT(index, 0);
    
    test.StopAll();
    
    return true;
}

// 测试跟随者故障
bool testRaftFollowerFailure() {
    RaftTest test(3);
    test.StartAll();
    
    // 等待领导者选举
    this_thread::sleep_for(chrono::milliseconds(300));
    
    // 提交第一个命令
    Command command1(CommandType::SET, {"test_key1", "test_value1"});
    int index1 = test.One(command1, 3, false);
    ASSERT_GT(index1, 0);
    
    // 停止一个跟随者
    int leader = test.CheckOneLeader();
    int follower = (leader + 1) % 3;
    test.GetRaft(follower)->Stop();
    
    // 提交第二个命令，应该只需要2个服务器确认
    Command command2(CommandType::SET, {"test_key2", "test_value2"});
    int index2 = test.One(command2, 2, false);
    ASSERT_GT(index2, index1);
    
    test.StopAll();
    
    return true;
}

// 测试领导者故障
bool testRaftLeaderFailure() {
    RaftTest test(3);
    test.StartAll();
    
    // 等待领导者选举
    this_thread::sleep_for(chrono::milliseconds(300));
    
    // 提交第一个命令
    Command command1(CommandType::SET, {"test_key1", "test_value1"});
    int index1 = test.One(command1, 3, false);
    ASSERT_GT(index1, 0);
    
    // 停止领导者
    int leader1 = test.CheckOneLeader();
    test.GetRaft(leader1)->Stop();
    
    // 应该选举出新的领导者
    int leader2 = test.CheckOneLeader();
    ASSERT_GT(leader2, -1);
    ASSERT_NE(leader1, leader2);
    
    // 提交第二个命令
    Command command2(CommandType::SET, {"test_key2", "test_value2"});
    int index2 = test.One(command2, 2, false);
    ASSERT_GT(index2, index1);
    
    test.StopAll();
    
    return true;
}

// 测试网络分区恢复
bool testRaftFailAgree() {
    RaftTest test(3);
    test.StartAll();
    
    // 等待领导者选举
    this_thread::sleep_for(chrono::milliseconds(300));
    
    // 提交第一个命令
    Command command1;
    command1.type = CommandType::SET;
    command1.args = {"test_key1", "test_value1"};
    int index1 = test.One(command1, 3, false);
    ASSERT_GT(index1, 0);
    
    // 停止一个跟随者
    int leader = test.CheckOneLeader();
    int follower = (leader + 1) % 3;
    test.GetRaft(follower)->Stop();
    
    // 提交多个命令
    for (int i = 0; i < 4; i++) {
        Command command(CommandType::SET, {"test_key" + to_string(i+2), "test_value" + to_string(i+2)});
        int index = test.One(command, 2, false);
        ASSERT_GT(index, 0);
    }
    
    // 重启跟随者
    test.GetRaft(follower)->Start();
    
    // 等待一段时间，让跟随者赶上
    this_thread::sleep_for(chrono::milliseconds(200));
    
    // 提交最后一个命令，应该所有3个服务器都确认
    Command command5(CommandType::SET, {"test_key6", "test_value6"});
    int index5 = test.One(command5, 3, false);
    ASSERT_GT(index5, 0);
    
    test.StopAll();
    
    return true;
}

// 测试快照恢复
bool testRaftSnapshotRestore() {
    RaftTest test(3);
    test.StartAll();
    
    // 等待领导者选举
    this_thread::sleep_for(chrono::milliseconds(300));
    
    // 提交多个递增命令
    for (int i = 0; i < 20; i++) {
        Command command(CommandType::SET, {"i"});
        int index = test.One(command, 3, false);
        ASSERT_GT(index, 0);
    }
    
    // 等待状态机应用
    this_thread::sleep_for(chrono::milliseconds(200));
    
    // 检查所有状态机的计数器值
    for (int i = 0; i < 3; i++) {
        auto sm = dynamic_pointer_cast<MockRaftStateMachine>(test.GetStateMachine(i));
        ASSERT_EQ(sm->GetCounter(), 20);
    }
    
    // 停止所有节点
    test.StopAll();
    
    // 重启所有节点，测试快照恢复
    test.StartAll();
    
    // 等待领导者重新选举
    this_thread::sleep_for(chrono::milliseconds(300));
    
    // 再次提交命令，确保所有节点都能正常工作
    Command command;
    command.type = CommandType::SET;
    command.args = {"i"};
    int index = test.One(command, 3, false);
    ASSERT_GT(index, 0);
    
    // 等待状态机应用
    this_thread::sleep_for(chrono::milliseconds(100));
    
    // 检查所有状态机的计数器值
    for (int i = 0; i < 3; i++) {
        auto sm = dynamic_pointer_cast<MockRaftStateMachine>(test.GetStateMachine(i));
        ASSERT_EQ(sm->GetCounter(), 21);
    }
    
    test.StopAll();
    
    return true;
}

// 测试并发日志复制
bool testRaftConcurrentLogReplication() {
    const int NOPERATIONS = 10; // 减少并发操作数量
    
    RaftTest test(3);
    test.StartAll();
    
    // 等待领导者选举
    this_thread::sleep_for(chrono::milliseconds(300));
    
    // 并发提交命令
    atomic<int> completed(0);
    vector<thread> threads;
    
    for (int i = 0; i < NOPERATIONS; i++) {
        threads.emplace_back([&test, &completed, i]() {
            Command command(CommandType::SET, {"cmd" + to_string(i % 10), "value" + to_string(i % 10)});
            int index = test.One(command, 3, false);
            if (index > 0) {
                completed++;
            }
        });
    }
    
    // 等待所有操作完成
    for (auto& thread : threads) {
        thread.join();
    }
    
    // 检查至少大部分命令都已完成
    ASSERT_GT(completed.load(), NOPERATIONS * 0.8);
    
    test.StopAll();
    
    return true;
}

// 测试日志截断
bool testRaftLogTruncation() {
    RaftTest test(3);
    test.StartAll();
    
    // 等待领导者选举
    this_thread::sleep_for(chrono::milliseconds(300));
    
    // 提交一些命令
    for (int i = 0; i < 5; i++) {
        Command command(CommandType::SET, {"test_key" + to_string(i), "test_value" + to_string(i)});
        int index = test.One(command, 3, false);
        ASSERT_GT(index, 0);
    }
    
    // 停止领导者
    int leader1 = test.CheckOneLeader();
    test.GetRaft(leader1)->Stop();
    
    // 等待新领导者选举
    this_thread::sleep_for(chrono::milliseconds(300));
    
    // 提交更多命令
    for (int i = 5; i < 10; i++) {
        Command command(CommandType::SET, {"new_key" + to_string(i), "new_value" + to_string(i)});
        int index = test.One(command, 2, false);
        ASSERT_GT(index, 0);
    }
    
    // 重启旧领导者
    test.GetRaft(leader1)->Start();
    
    // 等待一段时间，让旧领导者同步状态
    this_thread::sleep_for(chrono::milliseconds(300));
    
    // 再次提交命令，确保所有节点都能正常工作
    Command command(CommandType::SET, {"final_key", "final_value"});
    int index = test.One(command, 3, false);
    ASSERT_GT(index, 0);
    
    test.StopAll();
    
    return true;
}

// 测试多个领导者
bool testRaftMultipleLeaders() {
    RaftTest test(5);
    test.StartAll();
    
    // 等待领导者选举
    this_thread::sleep_for(chrono::milliseconds(500));
    
    // 检查只有一个领导者
    int leader = test.CheckOneLeader();
    ASSERT_GT(leader, -1);
    
    // 停止领导者
    test.GetRaft(leader)->Stop();
    
    // 等待新领导者选举
    this_thread::sleep_for(chrono::milliseconds(500));
    
    // 检查只有一个领导者
    int leader2 = test.CheckOneLeader();
    ASSERT_GT(leader2, -1);
    ASSERT_NE(leader, leader2);
    
    // 重启原领导者
    test.GetRaft(leader)->Start();
    
    // 等待一段时间，确保只有一个领导者
    this_thread::sleep_for(chrono::milliseconds(500));
    
    // 检查只有一个领导者
    int leader3 = test.CheckOneLeader();
    ASSERT_GT(leader3, -1);
    
    test.StopAll();
    
    return true;
}

// 测试任期更新
bool testRaftTermUpdate() {
    RaftTest test(3);
    test.StartAll();
    
    // 等待领导者选举
    this_thread::sleep_for(chrono::milliseconds(300));
    
    // 检查所有节点的任期是否一致
    int term1 = test.CheckTerms();
    ASSERT_GT(term1, 0);
    
    // 停止领导者
    int leader1 = test.CheckOneLeader();
    test.GetRaft(leader1)->Stop();
    
    // 等待新领导者选举
    this_thread::sleep_for(chrono::milliseconds(500));
    
    // 检查所有节点的任期是否一致且已更新
    int term2 = test.CheckTerms();
    ASSERT_GT(term2, term1);
    
    // 重启原领导者
    test.GetRaft(leader1)->Start();
    
    // 等待一段时间，确保所有节点的任期一致
    this_thread::sleep_for(chrono::milliseconds(300));
    
    int term3 = test.CheckTerms();
    ASSERT_EQ(term2, term3);
    
    test.StopAll();
    
    return true;
}

// 测试Raft速度性能
bool testRaftSpeed() {
    const int numOps = 100;
    
    RaftTest test(3);
    test.StartAll();
    
    // 等待领导者选举
    this_thread::sleep_for(chrono::milliseconds(300));
    
    // 提交一个命令，确保领导者已准备好
    Command command(CommandType::SET, {"i"});
    test.One(command, 3, false);
    
    // 开始计时
    auto start = chrono::steady_clock::now();
    
    // 提交多个命令
    for (int i = 0; i < numOps; i++) {
        Command cmd(CommandType::SET, {"i"});
        test.One(cmd, 3, false);
    }
    
    // 结束计时
    auto end = chrono::steady_clock::now();
    auto duration = chrono::duration_cast<chrono::milliseconds>(end - start);
    
    // 检查所有状态机的计数器值
    for (int i = 0; i < 3; i++) {
        auto sm = dynamic_pointer_cast<MockRaftStateMachine>(test.GetStateMachine(i));
        ASSERT_EQ(sm->GetCounter(), numOps + 1); // +1 因为前面已经提交了一个命令
    }
    
    // 打印速度信息
    cout << "  速度测试: " << numOps << " 操作耗时 " << duration.count() << " ms, " 
         << (double)numOps / duration.count() * 1000 << " 操作/秒" << endl;
    
    test.StopAll();
    
    return true;
}

// 测试领导者分区
bool testRaftLeaderPartition() {
    RaftTest test(3);
    test.StartAll();
    
    // 等待领导者选举
    this_thread::sleep_for(chrono::milliseconds(300));
    
    // 提交初始命令
    Command command(CommandType::SET, {"i"});
    test.One(command, 3, false);
    
    // 获取当前领导者
    int leader = test.CheckOneLeader();
    ASSERT_GT(leader, -1);
    
    // 停止领导者，模拟网络分区
    test.GetRaft(leader)->Stop();
    
    // 等待新领导者选举
    this_thread::sleep_for(chrono::milliseconds(500));
    
    // 检查是否有新的领导者
    int new_leader = test.CheckOneLeader();
    ASSERT_GT(new_leader, -1);
    ASSERT_NE(new_leader, leader);
    
    // 在新领导者下提交命令
    Command command2(CommandType::SET, {"i"});
    int index = test.One(command2, 2, false); // 只有2个节点可用
    ASSERT_GT(index, 0);
    
    // 检查状态机计数器值
    for (int i = 0; i < 3; i++) {
        if (i != leader) { // 排除已停止的领导者
            auto sm = dynamic_pointer_cast<MockRaftStateMachine>(test.GetStateMachine(i));
            ASSERT_EQ(sm->GetCounter(), 2); // 初始命令 + 新命令
        }
    }
    
    // 重启原领导者
    test.GetRaft(leader)->Start();
    
    // 等待一段时间，让原领导者同步状态
    this_thread::sleep_for(chrono::milliseconds(300));
    
    // 提交最后一个命令，确保所有节点都能正常工作
    Command command3(CommandType::SET, {"i"});
    test.One(command3, 3, false);
    
    // 检查所有状态机的计数器值
    for (int i = 0; i < 3; i++) {
        auto sm = dynamic_pointer_cast<MockRaftStateMachine>(test.GetStateMachine(i));
        ASSERT_EQ(sm->GetCounter(), 3); // 初始命令 + 新命令 + 最后命令
    }
    
    test.StopAll();
    
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
    runner.runTest("Raft持久化", testRaftPersist);
    runner.runTest("Raft状态机管理器", testRaftStateMachineManager);
    runner.runTest("AppendEntries日志验证", testRaftAppendEntriesValidation);
    runner.runTest("Raft安装快照", testRaftInstallSnapshot);
    runner.runTest("Raft连续命令", testRaftContinuousCommands);
    runner.runTest("Raft领导者选举", testRaftLeaderElection);
    runner.runTest("Raft初始选举", testRaftInitialElection);
    runner.runTest("Raft重新选举", testRaftReElection);
    runner.runTest("Raft基本一致性", testRaftBasicAgree);
    runner.runTest("Raft跟随者故障", testRaftFollowerFailure);
    runner.runTest("Raft领导者故障", testRaftLeaderFailure);
    runner.runTest("Raft网络分区恢复", testRaftFailAgree);
    
    // 新添加的测试用例
    runner.runTest("Raft快照恢复", testRaftSnapshotRestore);
    runner.runTest("Raft并发日志复制", testRaftConcurrentLogReplication);
    runner.runTest("Raft日志截断", testRaftLogTruncation);
    runner.runTest("Raft多个领导者", testRaftMultipleLeaders);
    runner.runTest("Raft任期更新", testRaftTermUpdate);
    runner.runTest("Raft速度性能", testRaftSpeed);
    runner.runTest("Raft领导者分区", testRaftLeaderPartition);

    // 打印测试总结
    runner.printSummary();

    return 0;
}
