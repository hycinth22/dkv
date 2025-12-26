#include "multinode/raft/dkv_raft.hpp"
#include "multinode/raft/dkv_raft_network.hpp"
#include "multinode/raft/dkv_raft_statemachine.hpp"
#include "multinode/raft/dkv_raft_persist.hpp"
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

// 测试基本的状态机复制
bool testRaftStateMachineBasic() {
    RaftTest test(3);
    test.StartAll();
    
    // 等待领导者选举
    this_thread::sleep_for(chrono::milliseconds(300));
    
    // 提交递增命令
    Command command(CommandType::UNKNOWN, {"i"});
    int index = test.One(command, 3, false);
    ASSERT_GT(index, 0);
    
    // 等待状态机应用
    this_thread::sleep_for(chrono::milliseconds(100));
    
    // 检查所有状态机的计数器值
    for (int i = 0; i < 3; i++) {
        auto sm = dynamic_pointer_cast<MockRaftStateMachine>(test.GetStateMachine(i));
        ASSERT_EQ(sm->GetCounter(), 1);
    }
    
    test.StopAll();
    
    return true;
}

// 测试并发状态机操作
bool testRaftStateMachineConcurrent() {
    const int NOPERATIONS = 50;
    
    RaftTest test(3);
    test.StartAll();
    
    // 等待领导者选举
    this_thread::sleep_for(chrono::milliseconds(300));
    
    // 并发提交递增命令
    atomic<int> completed(0);
    vector<thread> threads;
    
    for (int i = 0; i < NOPERATIONS; i++) {
        threads.emplace_back([&test, &completed]() {
            Command command(CommandType::UNKNOWN, {"i"});
            test.One(command, 3, false);
            completed++;
        });
    }
    
    // 等待所有操作完成
    for (auto& thread : threads) {
        thread.join();
    }
    
    // 等待状态机应用
    this_thread::sleep_for(chrono::milliseconds(200));
    
    // 检查所有状态机的计数器值
    for (int i = 0; i < 3; i++) {
        auto sm = dynamic_pointer_cast<MockRaftStateMachine>(test.GetStateMachine(i));
        ASSERT_EQ(sm->GetCounter(), NOPERATIONS);
    }
    
    test.StopAll();
    
    return true;
}

// 测试快照创建和恢复
bool testRaftStateMachineSnapshot() {
    RaftTest test(3);
    test.StartAll();
    
    // 等待领导者选举
    this_thread::sleep_for(chrono::milliseconds(300));
    
    // 提交多个递增命令
    Command command(CommandType::UNKNOWN, {"i"});
    for (int i = 0; i < 10; i++) {
        test.One(command, 3, false);
    }
    
    // 等待状态机应用
    this_thread::sleep_for(chrono::milliseconds(200));
    
    // 手动创建快照
    // 这里需要修改测试框架，添加手动创建快照的功能
    
    // 重启所有节点
    test.StopAll();
    test.StartAll();
    
    // 等待领导者重新选举
    this_thread::sleep_for(chrono::milliseconds(300));
    
    // 再次提交命令
    test.One(command, 3, false);
    
    // 等待状态机应用
    this_thread::sleep_for(chrono::milliseconds(200));
    
    // 检查所有状态机的计数器值
    for (int i = 0; i < 3; i++) {
        auto sm = dynamic_pointer_cast<MockRaftStateMachine>(test.GetStateMachine(i));
        ASSERT_EQ(sm->GetCounter(), 11);
    }
    
    test.StopAll();
    
    return true;
}

// 测试领导者故障后的状态机一致性
bool testRaftStateMachineLeaderFailure() {
    RaftTest test(3);
    test.StartAll();
    
    // 等待领导者选举
    this_thread::sleep_for(chrono::milliseconds(300));
    
    // 获取当前领导者
    int leader = test.CheckOneLeader();
    ASSERT_GT(leader, -1);
    
    // 提交递增命令
    Command command(CommandType::UNKNOWN, {"i"});
    test.One(command, 3, false);
    
    // 停止领导者
    test.GetRaft(leader)->Stop();
    
    // 等待新领导者选举
    this_thread::sleep_for(chrono::milliseconds(300));
    
    // 再次提交命令
    test.One(command, 2, false);
    
    // 等待状态机应用
    this_thread::sleep_for(chrono::milliseconds(200));
    
    // 检查所有状态机的计数器值
    for (int i = 0; i < 3; i++) {
        auto sm = dynamic_pointer_cast<MockRaftStateMachine>(test.GetStateMachine(i));
        ASSERT_EQ(sm->GetCounter(), 2);
    }
    
    // 重启原领导者
    test.GetRaft(leader)->Start();
    
    // 等待一段时间，让原领导者同步状态
    this_thread::sleep_for(chrono::milliseconds(200));
    
    test.StopAll();
    
    return true;
}

// 测试网络分区后的状态机一致性
bool testRaftStateMachinePartition() {
    // 这个测试需要测试框架支持网络分区
    // 暂时留空，等待添加网络分区功能
    return true;
}

// 测试重启后重放操作
bool testRaftStateMachineRestartReplay() {
    const int NINC = 50;
    
    RaftTest test(3);
    test.StartAll();
    
    // 等待领导者选举
    this_thread::sleep_for(chrono::milliseconds(300));
    
    // 提交多个递增命令
    Command command(CommandType::UNKNOWN, {"i"});
    for (int i = 0; i < NINC; i++) {
        test.One(command, 3, false);
    }
    
    // 检查状态机计数器值
    for (int i = 0; i < 3; i++) {
        auto sm = dynamic_pointer_cast<MockRaftStateMachine>(test.GetStateMachine(i));
        ASSERT_EQ(sm->GetCounter(), NINC);
    }
    
    // 停止所有节点
    test.StopAll();
    
    // 重启所有节点
    test.StartAll();
    
    // 等待领导者重新选举
    this_thread::sleep_for(chrono::milliseconds(300));
    
    // 提交一个命令
    test.One(command, 3, false);
    
    // 等待状态机应用
    this_thread::sleep_for(chrono::milliseconds(200));
    
    // 检查所有状态机的计数器值
    for (int i = 0; i < 3; i++) {
        auto sm = dynamic_pointer_cast<MockRaftStateMachine>(test.GetStateMachine(i));
        ASSERT_EQ(sm->GetCounter(), NINC + 1);
    }
    
    test.StopAll();
    
    return true;
}

// 测试状态机关闭行为
bool testRaftStateMachineShutdown() {
    RaftTest test(3);
    test.StartAll();
    
    // 等待领导者选举
    this_thread::sleep_for(chrono::milliseconds(300));
    
    // 提交一些命令
    Command command(CommandType::UNKNOWN, {"i"});
    for (int i = 0; i < 10; i++) {
        test.One(command, 3, false);
    }
    
    // 停止所有节点
    test.StopAll();
    
    // 重启所有节点
    test.StartAll();
    
    // 等待领导者重新选举
    this_thread::sleep_for(chrono::milliseconds(300));
    
    // 提交更多命令
    for (int i = 0; i < 10; i++) {
        test.One(command, 3, false);
    }
    
    // 检查状态机计数器值
    for (int i = 0; i < 3; i++) {
        auto sm = dynamic_pointer_cast<MockRaftStateMachine>(test.GetStateMachine(i));
        ASSERT_EQ(sm->GetCounter(), 20);
    }
    
    test.StopAll();
    
    return true;
}

// 测试重启后提交操作
bool testRaftStateMachineRestartSubmit() {
    const int NINC = 30;
    
    RaftTest test(3);
    test.StartAll();
    
    // 等待领导者选举
    this_thread::sleep_for(chrono::milliseconds(300));
    
    // 提交多个递增命令
    Command command(CommandType::UNKNOWN, {"i"});
    for (int i = 0; i < NINC; i++) {
        test.One(command, 3, false);
    }
    
    // 停止所有节点
    test.StopAll();
    
    // 重启所有节点
    test.StartAll();
    
    // 等待领导者重新选举
    this_thread::sleep_for(chrono::milliseconds(300));
    
    // 提交一个递增命令
    test.One(command, 3, false);
    
    // 等待状态机应用
    this_thread::sleep_for(chrono::milliseconds(200));
    
    // 检查所有状态机的计数器值
    for (int i = 0; i < 3; i++) {
        auto sm = dynamic_pointer_cast<MockRaftStateMachine>(test.GetStateMachine(i));
        ASSERT_EQ(sm->GetCounter(), NINC + 1);
    }
    
    test.StopAll();
    
    return true;
}

} // 闭合dkv命名空间

int main() {
    using namespace dkv;

    cout << "DKV RAFT状态机测试\n" << endl;

    TestRunner runner;

    // 运行所有状态机测试
    runner.runTest("Raft状态机基本", testRaftStateMachineBasic);
    runner.runTest("Raft状态机并发", testRaftStateMachineConcurrent);
    runner.runTest("Raft状态机快照", testRaftStateMachineSnapshot);
    runner.runTest("Raft状态机领导者故障", testRaftStateMachineLeaderFailure);
    runner.runTest("Raft状态机网络分区", testRaftStateMachinePartition);
    runner.runTest("Raft状态机重启重放", testRaftStateMachineRestartReplay);
    runner.runTest("Raft状态机关闭", testRaftStateMachineShutdown);
    runner.runTest("Raft状态机重启提交", testRaftStateMachineRestartSubmit);

    // 打印测试总结
    runner.printSummary();

    return 0;
}
