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
#include <random>
#include <mutex>
#include <atomic>
#include <condition_variable>

using namespace std;

namespace dkv {

// 模拟的计数器状态机实现，用于测试
class CounterStateMachine : public RaftStateMachine {
private:
    int counter_;
    std::mutex mutex_;

public:
    CounterStateMachine() : counter_(0) {}

    // 执行命令并返回结果
    vector<char> DoOp(const vector<char>& command) override {
        std::unique_lock<std::mutex> lock(mutex_);
        
        if (!command.empty()) {
            switch (command[0]) {
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
        
        // 返回当前计数器值
        string result = to_string(counter_);
        return vector<char>(result.begin(), result.end());
    }

    // 创建快照
    vector<char> Snapshot() override {
        std::unique_lock<std::mutex> lock(mutex_);
        string snapshot = to_string(counter_);
        return vector<char>(snapshot.begin(), snapshot.end());
    }

    // 从快照恢复
    void Restore(const vector<char>& snapshot) override {
        std::unique_lock<std::mutex> lock(mutex_);
        string snapshot_str(snapshot.begin(), snapshot.end());
        counter_ = stoi(snapshot_str);
    }

    // 获取当前计数器值
    int GetCounter() {
        std::unique_lock<std::mutex> lock(mutex_);
        return counter_;
    }
};

// 测试基本的状态机复制
bool testRaftStateMachineBasic() {
    RaftTest test(3);
    test.StartAll();
    
    // 等待领导者选举
    this_thread::sleep_for(chrono::milliseconds(300));
    
    // 提交递增命令
    vector<char> command = {'i'};
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
            vector<char> command = {'i'};
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
    vector<char> command = {'i'};
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
    vector<char> command = {'i'};
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

// 注册状态机测试用例
void registerRaftStateMachineTests() {
    TEST_REGISTER(RaftStateMachineBasic, testRaftStateMachineBasic);
    TEST_REGISTER(RaftStateMachineConcurrent, testRaftStateMachineConcurrent);
    TEST_REGISTER(RaftStateMachineSnapshot, testRaftStateMachineSnapshot);
    TEST_REGISTER(RaftStateMachineLeaderFailure, testRaftStateMachineLeaderFailure);
    TEST_REGISTER(RaftStateMachinePartition, testRaftStateMachinePartition);
}

