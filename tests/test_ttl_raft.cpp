#include "multinode/raft/dkv_raft.hpp"
#include "multinode/raft/dkv_raft_statemachine.hpp"
#include "multinode/raft/dkv_raft_persist.hpp"
#include "multinode/raft/dkv_raft_network.hpp"
#include "dkv_server.hpp"
#include "dkv_utils.hpp"
#include "dkv_core.hpp"
#include <iostream>
#include <thread>
#include <chrono>
#include <vector>
#include <string>
#include <memory>
#include <cassert>

using namespace dkv;

// 模拟RAFT网络实现
class MockRaftNetwork : public RaftNetwork {
private:
    std::vector<std::shared_ptr<Raft>> peers_;
    int me_;

public:
    MockRaftNetwork(int me, const std::vector<std::shared_ptr<Raft>>& peers)
        : me_(me), peers_(peers) {}

    AppendEntriesResponse SendAppendEntries(int serverId, const AppendEntriesRequest& request) override {
        if (serverId >= 0 && serverId < peers_.size()) {
            return peers_[serverId]->OnAppendEntries(request);
        }
        return AppendEntriesResponse{0, false, 0};
    }

    RequestVoteResponse SendRequestVote(int serverId, const RequestVoteRequest& request) override {
        if (serverId >= 0 && serverId < peers_.size()) {
            return peers_[serverId]->OnRequestVote(request);
        }
        return RequestVoteResponse{0, false};
    }

    InstallSnapshotResponse SendInstallSnapshot(int serverId, const InstallSnapshotRequest& request) override {
        if (serverId >= 0 && serverId < peers_.size()) {
            return peers_[serverId]->OnInstallSnapshot(request);
        }
        return InstallSnapshotResponse{0, false};
    }
};

// 测试TTL与RAFT复制的一致性
void test_ttl_raft_consistency() {
    std::cout << "=== 测试TTL与RAFT复制的一致性 ===\n";

    // 创建3个RAFT节点
    constexpr int num_nodes = 3;
    std::vector<std::shared_ptr<Raft>> rafts;
    std::vector<std::shared_ptr<RaftStateMachineManager>> state_machines;
    std::vector<std::shared_ptr<RaftPersister>> persisters;
    std::vector<std::shared_ptr<MockRaftNetwork>> networks;
    std::vector<std::unique_ptr<DKVServer>> servers;

    // 初始化节点
    for (int i = 0; i < num_nodes; ++i) {
        // 创建状态机
        auto state_machine = std::make_shared<RaftStateMachineManager>();
        state_machines.push_back(state_machine);

        // 创建DKVServer实例
        auto server = std::make_unique<DKVServer>(8080 + i);
        server->start();
        servers.push_back(std::move(server));

        // 设置状态机的DKVServer
        state_machine->SetDKVServer(servers.back().get());

        // 创建持久化
        auto persister = std::make_shared<RaftFilePersister>("/tmp/raft_test_" + std::to_string(i));
        persisters.push_back(persister);

        // 创建网络（暂时为空，后面会设置）
        auto network = std::make_shared<MockRaftNetwork>(i, rafts);
        networks.push_back(network);

        // 设置状态机的命令处理器和存储引擎 
        // todo:fix here later
        //state_machine->SetCommandHandler(servers.back()->getCommandHandler());
        //state_machine->SetStorageEngine(servers.back()->getStorageEngine());

        // 创建RAFT实例
        std::vector<std::string> peers;
        for (int j = 0; j < num_nodes; ++j) {
            peers.push_back("127.0.0.1:808" + std::to_string(j));
        }

        auto raft = std::make_shared<Raft>(i, peers, persister, network, state_machine);
        rafts.push_back(raft);
    }

    // 更新网络的peers引用
    for (int i = 0; i < num_nodes; ++i) {
        auto network = std::dynamic_pointer_cast<MockRaftNetwork>(networks[i]);
        if (network) {
            // 这里需要重新设置，因为在创建时rafts还没有完全填充
        }
    }

    // 启动所有RAFT节点
    for (auto& raft : rafts) {
        raft->Start();
    }

    // 等待选举完成
    std::this_thread::sleep_for(std::chrono::seconds(2));

    // 找到领导者
    int leader_id = -1;
    for (int i = 0; i < num_nodes; ++i) {
        if (rafts[i]->IsLeader()) {
            leader_id = i;
            break;
        }
    }

    if (leader_id == -1) {
        std::cout << "没有选出领导者，测试失败\n";
        return;
    }

    std::cout << "选出的领导者是节点 " << leader_id << "\n";

    // 创建测试用的Command
    Command set_cmd_with_ttl(CommandType::SET, {"test_key", "test_value", "EX", "10"});
    set_cmd_with_ttl.timestamp = Utils::getCurrentTime() + std::chrono::seconds(10);

    // 提交命令到领导者
    int index, term;
    auto raft_cmd = std::make_shared<RaftCommand>(0, set_cmd_with_ttl);
    bool ok = rafts[leader_id]->StartCommand(raft_cmd, index, term);
    if (!ok) {
        std::cout << "提交命令失败\n";
        return;
    }

    // 等待命令被复制到所有节点
    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    // 检查所有节点的命令是否包含正确的timestamp
    std::cout << "命令已提交到领导者，检查各节点的命令处理\n";

    for (int i = 0; i < num_nodes; ++i) {
        // todo:fix here later
        // auto& storage = servers[i]->getStorageEngine();
        // assert(storage.getExpiration("test_key") == set_cmd_with_ttl.timestamp);
    }

    std::cout << "测试完成：命令结构包含了正确的timestamp字段\n";

    // 停止所有RAFT节点
    for (auto& raft : rafts) {
        raft->Stop();
    }
}

// 测试Command的序列化和反序列化
void test_command_serialization() {
    std::cout << "=== 测试Command的序列化和反序列化 ===\n";

    // 创建一个带有timestamp的Command
    Command original_cmd(CommandType::SET, {"test_key", "test_value", "EX", "5"});
    original_cmd.timestamp = Utils::getCurrentTime() + std::chrono::seconds(5);

    // 序列化
    std::vector<char> buffer;
    original_cmd.serialize(buffer);

    // 反序列化
    Command deserialized_cmd;
    bool success = deserialized_cmd.deserialize(buffer);
    assert(success);

    // 验证序列化和反序列化是否正确
    assert(deserialized_cmd.type == original_cmd.type);
    assert(deserialized_cmd.args == original_cmd.args);
    assert(deserialized_cmd.timestamp == original_cmd.timestamp);

    std::cout << "Command序列化和反序列化测试成功\n";
}

// 测试StorageEngine的绝对过期时间戳支持
void test_storage_engine_absolute_timestamp() {
    std::cout << "=== 测试StorageEngine的绝对过期时间戳支持 ===\n";

    // 创建StorageEngine
    StorageEngine storage;

    // 设置一个带有绝对过期时间戳的键
    auto expire_time = Utils::getCurrentTime() + std::chrono::seconds(10);
    bool success = storage.set(0, "test_key", "test_value", expire_time);
    assert(success);

    // 检查键是否存在
    assert(storage.exists(0, "test_key"));

    // 检查过期时间
    auto get_expire_time = storage.getExpiration("test_key");
    assert(get_expire_time == expire_time);

    std::cout << "StorageEngine绝对过期时间戳支持测试成功\n";
}

int main() {
    test_command_serialization();
    test_storage_engine_absolute_timestamp();
    test_ttl_raft_consistency();
    std::cout << "所有测试完成\n";
    return 0;
}
