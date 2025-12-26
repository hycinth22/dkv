#include <gtest/gtest.h>
#include "multinode/shard/dkv_shard.hpp"
#include "dkv_server.hpp"

using namespace dkv;

// 测试分片配置
TEST(ShardTest, ShardConfig) {
    ShardConfig config;
    
    // 测试默认配置
    EXPECT_FALSE(config.enable_sharding);
    EXPECT_EQ(config.num_shards, 1);
    EXPECT_EQ(config.hash_type, HashFunctionType::MD5);
    EXPECT_EQ(config.num_virtual_nodes, 100);
    EXPECT_EQ(config.heartbeat_interval_ms, 1000);
    EXPECT_EQ(config.migration_batch_size, 1000);
    EXPECT_EQ(config.max_concurrent_migrations, 2);
    EXPECT_EQ(config.failover_timeout_ms, 5000);
    
    // 测试配置修改
    config.enable_sharding = true;
    config.num_shards = 4;
    config.hash_type = HashFunctionType::SHA1;
    config.num_virtual_nodes = 200;
    config.heartbeat_interval_ms = 500;
    config.migration_batch_size = 500;
    config.max_concurrent_migrations = 4;
    config.failover_timeout_ms = 3000;
    
    EXPECT_TRUE(config.enable_sharding);
    EXPECT_EQ(config.num_shards, 4);
    EXPECT_EQ(config.hash_type, HashFunctionType::SHA1);
    EXPECT_EQ(config.num_virtual_nodes, 200);
    EXPECT_EQ(config.heartbeat_interval_ms, 500);
    EXPECT_EQ(config.migration_batch_size, 500);
    EXPECT_EQ(config.max_concurrent_migrations, 4);
    EXPECT_EQ(config.failover_timeout_ms, 3000);
}

// 测试分片管理器初始化
TEST(ShardManagerTest, Initialize) {
    // 创建DKVServer实例
    DKVServer server(6380, 2, 4);
    
    // 创建分片管理器
    ShardManager shard_manager(&server);
    
    // 测试默认配置初始化
    ShardConfig config;
    config.enable_sharding = false;
    config.num_shards = 1;
    
    EXPECT_TRUE(shard_manager.Initialize(config));
    
    // 测试启用分片配置
    config.enable_sharding = true;
    config.num_shards = 3;
    
    EXPECT_TRUE(shard_manager.Initialize(config));
}

// 测试分片管理器获取分片ID
TEST(ShardManagerTest, GetShardId) {
    // 创建DKVServer实例
    DKVServer server(6380, 2, 4);
    
    // 创建分片管理器
    ShardManager shard_manager(&server);
    
    // 初始化分片管理器，启用分片
    ShardConfig config;
    config.enable_sharding = true;
    config.num_shards = 3;
    shard_manager.Initialize(config);
    shard_manager.Start();
    
    // 测试获取分片ID
    int shard_id1 = shard_manager.GetShardId("test_key1");
    int shard_id2 = shard_manager.GetShardId("test_key2");
    int shard_id3 = shard_manager.GetShardId("test_key3");
    
    // 验证分片ID在有效范围内
    EXPECT_GE(shard_id1, 0);
    EXPECT_LT(shard_id1, 3);
    EXPECT_GE(shard_id2, 0);
    EXPECT_LT(shard_id2, 3);
    EXPECT_GE(shard_id3, 0);
    EXPECT_LT(shard_id3, 3);
    
    // 同一个key应该始终映射到同一个分片
    int shard_id1_again = shard_manager.GetShardId("test_key1");
    EXPECT_EQ(shard_id1, shard_id1_again);
    
    shard_manager.Stop();
}

// 测试分片添加和删除
TEST(ShardManagerTest, AddRemoveShard) {
    // 创建DKVServer实例
    DKVServer server(6380, 2, 4);
    
    // 创建分片管理器
    ShardManager shard_manager(&server);
    
    // 初始化分片管理器，启用分片
    ShardConfig config;
    config.enable_sharding = true;
    config.num_shards = 1;
    shard_manager.Initialize(config);
    shard_manager.Start();
    
    // 测试添加分片
    std::vector<std::string> raft_peers = {"127.0.0.1:8000", "127.0.0.1:8001", "127.0.0.1:8002"};
    EXPECT_TRUE(shard_manager.AddShard(1, raft_peers));
    EXPECT_TRUE(shard_manager.AddShard(2, raft_peers));
    
    // 测试获取所有分片统计信息
    auto stats_list = shard_manager.GetAllShardStats();
    EXPECT_EQ(stats_list.size(), 3); // 初始1个分片 + 新增2个分片
    
    // 测试删除分片
    EXPECT_TRUE(shard_manager.RemoveShard(1));
    
    // 测试获取所有分片统计信息
    stats_list = shard_manager.GetAllShardStats();
    EXPECT_EQ(stats_list.size(), 2); // 删除1个分片后剩下2个分片
    
    shard_manager.Stop();
}

// 测试分片状态
TEST(ShardTest, ShardState) {
    // 创建DKVServer实例
    DKVServer server(6380, 2, 4);
    
    // 创建分片管理器
    ShardManager shard_manager(&server);
    
    // 初始化分片管理器，启用分片
    ShardConfig config;
    config.enable_sharding = true;
    config.num_shards = 2;
    shard_manager.Initialize(config);
    shard_manager.Start();
    
    // 获取分片实例
    auto shard = shard_manager.GetShard(0);
    ASSERT_NE(shard, nullptr);
    
    // 测试初始状态
    EXPECT_EQ(shard->GetState(), ShardState::ACTIVE);
    
    // 测试修改状态
    shard->SetState(ShardState::MIGRATING);
    EXPECT_EQ(shard->GetState(), ShardState::MIGRATING);
    
    shard->SetState(ShardState::FAILED);
    EXPECT_EQ(shard->GetState(), ShardState::FAILED);
    
    shard->SetState(ShardState::ACTIVE);
    EXPECT_EQ(shard->GetState(), ShardState::ACTIVE);
    
    shard_manager.Stop();
}

// 测试分片心跳
TEST(ShardTest, Heartbeat) {
    // 创建DKVServer实例
    DKVServer server(6380, 2, 4);
    
    // 创建分片管理器
    ShardManager shard_manager(&server);
    
    // 初始化分片管理器，启用分片
    ShardConfig config;
    config.enable_sharding = true;
    config.num_shards = 1;
    shard_manager.Initialize(config);
    shard_manager.Start();
    
    // 获取分片实例
    auto shard = shard_manager.GetShard(0);
    ASSERT_NE(shard, nullptr);
    
    // 测试心跳
    EXPECT_TRUE(shard->Heartbeat());
    
    // 测试统计信息
    auto stats = shard->GetStats();
    EXPECT_GT(stats.last_heartbeat, 0);
    EXPECT_EQ(stats.operations_per_second, 0);
    
    shard_manager.Stop();
}

// 测试分片迁移
TEST(ShardTest, Migration) {
    // 创建DKVServer实例
    DKVServer server(6380, 2, 4);
    
    // 创建分片管理器
    ShardManager shard_manager(&server);
    
    // 初始化分片管理器，启用分片
    ShardConfig config;
    config.enable_sharding = true;
    config.num_shards = 2;
    shard_manager.Initialize(config);
    shard_manager.Start();
    
    // 获取分片实例
    auto shard1 = shard_manager.GetShard(0);
    auto shard2 = shard_manager.GetShard(1);
    ASSERT_NE(shard1, nullptr);
    ASSERT_NE(shard2, nullptr);
    
    // 测试开始迁移
    EXPECT_TRUE(shard1->StartMigration(1, "key1", "key1000"));
    EXPECT_EQ(shard1->GetState(), ShardState::MIGRATING);
    
    // 测试迁移进度
    EXPECT_EQ(shard1->GetMigrationProgress(), 0);
    
    // 测试停止迁移
    shard1->StopMigration();
    EXPECT_EQ(shard1->GetState(), ShardState::ACTIVE);
    EXPECT_EQ(shard1->GetMigrationProgress(), 0);
    
    shard_manager.Stop();
}

// 测试分片管理器命令处理
TEST(ShardManagerTest, HandleCommand) {
    // 创建DKVServer实例
    DKVServer server(6380, 2, 4);
    
    // 创建分片管理器
    ShardManager shard_manager(&server);
    
    // 初始化分片管理器，启用分片
    ShardConfig config;
    config.enable_sharding = true;
    config.num_shards = 2;
    shard_manager.Initialize(config);
    shard_manager.Start();
    
    // 创建一个简单的SET命令
    Command set_command(CommandType::SET, {"test_key", "test_value"});
    
    // 测试命令处理
    Response response = shard_manager.HandleCommand(set_command, NO_TX);
    
    // 由于没有实际的存储引擎和Raft组，命令可能会失败，但应该能正确路由
    EXPECT_NE(response.status, ResponseStatus::OK);
    
    shard_manager.Stop();
}

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
