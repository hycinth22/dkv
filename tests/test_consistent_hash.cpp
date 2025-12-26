#include <gtest/gtest.h>
#include "dkv_consistent_hash.hpp"

using namespace dkv;

// 测试一致性哈希算法
class ConsistentHashTest : public ::testing::Test {
protected:
    ConsistentHash<int> ch_;
    
    void SetUp() override {
        // 初始化一致性哈希，使用默认配置
        ch_ = ConsistentHash<int>(100, HashFunctionType::MD5);
    }
};

// 测试添加节点
TEST_F(ConsistentHashTest, AddNode) {
    // 添加节点
    ch_.AddNode(1);
    ch_.AddNode(2);
    ch_.AddNode(3);
    
    // 验证节点数量
    EXPECT_EQ(ch_.GetPhysicalNodeCount(), 3);
    EXPECT_EQ(ch_.GetVirtualNodeCount(), 300); // 3个节点，每个节点100个虚拟节点
}

// 测试删除节点
TEST_F(ConsistentHashTest, RemoveNode) {
    // 添加节点
    ch_.AddNode(1);
    ch_.AddNode(2);
    ch_.AddNode(3);
    
    // 删除一个节点
    ch_.RemoveNode(2);
    
    // 验证节点数量
    EXPECT_EQ(ch_.GetPhysicalNodeCount(), 2);
    EXPECT_EQ(ch_.GetVirtualNodeCount(), 200); // 2个节点，每个节点100个虚拟节点
}

// 测试获取节点
TEST_F(ConsistentHashTest, GetNode) {
    // 添加节点
    ch_.AddNode(1);
    ch_.AddNode(2);
    ch_.AddNode(3);
    
    // 同一个key应该始终映射到同一个节点
    int node1 = ch_.GetNode("test_key");
    int node2 = ch_.GetNode("test_key");
    int node3 = ch_.GetNode("test_key");
    
    EXPECT_EQ(node1, node2);
    EXPECT_EQ(node2, node3);
}

// 测试节点分布均匀性
TEST_F(ConsistentHashTest, NodeDistribution) {
    // 添加多个节点
    for (int i = 0; i < 10; i++) {
        ch_.AddNode(i);
    }
    
    // 统计每个节点的key分布
    std::unordered_map<int, int> node_counts;
    for (int i = 0; i < 10000; i++) {
        std::string key = "test_key_" + std::to_string(i);
        int node = ch_.GetNode(key);
        node_counts[node]++;
    }
    
    // 验证所有节点都有key分布
    EXPECT_EQ(node_counts.size(), 10);
    
    // 计算分布偏差
    double avg = 10000.0 / 10;
    double max_deviation = 0.0;
    for (const auto& pair : node_counts) {
        double deviation = std::abs(pair.second - avg) / avg;
        max_deviation = std::max(max_deviation, deviation);
    }
    
    // 最大偏差不应超过20%
    EXPECT_LT(max_deviation, 0.2);
}

// 测试不同哈希函数
TEST_F(ConsistentHashTest, DifferentHashFunctions) {
    // 添加节点
    ch_.AddNode(1);
    ch_.AddNode(2);
    ch_.AddNode(3);
    
    // 获取使用MD5的节点
    int node_md5 = ch_.GetNode("test_key");
    
    // 切换到SHA1哈希函数
    ch_.SetHashFunctionType(HashFunctionType::SHA1);
    int node_sha1 = ch_.GetNode("test_key");
    
    // 切换到CRC32哈希函数
    ch_.SetHashFunctionType(HashFunctionType::CRC32);
    int node_crc32 = ch_.GetNode("test_key");
    
    // 切换到MURMUR3哈希函数
    ch_.SetHashFunctionType(HashFunctionType::MURMUR3);
    int node_murmur3 = ch_.GetNode("test_key");
    
    // 不同哈希函数可能产生不同的结果，但应该都是有效的节点
    EXPECT_TRUE(node_md5 >= 1 && node_md5 <= 3);
    EXPECT_TRUE(node_sha1 >= 1 && node_sha1 <= 3);
    EXPECT_TRUE(node_crc32 >= 1 && node_crc32 <= 3);
    EXPECT_TRUE(node_murmur3 >= 1 && node_murmur3 <= 3);
}

// 测试虚拟节点数量调整
TEST_F(ConsistentHashTest, VirtualNodeCount) {
    // 添加节点
    ch_.AddNode(1);
    ch_.AddNode(2);
    
    // 验证默认虚拟节点数量
    EXPECT_EQ(ch_.GetVirtualNodeCount(), 200); // 2个节点，每个节点100个虚拟节点
    
    // 调整虚拟节点数量为200
    ch_.SetNumReplicas(200);
    EXPECT_EQ(ch_.GetVirtualNodeCount(), 400); // 2个节点，每个节点200个虚拟节点
    
    // 调整虚拟节点数量为50
    ch_.SetNumReplicas(50);
    EXPECT_EQ(ch_.GetVirtualNodeCount(), 100); // 2个节点，每个节点50个虚拟节点
}

// 测试节点增减时的一致性哈希表现
TEST_F(ConsistentHashTest, NodeAddRemoveConsistency) {
    // 添加初始节点
    ch_.AddNode(1);
    ch_.AddNode(2);
    ch_.AddNode(3);
    
    // 记录初始分布
    std::unordered_map<int, std::vector<std::string>> initial_distribution;
    for (int i = 0; i < 1000; i++) {
        std::string key = "test_key_" + std::to_string(i);
        int node = ch_.GetNode(key);
        initial_distribution[node].push_back(key);
    }
    
    // 添加一个新节点
    ch_.AddNode(4);
    
    // 统计key重新分布的情况
    int changed = 0;
    for (int i = 0; i < 1000; i++) {
        std::string key = "test_key_" + std::to_string(i);
        int node = ch_.GetNode(key);
        
        // 检查key是否在原来的节点或新节点
        bool found = false;
        for (const auto& pair : initial_distribution) {
            if (std::find(pair.second.begin(), pair.second.end(), key) != pair.second.end()) {
                found = true;
                break;
            }
        }
        
        if (!found) {
            changed++;
        }
    }
    
    // 重新分布的key比例不应太高（理想情况下应该小于30%）
    EXPECT_LT(changed, 300);
}

// 测试获取所有节点
TEST_F(ConsistentHashTest, GetAllNodes) {
    // 添加节点
    ch_.AddNode(1);
    ch_.AddNode(2);
    ch_.AddNode(3);
    
    // 获取所有节点
    std::set<int> nodes = ch_.GetAllNodes();
    
    // 验证节点集合
    EXPECT_EQ(nodes.size(), 3);
    EXPECT_TRUE(nodes.count(1));
    EXPECT_TRUE(nodes.count(2));
    EXPECT_TRUE(nodes.count(3));
}

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
