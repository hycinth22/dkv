#pragma once

#include <vector>
#include <string>
#include <unordered_map>
#include <functional>
#include <mutex>
#include <memory>
#include <set>

namespace dkv {

// 哈希函数类型枚举
enum class HashFunctionType {
    MD5,      // MD5哈希函数
    SHA1,     // SHA1哈希函数
    CRC32,    // CRC32哈希函数
    MURMUR3   // Murmur3哈希函数
};

// 虚拟节点结构体
template <typename NodeType>
struct VirtualNode {
    std::string hash_key;    // 虚拟节点的哈希键
    NodeType physical_node;  // 对应的物理节点
    
    VirtualNode(const std::string& hash_key, const NodeType& physical_node)
        : hash_key(hash_key), physical_node(physical_node) {}
    
    // 重载比较运算符，用于排序
    bool operator<(const VirtualNode& other) const {
        return hash_key < other.hash_key;
    }
};

// 一致性哈希算法实现
template <typename NodeType>
class ConsistentHash {
public:
    // 构造函数
    ConsistentHash(int num_replicas = 100, HashFunctionType hash_type = HashFunctionType::MD5);
    
    // 析构函数
    ~ConsistentHash() = default;
    
    // 添加节点
    void AddNode(const NodeType& node);
    
    // 删除节点
    void RemoveNode(const NodeType& node);
    
    // 获取key对应的节点
    NodeType GetNode(const std::string& key) const;
    
    // 获取所有节点
    std::set<NodeType> GetAllNodes() const;
    
    // 获取虚拟节点数量
    int GetVirtualNodeCount() const;
    
    // 获取物理节点数量
    int GetPhysicalNodeCount() const;
    
    // 设置哈希函数类型
    void SetHashFunctionType(HashFunctionType hash_type);
    
    // 设置虚拟节点数量
    void SetNumReplicas(int num_replicas);
    
    // 重新计算哈希环
    void RebuildRing();
    
private:
    // 计算哈希值
    std::string Hash(const std::string& key) const;
    
    // 生成虚拟节点
    void GenerateVirtualNodes(const NodeType& node);
    
    // 删除节点的虚拟节点
    void RemoveVirtualNodes(const NodeType& node);
    
    int num_replicas_;  // 每个物理节点对应的虚拟节点数量
    HashFunctionType hash_type_;  // 哈希函数类型
    
    // 哈希函数映射
    std::function<std::string(const std::string&)> hash_func_;
    
    // 哈希环，存储虚拟节点，按哈希值排序
    std::vector<VirtualNode<NodeType>> hash_ring_;
    
    // 物理节点到虚拟节点的映射，用于快速删除
    std::unordered_map<NodeType, std::vector<std::string>> node_to_virtual_;
    
    // 互斥锁，保证线程安全
    mutable std::mutex mutex_;
};

// 哈希函数实现
std::string CRC32Hash(const std::string& key);
std::string Murmur3Hash(const std::string& key);

} // namespace dkv
