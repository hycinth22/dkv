#include "multinode/shard/dkv_consistent_hash.hpp"
#include <algorithm>
#include <cstring>
#include <stdexcept>

namespace dkv {

// CRC32哈希函数实现
std::string CRC32Hash(const std::string& key) {
    unsigned int crc = 0xFFFFFFFF;
    for (unsigned char c : key) {
        crc ^= c;
        for (int i = 0; i < 8; i++) {
            crc = (crc >> 1) ^ (0xEDB88320 * (crc & 1));
        }
    }
    crc ^= 0xFFFFFFFF;
    
    char buf[9];
    snprintf(buf, sizeof(buf), "%08x", crc);
    return std::string(buf);
}

// Murmur3哈希函数实现
std::string Murmur3Hash(const std::string& key) {
    uint32_t h = 0x811C9DC5;
    const uint32_t c1 = 0xCC9E2D51;
    const uint32_t c2 = 0x1B873593;
    const uint32_t r1 = 15;
    const uint32_t r2 = 13;
    const uint32_t m = 5;
    const uint32_t n = 0xE6546B64;
    
    size_t len = key.length();
    const uint8_t* data = reinterpret_cast<const uint8_t*>(key.c_str());
    size_t i = 0;
    
    while (len >= 4) {
        uint32_t k = *(uint32_t*)(data + i);
        k *= c1;
        k = (k << r1) | (k >> (32 - r1));
        k *= c2;
        h ^= k;
        h = (h << r2) | (h >> (32 - r2));
        h = h * m + n;
        i += 4;
        len -= 4;
    }
    
    uint32_t k = 0;
    switch (len) {
        case 3:
            k ^= data[i + 2] << 16;
        case 2:
            k ^= data[i + 1] << 8;
        case 1:
            k ^= data[i];
            k *= c1;
            k = (k << r1) | (k >> (32 - r1));
            k *= c2;
            h ^= k;
    }
    
    h ^= key.length();
    h ^= h >> 16;
    h *= 0x85EBCA6B;
    h ^= h >> 13;
    h *= 0xC2B2AE35;
    h ^= h >> 16;
    
    char buf[9];
    snprintf(buf, sizeof(buf), "%08x", h);
    return std::string(buf);
}

// 模板类构造函数实现
template <typename NodeType>
ConsistentHash<NodeType>::ConsistentHash(int num_replicas, HashFunctionType hash_type)
    : num_replicas_(num_replicas), hash_type_(hash_type) {
    SetHashFunctionType(hash_type);
}

// 设置哈希函数类型
template <typename NodeType>
void ConsistentHash<NodeType>::SetHashFunctionType(HashFunctionType hash_type) {
    std::lock_guard<std::mutex> lock(mutex_);
    hash_type_ = hash_type;
    
    switch (hash_type_) {
        case HashFunctionType::CRC32:
            hash_func_ = CRC32Hash;
            break;
        case HashFunctionType::MURMUR3:
            hash_func_ = Murmur3Hash;
            break;
        default:
            hash_func_ = Murmur3Hash;
            break;
    }
    
    // 重新构建哈希环
    RebuildRing();
}

// 设置虚拟节点数量
template <typename NodeType>
void ConsistentHash<NodeType>::SetNumReplicas(int num_replicas) {
    std::lock_guard<std::mutex> lock(mutex_);
    num_replicas_ = num_replicas;
    
    // 重新构建哈希环
    RebuildRing();
}

// 计算哈希值
template <typename NodeType>
std::string ConsistentHash<NodeType>::Hash(const std::string& key) const {
    return hash_func_(key);
}

// 生成虚拟节点
template <typename NodeType>
void ConsistentHash<NodeType>::GenerateVirtualNodes(const NodeType& node) {
    std::vector<std::string> virtual_nodes;
    for (int i = 0; i < num_replicas_; i++) {
        std::string hash_key = Hash(std::to_string(i) + ":" + std::to_string(node));
        virtual_nodes.push_back(hash_key);
        hash_ring_.emplace_back(hash_key, node);
    }
    node_to_virtual_[node] = std::move(virtual_nodes);
}

// 删除节点的虚拟节点
template <typename NodeType>
void ConsistentHash<NodeType>::RemoveVirtualNodes(const NodeType& node) {
    auto it = node_to_virtual_.find(node);
    if (it != node_to_virtual_.end()) {
        const std::vector<std::string>& virtual_nodes = it->second;
        for (const auto& vn : virtual_nodes) {
            hash_ring_.erase(
                std::remove_if(hash_ring_.begin(), hash_ring_.end(),
                    [&vn](const VirtualNode<NodeType>& vnode) {
                        return vnode.hash_key == vn;
                    }),
                hash_ring_.end());
        }
        node_to_virtual_.erase(it);
    }
}

// 添加节点
template <typename NodeType>
void ConsistentHash<NodeType>::AddNode(const NodeType& node) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (node_to_virtual_.find(node) == node_to_virtual_.end()) {
        GenerateVirtualNodes(node);
        // 重新排序哈希环
        std::sort(hash_ring_.begin(), hash_ring_.end());
    }
}

// 删除节点
template <typename NodeType>
void ConsistentHash<NodeType>::RemoveNode(const NodeType& node) {
    std::lock_guard<std::mutex> lock(mutex_);
    RemoveVirtualNodes(node);
    // 重新排序哈希环
    std::sort(hash_ring_.begin(), hash_ring_.end());
}

// 获取key对应的节点
template <typename NodeType>
NodeType ConsistentHash<NodeType>::GetNode(const std::string& key) const {
    std::lock_guard<std::mutex> lock(mutex_);
    
    if (hash_ring_.empty()) {
        throw std::runtime_error("Consistent hash ring is empty");
    }
    
    std::string hash_key = Hash(key);
    
    // 查找第一个大于等于hash_key的虚拟节点
    auto it = std::lower_bound(hash_ring_.begin(), hash_ring_.end(), VirtualNode<NodeType>(hash_key, 0));
    
    // 如果没找到，返回第一个节点
    if (it == hash_ring_.end()) {
        it = hash_ring_.begin();
    }
    
    return it->physical_node;
}

// 获取所有节点
template <typename NodeType>
std::set<NodeType> ConsistentHash<NodeType>::GetAllNodes() const {
    std::lock_guard<std::mutex> lock(mutex_);
    
    std::set<NodeType> nodes;
    for (const auto& vn : hash_ring_) {
        nodes.insert(vn.physical_node);
    }
    
    return nodes;
}

// 获取虚拟节点数量
template <typename NodeType>
int ConsistentHash<NodeType>::GetVirtualNodeCount() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return static_cast<int>(hash_ring_.size());
}

// 获取物理节点数量
template <typename NodeType>
int ConsistentHash<NodeType>::GetPhysicalNodeCount() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return static_cast<int>(node_to_virtual_.size());
}

// 重新计算哈希环
template <typename NodeType>
void ConsistentHash<NodeType>::RebuildRing() {
    std::lock_guard<std::mutex> lock(mutex_);
    
    // 清空哈希环
    hash_ring_.clear();
    
    // 重新生成所有虚拟节点
    for (const auto& pair : node_to_virtual_) {
        GenerateVirtualNodes(pair.first);
    }
    
    // 重新排序哈希环
    std::sort(hash_ring_.begin(), hash_ring_.end());
}

// 显式实例化，支持int类型的节点
template class ConsistentHash<int>;

} // namespace dkv
