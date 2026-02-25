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

// MD5哈希函数实现（生成16字节MD5，再以32位十六进制字符串返回）
std::string MD5Hash(const std::string& key) {
    // 简单的 MD5 实现，来源于 RFC 1321 的常规模板代码，经适配为本项目使用
    // 为保持文件自包含，这里使用一个最小化的实现，而不依赖外部库。
    struct MD5Context {
        uint32_t state[4];
        uint32_t count[2];
        unsigned char buffer[64];
    };
    
    auto F = [](uint32_t x, uint32_t y, uint32_t z) { return (x & y) | (~x & z); };
    auto G = [](uint32_t x, uint32_t y, uint32_t z) { return (x & z) | (y & ~z); };
    auto H = [](uint32_t x, uint32_t y, uint32_t z) { return x ^ y ^ z; };
    auto I = [](uint32_t x, uint32_t y, uint32_t z) { return y ^ (x | ~z); };
    auto ROTATE_LEFT = [](uint32_t x, uint32_t n) { return (x << n) | (x >> (32 - n)); };
    
    auto FF = [&](uint32_t &a, uint32_t b, uint32_t c, uint32_t d, uint32_t x, uint32_t s, uint32_t ac) {
        a += F(b, c, d) + x + ac;
        a = ROTATE_LEFT(a, s);
        a += b;
    };
    auto GG = [&](uint32_t &a, uint32_t b, uint32_t c, uint32_t d, uint32_t x, uint32_t s, uint32_t ac) {
        a += G(b, c, d) + x + ac;
        a = ROTATE_LEFT(a, s);
        a += b;
    };
    auto HH = [&](uint32_t &a, uint32_t b, uint32_t c, uint32_t d, uint32_t x, uint32_t s, uint32_t ac) {
        a += H(b, c, d) + x + ac;
        a = ROTATE_LEFT(a, s);
        a += b;
    };
    auto II = [&](uint32_t &a, uint32_t b, uint32_t c, uint32_t d, uint32_t x, uint32_t s, uint32_t ac) {
        a += I(b, c, d) + x + ac;
        a = ROTATE_LEFT(a, s);
        a += b;
    };
    
    auto Encode = [](unsigned char *output, const uint32_t *input, size_t len) {
        for (size_t i = 0, j = 0; j < len; ++i, j += 4) {
            output[j] = (unsigned char)(input[i] & 0xff);
            output[j + 1] = (unsigned char)((input[i] >> 8) & 0xff);
            output[j + 2] = (unsigned char)((input[i] >> 16) & 0xff);
            output[j + 3] = (unsigned char)((input[i] >> 24) & 0xff);
        }
    };
    
    auto Decode = [](uint32_t *output, const unsigned char *input, size_t len) {
        for (size_t i = 0, j = 0; j < len; ++i, j += 4) {
            output[i] = ((uint32_t)input[j]) |
                        (((uint32_t)input[j + 1]) << 8) |
                        (((uint32_t)input[j + 2]) << 16) |
                        (((uint32_t)input[j + 3]) << 24);
        }
    };
    
    auto MD5Transform = [&](uint32_t state[4], const unsigned char block[64]) {
        uint32_t a = state[0], b = state[1], c = state[2], d = state[3];
        uint32_t x[16];
        Decode(x, block, 64);
        
        /* Round 1 */
        FF(a, b, c, d, x[0], 7, 0xd76aa478); /* 1 */
        FF(d, a, b, c, x[1], 12, 0xe8c7b756); /* 2 */
        FF(c, d, a, b, x[2], 17, 0x242070db); /* 3 */
        FF(b, c, d, a, x[3], 22, 0xc1bdceee); /* 4 */
        FF(a, b, c, d, x[4], 7, 0xf57c0faf); /* 5 */
        FF(d, a, b, c, x[5], 12, 0x4787c62a); /* 6 */
        FF(c, d, a, b, x[6], 17, 0xa8304613); /* 7 */
        FF(b, c, d, a, x[7], 22, 0xfd469501); /* 8 */
        FF(a, b, c, d, x[8], 7, 0x698098d8); /* 9 */
        FF(d, a, b, c, x[9], 12, 0x8b44f7af); /* 10 */
        FF(c, d, a, b, x[10], 17, 0xffff5bb1); /* 11 */
        FF(b, c, d, a, x[11], 22, 0x895cd7be); /* 12 */
        FF(a, b, c, d, x[12], 7, 0x6b901122); /* 13 */
        FF(d, a, b, c, x[13], 12, 0xfd987193); /* 14 */
        FF(c, d, a, b, x[14], 17, 0xa679438e); /* 15 */
        FF(b, c, d, a, x[15], 22, 0x49b40821); /* 16 */
        
        /* Round 2 */
        GG(a, b, c, d, x[1], 5, 0xf61e2562); /* 17 */
        GG(d, a, b, c, x[6], 9, 0xc040b340); /* 18 */
        GG(c, d, a, b, x[11], 14, 0x265e5a51); /* 19 */
        GG(b, c, d, a, x[0], 20, 0xe9b6c7aa); /* 20 */
        GG(a, b, c, d, x[5], 5, 0xd62f105d); /* 21 */
        GG(d, a, b, c, x[10], 9, 0x02441453); /* 22 */
        GG(c, d, a, b, x[15], 14, 0xd8a1e681); /* 23 */
        GG(b, c, d, a, x[4], 20, 0xe7d3fbc8); /* 24 */
        GG(a, b, c, d, x[9], 5, 0x21e1cde6); /* 25 */
        GG(d, a, b, c, x[14], 9, 0xc33707d6); /* 26 */
        GG(c, d, a, b, x[3], 14, 0xf4d50d87); /* 27 */
        GG(b, c, d, a, x[8], 20, 0x455a14ed); /* 28 */
        GG(a, b, c, d, x[13], 5, 0xa9e3e905); /* 29 */
        GG(d, a, b, c, x[2], 9, 0xfcefa3f8); /* 30 */
        GG(c, d, a, b, x[7], 14, 0x676f02d9); /* 31 */
        GG(b, c, d, a, x[12], 20, 0x8d2a4c8a); /* 32 */
        
        /* Round 3 */
        HH(a, b, c, d, x[5], 4, 0xfffa3942); /* 33 */
        HH(d, a, b, c, x[8], 11, 0x8771f681); /* 34 */
        HH(c, d, a, b, x[11], 16, 0x6d9d6122); /* 35 */
        HH(b, c, d, a, x[14], 23, 0xfde5380c); /* 36 */
        HH(a, b, c, d, x[1], 4, 0xa4beea44); /* 37 */
        HH(d, a, b, c, x[4], 11, 0x4bdecfa9); /* 38 */
        HH(c, d, a, b, x[7], 16, 0xf6bb4b60); /* 39 */
        HH(b, c, d, a, x[10], 23, 0xbebfbc70); /* 40 */
        HH(a, b, c, d, x[13], 4, 0x289b7ec6); /* 41 */
        HH(d, a, b, c, x[0], 11, 0xeaa127fa); /* 42 */
        HH(c, d, a, b, x[3], 16, 0xd4ef3085); /* 43 */
        HH(b, c, d, a, x[6], 23, 0x04881d05); /* 44 */
        HH(a, b, c, d, x[9], 4, 0xd9d4d039); /* 45 */
        HH(d, a, b, c, x[12], 11, 0xe6db99e5); /* 46 */
        HH(c, d, a, b, x[15], 16, 0x1fa27cf8); /* 47 */
        HH(b, c, d, a, x[2], 23, 0xc4ac5665); /* 48 */
        
        /* Round 4 */
        II(a, b, c, d, x[0], 6, 0xf4292244); /* 49 */
        II(d, a, b, c, x[7], 10, 0x432aff97); /* 50 */
        II(c, d, a, b, x[14], 15, 0xab9423a7); /* 51 */
        II(b, c, d, a, x[5], 21, 0xfc93a039); /* 52 */
        II(a, b, c, d, x[12], 6, 0x655b59c3); /* 53 */
        II(d, a, b, c, x[3], 10, 0x8f0ccc92); /* 54 */
        II(c, d, a, b, x[10], 15, 0xffeff47d); /* 55 */
        II(b, c, d, a, x[1], 21, 0x85845dd1); /* 56 */
        II(a, b, c, d, x[8], 6, 0x6fa87e4f); /* 57 */
        II(d, a, b, c, x[15], 10, 0xfe2ce6e0); /* 58 */
        II(c, d, a, b, x[6], 15, 0xa3014314); /* 59 */
        II(b, c, d, a, x[13], 21, 0x4e0811a1); /* 60 */
        II(a, b, c, d, x[4], 6, 0xf7537e82); /* 61 */
        II(d, a, b, c, x[11], 10, 0xbd3af235); /* 62 */
        II(c, d, a, b, x[2], 15, 0x2ad7d2bb); /* 63 */
        II(b, c, d, a, x[9], 21, 0xeb86d391); /* 64 */
        
        state[0] += a;
        state[1] += b;
        state[2] += c;
        state[3] += d;
    };
    
    auto MD5Init = [&](MD5Context *context) {
        context->count[0] = context->count[1] = 0;
        context->state[0] = 0x67452301;
        context->state[1] = 0xefcdab89;
        context->state[2] = 0x98badcfe;
        context->state[3] = 0x10325476;
    };
    
    auto MD5Update = [&](MD5Context *context, const unsigned char *input, size_t inputLen) {
        size_t i, index, partLen;
        index = (unsigned int)((context->count[0] >> 3) & 0x3F);
        if ((context->count[0] += ((uint32_t)inputLen << 3)) < ((uint32_t)inputLen << 3))
            context->count[1]++;
        context->count[1] += ((uint32_t)inputLen >> 29);
        partLen = 64 - index;
        if (inputLen >= partLen) {
            memcpy(&context->buffer[index], input, partLen);
            MD5Transform(context->state, context->buffer);
            for (i = partLen; i + 63 < inputLen; i += 64)
                MD5Transform(context->state, &input[i]);
            index = 0;
        } else {
            i = 0;
        }
        memcpy(&context->buffer[index], &input[i], inputLen - i);
    };
    
    auto MD5Final = [&](unsigned char digest[16], MD5Context *context) {
        unsigned char bits[8];
        unsigned int index, padLen;
        static unsigned char PADDING[64] = {
            0x80
        };
        Encode(bits, context->count, 8);
        index = (unsigned int)((context->count[0] >> 3) & 0x3f);
        padLen = (index < 56) ? (56 - index) : (120 - index);
        MD5Update(context, PADDING, padLen);
        MD5Update(context, bits, 8);
        Encode(digest, context->state, 16);
        memset(context, 0, sizeof(*context));
    };
    
    MD5Context ctx;
    unsigned char digest[16];
    MD5Init(&ctx);
    MD5Update(&ctx, reinterpret_cast<const unsigned char*>(key.data()), key.size());
    MD5Final(digest, &ctx);
    
    char buf[33];
    for (int i = 0; i < 16; ++i) {
        snprintf(buf + i * 2, 3, "%02x", digest[i]);
    }
    return std::string(buf, 32);
}

// SHA1哈希函数实现（生成20字节SHA1，再以40位十六进制字符串返回）
std::string SHA1Hash(const std::string& key) {
    struct SHA1Context {
        uint32_t state[5];
        uint32_t count[2];
        unsigned char buffer[64];
    };
    
    auto SHA1Transform = [](uint32_t state[5], const unsigned char buffer[64]) {
        uint32_t a = state[0];
        uint32_t b = state[1];
        uint32_t c = state[2];
        uint32_t d = state[3];
        uint32_t e = state[4];
        uint32_t w[80];
        
        auto rol = [](uint32_t value, uint32_t bits) {
            return (value << bits) | (value >> (32 - bits));
        };
        
        for (int i = 0; i < 16; ++i) {
            w[i]  = (uint32_t)buffer[i * 4] << 24;
            w[i] |= (uint32_t)buffer[i * 4 + 1] << 16;
            w[i] |= (uint32_t)buffer[i * 4 + 2] << 8;
            w[i] |= (uint32_t)buffer[i * 4 + 3];
        }
        for (int i = 16; i < 80; ++i) {
            w[i] = rol(w[i - 3] ^ w[i - 8] ^ w[i - 14] ^ w[i - 16], 1);
        }
        
        for (int i = 0; i < 80; ++i) {
            uint32_t f, k;
            if (i < 20) {
                f = (b & c) | ((~b) & d);
                k = 0x5a827999;
            } else if (i < 40) {
                f = b ^ c ^ d;
                k = 0x6ed9eba1;
            } else if (i < 60) {
                f = (b & c) | (b & d) | (c & d);
                k = 0x8f1bbcdc;
            } else {
                f = b ^ c ^ d;
                k = 0xca62c1d6;
            }
            uint32_t temp = rol(a, 5) + f + e + k + w[i];
            e = d;
            d = c;
            c = rol(b, 30);
            b = a;
            a = temp;
        }
        
        state[0] += a;
        state[1] += b;
        state[2] += c;
        state[3] += d;
        state[4] += e;
    };
    
    auto SHA1Init = [](SHA1Context *context) {
        context->state[0] = 0x67452301;
        context->state[1] = 0xefcdab89;
        context->state[2] = 0x98badcfe;
        context->state[3] = 0x10325476;
        context->state[4] = 0xc3d2e1f0;
        context->count[0] = context->count[1] = 0;
    };
    
    auto SHA1Update = [&](SHA1Context *context, const unsigned char *data, size_t len) {
        uint32_t j = (context->count[0] >> 3) & 63;
        if ((context->count[0] += (uint32_t)len << 3) < ((uint32_t)len << 3)) {
            context->count[1]++;
        }
        context->count[1] += (uint32_t)(len >> 29);
        uint32_t i = 0;
        if (j + len > 63) {
            memcpy(&context->buffer[j], data, (i = 64 - j));
            SHA1Transform(context->state, context->buffer);
            for (; i + 63 < len; i += 64) {
                SHA1Transform(context->state, &data[i]);
            }
            j = 0;
        }
        memcpy(&context->buffer[j], &data[i], len - i);
    };
    
    auto SHA1Final = [&](unsigned char digest[20], SHA1Context *context) {
        unsigned char finalcount[8];
        for (int i = 0; i < 8; ++i) {
            finalcount[i] = (unsigned char)((context->count[(i >= 4 ? 0 : 1)]
                               >> ((3 - (i & 3)) * 8)) & 255);
        }
        unsigned char c = 0200;
        SHA1Update(context, &c, 1);
        while ((context->count[0] & 504) != 448) {
            c = 0;
            SHA1Update(context, &c, 1);
        }
        SHA1Update(context, finalcount, 8);
        for (int i = 0; i < 20; ++i) {
            digest[i] = (unsigned char)
                ((context->state[i >> 2] >> ((3 - (i & 3)) * 8)) & 255);
        }
        memset(context, 0, sizeof(*context));
        memset(&finalcount, 0, sizeof(finalcount));
    };
    
    SHA1Context ctx;
    unsigned char digest[20];
    SHA1Init(&ctx);
    SHA1Update(&ctx,
               reinterpret_cast<const unsigned char*>(key.data()),
               key.size());
    SHA1Final(digest, &ctx);
    
    char buf[41];
    for (int i = 0; i < 20; ++i) {
        snprintf(buf + i * 2, 3, "%02x", digest[i]);
    }
    return std::string(buf, 40);
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
            [[fallthrough]];
        case 2:
            k ^= data[i + 1] << 8;
            [[fallthrough]];
        case 1:
            k ^= data[i];
            k *= c1;
            k = (k << r1) | (k >> (32 - r1));
            k *= c2;
            h ^= k;
        default:
            break;
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
        case HashFunctionType::MD5:
            hash_func_ = MD5Hash;
            break;
        case HashFunctionType::SHA1:
            hash_func_ = SHA1Hash;
            break;
        default:
            hash_func_ = MD5Hash;
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
