#include "dkv_datatype_hyperloglog.hpp"
#include "dkv_utils.hpp"
#include <sstream>
#include <algorithm>
#include <cmath>

namespace dkv {

// MurmurHash混合函数
static uint64_t murmur_fmix64(uint64_t k) {
    k ^= k >> 33;
    k *= 0xff51afd7ed558ccdULL;
    k ^= k >> 33;
    k *= 0xc4ceb9fe1a85ec53ULL;
    k ^= k >> 33;
    return k;
}

// MurmurHash3实现（简化版）
static uint64_t murmurHash3(const void* key, int len, uint32_t seed) {
    const uint8_t* data = static_cast<const uint8_t*>(key);
    const int nblocks = len / 16;

    uint64_t h1 = seed;
    uint64_t h2 = seed;

    const uint64_t c1 = 0x87c37b91114253d5ULL;
    const uint64_t c2 = 0x4cf5ad432745937fULL;

    // 主循环
    const uint64_t* blocks = reinterpret_cast<const uint64_t*>(data + nblocks * 16);
    for (int i = -nblocks; i; i++) {
        uint64_t k1 = blocks[i * 2];
        uint64_t k2 = blocks[i * 2 + 1];

        k1 *= c1; k1  = __builtin_bswap64(k1); k1 *= c2;
        h1 ^= k1;
        h1 = __builtin_bswap64(h1); h1 *= 5; h1 += 0x52dce729;

        k2 *= c2; k2  = __builtin_bswap64(k2); k2 *= c1;
        h2 ^= k2;
        h2 = __builtin_bswap64(h2); h2 *= 5; h2 += 0x38495ab5;
    }

    // 处理剩余字节
    const uint8_t* tail = data + nblocks * 16;
    uint64_t k1 = 0;
    uint64_t k2 = 0;

    switch (len & 15) {
        case 15: k2 ^= static_cast<uint64_t>(tail[14]) << 48;
                 [[fallthrough]];
        case 14: k2 ^= static_cast<uint64_t>(tail[13]) << 40;
                 [[fallthrough]];
        case 13: k2 ^= static_cast<uint64_t>(tail[12]) << 32;
                 [[fallthrough]];
        case 12: k2 ^= static_cast<uint64_t>(tail[11]) << 24;
                 [[fallthrough]];
        case 11: k2 ^= static_cast<uint64_t>(tail[10]) << 16;
                 [[fallthrough]];
        case 10: k2 ^= static_cast<uint64_t>(tail[9]) << 8;
                 [[fallthrough]];
        case 9:  k2 ^= static_cast<uint64_t>(tail[8]);
                 k2 *= c2; k2  = __builtin_bswap64(k2); k2 *= c1; h2 ^= k2;
                 [[fallthrough]];
        case 8:  k1 ^= static_cast<uint64_t>(tail[7]) << 56;
                 [[fallthrough]];
        case 7:  k1 ^= static_cast<uint64_t>(tail[6]) << 48;
                 [[fallthrough]];
        case 6:  k1 ^= static_cast<uint64_t>(tail[5]) << 40;
                 [[fallthrough]];
        case 5:  k1 ^= static_cast<uint64_t>(tail[4]) << 32;
                 [[fallthrough]];
        case 4:  k1 ^= static_cast<uint64_t>(tail[3]) << 24;
                 [[fallthrough]];
        case 3:  k1 ^= static_cast<uint64_t>(tail[2]) << 16;
                 [[fallthrough]];
        case 2:  k1 ^= static_cast<uint64_t>(tail[1]) << 8;
                 [[fallthrough]];
        case 1:  k1 ^= static_cast<uint64_t>(tail[0]);
                 k1 *= c1; k1  = __builtin_bswap64(k1); k1 *= c2; h1 ^= k1;
                 break;
        default: break;
    };

    // 最终混合
    h1 ^= len; h2 ^= len;
    h1 += h2; h2 += h1;
    h1 = murmur_fmix64(h1);
    h2 = murmur_fmix64(h2);
    h1 += h2; h2 += h1;

    return h1;
}

// HyperLogLogItem实现
HyperLogLogItem::HyperLogLogItem() 
    : registers_(kRegisterCount, 0), cardinality_(0), cache_valid_(false),
      DataItem() {
}

HyperLogLogItem::HyperLogLogItem(const HyperLogLogItem& other)
    : DataItem(other) {
    registers_ = other.registers_; // 深拷贝寄存器数组
    cardinality_ = other.cardinality_;
    cache_valid_ = other.cache_valid_;
}

std::unique_ptr<DataItem> HyperLogLogItem::clone() const {
    auto cloned = std::make_unique<HyperLogLogItem>(*this);
    return cloned;
}

HyperLogLogItem::HyperLogLogItem(Timestamp expire_time)
    : registers_(kRegisterCount, 0), cardinality_(0), cache_valid_(false),
      DataItem(expire_time) {
}

DataType HyperLogLogItem::getType() const {
    return DataType::HYPERLOGLOG;
}

std::string HyperLogLogItem::serialize() const {
    std::ostringstream oss;
    oss << "HYPERLOGLOG:";
    
    // 序列化寄存器数据
    for (uint8_t reg : registers_) {
        oss << static_cast<char>(reg);
    }
    
    // 序列化过期时间
    if (hasExpiration()) {
        auto duration = expire_time_.load().time_since_epoch();
        auto seconds = std::chrono::duration_cast<std::chrono::seconds>(duration).count();
        oss << ":" << seconds;
    }
    
    return oss.str();
}

void HyperLogLogItem::deserialize(const std::string& data) {
    std::istringstream iss(data);
    std::string type;
    
    if (std::getline(iss, type, ':') && type == "HYPERLOGLOG") {
        // 读取寄存器数据
        registers_.resize(kRegisterCount);
        for (size_t i = 0; i < kRegisterCount; ++i) {
            char byte;
            if (iss.get(byte)) {
                registers_[i] = static_cast<uint8_t>(byte);
            } else {
                registers_[i] = 0;
            }
        }
        
        // 读取过期时间
        std::string expire_str;
        if (std::getline(iss, expire_str)) {
            uint64_t seconds = std::stoull(expire_str);
            auto duration = std::chrono::seconds(seconds);
            setExpiration(std::chrono::system_clock::time_point(duration));
        }
        
        // 重置缓存
        cache_valid_ = false;
    }
}

uint64_t HyperLogLogItem::hash(const Value& value) const {
    return murmurHash3(value.data(), value.size(), 0x12345678);
}

bool HyperLogLogItem::add(const Value& element) {
    uint64_t hash_value = hash(element);
    
    // 提取桶索引（kPrecision位）
    uint32_t index = hash_value & ((1 << kPrecision) - 1);
    
    // 剩余位中的前导零个数+1
    uint8_t leading_zeros = 1;
    hash_value >>= kPrecision;
    
    while (hash_value > 0 && (hash_value & 1) == 0) {
        leading_zeros++;
        hash_value >>= 1;
    }
    
    // 如果当前前导零个数大于寄存器中的值，则更新
    if (leading_zeros > registers_[index]) {
        registers_[index] = leading_zeros;
        cache_valid_ = false; // 缓存失效
        return true;
    }
    
    return false;
}

void HyperLogLogItem::updateCardinality() const {
    double estimate = 0.0;
    
    // 计算调和平均数
    double sum = 0.0;
    for (uint8_t reg : registers_) {
        sum += 1.0 / (1ULL << reg);
    }
    
    estimate = kAlpha * kRegisterCount * kRegisterCount / sum;
    
    // 小基数修正
    if (estimate <= 5.0 * kRegisterCount / 2.0) {
        uint64_t zeros = 0;
        for (uint8_t reg : registers_) {
            if (reg == 0) {
                zeros++;
            }
        }
        
        if (zeros > 0) {
            estimate = kRegisterCount * log(static_cast<double>(kRegisterCount) / zeros);
        }
    }
    
    cardinality_ = static_cast<uint64_t>(estimate);
    cache_valid_ = true;
}

uint64_t HyperLogLogItem::count() const {
    if (!cache_valid_) {
        updateCardinality();
    }
    
    return cardinality_;
}

bool HyperLogLogItem::merge(const std::vector<HyperLogLogItem*>& hll_items) {
    if (hll_items.empty()) {
        return false;
    }
    
    bool modified = false;
    
    // 对每个寄存器，取最大值
    for (size_t i = 0; i < kRegisterCount; ++i) {
        uint8_t max_val = registers_[i];
        
        for (const auto& item : hll_items) {
            if (item->registers_[i] > max_val) {
                max_val = item->registers_[i];
            }
        }
        
        if (max_val != registers_[i]) {
            registers_[i] = max_val;
            modified = true;
        }
    }
    
    if (modified) {
        cache_valid_ = false;
    }
    
    return modified;
}

void HyperLogLogItem::clear() {
    std::fill(registers_.begin(), registers_.end(), 0);
    cache_valid_ = false;
}

bool HyperLogLogItem::empty() const {
    for (uint8_t reg : registers_) {
        if (reg != 0) {
            return false;
        }
    }
    return true;
}

// 全局工厂函数实现
dkv::DataItem* createHyperLogLogItem() {
    return new HyperLogLogItem();
}

dkv::DataItem* createHyperLogLogItem(dkv::Timestamp expire_time) {
    return new HyperLogLogItem(expire_time);
}

} // namespace dkv