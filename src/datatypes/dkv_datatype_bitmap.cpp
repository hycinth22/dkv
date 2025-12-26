#include "datatypes/dkv_datatype_bitmap.hpp"
#include "dkv_utils.hpp"
#include <sstream>
#include <algorithm>

namespace dkv {

// BitmapItem 实现
BitmapItem::BitmapItem() 
    : DataItem() {
}

BitmapItem::BitmapItem(Timestamp expire_time)
    : DataItem(expire_time) {
}

BitmapItem::BitmapItem(const BitmapItem& other)
    : DataItem(other) {
    bits_ = other.bits_; // 深拷贝位图数据
}

std::unique_ptr<DataItem> BitmapItem::clone() const {
    auto cloned = std::make_unique<BitmapItem>(*this);
    return cloned;
}

DataType BitmapItem::getType() const {
    return DataType::BITMAP;
}

std::string BitmapItem::serialize() const {
    std::ostringstream oss;
    oss << "BITMAP:" << bits_.size() << ":";
    
    // 序列化位图数据
    for (uint8_t byte : bits_) {
        oss << static_cast<char>(byte);
    }
    
    // 序列化过期时间
    if (hasExpiration()) {
        auto duration = getExpiration().time_since_epoch();
        auto seconds = std::chrono::duration_cast<std::chrono::seconds>(duration).count();
        oss << ":" << seconds;
    }
    
    return oss.str();
}

void BitmapItem::deserialize(const std::string& data) {
    std::istringstream iss(data);
    std::string type, size_str;
    
    if (std::getline(iss, type, ':') && type == "BITMAP" &&
        std::getline(iss, size_str, ':')) {
        
        size_t size = std::stoul(size_str);
        bits_.resize(size);
        
        // 读取位图数据
        for (size_t i = 0; i < size; ++i) {
            char byte;
            if (iss.get(byte)) {
                bits_[i] = static_cast<uint8_t>(byte);
            } else {
                bits_[i] = 0;
            }
        }
        
        // 检查是否有过期时间
        std::string expire_str;
        if (std::getline(iss, expire_str)) {
            int64_t seconds = std::stoll(expire_str);
            expire_time_ = Timestamp(std::chrono::seconds(seconds));
            setExpiration(expire_time_);
        }
    }
}

bool BitmapItem::setBit(uint64_t offset, bool value) {
    // 计算需要的字节数
    size_t byte_index = offset / 8;
    uint8_t bit_index = offset % 8;
    
    // 如果需要扩展位图大小
    if (byte_index >= bits_.size()) {
        bits_.resize(byte_index + 1, 0);
    }
    
    // 获取当前位的值
    bool old_value = (bits_[byte_index] & (1 << bit_index)) != 0;
    
    // 设置新值
    if (value) {
        bits_[byte_index] |= (1 << bit_index);
    } else {
        bits_[byte_index] &= ~(1 << bit_index);
    }
    
    // 返回位是否被修改
    return old_value != value;
}

bool BitmapItem::getBit(uint64_t offset) const {
    // 计算字节索引和位索引
    size_t byte_index = offset / 8;
    uint8_t bit_index = offset % 8;
    
    // 如果偏移量超出范围，返回false
    if (byte_index >= bits_.size()) {
        return false;
    }
    
    // 返回指定位的值
    return (bits_[byte_index] & (1 << bit_index)) != 0;
}

size_t BitmapItem::bitCount() const {
    size_t count = 0;
    
    // 统计所有字节中1的个数
    for (uint8_t byte : bits_) {
        // 使用内置函数计算单个字节中1的个数
        count += __builtin_popcount(byte);
    }
    
    return count;
}

size_t BitmapItem::bitCount(uint64_t start, uint64_t end) const {
    if (start > end) {
        return 0;
    }
    
    size_t count = 0;
    
    // 如果范围完全超出位图大小，返回0
    if (start >= bits_.size()) {
        return 0;
    }

    for (size_t i = start; i <= end && i < bits_.size(); ++i) {
        count += __builtin_popcount(bits_[i]);
    }
    return count;
}

size_t BitmapItem::size() const {
    return bits_.size();
}

void BitmapItem::clear() {
    bits_.clear();
}

bool BitmapItem::empty() const {
    return bits_.empty();
}

bool BitmapItem::bitOpAnd(const std::vector<BitmapItem*>& bitmap_items) {
    if (bitmap_items.empty()) {
        return false;
    }
    
    // 找出最大的位图大小
    size_t max_size = 0;
    for (const auto& item : bitmap_items) {
        if (item->size() > max_size) {
            max_size = item->size();
        }
    }
    
    // 调整当前位图大小
    bits_.resize(max_size, 0xFF); // 初始化为全1，因为AND操作中全1是中性元素
    
    // 对每个位图执行AND操作
    for (const auto& item : bitmap_items) {
        size_t item_size = item->size();
        for (size_t i = 0; i < max_size; ++i) {
            if (i < item_size) {
                bits_[i] &= item->bits_[i];
            } else {
                bits_[i] = 0; // 如果其他位图没有这个字节，结果就是0
            }
        }
    }
    
    return true;
}

bool BitmapItem::bitOpOr(const std::vector<BitmapItem*>& bitmap_items) {
    if (bitmap_items.empty()) {
        return false;
    }
    
    // 找出最大的位图大小
    size_t max_size = 0;
    for (const auto& item : bitmap_items) {
        if (item->size() > max_size) {
            max_size = item->size();
        }
    }
    
    // 调整当前位图大小
    bits_.resize(max_size, 0); // 初始化为全0，因为OR操作中全0是中性元素
    
    // 对每个位图执行OR操作
    for (const auto& item : bitmap_items) {
        size_t item_size = item->size();
        for (size_t i = 0; i < item_size; ++i) {
            bits_[i] |= item->bits_[i];
        }
    }
    
    return true;
}

bool BitmapItem::bitOpXor(const std::vector<BitmapItem*>& bitmap_items) {
    if (bitmap_items.empty()) {
        return false;
    }
    
    // 找出最大的位图大小
    size_t max_size = 0;
    for (const auto& item : bitmap_items) {
        if (item->size() > max_size) {
            max_size = item->size();
        }
    }
    
    // 调整当前位图大小
    bits_.resize(max_size, 0); // 初始化为全0，因为XOR操作中全0是中性元素
    
    // 对每个位图执行XOR操作
    for (const auto& item : bitmap_items) {
        size_t item_size = item->size();
        for (size_t i = 0; i < item_size; ++i) {
            bits_[i] ^= item->bits_[i];
        }
    }
    
    return true;
}

bool BitmapItem::bitOpNot(BitmapItem* bitmap_item) {
    if (!bitmap_item) {
        return false;
    }
    
    // 调整当前位图大小与源位图相同
    size_t size = bitmap_item->size();
    bits_.resize(size);
    
    // 对每个字节执行NOT操作
    for (size_t i = 0; i < size; ++i) {
        bits_[i] = ~bitmap_item->bits_[i];
    }
    
    return true;
}

// 全局工厂函数实现
dkv::DataItem* createBitmapItem() {
    return new BitmapItem();
}

dkv::DataItem* createBitmapItem(dkv::Timestamp expire_time) {
    return new BitmapItem(expire_time);
}

} // namespace dkv