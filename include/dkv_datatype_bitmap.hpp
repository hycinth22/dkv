#pragma once

#include "dkv_datatype_base.hpp"
#include <vector>
#include <cstdint>

namespace dkv {

// 位图数据项
class BitmapItem : public DataItem {
private:
    std::vector<uint8_t> bits_; // 位图数据，使用uint8_t数组存储

public:
    BitmapItem();
    BitmapItem(Timestamp expire_time);

    // 从DataItem继承的方法
    DataType getType() const override;
    std::string serialize() const override;
    void deserialize(const std::string& data) override;

    // Bitmap特有操作
    // 设置指定位的值
    bool setBit(uint64_t offset, bool value);
    
    // 获取指定位的值
    bool getBit(uint64_t offset) const;
    
    // 统计位图中值为1的位的数量
    size_t bitCount() const;
    
    // 统计指定位范围内值为1的位的数量
    size_t bitCount(uint64_t start, uint64_t end) const;
    
    // 获取位图数据大小（字节数）
    size_t size() const;
    
    // 清除位图数据
    void clear();
    
    // 检查位图是否为空
    bool empty() const;
    
    // 位图操作方法
    // 执行多个位图的按位与操作
    bool bitOpAnd(const std::vector<BitmapItem*>& bitmap_items);
    
    // 执行多个位图的按位或操作
    bool bitOpOr(const std::vector<BitmapItem*>& bitmap_items);
    
    // 执行多个位图的按位异或操作
    bool bitOpXor(const std::vector<BitmapItem*>& bitmap_items);
    
    // 执行位图的按位非操作
    bool bitOpNot(BitmapItem* bitmap_item);
};

} // namespace dkv