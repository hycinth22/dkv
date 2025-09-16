#pragma once

#include "dkv_datatype_base.hpp"
#include <vector>
#include <cstdint>
#include <string>
#include <memory>

namespace dkv {

// HyperLogLog数据项
class HyperLogLogItem : public DataItem {
private:
    // HyperLogLog寄存器数组
    std::vector<uint8_t> registers_;
    // 存储基数估计值的缓存
    mutable uint64_t cardinality_; 
    mutable bool cache_valid_; 
    
    Timestamp expire_time_;
    bool has_expiration_;

    // HyperLogLog参数
    static constexpr uint8_t kPrecision = 14; // 精度参数
    static constexpr size_t kRegisterCount = 1 << kPrecision; // 寄存器数量
    static constexpr double kAlpha = 0.7213 / (1 + 1.079 / kRegisterCount); // 常数因子

    // MurmurHash3哈希函数
    uint64_t hash(const Value& value) const;
    
    // 更新缓存的基数估计值
    void updateCardinality() const;

public:
    HyperLogLogItem();
    HyperLogLogItem(Timestamp expire_time);

    // 从DataItem继承的方法
    DataType getType() const override;
    std::string serialize() const override;
    void deserialize(const std::string& data) override;
    bool isExpired() const override;
    void setExpiration(Timestamp expire_time) override;
    Timestamp getExpiration() const override;
    bool hasExpiration() const override;

    // HyperLogLog特有操作
    // 添加元素到HyperLogLog
    bool add(const Value& element);
    
    // 获取基数估计值
    uint64_t count() const;
    
    // 合并多个HyperLogLog
    bool merge(const std::vector<HyperLogLogItem*>& hll_items);
    
    // 清除HyperLogLog数据
    void clear();
    
    // 检查HyperLogLog是否为空
    bool empty() const;
};

// 全局工厂函数声明
dkv::DataItem* createHyperLogLogItem();
dkv::DataItem* createHyperLogLogItem(dkv::Timestamp expire_time);

} // namespace dkv