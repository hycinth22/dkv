#ifndef DKV_DATATYPE_SET_HPP
#define DKV_DATATYPE_SET_HPP

#include "dkv_datatype_base.hpp"
#include <unordered_set>
#include <vector>
#include <string>

namespace dkv {

// 集合数据项
class SetItem : public DataItem {
private:
    std::unordered_set<Value> elements_;  // 集合元素
    Timestamp expire_time_;
    bool has_expiration_;

public:
    SetItem();
    SetItem(Timestamp expire_time);
    
    // 从DataItem继承的方法
    DataType getType() const override;
    std::string serialize() const override;
    void deserialize(const std::string& data) override;
    bool isExpired() const override;
    void setExpiration(Timestamp expire_time) override;
    Timestamp getExpiration() const override;
    bool hasExpiration() const override;
    
    // 集合特有操作
    // 向集合添加一个元素
    bool sadd(const Value& member);
    // 向集合添加多个元素，返回成功添加的个数（不包括已有）
    size_t sadd(const std::vector<Value>& members);
    // 从集合移除一个元素
    bool srem(const Value& member);
    // 从集合移除多个元素，返回成功删除的个数（不包括不存在）
    size_t srem(const std::vector<Value>& members);
    // 获取集合中所有元素
    std::vector<Value> smembers() const;
    // 判断元素是否在集合中
    bool sismember(const Value& member) const;
    // 获取集合的大小
    size_t scard() const;
    // 清空集合
    void clear();
    // 判断集合是否为空
    bool empty() const;
};

} // namespace dkv

#endif // DKV_DATATYPE_SET_HPP