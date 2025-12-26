#ifndef DKV_DATATYPE_ZSET_HPP
#define DKV_DATATYPE_ZSET_HPP

#include "dkv_datatype_base.hpp"
#include <unordered_map>
#include <unordered_set>
#include <map>
#include <vector>
#include <string>
#include <algorithm>

namespace dkv {

// 有序集合数据项
class ZSetItem : public DataItem {
private:
    // 使用map按分数排序元素（分数 -> 元素列表）
    std::map<double, std::unordered_set<Value>> elements_by_score_;
    // 使用unordered_map快速查找元素的分数
    std::unordered_map<Value, double> scores_;

public:
    ZSetItem();
    ZSetItem(Timestamp expire_time);
    ZSetItem(const ZSetItem& other);
    
    // 从DataItem继承的方法
    DataType getType() const override;
    std::string serialize() const override;
    void deserialize(const std::string& data) override;
    std::unique_ptr<DataItem> clone() const override;
    
    // 有序集合特有操作
    // 向有序集合添加元素及其分数
    bool zadd(const Value& member, double score);
    // 向有序集合添加多个元素及其分数，返回成功添加或更新的个数
    size_t zadd(const std::vector<std::pair<Value, double>>& members_with_scores);
    // 从有序集合移除一个元素
    bool zrem(const Value& member);
    // 从有序集合移除多个元素，返回成功删除的个数
    size_t zrem(const std::vector<Value>& members);
    // 获取元素的分数
    bool zscore(const Value& member, double& score) const;
    // 判断元素是否在有序集合中
    bool zismember(const Value& member) const;
    // 获取元素的排名（从小到大，从0开始）
    bool zrank(const Value& member, size_t& rank) const;
    // 获取元素的逆序排名（从大到小，从0开始）
    bool zrevrank(const Value& member, size_t& rank) const;
    // 获取指定排名范围的元素（从小到大）
    std::vector<std::pair<Value, double>> zrange(size_t start, size_t stop) const;
    // 获取指定排名范围的元素（从大到小）
    std::vector<std::pair<Value, double>> zrevrange(size_t start, size_t stop) const;
    // 获取指定分数范围的元素（从小到大）
    std::vector<std::pair<Value, double>> zrangebyscore(double min, double max) const;
    // 获取指定分数范围的元素（从大到小）
    std::vector<std::pair<Value, double>> zrevrangebyscore(double max, double min) const;
    // 获取指定分数范围内的元素个数
    size_t zcount(double min, double max) const;
    // 获取有序集合的大小
    size_t zcard() const;
    // 清空有序集合
    void clear();
    // 判断有序集合是否为空
    bool empty() const;
};

} // namespace dkv

#endif // DKV_DATATYPE_ZSET_HPP