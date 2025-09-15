#ifndef DKV_DATATYPE_LIST_HPP
#define DKV_DATATYPE_LIST_HPP

#include "dkv_datatype_base.hpp"
#include <list>
#include <vector>
#include <string>

namespace dkv {

// 列表数据项
class ListItem : public DataItem {
private:
    std::list<Value> elements_;  // 链表
    Timestamp expire_time_;
    bool has_expiration_;

public:
    ListItem();
    ListItem(Timestamp expire_time);
    
    // 从DataItem继承的方法
    DataType getType() const override;
    std::string serialize() const override;
    void deserialize(const std::string& data) override;
    bool isExpired() const override;
    void setExpiration(Timestamp expire_time) override;
    Timestamp getExpiration() const override;
    bool hasExpiration() const override;
    
    // 列表特有操作
    // 在列表左侧插入元素
    size_t lpush(const Value& value);
    // 在列表右侧插入元素
    size_t rpush(const Value& value);
    // 移除并返回列表左侧元素
    bool lpop(Value& value);
    // 移除并返回列表右侧元素
    bool rpop(Value& value);
    // 获取列表长度
    size_t size() const;
    // 获取列表指定范围的元素
    std::vector<Value> lrange(size_t start, size_t stop) const;
    // 清空列表
    void clear();
    // 判断列表是否为空
    bool empty() const;
};

} // namespace dkv

#endif // DKV_DATATYPE_LIST_HPP