#pragma once

#include "dkv_datatype_base.hpp"
#include <unordered_map>
#include <vector>

namespace dkv {

// 哈希数据项
class HashItem : public DataItem {
private:
    std::unordered_map<Value, Value> fields_;  // 字段-值映射
    Timestamp expire_time_;
    bool has_expiration_;

public:
    HashItem();
    HashItem(Timestamp expire_time);
    
    // 从DataItem继承的方法
    DataType getType() const override;
    std::string serialize() const override;
    void deserialize(const std::string& data) override;
    
    // 哈希特有操作
    bool setField(const Value& field, const Value& value);
    bool getField(const Value& field, Value& value) const;
    bool delField(const Value& field);
    bool existsField(const Value& field) const;
    std::vector<Value> getKeys() const;
    std::vector<Value> getValues() const;
    std::vector<std::pair<Value, Value>> getAll() const;
    size_t size() const;
    void clear();
};

} // namespace dkv