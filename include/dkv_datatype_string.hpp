#pragma once

#include "dkv_datatype_base.hpp"

namespace dkv {

// 字符串数据项
class StringItem : public DataItem {
private:
    Value value_;
    Timestamp expire_time_;
    bool has_expiration_;

public:
    explicit StringItem(const Value& value = "");
    StringItem(const Value& value, Timestamp expire_time);

    // 从DataItem继承的方法
    DataType getType() const override;
    std::string serialize() const override;
    void deserialize(const std::string& data) override;
    bool isExpired() const override;
    void setExpiration(Timestamp expire_time) override;
    Timestamp getExpiration() const override;
    bool hasExpiration() const;

    // String特有操作
    const Value& getValue() const;
    void setValue(const Value& value);
};

} // namespace dkv