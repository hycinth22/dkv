#include "dkv_datatype_string.hpp"
#include "dkv_utils.hpp"
#include <sstream>
#include <algorithm>
#include <cctype>

namespace dkv {

// StringItem 实现
StringItem::StringItem(const Value& value) 
    : value_(value), DataItem() {
}

StringItem::StringItem(const Value& value, Timestamp expire_time)
    : value_(value), DataItem(expire_time) {
}

DataType StringItem::getType() const {
    return DataType::STRING;
}

std::string StringItem::serialize() const {
    std::ostringstream oss;
    oss << "STRING:" << value_.length() << ":" << value_;
    if (has_expiration_) {
        auto duration = expire_time_.time_since_epoch();
        auto seconds = std::chrono::duration_cast<std::chrono::seconds>(duration).count();
        oss << ":" << seconds;
    }
    return oss.str();
}

void StringItem::deserialize(const std::string& data) {
    std::istringstream iss(data);
    std::string type, length_str, value_str;
    
    if (std::getline(iss, type, ':') && type == "STRING" &&
        std::getline(iss, length_str, ':') &&
        std::getline(iss, value_str, ':')) {
        
        size_t length = std::stoul(length_str);
        value_ = value_str.substr(0, length);
        
        // 检查是否有过期时间
        std::string expire_str;
        if (std::getline(iss, expire_str)) {
            int64_t seconds = std::stoll(expire_str);
            expire_time_ = Timestamp(std::chrono::seconds(seconds));
            has_expiration_ = true;
        } else {
            has_expiration_ = false;
        }
    }
}

const Value& StringItem::getValue() const {
    return value_;
}

void StringItem::setValue(const Value& value) {
    value_ = value;
}

} // namespace dkv