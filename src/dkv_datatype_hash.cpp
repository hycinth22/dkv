#include "dkv_datatype_hash.hpp"
#include "dkv_utils.hpp"
#include <sstream>
#include <algorithm>
#include <cctype>

namespace dkv {

// HashItem 实现
HashItem::HashItem() 
    : DataItem() {
}

HashItem::HashItem(Timestamp expire_time)
    : DataItem(expire_time) {
}

DataType HashItem::getType() const {
    return DataType::HASH;
}

std::string HashItem::serialize() const {
    std::ostringstream oss;
    oss << "HASH:" << fields_.size() << ":";
    
    for (const auto& pair : fields_) {
        oss << pair.first.length() << ":" << pair.first << ":";
        oss << pair.second.length() << ":" << pair.second << ":";
    }
    
    if (hasExpiration()) {
        auto duration = expire_time_.load().time_since_epoch();
        auto seconds = std::chrono::duration_cast<std::chrono::seconds>(duration).count();
        oss << "E:" << seconds;
    }
    
    return oss.str();
}

void HashItem::deserialize(const std::string& data) {
    std::istringstream iss(data);
    std::string type, size_str;
    
    if (std::getline(iss, type, ':') && type == "HASH" &&
        std::getline(iss, size_str, ':')) {
        
        size_t size = std::stoul(size_str);
        
        for (size_t i = 0; i < size; ++i) {
            std::string field_len_str, field, value_len_str, value;
            if (std::getline(iss, field_len_str, ':') &&
                std::getline(iss, field, ':') &&
                std::getline(iss, value_len_str, ':') &&
                std::getline(iss, value, ':')) {
                
                size_t field_len = std::stoul(field_len_str);
                size_t value_len = std::stoul(value_len_str);
                fields_[field.substr(0, field_len)] = value.substr(0, value_len);
            }
        }
        
        // 检查是否有过期时间
        std::string next_part;
        if (std::getline(iss, next_part)) {
            if (next_part.substr(0, 2) == "E:") {
                int64_t seconds = std::stoll(next_part.substr(2));
                expire_time_ = Timestamp(std::chrono::seconds(seconds));
                setExpiration(expire_time_);
            }
        }
    }
}

bool HashItem::setField(const Value& field, const Value& value) {
    fields_[field] = value;
    return true;
}

bool HashItem::getField(const Value& field, Value& value) const {
    auto it = fields_.find(field);
    if (it != fields_.end()) {
        value = it->second;
        return true;
    }
    return false;
}

bool HashItem::delField(const Value& field) {
    auto it = fields_.find(field);
    if (it != fields_.end()) {
        fields_.erase(it);
        return true;
    }
    return false;
}

bool HashItem::existsField(const Value& field) const {
    return fields_.find(field) != fields_.end();
}

std::vector<Value> HashItem::getKeys() const {
    std::vector<Value> keys;
    keys.reserve(fields_.size());
    for (const auto& pair : fields_) {
        keys.push_back(pair.first);
    }
    return keys;
}

std::vector<Value> HashItem::getValues() const {
    std::vector<Value> values;
    values.reserve(fields_.size());
    for (const auto& pair : fields_) {
        values.push_back(pair.second);
    }
    return values;
}

std::vector<std::pair<Value, Value>> HashItem::getAll() const {
    std::vector<std::pair<Value, Value>> all;
    all.reserve(fields_.size());
    for (const auto& pair : fields_) {
        all.push_back(pair);
    }
    return all;
}

size_t HashItem::size() const {
    return fields_.size();
}

void HashItem::clear() {
    fields_.clear();
}

} // namespace dkv