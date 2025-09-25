#include "dkv_datatype_list.hpp"
#include "dkv_utils.hpp"
#include <sstream>
#include <algorithm>

namespace dkv {

// ListItem 实现
ListItem::ListItem() 
    : DataItem() {
}

ListItem::ListItem(Timestamp expire_time)
    : DataItem(expire_time) {
}

DataType ListItem::getType() const {
    return DataType::LIST;
}

std::string ListItem::serialize() const {
    std::ostringstream oss;
    oss << "LIST:" << elements_.size() << ":";
    
    for (const auto& element : elements_) {
        oss << element.length() << ":" << element << ":";
    }
    
    if (has_expiration_) {
        auto duration = expire_time_.time_since_epoch();
        auto seconds = std::chrono::duration_cast<std::chrono::seconds>(duration).count();
        oss << "E:" << seconds;
    }
    
    return oss.str();
}

void ListItem::deserialize(const std::string& data) {
    std::istringstream iss(data);
    std::string type, size_str;
    
    if (std::getline(iss, type, ':') && type == "LIST" &&
        std::getline(iss, size_str, ':')) {
        
        size_t size = std::stoul(size_str);
        elements_.clear();
        
        for (size_t i = 0; i < size; ++i) {
            std::string len_str, element;
            if (std::getline(iss, len_str, ':') &&
                std::getline(iss, element, ':')) {
                
                size_t len = std::stoul(len_str);
                elements_.push_back(element.substr(0, len));
            }
        }
        
        // 检查是否有过期时间
        std::string next_part;
        if (std::getline(iss, next_part)) {
            if (next_part.substr(0, 2) == "E:") {
                int64_t seconds = std::stoll(next_part.substr(2));
                expire_time_ = Timestamp(std::chrono::seconds(seconds));
                has_expiration_ = true;
            }
        }
    }
}

// 列表特有操作实现

size_t ListItem::lpush(const Value& value) {
    elements_.insert(elements_.begin(), value);
    return elements_.size();
}

size_t ListItem::rpush(const Value& value) {
    elements_.push_back(value);
    return elements_.size();
}

bool ListItem::lpop(Value& value) {
    if (elements_.empty()) {
        return false;
    }
    value = elements_.front();
    elements_.erase(elements_.begin());
    return true;
}

bool ListItem::rpop(Value& value) {
    if (elements_.empty()) {
        return false;
    }
    value = elements_.back();
    elements_.pop_back();
    return true;
}

size_t ListItem::size() const {
    return elements_.size();
}

std::vector<Value> ListItem::lrange(size_t start, size_t stop) const {
    std::vector<Value> result;
    
    if (elements_.empty()) {
        return result;
    }
    
    // 处理负数索引（Redis风格）
    if (stop >= elements_.size()) {
        stop = elements_.size() - 1;
    }
    
    if (start > stop) {
        return result;
    }
    
    // 预分配结果空间
    result.reserve(stop - start + 1);
    
    // 使用迭代器遍历指定范围的元素
    auto it = elements_.begin();
    
    // 移动到起始位置
    std::advance(it, start);
    
    // 收集范围内的元素
    size_t count = 0;
    while (it != elements_.end() && count <= (stop - start)) {
        result.push_back(*it);
        ++it;
        ++count;
    }
    
    return result;
}

void ListItem::clear() {
    elements_.clear();
}

bool ListItem::empty() const {
    return elements_.empty();
}

} // namespace dkv