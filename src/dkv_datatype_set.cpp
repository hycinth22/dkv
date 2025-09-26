#include "dkv_datatype_set.hpp"
#include <sstream>
#include <algorithm>

namespace dkv {

SetItem::SetItem() : DataItem() {
}

SetItem::SetItem(Timestamp expire_time) : DataItem(expire_time) {
}

DataType SetItem::getType() const {
    return DataType::SET;
}

std::string SetItem::serialize() const {
    std::stringstream ss;
    
    // 序列化过期信息
    ss << (hasExpiration() ? "1" : "0") << "\n";
    if (hasExpiration()) {
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(
            expire_time_.load().time_since_epoch());
        ss << duration.count() << "\n";
    }
    
    // 序列化集合大小
    ss << elements_.size() << "\n";
    
    // 序列化每个元素
    for (const auto& element : elements_) {
        ss << element.size() << "\n" << element << "\n";
    }
    
    return ss.str();
}

void SetItem::deserialize(const std::string& data) {
    std::stringstream ss(data);
    std::string line;
    
    // 反序列化过期信息
    std::getline(ss, line);
    bool has_exp = (line == "1");
    
    if (has_exp) {
        std::getline(ss, line);
        uint64_t ms = std::stoull(line);
        setExpiration(Timestamp(std::chrono::milliseconds(ms)));
    }
    
    // 清空现有元素
    elements_.clear();
    
    // 反序列化集合大小
    std::getline(ss, line);
    size_t size = std::stoull(line);
    
    // 反序列化每个元素
    for (size_t i = 0; i < size; ++i) {
        // 读取元素大小
        std::getline(ss, line);
        size_t element_size = std::stoull(line);
        
        // 读取元素内容
        std::string element;
        element.resize(element_size);
        ss.read(&element[0], element_size);
        
        // 跳过换行符
        ss.ignore();
        
        elements_.insert(element);
    }
}

bool SetItem::sadd(const Value& member) {
    // 插入元素，如果元素已经存在则返回false
    auto [it, inserted] = elements_.insert(member);
    return inserted;
}

size_t SetItem::sadd(const std::vector<Value>& members) {
    // 添加多个元素，返回成功添加的个数（不含已有）
    size_t added_count = 0;
    for (const auto& member : members) {
        if (sadd(member)) {
            added_count++;
        }
    }
    return added_count;
}

bool SetItem::srem(const Value& member) {
    // 删除元素，如果元素不存在则返回false
    return elements_.erase(member) > 0;
}

size_t SetItem::srem(const std::vector<Value>& members) {
    // 删除多个元素，返回成功删除的个数（不含不存在）
    size_t removed_count = 0;
    for (const auto& member : members) {
        if (srem(member)) {
            removed_count++;
        }
    }
    return removed_count;
}

std::vector<Value> SetItem::smembers() const {
    std::vector<Value> members;
    members.reserve(elements_.size());
    for (const auto& element : elements_) {
        members.push_back(element);
    }
    return members;
}

bool SetItem::sismember(const Value& member) const {
    return elements_.count(member) > 0;
}

size_t SetItem::scard() const {
    return elements_.size();
}

void SetItem::clear() {
    elements_.clear();
}

bool SetItem::empty() const {
    return elements_.empty();
}

} // namespace dkv