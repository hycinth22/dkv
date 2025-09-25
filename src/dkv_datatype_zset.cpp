#include "dkv_datatype_zset.hpp"
#include "dkv_utils.hpp"
#include <sstream>
#include <algorithm>
#include <cmath>

namespace dkv {

ZSetItem::ZSetItem() : DataItem() {
}

ZSetItem::ZSetItem(Timestamp expire_time) : DataItem(expire_time) {
}

DataType ZSetItem::getType() const {
    return DataType::ZSET;
}

std::string ZSetItem::serialize() const {
    std::stringstream ss;
    
    // 序列化过期信息
    ss << has_expiration_ << "\n";
    if (has_expiration_) {
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(
            expire_time_.time_since_epoch());
        ss << duration.count() << "\n";
    }
    
    // 序列化元素数量
    ss << scores_.size() << "\n";
    
    // 序列化每个元素及其分数
    for (const auto& pair : scores_) {
        ss << pair.first.size() << "\n" << pair.first << "\n";
        ss << pair.second << "\n";
    }
    
    return ss.str();
}

void ZSetItem::deserialize(const std::string& data) {
    std::stringstream ss(data);
    std::string line;
    
    // 反序列化过期信息
    std::getline(ss, line);
    has_expiration_ = (line == "1");
    
    if (has_expiration_) {
        std::getline(ss, line);
        uint64_t ms = std::stoull(line);
        expire_time_ = Timestamp(std::chrono::milliseconds(ms));
    }
    
    // 清空现有元素
    elements_by_score_.clear();
    scores_.clear();
    
    // 反序列化元素数量
    std::getline(ss, line);
    size_t size = std::stoull(line);
    
    // 反序列化每个元素及其分数
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
        
        // 读取分数
        std::getline(ss, line);
        double score = std::stod(line);
        
        // 添加到数据结构中
        zadd(element, score);
    }
}

bool ZSetItem::zadd(const Value& member, double score) {
    // 检查元素是否已存在
    bool updated = false;
    auto it = scores_.find(member);
    if (it != scores_.end()) {
        // 如果分数不同，先从旧分数集合中移除
        if (std::abs(it->second - score) > 1e-9) {
            elements_by_score_[it->second].erase(member);
            if (elements_by_score_[it->second].empty()) {
                elements_by_score_.erase(it->second);
            }
            updated = true;
        } else {
            return false; // 分数相同，不需要更新
        }
    } else {
        updated = true;
    }
    
    // 添加到新分数集合中
    elements_by_score_[score].insert(member);
    scores_[member] = score;
    return updated;
}

size_t ZSetItem::zadd(const std::vector<std::pair<Value, double>>& members_with_scores) {
    size_t updated_count = 0;
    for (const auto& pair : members_with_scores) {
        if (zadd(pair.first, pair.second)) {
            updated_count++;
        }
    }
    return updated_count;
}

bool ZSetItem::zrem(const Value& member) {
    auto it = scores_.find(member);
    if (it != scores_.end()) {
        double score = it->second;
        elements_by_score_[score].erase(member);
        if (elements_by_score_[score].empty()) {
            elements_by_score_.erase(score);
        }
        scores_.erase(it);
        return true;
    }
    return false;
}

size_t ZSetItem::zrem(const std::vector<Value>& members) {
    size_t removed_count = 0;
    for (const auto& member : members) {
        if (zrem(member)) {
            removed_count++;
        }
    }
    return removed_count;
}

bool ZSetItem::zscore(const Value& member, double& score) const {
    auto it = scores_.find(member);
    if (it != scores_.end()) {
        score = it->second;
        return true;
    }
    return false;
}

bool ZSetItem::zismember(const Value& member) const {
    return scores_.count(member) > 0;
}

bool ZSetItem::zrank(const Value& member, size_t& rank) const {
    auto it = scores_.find(member);
    if (it == scores_.end()) {
        return false;
    }
    
    double target_score = it->second;
    size_t count = 0;
    
    // 计算排名（从小到大）
    for (const auto& score_group : elements_by_score_) {
        if (score_group.first == target_score) {
            // 找到相同分数的组，计算组内排名
            for (const auto& m : score_group.second) {
                if (m == member) {
                    rank = count;
                    return true;
                }
                count++;
            }
        } else if (score_group.first < target_score) {
            count += score_group.second.size();
        } else {
            // 已经超过目标分数，可以退出循环
            break;
        }
    }
    
    return false;
}

bool ZSetItem::zrevrank(const Value& member, size_t& rank) const {
    auto it = scores_.find(member);
    if (it == scores_.end()) {
        return false;
    }
    
    double target_score = it->second;
    size_t count = 0;
    
    // 计算逆序排名（从大到小）
    for (auto it_score = elements_by_score_.rbegin(); it_score != elements_by_score_.rend(); ++it_score) {
        if (it_score->first == target_score) {
            // 找到相同分数的组，计算组内排名
            for (const auto& m : it_score->second) {
                if (m == member) {
                    rank = count;
                    return true;
                }
                count++;
            }
        } else if (it_score->first > target_score) {
            count += it_score->second.size();
        } else {
            // 已经小于目标分数，可以退出循环
            break;
        }
    }
    
    return false;
}

std::vector<std::pair<Value, double>> ZSetItem::zrange(size_t start, size_t stop) const {
    std::vector<std::pair<Value, double>> result;
    size_t count = 0;
    
    // 从前往后遍历（从小到大）
    for (const auto& score_group : elements_by_score_) {
        for (const auto& member : score_group.second) {
            if (count >= start && count <= stop) {
                result.push_back({member, score_group.first});
            }
            count++;
            if (count > stop) {
                return result;
            }
        }
    }
    
    return result;
}

std::vector<std::pair<Value, double>> ZSetItem::zrevrange(size_t start, size_t stop) const {
    std::vector<std::pair<Value, double>> result;
    size_t count = 0;
    
    // 从后往前遍历（从大到小）
    for (auto it_score = elements_by_score_.rbegin(); it_score != elements_by_score_.rend(); ++it_score) {
        for (const auto& member : it_score->second) {
            if (count >= start && count <= stop) {
                result.push_back({member, it_score->first});
            }
            count++;
            if (count > stop) {
                return result;
            }
        }
    }
    
    return result;
}

std::vector<std::pair<Value, double>> ZSetItem::zrangebyscore(double min, double max) const {
    std::vector<std::pair<Value, double>> result;
    
    // 遍历分数在[min, max]范围内的元素
    for (const auto& score_group : elements_by_score_) {
        if (score_group.first > max) {
            break; // 由于map是有序的，后面的分数更大，可以退出循环
        }
        if (score_group.first >= min) {
            for (const auto& member : score_group.second) {
                result.push_back({member, score_group.first});
            }
        }
    }
    
    return result;
}

std::vector<std::pair<Value, double>> ZSetItem::zrevrangebyscore(double max, double min) const {
    std::vector<std::pair<Value, double>> result;
    
    // 遍历分数在[min, max]范围内的元素（从大到小）
    for (auto it_score = elements_by_score_.rbegin(); it_score != elements_by_score_.rend(); ++it_score) {
        if (it_score->first < min) {
            break; // 由于map是有序的，后面的分数更小，可以退出循环
        }
        if (it_score->first <= max) {
            for (const auto& member : it_score->second) {
                result.push_back({member, it_score->first});
            }
        }
    }
    
    return result;
}

size_t ZSetItem::zcount(double min, double max) const {
    size_t count = 0;
    
    // 统计分数在[min, max]范围内的元素个数
    for (const auto& score_group : elements_by_score_) {
        if (score_group.first > max) {
            break;
        }
        if (score_group.first >= min) {
            count += score_group.second.size();
        }
    }
    
    return count;
}

size_t ZSetItem::zcard() const {
    return scores_.size();
}

void ZSetItem::clear() {
    elements_by_score_.clear();
    scores_.clear();
}

bool ZSetItem::empty() const {
    return scores_.empty();
}

} // namespace dkv