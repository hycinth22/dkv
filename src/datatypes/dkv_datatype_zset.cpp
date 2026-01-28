#include "datatypes/dkv_datatype_zset.hpp"
#include "dkv_utils.hpp"
#include <sstream>
#include <algorithm>
#include <cmath>
#include <memory>
#include <cfloat>

namespace dkv {

// 全局配置：zset实现类型
ZSetImplType g_zset_impl_type = ZSetImplType::MAP;

// 设置zset实现类型
void setZSetImplType(ZSetImplType type) {
    g_zset_impl_type = type;
}

// 获取zset实现类型
ZSetImplType getZSetImplType() {
    return g_zset_impl_type;
}

// 从配置字符串设置zset实现类型
void setZSetImplTypeFromConfig(const std::string& config_value) {
    std::string lower_value = config_value;
    std::transform(lower_value.begin(), lower_value.end(), lower_value.begin(), ::tolower);
    
    if (lower_value == "map" || lower_value == "redblack" || lower_value == "rbtree") {
        setZSetImplType(ZSetImplType::MAP);
    } else if (lower_value == "skiplist" || lower_value == "skip_list") {
        setZSetImplType(ZSetImplType::SKIPLIST);
    }
}

// 创建zset实现实例
std::unique_ptr<IZSetImpl> createZSetImpl(ZSetImplType type) {
    switch (type) {
        case ZSetImplType::MAP:
            return std::make_unique<MapZSetImpl>();
        case ZSetImplType::SKIPLIST:
            return std::make_unique<SkiplistZSetImpl>();
        default:
            return std::make_unique<MapZSetImpl>(); // 默认使用map实现
    }
}

// MapZSetImpl实现
bool MapZSetImpl::insert(const Value& member, double score) {
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

bool MapZSetImpl::remove(const Value& member) {
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

bool MapZSetImpl::getScore(const Value& member, double& score) const {
    auto it = scores_.find(member);
    if (it != scores_.end()) {
        score = it->second;
        return true;
    }
    return false;
}

bool MapZSetImpl::contains(const Value& member) const {
    return scores_.count(member) > 0;
}

bool MapZSetImpl::getRank(const Value& member, size_t& rank) const {
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

bool MapZSetImpl::getRevRank(const Value& member, size_t& rank) const {
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

std::vector<std::pair<Value, double>> MapZSetImpl::rangeByRank(size_t start, size_t stop) const {
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

std::vector<std::pair<Value, double>> MapZSetImpl::revRangeByRank(size_t start, size_t stop) const {
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

std::vector<std::pair<Value, double>> MapZSetImpl::rangeByScore(double min, double max) const {
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

std::vector<std::pair<Value, double>> MapZSetImpl::revRangeByScore(double max, double min) const {
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

size_t MapZSetImpl::countByScore(double min, double max) const {
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

size_t MapZSetImpl::size() const {
    return scores_.size();
}

void MapZSetImpl::clear() {
    elements_by_score_.clear();
    scores_.clear();
}

bool MapZSetImpl::empty() const {
    return scores_.empty();
}

// SkiplistNode实现
SkiplistNode::SkiplistNode(const Value& member, double score, int level)
    : member(member), score(score), level(level) {
    forward = new SkiplistNode*[level + 1];
    for (int i = 0; i <= level; ++i) {
        forward[i] = nullptr;
    }
}

SkiplistNode::~SkiplistNode() {
    delete[] forward;
}

// Skiplist实现
template <typename Value>
Skiplist<Value>::Skiplist() 
    : head_(createNode("", -DBL_MAX, MAX_LEVEL)), 
      tail_(createNode("", DBL_MAX, MAX_LEVEL)), 
      level_(1), 
      size_(0),
      gen_(rd_()),
      dis_(0.0, 1.0) {
    // 初始化头节点的forward指针，指向尾节点
    for (int i = 0; i <= MAX_LEVEL; ++i) {
        head_->forward[i] = tail_;
    }
}

template <typename Value>
Skiplist<Value>::~Skiplist() {
    clear();
    delete head_;
    delete tail_;
}

template <typename Value>
int Skiplist<Value>::randomLevel() {
    int level = 1;
    while (dis_(gen_) < P && level < MAX_LEVEL) {
        level++;
    }
    return level;
}

template <typename Value>
SkiplistNode* Skiplist<Value>::createNode(const Value& member, double score, int level) {
    return new SkiplistNode(member, score, level);
}

template <typename Value>
bool Skiplist<Value>::insert(const Value& member, double score) {
    SkiplistNode* update[MAX_LEVEL + 1];
    SkiplistNode* x = head_;
    
    // 从最高层开始查找
    for (int i = level_; i >= 0; --i) {
        while (x->forward[i] != tail_ && (x->forward[i]->score < score || 
              (x->forward[i]->score == score && x->forward[i]->member < member))) {
            x = x->forward[i];
        }
        update[i] = x;
    }
    
    x = x->forward[0];
    
    // 如果元素已存在，更新分数
    if (x != tail_ && x->member == member) {
        return updateScore(member, score);
    }
    
    // 插入新节点
    int new_level = randomLevel();
    if (new_level > level_) {
        for (int i = level_ + 1; i <= new_level; ++i) {
            update[i] = head_;
        }
        level_ = new_level;
    }
    
    SkiplistNode* new_node = createNode(member, score, new_level);
    for (int i = 0; i <= new_level; ++i) {
        new_node->forward[i] = update[i]->forward[i];
        update[i]->forward[i] = new_node;
    }
    
    size_++;
    return true;
}

template <typename Value>
bool Skiplist<Value>::remove(const Value& member) {
    SkiplistNode* update[MAX_LEVEL + 1];
    SkiplistNode* x = head_;
    
    // 从最高层开始查找
    for (int i = level_; i >= 0; --i) {
        while (x->forward[i] != tail_ && (x->forward[i]->score < x->forward[i]->score || 
              (x->forward[i]->score == x->forward[i]->score && x->forward[i]->member < member))) {
            x = x->forward[i];
        }
        update[i] = x;
    }
    
    x = x->forward[0];
    
    // 如果元素不存在，返回false
    if (x == tail_ || x->member != member) {
        return false;
    }
    
    // 删除节点
    for (int i = 0; i <= level_; ++i) {
        if (update[i]->forward[i] != x) {
            break;
        }
        update[i]->forward[i] = x->forward[i];
    }
    
    // 更新跳表的最大层数
    while (level_ > 1 && head_->forward[level_] == tail_) {
        level_--;
    }
    
    delete x;
    size_--;
    return true;
}

template <typename Value>
bool Skiplist<Value>::updateScore(const Value& member, double new_score) {
    // 先删除元素，再插入新分数
    if (remove(member)) {
        return insert(member, new_score);
    }
    return false;
}

template <typename Value>
bool Skiplist<Value>::getScore(const Value& member, double& score) const {
    SkiplistNode* x = head_;
    
    // 从最高层开始查找
    for (int i = level_; i >= 0; --i) {
        while (x->forward[i] != tail_ && (x->forward[i]->score < x->forward[i]->score || 
              (x->forward[i]->score == x->forward[i]->score && x->forward[i]->member < member))) {
            x = x->forward[i];
        }
    }
    
    x = x->forward[0];
    
    // 如果元素存在，返回分数
    if (x != tail_ && x->member == member) {
        score = x->score;
        return true;
    }
    
    return false;
}

template <typename Value>
bool Skiplist<Value>::contains(const Value& member) const {
    double score;
    return getScore(member, score);
}

template <typename Value>
bool Skiplist<Value>::getRank(const Value& member, size_t& rank) const {
    SkiplistNode* x = head_;
    rank = 0;
    
    // 从最高层开始查找
    for (int i = level_; i >= 0; --i) {
        while (x->forward[i] != tail_ && (x->forward[i]->score < x->forward[i]->score || 
              (x->forward[i]->score == x->forward[i]->score && x->forward[i]->member < member))) {
            rank += 1;
            x = x->forward[i];
        }
    }
    
    x = x->forward[0];
    
    // 如果元素存在，返回排名
    if (x != tail_ && x->member == member) {
        return true;
    }
    
    return false;
}

template <typename Value>
bool Skiplist<Value>::getRevRank(const Value& member, size_t& rank) const {
    SkiplistNode* x = head_;
    size_t total = 0;
    
    // 从最高层开始查找
    for (int i = level_; i >= 0; --i) {
        while (x->forward[i] != tail_) {
            x = x->forward[i];
            total += 1;
        }
    }
    
    // 重新查找元素并计算逆序排名
    x = head_;
    rank = 0;
    for (int i = level_; i >= 0; --i) {
        while (x->forward[i] != tail_ && (x->forward[i]->score < x->forward[i]->score || 
              (x->forward[i]->score == x->forward[i]->score && x->forward[i]->member < member))) {
            rank += 1;
            x = x->forward[i];
        }
    }
    
    x = x->forward[0];
    
    // 如果元素存在，返回逆序排名
    if (x != tail_ && x->member == member) {
        rank = total - rank - 1;
        return true;
    }
    
    return false;
}

template <typename Value>
std::vector<std::pair<Value, double>> Skiplist<Value>::rangeByRank(size_t start, size_t stop) const {
    std::vector<std::pair<Value, double>> result;
    SkiplistNode* x = head_->forward[0];
    size_t current_rank = 0;
    
    // 找到起始位置
    while (x != tail_ && current_rank < start) {
        x = x->forward[0];
        current_rank++;
    }
    
    // 收集结果
    while (x != tail_ && current_rank <= stop) {
        result.push_back({x->member, x->score});
        x = x->forward[0];
        current_rank++;
    }
    
    return result;
}

template <typename Value>
std::vector<std::pair<Value, double>> Skiplist<Value>::revRangeByRank(size_t start, size_t stop) const {
    // 先获取所有元素，再反转
    std::vector<std::pair<Value, double>> all_elements;
    SkiplistNode* x = head_->forward[0];
    
    while (x != tail_) {
        all_elements.push_back({x->member, x->score});
        x = x->forward[0];
    }
    
    // 反转
    std::reverse(all_elements.begin(), all_elements.end());
    
    // 截取指定范围
    std::vector<std::pair<Value, double>> result;
    for (size_t i = start; i <= stop && i < all_elements.size(); ++i) {
        result.push_back(all_elements[i]);
    }
    
    return result;
}

template <typename Value>
std::vector<std::pair<Value, double>> Skiplist<Value>::rangeByScore(double min, double max) const {
    std::vector<std::pair<Value, double>> result;
    SkiplistNode* x = head_;
    
    // 从最高层开始查找
    for (int i = level_; i >= 0; --i) {
        while (x->forward[i] != tail_ && x->forward[i]->score < min) {
            x = x->forward[i];
        }
    }
    
    x = x->forward[0];
    
    // 收集分数在[min, max]范围内的元素
    while (x != tail_ && x->score <= max) {
        result.push_back({x->member, x->score});
        x = x->forward[0];
    }
    
    return result;
}

template <typename Value>
std::vector<std::pair<Value, double>> Skiplist<Value>::revRangeByScore(double max, double min) const {
    // 先获取正序范围，再反转
    std::vector<std::pair<Value, double>> result = rangeByScore(min, max);
    std::reverse(result.begin(), result.end());
    return result;
}

template <typename Value>
size_t Skiplist<Value>::countByScore(double min, double max) const {
    size_t count = 0;
    SkiplistNode* x = head_;
    
    // 从最高层开始查找
    for (int i = level_; i >= 0; --i) {
        while (x->forward[i] != tail_ && x->forward[i]->score < min) {
            x = x->forward[i];
        }
    }
    
    x = x->forward[0];
    
    // 计数分数在[min, max]范围内的元素
    while (x != tail_ && x->score <= max) {
        count++;
        x = x->forward[0];
    }
    
    return count;
}

template <typename Value>
size_t Skiplist<Value>::size() const {
    return size_;
}

template <typename Value>
void Skiplist<Value>::clear() {
    SkiplistNode* x = head_->forward[0];
    while (x != tail_) {
        SkiplistNode* next = x->forward[0];
        delete x;
        x = next;
    }
    
    // 重置头节点的指针
    for (int i = 0; i <= level_; ++i) {
        head_->forward[i] = tail_;
    }
    
    level_ = 1;
    size_ = 0;
}

template <typename Value>
bool Skiplist<Value>::empty() const {
    return size_ == 0;
}

// 显式实例化Skiplist类
template class Skiplist<Value>;

// SkiplistZSetImpl实现
bool SkiplistZSetImpl::insert(const Value& member, double score) {
    return skiplist_.insert(member, score);
}

bool SkiplistZSetImpl::remove(const Value& member) {
    return skiplist_.remove(member);
}

bool SkiplistZSetImpl::getScore(const Value& member, double& score) const {
    return skiplist_.getScore(member, score);
}

bool SkiplistZSetImpl::contains(const Value& member) const {
    return skiplist_.contains(member);
}

bool SkiplistZSetImpl::getRank(const Value& member, size_t& rank) const {
    return skiplist_.getRank(member, rank);
}

bool SkiplistZSetImpl::getRevRank(const Value& member, size_t& rank) const {
    return skiplist_.getRevRank(member, rank);
}

std::vector<std::pair<Value, double>> SkiplistZSetImpl::rangeByRank(size_t start, size_t stop) const {
    return skiplist_.rangeByRank(start, stop);
}

std::vector<std::pair<Value, double>> SkiplistZSetImpl::revRangeByRank(size_t start, size_t stop) const {
    return skiplist_.revRangeByRank(start, stop);
}

std::vector<std::pair<Value, double>> SkiplistZSetImpl::rangeByScore(double min, double max) const {
    return skiplist_.rangeByScore(min, max);
}

std::vector<std::pair<Value, double>> SkiplistZSetImpl::revRangeByScore(double max, double min) const {
    return skiplist_.revRangeByScore(max, min);
}

size_t SkiplistZSetImpl::countByScore(double min, double max) const {
    return skiplist_.countByScore(min, max);
}

size_t SkiplistZSetImpl::size() const {
    return skiplist_.size();
}

void SkiplistZSetImpl::clear() {
    skiplist_.clear();
}

bool SkiplistZSetImpl::empty() const {
    return skiplist_.empty();
}

// ZSetItem实现
ZSetItem::ZSetItem() : DataItem() {
    // 创建默认的zset实现
    impl_ = createZSetImpl(getZSetImplType());
}

ZSetItem::ZSetItem(Timestamp expire_time) : DataItem(expire_time) {
    // 创建默认的zset实现
    impl_ = createZSetImpl(getZSetImplType());
}

ZSetItem::ZSetItem(const ZSetItem& other)
    : DataItem(other) {
    // 创建新的zset实现实例
    impl_ = createZSetImpl(getZSetImplType());
    
    // 从原实现中获取所有元素
    auto all_elements = other.impl_->rangeByRank(0, other.zcard() - 1);
    
    // 重新插入所有元素到新的实现中
    for (const auto& pair : all_elements) {
        impl_->insert(pair.first, pair.second);
    }
}

std::unique_ptr<DataItem> ZSetItem::clone() const {
    auto cloned = std::make_unique<ZSetItem>(*this);
    return cloned;
}

DataType ZSetItem::getType() const {
    return DataType::ZSET;
}

std::string ZSetItem::serialize() const {
    std::stringstream ss;
    
    // 序列化过期信息
    ss << hasExpiration() << "\n";
    if (hasExpiration()) {
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(
            getExpiration().time_since_epoch());
        ss << duration.count() << "\n";
    }
    
    // 获取所有元素
    auto all_elements = impl_->rangeByRank(0, zcard() - 1);
    
    // 序列化元素数量
    ss << all_elements.size() << "\n";
    
    // 序列化每个元素及其分数
    for (const auto& pair : all_elements) {
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
    bool has_expiration = (line == "1");
    if (has_expiration) {
        std::getline(ss, line);
        uint64_t ms = std::stoull(line);
        setExpiration(Timestamp(std::chrono::milliseconds(ms)));
    }
    
    // 清空现有元素
    impl_->clear();
    
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
        impl_->insert(element, score);
    }
}

bool ZSetItem::zadd(const Value& member, double score) {
    return impl_->insert(member, score);
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
    return impl_->remove(member);
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
    return impl_->getScore(member, score);
}

bool ZSetItem::zismember(const Value& member) const {
    return impl_->contains(member);
}

bool ZSetItem::zrank(const Value& member, size_t& rank) const {
    return impl_->getRank(member, rank);
}

bool ZSetItem::zrevrank(const Value& member, size_t& rank) const {
    return impl_->getRevRank(member, rank);
}

std::vector<std::pair<Value, double>> ZSetItem::zrange(size_t start, size_t stop) const {
    return impl_->rangeByRank(start, stop);
}

std::vector<std::pair<Value, double>> ZSetItem::zrevrange(size_t start, size_t stop) const {
    return impl_->revRangeByRank(start, stop);
}

std::vector<std::pair<Value, double>> ZSetItem::zrangebyscore(double min, double max) const {
    return impl_->rangeByScore(min, max);
}

std::vector<std::pair<Value, double>> ZSetItem::zrevrangebyscore(double max, double min) const {
    return impl_->revRangeByScore(max, min);
}

size_t ZSetItem::zcount(double min, double max) const {
    return impl_->countByScore(min, max);
}

size_t ZSetItem::zcard() const {
    return impl_->size();
}

void ZSetItem::clear() {
    impl_->clear();
}

bool ZSetItem::empty() const {
    return impl_->empty();
}

} // namespace dkv