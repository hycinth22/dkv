#ifndef DKV_DATATYPE_ZSET_HPP
#define DKV_DATATYPE_ZSET_HPP

#include "dkv_datatype_base.hpp"
#include <unordered_map>
#include <unordered_set>
#include <map>
#include <vector>
#include <string>
#include <algorithm>
#include <random>
#include <memory>

namespace dkv {

// Forward declaration for SkiplistNode
class SkiplistNode;

// ZSet实现的公共接口
class IZSetImpl {
public:
    virtual ~IZSetImpl() = default;
    
    // 插入元素
    virtual bool insert(const Value& member, double score) = 0;
    
    // 删除元素
    virtual bool remove(const Value& member) = 0;
    
    // 获取元素的分数
    virtual bool getScore(const Value& member, double& score) const = 0;
    
    // 判断元素是否存在
    virtual bool contains(const Value& member) const = 0;
    
    // 获取元素的排名（从小到大，从0开始）
    virtual bool getRank(const Value& member, size_t& rank) const = 0;
    
    // 获取元素的逆序排名（从大到小，从0开始）
    virtual bool getRevRank(const Value& member, size_t& rank) const = 0;
    
    // 获取指定排名范围的元素（从小到大）
    virtual std::vector<std::pair<Value, double>> rangeByRank(size_t start, size_t stop) const = 0;
    
    // 获取指定排名范围的元素（从大到小）
    virtual std::vector<std::pair<Value, double>> revRangeByRank(size_t start, size_t stop) const = 0;
    
    // 获取指定分数范围的元素（从小到大）
    virtual std::vector<std::pair<Value, double>> rangeByScore(double min, double max) const = 0;
    
    // 获取指定分数范围的元素（从大到小）
    virtual std::vector<std::pair<Value, double>> revRangeByScore(double max, double min) const = 0;
    
    // 获取指定分数范围内的元素个数
    virtual size_t countByScore(double min, double max) const = 0;
    
    // 获取元素个数
    virtual size_t size() const = 0;
    
    // 清空集合
    virtual void clear() = 0;
    
    // 判断集合是否为空
    virtual bool empty() const = 0;
};

// 基于std::map的ZSet实现
class MapZSetImpl : public IZSetImpl {
private:
    // 使用map按分数排序元素（分数 -> 元素列表）
    std::map<double, std::unordered_set<Value>> elements_by_score_;
    // 使用unordered_map快速查找元素的分数
    std::unordered_map<Value, double> scores_;

public:
    MapZSetImpl() = default;
    ~MapZSetImpl() = default;
    
    bool insert(const Value& member, double score) override;
    bool remove(const Value& member) override;
    bool getScore(const Value& member, double& score) const override;
    bool contains(const Value& member) const override;
    bool getRank(const Value& member, size_t& rank) const override;
    bool getRevRank(const Value& member, size_t& rank) const override;
    std::vector<std::pair<Value, double>> rangeByRank(size_t start, size_t stop) const override;
    std::vector<std::pair<Value, double>> revRangeByRank(size_t start, size_t stop) const override;
    std::vector<std::pair<Value, double>> rangeByScore(double min, double max) const override;
    std::vector<std::pair<Value, double>> revRangeByScore(double max, double min) const override;
    size_t countByScore(double min, double max) const override;
    size_t size() const override;
    void clear() override;
    bool empty() const override;
};

// 跳表类
template <typename Value>
class Skiplist {
private:
    static constexpr int MAX_LEVEL = 32;
    static constexpr double P = 0.25;
    
    SkiplistNode* head_; // 头节点
    SkiplistNode* tail_; // 尾节点
    int level_; // 当前跳表的最大层数
    size_t size_; // 跳表中的元素个数
    std::random_device rd_;
    std::mt19937 gen_;
    std::uniform_real_distribution<> dis_;
    
    // 随机生成层数
    int randomLevel();
    
    // 创建新节点
    SkiplistNode* createNode(const Value& member, double score, int level);
    
public:
    Skiplist();
    ~Skiplist();
    
    // 插入元素
    bool insert(const Value& member, double score);
    
    // 删除元素
    bool remove(const Value& member);
    
    // 更新元素的分数
    bool updateScore(const Value& member, double new_score);
    
    // 查找元素的分数
    bool getScore(const Value& member, double& score) const;
    
    // 判断元素是否存在
    bool contains(const Value& member) const;
    
    // 获取元素的排名（从小到大，从0开始）
    bool getRank(const Value& member, size_t& rank) const;
    
    // 获取元素的逆序排名（从大到小，从0开始）
    bool getRevRank(const Value& member, size_t& rank) const;
    
    // 获取指定排名范围的元素（从小到大）
    std::vector<std::pair<Value, double>> rangeByRank(size_t start, size_t stop) const;
    
    // 获取指定排名范围的元素（从大到小）
    std::vector<std::pair<Value, double>> revRangeByRank(size_t start, size_t stop) const;
    
    // 获取指定分数范围的元素（从小到大）
    std::vector<std::pair<Value, double>> rangeByScore(double min, double max) const;
    
    // 获取指定分数范围的元素（从大到小）
    std::vector<std::pair<Value, double>> revRangeByScore(double max, double min) const;
    
    // 获取指定分数范围内的元素个数
    size_t countByScore(double min, double max) const;
    
    // 获取跳表大小
    size_t size() const;
    
    // 清空跳表
    void clear();
    
    // 判断跳表是否为空
    bool empty() const;
};

// 跳表节点类
class SkiplistNode {
public:
    SkiplistNode(const Value& member, double score, int level);
    ~SkiplistNode();
    
    Value member;
    double score;
    SkiplistNode** forward; // 指向各层后继节点的指针数组
    int level;
};

// 基于跳表的ZSet实现
class SkiplistZSetImpl : public IZSetImpl {
private:
    Skiplist<Value> skiplist_;

public:
    SkiplistZSetImpl() = default;
    ~SkiplistZSetImpl() = default;
    
    bool insert(const Value& member, double score) override;
    bool remove(const Value& member) override;
    bool getScore(const Value& member, double& score) const override;
    bool contains(const Value& member) const override;
    bool getRank(const Value& member, size_t& rank) const override;
    bool getRevRank(const Value& member, size_t& rank) const override;
    std::vector<std::pair<Value, double>> rangeByRank(size_t start, size_t stop) const override;
    std::vector<std::pair<Value, double>> revRangeByRank(size_t start, size_t stop) const override;
    std::vector<std::pair<Value, double>> rangeByScore(double min, double max) const override;
    std::vector<std::pair<Value, double>> revRangeByScore(double max, double min) const override;
    size_t countByScore(double min, double max) const override;
    size_t size() const override;
    void clear() override;
    bool empty() const override;
};

// 全局配置：zset实现类型
enum class ZSetImplType {
    MAP,    // 使用std::map（红黑树）实现
    SKIPLIST // 使用跳表实现
};

// 设置zset实现类型
extern void setZSetImplType(ZSetImplType type);

// 获取zset实现类型
extern ZSetImplType getZSetImplType();

// 从配置字符串设置zset实现类型
extern void setZSetImplTypeFromConfig(const std::string& config_value);

// 创建zset实现实例
extern std::unique_ptr<IZSetImpl> createZSetImpl(ZSetImplType type);

// 有序集合数据项
class ZSetItem : public DataItem {
private:
    // 使用策略模式存储zset实现
    std::unique_ptr<IZSetImpl> impl_;

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