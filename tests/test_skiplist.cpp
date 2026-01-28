#include "test_runner.hpp"
#include "datatypes/dkv_datatype_zset.hpp"
#include "dkv_logger.hpp"
#include <vector>
#include <string>

using namespace dkv;

// 测试基本的插入和删除操作
bool testSkiplistBasicInsertDelete() {
    // 设置使用跳表实现
    setZSetImplType(ZSetImplType::SKIPLIST);
    
    ZSetItem zset;
    
    // 测试插入
    ASSERT_TRUE(zset.zadd("member1", 10.0));
    ASSERT_TRUE(zset.zadd("member2", 20.0));
    ASSERT_TRUE(zset.zadd("member3", 15.0));
    
    ASSERT_EQ(zset.zcard(), 3);
    
    // 测试删除
    ASSERT_TRUE(zset.zrem("member2"));
    ASSERT_EQ(zset.zcard(), 2);
    
    // 测试删除不存在的元素
    ASSERT_FALSE(zset.zrem("nonexistent"));
    ASSERT_EQ(zset.zcard(), 2);
    
    // 测试清空
    zset.clear();
    ASSERT_EQ(zset.zcard(), 0);
    
    // 恢复默认实现
    setZSetImplType(ZSetImplType::MAP);
    
    return true;
}

// 测试分数更新
bool testSkiplistScoreUpdate() {
    // 设置使用跳表实现
    setZSetImplType(ZSetImplType::SKIPLIST);
    
    ZSetItem zset;
    
    // 插入元素
    ASSERT_TRUE(zset.zadd("member1", 10.0));
    
    // 更新分数
    ASSERT_TRUE(zset.zadd("member1", 30.0));
    
    // 验证更新后的分数
    double score;
    ASSERT_TRUE(zset.zscore("member1", score));
    ASSERT_EQ(score, 30.0);
    
    // 恢复默认实现
    setZSetImplType(ZSetImplType::MAP);
    
    return true;
}

// 测试排名计算
bool testSkiplistRankCalculation() {
    // 设置使用跳表实现
    setZSetImplType(ZSetImplType::SKIPLIST);
    
    ZSetItem zset;
    
    // 插入元素
    zset.zadd("member1", 10.0);
    zset.zadd("member2", 20.0);
    zset.zadd("member3", 15.0);
    zset.zadd("member4", 5.0);
    
    // 测试正序排名
    size_t rank;
    ASSERT_TRUE(zset.zrank("member4", rank));
    ASSERT_EQ(rank, 0);  // 5.0 最小
    
    ASSERT_TRUE(zset.zrank("member1", rank));
    ASSERT_EQ(rank, 1);  // 10.0
    
    ASSERT_TRUE(zset.zrank("member3", rank));
    ASSERT_EQ(rank, 2);  // 15.0
    
    ASSERT_TRUE(zset.zrank("member2", rank));
    ASSERT_EQ(rank, 3);  // 20.0 最大
    
    // 测试逆序排名
    ASSERT_TRUE(zset.zrevrank("member2", rank));
    ASSERT_EQ(rank, 0);  // 20.0 最大
    
    ASSERT_TRUE(zset.zrevrank("member3", rank));
    ASSERT_EQ(rank, 1);  // 15.0
    
    ASSERT_TRUE(zset.zrevrank("member1", rank));
    ASSERT_EQ(rank, 2);  // 10.0
    
    ASSERT_TRUE(zset.zrevrank("member4", rank));
    ASSERT_EQ(rank, 3);  // 5.0 最小
    
    // 恢复默认实现
    setZSetImplType(ZSetImplType::MAP);
    
    return true;
}

// 测试按排名范围查询
bool testSkiplistRangeByRank() {
    // 设置使用跳表实现
    setZSetImplType(ZSetImplType::SKIPLIST);
    
    ZSetItem zset;
    
    // 插入元素
    zset.zadd("member1", 10.0);
    zset.zadd("member2", 20.0);
    zset.zadd("member3", 15.0);
    zset.zadd("member4", 5.0);
    zset.zadd("member5", 25.0);
    
    // 测试正序范围查询
    auto range = zset.zrange(1, 3);
    ASSERT_EQ(range.size(), 3);
    ASSERT_EQ(range[0].first, "member1");
    ASSERT_EQ(range[1].first, "member3");
    ASSERT_EQ(range[2].first, "member2");
    
    // 测试逆序范围查询
    auto rev_range = zset.zrevrange(1, 3);
    ASSERT_EQ(rev_range.size(), 3);
    ASSERT_EQ(rev_range[0].first, "member2");
    ASSERT_EQ(rev_range[1].first, "member3");
    ASSERT_EQ(rev_range[2].first, "member1");
    
    // 恢复默认实现
    setZSetImplType(ZSetImplType::MAP);
    
    return true;
}

// 测试按分数范围查询
bool testSkiplistRangeByScore() {
    // 设置使用跳表实现
    setZSetImplType(ZSetImplType::SKIPLIST);
    
    ZSetItem zset;
    
    // 插入元素
    zset.zadd("member1", 10.0);
    zset.zadd("member2", 20.0);
    zset.zadd("member3", 15.0);
    zset.zadd("member4", 5.0);
    zset.zadd("member5", 25.0);
    
    // 测试正序分数范围查询
    auto range = zset.zrangebyscore(10.0, 20.0);
    ASSERT_EQ(range.size(), 3);
    ASSERT_EQ(range[0].first, "member1");
    ASSERT_EQ(range[1].first, "member3");
    ASSERT_EQ(range[2].first, "member2");
    
    // 测试逆序分数范围查询
    auto rev_range = zset.zrevrangebyscore(20.0, 10.0);
    ASSERT_EQ(rev_range.size(), 3);
    ASSERT_EQ(rev_range[0].first, "member2");
    ASSERT_EQ(rev_range[1].first, "member3");
    ASSERT_EQ(rev_range[2].first, "member1");
    
    // 恢复默认实现
    setZSetImplType(ZSetImplType::MAP);
    
    return true;
}

// 测试分数计数
bool testSkiplistCountByScore() {
    // 设置使用跳表实现
    setZSetImplType(ZSetImplType::SKIPLIST);
    
    ZSetItem zset;
    
    // 插入元素
    zset.zadd("member1", 10.0);
    zset.zadd("member2", 20.0);
    zset.zadd("member3", 15.0);
    zset.zadd("member4", 5.0);
    zset.zadd("member5", 25.0);
    
    // 测试计数
    ASSERT_EQ(zset.zcount(10.0, 20.0), 3);
    ASSERT_EQ(zset.zcount(5.0, 25.0), 5);
    ASSERT_EQ(zset.zcount(30.0, 40.0), 0);
    
    // 恢复默认实现
    setZSetImplType(ZSetImplType::MAP);
    
    return true;
}

// 测试元素是否存在
bool testSkiplistMemberExistence() {
    // 设置使用跳表实现
    setZSetImplType(ZSetImplType::SKIPLIST);
    
    ZSetItem zset;
    
    // 插入元素
    zset.zadd("member1", 10.0);
    
    // 测试存在的元素
    ASSERT_TRUE(zset.zismember("member1"));
    
    // 测试不存在的元素
    ASSERT_FALSE(zset.zismember("nonexistent"));
    
    // 测试删除后元素不存在
    zset.zrem("member1");
    ASSERT_FALSE(zset.zismember("member1"));
    
    // 恢复默认实现
    setZSetImplType(ZSetImplType::MAP);
    
    return true;
}

// 测试分数获取
bool testSkiplistScoreRetrieval() {
    // 设置使用跳表实现
    setZSetImplType(ZSetImplType::SKIPLIST);
    
    ZSetItem zset;
    
    // 插入元素
    zset.zadd("member1", 10.0);
    zset.zadd("member2", 20.0);
    
    // 测试获取分数
    double score;
    ASSERT_TRUE(zset.zscore("member1", score));
    ASSERT_EQ(score, 10.0);
    
    ASSERT_TRUE(zset.zscore("member2", score));
    ASSERT_EQ(score, 20.0);
    
    // 测试获取不存在元素的分数
    ASSERT_FALSE(zset.zscore("nonexistent", score));
    
    // 恢复默认实现
    setZSetImplType(ZSetImplType::MAP);
    
    return true;
}

// 测试批量插入和删除
bool testSkiplistBatchOperations() {
    // 设置使用跳表实现
    setZSetImplType(ZSetImplType::SKIPLIST);
    
    ZSetItem zset;
    
    // 测试批量插入
    std::vector<std::pair<Value, double>> members = {
        {"member1", 10.0},
        {"member2", 20.0},
        {"member3", 15.0}
    };
    ASSERT_EQ(zset.zadd(members), 3);
    ASSERT_EQ(zset.zcard(), 3);
    
    // 测试批量删除
    std::vector<Value> to_remove = {"member1", "member3"};
    ASSERT_EQ(zset.zrem(to_remove), 2);
    ASSERT_EQ(zset.zcard(), 1);
    
    // 恢复默认实现
    setZSetImplType(ZSetImplType::MAP);
    
    return true;
}

// 测试相同分数的元素
bool testSkiplistSameScoreElements() {
    // 设置使用跳表实现
    setZSetImplType(ZSetImplType::SKIPLIST);
    
    ZSetItem zset;
    
    // 插入相同分数的元素
    ASSERT_TRUE(zset.zadd("member1", 10.0));
    ASSERT_TRUE(zset.zadd("member2", 10.0));
    ASSERT_TRUE(zset.zadd("member3", 10.0));
    
    ASSERT_EQ(zset.zcard(), 3);
    ASSERT_EQ(zset.zcount(10.0, 10.0), 3);
    
    // 测试删除相同分数的元素
    ASSERT_TRUE(zset.zrem("member2"));
    ASSERT_EQ(zset.zcard(), 2);
    ASSERT_EQ(zset.zcount(10.0, 10.0), 2);
    
    // 恢复默认实现
    setZSetImplType(ZSetImplType::MAP);
    
    return true;
}

// 测试边界情况
bool testSkiplistEdgeCases() {
    // 设置使用跳表实现
    setZSetImplType(ZSetImplType::SKIPLIST);
    
    ZSetItem zset;
    
    // 测试大量元素插入
    for (int i = 0; i < 1000; ++i) {
        std::string member = "member" + std::to_string(i);
        zset.zadd(member, static_cast<double>(i));
    }
    ASSERT_EQ(zset.zcard(), 1000);
    
    // 测试大范围查询
    auto range = zset.zrange(0, 999);
    ASSERT_EQ(range.size(), 1000);
    
    // 测试空集合操作
    ZSetItem empty_zset;
    ASSERT_EQ(empty_zset.zcard(), 0);
    ASSERT_EQ(empty_zset.zcount(0.0, 100.0), 0);
    ASSERT_TRUE(empty_zset.zrange(0, 10).empty());
    
    // 恢复默认实现
    setZSetImplType(ZSetImplType::MAP);
    
    return true;
}

// 测试实现类型切换
bool testSkiplistImplementationSwitch() {
    // 设置使用跳表实现
    setZSetImplType(ZSetImplType::SKIPLIST);
    
    ZSetItem zset;
    
    // 当前应该使用跳表实现
    ASSERT_TRUE(zset.zadd("member1", 10.0));
    ASSERT_TRUE(zset.zadd("member2", 20.0));
    
    // 切换到map实现
    setZSetImplType(ZSetImplType::MAP);
    
    // 验证操作仍然正常工作
    ASSERT_EQ(zset.zcard(), 2);
    ASSERT_TRUE(zset.zrem("member1"));
    ASSERT_EQ(zset.zcard(), 1);
    
    // 切换回跳表实现
    setZSetImplType(ZSetImplType::SKIPLIST);
    
    // 验证操作仍然正常工作
    ASSERT_TRUE(zset.zadd("member3", 30.0));
    ASSERT_EQ(zset.zcard(), 2);
    
    // 恢复默认实现
    setZSetImplType(ZSetImplType::MAP);
    
    return true;
}

// 主函数，运行所有测试
int main() {
    DKV_LOG_INFO("Skiplist Implementation Test");
    
    TestRunner runner;
    
    // 注册并运行所有测试
    runner.runTest("BasicInsertDelete", testSkiplistBasicInsertDelete);
    runner.runTest("ScoreUpdate", testSkiplistScoreUpdate);
    runner.runTest("RankCalculation", testSkiplistRankCalculation);
    runner.runTest("RangeByRank", testSkiplistRangeByRank);
    runner.runTest("RangeByScore", testSkiplistRangeByScore);
    runner.runTest("CountByScore", testSkiplistCountByScore);
    runner.runTest("MemberExistence", testSkiplistMemberExistence);
    runner.runTest("ScoreRetrieval", testSkiplistScoreRetrieval);
    runner.runTest("BatchOperations", testSkiplistBatchOperations);
    runner.runTest("SameScoreElements", testSkiplistSameScoreElements);
    runner.runTest("EdgeCases", testSkiplistEdgeCases);
    runner.runTest("ImplementationSwitch", testSkiplistImplementationSwitch);
    
    // 打印测试结果
    runner.printSummary();
    
    return 0;
}