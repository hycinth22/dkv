#include "test_runner.hpp"
#include "dkv_storage.hpp"
#include "dkv_utils.hpp"
#include "dkv_logger.hpp"
#include <vector>
#include <string>
#include <cassert>
#include <thread>
#include <chrono>

namespace dkv {

void testZAddZRem() {
    StorageEngine engine;
    
    // 测试ZADD和ZREM命令
    DKV_LOG_INFO("Running testZAddZRem");
    
    // 添加元素
    std::vector<std::pair<Value, double>> members1 = { {"member1", 1.0} };
    std::vector<std::pair<Value, double>> members2 = { {"member2", 2.0} };
    std::vector<std::pair<Value, double>> members3 = { {"member3", 3.0} };
    
    engine.zadd("zset1", members1);
    engine.zadd("zset1", members2);
    engine.zadd("zset1", members3);
    
    // 验证添加成功
    size_t count = engine.zcard("zset1");
    assert(count == 3);
    
    bool isMember = engine.zismember("zset1", "member1");
    assert(isMember);
    
    // 更新已存在元素的分数
    std::vector<std::pair<Value, double>> updateMember = { {"member1", 1.5} };
    size_t updated = engine.zadd("zset1", updateMember);
    assert(updated == 1); // 返回1表示成功更新一个元素
    
    double score = 0;
    assert(engine.zscore("zset1", "member1", score));
    assert(score == 1.5);
    
    // 删除元素
    std::vector<Value> removeMembers = {"member2"};
    size_t removed = engine.zrem("zset1", removeMembers);
    assert(removed == 1);
    
    isMember = engine.zismember("zset1", "member2");
    assert(!isMember);
    
    // 批量删除元素
    removeMembers = {"member1", "member3"};
    removed = engine.zrem("zset1", removeMembers);
    assert(removed == 2);
    
    count = engine.zcard("zset1");
    assert(count == 0);
    
    DKV_LOG_INFO("testZAddZRem passed");
}

void testZScoreZIsMember() {
    StorageEngine engine;
    
    // 测试ZSCORE和ZISMEMBER命令
    DKV_LOG_INFO("Running testZScoreZIsMember");
    
    std::vector<std::pair<Value, double>> members1 = { {"member1", 10.5} };
    std::vector<std::pair<Value, double>> members2 = { {"member2", -5.25} };
    
    engine.zadd("zset2", members1);
    engine.zadd("zset2", members2);
    
    // 检查元素是否存在
    bool isMember = engine.zismember("zset2", "member1");
    assert(isMember);
    
    isMember = engine.zismember("zset2", "nonexistent");
    assert(!isMember);
    
    // 获取元素分数
    double score = 0;
    assert(engine.zscore("zset2", "member1", score));
    assert(score == 10.5);
    
    assert(engine.zscore("zset2", "member2", score));
    assert(score == -5.25);
    
    assert(!engine.zscore("zset2", "nonexistent", score));
    
    DKV_LOG_INFO("testZScoreZIsMember passed");
}

void testZRankZRevRank() {
    StorageEngine engine;
    
    // 测试ZRANK和ZREVRANK命令
    DKV_LOG_INFO("Running testZRankZRevRank");
    
    std::vector<std::pair<Value, double>> membersA = { {"A", 10} };
    std::vector<std::pair<Value, double>> membersB = { {"B", 5} };
    std::vector<std::pair<Value, double>> membersC = { {"C", 15} };
    std::vector<std::pair<Value, double>> membersD = { {"D", 0} };
    
    engine.zadd("zset3", membersA);
    engine.zadd("zset3", membersB);
    engine.zadd("zset3", membersC);
    engine.zadd("zset3", membersD);
    
    // 检查排名（升序）
    size_t rank = 0;
    assert(engine.zrank("zset3", "D", rank));
    assert(rank == 0);  // 最小的分数
    
    assert(engine.zrank("zset3", "B", rank));
    assert(rank == 1);
    
    assert(engine.zrank("zset3", "A", rank));
    assert(rank == 2);
    
    assert(engine.zrank("zset3", "C", rank));
    assert(rank == 3);  // 最大的分数
    
    assert(!engine.zrank("zset3", "nonexistent", rank));
    
    // 检查逆序排名（降序）
    assert(engine.zrevrank("zset3", "C", rank));
    assert(rank == 0);  // 最大的分数
    
    assert(engine.zrevrank("zset3", "A", rank));
    assert(rank == 1);
    
    assert(engine.zrevrank("zset3", "B", rank));
    assert(rank == 2);
    
    assert(engine.zrevrank("zset3", "D", rank));
    assert(rank == 3);  // 最小的分数
    
    DKV_LOG_INFO("testZRankZRevRank passed");
}

void testZRangeZRevRange() {
    StorageEngine engine;
    
    // 测试ZRANGE和ZREVRANGE命令
    DKV_LOG_INFO("Running testZRangeZRevRange");
    
    for (int i = 0; i < 10; ++i) {
        std::vector<std::pair<Value, double>> members = { {"member" + std::to_string(i), i} };
        engine.zadd("zset4", members);
    }
    
    // 测试ZRANGE
    auto range1 = engine.zrange("zset4", 2, 5);
    assert(range1.size() == 4);
    assert(range1[0].first == "member2");
    assert(range1[1].first == "member3");
    assert(range1[2].first == "member4");
    assert(range1[3].first == "member5");
    
    // 测试ZREVRANGE
    auto revRange1 = engine.zrevrange("zset4", 2, 5);
    assert(revRange1.size() == 4);
    assert(revRange1[0].first == "member7");  // 从高到低排序后的第3个元素
    assert(revRange1[1].first == "member6");
    assert(revRange1[2].first == "member5");
    assert(revRange1[3].first == "member4");
    
    DKV_LOG_INFO("testZRangeZRevRange passed");
}

void testZRangeByScoreZRevRangeByScore() {
    StorageEngine engine;
    
    // 测试ZRANGEBYSCORE和ZREVRANGEBYSCORE命令
    DKV_LOG_INFO("Running testZRangeByScoreZRevRangeByScore");
    
    std::vector<std::pair<Value, double>> membersA = { {"A", 10} };
    std::vector<std::pair<Value, double>> membersB = { {"B", 20} };
    std::vector<std::pair<Value, double>> membersC = { {"C", 30} };
    std::vector<std::pair<Value, double>> membersD = { {"D", 40} };
    std::vector<std::pair<Value, double>> membersE = { {"E", 50} };
    
    engine.zadd("zset5", membersA);
    engine.zadd("zset5", membersB);
    engine.zadd("zset5", membersC);
    engine.zadd("zset5", membersD);
    engine.zadd("zset5", membersE);
    
    // 测试ZRANGEBYSCORE
    auto range1 = engine.zrangebyscore("zset5", 15, 45);
    assert(range1.size() == 3);
    assert(range1[0].first == "B");
    assert(range1[1].first == "C");
    assert(range1[2].first == "D");
    
    // 测试ZREVRANGEBYSCORE
    auto revRange1 = engine.zrevrangebyscore("zset5", 45, 15);
    assert(revRange1.size() == 3);
    assert(revRange1[0].first == "D");
    assert(revRange1[1].first == "C");
    assert(revRange1[2].first == "B");
    
    DKV_LOG_INFO("testZRangeByScoreZRevRangeByScore passed");
}

void testZCountZCard() {
    StorageEngine engine;
    
    // 测试ZCOUNT和ZCARD命令
    DKV_LOG_INFO("Running testZCountZCard");
    
    for (int i = 0; i < 10; ++i) {
        std::vector<std::pair<Value, double>> members = { {"member" + std::to_string(i), i} };
        engine.zadd("zset6", members);
    }
    
    // 测试ZCARD
    size_t count = engine.zcard("zset6");
    assert(count == 10);
    
    // 测试ZCOUNT
    count = engine.zcount("zset6", 2, 7);
    assert(count == 6);  // 2,3,4,5,6,7
    
    count = engine.zcount("zset6", 5, 15);
    assert(count == 5);  // 5,6,7,8,9
    
    count = engine.zcount("zset6", -5, -1);
    assert(count == 0);  // 没有负数分数
    
    count = engine.zcount("nonexistent", 0, 10);
    assert(count == 0);
    
    DKV_LOG_INFO("testZCountZCard passed");
}

void testExpiration() {
    StorageEngine engine;
    
    // 测试ZSet元素的过期功能
    DKV_LOG_INFO("Running testExpiration");
    
    std::vector<std::pair<Value, double>> members1 = { {"member1", 10} };
    std::vector<std::pair<Value, double>> members2 = { {"member2", 20} };
    
    engine.zadd("zset7", members1);
    engine.zadd("zset7", members2);
    
    // 设置过期时间1秒
    assert(engine.expire("zset7", 1));
    
    // 验证元素存在
    assert(engine.exists("zset7"));
    
    size_t count = engine.zcard("zset7");
    assert(count == 2);
    
    // 等待过期
    std::this_thread::sleep_for(std::chrono::seconds(2));
    
    // 验证元素已过期
    assert(!engine.exists("zset7"));
    count = engine.zcard("zset7");
    assert(count == 0);
    
    DKV_LOG_INFO("testExpiration passed");
}

void testZAddMultipleMembers() {
    StorageEngine engine;
    
    // 测试ZADD命令支持多个member和score参数
    DKV_LOG_INFO("Running testZAddMultipleMembers");
    
    // 准备多个成员和分数对
    std::vector<std::pair<Value, double>> members_with_scores = {
        {"member1", 1.0},
        {"member2", 2.0},
        {"member3", 3.0},
        {"member4", 4.0},
        {"member5", 5.0}
    };
    
    // 一次添加多个成员
    size_t addedCount = engine.zadd("zset_multiple", members_with_scores);
    assert(addedCount == 5); // 应该成功添加5个元素
    
    // 验证所有成员都已添加
    size_t count = engine.zcard("zset_multiple");
    assert(count == 5);
    
    // 验证每个成员都存在且分数正确
    for (const auto& pair : members_with_scores) {
        const Value& member = pair.first;
        double expectedScore = pair.second;
        
        assert(engine.zismember("zset_multiple", member));
        
        double actualScore = 0;
        assert(engine.zscore("zset_multiple", member, actualScore));
        assert(actualScore == expectedScore);
    }
    
    // 测试混合添加新成员和更新已有成员
    std::vector<std::pair<Value, double>> mixed_members = {
        {"member1", 10.0}, // 更新已有成员的分数
        {"member6", 6.0}   // 添加新成员
    };
    
    addedCount = engine.zadd("zset_multiple", mixed_members);
    assert(addedCount == 2); // 应该返回2，表示更新和添加了2个元素
    
    // 验证更新和添加是否成功
    double updatedScore = 0;
    assert(engine.zscore("zset_multiple", "member1", updatedScore));
    assert(updatedScore == 10.0);
    
    assert(engine.zismember("zset_multiple", "member6"));
    
    DKV_LOG_INFO("testZAddMultipleMembers passed");
}

} // namespace dkv

int main() {
    using namespace dkv;
    
    std::cout << "DKV ZSet功能测试\n" << std::endl;
    
    try {
        testZAddZRem();
        testZScoreZIsMember();
        testZRankZRevRank();
        testZRangeZRevRange();
        testZRangeByScoreZRevRangeByScore();
        testZCountZCard();
        testExpiration();
        testZAddMultipleMembers();
        
        std::cout << "所有测试通过！" << std::endl;
        return 0;
    } catch (const std::exception& e) {
        std::cerr << "测试异常: " << e.what() << std::endl;
        return 1;
    }
}