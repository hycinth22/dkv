#include "datatypes/dkv_datatype_zset.hpp"
#include <iostream>

using namespace dkv;

int main() {
    std::cout << "Testing Skiplist implementation..." << std::endl;
    
    // 设置使用跳表实现
    setZSetImplType(ZSetImplType::SKIPLIST);
    std::cout << "ZSet implementation type set to SKIPLIST" << std::endl;
    
    // 创建ZSetItem实例
    ZSetItem zset;
    
    // 测试zadd操作
    std::cout << "\nTesting zadd operations..." << std::endl;
    zset.zadd("member1", 10.0);
    zset.zadd("member2", 20.0);
    zset.zadd("member3", 15.0);
    zset.zadd("member4", 5.0);
    
    // 测试zcard操作
    std::cout << "zcard: " << zset.zcard() << std::endl;
    
    // 测试zrange操作
    std::cout << "\nTesting zrange operation (0-3)..." << std::endl;
    auto range = zset.zrange(0, 3);
    for (const auto& pair : range) {
        std::cout << "  " << pair.first << ": " << pair.second << std::endl;
    }
    
    // 测试zrevrange操作
    std::cout << "\nTesting zrevrange operation (0-3)..." << std::endl;
    auto rev_range = zset.zrevrange(0, 3);
    for (const auto& pair : rev_range) {
        std::cout << "  " << pair.first << ": " << pair.second << std::endl;
    }
    
    // 测试zscore操作
    std::cout << "\nTesting zscore operation..." << std::endl;
    double score;
    if (zset.zscore("member2", score)) {
        std::cout << "zscore(member2): " << score << std::endl;
    }
    
    // 测试zrank操作
    std::cout << "\nTesting zrank operation..." << std::endl;
    size_t rank;
    if (zset.zrank("member3", rank)) {
        std::cout << "zrank(member3): " << rank << std::endl;
    }
    
    // 测试zrevrank操作
    std::cout << "\nTesting zrevrank operation..." << std::endl;
    if (zset.zrevrank("member3", rank)) {
        std::cout << "zrevrank(member3): " << rank << std::endl;
    }
    
    // 测试zrangebyscore操作
    std::cout << "\nTesting zrangebyscore operation (10-20)..." << std::endl;
    auto range_by_score = zset.zrangebyscore(10.0, 20.0);
    for (const auto& pair : range_by_score) {
        std::cout << "  " << pair.first << ": " << pair.second << std::endl;
    }
    
    // 测试zrem操作
    std::cout << "\nTesting zrem operation..." << std::endl;
    zset.zrem("member2");
    std::cout << "After zrem(member2), zcard: " << zset.zcard() << std::endl;
    
    // 测试zcount操作
    std::cout << "\nTesting zcount operation (10-20)..." << std::endl;
    std::cout << "zcount(10-20): " << zset.zcount(10.0, 20.0) << std::endl;
    
    std::cout << "\nSkiplist implementation test completed successfully!" << std::endl;
    
    return 0;
}