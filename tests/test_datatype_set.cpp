#include <iostream>
#include <string>
#include <cassert>
#include <vector>
#include <thread>
#include "dkv_storage.hpp"

void testSetBasicOperations() {
    std::cout << "测试集合基本操作..." << std::endl;
    
    dkv::StorageEngine storage;
    
    // 测试SADD和SCARD
    assert(storage.sadd("myset", {"a"}) == 1);
    assert(storage.sadd("myset", {"b"}) == 1);
    assert(storage.sadd("myset", {"c"}) == 1);
    assert(storage.scard("myset") == 3);
    
    // 测试重复元素添加
    assert(storage.sadd("myset", {"a"}) == 0);
    assert(storage.scard("myset") == 3);
    
    // 测试SISMEMBER
    assert(storage.sismember("myset", "a") == 1);
    assert(storage.sismember("myset", "d") == 0);
    
    // 测试SMEMBERS
    std::vector<dkv::Value> members = storage.smembers("myset");
    assert(members.size() == 3);
    
    // 检查所有元素是否存在
    bool has_a = false, has_b = false, has_c = false;
    for (const auto& member : members) {
        if (member == "a") has_a = true;
        if (member == "b") has_b = true;
        if (member == "c") has_c = true;
    }
    assert(has_a && has_b && has_c);
    
    // 测试SREM
    assert(storage.srem("myset", {"a"}) == 1);
    assert(storage.scard("myset") == 2);
    assert(storage.sismember("myset", {"a"}) == 0);
    
    // 测试删除不存在的元素
    assert(storage.srem("myset", {"d"}) == 0);
    
    // 测试删除所有元素
    assert(storage.srem("myset", {"b"}) == 1);
    assert(storage.srem("myset", {"c"}) == 1);
    assert(storage.scard("myset") == 0);
    
    std::cout << "集合基本操作测试通过！" << std::endl;
}

void testSetExpiration() {
    std::cout << "测试集合过期功能..." << std::endl;
    
    dkv::StorageEngine storage;
    
    // 添加带过期时间的集合
    storage.sadd("expireset", {"a"});
    storage.expire("expireset", 1); // 1秒后过期
    
    assert(storage.scard("expireset") == 1);
    
    // 等待过期
    std::this_thread::sleep_for(std::chrono::seconds(2));
    
    assert(storage.scard("expireset") == 0);
    
    std::cout << "集合过期功能测试通过！" << std::endl;
}

void testSetTypeChecking() {
    std::cout << "测试集合类型检查..." << std::endl;
    
    dkv::StorageEngine storage;
    
    // 先设置一个字符串键
    storage.set("testkey", "testvalue");
    
    // 尝试对字符串键执行集合操作
    assert(storage.sadd("testkey", {"a"}) == 0);
    
    std::cout << "集合类型检查测试通过！" << std::endl;
}

void testSetMultiElementOperations() {
    std::cout << "测试集合多元素操作..." << std::endl;
    
    dkv::StorageEngine storage;
    
    // 测试添加多个元素
    std::vector<dkv::Value> elements = {"a", "b", "c"};
    size_t added_count = storage.sadd("multiset", elements);
    assert(added_count == 3);
    assert(storage.scard("multiset") == 3);
    
    // 测试添加部分重复元素
    elements = {"c", "d", "e"};
    added_count = storage.sadd("multiset", elements);
    assert(added_count == 2); // 只有d和e是新元素
    assert(storage.scard("multiset") == 5);
    
    // 测试删除多个元素
    std::vector<dkv::Value> remove_elements = {"a", "c", "f"};
    size_t removed_count = storage.srem("multiset", remove_elements);
    assert(removed_count == 2); // 只有a和c存在于集合中
    assert(storage.scard("multiset") == 3);
    
    std::cout << "集合多元素操作测试通过！" << std::endl;
}

int main() {
    std::cout << "开始测试集合数据类型..." << std::endl;
    
    try {
        testSetBasicOperations();
        testSetExpiration();
        testSetTypeChecking();
        testSetMultiElementOperations();
        
        std::cout << "所有集合数据类型测试通过！" << std::endl;
        return 0;
    } catch (const std::exception& e) {
        std::cerr << "测试失败: " << e.what() << std::endl;
        return 1;
    } catch (...) {
        std::cerr << "未知错误导致测试失败" << std::endl;
        return 1;
    }
}