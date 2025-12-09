#include "dkv_datatype_hash.hpp"
#include "dkv_storage.hpp"
#include "dkv_core.hpp"
#include "dkv_utils.hpp"
#include "test_runner.hpp"
#include <iostream>
#include <cassert>
#include <chrono>

namespace dkv {

// 测试HashItem基本功能
bool testHashItem() {
    // 测试基本哈希项
    HashItem item1;
    assert(item1.getType() == DataType::HASH);
    assert(item1.size() == 0);
    assert(!item1.hasExpiration());
    
    // 测试设置和获取字段
    assert(item1.setField("field1", "value1"));
    assert(item1.size() == 1);
    
    Value value;
    assert(item1.getField("field1", value));
    assert(value == "value1");
    
    // 测试字段是否存在
    assert(item1.existsField("field1"));
    assert(!item1.existsField("field2"));
    
    // 测试删除字段
    assert(item1.delField("field1"));
    assert(item1.size() == 0);
    assert(!item1.existsField("field1"));
    
    // 测试带过期时间的哈希项
    auto expire_time = Utils::getCurrentTime() + std::chrono::seconds(10);
    HashItem item2(expire_time);
    assert(item2.hasExpiration());
    assert(!item2.isExpired());
    
    // 测试多个字段
    assert(item2.setField("field1", "value1"));
    assert(item2.setField("field2", "value2"));
    assert(item2.size() == 2);
    
    // 测试获取所有键值
    auto keys = item2.getKeys();
    assert(keys.size() == 2);
    
    auto values = item2.getValues();
    assert(values.size() == 2);
    
    auto all = item2.getAll();
    assert(all.size() == 2);
    
    // 测试序列化和反序列化
    std::string serialized = item1.serialize();
    HashItem item3;
    item3.deserialize(serialized);
    assert(item3.getType() == DataType::HASH);
    assert(item3.size() == 0);
    
    // 清空哈希项
    item2.clear();
    assert(item2.size() == 0);
    
    return true;
}

// 测试StorageEngine的哈希命令
bool testHashCommands() {
    StorageEngine storage;
    
    // 测试HSET和HGET
    assert(storage.hset(NO_TX, "user1", "name", "John"));
    assert(storage.hset(NO_TX, "user1", "age", "30"));
    assert(storage.hget(NO_TX, "user1", "name") == "John");
    assert(storage.hget(NO_TX, "user1", "age") == "30");
    
    // 测试字段不存在的情况
    assert(storage.hget(NO_TX, "user1", "email").empty());
    assert(storage.hget(NO_TX, "user2", "name").empty());
    
    // 测试HGETALL
    auto all_fields = storage.hgetall(NO_TX, "user1");
    assert(all_fields.size() == 2);
    
    // 测试HDEL
    assert(storage.hdel(NO_TX, "user1", "age"));
    assert(storage.hget(NO_TX, "user1", "age").empty());
    
    // 测试HEXISTS
    assert(storage.hexists(NO_TX, "user1", "name"));
    assert(!storage.hexists(NO_TX, "user1", "age"));
    
    // 测试HKEYS和HVALS
    auto keys = storage.hkeys(NO_TX, "user1");
    assert(keys.size() == 1);
    assert(keys[0] == "name");
    
    auto values = storage.hvals(NO_TX, "user1");
    assert(values.size() == 1);
    assert(values[0] == "John");
    
    // 测试HLEN
    assert(storage.hlen(NO_TX, "user1") == 1);
    assert(storage.hlen(NO_TX, "user2") == 0);
    
    // 测试更新字段
    assert(storage.hset(NO_TX, "user1", "name", "Mike"));
    assert(storage.hget(NO_TX, "user1", "name") == "Mike");
    
    // 测试删除整个哈希键
    assert(storage.del(NO_TX, "user1"));
    assert(!storage.exists(NO_TX, "user1"));
    
    return true;
}

} // namespace dkv

int main() {
    using namespace dkv;
    
    std::cout << "DKV Hash功能测试\n" << std::endl;
    
    TestRunner runner;
    
    runner.runTest("HashItem基本功能", testHashItem);
    runner.runTest("Hash命令测试", testHashCommands);
    
    runner.printSummary();
    
    return 0;
}