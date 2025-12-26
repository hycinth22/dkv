#include "storage/dkv_storage.hpp"
#include "dkv_core.hpp"
#include "dkv_utils.hpp"
#include "datatypes/dkv_datatype_list.hpp"
#include "test_runner.hpp"
#include <iostream>
#include <cassert>
#include <chrono>

namespace dkv {

// 测试ListItem基本功能
bool testListItem() {
    // 测试基本列表项
    ListItem item1;
    assert(item1.getType() == DataType::LIST);
    assert(item1.size() == 0);
    assert(item1.empty());
    assert(!item1.hasExpiration());
    
    // 测试左侧和右侧插入
    assert(item1.lpush("value1") == 1);
    assert(item1.rpush("value2") == 2);
    assert(item1.lpush("value3") == 3);
    assert(item1.size() == 3);
    assert(!item1.empty());
    
    // 测试获取范围
    auto range = item1.lrange(0, 2);
    assert(range.size() == 3);
    assert(range[0] == "value3");
    assert(range[1] == "value1");
    assert(range[2] == "value2");
    
    // 测试带过期时间的列表项
    auto expire_time = Utils::getCurrentTime() + std::chrono::seconds(10);
    ListItem item2(expire_time);
    assert(item2.hasExpiration());
    assert(!item2.isExpired());
    
    // 测试弹出元素
    Value value;
    assert(item1.lpop(value));
    assert(value == "value3");
    assert(item1.size() == 2);
    
    assert(item1.rpop(value));
    assert(value == "value2");
    assert(item1.size() == 1);
    
    // 测试序列化和反序列化
    std::string serialized = item1.serialize();
    ListItem item3;
    item3.deserialize(serialized);
    assert(item3.getType() == DataType::LIST);
    assert(item3.size() == 1);
    
    // 清空列表项
    item1.clear();
    assert(item1.size() == 0);
    assert(item1.empty());
    
    // 测试空列表的弹出
    assert(!item1.lpop(value));
    assert(!item1.rpop(value));
    
    return true;
}

// 测试StorageEngine的列表命令
bool testListCommands() {
    StorageEngine storage;
    
    // 测试LPUSH和LPOP
    assert(storage.lpush(NO_TX, "list1", "value1") == 1);
    assert(storage.lpush(NO_TX, "list1", "value2") == 2);
    assert(storage.lpop(NO_TX, "list1") == "value2");
    
    // 测试RPUSH和RPOP
    assert(storage.rpush(NO_TX, "list1", "value3") == 2);
    assert(storage.rpop(NO_TX, "list1") == "value3");
    
    // 测试LLEN
    assert(storage.llen(NO_TX, "list1") == 1);
    assert(storage.llen(NO_TX, "non_existent_list") == 0);
    
    // 测试LRANGE
    storage.lpush(NO_TX, "list2", "item1");
    storage.lpush(NO_TX, "list2", "item2");
    storage.lpush(NO_TX, "list2", "item3");
    
    auto items = storage.lrange(NO_TX, "list2", 0, -1);
    assert(items.size() == 3);
    assert(items[0] == "item3");
    assert(items[1] == "item2");
    assert(items[2] == "item1");
    
    // 测试获取部分范围
    auto partial_items = storage.lrange(NO_TX, "list2", 0, 1);
    assert(partial_items.size() == 2);
    
    // 测试不存在的列表
    auto empty_items = storage.lrange(NO_TX, "non_existent_list", 0, 10);
    assert(empty_items.empty());
    
    // 测试删除整个列表键
    assert(storage.del(NO_TX, "list1"));
    assert(!storage.exists(NO_TX, "list1"));
    
    // 测试LPOP和RPOP空列表
    assert(storage.lpop(NO_TX, "non_existent_list").empty());
    assert(storage.rpop(NO_TX, "non_existent_list").empty());
    
    return true;
}

} // namespace dkv

int main() {
    using namespace dkv;
    
    std::cout << "DKV List功能测试\n" << std::endl;
    
    TestRunner runner;
    
    runner.runTest("ListItem基本功能", testListItem);
    runner.runTest("List命令测试", testListCommands);
    
    runner.printSummary();
    
    return 0;
}