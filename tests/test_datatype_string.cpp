#include "datatypes/dkv_datatype_string.hpp"
#include "dkv_utils.hpp"
#include "test_runner.hpp"
#include <iostream>
#include <cassert>
#include <chrono>

namespace dkv {

// 测试StringItem基本功能
bool testStringItem() {
    // 测试基本字符串项
    StringItem item1("hello");
    assert(item1.getType() == DataType::STRING);
    assert(item1.getValue() == "hello");
    assert(!item1.hasExpiration());
    
    // 测试带过期时间的字符串项
    auto expire_time = Utils::getCurrentTime() + std::chrono::seconds(10);
    StringItem item2("world", expire_time);
    assert(item2.getValue() == "world");
    assert(item2.hasExpiration());
    assert(!item2.isExpired());
    
    // 测试序列化和反序列化
    std::string serialized = item1.serialize();
    StringItem item3("");
    item3.deserialize(serialized);
    assert(item3.getValue() == "hello");
    assert(item3.getType() == DataType::STRING);
    
    return true;
}

} // namespace dkv

int main() {
    using namespace dkv;
    
    std::cout << "DKV StringItem功能测试\n" << std::endl;
    
    TestRunner runner;
    
    runner.runTest("StringItem基本功能", testStringItem);
    
    runner.printSummary();
    
    return 0;
}