#include "dkv_storage.hpp"
#include "dkv_core.hpp"
#include "dkv_utils.hpp"
#include "dkv_datatype_bitmap.hpp"
#include "test_runner.hpp"
#include <iostream>
#include <cassert>
#include <chrono>
using namespace std;

namespace dkv {

// 测试BitmapItem基本功能
bool testBitmapItem() {
    // 测试基本位图项
    BitmapItem item1;
    assert(item1.getType() == DataType::BITMAP);
    assert(item1.bitCount() == 0);
    assert(!item1.hasExpiration());
    
    // 测试设置和获取位
    assert(item1.setBit(0, true));
    assert(item1.getBit(0) == true);
    assert(item1.bitCount() == 1);
    
    assert(item1.setBit(1, true));
    assert(item1.getBit(1) == true);
    assert(item1.bitCount() == 2);
    
    assert(item1.setBit(7, true));
    assert(item1.getBit(7) == true);
    assert(item1.bitCount() == 3);
    
    // 测试设置大偏移位
    assert(item1.setBit(100, true));
    assert(item1.getBit(100) == true);
    assert(item1.bitCount() == 4);
    
    // 测试清除位
    assert(item1.setBit(0, false));
    assert(item1.getBit(0) == false);
    assert(item1.bitCount() == 3);
    
    // 测试位计数范围
    assert(item1.bitCount(0, 7) == 2); // 位1和位7
    assert(item1.bitCount(8, 100) == 1); // 位100
    
    // 测试带过期时间的位图项
    auto expire_time = Utils::getCurrentTime() + chrono::seconds(10);
    BitmapItem item2(expire_time);
    assert(item2.hasExpiration());
    assert(!item2.isExpired());
    
    // 设置一些位
    assert(item2.setBit(0, true));
    assert(item2.setBit(1, true));
    assert(item2.setBit(2, true));
    assert(item2.bitCount() == 3);
    
    // 测试序列化和反序列化
    string serialized = item1.serialize();
    BitmapItem item3;
    item3.deserialize(serialized);
    assert(item3.getType() == DataType::BITMAP);
    assert(item3.bitCount() == 3);
    assert(item3.getBit(1) == true);
    assert(item3.getBit(7) == true);
    assert(item3.getBit(100) == true);
    
    // 测试位操作
    BitmapItem item4;
    item4.setBit(0, true);
    item4.setBit(2, true);
    
    BitmapItem item5;
    item5.setBit(1, true);
    item5.setBit(2, true);
    
    BitmapItem result_and, result_or, result_xor;
    vector<BitmapItem*> items = {&item4, &item5};
    
    // AND 操作
    assert(result_and.bitOpAnd(items));
    assert(result_and.getBit(0) == false);
    assert(result_and.getBit(1) == false);
    assert(result_and.getBit(2) == true);
    assert(result_and.bitCount() == 1);
    
    // OR 操作
    assert(result_or.bitOpOr(items));
    assert(result_or.getBit(0) == true);
    assert(result_or.getBit(1) == true);
    assert(result_or.getBit(2) == true);
    assert(result_or.bitCount() == 3);
    
    // XOR 操作
    assert(result_xor.bitOpXor(items));
    assert(result_xor.getBit(0) == true);
    assert(result_xor.getBit(1) == true);
    assert(result_xor.getBit(2) == false);
    assert(result_xor.bitCount() == 2);
    
    // NOT 操作
    BitmapItem result_not;
    assert(result_not.bitOpNot(&item4));
    // NOT操作后的结果取决于实现，这里我们只确保操作成功执行
    
    return true;
}

// 测试StorageEngine的位图命令
bool testBitmapCommands() {
    StorageEngine storage;
    // 测试SETBIT和GETBIT
    assert(storage.setBit("bitmap1", 0, true));
    assert(storage.getBit("bitmap1", 0) == true);
    assert(storage.setBit("bitmap1", 1, true));
    assert(storage.getBit("bitmap1", 1) == true);
    
    assert(storage.setBit("bitmap1", 100, true));
    assert(storage.getBit("bitmap1", 100) == true);
    
    // 测试不存在的位返回false
    assert(storage.getBit("bitmap1", 2) == false);
    assert(storage.getBit("non_existent", 0) == false);
    
    // 测试BITCOUNT
    assert(storage.bitCount("bitmap1") == 3);
    assert(storage.bitCount("bitmap1", 0, 1) == 2); // 位0和位1
    assert(storage.bitCount("bitmap1", 2, 100) == 1); // 位100
    assert(storage.bitCount("non_existent") == 0);
    
    // 测试位操作
    // 设置两个源位图
    assert(storage.setBit("bitmap2", 0, true));
    assert(storage.setBit("bitmap2", 2, true));
    
    assert(storage.setBit("bitmap3", 1, true));
    assert(storage.setBit("bitmap3", 2, true));
    
    // AND 操作
    vector<Key> keys_and = {"bitmap2", "bitmap3"};
    assert(storage.bitOp("AND", "bitmap_and", keys_and));
    assert(storage.getBit("bitmap_and", 0) == false);
    assert(storage.getBit("bitmap_and", 1) == false);
    assert(storage.getBit("bitmap_and", 2) == true);
    assert(storage.bitCount("bitmap_and") == 1);
    
    // OR 操作
    vector<Key> keys_or = {"bitmap2", "bitmap3"};
    assert(storage.bitOp("OR", "bitmap_or", keys_or));
    assert(storage.getBit("bitmap_or", 0) == true);
    assert(storage.getBit("bitmap_or", 1) == true);
    assert(storage.getBit("bitmap_or", 2) == true);
    assert(storage.bitCount("bitmap_or") == 3);
    
    // XOR 操作
    vector<Key> keys_xor = {"bitmap2", "bitmap3"};
    assert(storage.bitOp("XOR", "bitmap_xor", keys_xor));
    assert(storage.getBit("bitmap_xor", 0) == true);
    assert(storage.getBit("bitmap_xor", 1) == true);
    assert(storage.getBit("bitmap_xor", 2) == false);
    assert(storage.bitCount("bitmap_xor") == 2);
    
    // NOT 操作
    vector<Key> keys_not = {"bitmap2"};
    assert(storage.bitOp("NOT", "bitmap_not", keys_not));
    // NOT操作后的结果取决于实现，这里我们只确保操作成功执行
    
    // 测试删除位图键
    assert(storage.del("bitmap1"));
    assert(!storage.exists("bitmap1"));
    assert(storage.bitCount("bitmap1") == 0);
    
    return true;
}

} // namespace dkv

int main() {
    using namespace dkv;
    
    cout << "DKV Bitmap功能测试\n" << endl;
    
    TestRunner runner;
    
    runner.runTest("BitmapItem基本功能", testBitmapItem);
    runner.runTest("Bitmap命令测试", testBitmapCommands);
    
    runner.printSummary();
    
    return 0;
}