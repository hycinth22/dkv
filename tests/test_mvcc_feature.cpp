#include "transaction/dkv_transaction.hpp"
#include "dkv_storage.hpp"
#include "test_runner.hpp"
#include <thread>
#include <vector>
#include <atomic>

namespace dkv {

void testMVCCBasicFunctionality() {
    StorageEngine engine;
    Transaction txn(&engine, TransactionIsolationLevel::REPEATABLE_READ);
    
    // 初始设置
    engine.set("key1", "value1");
    
    // 开始事务
    txn.begin();
    
    // 读取值
    std::string value1 = txn.execute(Command(CommandType::GET, "key1")).value;
    assert(value1 == "value1");
    
    // 在事务外修改值
    engine.set("key1", "value2");
    
    // 在事务内再次读取，应该看到旧值（MVCC保证）
    std::string value2 = txn.execute(Command(CommandType::GET, "key1")).value;
    assert(value2 == "value1");
    
    // 提交事务
    txn.commit();
    
    // 事务后读取，应该看到新值
    std::string value3 = engine.get("key1");
    assert(value3 == "value2");
    
    std::cout << "testMVCCBasicFunctionality passed!" << std::endl;
}

void testMVCCConcurrentTransactions() {
    StorageEngine engine;
    std::atomic<bool> test_passed(true);
    
    // 初始设置
    engine.set("concurrent_key", "initial_value");
    
    // 创建第一个事务（读）
    std::thread reader([&]() {
        Transaction txn(&engine, TransactionIsolationLevel::REPEATABLE_READ);
        txn.begin();
        
        // 第一次读取
        std::string initial_value = txn.execute(Command(CommandType::GET, "concurrent_key")).value;
        
        // 等待写事务完成
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        
        // 第二次读取，应该看到相同的值
        std::string second_value = txn.execute(Command(CommandType::GET, "concurrent_key")).value;
        
        if (initial_value != second_value) {
            test_passed = false;
            std::cout << "MVCC failed: values differ in repeatable read" << std::endl;
        }
        
        txn.commit();
    });
    
    // 等待读事务开始
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    
    // 创建第二个事务（写）
    std::thread writer([&]() {
        Transaction txn(&engine, TransactionIsolationLevel::REPEATABLE_READ);
        txn.begin();
        txn.execute(Command(CommandType::SET, "concurrent_key", "updated_value"));
        txn.commit();
    });
    
    // 等待两个线程完成
    reader.join();
    writer.join();
    
    // 验证最终值
    std::string final_value = engine.get("concurrent_key");
    if (final_value != "updated_value") {
        test_passed = false;
        std::cout << "MVCC failed: final value not updated" << std::endl;
    }
    
    if (test_passed) {
        std::cout << "testMVCCConcurrentTransactions passed!" << std::endl;
    } else {
        std::cout << "testMVCCConcurrentTransactions failed!" << std::endl;
    }
}

void testMVCCSnapshotIsolation() {
    StorageEngine engine;
    
    // 设置初始值
    engine.set("snap_key1", "value1");
    engine.set("snap_key2", "value2");
    
    // 创建第一个事务（快照1）
    Transaction txn1(&engine, TransactionIsolationLevel::REPEATABLE_READ);
    txn1.begin();
    
    // 读取值，创建快照
    std::string val1_txn1 = txn1.execute(Command(CommandType::GET, "snap_key1")).value;
    
    // 创建第二个事务（修改）
    Transaction txn2(&engine, TransactionIsolationLevel::REPEATABLE_READ);
    txn2.begin();
    txn2.execute(Command(CommandType::SET, "snap_key1", "new_value1"));
    txn2.execute(Command(CommandType::SET, "snap_key2", "new_value2"));
    txn2.commit();
    
    // 在第一个事务中再次读取
    std::string val1_txn1_again = txn1.execute(Command(CommandType::GET, "snap_key1")).value;
    std::string val2_txn1 = txn1.execute(Command(CommandType::GET, "snap_key2")).value;
    
    // 验证快照隔离
    assert(val1_txn1 == val1_txn1_again);
    assert(val2_txn1 == "value2");
    
    txn1.commit();
    
    std::cout << "testMVCCSnapshotIsolation passed!" << std::endl;
}

void testMVCCWithDifferentOperations() {
    StorageEngine engine;
    
    // 测试哈希操作
    engine.hset("hash_key", "field1", "value1");
    
    Transaction txn(&engine, TransactionIsolationLevel::REPEATABLE_READ);
    txn.begin();
    
    // 读取哈希值
    std::string hash_val = txn.execute(Command(CommandType::HGET, "hash_key", "field1")).value;
    assert(hash_val == "value1");
    
    // 在事务外修改哈希值
    engine.hset("hash_key", "field1", "updated_value1");
    
    // 在事务内再次读取，应该看到旧值
    std::string hash_val_again = txn.execute(Command(CommandType::HGET, "hash_key", "field1")).value;
    assert(hash_val_again == "value1");
    
    txn.commit();
    
    std::cout << "testMVCCWithDifferentOperations passed!" << std::endl;
}

} // namespace dkv

int main() {
    dkv::testMVCCBasicFunctionality();
    dkv::testMVCCConcurrentTransactions();
    dkv::testMVCCSnapshotIsolation();
    dkv::testMVCCWithDifferentOperations();
    
    std::cout << "All MVCC tests completed!" << std::endl;
    return 0;
}