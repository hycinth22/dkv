#include "dkv_core.hpp"
#include "storage/dkv_storage.hpp"
#include "dkv_datatypes.hpp"
#include "test_runner.hpp"
#include <iostream>
#include <thread>
#include <vector>
#include <chrono>
#include <atomic>
#include <functional>
#include <string>
using namespace std;
namespace dkv {

// 测试多个线程同时读取不同的键
bool testConcurrentReadsDifferentKeys() {
    StorageEngine storage;
    const int NUM_THREADS = 100;
    const int KEYS_PER_THREAD = 100;
    
    // 准备测试数据
    for (int i = 0; i < NUM_THREADS * KEYS_PER_THREAD; ++i) {
        storage.set(NO_TX, "key" + to_string(i), "value" + to_string(i));
    }
    
    vector<thread> threads;
    atomic<int> successful_reads(0);
    
    // 创建多个线程同时读取不同的键
    for (int t = 0; t < NUM_THREADS; ++t) {
        threads.emplace_back([&storage, t, KEYS_PER_THREAD, &successful_reads]() {
            for (int i = 0; i < KEYS_PER_THREAD; ++i) {
                int key_index = t * KEYS_PER_THREAD + i;
                string key = "key" + to_string(key_index);
                string expected_value = "value" + to_string(key_index);
                
                string value = storage.get(NO_TX, key);
                if (value == expected_value) {
                    successful_reads++;
                }
                
                // 小延迟以模拟真实负载
                this_thread::sleep_for(chrono::microseconds(10));
            }
        });
    }
    
    // 等待所有线程完成
    for (auto& thread : threads) {
        thread.join();
    }
    
    // 验证所有读取都成功
    ASSERT_EQ(successful_reads.load(), NUM_THREADS * KEYS_PER_THREAD);
    
    return true;
}

// 测试多个线程同时读取相同的键
bool testConcurrentReadsSameKey() {
    StorageEngine storage;
    const int NUM_THREADS = 100;
    const int READS_PER_THREAD = 1000;
    
    // 准备测试数据
    const string TEST_KEY = "shared_key";
    const string TEST_VALUE = "shared_value";
    storage.set(NO_TX, TEST_KEY, TEST_VALUE);
    
    vector<thread> threads;
    atomic<int> successful_reads(0);
    
    // 创建多个线程同时读取相同的键
    for (int t = 0; t < NUM_THREADS; ++t) {
        threads.emplace_back([&storage, TEST_KEY, TEST_VALUE, READS_PER_THREAD, &successful_reads]() {
            for (int i = 0; i < READS_PER_THREAD; ++i) {
                string value = storage.get(NO_TX, TEST_KEY);
                if (value == TEST_VALUE) {
                    successful_reads++;
                }
                
                // 小延迟以模拟真实负载
                this_thread::sleep_for(chrono::microseconds(5));
            }
        });
    }
    
    // 等待所有线程完成
    for (auto& thread : threads) {
        thread.join();
    }
    
    // 验证所有读取都成功
    ASSERT_EQ(successful_reads.load(), NUM_THREADS * READS_PER_THREAD);
    
    return true;
}

// 测试多个线程同时读写相同的键
bool testConcurrentReadWriteSameKey() {
    StorageEngine storage;
    const int NUM_READER_THREADS = 50;
    const int NUM_WRITER_THREADS = 20;
    const int OPERATIONS_PER_THREAD = 500;
    
    // 准备测试数据
    const string TEST_KEY = "concurrent_key";
    storage.set(NO_TX, TEST_KEY, "initial_value");
    
    vector<thread> threads;
    atomic<int> successful_operations(0);
    atomic<int> current_value(0);
    
    // 创建写入线程
    for (int t = 0; t < NUM_WRITER_THREADS; ++t) {
        threads.emplace_back([&storage, TEST_KEY, OPERATIONS_PER_THREAD, &successful_operations, &current_value]() {
            for (int i = 0; i < OPERATIONS_PER_THREAD; ++i) {
                int new_value = current_value++; // 原子操作，确保每个写入的值都是唯一的
                bool success = storage.set(NO_TX, TEST_KEY, to_string(new_value));
                if (success) {
                    successful_operations++;
                }
                
                // 小延迟以模拟真实负载
                this_thread::sleep_for(chrono::microseconds(10));
            }
        });
    }
    
    // 创建读取线程
    for (int t = 0; t < NUM_READER_THREADS; ++t) {
        threads.emplace_back([&storage, TEST_KEY, OPERATIONS_PER_THREAD, &successful_operations]() {
            for (int i = 0; i < OPERATIONS_PER_THREAD; ++i) {
                string value = storage.get(NO_TX, TEST_KEY);
                if (!value.empty()) {
                    successful_operations++;
                }
                
                // 小延迟以模拟真实负载
                this_thread::sleep_for(chrono::microseconds(5));
            }
        });
    }
    
    // 等待所有线程完成
    for (auto& thread : threads) {
        thread.join();
    }
    
    // 验证所有操作都成功
    int total_operations = (NUM_READER_THREADS + NUM_WRITER_THREADS) * OPERATIONS_PER_THREAD;
    ASSERT_EQ(successful_operations.load(), total_operations);
    
    // 验证最终值与预期一致
    string final_value = storage.get(NO_TX, TEST_KEY);
    ASSERT_EQ(final_value, to_string(NUM_WRITER_THREADS * OPERATIONS_PER_THREAD - 1));
    
    return true;
}

// 测试多个线程同时递增同一个计数器
bool testConcurrentIncr() {
    StorageEngine storage;
    const int NUM_THREADS = 100;
    const int INCR_PER_THREAD = 1000;

    // 准备测试数据
    const string COUNTER_KEY = "counter";
    storage.set(NO_TX, COUNTER_KEY, "0");
    
    vector<thread> threads;

    // 创建多个线程同时递增同一个计数器
    for (int t = 0; t < NUM_THREADS; ++t) {
        threads.emplace_back([&storage, COUNTER_KEY, INCR_PER_THREAD, t]() {
            for (int i = 0; i < INCR_PER_THREAD; ++i) {
                storage.incr(NO_TX, COUNTER_KEY);
                
                // 小延迟以增加并发冲突的可能性
                this_thread::sleep_for(chrono::microseconds(2));
            }
        });
    }

    // 等待所有线程完成
    for (auto& thread : threads) {
        thread.join();
    }

    // 验证最终计数值是否正确
    int64_t final_value = stoll(storage.get(NO_TX, COUNTER_KEY));
    int64_t expected_value = NUM_THREADS * INCR_PER_THREAD;
    ASSERT_EQ(final_value, expected_value);
    
    return true;
}

// 测试哈希操作的并发安全性
bool testConcurrentHashOperations() {
    StorageEngine storage;
    const int NUM_THREADS = 80;
    const int OPERATIONS_PER_THREAD = 500;
    
    // 准备测试数据
    const string HASH_KEY = "concurrent_hash";
    
    vector<thread> threads;
    
    // 创建多个线程同时执行哈希操作
    for (int t = 0; t < NUM_THREADS; ++t) {
        threads.emplace_back([&storage, HASH_KEY, OPERATIONS_PER_THREAD, t]() {
            for (int i = 0; i < OPERATIONS_PER_THREAD; ++i) {
                // 每个线程操作自己的字段，但在同一个哈希中
                string field = "field_" + to_string(t);
                string value = "value_" + to_string(i);
                
                // 执行hset操作
                storage.hset(NO_TX, HASH_KEY, field, value);
                
                // 执行hget操作验证
                string retrieved_value = storage.hget(NO_TX, HASH_KEY, field);
                ASSERT_EQ(retrieved_value, value);
                
                // 小延迟以模拟真实负载
                this_thread::sleep_for(chrono::microseconds(3));
            }
        });
    }
    
    // 等待所有线程完成
    for (auto& thread : threads) {
        thread.join();
    }
    
    // 验证哈希的大小是否正确
    size_t hash_size = storage.hlen(NO_TX, HASH_KEY);
    ASSERT_EQ(hash_size, size_t(NUM_THREADS));
    
    // 验证所有字段的值都是最后设置的值
    for (int t = 0; t < NUM_THREADS; ++t) {
        string field = "field_" + to_string(t);
        string expected_value = "value_" + to_string(OPERATIONS_PER_THREAD - 1);
        string retrieved_value = storage.hget(NO_TX, HASH_KEY, field);
        ASSERT_EQ(retrieved_value, expected_value);
    }
    
    return true;
}

// 测试列表操作的并发安全性
bool testConcurrentListOperations() {
    StorageEngine storage;
    const int NUM_WRITER_THREADS = 4;
    const int NUM_READER_THREADS = 4;
    const int OPERATIONS_PER_THREAD = 200;
    
    // 准备测试数据
    const string LIST_KEY = "concurrent_list";
    vector<thread> wthreads, rthreads;
    atomic<int> items_in_list(0);
    atomic<int> items_read(0);
    // 创建写入线程
    for (int t = 0; t < NUM_WRITER_THREADS; ++t) {
        wthreads.emplace_back([&storage, LIST_KEY, OPERATIONS_PER_THREAD, &items_in_list, t]() {
            for (int i = 0; i < OPERATIONS_PER_THREAD; ++i) {
                string value = "item_" + to_string(t) + "_" + to_string(i);
                storage.lpush(NO_TX, LIST_KEY, value);
                items_in_list++;
                
                // 小延迟以模拟真实负载
                this_thread::sleep_for(chrono::microseconds(5));
            }
        });
    }
    // 创建读取线程
    for (int t = 0; t < NUM_READER_THREADS; ++t) {
        rthreads.emplace_back([&storage, LIST_KEY, &items_in_list, &items_read]() {
            while (items_read < NUM_WRITER_THREADS * OPERATIONS_PER_THREAD) {
                string value = storage.rpop(NO_TX, LIST_KEY);
                if (!value.empty()) {
                    stringstream ss;
                    ss << items_read << endl;
                    clog << ss.str();
                    items_read++;
                }
                
                // 小延迟以模拟真实负载
                this_thread::sleep_for(chrono::microseconds(3));
            }
        });
    }
    // 等待所有线程完成
    for (auto& thread : wthreads) {
        thread.join();
    }
    for (auto& thread : rthreads) {
        thread.join();
    }
            clog<<5;
    // 验证所有项目都被读取
    ASSERT_EQ(items_read.load(), NUM_WRITER_THREADS * OPERATIONS_PER_THREAD);
    
    // 验证列表为空
    size_t list_size = storage.llen(NO_TX, LIST_KEY);
    ASSERT_EQ(list_size, size_t(0));
    
    return true;
}

// 测试在高并发情况下的内存使用和性能
bool testHighConcurrencyPerformance() {
    StorageEngine storage;
    const int NUM_THREADS = 16;
    const int OPERATIONS_PER_THREAD = 1000;
    
    vector<thread> threads;
    atomic<int> successful_operations(0);
    
    // 创建多个线程执行混合操作
    for (int t = 0; t < NUM_THREADS; ++t) {
        threads.emplace_back([&storage, OPERATIONS_PER_THREAD, &successful_operations, t]() {
            for (int i = 0; i < OPERATIONS_PER_THREAD; ++i) {
                // 为每个线程使用独立的键空间
                string key = "key_" + to_string(t) + "_" + to_string(i % 10); // 每个线程复用10个键
                
                // 交替执行不同类型的操作
                int op_type = i % 4;
                switch (op_type) {
                    case 0: // 设置值
                        if (storage.set(NO_TX, key, "value_" + to_string(i))) {
                            successful_operations++;
                        }
                        break;
                    case 1: // 获取值
                        storage.decr(NO_TX, key + "_counter");
                        successful_operations++;
                        break;
                    case 2: // 递增
                        storage.incr(NO_TX, key + "_counter");
                        successful_operations++;
                        break;
                    case 3: // 哈希操作
                        storage.hset(NO_TX, key + "_hash", "field", "hash_value");
                        successful_operations++;
                        break;
                }
                
                // 极小延迟以增加并发度
                this_thread::sleep_for(chrono::microseconds(1));
            }
        });
    }
    
    // 等待所有线程完成
    for (auto& thread : threads) {
        thread.join();
    }
    
    // 验证操作成功率
    int total_operations = NUM_THREADS * OPERATIONS_PER_THREAD;
    double success_rate = static_cast<double>(successful_operations) / total_operations;
    cout << "高并发测试成功率: " << (success_rate * 100) << "%" << endl;
    
    // 允许一定的失败率，但应该很高
    ASSERT_GE(success_rate, 0.95);
    
    return true;
}

} // namespace dkv

int main() {
    using namespace dkv;
    
    cout << "DKV 并发锁定测试\n" << endl;
    
    TestRunner runner;
    
    // 运行所有并发测试
    runner.runTest("不同键的并发读取", testConcurrentReadsDifferentKeys);
    runner.runTest("相同键的并发读取", testConcurrentReadsSameKey);
    runner.runTest("相同键的并发读写", testConcurrentReadWriteSameKey);
    runner.runTest("并发递增计数器", testConcurrentIncr);
    runner.runTest("哈希操作并发安全性", testConcurrentHashOperations);
    runner.runTest("列表操作并发安全性", testConcurrentListOperations);
    runner.runTest("高并发性能测试", testHighConcurrencyPerformance);
    
    // 打印测试总结
    runner.printSummary();
    
    return 0;
}