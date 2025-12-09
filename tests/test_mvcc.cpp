#include "dkv_mvcc.hpp"
#include "dkv_storage.hpp"
#include "dkv_datatype_string.hpp"
#include "test_runner.hpp"
#include <memory>
#include <vector>
#include <iostream>
using namespace std;


namespace dkv {

bool testMVCCGetAndSet() {
    // 创建必要的对象
    InnerStorage inner_storage;
    MVCC mvcc(inner_storage);
    StorageEngine engine;
    TransactionManager tx_manager(&engine, TransactionIsolationLevel::REPEATABLE_READ);

    // 创建事务ID
    TransactionID tx_id1 = tx_manager.begin();
    TransactionID tx_id2 = tx_manager.begin();

    // 创建ReadView
    ReadView read_view = mvcc.createReadView(tx_id1, tx_manager);

    // 创建数据项
    auto item = std::make_unique<StringItem>("initial_value");

    // 设置键值
    bool set_result = mvcc.set(tx_id1, "test_key", std::move(item));
    ASSERT_TRUE(set_result);

    // 读取值
    DataItem* read_item = mvcc.get(read_view, "test_key");
    ASSERT_TRUE(read_item != nullptr);
    ASSERT_TRUE(read_item->getType() == DataType::STRING);

    // 转换为StringItem并验证值
    StringItem* string_item = static_cast<StringItem*>(read_item);
    ASSERT_EQ(string_item->getValue(), "initial_value");

    // 测试不可见的数据
    auto new_item = std::make_unique<StringItem>("updated_value");
    mvcc.set(tx_id2, "test_key", std::move(new_item));
  
    // 使用相同的ReadView再次读取，应该看到旧值
    DataItem* read_item_again = mvcc.get(read_view, "test_key");
    ASSERT_TRUE(read_item_again != nullptr);
    ASSERT_TRUE(read_item_again->getType() == DataType::STRING);
            clog << 0;
    StringItem* string_item_again = static_cast<StringItem*>(read_item_again);
    ASSERT_EQ(string_item_again->getValue(), "initial_value");
                clog << 1;
    // 提交事务
    tx_manager.commit(tx_id1);
    tx_manager.commit(tx_id2);
    
    std::cout << "testMVCCGetAndSet passed!" << std::endl;
    return true;
}

bool testMVCCDelete() {
    // 创建必要的对象
    InnerStorage inner_storage;
    MVCC mvcc(inner_storage);
    StorageEngine engine;
    TransactionManager tx_manager(&engine, TransactionIsolationLevel::REPEATABLE_READ);
    
    // 创建事务ID
    TransactionID tx_id1 = tx_manager.begin();
    TransactionID tx_id2 = tx_manager.begin();
    
    // 创建数据项
    auto item = std::make_unique<StringItem>("value_to_delete");
    mvcc.set(tx_id1, "delete_key", std::move(item));
    
    // 创建ReadView
    ReadView read_view = mvcc.createReadView(tx_id2, tx_manager);

    // 删除键
    bool del_result = mvcc.del(tx_id1, "delete_key");
    ASSERT_TRUE(del_result);
    
    // 使用ReadView读取已删除的键，应该返回nullptr
    DataItem* read_item = mvcc.get(read_view, "delete_key");
    ASSERT_FALSE(read_item != nullptr);
    
    // 提交事务
    tx_manager.commit(tx_id1);
    tx_manager.commit(tx_id2);
    
    std::cout << "testMVCCDelete passed!" << std::endl;
    return true;
}

bool testMVCCReadViewVisibility() {
    // 创建必要的对象
    InnerStorage inner_storage;
    MVCC mvcc(inner_storage);
    StorageEngine engine;
    TransactionManager tx_manager(&engine, TransactionIsolationLevel::REPEATABLE_READ);
    
    // 创建事务ID
    TransactionID tx_id1 = tx_manager.begin();
    TransactionID tx_id2 = tx_manager.begin();
    TransactionID tx_id3 = tx_manager.begin();
    tx_manager.commit(tx_id3);
    TransactionID tx_id4 = 100; // 模拟一个未开始的事务ID
    clog << "tx_id1: " << tx_id1 << endl;
    clog << "tx_id2: " << tx_id2 << endl;
    clog << "tx_id3: " << tx_id3 << endl;
    clog << "tx_id4: " << tx_id4 << endl;

    // 创建ReadView
    ReadView read_view = mvcc.createReadView(tx_id1, tx_manager);
    clog << "read_view.low: " << read_view.low << endl;
    clog << "read_view.high: " << read_view.high << endl;
    clog << "active_transactions: " << endl;
    for (auto tx_id : read_view.actives) {
        clog << tx_id << " ";
    }
    clog << endl;
    // 测试可见性规则
    bool is_visible1 = read_view.isVisible(read_view.low - 1); // 小于low应该可见
    bool is_visible2 = read_view.isVisible(tx_id1); // 等于creator应该可见
    bool is_visible3 = read_view.isVisible(tx_id2); // 在actives中应该不可见
    bool is_visible4 = read_view.isVisible(read_view.high); // 大于等于high应该不可见
    bool is_visible5 = read_view.isVisible(tx_id3); // 在[low, high)范围内但不在actives中应该可见
    bool is_visible6 = read_view.isVisible(tx_id4); // 未开始的事务ID应该不可见
    
    ASSERT_TRUE(is_visible1);
    ASSERT_TRUE(is_visible2);
    ASSERT_FALSE(is_visible3);
    ASSERT_FALSE(is_visible4);
    ASSERT_TRUE(is_visible5);
    ASSERT_FALSE(is_visible6);
    
    // 测试ReadView类的isVisible方法
    bool read_view_visible = read_view.isVisible(read_view.low - 1);
    ASSERT_TRUE(read_view_visible);
    
    // 提交事务
    tx_manager.commit(tx_id1);
    tx_manager.commit(tx_id2);
    
    std::cout << "testMVCCReadViewVisibility passed!" << std::endl;
    return true;
}

bool testMVCCUndoLog() {
    // 创建必要的对象
    InnerStorage inner_storage;
    MVCC mvcc(inner_storage);
    StorageEngine engine;
    TransactionManager tx_manager(&engine, TransactionIsolationLevel::REPEATABLE_READ);
    
    // 创建事务ID
    TransactionID tx_id1 = tx_manager.begin();
    TransactionID tx_id2 = tx_manager.begin();
    
    // 创建第一个数据项并设置
    auto item1 = std::make_unique<StringItem>("value1");
    mvcc.set(tx_id1, "undo_key", std::move(item1));
    
    // 创建第二个数据项并设置（覆盖第一个）
    auto item2 = std::make_unique<StringItem>("value2");
    mvcc.set(tx_id2, "undo_key", std::move(item2));
    
    // 创建两个不同的ReadView
    ReadView read_view1 = mvcc.createReadView(tx_id1, tx_manager);
    ReadView read_view2 = mvcc.createReadView(tx_id2, tx_manager);
    
    // 第一个ReadView应该看到第一个版本
    DataItem* read_item1 = mvcc.get(read_view1, "undo_key");
    ASSERT_TRUE(read_item1 != nullptr);
    StringItem* string_item1 = static_cast<StringItem*>(read_item1);
    ASSERT_EQ(string_item1->getValue(), "value1");
    
    // 第二个ReadView应该看到第二个版本
    DataItem* read_item2 = mvcc.get(read_view2, "undo_key");
    ASSERT_TRUE(read_item2 != nullptr);
    StringItem* string_item2 = static_cast<StringItem*>(read_item2);
    ASSERT_EQ(string_item2->getValue(), "value2");
    
    // 提交事务
    tx_manager.commit(tx_id1);
    tx_manager.commit(tx_id2);
    
    std::cout << "testMVCCUndoLog passed!" << std::endl;
    return true;
}

} // namespace dkv

int main() {
    using namespace dkv;
    TestRunner runner;
    runner.runTest("MVCCReadViewVisibility", dkv::testMVCCReadViewVisibility);
    runner.runTest("MVCCGetAndSet", dkv::testMVCCGetAndSet);
    runner.runTest("MVCCDelete", dkv::testMVCCDelete);
    runner.runTest("MVCCUndoLog", dkv::testMVCCUndoLog);
    std::cout << "所有MVCC类测试完成!" << std::endl;
    return 0;
}