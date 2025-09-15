#include "dkv_memory_allocator.hpp"
#include <iostream>
#include <string>
#include <vector>
#include <unordered_map>
#include <cassert>

// 简单的测试类，用于测试TrackedDeleter
class TestClass {
public:
    TestClass(int value = 0) : value_(value) {
        std::cout << "TestClass constructed with value: " << value_ << std::endl;
    }
    
    ~TestClass() {
        std::cout << "TestClass destroyed with value: " << value_ << std::endl;
    }
    
    int getValue() const { return value_; }
    void setValue(int value) { value_ = value; }
    
private:
    int value_;
};

int main() {
    std::cout << "=== 测试自定义内存分配器 ===\n";
    
    // 重置统计信息
    dkv::MemoryAllocator::getInstance().resetStats();
    
    std::cout << "\n1. 测试基本的内存分配和释放\n";
    
    // 检查初始状态
    std::cout << "初始内存使用量: " << dkv::MemoryAllocator::getInstance().getCurrentUsage() << " 字节\n";
    assert(dkv::MemoryAllocator::getInstance().getCurrentUsage() == 0 && "初始内存使用量应为0字节");
    assert(dkv::MemoryAllocator::getInstance().getTotalAllocations() == 0 && "初始总分配次数应为0");
    assert(dkv::MemoryAllocator::getInstance().getTotalDeallocations() == 0 && "初始总释放次数应为0");
    
    // 分配一些内存
    void* ptr1 = dkv::MemoryAllocator::getInstance().allocate(100, "test_block_1");
    std::cout << "分配100字节后内存使用量: " << dkv::MemoryAllocator::getInstance().getCurrentUsage() << " 字节\n";
    assert(dkv::MemoryAllocator::getInstance().getCurrentUsage() >= 100 && "内存使用量应增加至少100字节");
    assert(dkv::MemoryAllocator::getInstance().getTotalAllocations() == 1 && "总分配次数应为1");
    
    void* ptr2 = dkv::MemoryAllocator::getInstance().allocate(200, "test_block_2");
    std::cout << "再分配200字节后内存使用量: " << dkv::MemoryAllocator::getInstance().getCurrentUsage() << " 字节\n";
    assert(dkv::MemoryAllocator::getInstance().getCurrentUsage() >= 300 && "内存使用量应增加至少300字节");
    assert(dkv::MemoryAllocator::getInstance().getTotalAllocations() == 2 && "总分配次数应为2");
    
    // 释放内存
    dkv::MemoryAllocator::getInstance().deallocate(ptr1);
    std::cout << "释放100字节后内存使用量: " << dkv::MemoryAllocator::getInstance().getCurrentUsage() << " 字节\n";
    assert(dkv::MemoryAllocator::getInstance().getTotalDeallocations() == 1 && "总释放次数应为1");
    
    dkv::MemoryAllocator::getInstance().deallocate(ptr2);
    std::cout << "释放200字节后内存使用量: " << dkv::MemoryAllocator::getInstance().getCurrentUsage() << " 字节\n";
    assert(dkv::MemoryAllocator::getInstance().getTotalDeallocations() == 2 && "总释放次数应为2");
    
    std::cout << "\n2. 测试内存重新分配\n";
    
    // 测试重新分配
    void* ptr3 = dkv::MemoryAllocator::getInstance().allocate(50, "test_block_3");
    std::cout << "分配50字节后内存使用量: " << dkv::MemoryAllocator::getInstance().getCurrentUsage() << " 字节\n";
    assert(dkv::MemoryAllocator::getInstance().getCurrentUsage() >= 50 && "内存使用量应增加至少50字节");
    assert(dkv::MemoryAllocator::getInstance().getTotalAllocations() == 3 && "总分配次数应为3");
    
    ptr3 = dkv::MemoryAllocator::getInstance().reallocate(ptr3, 150, "test_block_3_resized");
    std::cout << "重新分配为150字节后内存使用量: " << dkv::MemoryAllocator::getInstance().getCurrentUsage() << " 字节\n";
    assert(dkv::MemoryAllocator::getInstance().getCurrentUsage() >= 150 && "内存使用量应增加至少150字节");
    
    dkv::MemoryAllocator::getInstance().deallocate(ptr3);
    std::cout << "释放重新分配的内存后内存使用量: " << dkv::MemoryAllocator::getInstance().getCurrentUsage() << " 字节\n";
    
    // 测试重新分配空指针
    void* ptr4 = dkv::MemoryAllocator::getInstance().reallocate(nullptr, 100, "test_block_4");
    assert(ptr4 != nullptr && "重新分配空指针应该等同于分配新内存");
    dkv::MemoryAllocator::getInstance().deallocate(ptr4);
    
    // 测试重新分配为0大小
    void* ptr5 = dkv::MemoryAllocator::getInstance().allocate(50, "test_block_5");
    dkv::MemoryAllocator::getInstance().reallocate(ptr5, 0, "test_block_5");
    
    std::cout << "\n3. 测试自定义分配器和容器\n";
    
    // 重置统计信息，准备测试自定义容器
    dkv::MemoryAllocator::getInstance().resetStats();
    
    // 使用自定义容器类型
    dkv::TrackedVector<int> tracked_vector;
    for (int i = 0; i < 100; ++i) {
        tracked_vector.push_back(i);
    }
    std::cout << "使用TrackedVector后内存使用量: " << dkv::MemoryAllocator::getInstance().getCurrentUsage() << " 字节\n";
    assert(tracked_vector.size() == 100 && "TrackedVector大小应为100");
    assert(dkv::MemoryAllocator::getInstance().getCurrentUsage() > 0 && "使用TrackedVector后内存使用量应大于0");
    
    dkv::TrackedUnorderedMap<std::string, int> tracked_map;
    for (int i = 0; i < 50; ++i) {
        tracked_map["key" + std::to_string(i)] = i;
    }
    std::cout << "使用TrackedUnorderedMap后内存使用量: " << dkv::MemoryAllocator::getInstance().getCurrentUsage() << " 字节\n";
    assert(tracked_map.size() == 50 && "TrackedUnorderedMap大小应为50");
    
    // 测试TrackedSet
    dkv::TrackedSet<double> tracked_set;
    for (int i = 0; i < 20; ++i) {
        tracked_set.insert(i * 1.1);
    }
    std::cout << "使用TrackedSet后内存使用量: " << dkv::MemoryAllocator::getInstance().getCurrentUsage() << " 字节\n";
    assert(tracked_set.size() == 20 && "TrackedSet大小应为20");
    
    // 测试释放内存
    tracked_vector.clear();
    tracked_vector.shrink_to_fit();
    std::cout << "清空TrackedVector后内存使用量: " << dkv::MemoryAllocator::getInstance().getCurrentUsage() << " 字节\n";
    
    tracked_map.clear();
    std::cout << "清空TrackedUnorderedMap后内存使用量: " << dkv::MemoryAllocator::getInstance().getCurrentUsage() << " 字节\n";
    
    tracked_set.clear();
    std::cout << "清空TrackedSet后内存使用量: " << dkv::MemoryAllocator::getInstance().getCurrentUsage() << " 字节\n";
    
    std::cout << "\n4. 测试TrackedDeleter\n";
    
    // 重置统计信息，准备测试TrackedDeleter
    dkv::MemoryAllocator::getInstance().resetStats();
    
    // 使用TrackedDeleter
    {   
        std::unique_ptr<TestClass, dkv::TrackedDeleter<TestClass>> tracked_ptr(
            new (dkv::MemoryAllocator::getInstance().allocate(sizeof(TestClass), "TestClass")) TestClass(42),
            dkv::TrackedDeleter<TestClass>());
        
        std::cout << "使用TrackedDeleter后内存使用量: " << dkv::MemoryAllocator::getInstance().getCurrentUsage() << " 字节\n";
        assert(tracked_ptr->getValue() == 42 && "TestClass的值应为42");
        assert(dkv::MemoryAllocator::getInstance().getCurrentUsage() > 0 && "使用TrackedDeleter后内存使用量应大于0");
        
        // 修改值测试
        tracked_ptr->setValue(100);
        assert(tracked_ptr->getValue() == 100 && "修改后TestClass的值应为100");
    }
    
    std::cout << "TrackedDeleter作用域结束后内存使用量: " << dkv::MemoryAllocator::getInstance().getCurrentUsage() << " 字节\n";
    
    std::cout << "\n5. 测试统计信息的准确性\n";
    
    // 重置统计信息
    dkv::MemoryAllocator::getInstance().resetStats();
    assert(dkv::MemoryAllocator::getInstance().getCurrentUsage() == 0 && "重置后内存使用量应为0");
    assert(dkv::MemoryAllocator::getInstance().getTotalAllocations() == 0 && "重置后总分配次数应为0");
    assert(dkv::MemoryAllocator::getInstance().getTotalDeallocations() == 0 && "重置后总释放次数应为0");
    // 分配和释放多个块，测试统计信息
    const int NUM_ALLOCS = 10;
    const size_t ALLOC_SIZE = 64;
    void* ptrs[NUM_ALLOCS] = {nullptr};
    for (int i = 0; i < NUM_ALLOCS; ++i) {
        ptrs[i] = dkv::MemoryAllocator::getInstance().allocate(ALLOC_SIZE, "test_loop");
    }
    assert(dkv::MemoryAllocator::getInstance().getTotalAllocations() == NUM_ALLOCS && "总分配次数应为10");
    assert(dkv::MemoryAllocator::getInstance().getTotalDeallocations() == 0 && "总释放次数应为0");
    for (int i = 0; i < NUM_ALLOCS; ++i) {
        dkv::MemoryAllocator::getInstance().deallocate(ptrs[i]);
    }
    assert(dkv::MemoryAllocator::getInstance().getTotalDeallocations() == NUM_ALLOCS && "总释放次数应为10");
    
    std::cout << "\n6. 打印详细统计信息\n";
    std::cout << dkv::MemoryAllocator::getInstance().getStats() << std::endl;
    
    std::cout << "\n=== 所有测试通过! ===\n";
    
    return 0;
}