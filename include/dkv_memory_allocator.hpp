#ifndef DKV_MEMORY_ALLOCATOR_HPP
#define DKV_MEMORY_ALLOCATOR_HPP

#include <cstddef>
#include <mutex>
#include <atomic>
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include <string>
#include <memory>

namespace dkv {

// 自定义内存分配器，用于跟踪所有内存分配、重分配和释放操作
class MemoryAllocator {
public:
    // 获取单例实例
    static MemoryAllocator& getInstance() {
        static MemoryAllocator instance;
        return instance;
    }

    // 分配内存
    void* allocate(size_t size, const char* allocation_type = "unknown");
    
    // 释放内存
    void deallocate(void* ptr);
    
    // 重新分配内存
    void* reallocate(void* ptr, size_t new_size, const char* allocation_type = "unknown");
    
    // 获取当前内存使用量
    size_t getCurrentUsage() const;
    
    // 获取总分配次数
    uint64_t getTotalAllocations() const;
    
    // 获取总释放次数
    uint64_t getTotalDeallocations() const;
    
    // 打印内存使用统计信息
    std::string getStats() const;
    
    // 重置统计信息
    void resetStats();

private:
    // 私有构造函数（单例模式）
    MemoryAllocator() = default;
    
    // 禁止拷贝和移动
    MemoryAllocator(const MemoryAllocator&) = delete;
    MemoryAllocator& operator=(const MemoryAllocator&) = delete;
    MemoryAllocator(MemoryAllocator&&) = delete;
    MemoryAllocator& operator=(MemoryAllocator&&) = delete;
    
    // 内存块信息结构
    struct MemoryBlockInfo {
        size_t size;           // 块大小
        std::string type;      // 分配类型
        uint64_t allocation_id;// 分配ID
    };
    
    // 同步锁
    mutable std::mutex mutex_;
    
    // 内存使用统计
    std::atomic<size_t> current_usage_{0};
    std::atomic<uint64_t> total_allocations_{0};
    std::atomic<uint64_t> total_deallocations_{0};
    std::atomic<uint64_t> allocation_counter_{0};
    
    // 内存块映射表
    std::unordered_map<void*, MemoryBlockInfo> memory_blocks_;
};

// 自定义分配器模板，用于std容器
template<typename T>
class TrackedAllocator {
public:
    using value_type = T;
    using pointer = T*;
    using const_pointer = const T*;
    using reference = T&;
    using const_reference = const T&;
    using size_type = std::size_t;
    using difference_type = std::ptrdiff_t;
    
    template<typename U>
    struct rebind {
        using other = TrackedAllocator<U>;
    };
    
    TrackedAllocator() = default;
    
    template<typename U>
    TrackedAllocator(const TrackedAllocator<U>&) {}
    
    pointer allocate(size_type n) {
        void* ptr = MemoryAllocator::getInstance().allocate(n * sizeof(T), typeid(T).name());
        return static_cast<pointer>(ptr);
    }
    
    void deallocate(pointer p, size_type) {
        MemoryAllocator::getInstance().deallocate(p);
    }
    
    bool operator==(const TrackedAllocator&) const {
        return true;
    }
    
    bool operator!=(const TrackedAllocator&) const {
        return false;
    }

    template<typename U>
    operator TrackedAllocator<U>() const {
        return TrackedAllocator<U>();
    }
};

// 辅助函数，用于跟踪标准容器的内存使用
template<typename T> using TrackedVector = std::vector<T, TrackedAllocator<T>>;
template<typename K, typename V> using TrackedUnorderedMap = std::unordered_map<K, V, std::hash<K>, std::equal_to<K>, TrackedAllocator<std::pair<const K, V>>>;
template<typename T> using TrackedSet = std::unordered_set<T, std::hash<T>, std::equal_to<T>, TrackedAllocator<T>>;

template<typename T>
struct TrackedDeleter {
    TrackedDeleter() noexcept = default;
    void operator()(T* ptr) const {
        if (ptr) {
            // 调用析构函数
            ptr->~T();
            // 使用 MemoryAllocator 释放内存
            MemoryAllocator::getInstance().deallocate(ptr);
        }
    }
};

} // namespace dkv

#endif // DKV_MEMORY_ALLOCATOR_HPP