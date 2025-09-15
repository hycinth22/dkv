#include "dkv_memory_allocator.hpp"
#include <cstdlib>
#include <iostream>
#include <sstream>
#include <new>

// 全局操作符重载，使用自定义MemoryAllocator
// 1. 基本单对象分配和释放
void* operator new(size_t size) {
    return dkv::MemoryAllocator::getInstance().allocate(size, "global_new");
}
void* operator new[](size_t size) {
    return dkv::MemoryAllocator::getInstance().allocate(size, "global_new_array");
}
void* operator new(size_t size, std::align_val_t) {
    return dkv::MemoryAllocator::getInstance().allocate(size, "global_new_aligned");
}
void* operator new[](size_t size, std::align_val_t) {
    return dkv::MemoryAllocator::getInstance().allocate(size, "global_new_array_aligned");
}
void* operator new(size_t size, const std::nothrow_t&) noexcept {
    try {
        return dkv::MemoryAllocator::getInstance().allocate(size, "global_new_nothrow");
    } catch (...) {
        return nullptr;
    }
}
void* operator new[](size_t size, const std::nothrow_t&) noexcept {
    try {
        return dkv::MemoryAllocator::getInstance().allocate(size, "global_new_array_nothrow");
    } catch (...) {
        return nullptr;
    }
}
void* operator new(size_t size, std::align_val_t, const std::nothrow_t&) noexcept {
    try {
        return dkv::MemoryAllocator::getInstance().allocate(size, "global_new_aligned_nothrow");
    } catch (...) {
        return nullptr;
    }
}
void* operator new[](size_t size, std::align_val_t, const std::nothrow_t&) noexcept {
    try {
        return dkv::MemoryAllocator::getInstance().allocate(size, "global_new_array_aligned_nothrow");
    } catch (...) {
        return nullptr;
    }
}

void operator delete(void* ptr) noexcept {
    dkv::MemoryAllocator::getInstance().deallocate(ptr);
}
void operator delete[](void* ptr) noexcept {
    dkv::MemoryAllocator::getInstance().deallocate(ptr);
}
void operator delete(void* ptr, std::align_val_t) noexcept {
    dkv::MemoryAllocator::getInstance().deallocate(ptr);
}
void operator delete[](void* ptr, std::align_val_t) noexcept {
    dkv::MemoryAllocator::getInstance().deallocate(ptr);
}
void operator delete(void* ptr, size_t) noexcept {
    dkv::MemoryAllocator::getInstance().deallocate(ptr);
}
void operator delete[](void* ptr, size_t) noexcept {
    dkv::MemoryAllocator::getInstance().deallocate(ptr);
}
void operator delete(void* ptr, size_t, std::align_val_t) noexcept {
    dkv::MemoryAllocator::getInstance().deallocate(ptr);
}
void operator delete[](void* ptr, size_t, std::align_val_t) noexcept {
    dkv::MemoryAllocator::getInstance().deallocate(ptr);
}

namespace dkv {

thread_local bool enter;

// EnterGuard类：RAII模式，在构造时设置enter=true，析构时设置enter=false
class EnterGuard {
public:
    EnterGuard() : is_first_(!enter) {
        if (!enter) {
            enter = true;
        }
    }
    
    ~EnterGuard() {
        if (is_first_) {
            enter = false;
        }
    }
    
    // 判断是否是第一次进入（用于检测递归调用）
    bool isFirst() const {
        return is_first_;
    }
    
private:
    bool is_first_; // 记录是否是第一次进入
    
    // 禁止拷贝和赋值
    EnterGuard(const EnterGuard&) = delete;
    EnterGuard& operator=(const EnterGuard&) = delete;
};

void* MemoryAllocator::allocate(size_t size, const char* allocation_type) {
    if (size == 0) {
        return nullptr;
    }
    EnterGuard guard;
    // 如果是递归调用，直接分配内存而不进行统计
    if (!guard.isFirst()) {
        void* ptr = std::malloc(size);
        if (!ptr) {
            throw std::bad_alloc();
        }
        return ptr;
    }
    
    // 分配内存
    void* ptr = std::malloc(size);
    if (!ptr) {
        throw std::bad_alloc();
    }
    
    // 记录内存使用情况
    {
        std::lock_guard<std::mutex> lock(mutex_);
        uint64_t allocation_id = ++allocation_counter_;
        memory_blocks_[ptr] = {size, allocation_type, allocation_id};
        current_usage_ += size;
        total_allocations_++;
    }
    
    return ptr;
}

void MemoryAllocator::deallocate(void* ptr) {
    if (!ptr) {
        return;
    }
    EnterGuard guard;
    // 如果是递归调用，直接释放内存而不进行统计更新
    if (!guard.isFirst()) {
        std::free(ptr);
        return;
    }

    // 释放内存并更新统计信息
    {
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = memory_blocks_.find(ptr);
        if (it != memory_blocks_.end()) {
            current_usage_ -= it->second.size;
            memory_blocks_.erase(it);
            total_deallocations_++;
        }
    }
    
    std::free(ptr);
}

void* MemoryAllocator::reallocate(void* ptr, size_t new_size, const char* allocation_type) {
    if (new_size == 0) {
        deallocate(ptr);
        return nullptr;
    }
    
    if (!ptr) {
        return allocate(new_size, allocation_type);
    }
    EnterGuard guard;
    // 如果是递归调用，直接重新分配内存而不进行统计更新
    if (!guard.isFirst()) {
        return std::realloc(ptr, new_size);
    }

    // 计算新旧内存块的大小差异
    size_t old_size = 0;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = memory_blocks_.find(ptr);
        if (it != memory_blocks_.end()) {
            old_size = it->second.size;
        }
    }
    
    // 重新分配内存
    void* new_ptr = std::realloc(ptr, new_size);
    if (!new_ptr) {
        throw std::bad_alloc();
    }
    
    // 更新内存使用统计
    if (new_ptr != ptr) {
        // 如果返回新指针，需要从旧指针记录中移除并添加新指针记录
        {
            std::lock_guard<std::mutex> lock(mutex_);
            auto it = memory_blocks_.find(ptr);
            if (it != memory_blocks_.end()) {
                memory_blocks_.erase(it);
            }
            
            uint64_t allocation_id = ++allocation_counter_;
            memory_blocks_[new_ptr] = {new_size, allocation_type, allocation_id};
            current_usage_ = current_usage_ - old_size + new_size;
            total_allocations_++;
            total_deallocations_++;
        }
    } else {
        // 如果返回相同指针，只需更新记录
        {
            std::lock_guard<std::mutex> lock(mutex_);
            auto it = memory_blocks_.find(ptr);
            if (it != memory_blocks_.end()) {
                current_usage_ = current_usage_ - old_size + new_size;
                it->second.size = new_size;
                if (allocation_type) {
                    it->second.type = allocation_type;
                }
            }
        }
    }
    
    return new_ptr;
}

size_t MemoryAllocator::getCurrentUsage() const {
    return current_usage_.load();
}

uint64_t MemoryAllocator::getTotalAllocations() const {
    return total_allocations_.load();
}

uint64_t MemoryAllocator::getTotalDeallocations() const {
    return total_deallocations_.load();
}

std::string MemoryAllocator::getStats() const {
    EnterGuard guard;

    std::ostringstream oss;
    oss << "# Memory Allocator Stats\n";
    oss << "current_usage:" << getCurrentUsage() << " bytes\n";
    oss << "total_allocations:" << getTotalAllocations() << "\n";
    oss << "total_deallocations:" << getTotalDeallocations() << "\n";
    oss << "active_allocations:" << (getTotalAllocations() - getTotalDeallocations()) << "\n";
    
    // 可以在这里添加更详细的统计信息
    {
        std::lock_guard<std::mutex> lock(mutex_);
        oss << "allocation_types:";
        std::unordered_map<std::string, size_t> type_counts;
        std::unordered_map<std::string, size_t> type_sizes;
        
        for (const auto& block : memory_blocks_) {
            type_counts[block.second.type]++;
            type_sizes[block.second.type] += block.second.size;
        }
        
        bool first = true;
        for (const auto& type_count : type_counts) {
            if (!first) {
                oss << ",";
            }
            oss << type_count.first << ":" << type_count.second << "(" << type_sizes[type_count.first] << "B)";
            first = false;
        }
        oss << "\n";
    }
    
    return oss.str();
}

void MemoryAllocator::resetStats() {
    EnterGuard guard;

    std::lock_guard<std::mutex> lock(mutex_);
    current_usage_ = 0;
    total_allocations_ = 0;
    total_deallocations_ = 0;
    allocation_counter_ = 0;
    memory_blocks_.clear();
}

} // namespace dkv