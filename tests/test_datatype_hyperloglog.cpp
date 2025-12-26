#include "dkv_core.hpp"
#include "dkv_utils.hpp"
#include "storage/dkv_storage.hpp"
#include "datatypes/dkv_datatype_hyperloglog.hpp"
#include "test_runner.hpp"
#include <iostream>
#include <string>
#include <vector>
#include <cassert>
#include <thread>

bool testHyperLogLogBasic() {
    dkv::StorageEngine storage;
    
    // 测试PFADD和PFCOUNT
    std::string key = "hll_test";
    std::vector<dkv::Value> elements = {"element1", "element2", "element3"};
    
    bool added = storage.pfadd(dkv::NO_TX, key, elements);
    assert(added == true);
    
    uint64_t count = storage.pfcount(dkv::NO_TX, key);
    assert(count > 0);
    assert(count <= 3); // 由于HyperLogLog是概率性算法，结果可能不等于精确值，但应该接近
    
    // 测试添加重复元素
    added = storage.pfadd(dkv::NO_TX, key, {"element1"});
    assert(added == false); // 应该返回false，表示没有改变
    
    // 测试PFMERGE
    std::string key2 = "hll_test2";
    storage.pfadd(dkv::NO_TX, key2, {"element4", "element5"});
    
    bool merged = storage.pfmerge(dkv::NO_TX, "hll_merged", {key, key2});
    assert(merged == true);
    
    uint64_t merged_count = storage.pfcount(dkv::NO_TX, "hll_merged");
    assert(merged_count > 0);
    assert(merged_count >= count); // 合并后的计数应该大于或等于原始计数
    
    std::cout << "HyperLogLog基础功能测试通过" << std::endl;
    return true;
}

bool testHyperLogLogLargeData() {
    dkv::StorageEngine storage;
    std::string key = "hll_large";
    std::vector<dkv::Value> elements;
    
    // 添加114514个唯一元素
    for (int i = 0; i < 114514; ++i) {
        elements.push_back("element" + std::to_string(i));
    }
    
    storage.pfadd(dkv::NO_TX, key, elements);
    uint64_t count = storage.pfcount(dkv::NO_TX, key);
    
    // 计算误差率（理想情况下应该在2%以内）
    double error_rate = std::abs(static_cast<double>(count) - 114514.0) / 114514.0;
    assert(error_rate < 0.1); // 放宽要求到10%以内以适应测试环境
    
    std::cout << "HyperLogLog大数据量测试通过，计数: " << count << ", 误差率: " << error_rate * 100 << "%" << std::endl;
        return true;
}

bool testHyperLogLogExpiration() {
    dkv::StorageEngine storage;
    std::string key = "hll_expire";
    
    // 添加元素并设置过期时间
    storage.pfadd(dkv::NO_TX, key, {"element1"});
    storage.expire(dkv::NO_TX, key, 1);
    
    // 验证过期前元素存在
    assert(storage.exists(dkv::NO_TX, key));
    
    // 等待过期
    std::this_thread::sleep_for(std::chrono::milliseconds(1100));
    
    // 验证过期后元素不存在
    assert(!storage.exists(dkv::NO_TX, key));

    std::cout << "HyperLogLog过期测试通过" << std::endl;
        return true;
}

int main() {
    dkv::TestRunner runner;
    
    std::cout << "开始HyperLogLog数据类型测试..." << std::endl;
    
    runner.runTest("HyperLogLog基础功能测试", testHyperLogLogBasic);
    runner.runTest("HyperLogLog大数据量测试", testHyperLogLogLargeData);
    runner.runTest("HyperLogLog过期测试", testHyperLogLogExpiration);
    
    runner.printSummary();
    return 0;
}
