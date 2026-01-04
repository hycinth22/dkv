#include "storage/dkv_storage.hpp"
#include "datatypes/dkv_datatype_string.hpp"
#include <memory>
#include <vector>
#include <thread>
#include <atomic>
#include <iostream>
#include <string>
#include <benchmark/benchmark.h>
using namespace std;

namespace dkv {

// 存储高并发基准测试的Fixture类
class StorageHighConcurrencyBenchmark : public ::benchmark::Fixture {
public:
    void SetUp(const ::benchmark::State& state) {
        if (state.thread_index() == 0) {
            // 获取桶数量参数
            size_t num_buckets = state.range(0);
            
            // 初始化存储引擎
            storage_engine_ = make_unique<StorageEngine>(TransactionIsolationLevel::READ_COMMITTED, num_buckets);
            
            // 初始化测试数据
            const size_t num_keys = 100000; // 10万条测试数据
            for (size_t i = 0; i < num_keys; ++i) {
                string key = "bench_key_" + to_string(i);
                string value = "value_" + to_string(i);
                storage_engine_->set(NO_TX, key, value);
            }
            total_keys_ = num_keys;
        }
    }
    
    void TearDown(const ::benchmark::State& state) {
        if (state.thread_index() == 0) {
            storage_engine_.reset();
        }
    }
    
protected:
    std::once_flag init_flag;
    unique_ptr<StorageEngine> storage_engine_;
    size_t total_keys_;
};

// 单线程读取不同key的性能测试
BENCHMARK_DEFINE_F(StorageHighConcurrencyBenchmark, BM_SingleThreadReadDifferentKeys)(benchmark::State& state) {
    for (auto _ : state) {
        // 读取不同的key，循环使用总key数
        size_t key_index = state.iterations() % total_keys_;
        string key = "bench_key_" + to_string(key_index);
        benchmark::DoNotOptimize(storage_engine_->get(NO_TX, key));
    }
    
    state.SetItemsProcessed(state.iterations());
}

// 多线程并发读取不同key的性能测试
BENCHMARK_DEFINE_F(StorageHighConcurrencyBenchmark, BM_MultiThreadReadDifferentKeys)(benchmark::State& state) {
    size_t thread_idx = state.thread_index();
    
    for (auto _ : state) {
        // 每个线程读取不同的key范围，避免热点key
        size_t key_index = (thread_idx * 100000 + state.iterations()) % total_keys_;
        string key = "bench_key_" + to_string(key_index);
        benchmark::DoNotOptimize(storage_engine_->get(NO_TX, key));
    }
    
    state.SetItemsProcessed(state.iterations());
}

// 多线程并发读取不同key，测试不同线程数的性能
BENCHMARK_DEFINE_F(StorageHighConcurrencyBenchmark, BM_MultiThreadReadScalability)(benchmark::State& state) {
    size_t thread_idx = state.thread_index();
    
    for (auto _ : state) {
        // 每个线程读取随机分布的key
        size_t key_index = (thread_idx * 31337 + state.iterations() * 1013) % total_keys_;
        string key = "bench_key_" + to_string(key_index);
        benchmark::DoNotOptimize(storage_engine_->get(NO_TX, key));
    }
    
    state.SetItemsProcessed(state.iterations());
}

// 测试不同桶数量下的读取性能
BENCHMARK_DEFINE_F(StorageHighConcurrencyBenchmark, BM_DifferentBucketCount)(benchmark::State& state) {
    for (auto _ : state) {
        string key = "bench_key_BM_DifferentBucketCount";
        benchmark::DoNotOptimize(storage_engine_->get(NO_TX, key));
    }
    
    state.SetItemsProcessed(state.iterations());
}

// 高并发读写混合测试（读多写少，模拟真实场景）
BENCHMARK_DEFINE_F(StorageHighConcurrencyBenchmark, BM_MixedReadWrite)(benchmark::State& state) {
    size_t thread_idx = state.thread_index();
    
    for (auto _ : state) {
        // 读多写少：90%读，10%写
        size_t operation = (thread_idx * 31337 + state.iterations() * 1013) % 10;
        size_t key_index = (thread_idx * 100000 + state.iterations()) % total_keys_;
        string key = "bench_key_" + to_string(key_index);
        
        if (operation < 9) {
            // 90%读取操作
            benchmark::DoNotOptimize(storage_engine_->get(NO_TX, key));
        } else {
            // 10%写入操作（交替set和del）
            if (operation % 2 == 0) {
                string value = "updated_value_" + to_string(state.iterations());
                storage_engine_->set(NO_TX, key, value);
            } else {
                storage_engine_->del(NO_TX, key);
            }
        }
    }
    
    state.SetItemsProcessed(state.iterations());
}

// 注册基准测试
BENCHMARK_REGISTER_F(StorageHighConcurrencyBenchmark, BM_SingleThreadReadDifferentKeys)
    ->Arg(1024) // 使用1024个桶
    ->UseRealTime();

BENCHMARK_REGISTER_F(StorageHighConcurrencyBenchmark, BM_MultiThreadReadDifferentKeys)
    ->Arg(1024)
    ->ThreadRange(1, 32)
    ->UseRealTime();

BENCHMARK_REGISTER_F(StorageHighConcurrencyBenchmark, BM_MultiThreadReadScalability)
    ->Arg(1024)
    ->ThreadRange(1, 32)
    ->UseRealTime();

BENCHMARK_REGISTER_F(StorageHighConcurrencyBenchmark, BM_DifferentBucketCount)
    ->Arg(1)
    ->Arg(256)
    ->Arg(512)
    ->Arg(1024)
    ->Arg(2048)
    ->Arg(4096)
    ->Arg(8192)
    ->Threads(32)
    ->UseRealTime();

// 注册读写混合测试
BENCHMARK_REGISTER_F(StorageHighConcurrencyBenchmark, BM_MixedReadWrite)
    ->Arg(1024)
    ->ThreadRange(1, 32)
    ->UseRealTime();

} // namespace dkv

int main(int argc, char** argv) {
    // 初始化Google Benchmark
    ::benchmark::Initialize(&argc, argv);
    
    // 如果没有指定基准测试，显示帮助信息
    if (::benchmark::ReportUnrecognizedArguments(argc, argv)) {
        return 1;
    }
    
    // 运行所有注册的基准测试
    ::benchmark::RunSpecifiedBenchmarks();
    
    return 0;
}
