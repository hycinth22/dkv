#include "storage/dkv_storage.hpp"
#include "datatypes/dkv_datatype_string.hpp"
#include "transaction/dkv_transaction_manager.hpp"
#include "storage/dkv_simple_storage.h"
#include <memory>
#include <vector>
#include <thread>
#include <atomic>
#include <iostream>
#include <string>
#include <benchmark/benchmark.h>
#include <iostream>
using namespace std;

namespace dkv {

// Google Benchmark的通用设置类
class MVCCBenchmark : public ::benchmark::Fixture {
public:
    void SetUp(const ::benchmark::State& state) {
        inner_storage_ = new SimpleInnerStorage();
        mvcc_ = new MVCC(*inner_storage_);
        engine_ = new StorageEngine<SimpleInnerStorage>();
        tx_manager_ = new TransactionManager(engine_, TransactionIsolationLevel::REPEATABLE_READ);
        
        // 初始化一些数据用于GET测试
        for (int i = 0; i < 1000; ++i) {
            TransactionID tx_id = tx_manager_->begin();
            string key = "bench_key_" + to_string(i);
            auto item = make_unique<StringItem>("value_" + to_string(i));
            mvcc_->set(tx_id, key, move(item));
            tx_manager_->commit(tx_id);
        }
        clog << "SetUp!" << endl;
    }
    
    void TearDown(const ::benchmark::State& state) {
        delete tx_manager_;
        delete engine_;
        delete mvcc_;
        delete inner_storage_;
        clog << "TearDown!" << endl;
    }
    
protected:
    SimpleInnerStorage* inner_storage_;
    MVCC* mvcc_;
    StorageEngine* engine_;
    TransactionManager* tx_manager_;
};

// 单线程SET操作基准测试
BENCHMARK_F(MVCCBenchmark, BM_MVCCSet)(benchmark::State& state) {
    int thread_idx = state.thread_index();
    
    for (auto _ : state) {
        // 每个迭代执行一次SET操作
        TransactionID tx_id = tx_manager_->begin();
        string key = "bench_key_" + to_string((thread_idx * 100000 + state.iterations()) % 1000);
        auto item = make_unique<StringItem>("value_" + to_string(state.iterations()));
        mvcc_->set(tx_id, key, move(item));
        tx_manager_->commit(tx_id);
    }
    
    // 设置计数器，记录每个线程执行的操作数
    state.SetItemsProcessed(state.iterations());
}

// 单线程GET操作基准测试
BENCHMARK_F(MVCCBenchmark, BM_MVCCGet)(benchmark::State& state) {
    int thread_idx = state.thread_index();
    
    for (auto _ : state) {
        // 每个迭代执行一次GET操作
        TransactionID tx_id = tx_manager_->begin();
        ReadView read_view = mvcc_->createReadView(tx_id, *tx_manager_);
        string key = "bench_key_" + to_string((thread_idx * 100000 + state.iterations()) % 1000);
        benchmark::DoNotOptimize(mvcc_->get(read_view, key));
        tx_manager_->commit(tx_id);
    }
    
    // 设置计数器，记录每个线程执行的操作数
    state.SetItemsProcessed(state.iterations());
}

// 单线程混合操作基准测试
BENCHMARK_F(MVCCBenchmark, BM_MVCCHybrid)(benchmark::State& state) {
    int thread_idx = state.thread_index();
    int successful_ops = 0;
    
    for (auto _ : state) {
        TransactionID tx_id = tx_manager_->begin();
        string key = "bench_key_" + to_string((thread_idx * 100000 + state.iterations()) % 1000);
        
        // 随机选择操作类型
        int op_type = state.iterations() % 3;
        if (op_type == 0) {
            // 读取操作
            ReadView read_view = mvcc_->createReadView(tx_id, *tx_manager_);
            if (mvcc_->get(read_view, key) != nullptr) {
                successful_ops++;
            }
        } else if (op_type == 1) {
            // 更新操作
            auto item = make_unique<StringItem>("updated_value_" + to_string(state.iterations()));
            if (mvcc_->set(tx_id, key, move(item))) {
                successful_ops++;
            }
        } else {
            // 删除操作
            auto virtual_item = make_unique<StringItem>();
            if (mvcc_->del(tx_id, key, move(virtual_item))) {
                successful_ops++;
            }
        }
        
        tx_manager_->commit(tx_id);
    }
    
    // 设置计数器，记录成功的操作数
    state.SetItemsProcessed(successful_ops);
}

// 多线程并发操作基准测试
BENCHMARK_F(MVCCBenchmark, BM_MVCCConcurrent)(benchmark::State& state) {
    int thread_idx = state.thread_index();
    atomic<int> local_successful_ops(0);
    
    for (auto _ : state) {
        TransactionID tx_id = tx_manager_->begin();
        string key = "concurrent_key_" + to_string((thread_idx * 10 + state.iterations()) % 1000);
        
        // 交替执行不同操作
        int op_type = state.iterations() % 4;
        if (op_type == 0) {
            // 读取操作
            ReadView read_view = mvcc_->createReadView(tx_id, *tx_manager_);
            benchmark::DoNotOptimize(mvcc_->get(read_view, key));
            local_successful_ops++;
        } else if (op_type == 1 || op_type == 2) {
            // 更新操作
            auto item = make_unique<StringItem>("thread_" + to_string(thread_idx) + "_value_" + to_string(state.iterations()));
            if (mvcc_->set(tx_id, key, move(item))) {
                local_successful_ops++;
            }
        } else {
            // 删除后立即添加回来
            auto virtual_item = make_unique<StringItem>();
            if (mvcc_->del(tx_id, key, move(virtual_item))) {
                local_successful_ops++;
                // 立即添加回来
                auto new_item = make_unique<StringItem>("re_added_value_" + to_string(state.iterations()));
                if (mvcc_->set(tx_id, key, move(new_item))) {
                    local_successful_ops++;
                }
            }
        }
        
        tx_manager_->commit(tx_id);
    }
    
    // 设置计数器，记录成功的操作数
    state.SetItemsProcessed(local_successful_ops);
}

// 设置并发线程数量
BENCHMARK_REGISTER_F(MVCCBenchmark, BM_MVCCConcurrent)
    ->ThreadRange(1, 8)
    ->UseRealTime();

// 不同事务隔离级别性能测试 - 专用的Fixture类
class MVCCIsolationBenchmark : public ::benchmark::Fixture {
public:
    void SetUp(const ::benchmark::State& state) {
        // 根据参数设置隔离级别
        TransactionIsolationLevel level = state.range(0) == 0 ? 
                                          TransactionIsolationLevel::READ_COMMITTED : 
                                          TransactionIsolationLevel::REPEATABLE_READ;
        
        inner_storage_ = new InnerStorage();
        mvcc_ = new MVCC(*inner_storage_);
        engine_ = new StorageEngine<SimpleInnerStorage>();
        tx_manager_ = new TransactionManager(engine_, level);
        isolation_level_ = level;
        
        // 初始化一些数据
        for (int i = 0; i < 1000; ++i) {
            TransactionID tx_id = tx_manager_->begin();
            string key = "iso_key_" + to_string(i);
            auto item = make_unique<StringItem>("initial_value_" + to_string(i));
            mvcc_->set(tx_id, key, move(item));
            tx_manager_->commit(tx_id);
        }
    }
    
    void TearDown(const ::benchmark::State& state) {
        delete tx_manager_;
        delete engine_;
        delete mvcc_;
        delete inner_storage_;
    }
    
protected:
    InnerStorage* inner_storage_;
    MVCC* mvcc_;
    StorageEngine* engine_;
    TransactionManager* tx_manager_;
    TransactionIsolationLevel isolation_level_;
};

// 隔离级别性能测试
BENCHMARK_F(MVCCIsolationBenchmark, BM_MVCCIsolationLevel)(benchmark::State& state) {
    for (auto _ : state) {
        TransactionID tx_id = tx_manager_->begin();
        string key = "iso_key_" + to_string(state.iterations() % 1000);
        
        if (isolation_level_ == TransactionIsolationLevel::READ_COMMITTED) {
            // READ COMMITTED - 每次读取都创建新的ReadView
            if (state.iterations() % 2 == 0) {
                ReadView read_view = mvcc_->createReadView(tx_id, *tx_manager_);
                benchmark::DoNotOptimize(mvcc_->get(read_view, key));
            } else {
                auto item = make_unique<StringItem>("rc_value_" + to_string(state.iterations()));
                mvcc_->set(tx_id, key, move(item));
            }
        } else {
            // REPEATABLE READ - 在事务开始时创建一次ReadView
            ReadView read_view = mvcc_->createReadView(tx_id, *tx_manager_);
            if (state.iterations() % 2 == 0) {
                benchmark::DoNotOptimize(mvcc_->get(read_view, key));
            } else {
                auto item = make_unique<StringItem>("rr_value_" + to_string(state.iterations()));
                mvcc_->set(tx_id, key, move(item));
            }
        }
        
        tx_manager_->commit(tx_id);
    }
    
    state.SetItemsProcessed(state.iterations());
}

// 注册不同隔离级别的测试
BENCHMARK_REGISTER_F(MVCCIsolationBenchmark, BM_MVCCIsolationLevel)
    ->Args({0}) // READ_COMMITTED
    ->Args({1}); // REPEATABLE_READ

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