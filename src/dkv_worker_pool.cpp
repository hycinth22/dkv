#include "dkv_worker_pool.hpp"
#include "dkv_core.hpp"
#include "dkv_network.hpp"
#include "dkv_server.hpp"
#include "dkv_logger.hpp"
#include <stdexcept>

namespace dkv {

WorkerThreadPool::WorkerThreadPool(DKVServer* server, size_t num_threads) 
    : server_(server), stop_(false) {
    // 创建工作线程 
    for (size_t i = 0; i < num_threads; ++i) {
        workers_.emplace_back(&WorkerThreadPool::workerThread, this);
    }
    
    DKV_LOG_INFO("工作线程池已创建，线程数: ", num_threads);
}

WorkerThreadPool::~WorkerThreadPool() {
    stop();
}

void WorkerThreadPool::enqueue(const CommandTask& task) {
    {
        std::lock_guard<std::mutex> lock(queue_mutex_);
        if (stop_.load()) {
            throw std::runtime_error("线程池已停止，无法添加新任务");
        }
        task_queue_.push(task);
    }
    
    // 通知一个等待的工作线程
    condition_.notify_one();
}

void WorkerThreadPool::stop() {
    {
        std::lock_guard<std::mutex> lock(queue_mutex_);
        stop_.store(true);
    }
    
    // 通知所有工作线程
    condition_.notify_all();
    
    // 等待所有工作线程完成
    for (auto& worker : workers_) {
        if (worker.joinable()) {
            worker.join();
        }
    }
}

Response WorkerThreadPool::executeCommand(const Command& command) {
    if (!server_) {
        return Response(ResponseStatus::ERROR, "DKV server not initialized");
    }
    
    return server_->executeCommand(command);
}

void WorkerThreadPool::workerThread() {
    CommandTask task;
    
    while (true) {
        {
            std::unique_lock<std::mutex> lock(queue_mutex_);
            
            // 等待直到有任务或线程池停止
            condition_.wait(lock, [this] { 
                return stop_.load() || !task_queue_.empty(); 
            });
            
            // 如果线程池已停止且任务队列为空，则退出
            if (stop_.load() && task_queue_.empty()) {
                break;
            }
            
            // 获取任务
            task = std::move(task_queue_.front());
            task_queue_.pop();
        }
        
        try {
            // 执行命令
            Response response;
            if (task.command.type != CommandType::UNKNOWN) {
                response = executeCommand(task.command);
                // 调用SubReactor处理结果
                if (task.sub_reactor) {
                    task.sub_reactor->handleCommandResult(task.client_fd, response);
                }
            }
        } catch (const std::exception& e) {
            DKV_LOG_ERROR("工作线程执行任务时出错: ", e.what());
        }
    }
}

} // namespace dkv