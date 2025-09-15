#pragma once

#include "dkv_core.hpp"
#include <vector>
#include <queue>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <atomic>

namespace dkv {

class SubReactor;
class Command;

// 命令任务，用于线程池执行
struct CommandTask {
    SubReactor* sub_reactor;
    Command command;
    int client_fd;
    std::string client_read_buffer;
    size_t parsed_pos;
};

class DKVServer;

// 工作线程池，执行命令
class WorkerThreadPool {
private:
    DKVServer* server_;
    std::atomic<bool> stop_;

    std::vector<std::thread> workers_;
    std::queue<CommandTask> task_queue_;
    std::mutex queue_mutex_;
    std::condition_variable condition_;
public:
    WorkerThreadPool(DKVServer* server, size_t num_threads = 4);
    ~WorkerThreadPool();
    
    // 提交任务到线程池
    void enqueue(const CommandTask& task);
    
    // 停止线程池
    void stop();

private:
    // 工作线程函数
    void workerThread();

    Response executeCommand(const Command& command);
};

} // namespace dkv