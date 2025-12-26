#pragma once

#include "../dkv_core.hpp"
#include "../storage/dkv_storage.hpp"
#include "../net/dkv_resp.hpp"
#include "../dkv_worker_pool.hpp"
#include <sys/epoll.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <fcntl.h>
#include <vector>
#include <queue>
#include <thread>
#include <condition_variable>
#include <atomic>

namespace dkv {

// 客户端连接信息
struct ClientConnection {
    int fd;
    sockaddr_in addr;
    std::string read_buffer;
    std::string write_buffer;
    bool connected;
    
    ClientConnection(int socket_fd, const sockaddr_in& address) 
        : fd(socket_fd), addr(address), connected(true) {}
    
    ~ClientConnection() {
        if (fd >= 0) {
            close(fd);
        }
    }
};


class DKVServer;
class CommandTask;

// 子Reactor，处理IO事件
class SubReactor {
private:
    int epoll_fd_;
    std::atomic<bool> running_;
    std::thread event_loop_thread_;
    std::unordered_map<int, std::unique_ptr<ClientConnection>> clients_;
    std::mutex clients_mutex_;
    std::queue<CommandTask> task_queue_;
    std::mutex task_queue_mutex_;
    WorkerThreadPool* worker_pool_;

public:
    SubReactor(WorkerThreadPool* worker_pool);
    ~SubReactor();

    void setWorkerPool(WorkerThreadPool* worker_pool);
    bool start();
    void stop();
    int getEpollFd() const { return epoll_fd_; }
    
    // 添加客户端连接到子Reactor
    void addClient(int client_fd, const sockaddr_in& client_addr);
    
    // 处理命令结果
    void handleCommandResult(int client_fd, const Response& response);
    
private:
    void eventLoop();
    void handleClientData(int client_fd);
    void handleClientDisconnect(int client_fd);
    void handleClientDisconnect_locked(int client_fd);
    void sendResponse(int client_fd, const Response& response);
    bool setNonBlocking(int fd);
    bool addEpollEvent(int fd, uint32_t events);
    bool modifyEpollEvent(int fd, uint32_t events);
    bool removeEpollEvent(int fd);
};



// 网络服务器
class NetworkServer {
private:
    int server_fd_;
    int epoll_fd_;
    sockaddr_in server_addr_;
    std::atomic<bool> running_;
    
    // 多线程Reactor相关
    std::vector<std::unique_ptr<SubReactor>> sub_reactors_;
    
    // 主事件循环线程
    std::thread main_event_loop_thread_;
    
public:
    NetworkServer(WorkerThreadPool* worker_pool, int port = 6379, size_t num_sub_reactors = 4);
    ~NetworkServer();
    
    // 启动和停止服务器
    bool start();
    void stop();

    // 设置DKV服务器
    void setDKVServer(DKVServer* server);

private:
    // 初始化服务器
    bool initializeServer(int port);
    
    // 主事件循环（仅处理新连接）
    void mainEventLoop();
    
    // 处理新连接并分配给子Reactor
    void handleNewConnection();
    
    // 设置非阻塞模式
    bool setNonBlocking(int fd);
    
    // 添加epoll事件
    bool addEpollEvent(int fd, uint32_t events);
    bool removeEpollEvent(int fd);
    
    // 修改epoll事件
    bool modifyEpollEvent(int fd, uint32_t events);
    

};

} // namespace dkv
