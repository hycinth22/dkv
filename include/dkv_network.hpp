#pragma once

#include "dkv_core.hpp"
#include "dkv_storage.hpp"
#include "dkv_resp.hpp"
#include <sys/epoll.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <fcntl.h>
#include <vector>
#include <queue>
#include <thread>
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
// 网络服务器
class NetworkServer {
private:
    int server_fd_;
    int epoll_fd_;
    sockaddr_in server_addr_;
    std::atomic<bool> running_;
    StorageEngine* storage_engine_;
    DKVServer* server_;
    
    // 客户端连接管理
    std::unordered_map<int, std::unique_ptr<ClientConnection>> clients_;
    std::mutex clients_mutex_;
    
    // 事件循环线程
    std::thread event_loop_thread_;
    
public:
    NetworkServer(int port = 6379);
    ~NetworkServer();
    
    // 启动和停止服务器
    bool start();
    void stop();
    
    // 设置存储引擎
    void setStorageEngine(StorageEngine* storage_engine);
    
    // 设置DKV服务器
    void setDKVServer(DKVServer* server);
    
private:
    // 初始化服务器
    bool initializeServer(int port);
    
    // 事件循环
    void eventLoop();
    
    // 处理新连接
    void handleNewConnection();
    
    // 处理客户端数据
    void handleClientData(int client_fd);
    
    // 处理客户端断开
    void handleClientDisconnect(int client_fd);
    
    // 执行命令
    Response executeCommand(const Command& command);
    
    // 发送响应
    void sendResponse(int client_fd, const Response& response);
    
    // 设置非阻塞模式
    bool setNonBlocking(int fd);
    
    // 添加epoll事件
    bool addEpollEvent(int fd, uint32_t events);
    
    // 修改epoll事件
    bool modifyEpollEvent(int fd, uint32_t events);
    
    // 删除epoll事件
    bool removeEpollEvent(int fd);
};

} // namespace dkv
