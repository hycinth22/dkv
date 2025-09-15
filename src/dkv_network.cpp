#include "dkv_network.hpp"
#include <iostream>
#include <cstring>
#include <algorithm>
#include <thread>
#include <chrono>
#include "dkv_server.hpp"
#include "dkv_worker_pool.hpp"

namespace dkv {

// NetworkServer 实现
NetworkServer::NetworkServer(WorkerThreadPool* worker_pool, int port, size_t num_sub_reactors)
    : server_fd_(-1), epoll_fd_(-1), running_(false) {
    memset(&server_addr_, 0, sizeof(server_addr_));
    server_addr_.sin_family = AF_INET;
    server_addr_.sin_addr.s_addr = INADDR_ANY;
    server_addr_.sin_port = htons(port);
    
    // 创建子Reactor
    for (size_t i = 0; i < num_sub_reactors; ++i) {
        sub_reactors_.emplace_back(std::make_unique<SubReactor>(worker_pool));
    }
}

NetworkServer::~NetworkServer() {
    stop();
}

bool NetworkServer::start() {
    if (!initializeServer(server_addr_.sin_port)) {
        return false;
    }
    
    running_ = true;
    
    // 启动所有子Reactor
    for (auto& reactor : sub_reactors_) {
        if (!reactor->start()) {
            std::cerr << "启动子Reactor失败" << std::endl;
            stop();
            return false;
        }
    }
    
    // 启动主事件循环线程（仅处理新连接）
    main_event_loop_thread_ = std::thread(&NetworkServer::mainEventLoop, this);
    
    std::cout << "DKV服务器启动成功（多线程Reactor模式），监听端口: " 
              << ntohs(server_addr_.sin_port) << std::endl;
    std::cout << "子Reactor数量: " << sub_reactors_.size() << std::endl;
    
    return true;
}

void NetworkServer::stop() {
    if (!running_.load()) {
        return;
    }
    running_ = false;
    
    // 等待主事件循环线程结束
    if (main_event_loop_thread_.joinable()) {
        main_event_loop_thread_.join();
    }
    
    // 停止所有子Reactor
    for (auto& reactor : sub_reactors_) {
        reactor->stop();
    }

    if (epoll_fd_ >= 0) {
        close(epoll_fd_);
        epoll_fd_ = -1;
    }
    
    if (server_fd_ >= 0) {
        close(server_fd_);
        server_fd_ = -1;
    }
    
    std::cout << "DKV网络服务已停止" << std::endl;
}

bool NetworkServer::initializeServer(int /*port*/) {
    // 创建socket
    server_fd_ = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd_ < 0) {
        std::cerr << "创建socket失败" << std::endl;
        return false;
    }
    
    // 设置socket选项
    int opt = 1;
    if (setsockopt(server_fd_, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) < 0) {
        std::cerr << "设置socket选项失败" << std::endl;
        close(server_fd_);
        return false;
    }
    
    // 设置非阻塞模式
    if (!setNonBlocking(server_fd_)) {
        std::cerr << "设置非阻塞模式失败" << std::endl;
        close(server_fd_);
        return false;
    }
    
    // 绑定地址
    if (bind(server_fd_, (struct sockaddr*)&server_addr_, sizeof(server_addr_)) < 0) {
        std::cerr << "绑定地址失败" << std::endl;
        close(server_fd_);
        return false;
    }
    
    // 开始监听
    if (listen(server_fd_, 128) < 0) {
        std::cerr << "开始监听失败" << std::endl;
        close(server_fd_);
        return false;
    }
    
    // 创建主Reactor的epoll实例
    epoll_fd_ = epoll_create1(0);
    if (epoll_fd_ < 0) {
        std::cerr << "创建epoll实例失败" << std::endl;
        close(server_fd_);
        return false;
    }
    
    // 添加服务器socket到主Reactor的epoll
    if (!addEpollEvent(server_fd_, EPOLLIN)) {
        std::cerr << "添加服务器socket到epoll失败" << std::endl;
        close(epoll_fd_);
        close(server_fd_);
        return false;
    }
    
    return true;
}

void NetworkServer::mainEventLoop() {
    const int MAX_EVENTS = 128;
    struct epoll_event events[MAX_EVENTS];
    
    while (running_) {
        int num_events = epoll_wait(epoll_fd_, events, MAX_EVENTS, 100); // 100ms超时
        
        if (num_events < 0) {
            if (errno == EINTR) {
                continue;
            }
            std::cerr << "主Reactor epoll_wait失败: " << strerror(errno) << std::endl;
            break;
        }
        
        for (int i = 0; i < num_events; i++) {
            int fd = events[i].data.fd;
            uint32_t event_mask = events[i].events;
            
            if (fd == server_fd_ && (event_mask & EPOLLIN)) {
                // 新连接
                handleNewConnection();
            }
        }
    }
}

void NetworkServer::handleNewConnection() {
    struct sockaddr_in client_addr;
    socklen_t client_len = sizeof(client_addr);
    
    // 尽可能多地接受连接
    while (true) {
        int client_fd = accept(server_fd_, (struct sockaddr*)&client_addr, &client_len);
        if (client_fd < 0) {
            if (errno != EAGAIN && errno != EWOULDBLOCK) {
                std::cerr << "接受连接失败: " << strerror(errno) << std::endl;
            }
            break; // 没有更多连接可接受
        }
        
        // 负载均衡：轮询选择一个子Reactor处理新连接
        static std::atomic<int> next_reactor_index(0);
        int reactor_index = next_reactor_index++ % sub_reactors_.size();
        
        // 将客户端连接交给子Reactor处理
        sub_reactors_[reactor_index]->addClient(client_fd, client_addr);
        
        std::cout << "新客户端连接: " << inet_ntoa(client_addr.sin_addr) 
                  << ":" << ntohs(client_addr.sin_port) 
                  << "，分配给子Reactor " << reactor_index << std::endl;
    }
}

bool NetworkServer::setNonBlocking(int fd) {
    int flags = fcntl(fd, F_GETFL, 0);
    if (flags < 0) {
        return false;
    }
    return fcntl(fd, F_SETFL, flags | O_NONBLOCK) >= 0;
}

bool NetworkServer::addEpollEvent(int fd, uint32_t events) {
    struct epoll_event event;
    event.events = events;
    event.data.fd = fd;
    return epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, fd, &event) >= 0;
}

bool NetworkServer::modifyEpollEvent(int fd, uint32_t events) {
    struct epoll_event event;
    event.events = events;
    event.data.fd = fd;
    return epoll_ctl(epoll_fd_, EPOLL_CTL_MOD, fd, &event) >= 0;
}

bool NetworkServer::removeEpollEvent(int fd) {
    return epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, fd, nullptr) >= 0;
}

} // namespace dkv
