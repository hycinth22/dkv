#include "dkv_network.hpp"
#include <iostream>
#include <cstring>
#include <algorithm>
#include "dkv_server.hpp"

namespace dkv {

// NetworkServer 实现
NetworkServer::NetworkServer(int port) 
    : server_fd_(-1), epoll_fd_(-1), running_(false), storage_engine_(nullptr), server_(nullptr) {
    memset(&server_addr_, 0, sizeof(server_addr_));
    server_addr_.sin_family = AF_INET;
    server_addr_.sin_addr.s_addr = INADDR_ANY;
    server_addr_.sin_port = htons(port);
}

NetworkServer::~NetworkServer() {
    stop();
}

bool NetworkServer::start() {
    if (!initializeServer(server_addr_.sin_port)) {
        return false;
    }
    
    running_ = true;
    event_loop_thread_ = std::thread(&NetworkServer::eventLoop, this);
    
    std::cout << "DKV服务器启动成功，监听端口: " << ntohs(server_addr_.sin_port) << std::endl;
    return true;
}

void NetworkServer::stop() {
    running_ = false;
    
    if (event_loop_thread_.joinable()) {
        event_loop_thread_.join();
    }
    
    if (epoll_fd_ >= 0) {
        close(epoll_fd_);
        epoll_fd_ = -1;
    }
    
    if (server_fd_ >= 0) {
        close(server_fd_);
        server_fd_ = -1;
    }
    
    {
        std::lock_guard<std::mutex> lock(clients_mutex_);
        clients_.clear();
    }
}

void NetworkServer::setStorageEngine(StorageEngine* storage_engine) {
    storage_engine_ = storage_engine;
}

void NetworkServer::setDKVServer(DKVServer* server) {
    server_ = server;
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
    
    // 创建epoll实例
    epoll_fd_ = epoll_create1(0);
    if (epoll_fd_ < 0) {
        std::cerr << "创建epoll实例失败" << std::endl;
        close(server_fd_);
        return false;
    }
    
    // 添加服务器socket到epoll
    if (!addEpollEvent(server_fd_, EPOLLIN)) {
        std::cerr << "添加服务器socket到epoll失败" << std::endl;
        close(epoll_fd_);
        close(server_fd_);
        return false;
    }
    
    return true;
}

void NetworkServer::eventLoop() {
    const int MAX_EVENTS = 128;
    struct epoll_event events[MAX_EVENTS];
    
    while (running_) {
        int num_events = epoll_wait(epoll_fd_, events, MAX_EVENTS, 100); // 100ms超时
        
        if (num_events < 0) {
            if (errno == EINTR) {
                continue;
            }
            std::cerr << "epoll_wait失败: " << strerror(errno) << std::endl;
            break;
        }
        
        for (int i = 0; i < num_events; i++) {
            int fd = events[i].data.fd;
            uint32_t event_mask = events[i].events;
            
            if (fd == server_fd_) {
                // 新连接
                handleNewConnection();
            } else {
                // 客户端数据
                if (event_mask & EPOLLIN) {
                    handleClientData(fd);
                }
                if (event_mask & (EPOLLHUP | EPOLLERR)) {
                    handleClientDisconnect(fd);
                }
            }
        }
    }
}

void NetworkServer::handleNewConnection() {
    struct sockaddr_in client_addr;
    socklen_t client_len = sizeof(client_addr);
    
    int client_fd = accept(server_fd_, (struct sockaddr*)&client_addr, &client_len);
    if (client_fd < 0) {
        return;
    }
    
    // 设置非阻塞模式
    if (!setNonBlocking(client_fd)) {
        close(client_fd);
        return;
    }
    
    // 添加到epoll
    if (!addEpollEvent(client_fd, EPOLLIN)) {
        close(client_fd);
        return;
    }
    
    // 创建客户端连接对象
    auto client = std::make_unique<ClientConnection>(client_fd, client_addr);
    
    {
        std::lock_guard<std::mutex> lock(clients_mutex_);
        clients_[client_fd] = std::move(client);
    }
    
    std::cout << "新客户端连接: " << inet_ntoa(client_addr.sin_addr) 
              << ":" << ntohs(client_addr.sin_port) << std::endl;
}

void NetworkServer::handleClientData(int client_fd) {
    std::cout << "处理客户端数据: " << client_fd << std::endl;
    std::lock_guard<std::mutex> lock(clients_mutex_);
    auto it = clients_.find(client_fd);
    if (it == clients_.end()) {
        return;
    }
    
    ClientConnection* client = it->second.get();
    char buffer[4096];
    ssize_t bytes_read = read(client_fd, buffer, sizeof(buffer));
    if (bytes_read <= 0) {
        handleClientDisconnect_locked(client_fd);
        return;
    }
    client->read_buffer.append(buffer, bytes_read);
    
    // 解析命令
    size_t pos = 0;
    while (pos < client->read_buffer.length()) {
        Command command = RESPProtocol::parseCommand(client->read_buffer.substr(pos), pos);
        if (command.type == CommandType::UNKNOWN) {
            break; // 等待更多数据
        }
        
        // 执行命令
        Response response = executeCommand(command);
        
        // 发送响应
        sendResponse(client_fd, response);
    }
    
    // 移除已处理的数据
    if (pos > 0) {
        client->read_buffer.erase(0, pos);
    }
}

// 内部版本，假设调用者已持有锁 clients_mutex_
void NetworkServer::handleClientDisconnect_locked(int client_fd) {
    auto it = clients_.find(client_fd);
    if (it != clients_.end()) {
        std::cout << "客户端断开连接: " << inet_ntoa(it->second->addr.sin_addr) 
                  << ":" << ntohs(it->second->addr.sin_port) << std::endl;
        removeEpollEvent(client_fd);
        clients_.erase(it);
    }
}

// 公开版本，会锁 clients_mutex_
void NetworkServer::handleClientDisconnect(int client_fd) {
    std::lock_guard<std::mutex> lock(clients_mutex_);
    handleClientDisconnect_locked(client_fd);
}

Response NetworkServer::executeCommand(const Command& command) {
    if (!server_) {
        return Response(ResponseStatus::ERROR, "DKV server not initialized");
    }
    
    return server_->executeCommand(command);
}

void NetworkServer::sendResponse(int client_fd, const Response& response) {
    std::string resp_str;
    
    if (response.data.empty()) {
        resp_str = RESPProtocol::serializeResponse(response);
    } else {
        resp_str = RESPProtocol::serializeBulkString(response.data);
    }
    
    ssize_t bytes_sent = write(client_fd, resp_str.c_str(), resp_str.length());
    if (bytes_sent < 0) {
        std::cerr << "发送响应失败" << std::endl;
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
