#include "dkv_network.hpp"
#include "dkv_server.hpp"
#include "dkv_storage.hpp"
#include "dkv_resp.hpp"
#include "dkv_utils.hpp"
#include "dkv_logger.hpp"
#include <iostream>
#include <cstring>
#include <string.h>
#include <thread>
#include <chrono>
#include <random>

namespace dkv {

// SubReactor实现
SubReactor::SubReactor(WorkerThreadPool* worker_pool) : 
    epoll_fd_(-1), 
    running_(false), 
    worker_pool_(worker_pool) {
    
    // 创建epoll实例
    epoll_fd_ = epoll_create1(0);
    if (epoll_fd_ < 0) {
        DKV_LOG_ERROR("创建epoll失败: ", strerror(errno));
    }
}

SubReactor::~SubReactor() {
    stop();
    if (epoll_fd_ >= 0) {
        close(epoll_fd_);
    }
}


bool SubReactor::start() {
    if (running_.load() || epoll_fd_ < 0) {
        return false;
    }
    
    running_.store(true);
    event_loop_thread_ = std::thread(&SubReactor::eventLoop, this);
    return true;
}

void SubReactor::stop() {
    if (!running_.load()) {
        return;
    }
    
    running_.store(false);
    
    // 唤醒阻塞的epoll_wait
    int dummy_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (dummy_fd >= 0) {
        epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, dummy_fd, nullptr);
        close(dummy_fd);
    }
    
    if (event_loop_thread_.joinable()) {
        event_loop_thread_.join();
    }
    
    // 清理所有客户端连接
    std::lock_guard<std::mutex> lock(clients_mutex_);
    for (auto& [fd, client] : clients_) {
        close(fd);
    }
    clients_.clear();
}

void SubReactor::addClient(int client_fd, const sockaddr_in& client_addr) {
    if (!setNonBlocking(client_fd)) {
        DKV_LOG_ERROR("设置客户端连接为非阻塞失败");
        close(client_fd);
        return;
    }
    
    if (!addEpollEvent(client_fd, EPOLLIN | EPOLLET)) {
        DKV_LOG_ERROR("添加客户端事件失败");
        close(client_fd);
        return;
    }
    
    std::lock_guard<std::mutex> lock(clients_mutex_);
    auto client = std::make_unique<ClientConnection>(client_fd, client_addr);
    clients_[client_fd] = std::move(client);
    
    DKV_LOG_INFO("子Reactor添加客户端连接: ", inet_ntoa(client_addr.sin_addr), ":", ntohs(client_addr.sin_port));
}

void SubReactor::handleCommandResult(int client_fd, const Response& response) {
    // 发送响应给客户端
    sendResponse(client_fd, response);
}

void SubReactor::eventLoop() {
    const int MAX_EVENTS = 1024;
    struct epoll_event events[MAX_EVENTS];
    
    while (running_.load()) {
        int event_count = epoll_wait(epoll_fd_, events, MAX_EVENTS, 1000);
        
        if (event_count < 0) {
            if (errno != EINTR) {
                DKV_LOG_ERROR("epoll_wait失败: ", strerror(errno));
                std::this_thread::sleep_for(std::chrono::milliseconds(10));
            }
            continue;
        }
        
        for (int i = 0; i < event_count; ++i) {
            int fd = events[i].data.fd;
            
            if (events[i].events & (EPOLLIN | EPOLLPRI)) {
                handleClientData(fd);
            } else if (events[i].events & (EPOLLHUP | EPOLLERR)) {
                handleClientDisconnect(fd);
            }
        }
    }
}

void SubReactor::handleClientData(int client_fd) {
    std::unique_ptr<ClientConnection>* client_ptr = nullptr;
    
    // 查找客户端连接
    { 
        std::lock_guard<std::mutex> lock(clients_mutex_);
        auto it = clients_.find(client_fd);
        if (it == clients_.end()) {
            return;
        }
        client_ptr = &(it->second);
    }
    
    ClientConnection* client = client_ptr->get();
    
    // 读取数据
    char buffer[8192];
    ssize_t bytes_read;
    
    while ((bytes_read = read(client_fd, buffer, sizeof(buffer))) > 0) {
        client->read_buffer.append(buffer, bytes_read);
    }
    
    if (bytes_read == 0) {
        handleClientDisconnect(client_fd);
        return;
    }
    
    if (bytes_read < 0 && errno != EAGAIN && errno != EWOULDBLOCK) {
        DKV_LOG_ERROR("读取客户端数据失败: ", strerror(errno));
        handleClientDisconnect(client_fd);
        return;
    }
    
    // 解析命令
    size_t pos = 0;
    while (pos < client->read_buffer.length()) {
        DKV_LOG_DEBUG("SubReactor::handleClientData 解析命令前: ", client->read_buffer.substr(pos));
        Command command = RESPProtocol::parseCommand(client->read_buffer, pos);
        DKV_LOG_DEBUG("SubReactor::handleClientData 解析命令后: ", Utils::commandTypeToString(command.type));
        for (const auto& arg : command.args) {
            DKV_LOG_DEBUG("SubReactor::handleClientData 解析命令参数: ", arg);
        }
        if (command.type == CommandType::UNKNOWN) {
            break; // 等待更多数据
        }
        
        DKV_LOG_DEBUG("子Reactor解析到命令: ", Utils::commandTypeToString(command.type));
        
        // 创建命令任务并提交到线程池
        if (worker_pool_) {
            CommandTask task;
            task.sub_reactor = this;
            task.command = command;
            task.client_fd = client_fd;
            task.client_read_buffer = client->read_buffer.substr(pos);
            task.parsed_pos = pos;
            
            worker_pool_->enqueue(task);
        }
    }
    
    // 移除已处理的数据
    if (pos > 0) {
        client->read_buffer.erase(0, pos);
    }
}

void SubReactor::handleClientDisconnect(int client_fd) {
    std::lock_guard<std::mutex> lock(clients_mutex_);
    handleClientDisconnect_locked(client_fd);
}

void SubReactor::handleClientDisconnect_locked(int client_fd) {
    auto it = clients_.find(client_fd);
    if (it != clients_.end()) {
        DKV_LOG_INFO("子Reactor客户端断开连接: ", 
                     inet_ntoa(it->second->addr.sin_addr), 
                     ":",
                     ntohs(it->second->addr.sin_port));
        removeEpollEvent(client_fd);
        close(client_fd);
        clients_.erase(it);
    }
}

void SubReactor::sendResponse(int client_fd, const Response& response) {
    std::string resp_str;
    
    if (response.data.empty()) {
        resp_str = RESPProtocol::serializeResponse(response);
    } else {
        resp_str = RESPProtocol::serializeBulkString(response.data);
    }
    
    ssize_t bytes_sent = write(client_fd, resp_str.c_str(), resp_str.length());
    if (bytes_sent < 0) {
        DKV_LOG_ERROR("子Reactor发送响应失败");
        // 发送失败可能意味着连接已断开，尝试清理
        handleClientDisconnect(client_fd);
    }
}

bool SubReactor::setNonBlocking(int fd) {
    int flags = fcntl(fd, F_GETFL, 0);
    if (flags < 0) {
        return false;
    }
    return fcntl(fd, F_SETFL, flags | O_NONBLOCK) >= 0;
}

bool SubReactor::addEpollEvent(int fd, uint32_t events) {
    struct epoll_event event;
    event.events = events;
    event.data.fd = fd;
    return epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, fd, &event) >= 0;
}

bool SubReactor::modifyEpollEvent(int fd, uint32_t events) {
    struct epoll_event event;
    event.events = events;
    event.data.fd = fd;
    return epoll_ctl(epoll_fd_, EPOLL_CTL_MOD, fd, &event) >= 0;
}

bool SubReactor::removeEpollEvent(int fd) {
    return epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, fd, nullptr) >= 0;
}



} // namespace dkv