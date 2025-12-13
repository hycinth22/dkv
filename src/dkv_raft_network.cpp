#include "dkv_raft_network.h"
#include "dkv_logger.hpp"
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <fcntl.h>
#include <cerrno>
#include <cstring>

namespace dkv {

// RAFT TCP网络实现构造函数
RaftTcpNetwork::RaftTcpNetwork(int me, const std::vector<std::string>& peers) : me_(me), peers_(peers) {
    // 启动监听线程
    StartListener();
}

// RAFT TCP网络实现析构函数
RaftTcpNetwork::~RaftTcpNetwork() {
    // 停止监听线程
    StopListener();
    
    // 关闭所有连接
    for (const auto& conn : connections_) {
        close(conn.second);
    }
    connections_.clear();
}

// 启动网络监听
void RaftTcpNetwork::StartListener() {
    if (listener_running_) {
        return;
    }
    
    listener_running_ = true;
    listener_thread_ = std::thread(&RaftTcpNetwork::Listen, this);
}

// 停止网络监听
void RaftTcpNetwork::StopListener() {
    if (!listener_running_) {
        return;
    }
    
    listener_running_ = false;
    
    // 关闭监听套接字，唤醒监听线程
    if (listen_fd_ != -1) {
        close(listen_fd_);
        listen_fd_ = -1;
    }
    
    // 等待监听线程结束
    if (listener_thread_.joinable()) {
        listener_thread_.join();
    }
}

// 监听连接
void RaftTcpNetwork::Listen() {
    // 从peers_中获取当前节点的地址和端口
    if (me_ >= (int)peers_.size()) {
        DKV_LOG_ERROR("无效的节点ID");
        return;
    }
    
    const std::string& self_addr = peers_[me_];
    size_t colon_pos = self_addr.find(':');
    if (colon_pos == std::string::npos) {
        DKV_LOG_ERROR("无效的节点地址格式: ", self_addr);
        return;
    }
    
    std::string ip = self_addr.substr(0, colon_pos);
    int port = std::stoi(self_addr.substr(colon_pos + 1));
    
    // 创建监听套接字
    listen_fd_ = socket(AF_INET, SOCK_STREAM, 0);
    if (listen_fd_ < 0) {
        DKV_LOG_ERROR("创建监听套接字失败: ", strerror(errno));
        return;
    }
    
    // 设置套接字选项，允许地址重用
    int optval = 1;
    if (setsockopt(listen_fd_, SOL_SOCKET, SO_REUSEADDR, &optval, sizeof(optval)) < 0) {
        DKV_LOG_ERROR("设置套接字选项失败: ", strerror(errno));
        close(listen_fd_);
        listen_fd_ = -1;
        return;
    }
    
    // 绑定地址
    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    
    if (inet_pton(AF_INET, ip.c_str(), &addr.sin_addr) <= 0) {
        DKV_LOG_ERROR("无效的IP地址: ", ip);
        close(listen_fd_);
        listen_fd_ = -1;
        return;
    }
    
    if (bind(listen_fd_, (struct sockaddr*)&addr, sizeof(addr)) < 0) {
        DKV_LOG_ERROR("绑定地址失败: ", strerror(errno));
        close(listen_fd_);
        listen_fd_ = -1;
        return;
    }
    
    // 开始监听
    if (listen(listen_fd_, 10) < 0) {
        DKV_LOG_ERROR("开始监听失败: ", strerror(errno));
        close(listen_fd_);
        listen_fd_ = -1;
        return;
    }
    
    DKV_LOG_INFO("Raft网络监听已启动，地址: ", self_addr);
    
    // 接受连接
    while (listener_running_) {
        struct sockaddr_in client_addr;
        socklen_t client_len = sizeof(client_addr);
        int client_fd = accept(listen_fd_, (struct sockaddr*)&client_addr, &client_len);
        
        if (client_fd < 0) {
            if (listener_running_) {
                DKV_LOG_ERROR("接受连接失败: ", strerror(errno));
            }
            continue;
        }
        
        // 创建线程处理连接
        std::thread(&RaftTcpNetwork::HandleConnection, this, client_fd).detach();
    }
    
    // 关闭监听套接字
    close(listen_fd_);
    listen_fd_ = -1;
}

// 处理连接
void RaftTcpNetwork::HandleConnection(int client_fd) {
    try {
        // 接收请求数据
        std::vector<char> request_data = ReceiveData(client_fd);
        if (request_data.empty()) {
            close(client_fd);
            return;
        }
        
        // 解析请求类型（第一个字节）
        if (request_data.empty()) {
            close(client_fd);
            return;
        }
        
        char request_type = request_data[0];
        std::vector<char> payload_data(request_data.begin() + 1, request_data.end());
        
        // 获取Raft实例
        auto raft = raft_.lock();
        if (!raft) {
            DKV_LOG_ERROR("Raft实例已失效");
            close(client_fd);
            return;
        }
        
        // 根据请求类型处理
        std::vector<char> response_data;
        switch (request_type) {
            case 'A': { // AppendEntries请求
                AppendEntriesRequest request = DeserializeAppendEntries(payload_data);
                AppendEntriesResponse response = raft->OnAppendEntries(request);
                response_data = SerializeAppendEntriesResponse(response);
                break;
            }
            case 'V': { // RequestVote请求
                RequestVoteRequest request = DeserializeRequestVote(payload_data);
                RequestVoteResponse response = raft->OnRequestVote(request);
                response_data = SerializeRequestVoteResponse(response);
                break;
            }
            case 'S': { // InstallSnapshot请求
                InstallSnapshotRequest request = DeserializeInstallSnapshot(payload_data);
                InstallSnapshotResponse response = raft->OnInstallSnapshot(request);
                response_data = SerializeInstallSnapshotResponse(response);
                break;
            }
            default:
                DKV_LOG_ERROR("未知的请求类型: ", request_type);
                close(client_fd);
                return;
        }
        
        // 发送响应
        SendData(client_fd, response_data);
        close(client_fd);
    } catch (const std::exception& e) {
        DKV_LOG_ERROR("处理连接时发生异常: ", e.what());
        close(client_fd);
    }
}

// 反序列化AppendEntries请求
AppendEntriesRequest RaftTcpNetwork::DeserializeAppendEntries(const std::vector<char>& data) {
    AppendEntriesRequest request;
    
    if (data.size() < 20) { // 5个uint32_t字段
        return request;
    }
    
    size_t offset = 0;
    
    // 反序列化term
    uint32_t term = 0;
    memcpy(&term, data.data() + offset, sizeof(term));
    request.term = ntohl(term);
    offset += sizeof(term);
    
    // 反序列化leaderId
    uint32_t leaderId = 0;
    memcpy(&leaderId, data.data() + offset, sizeof(leaderId));
    request.leaderId = ntohl(leaderId);
    offset += sizeof(leaderId);
    
    // 反序列化prevLogIndex
    uint32_t prevLogIndex = 0;
    memcpy(&prevLogIndex, data.data() + offset, sizeof(prevLogIndex));
    request.prevLogIndex = ntohl(prevLogIndex);
    offset += sizeof(prevLogIndex);
    
    // 反序列化prevLogTerm
    uint32_t prevLogTerm = 0;
    memcpy(&prevLogTerm, data.data() + offset, sizeof(prevLogTerm));
    request.prevLogTerm = ntohl(prevLogTerm);
    offset += sizeof(prevLogTerm);
    
    // 反序列化leaderCommit
    uint32_t leaderCommit = 0;
    memcpy(&leaderCommit, data.data() + offset, sizeof(leaderCommit));
    request.leaderCommit = ntohl(leaderCommit);
    offset += sizeof(leaderCommit);
    
    // 反序列化日志条目数量
    uint32_t entriesSize = 0;
    memcpy(&entriesSize, data.data() + offset, sizeof(entriesSize));
    entriesSize = ntohl(entriesSize);
    offset += sizeof(entriesSize);
    
    // 反序列化每个日志条目
    for (uint32_t i = 0; i < entriesSize && offset < data.size(); i++) {
        RaftLogEntry entry;
        
        // entry.term
        uint32_t entryTerm = 0;
        memcpy(&entryTerm, data.data() + offset, sizeof(entryTerm));
        entry.term = ntohl(entryTerm);
        offset += sizeof(entryTerm);
        
        // entry.index
        uint32_t entryIndex = 0;
        memcpy(&entryIndex, data.data() + offset, sizeof(entryIndex));
        entry.index = ntohl(entryIndex);
        offset += sizeof(entryIndex);
        
        // 反序列化Command对象
        auto cmd = std::make_shared<Command>();
        if (offset < data.size()) {
            std::vector<char> cmd_data(data.begin() + offset, data.end());
            if (cmd->deserialize(cmd_data)) {
                entry.command = cmd;
                // 计算命令的序列化大小
                offset += cmd->PersistBytes();
            }
        }
        
        request.entries.push_back(entry);
    }
    
    return request;
}

// 反序列化RequestVote请求
RequestVoteRequest RaftTcpNetwork::DeserializeRequestVote(const std::vector<char>& data) {
    RequestVoteRequest request;
    
    if (data.size() < 16) { // 4个uint32_t字段
        return request;
    }
    
    size_t offset = 0;
    
    // 反序列化term
    uint32_t term = 0;
    memcpy(&term, data.data() + offset, sizeof(term));
    request.term = ntohl(term);
    offset += sizeof(term);
    
    // 反序列化candidateId
    uint32_t candidateId = 0;
    memcpy(&candidateId, data.data() + offset, sizeof(candidateId));
    request.candidateId = ntohl(candidateId);
    offset += sizeof(candidateId);
    
    // 反序列化lastLogIndex
    uint32_t lastLogIndex = 0;
    memcpy(&lastLogIndex, data.data() + offset, sizeof(lastLogIndex));
    request.lastLogIndex = ntohl(lastLogIndex);
    offset += sizeof(lastLogIndex);
    
    // 反序列化lastLogTerm
    uint32_t lastLogTerm = 0;
    memcpy(&lastLogTerm, data.data() + offset, sizeof(lastLogTerm));
    request.lastLogTerm = ntohl(lastLogTerm);
    offset += sizeof(lastLogTerm);
    
    return request;
}

// 反序列化InstallSnapshot请求
InstallSnapshotRequest RaftTcpNetwork::DeserializeInstallSnapshot(const std::vector<char>& data) {
    InstallSnapshotRequest request;
    
    if (data.size() < 24) { // 6个uint32_t字段
        return request;
    }
    
    size_t offset = 0;
    
    // 反序列化term
    uint32_t term = 0;
    memcpy(&term, data.data() + offset, sizeof(term));
    request.term = ntohl(term);
    offset += sizeof(term);
    
    // 反序列化leaderId
    uint32_t leaderId = 0;
    memcpy(&leaderId, data.data() + offset, sizeof(leaderId));
    request.leaderId = ntohl(leaderId);
    offset += sizeof(leaderId);
    
    // 反序列化lastIncludedIndex
    uint32_t lastIncludedIndex = 0;
    memcpy(&lastIncludedIndex, data.data() + offset, sizeof(lastIncludedIndex));
    request.lastIncludedIndex = ntohl(lastIncludedIndex);
    offset += sizeof(lastIncludedIndex);
    
    // 反序列化lastIncludedTerm
    uint32_t lastIncludedTerm = 0;
    memcpy(&lastIncludedTerm, data.data() + offset, sizeof(lastIncludedTerm));
    request.lastIncludedTerm = ntohl(lastIncludedTerm);
    offset += sizeof(lastIncludedTerm);
    
    // 反序列化leaderCommit
    uint32_t leaderCommit = 0;
    memcpy(&leaderCommit, data.data() + offset, sizeof(leaderCommit));
    request.leaderCommit = ntohl(leaderCommit);
    offset += sizeof(leaderCommit);
    
    // 反序列化快照数据
    uint32_t snapshotSize = 0;
    memcpy(&snapshotSize, data.data() + offset, sizeof(snapshotSize));
    snapshotSize = ntohl(snapshotSize);
    offset += sizeof(snapshotSize);
    
    if (offset + snapshotSize <= data.size()) {
        request.snapshot.assign(data.begin() + offset, data.begin() + offset + snapshotSize);
    }
    
    return request;
}

// 序列化AppendEntries响应
std::vector<char> RaftTcpNetwork::SerializeAppendEntriesResponse(const AppendEntriesResponse& response) {
    std::vector<char> data;
    
    // 序列化term
    uint32_t term = htonl(response.term);
    data.insert(data.end(), (char*)&term, (char*)&term + sizeof(term));
    
    // 序列化success
    uint32_t success = htonl(response.success ? 1 : 0);
    data.insert(data.end(), (char*)&success, (char*)&success + sizeof(success));
    
    // 序列化matchIndex
    uint32_t matchIndex = htonl(response.matchIndex);
    data.insert(data.end(), (char*)&matchIndex, (char*)&matchIndex + sizeof(matchIndex));
    
    return data;
}

// 序列化RequestVote响应
std::vector<char> RaftTcpNetwork::SerializeRequestVoteResponse(const RequestVoteResponse& response) {
    std::vector<char> data;
    
    // 序列化term
    uint32_t term = htonl(response.term);
    data.insert(data.end(), (char*)&term, (char*)&term + sizeof(term));
    
    // 序列化voteGranted
    uint32_t voteGranted = htonl(response.voteGranted ? 1 : 0);
    data.insert(data.end(), (char*)&voteGranted, (char*)&voteGranted + sizeof(voteGranted));
    
    return data;
}

// 序列化InstallSnapshot响应
std::vector<char> RaftTcpNetwork::SerializeInstallSnapshotResponse(const InstallSnapshotResponse& response) {
    std::vector<char> data;
    
    // 序列化term
    uint32_t term = htonl(response.term);
    data.insert(data.end(), (char*)&term, (char*)&term + sizeof(term));
    
    // 序列化success
    uint32_t success = htonl(response.success ? 1 : 0);
    data.insert(data.end(), (char*)&success, (char*)&success + sizeof(success));
    
    return data;
}

// 发送AppendEntries请求
AppendEntriesResponse RaftTcpNetwork::SendAppendEntries(int serverId, const AppendEntriesRequest& request) {
    DKV_LOG_INFO("发送AppendEntries请求到节点 ", serverId);
    
    // 1. 序列化请求
    std::vector<char> requestData = SerializeAppendEntries(request);
    
    // 2. 添加请求类型标识 'A' 表示AppendEntries
    std::vector<char> fullRequestData;
    fullRequestData.push_back('A');
    fullRequestData.insert(fullRequestData.end(), requestData.begin(), requestData.end());
    
    // 3. 建立连接
    int sockfd = EstablishConnection(serverId);
    if (sockfd < 0) {
        AppendEntriesResponse response;
        response.term = 0;
        response.success = false;
        response.matchIndex = 0;
        return response;
    }
    
    // 4. 发送请求
    if (!SendData(sockfd, fullRequestData)) {
        close(sockfd);
        AppendEntriesResponse response;
        response.term = 0;
        response.success = false;
        response.matchIndex = 0;
        return response;
    }
    
    // 4. 接收响应
    std::vector<char> responseData = ReceiveData(sockfd);
    close(sockfd);
    
    // 5. 反序列化响应
    AppendEntriesResponse response = DeserializeAppendEntriesResponse(responseData);
    
    DKV_LOG_INFO("收到AppendEntries响应，节点 ", serverId, "，结果 ", response.success);
    
    return response;
}

// 发送RequestVote请求
RequestVoteResponse RaftTcpNetwork::SendRequestVote(int serverId, const RequestVoteRequest& request) {
    DKV_LOG_INFO("发送RequestVote请求到节点 ", serverId);
    
    // 1. 序列化请求
    std::vector<char> requestData = SerializeRequestVote(request);
    
    // 2. 添加请求类型标识 'V' 表示RequestVote
    std::vector<char> fullRequestData;
    fullRequestData.push_back('V');
    fullRequestData.insert(fullRequestData.end(), requestData.begin(), requestData.end());
    
    // 3. 建立连接
    int sockfd = EstablishConnection(serverId);
    if (sockfd < 0) {
        RequestVoteResponse response;
        response.term = 0;
        response.voteGranted = false;
        return response;
    }
    
    // 4. 发送请求
    if (!SendData(sockfd, fullRequestData)) {
        close(sockfd);
        RequestVoteResponse response;
        response.term = 0;
        response.voteGranted = false;
        return response;
    }
    
    // 4. 接收响应
    std::vector<char> responseData = ReceiveData(sockfd);
    close(sockfd);
    
    // 5. 反序列化响应
    RequestVoteResponse response = DeserializeRequestVoteResponse(responseData);
    
    DKV_LOG_INFO("收到RequestVote响应，节点 ", serverId, "，结果 ", response.voteGranted);
    
    return response;
}

// 建立连接
int RaftTcpNetwork::EstablishConnection(int serverId) {
    if (serverId < 0 || (size_t)serverId >= peers_.size()) {
        DKV_LOG_ERROR("无效的节点ID: ", serverId);
        return -1;
    }
    
    const std::string& peer = peers_[serverId];
    
    // 解析节点地址
    size_t colonPos = peer.find(':');
    if (colonPos == std::string::npos) {
        DKV_LOG_ERROR("无效的节点地址格式: ", peer);
        return -1;
    }
    
    std::string ip = peer.substr(0, colonPos);
    int port = stoi(peer.substr(colonPos + 1));
    
    // 创建socket
    int sockfd = socket(AF_INET, SOCK_STREAM, 0);
    if (sockfd < 0) {
        DKV_LOG_ERROR("创建socket失败: ", strerror(errno));
        return -1;
    }
    
    // 设置地址结构
    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    
    if (inet_pton(AF_INET, ip.c_str(), &addr.sin_addr) <= 0) {
        DKV_LOG_ERROR("无效的IP地址: ", ip);
        close(sockfd);
        return -1;
    }
    
    // 连接到节点
    if (connect(sockfd, (struct sockaddr*)&addr, sizeof(addr)) < 0) {
        DKV_LOG_ERROR("连接到节点 ", serverId, " 失败: ", strerror(errno));
        close(sockfd);
        return -1;
    }
    
    DKV_LOG_INFO("成功连接到节点 ", serverId, " (", ip, ":", port, ")");
    return sockfd;
}

// 发送数据
bool RaftTcpNetwork::SendData(int sockfd, const std::vector<char>& data) {
    if (sockfd < 0 || data.empty()) {
        return false;
    }
    
    // 发送数据长度
    uint32_t len = htonl(data.size());
    if (send(sockfd, &len, sizeof(len), 0) != sizeof(len)) {
        DKV_LOG_ERROR("发送数据长度失败: ", strerror(errno));
        return false;
    }
    
    // 发送数据内容
    ssize_t sent = 0;
    while (sent < (ssize_t)data.size()) {
        ssize_t n = send(sockfd, data.data() + sent, data.size() - sent, 0);
        if (n < 0) {
            DKV_LOG_ERROR("发送数据内容失败: ", strerror(errno));
            return false;
        }
        sent += n;
    }
    
    return true;
}

// 接收数据
std::vector<char> RaftTcpNetwork::ReceiveData(int sockfd) {
    if (sockfd < 0) {
        return {};
    }
    
    // 接收数据长度
    uint32_t len = 0;
    if (recv(sockfd, &len, sizeof(len), 0) != sizeof(len)) {
        DKV_LOG_ERROR("接收数据长度失败: ", strerror(errno));
        return {};
    }
    
    len = ntohl(len);
    if (len == 0) {
        return {};
    }
    
    // 接收数据内容
    std::vector<char> data(len);
    ssize_t received = 0;
    while (received < (ssize_t)len) {
        ssize_t n = recv(sockfd, data.data() + received, len - received, 0);
        if (n < 0) {
            DKV_LOG_ERROR("接收数据内容失败: ", strerror(errno));
            return {};
        }
        received += n;
    }
    
    return data;
}

// 序列化AppendEntries请求
std::vector<char> RaftTcpNetwork::SerializeAppendEntries(const AppendEntriesRequest& request) {
    std::vector<char> data;
    
    // 序列化固定长度字段
    // term
    uint32_t term = htonl(request.term);
    data.insert(data.end(), (char*)&term, (char*)&term + sizeof(term));
    
    // leaderId
    uint32_t leaderId = htonl(request.leaderId);
    data.insert(data.end(), (char*)&leaderId, (char*)&leaderId + sizeof(leaderId));
    
    // prevLogIndex
    uint32_t prevLogIndex = htonl(request.prevLogIndex);
    data.insert(data.end(), (char*)&prevLogIndex, (char*)&prevLogIndex + sizeof(prevLogIndex));
    
    // prevLogTerm
    uint32_t prevLogTerm = htonl(request.prevLogTerm);
    data.insert(data.end(), (char*)&prevLogTerm, (char*)&prevLogTerm + sizeof(prevLogTerm));
    
    // leaderCommit
    uint32_t leaderCommit = htonl(request.leaderCommit);
    data.insert(data.end(), (char*)&leaderCommit, (char*)&leaderCommit + sizeof(leaderCommit));
    
    // 序列化日志条目数量
    uint32_t entriesSize = htonl(request.entries.size());
    data.insert(data.end(), (char*)&entriesSize, (char*)&entriesSize + sizeof(entriesSize));
    
    // 序列化每个日志条目
    for (const auto& entry : request.entries) {
        // entry.term
        uint32_t entryTerm = htonl(entry.term);
        data.insert(data.end(), (char*)&entryTerm, (char*)&entryTerm + sizeof(entryTerm));
        
        // entry.index
        uint32_t entryIndex = htonl(entry.index);
        data.insert(data.end(), (char*)&entryIndex, (char*)&entryIndex + sizeof(entryIndex));
        
        // 序列化Command对象
        entry.command->serialize(data);
    }
    
    return data;
}

// 反序列化AppendEntries响应
AppendEntriesResponse RaftTcpNetwork::DeserializeAppendEntriesResponse(const std::vector<char>& data) {
    AppendEntriesResponse response;
    
    if (data.size() < 12) { // 3个uint32_t字段
        return response;
    }
    
    size_t offset = 0;
    
    // 反序列化term
    uint32_t term = 0;
    memcpy(&term, data.data() + offset, sizeof(term));
    response.term = ntohl(term);
    offset += sizeof(term);
    
    // 反序列化success
    uint32_t success = 0;
    memcpy(&success, data.data() + offset, sizeof(success));
    response.success = (success != 0);
    offset += sizeof(success);
    
    // 反序列化matchIndex
    uint32_t matchIndex = 0;
    memcpy(&matchIndex, data.data() + offset, sizeof(matchIndex));
    response.matchIndex = ntohl(matchIndex);
    offset += sizeof(matchIndex);
    
    return response;
}

// 序列化RequestVote请求
std::vector<char> RaftTcpNetwork::SerializeRequestVote(const RequestVoteRequest& request) {
    std::vector<char> data;
    
    // 序列化固定长度字段
    // term
    uint32_t term = htonl(request.term);
    data.insert(data.end(), (char*)&term, (char*)&term + sizeof(term));
    
    // candidateId
    uint32_t candidateId = htonl(request.candidateId);
    data.insert(data.end(), (char*)&candidateId, (char*)&candidateId + sizeof(candidateId));
    
    // lastLogIndex
    uint32_t lastLogIndex = htonl(request.lastLogIndex);
    data.insert(data.end(), (char*)&lastLogIndex, (char*)&lastLogIndex + sizeof(lastLogIndex));
    
    // lastLogTerm
    uint32_t lastLogTerm = htonl(request.lastLogTerm);
    data.insert(data.end(), (char*)&lastLogTerm, (char*)&lastLogTerm + sizeof(lastLogTerm));
    
    return data;
}

// 反序列化RequestVote响应
RequestVoteResponse RaftTcpNetwork::DeserializeRequestVoteResponse(const std::vector<char>& data) {
    RequestVoteResponse response;
    
    if (data.size() < 8) { // 2个uint32_t字段
        return response;
    }
    
    size_t offset = 0;
    
    // 反序列化term
    uint32_t term = 0;
    memcpy(&term, data.data() + offset, sizeof(term));
    response.term = ntohl(term);
    offset += sizeof(term);
    
    // 反序列化voteGranted
    uint32_t voteGranted = 0;
    memcpy(&voteGranted, data.data() + offset, sizeof(voteGranted));
    response.voteGranted = (voteGranted != 0);
    offset += sizeof(voteGranted);
    
    return response;
}

// 发送InstallSnapshot请求
InstallSnapshotResponse RaftTcpNetwork::SendInstallSnapshot(int serverId, const InstallSnapshotRequest& request) {
    DKV_LOG_INFO("发送InstallSnapshot请求到节点 ", serverId);
    
    // 1. 序列化请求
    std::vector<char> requestData = SerializeInstallSnapshot(request);
    
    // 2. 添加请求类型标识 'S' 表示InstallSnapshot
    std::vector<char> fullRequestData;
    fullRequestData.push_back('S');
    fullRequestData.insert(fullRequestData.end(), requestData.begin(), requestData.end());
    
    // 3. 建立连接
    int sockfd = EstablishConnection(serverId);
    if (sockfd < 0) {
        InstallSnapshotResponse response;
        response.term = 0;
        response.success = false;
        return response;
    }
    
    // 4. 发送请求
    if (!SendData(sockfd, fullRequestData)) {
        close(sockfd);
        InstallSnapshotResponse response;
        response.term = 0;
        response.success = false;
        return response;
    }
    
    // 4. 接收响应
    std::vector<char> responseData = ReceiveData(sockfd);
    close(sockfd);
    
    // 5. 反序列化响应
    InstallSnapshotResponse response = DeserializeInstallSnapshotResponse(responseData);
    
    DKV_LOG_INFO("收到InstallSnapshot响应，节点 ", serverId, "，结果 ", response.success);
    
    return response;
}

// 序列化InstallSnapshot请求
std::vector<char> RaftTcpNetwork::SerializeInstallSnapshot(const InstallSnapshotRequest& request) {
    std::vector<char> data;
    
    // 序列化固定长度字段
    // term
    uint32_t term = htonl(request.term);
    data.insert(data.end(), (char*)&term, (char*)&term + sizeof(term));
    
    // leaderId
    uint32_t leaderId = htonl(request.leaderId);
    data.insert(data.end(), (char*)&leaderId, (char*)&leaderId + sizeof(leaderId));
    
    // lastIncludedIndex
    uint32_t lastIncludedIndex = htonl(request.lastIncludedIndex);
    data.insert(data.end(), (char*)&lastIncludedIndex, (char*)&lastIncludedIndex + sizeof(lastIncludedIndex));
    
    // lastIncludedTerm
    uint32_t lastIncludedTerm = htonl(request.lastIncludedTerm);
    data.insert(data.end(), (char*)&lastIncludedTerm, (char*)&lastIncludedTerm + sizeof(lastIncludedTerm));
    
    // leaderCommit
    uint32_t leaderCommit = htonl(request.leaderCommit);
    data.insert(data.end(), (char*)&leaderCommit, (char*)&leaderCommit + sizeof(leaderCommit));
    
    // 序列化快照数据
    uint32_t snapshotSize = htonl(request.snapshot.size());
    data.insert(data.end(), (char*)&snapshotSize, (char*)&snapshotSize + sizeof(snapshotSize));
    data.insert(data.end(), request.snapshot.begin(), request.snapshot.end());
    
    return data;
}

// 反序列化InstallSnapshot响应
InstallSnapshotResponse RaftTcpNetwork::DeserializeInstallSnapshotResponse(const std::vector<char>& data) {
    InstallSnapshotResponse response;
    
    if (data.size() < 8) { // 2个uint32_t字段
        return response;
    }
    
    size_t offset = 0;
    
    // 反序列化term
    uint32_t term = 0;
    memcpy(&term, data.data() + offset, sizeof(term));
    response.term = ntohl(term);
    offset += sizeof(term);
    
    // 反序列化success
    uint32_t success = 0;
    memcpy(&success, data.data() + offset, sizeof(success));
    response.success = (success != 0);
    offset += sizeof(success);
    
    return response;
}

} // namespace dkv
