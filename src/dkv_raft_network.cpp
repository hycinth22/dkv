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
RaftTcpNetwork::RaftTcpNetwork(const std::vector<std::string>& peers) : peers_(peers) {
}

// RAFT TCP网络实现析构函数
RaftTcpNetwork::~RaftTcpNetwork() {
    // 关闭所有连接
    for (const auto& conn : connections_) {
        close(conn.second);
    }
    connections_.clear();
}

// 发送AppendEntries请求
AppendEntriesResponse RaftTcpNetwork::SendAppendEntries(int serverId, const AppendEntriesRequest& request) {
    DKV_LOG_INFO("发送AppendEntries请求到节点 ", serverId);
    
    // 1. 序列化请求
    std::vector<char> requestData = SerializeAppendEntries(request);
    
    // 2. 建立连接
    int sockfd = EstablishConnection(serverId);
    if (sockfd < 0) {
        AppendEntriesResponse response;
        response.term = 0;
        response.success = false;
        response.matchIndex = 0;
        return response;
    }
    
    // 3. 发送请求
    if (!SendData(sockfd, requestData)) {
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
    
    // 2. 建立连接
    int sockfd = EstablishConnection(serverId);
    if (sockfd < 0) {
        RequestVoteResponse response;
        response.term = 0;
        response.voteGranted = false;
        return response;
    }
    
    // 3. 发送请求
    if (!SendData(sockfd, requestData)) {
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
    
    // 2. 建立连接
    int sockfd = EstablishConnection(serverId);
    if (sockfd < 0) {
        InstallSnapshotResponse response;
        response.term = 0;
        response.success = false;
        return response;
    }
    
    // 3. 发送请求
    if (!SendData(sockfd, requestData)) {
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
