#pragma once

#include "dkv_raft.hpp"
#include <string>
#include <vector>
#include <memory>
#include <unordered_map>
#include <mutex>
#include <atomic>
#include <thread>
#include <condition_variable>
#include <chrono>

namespace dkv {

// 连接状态枚举
enum class ConnectionState {
    DISCONNECTED,  // 已断开连接
    CONNECTING,    // 正在连接
    CONNECTED,     // 已连接
    RECONNECTING   // 正在重连
};

// 连接状态信息结构体
struct ConnectionInfo {
    int sockfd{-1};                       // 套接字文件描述符
    ConnectionState state{ConnectionState::DISCONNECTED};  // 连接状态
    int retry_count{0};                   // 重试次数
    std::chrono::steady_clock::time_point next_retry_time;  // 下次重试时间
    std::string peer_addr;                // 对等节点地址
};

// RAFT网络实现类
class RaftTcpNetwork : public RaftNetwork {
public:
    // 构造函数
    RaftTcpNetwork(int me, const std::vector<std::string>& peers);
    
    // 析构函数
    ~RaftTcpNetwork() override;
    
    // 发送AppendEntries请求
    AppendEntriesResponse SendAppendEntries(int serverId, const AppendEntriesRequest& request) override;
    
    // 发送RequestVote请求
    RequestVoteResponse SendRequestVote(int serverId, const RequestVoteRequest& request) override;
    
    // 发送InstallSnapshot请求
    InstallSnapshotResponse SendInstallSnapshot(int serverId, const InstallSnapshotRequest& request) override;
    
    // 启动网络监听
    void StartListener();
    
    // 停止网络监听
    void StopListener();
    
    // 设置Raft实例指针
    void SetRaft(std::shared_ptr<Raft> raft) { raft_ = raft; }
    
private:
    // 当前节点ID
    int me_;
    
    // 集群节点列表
    std::vector<std::string> peers_;
    
    // 连接缓存和状态信息
    std::unordered_map<int, ConnectionInfo> connections_;
    
    // 连接状态锁
    std::mutex connections_mutex_;
    
    // Raft实例指针
    std::weak_ptr<Raft> raft_;
    
    // 监听线程
    std::thread listener_thread_;
    
    // 连接维护线程
    std::thread connection_maintenance_thread_;
    
    // 监听运行标志
    std::atomic<bool> listener_running_{false};
    
    // 连接维护运行标志
    std::atomic<bool> maintenance_running_{false};
    
    // 监听套接字
    int listen_fd_{-1};
    
    // 连接维护条件变量
    std::condition_variable maintenance_cv_;
    
    // 建立连接
    int EstablishConnection(int serverId);
    
    // 检查连接是否有效
    bool IsConnectionValid(int sockfd);
    
    // 关闭连接
    void CloseConnection(int serverId);
    
    // 发送数据
    bool SendData(int sockfd, const std::vector<char>& data);
    
    // 接收数据
    std::vector<char> ReceiveData(int sockfd);
    
    // 序列化AppendEntries请求
    std::vector<char> SerializeAppendEntries(const AppendEntriesRequest& request);
    
    // 反序列化AppendEntries响应
    AppendEntriesResponse DeserializeAppendEntriesResponse(const std::vector<char>& data);
    
    // 序列化RequestVote请求
    std::vector<char> SerializeRequestVote(const RequestVoteRequest& request);
    
    // 反序列化RequestVote响应
    RequestVoteResponse DeserializeRequestVoteResponse(const std::vector<char>& data);
    
    // 序列化InstallSnapshot请求
    std::vector<char> SerializeInstallSnapshot(const InstallSnapshotRequest& request);
    
    // 反序列化InstallSnapshot响应
    InstallSnapshotResponse DeserializeInstallSnapshotResponse(const std::vector<char>& data);
    
    // 监听连接
    void Listen();
    
    // 处理连接
    void HandleConnection(int client_fd);
    
    // 反序列化AppendEntries请求
    AppendEntriesRequest DeserializeAppendEntries(const std::vector<char>& data);
    
    // 反序列化RequestVote请求
    RequestVoteRequest DeserializeRequestVote(const std::vector<char>& data);
    
    // 反序列化InstallSnapshot请求
    InstallSnapshotRequest DeserializeInstallSnapshot(const std::vector<char>& data);
    
    // 序列化AppendEntries响应
    std::vector<char> SerializeAppendEntriesResponse(const AppendEntriesResponse& response);
    
    // 序列化RequestVote响应
    std::vector<char> SerializeRequestVoteResponse(const RequestVoteResponse& response);
    
    // 序列化InstallSnapshot响应
    std::vector<char> SerializeInstallSnapshotResponse(const InstallSnapshotResponse& response);
    
    // 连接维护线程函数
    void ConnectionMaintenance();
    
    // 尝试连接单个节点
    bool TryConnect(int serverId);
    
    // 计算下次重试时间（随机指数退避算法）
    std::chrono::steady_clock::time_point CalculateNextRetryTime(int retry_count);
    
    // 初始化所有连接
    void InitializeConnections();
    
    // 检查并更新连接状态
    void CheckAndUpdateConnections();
};

} // namespace dkv
