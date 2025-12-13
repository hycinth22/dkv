#pragma once

#include "dkv_raft.h"
#include <string>
#include <vector>
#include <memory>
#include <unordered_map>

namespace dkv {

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
    
    // 连接缓存
    std::unordered_map<int, int> connections_;
    
    // Raft实例指针
    std::weak_ptr<Raft> raft_;
    
    // 监听线程
    std::thread listener_thread_;
    
    // 监听运行标志
    std::atomic<bool> listener_running_{false};
    
    // 监听套接字
    int listen_fd_{-1};
    
    // 建立连接
    int EstablishConnection(int serverId);
    
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
};

} // namespace dkv
