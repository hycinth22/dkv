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
    explicit RaftTcpNetwork(const std::vector<std::string>& peers);
    
    // 析构函数
    ~RaftTcpNetwork() override;
    
    // 发送AppendEntries请求
    AppendEntriesResponse SendAppendEntries(int serverId, const AppendEntriesRequest& request) override;
    
    // 发送RequestVote请求
    RequestVoteResponse SendRequestVote(int serverId, const RequestVoteRequest& request) override;
    
    // 发送InstallSnapshot请求
    InstallSnapshotResponse SendInstallSnapshot(int serverId, const InstallSnapshotRequest& request) override;
    
private:
    // 集群节点列表
    std::vector<std::string> peers_;
    
    // 连接缓存
    std::unordered_map<int, int> connections_;
    
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
};

} // namespace dkv
