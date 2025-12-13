#include "dkv_raft_statemachine.h"
#include "dkv_command_handler.hpp"
#include "dkv_resp.hpp"
#include "dkv_logger.hpp"
#include "dkv_server.hpp"
#include <fstream>
#include <sstream>
#include <string>
#include <cstdio>

namespace dkv {

RaftStateMachineManager::RaftStateMachineManager()
    : commandHandler_(nullptr), storageEngine_(nullptr), dkvServer_(nullptr) {
}

Response RaftStateMachineManager::DoOp(const RaftCommand& raft_cmd) {
    std::lock_guard<std::mutex> lock(mutex_);
    
    if (!dkvServer_) {
        DKV_LOG_ERROR("DKVServer未初始化");
        return Response(ResponseStatus::ERROR, "DKVServer not initialized");
    }
    
    try {
        // 调用DKVServer::executeCommand执行命令
        Response response = dkvServer_->doCommandNative(raft_cmd.db_command, raft_cmd.tx_id);
        
        DKV_LOG_DEBUGF("执行RAFT命令成功，命令类型: {}, 响应状态: {}", 
                     static_cast<int>(raft_cmd.db_command.type), static_cast<int>(response.status));
        
        return response;
    } catch (const std::exception& e) {
        DKV_LOG_ERROR("执行RAFT命令失败: ", e.what());
        
        // 返回错误响应
        return Response(ResponseStatus::ERROR, "RAFT command execution failed");
    }
}

void RaftStateMachineManager::SetDKVServer(DKVServer* server) {
    std::lock_guard<std::mutex> lock(mutex_);
    dkvServer_ = server;
    DKV_LOG_INFO("设置DKVServer成功");
}

std::vector<char> RaftStateMachineManager::Snapshot() {
    std::lock_guard<std::mutex> lock(mutex_);
    
    if (!storageEngine_) {
        DKV_LOG_ERROR("StorageEngine未初始化");
        return {};
    }
    
    try {
        // 使用临时文件保存快照
        std::string temp_snapshot_file = "./temp_raft_snapshot.rdb";
        
        // 保存RDB快照
        if (storageEngine_->saveRDB(temp_snapshot_file)) {
            // 读取快照文件内容
            std::ifstream file(temp_snapshot_file, std::ios::binary | std::ios::ate);
            if (file.is_open()) {
                std::streamsize size = file.tellg();
                file.seekg(0, std::ios::beg);
                
                std::vector<char> buffer(size);
                if (file.read(buffer.data(), size)) {
                    file.close();
                    
                    // 删除临时文件
                    std::remove(temp_snapshot_file.c_str());
                    
                    DKV_LOG_INFO("创建快照成功，快照大小: ", buffer.size());
                    return buffer;
                }
                file.close();
            }
            
            // 删除临时文件
            std::remove(temp_snapshot_file.c_str());
        }
        
        DKV_LOG_ERROR("创建快照失败");
        return {};
    } catch (const std::exception& e) {
        DKV_LOG_ERROR("创建快照失败: ", e.what());
        return {};
    }
}

void RaftStateMachineManager::Restore(const std::vector<char>& snapshot) {
    std::lock_guard<std::mutex> lock(mutex_);
    
    if (!storageEngine_) {
        DKV_LOG_ERROR("StorageEngine未初始化");
        return;
    }
    
    try {
        if (snapshot.empty()) {
            DKV_LOG_WARNING("空的快照数据");
            return;
        }
        
        DKV_LOG_INFO("从快照恢复，快照大小: ", snapshot.size());
        
        // 1. 写入临时文件
        std::string temp_snapshot_file = "./temp_raft_restore.rdb";
        std::ofstream file(temp_snapshot_file, std::ios::binary);
        if (!file.is_open()) {
            DKV_LOG_ERROR("创建临时快照文件失败: ", temp_snapshot_file);
            return;
        }
        
        if (file.write(snapshot.data(), snapshot.size())) {
            file.close();
            
            // 2. 从临时文件恢复数据
            if (storageEngine_->loadRDB(temp_snapshot_file)) {
                DKV_LOG_INFO("从快照恢复成功");
            } else {
                DKV_LOG_ERROR("从快照文件恢复失败: ", temp_snapshot_file);
            }
            
            // 3. 删除临时文件
            std::remove(temp_snapshot_file.c_str());
        } else {
            file.close();
            DKV_LOG_ERROR("写入临时快照文件失败: ", temp_snapshot_file);
            std::remove(temp_snapshot_file.c_str());
        }
    } catch (const std::exception& e) {
        DKV_LOG_ERROR("从快照恢复失败: ", e.what());
    }
}

void RaftStateMachineManager::SetCommandHandler(void* commandHandler) {
    std::lock_guard<std::mutex> lock(mutex_);
    commandHandler_ = commandHandler;
}

void RaftStateMachineManager::SetStorageEngine(StorageEngine* storageEngine) {
    std::lock_guard<std::mutex> lock(mutex_);
    storageEngine_ = storageEngine;
}

} // namespace dkv
