#include "dkv_raft_statemachine.h"
#include "dkv_command_handler.hpp"
#include "dkv_resp.hpp"
#include "dkv_logger.hpp"
#include <fstream>
#include <sstream>
#include <string>
#include <cstdio>

namespace dkv {

RaftStateMachineManager::RaftStateMachineManager()
    : commandHandler_(nullptr), storageEngine_(nullptr) {
}

std::vector<char> RaftStateMachineManager::DoOp(const std::vector<char>& command) {
    std::lock_guard<std::mutex> lock(mutex_);
    
    if (!commandHandler_) {
        DKV_LOG_ERROR("CommandHandler未初始化");
        return {};
    }
    
    try {
        // 将vector<char>转换为string
        std::string cmd_str(command.begin(), command.end());
        
        // 解析命令
        size_t pos = 0;
        Command cmd = RESPProtocol::parseCommand(cmd_str, pos);
        
        // 执行命令并获取结果
        auto* handler = static_cast<CommandHandler*>(commandHandler_);
        Response response;
        bool need_inc_dirty = false;
        
        // 根据命令类型调用相应的处理函数
        switch (cmd.type) {
            case CommandType::SET:
                response = handler->handleSetCommand(NO_TX, cmd, need_inc_dirty);
                break;
            case CommandType::GET:
                response = handler->handleGetCommand(NO_TX, cmd);
                break;
            case CommandType::DEL:
                response = handler->handleDelCommand(NO_TX, cmd, need_inc_dirty);
                break;
            case CommandType::EXISTS:
                response = handler->handleExistsCommand(NO_TX, cmd);
                break;
            case CommandType::INCR:
                response = handler->handleIncrCommand(NO_TX, cmd, need_inc_dirty);
                break;
            case CommandType::DECR:
                response = handler->handleDecrCommand(NO_TX, cmd, need_inc_dirty);
                break;
            case CommandType::EXPIRE:
                response = handler->handleExpireCommand(NO_TX, cmd, need_inc_dirty);
                break;
            case CommandType::TTL:
                response = handler->handleTtlCommand(NO_TX, cmd);
                break;
            // 哈希命令
            case CommandType::HSET:
                response = handler->handleHSetCommand(NO_TX, cmd, need_inc_dirty);
                break;
            case CommandType::HGET:
                response = handler->handleHGetCommand(NO_TX, cmd);
                break;
            case CommandType::HGETALL:
                response = handler->handleHGetAllCommand(NO_TX, cmd);
                break;
            case CommandType::HDEL:
                response = handler->handleHDeldCommand(NO_TX, cmd, need_inc_dirty);
                break;
            case CommandType::HEXISTS:
                response = handler->handleHExistsCommand(NO_TX, cmd);
                break;
            // 列表命令
            case CommandType::LPUSH:
                response = handler->handleLPushCommand(NO_TX, cmd, need_inc_dirty);
                break;
            case CommandType::RPUSH:
                response = handler->handleRPushCommand(NO_TX, cmd, need_inc_dirty);
                break;
            case CommandType::LPOP:
                response = handler->handleLPopCommand(NO_TX, cmd, need_inc_dirty);
                break;
            case CommandType::RPOP:
                response = handler->handleRPopCommand(NO_TX, cmd, need_inc_dirty);
                break;
            case CommandType::LLEN:
                response = handler->handleLLenCommand(NO_TX, cmd);
                break;
            // 集合命令
            case CommandType::SADD:
                response = handler->handleSAddCommand(NO_TX, cmd, need_inc_dirty);
                break;
            case CommandType::SREM:
                response = handler->handleSRemCommand(NO_TX, cmd, need_inc_dirty);
                break;
            case CommandType::SMEMBERS:
                response = handler->handleSMembersCommand(NO_TX, cmd);
                break;
            case CommandType::SISMEMBER:
                response = handler->handleSIsMemberCommand(NO_TX, cmd);
                break;
            case CommandType::SCARD:
                response = handler->handleSCardCommand(NO_TX, cmd);
                break;
            // 有序集合命令
            case CommandType::ZADD:
                response = handler->handleZAddCommand(NO_TX, cmd, need_inc_dirty);
                break;
            case CommandType::ZREM:
                response = handler->handleZRemCommand(NO_TX, cmd, need_inc_dirty);
                break;
            case CommandType::ZSCORE:
                response = handler->handleZScoreCommand(NO_TX, cmd);
                break;
            case CommandType::ZISMEMBER:
                response = handler->handleZIsMemberCommand(NO_TX, cmd);
                break;
            case CommandType::ZRANK:
                response = handler->handleZRankCommand(NO_TX, cmd);
                break;
            case CommandType::ZREVRANK:
                response = handler->handleZRevRankCommand(NO_TX, cmd);
                break;
            case CommandType::ZRANGE:
                response = handler->handleZRangeCommand(NO_TX, cmd);
                break;
            case CommandType::ZREVRANGE:
                response = handler->handleZRevRangeCommand(NO_TX, cmd);
                break;
            case CommandType::ZRANGEBYSCORE:
                response = handler->handleZRangeByScoreCommand(NO_TX, cmd);
                break;
            case CommandType::ZREVRANGEBYSCORE:
                response = handler->handleZRevRangeByScoreCommand(NO_TX, cmd);
                break;
            case CommandType::ZCOUNT:
                response = handler->handleZCountCommand(NO_TX, cmd);
                break;
            case CommandType::ZCARD:
                response = handler->handleZCardCommand(NO_TX, cmd);
                break;
            // 位图命令
            case CommandType::SETBIT:
                response = handler->handleSetBitCommand(NO_TX, cmd, need_inc_dirty);
                break;
            case CommandType::GETBIT:
                response = handler->handleGetBitCommand(NO_TX, cmd);
                break;
            case CommandType::BITCOUNT:
                response = handler->handleBitCountCommand(NO_TX, cmd);
                break;
            case CommandType::BITOP:
                response = handler->handleBitOpCommand(NO_TX, cmd, need_inc_dirty);
                break;
            // HyperLogLog命令
            case CommandType::PFADD:
                response = handler->handlePFAddCommand(NO_TX, cmd, need_inc_dirty);
                break;
            case CommandType::PFCOUNT:
                response = handler->handlePFCountCommand(NO_TX, cmd);
                break;
            case CommandType::PFMERGE:
                response = handler->handlePFMergeCommand(NO_TX, cmd, need_inc_dirty);
                break;
            default:
                response = Response(ResponseStatus::ERROR, "Unsupported command type");
                break;
        }
        
        // 序列化响应
        std::string serializedResponse = RESPProtocol::serializeResponse(response);
        
        // 将结果转换为vector<char>
        return std::vector<char>(serializedResponse.begin(), serializedResponse.end());
    } catch (const std::exception& e) {
        DKV_LOG_ERROR("执行RAFT命令失败: ", e.what());
        
        // 返回错误响应
        Response errorResponse(ResponseStatus::ERROR, "RAFT command execution failed");
        std::string serializedError = RESPProtocol::serializeResponse(errorResponse);
        return std::vector<char>(serializedError.begin(), serializedError.end());
    }
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
