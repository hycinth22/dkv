#ifndef DKV_COMMAND_HANDLER_HPP
#define DKV_COMMAND_HANDLER_HPP

#include "dkv_core.hpp"
#include "dkv_storage.hpp"
#include "dkv_aof.hpp"
#include <memory>

namespace dkv {

class DKVServer;

// 命令处理器类，负责处理各种Redis命令
class CommandHandler {
public:
    // 构造函数，接收存储引擎和AOF持久化的引用
    CommandHandler(StorageEngine* storage_engine, AOFPersistence* aof_persistence, bool enable_aof);
    
    // 基本命令处理
    Response handleSetCommand(const Command& command, bool& need_inc_dirty);
    Response handleGetCommand(const Command& command);
    Response handleDelCommand(const Command& command, bool& need_inc_dirty);
    Response handleExistsCommand(const Command& command);
    Response handleIncrCommand(const Command& command, bool& need_inc_dirty);
    Response handleDecrCommand(const Command& command, bool& need_inc_dirty);
    Response handleExpireCommand(const Command& command, bool& need_inc_dirty);
    Response handleTtlCommand(const Command& command);
    
    // 哈希命令处理
    Response handleHSetCommand(const Command& command, bool& need_inc_dirty);
    Response handleHGetCommand(const Command& command);
    Response handleHGetAllCommand(const Command& command);
    Response handleHDeldCommand(const Command& command, bool& need_inc_dirty);
    Response handleHExistsCommand(const Command& command);
    Response handleHKeysCommand(const Command& command);
    Response handleHValsCommand(const Command& command);
    Response handleHLenCommand(const Command& command);
    
    // 列表命令处理
    Response handleLPushCommand(const Command& command, bool& need_inc_dirty);
    Response handleRPushCommand(const Command& command, bool& need_inc_dirty);
    Response handleLPopCommand(const Command& command, bool& need_inc_dirty);
    Response handleRPopCommand(const Command& command, bool& need_inc_dirty);
    Response handleLLenCommand(const Command& command);
    Response handleLRangeCommand(const Command& command);
    
    // 集合命令处理
    Response handleSAddCommand(const Command& command, bool& need_inc_dirty);
    Response handleSRemCommand(const Command& command, bool& need_inc_dirty);
    Response handleSMembersCommand(const Command& command);
    Response handleSIsMemberCommand(const Command& command);
    Response handleSCardCommand(const Command& command);
    
    // 有序集合命令处理
    Response handleZAddCommand(const Command& command, bool& need_inc_dirty);
    Response handleZRemCommand(const Command& command, bool& need_inc_dirty);
    Response handleZScoreCommand(const Command& command);
    Response handleZIsMemberCommand(const Command& command);
    Response handleZRankCommand(const Command& command);
    Response handleZRevRankCommand(const Command& command);
    Response handleZRangeCommand(const Command& command);
    Response handleZRevRangeCommand(const Command& command);
    Response handleZRangeByScoreCommand(const Command& command);
    Response handleZRevRangeByScoreCommand(const Command& command);
    Response handleZCountCommand(const Command& command);
    Response handleZCardCommand(const Command& command);
    
    // 位图命令处理
    Response handleSetBitCommand(const Command& command, bool& need_inc_dirty);
    Response handleGetBitCommand(const Command& command);
    Response handleBitCountCommand(const Command& command);
    Response handleBitOpCommand(const Command& command, bool& need_inc_dirty);
    
    // HyperLogLog命令处理
    Response handlePFAddCommand(const Command& command, bool& need_inc_dirty);
    Response handlePFCountCommand(const Command& command);
    Response handlePFMergeCommand(const Command& command, bool& need_inc_dirty);
    Response handleRestoreHLLCommand(const Command& command, bool& need_inc_dirty);
    
    // 服务器管理命令处理
    Response handleFlushDBCommand(bool& need_inc_dirty);
    Response handleDBSizeCommand();
    Response handleInfoCommand(size_t key_count, size_t expired_keys, size_t total_keys, 
                              size_t memory_usage, size_t max_memory);
    Response handleShutdownCommand(DKVServer* server);
    
    // RDB持久化命令处理
    Response handleSaveCommand(const std::string& rdb_filename);
    Response handleBgSaveCommand(const std::string& rdb_filename);

    // 检查命令是否可能分配内存
    bool isReadOnlyCommand(CommandType type);
    
    // 记录AOF命令
    void appendAOFCommand(const Command& command);
    
    // 更新AOF持久化指针
    void setAofPersistence(AOFPersistence* aof_persistence);
    
private:
    StorageEngine* storage_engine_;  // 存储引擎指针
    AOFPersistence* aof_persistence_;  // AOF持久化指针
    bool enable_aof_;  // 是否启用AOF
    
    // 通用参数验证
    bool validateParamCount(const Command& command, size_t min_count);
    bool validateParamCount(const Command& command, size_t min_count, size_t max_count);
};

} // namespace dkv

#endif // DKV_COMMAND_HANDLER_HPP