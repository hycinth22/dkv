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
    CommandHandler(StorageEngine* storage_engine, AOFPersistence* aof_persistence, bool enable_aof, TransactionManager* transaction_manager);
    
    // 基本命令处理
    Response handleSetCommand(TransactionID tx_id, const Command& command, bool& need_inc_dirty);
    Response handleGetCommand(TransactionID tx_id, const Command& command);
    Response handleDelCommand(TransactionID tx_id, const Command& command, bool& need_inc_dirty);
    Response handleExistsCommand(TransactionID tx_id, const Command& command);
    Response handleIncrCommand(TransactionID tx_id, const Command& command, bool& need_inc_dirty);
    Response handleDecrCommand(TransactionID tx_id, const Command& command, bool& need_inc_dirty);
    Response handleExpireCommand(TransactionID tx_id, const Command& command, bool& need_inc_dirty);
    Response handleTtlCommand(TransactionID tx_id, const Command& command);
    
    // 哈希命令处理
    Response handleHSetCommand(TransactionID tx_id, const Command& command, bool& need_inc_dirty);
    Response handleHGetCommand(TransactionID tx_id, const Command& command);
    Response handleHGetAllCommand(TransactionID tx_id, const Command& command);
    Response handleHDeldCommand(TransactionID tx_id, const Command& command, bool& need_inc_dirty);
    Response handleHExistsCommand(TransactionID tx_id, const Command& command);
    Response handleHKeysCommand(TransactionID tx_id, const Command& command);
    Response handleHValsCommand(TransactionID tx_id, const Command& command);
    Response handleHLenCommand(TransactionID tx_id, const Command& command);
    
    // 列表命令处理
    Response handleLPushCommand(TransactionID tx_id, const Command& command, bool& need_inc_dirty);
    Response handleRPushCommand(TransactionID tx_id, const Command& command, bool& need_inc_dirty);
    Response handleLPopCommand(TransactionID tx_id, const Command& command, bool& need_inc_dirty);
    Response handleRPopCommand(TransactionID tx_id, const Command& command, bool& need_inc_dirty);
    Response handleLLenCommand(TransactionID tx_id, const Command& command);
    Response handleLRangeCommand(TransactionID tx_id, const Command& command);
    
    // 集合命令处理
    Response handleSAddCommand(TransactionID tx_id, const Command& command, bool& need_inc_dirty);
    Response handleSRemCommand(TransactionID tx_id, const Command& command, bool& need_inc_dirty);
    Response handleSMembersCommand(TransactionID tx_id, const Command& command);
    Response handleSIsMemberCommand(TransactionID tx_id, const Command& command);
    Response handleSCardCommand(TransactionID tx_id, const Command& command);
    
    // 有序集合命令处理
    Response handleZAddCommand(TransactionID tx_id, const Command& command, bool& need_inc_dirty);
    Response handleZRemCommand(TransactionID tx_id, const Command& command, bool& need_inc_dirty);
    Response handleZScoreCommand(TransactionID tx_id, const Command& command);
    Response handleZIsMemberCommand(TransactionID tx_id, const Command& command);
    Response handleZRankCommand(TransactionID tx_id, const Command& command);
    Response handleZRevRankCommand(TransactionID tx_id, const Command& command);
    Response handleZRangeCommand(TransactionID tx_id, const Command& command);
    Response handleZRevRangeCommand(TransactionID tx_id, const Command& command);
    Response handleZRangeByScoreCommand(TransactionID tx_id, const Command& command);
    Response handleZRevRangeByScoreCommand(TransactionID tx_id, const Command& command);
    Response handleZCountCommand(TransactionID tx_id, const Command& command);
    Response handleZCardCommand(TransactionID tx_id, const Command& command);
    
    // 位图命令处理
    Response handleSetBitCommand(TransactionID tx_id, const Command& command, bool& need_inc_dirty);
    Response handleGetBitCommand(TransactionID tx_id, const Command& command);
    Response handleBitCountCommand(TransactionID tx_id, const Command& command);
    Response handleBitOpCommand(TransactionID tx_id, const Command& command, bool& need_inc_dirty);
    
    // HyperLogLog命令处理
    Response handlePFAddCommand(TransactionID tx_id, const Command& command, bool& need_inc_dirty);
    Response handlePFCountCommand(TransactionID tx_id, const Command& command);
    Response handlePFMergeCommand(TransactionID tx_id, const Command& command, bool& need_inc_dirty);
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

    // 记录AOF命令
    void appendAOFCommand(const Command& command);
    void appendAOFCommands(const std::vector<Command>& commands);
    
    // 更新AOF持久化指针
    void setAofPersistence(AOFPersistence* aof_persistence);
private:
    StorageEngine* storage_engine_;  // 存储引擎指针
    AOFPersistence* aof_persistence_;  // AOF持久化指针
    TransactionManager* transaction_manager_;  // 事务管理器指针
    bool enable_aof_;  // 是否启用AOF
    
    // 通用参数验证
    bool validateParamCount(const Command& command, size_t min_count);
    bool validateParamCount(const Command& command, size_t min_count, size_t max_count);
};

} // namespace dkv

#endif // DKV_COMMAND_HANDLER_HPP