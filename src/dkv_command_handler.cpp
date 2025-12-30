#include "dkv_command_handler.hpp"
#include "dkv_server.hpp"
#include "dkv_logger.hpp"
#include "net/dkv_resp.hpp"
#include "dkv_datatypes.hpp"

#include <thread>
#include <chrono>

namespace dkv {

CommandHandler::CommandHandler(StorageEngine* storage_engine, AOFPersistence* aof_persistence, bool enable_aof)
    : storage_engine_(storage_engine), aof_persistence_(aof_persistence), enable_aof_(enable_aof) {
}

Response CommandHandler::handleSetCommand(TransactionID tx_id, const Command& command, bool& need_inc_dirty) {
    if (command.args.size() < 2) {
        return Response(ResponseStatus::ERROR, "SET命令需要至少2个参数");
    }
    
    std::string key = command.args[0];
    std::string value = command.args[1];
    bool success;
    
    if (command.args.size() >= 4 && command.args[2] == "PX") {
        // 支持 PX 参数设置毫秒过期时间
        try {
            int64_t ms = std::stoll(command.args[3]);
            success = storage_engine_->set(tx_id, key, value, ms / 1000);
            DKV_LOG_DEBUG("设置键 ", key.c_str(), " 带有过期时间 ", ms, " 毫秒");
        } catch (const std::invalid_argument&) {
            return Response(ResponseStatus::ERROR, "无效的过期时间");
        }
    } else if (command.args.size() >= 4 && command.args[2] == "EX") {
        // 支持 EX 参数设置秒过期时间
        try {
            int64_t seconds = std::stoll(command.args[3]);
            success = storage_engine_->set(tx_id, key, value, seconds);
            DKV_LOG_DEBUG("设置键 ", key.c_str(), " 带有过期时间 ", seconds, " 秒");
        } catch (const std::invalid_argument&) {
            return Response(ResponseStatus::ERROR, "无效的过期时间");
        }
    } else {
        success = storage_engine_->set(tx_id, key, value);
        DKV_LOG_DEBUG("设置键 ", key.c_str());
    }
    
    if (success) {
        need_inc_dirty = true;
        return Response(ResponseStatus::OK, "OK");
    } else {
        DKV_LOG_ERROR("设置键值失败: ", key.c_str());
        return Response(ResponseStatus::ERROR, "设置键值失败");
    }
}

Response CommandHandler::handleGetCommand(TransactionID tx_id, const Command& command) {
    if (command.args.empty()) {
        return Response(ResponseStatus::ERROR, "GET命令需要1个参数");
    }
    std::string key = command.args[0];
    DKV_LOG_DEBUG("获取键 ", key.c_str(), " 的值");
    std::string value = storage_engine_->get(tx_id, key);
    if (value.empty()) {
        DKV_LOG_DEBUG("键 ", key.c_str(), " 不存在");
        return Response(ResponseStatus::NOT_FOUND);
    }
    DKV_LOG_DEBUG("获取键 ", key.c_str(), " 的值成功");
    return Response(ResponseStatus::OK, "", value);
}

Response CommandHandler::handleDelCommand(TransactionID tx_id, const Command& command, bool& need_inc_dirty) {
    if (command.args.empty()) {
        return Response(ResponseStatus::ERROR, "DEL命令需要至少1个参数");
    }
    
    int deleted_count = 0;
    for (const auto& key : command.args) {
        bool success = storage_engine_->del(tx_id, key);
        if (success) {
            deleted_count++;
            need_inc_dirty = true;
            DKV_LOG_DEBUG("删除键 ", key.c_str(), " 成功");
        } else {
            DKV_LOG_DEBUG("键 ", key.c_str(), " 不存在，删除失败");
        }
    }
    
    return Response(ResponseStatus::OK, "", std::to_string(deleted_count));
}

Response CommandHandler::handleExistsCommand(TransactionID tx_id, const Command& command) {
    if (command.args.empty()) {
        return Response(ResponseStatus::ERROR, "EXISTS命令需要至少1个参数");
    }
    
    int exists_count = 0;
    for (const auto& key : command.args) {
        if (storage_engine_->exists(tx_id, key)) {
            exists_count++;
        }
    }
    
    return Response(ResponseStatus::OK, "", std::to_string(exists_count));
}

Response CommandHandler::handleIncrCommand(TransactionID tx_id, const Command& command, bool& need_inc_dirty) {
    if (command.args.empty()) {
        return Response(ResponseStatus::ERROR, "INCR命令需要1个参数");
    }
    int64_t value = storage_engine_->incr(tx_id, command.args[0]);
    need_inc_dirty = true;
    return Response(ResponseStatus::OK, "", std::to_string(value));
}

Response CommandHandler::handleDecrCommand(TransactionID tx_id, const Command& command, bool& need_inc_dirty) {
    if (command.args.empty()) {
        return Response(ResponseStatus::ERROR, "DECR命令需要1个参数");
    }
    int64_t value = storage_engine_->decr(tx_id, command.args[0]);
    need_inc_dirty = true;
    return Response(ResponseStatus::OK, "", std::to_string(value));
}

Response CommandHandler::handleExpireCommand(TransactionID tx_id, const Command& command, bool& need_inc_dirty) {
    if (command.args.size() < 2) {
        return Response(ResponseStatus::ERROR, "EXPIRE命令需要2个参数");
    }
    try {
        int64_t seconds = std::stoll(command.args[1]);
        bool success = storage_engine_->expire(tx_id, command.args[0], seconds);
        
        if (success) {
            need_inc_dirty = true;
            return Response(ResponseStatus::OK, "", "1"); // 1表示成功设置过期时间
        } else {
            return Response(ResponseStatus::OK, "", "0"); // 0表示未设置过期时间
        }
    } catch (const std::invalid_argument&) {
        return Response(ResponseStatus::ERROR, "无效的过期时间");
    }
}

Response CommandHandler::handleTtlCommand(TransactionID tx_id, const Command& command) {
    if (command.args.empty()) {
        return Response(ResponseStatus::ERROR, "TTL命令需要1个参数");
    }
    int64_t ttl = storage_engine_->ttl(tx_id, command.args[0]);
    return Response(ResponseStatus::OK, "", std::to_string(ttl));
}

// 哈希命令处理
Response CommandHandler::handleHSetCommand(TransactionID tx_id, const Command& command, bool& need_inc_dirty) {
    if (command.args.size() < 3 || command.args.size() % 2 == 0) {
        return Response(ResponseStatus::ERROR, "HSET命令需要奇数个参数(至少3个)");
    }
    
    int added_count = 0;
    const std::string& key = command.args[0];
    
    // 遍历所有字段值对
    for (size_t i = 1; i < command.args.size(); i += 2) {
        const std::string& field = command.args[i];
        const std::string& value = command.args[i + 1];
        
        bool success = storage_engine_->hset(tx_id, key, field, value);
        if (success) {
            added_count++;
            need_inc_dirty = true;
        }
    }
    
    return Response(ResponseStatus::OK, "", std::to_string(added_count));
}

Response CommandHandler::handleHGetCommand(TransactionID tx_id, const Command& command) {
    if (command.args.size() < 2) {
        return Response(ResponseStatus::ERROR, "HGET命令需要至少2个参数");
    }
    std::string value = storage_engine_->hget(tx_id, command.args[0], command.args[1]);
    if (value.empty()) {
        return Response(ResponseStatus::NOT_FOUND);
    }
    return Response(ResponseStatus::OK, "", value);
}

Response CommandHandler::handleHGetAllCommand(TransactionID tx_id, const Command& command) {
    if (command.args.empty()) {
        return Response(ResponseStatus::ERROR, "HGETALL命令需要1个参数");
    }
    std::vector<std::pair<Value, Value>> all = storage_engine_->hgetall(tx_id, command.args[0]);
    
    // 将结果转换为RESP协议数组格式
    std::vector<std::string> result;
    for (const auto& pair : all) {
        result.push_back(pair.first);
        result.push_back(pair.second);
    }
    
    Response response;
    response.status = ResponseStatus::OK;
    response.data = RESPProtocol::serializeArray(result);
    return response;
}

Response CommandHandler::handleHDeldCommand(TransactionID tx_id, const Command& command, bool& need_inc_dirty) {
    if (command.args.size() < 2) {
        return Response(ResponseStatus::ERROR, "HDEL命令需要至少2个参数");
    }
    int deleted_count = 0;
    const Key& key = command.args[0];
    
    for (size_t i = 1; i < command.args.size(); ++i) {
        const Value& field = command.args[i];
        bool success = storage_engine_->hdel(tx_id, key, field);
        if (success) {
            deleted_count++;
            need_inc_dirty = true;
        }
    }
    
    return Response(ResponseStatus::OK, "", std::to_string(deleted_count));
}

Response CommandHandler::handleHExistsCommand(TransactionID tx_id, const Command& command) {
    if (command.args.size() < 2) {
        return Response(ResponseStatus::ERROR, "HEXISTS命令需要至少2个参数");
    }
    bool exists = storage_engine_->hexists(tx_id, command.args[0], command.args[1]);
    return Response(ResponseStatus::OK, "", exists ? "1" : "0");
}

Response CommandHandler::handleHKeysCommand(TransactionID tx_id, const Command& command) {
    if (command.args.empty()) {
        return Response(ResponseStatus::ERROR, "HKEYS命令需要1个参数");
    }
    std::vector<Value> keys = storage_engine_->hkeys(tx_id, command.args[0]);
    
    Response response;
    response.status = ResponseStatus::OK;
    response.data = RESPProtocol::serializeArray(keys);
    return response;
}

Response CommandHandler::handleHValsCommand(TransactionID tx_id, const Command& command) {
    if (command.args.empty()) {
        return Response(ResponseStatus::ERROR, "HVALS命令需要1个参数");
    }
    std::vector<Value> values = storage_engine_->hvals(tx_id, command.args[0]);
    
    Response response;
    response.status = ResponseStatus::OK;
    response.data = RESPProtocol::serializeArray(values);
    return response;
}

Response CommandHandler::handleHLenCommand(TransactionID tx_id, const Command& command) {
    if (command.args.empty()) {
        return Response(ResponseStatus::ERROR, "HLEN命令需要1个参数");
    }
    size_t len = storage_engine_->hlen(tx_id, command.args[0]);
    return Response(ResponseStatus::OK, "", std::to_string(len));
}

// 列表命令处理
Response CommandHandler::handleLPushCommand(TransactionID tx_id, const Command& command, bool& need_inc_dirty) {
    if (command.args.size() < 2) {
        return Response(ResponseStatus::ERROR, "LPUSH命令需要至少2个参数");
    }
    size_t len = 0;
    const Key& key = command.args[0];
    
    // 处理多个值参数
    for (size_t i = 1; i < command.args.size(); ++i) {
        len = storage_engine_->lpush(tx_id, key, command.args[i]);
    }
    
    need_inc_dirty = true;
    return Response(ResponseStatus::OK, "", std::to_string(len));
}

Response CommandHandler::handleRPushCommand(TransactionID tx_id, const Command& command, bool& need_inc_dirty) {
    if (command.args.size() < 2) {
        return Response(ResponseStatus::ERROR, "RPUSH命令需要至少2个参数");
    }
    size_t len = 0;
    const Key& key = command.args[0];
    
    // 处理多个值参数
    for (size_t i = 1; i < command.args.size(); ++i) {
        len = storage_engine_->rpush(tx_id, key, command.args[i]);
    }
    
    need_inc_dirty = true;
    return Response(ResponseStatus::OK, "", std::to_string(len));
}

Response CommandHandler::handleLPopCommand(TransactionID tx_id, const Command& command, bool& need_inc_dirty) {
    if (command.args.empty()) {
        return Response(ResponseStatus::ERROR, "LPOP命令需要至少1个参数");
    }
    
    const Key& key = command.args[0];
    
    // 只有键参数的情况 - 返回单个元素
    if (command.args.size() == 1) {
        std::string value = storage_engine_->lpop(tx_id, key);
        if (value.empty()) {
            return Response(ResponseStatus::NOT_FOUND);
        }
        
        need_inc_dirty = true;
        return Response(ResponseStatus::OK, "", value);
    }
    
    // 有count参数的情况 - 返回元素列表
    try {
        size_t count = std::stoull(command.args[1]);
        std::vector<std::string> values;
        
        for (size_t i = 0; i < count; ++i) {
            std::string value = storage_engine_->lpop(tx_id, key);
            if (value.empty()) {
                break;  // 列表为空，停止弹出
            }
            values.push_back(value);
        }
        
        if (values.empty()) {
            return Response(ResponseStatus::NOT_FOUND);
        }
        
        need_inc_dirty = true;
        
        // 返回列表格式的响应
        Response response(ResponseStatus::OK);
        response.data = RESPProtocol::serializeArray(values);
        return response;
    } catch (const std::invalid_argument&) {
        return Response(ResponseStatus::ERROR, "count参数必须是正整数");
    } catch (const std::out_of_range&) {
        return Response(ResponseStatus::ERROR, "count参数太大");
    }
}

Response CommandHandler::handleRPopCommand(TransactionID tx_id, const Command& command, bool& need_inc_dirty) {
    if (command.args.empty()) {
        return Response(ResponseStatus::ERROR, "RPOP命令需要至少1个参数");
    }
    
    const Key& key = command.args[0];
    
    // 只有键参数的情况 - 返回单个元素
    if (command.args.size() == 1) {
        std::string value = storage_engine_->rpop(tx_id, key);
        if (value.empty()) {
            return Response(ResponseStatus::NOT_FOUND);
        }
        
        need_inc_dirty = true;
        return Response(ResponseStatus::OK, "", value);
    }
    
    // 有count参数的情况 - 返回元素列表
    try {
        size_t count = std::stoull(command.args[1]);
        std::vector<std::string> values;
        
        for (size_t i = 0; i < count; ++i) {
            std::string value = storage_engine_->rpop(tx_id, key);
            if (value.empty()) {
                break;  // 列表为空，停止弹出
            }
            values.push_back(value);
        }
        
        if (values.empty()) {
            return Response(ResponseStatus::NOT_FOUND);
        }
        
        need_inc_dirty = true;
        
        // 返回列表格式的响应
        Response response(ResponseStatus::OK);
        response.data = RESPProtocol::serializeArray(values);
        return response;
    } catch (const std::invalid_argument&) {
        return Response(ResponseStatus::ERROR, "count参数必须是正整数");
    } catch (const std::out_of_range&) {
        return Response(ResponseStatus::ERROR, "count参数太大");
    }
}

Response CommandHandler::handleLLenCommand(TransactionID tx_id, const Command& command) {
    if (command.args.empty()) {
        return Response(ResponseStatus::ERROR, "LLEN命令需要1个参数");
    }
    size_t len = storage_engine_->llen(tx_id, command.args[0]);
    return Response(ResponseStatus::OK, "", std::to_string(len));
}

Response CommandHandler::handleLRangeCommand(TransactionID tx_id, const Command& command) {
    if (command.args.size() < 3) {
        return Response(ResponseStatus::ERROR, "LRANGE命令需要至少3个参数");
    }
    try {
        size_t start = std::stoull(command.args[1]);
        size_t stop = std::stoull(command.args[2]);
        std::vector<Value> values = storage_engine_->lrange(tx_id, command.args[0], start, stop);
        
        Response response;
        response.status = ResponseStatus::OK;
        response.data = RESPProtocol::serializeArray(values);
        return response;
    } catch (const std::invalid_argument&) {
        return Response(ResponseStatus::ERROR, "无效的范围参数");
    }
}

// 集合命令处理
Response CommandHandler::handleSAddCommand(TransactionID tx_id, const Command& command, bool& need_inc_dirty) {
    if (command.args.size() < 2) {
        return Response(ResponseStatus::ERROR, "SADD命令需要至少2个参数");
    }
    std::vector<Value> members(command.args.begin() + 1, command.args.end());
    size_t addedCount = storage_engine_->sadd(tx_id, command.args[0], members);
    
    if (addedCount > 0) {
        need_inc_dirty = true;
    }
    
    return Response(ResponseStatus::OK, "", std::to_string(addedCount));
}

Response CommandHandler::handleSRemCommand(TransactionID tx_id, const Command& command, bool& need_inc_dirty) {
    if (command.args.size() < 2) {
        return Response(ResponseStatus::ERROR, "SREM命令需要至少2个参数");
    }
    std::vector<Value> members(command.args.begin() + 1, command.args.end());
    size_t removedCount = storage_engine_->srem(tx_id, command.args[0], members);
    
    if (removedCount > 0) {
        need_inc_dirty = true;
    }
    
    return Response(ResponseStatus::OK, "", std::to_string(removedCount));
}

Response CommandHandler::handleSMembersCommand(TransactionID tx_id, const Command& command) {
    if (command.args.empty()) {
        return Response(ResponseStatus::ERROR, "SMEMBERS命令需要1个参数");
    }
    std::vector<Value> members = storage_engine_->smembers(tx_id, command.args[0]);
    
    Response response;
    response.status = ResponseStatus::OK;
    response.data = RESPProtocol::serializeArray(members);
    return response;
}

Response CommandHandler::handleSIsMemberCommand(TransactionID tx_id, const Command& command) {
    if (command.args.size() < 2) {
        return Response(ResponseStatus::ERROR, "SISMEMBER命令需要至少2个参数");
    }
    bool is_member = storage_engine_->sismember(tx_id, command.args[0], command.args[1]);
    return Response(ResponseStatus::OK, "", is_member ? "1" : "0");
}

Response CommandHandler::handleSCardCommand(TransactionID tx_id, const Command& command) {
    if (command.args.empty()) {
        return Response(ResponseStatus::ERROR, "SCARD命令需要1个参数");
    }
    size_t count = storage_engine_->scard(tx_id, command.args[0]);
    return Response(ResponseStatus::OK, "", std::to_string(count));
}

// 有序集合命令处理
Response CommandHandler::handleZAddCommand(TransactionID tx_id, const Command& command, bool& need_inc_dirty) {
    if (command.args.size() < 3 || command.args.size() % 2 != 1) {
        return Response(ResponseStatus::ERROR, "ZADD命令需要奇数个参数（1个键名 + 多个分数-成员对）");
    }
    
    try {
        std::string key = command.args[0];
        std::vector<std::pair<Value, double>> members_with_scores;
        
        // 解析所有的分数-成员对
        for (size_t i = 1; i < command.args.size(); i += 2) {
            double score = std::stod(command.args[i]);
            std::string member = command.args[i + 1];
            members_with_scores.push_back({member, score});
        }
        
        size_t addedCount = storage_engine_->zadd(tx_id, key, members_with_scores);
        
        if (addedCount > 0) {
            need_inc_dirty = true;
        }
        
        return Response(ResponseStatus::OK, "", std::to_string(addedCount));
    } catch (const std::invalid_argument&) {
        return Response(ResponseStatus::ERROR, "无效的分数参数");
    }
}

Response CommandHandler::handleZRemCommand(TransactionID tx_id, const Command& command, bool& need_inc_dirty) {
    if (command.args.size() < 2) {
        return Response(ResponseStatus::ERROR, "ZREM命令需要至少2个参数");
    }
    
    std::string key = command.args[0];
    std::vector<Value> members(command.args.begin() + 1, command.args.end());
    size_t removedCount = storage_engine_->zrem(tx_id, key, members);
    
    if (removedCount > 0) {
        need_inc_dirty = true;
    }
    
    return Response(ResponseStatus::OK, "", std::to_string(removedCount));
}

Response CommandHandler::handleZScoreCommand(TransactionID tx_id, const Command& command) {
    if (command.args.size() < 2) {
        return Response(ResponseStatus::ERROR, "ZSCORE命令需要至少2个参数");
    }
    
    std::string key = command.args[0];
    std::string member = command.args[1];
    double score = -1.0;
    bool found = storage_engine_->zscore(tx_id, key, member, score);
    
    if (!found) {
        return Response(ResponseStatus::NOT_FOUND);
    }
    
    return Response(ResponseStatus::OK, "", std::to_string(score));
}

Response CommandHandler::handleZIsMemberCommand(TransactionID tx_id, const Command& command) {
    if (command.args.size() < 2) {
        return Response(ResponseStatus::ERROR, "ZISMEMBER命令需要至少2个参数");
    }
    
    bool is_member = storage_engine_->zismember(tx_id, command.args[0], command.args[1]);
    return Response(ResponseStatus::OK, "", is_member ? "1" : "0");
}

Response CommandHandler::handleZRankCommand(TransactionID tx_id, const Command& command) {
    if (command.args.size() < 2) {
        return Response(ResponseStatus::ERROR, "ZRANK命令需要至少2个参数");
    }
    
    size_t rank = 0;
    bool found = storage_engine_->zrank(tx_id, command.args[0], command.args[1], rank);
    
    if (!found) {
        return Response(ResponseStatus::NOT_FOUND);
    }
    
    return Response(ResponseStatus::OK, "", std::to_string(rank));
}

Response CommandHandler::handleZRevRankCommand(TransactionID tx_id, const Command& command) {
    if (command.args.size() < 2) {
        return Response(ResponseStatus::ERROR, "ZREVRANK命令需要至少2个参数");
    }
    
    size_t rank = 0;
    bool found = storage_engine_->zrevrank(tx_id, command.args[0], command.args[1], rank);
    
    if (!found) {
        return Response(ResponseStatus::NOT_FOUND);
    }
    
    return Response(ResponseStatus::OK, "", std::to_string(rank));
}

Response CommandHandler::handleZRangeCommand(TransactionID tx_id, const Command& command) {
    if (command.args.size() < 3) {
        return Response(ResponseStatus::ERROR, "ZRANGE命令需要至少3个参数");
    }
    
    try {
        size_t start = std::stoull(command.args[1]);
        size_t stop = std::stoull(command.args[2]);
        bool withScores = (command.args.size() >= 4 && command.args[3] == "WITHSCORES");
        
        std::vector<std::pair<Value, double>> members = 
            storage_engine_->zrange(tx_id, command.args[0], start, stop);
        
        Response response;
        response.status = ResponseStatus::OK;
        
        if (withScores) {
            std::vector<std::string> result;
            for (const auto& pair : members) {
                result.push_back(pair.first);
                result.push_back(std::to_string(pair.second));
            }
            response.data = RESPProtocol::serializeArray(result);
        } else {
            std::vector<std::string> result;
            for (const auto& pair : members) {
                result.push_back(pair.first);
            }
            response.data = RESPProtocol::serializeArray(result);
        }
        
        return response;
    } catch (const std::invalid_argument&) {
        return Response(ResponseStatus::ERROR, "无效的范围参数");
    }
}

Response CommandHandler::handleZRevRangeCommand(TransactionID tx_id, const Command& command) {
    if (command.args.size() < 3) {
        return Response(ResponseStatus::ERROR, "ZREVRANGE命令需要至少3个参数");
    }
    try {
        size_t start = std::stoull(command.args[1]);
        size_t stop = std::stoull(command.args[2]);
        bool withScores = (command.args.size() >= 4 && command.args[3] == "WITHSCORES");
        
        std::vector<std::pair<Value, double>> members = 
            storage_engine_->zrevrange(tx_id, command.args[0], start, stop);
        
        Response response;
        response.status = ResponseStatus::OK;
        
        if (withScores) {
            std::vector<std::string> result;
            for (const auto& pair : members) {
                result.push_back(pair.first);
                result.push_back(std::to_string(pair.second));
            }
            response.data = RESPProtocol::serializeArray(result);
        } else {
            std::vector<std::string> result;
            for (const auto& pair : members) {
                result.push_back(pair.first);
            }
            response.data = RESPProtocol::serializeArray(result);
        }
        
        return response;
    } catch (const std::invalid_argument&) {
        return Response(ResponseStatus::ERROR, "无效的范围参数");
    }
}

Response CommandHandler::handleZRangeByScoreCommand(TransactionID tx_id, const Command& command) {
    if (command.args.size() < 3) {
        return Response(ResponseStatus::ERROR, "ZRANGEBYSCORE命令需要至少3个参数");
    }
    
    try {
        double min = std::stod(command.args[1]);
        double max = std::stod(command.args[2]);
        bool withScores = (command.args.size() >= 4 && command.args[3] == "WITHSCORES");
        
        std::vector<std::pair<Value, double>> members = 
            storage_engine_->zrangebyscore(tx_id, command.args[0], min, max);
        
        Response response;
        response.status = ResponseStatus::OK;
        
        if (withScores) {
            std::vector<std::string> result;
            for (const auto& pair : members) {
                result.push_back(pair.first);
                result.push_back(std::to_string(pair.second));
            }
            response.data = RESPProtocol::serializeArray(result);
        } else {
            std::vector<std::string> result;
            for (const auto& pair : members) {
                result.push_back(pair.first);
            }
            response.data = RESPProtocol::serializeArray(result);
        }
        
        return response;
    } catch (const std::invalid_argument&) {
        return Response(ResponseStatus::ERROR, "无效的分数参数");
    }
}

Response CommandHandler::handleZRevRangeByScoreCommand(TransactionID tx_id, const Command& command) {
    if (command.args.size() < 3) {
        return Response(ResponseStatus::ERROR, "ZREVRANGEBYSCORE命令需要至少3个参数");
    }
    
    try {
        double max = std::stod(command.args[1]);
        double min = std::stod(command.args[2]);
        bool withScores = (command.args.size() >= 4 && command.args[3] == "WITHSCORES");
        
        std::vector<std::pair<Value, double>> members = 
            storage_engine_->zrevrangebyscore(tx_id, command.args[0], max, min);
        
        Response response;
        response.status = ResponseStatus::OK;
        
        if (withScores) {
            std::vector<std::string> result;
            for (const auto& pair : members) {
                result.push_back(pair.first);
                result.push_back(std::to_string(pair.second));
            }
            response.data = RESPProtocol::serializeArray(result);
        } else {
            std::vector<std::string> result;
            for (const auto& pair : members) {
                result.push_back(pair.first);
            }
            response.data = RESPProtocol::serializeArray(result);
        }
        
        return response;
    } catch (const std::invalid_argument&) {
        return Response(ResponseStatus::ERROR, "无效的分数参数");
    }
}

Response CommandHandler::handleZCountCommand(TransactionID tx_id, const Command& command) {
    if (command.args.size() < 3) {
        return Response(ResponseStatus::ERROR, "ZCOUNT命令需要至少3个参数");
    }
    
    try {
        double min = std::stod(command.args[1]);
        double max = std::stod(command.args[2]);
        size_t count = storage_engine_->zcount(tx_id, command.args[0], min, max);
        return Response(ResponseStatus::OK, "", std::to_string(count));
    } catch (const std::invalid_argument&) {
        return Response(ResponseStatus::ERROR, "无效的分数参数");
    }
}

Response CommandHandler::handleZCardCommand(TransactionID tx_id, const Command& command) {
    if (command.args.empty()) {
        return Response(ResponseStatus::ERROR, "ZCARD命令需要1个参数");
    }
    
    size_t count = storage_engine_->zcard(tx_id, command.args[0]);
    return Response(ResponseStatus::OK, "", std::to_string(count));
}

// 位图命令处理
Response CommandHandler::handleSetBitCommand(TransactionID tx_id, const Command& command, bool& need_inc_dirty) {
    if (command.args.size() < 3) {
        return Response(ResponseStatus::ERROR, "SETBIT命令需要至少3个参数");
    }
    try {
        Key key(command.args[0]);
        uint64_t offset = std::stoull(command.args[1]);
        int bit = std::stoi(command.args[2]);
        
        bool oldBit = storage_engine_->getBit(tx_id, key, offset);
        storage_engine_->setBit(tx_id, key, offset, bit != 0);
        need_inc_dirty = true;
        return Response(ResponseStatus::OK, "", std::to_string(oldBit ? 1 : 0));
    } catch (const std::invalid_argument&) {
        return Response(ResponseStatus::ERROR, "无效的参数类型");
    }
}

Response CommandHandler::handleGetBitCommand(TransactionID tx_id, const Command& command) {
    if (command.args.size() < 2) {
        return Response(ResponseStatus::ERROR, "GETBIT命令需要至少2个参数");
    }
    try {
        Key key(command.args[0]);
        uint64_t offset = std::stoull(command.args[1]);
        
        bool bit = storage_engine_->getBit(tx_id, key, offset);
        return Response(ResponseStatus::OK, "", std::to_string(bit ? 1 : 0));
    } catch (const std::invalid_argument&) {
        return Response(ResponseStatus::ERROR, "无效的参数类型");
    }
}

Response CommandHandler::handleBitCountCommand(TransactionID tx_id, const Command& command) {
    if (command.args.empty()) {
        return Response(ResponseStatus::ERROR, "BITCOUNT命令需要至少1个参数");
    }
    
    Key key(command.args[0]);
    uint64_t count = 0;
    
    if (command.args.size() == 1) {
        // 没有额外参数，统计整个位图
        count = storage_engine_->bitCount(tx_id, key);
    } else if (command.args.size() == 3) {
        // 有start和end参数，这些参数指的是字节索引
        try {
            uint64_t start = std::stoull(command.args[1]);
            uint64_t end = std::stoull(command.args[2]);
            count = storage_engine_->bitCount(tx_id, key, start, end);
        } catch (const std::invalid_argument&) {
            return Response(ResponseStatus::ERROR, "无效的参数类型");
        }
    } else {
        return Response(ResponseStatus::ERROR, "BITCOUNT命令参数数量不正确");
    }
    
    return Response(ResponseStatus::OK, "", std::to_string(count));
}

Response CommandHandler::handleBitOpCommand(TransactionID tx_id, const Command& command, bool& need_inc_dirty) {
    if (command.args.size() < 4) {
        return Response(ResponseStatus::ERROR, "BITOP命令需要至少4个参数");
    }
    
    const std::string& operation = command.args[0];
    Key destKey(command.args[1]);
    std::vector<Key> srcKeys;
    for (size_t i = 2; i < command.args.size(); ++i) {
        srcKeys.push_back(Key(command.args[i]));
    }
    
    bool success = storage_engine_->bitOp(tx_id, operation, destKey, srcKeys);
    
    if (success) {
        need_inc_dirty = true;
        return Response(ResponseStatus::OK, "", "1"); // 返回成功状态
    } else {
        return Response(ResponseStatus::ERROR, "BITOP操作失败");
    }
}

// HyperLogLog命令处理
Response CommandHandler::handlePFAddCommand(TransactionID tx_id, const Command& command, bool& need_inc_dirty) {
    if (command.args.size() < 2) {
        return Response(ResponseStatus::ERROR, "PFADD命令需要至少2个参数");
    }
    
    Key key(command.args[0]);
    std::vector<Value> elements;
    for (size_t i = 1; i < command.args.size(); ++i) {
        elements.push_back(Value(command.args[i]));
    }
    
    bool success = storage_engine_->pfadd(tx_id, key, elements);
    
    if (success) {
        need_inc_dirty = true;
    }
    
    return Response(ResponseStatus::OK, "", success ? "1" : "0");
}

Response CommandHandler::handlePFCountCommand(TransactionID tx_id, const Command& command) {
    if (command.args.empty()) {
        return Response(ResponseStatus::ERROR, "PFCOUNT命令需要至少1个参数");
    }
    
    uint64_t count = 0;
    if (command.args.size() == 1) {
        // 单个键
        count = storage_engine_->pfcount(tx_id, command.args[0]);
    } else {
        // 多个键，需要创建临时合并后的HyperLogLog
        // 注意：此实现简化了Redis的PFCOUNT多个键的处理，实际应合并后再计数
        // 这里仅返回第一个键的计数作为示例
        count = storage_engine_->pfcount(tx_id, command.args[0]);
    }
    
    return Response(ResponseStatus::OK, "", std::to_string(count));
}

Response CommandHandler::handlePFMergeCommand(TransactionID tx_id, const Command& command, bool& need_inc_dirty) {
    if (command.args.size() < 2) {
        return Response(ResponseStatus::ERROR, "PFMERGE命令需要至少2个参数");
    }
    
    Key destKey(command.args[0]);
    std::vector<Key> sourceKeys;
    for (size_t i = 1; i < command.args.size(); ++i) {
        sourceKeys.push_back(Key(command.args[i]));
    }
    
    bool success = storage_engine_->pfmerge(tx_id, destKey, sourceKeys);
    
    if (success) {
        need_inc_dirty = true;
        return Response(ResponseStatus::OK, "", "OK");
    } else {
        return Response(ResponseStatus::ERROR, "PFMERGE操作失败");
    }
}

Response CommandHandler::handleRestoreHLLCommand(const Command& command, bool& need_inc_dirty) {
    if (command.args.size() < 2) {
        return Response(ResponseStatus::ERROR, "RESTORE_HLL命令需要至少2个参数");
    }
    const Key& key = command.args[0];
    const Value& serialized_data = command.args[1];
    
    // 创建HyperLogLogItem并反序列化数据
    std::unique_ptr<HyperLogLogItem> hll_item = std::make_unique<HyperLogLogItem>();
    hll_item->deserialize(serialized_data);
    
    // 存储HyperLogLogItem到存储引擎
    storage_engine_->setDataItem(key, std::move(hll_item));
    
    DKV_LOG_DEBUG("RESTORE_HLL: 成功恢复键 ", key.c_str());
    need_inc_dirty = true;
    return Response(ResponseStatus::OK);
}

// 服务器管理命令处理
Response CommandHandler::handleFlushDBCommand(bool& need_inc_dirty) {
    storage_engine_->flush();
    need_inc_dirty = true;
    return Response(ResponseStatus::OK, "OK");
}

Response CommandHandler::handleDBSizeCommand() {
    size_t size = storage_engine_->size();
    return Response(ResponseStatus::OK, "", std::to_string(size));
}

Response CommandHandler::handleInfoCommand(size_t key_count, size_t expired_keys, size_t total_keys, 
                                        size_t memory_usage, size_t max_memory) {
    std::string info = "# DKV Server Info\r\n";
    info += "total_keys:" + std::to_string(total_keys) + "\r\n";
    info += "expired_keys:" + std::to_string(expired_keys) + "\r\n";
    info += "current_keys:" + std::to_string(key_count) + "\r\n";
    info += "version:1.0.0\r\n";
    info += "used_memory:" + std::to_string(memory_usage) + "\r\n";
    info += "max_memory:" + std::to_string(max_memory) + "\r\n";
    
    // 详细内存统计信息，按行分割并添加到响应中
    std::string memory_stats = dkv::MemoryAllocator::getInstance().getStats();
    std::istringstream stats_stream(memory_stats);
    std::string line;
    while (std::getline(stats_stream, line)) {
        info += line + "\r\n";
    }
    
    return Response(ResponseStatus::OK, "", info);
}

Response CommandHandler::handleShutdownCommand(DKVServer* server) {
    // 异步停止服务器
    std::thread shutdown_thread([server]() {
        // 延迟一点时间，确保响应能够发送回去
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        server->stop();
    });
    shutdown_thread.detach();
    return Response(ResponseStatus::OK, "Shutting down...");
}

// RDB持久化命令处理
Response CommandHandler::handleSaveCommand(const std::string& rdb_filename) {
    // 同步保存数据到RDB文件
    bool success = storage_engine_->saveRDB(rdb_filename);
    if (success) {
        DKV_LOG_INFO("同步RDB保存成功");
    } else {
        DKV_LOG_ERROR("同步RDB保存失败");
    }
    return Response(success ? ResponseStatus::OK : ResponseStatus::ERROR);
}

Response CommandHandler::handleBgSaveCommand(const std::string& rdb_filename) {
    // 异步保存数据到RDB文件
    std::thread save_thread([this, rdb_filename]() {
        bool success = storage_engine_->saveRDB(rdb_filename);
        if (success) {
            DKV_LOG_INFO("异步RDB保存成功");
        } else {
            DKV_LOG_ERROR("异步RDB保存失败");
        }
    });
    save_thread.detach();
    return Response(ResponseStatus::OK, "Background saving started");
}

// 记录AOF命令
void CommandHandler::appendAOFCommand(const Command& command) {
    if (enable_aof_ && aof_persistence_) {
        aof_persistence_->appendCommand(command);
    }
}
void CommandHandler::appendAOFCommands(const std::vector<Command>& commands) {
    if (enable_aof_ && aof_persistence_) {
        aof_persistence_->appendCommands(commands);
    }
}


void CommandHandler::setAofPersistence(AOFPersistence* aof_persistence) {
    aof_persistence_ = aof_persistence;
}

// 通用参数验证
bool CommandHandler::validateParamCount(const Command& command, size_t min_count) {
    return command.args.size() >= min_count;
}

bool CommandHandler::validateParamCount(const Command& command, size_t min_count, size_t max_count) {
    return command.args.size() >= min_count && command.args.size() <= max_count;
}

} // namespace dkv