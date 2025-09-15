#include "dkv_server.hpp"
#include "dkv_memory_allocator.hpp"
#include <iostream>
#include <fstream>
#include <sstream>
#include <chrono>

namespace dkv {

DKVServer::DKVServer(int port) 
    : port_(port), running_(false), cleanup_running_(false), max_memory_(0) {
}

DKVServer::~DKVServer() {
    stop();
}

bool DKVServer::start() {
    if (running_) {
        return true;
    }
    
    if (!initialize()) {
        return false;
    }
    
    running_ = true;
    cleanup_running_ = true;
    
    // 启动清理线程
    cleanup_thread_ = std::thread(&DKVServer::cleanupExpiredKeys, this);
    
    // 启动网络服务器
    if (!network_server_->start()) {
        std::cerr << "启动网络服务失败" << std::endl;
        stop();
        return false;
    }
    
    std::cout << "DKV服务启动成功" << std::endl;
    return true;
}

void DKVServer::stop() {
    if (!running_) {
        return;
    }
    
    running_ = false;
    cleanup_running_ = false;
    
    // 停止网络服务器
    std::cout << "停止网络服务" << std::endl;
    if (network_server_) {
        network_server_->stop();
    }
    
    // 等待清理线程结束
    std::cout << "等待清理线程结束" << std::endl;
    if (cleanup_thread_.joinable()) {
        cleanup_thread_.join();
    }
    
    std::cout << "DKV服务已停止" << std::endl;
}

bool DKVServer::loadConfig(const std::string& config_file) {
    config_file_ = config_file;
    return parseConfigFile(config_file);
}

void DKVServer::setPort(int port) {
    port_ = port;
}

void DKVServer::setMaxMemory(size_t max_memory) {
    max_memory_ = max_memory;
}

size_t DKVServer::getKeyCount() const {
    return storage_engine_ ? storage_engine_->size() : 0;
}

uint64_t DKVServer::getTotalKeys() const {
    return storage_engine_ ? storage_engine_->getTotalKeys() : 0;
}

uint64_t DKVServer::getExpiredKeys() const {
    return storage_engine_ ? storage_engine_->getExpiredKeys() : 0;
}

bool DKVServer::isRunning() const {
    return running_.load();
}

size_t DKVServer::getMemoryUsage() const {
    return dkv::MemoryAllocator::getInstance().getCurrentUsage();
}

size_t DKVServer::getMaxMemory() const {
    return max_memory_;
}

bool DKVServer::initialize() {
    // 创建存储引擎实例
    storage_engine_ = std::make_unique<StorageEngine>();
    
    // 创建网络服务实例
    network_server_ = std::make_unique<NetworkServer>(port_);
    network_server_->setStorageEngine(storage_engine_.get());
    network_server_->setDKVServer(this);
    
    return true;
}

Response DKVServer::executeCommand(const Command& command) {
    if (!storage_engine_) {
        return Response(ResponseStatus::ERROR, "Storage engine not initialized");
    }
    
    // 检查是否是可能分配内存的命令
    bool mayAllocateMemory = true;
    switch (command.type) {
        // readonly list
        case CommandType::GET:
        case CommandType::EXISTS:
        case CommandType::SCARD:
        case CommandType::DBSIZE:
        case CommandType::INFO:
        case CommandType::SHUTDOWN:
            mayAllocateMemory = false;
            break;
        default:
            mayAllocateMemory = true;
            break;
    }
    
    // 如果是可能分配内存的命令，且设置了最大内存限制，则检查内存使用情况
    if (mayAllocateMemory && max_memory_ > 0) {
        size_t currentUsage = getMemoryUsage();
        if (currentUsage >= max_memory_) {
            return Response(ResponseStatus::ERROR, "OOM command not allowed when used memory > 'maxmemory'");
        }
    }
    
    switch (command.type) {
        case CommandType::SET: {
            if (command.args.size() < 2) {
                return Response(ResponseStatus::ERROR, "SET命令需要至少2个参数");
            }
            bool success;
            if (command.args.size() >= 4 && command.args[2] == "PX") {
                // 支持 PX 参数设置毫秒过期时间
                try {
                    int64_t ms = std::stoll(command.args[3]);
                    success = storage_engine_->set(command.args[0], command.args[1], ms / 1000);
                } catch (const std::invalid_argument&) {
                    return Response(ResponseStatus::ERROR, "无效的过期时间");
                }
            } else if (command.args.size() >= 4 && command.args[2] == "EX") {
                // 支持 EX 参数设置秒过期时间
                try {
                    int64_t seconds = std::stoll(command.args[3]);
                    success = storage_engine_->set(command.args[0], command.args[1], seconds);
                } catch (const std::invalid_argument&) {
                    return Response(ResponseStatus::ERROR, "无效的过期时间");
                }
            } else {
                success = storage_engine_->set(command.args[0], command.args[1]);
            }
            return Response(success ? ResponseStatus::OK : ResponseStatus::ERROR);
        }
        
        case CommandType::GET: {
            if (command.args.empty()) {
                return Response(ResponseStatus::ERROR, "GET命令需要1个参数");
            }
            std::string value = storage_engine_->get(command.args[0]);
            if (value.empty()) {
                return Response(ResponseStatus::NOT_FOUND);
            }
            return Response(ResponseStatus::OK, "", value);
        }
        
        case CommandType::DEL: {
            if (command.args.empty()) {
                return Response(ResponseStatus::ERROR, "DEL命令需要1个参数");
            }
            bool success = storage_engine_->del(command.args[0]);
            return Response(success ? ResponseStatus::OK : ResponseStatus::ERROR);
        }
        
        case CommandType::EXISTS: {
            if (command.args.empty()) {
                return Response(ResponseStatus::ERROR, "EXISTS命令需要1个参数");
            }
            bool exists = storage_engine_->exists(command.args[0]);
            return Response(ResponseStatus::OK, "", exists ? "1" : "0");
        }
        
        case CommandType::INCR: {
            if (command.args.empty()) {
                return Response(ResponseStatus::ERROR, "INCR命令需要1个参数");
            }
            int64_t value = storage_engine_->incr(command.args[0]);
            return Response(ResponseStatus::OK, "", std::to_string(value));
        }
        
        case CommandType::DECR: {
            if (command.args.empty()) {
                return Response(ResponseStatus::ERROR, "DECR命令需要1个参数");
            }
            int64_t value = storage_engine_->decr(command.args[0]);
            return Response(ResponseStatus::OK, "", std::to_string(value));
        }
        
        case CommandType::EXPIRE: {
            if (command.args.size() < 2) {
                return Response(ResponseStatus::ERROR, "EXPIRE命令需要2个参数");
            }
            try {
                int64_t seconds = std::stoll(command.args[1]);
                bool success = storage_engine_->expire(command.args[0], seconds);
                return Response(success ? ResponseStatus::OK : ResponseStatus::ERROR);
            } catch (const std::invalid_argument&) {
                return Response(ResponseStatus::ERROR, "无效的过期时间");
            }
        }
        
        case CommandType::TTL: {
            if (command.args.empty()) {
                return Response(ResponseStatus::ERROR, "TTL命令需要1个参数");
            }
            int64_t ttl = storage_engine_->ttl(command.args[0]);
            return Response(ResponseStatus::OK, "", std::to_string(ttl));
        }
        
        // 哈希命令
        case CommandType::HSET: {
            if (command.args.size() < 3) {
                return Response(ResponseStatus::ERROR, "HSET命令需要至少3个参数");
            }
            bool success = storage_engine_->hset(command.args[0], command.args[1], command.args[2]);
            return Response(success ? ResponseStatus::OK : ResponseStatus::ERROR);
        }
        
        case CommandType::HGET: {
            if (command.args.size() < 2) {
                return Response(ResponseStatus::ERROR, "HGET命令需要至少2个参数");
            }
            std::string value = storage_engine_->hget(command.args[0], command.args[1]);
            if (value.empty()) {
                return Response(ResponseStatus::NOT_FOUND);
            }
            return Response(ResponseStatus::OK, "", value);
        }
        
        case CommandType::HGETALL: {
            if (command.args.empty()) {
                return Response(ResponseStatus::ERROR, "HGETALL命令需要1个参数");
            }
            std::vector<std::pair<Value, Value>> all = storage_engine_->hgetall(command.args[0]);
            
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
        
        case CommandType::HDEL: {
            if (command.args.size() < 2) {
                return Response(ResponseStatus::ERROR, "HDEL命令需要至少2个参数");
            }
            bool success = storage_engine_->hdel(command.args[0], command.args[1]);
            return Response(success ? ResponseStatus::OK : ResponseStatus::ERROR);
        }
        
        case CommandType::HEXISTS: {
            if (command.args.size() < 2) {
                return Response(ResponseStatus::ERROR, "HEXISTS命令需要至少2个参数");
            }
            bool exists = storage_engine_->hexists(command.args[0], command.args[1]);
            return Response(ResponseStatus::OK, "", exists ? "1" : "0");
        }
        
        case CommandType::HKEYS: {
            if (command.args.empty()) {
                return Response(ResponseStatus::ERROR, "HKEYS命令需要1个参数");
            }
            std::vector<Value> keys = storage_engine_->hkeys(command.args[0]);
            
            Response response;
            response.status = ResponseStatus::OK;
            response.data = RESPProtocol::serializeArray(keys);
            return response;
        }
        
        case CommandType::HVALS: {
            if (command.args.empty()) {
                return Response(ResponseStatus::ERROR, "HVALS命令需要1个参数");
            }
            std::vector<Value> values = storage_engine_->hvals(command.args[0]);
            
            Response response;
            response.status = ResponseStatus::OK;
            response.data = RESPProtocol::serializeArray(values);
            return response;
        }
        
        case CommandType::HLEN: {
            if (command.args.empty()) {
                return Response(ResponseStatus::ERROR, "HLEN命令需要1个参数");
            }
            size_t len = storage_engine_->hlen(command.args[0]);
            return Response(ResponseStatus::OK, "", std::to_string(len));
        }
        
        // 列表命令
        case CommandType::LPUSH: {
            if (command.args.size() < 2) {
                return Response(ResponseStatus::ERROR, "LPUSH命令需要至少2个参数");
            }
            size_t len = storage_engine_->lpush(command.args[0], command.args[1]);
            return Response(ResponseStatus::OK, "", std::to_string(len));
        }
        
        case CommandType::RPUSH: {
            if (command.args.size() < 2) {
                return Response(ResponseStatus::ERROR, "RPUSH命令需要至少2个参数");
            }
            size_t len = storage_engine_->rpush(command.args[0], command.args[1]);
            return Response(ResponseStatus::OK, "", std::to_string(len));
        }
        
        case CommandType::LPOP: {
            if (command.args.empty()) {
                return Response(ResponseStatus::ERROR, "LPOP命令需要1个参数");
            }
            std::string value = storage_engine_->lpop(command.args[0]);
            if (value.empty()) {
                return Response(ResponseStatus::NOT_FOUND);
            }
            return Response(ResponseStatus::OK, "", value);
        }
        
        case CommandType::RPOP: {
            if (command.args.empty()) {
                return Response(ResponseStatus::ERROR, "RPOP命令需要1个参数");
            }
            std::string value = storage_engine_->rpop(command.args[0]);
            if (value.empty()) {
                return Response(ResponseStatus::NOT_FOUND);
            }
            return Response(ResponseStatus::OK, "", value);
        }
        
        case CommandType::LLEN: {
            if (command.args.empty()) {
                return Response(ResponseStatus::ERROR, "LLEN命令需要1个参数");
            }
            size_t len = storage_engine_->llen(command.args[0]);
            return Response(ResponseStatus::OK, "", std::to_string(len));
        }
        
        case CommandType::LRANGE: {
            if (command.args.size() < 3) {
                return Response(ResponseStatus::ERROR, "LRANGE命令需要至少3个参数");
            }
            try {
                size_t start = std::stoull(command.args[1]);
                size_t stop = std::stoull(command.args[2]);
                std::vector<Value> values = storage_engine_->lrange(command.args[0], start, stop);
                
                Response response;
                response.status = ResponseStatus::OK;
                response.data = RESPProtocol::serializeArray(values);
                return response;
            } catch (const std::invalid_argument&) {
                return Response(ResponseStatus::ERROR, "无效的范围参数");
            }
        }
        
        // 集合命令
        case CommandType::SADD: {
            if (command.args.size() < 2) {
                return Response(ResponseStatus::ERROR, "SADD命令需要至少2个参数");
            }
            std::vector<Value> members(command.args.begin() + 1, command.args.end());
            size_t addedCount = storage_engine_->sadd(command.args[0], members);
            return Response(ResponseStatus::OK, "", std::to_string(addedCount));
        }
        
        case CommandType::SREM: {
            if (command.args.size() < 2) {
                return Response(ResponseStatus::ERROR, "SREM命令需要至少2个参数");
            }
            std::vector<Value> members(command.args.begin() + 1, command.args.end());
            size_t removedCount = storage_engine_->srem(command.args[0], members);
            return Response(ResponseStatus::OK, "", std::to_string(removedCount));
        }
        
        case CommandType::SMEMBERS: {
            if (command.args.empty()) {
                return Response(ResponseStatus::ERROR, "SMEMBERS命令需要1个参数");
            }
            std::vector<Value> members = storage_engine_->smembers(command.args[0]);
            
            Response response;
            response.status = ResponseStatus::OK;
            response.data = RESPProtocol::serializeArray(members);
            return response;
        }
        
        case CommandType::SISMEMBER: {
            if (command.args.size() < 2) {
                return Response(ResponseStatus::ERROR, "SISMEMBER命令需要至少2个参数");
            }
            bool is_member = storage_engine_->sismember(command.args[0], command.args[1]);
            return Response(ResponseStatus::OK, "", is_member ? "1" : "0");
        }
        
        case CommandType::SCARD: {
            if (command.args.empty()) {
                return Response(ResponseStatus::ERROR, "SCARD命令需要1个参数");
            }
            size_t count = storage_engine_->scard(command.args[0]);
            return Response(ResponseStatus::OK, "", std::to_string(count));
        }
        
        case CommandType::FLUSHDB: {
            storage_engine_->flush();
            return Response(ResponseStatus::OK, "OK");
        }
        
        case CommandType::DBSIZE: {
            size_t size = storage_engine_->size();
            return Response(ResponseStatus::OK, "", std::to_string(size));
        }
        
        case CommandType::INFO: {
            std::string info = "# DKV Server Info\r\n";
            info += "total_keys:" + std::to_string(getTotalKeys()) + "\r\n";
            info += "expired_keys:" + std::to_string(getExpiredKeys()) + "\r\n";
            info += "current_keys:" + std::to_string(getKeyCount()) + "\r\n";
            info += "version:1.0.0\r\n";
            info += "used_memory:" + std::to_string(getMemoryUsage()) + "\r\n";
            info += "max_memory:" + std::to_string(getMaxMemory()) + "\r\n";
            
            // 详细内存统计信息，按行分割并添加到响应中
            std::string memory_stats = dkv::MemoryAllocator::getInstance().getStats();
            std::istringstream stats_stream(memory_stats);
            std::string line;
            while (std::getline(stats_stream, line)) {
                info += line + "\r\n";
            }
            
            return Response(ResponseStatus::OK, "", info);
        }
        
        case CommandType::SHUTDOWN: {
            // 异步停止服务器
            std::thread shutdown_thread([this]() {
                // 延迟一点时间，确保响应能够发送回去
                std::this_thread::sleep_for(std::chrono::milliseconds(100));
                stop();
            });
            shutdown_thread.detach();
            return Response(ResponseStatus::OK, "Shutting down...");
        }
        
        default:
            return Response(ResponseStatus::INVALID_COMMAND);
    }
}

void DKVServer::cleanupExpiredKeys() {
    while (cleanup_running_) {
        // 使用更短的睡眠时间，以便快速响应停止信号
        for (int i = 0; i < 60 && cleanup_running_; ++i) {
            std::this_thread::sleep_for(std::chrono::seconds(1));
        }
        
        if (cleanup_running_ && storage_engine_) {
            storage_engine_->cleanupExpiredKeys();
        }
    }
}

bool DKVServer::parseConfigFile(const std::string& config_file) {
    std::ifstream file(config_file);
    if (!file.is_open()) {
        std::cerr << "无法打开配置文件: " << config_file << std::endl;
        return false;
    }
    
    std::string line;
    while (std::getline(file, line)) {
        // 跳过注释和空行
        if (line.empty() || line[0] == '#') {
            continue;
        }
        
        std::istringstream iss(line);
        std::string key, value;
        
        if (iss >> key >> value) {
            if (key == "port") {
                port_ = std::stoi(value);
            } else if (key == "maxmemory") {
                max_memory_ = std::stoull(value);
            }
            // 可以在这里添加更多配置选项
        }
    }
    
    return true;
}

} // namespace dkv