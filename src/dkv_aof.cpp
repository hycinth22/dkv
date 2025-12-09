#include "dkv_aof.hpp"
#include "dkv_resp.hpp"
#include "dkv_logger.hpp"
#include "dkv_server.hpp"
#include "dkv_datatypes.hpp"
#include <filesystem>
#include <chrono>

namespace dkv {

AOFPersistence::AOFPersistence() : server_(nullptr),
    enabled_(false), recovering(false), 
    fsync_policy_(FsyncPolicy::EVERYSEC), running_(false), 
    rewrite_check_running_(false),
    auto_rewrite_percentage_(100), auto_rewrite_min_size_mb_(64), last_rewrite_size_(0) {
}

AOFPersistence::~AOFPersistence() {
    running_ = false;
    
    // 通知并重写检查线程
    rewrite_check_running_ = false;
    rewrite_check_cv_.notify_one();
    
    if (bg_fsync_thread_.joinable()) {
        bg_fsync_thread_.join();
    }
    
    if (bg_rewrite_check_thread_.joinable()) {
        bg_rewrite_check_thread_.join();
    }
    
    close();
}

void AOFPersistence::setServer(DKVServer* server) {
    server_ = server;
    DKV_LOG_DEBUG("Server reference set for AOF persistence");
}

bool AOFPersistence::initialize(const std::string& filename, FsyncPolicy fsync_policy) {
    if (enabled_) {
        DKV_LOG_WARNING("AOF is already initialized");
        return false;
    }

    filename_ = filename;
    fsync_policy_ = fsync_policy;
    last_fsync_time_ = std::chrono::system_clock::now();

    // 以追加模式打开文件，如果文件不存在则创建
    aof_file_.open(filename_, std::ios::out | std::ios::app | std::ios::binary);
    if (!aof_file_.is_open()) {
        DKV_LOG_ERROR("Failed to open AOF file: ", filename_);
        return false;
    }

    enabled_ = true;
    
    // 如果是EVERYSEC策略，启动后台线程
    if (fsync_policy_ == FsyncPolicy::EVERYSEC) {
        running_ = true;
        bg_fsync_thread_ = std::thread(&AOFPersistence::bgFsyncThreadFunc, this);
    }
    
    // 启动后台重写检查线程
    rewrite_check_running_ = true;
    bg_rewrite_check_thread_ = std::thread(&AOFPersistence::bgRewriteCheckThreadFunc, this);
    
    DKV_LOG_INFO("AOF initialized successfully with file: ", filename_);
    return true;
}

void AOFPersistence::close() {
    if (enabled_) {
        std::lock_guard<std::mutex> lock(file_mutex_);
        if (aof_file_.is_open()) {
            aof_file_.flush();
            aof_file_.close();
        }
        enabled_ = false;
        DKV_LOG_INFO("AOF closed");
    }
}

bool AOFPersistence::appendCommand(const Command& command) {
    if (!enabled_) {
        return true; // AOF未启用，视为成功
    }
    if (recovering) {
        return false; // 正在恢复中，忽略写入
    }

    std::lock_guard<std::mutex> lock(file_mutex_);
    if (!aof_file_.is_open()) {
        DKV_LOG_ERROR("AOF file is not open");
        return false;
    }

    bool result = writeCommandToFile(command);
    if (result) {
        fsyncIfNeeded();
    }
    return result;
}

bool AOFPersistence::appendCommands(const std::vector<Command>& commands) {
    if (!enabled_) {
        return true; // AOF未启用，视为成功
    }
    if (recovering) {
        return false; // 正在恢复中，忽略写入
    }

    std::lock_guard<std::mutex> lock(file_mutex_);
    if (!aof_file_.is_open()) {
        DKV_LOG_ERROR("AOF file is not open");
        return false;
    }

    bool result = true;
    for (const auto& command : commands) {
        result &= writeCommandToFile(command);
        if (!result) return false;
    }
    if (result) {
        fsyncIfNeeded();
    }
    return result;
}



bool AOFPersistence::writeCommandToFile(const Command& command) {
    try {
        // 将命令转换为RESP协议格式的数组
        std::vector<std::string> command_parts;
        
        // 添加命令类型对应的字符串
        command_parts.push_back(Utils::commandTypeToString(command.type));

        // 添加命令参数
        command_parts.insert(command_parts.end(), command.args.begin(), command.args.end());

        // 序列化为RESP协议格式
        std::string serialized = RESPProtocol::serializeArray(command_parts);

        // 写入文件
        aof_file_.write(serialized.c_str(), serialized.size());
        aof_file_.flush();

        return true;
    } catch (const std::exception& e) {
        DKV_LOG_ERROR("Error writing command to AOF: ", e.what());
        return false;
    }
}

bool AOFPersistence::loadFromFile(DKVServer* server) {
    if (!server) {
        DKV_LOG_ERROR("DKVServer is null");
        return false;
    }
    if (recovering) {
        return false;
    }

    std::ifstream file(filename_, std::ios::in | std::ios::binary);
    if (!file.is_open()) {
        DKV_LOG_ERROR("Failed to open AOF file for loading: ", filename_);
        return false;
    }

    try {
        recovering = true;
        // 读取文件内容
        std::string file_content((std::istreambuf_iterator<char>(file)),
                                std::istreambuf_iterator<char>());
        
        size_t pos = 0;
        while (pos < file_content.size()) {
            // 解析命令
            Command command = RESPProtocol::parseCommand(file_content, pos);
            if (command.type == CommandType::UNKNOWN) {
                DKV_LOG_WARNING("Failed to parse command in AOF file at position ", pos);
                continue;
            }

            // 执行命令
            server->executeCommand(command, NO_TX);

            DKV_LOG_DEBUG("Executing command from AOF: ", Utils::commandTypeToString(command.type));
        }

        recovering = false; 
        DKV_LOG_INFO("AOF file loaded successfully");
        return true;
    } catch (const std::exception& e) {
        recovering = false; 
        DKV_LOG_ERROR("Error loading AOF file: ", e.what());
        return false;
    }
}

bool AOFPersistence::rewrite(StorageEngine* storage_engine, const std::string& temp_filename) {
    if (!storage_engine || !enabled_) {
        DKV_LOG_ERROR("Invalid parameters for AOF rewrite");
        return false;
    }

    try {
        // 创建临时文件
        std::ofstream temp_file(temp_filename, std::ios::out | std::ios::binary);
        if (!temp_file.is_open()) {
            DKV_LOG_ERROR("Failed to create temporary file for AOF rewrite: ", temp_filename);
            return false;
        }

        // 获取所有键
        std::vector<Key> keys = storage_engine->keys();
        
        // 遍历所有键，将当前状态写入临时文件
        for (const auto& key : keys) {
            // 使用公共API检查键是否存在
            // 使用 0 作为特殊事务 ID，用于 AOF 重写
            TransactionID tx_id = 0;
            
            if (!storage_engine->exists(tx_id, key)) {
                continue;
            }
            
            DataItem* item = storage_engine->getDataItem(tx_id, key);
            if (!item) continue;
            
            // 根据数据类型执行不同的操作
            if (dynamic_cast<StringItem*>(item)) {
                // 处理字符串类型
                std::string value = storage_engine->get(tx_id, key);
                if (!value.empty()) {
                    Command command(CommandType::SET, {key, value});
                    // 写入临时文件
                    std::vector<std::string> command_parts;
                    command_parts.push_back(Utils::commandTypeToString(command.type));
                    command_parts.insert(command_parts.end(), command.args.begin(), command.args.end());
                    std::string serialized = RESPProtocol::serializeArray(command_parts);
                    temp_file.write(serialized.c_str(), serialized.size());
                    DKV_LOG_DEBUG("Rewriting string key: ", key);
                }
            } else if (dynamic_cast<HashItem*>(item)) {
                // 处理哈希类型
                std::vector<std::pair<Value, Value>> fields = storage_engine->hgetall(tx_id, key);
                for (const auto& [field, value] : fields) {
                    Command command(CommandType::HSET, {key, field, value});
                    std::vector<std::string> command_parts;
                    command_parts.push_back(Utils::commandTypeToString(command.type));
                    command_parts.insert(command_parts.end(), command.args.begin(), command.args.end());
                    std::string serialized = RESPProtocol::serializeArray(command_parts);
                    temp_file.write(serialized.c_str(), serialized.size());
                }
                DKV_LOG_DEBUG("Rewriting hash key: ", key);
            } else if (dynamic_cast<ListItem*>(item)) {
                // 处理列表类型
                std::vector<Value> elements = storage_engine->lrange(tx_id, key, 0, -1);
                for (const auto& element : elements) {
                    Command command(CommandType::RPUSH, {key, element});
                    std::vector<std::string> command_parts;
                    command_parts.push_back(Utils::commandTypeToString(command.type));
                    command_parts.insert(command_parts.end(), command.args.begin(), command.args.end());
                    std::string serialized = RESPProtocol::serializeArray(command_parts);
                    temp_file.write(serialized.c_str(), serialized.size());
                }
                DKV_LOG_DEBUG("Rewriting list key: ", key);
            } else if (dynamic_cast<SetItem*>(item)) {
                // 处理集合类型
                std::vector<Value> members = storage_engine->smembers(tx_id, key);
                Command command(CommandType::SADD, {key});
                command.args.insert(command.args.end(), members.begin(), members.end());
                std::vector<std::string> command_parts;
                command_parts.push_back(Utils::commandTypeToString(command.type));
                command_parts.insert(command_parts.end(), command.args.begin(), command.args.end());
                std::string serialized = RESPProtocol::serializeArray(command_parts);
                temp_file.write(serialized.c_str(), serialized.size());
                DKV_LOG_DEBUG("Rewriting set key: ", key);
            } else if (dynamic_cast<ZSetItem*>(item)) {
                // 处理有序集合类型
                std::vector<std::pair<Value, double>> members_with_scores = storage_engine->zrange(tx_id, key, 0, -1);
                for (const auto& [member, score] : members_with_scores) {
                    Command command(CommandType::ZADD, {key, std::to_string(score), member});
                    std::vector<std::string> command_parts;
                    command_parts.push_back(Utils::commandTypeToString(command.type));
                    command_parts.insert(command_parts.end(), command.args.begin(), command.args.end());
                    std::string serialized = RESPProtocol::serializeArray(command_parts);
                    temp_file.write(serialized.c_str(), serialized.size());
                }
                DKV_LOG_DEBUG("Rewriting zset key: ", key);
            } else if (dynamic_cast<BitmapItem*>(item)) {
                // 处理位图类型
                BitmapItem* bitmap_item = dynamic_cast<BitmapItem*>(item);
                if (bitmap_item) {
                    size_t bitmap_size = bitmap_item->size() * 8; // 转换为位数
                    
                    // 遍历位图中的每一位
                    for (size_t offset = 0; offset < bitmap_size; ++offset) {
                        if (bitmap_item->getBit(offset)) {
                            // 对于值为1的位，添加SETBIT命令
                            Command command(CommandType::SETBIT, {key, std::to_string(offset), "1"});
                            std::vector<std::string> command_parts;
                            command_parts.push_back(Utils::commandTypeToString(command.type));
                            command_parts.insert(command_parts.end(), command.args.begin(), command.args.end());
                            std::string serialized = RESPProtocol::serializeArray(command_parts);
                            temp_file.write(serialized.c_str(), serialized.size());
                        }
                    }
                    DKV_LOG_DEBUG("Rewriting bitmap key: ", key, " with ", bitmap_item->bitCount(), " bits set");
                }
            } else if (dynamic_cast<HyperLogLogItem*>(item)) {
                // 处理HyperLogLog类型
                HyperLogLogItem* hll_item = dynamic_cast<HyperLogLogItem*>(item);
                if (hll_item) {
                    // 序列化HyperLogLog对象的状态
                    std::string serialized = hll_item->serialize();
                    
                    // 写入特殊命令来恢复HyperLogLog状态
                    // 注意：这需要在加载时特殊处理
                    Command command(CommandType::RESTORE_HLL, {key, serialized});
                    std::vector<std::string> command_parts;
                    command_parts.push_back(Utils::commandTypeToString(command.type));
                    command_parts.insert(command_parts.end(), command.args.begin(), command.args.end());
                    std::string command_serialized = RESPProtocol::serializeArray(command_parts);
                    temp_file.write(command_serialized.c_str(), command_serialized.size());
                    
                    DKV_LOG_DEBUG("Rewriting hyperloglog key: ", key, " with cardinality ", hll_item->count());
                }
            }
            
            // 如果键有过期时间，添加EXPIRE命令
            if (item->hasExpiration()) {
                auto now = Utils::getCurrentTime();
                auto expire_time = item->getExpiration();
                auto duration = std::chrono::duration_cast<std::chrono::seconds>(expire_time - now).count();
                if (duration > 0) {
                    Command expire_command(CommandType::EXPIRE, {key, std::to_string(duration)});
                    std::vector<std::string> command_parts;
                    command_parts.push_back(Utils::commandTypeToString(expire_command.type));
                    command_parts.insert(command_parts.end(), expire_command.args.begin(), expire_command.args.end());
                    std::string serialized = RESPProtocol::serializeArray(command_parts);
                    temp_file.write(serialized.c_str(), serialized.size());
                }
            }
        }

        temp_file.flush();
        temp_file.close();

        // 替换原AOF文件
        std::filesystem::rename(temp_filename, filename_);

        // 更新上次重写后的文件大小
        last_rewrite_size_ = getFileSize();

        DKV_LOG_INFO("AOF rewrite completed successfully");
        return true;
    } catch (const std::exception& e) {
        DKV_LOG_ERROR("Error during AOF rewrite:", e.what());
        // 清理临时文件
        try {
            std::filesystem::remove(temp_filename);
        } catch (...) {}
        return false;
    }
}

void AOFPersistence::fsyncIfNeeded() {
    switch (fsync_policy_) {
        case FsyncPolicy::ALWAYS:
            aof_file_.flush();
            break;
        case FsyncPolicy::EVERYSEC:
            // EVERYSEC策略由后台线程处理，这里不做任何操作
            break;
        case FsyncPolicy::NEVER:
        default:
            // 不做任何操作
            break;
    }
}

void AOFPersistence::bgFsyncThreadFunc() {
    DKV_LOG_INFO("Background fsync thread started");
    
    while (running_) {
        // 每秒执行一次fsync
        std::this_thread::sleep_for(std::chrono::seconds(1));
        
        if (enabled_ && !recovering) {
            std::lock_guard<std::mutex> lock(file_mutex_);
            if (aof_file_.is_open()) {
                aof_file_.flush();
                last_fsync_time_.store(std::chrono::system_clock::now());
                DKV_LOG_DEBUG("Background fsync completed");
            }
        }
    }
    
    DKV_LOG_INFO("Background fsync thread stopped");
}

void AOFPersistence::bgRewriteCheckThreadFunc() {
    DKV_LOG_INFO("Background rewrite check thread started");
    
    while (rewrite_check_running_) {
        // 使用条件变量等待，允许提前唤醒
        std::unique_lock<std::mutex> lock(rewrite_check_mutex_);
        auto status = rewrite_check_cv_.wait_for(
            lock, 
            std::chrono::seconds(30), 
            [this] { return !rewrite_check_running_; }
        );
        
        // 如果是因为超时醒来，执行检查
        if (!status && enabled_ && !recovering) {
            // 检查是否需要重写
            if (shouldRewrite()) {
                DKV_LOG_INFO("Background rewrite check: AOF file needs to be rewritten");
                
                // 如果有服务器引用，执行异步重写
                if (server_) {
                    asyncRewrite(server_);
                } else {
                    DKV_LOG_WARNING("Cannot perform async rewrite: missing server instance reference");
                }
            }
        }
    }
    
    DKV_LOG_INFO("Background rewrite check thread stopped");
}

void AOFPersistence::asyncRewrite(DKVServer* server) {
    if (!server || !enabled_ || recovering) {
        DKV_LOG_ERROR("Invalid parameters for async AOF rewrite");
        return;
    }
    
    // 创建并启动新线程执行重写
    std::thread([this, server]() {
        DKV_LOG_INFO("Starting async AOF rewrite");
        
        try {
            // 使用临时文件名
            std::string temp_filename = filename_ + ".rewrite";
            
            // 执行重写
            if (rewrite(server->getStorageEngine(), temp_filename)) {
                DKV_LOG_INFO("Async AOF rewrite completed successfully");
            } else {
                DKV_LOG_ERROR("Async AOF rewrite failed");
            }
        } catch (const std::exception& e) {
            DKV_LOG_ERROR("Exception during async AOF rewrite: ", e.what());
        }
        
        DKV_LOG_INFO("Async AOF rewrite thread completed");
    }).detach(); // 使用detach使线程在后台独立运行
}

void AOFPersistence::setAutoRewriteParams(double percentage, size_t min_size_mb) {
    auto_rewrite_percentage_ = percentage;
    auto_rewrite_min_size_mb_ = min_size_mb;
    DKV_LOG_INFO("AOF auto-rewrite parameters set: percentage=", percentage, ", min_size=", min_size_mb, "MB");
}

bool AOFPersistence::shouldRewrite() {
    if (!enabled_ || recovering) {
        return false;
    }

    size_t current_size = getFileSize();
    size_t min_size_bytes = auto_rewrite_min_size_mb_ * 1024 * 1024; // 转换为字节

    // 检查是否满足最小文件大小条件
    if (current_size < min_size_bytes) {
        DKV_LOG_DEBUG("AOF file size (", current_size, ") is below min size threshold (", min_size_bytes, ")");
        return false;
    }

    // 如果是第一次重写，设置上次重写大小为当前大小
    if (last_rewrite_size_ == 0) {
        last_rewrite_size_ = current_size;
        DKV_LOG_DEBUG("Initializing last_rewrite_size to ", current_size);
        return false;
    }

    // 检查是否满足百分比增长条件
    double growth_percentage = ((double)(current_size - last_rewrite_size_) / last_rewrite_size_) * 100;
    bool should_rewrite = growth_percentage >= auto_rewrite_percentage_;
    
    DKV_LOG_DEBUG("AOF auto-rewrite check: current_size=", current_size, ", last_rewrite_size=", 
                 last_rewrite_size_, ", growth_percentage=", growth_percentage, ", should_rewrite=", should_rewrite);
    
    return should_rewrite;
}

size_t AOFPersistence::getFileSize() {
    std::lock_guard<std::mutex> lock(file_mutex_);
    
    // 保存当前文件位置
    std::streampos current_pos = aof_file_.tellp();
    
    // 移动到文件末尾
    aof_file_.seekp(0, std::ios::end);
    
    // 获取文件大小
    std::streampos size = aof_file_.tellp();
    
    // 恢复到原来的位置
    aof_file_.seekp(current_pos);
    
    return static_cast<size_t>(size);
}

} // namespace dkv