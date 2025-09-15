#include "dkv_storage.hpp"
#include "dkv_datatype_string.hpp"
#include <algorithm>
#include <mutex>

namespace dkv {

// StorageEngine 实现
bool StorageEngine::set(const Key& key, const Value& value) {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    
    // 检查键是否已存在且未过期
    auto it = data_.find(key);
    if (it != data_.end() && !isKeyExpired(key)) {
        // 更新现有值
        auto* string_item = dynamic_cast<StringItem*>(it->second.get());
        if (string_item) {
            string_item->setValue(value);
            return true;
        }
    }
    
    // 创建新的字符串项
    data_[key] = createStringItem(value);
    total_keys_++;
    return true;
}

bool StorageEngine::set(const Key& key, const Value& value, int64_t expire_seconds) {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    
    // 计算过期时间
    auto expire_time = Utils::getCurrentTime() + std::chrono::seconds(expire_seconds);
    
    // 检查键是否已存在且未过期
    auto it = data_.find(key);
    if (it != data_.end() && !isKeyExpired(key)) {
        // 更新现有值
        auto* string_item = dynamic_cast<StringItem*>(it->second.get());
        if (string_item) {
            string_item->setValue(value);
            string_item->setExpiration(expire_time);
            return true;
        }
    }
    
    // 创建新的字符串项
    data_[key] = createStringItem(value, expire_time);
    total_keys_++;
    return true;
}

std::string StorageEngine::get(const Key& key) {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    
    auto it = data_.find(key);
    if (it == data_.end() || isKeyExpired(key)) {
        return "";
    }
    
    auto* string_item = dynamic_cast<StringItem*>(it->second.get());
    if (string_item) {
        return string_item->getValue();
    }
    
    return "";
}

bool StorageEngine::del(const Key& key) {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    
    auto it = data_.find(key);
    if (it != data_.end()) {
        data_.erase(it);
        total_keys_--;
        return true;
    }
    
    return false;
}

bool StorageEngine::exists(const Key& key) {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    
    auto it = data_.find(key);
    return it != data_.end() && !isKeyExpired(key);
}

bool StorageEngine::expire(const Key& key, int64_t seconds) {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    
    auto it = data_.find(key);
    if (it == data_.end() || isKeyExpired(key)) {
        return false;
    }
    
    DataItem* item = it->second.get();
    if (item) {
        auto expire_time = Utils::getCurrentTime() + std::chrono::seconds(seconds);
        item->setExpiration(expire_time);
        return true;
    }
    
    return false;
}

int64_t StorageEngine::ttl(const Key& key) {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    
    auto it = data_.find(key);
    if (it == data_.end() || isKeyExpired(key)) {
        return -2; // 键不存在
    }
    
    DataItem* item = it->second.get();
    if (item && item->hasExpiration()) {
        auto now = Utils::getCurrentTime();
        auto expire_time = item->getExpiration();
        auto duration = std::chrono::duration_cast<std::chrono::seconds>(expire_time - now).count();
        return duration > 0 ? duration : -2; // 已过期但未清理
    }
    
    return -1; // 键存在但没有过期时间
}

int64_t StorageEngine::incr(const Key& key) {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    
    auto it = data_.find(key);
    if (it != data_.end() && !isKeyExpired(key)) {
        auto* string_item = dynamic_cast<StringItem*>(it->second.get());
        if (string_item) {
            std::string current_value = string_item->getValue();
            if (Utils::isNumeric(current_value)) {
                int64_t new_value = Utils::stringToInt(current_value) + 1;
                string_item->setValue(Utils::intToString(new_value));
                return new_value;
            }
        }
    }
    
    // 键不存在或不是数字，设置为1
    data_[key] = createStringItem("1");
    total_keys_++;
    return 1;
}

int64_t StorageEngine::decr(const Key& key) {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    
    auto it = data_.find(key);
    if (it != data_.end() && !isKeyExpired(key)) {
        auto* string_item = dynamic_cast<StringItem*>(it->second.get());
        if (string_item) {
            std::string current_value = string_item->getValue();
            if (Utils::isNumeric(current_value)) {
                int64_t new_value = Utils::stringToInt(current_value) - 1;
                string_item->setValue(Utils::intToString(new_value));
                return new_value;
            }
        }
    }
    
    // 键不存在或不是数字，设置为-1
    data_[key] = createStringItem("-1");
    total_keys_++;
    return -1;
}

void StorageEngine::flush() {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    data_.clear();
    total_keys_ = 0;
    expired_keys_ = 0;
}

size_t StorageEngine::size() const {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    return data_.size();
}

std::vector<Key> StorageEngine::keys() const {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    std::vector<Key> result;
    result.reserve(data_.size());
    
    for (const auto& pair : data_) {
        if (!isKeyExpired(pair.first)) {
            result.push_back(pair.first);
        }
    }
    
    return result;
}

uint64_t StorageEngine::getTotalKeys() const {
    return total_keys_.load();
}

uint64_t StorageEngine::getExpiredKeys() const {
    return expired_keys_.load();
}

void StorageEngine::cleanupExpiredKeys() {
    std::unique_lock<std::shared_mutex> lock(mutex_);
    
    auto it = data_.begin();
    while (it != data_.end()) {
        if (isKeyExpired(it->first)) {
            it = data_.erase(it);
            expired_keys_++;
        } else {
            ++it;
        }
    }
}

bool StorageEngine::isKeyExpired(const Key& key) const {
    auto it = data_.find(key);
    if (it == data_.end()) {
        return false;
    }
    
    return it->second->isExpired();
}

void StorageEngine::removeExpiredKey(const Key& key) {
    auto it = data_.find(key);
    if (it != data_.end()) {
        data_.erase(it);
        expired_keys_++;
    }
}

std::unique_ptr<DataItem> StorageEngine::createStringItem(const Value& value) {
    return std::make_unique<StringItem>(value);
}

std::unique_ptr<DataItem> StorageEngine::createStringItem(const Value& value, Timestamp expire_time) {
    return std::make_unique<StringItem>(value, expire_time);
}

// DataItemFactory 实现
std::unique_ptr<DataItem> DataItemFactory::create(DataType type, const std::string& data) {
    switch (type) {
        case DataType::STRING:
            return std::make_unique<StringItem>(data);
        default:
            return nullptr;
    }
}

std::unique_ptr<DataItem> DataItemFactory::create(DataType type, const std::string& data, Timestamp expire_time) {
    switch (type) {
        case DataType::STRING:
            return std::make_unique<StringItem>(data, expire_time);
        default:
            return nullptr;
    }
}

} // namespace dkv
